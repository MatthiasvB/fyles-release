# Backend Architecture

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Workspace Structure

The backend is a Cargo workspace. Its main crates:

| Crate | Type | Purpose |
|-------|------|---------|
| `core` | lib | Domain logic, Brain orchestrator, trait ports, SQLite adapter, domain models, proto conversions |
| `p2p` | lib | Local-network P2P: libp2p swarm, mDNS, request-response protocol, session management |
| `p2p_internet` | lib | Internet P2P: Kademlia, relay, DCUtR, `KademliaFileTrackerPlugin` |
| `crypto` | lib | Cryptographic primitives: ChaCha20-Poly1305, Dilithium5, Ed25519, nonce management |
| `daemon` | bin | Desktop entrypoint — local-network mode |
| `daemon_internet` | bin | Desktop entrypoint — internet mode |
| `c_ffi` | lib (cdylib) | C FFI bridge for iOS (Swift calls Rust via C function pointers) |
| `kotlin_ffi` | lib (cdylib) | JNI bridge for Android (Kotlin calls Rust via JNI) |
| `zxcvbn_lite` | lib | Vendored zxcvbn 3.1.1 without dictionaries, MIT-licensed. Estimates backup passwords ([ADR-0012](adr/0012-password-encrypted-backups.md)) |

All crates share workspace-level dependency versions via `[workspace.dependencies]` in the root `Cargo.toml`.

---

## 2. The Core Crate

The `core` crate is the heart of the application. It defines the domain, the ports (traits), and the central orchestrator.

### 2.1 Trait Ports

These traits define the boundaries of the hexagonal architecture:

| Trait | File | Role |
|-------|------|------|
| `FilerequestDb` | `core/src/core/db.rs` | ~30 async CRUD operations for filerequests, contacts, peers, pending files, received files, settings |
| `HostController` | `core/src/io_controller.rs` | File system abstraction: read, write, create directories, rename, remove, backup, plus partial-transfer (`.part`) and temporary-outgoing-file management (see [§4 File Storage & Cleanup](#4-file-storage--cleanup)). Also carries host policy signals — power source and network cost — as a `watch::Receiver<HostState>`, defaulted to "unsupported" so a platform that cannot observe them needs no code (see [ADR-0008](adr/0008-host-run-conditions-for-internet-mode.md)) |
| `NetworkNode` | `core/src/core/p2p.rs` | P2P operations: send files, cancel, update identity, get node info |
| `BrainPlugin` | `core/src/core/brain/plugin.rs` | Behaviour that depends on a schema `core` must stay ignorant of. Reaches back only through `PluginAction`. `daemon` uses `NoOpBrainPlugin`; the internet builds use `p2p_internet::brain_plugin::InternetBrainPlugin` (see [ADR-0008](adr/0008-host-run-conditions-for-internet-mode.md)) |
| `RunnableFilerequestServer` | `core/src/core/api_server.rs` | API server lifecycle (start/stop) |

### 2.2 The Brain

`Brain` (`core/src/core/brain.rs`) is the central mediator/actor. It:

- Owns all ephemeral application state (`EphemeralBrainData`)
- Holds references to DB, P2P, and HostController as trait objects
- Processes `BrainAction` messages in an async `tokio::select!` loop
- Never touches libp2p, SQLite, or the filesystem directly

**Type aliases for injected dependencies:**
```rust
pub type BrainDb = Arc<dyn FilerequestDb>;
pub type BrainNetwork = Arc<dyn NetworkNode>;
pub type BrainApiServer = Box<dyn RunnableFilerequestServer>;
pub type BrainHostController = Arc<dyn HostController>;
```

### 2.3 Command Processing

Commands arrive as enum variants at two levels:

1. **`BrainAction`** — top-level discriminant: `Client(ClientAction)`, `NetworkNode(NetworkNodeAction)`, `Test(...)`
2. **`ClientAction`** (~40 variants) — every gRPC RPC maps to one variant
3. **`NetworkNodeAction`** (~20 variants) — every P2P-originated event maps to one variant

The `handle_action.rs` module dispatches each variant to a dedicated handler method.

### 2.4 Domain Models

Key models in `core/src/core/domain_models.rs`:

- `Filerequest` — file inbox with access control (public/audience)
- `Contact` / `SelfContact` — identity with public keys
- `Peer` — a device belonging to a contact
- `PendingFile` — file queued for sending, with `SendStatus` state machine
- `ReceivedFile` — successfully received file record
- `RemoteFilerequest` — a contact's filerequest that we can send files to

### 2.5 Entrypoint & Composition

`core/src/lib.rs` exposes the `entrypoint()` function — the **composition root**:

```rust
pub fn entrypoint(
    internal_data_dir: PathBuf,
    db_factory: impl AsyncFn() -> BrainDb,
    host_controller_factory: impl AsyncFn() -> BrainHostController,
    network_factory: impl AsyncFn(Sender<BrainAction>, CancellationToken) -> BrainNetwork,
    plugin_factory: impl AsyncFn(Sender<BrainAction>) -> BrainPluginRef,
    api_server_factory: impl AsyncFn(
        Sender<BrainAction>,
        Receiver<ServerMessage>,
        CancellationToken,
    ) -> BrainApiServer,
)
```

Each caller (daemon, kotlin_ffi, c_ffi) provides different concrete factories, enabling the same core logic to run on every platform. The internet builds share `p2p_internet::internet_network` and `internet_plugin`. The factories receive the ends of the current generation's channels and its `CancellationToken` (§2.6). Whatever they build must stop on that token.

### 2.6 Generations

The brain runs in generations (`run_generations` in `core/src/lib.rs`). Each one creates its own action channel, client push channel and `CancellationToken`, and builds the database, the host controller, the API server, the plugin and the network node from the factories. Only the tokio runtime outlives a generation.

A generation ends with a `GenerationEnd`:

- `Stop` comes from `ClientAction::Shutdown`. The loop returns and `entrypoint` with it.
- `Restart` comes from a successful restore. The restore replaces the database in place, answers the client and then ends the generation. Everything that read the database once, such as the node keypair, the signing identity and the cached internet settings, is read again by the next generation, exactly as at startup.

The teardown cancels the token and waits for every task the generation spawned, the action handlers included. Meanwhile it discards incoming actions, so a sender outside the generation cannot block it on a full channel. The API server is not cancelled from outside. It shuts down gracefully on the token, so an answer in flight, like the restore's own, still reaches the client. Open client streams end on the same token, which lets their connections close. Only then does the next generation bind the socket and the swarm ports again.

Clients see their stream end and reconnect, reading the TCP port and token afresh, then reload all data. That reload on every reattach is also how a client picks up a restore, so no "database restored" push exists, since it could never be timed reliably. Transfers in flight are dropped, as they would be by a process restart.

### 2.7 Database

**Production:** `Sqlite` (`core/src/library/sqlite/`) — uses `rusqlite` with a schema defined in `schema.rs`.

**Testing:** `MockDb` (`core/src/mocks/db.rs`) — `HashMap`-based in-memory implementation that faithfully replicates cascading deletes.

Both are validated with the same shared test suite in `db.rs`, ensuring behavioural equivalence.

### 2.8 Proto ↔ Domain Conversion

`domain_proto_conversion.rs` contains 100+ `From`, `Into`, and `TryFrom` implementations translating between generated `tonic`/`prost` types and domain models. This is the backend's anti-corruption layer.

---

## 3. Concurrency Model

```
                    ┌──────────────┐
     gRPC calls ──→ │  ApiServer   │ ──→ mpsc::Sender<BrainAction>
                    └──────────────┘                │
                                                    ▼
                    ┌──────────────┐         ┌──────────────┐
     P2P events ──→ │  EventLoop   │ ──→     │    Brain     │ (tokio::select! loop)
                    └──────────────┘         └──────┬───────┘
                          ▲                         │
          P2pCommand ─────┘                         ▼
          (mpsc)                              DB / HostController / P2P calls
```

- **No shared mutable state** between Brain and EventLoop
- **`oneshot` channels** for request-response correlation (`BrainRequest<T, U>`)
- **`InnerEvent::DoWithSwarm`** closures for safe swarm access from outside the event loop

---

## 4. File Storage & Cleanup

### 4.1 On-disk Layout

The `HostController` keeps everything received or in-flight under a single root:

```
<root>/
├── Received/<filerequest-title>/<file>   ← completed incoming files
├── Backup/<date>_<time>_fyles.bak.crypt  ← password-encrypted database backups (ADR-0012)
└── Partial/<part-file>.part              ← in-progress incoming transfers
```

A separate **outgoing** directory holds sender-side copies of files shared into the app via the OS share sheet:

```
<app-support>/fyles_outgoing/<ts>_<name>  ← shared-intent copies awaiting send
```

The physical location of each root is platform-specific; see [Deployment §3–5](deployment.md).

### 4.2 Motivation

Two sources of unbounded storage growth motivated a dedicated cleanup subsystem:

- **Sender-side copies.** When a user shares files into Fyles, the OS (`receive_sharing_intent`) hands Flutter a copy in temporary storage. Flutter re-copies it into `fyles_outgoing/` and passes that path to the backend as a `PendingFile`. Nothing deleted these copies after sending. A single source file shared to several remote filerequests produces several `PendingFile` rows pointing at the **same path**, so a naive "delete on send" would corrupt the sibling transfers.
- **Receiver-side `.part` files.** Each incoming transfer streams into a `.part` file. If a transfer is interrupted and never resumed, the `.part` lingers forever.

Two storage decisions fall out of this:

- **App-support, not cache, for `fyles_outgoing/`.** The OS may purge cache directories at any time — including mid-transfer. App-support storage is stable until the app explicitly deletes the file (or is uninstalled), so a long-running or resumed send can rely on it.
- **A dedicated `Partial/` directory** rather than `.part` files next to received files. This makes a filesystem sweep safe: anything in `Partial/` is, by construction, a transfer artifact, so an orphan can be deleted without risking a real user file that merely happens to end in `.part`.

### 4.3 HostController Ports

The cleanup flows are expressed entirely through `HostController` methods, keeping platform specifics behind the port:

| Method | Purpose |
|--------|---------|
| `get_partial_file_for_writing(filename)` | Open/create a `.part` file in `Partial/` for **append** (a resumed transfer must continue the same file) |
| `remove_partial_file(filename)` | Delete a `.part` file |
| `finalize_partial_file(partial, final_namespace)` | Move a completed `.part` to `Received/<filerequest>/<file>` |
| `list_partial_files()` | Names of all `.part` files (input to the sweep) |
| `remove_source_file_if_temporary(path)` | Delete a source file **only if** it lives in `fyles_outgoing/` |
| `list_temporary_outgoing_files()` | `Vec<OutgoingFileInfo { path, modified_ms }>` (input to the sweep) |
| `get_path_to_db_backup_file(file_name)` | Create the backup file. Core chooses the name, `{date}_{time}_fyles.bak.crypt` ([ADR-0012](adr/0012-password-encrypted-backups.md)) |

`.part` files are named deterministically so the exact name can be reconstructed at resume, finalize, abort, or cleanup time without extra bookkeeping:

```
{started_at_ms}_{contact_id}_{peer_id}_{transfer_id}.part
```

`FilerequestDriveHandler` (`core/src/core/filerequest_drive_handler.rs`) wraps these ports — it sanitizes filenames and scopes `finalize_partial_file` to the filerequest's own subdirectory, so `ReceiveManager` (in the `p2p` crate) never needs broad filesystem access.

### 4.4 Receiver-side `.part` Lifecycle

```
NewTransfer       → get_partial_file_for_writing → write data frames into Partial/<name>.part
ContinueTransfer  → reopen the same .part (append), resume from its current length
End (Done on v1)  → finalize_partial_file → move to Received/<filerequest>/<file>
Abort             → remove_partial_file, delete the ReceivedFile record
I/O error         → remove_partial_file, mark ReceivedFile = Failed
```

Because the file is opened for **append**, resume is just "reopen and keep writing" — the deterministic name guarantees the continuation hits the same file.

### 4.5 Sender-side Cleanup (Structured)

After any **terminal** send status (`FileSent`, `FileFailed`, `FileRejected`, `FileMissing`, `FileIoError`), `handle_action.rs` calls `try_cleanup_source_file`. It deletes the `fyles_outgoing/` copy only when **no other active `PendingFile` references the same path**, determined by the DB query `count_non_terminal_pending_files_for_path` (counts siblings still `Pending`/`Sending`/`Interrupted`). `remove_source_file_if_temporary` is itself a no-op for any path outside `fyles_outgoing/`, so desktop — where files are never copied into an outgoing dir — is unaffected.

### 4.6 Periodic Two-Phase Sweep

Structured cleanup catches the common case, but interrupted transfers and orphaned copies need a backstop. `core/src/core/brain/stale_cleanup.rs::run_cleanups` runs **on startup and every 6 hours** (spawned into the Brain's `JoinSet`). Anything older than `DEFAULT_STALE_THRESHOLD` (7 days, a `std::time::Duration`) is reclaimed in three steps:

1. **Structured partial cleanup** — `get_stale_received_files` returns `Receiving`/`Interrupted` rows older than the threshold; for each, reconstruct the `.part` name, `remove_partial_file`, and mark the row `Failed`.
2. **Partial sweep** — `list_partial_files`; for any `.part` whose `started_at_ms` filename prefix is older than the threshold (even if orphaned from the DB), delete it and log a warning.
3. **Outgoing sweep** — `list_temporary_outgoing_files`; delete any entry whose `modified_ms` is older than the threshold.

This is the **structured + sweep** pattern: the DB-driven phase handles known records; the filesystem phase reclaims orphans the DB no longer knows about. The threshold filename/mtime checks make the sweep idempotent and safe to run repeatedly.

---

## 5. Multi-Device Sync (CRDT)

A user's own devices ("owned devices") keep three pieces of state eventually-consistent with each other: their **contacts**, their **remote filerequests**, and the **owned-device list** itself. This is a peer-to-peer CRDT sync — there is no server of record — built on [automerge](https://automerge.org/). It runs **only between a user's own devices**, never between contacts.

Key files: `core/src/core/sync.rs` (document model + protocol primitives), `core/src/core/brain/handle_action.rs` (orchestration: `sync_advance`, `initiate_sync`, `handle_sync_receive`, `handle_sync_reply`), `core/src/core/brain.rs` (`EphemeralBrainData.sync_slots`), and the `p2p` crate's `SyncWire` request-response behaviour.

### 5.1 The Document Model

Each synced concern is one **automerge document** identified by a `DocKind` (`sync.rs`):

| `DocKind` | View type (autosurgeon) | Contents |
|-----------|-------------------------|----------|
| `Contacts` | `ContactsDoc { contacts: Map<ContactId, ContactEntry> }` | Contact `name` only — the key fingerprint *is* the `ContactId` it is keyed under, and the public keys themselves never enter the document (see §5.7) |
| `Devices` | `DevicesDoc { devices: Map<PeerIdWrapper, DeviceEntry> }` | This identity's owned devices (peer id → device name) |
| `RemoteFilerequests` | `RemoteFrDoc { remote_filerequests: Map<…> }` | Filerequests learned from other devices |

The document is stored as an opaque automerge blob per kind in the `sync_docs` table; the DB exposes only `get_sync_doc`/`save_sync_doc`/`mutate_sync_doc` and knows nothing about automerge. The Brain owns all CRDT semantics.

Two invariants matter:

- **Deterministic genesis.** Every document is created from `sync::genesis(kind)`, which seeds a fixed actor id (`fyles-sync-genesis-v1`) and timestamp for the *genesis change only*, then randomises the actor. Because the genesis change hash is byte-identical on every device, the top-level map gets the same object id everywhere, so entries merge **per key** instead of the whole container conflicting. A document must never start from a bare `AutoCommit::new()`.
- **Projections.** The synced doc is the source of truth, but the app reads contacts/devices/remote-frs from ordinary projected tables. Local mutations go through helpers (`add_owned_device`, `ensure_own_device_registered`, `update_contact_projection`, …) that update the automerge doc; the sync exchange propagates them; on convergence the receiver re-derives its projections.

### 5.2 Message Types

Request and response are **distinct types** (`sync.rs`), because an initiator and a responder can legitimately say different things — an illegal frame is unrepresentable:

- **`SyncRequest`** (initiator → responder): `Sync(SyncPayload)` in the normal case, or `InternalError { doc_kind, sync_id }` to abort and release the responder's parked state.
- **`SyncResponse`** (responder → initiator): `Sync(SyncPayload)`; `NoMoreToSay`; `DocumentVersionNotSupported { doc_kind, sync_id }`; or `InternalError { doc_kind, sync_id }`.
- **`SyncPayload { doc_kind, schema_major, message: Option<Vec<u8>>, sync_id }`** carries automerge's own output. `message == None` means "my automerge is quiescent this round" — this is a real automerge signal, fed to the peer's `sync_step`, and it is what drives convergence.

The crucial distinction: **`Sync { message: None }` is not `NoMoreToSay`.** `Sync{None}` is automerge speaking (fed to the peer's automerge). `NoMoreToSay` is a pure request-response artifact — it never reaches automerge and is sent only when we were forced to answer but did not consult automerge at all (a glare-loser, or a stale request on an already-settled slot). Conflating the two is what caused the historical infinite loop: a terminal that re-entered `sync_advance` on an idle slot restarted the handshake forever.

A single automerge step is `sync::sync_step(doc, state, incoming) -> Option<outgoing>`. **The `automerge::sync::State` must live in memory across the whole exchange** — `State::encode` deliberately keeps only `shared_heads` and drops the in-flight negotiation, so round-tripping it between steps would reset the protocol and never converge. It lives inside the exchange's `ActiveExchange`, never serialized.

### 5.3 The Exchange & Convergence

An exchange is a request/response ping-pong with two roles:

- **Initiator** — opens with `initiate_sync` → `sync_advance(kind, None)`, sends the first `Sync` request, then processes each `SyncResponse` in `handle_sync_reply` (`NetworkNodeAction::SyncReplyReceived`).
- **Responder** — processes each `SyncRequest` in `handle_sync_receive` (`NetworkNodeAction::SyncReceive`) and returns exactly one `SyncResponse`.

Only `Sync` frames flow through `sync_advance`; terminals are handled directly by the callers and never touch the slot machinery. A side **converges** (persists once, records `last_synced`, slot → `Idle`) at *bilateral quiescence*: it received a `Sync{None}` **and** its own `sync_step` also yields `None` (`msg_none && outgoing.is_none()`). Termination then rests on two rules:

- **The responder always answers with its automerge output**, i.e. `Sync{None}` when it is quiescent. That `Sync{None}` is what lets the initiator reach its own bilateral quiescence.
- **The initiator stops on convergence.** Once it converges it sends nothing further. Sending another request would land a terminal on the peer's *idle* slot and restart the handshake — the old loop.

Because the responder persists at the same bilateral-quiescence step (it produced the `Sync{None}` the initiator consumed), no one is stranded, and no terminal ever re-enters `sync_advance`.

```
Initiator (initiate_sync / handle_sync_reply)        Responder (handle_sync_receive)
  Sync(changes) ──────────────────────────────────▶  apply, gen reply
                ◀─────────────────────────────────── Sync(changes)
  apply, gen ∅                                        (always answers with automerge output)
  Sync(∅)       ──────────────────────────────────▶  apply nothing, gen ∅ ⇒ CONVERGED, persist
                ◀─────────────────────────────────── Sync(∅)
  incoming ∅ + outgoing ∅ ⇒ CONVERGED, persist, STOP
```

`NoMoreToSay` appears only off this happy path (a glare-loser answering to avoid a transport failure); its receipt at the initiator is a guaranteed no-op, since a genuine `NoMoreToSay` can only arrive after the initiator has already converged. `DocumentVersionNotSupported` / `InternalError` make the recipient abandon *that document's* slot (`abandon_sync_claim`). A `Sync` for an unknown `doc_kind` or incompatible `schema_major` is declined with `DocumentVersionNotSupported` — this is how a newer peer's document unknown to an older one is handled: it declines that one document and keeps syncing the shared kinds.

`DocKind::ALL` is synced independently — `initiate_sync` runs one exchange per kind. On convergence, `post_convergence` re-runs `initiate_sync_kind`; its dirty gate (§5.5) makes that a no-op once the stored doc matches `last_synced`, so it settles, but it catches a local write that raced the exchange or an initiate that was single-flight-bailed while another exchange was in flight.

### 5.4 Triggers & Session Gating

Sync is **not** driven by the client; it is opportunistic:

- **`SyncOnPeerConnect(peer)`** fires when we dial and connect to a peer (`event_loop.rs`). We push a sync **only if `is_owned_device(peer)`** — the peer is on our owned-device list.
- **`UpdateIdentity`** (a new device adopting the inviter's identity during pairing) resets its `Devices` doc, `ensure_own_device_registered`s itself, and `initiate_sync`s toward the inviter — this is how a freshly paired device converges without any prior connect trigger.

The accept side is gated independently: `handle_sync_receive` serves a request **only if the authenticated session carries our own contact id** (`self_contact_id == contact_id`). Both gates enforce the same rule from each end — sync happens strictly between a user's own devices. There is no periodic re-sync timer; a dropped exchange re-syncs on the next connect, and a `SyncTransportFailed` (libp2p transport error mid-exchange) drops the peer's slots so the next connect starts clean.

### 5.5 Coordination: Optimistic Single-Flight & Glare

At most one exchange runs per `(peer, DocKind)` at a time (single-flight), and the Brain spawns every action handler as its own task, so `sync_advance` must coordinate concurrent steps. State lives in `EphemeralBrainData.sync_slots: Map<(peer, kind), ExchangeSlot>`, where `ExchangeSlot { state: SlotState, last_synced }` and:

```
SlotState = Idle | Parked(ActiveExchange) | InFlight { sync_id }
```

Coordination is **optimistic**, guarded only by the enclosing ephemeral lock, which is taken briefly and **never held across DB or network I/O** (so two mutually-initiating peers cannot deadlock). `sync_advance` has three phases:

1. **Claim** (locked): take a parked `Parked` exchange to continue, start a `Fresh` one, or bail — stamping the slot `InFlight { sync_id }`.
2. **Step** (off-lock): load the document for a fresh exchange, run `sync_step`, and on convergence persist.
3. **Commit** (locked): a compare-and-swap — only park the exchange / record convergence **if the slot still carries our `sync_id`**.

**Glare** (both ends open an exchange for the same document at once) is resolved by a deterministic tie-break on `sync_id` — the lower id wins, and because both ends compare the same pair they agree without negotiation. The `sync_id` in `InFlight` lets a glare tie-break even against a step that is mid-flight and **supersede** it; the superseded step finds its id gone at commit and silently drops its work. That is safe because (a) the two exchanges never share memory and (b) persistence is an atomic *merge* (§5.6), so a superseded loser's persist is a harmless union, never a clobber. Bail/error paths restore the slot via `abandon_sync_claim`, itself CAS-guarded so it never clobbers a winner.

A **dirty gate** avoids pointless exchanges: `last_synced` is the fingerprint of the heads last confirmed shared with the peer; an initiation whose document still matches `last_synced` bails without sending.

### 5.6 Persistence

The receiver mutates its **in-memory** exchange document across the round-trips and writes to the DB **exactly once, at convergence** — observers never see a half-synced document. The write reloads the currently-stored blob, merges the exchange's document into it, and saves, so a local write that landed *during* the exchange survives. This runs as a single atomic `FilerequestDb::mutate_sync_doc(kind, transform)`: the single-threaded SQLite actor performs load→transform→save in one uninterruptible turn, so concurrent writers (a sync convergence and a local `ensure_own_device_registered`, say) cannot interleave a stale read with an overwrite and drop each other's changes. `last_synced` is then set from the exchange's confirmed `shared_heads`.

### 5.7 Public-Key Side Channel

The `Contacts` document never carries the (large) public keys — this keeps the CRDT small and avoids leaking key material through the doc. It does not need a separate fingerprint either: a `ContactId` is `SHA-256` of the contact's public keys ([ADR 0009](adr/0009-contact-id-derived-from-identity-keys.md)), so the map key already holds those bytes. After the `Contacts` document converges, `post_convergence` runs `prune_contact_key_cache` (drops keys for contacts removed elsewhere) and `fetch_missing_contact_keys`: it lists contacts present in the doc but missing their key bytes locally (`keyless_synced_contacts`) and fetches them over a separate `/fyles/pubkey-fetch` request-response protocol. The response carries keys alone, and the requester **derives the `ContactId` from them**: keys that derive an id the document does not hold are dropped, so a lying responder is harmless. Like sync, the pubkey-fetch responder serves only owned-device sessions.

### 5.8 Migrating a Synced Document

A synced document's shape is part of the wire contract between a user's own devices, so changing it is not a SQL migration with extra steps. Schema 1.24.0 is the first one, and these are the rules it established.

- **A migration must not use live domain types.** It reads data written by an older build, so it works on frozen copies of the document shapes and of any id encoding, kept beside the migration (`migrations/doc_v1.rs`). A live type that later changes, or starts validating, would silently change what an old migration produces. Document mechanism (genesis, load, hydrate, reconcile) stays shared, since a change to any of it is a `schema_major` bump in its own right.
- **Rewrite the loaded document, never a rebuilt one.** The synced document is authoritative and the SQL table is only a projection. A document rebuilt from SQL silently drops entries another device has not yet sent. Reconciling the new value into the loaded document deletes the old keys and keeps the history.
- **Bump `schema_major` so no cross-version merge can happen.** Devices then migrate independently and converge, as long as the transformation is a pure function of data both already hold. A peer that has not migrated declines the document with `DocumentVersionNotSupported` and keeps syncing the other kinds (§5.2).
- **Write the downgrade too.** A downgrade restores the older shape from what survives, and it cannot resurrect data the upgrade dropped. Where the upgrade replaced identifiers, the downgrade keeps the new ones rather than inventing replacements, so two downgraded devices still agree.

Verify convergence against document fixtures captured from a real pre-migration sync exchange, not from freshly built documents: only a captured pair carries genuine shared history followed by divergence. The 1.24.0 fixtures and proof are in `fyles_service/core/proofs/`.
