# Backend Architecture

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Workspace Structure

The backend is a Cargo workspace with 8 crates:

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

All crates share workspace-level dependency versions via `[workspace.dependencies]` in the root `Cargo.toml`.

---

## 2. The Core Crate

The `core` crate is the heart of the application. It defines the domain, the ports (traits), and the central orchestrator.

### 2.1 Trait Ports

These traits define the boundaries of the hexagonal architecture:

| Trait | File | Role |
|-------|------|------|
| `FilerequestDb` | `core/src/core/db.rs` | ~30 async CRUD operations for filerequests, contacts, peers, pending files, received files, settings |
| `HostController` | `core/src/io_controller.rs` | File system abstraction: read, write, create directories, rename, remove, backup, plus partial-transfer (`.part`) and temporary-outgoing-file management (see [§4 File Storage & Cleanup](#4-file-storage--cleanup)) |
| `NetworkNode` | `core/src/core/p2p.rs` | P2P operations: send files, cancel, update identity, get node info |
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
    db_factory: impl AsyncFactory<BrainDb>,
    host_controller_factory: impl AsyncFactory<BrainHostController>,
    network_node_factory: impl AsyncFactory<BrainNetwork>,
    api_server_factory: impl AsyncFactory<BrainApiServer>,
    brain_receiver: mpsc::Receiver<BrainAction>,
    client_sender: mpsc::Sender<ClientMessage>,
)
```

Each caller (daemon, kotlin_ffi, c_ffi) provides different concrete factories, enabling the same core logic to run on every platform.

### 2.6 Database

**Production:** `Sqlite` (`core/src/library/sqlite/`) — uses `rusqlite` with a schema defined in `schema.rs`.

**Testing:** `MockDb` (`core/src/mocks/db.rs`) — `HashMap`-based in-memory implementation that faithfully replicates cascading deletes.

Both are validated with the same shared test suite in `db.rs`, ensuring behavioural equivalence.

### 2.7 Proto ↔ Domain Conversion

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
├── Backup/<date>_fyles.bak               ← database backups
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

`.part` files are named deterministically so the exact name can be reconstructed at resume, finalize, abort, or cleanup time without extra bookkeeping:

```
{started_at_ms}_{contact_id}_{peer_id}_{transfer_id}.part
```

`FilerequestDriveHandler` (`core/src/core/filerequest_drive_handler.rs`) wraps these ports — it sanitizes filenames and scopes `finalize_partial_file` to the filerequest's own subdirectory, so `ReceiveManager` (in the `p2p` crate) never needs broad filesystem access.

### 4.4 Receiver-side `.part` Lifecycle

```
NewTransfer       → get_partial_file_for_writing → stream chunks into Partial/<name>.part
ContinueTransfer  → reopen the same .part (append), resume from its current length
Done              → finalize_partial_file → move to Received/<filerequest>/<file>
Abort / I/O error → remove_partial_file, mark ReceivedFile = Failed
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
