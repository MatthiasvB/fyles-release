# Networking & P2P Architecture

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Two-Tier Design

Fyles supports two networking modes, layered via a plugin architecture:

| Mode | Crate | Discovery | Transport | Use Case |
|------|-------|-----------|-----------|----------|
| **Local** | `p2p` | mDNS | Direct TCP + QUIC | LAN file sharing, zero config |
| **Internet** | `p2p_internet` | Kademlia DHT | Relay + DCUtR hole-punch | WAN file sharing, NAT traversal |

The `p2p` crate provides all shared infrastructure. `p2p_internet` extends it by composing additional libp2p behaviours and providing the `KademliaFileTrackerPlugin`.

---

## 2. libp2p Behaviour Composition

Behaviours are composed using `#[derive(NetworkBehaviour)]` (Composite pattern):

```
CoreBehaviour
├── FilerequestBehaviour       ← chunked file transfer protocol
├── SelfContactInviteBehaviour ← device-joining handshake
├── ContactShareBehaviour      ← contact exchange protocol
└── SessionEstablishmentBehaviour ← cryptographic session negotiation

LocalNetworkBehaviour
├── CoreBehaviour
└── Toggle<mDNS>               ← optional mDNS discovery

InternetBehaviour
├── CoreBehaviour
├── Kademlia                   ← DHT for peer/provider discovery
├── Identify                   ← peer identification
├── Relay                      ← relay client for NAT traversal
└── DCUtR                      ← direct connection upgrade
```

DCUtR hands the other side our hole-punch candidates. The internet event loop keeps them in sync
with two sources, never with the overall NAT verdict: the direct addresses AutoNAT confirmed
reachable, and the addresses our live relay reservations observed us on. A node that is not behind
NAT therefore still offers its public addresses, and a NATed node offers its live NAT mappings.

Kademlia serves the DHT only while the AutoNAT verdict is *public*. Left in its automatic mode it
would become a server as soon as any external address is confirmed, and the relay client confirms
every circuit address. A NATed node would then draw the public DHT's queries through its relays,
over circuits limited to minutes and kilobytes. The verdict (`AutoNatEval::verdict` in
`p2p_internet/src/types.rs`) has three states with a hysteresis band between them. *Behind NAT*
enables relay reservations, *public* switches Kademlia to server mode, and the *undecided* band in
between does neither. A desktop node starts undecided. Android and iOS start behind NAT, so their
relay reservations begin with the first identify burst instead of after the first failed probes.
Phones are rarely used as servers, and on iOS, which only runs the app while it is open, the
warm-up would delay nearly every use. Every node starts as a Kademlia client and
needs several successful probes before it serves the DHT.

### Protocol Versions

Each request-response behaviour is registered under a versioned protocol string such as
`/session-establishment/0.1.0`. libp2p treats these as **opaque**: `StreamProtocol`'s `PartialEq` is
byte-for-byte string comparison and nothing parses the version, so there is no major/minor
compatibility. Any change to the string means multistream-select finds no mutual protocol and the dial
fails with `UnsupportedProtocols`. Supporting two versions at once means registering both and handling
both.

The version number is therefore a signal to humans about the size of the change, not something libp2p
acts on.

The request-response codec is CBOR through serde, and serde writes a plain `Vec<u8>` as an array of
integers, nearly two bytes per byte for encrypted data. Every byte field that crosses these protocols,
`Encrypted::message` and the ML-KEM and ML-DSA adapters among them, is therefore serialized with
`serde_bytes` as a CBOR byte string. A new byte field needs the same. Its bincode encoding is the same
either way, so signatures and stored data do not depend on it.

---

## 3. P2pClient Façade

`P2pClient` (`p2p/src/lib.rs`) hides the complexity of the swarm behind the `NetworkNode` trait:

```rust
impl NetworkNode for P2pClient {
    async fn send_files(&self, files: Vec<FileToSend>) -> P2pResult<()>;
    async fn cancel_file(&self, file_id: FylesId) -> P2pResult<bool>;
    async fn get_node_info(&self) -> P2pResult<NodeStatusInfo>;
    async fn update_identity(&self, ...);
}
```

Internally, each method sends a `P2pCommand` through an `mpsc` channel and awaits the result via a `oneshot` channel.

---

## 4. Event Loop

`LocalEventLoop` (`p2p/src/event_loop.rs`) runs a `tokio::select!` loop processing three event sources:

1. **`swarm.select_next_some()`** — libp2p swarm events (connections, protocol messages)
2. **`command_receiver.recv()`** — `P2pCommand` from the `P2pClient` façade
3. **`inner_event_receiver.recv()`** — internal events (`DoWithSwarm` closures, timeouts)

### WithSwarm Pattern

Code outside the event loop accesses the swarm safely via `WithSwarm::with_swarm_res`:

```rust
async fn with_swarm_res<R, F>(&self, block: F) -> Result<R, SwarmWorkDiscarded> {
    let (sender, receiver) = oneshot::channel();
    self.send(InnerEvent::DoWithSwarm {
        block: Box::new(|swarm| { let _ = sender.send(block(swarm)); }),
    });
    receiver.await.map_err(|_| SwarmWorkDiscarded)
}
```

`SwarmWorkDiscarded` means the closure was dropped without ever reaching a swarm, because the swarm
it was queued against was rebuilt or the loop stopped. A rebuild
(`InternetEventLoop::handle_restart`) deliberately drains the queue for that reason: every event in
it addresses the old swarm, and a stale `DoWithSwarm` run against the new one would return a
meaningless request id.

Only the loop completes these round-trips, so the loop must never await anything another task can
hold. Its `select!` arm handlers are `fn`, not `async fn`. See *Event Loop Concurrency* in
[fyles_service/AGENTS.md](../../fyles_service/AGENTS.md).

---

## 5. File Transfer Protocol

Control messages travel over request-response on `/file-push/0.3.0`. The bytes of each transfer
attempt travel over one stream on `/fyles/file-data/0.1.0` (libp2p-stream). The decision and the
alternatives are in [ADR 0011](adr/0011-file-data-on-streams.md).

### Control

```rust
enum FileRequestV2 { NewTransfer { .. }, ContinueTransfer { .. }, AbortTransfer { .. }, ProtocolViolation { .. } }
enum FileResponseV2 {
    AcceptNewTransfer { grant }, AcceptContinueTransfer { offset, grant },
    RejectNewTransfer, RestartContinueTransfer, RejectContinueTransfer,
    ConfirmAbort, AcknowledgeProtocolViolation, ProtocolViolation, InternalError,
}
```

An accept carries an `AttemptGrant`: a random attempt id and a random key, minted by the receiver for
that one attempt. It travels inside the session-encrypted response.

### Transfer State Machine

```
Sender:   Pending → Prepared → Sending → Sent
                        ↓          ↓
                   Interrupted   Failed / Rejected

Receiver: Receiving → Completed
              ↓
         Interrupted / Failed
```

### Stream Transfer Flow

1. The sender offers with `NewTransfer`, or resumes with `ContinueTransfer`.
2. The receiver checks authorization, opens the part file and reserves the attempt for ten seconds.
   It answers with the grant, and for a continuation also with the offset to resume from.
3. The sender opens a data stream, writes the attempt id in clear, then sends frames: `Begin`, the
   file's bytes as `Data` frames from the offset on, and `End`.
4. The receiver admits the stream only once `Begin` authenticates. It writes each `Data` frame to
   the part file, refusing a frame that would cross the declared size, and reports `Progress` at
   most once a second.
5. On `End` with exactly the declared size written, the receiver flushes, moves the part file into
   place, records it, and answers `Committed`. The sender marks the file sent only on `Committed`.

A stream that ends any other way interrupts the attempt, and the transfer resumes later from the
part file's length. Frames are authenticated per direction with an implicit counter, so a frame
that is dropped, replayed, reordered or forged ends the attempt. See [Security](security.md#61-file-data-streams).

The sender's slot holds a running attempt as a cancellation handle. Clearing the slot for any
reason, a cancel or a reset, stops the attempt's task. The task reports exactly one outcome back to
the tracker, which ignores it unless the slot still holds that attempt.

The receiver holds the lock on its per-peer state only to read or change it. Whoever wants a
running receive task gone takes it out of the map, then cancels it and waits for it to flush, so
the task never waits on a lock that its stopper holds. A continuation that finds its own transfer
still running waits for that task before it measures the part file.

A resume must not splice two versions of a file together. The sender records the source's size
and modification time when it offers a transfer (`TransferData`), compares them before every
`ContinueTransfer`, and rechecks the open file at most once a second while sending and once more
after the last read, before `End`. A difference abandons the transfer: the sender sends
`AbortTransfer` naming it and offers the file again as a new transfer. The receiver retires a named
transfer whether or not it still holds it in memory, so the abandoned partial file and its record
are removed right away rather than by stale cleanup. The sender does not wait for the confirmation
before offering the new transfer. The abort names the abandoned transfer, so whichever arrives
first, it cannot affect the new one.

### Peers on the chunked protocol

Builds from before the stream protocol speak only `/file-push/0.2.0`, which carries the data as
`Chunk` requests, at most 8 in flight, and completes with `Done`. Both protocols run side by side
until those builds are retired:
- The sender offers over `/file-push/0.3.0` first. If the peer does not support it, the sender
  offers the same file over `/file-push/0.2.0` at once, with no retry counted, and keeps using it
  for that peer for ten minutes before asking again.
- The receiver serves both. An offer on either protocol replaces whatever the peer had going on
  the other.
- Both protocols name part files and count offsets the same way, so a transfer begun on one can
  resume on the other.

---

## 6. File Tracker & Plugin Architecture

`CoreFileTracker<P: FileTrackerPlugin>` manages the lifecycle of pending file transfers:

- Tracks which peers have pending files to send
- Coordinates connection attempts and retries
- Delegates peer-discovery strategy to the plugin

### Plugin Trait

```rust
pub trait FileTrackerPlugin {
    async fn pending_file_added_for_peer(&self, peer: PeerId, after_reset: bool);
    async fn pending_file_removed_for_peer(&mut self, ...);
    async fn on_send_file_to_peer(&mut self, peer: PeerId);
    async fn interrupt_sending_to_peer(&mut self, peer: PeerId);
    async fn done_sending_to_peer(&mut self, peer: PeerId);
}
```

| Plugin | Behaviour |
|--------|-----------|
| `NoOpFileTrackerPlugin` | No active discovery; relies on mDNS for peer appearance |
| `KademliaFileTrackerPlugin` | Queries Kademlia for provider records, coordinates dialing, manages retries with backoff |

`KademliaFileTrackerPlugin` dials a peer's provider addresses one at a time. Each dial has its own
`ConnectionId`, and for a circuit address that is also the relayed connection DCUtR reports on. The
internet event loop hands it dial errors, DCUtR outcomes and connection closures directly
(`dial_finished`). Only a signal for the current dial's connection moves it on to the next address.
Signals from older or unrelated connections to the same peer are ignored. Once DCUtR has upgraded
the current dial, its relayed connection closing no longer counts as a failure. If the current address gets
no outcome at all within `UNSTUCK_REQUERY_INTERVAL`, a signal went missing, and the query is
restarted. That timer starts again whenever the coordinator moves to the next address, so a long
round that is still making progress is never cut short.

### Lock Ordering (Critical)

`CoreFileTracker` documentation mandates strict lock ordering to prevent deadlocks:

**`files` → `plugin` → `plugin-internal-locks` (e.g., `query_map`)**

---

## 7. Session Management

Cryptographic sessions are negotiated per-peer using the `SessionEstablishment` protocol. Sessions have a 30-minute validity window.

The `SessionResponder` implements a **chain of responsibility**: when a message arrives, it tries decryption with the current session, then falls back to recently-expired sessions (maintained in a `ScopedExpiryMap`).

A session authenticates the peer only if its keys were known when the session was established. A
share code redeemed over the network travels on a session set up before either side knew the other.
Once the redeemer's brain has stored the contact, it asks its node to discard the session with the
peer that sent it (`NetworkNode::discard_session`). The node drops it outright, without the copy in
the expired-session cache that retiring a session keeps. Through that copy the redeemer would
still accept the sharer's requests on the old session, unauthenticated. Without it, the sharer's
next request is answered `MissingSession`, and the sharer establishes a new session and sends again,
as after a peer that crashed. The new session authenticates whatever each side knows by then.
Contacts added by QR code or synced from an owned device come with no peer, which leaves those
cases open ([Accepted Inconsistencies](accepted-inconsistencies.md)).

See [Security & Cryptography](security.md) for details.

---

## 8. Data Structures

| Structure | File | Purpose |
|-----------|------|---------|
| `IdleExpiryMap` | `p2p/src/data_structures/idle_expiry_map.rs` | Cache that expires entries after a period of inactivity |
| `ScopedExpiryMap` | `p2p/src/data_structures/scoped_expiry_map.rs` | Map with scoped keys and TTL-based expiry |
| `TimeoutRequestRegistry` | `p2p/src/send_receive_traits/request_registry.rs` | Correlates libp2p request IDs with application metadata + timeout |
| `BoundedList` | `core/src/library/bounded_list.rs` | Fixed-capacity list that drops oldest entries |
| `TtlMap` | `core/src/library/ttlmap.rs` | Wrapper around `expiringmap` for time-to-live entries |

---

## 9. Listen Port

Peers reach a node on one port, over TCP and QUIC alike. Each user has their own port, stored in
the database (`listen_port` table), so two users signed in on one machine each run their own node.
See [ADR 0010](adr/0010-per-user-listen-port.md) for why it is shaped this way. A daemon started
with `--listen-port` uses that port instead, so that a system configuration can open it in a
firewall ([ADR 0013](adr/0013-linux-through-nix.md)). The brain then never stores it, refuses to
change it, and sets `GetListenStatusResponse.port_pinned`, so the app shows the port as set by
the system.

**Who decides.** The brain (`core/src/core/brain/listen_port.rs`). The network node only binds the
port it is asked for (`NetworkNode::listen_on_port`) and knows nothing of choices or fallbacks.
`Runnable::run` binds nothing itself. The brain establishes the listeners once the node reports
`Ready`, after every swarm rebuild (`RestartNetwork`), and whenever the user picks a port.

**Choosing.**
- A fresh install picks a random port from `20000..=31999`, below every OS's ephemeral range, and
  keeps the first that binds out of 20 tries.
- Installs that predate the table keep `24256`, the old fixed port, so existing firewall rules
  still apply.
- The stored port is the *configured* one, and the port actually bound is the *active* one. The two
  differ only when the configured port is taken. The node then falls back to a temporary random
  port, which is never stored. A user who switches to a taken port stays on the port they had. The
  status pushed to the clients (`ListenStatusChanged`) names the configured port, why it failed,
  and the active port.
- A watcher retries the configured port with backoff (2s up to 10min) for as long as the status is
  degraded.

**Binding** (`p2p/src/event_loop/listen.rs`, run inline on the loop task):
1. Probe the port with plain std UDP and TCP sockets. libp2p boxes transport errors, which hides
   `AddrInUse`. Its TCP listener also sets `SO_REUSEPORT` (and `SO_REUSEADDR` on Windows), so a
   second node would share the port silently and the kernel would split connections between the
   two. The probes have neither option, so they fail where libp2p would not.
2. Listen QUIC, then TCP. If either fails, remove what was bound for this port.
3. Only then remove the previous port's listeners. A failed switch leaves them untouched.

Binding a port the loop already holds is a no-op. The internet loop forgets its listeners when it
rebuilds the swarm, so the brain's re-listen after a rebuild binds afresh. The old swarm's QUIC
socket stays bound until its connections drain, so the brain retries `AddressInUse` on the configured
port briefly before falling back.

