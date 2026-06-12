# Networking & P2P Architecture

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Two-Tier Design

Fyles supports two networking modes, layered via a plugin architecture:

| Mode | Crate | Discovery | Transport | Use Case |
|------|-------|-----------|-----------|----------|
| **Local** | `p2p` | mDNS | Direct TCP | LAN file sharing, zero config |
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
async fn with_swarm_res<R, F>(&self, block: F) -> R {
    let (sender, receiver) = oneshot::channel();
    self.send(InnerEvent::DoWithSwarm {
        block: Box::new(|swarm| { let _ = sender.send(block(swarm)); }),
    });
    receiver.await.expect("...")
}
```

---

## 5. File Transfer Protocol

File transfers use libp2p's request-response protocol with a custom codec.

### Wire Types

```rust
enum FileRequest {
    NewTransfer { transfer_id, filerequest_id, file_name, file_size, ... },
    ContinueTransfer { transfer_id },
    Chunk { transfer_id, data, offset },
    Done { transfer_id },
}

enum FileRespons {  // intentional truncation to avoid name collision
    Accepted { transfer_id },
    Rejected { reason },
    Continue,
    ProtocolViolation { reason },
}
```

### Transfer State Machine

```
Sender:   Pending → Prepared → Sending → PendingSent → Sent
                        ↓          ↓
                   Interrupted   Failed / Rejected

Receiver: Receiving → Completed
              ↓
         Interrupted / Failed
```

### Chunked Transfer Flow

1. Sender sends `NewTransfer` with file metadata
2. Receiver checks authorization, responds `Accepted` or `Rejected`
3. Sender streams `Chunk` messages (each contains data + offset)
4. Sender sends `Done` when complete
5. Receiver writes chunks to disk via `HostController`

All wire messages are encrypted per-session using ChaCha20-Poly1305. See [Security](security.md).

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
    async fn outgoing_connection_error(&mut self, ...);
    async fn on_send_file_to_peer(&mut self, peer: PeerId);
    async fn interrupt_sending_to_peer(&mut self, peer: PeerId);
    async fn done_sending_to_peer(&mut self, peer: PeerId);
}
```

| Plugin | Behaviour |
|--------|-----------|
| `NoOpFileTrackerPlugin` | No active discovery; relies on mDNS for peer appearance |
| `KademliaFileTrackerPlugin` | Queries Kademlia for provider records, coordinates dialing, manages retries with backoff |

### Lock Ordering (Critical)

`CoreFileTracker` documentation mandates strict lock ordering to prevent deadlocks:

**`files` → `plugin` → `plugin-internal-locks` (e.g., `query_map`)**

---

## 7. Session Management

Cryptographic sessions are negotiated per-peer using the `SessionEstablishment` protocol. Sessions have a 30-minute validity window.

The `SessionResponder` implements a **chain of responsibility**: when a message arrives, it tries decryption with the current session, then falls back to recently-expired sessions (maintained in a `ScopedExpiryMap`).

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
