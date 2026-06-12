# Design Patterns in `fyles`

This document catalogues the software design patterns found throughout the **fyles** peer-to-peer file sharing application. Each section gives a concise theoretical definition of the pattern, followed by a concrete description of where and how it appears in the codebase.

---

## Table of Contents

1. [Hexagonal Architecture (Ports & Adapters)](#1-hexagonal-architecture-ports--adapters)
2. [Actor Pattern](#2-actor-pattern)
3. [Command Pattern](#3-command-pattern)
4. [Strategy Pattern (Plugin / Policy)](#4-strategy-pattern-plugin--policy)
5. [Factory Pattern](#5-factory-pattern)
6. [Dependency Injection & Inversion of Control](#6-dependency-injection--inversion-of-control)
7. [Adapter Pattern](#7-adapter-pattern)
8. [Façade Pattern](#8-façade-pattern)
9. [Observer / Event-Driven Architecture](#9-observer--event-driven-architecture)
10. [State Machine Pattern](#10-state-machine-pattern)
11. [Repository Pattern](#11-repository-pattern)
12. [Newtype / Wrapper Pattern](#12-newtype--wrapper-pattern)
13. [Composite Pattern](#13-composite-pattern)
14. [Mediator Pattern](#14-mediator-pattern)
15. [Template Method Pattern (via Trait Default Impls)](#15-template-method-pattern-via-trait-default-impls)
16. [Request–Response Correlation (Async Command–Reply)](#16-requestresponse-correlation-async-commandreply)
17. [Chain of Responsibility (Session Decryption)](#17-chain-of-responsibility-session-decryption)
18. [Contract-First Design / Interface Definition Language (IDL)](#18-contract-first-design--interface-definition-language-idl)
19. [BLoC Pattern (Business Logic Component)](#19-bloc-pattern-business-logic-component)
20. [Sealed Class Hierarchies (Algebraic Data Types in Dart)](#20-sealed-class-hierarchies-algebraic-data-types-in-dart)
21. [Anti-Corruption Layer (Frontend ↔ Wire Boundary)](#21-anti-corruption-layer-frontend--wire-boundary)
22. [Strategy Pattern (Platform-Specific Storage)](#22-strategy-pattern-platform-specific-storage)

---

## 1. Hexagonal Architecture (Ports & Adapters)

### Theory

Hexagonal Architecture (coined by Alistair Cockburn, also known as "Ports and Adapters") structures an application so that its core domain logic is completely independent of external concerns — databases, network transports, file systems, and UI. The core defines abstract *ports* (interfaces/traits) that are implemented by swappable *adapters* in the outer layers. This enables testability, since external dependencies can be replaced with mocks, and it enforces a clean unidirectional dependency flow: adapters depend on the core, never vice-versa.

### Application in fyles

The entire workspace is organised as a hexagonal architecture:

- **Core crate** (`core/`) defines all **ports** as Rust traits:
  - `NetworkNode` / `Runnable` (`core/src/core/p2p.rs`) — port for P2P networking.
  - `FilerequestDb` (`core/src/core/db.rs`) — port for persistence.
  - `HostController` (`core/src/io_controller.rs`) — port for file-system operations.
  - `RunnableFilerequestServer` (via wire module) — port for the gRPC API server.

- **Adapter crates** live outside the core and implement those ports:
  - `p2p/` — local-network P2P adapter using `libp2p`.
  - `p2p_internet/` — internet-capable P2P adapter layering Kademlia on top of the `p2p` crate.
  - `daemon/` — provides a `DirectFsController` implementing `HostController` for desktop.
  - `core/src/library/sqlite.rs` — SQLite adapter implementing `FilerequestDb`.
  - `core/src/mocks/` — `MockDb` and `MockP2pNode` implement the same ports for testing.

The `Brain` struct (the domain core) never touches `libp2p`, `sqlite`, or `tokio::fs` directly; it only talks to its injected trait objects.

---

## 2. Actor Pattern

### Theory

The Actor pattern encapsulates state inside a single autonomous entity that communicates exclusively via asynchronous messages. External callers never access the actor's internal state directly; instead, they send messages through a channel and (optionally) await a reply. This eliminates data races by design and provides clean concurrency semantics.

### Application in fyles

**`Brain`** (`core/src/core/brain.rs`) is the central actor. It owns all application state (`EphemeralBrainData`, references to the database, network node, and host controller) and runs a `tokio::select!` loop consuming `BrainAction` messages from an `mpsc` channel:

```rust
loop {
    tokio::select! {
        Some(action) = self.brain_action_receiver.recv() => {
            self.handle_action(action).await;
        }
    }
}
```

No other component holds a mutable reference to the Brain's state. The `ApiServer`, `P2pClient`, and `LocalEventLoop` all communicate with the Brain solely by sending `BrainAction` values through their cloned `mpsc::Sender<BrainAction>`.

**`LocalEventLoop`** (`p2p/src/event_loop.rs`) is another actor — it owns the libp2p `Swarm` and handles swarm events, P2P commands, and internal events via its own `tokio::select!` loop. External code must go through `InnerEvent::DoWithSwarm` closures to touch the swarm, ensuring single-threaded access.

---

## 3. Command Pattern

### Theory

The Command pattern turns a request into a first-class object that encapsulates all information needed to perform an action. Commands can be queued, serialised, logged, or dispatched by a central handler, decoupling the *invoker* (who requests something) from the *executor* (who carries it out).

### Application in fyles

Commands pervade the architecture at multiple levels:

| Enum | File | Purpose |
|------|------|---------|
| `BrainAction` | `core/src/core/brain/action.rs` | Top-level command sent to the Brain actor, categorised into `Client`, `NetworkNode`, and `Test` variants. |
| `ClientAction` | `core/src/core/brain/action_client.rs` | ~40 command variants representing every operation a gRPC client can request (CRUD on filerequests, contacts, pending files, settings, etc.). |
| `NetworkNodeAction` | `core/src/core/brain/action_p2p.rs` | ~20 command variants for P2P-originated events (file transfer updates, session events, contact discovery). |
| `P2pCommand` | `p2p/src/command.rs` | Commands from the `P2pClient` façade to the event loop (send files, cancel files, get node info, start listening, etc.). |
| `FileRequest` / `FileRespons` | `p2p/src/types.rs` | Wire-level command/response enums forming the file-transfer protocol (NewTransfer, ContinueTransfer, Chunk, Done, ProtocolViolation, etc.). |

The `handle_action.rs` dispatcher (`core/src/core/brain/handle_action.rs`) is a textbook command processor: a large `match` on the `BrainAction` enum that routes each variant to the appropriate handler method.

---

## 4. Strategy Pattern (Plugin / Policy)

### Theory

The Strategy pattern defines a family of interchangeable algorithms behind a common interface, letting the algorithm vary independently from the code that uses it. A closely related pattern is the *Plugin*, which allows new behaviour to be composed at construction time.

### Application in fyles

**`FileTrackerPlugin` trait** (`p2p/src/event_loop/filerequest/file_tracker.rs`) is a pure Strategy/Plugin interface:

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

Two concrete strategies exist:

| Strategy | Crate | Behaviour |
|----------|-------|-----------|
| `NoOpFileTrackerPlugin` | `p2p/` | Does nothing — used for local-only networking where Kademlia discovery is unnecessary. |
| `KademliaFileTrackerPlugin` | `p2p_internet/` | Orchestrates Kademlia `get_providers` queries, dial coordination, and scheduled retries to discover and connect to internet peers. |

The `CoreFileTracker<P: FileTrackerPlugin>` is generic over `P`, so the core file-tracking state machine (pending → sending → done) is completely agnostic about how peers are discovered. The plugin is injected via `CoreFileTracker::with_plugin(...)`.

**`PushStrategy`** and related strategy enums in `domain_models.rs` also define variant-driven strategies for data transfer approaches.

---

## 5. Factory Pattern

### Theory

The Factory pattern delegates object creation to a specialised function or closure, decoupling the consumer from the concrete type being instantiated. This is especially useful when object creation involves complex configuration or when the concrete type varies by environment.

### Application in fyles

**`SwarmFactory`** — the P2P swarm is created via a factory closure:

```rust
pub type SwarmFactory<S> = Arc<dyn Fn(Keypair, &[u8]) -> Result<S, ...> + Send + Sync>;
```

The daemon (`daemon/src/main.rs`) injects a concrete factory:

```rust
Arc::new(move |keypair: Keypair, _config: &[u8]| {
    LocalEventLoop::swarm_factory(keypair)
})
```

**Entrypoint factory closures** — the `entrypoint()` function in `core/src/lib.rs` accepts four `async` factory closures:
- `db_factory` — constructs the database adapter.
- `host_controller_factory` — constructs the file-system adapter.
- `network_node_factory` — constructs the P2P adapter.
- `api_server_factory` — constructs the gRPC server.

This means the entire application composition is defined by the caller (daemon, Android host, test harness), without the core knowing any concrete types.

---

## 6. Dependency Injection & Inversion of Control

### Theory

Dependency Injection (DI) is a technique where an object receives its dependencies from the outside rather than constructing them internally. Inversion of Control (IoC) is the broader principle: the framework or parent calls *you*, not the other way around. Together, they eliminate hard-coded dependencies and make unit testing trivial.

### Application in fyles

The `Brain` is constructed with three injected trait objects:

```rust
pub type BrainDb = Arc<dyn FilerequestDb>;
pub type BrainNetwork = Arc<dyn NetworkNode>;
pub type BrainApiServer = Box<dyn RunnableFilerequestServer>;
```

Plus a `host_controller: Arc<dyn HostController>` for file I/O.

The `entrypoint()` function acts as the **composition root**, wiring everything together through factory closures. Tests inject `MockDb`, `MockP2pNode`, and a `TestHostController` instead — demonstrating classic constructor-based DI with zero changes to the `Brain` code.

---

## 7. Adapter Pattern

### Theory

The Adapter pattern converts the interface of one class into another interface that a client expects. It lets classes work together that otherwise couldn't because of incompatible interfaces.

### Application in fyles

**Proto ↔ Domain Conversion** (`core/src/core/domain_proto_conversion.rs`) — Protobuf-generated types and domain model types have different structures. A comprehensive set of `From`, `Into`, and `TryFrom` implementations adapt between them:

```rust
impl From<&Contact> for ContactProto { ... }
impl TryFrom<FileRequestProto> for Filerequest { ... }
impl From<RemoteFilerequest> for RemoteFileRequestProto { ... }
impl From<&SendStatus> for SendStatusProto { ... }
```

This is the Adapter pattern in its purest form: the domain models never leak protobuf concerns, and vice versa.

**`Wrapper<T>` / `W<T>` newtypes** (`p2p/src/utils.rs`, `p2p_internet/src/lib.rs`) — The `Wrapper` newtype wraps `P2pClient` to solve Rust's orphan rule, acting as an adapter that enables trait implementations from other crates:

```rust
#[derive(Deref, DerefMut)]
pub struct Wrapper<T>(pub T);

impl NetworkNode for InternetP2pClient { ... } // InternetP2pClient = Wrapper<P2pClient<...>>
```

**`Wrap` / `Unwrap` traits** (`p2p/src/types.rs`) — Adapt `libp2p::PeerId` (which the core crate cannot depend on) to/from the domain's `PeerIdWrapper` newtype.

---

## 8. Façade Pattern

### Theory

The Façade pattern provides a simplified, unified interface to a complex subsystem. Clients interact with the façade instead of navigating the subsystem's internals.

### Application in fyles

**`P2pClient`** (`p2p/src/lib.rs`) is a textbook façade. The P2P subsystem internally consists of:
- A libp2p `Swarm` with multiple composed behaviours.
- An `EventLoopData` struct with session managers, request registries, and file trackers.
- A `LocalEventLoop` running a `tokio::select!` loop.
- Cryptographic session establishment and rotation.
- Kademlia provider tracking (in the internet variant).

`P2pClient` hides all of this behind a clean async interface:

```rust
impl NetworkNode for P2pClient {
    async fn send_files(&self, files: Vec<FileToSend>) -> P2pResult<()>;
    async fn get_node_info(&self) -> P2pResult<NodeStatusInfo>;
    async fn cancel_file(&self, file_id: FylesId) -> P2pResult<bool>;
    async fn update_identity(&self, ...);
}
```

Internally, each method sends a `P2pCommand` through an `mpsc` channel and awaits the response via a `oneshot` channel — but the caller sees none of this complexity.

**`ApiServer`** (`core/src/core/api_server.rs`) also acts as a façade between gRPC clients and the Brain, translating every gRPC call into a `BrainAction` dispatch.

---

## 9. Observer / Event-Driven Architecture

### Theory

The Observer pattern defines a one-to-many relationship where a subject notifies its dependents (observers) of state changes. Event-Driven Architecture generalises this: components communicate by emitting and reacting to events, with no direct coupling between producer and consumer.

### Application in fyles

The entire system is fundamentally event-driven, stitched together with `tokio::mpsc` and `oneshot` channels:

| Producer | Channel | Consumer |
|----------|---------|----------|
| `ApiServer` (gRPC) | `mpsc::Sender<BrainAction>` | `Brain` actor loop |
| `LocalEventLoop` (P2P) | `mpsc::Sender<BrainAction>` | `Brain` actor loop |
| `Brain` | `mpsc::Sender<ServerMessage>` | `StreamRegistry` → SSE/streaming gRPC clients |
| `P2pClient` | `mpsc::Sender<P2pCommand>` | `LocalEventLoop` command handler |
| Event loop internals | `mpsc::UnboundedSender<InnerEvent>` | `LocalEventLoop` inner event handler |

**Push notifications to clients**: The `Brain` emits `ServerMessage` events (pending file status changed, received file status changed, database restored) through a channel. The `StreamRegistry` fans these out to all connected bidirectional gRPC streams — a classic Pub/Sub implementation.

**libp2p events**: The `SwarmEvent` variants produced by `swarm.select_next_some()` drive the entire P2P layer. Each event type (behaviour event, new listen address, connection established, etc.) is dispatched to the appropriate handler, embodying the event-driven paradigm.

---

## 10. State Machine Pattern

### Theory

A State Machine models an entity as being in one of a finite number of states, with well-defined transitions between them triggered by events. States and transitions are typically encoded as enums in Rust, with the compiler enforcing exhaustive handling.

### Application in fyles

**`SendStatus`** (`core/src/core/domain_models.rs`) encodes the lifecycle of an outgoing file transfer:

```
Pending → InProgress(Prepared) → InProgress(Sending) → InProgress(PendingSent) → Sent
                   ↓                      ↓
              InProgress(Interrupted)   Failed / Rejected
```

The `InProgress` variant carries a nested `InProgressSendStatus` and a `TransferData` payload (progress bytes, file size, transfer UUID), ensuring that progress metadata only exists when the transfer is actually in-progress. This is a tagged state machine using Rust's algebraic data types.

**`ReceiveStatus`** mirrors the state machine for incoming files:

```
Receiving → Completed
    ↓
Interrupted / Failed
```

**`KadQueryType`** (`p2p_internet/src/event_loop/kademlia_file_tracker.rs`) encodes the lifecycle of a Kademlia peer-discovery query:

```
Ongoing(QueryId, QueryAccumulator) → Dialing(DialCoordinator) → Scheduled(TimeoutGuard)
                                                                        ↓
                                                                    Sending
```

Each state carries only the data relevant to that phase (e.g., `Dialing` carries a `DialCoordinator` with the addresses to try, while `Ongoing` carries a `QueryAccumulator`).

**`FileRequest` / `FileRespons`** enums on the wire also form a protocol state machine defining the valid sequences of request/response pairs during a transfer.

---

## 11. Repository Pattern

### Theory

The Repository pattern mediates between the domain layer and the data source. It provides a collection-like interface for accessing domain objects, hiding the details of how data is stored and retrieved (SQL, files, in-memory, etc.).

### Application in fyles

The **`FilerequestDb`** trait (`core/src/core/db.rs`) is a repository interface with ~30 operations:

```rust
#[async_trait]
pub trait FilerequestDb: Send + Sync {
    async fn get_filerequest(&self, id: &FylesId) -> DatabaseResult<Filerequest>;
    async fn create_filerequest(&self, fr: &CreateFilerequest) -> DatabaseResult<FylesId>;
    async fn update_filerequest(&self, fr: &Filerequest) -> DatabaseResult<()>;
    async fn delete_filerequest(&self, id: &FylesId) -> DatabaseResult<()>;
    async fn get_contact(&self, id: &ContactId) -> DatabaseResult<Contact>;
    async fn register_contact(&self, contact: Contact) -> DatabaseResult<()>;
    // ... pending files, received files, remote filerequests, settings, backup/restore ...
}
```

This trait is the domain's *sole* interface to persistence. Two implementations exist:
- **`Sqlite`** — production adapter using SQLite.
- **`MockDb`** — in-memory `HashMap`-based implementation for testing, faithfully replicating cascading deletes and error semantics.

Notably, the `db.rs` module also defines reusable test functions (`test::test_filerequest_crud_and_access`, etc.) that are executed against *both* the SQLite and Mock implementations, validating behavioural equivalence — a testament to the pattern's testability benefits.

---

## 12. Newtype / Wrapper Pattern

### Theory

The Newtype pattern wraps a primitive or foreign type in a single-field struct to provide type safety, prevent misuse of raw values, and enable trait implementations on foreign types. In Rust, this has zero runtime cost.

### Application in fyles

| Newtype | Inner Type | Purpose |
|---------|-----------|---------|
| `FylesId` | `String` | Domain identifier — prevents confusion with other strings (contact IDs, peer IDs, file paths). |
| `ContactId` | `String` | Distinguishes contact identifiers from file identifiers at the type level. |
| `PeerIdWrapper` | `String` (base58 peer ID) | Bridges the gap between `libp2p::PeerId` (unavailable in the core crate) and domain models. |
| `SelfContactInviteChallenge` | `Vec<u8>` | Gives a byte challenge a meaningful name and `Hash`/`Eq` implementations via `derive_more::Deref`. |
| `ContactShareChallenge` | `Vec<u8>` | Same as above for a different challenge type. |
| `RefCountEventLoopData` | `Arc<EventLoopData>` | Provides `Clone` and `Deref` for reference-counted event loop state. |
| `Wrapper<T>` / `W<T>` | Various | Solves Rust's orphan rule for implementing foreign traits on foreign types. |
| `Encrypted<T>` | Ciphertext bytes + nonce | Phantom-typed wrapper ensuring you can't accidentally mix encrypted payloads of different types. |

The `Encrypted<T>` newtype deserves special mention: it uses `PhantomData<T>` to carry the *original* type at compile time, so `Encrypted<FileRequest>` and `Encrypted<Contact>` are distinct types despite having the same runtime representation. This prevents accidentally decrypting a file-request payload as a contact.

---

## 13. Composite Pattern

### Theory

The Composite pattern composes objects into tree structures, allowing clients to treat individual objects and compositions uniformly. In networking, it often appears as protocol composition.

### Application in fyles

**`CoreBehaviour`** (`p2p/src/behaviour.rs`) uses `libp2p`'s `#[derive(NetworkBehaviour)]` macro to compose four independent protocol behaviours into a single aggregate behaviour:

```rust
#[derive(NetworkBehaviour)]
pub struct CoreBehaviour {
    pub filerequest: FilerequestBehaviour,
    pub self_contact_invite: SelfContactInviteBehaviour,
    pub contact_share: ContactShareBehaviour,
    pub session_establishment: SessionEstablishmentBehaviour,
}
```

This is further wrapped in the local behaviour:

```rust
#[derive(NetworkBehaviour)]
pub struct LocalNetworkBehaviour {
    pub core: CoreBehaviour,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
}
```

And the internet variant adds Kademlia, identify, relay, and DCUtR on top. The swarm treats the composite as a single entity, while internally each sub-behaviour handles its own protocol independently.

---

## 14. Mediator Pattern

### Theory

The Mediator pattern defines an object that encapsulates how a set of objects interact. Instead of objects referring to each other directly, they communicate through the mediator, reducing coupling.

### Application in fyles

The **`Brain`** is the application's central mediator. It stands between:

- The **API server** (client requests).
- The **P2P network node** (file transfer events, peer discovery).
- The **database** (persistence).
- The **host controller** (file system).
- The **gRPC streaming clients** (push notifications).

None of these subsystems communicate directly with each other. When the P2P layer receives a file, it sends a `BrainAction::NetworkNode(StoreReceivedFile(...))` to the Brain, which writes to the database, notifies the host controller, and pushes a `ServerMessage` to streaming clients — all orchestrated through the mediator.

The `BrainRequest<T, U>` struct (carrying a `request: T` payload and a `Mutex<Option<oneshot::Sender<U>>>` for the response) enables type-safe request/response correlation through the mediator.

---

## 15. Template Method Pattern (via Trait Default Impls)

### Theory

The Template Method pattern defines the skeleton of an algorithm in a base class (or trait), deferring some steps to subclasses (or implementors). The overall algorithm structure is fixed, but specific steps can be customised.

### Application in fyles

**`SessionResponder` blanket implementation** (`p2p/src/send_receive_traits/session_responder.rs`) defines a fixed algorithm for responding to encrypted requests:

1. Get all recent receive sessions for the peer.
2. Try decrypting the request with each session in order.
3. On successful decryption, delegate to `self.respond_to(...)`.
4. Encrypt the response with the same session.
5. If no session could decrypt, return an appropriate error.

This algorithm is defined once as a blanket `impl<T> SessionResponder for T where T: GetSession + RequestReceiver`. The *variable steps* (`get_recent_receive_sessions`, `respond_to`, `encrypt`) are defined by the concrete type implementing `GetSession` and `RequestReceiver`. The overall control flow is fixed.

---

## 16. Request–Response Correlation (Async Command–Reply)

### Theory

In asynchronous systems, a common challenge is correlating a response with the request that triggered it. The Request–Response Correlation pattern pairs each outgoing request with a unique identifier or callback, so the response can be routed back to the original caller.

### Application in fyles

This pattern appears at multiple layers:

**`BrainRequest<T, U>`** (`core/src/core/brain/types.rs`):
```rust
pub struct BrainRequest<T, U> {
    pub request: T,
    pub response_sender: Mutex<Option<oneshot::Sender<U>>>,
}
```
The caller creates a `(BrainRequest, oneshot::Receiver)` pair, sends the request through the channel, and awaits the receiver. The Brain handler extracts the `oneshot::Sender` and sends the result back — correlating request and response through the paired channel.

**`TimeoutRequestRegistry`** (`p2p/src/send_receive_traits/request_registry.rs`) correlates libp2p `OutboundRequestId`s with application-level metadata (peer ID, contact ID, session). When a response arrives, the registry resolves the original context, enabling the response handler to process it correctly. If no response arrives within the timeout, the registry fires a timeout event.

**`WithSwarm::with_swarm_res`** (`p2p/src/event_loop/with_swarm.rs`) sends a closure to the swarm thread and correlates the result back via a oneshot channel:
```rust
async fn with_swarm_res<R, F>(&self, block: F) -> R {
    let (sender, receiver) = oneshot::channel();
    self.send(InnerEvent::DoWithSwarm {
        block: Box::new(|swarm| { let _ = sender.send(block(swarm)); }),
        span: Span::current(),
    });
    receiver.await.expect("...")
}
```

---

## 17. Chain of Responsibility (Session Decryption)

### Theory

The Chain of Responsibility pattern passes a request along a chain of handlers. Each handler either processes the request or passes it to the next handler in the chain. This avoids coupling the sender to a specific receiver.

### Application in fyles

The **session decryption fallback logic** in `SessionResponder::decrypt_respond` (`p2p/src/send_receive_traits/session_responder.rs`) implements this pattern:

```rust
let recent_sessions = self.get_recent_receive_sessions(&node_id).await;
for session in &recent_sessions {
    let decrypted = match session.decrypt(&request).await {
        Err(e) => { decrypt_errors.push(e); continue; }
        Ok(decrypted) => decrypted,
    };
    // Success — respond using this session
    return Some(session.encrypt(&response).await);
}
// All sessions failed
```

The "chain" is the list of sessions (current + recently expired). Each session is tried in order; on failure, the request passes to the next session. This handles the real-world scenario where a message might arrive encrypted with a just-expired session while a new session is being negotiated.

The `ScopedExpiryMap` (`p2p/src/data_structures/scoped_expiry_map.rs`) maintains this cache of recently-expired sessions, ensuring the chain always has a reasonable set of candidates.

---

## 18. Contract-First Design / Interface Definition Language (IDL)

### Theory

Contract-First Design (also called "Schema-First" or "API-First") defines the interface between components using a language-neutral schema *before* any implementation code is written. An Interface Definition Language (IDL) — such as Protocol Buffers, OpenAPI, or Thrift — serves as the single source of truth. Both client and server code are then *generated* from this schema, guaranteeing type-safe, binary-compatible communication without manual serialisation code. This enforces a clear boundary between subsystems and enables independent evolution of client and server as long as the contract is respected.

### Application in fyles

The shared `wire/` directory at the project root contains `.proto` files that define the entire client–server contract:

| Proto File | Purpose |
|-----------|---------|
| `filerequest.proto` | Core API: `FileRequestService` with ~30 RPCs, all message types, the bidirectional `Stream` RPC, and push notification `ServerMessage` envelope. |
| `nodestatus.proto` | Diagnostics: `NodeStatusService` with health-check and status RPCs. |
| `internet_settings.proto` | Configuration messages for internet-mode toggle. |
| `pingpong.proto` | Lightweight connectivity probe. |

From these `.proto` files, code is generated into **two completely independent technology stacks**:

- **Rust backend** (`fyles_service/`) — `tonic` + `prost` generate server stubs and message types. The `ApiServer` implements the generated `FileRequestService` trait, dispatching each RPC to the `Brain` via `BrainAction` commands.
- **Flutter frontend** (`fyles/lib/wire/`) — `protoc` with the Dart gRPC plugin generates `*.pb.dart`, `*.pbgrpc.dart`, `*.pbenum.dart`, and `*.pbjson.dart` files. The `ProtobufClient` service consumes the generated `FileRequestServiceClient` stub.

The proto file even documents its own design philosophy:

```protobuf
// Design Philosophy:
// - No implicitly optional parameters in request messages
// - Each endpoint should have exactly one clear purpose
// - Split endpoints instead of using optional parameters
```

The bidirectional `Stream` RPC (`rpc Stream (stream ClientMessage) returns (stream ServerMessage)`) is particularly noteworthy: it defines a push-notification channel where the `ServerMessage` oneof envelope (`PendingFileStatusChanged`, `ReceivedFileStatusChanged`, `DatabaseRestoredFromBackup`, etc.) acts as a discriminated-union event bus transported over a single long-lived gRPC stream.

---

## 19. BLoC Pattern (Business Logic Component)

### Theory

BLoC (Business Logic Component) is a state management pattern originating in the Flutter/Dart ecosystem. It separates presentation from business logic by enforcing a strict unidirectional data flow: **Events → BLoC → State**. The UI dispatches discrete `Event` objects; the BLoC processes them and emits new immutable `State` objects; the UI reactively rebuilds based on state changes. This is conceptually equivalent to Redux/Flux in the web world or MVI (Model–View–Intent) in Android.

### Application in fyles (Flutter frontend)

The entire Flutter frontend uses a single `AppBloc` (`lib/bloc/app_bloc.dart`) as its central state manager:

- **Events** (`lib/bloc/app_event.dart`) — ~50 event classes extending the sealed `AppEvent` base, each representing a discrete user action or system notification (e.g. `CreateFileRequest`, `DeleteContact`, `PendingFileStatusChangedEvent`, `OpenShareContactDialogEvent`).
- **State** (`lib/bloc/app_state.dart`) — A single immutable `AppState` class holding all application data (contacts, file requests, remote requests, received files, active transfers, dialog state, etc.) with a `copyWith` method for immutable updates.
- **BLoC** (`lib/bloc/app_bloc.dart`) — Registers ~50 `on<EventType>` handlers in its constructor, each processing one event type and emitting new state via `emit(state.copyWith(...))`.

The BLoC also bridges backend push notifications into the event stream: `getServerMessageHandler()` converts `ServerMessage` protobuf messages from the gRPC stream into `AppEvent` objects, feeding them back into the BLoC — unifying server-push and user-initiated events into a single processing pipeline.

---

## 20. Sealed Class Hierarchies (Algebraic Data Types in Dart)

### Theory

Sealed classes restrict inheritance to a closed set of known subtypes, enabling the compiler to verify exhaustive handling in `switch` expressions. Combined with pattern matching, they model sum types (tagged unions) — the same technique Rust achieves with `enum`.

### Application in fyles (Flutter frontend)

**`DialogState`** (`lib/bloc/dialog_state.dart`) is a sealed class hierarchy with ~18 subtypes (`NoDialog`, `CreateFileRequestDialog`, `ConfirmDeleteDialog`, `ShareContactDialog`, `QRScannerForContactDialog`, etc.). Each subtype carries only the data relevant to that dialog. The `DialogManager.showDialogForState()` method exhaustively switches over all subtypes, guaranteeing every dialog state is handled.

**`Result<S, E>`** (`lib/utils/result.dart`) is a hand-rolled sealed union type:

```dart
sealed class Result<S, E> {
  T when<T>({required T Function(S) success, required T Function(E) failure});
  Result<T, E> map<T>(T Function(S) transform);
}
final class Success<S, E> extends Result<S, E> { ... }
final class Failure<S, E> extends Result<S, E> { ... }
```

This is used throughout the model layer (`FileRequest.fromProto`, `Contact.fromProto`, `RemoteFileRequest.fromProto`) to handle protobuf-to-domain conversion errors without exceptions — mirroring Rust's `Result<T, E>`.

**`FileRequest`** itself uses an abstract class with two concrete subtypes (`PublicFileRequest`, `AudienceFileRequest`), modelling the access-control variants at the type level rather than through runtime flags.

---

## 21. Anti-Corruption Layer (Frontend ↔ Wire Boundary)

### Theory

The Anti-Corruption Layer (from Domain-Driven Design) is a translation boundary that prevents a foreign model (e.g. generated code, third-party API types) from leaking into and corrupting the domain model. It translates between the external representation and the internal domain model, keeping the domain clean.

### Application in fyles (Flutter frontend)

The frontend defines a `Wire<T, M>` interface (`lib/models/wire.dart`):

```dart
abstract class ToWire<T extends GeneratedMessage> { T toWire(); }
abstract class FromWire<T extends GeneratedMessage, M> { Result<M, String> fromWire(T proto); }
abstract class Wire<T extends GeneratedMessage, M> implements ToWire<T>, FromWire<T, M> {}
```

Every domain model (`FileRequest`, `Contact`, `RemoteFileRequest`, `ReceivedFile`, `PendingFile`) implements this interface, providing explicit `toWire()` and `fromProto()` conversions. The generated protobuf types (`FileRequestProto`, `ContactProto`, etc.) never appear in UI code or state — they are confined to the `ProtobufClient` service and the model conversion boundary.

This mirrors the backend's `domain_proto_conversion.rs` adapter layer, creating a symmetric anti-corruption boundary on both sides of the wire.

---

## 22. Strategy Pattern (Platform-Specific Storage)

### Theory

(See [§4](#4-strategy-pattern-plugin--policy) for the general definition.)

### Application in fyles (Flutter frontend)

**`StorageAccess`** (`lib/services/filesystem.dart`) is an abstract class defining platform-agnostic file-picking and directory-persistence operations:

```dart
abstract class StorageAccess {
  Future<List<PickedFile>> pickDocuments({...});
  Future<String> ensurePersistedDirectory();
  Future<String?> getSavedDirectory();
  Future<void> clearPersistedDirectory();
}
```

Two concrete strategies exist:

| Strategy | Platform | Mechanism |
|----------|----------|-----------|
| `AndroidStorageAccess` | Android | Uses `MethodChannel('native.storage')` to call SAF (Storage Access Framework) APIs via Kotlin. |
| `DefaultStorageAccess` | iOS, macOS, Linux, Windows | Uses `file_picker` and `shared_preferences` packages for native file dialogs. |

The `StorageAccess.instance()` factory method selects the correct implementation at runtime based on `Platform.isAndroid`, implementing a classic strategy selection pattern.

---

## Summary

The following table gives a bird's-eye view of all identified patterns and their primary locations:

| # | Pattern | Layer | Primary Location(s) |
|---|---------|-------|---------------------|
| 1 | Hexagonal Architecture | Backend | Workspace crate organisation; `core/` ports vs adapter crates |
| 2 | Actor | Backend | `Brain::run()`, `LocalEventLoop::run()` |
| 3 | Command | Backend | `BrainAction`, `ClientAction`, `NetworkNodeAction`, `P2pCommand` |
| 4 | Strategy / Plugin | Backend | `FileTrackerPlugin` → `NoOp` / `Kademlia` |
| 5 | Factory | Backend | `SwarmFactory`, `entrypoint()` factory closures |
| 6 | Dependency Injection | Backend | `Brain` construction via trait-object type aliases |
| 7 | Adapter | Backend | `domain_proto_conversion.rs`, `Wrapper<T>`, `Wrap`/`Unwrap` |
| 8 | Façade | Backend | `P2pClient`, `ApiServer` |
| 9 | Observer / Event-Driven | Both | `mpsc` channels (backend), gRPC `Stream` push (cross-boundary) |
| 10 | State Machine | Backend | `SendStatus`, `ReceiveStatus`, `KadQueryType` |
| 11 | Repository | Backend | `FilerequestDb` trait → `Sqlite` / `MockDb` |
| 12 | Newtype / Wrapper | Backend | `FylesId`, `ContactId`, `PeerIdWrapper`, `Encrypted<T>` |
| 13 | Composite | Backend | `CoreBehaviour`, `LocalNetworkBehaviour` |
| 14 | Mediator | Backend | `Brain` orchestrating API ↔ P2P ↔ DB ↔ FS |
| 15 | Template Method | Backend | `SessionResponder` blanket impl |
| 16 | Request–Response Correlation | Backend | `BrainRequest<T,U>`, `TimeoutRequestRegistry` |
| 17 | Chain of Responsibility | Backend | Session decryption fallback chain |
| 18 | Contract-First / IDL | Cross-cutting | `wire/*.proto` → generated Rust (tonic) + Dart (grpc) code |
| 19 | BLoC (Event → State) | Frontend | `AppBloc`, `AppEvent` (~50 types), `AppState` |
| 20 | Sealed Class / ADT | Frontend | `DialogState`, `Result<S,E>`, `FileRequest` hierarchy |
| 21 | Anti-Corruption Layer | Frontend | `Wire<T,M>` interface, `toWire()` / `fromProto()` conversions |
| 22 | Strategy (Platform) | Frontend | `StorageAccess` → `AndroidStorageAccess` / `DefaultStorageAccess` |
