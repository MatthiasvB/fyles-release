# Fyles — Architecture Overview

> **Audience:** Developers, contributors, and future-self context.
> This document is the top-level entry point. It gives a bird's-eye view of the system and links to detailed sub-documents for each major area.

---

## 1. What is Fyles?

Fyles is a **cross-platform, peer-to-peer file-sharing application**. Users create *file requests* (inboxes), share them with contacts, and receive files directly — device to device — without a central server. Identity, device ownership, and authorization are managed through cryptographic contact records.

### Key Properties

| Property | Detail |
|----------|--------|
| **Transport** | libp2p (mDNS for LAN, Kademlia + relay + DCUtR for internet) |
| **Security** | Hybrid PQ crypto: ChaCha20-Poly1305 symmetric, Dilithium5 + Ed25519 signing, X25519 + Kyber key exchange |
| **Platforms** | Android, iOS, macOS, Linux, Windows |
| **Frontend** | Flutter / Dart |
| **Backend** | Rust (async, tokio) |
| **IPC** | gRPC over TCP (desktop) or Unix Domain Sockets (mobile) |
| **Persistence** | SQLite (rusqlite) |
| **Device sync** | Direct device-to-device identity sync |

---

## 2. Repository Layout

```
fyles/                              ← project root
├── wire/                           ← Protobuf IDL (shared contract)
│   ├── filerequest.proto
│   ├── nodestatus.proto
│   ├── internet_settings.proto
│   └── pingpong.proto
│
├── fyles/                          ← Flutter frontend
│   ├── lib/
│   │   ├── bloc/                   ← BLoC state management
│   │   ├── models/                 ← Domain models + Wire adapters
│   │   ├── services/               ← gRPC client, storage, dialog manager
│   │   ├── pages/                  ← Screen-level widgets
│   │   ├── widgets/                ← Reusable UI components
│   │   ├── wire/                   ← Generated protobuf code (Dart)
│   │   └── main.dart
│   ├── android/                    ← Android shell + Kotlin service
│   ├── ios/                        ← iOS shell + Swift bridge + Share Extension
│   └── app/src/main/java/          ← Android native (Kotlin) code
│
├── fyles_service/                  ← Rust backend workspace
│   ├── core/                       ← Domain logic, Brain, DB traits, IO traits
│   ├── p2p/                        ← Local-network P2P (mDNS, request-response)
│   ├── p2p_internet/               ← Internet P2P (Kademlia, relay, DCUtR)
│   ├── crypto/                     ← Cryptographic primitives
│   ├── daemon/                     ← Desktop binary (local-network mode)
│   ├── daemon_internet/            ← Desktop binary (internet mode)
│   ├── c_ffi/                      ← C FFI bridge → iOS/Swift
│   ├── kotlin_ffi/                 ← JNI bridge → Android/Kotlin
│   └── DESIGN_PATTERNS.md          ← Catalogue of design patterns
│
├── e2e_test/                       ← End-to-end integration tests (shell)
├── docs/                           ← This documentation tree
├── build.sh                        ← Unified build script
└── server_build.sh                 ← Backend-only build script
```

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Flutter Frontend                        │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐  ┌──────────┐  │
│  │  Pages   │→ │   BLoC   │→ │  Protobuf  │→ │  Models  │  │
│  │(Widgets) │← │(AppBloc) │← │  Client    │← │  (Wire)  │  │
│  └──────────┘  └──────────┘  └─────┬──────┘  └──────────┘  │
└────────────────────────────────────┬────────────────────────┘
                                     │ gRPC (TCP or UDS)
┌────────────────────────────────────┼────────────────────────┐
│                     Rust Backend   │                        │
│  ┌──────────┐  ┌──────────────┐  ┌┴─────────┐              │
│  │ Database │← │    Brain     │→ │ApiServer │              │
│  │ (SQLite) │  │  (Mediator)  │  │ (gRPC)   │              │
│  └──────────┘  └──────┬───────┘  └──────────┘              │
│                       │                                     │
│              ┌────────┴────────┐                            │
│              │  P2P Subsystem  │                            │
│              │  ┌────────────┐ │                            │
│              │  │ EventLoop  │ │                            │
│              │  │  (Swarm)   │ │                            │
│              │  └────────────┘ │                            │
│              └─────────────────┘                            │
│                       │                                     │
│              ┌────────┴────────┐                            │
│              │ HostController  │                            │
│              │ (File System)   │                            │
│              └─────────────────┘                            │
└─────────────────────────────────────────────────────────────┘
```

The architecture is **hexagonal** (ports & adapters). The `Brain` in the `core` crate defines abstract ports for database, networking, file I/O, and the API server. Concrete adapters are injected at startup via factory closures, enabling the same core to run as a desktop daemon, an Android foreground service, or an iOS background task.

---

## 4. Detailed Documentation

Each sub-document covers one major area in depth:

| Document | Covers |
|----------|--------|
| [Backend Architecture](architecture/backend.md) | Rust workspace, Brain orchestrator, database, domain models, entrypoint / DI |
| [Frontend Architecture](architecture/frontend.md) | Flutter app structure, BLoC state management, Wire boundary, platform-specific services |
| [Networking & P2P](architecture/networking.md) | libp2p integration, local vs internet modes, file transfer protocol, session management |
| [Deployment & Platform Integration](architecture/deployment.md) | Build targets, FFI bridges (C/Swift, JNI/Kotlin), daemon variants, IPC schemes |
| [Security & Cryptography](architecture/security.md) | Hybrid PQ crypto, session lifecycle, nonce management, identity sync |
| [Wire Protocol & API Contract](architecture/wire-protocol.md) | Protobuf schema design, gRPC services, bidirectional streaming, push notifications |
| [Design Patterns](../fyles_service/DESIGN_PATTERNS.md) | Catalogue of 22 identified patterns across frontend and backend |

---

## 5. Key Architectural Decisions

### 5.1 Hexagonal Architecture with Factory-Based DI

The `core` crate never depends on concrete implementations. All external concerns (DB, P2P, filesystem, API) are defined as Rust traits. The `entrypoint()` function accepts four `async` factory closures that construct the concrete adapters. This allows the same `Brain` to be driven by:

- A desktop `daemon` binary with `DirectFsController` and local P2P
- An Android foreground service via `kotlin_ffi` with JNI-based `HostController`
- An iOS background task via `c_ffi` with C-callback-based `HostController`
- A test harness with `MockDb` and `MockP2pNode`

### 5.2 gRPC as the IPC Boundary

Frontend and backend communicate exclusively via gRPC, defined by shared `.proto` files. On desktop, this uses TCP with bearer-token auth. On mobile, it uses Unix Domain Sockets (co-process). This means the frontend and backend can be developed, tested, and even replaced independently.

### 5.3 Actor-Based Concurrency

The `Brain` and `LocalEventLoop` each run as actor-like entities in `tokio::select!` loops. They own their state exclusively and communicate via typed `mpsc` channels. No shared mutable state, no locks in the hot path.

### 5.4 Two-Tier P2P: Local + Internet

The `p2p` crate provides local-network functionality (mDNS discovery, direct connections). The `p2p_internet` crate layers internet capabilities on top (Kademlia DHT, relay servers, DCUtR hole-punching) using a plugin architecture (`FileTrackerPlugin` trait).

**Open Source Strategy:** Great effort was invested to cleanly separate these two layers. Eventually, the backend code containing only the local discovery (`p2p`, `core`, `crypto`, etc.) shall be open-sourced, while the internet variants (`p2p_internet`, `daemon_internet`) shall remain closed source.

### 5.5 Hybrid Post-Quantum Cryptography

All peer-to-peer communication is encrypted with ChaCha20-Poly1305 using session keys derived from a hybrid X25519 + Kyber key exchange. Contact identity uses a dual-signature scheme: Ed25519 (classical) + Dilithium5 (post-quantum), following the hybrid TLS draft specification.

---

## 6. Data Flow Example: Sending a File

```
User drags file    →  Flutter picks file path
onto UI               │
                      ▼
                  AppBloc dispatches UploadFileToPendingFiles event
                      │
                      ▼
                  ProtobufClient.uploadFilesToPendingFiles()
                      │  gRPC StorePendingFile
                      ▼
                  ApiServer → BrainAction::Client(StorePendingFile)
                      │
                      ▼
                  Brain stores in DB, tells P2P: send_files(...)
                      │  mpsc
                      ▼
                  P2pClient sends P2pCommand::SendFiles
                      │  mpsc
                      ▼
                  EventLoop: FileTracker queues file,
                  discovers peer (mDNS or Kademlia),
                  opens request-response stream
                      │
                      ▼
                  Encrypted chunked transfer
                  (ChaCha20-Poly1305 session)
                      │
                      ▼
                  Receiver: HostController writes to disk,
                  Brain stores ReceivedFile in DB,
                  pushes ServerMessage to gRPC stream
                      │
                      ▼
                  Flutter: BLoC receives PendingFileStatusChangedEvent,
                  updates AppState, UI rebuilds
```

---

## 7. Testing Strategy

| Level | Tool | Location |
|-------|------|----------|
| **Unit tests** | `#[cfg(test)]` modules | Throughout `core/`, `p2p/`, `crypto/` |
| **Integration (backend)** | Shared test functions run against SQLite + MockDb | `core/src/core/db.rs` |
| **Integration (P2P)** | `libp2p-swarm-test` | `p2p/tests/` |
| **E2E** | Shell script driving two daemon instances | `e2e_test/e2e_file_loop.sh` |
| **Frontend** | Flutter integration tests | `fyles/integration_test/` |

Mock implementations (`MockDb`, `MockP2pNode`) implement the same trait ports as production code and are validated for behavioural equivalence using shared test suites.
