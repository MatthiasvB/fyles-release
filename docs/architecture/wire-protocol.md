# Wire Protocol & API Contract

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Contract-First Design

The entire client-server interface is defined in `.proto` files in the shared `wire/` directory. Code is generated into both technology stacks:

| Stack | Generator | Output |
|-------|-----------|--------|
| Rust backend | `tonic-build` + `prost` | Server stubs + message types |
| Dart frontend | `protoc` + `grpc` plugin | `*.pb.dart`, `*.pbgrpc.dart`, `*.pbenum.dart` |

### Design Philosophy (from `filerequest.proto`)

```protobuf
// - No implicitly optional parameters in request messages
// - Each endpoint should have exactly one clear purpose
// - Split endpoints instead of using optional parameters
```

---

## 2. Services

### 2.1 FileRequestService (~30 RPCs)

The primary service covering all application operations:

| Category | RPCs |
|----------|------|
| **Lifecycle** | `WaitForReady`, `GetVersion`, `Shutdown` |
| **FileRequests** | `CreateFileRequest`, `GetFileRequest`, `UpdateFileRequest`, `DeleteFileRequest`, `GetAllFileRequests` |
| **Contacts** | `GetContact`, `GetAllContacts`, `UpdateContact`, `DeleteContact`, `RegisterContact` |
| **Self Contact** | `GetFullSelfContact`, `UpdateIdentity`, `GetSelfContactDisplay`, `UpdateSelfContactName`, `SharePublicSelfContact` |
| **Remote FileRequests** | `StoreRemoteFileRequest`, `UpdateRemoteFileRequest`, `GetRemoteFileRequest`, `ListRemoteFileRequests`, `GetAllRemoteFileRequests`, `DeleteRemoteFileRequest` |
| **Pending Files** | `StorePendingFile`, `GetPendingFile`, `ListPendingFiles`, `GetAllPendingFiles`, `DeletePendingFile` |
| **Received Files** | `ListReceivedFiles`, `DeleteReceivedFile` |
| **Identity Exchange** | `GetSelfContactInviteChallenge`, `UnregisterSelfContactInviteChallenge`, `UseSelfContactInvite`, `GetContactShareChallenge`, `UnregisterContactShareChallenge`, `UseContactShareChallenge` |
| **Data Management** | `BackupData`, `RestoreData`, `UpdateSettings`, `GetSettings` |
| **Streaming** | `Stream` (bidirectional) |

### 2.2 NodeStatusService

Diagnostics and health-checking:

| RPC | Purpose |
|-----|---------|
| `GetNodeStatus` | Returns peer ID, multiaddresses, version, stats (uptime, peers, bandwidth) |
| `HealthCheck` | Simple liveness probe |

---

## 3. Key Message Types

### 3.1 Domain Messages

```protobuf
message FileRequestProto {
  string id = 1;
  string title = 2;
  string description = 3;
  AccessProto access = 4;    // oneof: PublicProto | SpecificUsersProto
  bool is_active = 6;
}

message ContactProto { string id = 1; string name = 2; }

message PendingFileProto {
  string id = 1;
  string file_path = 2;
  string target_file_request_id = 3;
  SendStatusProto send_status = 4;
  optional uint64 progress_bytes = 5;
  optional string transfer_id = 6;
  optional uint64 file_size_bytes = 7;
  optional string display_name = 8;
}
```

### 3.2 State Enums

```protobuf
enum SendStatusProto {
  PENDING = 0; SENDING = 1; SUCCESS = 2; REJECTED = 3;
  FAILED = 4; PREPARED = 5; INTERRUPTED = 6; PENDING_SENT = 7;
}

enum ReceiveStatusProto {
  RECEIVE_STATUS_RECEIVING = 0; RECEIVE_STATUS_INTERRUPTED = 1;
  RECEIVE_STATUS_COMPLETED = 2; RECEIVE_STATUS_FAILED = 3;
}
```

---

## 4. Bidirectional Streaming

The `Stream` RPC is the push-notification channel:

```protobuf
rpc Stream (stream ClientMessage) returns (stream ServerMessage);
```

### ServerMessage Envelope

```protobuf
message ServerMessage {
  oneof message {
    ReceivedSelfContactInviteOverNetwork  received_self_contact_invite  = 1;
    SelfContactInviteAcceptedOverNetwork  self_contact_invite_accepted  = 2;
    ReceivedContactShareOverNetwork       received_contact_share        = 3;
    DatabaseRestoredFromBackup            database_restored             = 4;
    PendingFileStatusChanged              pending_file_status_changed   = 5;
    ReceivedFileStatusChanged             received_file_status_changed  = 6;
  }
}
```

This acts as a **discriminated-union event bus** over a single long-lived gRPC stream. The frontend maps each variant to a BLoC event.

### Real-Time Transfer Progress

```protobuf
message PendingFileStatusChanged {
  string pending_file_id = 1;
  SendStatusProto new_status = 2;
  optional uint64 progress_bytes = 3;
  optional string transfer_id = 4;
  optional uint64 file_size_bytes = 5;
  string target_file_request_id = 6;
}

message ReceivedFileStatusChanged {
  string transfer_id = 1;
  string filerequest_id = 2;
  ReceiveStatusProto new_status = 3;
  uint64 progress_bytes = 4;
  uint64 file_size_bytes = 5;
  string file_name = 6;
}
```

These messages enable real-time progress bars and status updates in the UI.

---

## 5. Request/Response Conventions

All RPCs follow a consistent pattern:

- **Dedicated Request/Response types** per RPC (e.g., `CreateFileRequestRequest` / `CreateFileRequestResponse`)
- **Empty request messages** for parameterless queries (e.g., `GetAllFileRequestsRequest {}`)
- **`bool success`** for simple mutation acknowledgments
- **Returned entity** for creates/updates that need the server-generated ID

---

## 6. Anti-Corruption Layers

Generated protobuf types never leak into domain logic on either side:

| Side | Mechanism |
|------|-----------|
| **Rust backend** | `domain_proto_conversion.rs` — 100+ `From`/`TryFrom` impls between `*Proto` types and domain models |
| **Dart frontend** | `Wire<T, M>` interface — `toWire()` / `fromProto()` on every model class, returning `Result<M, String>` |

This creates a symmetric translation boundary, ensuring both frontend and backend domain models remain clean.
