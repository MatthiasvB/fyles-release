# Frontend Architecture

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Technology Stack

| Concern | Technology |
|---------|-----------|
| Framework | Flutter (cross-platform: Android, iOS, macOS, Linux, Windows) |
| Language | Dart |
| State management | flutter_bloc (BLoC pattern) |
| Backend communication | gRPC (via `protoc`-generated Dart stubs) |
| Localization | easy_localization |
| QR codes | Custom chunked QR encode/decode (`qr_chunk.dart`) |
| Sharing intent | receive_sharing_intent (Android/iOS share sheets) |

---

## 2. Directory Structure

```
fyles/lib/
├── main.dart              ← App entry point, routing, sharing intent handling
├── bloc/
│   ├── app_bloc.dart      ← Central BLoC (~50 event handlers)
│   ├── app_event.dart     ← ~50 event classes (sealed AppEvent hierarchy)
│   ├── app_state.dart     ← Immutable AppState with copyWith
│   ├── dialog_state.dart  ← Sealed DialogState hierarchy (~18 subtypes)
│   ├── app_message.dart   ← Snackbar message model
│   └── ui_state.dart      ← ActivePage enum
├── models/
│   ├── wire.dart          ← ToWire / FromWire / Wire interfaces
│   ├── file_request.dart  ← FileRequest + PublicFileRequest / AudienceFileRequest
│   ├── contact.dart       ← Contact model
│   ├── remote_file_request.dart  ← RemoteFileRequest + PendingFile
│   ├── received_file.dart ← ReceivedFile model
│   ├── peer.dart          ← Peer / CreatePeer models
│   ├── self_contact.dart  ← SelfContact model
│   └── share_data.dart    ← ShareData types for QR/clipboard exchange
├── services/
│   ├── protobuf_client.dart      ← gRPC client façade
│   ├── dialog_manager.dart       ← Stateless dialog-showing service
│   ├── filesystem.dart           ← StorageAccess abstract class
│   ├── filesystem_android.dart   ← Android SAF implementation
│   ├── filesystem_default.dart   ← Desktop/iOS implementation
│   ├── app_initialization_service.dart
│   ├── permissions_service.dart
│   └── log.dart
├── pages/                 ← 10 screen-level widgets
├── widgets/               ← 25 reusable components
├── wire/                  ← Generated protobuf code (*.pb.dart, *.pbgrpc.dart)
├── config/                ← App configuration, localization config
└── utils/
    ├── result.dart        ← Sealed Result<S,E> type
    └── conversion.dart
```

---

## 3. BLoC State Management

The frontend follows a strict **unidirectional data flow**:

```
UI interaction → AppEvent → AppBloc → emit(AppState) → UI rebuilds
```

### 3.1 Events (`app_event.dart`)

~50 event classes extending `AppEvent`. Categories:

| Category | Examples |
|----------|---------|
| Data CRUD | `CreateFileRequest`, `DeleteContact`, `UpdateRemoteFileRequest` |
| Navigation | `SetActivePage` |
| Dialog open | `OpenCreateFileRequestDialogEvent`, `OpenShareContactDialogEvent` |
| Dialog result | `FileRequestDialogResultEvent`, `ConfirmDeleteDialogResultEvent` |
| Server push | `PendingFileStatusChangedEvent`, `ReceivedFileStatusChangedEvent`, `DatabaseRestoredEvent` |
| UI feedback | `ShowMessageEvent` |

### 3.2 State (`app_state.dart`)

A single immutable `AppState` holding:

- `Map<String, Contact> contacts`
- `Map<String, FileRequest> fileRequests`
- `Map<String, RemoteFileRequest> remoteFileRequests`
- `Map<String, List<ReceivedFile>> receivedFiles`
- `Map<String, ActiveReceiveTransfer> activeReceiveTransfers`
- `DialogState dialogState` (sealed hierarchy)
- `ActivePage activePage`
- `StorageAccess? storageAccess`
- Navigation, loading, error, and self-contact fields

Updates use `copyWith()` for immutable transitions.

### 3.3 BLoC (`app_bloc.dart`)

The `AppBloc` constructor registers ~50 typed `on<EventType>` handlers. Each handler:
1. Optionally sets a loading flag
2. Calls `ProtobufClient` methods
3. Emits new state via `emit(state.copyWith(...))`

The BLoC also owns a `Timer.periodic` for polling refresh (received files, self-contact display) and bridges server push notifications via `getServerMessageHandler()`.

---

## 4. Wire Boundary (Anti-Corruption Layer)

### 4.1 The `Wire` Interface

```dart
abstract class ToWire<T extends GeneratedMessage> { T toWire(); }
abstract class FromWire<T extends GeneratedMessage, M> { Result<M, String> fromWire(T proto); }
abstract class Wire<T extends GeneratedMessage, M> implements ToWire<T>, FromWire<T, M> {}
```

Every domain model implements `Wire`, providing bidirectional conversion between protobuf types and domain types. Conversion errors are modelled as `Result<M, String>` (sealed type), not exceptions.

### 4.2 ProtobufClient

`ProtobufClient` (`services/protobuf_client.dart`) is a **façade** over the generated gRPC stubs. It:

- Manages `ClientChannel` lifecycle (TCP on desktop, UDS on mobile)
- Provides `_ensureConnection()` with automatic reconnection on `StatusCode.unavailable`
- Translates between protobuf types and domain types using the `Wire` interface
- Manages the bidirectional `Stream` RPC for server push notifications

### 4.3 Server Push → BLoC Events

```dart
Function(ServerMessage) getServerMessageHandler(AppBloc appBloc) {
  return (ServerMessage message) {
    switch (message.whichMessage()) {
      case ServerMessage_Message.pendingFileStatusChanged:
        appBloc.add(PendingFileStatusChangedEvent(...));
      case ServerMessage_Message.receivedFileStatusChanged:
        appBloc.add(ReceivedFileStatusChangedEvent(...));
      // ... other cases
    }
  };
}
```

This unifies server-push and user-initiated events into a single processing pipeline.

---

## 5. Dialog Management

Dialogs are managed through the BLoC as state, not imperatively:

1. UI dispatches `Open*DialogEvent`
2. BLoC prepares data, emits `state.copyWith(dialogState: SomeDialog(...))`
3. `BlocListener` in `main.dart` detects the change
4. `DialogManager.showDialogForState()` exhaustively switches over `DialogState` sealed hierarchy
5. Dialog result is dispatched back as a `*DialogResultEvent`
6. BLoC processes the result, emits `NoDialog` to close

This keeps all dialog logic in the BLoC, not scattered across widgets.

---

## 6. Platform-Specific Storage

`StorageAccess` is an abstract class with platform-specific implementations:

| Implementation | Platform | Mechanism |
|----------------|----------|-----------|
| `AndroidStorageAccess` | Android | `MethodChannel('native.storage')` → SAF APIs via Kotlin |
| `DefaultStorageAccess` | iOS, macOS, Linux, Windows | `file_picker` + `shared_preferences` |

Selected at runtime via `StorageAccess.instance()` factory method.

---

## 7. Sharing Intent Integration

The app handles incoming files from the OS share sheet:

1. `ReceiveSharingIntent.instance.getMediaStream()` provides a stream of shared files
2. `copySharedFilesToOutgoing()` (`services/outgoing_files_service.dart`, shared by `main.dart` and `AppInitializationService`) copies each shared file into `<app-support>/fyles_outgoing/` and returns `PathAndDisplayName` objects pointing at the copies
3. Navigates to `ShareTargetPage` where the user picks a remote file request target
4. BLoC dispatches `UploadFileToPendingFiles` → gRPC → backend queues the transfer

The copy in step 2 exists so the file survives in stable storage until the transfer finishes (the OS-provided share path is temporary), and so the backend can clean it up afterward. The destination is **application-support** storage, not cache — the OS must not purge it mid-transfer — and the backend's cleanup subsystem both deletes each copy once its send reaches a terminal state and periodically sweeps the directory for orphans. See [Backend §4: File Storage & Cleanup](backend.md#4-file-storage--cleanup).
