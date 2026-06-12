# Deployment & Platform Integration

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Deployment Model

Fyles runs as a **two-process architecture** on every platform:

| Process | Technology | Role |
|---------|-----------|------|
| **Backend** | Rust binary or shared library | P2P networking, file transfers, database, gRPC server |
| **Frontend** | Flutter app | UI, user interaction, gRPC client |

They communicate via gRPC. The transport depends on the platform:

| Platform | Transport | Auth |
|----------|-----------|------|
| Windows / Linux | TCP localhost (random port) | Bearer token (written to `api_server_config` file) |
| macOS | Unix Domain Socket (`~/Library/Application Support/Fyles/fyles.sock`) | None (file-system perms) |
| Android | Unix Domain Socket | None (same app sandbox) |
| iOS | Unix Domain Socket | None (same app sandbox) |

---

## 2. Platform Matrix

| Platform | Backend Binary | FFI Bridge | Frontend | P2P Mode |
|----------|---------------|------------|----------|----------|
| **Linux** | `daemon` / `daemon_internet` | None (separate process) | Flutter Linux | Local or Internet |
| **macOS** | `daemon` / `daemon_internet` | None (LaunchAgent) | Flutter macOS | Local or Internet |
| **Windows** | `daemon` | None (separate process) | Flutter Windows | Local |
| **Android** | `libfyles_kotlin_ffi.so` | JNI (`kotlin_ffi`) | Flutter Android | Internet |
| **iOS** | `libfyles_c_ffi.a` | C FFI (`c_ffi`) | Flutter iOS | Internet |

---

## 3. Desktop Deployment

### 3.1 Daemon Binaries

Two daemon variants exist as independent Rust binaries:

- **`daemon`** — uses `p2p` crate directly (mDNS, local connections)
- **`daemon_internet`** — uses `p2p_internet` (Kademlia, relay, DCUtR)

Both use `clap` for CLI argument parsing:

```
fyles-daemon [OPTIONS]
  -d, --db-path <PATH>         SQLite database path
  -e, --endpoint <ENDPOINT>    tcp:HOST:PORT or uds:/path/socket
  -o, --output-dir <DIR>       Directory for received files
  --otel-endpoint <URL>        Optional OTLP tracing endpoint
```

### 3.2 IPC Setup (Desktop)

**Windows / Linux** (TCP):
1. Daemon starts, binds gRPC to `tcp:127.0.0.1` (auto port assignment)
2. Writes `port:passcode` to `api_server_config` file
3. Flutter app reads this file via `TcpConfig.get()` to discover port and auth token
4. All gRPC calls include `Bearer <passcode>` in metadata

**macOS** (UDS): the daemon binds gRPC to a Unix Domain Socket at
`~/Library/Application Support/Fyles/fyles.sock`, and the Flutter app connects to the same fixed path
(`defaultUdsPath()`). No port, auth token, or `api_server_config` file is involved — access is gated
by file-system permissions, as on iOS/Android.

### 3.3 Host Controller (Desktop)

`DirectFsController` (`direct_host_utils/src/direct_fs_controller.rs`) implements `HostController` using standard filesystem operations (`tokio::fs`). It manages a configurable output directory for received files, organizes files into subdirectories per filerequest, and handles file deduplication by appending suffixes.

**Storage & cleanup (see [Backend §4](backend.md#4-file-storage--cleanup)).** The controller keeps `Received/`, `Backup/`, and `Partial/` siblings under the output root. `.part` files are opened for append in `Partial/`; `finalize_partial_file` splits the destination namespace into directory + filename, creates the per-filerequest directory (with the same path-escape and canonicalization checks used for normal writes), dedupes on collision, then renames across the (same-filesystem) `Partial/` → `Received/` boundary. Partial-file methods validate that the filename is a bare basename (defence-in-depth, since the name derives from the wire-supplied `transfer_id`). Desktop never copies shared files into an outgoing directory, so `remove_source_file_if_temporary` returns `false` and `list_temporary_outgoing_files` returns empty — the outgoing-cleanup flow is a no-op here.

### 3.4 macOS LaunchAgent (autostart at login)

On macOS the daemon (`daemon_internet`) runs as a per-user LaunchAgent so it starts at login without
the GUI. The app bundle ships the daemon, a tiny compiled launcher, and a LaunchAgent plist; the GUI
registers the agent on first run via `SMAppService` (`MethodChannel('app.fyles.fyles/service')` →
`registerLaunchAgent`). The launcher resolves `$HOME` at runtime (which `launchd` won't do in a
plist) and execs the otherwise-generic daemon with explicit UDS/DB/output paths. Requires Developer
ID signing + notarization and running from `/Applications`.

See **[macOS daemon autostart setup](../macos-autostart-setup.md)** for the full architecture, build
phases, signing/notarization, and verification steps.

### 3.5 Windows Subsystem

When built in release mode without the `console` feature, the daemon runs as a windowless process:
```rust
#[cfg_attr(
    all(target_os = "windows", not(debug_assertions), not(feature = "console")),
    windows_subsystem = "windows"
)]
```

---

## 4. Android Deployment

### 4.1 Architecture

```
┌─────────────────────────────────────┐
│          Flutter UI (Dart)          │
│  ProtobufClient ←→ gRPC (UDS)      │
└─────────────┬───────────────────────┘
              │ MethodChannel
┌─────────────┴───────────────────────┐
│       Kotlin Foreground Service     │
│  HostController (SAF file access)   │
│  NotificationManager                │
│           │ JNI                      │
│  ┌────────┴──────────────────────┐  │
│  │   libfyles_kotlin_ffi.so      │  │
│  │   (Rust → JNI bridge)         │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

### 4.2 JNI Bridge (`kotlin_ffi`)

The `kotlin_ffi` crate exposes a single JNI entry point:

```rust
pub extern "system" fn Java_app_fyles_fyles_service_Fyles_createFfiHostController(
    env: JNIEnv, _class: JClass,
    class_loader: JObject,       // for loading notification classes
    host_controller: JObject,    // Kotlin HostController instance
    db_path: JString,
    endpoint: JString,
    otel_endpoint: JString,
)
```

This function:
1. Stores the JVM and Kotlin `HostController` reference in a global `OnceLock`
2. Spawns the Rust `entrypoint()` on a new thread
3. Provides `UnixJvmHostController` — a `HostController` impl that calls back into Kotlin via JNI for every file operation

### 4.3 File Operations via JNI

The Kotlin `HostController` provides methods like `request_read`, `request_write`, `remove_file`, `create_resource` that operate through Android's Storage Access Framework. The JNI bridge:

1. Attaches to the JVM thread
2. Calls the Kotlin method, passing paths as `JString`
3. Extracts `FileDescriptor` from the returned `FfiFile` object
4. Converts the raw fd to a Rust `OwnedFd` → `tokio::fs::File`

### 4.4 Notifications

Rust triggers Android notifications via JNI by instantiating `FileReceivedNotification` objects and calling `HostController.sendNotification()` on the Kotlin side. The `ClassLoader` reference is needed because JNI cannot find app classes from native-spawned threads without it.

### 4.5 Storage & Cleanup (Android)

The Kotlin `HostController` (`FylesCoreImpl`) backs the cleanup ports (see [Backend §4](backend.md#4-file-storage--cleanup)) over the Storage Access Framework:

- **`Partial/`** is a sibling of `Received/`/`Backup/` in the SAF tree. `getPartialFileForWriting` opens the file in append mode (`"wa"`), `finalizePartialFile` moves it via `DocumentsContract.moveDocument` (with a copy-then-delete fallback) into the per-filerequest directory.
- **`fyles_outgoing/`** lives in internal app-support storage (`context.filesDir`) — *not* the cache dir, which Android may evict mid-transfer. `removeSourceFileIfTemporary` deletes only paths under that directory.

Listing crosses the JNI boundary as typed Java objects: `listPartialFiles()` returns `String[]`, and `listTemporaryOutgoingFiles()` returns `OutgoingFile[]` (a `@Keep` POJO with `path` + `lastModifiedMs`). The Rust bridge reads them with `get_array_length` / `get_object_array_element` / `get_field`, the same pattern used to read the `FfiFile` returned by `request_read`.

---

## 5. iOS Deployment

### 5.1 Architecture

```
┌─────────────────────────────────────┐
│          Flutter UI (Dart)          │
│  ProtobufClient ←→ gRPC (UDS)      │
└─────────────┬───────────────────────┘
              │ (in-process)
┌─────────────┴───────────────────────┐
│       Swift App Delegate            │
│           │ C function pointers     │
│  ┌────────┴──────────────────────┐  │
│  │   libfyles_c_ffi.a            │  │
│  │   (Rust → C FFI bridge)       │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

### 5.2 C FFI Bridge (`c_ffi`)

The `c_ffi` crate exposes a C-ABI entry point:

```rust
pub extern "C" fn create_ffi_host_controller(
    read_request: ReadRequestFn,                 // fn(i32, *const c_char) -> RawFd
    write_request: WriteRequestFn,               // fn(i32, *const c_char) -> FfiWriteResult
    write_exact_request: WriteExactRequestFn,
    remove_file: RemoveFileFn,
    create_resource: CreateResourceFn,
    get_path_to_db_backup_file: GetPathToDbBackupFileFn,
    rename_resource: RenameResourceFn,
    send_notification: SendNotificationFn,
    remove_source_file_if_temporary: RemoveSourceFileIfTemporaryFn,
    list_temporary_outgoing_files: ListTemporaryOutgoingFilesFn,  // -> FfiOutgoingFileArray
    list_partial_files: ListPartialFilesFn,                       // -> FfiStringArray
    db_path: *const c_char,
    endpoint: *const c_char,
)
```

Swift provides C function pointers for each file operation. The Rust side stores them in a global `OnceLock<State>` and implements `CHostController` which calls back through these function pointers.

> **Header generation.** `c_ffi/include/fyles.h` is produced by `cbindgen` (`c_ffi/build_cbindings.sh`) and imported into Swift via `Runner-Bridging-Header.h`. The header is the single source of truth for the C contract — the function-pointer and struct typedefs (`SendNotificationFn`, `FfiOutgoingFileArray`, …) come from it, so Swift must not hand-declare duplicates. **Regenerate the header after changing any `c_ffi` signature.**

### 5.3 Storage & Cleanup (iOS)

The Swift host implements the cleanup ports (see [Backend §4](backend.md#4-file-storage--cleanup)):

- **`Partial/`** lives inside the `Drop/` tree; `.part` files are opened for append and `finalize_partial_file` renames them into the per-filerequest directory under `Drop/`.
- **`fyles_outgoing/`** lives in the **Application Support** directory (matching Flutter's `getApplicationSupportDirectory()`), not a temp/cache directory the OS could purge mid-transfer. `ffi_remove_source_file_if_temporary` only deletes paths under it.

Listing crosses the C boundary as structs, following the existing `FfiWriteResult` ownership rule: the Swift host `malloc`s the array and `strdup`s each string, Rust reads the entries into a `Vec`, then frees each string and the backing array with `libc::free`. `list_partial_files` returns an `FfiStringArray { ptr, len }`; `list_temporary_outgoing_files` returns an `FfiOutgoingFileArray` of `FfiOutgoingFile { path, modified_ms }`. An empty listing is signalled by a null `ptr` / `len == 0`.

### 5.4 Share Extension

`FylesShareExtension` (iOS) is a separate target that handles the iOS share sheet, forwarding selected files to the main app.

### 5.5 UDS Path Considerations

iOS has strict `sockaddr_un.sun_path` length limits (~104 bytes). The UDS path is kept short: `"fyles.sock"` (relative path, relying on correct working directory).

---

## 6. Build System

### 6.1 Build Scripts

| Script | Purpose |
|--------|---------|
| `build.sh` | Orchestrates full build: server + Flutter for current or specified platform |
| `server_build.sh` | Builds backend binaries with cross-compilation support |
| `fyles_service/build_android.sh` | Cross-compiles Rust for `aarch64-linux-android` |
| `c_ffi/build_ios.sh` | Builds iOS static library |
| `c_ffi/build_full_ios.sh` | Full iOS build including universal binary creation |
| `FylesInstaller.iss` | Inno Setup script for Windows installer |
| `build_macos_installer.sh` | macOS `.pkg` installer builder |

### 6.2 Observability

The backend supports OpenTelemetry tracing via the `otel-jaeger` feature flag:

```toml
[features]
otel-jaeger = ["opentelemetry", "opentelemetry-otlp", "tracing-opentelemetry", ...]
```

Platform-specific tracing backends:
- Desktop: `tracing-subscriber` with env-filter
- Android: `tracing-android` (logcat)
- iOS: `tracing-oslog`

### 6.3 Nix

Both the project root and `fyles_service/` have `flake.nix` files for reproducible dev environments.
