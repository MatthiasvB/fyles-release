# Deployment & Platform Integration

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Deployment Model

Fyles runs as a **two-process architecture** on every platform:

| Process | Technology | Role |
|---------|-----------|------|
| **Backend** | Rust binary or shared library | P2P networking, file transfers, database, gRPC server |
| **Frontend** | Flutter app | UI, user interaction, gRPC client |

They communicate via gRPC. The transport depends on the platform. Every endpoint is private to one
user, so several users on one machine never meet on it, and the frontend never offers it as a
setting:

| Platform | Transport | Auth |
|----------|-----------|------|
| Windows | TCP localhost (random port) | Bearer token (written to `%LOCALAPPDATA%\Fyles\api_server_config`) |
| Linux | Unix Domain Socket (`$XDG_RUNTIME_DIR/fyles.sock`) | None (file-system perms) |
| macOS | Unix Domain Socket (`~/Library/Application Support/Fyles/fyles.sock`) | None (file-system perms) |
| Android | Unix Domain Socket | None (same app sandbox) |
| iOS | Unix Domain Socket | None (same app sandbox) |

Debug builds on Linux and macOS use the Windows TCP scheme, with the `api_server_config` location
passed as `--dart-define=FYLES_INTERNAL_DATA_DIR`.

The port peers reach the node on is a separate, per-user setting stored in the database. See
[Networking §9](networking.md#9-listen-port).

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

**Windows** (TCP):
1. Daemon starts, binds gRPC to `tcp:127.0.0.1` (auto port assignment)
2. Writes `port:passcode` to `api_server_config` file
3. Flutter app reads this file via `TcpConfig.get()` to discover port and auth token
4. All gRPC calls include `Bearer <passcode>` in metadata

**Linux** (UDS): the daemon binds `$XDG_RUNTIME_DIR/fyles.sock`, and the Flutter app connects to
that path. Where `XDG_RUNTIME_DIR` is unset, both use `~/.local/share/app.fyles.fyles/fyles.sock`.
Fyles reaches Linux as a Nix package ([ADR 0013](adr/0013-linux-through-nix.md)):
- `fyles-daemon-start` chooses the daemon's paths. The database lives in `$XDG_DATA_HOME/fyles`,
  and received files go to `~/Downloads/Fyles` unless `FYLES_OUTPUT_DIR` names another folder.
- `fyles` starts the daemon in the background and opens the app.
- A systemd user unit, from the package or from the NixOS or Home Manager module, keeps the daemon
  running from login.
- The daemon holds `<db-path>.lock`, and `<socket>.lock` for a Unix socket, for its life. A
  second daemon on the same database or socket exits with 75. The units count that as a clean
  exit, so they neither restart nor show as failed, and starting a daemon needs no check.
- `--listen-port` (or `FYLES_LISTEN_PORT`) pins the peer port. A user's `openFirewall` opens it,
  and mDNS, in the NixOS firewall.

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
plist) and execs the otherwise-generic daemon with explicit UDS/DB/output paths. It also sends the
daemon's output to `~/Library/Logs/Fyles/daemon.{out,err}.log`. A shared path such as `/tmp` would
belong to whichever user wrote it first, and `launchd` would then fail to start every other user's
agent. Requires Developer ID signing + notarization and running from `/Applications`.

See **[macOS daemon autostart setup](../macos-autostart-setup.md)** for the full architecture, build
phases, signing/notarization, and verification steps.

### 3.5 Windows Installer

The installer is per-user (`PrivilegesRequired=lowest`, installed under `%LOCALAPPDATA%`), and the
daemon autostarts from the user's `HKCU\...\Run` key. Stopping the daemon on upgrade or uninstall
filters `taskkill` by the current user, so another signed-in user's daemon keeps running.

### 3.6 Windows Subsystem

When built in release mode without the `console` feature, the daemon runs as a windowless process:
```rust
#[cfg_attr(
    all(target_os = "windows", not(debug_assertions), not(feature = "console")),
    windows_subsystem = "windows"
)]
```

### 3.7 Host Conditions (Desktop)

`host_state::subscribe()` (`direct_host_utils/src/host_state/`) backs `HostController::host_state()`
on desktop and feeds the internet-mode run conditions (see
[ADR-0008](adr/0008-host-run-conditions-for-internet-mode.md)). It owns one process-wide `watch`
channel and starts the platform watchers the first time anyone subscribes, so the readings survive a
client detaching and reattaching. Linux, macOS and Windows each answer from one `#[cfg]`-gated file.
Android and iOS answer differently — their host pushes the readings across the FFI (§4.2, §5.6) —
so no port is left on the `Unsupported` default.

**The composition root decides whether to observe at all.** `DirectFsController::new` takes the
receiver rather than subscribing, because subscribing is what starts the watchers: `daemon_internet`
passes `host_state::subscribe()`, and `daemon` — whose `NoOpBrainPlugin` discards the readings —
passes `host_state::unsupported()`. Without that split the local-network build would hold a D-Bus
connection, or WinRT event registrations, or an `NWPathMonitor`, reporting to nobody for the life of
the process. Android and iOS already work this way: `FylesService.onCreate` and `AppDelegate` start
their reporters, and nothing below them decides.

**The channel opens on `Unknown` wherever a watcher exists**, and on `Unsupported` only on the
fallback `#[cfg]` arm where nothing will ever publish. `Brain::run` reads the current value
synchronously and hands it to `BrainPlugin::start`, so on Linux — where the watcher is a
`tokio::spawn` that has not run yet — an `Unsupported` seed would let the swarm be built in
`NetworkMode::Internet` and only stop `DHT_STOP_DEBOUNCE` later. That is the same window the FFI
hosts open on `Unknown` to avoid. It costs nothing on a machine that genuinely cannot answer,
because a watcher that finds no service publishes `Unsupported` promptly.

**Linux reads the D-Bus system bus.** Two watchers own one `HostState` field each:

| service | property | reading |
|---|---|---|
| `org.freedesktop.UPower` | `OnBattery` | `false` → external power, `true` → battery |
| `org.freedesktop.NetworkManager` | `PrimaryConnection` | `"/"` → no network |
| `org.freedesktop.NetworkManager` | `Metered` (`NMMetered`) | `YES`/`GUESS_YES` → metered, `NO`/`GUESS_NO` → unmetered, `UNKNOWN` → unknown |

`PrimaryConnection == "/"` is NetworkManager's "no default route", the same thing Android's
default-network callback reports, so an absent network means the same on both. The `GUESS_*` values
are honoured: NetworkManager emits them where it inferred the cost from the device type rather than
reading an explicit setting.

A service that is not on the bus makes its reading `Unsupported`, which never blocks. That is how
a machine running systemd-networkd instead of NetworkManager ends up saying so in the UI rather
than guessing.

**Signals rather than the property cache.** Proxies are built with `CacheProperties::No` and every
wake re-reads live. The wake stream is `PropertiesChanged` for the object merged with the
destination's `NameOwnerChanged`, neither of which depends on zbus's property cache — which
`Proxy::receive_property_changed` requires and which would otherwise keep serving a value from
before a `systemctl restart NetworkManager`. Losing the name is itself a wake, and the read it
triggers fails and reports the signal unavailable.

The one case decided once is `Connection::system()` failing (a container with no
`/run/dbus/system_bus_socket`): there is no bus to be notified on, so both readings stay
`Unsupported`. zbus itself is not a new dependency — `notify-rust` already pulls it in for desktop
notifications, and its `async-io` backend is left alone.

**Windows reads WinRT.** `PowerManager::BatteryStatus` answers the power question and
`NetworkInformation::GetInternetConnectionProfile` the network one, refreshed from
`BatteryStatusChanged` and `NetworkStatusChanged`. `CoIncrementMTAUsage` puts the process in the
multi-threaded apartment for both the statics and the pool threads the callbacks arrive on, and the
count is never decremented because the watchers run until exit. A watcher that cannot register
reports `Unsupported`, the same fallback a missing D-Bus service gets.

Three readings are worth spelling out:

- **No battery means external power.** `BatteryStatus::NotPresent` is a desktop, and only
  `Discharging` is battery power. An inadequate charger still shows as `Discharging`, which is the
  answer the user's setting is asking for.
- **A missing connection profile is not an error.** With no profile to offer WinRT answers null,
  which windows-rs projects as an `Err` carrying a successful `HRESULT`. That shape alone maps to
  `NetworkStatus::Absent`, and a genuine failure (a failed `HRESULT`) maps to `Unknown` instead.
  Note what `Present` therefore asserts: Windows has a preferred interface for internet traffic,
  not that the internet is reachable over it. That is the same claim NetworkManager's
  `PrimaryConnection` makes on Linux, so the two platforms mean the same thing by it.
- **An unpriced link is still a link.** `NetworkCostType::Unknown` becomes `HostSignal::Unknown`,
  never `Absent`, so a cost Windows cannot classify is left to the strictness setting rather than
  stopping the DHT outright. Roaming, a passed data cap, or `BackgroundDataUsageRestricted` meters
  an otherwise unrestricted connection: reprovides are background traffic, and each of those says
  Windows would rather not carry it. That last one sits on a later interface than the others, so a
  build without it reads as unrestricted rather than poisoning the whole reading.

**macOS reads IOKit and Network.framework**, both onto **one serial dispatch queue** and no thread
of its own. Power comes from `IOPSGetProvidingPowerSourceType` over an `IOPSCopyPowerSourcesInfo`
snapshot, woken by the `kIOPSNotifyPowerSource` darwin-notify key through
`notify_register_dispatch`. That key is what avoids the obvious `IOPSNotificationCreateRunLoopSource`
route, which would need a thread parked in `CFRunLoopRun` for the process's life. Network comes from
a `nw_path_monitor`, whose update handler is handed the path rather than having to re-read it.

The queue being **serial is load-bearing, not incidental**. Blocks on it do not overlap, so the
block that publishes last is the block that read last — which is the guarantee Windows needs its two
refresh mutexes for. The initial power reading is therefore enqueued with `exec_async` rather than
read inline, so it is ordered against the notification callbacks instead of racing them. Making the
queue concurrent would silently reintroduce that race. The network half needs no initial read at
all, because `nw_path_monitor_start` delivers the current path through the same handler.

| `IOPSGetProvidingPowerSourceType` | reading |
|---|---|
| `"AC Power"` | external power |
| `"Battery Power"`, `"UPS Power"` | battery |
| no snapshot, or no named source | unknown |

| `nw_path_get_status` | reading |
|---|---|
| `satisfied` | present, metered when `nw_path_is_expensive` or `nw_path_is_constrained` |
| `unsatisfied` | no network |
| `satisfiable`, `invalid` | unknown |

Four readings are worth spelling out:

- **A UPS counts as battery.** macOS reports `"AC Power"` while the mains is up even with a UPS
  attached, so `"UPS Power"` appears only once the mains is lost and the machine is running down a
  store of charge. That is the state the setting exists to protect against.
- **`satisfiable` is unknown, not absent.** Apple's name for it in Swift is `requiresConnection`:
  the path is not usable now but a connection attempt could make it so, by dialling an on-demand
  VPN. Calling that `Absent` reads well until you notice the attempt that would activate the path
  could be ours, and `Absent` is the one reading no setting overrides — so a machine whose only
  route is such a VPN would be locked out of internet mode with no recourse. `Unknown` blocks
  identically under the default `strict_when_unknown`, and leaves that user a way out.
- **An inadequate charger reads as external power here, and as battery on Windows.** The providing
  source is still AC while the battery drains, where `BatteryStatus` reports `Discharging`. Same
  physical situation, different answer, and neither platform is wrong about its own question.
- **macOS never reports an unknown cost.** `nw_path_is_expensive` and `nw_path_is_constrained`
  return plain booleans and cannot decline to answer, so a `satisfied` path is always `Known`.
  `strict_when_unknown` therefore has no network work to do on macOS, unlike Linux
  (`NMMetered UNKNOWN`) and Windows (`NetworkCostType::Unknown`).

`nw_path_is_constrained` needs macOS 11, so `embed_daemon.sh` exports
`MACOSX_DEPLOYMENT_TARGET=11.0`. Without it the x86_64 slice would take rustc's 10.12 default, and
the symbol would weak-link to null and be called at runtime rather than failing the load.

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

The `kotlin_ffi` crate exposes two JNI entry points. The first is the initializer:

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

The second is the only place Kotlin pushes into Rust rather than being called from it:

```rust
pub extern "system" fn Java_app_fyles_fyles_service_Fyles_onHostStateChanged(
    _env: EnvUnowned, _class: JClass,
    power: jint,    // 0 unsupported, 1 unknown, 2 external, 3 battery
    network: jint,  // 0 unsupported, 1 unknown, 2 unmetered, 3 metered, 4 absent
)
```

`HostStateReporter`, owned by `FylesService`, reads `EXTRA_PLUGGED` off the sticky
`ACTION_BATTERY_CHANGED` and `NET_CAPABILITY_NOT_METERED` off `registerDefaultNetworkCallback`,
and calls this on every change. It reaches the brain plugin through a `watch` channel that both
ends `get_or_init`, so a reading pushed before the controller exists is not lost. The decoding —
the code table, the channel and the `Unknown` fallback — lives in `fyles_core::io_controller::
pushed_state` rather than in either FFI crate, because Android and iOS push the same codes and a
second copy in Rust could only drift from the first. An unrecognised code decodes to `Unknown`
rather than `Unsupported`, so an APK newer than the `.so` fails toward the conservative reading.
See [ADR-0008](adr/0008-host-run-conditions-for-internet-mode.md).

Everything the reporter touches runs on **one `HandlerThread`**, and both registrations are made
against its `Handler`. That is load-bearing for the reason macOS and iOS give for their serial
queue: left to their defaults the battery broadcast arrives on the main thread and the network
callback on ConnectivityManager's own looper, each writing one field of a pair and reading the
other, so a push could carry a stale half or land after a newer one.

**Data Saver meters the link.** `getRestrictBackgroundStatus() == RESTRICT_BACKGROUND_STATUS_ENABLED`
forces the metered reading whatever `NET_CAPABILITY_NOT_METERED` says, watched through
`ACTION_RESTRICT_BACKGROUND_CHANGED`. Reprovides are background traffic, and this is Android's way
of saying it would rather not carry them — the same concept as Windows'
`BackgroundDataUsageRestricted` and Apple's `isConstrained`. It is applied where the code is
computed rather than folded into the reading, because it is a property of the policy and not of the
link. Android only applies Data Saver on metered networks, so it rarely changes the verdict on its
own.

`onLost` reports `absent` only for the network the reporter currently holds. A handover that
announces the new default before dropping the old one would otherwise leave the reading stuck on
"no network" while the device is online.

The default network callback needs `ACCESS_NETWORK_STATE`. Without it no callback ever fires, and
the reading simply stays `Unknown`.

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

### 4.6 Release Signing

Every build type (debug, profile and release) is signed with the keystore named in `fyles/android/key.properties`. Android refuses an update whose signature differs from the installed app, and `flutter run` answers that by uninstalling, which wipes the app's data. One key for all build types lets a device switch between debug and release builds and keep its data. Both that file and the keystore are gitignored and stay on the release machine.

```properties
storeFile=../fyles-key.jks
keyAlias=fyles-key
storePassword=…
```

`storeFile` is relative to `fyles/android/`. The keystore is PKCS12, which keeps the key under the store password, so there is no separate key password. Without `key.properties` every build falls back to the debug key and the build output says so. Google Play rejects such a bundle. A `key.properties` that lacks one of the three entries fails the build.

### 4.7 Release APKs

`release/android.py` builds the APKs that a GitHub release of fyles-release carries ([ADR 0014](adr/0014-one-release-for-every-platform.md)): one each for `arm64-v8a`, `armeabi-v7a` and `x86_64`, and a universal one with all three. Each is built with `--target-platform`, and `fyles/android/app/build.gradle` derives Gradle's `abiFilters` from the target platforms (`gradle.properties` turns off Flutter's own filtering, which would replace them with every ABI), which also drops the native code that plugins ship for every ABI. All of them share the versionCode from `pubspec.yaml`. The script refuses a missing `key.properties`, since Gradle would quietly sign with the debug key, and checks every APK's ABIs, certificate and contents.

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

### 5.6 Host Conditions (iOS)

Swift pushes, Rust decodes — the Android shape rather than the desktop one, because `UIDevice` is
UIKit and Network.framework is not linked into the Runner target. `fyles_host_state_changed(power,
network)` is the only symbol Swift calls that is not `create_ffi_host_controller`, and it carries
the same integer codes Android uses (0 unsupported, 1 unknown, 2 external / 3 battery, 2 unmetered /
3 metered / 4 absent). They are hand-written on both sides: cbindgen runs with defaults and would
not follow them across crates, so the Swift `PowerCode` and `NetworkCode` enums confine every
literal to one `code` property instead, and an unrecognised code decodes to `Unknown`.

`HostStateReporter` owns a **serial** `DispatchQueue`, and that is load-bearing for the same reason
it is on macOS: two producers each writing one field of a pair would otherwise let an older reading
land after a newer one. iOS adds a twist — the battery read cannot move onto that queue, because
UIKit is main-only. So the read happens on main inside the notification and only the publish is
dispatched, which is what `readBattery` splits.

- **`.requiresConnection` reads as unknown**, matching macOS on the same underlying
  `nw_path_status_satisfiable`. `Absent` is the one reading no setting overrides, and a device whose
  only route is an on-demand VPN would be locked out of internet mode with no recourse.
- **Start order does not matter.** The Rust channel is `get_or_init` from whichever end reaches it
  first and opens on `Unknown`, so a push that arrives before the controller exists is kept and a
  subscription that arrives before the first push reads `Unknown` rather than `Unsupported`.
- **Re-reading the battery on `didBecomeActive` is required, not defensive.** A suspended process is
  not sent notifications, so a charger connected while suspended is missed outright, and
  `batteryState` is a live query. The network re-publish beside it *is* defensive — the monitor
  re-fires on resume in practice, but nothing promises it, and `send_if_modified` makes an unchanged
  republish free.
- Android's "no callback fires when there is no network, so read it directly" has **no iOS
  analogue**. `registerDefaultNetworkCallback` is silent without a default network, whereas
  `.unsatisfied` is a delivered path like any other.

The simulator cannot exercise power at all: `batteryState` is permanently `.unknown` there and
`simctl status_bar … --batteryState` repaints the status bar without moving the API. Cellular, and
therefore `isExpensive`, is also unreachable. Both need a real device.

---

## 6. Build System

### 6.1 Build Scripts

| Script | Purpose |
|--------|---------|
| `build.sh` | Orchestrates full build: server + Flutter for current or specified platform |
| `server_build.sh` | Builds backend binaries with cross-compilation support |
| `fyles_service/build_android.sh` | Cross-compiles Rust for `aarch64-linux-android` |
| `release/linux.py`, `release/android.py` | Build a release's assets into `fyles/build/release/v<version>/` ([ADR 0014](adr/0014-one-release-for-every-platform.md)) |
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

One `flake.nix` at the project root covers the whole project — Flutter, the Rust toolchain, the
Android SDK and the license-server image. `fyles_service/.envrc` loads it too (`use flake ..`).

`nix/rust.nix` holds the Rust toolchain pin and native tooling on its own, because a second flake
imports it: `public-flake.nix` is the trimmed dev-shell flake that `tools/export-public` emits as
the source-available export's `flake.nix` (see `fyles_service/EXPORT.md`).
