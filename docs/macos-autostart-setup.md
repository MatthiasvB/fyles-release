# macOS — daemon autostart at login

How the Fyles backend daemon (`daemon_internet`) starts automatically at login on macOS, how the
pieces fit together, and the one-time steps to build/sign/ship it.

## 1. Goal & shape of the solution

The backend daemon must run as a per-user background service that starts at login, so transfers work
even when the GUI is closed. The GUI does **not** autostart — the user opens it on demand and it
connects to the already-running daemon.

This is implemented with a **macOS LaunchAgent**, registered through **`SMAppService`** (macOS 13+):

- The app bundle ships a LaunchAgent property list and the daemon binary inside it.
- On first launch, the GUI calls `SMAppService.agent(plistName:).register()`. This installs the agent
  and makes it visible/toggleable in **System Settings → General → Login Items**.
- From then on, `launchd` starts the daemon at every login (and immediately, via `RunAtLoad`), and
  relaunches it if it crashes (`KeepAlive`).
- The GUI and daemon talk over a **Unix Domain Socket** (no TCP port, no auth token, no discovery
  file — those exist only on Windows).

## 2. Bundle layout

```
fyles.app/                                          (Developer ID-signed + notarized)
  Contents/MacOS/fyles                              ← Flutter GUI; registers the agent on first run
  Contents/Helpers/fyles-daemon                     ← daemon_internet, universal Mach-O (signed)
  Contents/Helpers/launch-daemon                    ← tiny compiled launcher, universal Mach-O (signed)
  Contents/Library/LaunchAgents/
    app.fyles.fyles.backend.plist                   ← Label + BundleProgram → Helpers/launch-daemon + RunAtLoad + KeepAlive
```

## 3. Why a compiled launcher (not a script, not daemon defaults)

The daemon needs concrete, per-user paths (database, received-files directory, socket). Two
constraints shape how those paths are supplied:

- `launchd` does **not** expand `~`/`$HOME` inside a plist, and the plist is read-only/signed inside
  the bundle — so the paths can't be baked into the plist.
- We deliberately keep the **daemon generic** (no macOS-specific default paths compiled into it), so
  the same binary behaves identically everywhere and is driven purely by its CLI arguments.

The bridge is **`launch-daemon`** — a ~60-line C program (`macos/Runner/Helpers/launch-daemon.c`).
At runtime it reads `$HOME` (which `launchd` *does* set for the agent), creates the data/output
directories, removes any stale socket, and `exec`s the daemon next to it with explicit arguments:

```
fyles-daemon --endpoint uds:$HOME/Library/Application Support/Fyles/fyles.sock \
             --db-path  $HOME/Library/Application Support/Fyles/fyles.db \
             --output-dir $HOME/Documents/Fyles
```

It is a **compiled Mach-O, not a shell script, on purpose**: Xcode requires every nested executable
in the bundle to be code-signed, and Developer ID **distribution re-signs Mach-O binaries but not
scripts**. A script therefore either fails the build's signing step or ships with a stale signature
that fails notarization. A Mach-O launcher is signed at build time and re-signed in lockstep with the
app during distribution.

### Canonical paths

The launcher (C) and the frontend (`lib/services/protobuf_client.dart` → `defaultUdsPath()` macOS
branch) independently compute the **same** paths — keep them in sync if either changes.

| Purpose            | Path                                             |
|--------------------|--------------------------------------------------|
| DB / internal data | `~/Library/Application Support/Fyles/`           |
| UDS socket         | `~/Library/Application Support/Fyles/fyles.sock` |
| Received files     | `~/Documents/Fyles/`                             |

## 4. How the embedding happens at build time

A single **Run Script** build phase on the Runner target runs
`macos/Runner/Scripts/embed_daemon.sh`, which:

1. Builds `fyles-daemon-internet` from the Rust workspace (universal arm64+x86_64 in Release;
   host-arch only in Debug for speed) and copies it to `Contents/Helpers/fyles-daemon`.
2. Compiles `launch-daemon.c` into a universal `Contents/Helpers/launch-daemon`.
3. Copies the LaunchAgent plist into `Contents/Library/LaunchAgents/`.
4. Code-signs **both** Mach-O helpers with the build's identity and the hardened runtime.

Because it runs as a build phase (before Xcode's final signing), the app's signature seals
everything, and Developer ID distribution later re-signs all the Mach-O parts together.

## 5. How registration is triggered at runtime

On startup the GUI's home widget (`lib/main.dart`, `_MyHomePageState._initializeServer`) invokes the
`app.fyles.fyles/service` method channel → `registerLaunchAgent`, handled in
`macos/Runner/MainFlutterWindow.swift`. Two important properties:

- Registration runs **first and independently** of the (best-effort) notification-permission request,
  so a permission hiccup can't prevent the daemon from being set up.
- The handler reports its `SMAppService.Status` back to Dart and logs it via `NSLog` (visible in
  Console / `log show`), so success/approval-needed/failure is observable.

The app must run from a **stable location (`/Applications`)**. A quarantined copy run from a DMG or
`~/Downloads` is *path-translocated* by Gatekeeper, and `SMAppService` will not register an agent for
a translocated bundle.

## 6. Signing & distribution

- Bundle identifier: `app.fyles.fyles` (set in `macos/Runner/Configs/AppInfo.xcconfig`); matches the
  method-channel name and the agent label `app.fyles.fyles.backend`.
- Sign with a **Developer ID Application** certificate, **Hardened Runtime** enabled, **App Sandbox
  off** (the daemon writes to `~/Documents` and `~/Library/Application Support` and runs as a
  LaunchAgent — sandboxing is incompatible).
- Notarization requires Developer ID + hardened runtime + secure timestamp + **no `get-task-allow`**.
  A plain `flutter build macos --release` is signed with a *development* certificate and carries
  `get-task-allow`, so it is **not** notarizable. Use Xcode's **Archive → Distribute → Direct
  Distribution**, which switches to Developer ID, strips `get-task-allow`, notarizes, and staples.

## 7. One-time setup steps

1. **Create a Developer ID Application certificate** (paid Developer Program required):
   Xcode → Settings → Accounts → select team → *Manage Certificates…* → **+** → *Developer ID
   Application*.
2. **Runner target → Signing & Capabilities:** select the Team, keep *Automatically manage signing*,
   add the **Hardened Runtime** capability, do **not** add App Sandbox.
3. **Runner target → Build Phases → New Run Script Phase**, body:
   `"$SRCROOT/Runner/Scripts/embed_daemon.sh"`. Leave it at the bottom; uncheck *Based on dependency
   analysis*.
4. Open the **`Runner.xcworkspace`** (not the bare `.xcodeproj`).

## 8. Build, ship, verify

```sh
# Build & notarize (GUI): Product → Archive → Organizer → Distribute App → Direct Distribution → Export
# Then install on the target Mac by dragging fyles.app into /Applications.

# Verify the exported app
APP=/Applications/fyles.app
for f in "" /Contents/Helpers/fyles-daemon /Contents/Helpers/launch-daemon; do
  printf '%-32s ' "${f:-<main app>}"; codesign -dvvv "$APP$f" 2>&1 | grep 'Authority=' | head -1
done                                  # all three → Developer ID Application
spctl -a -vvv -t exec "$APP"          # accepted, source=Notarized Developer ID

# Launch once, then confirm the agent + daemon
log stream --predicate 'eventMessage CONTAINS "Fyles:"' --style compact   # watch while launching
launchctl print "gui/$(id -u)/app.fyles.fyles.backend" | head -20
ls -l ~/Library/Application\ Support/Fyles/fyles.sock
```

There is also `build_macos_installer.sh` (repo root) — a CLI wrapper that builds, then notarizes via
`notarytool` and produces a DMG. It only works once a Developer ID cert exists and the Release config
signs with it; the Archive/Distribute GUI flow is the simplest path and auto-creates the certificate.

## 9. Caveats

- **Translocation:** install into `/Applications`; registration won't work from a quarantined/DMG
  location (see §5).
- **TCC:** writing received files to `~/Documents/Fyles` can prompt a privacy approval for a
  background agent. If that's a problem, change the launcher's `--output-dir` to a non-protected
  location such as `~/Library/Application Support/Fyles/Received`.
- Run **one** daemon only — do not also register the local-only `daemon`; both would fight over the
  socket.
- If the daemon CLI gains/loses arguments, update `launch-daemon.c`; if the data dir changes, update
  both `launch-daemon.c` and `defaultUdsPath()` (§3).
