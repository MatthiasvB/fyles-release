# 0013 — Fyles reaches Linux through Nix, as prebuilt binaries

## Status

Accepted (2026-11). Partially supersedes [0010](0010-per-user-listen-port.md): a daemon started
with `--listen-port` uses that port instead of the one stored in the database.

## Context

The full source is private. The public repository, fyles-release, holds only the subset that builds
`fyles-daemon`, so nobody can build the Linux app from it. Linux had no way to install Fyles at all.

On Linux nothing starts the daemon. The app only connects to `$XDG_RUNTIME_DIR/fyles.sock`. The
daemon also removes a leftover socket before binding it, so a second daemon takes the socket away
from a running one. A socket file therefore says nothing about whether a daemon is alive.

Each user's node listens on a port drawn at random and stored in the database (ADR 0010). A
declarative system configuration, such as NixOS, must know that port to open its firewall.

## Decision

**Prebuilt binaries, patched by the flake.** `fyles/release/linux.py` builds the app and the
internet daemon on the maintainer's machine. The tarball goes to a GitHub release of fyles-release,
and `nix/fyles-release.json` records its URL and hash. `nix/fyles-app.nix` fetches it and patches
the binaries with `autoPatchelfHook` against the flake's own nixpkgs. The export copies the
monorepo's `flake.lock` verbatim, so that nixpkgs is the one the build used, and the libraries the
binaries find are the ones they were linked against.

The tarball names nothing of the build machine. The script remaps the Rust paths, keeps the linker
from recording library paths, builds Flutter from a copy under `/tmp`, and sets the interpreter to
`/lib64/ld-linux-x86-64.so.2` and the app's library path to `$ORIGIN/lib`. It refuses to package
anything that still contains the maintainer's home or a store path.

**One daemon per database, and one per socket.** Before anything starts, the daemon takes
`<db-path>.lock` with `File::try_lock`, and with a Unix socket also `<socket>.lock`. It holds both
for its whole life. Two daemons on one database would answer as the same contact with diverging
state, so the second exits with code 75 (`EX_TEMPFAIL`). Daemons on different databases, such as a
development build beside an installed one, run side by side. The socket lock covers what the
database lock cannot: two daemons on different databases pointed at one socket. Without it the
second would remove the first's socket, and the app would talk to the wrong identity. The kernel
drops the locks with the process, so a lock never goes stale, and removing a leftover socket is
safe because only the holder does it. Starting the daemon needs no check before it. The units
count 75 as a clean exit (`SuccessExitStatus`), so a daemon that loses the race neither restarts
nor marks its unit failed.

**The units wait for a DNS server.** The daemon reads `/etc/resolv.conf` once, as it builds its
network node, and panics when the file names no nameserver. At login the network often has not
written one yet. A user unit cannot order itself after `network-online.target`, which belongs to the
system manager, so `ExecStartPre` runs `libexec/fyles/wait-for-dns`. It waits up to 30 seconds for
a `nameserver` line and then lets the daemon start regardless. `RestartSec=5` spaces out the
restarts, which otherwise exhaust systemd's start limit within two seconds. A resolver that reads
the system configuration at lookup time would make both unnecessary (TechnicalTodo).

**A launcher, a user unit and two modules.**
- `fyles-daemon-start` is the one place that chooses the daemon's paths: the database under
  `$XDG_DATA_HOME/fyles`, the socket the app expects, and received files in `~/Downloads/Fyles`
  unless `FYLES_OUTPUT_DIR` says otherwise.
- `fyles` starts the daemon in the background and opens the app. That alone makes `nix run` work.
- The package ships a systemd user unit for plain Nix.
- The Home Manager module gives a user the package and the unit.
- The NixOS module does the same per user from `programs.fyles.users`. When Home Manager runs
  inside NixOS, it adds the Home Manager module to every user.
- Each user's `openFirewall`, off by default as NixOS options usually are, opens that user's
  pinned port and mDNS. The NixOS module reads it from `programs.fyles.users` and from every Home
  Manager user. It needs a `listenPort`, since only a pinned port is known in advance.

**`--listen-port` pins the port.** The brain gets a `ListenPortChoice`, which is either `Stored` or
`Pinned(port)`. A pinned port replaces the stored one, is never written to the database, and still
falls back to a temporary port when it is taken. The status says so, and the daemon prints it to
stderr, which the journal or `daemon.log` keeps. Setting the port is refused with
`failed_precondition`. `GetListenStatusResponse.port_pinned` tells the app, which shows the port as
set by the system and does not offer to change it. The flag carries no port, because the status
already names it. The modules read one option per user for both the daemon's
flag and the firewall rule, so the two cannot disagree.

### Recorded departures

- The exit code 75 is shared by hand between Rust (`instance_lock::EXIT_ALREADY_RUNNING`) and Nix
  (`exitAlreadyRunning` in `nix/fyles-app.nix`), because nothing carries a constant across the two.

## Consequences

- Fyles installs on x86_64 Linux with `nix run` or `nix profile install`, and declaratively on
  NixOS and with Home Manager.
- The binaries need a glibc at least as new as the build's. A user who makes the flake follow an
  older nixpkgs breaks them.
- Outside NixOS, a Nix-installed GUI app may need nixGL to reach the system's OpenGL driver.
- A release needs complete translations, because `setup_repo.sh` stops on missing ones.
- Publishing stays manual: export, commit and push, then `gh release create` with the tarball.
- The package is marked unfree, and installing it follows the user's unfree policy. `nix run`,
  `nix profile install` and `nix flake check` need `NIXPKGS_ALLOW_UNFREE=1` with `--impure`, as
  nixpkgs' own unfree packages do. The modules patch the app against the flake's nixpkgs, but under
  the unfree settings of the configuration that uses them.
- A user configured both in Home Manager and in `programs.fyles.users` would have the system's
  unit replaced by Home Manager's. An assertion rules that out where Home Manager runs inside
  NixOS.
- Where the Nix code departs from community conventions, `nix/README.md` says why.
- With a pinned port the app cannot choose another. A firewall only covers the pinned port, so the
  temporary fallback reaches local peers only where no firewall is in the way.

## Alternatives considered

- **Building from source in the flake.** The source is private.
- **Locking only the endpoint.** A daemon on TCP would go unguarded, and the endpoint is not what
  two daemons must not share. A build-specific rule, such as telling development builds apart,
  would guess at what the database path already says.
- **Locking the socket file itself.** A socket cannot be opened, so it cannot be locked, and the
  daemon replaces it on every start.
- **Probing the socket for a live daemon.** A connect and a later start race, and a stale file
  looks like a dead daemon only until someone checks. The lock is atomic.
- **Opening the whole random port range.** That is 12,000 ports per machine.
- **A plain tarball for other distributions.** Its libraries would have to match each distribution.
  The tarball is that in principle, with the standard interpreter, but nothing promises it runs
  outside Nix.
