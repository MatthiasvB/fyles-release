# 0010 — Each user listens on a port of their own, chosen in the database

## Status

Accepted (2026-10)

## Context

Every node used to listen for peers on a fixed `0.0.0.0:24256`, over TCP and QUIC. Fyles runs once
per user, so two users signed in on one Mac or Windows machine both reached for that port. On every
platform, phones included, any other program may hold it already, because ports are shared across
the whole device. A bind failure was an `unwrap`, and release builds abort on panic,
so the daemon died, or on mobile the whole app, with nothing shown to the user.

The failure was also uneven. libp2p's TCP listener sets `SO_REUSEPORT` on unix and `SO_REUSEADDR` on
Windows. A second node's TCP bind therefore succeeds, and the kernel splits incoming connections
between two nodes that belong to different people. Only the QUIC bind fails, because it is a plain UDP
socket.

A port the OS assigns afresh on every start would never collide, but users behind a firewall need a
number they can open once.

The client API endpoint is not part of this problem. It is a Unix socket under a per-user directory,
or on Windows a loopback port the OS assigns and publishes in the per-user `api_server_config`.
Loopback is not firewall-relevant, so the frontend never offers it as a setting.

## Decision

**One port per user, stored in the database.** A single-row `listen_port` table holds it, and a
missing row means none has been chosen yet. The table lives in `core`, not in the internet settings
blob, because the local-network-only daemon listens too. The setting is changed at runtime from
Settings › Network and applies without a restart.

**A fresh install picks at random.** The port is drawn from `20000..=31999`, below the ephemeral range
of Linux (32768+) and of Windows and macOS (49152+). The first of 20 picks that binds is stored.
The pick happens once and is kept for the life of the install. At that moment another user's node is
rarely running, so a probe cannot see the collision that matters, which comes later when both users
are signed in at the same time. Walking a fixed range would hand every install on the machine the
same first free port, and the users would clash for good. Random draws make it unlikely that two
installs ever store the same port.

**Existing installs keep 24256.** The migration to 1.25.0 inserts it as a literal, so firewall rules
made for the old fixed port keep working.

**A taken port degrades instead of failing.** The stored port is the *configured* one, and the port
actually bound is the *active* one. When the configured port is taken, the node falls back to a
temporary random port as the active one, which is never stored, and keeps retrying the configured
one with backoff. A user who switches to a taken port stays on the port they had. The UI shows the
configured port, why it failed, and the active port peers reach the node on meanwhile. mDNS advertises the actual addresses, so local peers keep finding the node
wherever no firewall is in the way.

**The brain decides and the network only binds.** `NetworkNode::listen_on_port` binds one port and
knows nothing of choices, fallbacks or storage. The brain establishes listeners once the node reports
ready, after every swarm rebuild and on every change, all under one lock.

**Plain sockets probe the port before libp2p binds it.** They classify the failure, which libp2p's
boxed transport error hides, and they carry no reuse options. They therefore fail exactly where libp2p
would silently share the port.

### Wire shape and its recorded departures

`ListenStatusProto` is a oneof of `listening`, `temporary` and `not_listening`, pushed whole as
`ListenStatusChanged`. `GetListenStatusResponse` adds `starting`, which only the fetch can report,
because a push always follows a decision.

The one departure from types that admit only valid state is the port's width. Ports travel as
`uint32` because proto3 has no 16-bit type, and the service rejects anything outside `1024..=65535`
as an invalid argument.

An active port equal to the configured one would make no port temporary, which is meaningless. The wire can carry it, but neither
side can hold it. In Rust, `ListenStatus::Temporary` wraps a `TemporaryPort` whose fields are
private and whose constructor refuses equal ports. In Dart, `ListenOnTemporaryPort` has only a
private constructor behind the same check, and decoding such a message fails like any other
malformed one.

## Consequences

- Several users on one machine each get a working node, and none of them can take another's
  connections.
- No listen path panics. Losing the port shows up as a status in Settings, not as a crash.
- A firewall rule opened for the stored port stays valid, except while another program holds that
  port.
- A backup restored on another device brings its port along.
- After a port change in internet mode, peers learn the new address only as identify and AutoNAT
  refresh it.

## Alternatives considered

- **Port chosen by the installer.** That works only where an installer exists, which is Windows. It
  is hard to change afterwards and leaves macOS, Linux and mobile unsolved.
- **Port in the internet settings blob.** The local-network-only daemon does not interpret that blob,
  so it would have no setting at all.
- **Always let the OS assign the port.** It never collides, but a firewall cannot be opened for a port
  that changes on every start.
- **Reject a taken port when the user sets it.** That needs a mutation whose reply carries meaning,
  which ADR 0007 rules out. It also refuses a port the user may want for its firewall rule while
  another program briefly holds it. The reclaim watcher covers that case.
