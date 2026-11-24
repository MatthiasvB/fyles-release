# 0008 — Internet mode runs on host conditions, decided below Flutter

## Status

Accepted (2026-09)

## Context

Internet mode turns the swarm from `NetworkMode::LocalNetwork` into `NetworkMode::Internet`:
Kademlia, relay, DCUtR, identify, DHT reprovides. That is discretionary background work, and it is
exactly what a user does not want running on battery or on a metered link.

The daemon outlives the Flutter client — on Android it is a foreground service, on desktop a
process the UI attaches to — so the decision cannot live in the client. Whoever owns it has to be
able to make it with nobody watching.

The operating systems answer the two questions unevenly. Power state is close to authoritative
everywhere. Network cost is not: it is an OS *policy classification* built from transport, carrier
data, user settings and administrator policy, not a fact about the subscriber's contract. Windows
and Android expose it directly, Apple exposes the adjacent `isExpensive` / `isConstrained`, and
Linux has it only where NetworkManager is running. Transport type is not a substitute — a laptop
on Wi-Fi may be tethered to a phone, and an Ethernet link may sit behind a capped satellite ISP.

## Decision

**A brain plugin carries what `core` may not know.** `BrainPlugin` is a seam on the brain in the
mould of `FileTrackerPlugin`: `core` defines the trait and a `NoOpBrainPlugin`, `p2p_internet`
implements it, and each composition root picks one the way it already picks a network node. The
plugin holds a `Sender<BrainAction>` and nothing else, so `PluginAction` — load and store the
opaque settings blob, restart the network, push a settings view — is its entire reach.

Internet mode is the plugin's, whole: the settings, the host reading, and the decision between
them. The alternative was to leave the policy in the p2p event loop, which put power and metering
inside a component whose job is networking, and would have repeated itself for the next concern
that wants to gate work on something the brain knows.

**The network node is told what to run, not why.** `NetworkNode::restart(config)` takes opaque
bytes the plugin defines, and `GetNodeInfo` answers with the same. Nothing below the plugin knows
what power or metering are, and the node no longer reads or writes settings at all.

**The host signals themselves are a `core` capability.** `HostController` carries
`host_state() -> watch::Receiver<HostState>`, defaulted to a channel that reports both signals
unsupported, so a host that cannot observe them needs no code. `core` relays the value to the
plugin and knows nothing about what it is for.

**A signal the platform cannot read never blocks.** `HostSignal` separates `Unknown` — the API
exists and has no answer right now — from `Unsupported`, the platform or build having no API at
all. The strictness setting governs `Unknown`. `Unsupported` leaves the condition switched on but
inert and says so in the UI, because a condition the host can never answer would otherwise leave
the user no recourse but to switch it off, and a Linux box without NetworkManager would be unable
to run internet mode at all.

**Having no network at all is not a run condition.** `NetworkStatus` is `Absent` or
`Present(NetworkCost)`, so a cost only exists where there is a link to have one. `Absent` stops the
DHT unconditionally, with no setting to turn that off: Kademlia off a network reaches nobody and
only churns queries and reprovides. `LocalNetwork` keeps running, since it costs nothing when there
is nothing to find.

**Intent and decision are separate values.** `DhtSettings::enabled` is what the user asked for and
is only ever written by the user. Whether the DHT may run is
`enabled && host_policy::evaluate(..).is_empty()`, derived on demand and never persisted. The
daemon therefore never writes a setting the user did not make, and the switch never moves on its
own.

**The swarm's config is its own type.** `Settings::to_swarm_config` produces a `SwarmConfig`
carrying `dht_active` — a decision — where the persisted `SettingsV2` carries `dht_enabled` — an
intent. They are separate messages so the two can never be handed to the wrong consumer.

## Consequences

- Every install lands on all conditions enforced, fresh ones and those migrated from v1 alike. A
  host that can report neither signal is unaffected, because `Unsupported` never blocks.
- `PluginAction::PushSettingsView` carries a settings change the daemon made on its own to the
  clients. `MutateSettings` only pushes what a client asked for, so without a second path a
  host-driven change would diverge from every attached window.
- `BrainPlugin::start` receives the starting reading and the watcher carries only changes. The
  receiver is subscribed before `start` runs, so a host that moves in between is still reported
  rather than leaving the plugin on a stale value until it moves again.
- **A channel opens on `Unknown` wherever something will eventually publish**, and on `Unsupported`
  only where nothing ever will. `start` reads the current value synchronously, so a host whose first
  reading is still in flight — every FFI host, and Linux and macOS on desktop — would otherwise let
  the swarm be built unconditioned and stop only a debounce later. `Unsupported` is a claim about
  the platform, and making it before anyone has looked is the one way this design can fail open.
- **A reading is published from one thread per host.** Two producers each writing one field of a
  pair, with the publish reading both, can tear the pair or land an older reading after a newer one.
  Every host closes that the same way: a serial dispatch queue on macOS and iOS, a `HandlerThread`
  on Android, a mutex per reading on Windows, and one task per field on Linux.
- **Whether the host is observed at all is the composition root's choice.** Subscribing is what
  starts the watchers, so a build whose plugin discards the readings must not subscribe.
  `DirectFsController::new` therefore takes the receiver instead of subscribing, which is how
  the mobile hosts already work — their reporters are started by the app, not by anything below it.
- `BrainPlugin::start` runs after the action loop is live and before the network node starts, since
  a plugin loads its state through its own sender and the node asks it for a config straight away.
- Host-driven transitions are debounced, and asymmetrically. Stopping honours a constraint the user
  set, so it waits only long enough that a charger reseating costs no rebuild. Starting spends a
  swarm rebuild and a fresh round of relay reservations on a guess that the conditions will hold,
  so it waits longer. The push to clients is never debounced: the verdict is true the
  moment the reading changes, and only the expensive half waits.
- A user toggling a setting is never debounced. The delay exists for a host moving on its own.
- Linux is where `Unsupported` stops being a property of the build and becomes one of the machine.
  Whether the network cost can be read depends on NetworkManager owning its bus name, which can
  change while the daemon runs, so the capability is discovered from the bus and rediscovered when
  the name comes and goes. A platform reporting a reading it cached before a service restarted
  would be worse than one reporting nothing.
- `NetworkCost::Metered` covers the OS asking background work to stay off, not only who pays for
  the bytes, so each host folds in whatever it has of that: `isConstrained` on Apple,
  `BackgroundDataUsageRestricted` on Windows, Data Saver on Android. Linux has no equivalent in
  `NMMetered` and folds in nothing.
- The strictness setting is not equally busy everywhere. Apple answers the cost with two booleans
  that cannot decline, so a reachable path on macOS or iOS is always a definite reading and
  `strict_when_unknown` never governs the network condition there. It stays a setting about the
  hosts that can hesitate, not a promise that every host will. iOS is the one place the *power*
  signal hesitates instead: it reads unknown for a moment after battery monitoring is enabled, and
  permanently on the simulator.
- The plugin tracks what the node was last told to run, not what it last decided. A pending restart
  can be cancelled, so a reading that flips back inside the window must leave the swarm untouched
  rather than schedule the opposite transition.
- **Departure from "model types so they can express only valid state":** `SettingsView.blocked_by`
  is derivable from the other seven fields, so the message admits a contradiction
  (`POWER_EXTERNAL` alongside `BLOCK_ON_BATTERY`). It is sent anyway. Recomputing it in Dart would
  put a second copy of the policy in the frontend, and the one screen whose job is to reveal a bug
  in the Rust evaluation would be rendering the Dart evaluation instead. A single `evaluate` call
  in `current_settings_response` writes the readings and the verdict together, so they cannot
  diverge in practice.
