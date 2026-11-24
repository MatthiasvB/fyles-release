//! Constants of the core/brain layer. Network-layer ones live in `p2p::constants`, which this
//! crate sits below and so cannot reference.

use std::ops::RangeInclusive;
use std::time::Duration;

use crate::core::brain::client_presence::WakenessLevel;

/// Mirrored by `clientHeartbeatInterval` in `fyles/lib/constants.dart`, which drives the cadence.
pub const CLIENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Two lost beats tolerated.
pub const CLIENT_HEARTBEAT_TTL: Duration = Duration::from_secs(25);

pub const STALE_CLEANUP_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

pub const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Pairing a new device of the same user. Short on purpose: a stale code that still works is a
/// security problem.
pub const SELF_CONTACT_INVITE_TTL: Duration = Duration::from_secs(60);

/// Pairing with another person.
pub const CONTACT_SHARE_TTL: Duration = Duration::from_secs(2 * 60);

/// Doubles per attempt up to `RETRY_MAX_INTERVAL`.
pub const RETRY_BASE_INTERVAL: Duration = Duration::from_secs(5);

/// Four attempts a day, so at least one lands during waking hours when the peer is likely online.
pub const RETRY_MAX_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// Far above `p2p::constants::REQUEST_TIMEOUT` (15s), so crossing it is never transient.
pub const SYNC_SLOT_STALL_AFTER: Duration = Duration::from_secs(60);

/// One slot per (peer, document) and nothing prunes them, so growth past this is a quiet leak.
pub const SYNC_SLOT_COUNT_WARN: usize = 100;

/// Turns a hung task into a loud panic during development; compiled out of release.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

/// Fraction of the sweep period applied as random spread, so co-booted devices do not phase-lock.
pub const SWEEP_JITTER_FRACTION: f64 = 0.2;

pub const SWEEP_INTERVAL_IDLE: Duration = Duration::from_secs(300);
pub const SWEEP_INTERVAL_ATTACHED: Duration = Duration::from_secs(60);
/// Below `p2p::constants::REQUEST_TIMEOUT` (15s) on purpose: a sweep may fire while the previous
/// sync is outstanding, and single-flight absorbs it. Not assertable from here — see the module doc.
pub const SWEEP_INTERVAL_ACTIVE: Duration = Duration::from_secs(10);

/// How long an established install waits between license re-checks.
pub const LICENSE_RECHECK_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// How long a definitive "no license" answer from the server keeps the app on the free
/// tier without a fresh check.
pub const LICENSE_FREE_ANSWER_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Below the ephemeral ranges of Linux (32768+) and of Windows and macOS (49152+), so an outgoing
/// connection never happens to hold the port a user opened in their firewall.
pub const RANDOM_LISTEN_PORTS: RangeInclusive<u16> = 20000..=31999;

pub const LISTEN_PORT_PICKS: usize = 20;

/// A rebuilt swarm's predecessor keeps its UDP socket until its QUIC connections drain, so the
/// configured port can read as taken by ourselves for a moment.
pub const RELISTEN_DELAYS: [Duration; 3] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

/// The last delay repeats for as long as the configured port stays unavailable.
pub const LISTEN_RECLAIM_BACKOFF: [Duration; 5] = [
    Duration::from_secs(2),
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(2 * 60),
    Duration::from_secs(10 * 60),
];

pub const fn sweep_interval(level: WakenessLevel) -> Duration {
    match level {
        WakenessLevel::Idle => SWEEP_INTERVAL_IDLE,
        WakenessLevel::Attached => SWEEP_INTERVAL_ATTACHED,
        WakenessLevel::Active => SWEEP_INTERVAL_ACTIVE,
    }
}

const _: () = {
    assert!(CLIENT_HEARTBEAT_TTL.as_millis() > CLIENT_HEARTBEAT_INTERVAL.as_millis());
    assert!(RETRY_BASE_INTERVAL.as_millis() < RETRY_MAX_INTERVAL.as_millis());
    assert!(SWEEP_INTERVAL_ACTIVE.as_millis() < SWEEP_INTERVAL_ATTACHED.as_millis());
    assert!(SWEEP_INTERVAL_ATTACHED.as_millis() < SWEEP_INTERVAL_IDLE.as_millis());
};
