//! Constants of the core/brain layer. Network-layer ones live in `p2p::constants`, which this
//! crate sits below and so cannot reference.

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
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Fraction of the sweep period applied as random spread, so co-booted devices do not phase-lock.
pub const SWEEP_JITTER_FRACTION: f64 = 0.2;

pub const SWEEP_INTERVAL_IDLE: Duration = Duration::from_secs(300);
pub const SWEEP_INTERVAL_ATTACHED: Duration = Duration::from_secs(60);
/// Below `p2p::constants::REQUEST_TIMEOUT` (15s) on purpose: a sweep may fire while the previous
/// sync is outstanding, and single-flight absorbs it. Not assertable from here — see the module doc.
pub const SWEEP_INTERVAL_ACTIVE: Duration = Duration::from_secs(10);

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
