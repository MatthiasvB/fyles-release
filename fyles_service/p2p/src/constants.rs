use std::time::Duration;

use crypto::session::SessionLifetime;

/// Raise this whenever anything covered by a signature changes shape — see
/// `docs/architecture/security.md` §4.2.
pub const SESSION_ESTABLISHMENT_PROTOCOL: &str = "/session-establishment/0.3.0";

/// Timeout registry for all five request-response protocols. Not shortened under `cfg(test)` —
/// tests shorten [`BEHAVIOUR_REQUEST_TIMEOUT`] instead so libp2p reports the failure first.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// libp2p's own request-response timeout; production keeps libp2p's default. Must stay above what a
/// session handshake costs: the responder does a brain round trip and a signature check before it
/// can answer, which on a loaded machine ran to 764ms.
#[cfg(test)]
pub const BEHAVIOUR_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(not(test))]
pub const SESSION_ESTABLISHMENT_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
pub const SESSION_ESTABLISHMENT_TIMEOUT: Duration = Duration::from_secs(3);

pub const SESSION_LIFETIME: SessionLifetime = SessionLifetime {
    usable_for: Duration::from_secs(30 * 60),
    accepted_past: Duration::from_secs(60),
};

pub const SESSION_CONSTRUCTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Retention for just-expired sessions, so a late response can still be decrypted.
pub const EXPIRED_SESSION_IDLE: Duration = Duration::from_secs(15);
pub const EXPIRED_SESSION_SCOPE: Duration = Duration::from_secs(15);
pub const EXPIRED_SESSION_MAX: Duration = Duration::from_secs(60);

pub const RECEIVE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub const RECEIVE_IDLE_SLACK: Duration = Duration::from_secs(35);

/// A send slot idle this long is wedged: no request is outstanding, so nothing will time out and
/// unblock it. Reported, never repaired.
pub const STALL_ERROR_AFTER: Duration = Duration::from_secs(60);

pub const SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL: Duration = Duration::from_secs(1);

pub const FILE_PUSH_V2_PROTOCOL: &str = "/file-push/0.3.0";
pub const FILE_DATA_PROTOCOL: &str = "/fyles/file-data/0.1.0";

/// Upper bound on how often a receiver reports progress, and so on the sender's database writes.
pub const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
pub const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Most file bytes one stream frame carries. A whole frame must arrive within
/// [`STREAM_IDLE_TIMEOUT`], so this sets the slowest link a transfer survives.
pub const STREAM_FRAME_PAYLOAD: usize = 256 * 1024;
/// Most file bytes an incoming frame may carry. Larger than [`STREAM_FRAME_PAYLOAD`], so a later
/// release can send larger frames without breaking this one.
pub const MAX_INCOMING_FRAME_PAYLOAD: usize = 1024 * 1024;
/// How long a reservation waits for its stream, and a new stream for its attempt id and `Begin`.
pub const ATTEMPT_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
pub const TASK_STOP_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a peer that lacks the stream protocol is served the old one before it is asked again.
pub const LEGACY_RECHECK: Duration = Duration::from_secs(10 * 60);

pub const BRAIN_READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

const _: () = {
    assert!(SESSION_ESTABLISHMENT_TIMEOUT.as_millis() < REQUEST_TIMEOUT.as_millis());
    assert!(STALL_ERROR_AFTER.as_millis() > REQUEST_TIMEOUT.as_millis());
    assert!(EXPIRED_SESSION_MAX.as_millis() >= REQUEST_TIMEOUT.as_millis());
    // A receiver reports progress while bytes flow, so a few missed reports must not look idle.
    assert!(STREAM_IDLE_TIMEOUT.as_millis() > 5 * PROGRESS_INTERVAL.as_millis());
    assert!(MAX_INCOMING_FRAME_PAYLOAD >= STREAM_FRAME_PAYLOAD);
};

/// Separate block because `BEHAVIOUR_REQUEST_TIMEOUT` only exists in tests.
#[cfg(test)]
const _: () = {
    // Equal values made it a coin flip which fired, so a handshake still in flight was torn down by
    // the outer wait before libp2p could report on it.
    assert!(SESSION_ESTABLISHMENT_TIMEOUT.as_millis() > BEHAVIOUR_REQUEST_TIMEOUT.as_millis());
};
