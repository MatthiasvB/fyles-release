use std::time::Duration;

/// Raise this whenever anything covered by a signature changes shape — see
/// `docs/architecture/security.md` §4.2.
pub const SESSION_ESTABLISHMENT_PROTOCOL: &str = "/session-establishment/0.1.0";

/// HKDF `info`, to domain-separate the session key from any other derivation off the same secrets.
pub const SESSION_KDF_INFO: &[u8] = b"fyles-session-chacha20poly1305-key";

/// HKDF `info` for the session's identifier. Must differ from [`SESSION_KDF_INFO`], or the id would
/// be the key.
pub const SESSION_ID_INFO: &[u8] = b"fyles-session-id";

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

pub const SESSION_VALIDITY_DURATION: Duration = Duration::from_secs(30 * 60);

/// Absorbs clock skew between devices.
pub const ACCEPT_SESSION_PAST_VALID_DURATION: Duration = Duration::from_secs(60);

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

pub const BRAIN_READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

const _: () = {
    assert!(SESSION_ESTABLISHMENT_TIMEOUT.as_millis() < REQUEST_TIMEOUT.as_millis());
    assert!(STALL_ERROR_AFTER.as_millis() > REQUEST_TIMEOUT.as_millis());
    assert!(EXPIRED_SESSION_MAX.as_millis() >= REQUEST_TIMEOUT.as_millis());
};

/// Separate block because `BEHAVIOUR_REQUEST_TIMEOUT` only exists in tests.
#[cfg(test)]
const _: () = {
    // Equal values made it a coin flip which fired, so a handshake still in flight was torn down by
    // the outer wait before libp2p could report on it.
    assert!(SESSION_ESTABLISHMENT_TIMEOUT.as_millis() > BEHAVIOUR_REQUEST_TIMEOUT.as_millis());
};
