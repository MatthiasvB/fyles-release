use crypto::BINCODE_CONFIG;
use license_format::LicenseCertificate;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::LICENSE_RECHECK_INTERVAL;

pub const CACHE_FORMAT_VERSION: u16 = 1;

#[derive(Serialize, Deserialize, Debug)]
pub struct CachedLicenseState {
    pub version: u16,
    pub cert: Option<LicenseCertificate>,
    pub refreshed_at: Option<u64>,
    /// When the server last definitively answered "no license for this identity".
    pub last_definitive_no: Option<u64>,
}

impl Default for CachedLicenseState {
    fn default() -> Self {
        Self {
            version: CACHE_FORMAT_VERSION,
            cert: None,
            refreshed_at: None,
            last_definitive_no: None,
        }
    }
}

impl CachedLicenseState {
    pub fn recheck_is_due(&self, now: u64) -> bool {
        match self.refreshed_at {
            None => true,
            Some(at) => now >= at + LICENSE_RECHECK_INTERVAL.as_secs(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CacheDecodeError {
    #[error("license cache blob is empty")]
    Empty,
    #[error("license cache blob is unreadable")]
    Unreadable,
    #[error("unsupported license cache version {0}")]
    UnsupportedVersion(u16),
}

pub fn decode(blob: &[u8]) -> Result<CachedLicenseState, CacheDecodeError> {
    if blob.is_empty() {
        return Err(CacheDecodeError::Empty);
    }
    let (state, _) =
        bincode::serde::decode_from_slice::<CachedLicenseState, _>(blob, BINCODE_CONFIG)
            .map_err(|_| CacheDecodeError::Unreadable)?;
    if state.version != CACHE_FORMAT_VERSION {
        return Err(CacheDecodeError::UnsupportedVersion(state.version));
    }
    Ok(state)
}

pub fn encode(state: &CachedLicenseState) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(state, BINCODE_CONFIG)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cache_round_trips_and_reports_garbage_as_none() {
        let state = CachedLicenseState {
            refreshed_at: Some(123),
            ..Default::default()
        };
        let decoded = decode(&encode(&state).unwrap()).unwrap();
        assert_eq!(decoded.refreshed_at, Some(123));

        assert!(decode(&[]).is_err());
        assert!(decode(b"not a cache blob").is_err());
    }

    #[test]
    fn a_never_checked_state_is_always_due() {
        assert!(CachedLicenseState::default().recheck_is_due(1_000));
    }

    #[test]
    fn a_recent_check_is_not_due_until_the_interval_elapses() {
        let state = CachedLicenseState {
            refreshed_at: Some(1_000),
            ..Default::default()
        };
        let interval = LICENSE_RECHECK_INTERVAL.as_secs();
        assert!(!state.recheck_is_due(1_000 + interval - 1));
        assert!(state.recheck_is_due(1_000 + interval));
    }
}
