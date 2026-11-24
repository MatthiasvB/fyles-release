//! Frozen copies of what migrations read and write: the synced-document shapes and the contact id
//! encoding. The live domain types drift, and a migration that drifts with them stops reproducing
//! the state it was written for.
//!
//! Genesis, load, hydrate and reconcile stay shared. A change to any of them is a `schema_major`
//! bump in its own right.

use std::collections::HashMap;

use autosurgeon::{Hydrate, Reconcile};
use data_encoding::BASE32_NOPAD;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::core::sync::{self, DocKind, SyncDocError};

const FC1_PREFIX: &str = "fc1-";
const FINGERPRINT_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
pub struct ContactEntryV1 {
    pub name: String,
    pub key_fingerprint: Vec<u8>,
}

pub fn key_fingerprint_v1(pq_signing_public: &[u8], ed25519_public: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(pq_signing_public);
    hasher.update(ed25519_public);
    hasher.finalize().to_vec()
}

#[derive(Debug, Error, PartialEq)]
#[error("a key fingerprint is {FINGERPRINT_LEN} bytes, got {0}")]
pub struct FingerprintLengthError(usize);

pub fn fc1_contact_id(fingerprint: &[u8]) -> Result<String, FingerprintLengthError> {
    let digest: &[u8; FINGERPRINT_LEN] = fingerprint
        .try_into()
        .map_err(|_| FingerprintLengthError(fingerprint.len()))?;
    Ok(format!("{FC1_PREFIX}{}", BASE32_NOPAD.encode(digest)))
}

#[derive(Debug, Error, PartialEq)]
#[error("not an fc1 contact id: {0}")]
pub struct NotAnFc1IdError(String);

pub fn fc1_fingerprint(id: &str) -> Result<Vec<u8>, NotAnFc1IdError> {
    id.strip_prefix(FC1_PREFIX)
        .and_then(|body| BASE32_NOPAD.decode(body.as_bytes()).ok())
        .filter(|fingerprint| fingerprint.len() == FINGERPRINT_LEN)
        .ok_or_else(|| NotAnFc1IdError(id.to_string()))
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
pub struct RemoteFrEntryV1 {
    pub peer_id_hint: String,
    pub filerequest_id: String,
    pub contact_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Default, Reconcile, Hydrate)]
pub struct ContactsDocV1 {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub contacts: HashMap<String, ContactEntryV1>,
}

#[derive(Debug, Clone, Default, Reconcile, Hydrate)]
pub struct RemoteFrDocV1 {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub remote_filerequests: HashMap<String, RemoteFrEntryV1>,
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
pub struct DeviceEntryV1 {
    pub name: String,
}

#[derive(Debug, Clone, Default, Reconcile, Hydrate)]
pub struct DevicesDocV1 {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub devices: HashMap<String, DeviceEntryV1>,
}

pub fn new_blob<T: Reconcile>(kind: DocKind, view: &T) -> Result<Vec<u8>, SyncDocError> {
    let mut doc = sync::genesis(kind)?;
    sync::reconcile(&mut doc, view)?;
    Ok(doc.save())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fingerprint_of_the_right_length_round_trips_through_its_id() {
        let id = fc1_contact_id(&[7; FINGERPRINT_LEN]).unwrap();

        assert_eq!(fc1_fingerprint(&id), Ok(vec![7; FINGERPRINT_LEN]));
    }

    #[test]
    fn a_fingerprint_of_any_other_length_is_refused() {
        for len in [0, FINGERPRINT_LEN - 1, FINGERPRINT_LEN + 1] {
            assert_eq!(
                fc1_contact_id(&vec![0; len]),
                Err(FingerprintLengthError(len))
            );
        }
    }

    #[test]
    fn the_frozen_encoding_matches_the_live_derivation() {
        let keys = crypto::ContactKeys::new().public;
        let fingerprint = key_fingerprint_v1(
            &crypto::serialize_pq_signing_public_key(&keys.pq_signing),
            &crypto::serialize_ed25519_public_key(&keys.ed25519),
        );

        assert_eq!(fc1_contact_id(&fingerprint).unwrap(), keys.id().to_string());
    }
}
