use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::{Mutex, oneshot};

use crate::core::domain_models::{ContactId, FylesId};

use super::error::FilerequestError;
use crypto::ContactPublicKeys;

pub type FilerequestResult<T> = Result<T, FilerequestError>;

pub struct BrainRequest<T, U> {
    pub request: T,
    pub response_sender: Mutex<Option<oneshot::Sender<U>>>,
}

impl<T, U> BrainRequest<T, U> {
    pub fn with_receiver(request: T) -> (Self, oneshot::Receiver<U>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                request,
                response_sender: Mutex::new(Some(sender)),
            },
            receiver,
        )
    }
}

impl<T: Debug, U> Debug for BrainRequest<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrainRequest")
            .field("request", &self.request)
            .finish()
    }
}

pub type CreationResult = FilerequestResult<FylesId>;

pub type ByteChallenge = Vec<u8>;

#[derive(Debug, Serialize, Deserialize, Deref, DerefMut, Clone, Hash, PartialEq, Eq)]
pub struct SelfContactInviteChallenge(pub ByteChallenge);

impl From<ByteChallenge> for SelfContactInviteChallenge {
    fn from(value: ByteChallenge) -> Self {
        Self(value)
    }
}

#[derive(Debug, Serialize, Deserialize, Deref, DerefMut, Clone, Hash, PartialEq, Eq)]
pub struct ContactShareChallenge(pub ByteChallenge);

impl From<ByteChallenge> for ContactShareChallenge {
    fn from(value: ByteChallenge) -> Self {
        Self(value)
    }
}

/// Batched request for several contacts' public keys in one round trip.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PubkeyFetchRequest {
    pub contact_ids: Vec<ContactId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchedContactKeys {
    pub contact_id: ContactId,
    pub keys: ContactPublicKeys,
}

/// The requester MUST verify each entry's `fingerprint(keys)` against its own synced
/// fingerprint before trusting it — so a lying responder is harmless. Only contacts the
/// responder actually holds keys for are included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubkeyFetchResponse {
    pub keys: Vec<FetchedContactKeys>,
}
