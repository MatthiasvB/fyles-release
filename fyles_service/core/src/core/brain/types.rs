use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;
use tokio::sync::{Mutex, oneshot};

use crate::core::domain_models::{Contact, ContactId, FylesId};

use super::error::FilerequestError;
use crypto::signed::{SignError, SignatureVerificationError, Signed};
use crypto::{ContactPrivateKeys, ContactPublicKeys};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedContactShare(Signed<Contact>);

impl Contact {
    pub fn sign(
        self,
        signing_keys: &mut ContactPrivateKeys,
    ) -> Result<SignedContactShare, SignError> {
        let signed_by = self.id().to_string();
        Signed::sign(signed_by, self, signing_keys).map(SignedContactShare)
    }
}

#[derive(Debug, Error)]
#[error("contact share claiming {claimed_id}: {source}")]
pub struct ContactShareRejected {
    claimed_id: ContactId,
    source: SignatureVerificationError,
}

impl SignedContactShare {
    pub fn verify(self) -> Result<Contact, ContactShareRejected> {
        if let Err(source) = self.0.verify(&self.0.payload_unverified().public_keys) {
            return Err(ContactShareRejected {
                claimed_id: self.0.payload_unverified().id(),
                source,
            });
        }
        Ok(self.0.into_payload_unverified())
    }

    pub fn contact_unverified(&self) -> &Contact {
        self.0.payload_unverified()
    }
}

#[derive(Debug)]
pub struct ContactRegistration {
    pub share: SignedContactShare,
    pub user_edited_name: Option<String>,
}

impl From<SignedContactShare> for ContactRegistration {
    fn from(share: SignedContactShare) -> Self {
        Self {
            share,
            user_edited_name: None,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl SignedContactShare {
    pub fn for_test() -> Self {
        let mut keys = crypto::ContactKeys::new();
        let contact = Contact {
            name: "Test Contact".into(),
            public_keys: keys.public.clone(),
        };
        contact
            .sign(&mut keys.private)
            .expect("signing a test share to work")
    }

    pub fn forged_for_test(contact: Contact) -> Self {
        let mut attacker = crypto::ContactKeys::new();
        let signed_by = contact.id().to_string();
        Signed::sign(signed_by, contact, &mut attacker.private)
            .map(Self)
            .expect("signing a test share to work")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PubkeyFetchRequest {
    pub contact_ids: Vec<ContactId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchedContactKeys {
    pub keys: ContactPublicKeys,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubkeyFetchResponse {
    pub keys: Vec<FetchedContactKeys>,
}

#[cfg(test)]
mod contact_share_tests {
    use super::*;

    #[test]
    fn a_share_signed_by_the_keys_it_carries_verifies() {
        assert!(SignedContactShare::for_test().verify().is_ok());
    }

    /// Tampering with a share cannot be hidden by re-signing it, since only the key holder can
    /// sign for the keys it carries.
    #[test]
    fn a_share_signed_by_anyone_else_is_refused() {
        let victim = SignedContactShare::for_test().verify().unwrap();

        assert!(
            SignedContactShare::forged_for_test(victim)
                .verify()
                .is_err()
        );
    }

    #[test]
    fn editing_the_payload_breaks_the_signature() {
        let mut share = SignedContactShare::for_test();
        share
            .0
            .tamper_payload_for_test(|contact| contact.name = "Someone else".into());

        assert!(share.verify().is_err());
    }
}
