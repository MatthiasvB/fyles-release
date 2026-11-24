use crate::constants::{SESSION_CONSTRUCTION_TIMEOUT, SESSION_LIFETIME};
use crate::utils::W;
use crypto::ContactPublicKeys;
use crypto::session::{
    KeyExchangeResponse, Session, SessionCreationError, SessionInitiatorPrivateKeys,
};
use fyles_core::{core::domain_models::ContactId, library::ttlmap::TtlMap};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tracing::error;

pub struct SessionStore<NodeId: Hash + Eq> {
    pub store: TtlMap<NodeId, W<Arc<Session>>>,
}

impl<T: Hash + Eq> Default for SessionStore<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Hash + Eq> SessionStore<T> {
    pub fn new() -> Self {
        Self {
            store: TtlMap::new(SESSION_LIFETIME.usable_for + SESSION_LIFETIME.accepted_past),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum AuthenticationRequirement {
    /// Peers don't know each other, so they can't authenticate.
    ///
    /// There is hardly any reason to use this except performance
    Unauthenticated,
    /// We do not know the peer we talk to, but he knows us and needs
    /// to authenticate us.
    ///
    /// This is useful when we _know_ that we don't know the peer, but he knows us, and want to make that
    /// explicit.
    InitiatorAuthenticated,
    /// Give the responder a chance to authenticate us, even if we are not sure that he knows us
    OptimisticInitiatorAuthenticated,
    /// The responder does not know us, but we know him and want to authenticate him
    ///
    /// This is useful when we _know_ that we know the responder, but he does not know us, and want to make that
    /// explicit.
    ResponderAuthenticated(ContactId),
    /// Both peers know each other and authenticate each other
    ///
    /// This is the most common case, where both peers know each other and authenticate each other. It shoud be the default.
    BilaterallyAuthenticated(ContactId),
    /// We know the contact but aren't sure if he knowns us. We want him to authenticate us if he knows us
    OptimisticBilaterallyAuthenticated(ContactId),
}

pub struct OneshotBroadcastSender<T> {
    has_sent: bool,
    sender: broadcast::Sender<T>,
}

pub struct SessionEstablishmentSender(
    OneshotBroadcastSender<Result<Arc<Session>, Arc<SessionEstablishmentError>>>,
);

impl Deref for SessionEstablishmentSender {
    type Target = OneshotBroadcastSender<Result<Arc<Session>, Arc<SessionEstablishmentError>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SessionEstablishmentSender {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Clone> OneshotBroadcastSender<T> {
    pub fn new() -> (Self, broadcast::Receiver<T>) {
        let (sender, receiver) = broadcast::channel(1);
        (
            Self {
                sender,
                has_sent: false,
            },
            receiver,
        )
    }

    /// Only use this if you know what you are doing!
    fn non_destructive_send(&mut self, value: T) -> Result<usize, broadcast::error::SendError<T>> {
        self.has_sent = true;
        self.sender.send(value)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<T> {
        self.sender.subscribe()
    }
}

impl SessionEstablishmentSender {
    #[allow(clippy::type_complexity)]
    pub fn new() -> (
        Self,
        broadcast::Receiver<Result<Arc<Session>, Arc<SessionEstablishmentError>>>,
    ) {
        let (sender, receiver) = OneshotBroadcastSender::new();
        (Self(sender), receiver)
    }

    #[allow(clippy::type_complexity)]
    pub fn send(
        mut self,
        value: Result<Arc<Session>, Arc<SessionEstablishmentError>>,
    ) -> Result<usize, SendError<Result<Arc<Session>, Arc<SessionEstablishmentError>>>> {
        self.non_destructive_send(value)
    }
}

impl Drop for SessionEstablishmentSender {
    fn drop(&mut self) {
        if !self.has_sent {
            error!("OneshotBroadcastSender dropped without sending a value");
            let _ = self.sender.send(Err(Arc::new(
                InitiatorSessionEstablishmentError::DroppedWithoutResponse.into(),
            )));
        }
    }
}

pub struct InitiatorSessionCreation {
    // pub contact_id: ContactId,
    initiator_secrets: SessionInitiatorPrivateKeys,
    /// Our half of the handshake transcript. Kept because the request itself is moved out to the
    /// swarm and the public keys inside it are never retained.
    request_transcript: Vec<u8>,
    // pub authenticate_contact: Option<(ContactId, ContactPublicKeys)>,
    session_creation_broadcast_sender: SessionEstablishmentSender,
}

impl InitiatorSessionCreation {
    pub fn new(
        initiator_secrets: SessionInitiatorPrivateKeys,
        request_transcript: Vec<u8>,
        // authenticate_contact: Option<(ContactId, ContactPublicKeys)>,
    ) -> Self {
        let (sender, _) = SessionEstablishmentSender::new();
        Self {
            initiator_secrets,
            request_transcript,
            // authenticate_contact,
            session_creation_broadcast_sender: sender,
        }
    }

    pub fn to_session(
        self,
        keys: KeyExchangeResponse,
        other_public_keys: Option<ContactPublicKeys>,
    ) -> Result<Arc<Session>, SessionCreationError> {
        let session = Arc::new(keys.into_session(
            self.initiator_secrets,
            self.request_transcript,
            other_public_keys.as_ref(),
            SESSION_LIFETIME,
        )?);

        let _ = self
            .session_creation_broadcast_sender
            .send(Ok(session.clone()));
        Ok(session)
    }

    pub fn get_session_receiver(
        &self,
    ) -> broadcast::Receiver<Result<Arc<Session>, Arc<SessionEstablishmentError>>> {
        self.session_creation_broadcast_sender.subscribe()
    }

    pub fn cancel(self, cause: SessionEstablishmentError) {
        let _ = self
            .session_creation_broadcast_sender
            .send(Err(Arc::new(cause)));
    }
}

pub struct SessionBuildStore<NodeId: Hash + Eq + Send> {
    pub store: TtlMap<NodeId, InitiatorSessionCreation>,
}

impl<NodeId: Hash + Eq + Send> Default for SessionBuildStore<NodeId> {
    fn default() -> Self {
        Self::new()
    }
}

impl<NodeId: Hash + Eq + Send> SessionBuildStore<NodeId> {
    pub fn new() -> Self {
        Self {
            store: TtlMap::new(SESSION_CONSTRUCTION_TIMEOUT),
        }
    }
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum SessionEstablishmentError {
    #[error("Initiator session establishment error: {0}")]
    Initiator(#[from] InitiatorSessionEstablishmentError),
    #[error("Responder session establishment error: {0}")]
    Responder(#[from] ResponderSessionEstablishmentError),
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum InitiatorSessionEstablishmentError {
    #[error("Could not generate ephemeral secret for handshake")]
    EphemeralSecretGenerationError,
    #[error("Error during signing of public keys")]
    SigningError,
    #[error("Did not receive response during session initialization")]
    NoResponse,
    #[error("Contact {0} is not known")]
    ContactNotKnown(ContactId),
    #[error("Session was dropped without response")]
    DroppedWithoutResponse,
    #[error("Unable to generate shared secret from key exchange")]
    SharedSecretGenerationFailed,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// A session establishment error that was raised on the responder's side
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum ResponderSessionEstablishmentError {
    #[error("Could not verify identity of contact {0}")]
    ContactVerificationFailed(ContactId),
    #[error("The claimed contact id is not a contact id")]
    MalformedContactId,
    #[error("Can not authenticate {0} because it is unknown")]
    UnknownContact(ContactId),
    #[error("Unable to generate shared secret from key exchange")]
    SharedSecretGenerationFailed,
    #[error("The authentication requirement \"{0:?}\" could not be met")]
    AuthenticationRequirementNotMet(AuthenticationRequirement),
    #[error("Internal error: {0}")]
    Internal(String),
}
