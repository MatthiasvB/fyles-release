use crate::utils::W;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
use crypto::{
    rolling_nonce_window::{
        new_nonce_generator_validator_for_session_initiator, new_nonce_generator_validator_for_session_responder,
        NonceGenerationError,
    }, ContactPrivateKeys, ContactPublicKeys, DeSerCryptError, Dencryptor,
    Encrypt, Encrypted,
    SerCryptError,
};
use ed25519_dalek::ed25519::signature::SignerMut;
use fyles_core::{core::domain_models::ContactId, library::ttlmap::TtlMap};
use hkdf::Hkdf;
use pqc_kyber::{KyberError, KYBER_CIPHERTEXTBYTES};
use pqcrypto_dilithium::dilithium5::*;
use pqcrypto_traits::sign::DetachedSignature;
use rand::rngs::OsRng;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::Sha256;
use std::hash::Hash;
use std::{
    fmt::{self, Debug},
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tap::Tap;
use thiserror::Error;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::{broadcast, Mutex};
use tracing::{error, trace, warn};
use x25519_dalek::{EphemeralSecret, PublicKey};

pub const SESSION_VALIDITY_DURATION: Duration = Duration::from_secs(30 * 60);
pub const ACCEPT_SESSION_PAST_VALID_DURATION: Duration = Duration::from_secs(60);
pub const SESSION_CONSTRUCTION_TIMEOUT: Duration = Duration::from_secs(60);

pub struct SessionInitiatorSecrets {
    pub private: SessionInitiatorPrivateKeys,
    pub public: SessionInitiatorPublicKeys,
}

pub struct SessionInitiatorPrivateKeys {
    /// diffie-hellman private key
    x25519: EphemeralSecret,
    kyber: pqc_kyber::SecretKey,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SessionInitiatorPublicKeys {
    /// diffie-hellman public key
    x25519: PublicKey,
    // needs a wrapper because othewise Serialize can't be implemented
    kyber: KyberPublicKeyWrapper,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct KyberPublicKeyWrapper {
    #[serde(with = "serde_bytes")]
    public_key: pqc_kyber::PublicKey,
}

impl From<pqc_kyber::PublicKey> for KyberPublicKeyWrapper {
    fn from(public_key: pqc_kyber::PublicKey) -> Self {
        Self { public_key }
    }
}

impl SessionInitiatorPublicKeys {
    pub fn get_vec(&self) -> Vec<u8> {
        self.x25519
            .as_bytes()
            .iter()
            .chain(&self.kyber.public_key)
            .copied()
            .collect::<Vec<_>>()
    }

    fn to_shared_secrets_and_response(
        &self,
    ) -> Result<(SessionSecrets, Box<KeyExchangeResponseMessage>), pqc_kyber::KyberError> {
        let responder_x25519_secret = EphemeralSecret::random_from_rng(OsRng);
        let responder_x25519_public = PublicKey::from(&responder_x25519_secret);
        let x25519_shared_secret = responder_x25519_secret.diffie_hellman(&self.x25519);
        let (kyber_ciphertext, kyber_shared_secret) =
            pqc_kyber::encapsulate(&self.kyber.public_key, &mut OsRng)?;
        let responder_shared_secrets = SessionSecrets {
            kyber: kyber_shared_secret,
            x25519: x25519_shared_secret,
        };
        let response_message = Box::new(KeyExchangeResponseMessage {
            kyber_ciphertext: kyber_ciphertext.into(),
            x25519_public: responder_x25519_public,
        });
        Ok((responder_shared_secrets, response_message))
    }
}

impl SessionInitiatorSecrets {
    pub fn new() -> Result<Self, pqc_kyber::KyberError> {
        let mut rng = OsRng;
        let kyber = pqc_kyber::keypair(&mut rng)?;
        let private = SessionInitiatorPrivateKeys {
            x25519: EphemeralSecret::random_from_rng(rng),
            kyber: kyber.secret,
        };
        let public = SessionInitiatorPublicKeys {
            x25519: PublicKey::from(&private.x25519),
            kyber: kyber.public.into(),
        };
        Ok(Self { private, public })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionInitiationRequest {
    pub session_initiator_public_keys: Box<SessionInitiatorPublicKeys>,
    will_authenticate_responder: bool,
}

impl SessionInitiationRequest {
    pub fn new(
        session_initiator_public_keys: Box<SessionInitiatorPublicKeys>,
        other_contact_id: Option<&ContactId>,
    ) -> Self {
        Self {
            session_initiator_public_keys,
            will_authenticate_responder: other_contact_id.is_some(),
        }
    }

    pub fn sign(
        self,
        signing_keys: &mut ContactPrivateKeys,
        self_contact_id: ContactId,
    ) -> Result<AuthWireRequest<Box<SessionInitiationRequest>>, ed25519_dalek::ed25519::Error> {
        let will_authenticate_other = self.will_authenticate_responder;
        let compound_key = self.get_vec();
        let dilithium_signature = detached_sign(&compound_key, &signing_keys.dilithium).into();
        let ed25519_signature = signing_keys.ed25519.try_sign(&compound_key)?;
        let session_init_request = Box::new(SessionInitiationRequest {
            session_initiator_public_keys: self.session_initiator_public_keys,
            will_authenticate_responder: will_authenticate_other,
        });
        Ok(AuthWireRequest {
            dilithium_signature,
            ed25519_signature,
            message: session_init_request,
            sender: self_contact_id,
        })
    }

    fn get_vec(&self) -> Vec<u8> {
        self.session_initiator_public_keys
            .get_vec()
            .tap_mut(|bytes| bytes.push(self.will_authenticate_responder as u8))
    }
}

impl AuthWireRequest<Box<SessionInitiationRequest>> {
    /// Verify the initiator's identity by checking both signature schemes
    /// against the provided public keys.
    fn verify_initiator_signature(
        &self,
        contact_public: &ContactPublicKeys,
    ) -> Result<(), SignatureVerificationError> {
        let compound_key = self.message.get_vec();
        verify_detached_signature(
            &self.dilithium_signature.dilithium_signature,
            &compound_key,
            &contact_public.dilithium,
        )?;
        contact_public
            .ed25519
            .verify_strict(&compound_key, &self.ed25519_signature)?;
        Ok(())
    }

    pub fn to_session_and_response(
        &self,
        contact_data_for_verification: Option<(ContactId, &ContactPublicKeys)>,
    ) -> Result<(Session, Box<KeyExchangeResponseMessage>), SessionFromRequestError> {
        // Verify initiator's identity signatures when we have their public keys
        if let Some((contact_id, contact_public)) = &contact_data_for_verification {
            self.verify_initiator_signature(contact_public).inspect_err(|e| {
                warn!(?e, ?contact_id, "Somebody tried to impersonate this contact. Crypto material doesn't match. Not accepting.");
            })?;
        }

        let (shared_secret, response) = self
            .message
            .session_initiator_public_keys
            .to_shared_secrets_and_response()?;

        let established_authentication = match (
            contact_data_for_verification,
            self.message.will_authenticate_responder,
        ) {
            (None, true) => EstablishedSessionAuthentication::OtherKnowsMe,
            (None, false) => EstablishedSessionAuthentication::Unauthenticated,
            (Some((contact_id, _)), true) => {
                EstablishedSessionAuthentication::WeKnowEachOther(contact_id)
            }
            (Some((contact_id, _)), false) => {
                EstablishedSessionAuthentication::IKnowOther(contact_id)
            }
        };

        let session = Session::new(established_authentication, shared_secret, false);

        Ok((session, response))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthWireRequest<T> {
    pub sender: ContactId,
    pub message: T,
    pub dilithium_signature: DilithiumDetachedSignatureWrapper,
    pub ed25519_signature: ed25519_dalek::Signature,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthWireResponse<T> {
    pub sender: ContactId,
    pub message: T,
    // FIXME: This should become part of the signed message
    pub is_authenticated: bool,
    pub dilithium_signature: DilithiumDetachedSignatureWrapper,
    pub ed25519_signature: ed25519_dalek::Signature,
}

#[derive(Serialize, Deserialize)]
pub struct DilithiumDetachedSignatureWrapper {
    dilithium_signature: pqcrypto_dilithium::dilithium5::DetachedSignature,
}

impl From<pqcrypto_dilithium::dilithium5::DetachedSignature> for DilithiumDetachedSignatureWrapper {
    fn from(dilithium_signature: pqcrypto_dilithium::dilithium5::DetachedSignature) -> Self {
        Self {
            dilithium_signature,
        }
    }
}

impl std::fmt::Debug for DilithiumDetachedSignatureWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DilithiumDetachedSignatureWrapper({:?})",
            self.dilithium_signature.as_bytes()
        )
    }
}

#[derive(Error, Debug)]
pub enum SignatureVerificationError {
    #[error("Dilithium signature verification failed: {0}")]
    Dilithium(#[from] pqcrypto_traits::sign::VerificationError),
    #[error("ed25519 signature verification failed: {0}")]
    Ed25519(#[from] ed25519_dalek::ed25519::Error),
}

#[derive(Error, Debug)]
pub enum SessionFromRequestError {
    #[error("Kyber error during session creation")]
    Kyber(pqc_kyber::KyberError),
    #[error("Initiator signature verification failed: {0}")]
    SignatureVerification(#[from] SignatureVerificationError),
}

impl From<pqc_kyber::KyberError> for SessionFromRequestError {
    fn from(e: pqc_kyber::KyberError) -> Self {
        Self::Kyber(e)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KeyExchangeResponseMessage {
    kyber_ciphertext: KyberSharedCipherWrapper,
    x25519_public: PublicKey,
}

#[derive(Serialize, Deserialize, Debug)]
struct KyberSharedCipherWrapper {
    #[serde(with = "serde_bytes")]
    kyber_ciphertext: KyberSharedCipher,
}

impl From<KyberSharedCipher> for KyberSharedCipherWrapper {
    fn from(ciphertext: KyberSharedCipher) -> Self {
        Self {
            kyber_ciphertext: ciphertext,
        }
    }
}

impl KeyExchangeResponseMessage {
    pub fn get_vec(&self) -> Vec<u8> {
        self.x25519_public
            .as_bytes()
            .iter()
            .chain(&self.kyber_ciphertext.kyber_ciphertext)
            .copied()
            .collect::<Vec<_>>()
    }

    pub fn sign(
        self,
        contact_secrets: &mut ContactPrivateKeys,
        self_contact_id: ContactId,
        request_authenticated: bool,
    ) -> Result<KeyExchangeResponse, ed25519_dalek::ed25519::Error> {
        let compound_key = self.get_vec();
        let dilithium_signature = detached_sign(&compound_key, &contact_secrets.dilithium).into();
        let ed25519_signature = contact_secrets.ed25519.try_sign(&compound_key)?;
        Ok(KeyExchangeResponse {
            sender: self_contact_id,
            message: Box::new(self),
            dilithium_signature,
            ed25519_signature,
            is_authenticated: request_authenticated,
        })
    }

    pub fn to_shared_secrets(
        self,
        private_keys: SessionInitiatorPrivateKeys,
    ) -> Result<SessionSecrets, KyberError> {
        let initiator_kyber_shared_secret =
            pqc_kyber::decapsulate(&self.kyber_ciphertext.kyber_ciphertext, &private_keys.kyber)?;
        let initiator_x25519_shared_secret =
            private_keys.x25519.diffie_hellman(&self.x25519_public);

        Ok(SessionSecrets {
            kyber: initiator_kyber_shared_secret,
            x25519: initiator_x25519_shared_secret,
        })
    }
}

pub type KeyExchangeResponse = AuthWireResponse<Box<KeyExchangeResponseMessage>>;

impl KeyExchangeResponse {
    pub fn unpack_verified(
        self,
        contact_public: &ContactPublicKeys,
    ) -> Result<Box<KeyExchangeResponseMessage>, SignatureVerificationError> {
        let compound_key = self.message.get_vec();
        pqcrypto_dilithium::dilithium5::verify_detached_signature(
            &self.dilithium_signature.dilithium_signature,
            &compound_key,
            &contact_public.dilithium,
        )?;
        contact_public
            .ed25519
            .verify_strict(&compound_key, &self.ed25519_signature)?;
        Ok(self.message)
    }

    pub fn unpack_unverified(self) -> Box<KeyExchangeResponseMessage> {
        self.message
    }
}

pub type KyberSharedCipher = [u8; KYBER_CIPHERTEXTBYTES];

pub struct SessionSecrets {
    pub kyber: pqc_kyber::SharedSecret,
    pub x25519: x25519_dalek::SharedSecret,
}

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("Could not generate a nonce for encryption: {0}")]
    NonceGenerationError(#[from] NonceGenerationError),
    #[error("Error during encryption")]
    EncryptionFailure,
}

impl SessionSecrets {
    fn to_cipher(self, for_initiator: bool) -> Dencryptor {
        let ikm = [self.kyber, *self.x25519.as_bytes()].concat();
        let hk = Hkdf::<Sha256>::new(None, &ikm);
        let mut okm = [0u8; 32];
        hk.expand(&[0u8; 0], &mut okm).expect("works");
        let key = chacha20poly1305::Key::from_slice(okm.as_slice());
        let cipher = ChaCha20Poly1305::new(key);
        Dencryptor {
            cipher,
            nonce_generator_validator: if for_initiator {
                new_nonce_generator_validator_for_session_initiator()
            } else {
                new_nonce_generator_validator_for_session_responder()
            },
        }
    }

    pub fn to_session(
        self,
        authentication: EstablishedSessionAuthentication,
        for_initiator: bool,
    ) -> Session {
        Session::new(authentication, self, for_initiator)
    }
}

pub trait Dencrypt<T: Encrypt + DeserializeOwned> {
    fn encrypt(&mut self, payload: &T) -> Result<Encrypted<T>, SerCryptError>;
    fn decrypt(&mut self, blob: &Encrypted<T>) -> Result<T, DeSerCryptError>;
}

pub struct Session {
    pub use_expiry: SystemTime,
    pub accept_expiry: SystemTime,
    pub authentication_type: EstablishedSessionAuthentication,
    secret: Vec<u8>,
    pub dencryptor: Mutex<Dencryptor>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("use_expiry", &self.use_expiry)
            .field("accept_expiry", &self.accept_expiry)
            .field("authentication_type", &self.authentication_type)
            .field("secret", &"---redacted---")
            .field("dencryptor", &self.dencryptor)
            .finish()
    }
}

impl PartialEq<Self> for Session {
    fn eq(&self, other: &Self) -> bool {
        self.secret == other.secret
    }
}

impl Eq for Session {}

impl Session {
    pub fn new(
        established_authentication: EstablishedSessionAuthentication,
        session_secrets: SessionSecrets,
        for_initiator: bool,
    ) -> Self {
        let use_expiry = SystemTime::now() + SESSION_VALIDITY_DURATION;
        Self {
            use_expiry,
            accept_expiry: use_expiry + ACCEPT_SESSION_PAST_VALID_DURATION,
            authentication_type: established_authentication,
            secret: session_secrets
                .kyber
                .to_vec()
                .tap_mut(|it| it.append(&mut session_secrets.x25519.as_bytes().into())),
            dencryptor: session_secrets.to_cipher(for_initiator).into(),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
#[allow(dead_code)] // might become useful in tests
mod test_support {
    use crypto::Encrypted;

    use super::*;

    struct NoOpSession;

    impl<T: Serialize + DeserializeOwned> Dencrypt<T> for NoOpSession {
        fn encrypt(&mut self, payload: &T) -> Result<Encrypted<T>, SerCryptError> {
            Ok(Encrypted::new_plain(&payload))
        }

        fn decrypt(&mut self, blob: &Encrypted<T>) -> Result<T, DeSerCryptError> {
            Ok(blob.extract_plain())
        }
    }
}

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
            store: TtlMap::new(SESSION_VALIDITY_DURATION + ACCEPT_SESSION_PAST_VALID_DURATION),
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

#[derive(Clone, Debug)]
pub enum EstablishedSessionAuthentication {
    Unauthenticated,
    IKnowOther(ContactId),
    OtherKnowsMe,
    WeKnowEachOther(ContactId),
}

impl EstablishedSessionAuthentication {
    pub fn get_contact_id(&self) -> Option<&ContactId> {
        match self {
            EstablishedSessionAuthentication::Unauthenticated
            | EstablishedSessionAuthentication::OtherKnowsMe => None,
            EstablishedSessionAuthentication::IKnowOther(contact_id) => Some(contact_id),
            EstablishedSessionAuthentication::WeKnowEachOther(contact_id) => Some(contact_id),
        }
    }
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
    pub fn new() -> (
        Self,
        broadcast::Receiver<Result<Arc<Session>, Arc<SessionEstablishmentError>>>,
    ) {
        let (sender, receiver) = OneshotBroadcastSender::new();
        (Self(sender), receiver)
    }

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
    // pub authenticate_contact: Option<(ContactId, ContactPublicKeys)>,
    session_creation_broadcast_sender: SessionEstablishmentSender,
}

// FIXME: This should be obsolete
#[derive(Error, Debug)]
pub enum SessionCreationError {
    #[error("Could not create Session because Kyber shared secret creation failed")]
    Crypto(KyberError),
    #[error("Could not create session because sufficient authentication could not be established")]
    Authentication,
}

impl InitiatorSessionCreation {
    pub fn new(
        initiator_secrets: SessionInitiatorPrivateKeys,
        // authenticate_contact: Option<(ContactId, ContactPublicKeys)>,
    ) -> Self {
        let (sender, _) = SessionEstablishmentSender::new();
        Self {
            initiator_secrets,
            // authenticate_contact,
            session_creation_broadcast_sender: sender,
        }
    }

    pub fn to_session(
        self,
        keys: KeyExchangeResponse,
        other_public_keys: Option<ContactPublicKeys>
    ) -> Result<Arc<Session>, SessionCreationError> {
        let is_authenticated = keys.is_authenticated;
        let sender_contact_id = keys.sender.clone();
        let key_exchange_response = match other_public_keys {
            None => {
                trace!("Creating unauthenticated session because we don't have keys for this contact");
                keys.unpack_unverified()
            },
            Some(ref other_public_keys) => keys
                .unpack_verified(other_public_keys)
                .map_err(|_| SessionCreationError::Authentication)?,
        };

        let other_is_authenticated = other_public_keys.is_some();
        let authentication = match (other_is_authenticated, is_authenticated) {
            (false, true) => EstablishedSessionAuthentication::OtherKnowsMe,
            (false, false) => EstablishedSessionAuthentication::Unauthenticated,
            (true, true) => {
                EstablishedSessionAuthentication::WeKnowEachOther(sender_contact_id)
            }
            (true, false) => {
                EstablishedSessionAuthentication::IKnowOther(sender_contact_id)
            }
        };

        let session_res = key_exchange_response
            .to_shared_secrets(self.initiator_secrets)
            .map(|s| s.to_session(authentication, true))
            .map(Arc::new);

        let _ = self
            .session_creation_broadcast_sender
            .send(match &session_res {
                Ok(session) => Ok(session.clone()),
                Err(_) => Err(Arc::new(
                    InitiatorSessionEstablishmentError::SharedSecretGenerationFailed.into(),
                )),
            });
        session_res.map_err(SessionCreationError::Crypto)
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
    #[error("Can not authenticate {0} because it is unknown")]
    UnknownContact(ContactId),
    #[error("Unable to generate shared secret from key exchange")]
    SharedSecretGenerationFailed,
    #[error("The authentication requirement \"{0:?}\" could not be met")]
    AuthenticationRequirementNotMet(AuthenticationRequirement),
    #[error("Internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crypto::ContactKeys;
    use libp2p::PeerId;

    use super::{
        ContactId, ContactPublicKeys, InitiatorSessionCreation, SessionBuildStore,
        SessionInitiatorSecrets, SessionStore,
    };
    use crate::crypto::SessionInitiationRequest;
    use crate::send_receive_traits::session::Session;

    struct PeerData {
        contact_id: ContactId,
        peer_id: PeerId,
        own_keys: ContactKeys,
        others_keys: HashMap<ContactId, ContactPublicKeys>,
        session_store: SessionStore<PeerId>,
        session_build_store: SessionBuildStore<PeerId>,
    }

    impl PeerData {
        fn new() -> Self {
            Self {
                contact_id: ContactId::new(),
                peer_id: PeerId::random(),
                own_keys: ContactKeys::new(),
                others_keys: Default::default(),
                session_store: SessionStore::new(),
                session_build_store: SessionBuildStore::new(),
            }
        }
    }

    struct PeerPair {
        a: PeerData,
        b: PeerData,
    }

    impl PeerPair {
        fn new() -> Self {
            let mut a = PeerData::new();
            let mut b = PeerData::new();

            a.others_keys
                .insert(b.contact_id.clone().into(), b.own_keys.public.clone());
            b.others_keys
                .insert(a.contact_id.clone().into(), a.own_keys.public.clone());

            Self { a, b }
        }
    }

    #[tokio::test]
    async fn simulated_key_exchange_works() {
        let peer_pair = PeerPair::new();

        let mut initiator = peer_pair.a;
        let mut responder = peer_pair.b;

        let request = {
            let initiator_secrets = Box::new(SessionInitiatorSecrets::new().unwrap());
            let request = SessionInitiationRequest {
                session_initiator_public_keys: Box::new(initiator_secrets.public),
                will_authenticate_responder: true,
            }
                .sign(&mut initiator.own_keys.private, initiator.contact_id)
                .unwrap();
            initiator.session_build_store.store.insert(
                responder.peer_id,
                InitiatorSessionCreation::new(
                    initiator_secrets.private,
                ),
            );
            request
        };
        // send request over the wire
        let response = {
            // Would be available from request
            let initiator_peer_id = &initiator.peer_id;

            let other_contact_id_to_be_verified = request.sender.clone();
            let initiator_public_keys = responder
                .others_keys
                .get(&request.sender)
                .expect("Keys to be known");

            let (session, response) = request
                .to_session_and_response(Some((
                    other_contact_id_to_be_verified.clone(),
                    initiator_public_keys,
                )))
                .unwrap();
            responder
                .session_store
                .store
                .insert(*initiator_peer_id, Arc::new(session).into());
            response
                .sign(
                    &mut responder.own_keys.private,
                    other_contact_id_to_be_verified,
                    true,
                )
                .expect("Signing to work")
        };
        // send response back
        {
            // I would get this from _somewhere_
            let responder_peer_id = &responder.peer_id;

            let session_creation = initiator
                .session_build_store
                .store
                .remove(responder_peer_id)
                .expect("Session builder to be there");
            let session = session_creation.to_session(response, Some(initiator.others_keys.get(&responder.contact_id).unwrap().clone())).unwrap();

            initiator
                .session_store
                .store
                .insert(*responder_peer_id, session.into());
        }

        // Both sides have working Dencryptors now, so we can try to exchange encrypted messages
        let message = b"Hello, world!".to_vec();
        let initiator_session = initiator
            .session_store
            .store
            .get_mut(&responder.peer_id)
            .expect("Session to be there");

        let encrypted_message = initiator_session.encrypt(&message).await.unwrap();

        let responder_session = responder
            .session_store
            .store
            .get_mut(&initiator.peer_id)
            .expect("Session to be there");
        let decrypted_message = responder_session.decrypt(&encrypted_message).await.unwrap();
        assert_eq!(
            message.to_ascii_lowercase(),
            decrypted_message.as_slice().to_ascii_lowercase(),
            "Decrypted message should match original"
        );

        // Try the other way around
        let message = b"Let's use a somewhat longer message to test the encryption and decryption, just to make sure everything works as expected!".to_vec();
        let encrypted_message = responder_session.encrypt(&message).await.unwrap();
        let decrypted_message = initiator_session.decrypt(&encrypted_message).await.unwrap();
        assert_eq!(
            message.to_ascii_lowercase(),
            decrypted_message.as_slice().to_ascii_lowercase(),
            "Decrypted message should match original"
        );
    }

    /// Regression test for CVE-like Finding 1: the responder must reject a session
    /// initiation request where the sender spoofs a known ContactId but signs with
    /// different keys (i.e. the attacker's own keys).
    ///
    /// Before the fix, `to_session_and_response` never verified the initiator's
    /// signatures, so this attack would succeed and produce a session the responder
    /// falsely believed was mutually authenticated.
    #[tokio::test]
    async fn spoofed_contact_id_is_rejected() {
        // Set up a legitimate peer pair where both sides know each other's keys
        let peer_pair = PeerPair::new();
        let legitimate_initiator = peer_pair.a;
        let responder = peer_pair.b;

        // The attacker generates their own identity (different keys!)
        let attacker = PeerData::new();

        // The attacker crafts a session request using their OWN keys to sign,
        // but claims to be the legitimate initiator by spoofing the ContactId
        let spoofed_request = {
            let attacker_secrets = Box::new(SessionInitiatorSecrets::new().unwrap());
            SessionInitiationRequest {
                session_initiator_public_keys: Box::new(attacker_secrets.public),
                will_authenticate_responder: true,
            }
                // Signed with attacker's keys, but sender field is the legitimate contact's ID
                .sign(
                    &mut attacker.own_keys.private.clone(),
                    legitimate_initiator.contact_id.clone(),
                )
                .unwrap()
        };

        // The responder looks up the claimed sender's public keys (the real ones)
        let real_initiator_public_keys = responder
            .others_keys
            .get(&legitimate_initiator.contact_id)
            .expect("Responder knows the legitimate initiator's keys");

        // This must fail: the signatures were made with the attacker's keys,
        // but verification uses the legitimate contact's public keys
        let result = spoofed_request.to_session_and_response(Some((
            legitimate_initiator.contact_id.clone(),
            real_initiator_public_keys,
        )));

        assert!(
            result.is_err(),
            "Spoofed request must be rejected, but was accepted — this is the vulnerability!"
        );
        assert!(
            matches!(
                result.unwrap_err(),
                super::SessionFromRequestError::SignatureVerification(_)
            ),
            "Error must be a signature verification failure"
        );
    }
}
