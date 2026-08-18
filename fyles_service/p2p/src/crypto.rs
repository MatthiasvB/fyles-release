use crate::utils::W;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
use crypto::{
    BINCODE_CONFIG, ContactPrivateKeys, ContactPublicKeys, DeSerCryptError, Dencryptor, Encrypt,
    Encrypted, SerCryptError,
    rolling_nonce_window::{
        NonceGenerationError, new_nonce_generator_validator_for_session_initiator,
        new_nonce_generator_validator_for_session_responder,
    },
};
use ed25519_dalek::ed25519::signature::SignerMut;
use fyles_core::{core::domain_models::ContactId, library::ttlmap::TtlMap};
use hkdf::Hkdf;
use ml_dsa::{EncodedSignature, MlDsa87, Signature, Signer, Verifier};
use ml_kem::{
    Ciphertext, Decapsulate, DecapsulationKey, Encapsulate, EncapsulationKey, Kem, Key, KeyExport,
    MlKem1024,
};
use rand_core::OsRng;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::hash::Hash;
use std::{
    fmt::{self, Debug},
    ops::{Deref, DerefMut},
    sync::Arc,
    time::SystemTime,
};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};
use tokio::sync::broadcast::error::SendError;
use tokio::sync::{Mutex, broadcast};
use tracing::{error, trace, warn};
use x25519_dalek::{EphemeralSecret, PublicKey};

// Re-exported from `crate::constants`, which owns every wall-clock duration in this crate and
// documents how they relate to each other. Kept as re-exports so existing call sites and tests
// that reach for `crypto::SESSION_VALIDITY_DURATION` keep working.
pub use crate::constants::{
    ACCEPT_SESSION_PAST_VALID_DURATION, SESSION_CONSTRUCTION_TIMEOUT, SESSION_VALIDITY_DURATION,
};

use crate::constants::{SESSION_ID_INFO, SESSION_KDF_INFO};

pub struct SessionInitiatorSecrets {
    pub private: SessionInitiatorPrivateKeys,
    pub public: SessionInitiatorPublicKeys,
}

pub struct SessionInitiatorPrivateKeys {
    /// diffie-hellman private key
    x25519: EphemeralSecret,
    pq_encryption: DecapsulationKey<MlKem1024>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SessionInitiatorPublicKeys {
    /// diffie-hellman public key
    x25519: PublicKey,
    // needs a wrapper because othewise Serialize can't be implemented
    pq_encryption: PqEncryptionPublicKeyWrapper,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct PqEncryptionPublicKeyWrapper {
    #[serde(with = "serde_pq_encryption_public_key")]
    public_key: EncapsulationKey<MlKem1024>,
}

impl From<EncapsulationKey<MlKem1024>> for PqEncryptionPublicKeyWrapper {
    fn from(public_key: EncapsulationKey<MlKem1024>) -> Self {
        Self { public_key }
    }
}

impl SessionInitiatorPublicKeys {
    fn to_shared_secrets_and_response(&self) -> (SessionSecrets, Box<KeyExchangeResponseMessage>) {
        let responder_x25519_secret = EphemeralSecret::random_from_rng(OsRng);
        let responder_x25519_public = PublicKey::from(&responder_x25519_secret);
        let x25519_shared_secret = responder_x25519_secret.diffie_hellman(&self.x25519);
        let (pq_encryption_ciphertext, pq_encryption_shared_secret) =
            self.pq_encryption.public_key.encapsulate();
        let responder_shared_secrets = SessionSecrets {
            pq_encryption: pq_encryption_shared_secret,
            x25519: x25519_shared_secret,
        };
        let response_message = Box::new(KeyExchangeResponseMessage {
            pq_encryption_ciphertext: pq_encryption_ciphertext.into(),
            x25519_public: responder_x25519_public,
        });
        (responder_shared_secrets, response_message)
    }
}

impl Default for SessionInitiatorSecrets {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionInitiatorSecrets {
    pub fn new() -> Self {
        let (pq_encryption_secret, pq_encryption_public) = MlKem1024::generate_keypair();
        let x25519 = EphemeralSecret::random_from_rng(OsRng);
        let public = SessionInitiatorPublicKeys {
            x25519: PublicKey::from(&x25519),
            pq_encryption: pq_encryption_public.into(),
        };
        let private = SessionInitiatorPrivateKeys {
            x25519,
            pq_encryption: pq_encryption_secret,
        };
        Self { private, public }
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
        Signed::sign(
            RequestPayload {
                sender: self_contact_id,
                message: Box::new(self),
            },
            signing_keys,
        )
    }
}

impl AuthWireRequest<Box<SessionInitiationRequest>> {
    pub fn to_session_and_response(
        &self,
        contact_data_for_verification: Option<(ContactId, &ContactPublicKeys)>,
        self_contact_id: ContactId,
        signing_keys: &mut ContactPrivateKeys,
    ) -> Result<(Session, AuthWireResponse<Box<KeyExchangeResponseMessage>>), SessionFromRequestError>
    {
        // Produced by the verification rather than alongside it: this is `Some` only where the
        // signatures actually checked out, so nothing downstream can read "we hold keys for this
        // contact" as "this is that contact".
        let authenticated_contact = match contact_data_for_verification {
            Some((contact_id, contact_public)) => {
                self.verify(contact_public).inspect_err(|e| {
                    warn!(?e, ?contact_id, "Somebody tried to impersonate this contact. Crypto material doesn't match. Not accepting.");
                })?;
                Some(contact_id)
            }
            None => None,
        };

        let is_authenticated = authenticated_contact.is_some();

        let (shared_secret, response_message) = self
            .payload
            .message
            .session_initiator_public_keys
            .to_shared_secrets_and_response();

        let established_authentication = match (
            authenticated_contact,
            self.payload.message.will_authenticate_responder,
        ) {
            (None, true) => EstablishedSessionAuthentication::OtherKnowsMe,
            (None, false) => EstablishedSessionAuthentication::Unauthenticated,
            (Some(contact_id), true) => {
                EstablishedSessionAuthentication::WeKnowEachOther(contact_id)
            }
            (Some(contact_id), false) => EstablishedSessionAuthentication::IKnowOther(contact_id),
        };

        // The response payload has to be complete before the session exists: it is half the
        // transcript the key is derived from.
        let response = Signed::sign(
            ResponsePayload {
                sender: self_contact_id,
                is_authenticated,
                message: response_message,
            },
            signing_keys,
        )?;

        let mut transcript = self.transcript_bytes();
        transcript.extend_from_slice(&response.transcript_bytes());
        let session = Session::new(established_authentication, shared_secret, false, &transcript);

        Ok((session, response))
    }
}

/// Everything the initiator's signature covers. Signed bytes are derived from `Serialize`, so a
/// field added here is authenticated without anyone having to remember to extend a hand-written
/// encoder — see `docs/architecture/security.md` §4.2.
#[derive(Serialize, Deserialize, Debug)]
pub struct RequestPayload<T> {
    pub sender: ContactId,
    pub message: T,
}

/// The responder's counterpart to [`RequestPayload`]. `is_authenticated` lives in here rather than
/// beside the signatures so that it is covered by them.
#[derive(Serialize, Deserialize, Debug)]
pub struct ResponsePayload<T> {
    pub sender: ContactId,
    pub is_authenticated: bool,
    pub message: T,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Signed<P> {
    pub payload: P,
    pub pq_signature: PqSignatureWrapper,
    pub ed25519_signature: ed25519_dalek::Signature,
}

pub type AuthWireRequest<T> = Signed<RequestPayload<T>>;
pub type AuthWireResponse<T> = Signed<ResponsePayload<T>>;

impl<P: Serialize> Signed<P> {
    fn signed_bytes(payload: &P) -> Vec<u8> {
        bincode::serde::encode_to_vec(payload, BINCODE_CONFIG).expect("serialization to work")
    }

    pub fn sign(
        payload: P,
        signing_keys: &mut ContactPrivateKeys,
    ) -> Result<Self, ed25519_dalek::ed25519::Error> {
        let compound_key = Self::signed_bytes(&payload);
        let pq_signature = signing_keys.pq_signing.sign(&compound_key).into();
        let ed25519_signature = signing_keys.ed25519.try_sign(&compound_key)?;
        Ok(Self {
            payload,
            pq_signature,
            ed25519_signature,
        })
    }

    /// Both schemes must validate. Driven by the same encoding as [`Self::sign`], so verification
    /// cannot fall behind a newly added field either.
    pub fn verify(
        &self,
        contact_public: &ContactPublicKeys,
    ) -> Result<&P, SignatureVerificationError> {
        let compound_key = Self::signed_bytes(&self.payload);
        contact_public
            .pq_signing
            .verify(&compound_key, &self.pq_signature.pq_signature)?;
        contact_public
            .ed25519
            .verify_strict(&compound_key, &self.ed25519_signature)?;
        Ok(&self.payload)
    }

    /// This side's half of the handshake transcript that the session key is bound to. Deliberately
    /// the same bytes the signature covers, so the two cannot describe different handshakes.
    pub fn transcript_bytes(&self) -> Vec<u8> {
        Self::signed_bytes(&self.payload)
    }
}

#[derive(Serialize, Deserialize)]
pub struct PqSignatureWrapper {
    #[serde(with = "serde_pq_signature")]
    pq_signature: Signature<MlDsa87>,
}

impl From<Signature<MlDsa87>> for PqSignatureWrapper {
    fn from(pq_signature: Signature<MlDsa87>) -> Self {
        Self { pq_signature }
    }
}

impl std::fmt::Debug for PqSignatureWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PqSignatureWrapper({:?})",
            self.pq_signature.encode().as_slice()
        )
    }
}

#[derive(Error, Debug)]
pub enum SignatureVerificationError {
    #[error("Post-quantum signature verification failed: {0}")]
    PqSigning(#[from] ml_dsa::Error),
    #[error("ed25519 signature verification failed: {0}")]
    Ed25519(#[from] ed25519_dalek::ed25519::Error),
}

#[derive(Error, Debug)]
pub enum SessionFromRequestError {
    #[error("Initiator signature verification failed: {0}")]
    SignatureVerification(#[from] SignatureVerificationError),
    #[error("Signing the key exchange response failed: {0}")]
    Signing(#[from] ed25519_dalek::ed25519::Error),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KeyExchangeResponseMessage {
    pq_encryption_ciphertext: PqEncryptionCiphertextWrapper,
    x25519_public: PublicKey,
}

#[derive(Serialize, Deserialize, Debug)]
struct PqEncryptionCiphertextWrapper {
    #[serde(with = "serde_pq_encryption_ciphertext")]
    pq_encryption_ciphertext: Ciphertext<MlKem1024>,
}

impl From<Ciphertext<MlKem1024>> for PqEncryptionCiphertextWrapper {
    fn from(ciphertext: Ciphertext<MlKem1024>) -> Self {
        Self {
            pq_encryption_ciphertext: ciphertext,
        }
    }
}

impl KeyExchangeResponseMessage {
    pub fn to_shared_secrets(self, private_keys: SessionInitiatorPrivateKeys) -> SessionSecrets {
        let initiator_pq_encryption_shared_secret = private_keys
            .pq_encryption
            .decapsulate(&self.pq_encryption_ciphertext.pq_encryption_ciphertext);
        let initiator_x25519_shared_secret =
            private_keys.x25519.diffie_hellman(&self.x25519_public);

        SessionSecrets {
            pq_encryption: initiator_pq_encryption_shared_secret,
            x25519: initiator_x25519_shared_secret,
        }
    }
}

pub type KeyExchangeResponse = AuthWireResponse<Box<KeyExchangeResponseMessage>>;

impl KeyExchangeResponse {
    pub fn unpack_verified(
        self,
        contact_public: &ContactPublicKeys,
    ) -> Result<Box<KeyExchangeResponseMessage>, SignatureVerificationError> {
        self.verify(contact_public)?;
        Ok(self.payload.message)
    }

    pub fn unpack_unverified(self) -> Box<KeyExchangeResponseMessage> {
        self.payload.message
    }
}

pub struct SessionSecrets {
    /// Private because it is `Copy` and has no `ZeroizeOnDrop`: a `pub` field lets any caller
    /// duplicate the secret by assignment, and only the original is zeroized — by our [`Drop`]
    /// below — leaving the copy in memory for good. Reach it through [`Self::pq_bytes`].
    pq_encryption: ml_kem::kem::SharedKey<MlKem1024>,
    pub x25519: x25519_dalek::SharedSecret,
}

impl SessionSecrets {
    fn pq_bytes(&self) -> &[u8] {
        self.pq_encryption.as_slice()
    }
}

impl Drop for SessionSecrets {
    fn drop(&mut self) {
        // The x25519 half wipes itself upstream; the ML-KEM half is a bare `Array<u8, U32>`.
        self.pq_encryption.as_mut_slice().zeroize();
    }
}

/// These wipe themselves only while their crates' `zeroize` features are on. A dependency bump that
/// drops one would silently stop clearing key material, so fail the build instead.
///
/// Two different checks because the crates advertise it differently: ml-kem implements the
/// `ZeroizeOnDrop` marker, while x25519-dalek uses the older `zeroize(drop)` attribute, which
/// generates a `Drop` impl without the marker. `needs_drop` is meaningful for the latter only
/// because both types are bare `[u8; 32]` — it would be vacuous for anything holding a `Box`.
const _: fn() = || {
    fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>() {}
    assert_zeroize_on_drop::<DecapsulationKey<MlKem1024>>();

    // Not self-wiping: `Drop for SessionSecrets` clears this one by hand, which needs the impl.
    fn assert_zeroize<T: zeroize::Zeroize>() {}
    assert_zeroize::<ml_kem::kem::SharedKey<MlKem1024>>();
};
const _: () = {
    assert!(std::mem::needs_drop::<x25519_dalek::SharedSecret>());
    assert!(std::mem::needs_drop::<EphemeralSecret>());
};

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("Could not generate a nonce for encryption: {0}")]
    NonceGenerationError(#[from] NonceGenerationError),
    #[error("Error during encryption")]
    EncryptionFailure,
}

impl SessionSecrets {
    /// `transcript` varies per session, and it covers the
    /// handshake bytes that are not themselves KDF inputs.
    ///
    /// The id comes off the same PRK under a different `info`, so it identifies the session without
    /// being reversible to the key.
    fn into_cipher_and_id(self, for_initiator: bool, transcript: &[u8]) -> (Dencryptor, [u8; 32]) {
        // Annotated to pin these as borrows. The ML-KEM key is `Copy` and only zeroizes on our
        // `Drop`, never its own, so dropping the `&` would silently leave a duplicate that nothing
        // ever zeroizes.
        let pq: &[u8] = self.pq_bytes();
        let x25519: &[u8] = self.x25519.as_bytes();

        // Capacity from the slices themselves, because a realloc would leave a copy of the ML-KEM
        // secret in freed memory.
        let mut ikm = Zeroizing::new(Vec::with_capacity(pq.len() + x25519.len()));
        ikm.extend_from_slice(pq);
        ikm.extend_from_slice(x25519);

        let hk = Hkdf::<Sha256>::new(Some(transcript), &ikm);
        let mut okm = Zeroizing::new([0u8; 32]);
        hk.expand(SESSION_KDF_INFO, &mut *okm).expect("works");
        let mut id = [0u8; 32];
        hk.expand(SESSION_ID_INFO, &mut id).expect("works");

        let cipher = ChaCha20Poly1305::new(chacha20poly1305::Key::from_slice(okm.as_slice()));
        let dencryptor = Dencryptor {
            cipher,
            nonce_generator_validator: if for_initiator {
                new_nonce_generator_validator_for_session_initiator()
            } else {
                new_nonce_generator_validator_for_session_responder()
            },
        };
        (dencryptor, id)
    }

    pub fn to_session(
        self,
        authentication: EstablishedSessionAuthentication,
        for_initiator: bool,
        transcript: &[u8],
    ) -> Session {
        Session::new(authentication, self, for_initiator, transcript)
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
    /// Both peers derive the same value, and it says nothing about the key, so unlike the raw secret
    /// so this can be kept for the session's lifetime and logged.
    id: [u8; 32],
    pub dencryptor: Mutex<Dencryptor>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("use_expiry", &self.use_expiry)
            .field("accept_expiry", &self.accept_expiry)
            .field("authentication_type", &self.authentication_type)
            .field("id", &self.id)
            .field("dencryptor", &self.dencryptor)
            .finish()
    }
}

impl PartialEq<Self> for Session {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Session {}

impl Session {
    pub fn new(
        established_authentication: EstablishedSessionAuthentication,
        session_secrets: SessionSecrets,
        for_initiator: bool,
        transcript: &[u8],
    ) -> Self {
        let use_expiry = SystemTime::now() + SESSION_VALIDITY_DURATION;
        let (dencryptor, id) = session_secrets.into_cipher_and_id(for_initiator, transcript);
        Self {
            use_expiry,
            accept_expiry: use_expiry + ACCEPT_SESSION_PAST_VALID_DURATION,
            authentication_type: established_authentication,
            id,
            dencryptor: dencryptor.into(),
        }
    }
}

/// serde adapter for an ML-DSA-87 signature, persisted as its FIPS 204 encoding.
mod serde_pq_signature {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        sig: &Signature<MlDsa87>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        sig.encode().as_slice().to_vec().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Signature<MlDsa87>, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let encoded = EncodedSignature::<MlDsa87>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-DSA signature length"))?;
        Signature::decode(&encoded)
            .ok_or_else(|| serde::de::Error::custom("invalid ML-DSA signature"))
    }
}

/// serde adapter for an ML-KEM-1024 encapsulation (public) key.
mod serde_pq_encryption_public_key {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        key: &EncapsulationKey<MlKem1024>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        key.to_bytes().as_slice().to_vec().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<EncapsulationKey<MlKem1024>, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let key = Key::<EncapsulationKey<MlKem1024>>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-KEM encapsulation key length"))?;
        EncapsulationKey::new(&key)
            .map_err(|_| serde::de::Error::custom("invalid ML-KEM encapsulation key"))
    }
}

/// serde adapter for an ML-KEM-1024 ciphertext.
mod serde_pq_encryption_ciphertext {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        ciphertext: &Ciphertext<MlKem1024>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        ciphertext.as_slice().to_vec().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Ciphertext<MlKem1024>, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Ciphertext::<MlKem1024>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-KEM ciphertext length"))
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
            Ok(Encrypted::new_plain(payload))
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

// FIXME: This should be obsolete
#[derive(Error, Debug)]
pub enum SessionCreationError {
    #[error("Could not create session because sufficient authentication could not be established")]
    Authentication,
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
        let is_authenticated = keys.payload.is_authenticated;
        let sender_contact_id = keys.payload.sender.clone();
        // Taken before `to_shared_secrets` consumes the message.
        let mut transcript = self.request_transcript;
        transcript.extend_from_slice(&keys.transcript_bytes());
        let key_exchange_response = match other_public_keys {
            None => {
                trace!(
                    "Creating unauthenticated session because we don't have keys for this contact"
                );
                keys.unpack_unverified()
            }
            Some(ref other_public_keys) => keys
                .unpack_verified(other_public_keys)
                .map_err(|_| SessionCreationError::Authentication)?,
        };

        let other_is_authenticated = other_public_keys.is_some();
        let authentication = match (other_is_authenticated, is_authenticated) {
            (false, true) => EstablishedSessionAuthentication::OtherKnowsMe,
            (false, false) => EstablishedSessionAuthentication::Unauthenticated,
            (true, true) => EstablishedSessionAuthentication::WeKnowEachOther(sender_contact_id),
            (true, false) => EstablishedSessionAuthentication::IKnowOther(sender_contact_id),
        };

        let session = Arc::new(
            key_exchange_response
                .to_shared_secrets(self.initiator_secrets)
                .to_session(authentication, true, &transcript),
        );

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
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use crypto::ContactKeys;
    use libp2p::PeerId;

    use super::{
        ContactId, ContactPublicKeys, InitiatorSessionCreation, SessionBuildStore,
        SessionInitiatorSecrets, SessionSecrets, SessionStore,
    };
    use crate::crypto::SessionInitiationRequest;
    use crate::send_receive_traits::session::Session;
    use crate::utils::Wrapper;

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
                .insert(b.contact_id.clone(), b.own_keys.public.clone());
            b.others_keys
                .insert(a.contact_id.clone(), a.own_keys.public.clone());

            Self { a, b }
        }
    }

    #[tokio::test]
    async fn simulated_key_exchange_works() {
        let peer_pair = PeerPair::new();

        let mut initiator = peer_pair.a;
        let mut responder = peer_pair.b;

        let request = {
            let initiator_secrets = Box::new(SessionInitiatorSecrets::new());
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
                    request.transcript_bytes(),
                ),
            );
            request
        };
        // send request over the wire
        let response = {
            // Would be available from request
            let initiator_peer_id = &initiator.peer_id;

            let other_contact_id_to_be_verified = request.payload.sender.clone();
            let initiator_public_keys = responder
                .others_keys
                .get(&request.payload.sender)
                .expect("Keys to be known");

            let (session, response) = request
                .to_session_and_response(
                    Some((
                        other_contact_id_to_be_verified.clone(),
                        initiator_public_keys,
                    )),
                    responder.contact_id.clone(),
                    &mut responder.own_keys.private,
                )
                .unwrap();
            responder
                .session_store
                .store
                .insert(*initiator_peer_id, Arc::new(session).into());
            response
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
            let session = session_creation
                .to_session(
                    response,
                    Some(
                        initiator
                            .others_keys
                            .get(&responder.contact_id)
                            .unwrap()
                            .clone(),
                    ),
                )
                .unwrap();

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
        let mut responder = peer_pair.b;

        // The attacker generates their own identity (different keys!)
        let attacker = PeerData::new();

        // The attacker crafts a session request using their OWN keys to sign,
        // but claims to be the legitimate initiator by spoofing the ContactId
        let spoofed_request = {
            let attacker_secrets = Box::new(SessionInitiatorSecrets::new());
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
            .expect("Responder knows the legitimate initiator's keys")
            .clone();

        // This must fail: the signatures were made with the attacker's keys,
        // but verification uses the legitimate contact's public keys
        let result = spoofed_request.to_session_and_response(
            Some((
                legitimate_initiator.contact_id.clone(),
                &real_initiator_public_keys,
            )),
            responder.contact_id.clone(),
            &mut responder.own_keys.private,
        );

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

    /// `is_authenticated` used to travel next to the signatures rather than under them
    /// (SECURITY_AUDIT Finding 5's FIXME), so anyone on the path could flip it and make the
    /// initiator believe the responder had authenticated it.
    #[tokio::test]
    async fn tampered_is_authenticated_fails_verification() {
        let mut responder = PeerData::new();

        let request = SessionInitiationRequest {
            session_initiator_public_keys: Box::new(SessionInitiatorSecrets::new().public),
            will_authenticate_responder: false,
        }
        .sign(&mut responder.own_keys.private.clone(), ContactId::new())
        .unwrap();

        let (_session, mut response) = request
            .to_session_and_response(
                None,
                responder.contact_id.clone(),
                &mut responder.own_keys.private,
            )
            .unwrap();

        assert!(
            !response.payload.is_authenticated,
            "Responder had no keys for the initiator, so it cannot claim authentication"
        );
        assert!(
            response.verify(&responder.own_keys.public).is_ok(),
            "The untampered response must verify"
        );

        response.payload.is_authenticated = true;

        assert!(
            response.verify(&responder.own_keys.public).is_err(),
            "Flipping is_authenticated must invalidate the signature"
        );
    }

    /// The session key is salted with the handshake transcript, so two peers that saw different
    /// handshakes must not end up able to talk to each other.
    #[tokio::test]
    async fn diverging_transcripts_produce_incompatible_sessions() {
        let peer_pair = PeerPair::new();
        let initiator = peer_pair.a;
        let mut responder = peer_pair.b;

        let initiator_secrets = Box::new(SessionInitiatorSecrets::new());
        let request = SessionInitiationRequest {
            session_initiator_public_keys: Box::new(initiator_secrets.public),
            will_authenticate_responder: true,
        }
        .sign(&mut initiator.own_keys.private.clone(), initiator.contact_id)
        .unwrap();

        let initiator_public_keys = responder
            .others_keys
            .get(&request.payload.sender)
            .expect("Keys to be known")
            .clone();
        let sender = request.payload.sender.clone();

        // The initiator remembers a transcript that differs from what the responder saw by a
        // single byte — everything else about the handshake is identical.
        let mut tampered_transcript = request.transcript_bytes();
        tampered_transcript[0] ^= 0x01;

        let (responder_session, response) = request
            .to_session_and_response(
                Some((sender, &initiator_public_keys)),
                responder.contact_id.clone(),
                &mut responder.own_keys.private,
            )
            .unwrap();

        let initiator_session =
            InitiatorSessionCreation::new(initiator_secrets.private, tampered_transcript)
                .to_session(
                    response,
                    Some(
                        initiator
                            .others_keys
                            .get(&responder.contact_id)
                            .unwrap()
                            .clone(),
                    ),
                )
                .unwrap();

        let responder_session = Wrapper(Arc::new(responder_session));
        let initiator_session = Wrapper(initiator_session);

        let encrypted = responder_session.encrypt(&b"hello".to_vec()).await.unwrap();
        assert!(
            initiator_session.decrypt(&encrypted).await.is_err(),
            "A one-byte transcript difference must yield a different key"
        );
    }

    /// Runs a full mutually-authenticated handshake, handing back both ends.
    fn established_pair(
        initiator: &mut PeerData,
        responder: &mut PeerData,
    ) -> (Arc<super::Session>, Arc<super::Session>) {
        let initiator_secrets = Box::new(SessionInitiatorSecrets::new());
        let request = SessionInitiationRequest {
            session_initiator_public_keys: Box::new(initiator_secrets.public),
            will_authenticate_responder: true,
        }
        .sign(
            &mut initiator.own_keys.private,
            initiator.contact_id.clone(),
        )
        .unwrap();

        let initiator_public_keys = responder
            .others_keys
            .get(&request.payload.sender)
            .expect("Keys to be known")
            .clone();
        let sender = request.payload.sender.clone();
        let transcript = request.transcript_bytes();

        let (responder_session, response) = request
            .to_session_and_response(
                Some((sender, &initiator_public_keys)),
                responder.contact_id.clone(),
                &mut responder.own_keys.private,
            )
            .unwrap();

        let initiator_session = InitiatorSessionCreation::new(initiator_secrets.private, transcript)
            .to_session(
                response,
                Some(
                    initiator
                        .others_keys
                        .get(&responder.contact_id)
                        .unwrap()
                        .clone(),
                ),
            )
            .unwrap();

        (initiator_session, Arc::new(responder_session))
    }

    /// Backs the resend-after-session-error path in `request_registry`, which asks whether the
    /// session we hold now is the one a failed request went out on.
    #[tokio::test]
    async fn sessions_from_different_handshakes_are_not_equal() {
        let peer_pair = PeerPair::new();
        let mut initiator = peer_pair.a;
        let mut responder = peer_pair.b;

        let (first, _) = established_pair(&mut initiator, &mut responder);
        let (second, _) = established_pair(&mut initiator, &mut responder);

        assert_eq!(first, first, "A session must equal itself");
        assert_ne!(
            first, second,
            "Separate handshakes must produce distinct sessions"
        );
    }

    /// Both ends derive the id from the same PRK, so it names the shared session rather than one
    /// side's view of it.
    #[tokio::test]
    async fn both_ends_of_a_handshake_agree_on_the_id() {
        let peer_pair = PeerPair::new();
        let mut initiator = peer_pair.a;
        let mut responder = peer_pair.b;

        let (initiator_session, responder_session) =
            established_pair(&mut initiator, &mut responder);

        assert_eq!(initiator_session, responder_session);
    }

    /// Nothing in the type system pins our hand-written `Drop for SessionSecrets`, so check the
    /// bytes: run the destructor over storage we own and look for the secret still sitting in it.
    #[test]
    fn dropping_session_secrets_wipes_them() {
        let initiator_secrets = SessionInitiatorSecrets::new();
        let (secrets, _response) = initiator_secrets.public.to_shared_secrets_and_response();

        let pq = secrets.pq_bytes().to_vec();
        let x25519 = secrets.x25519.as_bytes().to_vec();
        assert!(
            pq.iter().any(|b| *b != 0) && x25519.iter().any(|b| *b != 0),
            "Needs non-zero secrets to search for, or the assertions below pass vacuously"
        );

        let mut slot = MaybeUninit::new(secrets);
        // SAFETY: `slot` owns the value and is never read as a `SessionSecrets` again, so running
        // its destructor by hand cannot double-drop. Both fields are byte arrays of alignment 1, so
        // the struct carries no padding and every byte read below was written.
        let bytes = unsafe {
            std::ptr::drop_in_place(slot.as_mut_ptr());
            std::slice::from_raw_parts(slot.as_ptr().cast::<u8>(), size_of::<SessionSecrets>())
        };

        assert!(
            !bytes.windows(pq.len()).any(|w| w == pq),
            "ML-KEM shared secret survived the drop"
        );
        assert!(
            !bytes.windows(x25519.len()).any(|w| w == x25519),
            "x25519 shared secret survived the drop"
        );
    }
}
