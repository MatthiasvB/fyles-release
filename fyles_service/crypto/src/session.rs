use std::fmt;
use std::time::{Duration, SystemTime};

use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
use hkdf::Hkdf;
use ml_kem::{
    Ciphertext, Decapsulate, DecapsulationKey, Encapsulate, EncapsulationKey, Kem, Key, KeyExport,
    MlKem1024,
};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{trace, warn};
use x25519_dalek::{EphemeralSecret, PublicKey};
use zeroize::{Zeroize, Zeroizing};

use crate::rolling_nonce_window::{
    new_nonce_generator_validator_for_session_initiator,
    new_nonce_generator_validator_for_session_responder,
};
use crate::signed::{SignError, SignatureVerificationError, Signed};
use crate::{ContactId, ContactPrivateKeys, ContactPublicKeys, Dencryptor};

/// HKDF `info`, to domain-separate the session key from any other derivation off the same secrets.
const SESSION_KDF_INFO: &[u8] = b"fyles-session-chacha20poly1305-key";

/// HKDF `info` for the session's identifier. Must differ from [`SESSION_KDF_INFO`], or the id would
/// be the key.
const SESSION_ID_INFO: &[u8] = b"fyles-session-id";

#[derive(Clone, Copy, Debug)]
pub struct SessionLifetime {
    pub usable_for: Duration,
    /// Absorbs clock skew between devices.
    pub accepted_past: Duration,
}

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
    ) -> Result<AuthWireRequest<Box<SessionInitiationRequest>>, SignError> {
        Signed::sign(
            self_contact_id.to_string(),
            RequestPayload {
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
        lifetime: SessionLifetime,
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
            .payload_unverified()
            .message
            .session_initiator_public_keys
            .to_shared_secrets_and_response();

        let established_authentication = match (
            authenticated_contact,
            self.payload_unverified()
                .message
                .will_authenticate_responder,
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
            self_contact_id.to_string(),
            ResponsePayload {
                is_authenticated,
                message: response_message,
            },
            signing_keys,
        )?;

        let mut transcript = self.canonical_payload_bytes()?;
        transcript.extend_from_slice(&response.canonical_payload_bytes()?);
        let session = Session::new(
            established_authentication,
            shared_secret,
            false,
            &transcript,
            lifetime,
        );

        Ok((session, response))
    }
}

/// Everything the initiator's signature covers. Signed bytes are derived from `Serialize`, so a
/// field added here is authenticated without anyone having to remember to extend a hand-written
/// encoder
#[derive(Serialize, Deserialize, Debug)]
pub struct RequestPayload<T> {
    pub message: T,
}

/// The responder's counterpart to [`RequestPayload`]. `is_authenticated` lives in here rather than
/// beside the signatures so that it is covered by them.
#[derive(Serialize, Deserialize, Debug)]
pub struct ResponsePayload<T> {
    pub is_authenticated: bool,
    pub message: T,
}

pub type AuthWireRequest<T> = Signed<RequestPayload<T>>;
pub type AuthWireResponse<T> = Signed<ResponsePayload<T>>;

#[derive(Error, Debug)]
pub enum SessionFromRequestError {
    #[error("Initiator signature verification failed: {0}")]
    SignatureVerification(#[from] SignatureVerificationError),
    #[error("Signing the key exchange response failed: {0}")]
    Signing(#[from] SignError),
    #[error("Serializing the handshake transcript failed: {0}")]
    Serialize(#[from] bincode::error::EncodeError),
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
    fn into_shared_secrets(self, private_keys: SessionInitiatorPrivateKeys) -> SessionSecrets {
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

// FIXME: This should be obsolete
#[derive(Error, Debug)]
pub enum SessionCreationError {
    #[error("Could not create session because sufficient authentication could not be established")]
    Authentication,
    #[error("Serializing the handshake transcript failed: {0}")]
    Serialize(#[from] bincode::error::EncodeError),
}

impl KeyExchangeResponse {
    /// `request_transcript` is the initiator's half of the handshake transcript.
    pub fn into_session(
        self,
        initiator_secrets: SessionInitiatorPrivateKeys,
        mut request_transcript: Vec<u8>,
        other_public_keys: Option<&ContactPublicKeys>,
        lifetime: SessionLifetime,
    ) -> Result<Session, SessionCreationError> {
        let is_authenticated = self.payload_unverified().is_authenticated;
        let sender_contact_id: ContactId = self
            .claimed_signed_by()
            .parse()
            .map_err(|_| SessionCreationError::Authentication)?;
        // Taken before `into_shared_secrets` consumes the message.
        request_transcript.extend_from_slice(&self.canonical_payload_bytes()?);
        match other_public_keys {
            None => trace!(
                "Creating unauthenticated session because we don't have keys for this contact"
            ),
            Some(other_public_keys) => {
                self.verify(other_public_keys)
                    .map_err(|_| SessionCreationError::Authentication)?;
            }
        }
        let key_exchange_response = self.into_payload_unverified().message;

        let authentication = match (other_public_keys.is_some(), is_authenticated) {
            (false, true) => EstablishedSessionAuthentication::OtherKnowsMe,
            (false, false) => EstablishedSessionAuthentication::Unauthenticated,
            (true, true) => EstablishedSessionAuthentication::WeKnowEachOther(sender_contact_id),
            (true, false) => EstablishedSessionAuthentication::IKnowOther(sender_contact_id),
        };

        Ok(Session::new(
            authentication,
            key_exchange_response.into_shared_secrets(initiator_secrets),
            true,
            &request_transcript,
            lifetime,
        ))
    }
}

struct SessionSecrets {
    /// Private because it is `Copy` and has no `ZeroizeOnDrop`: a `pub` field lets any caller
    /// duplicate the secret by assignment, and only the original is zeroized — by our [`Drop`]
    /// below — leaving the copy in memory for good. Reach it through [`Self::pq_bytes`].
    pq_encryption: ml_kem::kem::SharedKey<MlKem1024>,
    x25519: x25519_dalek::SharedSecret,
}

impl SessionSecrets {
    fn pq_bytes(&self) -> &[u8] {
        self.pq_encryption.as_slice()
    }
}

impl Drop for SessionSecrets {
    fn drop(&mut self) {
        // The x25519 half wipes itself upstream. The ML-KEM half is a bare `Array<u8, U32>`.
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
    fn new(
        established_authentication: EstablishedSessionAuthentication,
        session_secrets: SessionSecrets,
        for_initiator: bool,
        transcript: &[u8],
        lifetime: SessionLifetime,
    ) -> Self {
        let use_expiry = SystemTime::now() + lifetime.usable_for;
        let (dencryptor, id) = session_secrets.into_cipher_and_id(for_initiator, transcript);
        Self {
            use_expiry,
            accept_expiry: use_expiry + lifetime.accepted_past,
            authentication_type: established_authentication,
            id,
            dencryptor: dencryptor.into(),
        }
    }
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

/// serde adapter for an ML-KEM-1024 encapsulation (public) key.
mod serde_pq_encryption_public_key {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        key: &EncapsulationKey<MlKem1024>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(key.to_bytes().as_slice())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<EncapsulationKey<MlKem1024>, D::Error> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let key = Key::<EncapsulationKey<MlKem1024>>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-KEM encapsulation key length"))?;
        EncapsulationKey::new(&key)
            .map_err(|_| serde::de::Error::custom("invalid ML-KEM encapsulation key"))
    }
}

/// serde adapter for an ML-KEM-1024 ciphertext.
mod serde_pq_encryption_ciphertext {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        ciphertext: &Ciphertext<MlKem1024>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(ciphertext.as_slice())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Ciphertext<MlKem1024>, D::Error> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        Ciphertext::<MlKem1024>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-KEM ciphertext length"))
    }
}

#[cfg(test)]
mod test {
    use std::mem::MaybeUninit;

    use super::*;
    use crate::{ContactKeys, DeSerCryptError, Decrypt, Encrypt, Encrypted};

    const LIFETIME: SessionLifetime = SessionLifetime {
        usable_for: Duration::from_secs(60),
        accepted_past: Duration::from_secs(60),
    };

    struct PeerData {
        contact_id: ContactId,
        own_keys: ContactKeys,
    }

    impl PeerData {
        fn new() -> Self {
            Self {
                contact_id: ContactId::for_test(1),
                own_keys: ContactKeys::new(),
            }
        }
    }

    fn encrypt(session: &Session, message: &Vec<u8>) -> Encrypted<Vec<u8>> {
        message
            .encrypt(&mut session.dencryptor.blocking_lock())
            .unwrap()
    }

    fn decrypt(
        session: &Session,
        encrypted: &Encrypted<Vec<u8>>,
    ) -> Result<Vec<u8>, DeSerCryptError> {
        encrypted.decrypt(&mut session.dencryptor.blocking_lock())
    }

    /// Runs a full mutually-authenticated handshake, handing back both ends.
    fn established_pair(initiator: &mut PeerData, responder: &mut PeerData) -> (Session, Session) {
        let initiator_secrets = SessionInitiatorSecrets::new();
        let request = SessionInitiationRequest {
            session_initiator_public_keys: Box::new(initiator_secrets.public),
            will_authenticate_responder: true,
        }
        .sign(
            &mut initiator.own_keys.private,
            initiator.contact_id.clone(),
        )
        .unwrap();

        let sender = request.claimed_signed_by().parse::<ContactId>().unwrap();
        let transcript = request.canonical_payload_bytes().unwrap();

        let (responder_session, response) = request
            .to_session_and_response(
                Some((sender, &initiator.own_keys.public)),
                responder.contact_id.clone(),
                &mut responder.own_keys.private,
                LIFETIME,
            )
            .unwrap();

        let initiator_session = response
            .into_session(
                initiator_secrets.private,
                transcript,
                Some(&responder.own_keys.public),
                LIFETIME,
            )
            .unwrap();

        (initiator_session, responder_session)
    }

    #[test]
    fn simulated_key_exchange_works() {
        let (mut initiator, mut responder) = (PeerData::new(), PeerData::new());
        let (initiator_session, responder_session) =
            established_pair(&mut initiator, &mut responder);

        let message = b"Hello, world!".to_vec();
        let encrypted_message = encrypt(&initiator_session, &message);
        assert_eq!(
            message,
            decrypt(&responder_session, &encrypted_message).unwrap(),
            "Decrypted message should match original"
        );

        let message = b"Let's use a somewhat longer message to test the encryption and decryption, just to make sure everything works as expected!".to_vec();
        let encrypted_message = encrypt(&responder_session, &message);
        assert_eq!(
            message,
            decrypt(&initiator_session, &encrypted_message).unwrap(),
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
    #[test]
    fn spoofed_contact_id_is_rejected() {
        let legitimate_initiator = PeerData::new();
        let mut responder = PeerData::new();

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

        // This must fail: the signatures were made with the attacker's keys,
        // but verification uses the legitimate contact's public keys
        let result = spoofed_request.to_session_and_response(
            Some((
                legitimate_initiator.contact_id.clone(),
                &legitimate_initiator.own_keys.public,
            )),
            responder.contact_id.clone(),
            &mut responder.own_keys.private,
            LIFETIME,
        );

        assert!(
            result.is_err(),
            "Spoofed request must be rejected, but was accepted — this is the vulnerability!"
        );
        assert!(
            matches!(
                result.unwrap_err(),
                SessionFromRequestError::SignatureVerification(_)
            ),
            "Error must be a signature verification failure"
        );
    }

    /// `is_authenticated` used to travel next to the signatures rather than under them
    /// (SECURITY_AUDIT Finding 5's FIXME), so anyone on the path could flip it and make the
    /// initiator believe the responder had authenticated it.
    #[test]
    fn tampered_is_authenticated_fails_verification() {
        let mut responder = PeerData::new();

        let request = SessionInitiationRequest {
            session_initiator_public_keys: Box::new(SessionInitiatorSecrets::new().public),
            will_authenticate_responder: false,
        }
        .sign(
            &mut responder.own_keys.private.clone(),
            ContactId::for_test(1),
        )
        .unwrap();

        let (_session, mut response) = request
            .to_session_and_response(
                None,
                responder.contact_id.clone(),
                &mut responder.own_keys.private,
                LIFETIME,
            )
            .unwrap();

        assert!(
            !response.payload_unverified().is_authenticated,
            "Responder had no keys for the initiator, so it cannot claim authentication"
        );
        assert!(
            response.verify(&responder.own_keys.public).is_ok(),
            "The untampered response must verify"
        );

        response.tamper_payload_for_test(|payload| payload.is_authenticated = true);

        assert!(
            response.verify(&responder.own_keys.public).is_err(),
            "Flipping is_authenticated must invalidate the signature"
        );
    }

    /// The session key is salted with the handshake transcript, so two peers that saw different
    /// handshakes must not end up able to talk to each other.
    #[test]
    fn diverging_transcripts_produce_incompatible_sessions() {
        let initiator = PeerData::new();
        let mut responder = PeerData::new();

        let initiator_secrets = SessionInitiatorSecrets::new();
        let request = SessionInitiationRequest {
            session_initiator_public_keys: Box::new(initiator_secrets.public),
            will_authenticate_responder: true,
        }
        .sign(
            &mut initiator.own_keys.private.clone(),
            initiator.contact_id.clone(),
        )
        .unwrap();

        let sender = request.claimed_signed_by().parse::<ContactId>().unwrap();

        // The initiator remembers a transcript that differs from what the responder saw by a
        // single byte — everything else about the handshake is identical.
        let mut tampered_transcript = request.canonical_payload_bytes().unwrap();
        tampered_transcript[0] ^= 0x01;

        let (responder_session, response) = request
            .to_session_and_response(
                Some((sender, &initiator.own_keys.public)),
                responder.contact_id.clone(),
                &mut responder.own_keys.private,
                LIFETIME,
            )
            .unwrap();

        let initiator_session = response
            .into_session(
                initiator_secrets.private,
                tampered_transcript,
                Some(&responder.own_keys.public),
                LIFETIME,
            )
            .unwrap();

        let encrypted = encrypt(&responder_session, &b"hello".to_vec());
        assert!(
            decrypt(&initiator_session, &encrypted).is_err(),
            "A one-byte transcript difference must yield a different key"
        );
    }

    /// Backs the resend-after-session-error path in `request_registry`, which asks whether the
    /// session we hold now is the one a failed request went out on.
    #[test]
    fn sessions_from_different_handshakes_are_not_equal() {
        let (mut initiator, mut responder) = (PeerData::new(), PeerData::new());

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
    #[test]
    fn both_ends_of_a_handshake_agree_on_the_id() {
        let (mut initiator, mut responder) = (PeerData::new(), PeerData::new());

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
