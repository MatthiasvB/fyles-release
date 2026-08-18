use std::{fmt::Debug, marker::PhantomData};

use aes_gcm::aead::Aead;
use chacha20poly1305::ChaCha20Poly1305;
use ed25519_dalek::ed25519::signature::{self};
use ml_dsa::{EncodedVerifyingKey, Generate, Keypair, MlDsa87, Seed, SigningKey, VerifyingKey};
use rand_core::OsRng;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use rolling_nonce_window::{NonceGenerationError, NonceGeneratorValidator, NonceValidationError};

pub mod rolling_nonce_window;

// All of this essentially implements https://datatracker.ietf.org/doc/html/draft-ietf-tls-hybrid-design

pub const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Debug, Serialize, Deserialize)]
pub struct Encrypted<T: ?Sized> {
    nonce: u64,
    message: Vec<u8>,
    _type: PhantomData<T>,
}

#[cfg(any(test, feature = "test-support"))]
impl<T: ?Sized + Serialize + DeserializeOwned> Encrypted<T> {
    pub fn new_plain(message: &T) -> Self {
        Self {
            nonce: 0,
            message: bincode::serde::encode_to_vec(message, BINCODE_CONFIG)
                .expect("serialization to work"),
            _type: PhantomData,
        }
    }

    pub fn extract_plain(&self) -> T {
        let (value, _) = bincode::serde::decode_from_slice(&*self.message, BINCODE_CONFIG)
            .expect("deserialization to work");
        value
    }
}

pub struct Dencryptor {
    pub cipher: ChaCha20Poly1305,
    pub nonce_generator_validator: NonceGeneratorValidator,
}

impl Debug for Dencryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dencryptor")
            .field("cipher", &"ChaCha20Poly1305 { ... }")
            .field("nonce_generator_validator", &"-no-debug-")
            .finish()
    }
}

pub struct EncryptAndNonce {
    encrypted: Vec<u8>,
    nonce: u64,
}

impl Dencryptor {
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<EncryptAndNonce, EncryptionError> {
        let nonce = self.nonce_generator_validator.get_next_send_nonce()?;
        let mut nonce_vec = Vec::with_capacity(12);
        nonce_vec.extend_from_slice(&nonce.to_le_bytes());
        nonce_vec.extend_from_slice(&[0u8; 4]);
        Ok(EncryptAndNonce {
            nonce,
            encrypted: self
                .cipher
                .encrypt(nonce_vec[..].into(), plaintext)
                .map_err(|_| EncryptionError::EncryptionFailure)?,
        })
    }

    pub fn decrypt(&mut self, ciphertext: &[u8], nonce: u64) -> Result<Vec<u8>, DecryptionError> {
        let accepted_nonce = self.nonce_generator_validator.check(nonce)?;
        let mut nonce_vec = Vec::with_capacity(12);
        nonce_vec.extend_from_slice(&nonce.to_le_bytes());
        nonce_vec.extend_from_slice(&[0u8; 4]);
        let plaintext = self
            .cipher
            .decrypt(nonce_vec.as_slice().into(), ciphertext)
            .map_err(|_e| DecryptionError::DecryptionFailure)?;
        // Only now: spending the slot for a payload that failed the tag check would reject the
        // genuine message carrying that nonce as a replay.
        self.nonce_generator_validator.commit(accepted_nonce)?;
        Ok(plaintext)
    }
}

#[derive(Error, Debug)]
pub enum SerCryptError {
    #[error("Error during serialization: {0}")]
    SelializationError(#[from] bincode::error::EncodeError),
    #[error("Error during encryption: {0}")]
    EncryptionError(#[from] EncryptionError),
}

pub trait Encrypt: Serialize {
    fn encrypt(&self, dencryptor: &mut Dencryptor) -> Result<Encrypted<Self>, SerCryptError>;
}

impl<T: Serialize> Encrypt for T {
    fn encrypt(&self, dencryptor: &mut Dencryptor) -> Result<Encrypted<Self>, SerCryptError> {
        let serialized = bincode::serde::encode_to_vec(self, BINCODE_CONFIG)?;
        let encrypted = dencryptor.encrypt(&serialized)?;
        Ok(Encrypted {
            nonce: encrypted.nonce,
            message: encrypted.encrypted,
            _type: PhantomData,
        })
    }
}

pub trait Decrypt: DeserializeOwned {
    type Payload;

    fn decrypt(&self, dencryptor: &mut Dencryptor) -> Result<Self::Payload, DeSerCryptError>;
}

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("Could not generate a nonce for encryption: {0}")]
    NonceGenerationError(#[from] NonceGenerationError),
    #[error("Error during encryption")]
    EncryptionFailure,
}

#[derive(Error, Debug)]
pub enum DecryptionError {
    #[error("Error while validating nonce: {0}")]
    NonceValidationError(#[from] NonceValidationError),
    #[error("Error during decryption")]
    DecryptionFailure,
}

#[derive(Error, Debug)]
pub enum DeSerCryptError {
    #[error("Unable to deserialize: {0}")]
    DeserializationError(#[from] bincode::error::DecodeError),
    #[error("Unable to decrypt: {0}")]
    DecryptionError(#[from] DecryptionError),
}

impl<T: Encrypt + DeserializeOwned> Decrypt for Encrypted<T> {
    type Payload = T;

    fn decrypt(&self, dencryptor: &mut Dencryptor) -> Result<Self::Payload, DeSerCryptError> {
        let serialized = dencryptor.decrypt(&self.message, self.nonce)?;
        let (value, _) = bincode::serde::decode_from_slice(&serialized, BINCODE_CONFIG)?;
        Ok(value)
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct ContactKeys {
    pub private: ContactPrivateKeys,
    pub public: ContactPublicKeys,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ContactPrivateKeys {
    /// Post-quantum signing key for quantum safe signing. They are huge, so
    /// we put them on the heap. Serialized as the 32-byte seed it is derived from.
    #[serde(with = "serde_pq_signing_private_key")]
    pub pq_signing: Box<SigningKey<MlDsa87>>,
    /// ed25519 private key for signing
    pub ed25519: ed25519_dalek::SigningKey,
}
impl std::fmt::Debug for ContactPrivateKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ContactPrivateKeys(pq_signing: -mööp-no-debu-impl-, ed25519: {:?})",
            self.ed25519.to_bytes()
        )
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ContactPublicKeys {
    /// Post-quantum verification key for quantum safe signature verification. They are huge, so
    /// we put them on the heap.
    #[serde(with = "serde_pq_signing_public_key")]
    pub pq_signing: Box<VerifyingKey<MlDsa87>>,
    /// ed25519 public key for signature verification
    pub ed25519: ed25519_dalek::VerifyingKey,
}
#[cfg(any(test, feature = "test-support"))]
static CONTACT_PUBLIC_KEYS: std::sync::LazyLock<ContactPublicKeys> =
    std::sync::LazyLock::new(|| ContactKeys::new().public);
#[cfg(any(test, feature = "test-support"))]
impl ContactPublicKeys {
    pub fn new() -> Self {
        CONTACT_PUBLIC_KEYS.to_owned()
    }

    pub fn random() -> Self {
        ContactKeys::new().public.to_owned()
    }
}

impl Default for ContactKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl ContactPrivateKeys {
    /// Derives the matching public keys. Both the post-quantum signing key and the
    /// ed25519 key deterministically yield their verification keys, so the public
    /// keys never need to be stored or transmitted alongside the private ones.
    pub fn derive_public(&self) -> ContactPublicKeys {
        ContactPublicKeys {
            pq_signing: Box::new(self.pq_signing.verifying_key()),
            ed25519: self.ed25519.verifying_key(),
        }
    }
}

impl ContactKeys {
    pub fn new() -> Self {
        let mut rng = OsRng;
        let pq_signing = SigningKey::<MlDsa87>::generate();
        let ed25519 = ed25519_dalek::SigningKey::generate(&mut rng);
        Self::from_private(ContactPrivateKeys {
            pq_signing: Box::new(pq_signing),
            ed25519,
        })
    }

    /// Builds the full keypair from the private keys, deriving the public keys.
    pub fn from_private(private: ContactPrivateKeys) -> Self {
        let public = private.derive_public();
        Self { private, public }
    }
}

impl Debug for ContactPublicKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ContactPublicKeys(pq_signing: -no-debug-, ed25519: {:?})",
            self.ed25519.to_bytes()
        )
    }
}

#[derive(Debug, Error)]
pub enum PublicEd25519DeserializationError {
    #[error(
        "Failed to deserialize public ed25519 key. Wrong length. Expected {expected} bytes, got {got}"
    )]
    PublicEd25519Length { got: usize, expected: usize },
    #[error("Failed to deserialize public ed25519 key. Wrong key format?")]
    PublicEd25519Format(signature::Error),
}

#[derive(Debug, Error)]
pub enum PrivateEd25519DeserializationError {
    #[error(
        "Failed to deserialize private ed25519 key. Wrong length. Expected {expected} bytes, got {got}"
    )]
    PrivateEd25519Length { got: usize, expected: usize },
}

pub fn deserialize_ed25519_public_key(
    key: Vec<u8>,
) -> Result<ed25519_dalek::VerifyingKey, PublicEd25519DeserializationError> {
    ed25519_dalek::VerifyingKey::from_bytes(key.as_slice().try_into().map_err(|_| {
        PublicEd25519DeserializationError::PublicEd25519Length {
            got: key.len(),
            expected: ed25519_dalek::PUBLIC_KEY_LENGTH,
        }
    })?)
    .map_err(PublicEd25519DeserializationError::PublicEd25519Format)
}

pub fn deserialize_ed25519_private_key(
    key: Vec<u8>,
) -> Result<ed25519_dalek::SigningKey, PrivateEd25519DeserializationError> {
    Ok(ed25519_dalek::SigningKey::from_bytes(
        key.as_slice().try_into().map_err(|_| {
            PrivateEd25519DeserializationError::PrivateEd25519Length {
                got: key.len(),
                expected: ed25519_dalek::PUBLIC_KEY_LENGTH,
            }
        })?,
    ))
}

#[derive(Debug, Error)]
#[error(
    "Failed to deserialize post-quantum public signing key. Wrong length, got {got} bytes, expected {expected}"
)]
pub struct PqSigningPublicKeyDeserializationError {
    pub got: usize,
    pub expected: usize,
}

#[derive(Debug, Error)]
#[error(
    "Failed to deserialize post-quantum private signing key seed. Wrong length, got {got} bytes, expected {expected}"
)]
pub struct PqSigningPrivateKeyDeserializationError {
    pub got: usize,
    pub expected: usize,
}

/// Post-quantum verification keys serialize to their fixed-size FIPS 204 encoding.
pub fn deserialize_pq_signing_public_key(
    key: Vec<u8>,
) -> Result<VerifyingKey<MlDsa87>, PqSigningPublicKeyDeserializationError> {
    let encoded = EncodedVerifyingKey::<MlDsa87>::try_from(key.as_slice()).map_err(|_| {
        PqSigningPublicKeyDeserializationError {
            got: key.len(),
            expected: EncodedVerifyingKey::<MlDsa87>::default().len(),
        }
    })?;
    Ok(VerifyingKey::decode(&encoded))
}

/// Post-quantum signing keys are stored and reconstructed from their 32-byte seed.
pub fn deserialize_pq_signing_private_key(
    key: Vec<u8>,
) -> Result<SigningKey<MlDsa87>, PqSigningPrivateKeyDeserializationError> {
    let seed =
        Seed::try_from(key.as_slice()).map_err(|_| PqSigningPrivateKeyDeserializationError {
            got: key.len(),
            expected: Seed::default().len(),
        })?;
    Ok(SigningKey::from_seed(&seed))
}

pub fn serialize_ed25519_public_key(key: &ed25519_dalek::VerifyingKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

pub fn serialize_ed25519_private_key(key: &ed25519_dalek::SigningKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

pub fn serialize_pq_signing_public_key(key: &VerifyingKey<MlDsa87>) -> Vec<u8> {
    key.encode().as_slice().to_vec()
}

pub fn serialize_pq_signing_private_key(key: &SigningKey<MlDsa87>) -> Vec<u8> {
    key.to_seed().as_slice().to_vec()
}

/// serde adapter persisting a post-quantum signing key as its 32-byte seed.
mod serde_pq_signing_private_key {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        key: &SigningKey<MlDsa87>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        key.to_seed().as_slice().to_vec().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Box<SigningKey<MlDsa87>>, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let seed = Seed::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-DSA signing key seed length"))?;
        Ok(Box::new(SigningKey::from_seed(&seed)))
    }
}

/// serde adapter persisting a post-quantum verification key as its FIPS 204 encoding.
mod serde_pq_signing_public_key {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        key: &VerifyingKey<MlDsa87>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        key.encode().as_slice().to_vec().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Box<VerifyingKey<MlDsa87>>, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let encoded = EncodedVerifyingKey::<MlDsa87>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-DSA verifying key length"))?;
        Ok(Box::new(VerifyingKey::decode(&encoded)))
    }
}

#[cfg(test)]
mod dencryptor_tests {
    use super::*;
    use chacha20poly1305::KeyInit;
    use rolling_nonce_window::{
        new_nonce_generator_validator_for_session_initiator,
        new_nonce_generator_validator_for_session_responder,
    };

    fn pair() -> (Dencryptor, Dencryptor) {
        let key = ChaCha20Poly1305::generate_key(&mut OsRng);
        (
            Dencryptor {
                cipher: ChaCha20Poly1305::new(&key),
                nonce_generator_validator: new_nonce_generator_validator_for_session_initiator(),
            },
            Dencryptor {
                cipher: ChaCha20Poly1305::new(&key),
                nonce_generator_validator: new_nonce_generator_validator_for_session_responder(),
            },
        )
    }

    /// Callers hand an inbound payload to every recent session in turn, so a foreign session
    /// rejecting it must not disturb the session that can actually read it.
    #[test]
    fn a_foreign_session_cannot_read_the_payload_or_disturb_the_right_one() {
        let (mut sender, mut receiver) = pair();
        let (_, mut wrong_receiver) = pair();

        let sent = sender.encrypt(b"the real payload").unwrap();

        assert!(
            wrong_receiver.decrypt(&sent.encrypted, sent.nonce).is_err(),
            "a foreign session must not decrypt this"
        );
        assert!(
            receiver.decrypt(&sent.encrypted, sent.nonce).is_ok(),
            "the right session must still accept the payload"
        );
    }

    #[test]
    fn tampered_ciphertext_does_not_spend_the_nonce_of_the_genuine_message() {
        let (mut sender, mut receiver) = pair();
        let sent = sender.encrypt(b"the real payload").unwrap();

        let mut forged = sent.encrypted.clone();
        forged[0] ^= 0xff;
        assert!(receiver.decrypt(&forged, sent.nonce).is_err());

        assert_eq!(
            receiver.decrypt(&sent.encrypted, sent.nonce).unwrap(),
            b"the real payload",
            "the genuine message must still be accepted"
        );
    }

    #[test]
    fn a_replayed_payload_is_still_rejected() {
        let (mut sender, mut receiver) = pair();
        let sent = sender.encrypt(b"the real payload").unwrap();
        let _ = sender
            .encrypt(b"keeps the window from rolling past the first")
            .unwrap();

        assert!(receiver.decrypt(&sent.encrypted, sent.nonce).is_ok());
        assert!(
            receiver.decrypt(&sent.encrypted, sent.nonce).is_err(),
            "committing on success must still close the door on replays"
        );
    }
}
