use std::{fmt::Debug, marker::PhantomData};

use aes_gcm::aead::Aead;
use chacha20poly1305::ChaCha20Poly1305;
use ed25519_dalek::ed25519::signature::{self};
use rand::rngs::OsRng;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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
        self.nonce_generator_validator.validate(nonce)?;
        let mut nonce_vec = Vec::with_capacity(12);
        nonce_vec.extend_from_slice(&nonce.to_le_bytes());
        nonce_vec.extend_from_slice(&[0u8; 4]);
        self.cipher
            .decrypt(nonce_vec.as_slice().into(), ciphertext)
            .map_err(|_e| DecryptionError::DecryptionFailure)
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
    /// dilithium private key for quantum safe signing. They are huge, so
    /// we put them on the heap
    pub dilithium: Box<pqcrypto_dilithium::dilithium5::SecretKey>,
    /// ed25519 private key for signing
    pub ed25519: ed25519_dalek::SigningKey,
}
impl std::fmt::Debug for ContactPrivateKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ContactPrivateKeys(dilithium: -mööp-no-debu-impl-, ed25519: {:?})",
            self.ed25519.to_bytes()
        )
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ContactPublicKeys {
    /// dilithium public key for quantum safe signature verification. They are huge, so
    /// we put them on the heap
    pub dilithium: Box<pqcrypto_dilithium::dilithium5::PublicKey>,
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
}

impl Default for ContactKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl ContactKeys {
    pub fn new() -> Self {
        let mut rng = OsRng;
        let (dilithium_public, dilithium_secret) = pqcrypto_dilithium::dilithium5_keypair();
        let ed25519_secret = ed25519_dalek::SigningKey::generate(&mut rng);
        let ed25519_public = ed25519_secret.verifying_key();
        Self {
            private: ContactPrivateKeys {
                dilithium: Box::new(dilithium_secret),
                ed25519: ed25519_secret,
            },
            public: ContactPublicKeys {
                dilithium: Box::new(dilithium_public),
                ed25519: ed25519_public,
            },
        }
    }
}

impl Debug for ContactPublicKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ContactPublicKeys(dilithium: -dilithium-no-debug-, ed25519: {:?})",
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

pub fn deserialize_dilithium_public_key(
    key: Vec<u8>,
) -> Result<pqcrypto_dilithium::dilithium5::PublicKey, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice::<pqcrypto_dilithium::dilithium5::PublicKey, _>(
        &key,
        BINCODE_CONFIG,
    )
    .map(|(x, _)| x)
}

pub fn deserialize_dilithium_private_key(
    key: Vec<u8>,
) -> Result<pqcrypto_dilithium::dilithium5::SecretKey, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice::<pqcrypto_dilithium::dilithium5::SecretKey, _>(
        &key,
        BINCODE_CONFIG,
    )
    .map(|(x, _)| x)
}

pub fn serialize_ed25519_public_key(key: &ed25519_dalek::VerifyingKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

pub fn serialize_ed25519_private_key(key: &ed25519_dalek::SigningKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

pub fn serialize_dilithium_public_key(
    key: &pqcrypto_dilithium::dilithium5::PublicKey,
) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(key, BINCODE_CONFIG)
}

pub fn serialize_dilithium_private_key(
    key: &pqcrypto_dilithium::dilithium5::SecretKey,
) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(key, BINCODE_CONFIG)
}
