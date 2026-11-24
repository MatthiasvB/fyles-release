use ed25519_dalek::ed25519::signature::SignerMut;
use ml_dsa::{EncodedSignature, MlDsa87, Signature, Signer, Verifier};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BINCODE_CONFIG, ContactPrivateKeys, ContactPublicKeys};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Signed<P> {
    /// Unauthenticated routing hint for selecting the verification key. Outside the signed
    /// bytes, so a wrong value only makes [`Self::verify`] fail.
    signed_by: String,
    payload: P,
    pq_signature: PqSignatureWrapper,
    ed25519_signature: ed25519_dalek::Signature,
}

#[derive(Error, Debug)]
pub enum SignError {
    #[error("failed to serialize payload for signing: {0}")]
    Serialize(#[from] bincode::error::EncodeError),
    #[error("ed25519 signing failed: {0}")]
    Ed25519(#[from] ed25519_dalek::ed25519::Error),
}

impl<P: Serialize> Signed<P> {
    fn canonical_bytes(payload: &P) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec(payload, BINCODE_CONFIG)
    }

    pub fn sign(
        signed_by: String,
        payload: P,
        signing_keys: &mut ContactPrivateKeys,
    ) -> Result<Self, SignError> {
        let bytes_to_sign = Self::canonical_bytes(&payload)?;
        let pq_signature = signing_keys.pq_signing.sign(&bytes_to_sign).into();
        let ed25519_signature = signing_keys.ed25519.try_sign(&bytes_to_sign)?;
        Ok(Self {
            signed_by,
            payload,
            pq_signature,
            ed25519_signature,
        })
    }

    pub fn claimed_signed_by(&self) -> &str {
        &self.signed_by
    }

    pub fn payload_unverified(&self) -> &P {
        &self.payload
    }

    pub fn into_payload_unverified(self) -> P {
        self.payload
    }

    pub fn verify(
        &self,
        contact_public: &ContactPublicKeys,
    ) -> Result<&P, SignatureVerificationError> {
        let bytes_to_verify = Self::canonical_bytes(&self.payload)?;
        contact_public
            .pq_signing
            .verify(&bytes_to_verify, &self.pq_signature.pq_signature)?;
        contact_public
            .ed25519
            .verify_strict(&bytes_to_verify, &self.ed25519_signature)?;
        Ok(&self.payload)
    }

    pub fn canonical_payload_bytes(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        Self::canonical_bytes(&self.payload)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<P> Signed<P> {
    pub fn tamper_payload_for_test(&mut self, mutate: impl FnOnce(&mut P)) {
        mutate(&mut self.payload);
    }
}

#[derive(Serialize, Deserialize, Clone)]
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
    #[error("failed to serialize payload for verification: {0}")]
    Serialize(#[from] bincode::error::EncodeError),
    #[error("Post-quantum signature verification failed: {0}")]
    PqSigning(#[from] ml_dsa::Error),
    #[error("ed25519 signature verification failed: {0}")]
    Ed25519(#[from] ed25519_dalek::ed25519::Error),
}

mod serde_pq_signature {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        sig: &Signature<MlDsa87>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(sig.encode().as_slice())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Signature<MlDsa87>, D::Error> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let encoded = EncodedSignature::<MlDsa87>::try_from(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom("invalid ML-DSA signature length"))?;
        Signature::decode(&encoded)
            .ok_or_else(|| serde::de::Error::custom("invalid ML-DSA signature"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContactKeys;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Payload {
        message: String,
        number: u64,
    }

    fn payload() -> Payload {
        Payload {
            message: "attested".into(),
            number: 42,
        }
    }

    #[test]
    fn a_signed_payload_survives_serialization_and_verifies() {
        let mut keys = ContactKeys::new();
        let signed = Signed::sign("contact-1".into(), payload(), &mut keys.private).unwrap();

        let bytes = bincode::serde::encode_to_vec(&signed, BINCODE_CONFIG).unwrap();
        let (roundtripped, _): (Signed<Payload>, _) =
            bincode::serde::decode_from_slice(&bytes, BINCODE_CONFIG).unwrap();

        assert_eq!(roundtripped.claimed_signed_by(), "contact-1");
        assert_eq!(roundtripped.verify(&keys.public).unwrap(), &payload());
    }

    #[test]
    fn a_foreign_key_does_not_verify() {
        let mut keys = ContactKeys::new();
        let signed = Signed::sign("contact-1".into(), payload(), &mut keys.private).unwrap();

        assert!(signed.verify(&ContactKeys::new().public).is_err());
    }

    #[test]
    fn a_tampered_payload_does_not_verify() {
        let mut keys = ContactKeys::new();
        let mut signed = Signed::sign("contact-1".into(), payload(), &mut keys.private).unwrap();
        signed.payload.number += 1;

        assert!(signed.verify(&keys.public).is_err());
    }

    #[test]
    fn signed_by_is_a_hint_that_does_not_affect_the_signature() {
        let mut keys = ContactKeys::new();
        let mut signed = Signed::sign("contact-1".into(), payload(), &mut keys.private).unwrap();
        signed.signed_by = "someone-else".into();
        assert_eq!(signed.verify(&keys.public).unwrap(), &payload());
    }
}
