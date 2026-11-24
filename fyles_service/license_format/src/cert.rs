use crypto::signed::{SignError, SignatureVerificationError, Signed};
use crypto::{ContactPrivateKeys, ContactPublicKeys};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::wire::proto;

pub const CERT_FORMAT_VERSION: u16 = 1;
pub const PRO_ENTITLEMENT: &str = "pro";

#[derive(Debug, Clone, PartialEq)]
pub struct Entitlement {
    pub id: String,
    /// Unix seconds. `None` means the entitlement never expires (one-time purchase).
    pub expires_at: Option<u64>,
    /// SHA-256 of the payment provider's transaction id, when the entitlement stems from a
    /// purchase.
    pub vendor_txn_hash: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LicenseCertificatePayload {
    pub version: u16,
    pub contact_id: String,
    pub entitlements: Vec<Entitlement>,
    pub issued_at: u64,
    pub serial: [u8; 16],
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LicenseCertificate(Signed<Vec<u8>>);

impl LicenseCertificatePayload {
    pub fn active_entitlements(&self, now_unix_secs: u64) -> impl Iterator<Item = &Entitlement> {
        self.entitlements
            .iter()
            .filter(move |entitlement| entitlement.expires_at.is_none_or(|at| at > now_unix_secs))
    }
}

impl LicenseCertificate {
    pub fn issue(
        payload: &LicenseCertificatePayload,
        signing_key_id: u8,
        signing_keys: &mut ContactPrivateKeys,
    ) -> Result<Self, SignError> {
        let bytes = proto::LicenseCertificatePayload::from(payload).encode_to_vec();
        Ok(Self(Signed::sign(
            signing_key_id.to_string(),
            bytes,
            signing_keys,
        )?))
    }
}

pub fn vendor_txn_hash(transaction_id: &str) -> [u8; 32] {
    Sha256::digest(transaction_id.trim().as_bytes()).into()
}

#[derive(Error, Debug)]
pub enum CertificateVerificationError {
    #[error("Certificate names signing key {0}, which is not in the trusted-key list")]
    UnknownSigningKey(String),
    #[error("Certificate format version {version} is newer than this client understands")]
    UnsupportedVersion { version: u16 },
    #[error("Certificate payload is malformed")]
    Malformed,
    #[error(transparent)]
    Signature(#[from] SignatureVerificationError),
}

pub fn verify_certificate(
    cert: &LicenseCertificate,
    trusted_keys: &[ContactPublicKeys],
) -> Result<LicenseCertificatePayload, CertificateVerificationError> {
    let signer = cert.0.claimed_signed_by();
    let key_id: usize = signer
        .parse()
        .map_err(|_| CertificateVerificationError::UnknownSigningKey(signer.to_string()))?;
    let key = trusted_keys
        .get(key_id)
        .ok_or_else(|| CertificateVerificationError::UnknownSigningKey(signer.to_string()))?;
    let bytes = cert.0.verify(key)?;
    let payload = LicenseCertificatePayload::try_from(
        proto::LicenseCertificatePayload::decode(bytes.as_slice())
            .map_err(|_| CertificateVerificationError::Malformed)?,
    )?;
    if payload.version > CERT_FORMAT_VERSION {
        return Err(CertificateVerificationError::UnsupportedVersion {
            version: payload.version,
        });
    }
    Ok(payload)
}

impl From<&Entitlement> for proto::Entitlement {
    fn from(entitlement: &Entitlement) -> Self {
        proto::Entitlement {
            id: entitlement.id.clone(),
            expires_at_unix_sec: entitlement.expires_at,
            vendor_txn_hash: entitlement.vendor_txn_hash.map(|hash| hash.to_vec()),
        }
    }
}

impl TryFrom<proto::Entitlement> for Entitlement {
    type Error = CertificateVerificationError;

    fn try_from(entitlement: proto::Entitlement) -> Result<Self, Self::Error> {
        Ok(Entitlement {
            id: entitlement.id,
            expires_at: entitlement.expires_at_unix_sec,
            vendor_txn_hash: entitlement
                .vendor_txn_hash
                .map(|bytes| {
                    bytes
                        .try_into()
                        .map_err(|_| CertificateVerificationError::Malformed)
                })
                .transpose()?,
        })
    }
}

impl From<&LicenseCertificatePayload> for proto::LicenseCertificatePayload {
    fn from(payload: &LicenseCertificatePayload) -> Self {
        proto::LicenseCertificatePayload {
            version: payload.version as u32,
            contact_id: payload.contact_id.clone(),
            entitlements: payload
                .entitlements
                .iter()
                .map(proto::Entitlement::from)
                .collect(),
            issued_at: payload.issued_at,
            serial: payload.serial.to_vec(),
        }
    }
}

impl TryFrom<proto::LicenseCertificatePayload> for LicenseCertificatePayload {
    type Error = CertificateVerificationError;

    fn try_from(proto: proto::LicenseCertificatePayload) -> Result<Self, Self::Error> {
        Ok(LicenseCertificatePayload {
            version: proto.version as u16,
            contact_id: proto.contact_id,
            entitlements: proto
                .entitlements
                .into_iter()
                .map(Entitlement::try_from)
                .collect::<Result<_, _>>()?,
            issued_at: proto.issued_at,
            serial: proto
                .serial
                .try_into()
                .map_err(|_| CertificateVerificationError::Malformed)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::ContactKeys;

    fn issued_cert(server_keys: &mut ContactKeys, contact_id: &str) -> LicenseCertificate {
        let payload = LicenseCertificatePayload {
            version: CERT_FORMAT_VERSION,
            contact_id: contact_id.to_string(),
            entitlements: vec![
                Entitlement {
                    id: PRO_ENTITLEMENT.into(),
                    expires_at: None,
                    vendor_txn_hash: Some(vendor_txn_hash("txn_123")),
                },
                Entitlement {
                    id: "expired-extension".into(),
                    expires_at: Some(1_000),
                    vendor_txn_hash: None,
                },
            ],
            issued_at: 500,
            serial: [7; 16],
        };
        LicenseCertificate::issue(&payload, 0, &mut server_keys.private).unwrap()
    }

    #[test]
    fn a_cert_verifies_against_its_trusted_key_and_only_that() {
        let mut server_keys = ContactKeys::new();
        let cert = issued_cert(&mut server_keys, "contact-1");

        assert!(verify_certificate(&cert, &[server_keys.public.clone()]).is_ok());
        assert!(matches!(
            verify_certificate(&cert, &[ContactKeys::new().public]),
            Err(CertificateVerificationError::Signature(_))
        ));
        assert!(matches!(
            verify_certificate(&cert, &[]),
            Err(CertificateVerificationError::UnknownSigningKey(_))
        ));
    }

    #[test]
    fn expired_entitlements_are_filtered_out() {
        let mut server_keys = ContactKeys::new();
        let cert = issued_cert(&mut server_keys, "contact-1");
        let payload = verify_certificate(&cert, &[server_keys.public.clone()]).unwrap();

        let active: Vec<_> = payload
            .active_entitlements(2_000)
            .map(|entitlement| entitlement.id.as_str())
            .collect();
        assert_eq!(active, vec![PRO_ENTITLEMENT]);
    }

    #[test]
    fn the_per_entitlement_txn_hash_survives_the_round_trip() {
        let mut server_keys = ContactKeys::new();
        let cert = issued_cert(&mut server_keys, "contact-1");
        let payload = verify_certificate(&cert, &[server_keys.public.clone()]).unwrap();

        assert_eq!(
            payload.entitlements[0].vendor_txn_hash,
            Some(vendor_txn_hash("txn_123"))
        );
        assert_eq!(payload.entitlements[1].vendor_txn_hash, None);
    }

    #[test]
    fn the_txn_hash_normalizes_whitespace_but_nothing_else() {
        assert_eq!(vendor_txn_hash("txn_1 "), vendor_txn_hash(" txn_1"));
        assert_ne!(vendor_txn_hash("txn_1"), vendor_txn_hash("txn_2"));
    }
}
