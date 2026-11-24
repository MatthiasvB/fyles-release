use crypto::signed::{SignError, SignatureVerificationError, Signed};
use crypto::{BINCODE_CONFIG, ContactPrivateKeys, ContactPublicKeys};
use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::cert::LicenseCertificate;
use crate::wire::proto;

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct LicenseQuery {
    pub version: u16,
    /// The keys the server verifies against for trust-on-first-use and post-disaster
    /// recovery. They are ignored once it holds keys-of-record for the ContactId.
    pub identity_keys: ContactPublicKeys,
    /// A previously issued certificate, if cached. Presenting it re-creates the license
    /// row after a server data loss (self-healing recovery).
    pub cached_cert: Option<LicenseCertificate>,
}

#[derive(Debug, Clone)]
pub enum LicenseQueryResponse {
    /// Re-signed fresh on every query, so refreshes pick up revocations and entitlement
    /// changes without a separate mechanism.
    License(Box<LicenseCertificate>),
    /// The definitive "no license for this identity" answer, cached client-side. Also
    /// covers revoked and denylisted identities.
    NoLicense,
}

#[derive(Error, Debug)]
pub enum WireDecodeError {
    #[error("license wire envelope is malformed")]
    Envelope,
    #[error("license wire payload is malformed")]
    Payload,
}

#[derive(Debug, Clone)]
pub struct SignedLicenseQuery(Signed<Vec<u8>>);

impl SignedLicenseQuery {
    pub fn sign(
        query: &LicenseQuery,
        signing_keys: &mut ContactPrivateKeys,
    ) -> Result<Self, SignError> {
        let bytes = proto::LicenseQuery::try_from(query)?.encode_to_vec();
        Ok(Self(Signed::sign(
            query.identity_keys.id().to_string(),
            bytes,
            signing_keys,
        )?))
    }

    pub fn to_body(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode_encode(&self.0)
    }

    pub fn from_body(body: &[u8]) -> Result<Self, WireDecodeError> {
        Ok(Self(bincode_decode_envelope(body)?))
    }

    pub fn unverified_contact_id(&self) -> &str {
        self.0.claimed_signed_by()
    }

    pub fn decode_unverified(&self) -> Result<LicenseQuery, WireDecodeError> {
        LicenseQuery::try_from(decode_proto::<proto::LicenseQuery>(
            self.0.payload_unverified(),
        )?)
    }

    pub fn verify(&self, keys: &ContactPublicKeys) -> Result<(), SignatureVerificationError> {
        self.0.verify(keys).map(|_| ())
    }
}

impl LicenseQueryResponse {
    pub fn encode(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        use proto::license_query_response::Result as ProtoResult;
        let result = match self {
            LicenseQueryResponse::License(cert) => ProtoResult::License(bincode_encode(cert)?),
            LicenseQueryResponse::NoLicense => ProtoResult::NoLicense(proto::NoLicense {}),
        };
        Ok(proto::LicenseQueryResponse {
            result: Some(result),
        }
        .encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WireDecodeError> {
        use proto::license_query_response::Result as ProtoResult;
        let proto =
            proto::LicenseQueryResponse::decode(bytes).map_err(|_| WireDecodeError::Payload)?;
        match proto.result {
            Some(ProtoResult::License(cert)) => {
                Ok(LicenseQueryResponse::License(bincode_decode(&cert)?))
            }
            _ => Ok(LicenseQueryResponse::NoLicense),
        }
    }
}

impl TryFrom<&LicenseQuery> for proto::LicenseQuery {
    type Error = bincode::error::EncodeError;

    fn try_from(query: &LicenseQuery) -> Result<Self, Self::Error> {
        Ok(proto::LicenseQuery {
            version: query.version as u32,
            identity_keys: bincode_encode(&query.identity_keys)?,
            cached_cert: query.cached_cert.as_ref().map(bincode_encode).transpose()?,
        })
    }
}

impl TryFrom<proto::LicenseQuery> for LicenseQuery {
    type Error = WireDecodeError;

    fn try_from(proto: proto::LicenseQuery) -> Result<Self, Self::Error> {
        Ok(LicenseQuery {
            version: proto.version as u16,
            identity_keys: bincode_decode(&proto.identity_keys)?,
            cached_cert: proto
                .cached_cert
                .map(|bytes| bincode_decode(&bytes))
                .transpose()?,
        })
    }
}

fn decode_proto<M: Message + Default>(bytes: &[u8]) -> Result<M, WireDecodeError> {
    M::decode(bytes).map_err(|_| WireDecodeError::Payload)
}

fn bincode_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, BINCODE_CONFIG)
}

fn bincode_decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireDecodeError> {
    bincode::serde::decode_from_slice(bytes, BINCODE_CONFIG)
        .map(|(value, _)| value)
        .map_err(|_| WireDecodeError::Payload)
}

fn bincode_decode_envelope(body: &[u8]) -> Result<Signed<Vec<u8>>, WireDecodeError> {
    bincode::serde::decode_from_slice(body, BINCODE_CONFIG)
        .map(|(value, _)| value)
        .map_err(|_| WireDecodeError::Envelope)
}
