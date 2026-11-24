use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use crypto::{BINCODE_CONFIG, ContactPublicKeys};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PublicLicenseIdentity {
    pub keys: ContactPublicKeys,
}

pub const IDENTITY_PREFIX: &str = "FYLES-ID2:";
const CHECKSUM_LEN: usize = 4;

pub fn encode_identity(
    identity: &PublicLicenseIdentity,
) -> Result<String, bincode::error::EncodeError> {
    let mut bytes = bincode::serde::encode_to_vec(identity, BINCODE_CONFIG)?;
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum[..CHECKSUM_LEN]);
    Ok(format!(
        "{IDENTITY_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(bytes)
    ))
}

#[derive(Error, Debug, PartialEq)]
pub enum IdentityDecodeError {
    #[error("Not a Fyles license identity (missing `{IDENTITY_PREFIX}` prefix)")]
    MissingPrefix,
    #[error("License identity is not valid base64")]
    Base64,
    #[error("License identity failed its checksum — mistyped or truncated?")]
    Checksum,
    #[error("License identity payload is malformed")]
    Malformed,
}

pub fn decode_identity(text: &str) -> Result<PublicLicenseIdentity, IdentityDecodeError> {
    // Strips *interior* whitespace too: pasted identities wrap across lines in email/QR.
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let encoded = compact
        .strip_prefix(IDENTITY_PREFIX)
        .ok_or(IdentityDecodeError::MissingPrefix)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| IdentityDecodeError::Base64)?;
    if bytes.len() <= CHECKSUM_LEN {
        return Err(IdentityDecodeError::Checksum);
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - CHECKSUM_LEN);
    if Sha256::digest(payload)[..CHECKSUM_LEN] != *checksum {
        return Err(IdentityDecodeError::Checksum);
    }
    let (identity, _) = bincode::serde::decode_from_slice(payload, BINCODE_CONFIG)
        .map_err(|_| IdentityDecodeError::Malformed)?;
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::ContactKeys;

    fn identity() -> PublicLicenseIdentity {
        PublicLicenseIdentity {
            keys: ContactKeys::new().public,
        }
    }

    #[test]
    fn the_identity_round_trips_even_with_pasted_whitespace() {
        let original = identity();
        let mut encoded = encode_identity(&original).unwrap();
        encoded.insert(20, '\n');
        encoded.push(' ');

        assert_eq!(decode_identity(&encoded).unwrap(), original);
    }

    #[test]
    fn a_mangled_paste_is_rejected_not_misbound() {
        let encoded = encode_identity(&identity()).unwrap();

        let truncated = &encoded[..encoded.len() - 5];
        assert!(decode_identity(truncated).is_err());

        let mut flipped = encoded.clone().into_bytes();
        let mid = flipped.len() / 2;
        flipped[mid] = if flipped[mid] == b'A' { b'B' } else { b'A' };
        let flipped = String::from_utf8(flipped).unwrap();
        assert!(matches!(
            decode_identity(&flipped),
            Err(IdentityDecodeError::Checksum) | Err(IdentityDecodeError::Base64)
        ));

        assert_eq!(
            decode_identity("not an identity"),
            Err(IdentityDecodeError::MissingPrefix)
        );
    }
}
