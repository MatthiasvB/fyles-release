use std::fmt::Display;
use std::str::FromStr;

use data_encoding::BASE32_NOPAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ContactKeys, ContactPublicKeys, serialize_ed25519_public_key, serialize_pq_signing_public_key,
};

pub const CONTACT_ID_PREFIX: &str = "fc1-";
const DIGEST_LEN: usize = 32;
const ENCODED_LEN: usize = (DIGEST_LEN * 8).div_ceil(5);

/// Derived from an identity's public keys, so every holder of those keys computes the same id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ContactId(String);

#[derive(Debug, Error, PartialEq)]
pub enum ContactIdParseError {
    #[error("not a contact id (missing `{CONTACT_ID_PREFIX}` prefix)")]
    MissingPrefix,
    #[error("contact id body must be {ENCODED_LEN} characters, got {got}")]
    Length { got: usize },
    #[error("contact id is not canonical base32")]
    Encoding,
}

impl ContactId {
    fn from_digest(digest: [u8; DIGEST_LEN]) -> Self {
        Self(format!(
            "{CONTACT_ID_PREFIX}{}",
            BASE32_NOPAD.encode(&digest)
        ))
    }
}

impl ContactPublicKeys {
    pub fn id(&self) -> ContactId {
        let mut hasher = Sha256::new();
        hasher.update(serialize_pq_signing_public_key(&self.pq_signing));
        hasher.update(serialize_ed25519_public_key(&self.ed25519));
        ContactId::from_digest(hasher.finalize().into())
    }
}

impl ContactKeys {
    pub fn id(&self) -> ContactId {
        self.public.id()
    }
}

impl FromStr for ContactId {
    type Err = ContactIdParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let body = text
            .strip_prefix(CONTACT_ID_PREFIX)
            .ok_or(ContactIdParseError::MissingPrefix)?;
        if body.len() != ENCODED_LEN {
            return Err(ContactIdParseError::Length { got: body.len() });
        }
        BASE32_NOPAD
            .decode(body.as_bytes())
            .map_err(|_| ContactIdParseError::Encoding)?;
        Ok(Self(text.to_string()))
    }
}

impl Display for ContactId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ContactId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ContactId {
    type Error = ContactIdParseError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<ContactId> for String {
    fn from(id: ContactId) -> Self {
        id.0
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ContactId {
    pub fn for_test(seed: u8) -> Self {
        Self::from_digest([seed; DIGEST_LEN])
    }

    pub fn for_test_labelled(label: &str) -> Self {
        let mut digest = [0u8; DIGEST_LEN];
        for (slot, byte) in digest
            .iter_mut()
            .zip(label.bytes().cycle().take(DIGEST_LEN))
        {
            *slot = byte;
        }
        Self::from_digest(digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_derived_id_round_trips_through_its_text_form() {
        let id = ContactKeys::new().id();

        assert_eq!(id.to_string().parse::<ContactId>().unwrap(), id);
    }

    #[test]
    fn only_one_spelling_of_a_digest_parses() {
        let id = ContactId::for_test(7);
        let body = &id.as_ref()[CONTACT_ID_PREFIX.len()..];

        assert_eq!(
            format!("{CONTACT_ID_PREFIX}{}", body.to_lowercase()).parse::<ContactId>(),
            Err(ContactIdParseError::Encoding),
            "lowercase must not be a second spelling of the same map key"
        );
        assert_eq!(
            format!("{CONTACT_ID_PREFIX}{body}=").parse::<ContactId>(),
            Err(ContactIdParseError::Length {
                got: ENCODED_LEN + 1
            }),
        );
    }

    #[test]
    fn malformed_ids_are_rejected() {
        assert_eq!(
            "0198c0de-0000-7000-8000-000000000001".parse::<ContactId>(),
            Err(ContactIdParseError::MissingPrefix),
        );
        assert_eq!(
            "".parse::<ContactId>(),
            Err(ContactIdParseError::MissingPrefix)
        );

        let bare = BASE32_NOPAD.encode(&[0u8; DIGEST_LEN]);
        assert_eq!(
            bare.parse::<ContactId>(),
            Err(ContactIdParseError::MissingPrefix)
        );

        assert_eq!(
            format!("{CONTACT_ID_PREFIX}AAAA").parse::<ContactId>(),
            Err(ContactIdParseError::Length { got: 4 }),
        );

        let mut body: Vec<u8> = bare.into_bytes();
        body[0] = b'1';
        assert_eq!(
            format!("{CONTACT_ID_PREFIX}{}", String::from_utf8(body).unwrap()).parse::<ContactId>(),
            Err(ContactIdParseError::Encoding),
        );
    }

    #[test]
    fn distinct_keys_give_distinct_ids() {
        assert_ne!(ContactKeys::new().id(), ContactKeys::new().id());
    }
}
