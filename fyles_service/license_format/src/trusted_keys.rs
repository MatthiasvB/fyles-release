use crypto::ContactPublicKeys;

/// Server public keys clients accept license certificates from, indexed by
/// `LicenseCertificatePayload::signing_key_id`. Production keys are appended here, so
/// existing indices must never be reordered.
pub fn trusted_server_keys() -> Vec<ContactPublicKeys> {
    #[cfg(feature = "dev-keys")]
    {
        vec![dev_server_keys().public]
    }
    #[cfg(not(feature = "dev-keys"))]
    {
        vec![]
    }
}

/// Deterministic development keypair. Behind a feature so a production build cannot
/// silently trust it.
#[cfg(feature = "dev-keys")]
pub fn dev_server_keys() -> crypto::ContactKeys {
    use crypto::{
        ContactKeys, ContactPrivateKeys, deserialize_ed25519_private_key,
        deserialize_pq_signing_private_key,
    };

    let seed = vec![0x42; 32];
    let ed25519 = deserialize_ed25519_private_key(seed.clone()).expect("valid dev seed");
    let pq_signing = deserialize_pq_signing_private_key(seed).expect("valid dev seed");
    ContactKeys::from_private(ContactPrivateKeys {
        pq_signing: Box::new(pq_signing),
        ed25519,
    })
}

#[cfg(all(test, feature = "dev-keys"))]
mod tests {
    use super::*;

    #[test]
    fn the_dev_key_is_deterministic_and_trusted_at_index_zero() {
        assert_eq!(dev_server_keys().public, dev_server_keys().public);
        assert_eq!(trusted_server_keys()[0], dev_server_keys().public);
    }
}
