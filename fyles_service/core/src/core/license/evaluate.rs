use crypto::{ContactId, ContactPublicKeys};
use license_format::{Entitlement, PRO_ENTITLEMENT, verify_certificate};

use super::cache::CachedLicenseState;
use crate::constants::LICENSE_FREE_ANSWER_TTL;

#[derive(Debug, Clone, PartialEq)]
pub struct Entitlements {
    pub entitlements: Vec<Entitlement>,
    /// True when pro is granted only by fail-open, without a verified certificate.
    pub provisional: bool,
}

/// Pure evaluation of the cached state against the *current* identity's ContactId.
/// A certificate for any other identity (backup restore, someone else's activation)
/// simply does not match and grants nothing
pub fn evaluate(
    state: &CachedLicenseState,
    current_contact_id: Option<&ContactId>,
    trusted_license_server_keys: &[ContactPublicKeys],
    now: u64,
) -> Entitlements {
    if let (Some(cert), Some(contact_id)) = (&state.cert, current_contact_id)
        && let Ok(payload) = verify_certificate(cert, trusted_license_server_keys)
        && payload.contact_id == contact_id.as_ref()
    {
        return Entitlements {
            entitlements: payload.active_entitlements(now).cloned().collect(),
            provisional: false,
        };
    }
    if let Some(no_at) = state.last_definitive_no
        && now < no_at + LICENSE_FREE_ANSWER_TTL.as_secs()
    {
        return Entitlements {
            entitlements: vec![],
            provisional: false,
        };
    }
    Entitlements {
        entitlements: vec![Entitlement {
            id: PRO_ENTITLEMENT.into(),
            expires_at: None,
            vendor_txn_hash: None,
        }],
        provisional: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::ContactKeys;
    use license_format::{CERT_FORMAT_VERSION, LicenseCertificate, LicenseCertificatePayload};

    const NOW: u64 = 1_700_000_000;

    fn cert_for(server: &mut ContactKeys, contact_id: &str) -> CachedLicenseState {
        let payload = LicenseCertificatePayload {
            version: CERT_FORMAT_VERSION,
            contact_id: contact_id.into(),
            entitlements: vec![Entitlement {
                id: PRO_ENTITLEMENT.into(),
                expires_at: None,
                vendor_txn_hash: None,
            }],
            issued_at: NOW - 1000,
            serial: [1; 16],
        };
        CachedLicenseState {
            cert: Some(LicenseCertificate::issue(&payload, 0, &mut server.private).unwrap()),
            refreshed_at: Some(NOW - 1000),
            ..Default::default()
        }
    }

    #[test]
    fn a_verified_cert_for_the_current_identity_licenses_without_provisionality() {
        let mut server = ContactKeys::new();
        let state = cert_for(&mut server, ContactId::for_test(7).as_ref());

        let result = evaluate(
            &state,
            Some(&ContactId::for_test(7)),
            &[server.public.clone()],
            NOW,
        );
        assert!(!result.provisional);
        assert_eq!(result.entitlements[0].id, PRO_ENTITLEMENT);
    }

    #[test]
    fn a_cert_for_a_different_identity_grants_nothing_from_the_cert() {
        let mut server = ContactKeys::new();
        let state = cert_for(&mut server, "someone-elses-contact");

        // No definitive "no" cached either, so this falls through to provisional
        // fail-open — the cert itself must not license the restored identity.
        let result = evaluate(
            &state,
            Some(&ContactId::for_test(7)),
            &[server.public.clone()],
            NOW,
        );
        assert!(result.provisional);
    }

    #[test]
    fn a_fresh_definitive_no_means_free() {
        let state = CachedLicenseState {
            last_definitive_no: Some(NOW - 60),
            ..Default::default()
        };
        let result = evaluate(&state, None, &[], NOW);
        assert!(!result.provisional);
        assert!(result.entitlements.is_empty());
    }

    #[test]
    fn an_expired_definitive_no_falls_back_to_provisional_pro() {
        let state = CachedLicenseState {
            last_definitive_no: Some(NOW - LICENSE_FREE_ANSWER_TTL.as_secs() - 1),
            ..Default::default()
        };
        assert!(evaluate(&state, None, &[], NOW).provisional);
    }

    #[test]
    fn no_knowledge_at_all_is_provisional_pro() {
        assert!(evaluate(&CachedLicenseState::default(), None, &[], NOW).provisional);
    }
}
