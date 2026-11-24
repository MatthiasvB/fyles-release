use std::time::{SystemTime, UNIX_EPOCH};

use license_format::{
    LicenseQuery, LicenseQueryResponse, PROTOCOL_VERSION, PublicLicenseIdentity,
    SignedLicenseQuery, encode_identity, trusted_server_keys,
};
use tracing::{debug, error, warn};

use crate::core::brain::{BrainClientPushSender, BrainDb};
use crate::core::db::DbError;
use crate::library::wire::api::{EntitlementsChanged, ServerMessage, server_message};

pub mod cache;
pub mod evaluate;

use cache::CachedLicenseState;
use evaluate::Entitlements;

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time moves forward")
        .as_secs()
}

/// A request the core has signed for the frontend to POST on its behalf.
#[derive(Debug, Clone)]
pub struct PreparedLicenseRequest {
    pub body: Vec<u8>,
}

pub async fn current_entitlements(db: &BrainDb) -> Entitlements {
    let state = load_state(db).await;
    let contact_id = match db.get_self_contact().await {
        Ok(self_contact) => Some(self_contact.id()),
        Err(error) => {
            warn!(?error, "no self contact while evaluating entitlements");
            None
        }
    };
    evaluate::evaluate(
        &state,
        contact_id.as_ref(),
        &trusted_server_keys(),
        unix_now(),
    )
}

pub async fn export_identity(db: &BrainDb) -> Result<String, DbError> {
    let self_contact = db.get_self_contact().await?;
    encode_identity(&PublicLicenseIdentity {
        keys: self_contact.keys.public,
    })
    .map_err(|error| DbError::DataConversion {
        message: error.to_string(),
    })
}

/// Gate the schedule and, when a check is due, hand the frontend a signed request to
/// POST.
pub async fn prepare_check(db: &BrainDb, force: bool) -> Option<PreparedLicenseRequest> {
    let state = load_state(db).await;
    if !force && !state.recheck_is_due(unix_now()) {
        return None;
    }
    let self_contact = match db.get_self_contact().await {
        Ok(contact) => contact,
        Err(error) => {
            debug!(?error, "no self contact; skipping license check");
            return None;
        }
    };
    let mut keys = self_contact.keys;
    let query = LicenseQuery {
        version: PROTOCOL_VERSION,
        identity_keys: keys.public.clone(),
        cached_cert: state.cert,
    };
    let signed = match SignedLicenseQuery::sign(&query, &mut keys.private) {
        Ok(signed) => signed,
        Err(error) => {
            error!(?error, "failed to sign license query");
            return None;
        }
    };
    match signed.to_body() {
        Ok(body) => Some(PreparedLicenseRequest { body }),
        Err(error) => {
            error!(?error, "failed to serialize license query");
            None
        }
    }
}

/// A transport failure keeps the cached state untouched.
pub async fn submit_response(
    db: &BrainDb,
    client_push_sender: &BrainClientPushSender,
    http_status: u16,
    body: &[u8],
) -> Entitlements {
    if !(200..300).contains(&http_status) {
        debug!(
            http_status,
            "license check returned non-2xx; keeping cached state"
        );
        return current_entitlements(db).await;
    }

    let Ok(response) = LicenseQueryResponse::decode(body) else {
        debug!("license response undecodable; keeping cached state");
        return current_entitlements(db).await;
    };

    let now = unix_now();
    let mut state = load_state(db).await;
    let Ok(self_contact) = db.get_self_contact().await else {
        return current_entitlements(db).await;
    };
    let trusted = trusted_server_keys();
    let contact_id = self_contact.id();
    let before = evaluate::evaluate(&state, Some(&contact_id), &trusted, now);
    match response {
        LicenseQueryResponse::License(cert) => {
            state.cert = Some(*cert);
            state.refreshed_at = Some(now);
            state.last_definitive_no = None;
        }
        LicenseQueryResponse::NoLicense => {
            state.cert = None;
            state.refreshed_at = Some(now);
            state.last_definitive_no = Some(now);
        }
    }
    store_state(db, &state).await;
    let after = evaluate::evaluate(&state, Some(&contact_id), &trusted, now);
    if after != before {
        push_entitlements_message(client_push_sender, after.clone()).await;
    }
    after
}

pub async fn push_entitlements(db: &BrainDb, client_push_sender: &BrainClientPushSender) {
    let entitlements = current_entitlements(db).await;
    push_entitlements_message(client_push_sender, entitlements).await;
}

async fn push_entitlements_message(sender: &BrainClientPushSender, entitlements: Entitlements) {
    let _ = sender
        .send(ServerMessage {
            message: Some(server_message::Message::EntitlementsChanged(
                EntitlementsChanged::from(entitlements),
            )),
        })
        .await;
}

async fn load_state(db: &BrainDb) -> CachedLicenseState {
    match db.get_license_cache().await {
        Ok(Some(blob)) => cache::decode(&blob).unwrap_or_default(),
        Ok(None) => CachedLicenseState::default(),
        Err(error) => {
            error!(?error, "failed to load license cache");
            CachedLicenseState::default()
        }
    }
}

async fn store_state(db: &BrainDb, state: &CachedLicenseState) {
    match cache::encode(state) {
        Ok(blob) => {
            if let Err(error) = db.store_license_cache(blob).await {
                error!(?error, "failed to store license cache");
            }
        }
        Err(error) => error!(?error, "failed to encode license cache"),
    }
}
