use fyles_core::library::util::duration_ext::DurationExt;
use tokio::time::timeout;
use tracing::{Instrument, Level, span};

use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::PeerIdWrapper;

use crate::types::Wrap;

use super::test_utils::*;

/// How the inviter's identity is delivered to the joiner. Both mechanisms must drive the same
/// downstream convergence; only the exchanged messages differ.
enum PairingMethod {
    /// Over-the-network self-contact invite challenge.
    NetworkChallenge,
    /// Out-of-band invite blob (as a QR would carry): `GetFullSelfContact` → `UpdateIdentity`.
    QrInvite,
}

/// Pairing auto-registers both devices in each other's owned-device list and the joiner syncs the
/// inviter's state — regardless of *how* the invite is delivered. Only the pairing step differs
/// between methods; the convergence it drives is identical.
async fn pair_and_assert_convergence(method: PairingMethod) {
    let mut owner = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Owner setup"))
        .await;
    let mut joiner = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Joiner setup"))
        .await;

    owner.listen_external().await;
    joiner.connect(&mut owner).await;

    let owner = owner.run().instrument(span!(Level::INFO, "Owner")).await;
    let joiner = joiner.run().instrument(span!(Level::INFO, "Joiner")).await;

    // The owner has a contact before anyone pairs.
    let contact = owner.register_new_contact().await;

    // Deliver the invite by the method under test — the only step that differs.
    match method {
        PairingMethod::NetworkChallenge => {
            // The owner registers an invite challenge; the joiner uses it over the network.
            let (request, response) = BrainRequest::with_receiver(());
            owner
                .act(ClientAction::RegisterSelfContactInviteChallenge(request))
                .await;
            let challenge = response.await.expect("challenge registered");

            let (request, response) =
                BrainRequest::with_receiver((challenge, owner.peer_id.wrap()));
            joiner
                .act(ClientAction::UseSelfContactInviteChallenge(request))
                .await;
            response.await.expect("use invite prepared");
        }
        PairingMethod::QrInvite => {
            // The owner produces its shareable invite (as a QR would); the joiner adopts it. The
            // invite carries the owner's peer id + device name, so adopting it registers the owner
            // as an owned device and kicks off the sync — no network challenge involved.
            let (request, response) = BrainRequest::with_receiver(());
            owner.act(ClientAction::GetFullSelfContact(request)).await;
            let invite = response
                .await
                .expect("invite generated")
                .expect("invite ok");

            let (request, response) = BrainRequest::with_receiver(invite);
            joiner.act(ClientAction::UpdateIdentity(request)).await;
            response
                .await
                .expect("adopt invite prepared")
                .expect("adopt invite ok");
        }
    }

    // After pairing, the joiner auto-syncs and converges the owner's contact.
    let converged = timeout(5.seconds(), async {
        loop {
            let has = joiner
                .db
                .get_contacts()
                .await
                .unwrap()
                .iter()
                .any(|c| c.id == contact.id());
            if has {
                break;
            }
            tokio::time::sleep(50.millis()).await;
        }
    })
    .await;
    converged.expect("joiner should auto-sync the owner's contact after pairing");

    // The contact's public keys are *not* carried by the sync protocol; the joiner learns
    // only a fingerprint and then fetches the bytes on demand via `/fyles/pubkey-fetch`,
    // verifies them against the fingerprint, and caches them — all asynchronously after the
    // contacts document converges.
    let keys_arrived = timeout(5.seconds(), async {
        loop {
            if joiner
                .db
                .get_contact_public_keys(contact.id())
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(50.millis()).await;
        }
    })
    .await;
    keys_arrived.expect("joiner should fetch the contact's keys on demand after sync");

    // Both devices record each other as owned devices. The devices document converges over the
    // same asynchronous sync as the contacts document, so poll rather than checking once.
    let list = async |h: &RunningP2pTestHarness| {
        let (request, response) = BrainRequest::with_receiver(());
        h.act(ClientAction::ListOwnedDevices(request)).await;
        response.await.unwrap().unwrap()
    };
    let lists_peer = async |h: &RunningP2pTestHarness, wanted: PeerIdWrapper| {
        timeout(5.seconds(), async {
            loop {
                if list(h).await.iter().any(|(p, _)| *p == wanted) {
                    return;
                }
                tokio::time::sleep(50.millis()).await;
            }
        })
        .await
        .is_ok()
    };
    assert!(
        lists_peer(&joiner, owner.peer_id.wrap()).await,
        "joiner should list the owner as an owned device, got {:?}",
        list(&joiner).await
    );
    assert!(
        lists_peer(&owner, joiner.peer_id.wrap()).await,
        "owner should list the joiner as an owned device, got {:?}",
        list(&owner).await
    );
}

/// Pairing over the real network self-contact-invite flow, driven entirely by the
/// connection/pairing signals with no client-issued sync trigger.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn network_pairing_auto_registers_and_syncs() {
    pair_and_assert_convergence(PairingMethod::NetworkChallenge).await;
}

/// The same convergence, but the identity is delivered out of band via the invite blob (as a QR
/// would): adopting it registers the inviter as an owned device and syncs.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn qr_invite_auto_registers_and_syncs() {
    pair_and_assert_convergence(PairingMethod::QrInvite).await;
}

/// When an owned device is registered and (re)connects, the brain auto-syncs with it —
/// no manual trigger needed. We drive the connection signal directly via `PeerConnected`.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn owned_device_auto_syncs_on_connect() {
    let mut owner_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Owner setup"))
        .await;
    let mut sender_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Sender setup"))
        .await;

    owner_harness.listen_external().await;
    sender_harness.connect(&mut owner_harness).await;

    let owner_harness = owner_harness
        .run()
        .instrument(span!(Level::INFO, "Owner"))
        .await;
    let sender_harness = sender_harness
        .run()
        .instrument(span!(Level::INFO, "Sender"))
        .await;

    // Owned devices: adopt the same identity.
    let (request, response) = BrainRequest::with_receiver(());
    owner_harness
        .act(ClientAction::GetFullSelfContact(request))
        .await;
    let invite = response.await.unwrap().unwrap();
    let (request, response) = BrainRequest::with_receiver(invite);
    sender_harness
        .act(ClientAction::UpdateIdentity(request))
        .await;
    response.await.unwrap().unwrap();

    // The sender knows the owner as one of its own devices (normally learned via sync;
    // seeded directly here to test the connect trigger in isolation).
    let _ = fyles_core::core::sync::add_or_update_owned_device(
        &*sender_harness.db,
        owner_harness.peer_id.wrap(),
        "Owner".to_string(),
    )
    .await
    .unwrap();

    // A contact exists only on the sender.
    let contact = sender_harness.register_new_contact().await;

    // Simulate the owner connecting to the sender: the sender should auto-sync.
    sender_harness
        .brain_sender
        .send(BrainAction::NetworkNode(
            NetworkNodeAction::SyncOnPeerConnect(owner_harness.peer_id.wrap()),
        ))
        .await
        .unwrap();

    let converged = timeout(5.seconds(), async {
        loop {
            let has = owner_harness
                .db
                .get_contacts()
                .await
                .unwrap()
                .iter()
                .any(|c| c.id == contact.id());
            if has {
                break;
            }
            tokio::time::sleep(50.millis()).await;
        }
    })
    .await;

    converged.expect("Owner should auto-receive the synced contact on connect");

    // Dirty-tracking re-syncs on a *subsequent* change: the owner now holds contact1 (persisted
    // only at convergence, so the first exchange has finished), so registering another contact
    // and signalling one more connection converges too.
    let contact2 = sender_harness.register_new_contact().await;
    sender_harness
        .brain_sender
        .send(BrainAction::NetworkNode(
            NetworkNodeAction::SyncOnPeerConnect(owner_harness.peer_id.wrap()),
        ))
        .await
        .unwrap();

    let converged2 = timeout(5.seconds(), async {
        loop {
            let has = owner_harness
                .db
                .get_contacts()
                .await
                .unwrap()
                .iter()
                .any(|c| c.id == contact2.id());
            if has {
                break;
            }
            tokio::time::sleep(50.millis()).await;
        }
    })
    .await;
    converged2.expect("A later change should re-sync (dirty-tracking) and converge");
}
