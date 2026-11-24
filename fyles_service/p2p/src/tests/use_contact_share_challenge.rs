use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::types::{BrainRequest, ContactShareChallenge};
use fyles_core::core::domain_models::ContactId;
use fyles_core::library::util::duration_ext::DurationExt;
use tokio::time::timeout;

use super::test_utils::*;
use crate::types::Wrap;

async fn issue_challenge(sharer: &RunningP2pTestHarness) -> ContactShareChallenge {
    let (request, response) = BrainRequest::with_receiver(());
    sharer
        .act(ClientAction::RegisterContactShareChallenge(request))
        .await;
    response.await.expect("issuing a challenge to work")
}

async fn redeem(
    scanner: &RunningP2pTestHarness,
    sharer: &RunningP2pTestHarness,
    challenge: ContactShareChallenge,
) {
    let (request, response) = BrainRequest::with_receiver((challenge, sharer.peer_id.wrap()));
    scanner
        .act(ClientAction::UseContactShareChallenge(request))
        .await;
    response.await.expect("redeeming a challenge to work");
}

async fn await_contact(harness: &RunningP2pTestHarness, id: &ContactId) -> bool {
    timeout(5.seconds(), async {
        loop {
            let has = harness
                .db
                .get_contacts()
                .await
                .unwrap()
                .iter()
                .any(|c| c.id == *id);
            if has {
                return;
            }
            tokio::time::sleep(50.millis()).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn redeeming_a_share_code_stores_the_sharer_as_a_contact() {
    let (sharer, scanner) = connected_running_pair().await;
    let sharer_id = sharer
        .get_sharable_self_contact()
        .await
        .contact_unverified()
        .id();

    let challenge = issue_challenge(&sharer).await;
    redeem(&scanner, &sharer, challenge).await;

    assert!(
        await_contact(&scanner, &sharer_id).await,
        "redeeming the sharer's code should store the sharer as a contact"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_share_code_stores_nothing() {
    let (sharer, scanner) = connected_running_pair().await;
    let sharer_id = sharer
        .get_sharable_self_contact()
        .await
        .contact_unverified()
        .id();

    issue_challenge(&sharer).await;
    redeem(&scanner, &sharer, ContactShareChallenge(vec![0xab; 32])).await;

    assert!(
        !await_contact(&scanner, &sharer_id).await,
        "the sharer must not answer a code it never issued"
    );
}
