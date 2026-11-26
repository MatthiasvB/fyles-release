use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::types::{BrainRequest, ContactShareChallenge};
use fyles_core::core::domain_models::{ContactId, CreateFilerequest, FilerequestAccess};
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

/// Redeems `sharer`'s code on `scanner`, which teaches only the scanner the sharer's contact.
async fn learn_contact_of(sharer: &RunningP2pTestHarness, scanner: &RunningP2pTestHarness) {
    let sharer_id = sharer
        .get_sharable_self_contact()
        .await
        .contact_unverified()
        .id();
    let challenge = issue_challenge(sharer).await;
    redeem(scanner, sharer, challenge).await;
    assert!(
        await_contact(scanner, &sharer_id).await,
        "redeeming the code should store the sharer as a contact"
    );
}

/// Sends a file to a private file request of `owner` that admits only `sender`.
async fn send_privately(sender: &RunningP2pTestHarness, owner: &RunningP2pTestHarness) {
    let (request, response) = BrainRequest::with_receiver(CreateFilerequest {
        title: "Private".into(),
        description: String::new(),
        access: FilerequestAccess::Audience {
            contact_ids: vec![sender.node_info.self_contact_keys.id()],
        },
        is_active: true,
    });
    owner.act(ClientAction::CreateFilerequest(request)).await;
    let id = response
        .await
        .expect("Sender not dropped")
        .expect("Creation successful");
    let remote = sender
        .create_remote_filerequest(owner, id.0, "Private")
        .await;
    sender
        .send_files_and_wait(remote, vec![10_000], 5.seconds(), 50.millis())
        .await
        .expect("the sender should reach the owner's private file request");
}

enum Exchange {
    /// Only the scanner redeems a code, so only it knows the other.
    OneWay,
    BothWays,
}

/// The codes are redeemed over a session set up before either side knew the other. The owner of a
/// private file request has to know the sender, while the sender need not know the owner.
async fn send_privately_after_redeeming(exchange: Exchange) {
    let (sharer, scanner) = connected_running_pair().await;

    learn_contact_of(&sharer, &scanner).await;
    if let Exchange::BothWays = exchange {
        learn_contact_of(&scanner, &sharer).await;
    }

    send_privately(&sharer, &scanner).await;
    if let Exchange::BothWays = exchange {
        send_privately(&scanner, &sharer).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn the_sharer_can_send_privately_to_whoever_redeemed_its_code() {
    send_privately_after_redeeming(Exchange::OneWay).await;
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn contacts_who_redeemed_each_others_codes_can_send_each_other_files_privately() {
    send_privately_after_redeeming(Exchange::BothWays).await;
}
