use core::panic;
use fyles_core::library::util::duration_ext::DurationExt;
use tokio::time::timeout;
use tracing::{span, trace, Instrument, Level};

use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;

use crate::types::Wrap;

use super::test_utils::*;

async fn use_self_contact_invite_challenge(start_with_incorrect_challenge: bool) {
    let mut owned_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Owned setup"))
        .await;
    let mut new_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "New setup"))
        .await;

    let mut secondary_new_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Secondary New setup"))
        .await;

    owned_harness.listen_external().await;
    new_harness.connect(&mut owned_harness).await;
    secondary_new_harness.connect(&mut owned_harness).await;

    let owner_harness = owned_harness
        .run()
        .instrument(span!(Level::INFO, "Owned"))
        .await;
    let new_harness = new_harness
        .run()
        .instrument(span!(Level::INFO, "New"))
        .await;
    let secondary_new_harness = secondary_new_harness
        .run()
        .instrument(span!(Level::INFO, "Secondary New"))
        .await;

    let (request, response) = BrainRequest::with_receiver(());
    owner_harness
        .act(ClientAction::GetSelfContactDisplay(request))
        .await;
    let owned_contact = response
        .await
        .expect("Getting self contact to work")
        .expect("Getting self contact to succeed");

    let (request, response) = BrainRequest::with_receiver(());
    new_harness
        .act(ClientAction::GetSelfContactDisplay(request))
        .await;
    let new_contact = response
        .await
        .expect("Getting self contact to work")
        .expect("Getting self contact to succeed");

    assert_ne!(new_contact.id, owned_contact.id, "Contact IDs not to match");
    assert_ne!(
        new_contact.name, owned_contact.name,
        "Contact names not to match"
    );

    let (request, response) = BrainRequest::with_receiver(());
    owner_harness
        .act(ClientAction::RegisterSelfContactInviteChallenge(request))
        .await;
    let invite_challenge = response
        .await
        .expect("Registering invite challenge to work");

    let incorrect_challenge = {
        let mut incorrect_challenge = invite_challenge.clone();
        incorrect_challenge[0] ^= 0xFF;
        incorrect_challenge
    };

    let (first_challenge, second_challenge) = if start_with_incorrect_challenge {
        (incorrect_challenge, invite_challenge)
    } else {
        (invite_challenge.clone(), invite_challenge)
    };

    let (request, response) =
        BrainRequest::with_receiver((first_challenge, owner_harness.peer_id.clone().wrap()));
    new_harness
        .act(ClientAction::UseSelfContactInviteChallenge(request))
        .await;
    response.await.expect("Preparing to use invite to work");

    timeout(
        5.seconds(),
        new_harness.intercept_actions(async |action| match action {
            BrainAction::NetworkNode(NetworkNodeAction::UpdateIdentity { .. }) => {
                if start_with_incorrect_challenge {
                    panic!("Did not expect identity to be updated");
                }
                trace!("Breaking new loop because identity was updated successfully");
                InterceptionRes::Exit(Some(action))
            }
            BrainAction::NetworkNode(NetworkNodeAction::SelfContactInviteGotRejected) => {
                if !start_with_incorrect_challenge {
                    panic!("Did not expect invite to be rejected");
                }
                trace!("Breaking new loop because invite was rejected as expected");
                InterceptionRes::Exit(Some(action))
            }
            a => InterceptionRes::Continue(Some(a)),
        }),
    )
    .await
    .expect("Timeout waiting for identity update");

    let (request, response) = BrainRequest::with_receiver(());
    new_harness
        .act(ClientAction::GetSelfContactDisplay(request))
        .await;
    let updated_contact = response
        .await
        .expect("Getting self contact to work")
        .expect("Getting self contact to succeed");

    if start_with_incorrect_challenge {
        assert_ne!(
            updated_contact.id, owned_contact.id,
            "Contact IDs not to match"
        );
        assert_ne!(
            updated_contact.name, owned_contact.name,
            "Contact names not to match"
        );
    } else {
        assert_eq!(updated_contact.id, owned_contact.id, "Contact IDs to match");
        assert_eq!(
            updated_contact.name, owned_contact.name,
            "Contact names to match"
        );
    }

    let (request, response) =
        BrainRequest::with_receiver((second_challenge, owner_harness.peer_id.clone().wrap()));

    secondary_new_harness
        .act(ClientAction::UseSelfContactInviteChallenge(request))
        .await;
    response.await.expect("Preparing to use invite to work");

    timeout(
        5.seconds(),
        secondary_new_harness.intercept_actions(async |action| match action {
            BrainAction::NetworkNode(NetworkNodeAction::UpdateIdentity { .. }) => {
                if !start_with_incorrect_challenge {
                    panic!("Did not expect identity to be updated");
                }
                trace!("Breaking new loop because identity was updated successfully");
                InterceptionRes::Exit(Some(action))
            }
            BrainAction::NetworkNode(NetworkNodeAction::SelfContactInviteGotRejected) => {
                if start_with_incorrect_challenge {
                    panic!("Did not expect invite to be rejected");
                }
                trace!("Breaking new loop because invite was rejected as expected");
                InterceptionRes::Exit(Some(action))
            }
            a => InterceptionRes::Continue(Some(a)),
        }),
    )
    .await
    .expect("Timeout waiting for identity update");

    let (request, response) = BrainRequest::with_receiver(());
    secondary_new_harness
        .act(ClientAction::GetSelfContactDisplay(request))
        .await;
    let final_contact = response
        .await
        .expect("Getting self contact to work")
        .expect("Getting self contact to succeed");

    if start_with_incorrect_challenge {
        assert_eq!(final_contact.id, owned_contact.id, "Contact IDs to match");
        assert_eq!(
            final_contact.name, owned_contact.name,
            "Contact names to match"
        );
    } else {
        assert_ne!(final_contact.id, owned_contact.id, "Contact IDs to match");
        assert_ne!(
            final_contact.name, owned_contact.name,
            "Contact names to match"
        );
    }

    owner_harness.abort();
    new_harness.abort();
    secondary_new_harness.abort();
}

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_use_self_contact_invite_challenge_start_correct() {
    use_self_contact_invite_challenge(false).await;
}

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_use_self_contact_invite_challenge_start_incorrect() {
    use_self_contact_invite_challenge(true).await;
}
