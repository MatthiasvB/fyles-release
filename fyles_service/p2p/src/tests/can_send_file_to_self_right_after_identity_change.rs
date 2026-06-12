use fyles_core::library::util::duration_ext::DurationExt;
use tokio::time::timeout;
use tracing::{debug, info, span, Instrument, Level};

use crate::tests::test_utils::{IdleP2pTestHarness, InterceptionRes};
use crate::types::Wrap;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{CreateFilerequest, FilerequestAccess};

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn can_send_file_to_self_right_after_identity_change() {
    let mut old_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Old device setup"))
        .await;
    let mut new_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "New device setup"))
        .await;

    debug!("Old: {}", old_harness.peer_id);
    debug!("New {}", new_harness.peer_id);

    old_harness.listen_external().await;
    new_harness.connect(&mut old_harness).await;

    let old_harness = old_harness
        .run()
        .instrument(span!(Level::INFO, "Old device"))
        .await;
    let new_harness = new_harness
        .run()
        .instrument(span!(Level::INFO, "New device"))
        .await;

    let filerequest = CreateFilerequest {
        title: "Test FR".to_string(),
        description: "Test description".to_string(),
        access: FilerequestAccess::Audience {
            contact_ids: vec![],
        },
        is_active: true,
    };

    info!("Register filerequest with new harness");

    let (request, response) = BrainRequest::with_receiver(filerequest.clone());
    new_harness
        .act(ClientAction::CreateFilerequest(request))
        .await;
    let filerequest_id = response
        .await
        .expect("Filerequest should be created")
        .expect("Filerequest should be created successfully");

    info!(
        "Filerequest created with ID: {}. Going to register with old harness",
        filerequest_id
    );

    let internal_filerequest_id = old_harness
        .create_remote_filerequest(&new_harness, filerequest_id.clone().0, "New's filerequest")
        .await;

    info!(
        "Initiate identity update via network to trigger session establishment (of not authenticated session)"
    );

    let (challenge_request, challenge_response) = BrainRequest::with_receiver(());
    old_harness
        .act(ClientAction::RegisterSelfContactInviteChallenge(
            challenge_request,
        ))
        .await;
    let challenge_bytes = challenge_response.await.expect("to get challenge");

    let (register_challenge_request, res) =
        BrainRequest::with_receiver((challenge_bytes, old_harness.peer_id.wrap()));
    new_harness
        .act(ClientAction::UseSelfContactInviteChallenge(
            register_challenge_request,
        ))
        .await;
    let _ = res.await;

    info!("Wait for identity update");
    timeout(
        5.seconds(),
        new_harness.intercept_actions(async |action| match action {
            BrainAction::NetworkNode(NetworkNodeAction::UpdateIdentity { .. }) => {
                InterceptionRes::Exit(Some(action))
            }
            a => InterceptionRes::Continue(Some(a)),
        }),
    )
    .await
    .expect("Timeout waiting for identity update");

    info!(
        "Identity updated. This should reset session of new harness. So session establishment needs to be re-triggered"
    );

    old_harness
        .send_files_and_wait(
            internal_filerequest_id.clone(),
            vec![100],
            1.seconds(),
            50.millis(),
        )
        .await
        .expect("File send should work");

    info!("Done");

    // Clean up
    old_harness.abort();
    new_harness.abort();
}
