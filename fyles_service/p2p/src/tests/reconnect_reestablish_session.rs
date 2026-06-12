use fyles_core::library::util::duration_ext::DurationExt;
use tracing::{span, trace, Instrument, Level};

use crate::tests::test_utils::IdleP2pTestHarness;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{CreateFilerequest, FilerequestAccess};

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn reestablish_session_after_peer_crash() {
    let mut crashing_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Crashing setup"))
        .await;
    let mut persisting_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Persisting setup"))
        .await;

    crashing_harness.listen_external().await;
    persisting_harness.connect(&mut crashing_harness).await;

    let crashing_harness = crashing_harness
        .run()
        .instrument(span!(Level::INFO, "Crashing"))
        .await;
    let persisting_harness = persisting_harness
        .run()
        .instrument(span!(Level::INFO, "Persisting"))
        .await;

    let filerequest = CreateFilerequest {
        title: "Test FR".to_string(),
        description: "Test description".to_string(),
        access: FilerequestAccess::Public,
        is_active: true,
    };

    // Register the filerequest with the owner
    let (request, response) = BrainRequest::with_receiver(filerequest.clone());
    crashing_harness
        .act(ClientAction::CreateFilerequest(request))
        .await;
    let filerequest_id = response
        .await
        .expect("Filerequest should be created")
        .expect("Filerequest should be created successfully");

    trace!("Filerequest created with ID: {}", filerequest_id);

    let internal_filerequest_id = persisting_harness
        .create_remote_filerequest(
            &crashing_harness,
            filerequest_id.clone().0,
            "Crashing's filerequest",
        )
        .await;

    persisting_harness
        .send_files_and_wait(
            internal_filerequest_id.clone(),
            vec![100],
            1.seconds(),
            50.millis(),
        )
        .await
        .expect("Initial file send should work");

    let crashing_reborn = crashing_harness
        .restart_and_get_connected_by(vec![&persisting_harness])
        .instrument(span!(Level::INFO, "Crashing restarting"))
        .await
        .initialize()
        .instrument(span!(Level::INFO, "Crashing reborn initialization"))
        .await
        .run()
        .instrument(span!(Level::INFO, "Crashing reborn"))
        .await;

    // Sending files again should work
    persisting_harness
        .send_files_and_wait(internal_filerequest_id, vec![100], 1.seconds(), 50.millis())
        .await
        .expect("Sending files after reconnecting should work");

    // Clean up
    crashing_reborn.abort();
    persisting_harness.abort();
}
