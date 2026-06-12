use tracing::{span, Instrument, Level};

use crate::tests::test_utils::IdleP2pTestHarness;
use fyles_core::{
    core::{
        brain::{action_client::ClientAction, types::BrainRequest},
        domain_models::{CreateFilerequest, FilerequestAccess},
    },
    library::util::duration_ext::DurationExt,
};

const TEST_FILE_SIZE: usize = 10_000;

/// This test ensures that after exchanging files with a public filerequest,
/// which does not require authentication, the session can be upgraded
/// to also send a file to a private filerequest,
/// which requires authentication.
/// This is done by first creating a public filerequest, sending files to it,
/// and then creating a private filerequest that requires authentication.
///
/// Likely, no _actual_ upgrade needs to happen as the session is already authenticated
/// optimistically. But this ensures that this is valid semantically.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_upgrade_session_requirement() {
    let mut with_public_request = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "With public setup"))
        .await;
    let mut with_private_request = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "With private setup"))
        .await;

    with_public_request.listen_external().await;
    with_private_request.connect(&mut with_public_request).await;

    let with_public_request = with_public_request
        .run()
        .instrument(span!(Level::INFO, "With public request"))
        .await;
    let with_private_request = with_private_request
        .run()
        .instrument(span!(Level::INFO, "With private request"))
        .await;

    // Create public filerequest on public node
    let (public_request, response) = BrainRequest::with_receiver(CreateFilerequest {
        title: "Public request".into(),
        description: "Is public".into(),
        access: FilerequestAccess::Public,
        is_active: true,
    });
    with_public_request
        .act(ClientAction::CreateFilerequest(public_request))
        .await;
    let public_request_id = response
        .await
        .expect("Sender not dropped")
        .expect("Creation successful");

    // Register public node as contact of private node
    with_private_request
        .register_as_contact_of(&with_public_request)
        .await;

    // Private node registers remote public filerequest and sends a file
    let public_request_registered_id = with_private_request
        .create_remote_filerequest(
            &with_public_request,
            public_request_id.0.clone(),
            "The public request",
        )
        .await;
    with_private_request
        .send_files_and_wait(
            public_request_registered_id,
            vec![TEST_FILE_SIZE],
            3.seconds(),
            50.millis(),
        )
        .await
        .unwrap();

    // Create private filerequest (audience includes public node)
    let (private_request, response) = BrainRequest::with_receiver(CreateFilerequest {
        title: "Private request".into(),
        description: "Is private".into(),
        access: FilerequestAccess::Audience {
            contact_ids: vec![with_public_request.node_info.self_contact_id.clone()],
        },
        is_active: true,
    });
    with_private_request
        .act(ClientAction::CreateFilerequest(private_request))
        .await;
    let private_request_id = response
        .await
        .expect("Sender not dropped")
        .expect("Creation successful");

    // Public node registers remote private filerequest and sends a file (session upgrade path)
    let private_request_registered_id = with_public_request
        .create_remote_filerequest(
            &with_private_request,
            private_request_id.0.clone(),
            "The private request",
        )
        .await;
    with_public_request
        .send_files_and_wait(
            private_request_registered_id,
            vec![TEST_FILE_SIZE],
            3.seconds(),
            50.millis(),
        )
        .await
        .unwrap();

    with_private_request.abort();
    with_public_request.abort();
}
