use fyles_core::library::util::duration_ext::DurationExt;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tracing::{span, trace, Instrument, Level};

use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{CreateFilerequest, FilerequestAccess, FylesId, SendStatus};
use fyles_core::core::p2p::{FileToSend, NetworkNode};

use crate::types::Wrap;

use super::test_utils::*;

async fn create_test_file(dir: &std::path::Path) -> std::path::PathBuf {
    let test_file_path = dir.join("test_file.txt");
    let mut file = File::create(&test_file_path).await.unwrap();
    file.write_all(b"test content").await.unwrap();
    file.flush().await.unwrap();
    test_file_path
}

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn always_accept_files_from_self() {
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

    let sender_manual = span!(Level::INFO, "Sender manual");

    // Set all peers up according to the test configuration

    let (request, response) = BrainRequest::with_receiver(());
    owner_harness
        .act(ClientAction::GetFullSelfContact(request))
        .await;
    let invite = response
        .await
        .expect("Getting self contact to work")
        .expect("Getting self contact to succeed");

    let (request, response) = BrainRequest::with_receiver(invite);
    sender_harness
        .act(ClientAction::UpdateIdentity(request))
        .await;
    response
        .await
        .expect("Updating identity to work")
        .expect("Updating identity to succeed");

    // Create a private filerequest with no recipients
    let filerequest = CreateFilerequest {
        title: "Test FR".to_string(),
        description: "Test description".to_string(),
        access: FilerequestAccess::Audience {
            // technically, these should be internal IDs, but we're not testing the
            // DB here, so we simply omit this detail
            contact_ids: vec![],
        },
        is_active: true,
    };

    // Register the filerequest with the owner
    let (request, response) = BrainRequest::with_receiver(filerequest.clone());
    owner_harness
        .act(ClientAction::CreateFilerequest(request))
        .await;
    let filerequest_id = response
        .await
        .expect("Filerequest should be created")
        .expect("Filerequest should be created successfully");

    // Create test file
    let test_file_path = create_test_file(sender_harness.temp_dir.path()).await;

    // Test file sending from device belonging to same contact as owner
    let allowed_file = FileToSend {
        id: FylesId::new(),
        peer_id: owner_harness.peer_id.wrap(),
        filerequest_id: filerequest_id.clone(),
        file_path: test_file_path.to_str().unwrap().to_string(),
        retry_count: 0,
        contact_id: owner_harness.node_info.self_contact_id.clone(),
        status: SendStatus::Pending,
    };

    sender_harness
        .p2p_client
        .send_files(vec![allowed_file])
        .instrument(sender_manual)
        .await
        .expect("Sending to succeed");

    let file_send_request = timeout(
        5.seconds(),
        sender_harness.intercept_actions(async |action| match action {
            BrainAction::NetworkNode(NetworkNodeAction::FileSent { .. }) => {
                trace!("Breaking sender loop because file was sent successfully");
                InterceptionRes::Exit(Some(action))
            }
            BrainAction::NetworkNode(NetworkNodeAction::FileRejected { .. }) => {
                panic!("File should not be rejected, but was");
            }
            a => InterceptionRes::Continue(Some(a)),
        }),
    );

    let file_send_res = file_send_request.await;

    file_send_res.expect("File should be sent in time");

    // Clean up
    owner_harness.abort();
    sender_harness.abort();
}
