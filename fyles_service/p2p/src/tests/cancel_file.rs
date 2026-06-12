use fyles_core::library::util::duration_ext::DurationExt;
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, time::timeout};

use crate::{
    tests::test_utils::{
        generate_swarm_factory, BrainlessIdleP2pTestHarness,
    },
    types::Wrap,
};
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::domain_models::SendStatus;
use fyles_core::core::{
    domain_models::{ContactId, FylesId},
    p2p::FileToSend,
};
use fyles_core::io_controller::FileMeta;
use fyles_core::library::util::util::TimeoutLock;

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_cancel_file() {
    // Set up test environment
    let (swarm_factory, node_info, peer_id) = generate_swarm_factory(None);
    let mut harness = BrainlessIdleP2pTestHarness::new(swarm_factory, node_info, peer_id)
        .await
        .run()
        .await;

    // Create a temporary test file
    let test_file_path = harness.temp_dir.path().join("test_file.txt");
    let mut file = File::create(&test_file_path).await.unwrap();
    file.write_all(b"test content").await.unwrap();
    file.flush().await.unwrap();
    drop(file);

    // Create dummy file data
    let file_id: FylesId = FylesId::new();
    let filerequest_id: FylesId = FylesId::new();
    let test_file = FileToSend {
        id: file_id.clone(),
        peer_id: peer_id.wrap(),
        filerequest_id,
        file_path: test_file_path.to_str().unwrap().to_string(),
        retry_count: 0,
        contact_id: ContactId("test_contact".to_string()),
        status: SendStatus::Pending,
    };

    // First send a file
    harness
        .p2p_client
        .send_files(vec![test_file])
        .await
        .unwrap();

    // unblock thread
    tokio::time::timeout(1.seconds(), async {
        loop {
            if let BrainAction::NetworkNode(p2p_node_action) =
                harness.brain_receiver.recv().await.expect("sender dropped")
            {
                match p2p_node_action {
                    NetworkNodeAction::OpenFileForReading(brain_request) => {
                        let _ = brain_request
                            .response_sender
                            .timeout_lock()
                            .await
                            .take()
                            .expect("no channel")
                            .send(Ok(FileMeta {
                                file: File::open(&test_file_path).await.unwrap(),
                                path: test_file_path.to_str().unwrap().to_string(),
                                file_name: "test_file.txt".to_string(),
                            }));
                        break;
                    },
                    _ => (),
                }
            }
        }
    })
    .await
    .unwrap();

    let mut brain_rx = harness.brain_receiver;
    tokio::spawn(async move {
        while let Some(action) = brain_rx.recv().await {
            // If any action contains a oneshot response sender, respond with
            // a sensible default so the file tracker doesn't block.
            // For now, just drop/acknowledge everything.
            tracing::warn!(?action, "Fake brain received action");
        }
    });

    // Now try to cancel it
    let cancel_result = timeout(5.seconds(), harness.p2p_client.cancel_file(file_id.clone()))
        .await
        .unwrap()
        .unwrap();

    assert!(
        cancel_result,
        "File should have been cancelled successfully"
    );

    // Try to cancel again - should return false as file is already cancelled
    let cancel_result = harness.p2p_client.cancel_file(file_id).await.unwrap();
    assert!(!cancel_result, "Second cancellation should return false");
}

#[tokio::test(flavor = "current_thread")]
async fn test_cancel_nonexistent_file() {
    let (swarm_factory, node_info, peer_id) = generate_swarm_factory(None);
    let harness = BrainlessIdleP2pTestHarness::new(swarm_factory, node_info, peer_id)
        .await
        .run()
        .await;

    let nonexistent_id: FylesId = FylesId::new();
    let cancel_result = harness
        .p2p_client
        .cancel_file(nonexistent_id)
        .await
        .unwrap();
    assert!(
        !cancel_result,
        "Cancelling nonexistent file should return false"
    );
}
