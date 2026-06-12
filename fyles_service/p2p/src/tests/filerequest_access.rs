use futures::join;
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

struct FilerequestAccessTestConfig {
    sender_known: bool,
    unauthorized_known: bool,
    owner_to_sender_known: bool,
    owner_to_unauthorized_known: bool,
}

impl Default for FilerequestAccessTestConfig {
    fn default() -> Self {
        Self {
            sender_known: true,
            unauthorized_known: true,
            owner_to_sender_known: true,
            owner_to_unauthorized_known: true,
        }
    }
}

async fn run_private_filerequest_access(config: FilerequestAccessTestConfig) {
    // Initially, assume worst case scenario:
    let mut owner_received_file_from_sender = false;
    let mut sender_got_confirmation_of_file_receipt = false;
    let mut unauthorized_got_file_rejection_notice = false;

    let mut owner_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Owner setup"))
        .await;
    let mut sender_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Sender setup"))
        .await;
    let mut unauthorized_harness = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Unauthorized setup"))
        .await;

    owner_harness.listen_external().await;
    sender_harness.connect(&mut owner_harness).await;
    unauthorized_harness.connect(&mut owner_harness).await;

    let owner_harness = owner_harness
        .run()
        .instrument(span!(Level::INFO, "Owner"))
        .await;
    let sender_harness = sender_harness
        .run()
        .instrument(span!(Level::INFO, "Sender"))
        .await;
    let unauthorized_harness = unauthorized_harness
        .run()
        .instrument(span!(Level::INFO, "Unauthorized"))
        .await;

    let sender_manual = span!(Level::INFO, "Sender manual");
    let unauthorized_manual = span!(Level::INFO, "Unauthorized manual");

    // Set all peers up according to the test configuration

    if config.sender_known {
        owner_harness.register_as_contact_of(&sender_harness).await;
    }

    if config.owner_to_sender_known {
        sender_harness.register_as_contact_of(&owner_harness).await;
    }

    if config.unauthorized_known {
        owner_harness
            .register_as_contact_of(&unauthorized_harness)
            .await;
    }

    if config.owner_to_unauthorized_known {
        unauthorized_harness
            .register_as_contact_of(&owner_harness)
            .await;
    }

    // Create a private filerequest that only allows the sender
    let filerequest = CreateFilerequest {
        title: "Test FR".to_string(),
        description: "Test description".to_string(),
        access: FilerequestAccess::Audience {
            // technically, these should be internal IDs, but we're not testing the
            // DB here, so we simply omit this detail
            contact_ids: if config.sender_known {
                vec![sender_harness.node_info.self_contact_id.clone()]
            } else {
                vec![]
            },
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

    // Test file sending from allowed peer
    let allowed_file = FileToSend {
        id: FylesId::new(),
        peer_id: owner_harness.peer_id.wrap(),
        filerequest_id: filerequest_id.clone(),
        file_path: test_file_path.to_str().unwrap().to_string(),
        retry_count: 0,
        contact_id: owner_harness.node_info.self_contact_id.clone(),
        status: SendStatus::Pending,
    };

    let sender_key_request = timeout(
        5.seconds(),
        sender_harness.intercept_actions_and_await(
            async |action| match action {
                BrainAction::NetworkNode(NetworkNodeAction::FileSent { .. }) => {
                    if !config.sender_known {
                        panic!("File should not be sent, but was");
                    }
                    sender_got_confirmation_of_file_receipt = true;
                    trace!("Breaking sender loop because file was sent successfully");
                    InterceptionRes::Exit(Some(action))
                }
                BrainAction::NetworkNode(NetworkNodeAction::FileRejected { .. }) => {
                    if config.sender_known {
                        panic!("File should not be rejected, but was");
                    } else {
                        trace!("Breaking sender loop because the file was rejected");
                        InterceptionRes::Exit(Some(action))
                    }
                }
                a => InterceptionRes::Continue(Some(a)),
            },
            async {
                sender_harness
                    .p2p_client
                    .clone()
                    .send_files(vec![allowed_file])
                    .instrument(sender_manual)
                    .await
                    .expect("Sending to succeed");
            },
        ),
    );

    // Wait for p2p node to request access for incoming file
    let owner_actions = timeout(
        2.seconds(),
        owner_harness.intercept_actions(async |action| {
            trace!("Waiting for file request event");
            match action {
                BrainAction::NetworkNode(NetworkNodeAction::RequestFileDrop(..)) => {
                    if config.sender_known {
                        trace!("File drop request was sent");
                        InterceptionRes::Continue(Some(action))
                    } else {
                        trace!("Breaking owner loop because file drop request will be declined for unauthorized sender");
                        InterceptionRes::Exit(Some(action))
                    }
                }
                BrainAction::NetworkNode(NetworkNodeAction::StoreReceivedFile(..)) => {
                    if !config.sender_known {
                        panic!("File should not be stored, but was");
                    }
                    owner_received_file_from_sender = true;
                    trace!("Breaking owner-sender loop because file was received");
                    InterceptionRes::Exit(Some(action))
                }
                a => InterceptionRes::Continue(Some(a)),
            }
        }),
    );

    let (sender_res, owner_res) = join!(sender_key_request, owner_actions);

    sender_res.expect("Public keys should be sent");
    owner_res.expect("File drop request should be sent");

    let unauthorized_file = FileToSend {
        id: FylesId::new(),
        peer_id: owner_harness.peer_id.wrap(),
        filerequest_id: filerequest_id.clone(),
        file_path: test_file_path.to_str().unwrap().to_string(),
        retry_count: 0,
        contact_id: owner_harness.node_info.self_contact_id.clone(),
        status: SendStatus::Pending,
    };

    // Wait for file transfer to be rejected
    timeout(
        1.seconds(),
        unauthorized_harness.intercept_actions_and_await(
            async |action| match action {
                BrainAction::NetworkNode(NetworkNodeAction::FileRejected { .. }) => {
                    unauthorized_got_file_rejection_notice = true;
                    InterceptionRes::Exit(Some(action))
                }
                a => InterceptionRes::Continue(Some(a)),
            },
            async {
                // Sending should fail because peer is not in audience
                unauthorized_harness
                    .p2p_client
                    .send_files(vec![unauthorized_file])
                    .instrument(unauthorized_manual)
                    .await
                    .expect("Channel to work");
            },
        ),
    )
    .await
    .expect("File should be rejected successfully");

    assert_eq!(
        owner_received_file_from_sender, config.sender_known,
        "Owner did not receive file even though sender is known"
    );
    assert_eq!(
        sender_got_confirmation_of_file_receipt, config.sender_known,
        "Sender did not receive confirmation of file receipt even though sender is known"
    );
    assert!(
        unauthorized_got_file_rejection_notice,
        "Unauthorized did not receive file rejection notice"
    );

    // Clean up
    owner_harness.abort();
    sender_harness.abort();
    unauthorized_harness.abort();
}

// Not every aspect of below test description are verified in this detail.
// Rather, it's my mental model of what should happen as the text executes

/// Test that a correctly authenticated and authorized contact can send files to a private filerequest
///
/// In this case, the sender should know the owner, so it should attempt to establish
/// an OptimisticBilaterallyAuthenticated session. This session asteablishment should succeed
/// and the file be transferred.
///
/// The Unauthorized should attempt to establish the same kind of session
/// but fail, falling back to a ResponderAuthenticated session. However, this
/// will not be sufficient to even try to see if this peer is authorized to send
/// a file to the particular filerequest, and the transfer should fail due to the
/// insufficient authentication
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_a() {
    run_private_filerequest_access(Default::default()).await;
}

/// Test that a sender that is not known to the owner cannot send files to a private filerequest.
/// This test goes further: If the contact's public keys were known, the file would be accepted, because
/// the sender's ID is actually authorized
///
/// In this instance, where the sender isn't known to the owner, the resulting
/// session will only be ResponderAuthenticated, which will lead to the same transfer
/// error as in the test above with the Unauthorized. The transfer of Unauthorized
/// itself will fail as before.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_b() {
    run_private_filerequest_access(FilerequestAccessTestConfig {
        sender_known: false,
        ..Default::default()
    })
    .await;
}

/// Test that sender can send files to a private filerequest, even if the sender does not know the owner.
///
/// In this case, the Sender will attempt to establish an OptimisticInitiatorAuthenticated session, which
/// will succeed and become InitiatorAuthenticated, sufficient to transfer the file.
/// This means that there is no guarantee
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_c() {
    run_private_filerequest_access(FilerequestAccessTestConfig {
        owner_to_sender_known: false,
        ..Default::default()
    })
    .await;
}

/// Test that sender can't send files to a private filerequest, if the sender and owner don't know each other.
///
/// In this case, session establishment will also not yield a sufficiently authenticated session,
/// and the transfer will fail.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_d() {
    run_private_filerequest_access(FilerequestAccessTestConfig {
        owner_to_sender_known: false,
        sender_known: false,
        ..Default::default()
    })
    .await;
}

/// Test that nothing weird happens if unauthorized isn't known to the owner.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_e() {
    run_private_filerequest_access(FilerequestAccessTestConfig {
        unauthorized_known: false,
        ..Default::default()
    })
    .await;
}

/// Test that nothing weird happens if unauthorized and owner don't know each other.
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_f() {
    run_private_filerequest_access(FilerequestAccessTestConfig {
        unauthorized_known: false,
        owner_to_unauthorized_known: false,
        ..Default::default()
    })
    .await;
}

/// Test that nothing weird happens if nobody knows anyone
#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn test_private_filerequest_access_g() {
    run_private_filerequest_access(FilerequestAccessTestConfig {
        unauthorized_known: false,
        owner_to_unauthorized_known: false,
        sender_known: false,
        owner_to_sender_known: false,
    })
    .await;
}
