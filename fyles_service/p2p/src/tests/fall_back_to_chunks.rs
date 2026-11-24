use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{CreateFilerequest, FilerequestAccess, FylesId, SendStatus};
use fyles_core::core::p2p::{FileToSend, NetworkNode};
use fyles_core::library::util::duration_ext::DurationExt;
use tokio::time::timeout;
use tracing::{Instrument, Level, span};

use super::test_utils::*;
use crate::behaviour::LocalNetworkBehaviour;
use crate::types::Wrap;

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn a_peer_without_the_stream_protocol_receives_over_chunks() {
    let (factory, node_info, peer_id) =
        swarm_factory_with(None, LocalNetworkBehaviour::chunks_only);
    let mut owner = IdleP2pTestHarness::new(factory, node_info, peer_id, None, None)
        .instrument(span!(Level::INFO, "Owner setup"))
        .await;
    let mut sender = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Sender setup"))
        .await;
    owner.listen_external().await;
    sender.connect(&mut owner).await;
    let owner = owner.run().instrument(span!(Level::INFO, "Owner")).await;
    let sender = sender.run().instrument(span!(Level::INFO, "Sender")).await;

    let (request, response) = BrainRequest::with_receiver(());
    owner.act(ClientAction::GetFullSelfContact(request)).await;
    let invite = response.await.unwrap().unwrap();
    let (request, response) = BrainRequest::with_receiver(invite);
    sender.act(ClientAction::UpdateIdentity(request)).await;
    response.await.unwrap().unwrap();

    let (request, response) = BrainRequest::with_receiver(CreateFilerequest {
        title: "Fallback".to_string(),
        description: String::new(),
        access: FilerequestAccess::Audience {
            contact_ids: vec![],
        },
        is_active: true,
    });
    owner.act(ClientAction::CreateFilerequest(request)).await;
    let filerequest_id = response.await.unwrap().unwrap();

    let source = sender.temp_dir.path().join("source.txt");
    std::fs::write(&source, b"sent the old way").unwrap();
    sender
        .p2p_client
        .read()
        .await
        .send_files(vec![FileToSend {
            id: FylesId::new(),
            peer_id: owner.peer_id.wrap(),
            filerequest_id,
            file_path: source.to_str().unwrap().to_string(),
            retry_count: 0,
            contact_id: owner.node_info.self_contact_keys.id(),
            status: SendStatus::Pending,
        }])
        .await
        .unwrap();

    timeout(
        10.seconds(),
        sender.intercept_actions(async |action| match action {
            BrainAction::NetworkNode(NetworkNodeAction::FileSent { .. }) => {
                InterceptionRes::Exit(Some(action))
            }
            BrainAction::NetworkNode(NetworkNodeAction::FileSendReset { reason, .. }) => {
                panic!("Falling back must not reset the file, but it was reset: {reason:?}")
            }
            action => InterceptionRes::Continue(Some(action)),
        }),
    )
    .await
    .expect("the file to be sent over the chunked protocol");
}
