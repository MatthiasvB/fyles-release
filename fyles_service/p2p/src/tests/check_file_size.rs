use futures::StreamExt;
use fyles_core::library::util::duration_ext::DurationExt;
use fyles_core::library::util::util::TimeoutLock;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::{time};
use tracing::{info, span, warn, Instrument, Level};

use libp2p::swarm::SwarmEvent;
use libp2p::{request_response, Swarm};

use fyles_core::core::{
    brain::{action::BrainAction, action_p2p::NetworkNodeAction},
    domain_models::{Filerequest, FilerequestAccess, FylesId},
    filerequest_drive_handler::FilerequestDriveHandler,
};

use crate::behaviour::CoreBehaviourEvent;
use crate::event_loop::InnerAsyncEvent;
use crate::send_receive_traits::session::Session;
use crate::tests::test_utils::IdleP2pTestHarness;
use crate::types::{DataChunk, FileRequest, FileResponse};
use crate::utils::W;
use crate::{
    behaviour::{LocalNetworkBehaviour, LocalNetworkBehaviourEvent},
    crypto::Session as CryptoSession,
    tests::test_utils::BrainlessIdleP2pTestHarness,
};

async fn wait_for_and_assert_accepted(
    swarm: &mut Swarm<LocalNetworkBehaviour>,
    expected: FileResponse,
    session: &W<Arc<CryptoSession>>,
) {
    let timeout = time::timeout(1.seconds(), async {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(LocalNetworkBehaviourEvent::Core(
                    CoreBehaviourEvent::Filerequest(request_response::Event::Message {
                        message: request_response::Message::Response { response, .. },
                        ..
                    }),
                )) => {
                    let decrypted = session.decrypt(&response.unwrap()).await.unwrap();
                    assert_eq!(decrypted, expected);
                    break;
                }
                event => {
                    warn!("Unexpected event: {:?}", event);
                }
            }
        }
    });
    match timeout.await {
        Ok(_) => {}
        Err(_) => panic!("Timed out waiting for accepted"),
    }
}

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
pub async fn test_reject_oversized_file() {
    let mut host = BrainlessIdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "host default"))
        .await;
    let mut misbehaved = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "misbehaved default"))
        .await;

    host.listen_external()
        .instrument(span!(Level::INFO, "host listen external"))
        .await;
    misbehaved
        .connect_brainless(&mut host)
        .instrument(span!(Level::INFO, "misbehaved connect"))
        .await;

    info!(
        "host: {} -- misbehaved {}",
        host.peer_id, misbehaved.peer_id
    );

    let mut host = host.run().instrument(span!(Level::INFO, "host run")).await;
    let misbehaved = misbehaved
        .run()
        .instrument(span!(Level::INFO, "misbehaved run"))
        .await;

    let respond_keys = tokio::time::timeout(1.seconds(), async {
        loop {
            if let BrainAction::NetworkNode(p2p_node_action) =
                host.brain_receiver.recv().await.expect("sender dropped")
            {
                if let NetworkNodeAction::GetContactPublicKeys(brain_request) = p2p_node_action {
                    let _ = brain_request
                        .response_sender
                        .timeout_lock()
                        .await
                        .take()
                        .expect("no channel")
                        .send(None);
                    break;
                }
            }
        }
    });

    let (event_loop_sender, event_loop_receiver) = oneshot::channel();
    misbehaved
        .p2p_client
        .inner_async_event_sender
        .send(InnerAsyncEvent::GetEventLoop(event_loop_sender))
        .unwrap();
    let misbehaved_event_loop = event_loop_receiver.await.unwrap();
    let misbehaved_session_fut = tokio::time::timeout(3.seconds(), async {
        misbehaved_event_loop
            .get_session_for_test(host.peer_id, None, true)
            .await
    });

    let (_, misbehaved_session) = tokio::join!(respond_keys, misbehaved_session_fut);
    let misbehaved_session = misbehaved_session
        .expect("Session created in time")
        .expect("Session to be created");

    let transfer_uuid: FylesId = FylesId::new();
    let filerequest = Filerequest {
        id: FylesId::new(),
        title: "Test File".into(),
        description: "A test file".into(),
        access: FilerequestAccess::Public,
        is_active: true,
    };
    let filerequest_id: FylesId = FylesId::new();
    let file_name: String = "Larger than advertised".into();
    let file_size_bytes = 1024;
    let small_offset = 5;
    let legal_chuck_count = 4;
    let chunk_size_bytes = file_size_bytes / legal_chuck_count - small_offset;
    misbehaved_event_loop.with_swarm_test(move |swarm| {
        futures::executor::block_on(async move {
            swarm.behaviour_mut().core.filerequest.send_request(
                &host.peer_id,
                misbehaved_session
                    .encrypt(&FileRequest::NewTransfer {
                        filerequest_id,
                        file_name,
                        file_size_bytes: file_size_bytes as u64,
                        transfer_uuid: transfer_uuid.clone(),
                    })
                    .await
                    .unwrap(),
            );
            println!("{:?}", swarm.next().await);
            loop {
                let action = host.brain_receiver.recv().await.expect("sender dropped");
                if let BrainAction::NetworkNode(NetworkNodeAction::RequestFileDrop(
                    filerequest_event,
                    span,
                )) = action
                {
                    filerequest_event
                        .response_sender
                        .lock()
                        .instrument(span)
                        .await
                        .take()
                        .expect("no channel")
                        .send(Some(FilerequestDriveHandler::new(
                            misbehaved.out_dir_handler.clone(),
                            filerequest,
                        )))
                        .unwrap_or_else(|_| panic!("Send should work"));
                    break;
                }
            }
            println!("Before first accept");
            wait_for_and_assert_accepted(
                swarm,
                FileResponse::AcceptNewTransfer,
                &misbehaved_session,
            )
            .await;
            println!("After first accept");
            for i in 0..legal_chuck_count as u32 {
                println!("Sending {i}th chunk");
                swarm.behaviour_mut().core.filerequest.send_request(
                    &host.peer_id,
                    misbehaved_session
                        // .encrypt(chunk)
                        .encrypt(&FileRequest::Chunk(DataChunk {
                            transfer_uuid: transfer_uuid.clone(),
                            data: vec![0u8; chunk_size_bytes].to_vec(),
                            idx: i,
                        }))
                        .await
                        .unwrap(),
                );
                println!("Before {i}th accept");
                wait_for_and_assert_accepted(swarm, FileResponse::ConfirmChunk, &misbehaved_session)
                    .await;
                println!("After {i}th accept");
            }

            swarm.behaviour_mut().core.filerequest.send_request(
                &host.peer_id,
                misbehaved_session
                    .encrypt(&FileRequest::Chunk(DataChunk {
                        transfer_uuid,
                        data: vec![0u8; 60],
                        idx: legal_chuck_count as u32,
                    }))
                    .await
                    .unwrap(),
            );
            wait_for_and_assert_accepted(swarm, FileResponse::RejectChunk, &misbehaved_session)
                .await;
            println!("After last accept");
        });
    });
}
