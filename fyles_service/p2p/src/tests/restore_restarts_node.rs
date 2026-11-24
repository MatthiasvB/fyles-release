use std::path::PathBuf;
use std::sync::Arc;

use fyles_core::core::api_server::MockRunnableFilerequestServer;
use fyles_core::core::backup::test_support::{TEST_PASSWORD, write_backup};
use fyles_core::core::backup::{
    Password, RestoreOutcome, RestoreRequest, UnpackedFormat, VerificationSchedule,
};
use fyles_core::core::brain::BrainNetwork;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_client::ClientAction;
use fyles_core::core::brain::plugin::{BrainPluginRef, NoOpBrainPlugin};
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::db::test::setup_test_db;
use fyles_core::entrypoint;
use fyles_core::io_controller::Uri;
use fyles_core::io_controller::test::TestHostController;
use fyles_core::library::util::duration_ext::DurationExt;
use libp2p::Swarm;
use libp2p::identity::Keypair;
use libp2p_swarm_test::SwarmExt;
use tempfile::TempDir;
use tokio::sync::{RwLock, mpsc, watch};

use crate::P2pClient;
use crate::behaviour::LocalNetworkBehaviour;
use crate::tests::test_utils::{generate_swarm_factory, repeat_till_success};
use crate::types::Wrap;

/// Asks whichever brain is serving clients right now, the way a reconnecting client would.
/// `None` while that brain is going away.
async fn running_peer_id(
    senders: &watch::Receiver<Vec<mpsc::Sender<BrainAction>>>,
) -> Option<String> {
    let current = senders.borrow().last().cloned()?;
    let (request, response) = BrainRequest::with_receiver(());
    current
        .send(BrainAction::Client(ClientAction::GetNodeStatus(request)))
        .await
        .ok()?;
    Some(response.await.ok()?.ok()?.peer_id.to_string())
}

async fn eventually_runs_as(
    senders: &watch::Receiver<Vec<mpsc::Sender<BrainAction>>>,
    expected: String,
) {
    repeat_till_success(10.seconds(), 100.millis(), || async {
        match running_peer_id(senders).await {
            Some(running) if running == expected => Ok(()),
            other => Err(other),
        }
    })
    .await
    .unwrap_or_else(|last| panic!("the node should run as {expected}, last seen {last:?}"));
}

#[tokio::test(flavor = "current_thread")]
#[test_log::test]
async fn a_restore_restarts_the_node_under_the_restored_peer_id() {
    let (_, node_info, peer_id) = generate_swarm_factory(None);
    let (_, retired_node_info, retired_peer_id) = generate_swarm_factory(None);
    let db = setup_test_db(Some(node_info)).await;
    let retired_db = setup_test_db(Some(retired_node_info)).await;
    let host = Arc::new(TestHostController::new(None));
    let (announce, senders) = watch::channel(Vec::new());

    std::thread::spawn(move || {
        entrypoint(
            PathBuf::from("."),
            async || db.clone(),
            async || host.clone() as _,
            async |sender, shutdown| {
                Arc::new(RwLock::new(
                    P2pClient::new(
                        sender,
                        shutdown,
                        Arc::new(|keypair: Keypair, _config: &[u8]| {
                            Ok(Swarm::new_ephemeral_tokio_with_preexisting_keypair(
                                keypair,
                                |kp| LocalNetworkBehaviour::new(&kp),
                            ))
                        }),
                    )
                    .await
                    .unwrap(),
                )) as BrainNetwork
            },
            async |_| Arc::new(RwLock::new(NoOpBrainPlugin)) as BrainPluginRef,
            async |sender, mut pushes, _| {
                tokio::spawn(async move { while pushes.recv().await.is_some() {} });
                announce.send_modify(|senders| senders.push(sender));
                Box::new(MockRunnableFilerequestServer {}) as _
            },
        )
    });

    eventually_runs_as(&senders, peer_id.wrap().to_string()).await;

    let dir = TempDir::new().unwrap();
    let backup = dir.path().join("retired.bak.crypt");
    write_backup(retired_db.as_ref(), VerificationSchedule::fresh(0), &backup).await;

    let (request, response) = BrainRequest::with_receiver(RestoreRequest {
        uri: Uri(backup.to_string_lossy().into_owned()),
        password: Some(Password::new(TEST_PASSWORD.to_string())),
    });
    let current = senders.borrow().last().cloned().unwrap();
    current
        .send(BrainAction::Client(ClientAction::RestoreData(request)))
        .await
        .unwrap();
    assert_eq!(
        response
            .await
            .expect("Sender not dropped")
            .expect("the restore to succeed"),
        RestoreOutcome::Restored(UnpackedFormat::Encrypted)
    );

    eventually_runs_as(&senders, retired_peer_id.wrap().to_string()).await;
}
