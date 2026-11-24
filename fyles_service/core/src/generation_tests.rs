use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::{RwLock, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::core::api_server::MockRunnableFilerequestServer;
use crate::core::backup::test_support::{TEST_PASSWORD, write_backup};
use crate::core::backup::{
    Password, RestoreOutcome, RestoreRefusal, RestoreRequest, UnpackedFormat, VerificationSchedule,
};
use crate::core::brain::action::BrainAction;
use crate::core::brain::action_client::ClientAction;
use crate::core::brain::action_p2p::{NetworkNodeAction, NodeInfo};
use crate::core::brain::error::FilerequestError;
use crate::core::brain::plugin::BrainPluginRef;
use crate::core::brain::types::BrainRequest;
use crate::core::brain::{BrainNetwork, NodeSettings};
use crate::core::db::test::setup_test_db;
use crate::core::domain_models::PeerIdWrapper;
use crate::core::p2p::NodeStatusInfo;
use crate::entrypoint;
use crate::io_controller::{Uri, test::TestHostController};
use crate::mocks::{p2p::MockP2pNode, plugin::MockBrainPlugin};

struct Generations {
    senders: watch::Receiver<Vec<mpsc::Sender<BrainAction>>>,
    tokens: Arc<StdMutex<Vec<CancellationToken>>>,
    run: oneshot::Receiver<()>,
}

impl Generations {
    async fn start() -> Self {
        let db = setup_test_db(None).await;
        let host = Arc::new(TestHostController::new(None));
        let node = MockP2pNode::new(NodeStatusInfo {
            peer_id: PeerIdWrapper::for_test(),
            external_addresses: vec![],
            internal_addresses: vec![],
            connected_peers: 0,
            start_timestamp: 0,
        });
        let tokens = Arc::new(StdMutex::new(Vec::new()));
        let (announce, senders) = watch::channel(Vec::new());

        let (stopped, run) = oneshot::channel();
        std::thread::spawn({
            let tokens = tokens.clone();
            move || {
                entrypoint(
                    NodeSettings::with_stored_port(PathBuf::from(".")),
                    async || db.clone(),
                    async || host.clone() as _,
                    async |_, shutdown| {
                        tokens.lock().unwrap().push(shutdown);
                        Arc::new(RwLock::new(node.clone())) as BrainNetwork
                    },
                    async |_| Arc::new(RwLock::new(MockBrainPlugin::default())) as BrainPluginRef,
                    async |sender, mut pushes, _| {
                        tokio::spawn(async move { while pushes.recv().await.is_some() {} });
                        announce.send_modify(|senders| senders.push(sender));
                        Box::new(MockRunnableFilerequestServer {}) as _
                    },
                );
                let _ = stopped.send(());
            }
        });

        let mut generations = Self {
            senders,
            tokens,
            run,
        };
        node_info(&generations.generation(0).await).await;
        generations
    }

    async fn generation(&mut self, index: usize) -> mpsc::Sender<BrainAction> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.senders.wait_for(|senders| senders.len() > index),
        )
        .await
        .unwrap_or_else(|_| panic!("generation {index} never started"))
        .unwrap()[index]
            .clone()
    }

    fn started(&self) -> usize {
        self.senders.borrow().len()
    }
}

struct Backup {
    _dir: TempDir,
    path: PathBuf,
    node_info: NodeInfo,
}

async fn backup_of(node_key_pair: &[u8]) -> Backup {
    let node_info = NodeInfo::generate_random_bytes(node_key_pair.to_vec());
    let db = setup_test_db(Some(node_info.clone())).await;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("restore.bak.crypt");
    write_backup(db.as_ref(), VerificationSchedule::fresh(0), &path).await;
    Backup {
        _dir: dir,
        path,
        node_info,
    }
}

async fn restore(
    sender: &mpsc::Sender<BrainAction>,
    path: &Path,
    password: &str,
) -> Result<RestoreOutcome, FilerequestError> {
    let (request, response) = BrainRequest::with_receiver(RestoreRequest {
        uri: Uri(path.to_string_lossy().into_owned()),
        password: Some(Password::new(password.to_string())),
    });
    sender
        .send(BrainAction::Client(ClientAction::RestoreData(request)))
        .await
        .unwrap();
    response.await.unwrap()
}

/// What a freshly started event loop asks for before it builds its swarm.
async fn node_info(sender: &mpsc::Sender<BrainAction>) -> NodeInfo {
    let (request, response) = BrainRequest::with_receiver(());
    sender
        .send(BrainAction::NetworkNode(NetworkNodeAction::GetNodeInfo(
            request,
        )))
        .await
        .unwrap();
    response.await.unwrap().unwrap().0
}

#[tokio::test]
async fn a_restore_restarts_on_the_restored_identity() {
    let mut generations = Generations::start().await;
    let backup = backup_of(b"restored node").await;
    let first = generations.generation(0).await;

    assert_eq!(
        restore(&first, &backup.path, TEST_PASSWORD).await.unwrap(),
        RestoreOutcome::Restored(UnpackedFormat::Encrypted)
    );
    let second = generations.generation(1).await;

    assert!(
        first.is_closed(),
        "the first generation must be torn down before the second starts"
    );
    assert!(generations.tokens.lock().unwrap()[0].is_cancelled());
    let restored = node_info(&second).await;
    assert_eq!(restored.node_key_pair, backup.node_info.node_key_pair);
    assert_eq!(
        restored.self_contact_keys.id(),
        backup.node_info.self_contact_keys.id()
    );
}

#[tokio::test]
async fn a_corrupt_backup_is_refused_without_a_restart() {
    let mut generations = Generations::start().await;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("corrupt.bak");
    std::fs::write(&path, b"not a backup").unwrap();
    let first = generations.generation(0).await;

    assert_eq!(
        restore(&first, &path, TEST_PASSWORD).await.unwrap(),
        RestoreOutcome::Refused(RestoreRefusal::NotABackup)
    );

    node_info(&first).await;
    assert_eq!(generations.started(), 1);
}

#[tokio::test]
async fn a_wrong_password_is_refused_without_a_restart() {
    let mut generations = Generations::start().await;
    let backup = backup_of(b"locked node").await;
    let first = generations.generation(0).await;

    assert_eq!(
        restore(&first, &backup.path, "not the password")
            .await
            .unwrap(),
        RestoreOutcome::Refused(RestoreRefusal::WrongPassword)
    );

    node_info(&first).await;
    assert_eq!(generations.started(), 1);
}

#[tokio::test]
async fn a_second_restore_restarts_again() {
    let mut generations = Generations::start().await;
    let first_backup = backup_of(b"first restore").await;
    let second_backup = backup_of(b"second restore").await;

    restore(
        &generations.generation(0).await,
        &first_backup.path,
        TEST_PASSWORD,
    )
    .await
    .unwrap();
    restore(
        &generations.generation(1).await,
        &second_backup.path,
        TEST_PASSWORD,
    )
    .await
    .unwrap();

    assert_eq!(
        node_info(&generations.generation(2).await)
            .await
            .node_key_pair,
        second_backup.node_info.node_key_pair
    );
}

#[tokio::test]
async fn a_shutdown_stops_without_a_restart() {
    let mut generations = Generations::start().await;
    let (request, response) = BrainRequest::with_receiver(());
    generations
        .generation(0)
        .await
        .send(BrainAction::Client(ClientAction::Shutdown(request)))
        .await
        .unwrap();
    assert!(response.await.unwrap().unwrap());

    assert_eq!(generations.started(), 1);
    tokio::time::timeout(Duration::from_secs(5), generations.run)
        .await
        .expect("the generations to stop")
        .unwrap();
}
