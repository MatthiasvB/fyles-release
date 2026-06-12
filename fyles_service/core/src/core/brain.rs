use std::{path::PathBuf, sync::Arc};

use derive_more::Deref;
use tokio::{
    sync::{mpsc, Mutex, RwLock},
    task::{JoinHandle, JoinSet},
};
use tonic::Status;
use tracing::{debug, instrument, span, Instrument, Level};

use crate::{
    core::brain::types::ContactShareChallenge,
    library::util::{duration_ext::DurationExt, util::TimeoutLock},
    BrainAction,
};
use crate::{
    core::api_server::stream_registry::DroppableStream,
    io_controller::HostController,
    library::{
        bounded_list::BoundedList,
        ttlmap::TtlSet,
        wire::{
            api::{ClientMessage, ServerMessage},
            RunnableFilerequestServer,
        },
    },
};
use crate::{
    core::{brain::types::SelfContactInviteChallenge, p2p::RunnableNetworkNode},
    library::util::util::OptionInspectMut,
};

use super::{db::FilerequestDb};

pub mod action;
pub mod action_client;
pub mod action_p2p;
#[cfg(any(test, feature = "test-support"))]
pub mod action_test;
pub mod error;
pub mod handle_action;
pub mod stale_cleanup;
pub mod types;

#[cfg(any(test, feature = "test-support"))]
pub struct ActionInterceptor {
    pub action_sender: mpsc::Sender<BrainAction>,
    pub action_receiver: mpsc::Receiver<Option<BrainAction>>,
}

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for ActionInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionInterceptor").finish()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ActionInterceptor {
    pub fn new() -> (
        Self,
        mpsc::Sender<Option<BrainAction>>,
        mpsc::Receiver<BrainAction>,
    ) {
        let (out_sender, out_receiver) = mpsc::channel(1);
        let (in_sender, in_receiver) = mpsc::channel(1);
        (
            ActionInterceptor {
                action_sender: out_sender,
                action_receiver: in_receiver,
            },
            in_sender,
            out_receiver,
        )
    }
}

pub struct Brain {
    db: BrainDb,
    network: Arc<RwLock<BrainNetwork>>,
    host_controller: BrainHostController,
    ephemeral_data: Arc<Mutex<EphemeralBrainData>>,
    client_push_sender: BrainClientPushSender,
    workload_join_set: Mutex<Option<JoinSet<()>>>,
    internal_data_dir: std::path::PathBuf,
    #[cfg(any(test, feature = "test-support"))]
    pub action_interceptor: Arc<Mutex<Option<ActionInterceptor>>>,
}

#[derive(Clone, Deref)]
#[deref(forward)]
pub struct GigaBrain(Arc<Brain>);

pub type BrainDb = Arc<dyn FilerequestDb>;
pub type BrainNetwork = Arc<dyn RunnableNetworkNode>;
pub type BrainHostController = Arc<dyn HostController>;
pub type BrainApiServer = Box<
    dyn RunnableFilerequestServer<
        StreamStream = DroppableStream<Result<ServerMessage, Status>, ClientMessage>,
    >,
>;
pub type BrainClientPushSender = mpsc::Sender<ServerMessage>;

impl Brain {
    #[instrument(skip_all, level = "trace")]
    pub async fn run(
        internal_data_dir: PathBuf,
        run_db: impl AsyncFn() -> BrainDb,
        host_controller_factory: impl AsyncFn() -> BrainHostController,
        network_factory: impl AsyncFn() -> BrainNetwork,
        api_server_factory: impl AsyncFnOnce() -> BrainApiServer,
        mut brain_receiver: mpsc::Receiver<BrainAction>,
        client_push_sender: BrainClientPushSender,
    ) -> (JoinHandle<()>, GigaBrain) {
        let mut workload_join_set = JoinSet::new();

        debug!("Running database");
        let db = run_db().await;
        let db_clone = db.clone();
        workload_join_set.spawn(async move { db_clone.run().await }.in_current_span());

        debug!("Creating host controller");
        let host_controller = host_controller_factory().await;

        let db_cleanup = db.clone();
        let hc_cleanup = host_controller.clone();
        workload_join_set.spawn(async move {
            loop {
                crate::core::brain::stale_cleanup::run_cleanups(db_cleanup.clone(), hc_cleanup.clone()).await;
                tokio::time::sleep(std::time::Duration::from_secs(6 * 60 * 60)).await;
            }
        }.in_current_span());

        debug!("Spawing API server");
        let api_server = api_server_factory().await;
        workload_join_set.spawn(api_server.run());

        debug!("Creating network node");
        let net = Arc::new(RwLock::new(network_factory().await));
        let workload_join_set = Mutex::new(Some(workload_join_set));

        let ephemeral_data = Arc::new(Mutex::new(EphemeralBrainData::new()));

        let giga_brain = GigaBrain(Arc::new(Brain {
            db,
            network: net.clone(),
            host_controller,
            ephemeral_data,
            client_push_sender,
            workload_join_set,
            internal_data_dir,
            #[cfg(any(test, feature = "test-support"))]
            action_interceptor: Arc::new(Mutex::new(None)),
        }));

        debug!("Spawning brain");
        let giga_brain_clone = giga_brain.clone();
        let brain_handle = tokio::spawn(
            async move {
                loop {
                    let action = brain_receiver
                        .recv()
                        .instrument(span!(Level::TRACE, "Waiting for action"))
                        .await
                        .expect("All action senders got closed (this should be impossible)");

                    let giga_brain_clone = giga_brain_clone.clone();
                    tokio::task::spawn(
                        async move { giga_brain_clone.clone().handle_action(action).await }
                            .in_current_span(),
                    );
                }
            }
            .in_current_span(),
        );
        debug!("Spawing network node");
        giga_brain
            .workload_join_set
            .timeout_lock()
            .await
            .inspect_mut_ref(|join_set| {
                join_set.spawn(
                    async move {
                        let net_guard = net.read().await;
                        net_guard.run().await;
                    }
                    .in_current_span(),
                );
            });

        (brain_handle, giga_brain)
    }
}

struct EphemeralBrainData {
    successfully_started: bool,
    queued_wait_for_ready_requests: Option<BoundedList<tokio::sync::oneshot::Sender<bool>>>,
    self_contact_invite_challenges: TtlSet<SelfContactInviteChallenge>,
    contact_share_challenges: TtlSet<ContactShareChallenge>,
}

impl EphemeralBrainData {
    fn new() -> Self {
        EphemeralBrainData {
            successfully_started: false,
            queued_wait_for_ready_requests: Some(BoundedList::new(25)),
            self_contact_invite_challenges: TtlSet::new(1.minutes()),
            contact_share_challenges: TtlSet::new(2.minutes()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::{
        core::{
            api_server::MockRunnableFilerequestServer,
            brain::{
                action_client::ClientAction, action_p2p::NetworkNodeAction, types::BrainRequest,
            },
            domain_models::{
                CompleteReceivedFile, ContactId, CreateFilerequest, CreateIncomingFile,
                FilerequestAccess, FylesId, PeerIdWrapper,
            },
            p2p::NodeStatusInfo,
        },
        io_controller::test::TestHostController,
        mocks::{db::MockDb, p2p::MockP2pNode},
    };
    use tokio::sync::{oneshot, Mutex};
    use crate::library::util::epoch::unix_epoch_millis;

    async fn setup_brain() -> (GigaBrain, mpsc::Sender<BrainAction>) {
        let out_dir_handler = Arc::new(TestHostController::new(None));
        let node_info = NodeStatusInfo {
            peer_id: PeerIdWrapper::for_test(),
            external_addresses: vec!["/ip4/127.0.0.1/tcp/8080".to_string()],
            internal_addresses: vec!["/ip4/127.0.0.1/tcp/8080".to_string()],
            connected_peers: 0,
            start_timestamp: 0,
        };

        let (stream_sender, mut receiver) = mpsc::channel(16);
        // throw away everything the receiver gets for now, to unclog the channel
        tokio::spawn(async move { while let Some(_) = receiver.recv().await {} });

        let (sender, receiver) = mpsc::channel(16);
        let brain = Brain::run(
            PathBuf::from("."),
            async || Arc::new(MockDb::new()) as _,
            async || out_dir_handler.clone() as _,
            async || Arc::new(MockP2pNode::new(node_info.clone())) as _,
            async || Box::new(MockRunnableFilerequestServer {}) as _,
            receiver,
            stream_sender,
        )
        .await;
        (brain.1, sender)
    }

    async fn create_test_filerequest(sender: &mpsc::Sender<BrainAction>) -> FylesId {
        let (response_sender, response_receiver) = oneshot::channel();
        let action = BrainAction::Client(ClientAction::CreateFilerequest(types::BrainRequest {
            request: CreateFilerequest {
                title: "Test Request".to_string(),
                description: "Test Description".to_string(),
                is_active: true,
                access: FilerequestAccess::Public,
            },
            response_sender: Mutex::new(Some(response_sender)),
        }));
        sender.send(action).await.unwrap();
        response_receiver.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn test_create_and_get_filerequest() {
        let (_, sender) = setup_brain().await;

        // First create a filerequest
        let (create_sender, create_receiver) = oneshot::channel();
        let create_action = BrainAction::Client(ClientAction::CreateFilerequest(BrainRequest {
            request: CreateFilerequest {
                title: "Test Request".to_string(),
                description: "Test Description".to_string(),
                is_active: true,
                access: FilerequestAccess::Public,
            },
            response_sender: Mutex::new(Some(create_sender)),
        }));
        sender.send(create_action).await.unwrap();

        // Get the ID from creation response
        let created_id = match create_receiver.await.unwrap().as_ref() {
            Ok(id) => id.clone(),
            Err(e) => panic!("Failed to create filerequest: {:?}", e),
        };

        // Now try to get the created filerequest
        let (get_sender, get_receiver) = oneshot::channel();
        let get_action = BrainAction::Client(ClientAction::ReadFilerequest(BrainRequest {
            request: created_id,
            response_sender: Mutex::new(Some(get_sender)),
        }));
        sender.send(get_action).await.unwrap();

        // Verify the retrieved filerequest
        match get_receiver.await.unwrap().as_ref() {
            Ok(filerequest) => {
                assert_eq!(filerequest.title, "Test Request");
                assert_eq!(filerequest.description, "Test Description");
                assert!(filerequest.is_active);
            }
            Err(e) => panic!("Failed to get filerequest: {:?}", e),
        }

        // Request all filerequests
        let (list_sender, list_receiver) = oneshot::channel();
        let list_action = BrainAction::Client(ClientAction::ListFilerequests(BrainRequest {
            request: (),
            response_sender: Mutex::new(Some(list_sender)),
        }));
        sender.send(list_action).await.unwrap();

        // Verify the list of filerequests
        match list_receiver.await.unwrap().as_ref() {
            Ok(filerequests) => {
                assert_eq!(filerequests.len(), 1);
                assert_eq!(filerequests[0].title, "Test Request");
                assert_eq!(filerequests[0].description, "Test Description");
                assert!(filerequests[0].is_active);
            }
            Err(e) => panic!("Failed to list filerequests: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_get_node_status() {
        let (_, sender) = setup_brain().await;

        let (response_sender, response_receiver) = oneshot::channel();
        let action = BrainAction::Client(ClientAction::GetNodeStatus(BrainRequest {
            request: (),
            response_sender: Mutex::new(Some(response_sender)),
        }));
        sender.send(action).await.unwrap();

        match response_receiver.await.unwrap().as_ref() {
            Ok(node_info) => {
                assert_eq!(node_info.external_addresses[0], "/ip4/127.0.0.1/tcp/8080");
                assert_eq!(node_info.connected_peers, 0);
            }
            Err(e) => panic!("Failed to get node status: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_store_received_file() {
        let (_, sender) = setup_brain().await;

        // Create test data first
        let filerequest = create_test_filerequest(&sender).await;

        // First, create an incoming file (simulates starting a transfer)
        let contact_id = ContactId::new();
        let transfer_id = FylesId::new();
        let (create_sender, create_receiver) = oneshot::channel();
        let incoming = CreateIncomingFile {
            contact_id: Some(contact_id.clone()),
            filerequest_id: filerequest.clone(),
            transfer_id: transfer_id.clone(),
            peer_id: "test-peer-id".to_string(),
            file_name: "test.txt".to_string(),
            file_size_bytes: 1234,
            started_at_ms: unix_epoch_millis().unwrap(),
        };
        let action =
            BrainAction::NetworkNode(NetworkNodeAction::CreateIncomingFile(types::BrainRequest {
                request: incoming,
                response_sender: Mutex::new(Some(create_sender)),
            }));
        sender.send(action).await.unwrap();
        create_receiver.await.unwrap().unwrap();

        // Now complete the transfer
        let (response_sender, response_receiver) = oneshot::channel();
        let complete = CompleteReceivedFile {
            transfer_id: transfer_id.clone(),
            file_path: "/tmp/test.txt".to_string(),
            received_at_ms: unix_epoch_millis().unwrap(),
        };
        let action =
            BrainAction::NetworkNode(NetworkNodeAction::StoreReceivedFile(types::BrainRequest {
                request: complete,
                response_sender: Mutex::new(Some(response_sender)),
            }));
        sender.send(action).await.unwrap();

        // Verify file was stored through Brain
        let result_id = response_receiver.await.unwrap().unwrap();

        // Now verify the file can be retrieved
        let (response_sender, response_receiver) = oneshot::channel();
        let action = BrainAction::Client(ClientAction::ListReceivedFilesForRequest(
            types::BrainRequest {
                request: filerequest.clone(),
                response_sender: Mutex::new(Some(response_sender)),
            },
        ));
        sender.send(action).await.unwrap();

        let temp = response_receiver.await.unwrap();
        let mut files = temp.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, result_id);
        assert_eq!(files[0].file_name, "test.txt");
        assert_eq!(files[0].contact_id.take().unwrap(), contact_id);
    }
}
