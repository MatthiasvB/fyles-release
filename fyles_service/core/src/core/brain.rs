use std::{collections::HashMap, path::PathBuf, sync::Arc};

use automerge::sync::State as SyncProtocolState;
use derive_more::Deref;
use futures::FutureExt;
use tokio::{
    sync::{Mutex, MutexGuard, Notify, RwLock, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::Status;
use tracing::{Instrument, Level, debug, instrument, span};

use crate::{
    BrainAction,
    core::brain::types::ContactShareChallenge,
    core::domain_models::PeerIdWrapper,
    core::sync::{DocKind, SyncId},
};
use crate::{
    core::api_server::stream_registry::DroppableStream,
    io_controller::HostController,
    library::{
        bounded_list::BoundedList,
        ttlmap::TtlSet,
        wire::{
            RunnableFilerequestServer,
            api::{ClientMessage, ServerMessage},
        },
    },
};
use crate::core::brain::client_presence::ClientPresence;
use crate::core::{brain::types::SelfContactInviteChallenge, p2p::RunnableNetworkNode};
use crate::library::util::util::TimeoutLock;

use super::db::FilerequestDb;
use crate::constants::{CONTACT_SHARE_TTL, SELF_CONTACT_INVITE_TTL, STALE_CLEANUP_INTERVAL};

pub mod action;
pub mod action_client;
pub mod action_p2p;
#[cfg(any(test, feature = "test-support"))]
pub mod action_test;
pub mod client_presence;
pub mod error;
pub mod handle_action;
pub mod stale_cleanup;
mod sweep_timer;
pub mod sync_exchange;
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
    network: BrainNetwork,
    host_controller: BrainHostController,
    ephemeral_data: Arc<Mutex<EphemeralBrainData>>,
    client_push_sender: BrainClientPushSender,
    /// Wakes the presence decay watcher, which otherwise parks indefinitely at `Idle`.
    presence_wakeup: Notify,
    tasks: TaskTracker,
    /// Cancelled before the workloads are awaited, so tasks spawned by the p2p loop stop before
    /// their counterparties disappear rather than sending into a dropped receiver.
    shutdown: CancellationToken,
    internal_data_dir: std::path::PathBuf,
    #[cfg(any(test, feature = "test-support"))]
    pub action_interceptor: Arc<Mutex<Option<ActionInterceptor>>>,
}

#[derive(Clone, Deref)]
#[deref(forward)]
pub struct GigaBrain(Arc<Brain>);

pub type BrainDb = Arc<dyn FilerequestDb>;
pub type BrainNetwork = Arc<RwLock<dyn RunnableNetworkNode>>;
pub type BrainHostController = Arc<dyn HostController>;
pub type BrainApiServer = Box<
    dyn RunnableFilerequestServer<
        StreamStream = DroppableStream<Result<ServerMessage, Status>, ClientMessage>,
    >,
>;
pub type BrainClientPushSender = mpsc::Sender<ServerMessage>;

impl Brain {
    /// Spells out the guard type, which rust-analyzer cannot infer through `timeout_lock` while
    /// `EphemeralBrainData` transitively holds an `automerge::AutoCommit`.
    /// see https://github.com/rust-lang/rust-analyzer/issues/22941
    async fn ephemeral(&self) -> MutexGuard<'_, EphemeralBrainData> {
        self.ephemeral_data.timeout_lock().await
    }

    // A dependency-injection seam; the argument count is the point.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, level = "trace")]
    pub async fn run(
        internal_data_dir: PathBuf,
        run_db: impl AsyncFn() -> BrainDb,
        host_controller_factory: impl AsyncFn() -> BrainHostController,
        network_factory: impl AsyncFn() -> BrainNetwork,
        api_server_factory: impl AsyncFnOnce() -> BrainApiServer,
        mut brain_receiver: mpsc::Receiver<BrainAction>,
        client_push_sender: BrainClientPushSender,
        shutdown: CancellationToken,
    ) -> (JoinHandle<()>, GigaBrain) {
        let tasks = TaskTracker::new();

        let db = run_db().await;

        debug!("Creating host controller");
        let host_controller = host_controller_factory().await;

        let db_cleanup = db.clone();
        let hc_cleanup = host_controller.clone();
        // Every workload is spawned through the token, so `shutdown_workloads` can wait on all of
        // them without depending on each one being cancellation-aware itself.
        let spawn = {
            let tasks = tasks.clone();
            let shutdown = shutdown.clone();
            move |fut| {
                tasks.spawn(
                    shutdown
                        .clone()
                        .run_until_cancelled_owned(fut)
                        .map(|_: Option<()>| ()),
                )
            }
        };

        spawn(
            async move {
                loop {
                    crate::core::brain::stale_cleanup::run_cleanups(
                        db_cleanup.clone(),
                        hc_cleanup.clone(),
                    )
                    .await;
                    tokio::time::sleep(STALE_CLEANUP_INTERVAL).await;
                }
            }
            .in_current_span()
            .boxed(),
        );

        debug!("Spawing API server");
        let api_server = api_server_factory().await;
        spawn(api_server.run().boxed());

        debug!("Creating network node");
        let net = network_factory().await;

        let ephemeral_data = Arc::new(Mutex::new(EphemeralBrainData::new()));

        let giga_brain = GigaBrain(Arc::new(Brain {
            db,
            network: net.clone(),
            host_controller,
            ephemeral_data,
            client_push_sender,
            presence_wakeup: Notify::new(),
            tasks,
            shutdown: shutdown.clone(),
            internal_data_dir,
            #[cfg(any(test, feature = "test-support"))]
            action_interceptor: Arc::new(Mutex::new(None)),
        }));

        debug!("Spawning brain");
        let giga_brain_clone = giga_brain.clone();
        let brain_shutdown = shutdown.clone();
        let brain_handle = tokio::spawn(
            brain_shutdown.run_until_cancelled_owned(async move {
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
            .in_current_span())
            .map(|_: Option<()>| ()),
        );
        let presence_brain = giga_brain.clone();
        spawn(
            async move { presence_brain.watch_presence_decay().await }
                .in_current_span()
                .boxed(),
        );

        debug!("Spawing network node");
        spawn(
            async move {
                let net_guard = net.read().await;
                net_guard.run().await;
            }
            .in_current_span()
            .boxed(),
        );

        (brain_handle, giga_brain)
    }

    /// Cancel every background workload and wait for it to actually stop, so the caller can rely
    /// on nothing still running against a torn-down brain.
    pub async fn shutdown_workloads(&self) {
        self.tasks.close();
        self.shutdown.cancel();
        self.tasks.wait().await;
    }
}

struct EphemeralBrainData {
    successfully_started: bool,
    queued_wait_for_ready_requests: Option<BoundedList<tokio::sync::oneshot::Sender<bool>>>,
    self_contact_invite_challenges: TtlSet<SelfContactInviteChallenge>,
    contact_share_challenges: TtlSet<ContactShareChallenge>,
    /// Per-document sync coordination, one slot per `(peer, kind)`, guarded only by the enclosing
    /// ephemeral lock (taken briefly, never across I/O). Optimistic single-flight.
    sync_slots: HashMap<(PeerIdWrapper, DocKind), ExchangeSlot>,
    /// Latch: report map growth once, not per sync step.
    sync_slot_growth_reported: bool,
    client_presence: ClientPresence,
}

pub(super) struct ExchangeSlot {
    /// Private: every transition goes through [`Self::set_state`], which stamps `state_since`.
    state: SlotState,
    /// Non-idle for longer than `SYNC_SLOT_STALL_AFTER` means an exchange missed its exit and this
    /// `(peer, doc)` is blocked for good.
    state_since: tokio::time::Instant,
    /// Latch so a wedged slot is reported once per episode rather than on every sync attempt.
    stall_reported: bool,
    /// Dirty gate: heads fingerprint last confirmed synced with the peer. Absent = "sync once".
    pub(super) last_synced: Option<Vec<u8>>,
}

impl Default for ExchangeSlot {
    fn default() -> Self {
        Self {
            state: SlotState::default(),
            state_since: tokio::time::Instant::now(),
            stall_reported: false,
            last_synced: None,
        }
    }
}

impl ExchangeSlot {
    pub(super) fn state(&self) -> &SlotState {
        &self.state
    }

    /// The only way to change `state`, so `state_since` cannot be forgotten.
    pub(super) fn set_state(&mut self, state: SlotState) -> SlotState {
        self.state_since = tokio::time::Instant::now();
        self.stall_reported = false;
        std::mem::replace(&mut self.state, state)
    }

    pub(super) fn should_report_stall(&mut self, threshold: std::time::Duration) -> bool {
        if !matches!(self.state, SlotState::Idle)
            && self.state_since.elapsed() > threshold
            && !self.stall_reported
        {
            self.stall_reported = true;
            true
        } else {
            false
        }
    }

    pub(super) fn state_age(&self) -> std::time::Duration {
        self.state_since.elapsed()
    }
}

#[derive(Default)]
pub(super) enum SlotState {
    #[default]
    Idle,
    /// An exchange parked between steps.
    Parked(Box<ActiveExchange>),
    /// A task is stepping this exchange off-lock; the `sync_id` lets a glare tie-break/supersede
    /// it and lets that task detect on re-entry whether it was superseded.
    InFlight { sync_id: SyncId },
}

pub(super) struct ActiveExchange {
    pub(super) sync_id: SyncId,
    /// Live automerge sync state — must stay in memory: `State::encode` keeps only `shared_heads`,
    /// so round-tripping it mid-exchange would reset the protocol.
    pub(super) state: SyncProtocolState,
    /// In-memory document, persisted only at convergence.
    pub(super) doc: automerge::AutoCommit,
}

impl EphemeralBrainData {
    fn new() -> Self {
        EphemeralBrainData {
            successfully_started: false,
            queued_wait_for_ready_requests: Some(BoundedList::new(25)),
            self_contact_invite_challenges: TtlSet::new(SELF_CONTACT_INVITE_TTL),
            contact_share_challenges: TtlSet::new(CONTACT_SHARE_TTL),
            sync_slots: Default::default(),
            sync_slot_growth_reported: false,
            client_presence: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::constants::{CLIENT_HEARTBEAT_TTL, SWEEP_INTERVAL_ACTIVE, SWEEP_JITTER_FRACTION};
    use crate::core::db::test::setup_test_db;
    use crate::core::sync;
    use crate::library::util::epoch::unix_epoch_millis;
    use crate::{
        core::{
            api_server::MockRunnableFilerequestServer,
            brain::{
                action_client::ClientAction, action_p2p::NetworkNodeAction, types::BrainRequest,
            },
            domain_models::{
                CompleteReceivedFile, ContactId, CreateFilerequest, CreateIncomingFile,
                FilerequestAccess, FilerequestLocation, FylesId, PeerIdWrapper,
            },
            p2p::NodeStatusInfo,
        },
        io_controller::test::TestHostController,
        mocks::p2p::MockP2pNode,
    };
    use tokio::sync::{Mutex, oneshot};

    async fn setup_brain() -> (GigaBrain, mpsc::Sender<BrainAction>) {
        let (brain, sender, _, _) = setup_brain_with_node().await;
        (brain, sender)
    }

    async fn setup_brain_with_node() -> (
        GigaBrain,
        mpsc::Sender<BrainAction>,
        Arc<MockP2pNode>,
        CancellationToken,
    ) {
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

        let node = Arc::new(MockP2pNode::new(node_info.clone()));
        let node_for_factory = node.clone();
        let shutdown = CancellationToken::new();
        let (sender, receiver) = mpsc::channel(16);
        let brain = Brain::run(
            PathBuf::from("."),
            async || setup_test_db(None).await,
            async || out_dir_handler.clone() as _,
            async || Arc::new(RwLock::new(MockP2pNode::clone(&node_for_factory))) as _,
            async || Box::new(MockRunnableFilerequestServer {}) as _,
            receiver,
            stream_sender,
            shutdown.clone(),
        )
        .await;

        // Trigger identity generation
        let (request, response) = BrainRequest::with_receiver(());
        sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::GetNodeInfo(
                request,
            )))
            .await
            .unwrap();
        response.await.unwrap().unwrap();

        (brain.1, sender, node, shutdown)
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
    async fn test_resolve_filerequest_location() {
        let (_, sender) = setup_brain().await;

        // A title with characters that filename sanitization rewrites, so the
        // resolved folder name is not trivially equal to the title.
        let title = "Holiday: 2026/Trip".to_string();
        let (create_sender, create_receiver) = oneshot::channel();
        sender
            .send(BrainAction::Client(ClientAction::CreateFilerequest(
                BrainRequest {
                    request: CreateFilerequest {
                        title: title.clone(),
                        description: String::new(),
                        is_active: true,
                        access: FilerequestAccess::Public,
                    },
                    response_sender: Mutex::new(Some(create_sender)),
                },
            )))
            .await
            .unwrap();
        let id = create_receiver.await.unwrap().unwrap();

        let (resolve_sender, resolve_receiver) = oneshot::channel();
        sender
            .send(BrainAction::Client(
                ClientAction::ResolveFilerequestLocation(BrainRequest {
                    request: id,
                    response_sender: Mutex::new(Some(resolve_sender)),
                }),
            ))
            .await
            .unwrap();

        // The test host controller exposes no absolute drop dir (scoped-storage
        // behaviour), so the resolver yields a Relative location matching exactly
        // the directory name the drive handler creates on disk.
        match resolve_receiver.await.unwrap().unwrap() {
            FilerequestLocation::Relative(path) => {
                assert_eq!(path, sanitize_filename::sanitize(&title));
                assert!(!path.is_empty());
            }
            FilerequestLocation::Absolute(path) => {
                panic!("expected a Relative location, got Absolute({path})");
            }
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
            persisted_as_name: "test.txt".to_string(),
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
            persisted_as_name: "test.txt".to_string(),
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

    /// A local write hands the owned-device list to the network exactly once, tagged with the
    /// document that changed. The network decides who of them is reachable.
    #[tokio::test]
    async fn a_local_write_announces_the_owned_devices_once() {
        use crate::core::domain_models::Contact;
        use crate::core::sync::{DocKind, add_or_update_owned_device};

        let (brain, sender, node, _shutdown) = setup_brain_with_node().await;
        let device = PeerIdWrapper::for_test();
        let _ = add_or_update_owned_device(brain.db.as_ref(), device.clone(), "Other".into())
            .await
            .unwrap();
        // That registration announces too; only the contact write below is under test.
        node.take_sync_with_reachable_calls();

        let (response_sender, response) = oneshot::channel();
        sender
            .send(BrainAction::Client(ClientAction::RegisterContact(
                BrainRequest {
                    request: Contact::for_test(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .unwrap();
        response.await.unwrap().unwrap();

        // `announce` is spawned, so give it a turn to land.
        for _ in 0..50 {
            if !node.sync_with_reachable_calls().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let calls = node.take_sync_with_reachable_calls();
        assert_eq!(calls.len(), 1, "expected exactly one announcement");
        let (devices, kind) = &calls[0];
        assert_eq!(kind, &DocKind::Contacts, "only the changed document");
        assert!(
            devices.contains(&device),
            "the owned device should be handed to the network to filter"
        );
    }

    /// `shutdown_workloads` must not return until every workload has stopped, including ones that
    /// never poll the token themselves.
    #[tokio::test]
    async fn shutdown_waits_for_workloads_to_stop() {
        let (brain, _sender, _node, _shutdown) = setup_brain_with_node().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), brain.shutdown_workloads())
            .await
            .expect("shutdown_workloads should not hang on a non-cancellation-aware workload");
        assert!(brain.tasks.is_empty(), "a workload outlived the shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn client_heartbeats_drive_the_sweep_cadence() {
        /// Many short parks rather than one long one. What has to finish hops across spawned
        /// handlers and contended `timeout_lock`s, all of which register timers, so a single wake
        /// can be the earliest deadline and resume the test mid-chain. The paused clock makes the
        /// extra iterations free, and 50ms stays far below the decay deadline.
        async fn settle() {
            for _ in 0..50 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        let (_brain, sender, node, _shutdown) = setup_brain_with_node().await;

        for _ in 0..5 {
            sender
                .send(BrainAction::Client(ClientAction::ClientHeartbeat {
                    foreground: true,
                }))
                .await
                .unwrap();
        }
        settle().await;
        assert_eq!(
            node.sweep_calls(),
            1,
            "the rise to Active sweeps at once; redundant beats must not rebuild the timer"
        );

        // One full period, jitter included — and comfortably inside the TTL, so the level is still
        // Active and this measures the fast cadence alone.
        tokio::time::advance(SWEEP_INTERVAL_ACTIVE.mul_f64(1.0 + SWEEP_JITTER_FRACTION)).await;
        settle().await;
        assert_eq!(node.sweep_calls(), 2, "an Active client keeps being swept");

        // The falling edge is what needs a watcher: no beat arrives to carry it. The exact count
        // across this jump is not pinned — `MissedTickBehavior::Delay` coalesces the periods it
        // spans, and whether the last one lands before the decay is a `select!` coin flip.
        tokio::time::advance(CLIENT_HEARTBEAT_TTL * 2).await;
        settle().await;
        let before_idle = node.sweep_calls();

        tokio::time::advance(SWEEP_INTERVAL_ACTIVE * 5).await;
        settle().await;
        assert_eq!(
            node.sweep_calls(),
            before_idle,
            "the level decayed to Idle, so five Active periods must now pass without a sweep"
        );
    }

    /// Two owned devices must *settle* after syncing: the `Sync{None}` handshake has to converge
    /// on both sides within a bounded number of steps. We drive `sync_advance` directly (the
    /// automerge half of the protocol), shuttling each side's output into the other, and stop
    /// once both sides have persisted. A regression that made the exchange never converge (the
    /// production infinite loop) would exhaust the bound instead.
    #[tokio::test]
    async fn sync_between_owned_devices_settles() {
        use crate::core::sync::DocKind;

        let (a, _sender_a) = setup_brain().await;
        let (b, _sender_b) = setup_brain().await;
        // Each brain keys its own slot map by the *other* peer; the concrete id is irrelevant.
        let peer = PeerIdWrapper::for_test();

        // A initiates; the message then bounces between the two. In the real handlers a converged
        // responder answers `NoMoreToSay` (a terminal that never re-enters `sync_advance`); here
        // we detect the same end by watching both sides reach convergence.
        let (mut msg, _) = a
            .sync_advance_for_test(&peer, DocKind::Contacts, None)
            .await
            .expect("initiate")
            .expect("opening message");
        let mut deliver_to_b = true;
        let mut a_converged = false;
        let mut b_converged = false;

        for _ in 0..20 {
            let target = if deliver_to_b { &b } else { &a };
            let (out, converged) = target
                .sync_advance_for_test(&peer, DocKind::Contacts, Some(msg))
                .await
                .expect("step")
                .expect("a live 2-party exchange never bails");
            if converged {
                if deliver_to_b {
                    b_converged = true;
                } else {
                    a_converged = true;
                }
            }
            if a_converged && b_converged {
                return; // settled: both sides persisted
            }
            msg = out;
            deliver_to_b = !deliver_to_b;
        }

        panic!("runaway sync loop: the exchange never settled within 20 steps");
    }

    /// Both ends must *persist*, not just converge in memory.
    ///
    /// The initiator converges one step ahead of the responder, which reaches quiescence on the
    /// step that applies the initiator's last changes — but its incoming was non-empty there, so
    /// only the initiator's terminal empty message makes it converge and write. An initiator that
    /// withholds that message leaves the responder's merge in memory, and the initiator's own
    /// dirty gate (now recorded as synced) suppresses every later attempt, so the change is lost
    /// until a sweep. Drives the real handlers rather than `sync_advance` so the terminal frame is
    /// actually part of what is under test.
    #[tokio::test]
    async fn a_converged_exchange_persists_on_both_sides() {
        use crate::core::domain_models::Contact;
        use crate::core::sync::DocKind;

        async fn synced_contacts(brain: &GigaBrain) -> Vec<ContactId> {
            let blob = brain.db.get_sync_doc(DocKind::Contacts).await.unwrap();
            let doc = sync::load(&blob).unwrap();
            let view: sync::ContactsDoc = sync::hydrate(&doc).unwrap();
            view.contacts.into_keys().collect()
        }

        async fn register(sender: &mpsc::Sender<BrainAction>, contact: Contact) {
            let (request, response) = BrainRequest::with_receiver(contact);
            sender
                .send(BrainAction::Client(ClientAction::RegisterContact(request)))
                .await
                .unwrap();
            response.await.unwrap().unwrap();
        }

        let (a, sender_a, node_a, _shutdown_a) = setup_brain_with_node().await;
        let (b, sender_b, _node_b, _shutdown_b) = setup_brain_with_node().await;

        // One contact per side, so convergence has something to carry in both directions.
        let contact_a = Contact::for_test();
        let contact_b = Contact::for_test();
        register(&sender_a, contact_a.clone()).await;
        register(&sender_b, contact_b.clone()).await;

        let peer = PeerIdWrapper::for_test();
        let b_self_id = b.db.get_self_contact_id().await.unwrap();

        // A initiates and B answers; every frame A emits goes back to B until A emits none, which
        // is the exchange closing.
        a.initiate_sync_if_dirty(&peer, DocKind::Contacts).await;
        let mut steps = 0;
        while let Some((_, request)) = node_a.pop_sync_message() {
            steps += 1;
            assert!(steps < 20, "runaway sync loop: the exchange never closed");
            let reply = b
                .handle_sync_receive_for_test(b_self_id.clone(), peer.clone(), request)
                .await
                .expect("a peer on the same identity is never declined");
            a.handle_sync_reply(peer.clone(), reply).await;
        }

        for (label, brain) in [("initiator", &a), ("responder", &b)] {
            let synced = synced_contacts(brain).await;
            assert!(
                synced.contains(&contact_a.id) && synced.contains(&contact_b.id),
                "{label} should have persisted both contacts, has {synced:?}"
            );
        }
    }

    /// A `Sync` request for a document the responder can't handle — an unknown `doc_kind` (as a
    /// newer peer's new document would appear to an older one) or an incompatible `schema_major`
    /// — is declined with `DocumentVersionNotSupported`, never a panic or a fresh exchange.
    #[tokio::test]
    async fn sync_receive_declines_unsupported_document() {
        use crate::core::sync::{DocKind, SyncPayload, SyncRequest, SyncResponse};

        let (a, _sender) = setup_brain().await;
        let self_id = a.db.get_self_contact_id().await.unwrap();
        let peer = PeerIdWrapper::for_test();

        let unknown = SyncRequest::Sync(SyncPayload {
            doc_kind: "a_document_this_version_has_never_heard_of".to_string(),
            schema_major: 1,
            message: Some(vec![1, 2, 3]),
            sync_id: sync::SyncId::new(),
        });
        assert!(
            matches!(
                a.handle_sync_receive_for_test(self_id.clone(), peer.clone(), unknown)
                    .await,
                Some(SyncResponse::DocumentVersionNotSupported { .. })
            ),
            "an unknown document kind must be declined"
        );

        let bad_schema = SyncRequest::Sync(SyncPayload {
            doc_kind: DocKind::Contacts.as_str().to_string(),
            schema_major: DocKind::Contacts.schema_major() + 1,
            message: Some(vec![1, 2, 3]),
            sync_id: sync::SyncId::new(),
        });
        assert!(
            matches!(
                a.handle_sync_receive_for_test(self_id, peer, bad_schema)
                    .await,
                Some(SyncResponse::DocumentVersionNotSupported { .. })
            ),
            "an incompatible schema major must be declined"
        );
    }
}
