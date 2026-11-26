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

use crate::core::brain::client_presence::ClientPresence;
use crate::core::brain::plugin::BrainPluginRef;
use crate::core::listen::{ListenPortChoice, ListenStatus};
use crate::core::{brain::types::SelfContactInviteChallenge, p2p::RunnableNetworkNode};
use crate::library::util::util::TimeoutLock;
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

use super::db::FilerequestDb;
use crate::constants::{CONTACT_SHARE_TTL, SELF_CONTACT_INVITE_TTL, STALE_CLEANUP_INTERVAL};

pub mod action;
pub mod action_client;
pub mod action_p2p;
pub mod action_plugin;
#[cfg(any(test, feature = "test-support"))]
pub mod action_test;
mod backup;
pub mod client_presence;
pub mod client_push;
#[cfg(feature = "debug-server")]
mod debug_server;
pub mod error;
pub mod handle_action;
mod host_state;
mod listen_port;
pub mod plugin;
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
    plugin: BrainPluginRef,
    ephemeral_data: Arc<Mutex<EphemeralBrainData>>,
    client_push_sender: BrainClientPushSender,
    /// Wakes the presence decay watcher, which otherwise parks indefinitely at `Idle`.
    presence_wakeup: Notify,
    /// `None` until the node first reported ready. Held across every bind, so changes apply one
    /// at a time.
    listen_status: Mutex<Option<ListenStatus>>,
    listen_wakeup: Notify,
    /// A restore restarts the node, which would cut a running export short, so backups and
    /// restores run one at a time.
    backup_files: Mutex<()>,
    tasks: TaskTracker,
    end: mpsc::Sender<GenerationEnd>,
    /// Cancelled before the workloads are awaited, so tasks spawned by the p2p loop stop before
    /// their counterparties disappear rather than sending into a dropped receiver.
    shutdown: CancellationToken,
    internal_data_dir: PathBuf,
    listen_port: ListenPortChoice,
    #[cfg(any(test, feature = "test-support"))]
    pub action_interceptor: Arc<Mutex<Option<ActionInterceptor>>>,
}

#[derive(Debug, Clone)]
pub struct NodeSettings {
    pub internal_data_dir: PathBuf,
    pub listen_port: ListenPortChoice,
}

impl NodeSettings {
    pub fn with_stored_port(internal_data_dir: PathBuf) -> Self {
        Self {
            internal_data_dir,
            listen_port: ListenPortChoice::Stored,
        }
    }
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

/// How a brain's generation ended. Every generation is built from scratch, so a restart starts on
/// whatever the database holds by then.
#[derive(Debug)]
pub enum GenerationEnd {
    Stop,
    Restart,
}

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
        settings: NodeSettings,
        run_db: impl AsyncFn() -> BrainDb,
        host_controller_factory: impl AsyncFn() -> BrainHostController,
        network_factory: impl AsyncFn() -> BrainNetwork,
        plugin_factory: impl AsyncFn() -> BrainPluginRef,
        api_server_factory: impl AsyncFnOnce() -> BrainApiServer,
        mut brain_receiver: mpsc::Receiver<BrainAction>,
        brain_sender: mpsc::Sender<BrainAction>,
        client_push_sender: BrainClientPushSender,
        shutdown: CancellationToken,
    ) -> (JoinHandle<GenerationEnd>, GigaBrain) {
        let tasks = TaskTracker::new();

        let db = run_db().await;

        debug!("Creating host controller");
        let host_controller = host_controller_factory().await;

        debug!("Creating brain plugin");
        let plugin = plugin_factory().await;

        let db_cleanup = db.clone();
        let hc_cleanup = host_controller.clone();
        // Every workload is spawned through the token, so `shutdown_workloads` can wait on all of
        // them without depending on each one being cancellation-aware itself.
        let spawn = {
            let tasks = tasks.clone();
            let shutdown = shutdown.clone();
            move |fut| {
                tasks.spawn(shutdown.clone().run_until_cancelled_owned(fut));
            }
        };

        debug!("Spawning cleanup task");
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
        // Not cancelled from outside: the server shuts down gracefully on the token, so the
        // socket is released and in-flight answers are delivered before the generation ends.
        tasks.spawn(api_server.run());

        debug!("Creating network node");
        let net = network_factory().await;

        let ephemeral_data = Arc::new(Mutex::new(EphemeralBrainData::new()));
        let (end, mut end_receiver) = mpsc::channel(1);

        let giga_brain = GigaBrain(Arc::new(Brain {
            db,
            network: net.clone(),
            host_controller,
            plugin: plugin.clone(),
            ephemeral_data,
            client_push_sender,
            presence_wakeup: Notify::new(),
            listen_status: Mutex::new(None),
            listen_wakeup: Notify::new(),
            backup_files: Mutex::new(()),
            tasks,
            end,
            shutdown: shutdown.clone(),
            internal_data_dir: settings.internal_data_dir,
            listen_port: settings.listen_port,
            #[cfg(any(test, feature = "test-support"))]
            action_interceptor: Arc::new(Mutex::new(None)),
        }));

        debug!("Spawning brain");
        let giga_brain_clone = giga_brain.clone();
        let spawn_handler = spawn.clone();
        let brain_handle = tokio::spawn(
            async move {
                let end = loop {
                    tokio::select! {
                        end = end_receiver.recv() => {
                            break end.expect("the brain to hold its own end sender");
                        }
                        () = giga_brain_clone.shutdown.cancelled() => break GenerationEnd::Stop,
                        action = brain_receiver
                            .recv()
                            .instrument(span!(Level::TRACE, "Waiting for action")) => {
                            let action = action.expect(
                                "All action senders got closed (this should be impossible)",
                            );
                            let brain = giga_brain_clone.clone();
                            spawn_handler(
                                async move { brain.handle_action(action).await }
                                    .in_current_span()
                                    .boxed(),
                            );
                        }
                    }
                };
                debug!(?end, "Ending the brain's generation");
                let teardown = giga_brain_clone.shutdown_workloads();
                tokio::pin!(teardown);
                loop {
                    tokio::select! {
                        () = &mut teardown => break,
                        // Nobody handles actions any more, but callers outside the tracker, such
                        // as API calls in flight, may still send them. Once the channel is full
                        // they would wait forever, and the teardown waits for them. Dropping what
                        // arrives lets them fail fast instead.
                        Some(_) = brain_receiver.recv() => {}
                    }
                }
                end
            }
            .in_current_span(),
        );
        // Subscribed before `start`, so a reading that lands between the two is still delivered
        // to the watcher rather than lost.
        let host_state = giga_brain.host_controller.host_state();
        debug!("Starting brain plugin");
        let reading = *host_state.borrow();
        plugin.write().await.start(reading).await;

        let presence_brain = giga_brain.clone();
        debug!("Spawning client presence watcher");
        spawn(
            async move { presence_brain.watch_presence_decay().await }
                .in_current_span()
                .boxed(),
        );

        let listen_brain = giga_brain.clone();
        debug!("Spawning listen status watcher");
        spawn(
            async move { listen_brain.watch_listen_status().await }
                .in_current_span()
                .boxed(),
        );

        let host_state_brain = giga_brain.clone();
        debug!("Spawning host state watcher");
        spawn(
            async move { host_state_brain.watch_host_state(host_state).await }
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

        #[cfg(feature = "debug-server")]
        {
            use crate::core::brain::debug_server::run_debug_server;
            debug!("Spawning debug server");
            spawn(run_debug_server(brain_sender).boxed());
        }
        #[cfg(not(feature = "debug-server"))]
        drop(brain_sender);

        (brain_handle, giga_brain)
    }

    /// Cancel every background workload and wait for it to actually stop, so the caller can rely
    /// on nothing still running against a torn-down brain.
    pub async fn shutdown_workloads(&self) {
        self.tasks.close();
        self.shutdown.cancel();
        self.tasks.wait().await;
    }

    /// A generation ends once. Asking again while an end is pending changes nothing.
    pub(super) fn end_generation(&self, end: GenerationEnd) {
        let _ = self.end.try_send(end);
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
    use crate::constants::{
        CLIENT_HEARTBEAT_TTL, LISTEN_PORT_PICKS, RANDOM_LISTEN_PORTS, SWEEP_INTERVAL_ACTIVE,
        SWEEP_JITTER_FRACTION,
    };
    use crate::core::brain::error::FilerequestError;
    use crate::core::brain::types::{ContactRegistration, SignedContactShare};
    use crate::core::db::test::setup_test_db;
    use crate::core::listen::{ListenFailure, ListenPort, ListenReport, PortSetBy, TemporaryPort};
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
                FilerequestAccess, FilerequestLocation, FylesId, PeerIdWrapper, ReceiveStatus,
            },
            p2p::NodeStatusInfo,
        },
        io_controller::test::TestHostController,
        mocks::{p2p::MockP2pNode, plugin::MockBrainPlugin},
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
        let (brain, sender, node, shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        // Unclog the channel for tests that don't care what the clients are told.
        tokio::spawn(async move { while pushes.recv().await.is_some() {} });
        (brain, sender, node, shutdown)
    }

    async fn setup_brain_watching_pushes() -> (
        GigaBrain,
        mpsc::Sender<BrainAction>,
        Arc<MockP2pNode>,
        CancellationToken,
        mpsc::Receiver<ServerMessage>,
        Arc<TestHostController>,
        MockBrainPlugin,
    ) {
        setup_brain_watching_pushes_with(ListenPortChoice::Stored).await
    }

    async fn setup_brain_watching_pushes_with(
        listen_port: ListenPortChoice,
    ) -> (
        GigaBrain,
        mpsc::Sender<BrainAction>,
        Arc<MockP2pNode>,
        CancellationToken,
        mpsc::Receiver<ServerMessage>,
        Arc<TestHostController>,
        MockBrainPlugin,
    ) {
        let host_controller = Arc::new(TestHostController::new(None));
        let out_dir_handler = host_controller.clone();
        let node_info = NodeStatusInfo {
            peer_id: PeerIdWrapper::for_test(),
            external_addresses: vec!["/ip4/127.0.0.1/tcp/8080".to_string()],
            internal_addresses: vec!["/ip4/127.0.0.1/tcp/8080".to_string()],
            connected_peers: 0,
            start_timestamp: 0,
        };

        let (stream_sender, pushes) = mpsc::channel(16);

        let node = Arc::new(MockP2pNode::new(node_info.clone()));
        let node_for_factory = node.clone();
        let plugin = MockBrainPlugin::default();
        let plugin_for_factory = plugin.clone();
        let shutdown = CancellationToken::new();
        let (sender, receiver) = mpsc::channel(16);
        let brain = Brain::run(
            NodeSettings {
                internal_data_dir: PathBuf::from("."),
                listen_port,
            },
            async || setup_test_db(None).await,
            async || out_dir_handler.clone() as _,
            async || Arc::new(RwLock::new(MockP2pNode::clone(&node_for_factory))) as _,
            async || Arc::new(RwLock::new(plugin_for_factory.clone())) as _,
            async || Box::new(MockRunnableFilerequestServer {}) as _,
            receiver,
            sender.clone(),
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

        (
            brain.1,
            sender,
            node,
            shutdown,
            pushes,
            host_controller,
            plugin,
        )
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
        let contact_id = ContactId::for_test(1);
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

    async fn start_incoming_transfer(
        sender: &mpsc::Sender<BrainAction>,
        filerequest_id: FylesId,
        contact_id: Option<ContactId>,
    ) -> FylesId {
        let transfer_id = FylesId::new();
        let (create_sender, create_receiver) = oneshot::channel();
        sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::CreateIncomingFile(types::BrainRequest {
                    request: CreateIncomingFile {
                        contact_id,
                        filerequest_id,
                        transfer_id: transfer_id.clone(),
                        peer_id: "test-peer-id".to_string(),
                        file_name: "abandoned.txt".to_string(),
                        persisted_as_name: "abandoned.txt".to_string(),
                        file_size_bytes: 1234,
                        started_at_ms: unix_epoch_millis().unwrap(),
                    },
                    response_sender: Mutex::new(Some(create_sender)),
                }),
            ))
            .await
            .unwrap();
        create_receiver.await.unwrap().unwrap();
        transfer_id
    }

    #[tokio::test]
    async fn a_withdrawn_incoming_transfer_is_removed() {
        let (brain, sender) = setup_brain().await;
        let filerequest = create_test_filerequest(&sender).await;
        let contact_id = Some(ContactId::for_test(1));
        let transfer_id = start_incoming_transfer(&sender, filerequest, contact_id.clone()).await;

        brain
            .handle_action(BrainAction::NetworkNode(
                NetworkNodeAction::DropIncomingTransfer {
                    transfer_id: transfer_id.clone(),
                    peer_id: "test-peer-id".to_string(),
                    contact_id,
                },
            ))
            .await;

        assert!(
            brain
                .db
                .get_received_file_by_transfer_id(&transfer_id)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn another_contact_cannot_withdraw_a_transfer() {
        let (brain, sender) = setup_brain().await;
        let filerequest = create_test_filerequest(&sender).await;
        let transfer_id =
            start_incoming_transfer(&sender, filerequest, Some(ContactId::for_test(1))).await;

        brain
            .handle_action(BrainAction::NetworkNode(
                NetworkNodeAction::DropIncomingTransfer {
                    transfer_id: transfer_id.clone(),
                    peer_id: "test-peer-id".to_string(),
                    contact_id: Some(ContactId::for_test(2)),
                },
            ))
            .await;

        assert_eq!(
            brain
                .db
                .get_received_file_by_transfer_id(&transfer_id)
                .await
                .unwrap()
                .status,
            ReceiveStatus::Receiving
        );
    }

    #[tokio::test]
    async fn a_user_edited_name_replaces_the_one_the_share_attests_to() {
        let (brain, sender, _node, _shutdown) = setup_brain_with_node().await;
        let share = SignedContactShare::for_test();
        let id = share.contact_unverified().id();
        let attested_name = share.contact_unverified().name.clone();

        let (response_sender, response) = oneshot::channel();
        sender
            .send(BrainAction::Client(ClientAction::RegisterContact(
                BrainRequest {
                    request: ContactRegistration {
                        share,
                        user_edited_name: Some("My name for them".into()),
                    },
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .unwrap();
        response.await.unwrap().unwrap();

        let stored = brain.db.get_contact(&id).await.unwrap();
        assert_eq!(stored.name, "My name for them");
        assert_ne!(stored.name, attested_name);
        assert_eq!(
            stored.id(),
            id,
            "renaming must not move the contact off the id its keys derive"
        );
    }

    #[tokio::test]
    async fn decoding_reports_the_signed_contact_and_refuses_a_forged_one() {
        let (_brain, sender, _node, _shutdown) = setup_brain_with_node().await;
        let share = SignedContactShare::for_test();
        let expected = share.contact_unverified().clone();

        let (response_sender, response) = oneshot::channel();
        sender
            .send(BrainAction::Client(ClientAction::DecodeSignedContact(
                BrainRequest {
                    request: share,
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .unwrap();
        let decoded = response.await.unwrap().expect("a valid share to decode");
        assert_eq!(decoded.id, expected.id());
        assert_eq!(decoded.name, expected.name);

        let (response_sender, response) = oneshot::channel();
        sender
            .send(BrainAction::Client(ClientAction::DecodeSignedContact(
                BrainRequest {
                    request: SignedContactShare::forged_for_test(expected),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .unwrap();
        assert!(
            response.await.unwrap().is_none(),
            "a share not signed by the keys it carries must not decode"
        );
    }

    /// A local write hands the owned-device list to the network exactly once, tagged with the
    /// document that changed. The network decides who of them is reachable.
    #[tokio::test]
    async fn a_local_write_announces_the_owned_devices_once() {
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
                    request: SignedContactShare::for_test().into(),
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

    async fn await_host_state(plugin: &MockBrainPlugin, wanted: crate::io_controller::HostState) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !plugin.host_states().contains(&wanted) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the plugin should have been told about {wanted:?}"));
    }

    /// The reading arrives with `start`, not as a change, so the network node can be built from a
    /// correct config the first time instead of being rebuilt the moment the watcher catches up.
    #[tokio::test]
    async fn the_startup_host_reading_arrives_with_start() {
        use crate::io_controller::HostState;

        let (_brain, _sender, _node, _shutdown, mut pushes, _host, plugin) =
            setup_brain_watching_pushes().await;
        tokio::spawn(async move { while pushes.recv().await.is_some() {} });

        assert_eq!(plugin.started_with(), vec![HostState::UNSUPPORTED]);
        assert!(
            plugin.host_states().is_empty(),
            "a host that never moved is not a change: {:?}",
            plugin.host_states()
        );
    }

    /// The daemon has to notice the host moving under it even with no client attached, so the
    /// signal travels host controller → brain → plugin rather than over the client stream.
    #[tokio::test]
    async fn a_host_state_change_reaches_the_plugin() {
        use crate::io_controller::{
            HostSignal, HostState, NetworkCost, NetworkStatus, PowerSource,
        };

        let (_brain, _sender, _node, _shutdown, mut pushes, host, plugin) =
            setup_brain_watching_pushes().await;
        tokio::spawn(async move { while pushes.recv().await.is_some() {} });

        let unplugged = HostState {
            power: HostSignal::Known(PowerSource::Battery),
            network: HostSignal::Known(NetworkStatus::Present(NetworkCost::Unmetered)),
        };
        host.set_host_state(unplugged);

        await_host_state(&plugin, unplugged).await;
    }

    /// The plugin, not the network node, is what a settings mutation reaches, and its answer is
    /// both the RPC reply and what every attached window is pushed.
    #[tokio::test]
    async fn a_settings_mutation_goes_through_the_plugin() {
        use crate::core::settings::{SettingsMutation, SettingsView};
        use crate::library::wire::api::server_message::Message;

        let (_brain, sender, node, _shutdown, mut pushes, _host, plugin) =
            setup_brain_watching_pushes().await;
        plugin.set_view(b"the-view".to_vec());

        let (request, response) =
            BrainRequest::with_receiver(SettingsMutation(b"the-request".to_vec()));
        sender
            .send(BrainAction::Client(ClientAction::MutateSettings(request)))
            .await
            .unwrap();

        assert_eq!(
            response.await.unwrap().unwrap(),
            SettingsView(b"the-view".to_vec())
        );
        assert_eq!(plugin.mutations(), vec![b"the-request".to_vec()]);
        // The node is not in this path at all any more.
        assert!(node.restarts().is_empty());

        let pushed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(message) = pushes.recv().await {
                if let Some(Message::SettingsChanged(changed)) = message.message {
                    return changed.view;
                }
            }
            panic!("the push channel closed before a settings push arrived");
        })
        .await
        .expect("a settings mutation should push the resulting view");
        assert_eq!(pushed, b"the-view".to_vec());
    }

    /// The plugin's only way to reach the network node, so a restart it asks for has to land.
    #[tokio::test]
    async fn a_plugin_restart_reaches_the_network_node() {
        use crate::core::brain::action_plugin::PluginAction;
        use crate::core::p2p::NodeConfig;

        let (_brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        tokio::spawn(async move { while pushes.recv().await.is_some() {} });

        sender
            .send(BrainAction::Plugin(PluginAction::RestartNetwork(
                NodeConfig(b"the-config".to_vec()),
            )))
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while node.restarts().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the brain should restart the network node when the plugin asks");
        assert_eq!(node.restarts(), vec![NodeConfig(b"the-config".to_vec())]);
    }

    /// A window that issued no request still learns about the change. Without this, a mutation made
    /// in one desktop window stays invisible in every other one, which is the whole reason clients
    /// no longer build state out of their own replies.
    #[tokio::test]
    async fn a_local_write_pushes_the_new_state_to_clients() {
        use crate::library::wire::api::server_message::Message;

        let (_brain, sender, _node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;

        let (response_sender, response) = oneshot::channel();
        sender
            .send(BrainAction::Client(ClientAction::RegisterContact(
                BrainRequest {
                    request: SignedContactShare::for_test().into(),
                    response_sender: Mutex::new(Some(response_sender)),
                },
            )))
            .await
            .unwrap();
        response.await.unwrap().unwrap();

        // Self-registration pushed its own devices first, so look past whatever precedes it.
        let upserted = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(message) = pushes.recv().await {
                if let Some(Message::ContactUpserted(upserted)) = message.message {
                    return upserted.contact;
                }
            }
            panic!("the push channel closed before a contact push arrived");
        })
        .await
        .expect("registering a contact should push the registered contact");

        assert!(
            upserted.is_some(),
            "the push should carry the contact that was registered"
        );
    }

    /// `shutdown_workloads` must not return until every workload has stopped, including ones that
    /// never poll the token themselves.
    #[tokio::test]
    async fn shutdown_waits_for_workloads_to_stop() {
        let (brain, _sender, _node, _shutdown) = setup_brain_with_node().await;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            brain.shutdown_workloads(),
        )
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
        use crate::core::sync::DocKind;

        async fn synced_contacts(brain: &GigaBrain) -> Vec<ContactId> {
            let blob = brain.db.get_sync_doc(DocKind::Contacts).await.unwrap();
            let doc = sync::load(&blob).unwrap();
            let view: sync::ContactsDoc = sync::hydrate(&doc).unwrap();
            view.contacts.into_keys().collect()
        }

        async fn register(sender: &mpsc::Sender<BrainAction>, share: SignedContactShare) {
            let (request, response) = BrainRequest::with_receiver(share.into());
            sender
                .send(BrainAction::Client(ClientAction::RegisterContact(request)))
                .await
                .unwrap();
            response.await.unwrap().unwrap();
        }

        let (a, sender_a, node_a, _shutdown_a) = setup_brain_with_node().await;
        let (b, sender_b, _node_b, _shutdown_b) = setup_brain_with_node().await;

        // One contact per side, so convergence has something to carry in both directions.
        let share_a = SignedContactShare::for_test();
        let share_b = SignedContactShare::for_test();
        let contact_a = share_a.contact_unverified().id();
        let contact_b = share_b.contact_unverified().id();
        register(&sender_a, share_a).await;
        register(&sender_b, share_b).await;

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
                synced.contains(&contact_a) && synced.contains(&contact_b),
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

    fn listen_port(port: u16) -> ListenPort {
        ListenPort::try_from(port).unwrap()
    }

    async fn report_ready(sender: &mpsc::Sender<BrainAction>) {
        sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::Ready))
            .await
            .unwrap();
    }

    async fn await_listen_push(pushes: &mut mpsc::Receiver<ServerMessage>) {
        use crate::library::wire::api::server_message::Message;

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while let Some(message) = pushes.recv().await {
                if let Some(Message::ListenStatusChanged(_)) = message.message {
                    return;
                }
            }
            panic!("the push channel closed before a listen status arrived");
        })
        .await
        .expect("a new listen status should be pushed");
    }

    fn assert_random_pick(port: ListenPort) {
        assert!(
            RANDOM_LISTEN_PORTS.contains(&port.get()),
            "{port} is no random pick"
        );
    }

    #[tokio::test]
    async fn a_fresh_install_keeps_the_first_port_that_binds() {
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        let mut first = true;
        node.set_listen_outcome(move |_| {
            if std::mem::take(&mut first) {
                Err(ListenFailure::AddressInUse)
            } else {
                Ok(())
            }
        });

        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        let calls = node.listen_calls();
        assert_eq!(calls.len(), 2);
        calls.iter().copied().for_each(assert_random_pick);
        assert_eq!(
            brain.listen_status().await,
            Some(ListenStatus::Listening(calls[1]))
        );
        assert_eq!(brain.db.get_listen_port().await.unwrap(), Some(calls[1]));
    }

    #[tokio::test]
    async fn a_fresh_install_without_a_free_port_chooses_none() {
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        node.set_listen_outcome(|_| Err(ListenFailure::AddressInUse));

        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        assert_eq!(node.listen_calls().len(), LISTEN_PORT_PICKS);
        assert_eq!(
            brain.listen_status().await,
            Some(ListenStatus::NotListening {
                configured: None,
                failure: ListenFailure::AddressInUse,
            })
        );
        assert_eq!(brain.db.get_listen_port().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_taken_configured_port_falls_back_without_being_forgotten() {
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        let configured = listen_port(24256);
        brain.db.store_listen_port(configured).await.unwrap();
        node.set_listen_outcome(move |port| {
            if port == configured {
                Err(ListenFailure::AddressInUse)
            } else {
                Ok(())
            }
        });

        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        let Some(ListenStatus::Temporary(temporary)) = brain.listen_status().await else {
            panic!("the node should listen on a temporary port");
        };
        assert_eq!(temporary.configured(), configured);
        assert_eq!(temporary.failure(), &ListenFailure::AddressInUse);
        assert_random_pick(temporary.active());
        assert_eq!(brain.db.get_listen_port().await.unwrap(), Some(configured));
    }

    #[tokio::test]
    async fn a_taken_configured_port_without_a_free_fallback_listens_nowhere() {
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        let configured = listen_port(24256);
        brain.db.store_listen_port(configured).await.unwrap();
        node.set_listen_outcome(|_| Err(ListenFailure::AccessDenied));

        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        assert_eq!(
            brain.listen_status().await,
            Some(ListenStatus::NotListening {
                configured: Some(configured),
                failure: ListenFailure::AccessDenied,
            })
        );
    }

    #[tokio::test]
    async fn setting_a_free_port_moves_the_node_there() {
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;
        let next = listen_port(30000);

        let (request, response) = BrainRequest::with_receiver(next);
        sender
            .send(BrainAction::Client(ClientAction::SetListenPort(request)))
            .await
            .unwrap();
        response.await.unwrap().unwrap();
        await_listen_push(&mut pushes).await;

        assert_eq!(
            brain.listen_status().await,
            Some(ListenStatus::Listening(next))
        );
        assert_eq!(brain.db.get_listen_port().await.unwrap(), Some(next));
        assert_eq!(node.listen_calls().last(), Some(&next));
    }

    #[tokio::test]
    async fn setting_a_taken_port_keeps_the_node_where_it_was() {
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;
        let Some(ListenStatus::Listening(before)) = brain.listen_status().await else {
            panic!("the node should listen on its first pick");
        };
        let taken = listen_port(24256);
        node.set_listen_outcome(move |port| {
            if port == taken {
                Err(ListenFailure::AddressInUse)
            } else {
                Ok(())
            }
        });

        let (request, response) = BrainRequest::with_receiver(taken);
        sender
            .send(BrainAction::Client(ClientAction::SetListenPort(request)))
            .await
            .unwrap();
        response.await.unwrap().unwrap();
        await_listen_push(&mut pushes).await;

        assert_eq!(
            brain.listen_status().await,
            TemporaryPort::new(taken, ListenFailure::AddressInUse, before)
                .map(ListenStatus::Temporary)
        );
        assert_eq!(brain.db.get_listen_port().await.unwrap(), Some(taken));
        assert_eq!(node.listen_calls().last(), Some(&before));
    }

    #[tokio::test]
    async fn a_pinned_port_wins_over_the_stored_one_and_is_not_stored() {
        let pinned = listen_port(24301);
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes_with(ListenPortChoice::Pinned(pinned)).await;
        let stored = listen_port(24256);
        brain.db.store_listen_port(stored).await.unwrap();

        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        assert_eq!(node.listen_calls(), vec![pinned]);
        assert_eq!(
            brain.listen_status().await,
            Some(ListenStatus::Listening(pinned))
        );
        assert_eq!(brain.db.get_listen_port().await.unwrap(), Some(stored));
    }

    #[tokio::test]
    async fn a_taken_pinned_port_falls_back_to_a_temporary_one_that_is_not_stored() {
        let pinned = listen_port(24301);
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes_with(ListenPortChoice::Pinned(pinned)).await;
        node.set_listen_outcome(move |port| {
            if port == pinned {
                Err(ListenFailure::AddressInUse)
            } else {
                Ok(())
            }
        });

        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        let Some(ListenStatus::Temporary(temporary)) = brain.listen_status().await else {
            panic!("a taken pinned port should fall back to a temporary one");
        };
        assert_eq!(temporary.configured(), pinned);
        assert_eq!(brain.db.get_listen_port().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_pinned_port_cannot_be_changed_and_is_reported() {
        let pinned = listen_port(24301);
        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes_with(ListenPortChoice::Pinned(pinned)).await;
        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;

        let (request, response) = BrainRequest::with_receiver(listen_port(30000));
        sender
            .send(BrainAction::Client(ClientAction::SetListenPort(request)))
            .await
            .unwrap();
        assert!(matches!(
            response.await.unwrap(),
            Err(FilerequestError::InputError(_))
        ));

        assert_eq!(node.listen_calls(), vec![pinned]);
        assert_eq!(brain.db.get_listen_port().await.unwrap(), None);
        let (request, response) = BrainRequest::with_receiver(());
        sender
            .send(BrainAction::Client(ClientAction::GetListenStatus(request)))
            .await
            .unwrap();
        assert_eq!(
            response.await.unwrap(),
            ListenReport {
                status: Some(ListenStatus::Listening(pinned)),
                set_by: PortSetBy::System,
            }
        );
    }

    #[tokio::test]
    async fn a_rebuilt_swarm_listens_again() {
        use crate::core::brain::action_plugin::PluginAction;
        use crate::core::p2p::NodeConfig;

        let (brain, sender, node, _shutdown, mut pushes, _host, _plugin) =
            setup_brain_watching_pushes().await;
        report_ready(&sender).await;
        await_listen_push(&mut pushes).await;
        let Some(ListenStatus::Listening(port)) = brain.listen_status().await else {
            panic!("the node should listen on its first pick");
        };

        sender
            .send(BrainAction::Plugin(PluginAction::RestartNetwork(
                NodeConfig(Vec::new()),
            )))
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while node.listen_calls().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the rebuilt swarm should be told to listen");
        assert_eq!(node.listen_calls(), vec![port, port]);
    }

    #[tokio::test]
    async fn a_restart_before_ready_leaves_listening_to_ready() {
        use crate::core::brain::action_plugin::PluginAction;
        use crate::core::p2p::NodeConfig;

        let (brain, sender, node, _shutdown) = setup_brain_with_node().await;

        sender
            .send(BrainAction::Plugin(PluginAction::RestartNetwork(
                NodeConfig(Vec::new()),
            )))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while node.restarts().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the brain should restart the network node");

        assert!(node.listen_calls().is_empty());
        assert_eq!(brain.listen_status().await, None);
    }
}
