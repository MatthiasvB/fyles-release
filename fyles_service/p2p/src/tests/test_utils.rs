use crypto::ContactKeys;
use futures::StreamExt;
use fyles_core::core::api_server::MockRunnableFilerequestServer;
use fyles_core::core::brain::action_test::TestAction;
use fyles_core::core::brain::plugin::{BrainPluginRef, NoOpBrainPlugin};
use fyles_core::core::brain::{
    ActionInterceptor, BrainNetwork, GenerationEnd, GigaBrain, NodeSettings,
};
use fyles_core::core::db::test::{DbRef, setup_test_db};
use fyles_core::core::p2p::{NetworkNode, NodeConfig, Runnable};
use fyles_core::io_controller::HostController;
use fyles_core::io_controller::test::TestHostController;
use fyles_core::library::util::duration_ext::DurationExt;
use fyles_core::library::util::util::TimeoutLock;
use fyles_core::library::wire::api::ServerMessage;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId, identity::Keypair};
use libp2p_swarm_test::SwarmExt;
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, instrument, span, trace};

use fyles_core::core::{
    brain::{
        Brain,
        action::BrainAction,
        action_client::ClientAction,
        action_p2p::{NetworkNodeAction, NodeInfo},
        types::{BrainRequest, SignedContactShare},
    },
    domain_models::{Contact, CreatePendingFiles, CreateRemoteFilerequest, FylesId, SendStatus},
};

use crate::TestSwarmFactory;
use crate::event_loop::CoreFileTracker;
use crate::types::Wrap;
use crate::utils::{decode_stored_keys, encode_keypair};
use crate::{
    P2pClient,
    behaviour::{LocalNetworkBehaviour, LocalNetworkBehaviourEvent},
    event_loop::InnerAsyncEvent,
};

pub fn generate_random_keypair(node_key_pair: &Keypair) -> NodeInfo {
    NodeInfo {
        node_key_pair: encode_keypair(node_key_pair).expect("Encoding keypair to work"),
        self_contact_keys: ContactKeys::new(),
    }
}

async fn listen_and_wait(
    swarm: &mut Swarm<LocalNetworkBehaviour>,
    address: Multiaddr,
) -> Multiaddr {
    let listener = swarm.listen_on(address).expect("listening to work");
    swarm
        .wait(|event| match event {
            SwarmEvent::NewListenAddr {
                address,
                listener_id,
            } => (listener_id == listener).then_some(address),
            _ => None,
        })
        .await
}

/// Replaces `SwarmExt::listen`, which panics on any event other than the address it waits for.
/// mDNS reports devices that really are on the network, at a moment no test controls.
async fn listen_with_memory_addr_external(swarm: &mut Swarm<LocalNetworkBehaviour>) {
    let memory = listen_and_wait(swarm, "/memory/0".parse().expect("a valid memory address")).await;
    listen_and_wait(
        swarm,
        "/ip4/127.0.0.1/tcp/0".parse().expect("a valid tcp address"),
    )
    .await;
    swarm.add_external_address(memory);
}

pub fn generate_swarm_factory(
    keypair: Option<Keypair>,
) -> (
    TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
    NodeInfo,
    PeerId,
) {
    swarm_factory_with(keypair, LocalNetworkBehaviour::new)
}

pub fn swarm_factory_with(
    keypair: Option<Keypair>,
    behaviour: fn(&Keypair) -> LocalNetworkBehaviour,
) -> (
    TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
    NodeInfo,
    PeerId,
) {
    let keypair = keypair.unwrap_or_else(Keypair::generate_ed25519);
    let node_info = generate_random_keypair(&keypair);
    let peer_id = keypair.public().to_peer_id();
    let swarm_factory = Arc::new(move || {
        Ok(Swarm::new_ephemeral_tokio_with_preexisting_keypair(
            keypair.clone(),
            |kp| behaviour(&kp),
        ))
    });
    (swarm_factory, node_info, peer_id)
}

/// The most unititialized version of a full Peer test harness. Only the DB is already spawned and running.
/// This is useful if you need access to the `brain_receiver`, which will be moved into the `EventLoop` in
/// the next stage of initialization.
pub struct IdleP2pTestHarness {
    pub node_info: NodeInfo,
    /// The Swarm's local PeerId
    pub peer_id: PeerId,
    /// Broadcast channel receiver for brain actions
    pub brain_receiver: mpsc::Receiver<BrainAction>,
    pub brain_sender: mpsc::Sender<BrainAction>,
    pub temp_dir: TempDir,
    pub out_dir: PathBuf,
    pub out_dir_handler: Arc<dyn HostController>,
    pub db: DbRef,
    swarm: std::sync::Mutex<Option<Swarm<LocalNetworkBehaviour>>>,
    swarm_factory: TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
}

impl IdleP2pTestHarness {
    #[instrument(skip_all, level = "trace")]
    pub async fn new(
        swarm_factory: TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
        node_info: NodeInfo,
        peer_id: PeerId,
        db: Option<DbRef>,
        temp_dir: Option<TempDir>,
    ) -> Self {
        let temp_dir =
            temp_dir.unwrap_or_else(|| TempDir::new().expect("failed to create tempdir"));
        let out_dir = temp_dir.path().to_path_buf();

        let (brain_sender, brain_receiver) = mpsc::channel(50);

        let db = match db {
            Some(db) => db,
            None => setup_test_db(Some(node_info.clone())).await,
        };

        let swarm = std::sync::Mutex::new(Some(swarm_factory().unwrap()));

        Self {
            node_info,
            peer_id,
            brain_receiver,
            brain_sender,
            temp_dir,
            out_dir_handler: Arc::new(TestHostController::new(Some(out_dir.clone()))),
            out_dir,
            db,
            swarm,
            swarm_factory,
        }
    }

    /// Creates a new `IdleP2pTestHarness` with a local network swarm.
    #[instrument(skip_all, level = "trace")]
    pub async fn default() -> Self {
        let (swarm_factory, node_info, peer_id) = generate_swarm_factory(None);
        Self::new(swarm_factory, node_info, peer_id, None, None).await
    }

    /// Initializes the P2P test harness, creating the event loop and starting the brain.
    // #[instrument(skip_all, level = "trace")]
    pub async fn initialize(self) -> InitializingP2pTestHarness {
        let (client_sender, client_push_receiver) = mpsc::channel(256);
        let shutdown = CancellationToken::new();
        let swarm_factory_clone = self.swarm_factory.clone();
        let p2p = Arc::new(RwLock::new(
            P2pClient::new(
                self.brain_sender.clone(),
                shutdown.clone(),
                Arc::new(move |_keypair: Keypair, _config: &[u8]| {
                    Ok(if let Some(swarm) = self.swarm.lock().unwrap().take() {
                        swarm
                    } else {
                        swarm_factory_clone().unwrap()
                    })
                }),
            )
            .await
            .unwrap(),
        ));
        self.brain_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::Ready))
            .await
            .unwrap();
        let p2p_clone = p2p.clone();
        let db = self.db.clone();
        let out_dir_handler = self.out_dir_handler.clone();
        let internal_data_dir = self.temp_dir.path().to_path_buf();
        let brain_runner = Brain::run(
            NodeSettings::with_stored_port(internal_data_dir),
            async move || db.clone(),
            async move || out_dir_handler.clone(),
            async move || p2p_clone.clone() as BrainNetwork,
            async || Arc::new(RwLock::new(NoOpBrainPlugin)) as BrainPluginRef,
            async || Box::new(MockRunnableFilerequestServer {}) as _,
            self.brain_receiver,
            self.brain_sender.clone(),
            client_sender,
            shutdown,
        );
        let (brain_handle, giga_brain) = brain_runner.in_current_span().await;

        InitializingP2pTestHarness {
            p2p_client: p2p,
            db: self.db,
            node_info: self.node_info,
            peer_id: self.peer_id,
            brain_sender: self.brain_sender,
            temp_dir: self.temp_dir,
            out_dir_handler: self.out_dir_handler,
            out_dir: self.out_dir,
            brain_handle,
            giga_brain,
            swarm_factory: self.swarm_factory,
            client_push_receiver: Some(client_push_receiver),
        }
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn run(self) -> RunningP2pTestHarness {
        self.initialize().await.run().await
    }

    /// Connects this harness to another one, allowing them to communicate with each other.
    #[instrument(skip_all, level = "trace")]
    pub async fn connect(&mut self, other: &mut Self) {
        self.swarm
            .get_mut()
            .expect("locking to work")
            .as_mut()
            .expect("swarm not initialized")
            .connect(
                other
                    .swarm
                    .get_mut()
                    .expect("locking to work")
                    .as_mut()
                    .expect("swarm not initialized"),
            )
            .await;
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn connect_brainless(&mut self, other: &mut BrainlessIdleP2pTestHarness) {
        self.swarm
            .get_mut()
            .expect("locking to work")
            .as_mut()
            .expect("swarm not initialized")
            .connect(
                other
                    .swarm
                    .get_mut()
                    .expect("locking to work")
                    .as_mut()
                    .expect("swarm not initialized"),
            )
            .await;
    }

    /// Listens for incoming connections on the swarm, using an external memory address.
    /// This is useful for testing purposes, allowing the swarm to accept connections from other peers.
    #[instrument(skip_all, level = "trace")]
    pub async fn listen_external(&mut self) {
        listen_with_memory_addr_external(
            self.swarm
                .get_mut()
                .expect("locking to work")
                .as_mut()
                .expect("swarm not initialized"),
        )
        .await;
    }
}

/// This is the uninitialized version of a bare P2P node test harness, without the orchestration of the `Brain`.
/// This harness is useful when you want raw access to P2P functionality, for example to test invalid usecases, like
/// sending oversized files. In this state, you also still have access to the `brain_receiver`, which will
/// be moved into the `EventLoop` in the next stage of initialization.
#[allow(unused)]
pub struct BrainlessIdleP2pTestHarness {
    pub node_info: NodeInfo,
    /// The Swarm's local PeerId
    pub peer_id: PeerId,
    /// Broadcast channel receiver for brain actions
    pub brain_receiver: mpsc::Receiver<BrainAction>,
    pub brain_sender: mpsc::Sender<BrainAction>,
    /// Keeps the temporary directory alive
    pub temp_dir: TempDir,
    /// Path to the temp directory (for reading files)
    pub out_dir: PathBuf,
    pub db: DbRef,
    /// Must be available initially to connect other swarms, but will be moved out upon initialization
    pub swarm: std::sync::Mutex<Option<Swarm<LocalNetworkBehaviour>>>,
    pub swarm_factory: TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
}

#[allow(unused)]
impl BrainlessIdleP2pTestHarness {
    /// Creates a new `BrainlessIdleP2pTestHarness` with the given swarm, keypair, and peer ID.
    #[instrument(skip_all, level = "trace")]
    pub async fn new(
        swarm_factory: TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
        node_info: NodeInfo,
        peer_id: PeerId,
    ) -> Self {
        let temp_dir = TempDir::new().expect("failed to create tempdir");
        let out_dir = temp_dir.path().to_path_buf();

        let (brain_sender, mut brain_receiver) = mpsc::channel(50);
        let (command_sender, command_receiver) =
            P2pClient::<CoreFileTracker, Swarm<LocalNetworkBehaviour>>::get_channel();

        let db = setup_test_db(Some(node_info.clone())).await;

        let swarm = swarm_factory().expect("Swarm factory should produce a swarm");

        Self {
            node_info,
            peer_id,
            brain_receiver,
            brain_sender,
            temp_dir,
            out_dir,
            db,
            swarm: Some(swarm).into(),
            swarm_factory,
        }
    }

    /// Creates a new `BrainlessIdleP2pTestHarness` with a local network swarm.
    #[instrument(skip_all)]
    pub async fn default() -> Self {
        let (swarm_factory, node_info, peer_id) = generate_swarm_factory(None);
        Self::new(swarm_factory, node_info, peer_id).await
    }

    /// Fully initializes the P2P test harness, starting the event loop
    #[instrument(skip_all, level = "trace")]
    pub async fn run(mut self) -> BrainlessRunningP2pTestHarness {
        let keypair = decode_stored_keys(&self.node_info.node_key_pair).unwrap();
        let p2p_client = Arc::new(
            P2pClient::new(
                self.brain_sender.clone(),
                CancellationToken::new(),
                Arc::new(move |_k: Keypair, _config: &[u8]| {
                    Ok(if let Some(swarm) = self.swarm.lock().unwrap().take() {
                        swarm
                    } else {
                        (self.swarm_factory)().unwrap()
                    })
                }),
            )
            .await
            .unwrap(),
        );
        let p2p_clone = p2p_client.clone();
        let p2p_handle = tokio::spawn(
            async move { p2p_clone.run().await }
                .instrument(span!(Level::INFO, "Running p2p client")),
        );
        wait_for_ready(&mut self.brain_receiver, self.node_info.clone()).await;
        BrainlessRunningP2pTestHarness {
            p2p_client,
            node_info: self.node_info,
            peer_id: self.peer_id,
            brain_receiver: self.brain_receiver,
            brain_sender: self.brain_sender,
            temp_dir: self.temp_dir,
            out_dir: self.out_dir,
            p2p_handle,
        }
    }

    /// Connects this harness to another one, allowing them to communicate with each other.
    #[instrument(skip_all, level = "trace")]
    pub async fn connect(&mut self, other: &mut BrainlessIdleP2pTestHarness) {
        self.swarm
            .get_mut()
            .expect("locking to work")
            .as_mut()
            .expect("swarm not yet initialized")
            .connect(
                other
                    .swarm
                    .get_mut()
                    .expect("locking to work")
                    .as_mut()
                    .expect("swarm not yet initialized"),
            )
            .await;
    }

    /// Listens for incoming connections on the swarm, using an external memory address.
    /// This is useful for testing purposes, allowing the swarm to accept connections from other peers.
    #[instrument(skip_all, level = "trace")]
    pub async fn listen_external(&mut self) {
        listen_with_memory_addr_external(
            self.swarm
                .get_mut()
                .expect("locking to work")
                .as_mut()
                .expect("swarm not yet initialized"),
        )
        .await;
    }
}

/// This is a full Peer test harness in the second stage of initialization.
pub struct InitializingP2pTestHarness {
    pub p2p_client: Arc<RwLock<P2pClient<CoreFileTracker, Swarm<LocalNetworkBehaviour>>>>,
    pub db: DbRef,
    pub node_info: NodeInfo,
    /// The Swarm's local PeerId
    pub peer_id: PeerId,
    pub brain_sender: mpsc::Sender<BrainAction>,
    pub temp_dir: TempDir,
    pub out_dir_handler: Arc<dyn HostController>,
    pub out_dir: PathBuf,
    brain_handle: JoinHandle<GenerationEnd>,
    giga_brain: GigaBrain,
    swarm_factory: TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
    /// Messages the brain pushes toward connected clients. A test may `take()` this before
    /// `run()` to assert against the pushes; otherwise `run()` spawns a task that drains it.
    pub client_push_receiver: Option<mpsc::Receiver<ServerMessage>>,
}

#[allow(unused)]
impl InitializingP2pTestHarness {
    #[instrument(skip_all, level = "trace")]
    pub async fn default(internal_data_dir: PathBuf) -> Self {
        let harness = IdleP2pTestHarness::default().await;
        harness.initialize().await
    }

    // TODO: The distinction between initializing and running harnesses has become obsolete.
    /// NoOp
    pub async fn run(self) -> RunningP2pTestHarness {
        if let Some(mut rx) = self.client_push_receiver {
            tokio::spawn(async move { while rx.recv().await.is_some() {} });
        }
        RunningP2pTestHarness {
            p2p_client: self.p2p_client,
            db: self.db,
            node_info: self.node_info,
            peer_id: self.peer_id,
            brain_sender: self.brain_sender,
            temp_dir: self.temp_dir,
            out_dir_handler: self.out_dir_handler,
            out_dir: self.out_dir,
            brain_handle: self.brain_handle,
            giga_brain: self.giga_brain,
            swarm_factory: self.swarm_factory,
        }
    }

    /// Access to the full client interface
    #[instrument(skip_all, level = "trace")]
    pub async fn act(&self, action: ClientAction) {
        act(&self.brain_sender, action).await;
    }
}

/// While testing, you may sometimes want to be notified of actions the brain processes. Usually,
/// this is to verify that a certain behaviour is triggered. You may or may not want the brain to
/// "handle" that action as well. Since many actions pass through the brain, it may take a while for
/// the one you need to come up. Therefore, the `handler` you pass the `RunningP2pTestHarness::intercept_actions`
/// must return one variant of this enum on each action it receives.
pub enum InterceptionRes {
    /// Continue intecepeting actions after the current one. If the brain should get to process this action,
    /// pass it back. If you pass `None`, the brain will not see this action.
    Continue(Option<BrainAction>),
    /// Stop intercepting actions after the current one. If the brain should get to process this action,
    /// pass it back. If you pass `None`, the brain will not see this action.
    Exit(Option<BrainAction>),
}

/// A full Peer, fully initialized test harness, acting like an actual device would.
/// You can use its `act` function to emulate all client functionality, causing it to
/// act like the real device would.
#[allow(unused)]
pub struct RunningP2pTestHarness {
    pub p2p_client: Arc<RwLock<P2pClient<CoreFileTracker, Swarm<LocalNetworkBehaviour>>>>,
    pub db: DbRef,
    pub node_info: NodeInfo,
    pub peer_id: PeerId,
    pub brain_sender: mpsc::Sender<BrainAction>,
    pub temp_dir: TempDir,
    pub out_dir_handler: Arc<dyn HostController>,
    pub out_dir: PathBuf,
    brain_handle: JoinHandle<GenerationEnd>,
    giga_brain: GigaBrain,
    swarm_factory: TestSwarmFactory<Swarm<LocalNetworkBehaviour>>,
}

impl RunningP2pTestHarness {
    /// Access to the full client interface
    #[instrument(skip_all, level = "trace")]
    pub async fn act(&self, action: ClientAction) {
        act(&self.brain_sender, action).await;
    }

    /// Intercepts brain actions, passing them to the given handler. The handler must return an `InterceptionRes` indicating
    /// whether to continue intercepting actions or exit the interception loop. Based on the `handler`'s return value, the action
    /// may or may not be passed back to the brain for processing.
    #[instrument(skip_all, level = "trace")]
    pub async fn intercept_actions<F>(&self, mut handler: F)
    where
        F: AsyncFnMut(BrainAction) -> InterceptionRes,
    {
        let mut message_count = 0;
        let (interceptor, sender, mut receiver) = ActionInterceptor::new();
        let (request, response) = BrainRequest::with_receiver(interceptor);
        self.brain_sender
            .send(BrainAction::Test(TestAction::RegisterActionInterceptor(
                request,
            )))
            .await
            .unwrap();
        response.await.unwrap();
        loop {
            let action = match receiver.recv().await {
                Some(action) => action,
                None => {
                    panic!(
                        "Action interceptor channel closed unexpectedly after {} messages",
                        message_count
                    );
                }
            };
            match handler(action).await {
                InterceptionRes::Continue(action) => {
                    sender.send(action).await.expect("Sending to work");
                }
                InterceptionRes::Exit(action) => {
                    sender.send(action).await.expect("Sending to work");
                    break;
                }
            };
            message_count += 1;
        }
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn intercept_actions_and_await<F, W>(&self, mut interceptor: F, work: W)
    where
        F: AsyncFnMut(BrainAction) -> InterceptionRes,
        W: Future<Output = ()>,
    {
        let mut message_count = 0;
        let (action_interceptor, sender, mut receiver) = ActionInterceptor::new();
        let (request, response) = BrainRequest::with_receiver(action_interceptor);
        self.brain_sender
            .send(BrainAction::Test(TestAction::RegisterActionInterceptor(
                request,
            )))
            .await
            .unwrap();
        response.await.unwrap();
        let interception = async {
            loop {
                let action = match receiver.recv().await {
                    Some(action) => action,
                    None => {
                        panic!(
                            "Action interceptor channel closed unexpectedly after {} messages",
                            message_count
                        );
                    }
                };
                match interceptor(action).await {
                    InterceptionRes::Continue(action) => {
                        sender.send(action).await.expect("Sending to work");
                    }
                    InterceptionRes::Exit(action) => {
                        sender.send(action).await.expect("Sending to work");
                        break;
                    }
                };
                message_count += 1;
            }
        };
        tokio::join!(interception, work);
    }

    /// Crash the whole node: stop the p2p event loop (and other workloads) before
    /// the brain, so it can't serve requests against a torn-down brain.
    pub async fn abort(&self) {
        self.giga_brain.shutdown_workloads().await;
        self.brain_handle.abort();
    }

    // convenience functions
    #[instrument(skip_all, level = "trace")]
    pub async fn get_sharable_self_contact(&self) -> SignedContactShare {
        let (request, response) = BrainRequest::with_receiver(());
        self.act(ClientAction::SharePublicSelfContact(request))
            .await;
        response
            .await
            .expect("Contact should be retrieved")
            .expect("Contact should be retrieved successfully")
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn register_new_contact(&self) -> Contact {
        let share = SignedContactShare::for_test();
        let contact = share.contact_unverified().clone();
        self.register_contact(share).await;
        contact
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn register_contact(&self, share: SignedContactShare) {
        let (request, response) = BrainRequest::with_receiver(share.into());
        self.act(ClientAction::RegisterContact(request)).await;
        response
            .await
            .expect("Contact should be registered successfully")
            .expect("Contact should be registered successfully");
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn register_as_contact_of(&self, other: &RunningP2pTestHarness) {
        let contact = other.get_sharable_self_contact().await;
        self.register_contact(contact).await;
    }

    /// Create a remote filerequest entry pointing at a filerequest owned by `remote`
    #[instrument(skip_all, level = "trace")]
    pub async fn create_remote_filerequest(
        &self,
        remote: &RunningP2pTestHarness,
        remote_filerequest_id: String,
        name: &str,
    ) -> FylesId {
        let req_body = CreateRemoteFilerequest {
            peer_id: remote.peer_id.wrap(),
            filerequest_id: remote_filerequest_id,
            name: name.to_string(),
            contact_id: remote.node_info.self_contact_keys.id(),
        };
        let (request, response) = BrainRequest::with_receiver(req_body);
        self.act(ClientAction::CreateRemoteFilerequest(request))
            .await;
        response
            .await
            .expect("Sender not dropped")
            .expect("Remote filerequest creation should succeed")
    }

    /// Create pending files for given remote filerequest id (sizes in bytes). Returns pending file IDs.
    #[instrument(skip_all, level = "trace")]
    pub async fn create_pending_files(
        &self,
        remote_filerequest_id: FylesId,
        file_sizes: Vec<usize>,
    ) -> (TempDir, Vec<FylesId>) {
        let (_tempdir, pending) =
            CreatePendingFiles::for_test(remote_filerequest_id, file_sizes).await;
        let (request, response) = BrainRequest::with_receiver(pending);
        self.act(ClientAction::CreatePendingFiles(request)).await;
        (
            _tempdir,
            response
                .await
                .expect("Sender not dropped")
                .expect("Pending files creation should succeed"),
        )
    }

    /// Wait until all pending files are in Sent status.
    #[instrument(skip_all, level = "trace")]
    pub async fn wait_all_pending_files_sent(
        &self,
        timeout: Duration,
        backoff: Duration,
    ) -> Result<(), Option<&str>> {
        repeat_till_success(timeout, backoff, || async {
            let (req, res) = BrainRequest::with_receiver(());
            self.act(ClientAction::GetAllPendingFiles(req)).await;
            res.await
                .expect("Sender dropped")
                .expect("GetAllPendingFiles should succeed")
                .into_iter()
                .all(|pf| pf.status == SendStatus::Sent)
                .then_some(())
                .ok_or("Not all files were sent within time limit")
        })
        .await
    }

    /// Convenience: create pending files and wait for them to be sent.
    #[instrument(skip_all, level = "trace")]
    pub async fn send_files_and_wait(
        &self,
        remote_filerequest_id: FylesId,
        file_sizes: Vec<usize>,
        timeout: Duration,
        backoff: Duration,
    ) -> Result<(), Option<&str>> {
        let (_tempdir, _) = self
            .create_pending_files(remote_filerequest_id, file_sizes)
            .await;
        self.wait_all_pending_files_sent(timeout, backoff).await
    }

    /// Aborts this harness, creates a new one with the same identity, connects it to all `connectors`, and returns it.
    /// This simulates restarting the node.
    #[instrument(skip_all, level = "trace")]
    pub async fn restart_and_get_connected_by(
        self,
        connectors: Vec<&RunningP2pTestHarness>,
    ) -> IdleP2pTestHarness {
        self.abort().await;

        let mut reborn_swarm = (self.swarm_factory)().unwrap();

        listen_with_memory_addr_external(&mut reborn_swarm).await;

        let mut swarm_harbor = Some(reborn_swarm);
        for connector in connectors {
            let (swarm_sender, swarm_receiver) = tokio::sync::oneshot::channel();
            connector
                .p2p_client
                .read()
                .await
                .inner_async_event_sender
                .send(InnerAsyncEvent::ConnectOtherSwarm {
                    other: Box::new(swarm_harbor.take().expect("Only one connector allowed")),
                    return_connected_sender: swarm_sender,
                })
                .expect("Connecting to reborn swarm should succeed");

            swarm_harbor.replace(swarm_receiver.await.expect("Swarm should be sent back"));
        }

        let reborn_swarm = std::sync::Mutex::new(Some(
            swarm_harbor.expect("swarm being back after connecting"),
        ));

        let reborn_swarm_factory = Arc::new(move || {
            Ok(if let Some(swarm) = reborn_swarm.lock().unwrap().take() {
                swarm
            } else {
                (self.swarm_factory)().unwrap()
            })
        });

        IdleP2pTestHarness::new(
            reborn_swarm_factory,
            self.node_info,
            self.peer_id,
            Some(self.db),
            Some(self.temp_dir),
        )
        .instrument(span!(Level::INFO, "Crashing reborn setup"))
        .await
    }
}

/// A little test‐harness holding everything you need,
/// *including* the TempDir so it doesn't get deleted too soon.
#[allow(unused)]
pub struct BrainlessRunningP2pTestHarness {
    pub p2p_client: Arc<dyn NetworkNode>,
    pub node_info: NodeInfo,
    /// The Swarm's local PeerId
    pub peer_id: PeerId,
    /// Broadcast channel receiver for brain actions
    pub brain_receiver: mpsc::Receiver<BrainAction>,
    pub brain_sender: mpsc::Sender<BrainAction>,
    /// Keeps the temporary directory alive
    pub temp_dir: TempDir,
    /// Path to the temp directory (for reading files)
    pub out_dir: PathBuf,
    pub p2p_handle: JoinHandle<()>,
}

/// access to the full client interface
#[instrument(skip_all, level = "trace")]
pub async fn act(brain_sender: &mpsc::Sender<BrainAction>, action: ClientAction) {
    brain_sender
        .send(BrainAction::Client(action))
        .await
        .expect("Failed to send action to brain");
}

#[instrument(skip_all, level = "trace")]
pub async fn connected_running_pair() -> (RunningP2pTestHarness, RunningP2pTestHarness) {
    let mut first = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "First setup"))
        .await;
    let mut second = IdleP2pTestHarness::default()
        .instrument(span!(Level::INFO, "Second setup"))
        .await;

    first.listen_external().await;
    second.connect(&mut first).await;

    let first = first.run().instrument(span!(Level::INFO, "First")).await;
    let second = second.run().instrument(span!(Level::INFO, "Second")).await;
    (first, second)
}

/// Creates a local network swarm with a keypair and peer ID.
pub fn get_local_network_swarm(
    mut keypair: Option<Keypair>,
) -> (NodeInfo, PeerId, Swarm<LocalNetworkBehaviour>) {
    let swarm = if let Some(ref kp) = keypair {
        Swarm::new_ephemeral_tokio_with_preexisting_keypair(kp.clone(), |kp| {
            LocalNetworkBehaviour::new(&kp)
        })
    } else {
        Swarm::new_ephemeral_tokio(|kp| {
            keypair.replace(kp.clone());
            LocalNetworkBehaviour::new(&kp)
        })
    };
    let peer_id = *swarm.local_peer_id();
    (
        generate_random_node_info(keypair.as_ref().unwrap()),
        peer_id,
        swarm,
    )
}

pub fn get_local_network_swarm_with_keypair(
    keypair: Keypair,
) -> (NodeInfo, PeerId, Swarm<LocalNetworkBehaviour>) {
    let id = keypair.public().to_peer_id();
    trace!("Creating local network swarm with provided keypair resolving to peer id {id:?}");
    let swarm = Swarm::new_ephemeral_tokio_with_preexisting_keypair(keypair.clone(), |keypair| {
        LocalNetworkBehaviour::new(&keypair)
    });
    let peer_id = *swarm.local_peer_id();
    (generate_random_node_info(&keypair), peer_id, swarm)
}

pub fn generate_random_node_info(keypair: &Keypair) -> NodeInfo {
    NodeInfo {
        node_key_pair: encode_keypair(keypair).expect("Encoding keypair to work"),
        self_contact_keys: ContactKeys::new(),
    }
}

/// Waits for the P2P node to be ready by listening for the `Ready` action.
/// It also handles the `GetNodeKeys` action to respond with the node's keys.
#[instrument(skip_all, level = "trace")]
pub async fn wait_for_ready(brain_receiver: &mut mpsc::Receiver<BrainAction>, node_info: NodeInfo) {
    timeout(3.seconds(), async {
        loop {
            let action = brain_receiver.recv().await.expect("sender dropped");
            match action {
                BrainAction::NetworkNode(NetworkNodeAction::Ready) => break,
                BrainAction::NetworkNode(NetworkNodeAction::GetNodeInfo(req)) => {
                    let _ = req
                        .response_sender
                        .timeout_lock()
                        .await
                        .take()
                        .expect("no channel")
                        .send(Ok((node_info.clone(), NodeConfig(Vec::new()))));
                }
                _ => {}
            }
        }
    })
    .await
    .expect("Timed out waiting for Ready");
}

/// Repeatedly calls the provided future function until it succeeds or the timeout is reached.  
/// - If the future function returns an error, it will retry after a short sleep (retry case).  
/// - If the timeout is reached, it returns `Err(Some(e))` if error `e` was last encountered (timeout case).  
/// - If the timeout is reached before an error is encountered, it returns `Err(None)` (timeout case).
/// - Otherwise it returns `Ok(T)` when the future function succeeds (success case).  
#[instrument(skip_all, level = "trace")]
pub async fn repeat_till_success<F, T, E>(
    timeout: Duration,
    backoff: Duration,
    mut f: impl FnMut() -> F,
) -> Result<T, Option<E>>
where
    F: Future<Output = Result<T, E>>,
{
    let mut err = None;
    tokio::time::timeout(timeout, async {
        loop {
            match f().await {
                Ok(result) => return result,
                Err(e) => {
                    err = Some(e);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    })
    .await
    .map_err(|_| err)
}

#[allow(unused)]
#[instrument(skip_all, level = "trace")]
pub async fn wait_for_response<F, T, E>(
    wait_for: Duration,
    swarm: &mut Swarm<LocalNetworkBehaviour>,
    mut f: impl FnMut(SwarmEvent<LocalNetworkBehaviourEvent>) -> F,
) -> Result<T, Option<E>>
where
    F: Future<Output = Result<T, E>>,
{
    let mut err = None;
    tokio::time::timeout(wait_for, async {
        loop {
            match f(swarm.select_next_some().await).await {
                Ok(v) => return v,
                Err(e) => {
                    err = Some(e);
                }
            };
        }
    })
    .await
    .map_err(|_| err)
}
