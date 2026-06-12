use std::{error::Error, sync::Arc};

use command::P2pCommand;

use async_trait::async_trait;
use ::crypto::ContactKeys;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::types::{ContactShareChallenge, SelfContactInviteChallenge};
use fyles_core::library::util::error_handling::{AutoMapError, ToArcedDynError};
use fyles_core::library::util::util::TimeoutLock;
use libp2p::identity::Keypair;
#[cfg(not(test))]
use libp2p::Multiaddr;
use libp2p::{identity, Swarm};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tracing::{error, info, instrument};

use fyles_core::core::domain_models::{ContactId, FylesId, PeerIdWrapper};

use fyles_core::core::p2p::{
    FileToSend, KeypairGenerationError, NetworkNode, NodeStatusInfo, P2pError, P2pResult, Runnable,
};
#[cfg(all(not(test), not(feature = "test-support")))]
use std::str::FromStr;

use crate::behaviour::LocalNetworkBehaviour;
use crate::event_loop::filerequest::file_tracker::{CoreFileTracker, FileTracker};
use crate::event_loop::AsyncEventLoopSender;
use crate::event_loop::{AsyncEventLoopReceiver, LocalNetworkSwarm};
use crate::event_loop::{EventLoopError, LocalEventLoop};
use crate::types::Unwrap;
use crate::utils::{encode_keypair, extract_peer_id};

pub mod behaviour;
pub mod command;
pub mod crypto;
mod data_structures;
pub mod event_loop;
pub mod file_encryptor;
mod file_reader;
pub mod send_receive_traits;
pub mod types;
pub mod utils;

#[cfg(test)]
pub mod tests;



const CHUNK_SIZE: usize = 512 * 1024;
type Chunk = Box<[u8; CHUNK_SIZE]>;
fn chunk() -> Chunk {
    Box::new([0; CHUNK_SIZE])
}

pub struct PreparedEventLoopArgs<T: FileTracker> {
    pub p2p_command_receiver: Receiver<P2pCommand>,
    pub brain_action_sender: Sender<BrainAction>,
    /// only set in test configurations
    inner_async_event_loop_receiver: Option<AsyncEventLoopReceiver<T>>,
}

impl<T: FileTracker> PreparedEventLoopArgs<T> {
    pub fn new(
        p2p_command_receiver: Receiver<P2pCommand>,
        brain_action_sender: Sender<BrainAction>,
        inner_async_event_loop_receiver: Option<AsyncEventLoopReceiver<T>>,
    ) -> Self {
        Self {
            p2p_command_receiver,
            brain_action_sender,
            inner_async_event_loop_receiver,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn get_inner_async_event_loop_receiver(self) -> Option<AsyncEventLoopReceiver<T>> {
        self.inner_async_event_loop_receiver
    }
}

pub type SwarmFactory<T = Swarm<LocalNetworkBehaviour>> = Arc<
    dyn Fn(Keypair, &[u8]) -> Result<T, Arc<dyn std::error::Error + std::marker::Send + Sync>>
        + Send
        + Sync,
>;

#[cfg(any(test, feature = "test-support"))]
pub type TestSwarmFactory<T = Swarm<LocalNetworkBehaviour>> =
    Arc<dyn Fn() -> Result<T, Arc<dyn std::error::Error + std::marker::Send + Sync>> + Send + Sync>;

pub trait EventLoop {}

/// While the p2p node runs in one thread and isn't Send + Sync, the client
/// can be cloned and sent across threads, allowing for asynchronous communication
#[derive(Clone)]
pub struct P2pClient<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> {
    command_sender: Sender<P2pCommand>,
    pub prepared_event_loop_args: Arc<Mutex<Option<PreparedEventLoopArgs<T>>>>,
    pub swarm_factory: SwarmFactory<S>,
    #[cfg(any(test, feature = "test-support"))]
    pub inner_async_event_sender: AsyncEventLoopSender<T>,
}

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> P2pClient<T, S> {
    pub fn new_external(
        command_sender: Sender<P2pCommand>,
        prepared_event_loop_args: Arc<Mutex<Option<PreparedEventLoopArgs<T>>>>,
        swarm_factory: SwarmFactory<S>,
        inner_async_event_sender: Option<AsyncEventLoopSender<T>>,
    ) -> Self {
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = inner_async_event_sender;

        Self {
            command_sender,
            prepared_event_loop_args,
            swarm_factory,
            #[cfg(any(test, feature = "test-support"))]
            inner_async_event_sender: inner_async_event_sender.expect(
                "inner_async_event_sender must be provided in test or test-support configuration",
            ),
        }
    }
}

impl P2pClient<CoreFileTracker, Swarm<LocalNetworkBehaviour>> {
    pub async fn new(
        brain_action_sender: Sender<BrainAction>,
        swarm_factory: SwarmFactory<Swarm<LocalNetworkBehaviour>>,
    ) -> Result<Self, EventLoopError> {
        info!("Setup: Creating new P2P node");
        let (sender, p2p_command_receiver) = mpsc::channel(1);
        #[cfg(any(test, feature = "test-support"))]
        let (inner_async_event_sender, inner_async_event_loop_receiver) = mpsc::unbounded_channel();
        Ok(Self {
            command_sender: sender,
            prepared_event_loop_args: Arc::new(Mutex::new(Some(PreparedEventLoopArgs {
                p2p_command_receiver,
                brain_action_sender,
                #[cfg(any(test, feature = "test-support"))]
                inner_async_event_loop_receiver: Some(inner_async_event_loop_receiver),
                #[cfg(not(any(test, feature = "test-support")))]
                inner_async_event_loop_receiver: None,
            }))),
            swarm_factory,
            #[cfg(any(test, feature = "test-support"))]
            inner_async_event_sender,
        })
    }
}

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> P2pClient<T, S> {
    #[cfg(not(test))]
    pub async fn start_listening(
        &self,
        addr: Multiaddr,
    ) -> Result<(), Arc<dyn Error + Send + Sync>> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender
            .send(P2pCommand::StartListening { addr, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    pub fn get_channel() -> (
        tokio::sync::mpsc::Sender<P2pCommand>,
        tokio::sync::mpsc::Receiver<P2pCommand>,
    ) {
        tokio::sync::mpsc::channel(16)
    }
}

#[async_trait]
impl Runnable for P2pClient<CoreFileTracker, Swarm<LocalNetworkBehaviour>> {
    #[instrument(skip_all, level = "trace")]
    async fn run(&self) {
        tokio::join!(
            async {
                match self.prepared_event_loop_args.timeout_lock().await.take() {
                    Some(PreparedEventLoopArgs {
                        p2p_command_receiver,
                        brain_action_sender,
                        inner_async_event_loop_receiver,
                    }) => {
                        #[cfg(not(any(test, feature = "test-support")))]
                        let _ = inner_async_event_loop_receiver;

                        // Need to allocate future in order to reduce stack usage
                        Box::pin(
                            LocalEventLoop::new(
                                p2p_command_receiver,
                                brain_action_sender,
                                self.swarm_factory.clone(),
                                #[cfg(any(test, feature = "test-support"))]
                                inner_async_event_loop_receiver.expect(
                                    "Inner event loop receiver to be present in test config",
                                ),
                            )
                            .await
                            .expect("Event loop to be created")
                            .run(),
                        )
                        .await
                        .expect("Event loop to run");
                    }
                    None => {
                        error!("Trying to run an already running P2pClient");
                    }
                }
            },
            async {
                #[cfg(all(not(test), not(feature = "test-support")))]
                self.start_listening(Multiaddr::from_str("/ip4/0.0.0.0/tcp/24256").unwrap())
                    .await
                    .unwrap();
            },
            async {
                #[cfg(all(not(test), not(feature = "test-support")))]
                self.start_listening(
                    Multiaddr::from_str("/ip4/0.0.0.0/udp/24256/quic-v1").unwrap(),
                )
                .await
                .unwrap();
            }
        );
    }
}

#[async_trait]
impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> NetworkNode for P2pClient<T, S> {
    fn display_keypair(
        &self,
        keypair: &Vec<u8>,
    ) -> Result<String, Box<dyn Error + std::marker::Send + Sync + 'static>> {
        extract_peer_id(keypair).auto_map_err()
    }

    #[instrument(skip_all, level = "info")]
    async fn send_files(&self, files_to_send: Vec<FileToSend>) -> P2pResult<()> {
        let (sender, receiver) = oneshot::channel();
        for file_to_send in &files_to_send {
            info!(
                "Client: Sending file {:?} with pending file id {} to peer: {}",
                file_to_send.file_path, file_to_send.id, file_to_send.peer_id
            );
        }
        self.command_sender
            .send(P2pCommand::SendFiles {
                files_to_send,
                sender,
            })
            .await
            .map_err(|e| P2pError::InternalCommunicationError {
                msg: "Could not instruct node to send file".into(),
                source: Arc::new(e),
            })?;
        receiver.await.to_arced_dyn_err()?.auto_map_err()
    }

    async fn initial_files_to_send(&self, files: Vec<FileToSend>) -> P2pResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender
            .send(P2pCommand::InitialFilesToSend {
                files_to_send: files,
                sender,
            })
            .await
            .map_err(|e| P2pError::InternalCommunicationError {
                msg: "Could not instruct node to send file".into(),
                source: Arc::new(e),
            })?;
        receiver.await.to_arced_dyn_err()?.auto_map_err()
    }

    async fn get_node_info(&self) -> P2pResult<NodeStatusInfo> {
        info!("Client: Getting node info");
        let (response_sender, receiver) = oneshot::channel();
        self.command_sender
            .send(P2pCommand::GetNodeInfo {
                sender: response_sender,
            })
            .await
            .to_arced_dyn_err()?;

        receiver.await.to_arced_dyn_err().auto_map_err()
    }

    async fn cancel_file(&self, file_id: FylesId) -> P2pResult<bool> {
        let (sender, receiver) = oneshot::channel();
        info!("Client: Cancelling file {}", file_id);
        self.command_sender
            .send(P2pCommand::CancelFile { file_id, sender })
            .await
            .map_err(|e| P2pError::InternalCommunicationError {
                msg: "Could not instruct node to cancel file".into(),
                source: Arc::new(e),
            })?;
        receiver.await.to_arced_dyn_err()?.auto_map_err()
    }

    async fn cancel_files_for_remote_filerequest(
        &self,
        target_filerequest_id: FylesId,
        peer_id: PeerIdWrapper,
    ) -> P2pResult<bool> {
        let (sender, receiver) = oneshot::channel();
        info!(
            "Client: Cancelling files for remote filerequest {}",
            target_filerequest_id
        );
        self.command_sender
            .send(P2pCommand::CancelFilesForRemoteFilerequest {
                peer_id: peer_id.unwrap_thing(),
                target_filerequest_id,
                sender,
            })
            .await
            .map_err(|e| P2pError::InternalCommunicationError {
                msg: "Could not instruct node to cancel file".into(),
                source: Arc::new(e),
            })?;
        receiver.await.to_arced_dyn_err()?.auto_map_err()
    }

    fn generate_keypair(&self) -> Result<Vec<u8>, KeypairGenerationError> {
        let keypair = identity::Keypair::generate_ed25519();
        encode_keypair(&keypair).map_err(|_| KeypairGenerationError::SerializationError)
    }

    async fn update_identity(&self, contact_id: ContactId, keys: ContactKeys) {
        info!("Client: Updating identity for contact {}", contact_id);
        self.command_sender
            .send(P2pCommand::UpdateIdentity { contact_id, keys })
            .await
            .expect("Command receiver not to be dropped.");
    }

    async fn use_self_contact_invite(
        &self,
        invite_code: SelfContactInviteChallenge,
        peer_id: PeerIdWrapper,
    ) -> () {
        info!("Client: Using self contact invite");
        let command = P2pCommand::UseSelfContactInviteChallenge {
            invite_code,
            peer_id,
        };
        self.command_sender
            .send(command)
            .await
            .expect("Command receiver not to be dropped.");
    }

    async fn use_contact_share_challenge(
        &self,
        share_code: ContactShareChallenge,
        peer_id: PeerIdWrapper,
    ) -> () {
        info!("Client: Using self contact invite");
        let command = P2pCommand::UseContactShareChallenge {
            share_code,
            peer_id,
        };
        self.command_sender
            .send(command)
            .await
            .expect("Command receiver not to be dropped.");
    }

    async fn apply_settings(&self, settings: &[u8]) -> P2pResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender
            .send(P2pCommand::ApplySettings {
                settings: settings.to_vec(),
                sender,
            })
            .await
            .map_err(|e| P2pError::InternalCommunicationError {
                msg: "Could not send ApplySettings command".into(),
                source: Arc::new(e),
            })?;
        receiver.await.to_arced_dyn_err()?.auto_map_err()
    }
}

#[cfg(test)]
mod test {
    use libp2p::identity;

    #[test]
    fn test_generate_encode_decode_does_not_error() {
        let keypair = identity::Keypair::generate_ed25519();
        let key_bytes = keypair.clone().to_protobuf_encoding().unwrap();
        let restored_keypair =
            identity::Keypair::from_protobuf_encoding(&key_bytes).expect("Keypair to be restored");
        assert_eq!(
            keypair.public().to_peer_id().to_base58(),
            restored_keypair.public().to_peer_id().to_base58()
        );
    }

    #[test]
    fn test_generate_encode_decode_from_bytes_does_not_error() {
        let keypair = identity::Keypair::generate_ed25519();
        let key_bytes = keypair.clone().try_into_ed25519().unwrap().to_bytes();

        let private_key_bytes: [u8; 32] = key_bytes[..32].try_into().unwrap();

        let restored_keypair = identity::Keypair::ed25519_from_bytes(private_key_bytes)
            .expect("Keypair to be restored");

        assert_eq!(
            keypair.public().to_peer_id().to_base58(),
            restored_keypair.public().to_peer_id().to_base58()
        );
    }
}
