use async_trait::async_trait;
use crypto::ContactKeys;
use std::sync::Arc;
use thiserror::Error;

use crate::core::{
    brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
    domain_models::{ContactId, PeerIdWrapper, SendStatus},
};

use super::domain_models::FylesId;

pub type P2pResult<T> = Result<T, P2pError>;

pub type NodeKeyPairBinary = Vec<u8>;

#[derive(Debug, Clone)]
pub struct NodeStatusInfo {
    pub peer_id: PeerIdWrapper,
    pub external_addresses: Vec<String>,
    pub internal_addresses: Vec<String>,
    pub connected_peers: usize,
    pub start_timestamp: u128,
}

/// Very similar to `PendingFile`, but the target filerequest id has
/// been resolved to the external id
#[derive(Debug)]
pub struct FileToSend {
    pub id: FylesId,
    pub peer_id: PeerIdWrapper,
    pub contact_id: ContactId,
    pub filerequest_id: FylesId,
    pub file_path: String,
    pub retry_count: usize,
    pub status: SendStatus,
}

#[derive(Error, Debug)]
pub enum KeypairGenerationError {
    #[error("Error during key serialization")]
    SerializationError,
}

#[async_trait]
pub trait Runnable {
    async fn run(&self);
}

/// All the high level operations the p2p node needs to perform.
#[async_trait]
pub trait NetworkNode: Send + Sync {
    fn display_keypair(
        &self,
        keypair: &NodeKeyPairBinary,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;
    async fn send_files(&self, file_to_send: Vec<FileToSend>) -> P2pResult<()>;
    async fn initial_files_to_send(&self, files: Vec<FileToSend>) -> P2pResult<()>;
    async fn get_node_info(&self) -> P2pResult<NodeStatusInfo>;
    async fn cancel_file(&self, file_id: FylesId) -> P2pResult<bool>;
    async fn cancel_files_for_remote_filerequest(
        &self,
        target_filerequest_id: FylesId,
        peer_id: PeerIdWrapper
    ) -> P2pResult<bool>;
    fn generate_keypair(&self) -> Result<Vec<u8>, KeypairGenerationError>;
    async fn update_identity(&self, contact_id: ContactId, keys: ContactKeys);
    async fn use_self_contact_invite(
        &self,
        invite_code: SelfContactInviteChallenge,
        peer_id: PeerIdWrapper,
    ) -> ();
    async fn use_contact_share_challenge(
        &self,
        share_code: ContactShareChallenge,
        peer_id: PeerIdWrapper,
    ) -> ();
    /// Apply opaque settings payload. The idea is to pass settings through in serialized form. Your swarm factory
    /// can choose to deserialize and apply them as needed. This gives a lot of flexibility in using this library.
    async fn apply_settings(&self, _settings: &[u8]) -> P2pResult<()>;
}

pub trait RunnableNetworkNode: NetworkNode + Runnable {}

impl<T> RunnableNetworkNode for T where T: NetworkNode + Runnable {}

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum P2pError {
    #[error("File not accepted for filerequest_id: {filerequest_id}")]
    FileNotAccepted {
        filerequest_id: FylesId,
        #[source]
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
    #[error("Network error: \"{msg}\": {source}")]
    NetworkError {
        msg: String,
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    #[error("Internal communication error: \"{msg}\": {source}")]
    InternalCommunicationError {
        msg: String,
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    #[error("Unspecified internal error: {0}")]
    UnspecifiedError(#[from] Arc<dyn std::error::Error + Send + Sync>),
}
