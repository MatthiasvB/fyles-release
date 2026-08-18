use async_trait::async_trait;
use crypto::ContactKeys;
use std::sync::Arc;
use thiserror::Error;

use crate::core::{
    brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
    domain_models::{ContactId, PeerIdWrapper, SendStatus},
    sync::{DocKind, SyncRequest},
};

use super::domain_models::FylesId;

pub type P2pResult<T> = Result<T, P2pError>;

pub type NodeKeyPairBinary = Vec<u8>;

/// Derive the base58 `PeerId` string from a stored node keypair.
///
/// CRATE-HYGIENE EXCEPTION: key / `PeerId` handling normally lives entirely in the `p2p` crate
/// (`p2p::utils::extract_peer_id`); `core` is otherwise transport-agnostic and does not touch
/// libp2p key material. This single function is a deliberate exception so the `seed_sync_docs`
/// migration can register the local device in the synced `Devices` document without a running
/// network node. It is a free function rather than a method because `NodeKeyPairBinary` is a
/// `Vec<u8>` alias. Mirrors `p2p::utils::extract_peer_id` (`to_base58()` is the bs58 encoding of
/// the peer id bytes that helper produces).
///
/// Crate hygiene may be restored eventually when that migration isn't needed anymore
///
/// Offending migration: 1.22.0
pub fn peer_id_from_node_keypair(
    keypair: &NodeKeyPairBinary,
) -> Result<String, libp2p::identity::DecodingError> {
    let kp = libp2p::identity::Keypair::from_protobuf_encoding(keypair)?;
    Ok(kp.public().to_peer_id().to_base58())
}

#[test]
fn check_crate_hygiene_restoration_ready() {
    if crate::library::sqlite::schema::CURRENT_VERSION >= semver::Version::new(1, 100, 0) {
        panic!("Crate hygiene should be restored");
    }
}

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
        peer_id: PeerIdWrapper,
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
    /// Send one automerge sync-protocol message to an owned device. The peer's reply
    /// arrives asynchronously as `NetworkNodeAction::SyncReplyReceived`. Best-effort.
    async fn send_sync_message(&self, peer_id: PeerIdWrapper, wire: SyncRequest);
    /// Batch-fetch several contacts' public keys from `peer` (over `/fyles/pubkey-fetch`) in
    /// one round trip. The reply arrives asynchronously as
    /// `NetworkNodeAction::StoreFetchedContactKeys`. Best-effort — used to resolve contacts
    /// learned via sync that lack cached keys.
    async fn fetch_contact_keys(&self, peer_id: PeerIdWrapper, contact_ids: Vec<ContactId>);
    /// Re-poke every peer the node can currently see: send anything queued for it, and sync with it
    /// if it is one of our devices.
    async fn trigger_interactions_with_visible_peers(&self);
    /// Sync `kind` with whichever of `devices` the node can currently reach, answering with a
    /// [`NetworkNodeAction::SyncDocWithPeer`](crate::core::brain::action_p2p::NetworkNodeAction)
    /// per hit.
    async fn sync_docs_with_reachable(&self, devices: Vec<PeerIdWrapper>, kind: DocKind);
    /// Process an opaque, serialized settings request and return an opaque,
    /// serialized response. Core stays schema-agnostic: it neither builds nor
    /// interprets these bytes, it only relays them. The concrete network
    /// implementation decodes the request, applies/persists any changes, and
    /// encodes a response. This is the single channel for all settings reads and
    /// mutations from the frontend.
    async fn process_settings_request(&self, request: &[u8]) -> P2pResult<Vec<u8>>;
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
