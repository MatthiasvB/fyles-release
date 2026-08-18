use std::{error::Error, sync::Arc};

use crypto::ContactKeys;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::oneshot;

use fyles_core::core::{
    brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
    domain_models::{ContactId, FylesId, PeerIdWrapper},
    p2p::{FileToSend, NodeStatusInfo},
    sync::{DocKind, SyncRequest},
};

/// One channel, two handlers. [`LoopCommand`] needs `&mut LocalEventLoop`; [`DataCommand`] only
/// needs the shared loop data and is spawned. Keeping them in one enum on one channel preserves
/// ordering between them, and keeps `handle_command` structurally unable to receive a loop command.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum P2pCommand {
    Loop(LoopCommand),
    Data(DataCommand),
}

impl From<LoopCommand> for P2pCommand {
    fn from(command: LoopCommand) -> Self {
        Self::Loop(command)
    }
}

impl From<DataCommand> for P2pCommand {
    fn from(command: DataCommand) -> Self {
        Self::Data(command)
    }
}

/// Handled inline on the loop task, because it needs state the loop owns exclusively.
#[derive(Debug)]
pub enum LoopCommand {
    /// Needs the loop task because the peer set is read straight off the swarm.
    TriggerVisiblePeerInteractions,
    /// The base crate has no settings schema and answers empty; `p2p_internet` overrides it and
    /// needs `&mut self` to persist changes.
    ProcessSettingsRequest {
        request: Vec<u8>,
        sender: oneshot::Sender<Result<Vec<u8>, Arc<dyn Error + Send + Sync>>>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum DataCommand {
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Arc<dyn Error + Send + Sync>>>,
    },
    SendFiles {
        files_to_send: Vec<FileToSend>,
        sender: oneshot::Sender<Result<(), Arc<dyn Error + Send + Sync>>>,
    },
    InitialFilesToSend {
        files_to_send: Vec<FileToSend>,
        sender: oneshot::Sender<Result<(), Arc<dyn Error + Send + Sync>>>,
    },
    GetNodeInfo {
        sender: oneshot::Sender<NodeStatusInfo>,
    },
    CancelFile {
        file_id: FylesId,
        sender: oneshot::Sender<Result<bool, Arc<dyn Error + Send + Sync>>>,
    },
    CancelFilesForRemoteFilerequest {
        target_filerequest_id: FylesId,
        peer_id: PeerId,
        sender: oneshot::Sender<Result<bool, Arc<dyn Error + Send + Sync>>>,
    },
    UpdateIdentity {
        contact_id: ContactId,
        keys: ContactKeys,
    },
    UseSelfContactInviteChallenge {
        invite_code: SelfContactInviteChallenge,
        peer_id: PeerIdWrapper,
    },
    UseContactShareChallenge {
        share_code: ContactShareChallenge,
        peer_id: PeerIdWrapper,
    },
    SendSyncMessage {
        peer_id: PeerIdWrapper,
        wire: SyncRequest,
    },
    FetchContactKeys {
        peer_id: PeerIdWrapper,
        contact_ids: Vec<ContactId>,
    },
    /// Intersect `devices` with what is reachable and ask the brain to sync `kind` with each hit.
    SyncDocsWithReachable {
        devices: Vec<PeerIdWrapper>,
        kind: DocKind,
    },
}
