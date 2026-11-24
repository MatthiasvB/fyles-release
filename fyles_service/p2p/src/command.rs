use std::{error::Error, sync::Arc};

use crypto::ContactKeys;
use libp2p::PeerId;
use tokio::sync::oneshot;

#[cfg(feature = "debug-server")]
use crate::debug::SerializeDebug;

use fyles_core::core::{
    brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
    domain_models::{ContactId, FylesId, PeerIdWrapper},
    listen::{ListenFailure, ListenPort},
    p2p::{FileToSend, NodeConfig, NodeStatusInfo},
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
    /// Rebuild from an opaque runtime configuration. The base crate has nothing to rebuild and
    /// drops it. `p2p_internet` overrides this and needs `&mut self` to swap the swarm.
    Restart(NodeConfig),
    ListenOnPort {
        port: ListenPort,
        reply: oneshot::Sender<Result<(), ListenFailure>>,
    },
    #[cfg(feature = "debug-server")]
    GetDebugBlob {
        sender: oneshot::Sender<Box<dyn SerializeDebug + Send>>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum DataCommand {
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

    #[cfg(feature = "debug-server")]
    GetDebugBlob {
        sender: oneshot::Sender<Box<dyn SerializeDebug + Send>>,
    },
}
