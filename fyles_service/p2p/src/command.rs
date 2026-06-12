use std::{error::Error, sync::Arc};

use crypto::ContactKeys;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::oneshot;

use fyles_core::core::{
    brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
    domain_models::{ContactId, FylesId, PeerIdWrapper},
    p2p::{FileToSend, NodeStatusInfo},
};

#[derive(Debug)]
pub enum P2pCommand {
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
    ApplySettings {
        settings: Vec<u8>,
        sender: oneshot::Sender<Result<(), Arc<dyn Error + Send + Sync>>>,
    },
}
