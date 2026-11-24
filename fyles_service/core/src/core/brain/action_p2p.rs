use crate::core::p2p::NodeConfig;
use crypto::{ContactKeys, ContactPublicKeys};

use crate::{
    core::{
        brain::types::{
            ContactShareChallenge, PubkeyFetchResponse, SelfContactInviteChallenge,
            SignedContactShare,
        },
        domain_models::{ContactId, FylesId, SelfContact},
        filerequest_drive_handler::FilerequestDriveHandler,
        p2p::NodeKeyPairBinary,
    },
    io_controller::{FileMeta, Uri},
};

use super::types::{BrainRequest, FilerequestResult};

use crate::core::domain_models::{
    CompleteReceivedFile, CreateIncomingFile, PeerIdWrapper, ReceiveStatus, SendStatus,
};
use crate::core::sync::{DocKind, SyncRequest, SyncResponse};

#[derive(Debug)]
pub struct SyncReceiveRequest {
    pub contact_id: ContactId,
    pub peer: PeerIdWrapper,
    pub wire: SyncRequest,
}

#[derive(Debug)]
pub struct PubkeyFetchReceiveRequest {
    pub contact_id: ContactId,
    pub wanted_contact_ids: Vec<ContactId>,
}

#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(PartialEq, Debug))]
pub struct NodeInfo {
    pub node_key_pair: NodeKeyPairBinary,
    pub self_contact_keys: ContactKeys,
}

impl NodeInfo {
    pub fn generate_random_bytes(node_key_pair: Vec<u8>) -> Self {
        Self {
            node_key_pair,
            self_contact_keys: ContactKeys::new(),
        }
    }
}

#[derive(Debug)]
pub struct OpenFileForReadingRequest {
    pub uri: Uri,
    pub id: FylesId,
    pub byte_offset: u64,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum NetworkNodeAction {
    RequestFileDrop(
        BrainRequest<FilerequestAccessRequest, Option<FilerequestDriveHandler>>,
        tracing::Span,
    ),
    RequestFileTransferContinuation(
        BrainRequest<FilerequestContinueRequest, Option<FilerequestContinueResponse>>,
    ),
    GetNodeInfo(BrainRequest<(), FilerequestResult<(NodeInfo, NodeConfig)>>),
    Ready,
    FileSending {
        pending_file_id: FylesId,
        status: SendStatus,
    },
    /// Sent whenever a pending file ceased to be sending while not being done
    FileSendReset {
        pending_file_id: FylesId,
        status: SendStatus,
        retry_count: Option<usize>,
        reason: Option<String>,
    },
    FileSent {
        pending_file_id: FylesId,
    },
    FileRejected {
        pending_file_id: FylesId,
    },
    FileFailed {
        pending_file_id: FylesId,
    },
    FileMissing {
        pending_file_id: FylesId,
    },
    FileIoError {
        file: FylesId,
        error: std::io::Error,
    },
    StoreReceivedFile(BrainRequest<CompleteReceivedFile, FilerequestResult<FylesId>>),
    /// Persist a new receiver-side in-progress file entry and notify the frontend.
    CreateIncomingFile(BrainRequest<CreateIncomingFile, FilerequestResult<FylesId>>),
    /// Update the status / progress of an in-progress receive and push to frontend.
    UpdateReceivedFileStatus {
        transfer_id: FylesId,
        status: ReceiveStatus,
        progress_bytes: u64,
    },
    /// The sending peer withdrew this transfer. Its partial file and its record go, as long as it is
    /// still in progress and belongs to that peer and contact.
    DropIncomingTransfer {
        transfer_id: FylesId,
        peer_id: String,
        contact_id: Option<ContactId>,
    },
    GetContactPublicKeys(BrainRequest<ContactId, Option<ContactPublicKeys>>),
    /// Batched key lookup for the pubkey-fetch responder. `None` means decline to answer at all —
    /// the requesting session isn't one of our own devices.
    GetContactsPublicKeys(
        BrainRequest<PubkeyFetchReceiveRequest, Option<Vec<(ContactId, ContactPublicKeys)>>>,
    ),
    /// Whether a session could prove this contact, which takes its keys. Our own identity counts.
    HoldsContactKeys(BrainRequest<ContactId, Result<bool, String>>),
    OpenFileForReading(BrainRequest<OpenFileForReadingRequest, Result<FileMeta, ()>>),
    ValidateSelfContactInviteChallenge(
        BrainRequest<SelfContactInviteChallenge, Option<SelfContact>>,
    ),
    UpdateIdentity(SelfContact, PeerIdWrapper),
    AnsweredSelfContactInvite,
    RejectedSelfContactInvite,
    SelfContactInviteGotRejected,
    ValidateContactShareChallenge(BrainRequest<ContactShareChallenge, Option<SignedContactShare>>),
    ContactShareReceived(SignedContactShare),
    AnsweredContactShare,
    RejectedContactShare,
    ContactShareGotRejected,
    /// `None` means don't reply at all, likely because we suspect being under attack.
    SyncReceive(BrainRequest<SyncReceiveRequest, Option<SyncResponse>>),
    SyncReplyReceived {
        peer: PeerIdWrapper,
        reply: SyncResponse,
    },
    SyncOnPeerConnect(PeerIdWrapper),
    SyncDocWithPeer {
        peer: PeerIdWrapper,
        kind: DocKind,
    },
    SyncTransportFailed {
        peer: PeerIdWrapper,
    },
    StoreFetchedContactKeys(PubkeyFetchResponse),
}

#[derive(Debug)]
pub struct FilerequestAccessRequest {
    pub filerequest_id: FylesId,
    pub contact_id: Option<ContactId>,
}

#[derive(Debug)]
pub struct FilerequestContinueRequest {
    pub filerequest_id: FylesId,
    pub contact_id: Option<ContactId>,
    pub peer_id: String,
    pub transfer_id: FylesId,
}

pub struct FilerequestContinueResponse {
    pub drive_handler: FilerequestDriveHandler,
    pub file_name: String,
    pub file_size_bytes: u64,
    /// Milliseconds since UNIX epoch when the transfer was first started (from DB).
    pub started_at_ms: i64,
}
