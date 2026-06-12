use crypto::{ContactKeys, ContactPublicKeys};

use crate::{
    core::{
        brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
        domain_models::{Contact, ContactId, FylesId, SelfContact},
        filerequest_drive_handler::FilerequestDriveHandler,
        p2p::NodeKeyPairBinary,
    },
    io_controller::{FileMeta, Uri},
};

use super::types::{BrainRequest, FilerequestResult};

use crate::core::domain_models::{
    CompleteReceivedFile, CreateIncomingFile, ReceiveStatus, SendStatus,
};

#[derive(Clone)]
#[cfg_attr(any(test, feature = "test-support"), derive(PartialEq, Debug))]
pub struct NodeInfo {
    pub node_key_pair: NodeKeyPairBinary,
    pub self_contact_id: ContactId,
    pub self_contact_keys: ContactKeys,
}

impl NodeInfo {
    pub fn generate_random_bytes(node_key_pair: Vec<u8>) -> Self {
        Self {
            node_key_pair,
            self_contact_id: ContactId::new(),
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

#[derive(Debug)]
pub enum NetworkNodeAction {
    RequestFileDrop(
        BrainRequest<FilerequestAccessRequest, Option<FilerequestDriveHandler>>,
        tracing::Span,
    ),
    RequestFileTransferContinuation(
        BrainRequest<FilerequestContinueRequest, Option<FilerequestContinueResponse>>,
    ),
    GetNodeInfo(BrainRequest<(), FilerequestResult<(NodeInfo, Vec<u8>)>>),
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
    /// Remove a completed/failed received file entry from the table.
    DeleteReceivedFile {
        transfer_id: FylesId,
    },
    GetContactPublicKeys(BrainRequest<ContactId, Option<ContactPublicKeys>>),
    IsContactKnown(BrainRequest<ContactId, Result<bool, String>>),
    OpenFileForReading(BrainRequest<OpenFileForReadingRequest, Result<FileMeta, ()>>),
    ValidateSelfContactInviteChallenge(
        BrainRequest<SelfContactInviteChallenge, Option<SelfContact>>,
    ),
    UpdateIdentity(SelfContact),
    AnsweredSelfContactInvite,
    RejectedSelfContactInvite,
    SelfContactInviteGotRejected,
    ValidateContactShareChallenge(BrainRequest<ContactShareChallenge, Option<Contact>>),
    CreateContact(Contact),
    AnsweredContactShare,
    RejectedContactShare,
    ContactShareGotRejected,
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
