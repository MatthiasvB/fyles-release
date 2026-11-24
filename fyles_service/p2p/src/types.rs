use crypto::stream::{DirectionKeys, Role};
use libp2p::PeerId;
use rand::{Rng, RngExt};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::{path::PathBuf, str::FromStr};
use tokio::fs::File;
use uuid::Uuid;
use zeroize::Zeroize;

use fyles_core::core::domain_models::{FylesId, PeerIdWrapper};

#[derive(Debug, Clone)]
#[repr(C)]
pub struct Config {
    pub db_path: PathBuf,
    pub internal_data_dir: PathBuf,
    pub endpoint: String,
}

pub struct FileReceiveState {
    pub has_errored: bool,
    pub file: File,
    pub path: String,
    pub bytes_received: u64,
    pub total_size: u64,
}

/// File transfer requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileRequest {
    /// Responses: `AcceptNewTransfer`, `RejectNewTransfer`, Errors
    ///
    /// Initiates a new file transfer
    NewTransfer {
        filerequest_id: FylesId,
        file_name: String,
        file_size_bytes: u64,
        transfer_uuid: FylesId,
    },
    /// Responses: `AcceptContinueTransfer`, `RestartContinueTransfer`, Errors
    ///
    /// Requests continuation of a previously interrupted transfer. Other end responds
    /// with the byte position in the *.part file at which to continue. If for any reason
    /// it is not possible to continue, for example because the *.part file has become stale
    /// and was cleaned up, the other end may respond with [`RestartContinueTransfer`], in which
    /// case we simply continue with [`NewTransfer`].
    ContinueTransfer {
        filerequest_id: FylesId,
        transfer_uuid: FylesId,
    },
    /// Optional response: `ConfirmAbort`, Errors
    ///
    /// If for whatever reason we will never be able to continue the transfer. The other end will
    /// acknowledge with `ConfirmAbort`, if the connection is still alive.
    AbortTransfer { transfer_uuid: Option<FylesId> },
    /// Responses: `ConfirmChunk`, `RejectChunk`, Errors
    ///
    /// Transfers a chunk of data. The `idx` field indicates the position of the chunk in the transfer, starting with 0.
    /// Resets to 0 even for continued transfers.
    Chunk(DataChunk),
    /// Responses: `ConfirmDone`, `RejectDone`, Errors
    ///
    /// Indicates that the sender has finished sending all chunks. The receiver should respond with `ConfirmDone` if
    /// the transfer is complete and the file can be finalized, for example by renaming the *.part file to the final name,
    /// or with `RejectDone` if something went wrong. In any case, the sender will not retry sending the file.
    Done { transfer_uuid: FylesId },
    /// Responses: `AcknowledgeProtocolViolation`, Errors
    ///
    /// Sent whenever the response doesn't fit the request
    ProtocolViolation { transfer_uuid: Option<FylesId> },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataChunk {
    pub transfer_uuid: FylesId,
    pub offset: u64,
    pub data: Vec<u8>,
}

impl Debug for DataChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        struct OpaqueData {
            len: usize,
        }

        impl Debug for OpaqueData {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "<opaque {} bytes>", self.len)
            }
        }

        f.debug_struct("DataChunk")
            .field("transfer_uuid", &self.transfer_uuid)
            .field("offset", &self.offset)
            .field(
                "data",
                &OpaqueData {
                    len: self.data.len(),
                },
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileResponse {
    AcceptNewTransfer,
    RejectNewTransfer,
    AcceptContinueTransfer {
        /// Byte offset (index, 0 based, equal to "`*part.len()`"`) at which to continue the transfer. So this is the
        /// index of the first byte that needs to be appended to the *part file.
        offset: u64,
    },
    /// Do not continue, start fresh
    RestartContinueTransfer,
    RejectContinueTransfer,
    ConfirmChunk,
    RejectChunk,
    ConfirmAbort,
    ConfirmDone,
    RejectDone,
    AcknowledgeProtocolViolation,
    // Errors
    ProtocolViolation,
    /// Indicates and unknown internal error occurs. Unless there is additional context, the sender should assume
    /// that the transfer has been rejected.
    InternalError,
}

impl FileResponse {
    pub fn random() -> Self {
        let mut rng = rand::rng();
        match rng.random_range(1..=13) {
            1 => Self::AcceptNewTransfer,
            2 => Self::RejectNewTransfer,
            3 => Self::AcceptContinueTransfer {
                offset: rng.next_u64(),
            },
            4 => Self::RestartContinueTransfer,
            5 => Self::RejectContinueTransfer,
            6 => Self::ConfirmChunk,
            7 => Self::RejectChunk,
            8 => Self::ConfirmAbort,
            9 => Self::ConfirmDone,
            10 => Self::RejectDone,
            11 => Self::AcknowledgeProtocolViolation,
            12 => Self::ProtocolViolation,
            _ => Self::InternalError,
        }
    }
}

/// File transfer control for peers that carry the data over a stream. The data itself and the
/// completion travel on the stream the grant in an accept opens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileRequestV2 {
    NewTransfer {
        filerequest_id: FylesId,
        file_name: String,
        file_size_bytes: u64,
        transfer_uuid: FylesId,
    },
    ContinueTransfer {
        filerequest_id: FylesId,
        transfer_uuid: FylesId,
    },
    AbortTransfer {
        transfer_uuid: Option<FylesId>,
    },
    ProtocolViolation {
        transfer_uuid: Option<FylesId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileResponseV2 {
    AcceptNewTransfer { grant: AttemptGrant },
    AcceptContinueTransfer { offset: u64, grant: AttemptGrant },
    RejectNewTransfer,
    RestartContinueTransfer,
    RejectContinueTransfer,
    ConfirmAbort,
    AcknowledgeProtocolViolation,
    ProtocolViolation,
    InternalError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttemptId(Uuid);

impl AttemptId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Admits one stream for one attempt. It only ever travels inside the session-encrypted accept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptGrant {
    pub attempt: AttemptId,
    pub key: GrantKey,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantKey(pub [u8; 32]);

impl Drop for GrantKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl Debug for GrantKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("GrantKey(..)")
    }
}

impl AttemptGrant {
    pub fn mint() -> Self {
        let mut key = GrantKey([0u8; 32]);
        OsRng.fill_bytes(&mut key.0);
        Self {
            attempt: AttemptId(Uuid::new_v4()),
            key,
        }
    }

    pub fn keys(&self, role: Role) -> DirectionKeys {
        DirectionKeys::derive(self.attempt.as_bytes(), &self.key.0, role)
    }
}

pub(crate) trait Wrap {
    type Wrapper;

    fn wrap(self) -> Self::Wrapper;
}

impl Wrap for PeerId {
    type Wrapper = PeerIdWrapper;

    fn wrap(self) -> Self::Wrapper {
        PeerIdWrapper(self.to_base58())
    }
}

pub trait Unwrap {
    type Inner;

    fn unwrap_thing(self) -> Self::Inner;
}

impl Unwrap for PeerIdWrapper {
    type Inner = PeerId;

    fn unwrap_thing(self) -> Self::Inner {
        (&self).unwrap_thing()
    }
}

impl Unwrap for &PeerIdWrapper {
    type Inner = PeerId;

    fn unwrap_thing(self) -> Self::Inner {
        PeerId::from_str(&self.0).expect("Invalid PeerId") // FIXME
    }
}
