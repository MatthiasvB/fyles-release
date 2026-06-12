use crate::core::brain::action_p2p::NodeInfo;
use crate::core::db::DbError;
use crate::core::domain_models::{
    CompleteReceivedFile, Contact, ContactId, CreateFilerequest, CreateIncomingFile,
    CreatePendingFiles, CreateRemoteFilerequest, DisplayContact, Filerequest, FylesId, PendingFile,
    ReceiveStatus, ReceivedFile, RemoteFilerequest, SelfContact, SendStatus,
};
use crypto::ContactPublicKeys;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::oneshot::Sender;
use tracing::Span;

/// Message types for actor communication
pub enum SqliteMsg {
    GetFilerequest(FylesId, Sender<Result<Filerequest, DbError>>, Span),
    GetAllFilerequests(Sender<Result<Vec<Filerequest>, DbError>>),
    CreateFilerequest(CreateFilerequest, Sender<Result<FylesId, DbError>>),
    UpdateFilerequest(Filerequest, Sender<Result<(), DbError>>),
    DeleteFilerequest(FylesId, Sender<Result<(), DbError>>),
    // Contact messages
    GetContactName(ContactId, Sender<Result<String, DbError>>),
    GetContactPublicKeys(
        ContactId,
        Sender<Result<Option<ContactPublicKeys>, DbError>>,
    ),
    GetContactNames(
        Vec<ContactId>,
        Sender<Result<HashMap<ContactId, String>, DbError>>,
    ),
    GetContact(ContactId, Sender<Result<Contact, DbError>>),
    GetContacts(Sender<Result<Vec<DisplayContact>, DbError>>),
    UpdateContact(DisplayContact, Sender<Result<(), DbError>>),
    DeleteContact(ContactId, Sender<Result<(), DbError>>),
    StoreNodeKeys(NodeInfo, Sender<Result<(), DbError>>),
    GetNodeInfo(Sender<Result<NodeInfo, DbError>>),
    // Remote filerequest messages
    GetRemoteFilerequest(FylesId, Sender<Result<RemoteFilerequest, DbError>>),
    GetRemoteFilerequestsByContact(ContactId, Sender<Result<Vec<RemoteFilerequest>, DbError>>),
    GetAllRemoteFilerequests(Sender<Result<Vec<RemoteFilerequest>, DbError>>),
    CreateRemoteFilerequest(CreateRemoteFilerequest, Sender<Result<FylesId, DbError>>),
    UpdateRemoteFilerequest(FylesId, String, Sender<Result<(), DbError>>),
    DeleteRemoteFilerequest(FylesId, Sender<Result<(), DbError>>),

    // Pending file messages
    GetPendingFile(FylesId, Sender<Result<PendingFile, DbError>>),
    GetPendingFiles(String, Sender<Result<Vec<PendingFile>, DbError>>),
    GetAllPendingFiles(Sender<Result<Vec<PendingFile>, DbError>>),
    CreatePendingFile(CreatePendingFiles, Sender<Result<Vec<FylesId>, DbError>>),
    UpdatePendingFileStatus((FylesId, SendStatus, Option<usize>), Sender<Result<FylesId, DbError>>),
    AddInterruptionReason((FylesId, String), Sender<Result<(), DbError>>),
    DeletePendingFile(FylesId, Sender<Result<(), DbError>>),
    CountNonTerminalPendingFilesForPath(FylesId, Sender<Result<usize, DbError>>),

    // Received file messages (covers both in-progress and completed)
    CreateIncomingFile(CreateIncomingFile, Sender<Result<FylesId, DbError>>),
    UpdateReceivedFileStatus((FylesId, ReceiveStatus, u64), Sender<Result<(), DbError>>),
    GetReceivedFileByTransferId(FylesId, Sender<Result<ReceivedFile, DbError>>),
    CompleteReceivedFile(CompleteReceivedFile, Sender<Result<FylesId, DbError>>),
    ListReceivedFiles(FylesId, Sender<Result<Vec<ReceivedFile>, DbError>>),
    DeleteReceivedFile(FylesId, Sender<Result<(), DbError>>),
    GetStaleReceivedFiles(i64, Sender<Result<Vec<ReceivedFile>, DbError>>),

    // SelfContact messages
    GetSelfContact(Sender<Result<SelfContact, DbError>>),
    GetSelfContactForDisplay(Sender<Result<DisplayContact, DbError>>),
    UpdateSelfContactName(String, Sender<Result<(), DbError>>),
    UpdateIdentity(SelfContact, Sender<Result<(), DbError>>),
    GetSharablePublicSelfContact(Sender<Result<Contact, DbError>>),
    RegisterContact(Contact, Sender<Result<(), DbError>>),

    Backup {
        backup_file: std::fs::File,
        internal_data_dir: PathBuf,
        response_sender: Sender<Result<(), DbError>>,
    },

    Restore {
        backup_file: tokio::fs::File,
        response_sender: Sender<Result<(), DbError>>,
    },

    // Opaque settings
    GetSettings(Sender<Result<Vec<u8>, DbError>>),
    StoreSettings(Vec<u8>, Sender<Result<(), DbError>>),
}
