use crate::core::backup::{RestoredFrom, StoredBackupSecret, VerificationSchedule};
use crate::core::brain::action_p2p::NodeInfo;
use crate::core::db::DbError;
use crate::core::domain_models::{
    CompleteReceivedFile, Contact, ContactId, CreateFilerequest, CreateIncomingFile,
    CreatePendingFiles, CreateRemoteFilerequest, DisplayContact, Filerequest, FilerequestId,
    FylesId, PendingFile, ReceiveStatus, ReceivedFile, RemoteFilerequest, SelfContact, SendStatus,
};
use crate::core::listen::ListenPort;
use crate::core::p2p::NodeKeyPairBinary;
use crate::core::settings::SettingsBlob;
use crate::core::sync::{DeviceChange, DevicesDoc, DocKind};
use automerge::AutoCommit;
use crypto::{ContactKeys, ContactPublicKeys};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::oneshot::Sender;
use tracing::Span;

/// Message types for actor communication
pub enum SqliteMsg {
    GetFilerequest(FylesId, Sender<Result<Filerequest, DbError>>, Span),
    GetAllFilerequests(Sender<Result<Vec<Filerequest>, DbError>>),
    CreateFilerequest(FylesId, CreateFilerequest, Sender<Result<(), DbError>>),
    UpdateFilerequest(Filerequest, Sender<Result<(), DbError>>),
    DeleteFilerequest(FylesId, Sender<Result<(), DbError>>),
    // Contact messages
    GetContactName(ContactId, Sender<Result<String, DbError>>),
    GetContactPublicKeys(
        ContactId,
        Sender<Result<Option<ContactPublicKeys>, DbError>>,
    ),
    KeylessSyncedContacts(Sender<Result<Vec<ContactId>, DbError>>),
    GetContactsPublicKeys(
        Vec<ContactId>,
        Sender<Result<Vec<(ContactId, ContactPublicKeys)>, DbError>>,
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
    GetNodeKeypair(Sender<Result<NodeKeyPairBinary, DbError>>),
    GetSelfContactId(Sender<Result<ContactId, DbError>>),
    StoreSigningKeys(ContactKeys, Sender<Result<(), DbError>>),
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
    UpdatePendingFileStatus(
        (FylesId, SendStatus, Option<usize>),
        Sender<Result<FylesId, DbError>>,
    ),
    AddInterruptionReason((FylesId, String), Sender<Result<(), DbError>>),
    DeletePendingFile(FylesId, Sender<Result<(), DbError>>),
    CountNonTerminalPendingFilesForPath(FylesId, Sender<Result<usize, DbError>>),

    // Received file messages (covers both in-progress and completed)
    CreateIncomingFile(CreateIncomingFile, Sender<Result<FylesId, DbError>>),
    UpdateReceivedFileStatus((FylesId, ReceiveStatus, u64), Sender<Result<(), DbError>>),
    GetReceivedFileByTransferId(FylesId, Sender<Result<ReceivedFile, DbError>>),
    CompleteReceivedFile(CompleteReceivedFile, Sender<Result<FylesId, DbError>>),
    ListReceivedFiles(FylesId, Sender<Result<Vec<ReceivedFile>, DbError>>),
    DeleteReceivedFile(FylesId, Sender<Result<FilerequestId, DbError>>),
    GetStaleReceivedFiles(i64, Sender<Result<Vec<ReceivedFile>, DbError>>),

    // SelfContact messages
    GetSelfContact(Sender<Result<SelfContact, DbError>>),
    GetSelfContactForDisplay(Sender<Result<DisplayContact, DbError>>),
    UpdateSelfContactName(String, Sender<Result<(), DbError>>),
    UpdateIdentity(SelfContact, Sender<Result<(), DbError>>),
    UpdateContactProjection(Contact, Sender<Result<(), DbError>>),
    PruneContactKeyCache(Sender<Result<(), DbError>>),
    PruneOrphanedPendingFiles(Sender<Result<(), DbError>>),

    Snapshot(PathBuf, Sender<Result<(), DbError>>),
    Restore(PathBuf, RestoredFrom, Sender<Result<(), DbError>>),
    GetBackupSecret(Sender<Result<Option<StoredBackupSecret>, DbError>>),
    StoreBackupSecret(StoredBackupSecret, Sender<Result<(), DbError>>),
    StoreVerificationSchedule(VerificationSchedule, Sender<Result<(), DbError>>),

    // Opaque settings
    GetSettings(Sender<Result<SettingsBlob, DbError>>),
    StoreSettings(SettingsBlob, Sender<Result<(), DbError>>),

    GetLicenseCache(Sender<Result<Option<Vec<u8>>, DbError>>),
    StoreLicenseCache(Vec<u8>, Sender<Result<(), DbError>>),

    GetListenPort(Sender<Result<Option<ListenPort>, DbError>>),
    StoreListenPort(ListenPort, Sender<Result<(), DbError>>),

    // Multi-device CRDT sync documents (one automerge blob per DocKind)
    GetSyncDoc(DocKind, Sender<Result<Vec<u8>, DbError>>),
    SaveSyncDoc(DocKind, Vec<u8>, Sender<Result<(), DbError>>),
    /// Atomic read-modify-write, all in one actor turn so concurrent writers can't interleave a
    /// stale read with an overwrite.
    MutateDevicesDoc(
        #[allow(clippy::type_complexity)]
        Box<dyn for<'a> FnOnce(&'a mut DevicesDoc) -> DeviceChange + Send>,
        Sender<Result<DeviceChange, DbError>>,
    ),
    MergeSyncDoc(DocKind, Box<AutoCommit>, Sender<Result<(), DbError>>),
}
