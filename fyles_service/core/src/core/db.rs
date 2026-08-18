use crate::core::brain::action_p2p::NodeInfo;
use crate::core::domain_models::{ContactId, DisplayContact};
use crate::core::p2p::NodeKeyPairBinary;
use crate::core::sync::{DocKind, LocalDocWrite};
use async_trait::async_trait;
use crypto::{ContactKeys, ContactPublicKeys};
use derive_more::Display;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use super::domain_models::{
    CompleteReceivedFile, Contact, CreateFilerequest, CreateIncomingFile, CreatePendingFiles,
    CreateRemoteFilerequest, Filerequest, FylesId, PendingFile, ReceiveStatus, ReceivedFile,
    RemoteFilerequest, SelfContact, SendStatus,
};

pub type DatabaseResult<T> = Result<T, DbError>;

#[cfg(any(test, feature = "test-support"))]
pub mod test;

#[async_trait]
pub trait FilerequestDb: Send + Sync {
    // Filerequest operations
    async fn get_filerequest(&self, id: &FylesId) -> DatabaseResult<Filerequest>;
    async fn get_filerequests(&self) -> DatabaseResult<Vec<Filerequest>>;
    /// Persist a new filerequest under a caller-supplied id. The id is generated
    /// (and its directory claimed) before this call so there is nothing to roll
    /// back if persistence fails.
    async fn create_filerequest(
        &self,
        id: &FylesId,
        filerequest: &CreateFilerequest,
    ) -> DatabaseResult<()>;
    async fn update_filerequest(&self, filerequest: &Filerequest) -> DatabaseResult<()>;
    async fn delete_filerequest(&self, id: &FylesId) -> DatabaseResult<()>;

    // Contact operations
    async fn get_contact_name(&self, id: &ContactId) -> DatabaseResult<String>;
    async fn get_contact_names(
        &self,
        ids: &[ContactId],
    ) -> DatabaseResult<HashMap<ContactId, String>>;
    async fn get_contact(&self, id: &ContactId) -> DatabaseResult<Contact>;
    async fn get_contacts(&self) -> DatabaseResult<Vec<DisplayContact>>;
    async fn update_contact(&self, contact: &DisplayContact) -> DatabaseResult<LocalDocWrite>;
    async fn delete_contact(&self, id: &ContactId) -> DatabaseResult<LocalDocWrite>;
    async fn get_contact_public_keys(
        &self,
        contact_id: ContactId,
    ) -> DatabaseResult<Option<ContactPublicKeys>>;
    /// Contact ids present in the synced contacts document but with no keys cached locally.
    async fn keyless_synced_contacts(&self) -> DatabaseResult<Vec<ContactId>>;
    /// Public keys for the requested contacts, for those we hold (others omitted).
    async fn get_contacts_public_keys(
        &self,
        contact_ids: Vec<ContactId>,
    ) -> DatabaseResult<Vec<(ContactId, ContactPublicKeys)>>;
    // Node key operations
    async fn store_node_keys(&self, keys: NodeInfo) -> DatabaseResult<()>;
    async fn get_node_keys(&self) -> DatabaseResult<NodeInfo>;
    /// Reads the libp2p keypair and self contact id, without the signing keys.
    async fn get_node_and_self_contact_id(&self) -> DatabaseResult<(NodeKeyPairBinary, ContactId)>;
    /// Reads only the self contact id (cheaper than the keypair variant above).
    async fn get_self_contact_id(&self) -> DatabaseResult<ContactId>;
    /// Persists the signing keys into the existing self_contact row, leaving the
    /// contact id, name and node_keys untouched.
    async fn store_signing_keys(&self, keys: ContactKeys) -> DatabaseResult<()>;

    // Remote filerequest operations
    async fn get_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<RemoteFilerequest>;
    async fn get_remote_filerequests_by_contact(
        &self,
        contact_id: &ContactId,
    ) -> DatabaseResult<Vec<RemoteFilerequest>>;
    async fn get_all_remote_filerequests(&self) -> DatabaseResult<Vec<RemoteFilerequest>>;
    async fn create_remote_filerequest(
        &self,
        remote_fr: &CreateRemoteFilerequest,
    ) -> DatabaseResult<(FylesId, LocalDocWrite)>;
    async fn delete_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<LocalDocWrite>;
    async fn update_remote_filerequest(
        &self,
        id: FylesId,
        name: String,
    ) -> DatabaseResult<LocalDocWrite>;

    // Pending file operations
    async fn get_pending_file(&self, id: &FylesId) -> DatabaseResult<PendingFile>;
    async fn get_pending_files(
        &self,
        target_filerequest_id: &FylesId,
    ) -> DatabaseResult<Vec<PendingFile>>;
    async fn get_all_pending_files(&self) -> DatabaseResult<Vec<PendingFile>>;
    async fn create_pending_files(
        &self,
        pending_files: &CreatePendingFiles,
    ) -> DatabaseResult<Vec<FylesId>>;

    /// Returns the ID of the filerequest this file belongs to
    async fn handle_update_pending_file_status(
        &self,
        id: &FylesId,
        status: &SendStatus,
        retry_count: Option<usize>,
    ) -> DatabaseResult<FylesId>;
    async fn add_interruption_reason(&self, id: &FylesId, reason: String) -> DatabaseResult<()>;
    async fn delete_pending_file(&self, id: &FylesId) -> DatabaseResult<()>;
    async fn count_non_terminal_pending_files_for_path(
        &self,
        pending_file_id: &FylesId,
    ) -> DatabaseResult<usize>;

    // Received-file operations (covers both in-progress and completed)
    async fn create_incoming_file(&self, incoming: &CreateIncomingFile) -> DatabaseResult<FylesId>;
    async fn update_received_file_status(
        &self,
        transfer_id: &FylesId,
        status: &ReceiveStatus,
        progress_bytes: u64,
    ) -> DatabaseResult<()>;
    async fn get_received_file_by_transfer_id(
        &self,
        transfer_id: &FylesId,
    ) -> DatabaseResult<ReceivedFile>;
    async fn complete_received_file(
        &self,
        completed: &CompleteReceivedFile,
    ) -> DatabaseResult<FylesId>;
    async fn list_received_files(
        &self,
        filerequest_id: &FylesId,
    ) -> DatabaseResult<Vec<ReceivedFile>>;
    async fn delete_received_file(&self, id: &FylesId) -> DatabaseResult<()>;
    async fn get_stale_received_files(
        &self,
        older_than_ms: i64,
    ) -> DatabaseResult<Vec<ReceivedFile>>;

    // SelfContact operations
    async fn get_self_contact(&self) -> DatabaseResult<SelfContact>;
    async fn get_self_contact_for_display(&self) -> DatabaseResult<DisplayContact>;
    async fn update_self_contact_name(&self, name: String) -> DatabaseResult<()>;
    async fn update_identity(&self, self_contact: SelfContact) -> DatabaseResult<()>;
    async fn get_sharable_public_self_contact(&self) -> DatabaseResult<Contact>;
    async fn update_contact_projection(&self, contact: Contact) -> DatabaseResult<LocalDocWrite>;
    /// Delete cached key rows for contacts no longer in the synced contacts document —
    /// how a contact removed on another device is pruned locally after sync.
    async fn prune_contact_key_cache(&self) -> DatabaseResult<()>;
    /// Delete pending files whose target remote filerequest is no longer in the synced
    /// remote-filerequests document — how files queued for a remote filerequest removed on
    /// another device are cleaned up locally after sync (formerly a foreign-key cascade).
    async fn prune_orphaned_pending_files(&self) -> DatabaseResult<()>;

    async fn backup_database(
        &self,
        backup_file: std::fs::File,
        internal_data_dir: PathBuf,
    ) -> DatabaseResult<()>;
    async fn restore_database(&self, backup_file: tokio::fs::File) -> DatabaseResult<()>;

    // Opaque settings – stored as a binary blob (version is managed internally by the DB layer)
    async fn get_settings(&self) -> DatabaseResult<Vec<u8>>;
    async fn store_settings(&self, settings: Vec<u8>) -> DatabaseResult<()>;

    /// Creates a shared-genesis empty document if none is stored yet.
    async fn get_sync_doc(&self, kind: DocKind) -> DatabaseResult<Vec<u8>>;
    async fn save_sync_doc(&self, kind: DocKind, blob: Vec<u8>) -> DatabaseResult<()>;
    /// Read-modify-write in one actor turn, so concurrent writers cannot drop each other's changes.
    ///
    /// Returns no [`LocalDocWrite`] on purpose: convergence merges go through here, and being
    /// unable to mint the token is what stops a merge announcing itself back around the device set.
    async fn mutate_sync_doc(
        &self,
        kind: DocKind,
        transform: Box<dyn FnOnce(Vec<u8>) -> DatabaseResult<Vec<u8>> + Send>,
    ) -> DatabaseResult<()>;
}

#[derive(Debug, Clone)]
pub struct DbOperationInfo {
    table_name: String,
    operation_type: OperationType,
    description: String,
}

impl Display for DbOperationInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} operation on table {}: {}",
            self.operation_type, self.table_name, self.description
        )
    }
}

impl DbOperationInfo {
    pub fn new(table_name: &str, operation_type: OperationType, description: String) -> Self {
        Self {
            table_name: table_name.into(),
            operation_type,
            description,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_error(self, error: Arc<dyn std::error::Error + Sync + Send>) -> DbError {
        DbError::Operation {
            info: self,
            source: error,
        }
    }
}

#[allow(unused)]
#[derive(Debug, Clone, Display)]
pub enum OperationType {
    CreateTable,
    Create,
    Read,
    Update,
    Delete,
    Init,
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("The type of data queried was not yet initialized")]
    DataNotYetInitialized,
    #[error("The node identity exists but its signing keys were cleared and need regeneration")]
    SigningKeysMissing,
    #[error("Database operation error: {info} - {source}")]
    Operation {
        info: DbOperationInfo,
        #[source]
        source: Arc<dyn std::error::Error + Sync + Send>,
    },
    #[error("Database error: {source}")]
    Database {
        #[from]
        source: rusqlite::Error,
    },
    #[error("Database communication error")]
    Communication {
        #[source]
        source: Arc<dyn std::error::Error + Sync + Send>,
    },
    #[error("Validation error: {message}")]
    Validation { message: String },
    #[error("Data conversion error: {message}")]
    DataConversion { message: String },
    #[error("Record not found: {message}")]
    NotFound { message: String },
    /// Identity known (arrived via sync) but some out-of-band data isn't resolved yet — a
    /// contact's public keys are one example.
    #[error("Data not yet fully synced: {message}")]
    DataNotYetFullySynced { message: String },
    #[error("I/O error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
    #[error("Error: {message}")]
    Generic { message: String },
}
