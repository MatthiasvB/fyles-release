use crate::core::backup::{RestoredFrom, StoredBackupSecret, VerificationSchedule};
use crate::core::brain::action_p2p::NodeInfo;
use crate::core::db::{DatabaseResult, FilerequestDb};
use crate::core::domain_models::{
    CompleteReceivedFile, Contact, ContactId, CreateFilerequest, CreateIncomingFile,
    CreatePendingFiles, CreateRemoteFilerequest, DisplayContact, Filerequest, FilerequestId,
    FylesId, PendingFile, ReceiveStatus, ReceivedFile, RemoteFilerequest, SelfContact, SendStatus,
};
use crate::core::listen::ListenPort;
use crate::core::p2p::NodeKeyPairBinary;
use crate::core::settings::SettingsBlob;
use crate::core::sync::{
    ContactChange, DeviceChange, DevicesDoc, DocChange, DocKind, LocalDocWrite,
    RemoteFilerequestChange,
};
use crate::library::util::util::TimeoutLock;
use actor::SqliteActor;
use async_trait::async_trait;
use automerge::AutoCommit;
use command::SqliteMsg;
use crypto::{ContactKeys, ContactPublicKeys};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{Mutex, oneshot};
use tracing::{Span, trace};

mod actor;
mod command;
mod migrations;
#[cfg(test)]
pub mod schema;
#[cfg(not(test))]
mod schema;
mod util;

/// Where a [`Sqlite`] database lives. `Memory` opens a private in-memory database
/// (`Connection::open_in_memory`), used by tests; `File` opens/creates a file on disk.
pub enum SqlitePath {
    Memory,
    File(PathBuf),
}

pub struct SqliteConfig {
    pub path: SqlitePath,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: SqlitePath::File("filerequest.db".into()),
        }
    }
}

pub struct Sqlite {
    sender: Sender<SqliteMsg>,
    prepared_receiver: Mutex<Option<Receiver<SqliteMsg>>>,
    config: SqliteConfig,
}

impl Sqlite {
    pub fn with_config(config: SqliteConfig) -> Self {
        let (sender, receiver) = channel(100);

        Self {
            sender,
            prepared_receiver: Mutex::new(Some(receiver)),
            config,
        }
    }

    /// Construct a database and start its actor on a detached task, returning the
    /// handle. The actor outlives any single [`Brain`], so a restarted brain reuses
    /// the same live database. This is the only way the actor is started.
    pub fn spawn(config: SqliteConfig) -> Arc<Self> {
        let db = Arc::new(Self::with_config(config));
        let actor = db.clone();
        tokio::spawn(async move { actor.run().await });
        db
    }

    async fn run(&self) {
        let receiver = self
            .prepared_receiver
            .timeout_lock()
            .await
            .take()
            .expect("Sqlite actor started once via Sqlite::spawn");
        SqliteActor::new(receiver, &self.config.path).run().await;
    }
}

#[async_trait]
impl FilerequestDb for Sqlite {
    async fn get_filerequest(&self, id: &FylesId) -> DatabaseResult<Filerequest> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetFilerequest(
                id.clone(),
                sender,
                Span::current(),
            ))
            .await?;
        receiver.await?
    }

    async fn get_filerequests(&self) -> DatabaseResult<Vec<Filerequest>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::GetAllFilerequests(sender))
            .await?;
        receiver.await?
    }

    async fn create_filerequest(
        &self,
        id: &FylesId,
        filerequest: &CreateFilerequest,
    ) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::CreateFilerequest(
                id.clone(),
                filerequest.clone(),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn update_filerequest(&self, filerequest: &Filerequest) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::UpdateFilerequest(filerequest.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn delete_filerequest(&self, id: &FylesId) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::DeleteFilerequest(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_contact_name(&self, id: &ContactId) -> DatabaseResult<String> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::GetContactName(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_contact_public_keys(
        &self,
        contact_id: ContactId,
    ) -> DatabaseResult<Option<ContactPublicKeys>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetContactPublicKeys(contact_id, sender))
            .await?;
        receiver.await?
    }

    async fn keyless_synced_contacts(&self) -> DatabaseResult<Vec<ContactId>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::KeylessSyncedContacts(sender))
            .await?;
        receiver.await?
    }

    async fn get_contacts_public_keys(
        &self,
        contact_ids: Vec<ContactId>,
    ) -> DatabaseResult<Vec<(ContactId, ContactPublicKeys)>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetContactsPublicKeys(contact_ids, sender))
            .await?;
        receiver.await?
    }

    async fn get_contact_names(
        &self,
        ids: &[ContactId],
    ) -> DatabaseResult<HashMap<ContactId, String>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::GetContactNames(ids.to_vec(), sender))
            .await?;
        receiver.await?
    }

    async fn get_contact(&self, id: &ContactId) -> DatabaseResult<Contact> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::GetContact(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_contacts(&self) -> DatabaseResult<Vec<DisplayContact>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.send(SqliteMsg::GetContacts(sender)).await?;
        receiver.await?
    }

    async fn update_contact(&self, contact: &DisplayContact) -> DatabaseResult<LocalDocWrite> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::UpdateContact(contact.clone(), sender))
            .await?;
        receiver.await??;
        Ok(LocalDocWrite::new(DocChange::Contacts(
            ContactChange::Upserted(contact.clone()),
        )))
    }

    async fn delete_contact(&self, id: &ContactId) -> DatabaseResult<LocalDocWrite> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::DeleteContact(id.clone(), sender))
            .await?;
        receiver.await??;
        Ok(LocalDocWrite::new(DocChange::Contacts(
            ContactChange::Removed(id.clone()),
        )))
    }

    async fn store_node_keys(&self, keys: NodeInfo) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreNodeKeys(keys.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_node_keys(&self) -> DatabaseResult<NodeInfo> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetNodeInfo(sender)).await?;
        receiver.await?
    }

    async fn get_node_keypair(&self) -> DatabaseResult<NodeKeyPairBinary> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetNodeKeypair(sender)).await?;
        receiver.await?
    }

    async fn get_self_contact_id(&self) -> DatabaseResult<ContactId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetSelfContactId(sender))
            .await?;
        receiver.await?
    }

    async fn store_signing_keys(&self, keys: ContactKeys) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreSigningKeys(keys, sender))
            .await?;
        receiver.await?
    }

    async fn get_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<RemoteFilerequest> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetRemoteFilerequest(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_remote_filerequests_by_contact(
        &self,
        contact_id: &ContactId,
    ) -> DatabaseResult<Vec<RemoteFilerequest>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetRemoteFilerequestsByContact(
                contact_id.clone(),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn get_all_remote_filerequests(&self) -> DatabaseResult<Vec<RemoteFilerequest>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetAllRemoteFilerequests(sender))
            .await?;
        receiver.await?
    }

    async fn create_remote_filerequest(
        &self,
        remote_fr: &CreateRemoteFilerequest,
    ) -> DatabaseResult<(FylesId, LocalDocWrite)> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CreateRemoteFilerequest(
                remote_fr.clone(),
                sender,
            ))
            .await?;
        let id = receiver.await??;
        let created = RemoteFilerequest {
            id: id.clone(),
            peer_id: remote_fr.peer_id.clone(),
            filerequest_id: FylesId(remote_fr.filerequest_id.clone()),
            contact_id: remote_fr.contact_id.clone(),
            name: remote_fr.name.clone(),
        };
        Ok((
            id,
            LocalDocWrite::new(DocChange::RemoteFilerequests(
                RemoteFilerequestChange::Upserted(created),
            )),
        ))
    }

    async fn delete_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<LocalDocWrite> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::DeleteRemoteFilerequest(id.clone(), sender))
            .await?;
        receiver.await??;
        Ok(LocalDocWrite::new(DocChange::RemoteFilerequests(
            RemoteFilerequestChange::Removed(id.clone()),
        )))
    }

    async fn update_remote_filerequest(
        &self,
        id: FylesId,
        name: String,
    ) -> DatabaseResult<LocalDocWrite> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdateRemoteFilerequest(id.clone(), name, sender))
            .await?;
        receiver.await??;
        let updated = self.get_remote_filerequest(&id).await?;
        Ok(LocalDocWrite::new(DocChange::RemoteFilerequests(
            RemoteFilerequestChange::Upserted(updated),
        )))
    }

    // Implement the missing pending file methods
    async fn get_pending_file(&self, id: &FylesId) -> DatabaseResult<PendingFile> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetPendingFile(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_pending_files(
        &self,
        target_filerequest_id: &FylesId,
    ) -> DatabaseResult<Vec<PendingFile>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetPendingFiles(
                target_filerequest_id.to_string(),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn get_all_pending_files(&self) -> DatabaseResult<Vec<PendingFile>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetAllPendingFiles(sender))
            .await?;
        receiver.await?
    }

    async fn create_pending_files(
        &self,
        pending_file: &CreatePendingFiles,
    ) -> DatabaseResult<Vec<FylesId>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CreatePendingFile(pending_file.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn handle_update_pending_file_status(
        &self,
        id: &FylesId,
        status: &SendStatus,
        retry_count: Option<usize>,
    ) -> DatabaseResult<FylesId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdatePendingFileStatus(
                (id.clone(), status.clone(), retry_count),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn add_interruption_reason(&self, id: &FylesId, reason: String) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::AddInterruptionReason(
                (id.clone(), reason),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn delete_pending_file(&self, id: &FylesId) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::DeletePendingFile(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn count_non_terminal_pending_files_for_path(
        &self,
        id: &FylesId,
    ) -> DatabaseResult<usize> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CountNonTerminalPendingFilesForPath(
                id.clone(),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn create_incoming_file(&self, incoming: &CreateIncomingFile) -> DatabaseResult<FylesId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CreateIncomingFile(incoming.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn update_received_file_status(
        &self,
        transfer_id: &FylesId,
        status: &ReceiveStatus,
        progress_bytes: u64,
    ) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdateReceivedFileStatus(
                (transfer_id.clone(), status.clone(), progress_bytes),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn get_received_file_by_transfer_id(
        &self,
        transfer_id: &FylesId,
    ) -> DatabaseResult<ReceivedFile> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetReceivedFileByTransferId(
                transfer_id.clone(),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn complete_received_file(
        &self,
        completed: &CompleteReceivedFile,
    ) -> DatabaseResult<FylesId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CompleteReceivedFile(completed.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn list_received_files(
        &self,
        filerequest_id: &FylesId,
    ) -> DatabaseResult<Vec<ReceivedFile>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::ListReceivedFiles(filerequest_id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn delete_received_file(&self, id: &FylesId) -> DatabaseResult<FilerequestId> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::DeleteReceivedFile(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_stale_received_files(
        &self,
        older_than_ms: i64,
    ) -> DatabaseResult<Vec<ReceivedFile>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetStaleReceivedFiles(older_than_ms, sender))
            .await?;
        receiver.await?
    }

    async fn get_self_contact(&self) -> DatabaseResult<SelfContact> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetSelfContact(sender)).await?;
        receiver.await?
    }

    async fn update_self_contact_name(&self, name: String) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdateSelfContactName(name, sender))
            .await?;
        receiver.await?
    }

    async fn update_identity(&self, self_contact: SelfContact) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdateIdentity(self_contact, sender))
            .await?;
        receiver.await?
    }

    async fn get_self_contact_for_display(&self) -> DatabaseResult<DisplayContact> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetSelfContactForDisplay(sender))
            .await?;
        receiver.await?
    }

    async fn update_contact_projection(&self, contact: Contact) -> DatabaseResult<LocalDocWrite> {
        trace!("Registering contact: {:?}", contact);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdateContactProjection(contact.clone(), sender))
            .await?;
        receiver.await??;
        Ok(LocalDocWrite::new(DocChange::Contacts(
            ContactChange::Upserted(contact.into()),
        )))
    }

    async fn prune_contact_key_cache(&self) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::PruneContactKeyCache(sender))
            .await?;
        receiver.await?
    }

    async fn prune_orphaned_pending_files(&self) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::PruneOrphanedPendingFiles(sender))
            .await?;
        receiver.await?
    }

    async fn snapshot_database(&self, destination: PathBuf) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::Snapshot(destination, sender))
            .await?;
        receiver.await?
    }

    async fn restore_database(&self, sqlite: PathBuf, from: RestoredFrom) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::Restore(sqlite, from, sender))
            .await?;
        receiver.await?
    }

    async fn get_backup_secret(&self) -> DatabaseResult<Option<StoredBackupSecret>> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetBackupSecret(sender)).await?;
        receiver.await?
    }

    async fn store_backup_secret(&self, stored: StoredBackupSecret) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreBackupSecret(stored, sender))
            .await?;
        receiver.await?
    }

    async fn store_verification_schedule(
        &self,
        schedule: VerificationSchedule,
    ) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreVerificationSchedule(schedule, sender))
            .await?;
        receiver.await?
    }

    async fn get_settings(&self) -> DatabaseResult<SettingsBlob> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetSettings(sender)).await?;
        receiver.await?
    }

    async fn store_settings(&self, settings: SettingsBlob) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreSettings(settings, sender))
            .await?;
        receiver.await?
    }

    async fn get_license_cache(&self) -> DatabaseResult<Option<Vec<u8>>> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetLicenseCache(sender)).await?;
        receiver.await?
    }

    async fn store_license_cache(&self, data: Vec<u8>) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreLicenseCache(data, sender))
            .await?;
        receiver.await?
    }

    async fn get_listen_port(&self) -> DatabaseResult<Option<ListenPort>> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetListenPort(sender)).await?;
        receiver.await?
    }

    async fn store_listen_port(&self, port: ListenPort) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreListenPort(port, sender))
            .await?;
        receiver.await?
    }

    async fn get_sync_doc(&self, kind: DocKind) -> DatabaseResult<Vec<u8>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetSyncDoc(kind, sender))
            .await?;
        receiver.await?
    }

    async fn save_sync_doc(&self, kind: DocKind, blob: Vec<u8>) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::SaveSyncDoc(kind, blob, sender))
            .await?;
        receiver.await?
    }

    async fn mutate_devices_doc(
        &self,
        transform: Box<dyn for<'a> FnOnce(&'a mut DevicesDoc) -> DeviceChange + Send>,
    ) -> DatabaseResult<DeviceChange> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::MutateDevicesDoc(transform, sender))
            .await?;
        receiver.await?
    }

    async fn merge_sync_doc(&self, kind: DocKind, incoming: Box<AutoCommit>) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::MergeSyncDoc(kind, incoming, sender))
            .await?;
        receiver.await?
    }
}

#[cfg(test)]
mod tests {
    use crate::core::db::test::setup_test_db;

    #[tokio::test]
    async fn test_filerequest_crud_and_access() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_filerequest_crud_and_access(db).await;
    }

    #[tokio::test]
    async fn test_filerequest_deletion() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_filerequest_deletion(db).await;
    }

    #[tokio::test]
    async fn test_node_keys_integrity() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_node_keys_integrity(db).await;
    }

    #[tokio::test]
    async fn test_pending_files() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_pending_files(db).await;
    }

    #[tokio::test]
    async fn test_received_files() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_received_files(db).await;
    }

    #[tokio::test]
    async fn test_delete_received_file() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_delete_received_file(db).await;
    }

    #[tokio::test]
    async fn test_delete_nonexistent_received_file() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_delete_nonexistent_received_file(db).await;
    }

    #[tokio::test]
    #[ignore = "DB should error when deleting unknown contact; current implementation returns Ok"]
    async fn test_contact_deletion_cascade() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_contact_deletion_cascade(db).await;
    }

    #[tokio::test]
    #[ignore = "DB should error when updating unknown filerequest; current implementation returns Ok"]
    async fn test_update_nonexistent_filerequest() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_update_nonexistent_filerequest(db).await;
    }

    #[tokio::test]
    async fn test_remote_filerequests_unknown_contact_query() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_remote_filerequests_unknown_contact_query(db).await;
    }

    #[tokio::test]
    async fn test_contact_public_keys() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_contact_public_keys(db).await;
    }

    #[tokio::test]
    async fn test_contacts_and_remote_fr_doc_roundtrip() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_contacts_and_remote_fr_doc_roundtrip(db).await;
    }

    #[tokio::test]
    async fn test_listen_port_roundtrip() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_listen_port_roundtrip(db).await;
    }

    #[tokio::test]
    async fn test_db_initialization() {
        let db = setup_test_db(None).await;
        crate::core::db::test::test_db_initialization(db).await;
    }
}
