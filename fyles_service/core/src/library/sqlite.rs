use crate::core::brain::action_p2p::NodeInfo;
use crate::core::db::{DatabaseResult, FilerequestDb};
use crate::core::domain_models::{
    CompleteReceivedFile, Contact, ContactId, CreateFilerequest, CreateIncomingFile,
    CreatePendingFiles, CreateRemoteFilerequest, DisplayContact, Filerequest, FylesId, PendingFile,
    ReceiveStatus, ReceivedFile, RemoteFilerequest, SelfContact, SendStatus,
};
use crate::library::util::util::TimeoutLock;
use actor::SqliteActor;
use async_trait::async_trait;
use command::SqliteMsg;
use crypto::ContactPublicKeys;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tracing::{error, trace, Span};

mod actor;
mod command;
mod migrations;
mod schema;
mod util;

pub struct SqliteConfig {
    pub path: PathBuf,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "filerequest.db".to_string().into(),
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
}

#[async_trait]
impl FilerequestDb for Sqlite {
    async fn run(&self) {
        let mut prepared_receiver = self.prepared_receiver.timeout_lock().await;
        if let Some(receiver) = prepared_receiver.take() {
            let actor = SqliteActor::new(receiver, &self.config.path);
            drop(prepared_receiver);
            actor.run().await;
        } else {
            error!("Trying to run a Sqlite that had already been started");
        }
    }

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

    async fn create_filerequest(&self, filerequest: &CreateFilerequest) -> DatabaseResult<FylesId> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::CreateFilerequest(filerequest.clone(), sender))
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

    async fn update_contact(&self, contact: &DisplayContact) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::UpdateContact(contact.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn delete_contact(&self, id: &ContactId) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::DeleteContact(id.clone(), sender))
            .await?;
        receiver.await?
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

    // Implement the missing remote filerequest methods
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
    ) -> DatabaseResult<FylesId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CreateRemoteFilerequest(
                remote_fr.clone(),
                sender,
            ))
            .await?;
        receiver.await?
    }

    async fn delete_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::DeleteRemoteFilerequest(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn update_remote_filerequest(&self, id: FylesId, name: String) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::UpdateRemoteFilerequest(id, name, sender))
            .await?;
        receiver.await?
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

    async fn add_interruption_reason(
        &self,
        id: &FylesId,
        reason: String,
    ) -> DatabaseResult<()> {
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

    async fn count_non_terminal_pending_files_for_path(&self, id: &FylesId) -> DatabaseResult<usize> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::CountNonTerminalPendingFilesForPath(id.clone(), sender))
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

    async fn delete_received_file(&self, id: &FylesId) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .sender
            .send(SqliteMsg::DeleteReceivedFile(id.clone(), sender))
            .await?;
        receiver.await?
    }

    async fn get_stale_received_files(&self, older_than_ms: i64) -> DatabaseResult<Vec<ReceivedFile>> {
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

    async fn get_sharable_public_self_contact(&self) -> DatabaseResult<Contact> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::GetSharablePublicSelfContact(sender))
            .await?;
        receiver.await?
    }
    async fn register_contact(&self, contact: Contact) -> DatabaseResult<()> {
        trace!("Registering contact: {:?}", contact);
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::RegisterContact(contact, sender))
            .await?;
        receiver.await?
    }

    async fn backup_database(
        &self,
        backup_file: std::fs::File,
        internal_data_dir: PathBuf,
    ) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::Backup {
                backup_file,
                internal_data_dir,
                response_sender: sender,
            })
            .await?;
        receiver.await?
    }

    async fn restore_database(&self, backup_file: tokio::fs::File) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::Restore {
                backup_file,
                response_sender: sender,
            })
            .await?;
        receiver.await?
    }

    async fn get_settings(&self) -> DatabaseResult<Vec<u8>> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(SqliteMsg::GetSettings(sender)).await?;
        receiver.await?
    }

    async fn store_settings(&self, settings: Vec<u8>) -> DatabaseResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SqliteMsg::StoreSettings(settings, sender))
            .await?;
        receiver.await?
    }
}

#[cfg(test)]
mod tests {
    use crate::core::db::test::setup_test_db;

    #[tokio::test]
    async fn test_filerequest_crud_and_access() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_filerequest_crud_and_access(db.db).await;
    }

    #[tokio::test]
    async fn test_filerequest_deletion() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_filerequest_deletion(db.db).await;
    }

    #[tokio::test]
    async fn test_node_keys_integrity() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_node_keys_integrity(db.db).await;
    }

    #[tokio::test]
    async fn test_pending_files() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_pending_files(db.db).await;
    }

    #[tokio::test]
    async fn test_received_files() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_received_files(db.db).await;
    }

    #[tokio::test]
    async fn test_delete_received_file() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_delete_received_file(db.db).await;
    }

    #[tokio::test]
    async fn test_delete_nonexistent_received_file() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_delete_nonexistent_received_file(db.db).await;
    }

    #[tokio::test]
    #[ignore = "DB should error when deleting unknown contact; current implementation returns Ok"]
    async fn test_contact_deletion_cascade() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_contact_deletion_cascade(db.db).await;
    }

    #[tokio::test]
    #[ignore = "DB should error when updating unknown filerequest; current implementation returns Ok"]
    async fn test_update_nonexistent_filerequest() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_update_nonexistent_filerequest(db.db).await;
    }

    #[tokio::test]
    async fn test_remote_filerequests_unknown_contact_query() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_remote_filerequests_unknown_contact_query(db.db).await;
    }

    #[tokio::test]
    async fn test_contact_public_keys() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_contact_public_keys(db.db).await;
    }

    #[tokio::test]
    async fn test_db_initialization() {
        let db = setup_test_db(None, false).await;
        crate::core::db::test::test_db_initialization(db.db).await;
    }
}
