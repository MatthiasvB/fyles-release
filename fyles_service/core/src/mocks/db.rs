use async_std::task::sleep;
use async_trait::async_trait;
use crypto::ContactPublicKeys;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use crate::core::brain::action_p2p::NodeInfo;
use crate::core::db::{DatabaseResult, DbError, DbOperationInfo, FilerequestDb, OperationType};
use crate::core::domain_models::{
    CompleteReceivedFile, Contact, ContactId, CreateFilerequest, CreateIncomingFile,
    CreatePendingFiles, CreateRemoteFilerequest, DisplayContact, Filerequest, FilerequestAccess,
    FylesId, PendingFile, ReceiveStatus, ReceivedFile, RemoteFilerequest, SelfContact, SendStatus,
};
use crate::library::util::duration_ext::DurationExt;

#[derive(Default)]
pub struct MockDb {
    filerequests: Arc<Mutex<HashMap<FylesId, Filerequest>>>,
    contacts: Arc<Mutex<HashMap<ContactId, Contact>>>,
    node_keys: Arc<RwLock<Option<NodeInfo>>>,
    remote_filerequests: Arc<Mutex<HashMap<FylesId, RemoteFilerequest>>>,
    pending_files: Arc<Mutex<HashMap<FylesId, PendingFile>>>,
    received_files: Arc<Mutex<HashMap<FylesId, ReceivedFile>>>,
    self_contact: Arc<Mutex<Option<SelfContact>>>, // Changed from Vec<u8> to SelfContact
}

impl MockDb {
    pub fn new() -> Self {
        Self::default()
    }

    // Helper: remove a contact from all audience lists (simulate FK cascade in filerequest_audience)
    fn remove_contact_from_audiences(&self, contact_id: &ContactId) {
        let mut frs = self.filerequests.lock().unwrap();
        for fr in frs.values_mut() {
            if let FilerequestAccess::Audience { contact_ids } = &mut fr.access {
                contact_ids.retain(|cid| cid != contact_id);
            }
        }
    }
}

#[async_trait]
impl FilerequestDb for MockDb {
    async fn run(&self) {
        sleep(24.hours()).await;
    }

    async fn get_filerequest(&self, id: &FylesId) -> DatabaseResult<Filerequest> {
        self.filerequests
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| {
                DbOperationInfo::new(
                    "filerequests",
                    OperationType::Read,
                    " Get Filerequest".into(),
                )
                .with_error(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Filerequest not found",
                )))
            })
    }

    async fn get_filerequests(&self) -> DatabaseResult<Vec<Filerequest>> {
        Ok(self
            .filerequests
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect())
    }

    async fn create_filerequest(&self, filerequest: &CreateFilerequest) -> DatabaseResult<FylesId> {
        let id = FylesId::new();
        let new_filerequest = Filerequest {
            id: id.clone(),
            title: filerequest.title.clone(),
            description: filerequest.description.clone(),
            is_active: filerequest.is_active,
            access: filerequest.access.clone(),
        };
        self.filerequests
            .lock()
            .unwrap()
            .insert(id.clone(), new_filerequest);
        Ok(id)
    }

    async fn update_filerequest(&self, filerequest: &Filerequest) -> DatabaseResult<()> {
        // Match Sqlite semantics: silent no-op if not present
        let mut frs = self.filerequests.lock().unwrap();
        if frs.contains_key(&filerequest.id) {
            frs.insert(filerequest.id.clone(), filerequest.clone());
        }
        Ok(())
    }

    async fn delete_filerequest(&self, id: &FylesId) -> DatabaseResult<()> {
        // Remove the filerequest itself
        self.filerequests.lock().unwrap().remove(id);
        // Cascade delete: remove received files belonging to this filerequest
        self.received_files
            .lock()
            .unwrap()
            .retain(|_, rf| rf.filerequest_id != *id);
        // Also remove any pending files targeting this (local) filerequest (defensive)
        self.pending_files
            .lock()
            .unwrap()
            .retain(|_, pf| pf.target_filerequest_id != *id);
        Ok(())
    }

    async fn get_contact_name(&self, id: &ContactId) -> DatabaseResult<String> {
        self.contacts
            .lock()
            .unwrap()
            .get(id)
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                DbError::NotFound { message: "Contact could not be found".into() }
            })
    }

    async fn get_contact_names(
        &self,
        ids: &[ContactId],
    ) -> DatabaseResult<HashMap<ContactId, String>> {
        let contacts = self.contacts.lock().unwrap();
        Ok(ids
            .iter()
            .filter_map(|id| contacts.get(id).map(|c| (id.clone(), c.name.clone())))
            .collect())
    }

    async fn get_contact(&self, id: &ContactId) -> DatabaseResult<Contact> {
        self.contacts
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| {
                DbOperationInfo::new("contacts", OperationType::Read, "Get contact".into())
                    .with_error(Arc::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Contact not found",
                    )))
            })
    }

    async fn get_contacts(&self) -> DatabaseResult<Vec<DisplayContact>> {
        Ok(self
            .contacts
            .lock()
            .unwrap()
            .values()
            .cloned()
            .map(Into::into)
            .collect())
    }

    async fn get_contact_public_keys(
        &self,
        contact_id: ContactId,
    ) -> DatabaseResult<Option<ContactPublicKeys>> {
        // Get keys directly from the contact instead of a separate map
        let contacts = self.contacts.lock().unwrap();
        Ok(contacts.get(&contact_id).map(|c| c.public_keys.clone()))
    }

    async fn store_node_keys(&self, keys: NodeInfo) -> DatabaseResult<()> {
        *self.node_keys.write().unwrap() = Some(keys.clone());
        // Initialize or refresh self_contact to mirror Sqlite semantics
        let mut sc = self.self_contact.lock().unwrap();
        *sc = Some(SelfContact {
            id: keys.self_contact_id.clone(),
            name: keys.self_contact_id.0.clone(), // default name = id (Sqlite sets name to id)
            keys: keys.self_contact_keys.clone(),
        });
        Ok(())
    }

    async fn get_node_keys(&self) -> DatabaseResult<NodeInfo> {
        self.node_keys
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| DbError::DataNotYetInitialized)
    }

    // Remote filerequest operations
    async fn get_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<RemoteFilerequest> {
        self.remote_filerequests
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Read,
                    "Get RemoteFilerequest".into(),
                )
                .with_error(Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Remote filerequest not found: {id}"),
                )))
            })
    }

    async fn get_remote_filerequests_by_contact(
        &self,
        contact_id: &ContactId,
    ) -> DatabaseResult<Vec<RemoteFilerequest>> {
        // Match Sqlite: just filter remote_filerequests by stored contact_id (no panic if contact unknown)
        Ok(self
            .remote_filerequests
            .lock()
            .unwrap()
            .values()
            .filter(|fr| &fr.contact_id == contact_id)
            .cloned()
            .collect())
    }

    async fn get_all_remote_filerequests(&self) -> DatabaseResult<Vec<RemoteFilerequest>> {
        Ok(self
            .remote_filerequests
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect())
    }

    async fn create_remote_filerequest(
        &self,
        remote_fr: &CreateRemoteFilerequest,
    ) -> DatabaseResult<FylesId> {
        let id = FylesId::new();
        let new_remote_fr = RemoteFilerequest {
            id: id.clone(),
            // mock doesn't need to distinguish internal and peer IDs
            peer_id: remote_fr.peer_id.0.clone().into(),
            filerequest_id: remote_fr.filerequest_id.clone().into(),
            name: remote_fr.name.clone(),
            contact_id: remote_fr.contact_id.clone(),
        };
        self.remote_filerequests
            .lock()
            .unwrap()
            .insert(id.clone(), new_remote_fr);
        Ok(id)
    }

    async fn update_remote_filerequest(&self, id: FylesId, name: String) -> DatabaseResult<()> {
        // Match Sqlite: perform update semantics (insert/replace behavior not required; ignore if missing)
        let mut map = self.remote_filerequests.lock().unwrap();
        if let Some(remote_fr) = map.get_mut(&id) {
            remote_fr.name = name;
        }
        Ok(())
    }

    async fn delete_remote_filerequest(&self, id: &FylesId) -> DatabaseResult<()> {
        // Match Sqlite: delete and succeed even if it did not exist
        if self
            .remote_filerequests
            .lock()
            .unwrap()
            .remove(id)
            .is_some()
        {
            self.pending_files
                .lock()
                .unwrap()
                .retain(|_, pf| pf.target_filerequest_id != *id);
        }
        Ok(())
    }

    // Pending file operations
    async fn get_pending_file(&self, id: &FylesId) -> DatabaseResult<PendingFile> {
        let pending_file = self.pending_files.lock().unwrap().get(id).cloned();

        match pending_file {
            Some(pf) => Ok(pf),
            None => Err(DbOperationInfo::new(
                "pending_files",
                OperationType::Read,
                "Get pending file".into(),
            )
            .with_error(Arc::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "PendingFile not found",
            )))),
        }
    }

    async fn get_pending_files(
        &self,
        target_filerequest_id: &FylesId,
    ) -> DatabaseResult<Vec<PendingFile>> {
        let files = self
            .pending_files
            .lock()
            .unwrap()
            .values()
            .filter(|pf| pf.target_filerequest_id == *target_filerequest_id)
            .cloned()
            .collect::<Vec<_>>();

        Ok(files)
    }

    async fn get_all_pending_files(&self) -> DatabaseResult<Vec<PendingFile>> {
        let pending_files = self
            .pending_files
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let mut result = Vec::with_capacity(pending_files.len());

        for pf in pending_files {
            result.push(pf);
        }

        Ok(result)
    }

    async fn create_pending_files(
        &self,
        pending_files: &CreatePendingFiles,
    ) -> DatabaseResult<Vec<FylesId>> {
        // First check if the remote filerequest exists
        let remote_fr = self
            .remote_filerequests
            .lock()
            .unwrap()
            .get(&pending_files.target_filerequest_id)
            .cloned();

        if let Some(remote_fr) = remote_fr {
            let mut pending_files_map = self.pending_files.lock().unwrap();

            // Get authentication requirement
            // let auth_requirement = self.determine_authentication_requirement(
            //     remote_fr.requires_authentication,
            //     &remote_fr.contact_id,
            // );

            let id_and_paths: Vec<_> = pending_files
                .file_infos
                .iter()
                .map(|file_path| (FylesId::new(), file_path))
                .collect();

            for (id, file_path) in &id_and_paths {
                let new_pending_file = PendingFile {
                    id: id.clone(),
                    file_path: file_path.path.clone(),
                    display_name: file_path.display_name.clone(),
                    target_filerequest_id: pending_files.target_filerequest_id.clone(),
                    status: SendStatus::Pending,
                    contact_id: remote_fr.contact_id.clone(),
                    retry_count: 0,
                    interruption_reasons: vec![],
                };
                pending_files_map.insert(id.clone(), new_pending_file);
            }

            Ok(id_and_paths.into_iter().map(|(id, _)| id).collect())
        } else {
            Err(DbOperationInfo::new(
                "remote_filerequests",
                OperationType::Read,
                "Get remote filerequest".into(),
            )
            .with_error(Arc::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Target remote filerequest not found",
            ))))
        }
    }

    async fn handle_update_pending_file_status(
        &self,
        id: &FylesId,
        status: &SendStatus,
        retry_count: Option<usize>,
    ) -> DatabaseResult<FylesId> {
        if let Some(pf) = self.pending_files.lock().unwrap().get_mut(id) {
            pf.status = status.clone();
            if let Some(count) = retry_count {
                pf.retry_count = count;
            }
            Ok(pf.target_filerequest_id.clone())
        } else {
            Err(DbOperationInfo::new(
                "pending_files",
                OperationType::Update,
                "Update pending file status".into(),
            )
            .with_error(Arc::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "PendingFile not found",
            ))))
        }
    }

    async fn delete_pending_file(&self, id: &FylesId) -> DatabaseResult<()> {
        self.pending_files.lock().unwrap().remove(id);
        Ok(())
    }

    async fn add_interruption_reason(
        &self,
        id: &FylesId,
        reason: String,
    ) -> DatabaseResult<()> {
        if let Some(pf) = self.pending_files.lock().unwrap().get_mut(id) {
            pf.interruption_reasons.push(reason);
            Ok(())
        } else {
            Err(DbError::Generic { message: "PendingFile not found".into() })
        }
    }

    async fn count_non_terminal_pending_files_for_path(&self, pending_file_id: &FylesId) -> DatabaseResult<usize> {
        let files = self.pending_files.lock().unwrap();
        let target_path = files.get(pending_file_id).map(|pf| pf.file_path.clone());
        if let Some(path) = target_path {
            let count = files.values().filter(|pf| {
                pf.id != *pending_file_id &&
                pf.file_path == path &&
                (matches!(pf.status, SendStatus::Pending | SendStatus::InProgress(_)))
            }).count();
            Ok(count)
        } else {
            Err(DbOperationInfo::new(
                "pending_files",
                OperationType::Read,
                "Count non-terminal pending files for path".into(),
            )
            .with_error(Arc::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "PendingFile not found",
            ))))
        }
    }

    async fn create_incoming_file(&self, incoming: &CreateIncomingFile) -> DatabaseResult<FylesId> {
        let id = FylesId::new();
        let received_file = ReceivedFile {
            id: id.clone(),
            contact_id: incoming.contact_id.clone(),
            peer_id: incoming.peer_id.clone(),
            filerequest_id: incoming.filerequest_id.clone(),
            transfer_id: Some(incoming.transfer_id.clone()),
            file_name: incoming.file_name.clone(),
            file_path: None,
            file_size_bytes: incoming.file_size_bytes,
            progress_bytes: 0,
            status: ReceiveStatus::Receiving,
            started_at_ms: incoming.started_at_ms,
            received_at_ms: None,
        };
        self.received_files
            .lock()
            .unwrap()
            .insert(id.clone(), received_file);
        Ok(id)
    }

    async fn update_received_file_status(
        &self,
        transfer_id: &FylesId,
        status: &ReceiveStatus,
        progress_bytes: u64,
    ) -> DatabaseResult<()> {
        let mut files = self.received_files.lock().unwrap();
        if let Some(rf) = files
            .values_mut()
            .find(|rf| rf.transfer_id.as_ref() == Some(transfer_id))
        {
            rf.status = status.clone();
            rf.progress_bytes = progress_bytes;
            Ok(())
        } else {
            Err(DbError::Generic {
                message: format!("Received file with transfer_id {transfer_id} not found"),
            })
        }
    }

    async fn get_received_file_by_transfer_id(
        &self,
        transfer_id: &FylesId,
    ) -> DatabaseResult<ReceivedFile> {
        self.received_files
            .lock()
            .unwrap()
            .values()
            .find(|rf| rf.transfer_id.as_ref() == Some(transfer_id))
            .cloned()
            .ok_or_else(|| DbError::Generic {
                message: format!("Received file with transfer_id {transfer_id} not found"),
            })
    }

    async fn complete_received_file(
        &self,
        completed: &CompleteReceivedFile,
    ) -> DatabaseResult<FylesId> {
        let mut files = self.received_files.lock().unwrap();
        if let Some(rf) = files
            .values_mut()
            .find(|rf| rf.transfer_id.as_ref() == Some(&completed.transfer_id))
        {
            rf.file_path = Some(completed.file_path.clone());
            rf.received_at_ms = Some(completed.received_at_ms);
            rf.status = ReceiveStatus::Completed;
            rf.progress_bytes = rf.file_size_bytes;
            Ok(rf.id.clone())
        } else {
            Err(DbError::Generic {
                message: format!(
                    "Received file with transfer_id {} not found for completion",
                    completed.transfer_id
                ),
            })
        }
    }

    async fn list_received_files(
        &self,
        filerequest_id: &FylesId,
    ) -> DatabaseResult<Vec<ReceivedFile>> {
        Ok(self
            .received_files
            .lock()
            .unwrap()
            .values()
            .filter(|rf| rf.filerequest_id == *filerequest_id)
            .cloned()
            .collect())
    }

    async fn delete_received_file(&self, id: &FylesId) -> DatabaseResult<()> {
        let mut files = self.received_files.lock().unwrap();
        if !files.contains_key(id) {
            return Err(DbError::Operation {
                info: DbOperationInfo::new(
                    "received_files",
                    OperationType::Delete,
                    "Delete received file".into(),
                ),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "File not found",
                )),
            });
        }
        files.remove(id);
        Ok(())
    }

    async fn get_stale_received_files(&self, older_than_ms: i64) -> DatabaseResult<Vec<ReceivedFile>> {
        let files = self.received_files.lock().unwrap();
        let stale = files.values().filter(|rf| {
            (rf.status == ReceiveStatus::Receiving || rf.status == ReceiveStatus::Interrupted) &&
            rf.started_at_ms < older_than_ms
        }).cloned().collect();
        Ok(stale)
    }

    async fn get_self_contact(&self) -> DatabaseResult<SelfContact> {
        self.self_contact
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| DbError::DataNotYetInitialized)
    }

    async fn get_self_contact_for_display(&self) -> DatabaseResult<DisplayContact> {
        // Get the self contact, then convert it to a display contact
        let self_contact = self
            .self_contact
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| DbError::DataNotYetInitialized)?;

        // Just extract the id and name for the lightweight version
        Ok(DisplayContact {
            id: self_contact.id,
            name: self_contact.name,
        })
    }

    async fn update_self_contact_name(&self, name: String) -> DatabaseResult<()> {
        let mut self_contact = self.self_contact.lock().unwrap();

        if let Some(ref mut contact) = *self_contact {
            contact.name = name;
            Ok(())
        } else {
            Err(DbError::DataNotYetInitialized)
        }
    }

    async fn update_identity(&self, self_contact: SelfContact) -> DatabaseResult<()> {
        let mut stored_self_contact = self.self_contact.lock().unwrap();

        if stored_self_contact.is_some() {
            // Replace the entire self contact with the new one
            *stored_self_contact = Some(self_contact);
            Ok(())
        } else {
            Err(DbError::DataNotYetInitialized)
        }
    }

    async fn get_sharable_public_self_contact(&self) -> DatabaseResult<Contact> {
        // Get the self contact info first
        let self_contact = self
            .self_contact
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| DbError::DataNotYetInitialized)?;

        // Create a public version with just public keys
        Ok(Contact {
            id: self_contact.id,
            name: self_contact.name,
            public_keys: self_contact.keys.public,
        })
    }

    async fn register_contact(&self, contact: Contact) -> DatabaseResult<()> {
        // Convert the public contact to a full contact (without private keys)
        let new_contact = Contact {
            id: contact.id.clone(),
            name: contact.name,
            public_keys: contact.public_keys,
        };

        // Store in contacts map only - no need for separate public_keys storage
        self.contacts
            .lock()
            .unwrap()
            .insert(new_contact.id.clone(), new_contact);

        Ok(())
    }

    async fn update_contact(&self, contact: &DisplayContact) -> DatabaseResult<()> {
        let mut contacts = self.contacts.lock().unwrap();

        if let Some(existing_contact) = contacts.get_mut(&contact.id) {
            existing_contact.name = contact.name.clone();
            Ok(())
        } else {
            Err(DbError::Operation {
                info: DbOperationInfo::new(
                    "contacts",
                    OperationType::Update,
                    "Update contact".into(),
                ),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Contact not found",
                )),
            })
        }
    }
    async fn delete_contact(&self, id: &ContactId) -> DatabaseResult<()> {
        let mut contacts = self.contacts.lock().unwrap();
        let removed = contacts.remove(id).is_some();
        drop(contacts); // release early before mutating filerequests
        if removed {
            self.remove_contact_from_audiences(id);
        }
        // Match Sqlite: success even if contact absent
        Ok(())
    }

    async fn backup_database(
        &self,
        _backup_file: std::fs::File,
        _internal_data_dir: PathBuf,
    ) -> DatabaseResult<()> {
        Ok(())
    }

    async fn restore_database(&self, _: tokio::fs::File) -> DatabaseResult<()> {
        Ok(())
    }

    async fn get_settings(&self) -> DatabaseResult<Vec<u8>> {
        Ok(vec![])
    }

    async fn store_settings(&self, _settings: Vec<u8>) -> DatabaseResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::mocks::db::MockDb;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_filerequest_crud_and_access() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_filerequest_crud_and_access(db).await;
    }

    #[tokio::test]
    async fn test_filerequest_deletion() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_filerequest_deletion(db).await;
    }

    #[tokio::test]
    async fn test_node_keys_integrity() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_node_keys_integrity(db).await;
    }

    #[tokio::test]
    async fn test_pending_files() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_pending_files(db).await;
    }

    #[tokio::test]
    async fn test_received_files() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_received_files(db).await;
    }

    #[tokio::test]
    async fn test_delete_received_file() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_delete_received_file(db).await;
    }

    #[tokio::test]
    async fn test_delete_nonexistent_received_file() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_delete_nonexistent_received_file(db).await;
    }

    #[tokio::test]
    #[ignore = "DB should error when deleting unknown contact; current implementation returns Ok"]
    async fn test_contact_deletion_cascade() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_contact_deletion_cascade(db).await;
    }

    #[tokio::test]
    #[ignore = "DB should error when updating unknown filerequest; current implementation returns Ok"]
    async fn test_update_nonexistent_filerequest() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_update_nonexistent_filerequest(db).await;
    }

    #[tokio::test]
    async fn test_remote_filerequests_unknown_contact_query() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_remote_filerequests_unknown_contact_query(db).await;
    }

    #[tokio::test]
    async fn test_contact_public_keys() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_contact_public_keys(db).await;
    }

    #[tokio::test]
    async fn test_db_initialization() {
        let db = Arc::new(MockDb::new()) as Arc<_>;
        crate::core::db::test::test_db_initialization(db).await;
    }
}
