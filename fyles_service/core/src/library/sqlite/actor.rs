use super::SqliteMsg;
use super::SqlitePath;
use super::migrations::{MigrationError, run_migrations};
use super::schema::initialize_tables;
use super::util::db_op;
use crate::core::backup::{
    Gap, RestoredFrom, StoredBackupSecret, VerificationSchedule, VerifyEvent,
};
use crate::core::brain::action_p2p::NodeInfo;
use crate::core::db::{DbError, DbOperationInfo, OperationType};
use crate::core::domain_models::{
    CompleteReceivedFile, Contact, ContactId, CreateFilerequest, CreateIncomingFile,
    CreatePendingFiles, CreateRemoteFilerequest, DisplayContact, Filerequest, FilerequestAccess,
    FilerequestId, FylesId, GivenName, PendingFile, ReceiveStatus, ReceivedFile, RemoteFilerequest,
    SelfContact, SendStatus,
};
use crate::core::listen::ListenPort;
use crate::core::p2p::NodeKeyPairBinary;
use crate::core::settings::SettingsBlob;
use crate::core::sync::{
    self, ContactEntry, ContactsDoc, DeviceChange, DevicesDoc, DocKind, RemoteFrDoc, RemoteFrEntry,
};
use crate::library::util::error_handling::AutoMapError;
use automerge::AutoCommit;
use crypto::backup::{BackupKey, BackupSecret, KdfParams, Salt};
use crypto::{
    ContactKeys, ContactPrivateKeys, ContactPublicKeys, deserialize_ed25519_private_key,
    deserialize_ed25519_public_key, deserialize_pq_signing_private_key,
    deserialize_pq_signing_public_key, serialize_ed25519_private_key, serialize_ed25519_public_key,
    serialize_pq_signing_private_key, serialize_pq_signing_public_key,
};
use rusqlite::types::FromSql;
use rusqlite::{Connection, OptionalExtension, Result, ToSql, backup::Backup};
use semver::Version;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, instrument, trace, warn};

pub(super) struct SqliteActor {
    pub(super) conn: Connection,
    pub(super) receiver: Receiver<SqliteMsg>,
}

pub(super) struct SqlContactId(pub(super) ContactId);

impl ToSql for SqlContactId {
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>> {
        ToSql::to_sql(self.0.as_ref())
    }
}

impl FromSql for SqlContactId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let text: String = FromSql::column_result(value)?;
        text.parse()
            .map(SqlContactId)
            .map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(e)))
    }
}

fn migration_failed(error: MigrationError) -> DbError {
    DbError::Operation {
        info: DbOperationInfo::new(
            "schema_version",
            OperationType::Init,
            "running migrations".into(),
        ),
        source: Arc::new(error),
    }
}

impl SqliteActor {
    fn initialize_database(path: &SqlitePath) -> Result<Connection, DbError> {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            | rusqlite::OpenFlags::SQLITE_OPEN_URI;

        let mut conn = match path {
            SqlitePath::Memory => Connection::open_in_memory_with_flags(flags)?,
            SqlitePath::File(path) => Connection::open_with_flags(path, flags)?,
        };
        conn.execute("PRAGMA foreign_keys = ON", [])?;

        // First initialize tables according to current schema
        initialize_tables(&mut conn)?;

        // Then run any necessary migrations if schema version is older
        run_migrations(&mut conn).map_err(migration_failed)?;

        Ok(conn)
    }

    pub(super) fn new(receiver: Receiver<SqliteMsg>, path: &SqlitePath) -> Self {
        let conn = Self::initialize_database(path).expect("Failed to initialize database");
        Self { conn, receiver }
    }

    pub(super) async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                SqliteMsg::GetFilerequest(id, response, span) => {
                    span.in_scope(|| {
                        let result = self.handle_get_filerequest(&id);
                        let _ = response.send(result);
                    });
                }
                SqliteMsg::GetAllFilerequests(response) => {
                    let result = self.handle_get_all();
                    let _ = response.send(result);
                }
                SqliteMsg::CreateFilerequest(id, fr, response) => {
                    let result = self.handle_create(&id, &fr);
                    let _ = response.send(result);
                }
                SqliteMsg::UpdateFilerequest(fr, response) => {
                    let result = self.handle_update(fr);
                    let _ = response.send(result);
                }
                SqliteMsg::DeleteFilerequest(id, response) => {
                    let result = self.handle_delete(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::GetContactName(id, response) => {
                    let _ = response.send(self.handle_get_contact_name(&id));
                }
                SqliteMsg::GetContactNames(ids, response) => {
                    let _ = response.send(self.handle_get_contact_names(&ids));
                }
                SqliteMsg::GetContact(id, response) => {
                    let _ = response.send(self.handle_get_contact(&id));
                }
                SqliteMsg::GetContacts(response) => {
                    let _ = response.send(self.handle_get_contacts());
                }
                SqliteMsg::UpdateContact(contact, response) => {
                    let _ = response.send(self.handle_update_contact(&contact));
                }
                SqliteMsg::DeleteContact(id, response) => {
                    let result = self.handle_delete_contact(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::StoreNodeKeys(keys, response) => {
                    let _ = response.send(self.handle_store_node_keys(&keys));
                }
                SqliteMsg::GetNodeInfo(response) => {
                    let _ = response.send(self.handle_get_node_keys());
                }
                SqliteMsg::GetNodeKeypair(response) => {
                    let _ = response.send(self.handle_get_node_keypair());
                }
                SqliteMsg::GetSelfContactId(response) => {
                    let _ = response.send(self.handle_get_self_contact_id());
                }
                SqliteMsg::StoreSigningKeys(keys, response) => {
                    let _ = response.send(self.handle_store_signing_keys(&keys));
                }
                SqliteMsg::GetRemoteFilerequest(id, response) => {
                    let result = self.handle_get_remote_filerequest(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::GetRemoteFilerequestsByContact(contact_id, response) => {
                    let result = self.handle_get_remote_filerequests_by_contact(&contact_id);
                    let _ = response.send(result);
                }
                SqliteMsg::GetAllRemoteFilerequests(response) => {
                    let result = self.handle_get_all_remote_filerequests();
                    let _ = response.send(result);
                }
                SqliteMsg::CreateRemoteFilerequest(remote_fr, response) => {
                    let result = self.handle_create_remote_filerequest(&remote_fr);
                    let _ = response.send(result);
                }
                SqliteMsg::DeleteRemoteFilerequest(id, response) => {
                    let result = self.handle_delete_remote_filerequest(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::UpdateRemoteFilerequest(id, name, response) => {
                    let result = self.handle_update_remote_filerequest(&id, &name);
                    let _ = response.send(result);
                }
                SqliteMsg::GetPendingFile(id, response) => {
                    let result = self.handle_get_pending_file(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::GetPendingFiles(target_id, response) => {
                    let result = self.handle_get_pending_files(&target_id);
                    let _ = response.send(result);
                }
                SqliteMsg::GetAllPendingFiles(response) => {
                    let result = self.handle_get_all_pending_files();
                    let _ = response.send(result);
                }
                SqliteMsg::CreatePendingFile(pending_file, response) => {
                    let result = self.handle_create_pending_files(&pending_file);
                    let _ = response.send(result);
                }
                SqliteMsg::UpdatePendingFileStatus((id, status, retry_count), response) => {
                    let result = self.handle_update_pending_file_status(&id, &status, retry_count);
                    let _ = response.send(result);
                }
                SqliteMsg::AddInterruptionReason((id, reason), response) => {
                    let result = self.handle_add_interruption_reason(&id, &reason);
                    let _ = response.send(result);
                }
                SqliteMsg::DeletePendingFile(id, response) => {
                    let result = self.handle_delete_pending_file(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::CountNonTerminalPendingFilesForPath(id, response) => {
                    let result = self.handle_count_non_terminal_pending_files_for_path(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::CreateIncomingFile(req, response) => {
                    let result = self.handle_create_incoming_file(&req);
                    let _ = response.send(result);
                }
                SqliteMsg::UpdateReceivedFileStatus(
                    (transfer_id, status, progress_bytes),
                    response,
                ) => {
                    let result = self.handle_update_received_file_status(
                        &transfer_id,
                        &status,
                        progress_bytes,
                    );
                    let _ = response.send(result);
                }
                SqliteMsg::GetReceivedFileByTransferId(transfer_id, response) => {
                    let result = self.handle_get_received_file_by_transfer_id(&transfer_id);
                    let _ = response.send(result);
                }
                SqliteMsg::CompleteReceivedFile(req, response) => {
                    let result = self.handle_complete_received_file(&req);
                    let _ = response.send(result);
                }
                SqliteMsg::ListReceivedFiles(fr_id, tx) => {
                    let res = self.handle_list_received_files(&fr_id);
                    let _ = tx.send(res);
                }
                SqliteMsg::DeleteReceivedFile(id, response) => {
                    let result = self.handle_delete_received_file(&id);
                    let _ = response.send(result);
                }
                SqliteMsg::GetStaleReceivedFiles(older_than_ms, response) => {
                    let result = self.handle_get_stale_received_files(older_than_ms);
                    let _ = response.send(result);
                }
                SqliteMsg::GetSelfContact(response) => {
                    let _ = response.send(self.handle_get_self_contact());
                }
                SqliteMsg::GetSelfContactForDisplay(response) => {
                    let _ = response.send(self.handle_get_self_contact_for_display());
                }
                SqliteMsg::GetContactPublicKeys(contact_id, sender) => {
                    let result = self.handle_get_contact_public_keys(&contact_id);
                    let _ = sender.send(result);
                }
                SqliteMsg::KeylessSyncedContacts(sender) => {
                    let _ = sender.send(self.handle_keyless_synced_contacts());
                }
                SqliteMsg::GetContactsPublicKeys(ids, sender) => {
                    let _ = sender.send(self.handle_get_contacts_public_keys(&ids));
                }
                SqliteMsg::UpdateSelfContactName(name, sender) => {
                    let result = self.handle_update_self_contact_name(name);
                    let _ = sender.send(result);
                }
                SqliteMsg::UpdateIdentity(self_contact, sender) => {
                    let result = self.handle_update_identity(self_contact);
                    let _ = sender.send(result);
                }
                SqliteMsg::UpdateContactProjection(contact, sender) => {
                    let result = self.handle_update_contact_projection(contact);
                    let _ = sender.send(result);
                }
                SqliteMsg::PruneContactKeyCache(sender) => {
                    let _ = sender.send(self.handle_prune_contact_key_cache());
                }
                SqliteMsg::PruneOrphanedPendingFiles(sender) => {
                    let _ = sender.send(self.handle_prune_orphaned_pending_files());
                }
                SqliteMsg::Snapshot(destination, sender) => {
                    let _ = sender.send(self.snapshot_database(&destination));
                }
                SqliteMsg::Restore(sqlite, from, sender) => {
                    let _ = sender.send(self.restore_database(&sqlite, from));
                }
                SqliteMsg::GetBackupSecret(sender) => {
                    let _ = sender.send(self.handle_get_backup_secret());
                }
                SqliteMsg::StoreBackupSecret(stored, sender) => {
                    let _ = sender.send(self.handle_store_backup_secret(&stored));
                }
                SqliteMsg::StoreVerificationSchedule(schedule, sender) => {
                    let _ = sender.send(self.handle_store_verification_schedule(schedule));
                }
                SqliteMsg::GetSettings(sender) => {
                    let _ = sender.send(self.handle_get_settings());
                }
                SqliteMsg::StoreSettings(data, sender) => {
                    let _ = sender.send(self.handle_store_settings(data));
                }
                SqliteMsg::GetLicenseCache(sender) => {
                    let _ = sender.send(self.handle_get_license_cache());
                }
                SqliteMsg::StoreLicenseCache(data, sender) => {
                    let _ = sender.send(self.handle_store_license_cache(data));
                }
                SqliteMsg::GetListenPort(sender) => {
                    let _ = sender.send(self.handle_get_listen_port());
                }
                SqliteMsg::StoreListenPort(port, sender) => {
                    let _ = sender.send(self.handle_store_listen_port(port));
                }
                SqliteMsg::GetSyncDoc(kind, sender) => {
                    let _ = sender.send(self.handle_get_sync_doc(kind));
                }
                SqliteMsg::SaveSyncDoc(kind, blob, sender) => {
                    let _ = sender.send(self.handle_save_sync_doc(kind, blob));
                }
                SqliteMsg::MutateDevicesDoc(transform, sender) => {
                    let _ = sender.send(self.handle_mutate_devices_doc(transform));
                }
                SqliteMsg::MergeSyncDoc(kind, incoming, sender) => {
                    let _ = sender.send(self.handle_merge_sync_doc(kind, incoming));
                }
            }
        }
    }

    // Original handler implementations moved here
    #[instrument(skip(self))]
    fn handle_get_filerequest(&self, id: &FylesId) -> Result<Filerequest, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "filerequests",
                    OperationType::Read,
                    format!("Retrieve filerequest with ID {id}"),
                )
            },
            || {
                let fr = self.conn.query_row(
                    "SELECT id, title, description, status, access_type FROM filerequests WHERE id = ?",
                    [id.as_str()],
                    |row| {
                        let access_type: String = row.get(4)?;
                        let access = match access_type.as_str() {
                            "public" => FilerequestAccess::Public,
                            "audience" => {
                                let mut stmt = self.conn.prepare(
                                    "SELECT contact_id FROM filerequest_audience WHERE filerequest_id = ?"
                                )?;
                                let contact_ids = stmt.query_map([id.as_str()], |row| row.get::<_, SqlContactId>(0).map(|c| c.0))?
                                    .collect::<Result<Vec<_>, _>>()?;
                                FilerequestAccess::Audience { contact_ids }
                            },
                            _ => FilerequestAccess::Audience { contact_ids: vec![] }, // Default fallback
                        };

                        Ok(Filerequest {
                            id: row.get::<_, String>(0)?.into(),
                            title: row.get(1)?,
                            description: row.get(2)?,
                            is_active: row.get::<_, String>(3)? == "active",
                            access,
                        })
                    },
                )?;
                Ok(fr)
            },
        )
    }

    fn handle_get_all(&self) -> Result<Vec<Filerequest>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "filerequests",
                    OperationType::Read,
                    "Retrieve all filerequests".into(),
                )
            },
            || {
                let mut stmt = self.conn.prepare(
                    "SELECT id, title, description, status, access_type FROM filerequests",
                )?;
                let res = stmt
                    .query_map([], |row| {
                        let id: String = row.get(0)?;
                        let access_type: String = row.get(4)?;
                        let access = match access_type.as_str() {
                            "public" => FilerequestAccess::Public,
                            "audience" => {
                                let mut stmt = self.conn.prepare(
                                    "SELECT contact_id FROM filerequest_audience WHERE filerequest_id = ?"
                                )?;
                                let contact_ids = stmt.query_map([&id], |row| row.get::<_, SqlContactId>(0).map(|c| c.0))?
                                    .collect::<Result<Vec<_>, _>>()?;
                                FilerequestAccess::Audience { contact_ids }
                            },
                            _ => FilerequestAccess::Audience { contact_ids: vec![] }, // Default fallback
                        };

                        Ok(Filerequest {
                            id: id.into(),
                            title: row.get(1)?,
                            description: row.get(2)?,
                            is_active: row.get::<_, String>(3)? == "active",
                            access,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>();
                res.auto_map_err()
            },
        )
    }

    fn handle_create(
        &mut self,
        id: &FylesId,
        filerequest: &CreateFilerequest,
    ) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "filerequests",
                    OperationType::Create,
                    format!("Create filerequest {filerequest:?}"),
                )
            },
            || {
                let tx = self.conn.transaction()?;
                let id = id.to_string();

                let (access_type, contact_ids) = match &filerequest.access {
                    FilerequestAccess::Public => ("public", vec![]),
                    FilerequestAccess::Audience {
                        contact_ids: peer_ids,
                    } => ("audience", peer_ids.clone()),
                };

                tx.execute(
                    "INSERT INTO filerequests (id, title, description, created_at, updated_at, status, access_type)
                     VALUES (?1, ?2, ?3, datetime('now'), datetime('now'), ?4, ?5)",
                    (
                        &id,
                        &filerequest.title,
                        &filerequest.description,
                        if filerequest.is_active { "active" } else { "paused" },
                        access_type,
                    ),
                )?;

                // Insert audience members if any
                for contact_id in contact_ids {
                    tx.execute(
                        "INSERT INTO filerequest_audience (filerequest_id, contact_id, created_at)
                         VALUES (?, ?, datetime('now'))",
                        (&id, contact_id.as_ref()),
                    )?;
                }

                tx.commit()?;
                Ok(())
            },
        )
    }

    fn handle_update(&mut self, filerequest: Filerequest) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "filerequests",
                    OperationType::Update,
                    format!("Update filerequest {filerequest:?}"),
                )
            },
            || {
                let tx = self.conn.transaction()?;

                let (access_type, contact_ids) = match &filerequest.access {
                    FilerequestAccess::Public => ("public", vec![]),
                    FilerequestAccess::Audience {
                        contact_ids: peer_ids,
                    } => ("audience", peer_ids.clone()),
                };

                tx.execute(
                    "UPDATE filerequests
                     SET title = ?1, description = ?2, access_type = ?3, updated_at = datetime('now'), status = ?4
                     WHERE id = ?5",
                    (
                        &filerequest.title,
                        &filerequest.description,
                        access_type,
                        if filerequest.is_active { "active" } else { "paused" },
                        &filerequest.id.0,
                    ),
                )?;

                // Update audience members - first remove old entries
                tx.execute(
                    "DELETE FROM filerequest_audience WHERE filerequest_id = ?",
                    [&filerequest.id.0],
                )?;

                // Then insert new ones
                for contact_id in contact_ids {
                    tx.execute(
                        "INSERT INTO filerequest_audience (filerequest_id, contact_id, created_at)
                         VALUES (?, ?, datetime('now'))",
                        (&filerequest.id.0, contact_id.as_ref()),
                    )?;
                }

                tx.commit()?;
                Ok(())
            },
        )
    }

    fn handle_delete(&self, id: &FylesId) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "filerequests",
                    OperationType::Delete,
                    format!("Delete filerequest with ID {id}"),
                )
            },
            || {
                self.conn
                    .execute("DELETE FROM filerequests WHERE id = ?", [id.to_string()])?;
                Ok(())
            },
        )
    }

    fn handle_get_contact_name(&self, id: &ContactId) -> Result<String, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Read,
                    format!("Retrieve contact name with ID {id}"),
                )
            },
            || {
                let view = self.read_contacts_view()?;
                view.contacts
                    .get(id)
                    .map(|e| e.name.clone())
                    .ok_or_else(|| DbError::NotFound {
                        message: format!("The contact with id {id} could not be found"),
                    })
                    .map_err(Into::into)
            },
        )
    }

    fn handle_get_contact_public_keys(
        &self,
        id: &ContactId,
    ) -> Result<Option<ContactPublicKeys>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Read,
                    format!("Retrieve contact public keys with ID {id}"),
                )
            },
            || {
                let result = self.conn.query_row(
                    "SELECT pq_signing_public_key, ed25519_public_key FROM contacts WHERE id = ?",
                    [id.as_ref()],
                    |row| {
                        let pq_signing_key: Option<Vec<u8>> = row.get(0)?;
                        let ed25519_key: Option<Vec<u8>> = row.get(1)?;
                        Ok((pq_signing_key, ed25519_key))
                    },
                ).optional()?;

                match result {
                    Some((Some(pq_signing_bytes), Some(ed25519_bytes))) => {
                        let pq_signing = deserialize_pq_signing_public_key(pq_signing_bytes)
                            .map_err(|e| DbError::DataConversion {
                                message: format!(
                                    "Failed to deserialize post-quantum public signing key: {e}"
                                ),
                            })?;

                        let ed25519 =
                            deserialize_ed25519_public_key(ed25519_bytes).map_err(|e| {
                                DbError::DataConversion {
                                    message: format!(
                                        "Failed to deserialize ed25519 public key: {e}"
                                    ),
                                }
                            })?;

                        Ok(Some(ContactPublicKeys {
                            pq_signing: Box::new(pq_signing),
                            ed25519,
                        }))
                    }
                    _ => Ok(None),
                }
            },
        )
    }

    fn handle_keyless_synced_contacts(&self) -> Result<Vec<ContactId>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Read,
                    "List synced contacts without cached keys".into(),
                )
            },
            || {
                let cached: std::collections::HashSet<String> = self
                    .conn
                    .prepare("SELECT id FROM contacts WHERE pq_signing_public_key IS NOT NULL")?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<_, _>>()?;
                Ok(self
                    .read_contacts_view()?
                    .contacts
                    .into_keys()
                    .filter(|id| !cached.contains(id.as_ref()))
                    .collect())
            },
        )
    }

    fn handle_get_contacts_public_keys(
        &self,
        ids: &[ContactId],
    ) -> Result<Vec<(ContactId, ContactPublicKeys)>, DbError> {
        let mut out = Vec::new();
        for id in ids {
            if let Some(keys) = self.handle_get_contact_public_keys(id)? {
                out.push((id.clone(), keys));
            }
        }
        Ok(out)
    }

    fn handle_get_contact_names(
        &self,
        ids: &[ContactId],
    ) -> Result<HashMap<ContactId, String>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Read,
                    format!("Retrieve contacts with IDs {ids:?}"),
                )
            },
            || {
                let view = self.read_contacts_view()?;
                Ok(ids
                    .iter()
                    .filter_map(|id| view.contacts.get(id).map(|e| (id.clone(), e.name.clone())))
                    .collect())
            },
        )
    }

    fn handle_get_contact(&self, id: &ContactId) -> Result<Contact, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Read,
                    format!("Retrieve contact with ID {id}"),
                )
            },
            || {
                let view = self.read_contacts_view()?;
                let name = view
                    .contacts
                    .get(id)
                    .map(|e| e.name.clone())
                    .ok_or_else(|| DbError::NotFound {
                        message: format!("The contact with id {id} could not be found"),
                    })?;
                let id_str = id.to_string();

                let (pq_signing_key, ed25519_key) = self.conn.query_row(
                    "SELECT pq_signing_public_key, ed25519_public_key FROM contacts WHERE id = ?",
                    [id.as_ref()],
                    |row| {
                        let pq_signing_key: Option<Vec<u8>> = row.get(0)?;
                        let ed25519_key: Option<Vec<u8>> = row.get(1)?;
                        Ok((pq_signing_key, ed25519_key))
                    },
                ).optional()?.unwrap_or((None, None));

                let public_keys = if let (Some(pq_signing_bytes), Some(ed25519_bytes)) =
                    (pq_signing_key, ed25519_key)
                {
                    let pq_signing =
                        deserialize_pq_signing_public_key(pq_signing_bytes).map_err(|e| {
                            DbError::DataConversion {
                                message: format!(
                                    "Failed to deserialize post-quantum public signing key: {e}"
                                ),
                            }
                        })?;

                    let ed25519 = deserialize_ed25519_public_key(ed25519_bytes).map_err(|e| {
                        DbError::DataConversion {
                            message: format!("Failed to deserialize ed25519 public key: {e}"),
                        }
                    })?;

                    ContactPublicKeys {
                        pq_signing: Box::new(pq_signing),
                        ed25519,
                    }
                } else {
                    // The contact is known (synced) but its public keys haven't been
                    // fetched out of band yet.
                    return Err(DbError::DataNotYetFullySynced {
                        message: format!("Contact {} has no public keys yet", id_str),
                    }
                    .into());
                };

                Ok(Contact { name, public_keys })
            },
        )
    }

    fn handle_get_contacts(&self) -> Result<Vec<DisplayContact>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Read,
                    "Retrieve all contacts".into(),
                )
            },
            || {
                let view = self.read_contacts_view()?;
                Ok(view
                    .contacts
                    .into_iter()
                    .map(|(id, entry)| DisplayContact {
                        id,
                        name: entry.name,
                    })
                    .collect())
            },
        )
    }

    fn handle_update_contact(&self, contact: &DisplayContact) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Update,
                    format!("Update a contact {contact:?}"),
                )
            },
            || {
                let name = contact.name.clone();
                self.mutate_contacts_view(|view| {
                    if let Some(entry) = view.contacts.get_mut(&contact.id) {
                        entry.name = name;
                    }
                })?;
                Ok(())
            },
        )
    }

    fn handle_delete_contact(&self, id: &ContactId) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Delete,
                    format!("Delete contact with ID {id}"),
                )
            },
            || {
                // Remove from the synced document (authoritative) and from the local key
                // cache. The cache delete cascades to filerequest_audience.
                self.mutate_contacts_view(|view| {
                    view.contacts.remove(id);
                })?;
                self.conn
                    .execute("DELETE FROM contacts WHERE id = ?", [id.as_ref()])?;
                Ok(())
            },
        )
    }

    fn handle_store_node_keys(&self, keys: &NodeInfo) -> Result<(), DbError> {
        db_op(
            || DbOperationInfo::new("node_keys", OperationType::Update, "Store node keys".into()),
            || {
                let key_bytes = &keys.node_key_pair;

                // Store the post-quantum signing key as its 32-byte seed and the ed25519
                // private key. Public keys and the contact id are derived on read.
                let pq_signing_private_key =
                    serialize_pq_signing_private_key(&keys.self_contact_keys.private.pq_signing);
                let ed25519_private_key = keys.self_contact_keys.private.ed25519.as_bytes();

                self.conn.execute(
                    "INSERT OR REPLACE INTO node_keys (id, key_bytes) VALUES (1, ?)",
                    rusqlite::params![key_bytes],
                )?;

                trace!(
                    "Storing self contact with ID: {}",
                    keys.self_contact_keys.id()
                );

                self.conn.execute(
                    "INSERT INTO self_contact (id, name, pq_signing_private_key, ed25519_private_key)
                     VALUES (1, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET
                         pq_signing_private_key = excluded.pq_signing_private_key,
                         ed25519_private_key = excluded.ed25519_private_key",
                    rusqlite::params![GivenName::unnamed(), pq_signing_private_key, ed25519_private_key],
                )?;

                Ok(())
            },
        )
    }

    fn handle_get_node_keys(&self) -> Result<NodeInfo, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "node_keys and self_contact",
                    OperationType::Read,
                    "Retrieve node keys and self contact".into(),
                )
            },
            || {
                // First, get the LibP2P key bytes from node_keys table
                let key_bytes = match self
                    .conn
                    .query_row("SELECT key_bytes FROM node_keys WHERE id = 1", [], |row| {
                        let key_bytes: Vec<u8> = row.get(0)?;
                        Ok(key_bytes)
                    })
                    .optional()?
                {
                    Some(bytes) => bytes,
                    None => {
                        return Err(DbError::DataNotYetInitialized.into());
                    }
                };

                Ok(NodeInfo {
                    node_key_pair: key_bytes,
                    self_contact_keys: self.read_self_signing_keys()?,
                })
            },
        )
    }

    fn handle_get_node_keypair(&self) -> Result<NodeKeyPairBinary, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "node_keys",
                    OperationType::Read,
                    "Retrieve node keypair".into(),
                )
            },
            || {
                self.conn
                    .query_row("SELECT key_bytes FROM node_keys WHERE id = 1", [], |row| {
                        row.get(0)
                    })
                    .optional()?
                    .ok_or(DbError::DataNotYetInitialized.into())
            },
        )
    }

    fn handle_get_self_contact_id(&self) -> Result<ContactId, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "self_contact",
                    OperationType::Read,
                    "Retrieve self contact id".into(),
                )
            },
            || Ok(self.read_self_signing_keys()?.id()),
        )
    }

    fn self_contact_name(&self) -> Result<String, DbError> {
        self.conn
            .query_row("SELECT name FROM self_contact WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?
            .ok_or(DbError::DataNotYetInitialized)
    }

    fn read_self_signing_keys(&self) -> Result<ContactKeys, DbError> {
        let (ed25519_private, pq_signing_private) = self
            .conn
            .query_row(
                "SELECT ed25519_private_key, pq_signing_private_key FROM self_contact WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<Vec<u8>>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                    ))
                },
            )
            .optional()?
            .ok_or(DbError::DataNotYetInitialized)?;

        let (Some(ed25519_private), Some(pq_signing_private)) =
            (ed25519_private, pq_signing_private)
        else {
            return Err(DbError::SigningKeysMissing);
        };

        let pq_signing = Box::new(
            deserialize_pq_signing_private_key(pq_signing_private).map_err(|_| {
                DbError::Validation {
                    message: "Could not deserialize post-quantum private signing key".into(),
                }
            })?,
        );
        let ed25519 =
            deserialize_ed25519_private_key(ed25519_private).map_err(|_| DbError::Validation {
                message: "Could not deserialize ed25519 private key".into(),
            })?;

        Ok(ContactKeys::from_private(ContactPrivateKeys {
            pq_signing,
            ed25519,
        }))
    }

    /// Writes the signing keys into the existing self_contact row, leaving the name and
    /// node_keys untouched. Only the private keys are stored.
    fn handle_store_signing_keys(&self, keys: &ContactKeys) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "self_contact",
                    OperationType::Update,
                    "Store signing keys".into(),
                )
            },
            || {
                let pq_signing_private_key =
                    serialize_pq_signing_private_key(&keys.private.pq_signing);
                let ed25519_private_key = keys.private.ed25519.as_bytes();

                self.conn.execute(
                    "UPDATE self_contact
                     SET pq_signing_private_key = ?,
                         ed25519_private_key = ?
                     WHERE id = 1",
                    rusqlite::params![pq_signing_private_key, ed25519_private_key],
                )?;
                Ok(())
            },
        )
    }

    fn handle_create_remote_filerequest(
        &self,
        remote_filerequest: &CreateRemoteFilerequest,
    ) -> Result<FylesId, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Create,
                    format!("Create remote filerequest {remote_filerequest:?}"),
                )
            },
            || {
                let id = FylesId::new().0;
                let fid = FylesId(id.clone());
                let entry = RemoteFrEntry {
                    peer_id_hint: remote_filerequest.peer_id.clone(),
                    filerequest_id: remote_filerequest.filerequest_id.clone().into(),
                    contact_id: remote_filerequest.contact_id.clone(),
                    name: remote_filerequest.name.clone(),
                };
                self.mutate_remote_fr_view(move |view| {
                    view.remote_filerequests.insert(fid, entry);
                })?;
                Ok(id.into())
            },
        )
    }

    fn handle_get_remote_filerequest(&self, id: &FylesId) -> Result<RemoteFilerequest, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Read,
                    format!("Retrieve remote filerequest with ID {id}"),
                )
            },
            || {
                let view = self.read_remote_fr_view()?;
                view.remote_filerequests
                    .get(id)
                    .map(|entry| remote_fr_from_entry(id.clone(), entry))
                    .ok_or_else(|| DbError::NotFound {
                        message: format!("The remote filerequest with id {id} could not be found"),
                    })
                    .map_err(Into::into)
            },
        )
    }

    fn handle_get_remote_filerequests_by_contact(
        &self,
        contact_id: &ContactId,
    ) -> Result<Vec<RemoteFilerequest>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Read,
                    format!("Retrieve remote filerequests for contact with ID {contact_id}"),
                )
            },
            || {
                let view = self.read_remote_fr_view()?;
                Ok(view
                    .remote_filerequests
                    .into_iter()
                    .filter(|(_, entry)| &entry.contact_id == contact_id)
                    .map(|(id, entry)| remote_fr_from_entry(id, &entry))
                    .collect())
            },
        )
    }

    fn handle_get_all_remote_filerequests(&self) -> Result<Vec<RemoteFilerequest>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Read,
                    "Retrieve all remote filerequests".into(),
                )
            },
            || {
                let view = self.read_remote_fr_view()?;
                Ok(view
                    .remote_filerequests
                    .into_iter()
                    .map(|(id, entry)| remote_fr_from_entry(id, &entry))
                    .collect())
            },
        )
    }
    fn handle_delete_remote_filerequest(&self, id: &FylesId) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Delete,
                    format!("Delete remote filerequest with ID {id}"),
                )
            },
            || {
                // Delete any pending files for this remote filerequest and remove it from the doc.
                self.conn.execute(
                    "DELETE FROM pending_files WHERE target_filerequest_id = ?",
                    [id.as_str()],
                )?;
                self.mutate_remote_fr_view(|view| {
                    view.remote_filerequests.remove(id);
                })?;
                Ok(())
            },
        )
    }
    fn handle_update_remote_filerequest(&self, id: &FylesId, name: &str) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "remote_filerequests",
                    OperationType::Update,
                    format!("Update remote filerequest {id:?}"),
                )
            },
            || {
                let name = name.to_string();
                self.mutate_remote_fr_view(|view| {
                    if let Some(entry) = view.remote_filerequests.get_mut(id) {
                        entry.name = name;
                    }
                })?;
                Ok(())
            },
        )
    }

    fn handle_get_pending_file(&self, id: &FylesId) -> Result<PendingFile, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Read,
                    format!("Retrieve pending file with ID {id}"),
                )
            },
            || {
                // Get the basic pending file data
                let basic_file = self.conn.query_row(
                    "SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, source_modified_ms FROM pending_files WHERE id = ?",
                    [id.as_str()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)? as usize,
                            row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                            row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                            row.get::<_, Option<String>>(7)?,
                            row.get::<_, Option<String>>(8)?,
                            row.get::<_, String>(9)?,
                            row.get::<_, Option<i64>>(10)?,
                        ))
                    },
                )?;

                let (
                    id_str,
                    file_path,
                    target_filerequest_id,
                    status_tag,
                    retry_count,
                    progress_bytes,
                    file_size_bytes,
                    transfer_id,
                    display_name,
                    interruption_reasons_json,
                    source_modified_ms,
                ) = basic_file;

                let interruption_reasons: Vec<String> =
                    serde_json::from_str(&interruption_reasons_json).unwrap_or_default();

                // Resolve the associated remote filerequest's contact_id from the synced document.
                let contact_id: ContactId = self
                    .remote_fr_contact_id(&target_filerequest_id)?
                    .ok_or_else(|| DbError::NotFound {
                        message: format!(
                            "No remote filerequest {target_filerequest_id} backs pending file {id_str}"
                        ),
                    })?;

                let status = SendStatus::from_db_columns(
                    &status_tag,
                    progress_bytes,
                    file_size_bytes,
                    transfer_id,
                    source_modified_ms,
                )
                .map_err(|e| DbError::DataConversion {
                    message: format!("Failed to parse send status: {e}"),
                })?;

                Ok(PendingFile {
                    id: id_str.into(),
                    contact_id,
                    file_path,
                    retry_count,
                    target_filerequest_id: target_filerequest_id.into(),
                    status,
                    display_name,
                    interruption_reasons,
                })
            },
        )
    }

    fn handle_get_pending_files(
        &self,
        target_filerequest_id: &str,
    ) -> Result<Vec<PendingFile>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Read,
                    format!(
                        "Retrieve pending files for target filerequest with ID {target_filerequest_id}"
                    ),
                )
            },
            || {
                // First get all the basic pending file data
                let mut stmt = self.conn.prepare(
                    "SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, source_modified_ms FROM pending_files WHERE target_filerequest_id = ?",
                )?;

                #[allow(clippy::type_complexity)]
                let pending_files: Result<
                    Vec<(
                        String,
                        String,
                        String,
                        String,
                        usize,
                        Option<u64>,
                        Option<u64>,
                        Option<String>,
                        Option<String>,
                        String,
                        Option<i64>,
                    )>,
                    rusqlite::Error,
                > = stmt
                    .query_map([target_filerequest_id], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get::<_, i64>(4)? as usize,
                            row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                            row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                            row.get(7)?,
                            row.get(8)?,
                            row.get(9)?,
                            row.get(10)?,
                        ))
                    })?
                    .collect();

                let pending_files = pending_files?;

                // Resolve the remote filerequest's contact_id once, from the synced document.
                let contact_id: ContactId = self
                    .remote_fr_contact_id(target_filerequest_id)?
                    .ok_or_else(|| DbError::NotFound {
                        message: format!(
                            "No remote filerequest {target_filerequest_id} backs its pending files"
                        ),
                    })?;

                // Create PendingFile objects with the determined authentication
                let mut result = Vec::with_capacity(pending_files.len());
                for (
                    id,
                    file_path,
                    target_id,
                    status_tag,
                    retry_count,
                    progress_bytes,
                    file_size_bytes,
                    transfer_id,
                    display_name,
                    interruption_reasons_json,
                    source_modified_ms,
                ) in pending_files
                {
                    let status = SendStatus::from_db_columns(
                        &status_tag,
                        progress_bytes,
                        file_size_bytes,
                        transfer_id,
                        source_modified_ms,
                    )
                    .map_err(|e| DbError::DataConversion {
                        message: format!("Failed to parse send status: {e}"),
                    })?;
                    result.push(PendingFile {
                        id: id.into(),
                        file_path,
                        retry_count,
                        target_filerequest_id: target_id.into(),
                        status,
                        contact_id: contact_id.clone(),
                        display_name,
                        interruption_reasons: serde_json::from_str(&interruption_reasons_json)
                            .unwrap_or_default(),
                    });
                }

                Ok(result)
            },
        )
    }

    fn handle_get_all_pending_files(&self) -> Result<Vec<PendingFile>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Read,
                    "Retrieve all pending files".into(),
                )
            },
            || {
                let mut stmt = self.conn.prepare(
                    "SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, source_modified_ms
                     FROM pending_files",
                )?;

                let pending_files_data = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,                        // id
                            row.get::<_, String>(1)?,                        // file_path
                            row.get::<_, String>(2)?, // target_filerequest_id
                            row.get::<_, String>(3)?, // status
                            row.get::<_, i64>(4)? as usize, // retry_count
                            row.get::<_, Option<i64>>(5)?.map(|v| v as u64), // progress_bytes
                            row.get::<_, Option<i64>>(6)?.map(|v| v as u64), // file_size_bytes
                            row.get::<_, Option<String>>(7)?, // transfer_id
                            row.get::<_, Option<String>>(8)?, // display_name
                            row.get::<_, String>(9)?, // interruption_reasons
                            row.get::<_, Option<i64>>(10)?, // source_modified_ms
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;

                // Resolve every contact_id from the document in one hydrate.
                let remote_frs = self.read_remote_fr_view()?.remote_filerequests;

                let mut result = Vec::with_capacity(pending_files_data.len());

                for (
                    id,
                    file_path,
                    target_id,
                    status_tag,
                    retry_count,
                    progress_bytes,
                    file_size_bytes,
                    transfer_id,
                    display_name,
                    interruption_reasons_json,
                    source_modified_ms,
                ) in pending_files_data
                {
                    // A pending file whose remote filerequest is gone is an orphan; skip it rather
                    // than fail the whole listing.
                    let Some(entry) = remote_frs.get(&FylesId(target_id.clone())) else {
                        warn!(
                            target_filerequest_id = %target_id,
                            pending_file_id = %id,
                            "Skipping pending file with no backing remote filerequest"
                        );
                        continue;
                    };
                    let status = SendStatus::from_db_columns(
                        &status_tag,
                        progress_bytes,
                        file_size_bytes,
                        transfer_id,
                        source_modified_ms,
                    )
                    .map_err(|e| DbError::DataConversion {
                        message: format!("Failed to parse send status: {e}"),
                    })?;
                    result.push(PendingFile {
                        id: id.into(),
                        file_path,
                        retry_count,
                        target_filerequest_id: target_id.into(),
                        status,
                        contact_id: entry.contact_id.clone(),
                        display_name,
                        interruption_reasons: serde_json::from_str(&interruption_reasons_json)
                            .unwrap_or_default(),
                    });
                }

                Ok(result)
            },
        )
    }

    fn handle_create_pending_files(
        &mut self,
        pending_file: &CreatePendingFiles,
    ) -> Result<Vec<FylesId>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Create,
                    format!("Create {} pending files", pending_file.file_infos.len()),
                )
            },
            || {
                if self
                    .remote_fr_contact_id(&pending_file.target_filerequest_id.0)?
                    .is_none()
                {
                    return Err(DbError::NotFound {
                        message: format!(
                            "Cannot create pending files for unknown remote filerequest {}",
                            pending_file.target_filerequest_id
                        ),
                    }
                    .into());
                }

                let tx = self.conn.transaction()?;

                let id_and_paths: Vec<_> = pending_file
                    .file_infos
                    .iter()
                    .map(|file_info| (FylesId::new(), &file_info.path, &file_info.display_name))
                    .collect();

                for (id, file_path, display_name) in &id_and_paths {
                    tx.execute(
                        "INSERT INTO pending_files (id, file_path, target_filerequest_id, status, display_name, created_at)
                         VALUES (?, ?, ?, ?, ?, datetime('now'))",
                        (
                            id.as_str(),
                            *file_path,
                            &pending_file.target_filerequest_id.0,
                            "Pending",
                            display_name.as_deref(),
                        ),
                    )?;
                }

                tx.commit()?;

                Ok(id_and_paths.into_iter().map(|(id, _, _)| id).collect())
            },
        )
    }

    fn handle_delete_pending_file(&self, id: &FylesId) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Delete,
                    format!("Delete pending file with ID {id}"),
                )
            },
            || {
                self.conn
                    .execute("DELETE FROM pending_files WHERE id = ?", [id.as_str()])?;
                Ok(())
            },
        )
    }

    fn handle_count_non_terminal_pending_files_for_path(
        &self,
        id: &FylesId,
    ) -> Result<usize, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Read,
                    format!("Count non-terminal pending files for path of ID {id}"),
                )
            },
            || {
                let count: usize = self.conn.query_row(
                    "SELECT COUNT(*) FROM pending_files
                     WHERE file_path = (SELECT file_path FROM pending_files WHERE id = ?1)
                       AND id != ?1
                       AND status IN ('Pending', 'Sending', 'Interrupted')",
                    [id.as_str()],
                    |row| Ok(row.get::<_, i64>(0)? as usize),
                )?;
                Ok(count)
            },
        )
    }

    fn handle_add_interruption_reason(&self, id: &FylesId, reason: &str) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Update,
                    "Add interruption reason".into(),
                )
            },
            || {
                // Read current, parse, append, serialize, update
                let current_json: String = self.conn.query_row(
                    "SELECT interruption_reasons FROM pending_files WHERE id = ?",
                    [id.as_str()],
                    |row| row.get(0),
                )?;
                let mut reasons: Vec<String> =
                    serde_json::from_str(&current_json).unwrap_or_default();
                reasons.push(reason.to_string());
                let new_json = serde_json::to_string(&reasons).unwrap_or_else(|_| "[]".to_string());

                self.conn.execute(
                    "UPDATE pending_files SET interruption_reasons = ? WHERE id = ?",
                    [&new_json, id.as_str()],
                )?;
                Ok(())
            },
        )
    }

    fn handle_update_pending_file_status(
        &self,
        id: &FylesId,
        status: &SendStatus,
        retry_count: Option<usize>,
    ) -> Result<FylesId, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Update,
                    format!("Update pending file status to {status}"),
                )
            },
            || {
                trace!("SQLITE updating pending file status to {:?}", status);
                let tag = status.status_tag().map_err(|e| DbError::DataConversion {
                    message: format!("Failed to convert status to tag: {e}"),
                })?;
                let transfer_data = status.transfer_data();
                let target_filerequest_id: FylesId = match (retry_count, transfer_data) {
                    (Some(count), _) => {
                        // Full update: status, retry_count, and transfer data columns
                        self.conn.query_row(
                            "UPDATE pending_files SET status = ?, retry_count = ?, progress_bytes = ?, file_size_bytes = ?, transfer_id = ?, source_modified_ms = ? WHERE id = ? RETURNING target_filerequest_id",
                            (
                                tag,
                                count as i64,
                                transfer_data.map(|d| d.progress_bytes as i64),
                                transfer_data.map(|d| d.file_size_bytes as i64),
                                transfer_data.map(|d| &*d.transfer_id),
                                transfer_data.map(|d| d.source_modified_ms),
                                id.as_str(),
                            ),
                            |row| {
                                let target_filerequest_id: String = row.get(0)?;
                                Ok(target_filerequest_id)
                            }
                        )?.into()
                    }
                    (None, Some(td)) => {
                        // Progress update without retry_count change (e.g. FileSending)
                        self.conn.query_row(
                            "UPDATE pending_files SET status = ?, progress_bytes = ?, file_size_bytes = ?, transfer_id = ?, source_modified_ms = ? WHERE id = ? RETURNING target_filerequest_id",
                            (
                                tag,
                                td.progress_bytes as i64,
                                td.file_size_bytes as i64,
                                &*td.transfer_id,
                                td.source_modified_ms,
                                id.as_str(),
                            ),
                            |row| {
                                let target_filerequest_id: String = row.get(0)?;
                                Ok(target_filerequest_id)
                            }
                        )?.into()
                    }
                    (None, None) if matches!(status, SendStatus::Pending) => {
                        self.conn.query_row(
                            "UPDATE pending_files SET status = ?, progress_bytes = NULL, file_size_bytes = NULL, transfer_id = NULL, source_modified_ms = NULL WHERE id = ? RETURNING target_filerequest_id",
                            (
                                tag,
                                id.as_str(),
                            ),
                            |row| {
                                let target_filerequest_id: String = row.get(0)?;
                                Ok(target_filerequest_id)
                            }
                        )?.into()
                    }
                    (None, None) => {
                        // Terminal state transition: only update status, preserve all
                        // transfer metadata (progress_bytes, file_size_bytes, transfer_id,
                        // retry_count) for post-mortem diagnostics.
                        self.conn.query_row(
                            "UPDATE pending_files SET status = ? WHERE id = ? RETURNING target_filerequest_id",
                            (
                                tag,
                                id.as_str(),
                            ),
                            |row| {
                                let target_filerequest_id: String = row.get(0)?;
                                Ok(target_filerequest_id)
                            }
                        )?.into()
                    }
                };
                Ok(target_filerequest_id)
            },
        )
    }

    fn handle_create_incoming_file(
        &self,
        incoming: &CreateIncomingFile,
    ) -> Result<FylesId, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Create,
                    format!("Create incoming file record for {}", incoming.file_name),
                )
            },
            || {
                let new_id = FylesId::new().0;
                self.conn.execute(
                    "INSERT INTO received_files
                     (id, contact_id, peer_id, filerequest_id, transfer_id, file_name, persisted_as_name, file_path, file_size_bytes, progress_bytes, status, started_at_ms, received_at_ms)
                     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, 0, 'Receiving', ?, NULL)",
                    (
                        &new_id,
                        incoming.contact_id.as_ref().map(|id| id.to_string()),
                        &incoming.peer_id,
                        &incoming.filerequest_id.0,
                        &incoming.transfer_id.0,
                        &incoming.file_name,
                        &incoming.persisted_as_name,
                        incoming.file_size_bytes as i64,
                        incoming.started_at_ms,
                    ),
                )?;
                Ok(new_id.into())
            },
        )
    }

    fn handle_update_received_file_status(
        &self,
        transfer_id: &FylesId,
        status: &ReceiveStatus,
        progress_bytes: u64,
    ) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Update,
                    format!("Update received file status to {status}"),
                )
            },
            || {
                let tag = status.status_tag().map_err(|e| DbError::DataConversion {
                    message: format!("Failed to convert status to tag: {e}"),
                })?;
                let changes = self.conn.execute(
                    "UPDATE received_files SET status = ?, progress_bytes = ? WHERE transfer_id = ?",
                    (tag, progress_bytes as i64, &transfer_id.0),
                )?;
                if changes == 0 {
                    return Err(DbError::NotFound {
                        message: format!(
                            "Received file with transfer_id {} not found",
                            transfer_id.0
                        ),
                    }
                    .into());
                }
                Ok(())
            },
        )
    }

    fn handle_get_received_file_by_transfer_id(
        &self,
        transfer_id: &FylesId,
    ) -> Result<ReceivedFile, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Read,
                    format!("Get received file by transfer_id {transfer_id}"),
                )
            },
            || {
                self.conn.query_row(
                    "SELECT id, contact_id, peer_id, filerequest_id, transfer_id, file_name, persisted_as_name, file_path, file_size_bytes, progress_bytes, status, started_at_ms, received_at_ms
                     FROM received_files WHERE transfer_id = ?",
                    [&transfer_id.0],
                    |row| {
                        Ok(ReceivedFile {
                            id: row.get::<_, String>(0)?.into(),
                            contact_id: row.get::<_, Option<SqlContactId>>(1)?.map(|c| c.0),
                            peer_id: row.get(2)?,
                            filerequest_id: row.get::<_, String>(3)?.into(),
                            transfer_id: row.get::<_, Option<String>>(4)?.map(Into::into),
                            file_name: row.get(5)?,
                            persisted_as_name: row.get(6)?,
                            file_path: row.get(7)?,
                            file_size_bytes: row.get::<_, i64>(8)? as u64,
                            progress_bytes: row.get::<_, i64>(9)? as u64,
                            status: ReceiveStatus::from_db_column(&row.get::<_, String>(10)?),
                            started_at_ms: row.get(11)?,
                            received_at_ms: row.get(12)?,
                        })
                    },
                )
                .map_err(|e| e.into())
            },
        )
    }

    fn handle_complete_received_file(
        &self,
        completed: &CompleteReceivedFile,
    ) -> Result<FylesId, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Update,
                    format!(
                        "Complete received file for transfer {}",
                        completed.transfer_id
                    ),
                )
            },
            || {
                let id: String = self.conn.query_row(
                    "UPDATE received_files
                         SET file_path = ?, persisted_as_name = ?, received_at_ms = ?, status = 'Completed'
                         WHERE transfer_id = ?
                         RETURNING id",
                    (
                        &completed.file_path,
                        &completed.persisted_as_name,
                        completed.received_at_ms,
                        &completed.transfer_id.0,
                    ),
                    |row| row.get(0),
                )?;

                Ok(id.into())
            },
        )
    }

    fn handle_list_received_files(
        &self,
        filerequest_id: &FylesId,
    ) -> Result<Vec<ReceivedFile>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Read,
                    format!("List received files for filerequest {}", filerequest_id),
                )
            },
            || {
                let mut stmt = self.conn.prepare(
                    "SELECT id, contact_id, peer_id, filerequest_id, transfer_id, file_name, persisted_as_name, file_path, file_size_bytes, progress_bytes, status, started_at_ms, received_at_ms
                     FROM received_files WHERE filerequest_id = ?",
                )?;
                let results = stmt
                    .query_map([&filerequest_id.0], |row| {
                        Ok(ReceivedFile {
                            id: row.get::<_, String>(0)?.into(),
                            contact_id: row.get::<_, Option<SqlContactId>>(1)?.map(|c| c.0),
                            peer_id: row.get(2)?,
                            filerequest_id: row.get::<_, String>(3)?.into(),
                            transfer_id: row.get::<_, Option<String>>(4)?.map(Into::into),
                            file_name: row.get(5)?,
                            persisted_as_name: row.get(6)?,
                            file_path: row.get(7)?,
                            file_size_bytes: row.get::<_, i64>(8)? as u64,
                            progress_bytes: row.get::<_, i64>(9)? as u64,
                            status: ReceiveStatus::from_db_column(&row.get::<_, String>(10)?),
                            started_at_ms: row.get(11)?,
                            received_at_ms: row.get(12)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(results)
            },
        )
    }

    fn handle_delete_received_file(&self, id: &FylesId) -> Result<FilerequestId, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Delete,
                    "Delete received file".into(),
                )
            },
            || {
                self.conn
                    .query_row(
                        "DELETE FROM received_files WHERE id = ? RETURNING filerequest_id",
                        [id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .map(|id| FilerequestId(FylesId(id)))
                    .ok_or_else(|| {
                        DbError::NotFound {
                            message: format!("Received file with id {} not found", id.0),
                        }
                        .into()
                    })
            },
        )
    }

    fn handle_get_stale_received_files(
        &self,
        older_than_ms: i64,
    ) -> Result<Vec<ReceivedFile>, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "received_files",
                    OperationType::Read,
                    "List stale received files".into(),
                )
            },
            || {
                let mut stmt = self.conn.prepare(
                    "SELECT id, contact_id, peer_id, filerequest_id, transfer_id, file_name, persisted_as_name, file_path, file_size_bytes, progress_bytes, status, started_at_ms, received_at_ms
                     FROM received_files
                     WHERE status IN ('Receiving', 'Interrupted')
                       AND started_at_ms < ?"
                )?;
                let results = stmt
                    .query_map([older_than_ms], |row| {
                        Ok(ReceivedFile {
                            id: row.get::<_, String>(0)?.into(),
                            contact_id: row.get::<_, Option<SqlContactId>>(1)?.map(|c| c.0),
                            peer_id: row.get(2)?,
                            filerequest_id: row.get::<_, String>(3)?.into(),
                            transfer_id: row.get::<_, Option<String>>(4)?.map(Into::into),
                            file_name: row.get(5)?,
                            persisted_as_name: row.get(6)?,
                            file_path: row.get(7)?,
                            file_size_bytes: row.get::<_, i64>(8)? as u64,
                            progress_bytes: row.get::<_, i64>(9)? as u64,
                            status: ReceiveStatus::from_db_column(&row.get::<_, String>(10)?),
                            started_at_ms: row.get(11)?,
                            received_at_ms: row.get(12)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(results)
            },
        )
    }

    fn handle_get_self_contact(&self) -> Result<SelfContact, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "self_contact",
                    OperationType::Read,
                    "Retrieve self contact information".into(),
                )
            },
            || {
                trace!("SQLITE retrieving self contact information");
                let name = self.self_contact_name()?;
                Ok(SelfContact {
                    name,
                    keys: self.read_self_signing_keys()?,
                })
            },
        )
    }

    fn handle_get_self_contact_for_display(&self) -> Result<DisplayContact, DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "self_contact",
                    OperationType::Read,
                    "Retrieve self contact display information".into(),
                )
            },
            || {
                Ok(DisplayContact {
                    id: self.read_self_signing_keys()?.id(),
                    name: self.self_contact_name()?,
                })
            },
        )
    }

    fn handle_update_self_contact_name(&self, name: String) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "self_contact",
                    OperationType::Update,
                    "Update self contact name".into(),
                )
            },
            || {
                self.conn
                    .execute("UPDATE self_contact SET name = ? WHERE id = 1", [&name])?;
                Ok(())
            },
        )
    }

    fn handle_update_identity(&self, self_contact: SelfContact) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "self_contact",
                    OperationType::Update,
                    "Update self contact identity".into(),
                )
            },
            || {
                // Only the private keys are stored; the public keys are derived on read.
                let pq_signing_private_key =
                    serialize_pq_signing_private_key(&self_contact.keys.private.pq_signing);
                let ed25519_private_key =
                    serialize_ed25519_private_key(&self_contact.keys.private.ed25519);

                self.conn.execute(
                    "UPDATE self_contact
                     SET name = ?,
                         ed25519_private_key = ?,
                         pq_signing_private_key = ?
                     WHERE id = 1",
                    (
                        &self_contact.name,
                        &ed25519_private_key,
                        &pq_signing_private_key,
                    ),
                )?;

                Ok(())
            },
        )
    }

    fn handle_update_contact_projection(&mut self, contact: Contact) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Create,
                    "Register a contact".into(),
                )
            },
            || {
                // Serialize keys (post-quantum public key serialization is infallible)
                let pq_signing_public_key =
                    serialize_pq_signing_public_key(&contact.public_keys.pq_signing);
                let ed25519_public_key = serialize_ed25519_public_key(&contact.public_keys.ed25519);

                self.conn.execute(
                    "INSERT INTO contacts (id, name, created_at, updated_at, ed25519_public_key, pq_signing_public_key)
                     VALUES (?, ?, datetime('now'), datetime('now'), ?, ?)
                     ON CONFLICT(id) DO UPDATE SET
                         name = excluded.name,
                         updated_at = datetime('now'),
                         ed25519_public_key = excluded.ed25519_public_key,
                         pq_signing_public_key = excluded.pq_signing_public_key",
                    (
                        &SqlContactId(contact.id()),
                        &contact.name,
                        &ed25519_public_key,
                        &pq_signing_public_key,
                    ),
                )?;

                let id = contact.id();
                let name = contact.name.clone();
                self.mutate_contacts_view(move |view| {
                    view.contacts.insert(id, ContactEntry { name });
                })?;

                Ok(())
            },
        )
    }

    fn handle_prune_contact_key_cache(&self) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "contacts",
                    OperationType::Delete,
                    "Prune cached keys for contacts removed via sync".into(),
                )
            },
            || {
                let keep: Vec<String> = self
                    .read_contacts_view()?
                    .contacts
                    .into_keys()
                    .map(|id| id.to_string())
                    .collect();
                if keep.is_empty() {
                    self.conn.execute("DELETE FROM contacts", [])?;
                } else {
                    let placeholders = std::iter::repeat_n("?", keep.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    self.conn.execute(
                        &format!("DELETE FROM contacts WHERE id NOT IN ({placeholders})"),
                        rusqlite::params_from_iter(keep.iter()),
                    )?;
                }
                Ok(())
            },
        )
    }

    fn handle_prune_orphaned_pending_files(&self) -> Result<(), DbError> {
        db_op(
            || {
                DbOperationInfo::new(
                    "pending_files",
                    OperationType::Delete,
                    "Prune pending files for remote filerequests removed via sync".into(),
                )
            },
            || {
                let keep: Vec<String> = self
                    .read_remote_fr_view()?
                    .remote_filerequests
                    .into_keys()
                    .map(|id| id.0)
                    .collect();
                if keep.is_empty() {
                    self.conn.execute("DELETE FROM pending_files", [])?;
                } else {
                    let placeholders = std::iter::repeat_n("?", keep.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    self.conn.execute(
                        &format!(
                            "DELETE FROM pending_files WHERE target_filerequest_id NOT IN ({placeholders})"
                        ),
                        rusqlite::params_from_iter(keep.iter()),
                    )?;
                }
                Ok(())
            },
        )
    }

    fn snapshot_database(&mut self, destination: &Path) -> Result<(), DbError> {
        debug!("snapshotting the database to {}", destination.display());
        let mut snapshot = Connection::open(destination)?;
        Backup::new(&self.conn, &mut snapshot)?.step(-1)?;
        snapshot.close().map_err(|(_, e)| e)?;
        Ok(())
    }

    fn restore_database(&mut self, sqlite: &Path, from: RestoredFrom) -> Result<(), DbError> {
        debug!("restoring the database from {}", sqlite.display());
        let mut restored = Connection::open(sqlite)?;
        run_migrations(&mut restored).map_err(migration_failed)?;
        Backup::new(&restored, &mut self.conn)?.step(-1)?;
        restored.close().map_err(|(_, e)| e)?;
        if let RestoredFrom::Encrypted { at_ms } = from
            && let Err(e) = self.count_restore_as_verified(at_ms)
        {
            warn!("the restore did not advance the backup password schedule: {e}");
        }
        Ok(())
    }

    /// Runs after the database is already replaced, so a failure here must not fail the restore.
    fn count_restore_as_verified(&self, at_ms: i64) -> Result<(), DbError> {
        match self.handle_get_backup_secret()? {
            Some(stored) => self.handle_store_verification_schedule(
                stored.schedule.verified(VerifyEvent::Correct, at_ms),
            ),
            None => Ok(()),
        }
    }

    fn handle_get_backup_secret(&self) -> Result<Option<StoredBackupSecret>, DbError> {
        let row = self
            .conn
            .query_row(
                "SELECT salt, m_cost_kib, t_cost, p_cost, key, verify_gap, verify_due_at_ms, \
                 verify_missed FROM backup_secret WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, [u8; _]>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, u32>(3)?,
                        row.get::<_, [u8; _]>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, bool>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((salt, m_cost, t_cost, p_cost, key, gap, due_at_ms, missed)) = row else {
            return Ok(None);
        };
        let conversion = |message: String| DbError::DataConversion { message };
        let params =
            KdfParams::new(m_cost, t_cost, p_cost).map_err(|e| conversion(e.to_string()))?;
        let gap = Gap::try_from(gap).map_err(|e| conversion(e.to_string()))?;
        Ok(Some(StoredBackupSecret {
            secret: BackupSecret::from_parts(Salt::from(salt), params, BackupKey::from(key)),
            schedule: VerificationSchedule::from_parts(gap, due_at_ms, missed),
        }))
    }

    fn handle_store_backup_secret(&self, stored: &StoredBackupSecret) -> Result<(), DbError> {
        let StoredBackupSecret { secret, schedule } = stored;
        self.conn.execute(
            "INSERT INTO backup_secret (id, salt, m_cost_kib, t_cost, p_cost, key, verify_gap, \
             verify_due_at_ms, verify_missed) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(id) DO UPDATE SET salt = excluded.salt, m_cost_kib = excluded.m_cost_kib, \
             t_cost = excluded.t_cost, p_cost = excluded.p_cost, key = excluded.key, \
             verify_gap = excluded.verify_gap, verify_due_at_ms = excluded.verify_due_at_ms, \
             verify_missed = excluded.verify_missed",
            rusqlite::params![
                secret.salt().as_bytes(),
                secret.params().m_cost_kib(),
                secret.params().t_cost(),
                secret.params().p_cost(),
                secret.key().as_bytes(),
                i64::from(schedule.gap()),
                schedule.due_at_ms(),
                schedule.missed(),
            ],
        )?;
        Ok(())
    }

    fn handle_store_verification_schedule(
        &self,
        schedule: VerificationSchedule,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE backup_secret SET verify_gap = ?1, verify_due_at_ms = ?2, verify_missed = ?3 \
             WHERE id = 1",
            rusqlite::params![
                i64::from(schedule.gap()),
                schedule.due_at_ms(),
                schedule.missed(),
            ],
        )?;
        Ok(())
    }

    /// Current version of the binary settings format, stored alongside the blob for future migrations.
    const SETTINGS_VERSION: Version = Version::new(1, 0, 0);

    fn handle_get_settings(&self) -> Result<SettingsBlob, DbError> {
        let result: Option<Vec<u8>> = self
            .conn
            .query_row("SELECT data FROM settings WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(SettingsBlob(result.unwrap_or_default()))
    }

    fn handle_store_settings(&self, data: SettingsBlob) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO settings (id, data, version) VALUES (1, ?1, ?2) \
             ON CONFLICT(id) DO UPDATE SET data = excluded.data, version = excluded.version",
            rusqlite::params![&data.0, Self::SETTINGS_VERSION.to_string()],
        )?;
        Ok(())
    }

    /// Current version of the binary license-cache format, stored alongside the blob for
    /// future migrations.
    const LICENSE_CACHE_VERSION: Version = Version::new(1, 0, 0);

    fn handle_get_license_cache(&self) -> Result<Option<Vec<u8>>, DbError> {
        let result: Option<Vec<u8>> = self
            .conn
            .query_row("SELECT data FROM license_cache WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(result)
    }

    fn handle_store_license_cache(&self, data: Vec<u8>) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO license_cache (id, data, version) VALUES (1, ?1, ?2) \
             ON CONFLICT(id) DO UPDATE SET data = excluded.data, version = excluded.version",
            rusqlite::params![&data, Self::LICENSE_CACHE_VERSION.to_string()],
        )?;
        Ok(())
    }

    fn handle_get_listen_port(&self) -> Result<Option<ListenPort>, DbError> {
        let stored: Option<u32> = self
            .conn
            .query_row("SELECT port FROM listen_port WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        stored
            .map(|port| {
                ListenPort::try_from(port).map_err(|e| DbError::DataConversion {
                    message: e.to_string(),
                })
            })
            .transpose()
    }

    fn handle_store_listen_port(&self, port: ListenPort) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO listen_port (id, port) VALUES (1, ?1) \
             ON CONFLICT(id) DO UPDATE SET port = excluded.port",
            [port.get()],
        )?;
        Ok(())
    }

    fn handle_get_sync_doc(&self, kind: DocKind) -> Result<Vec<u8>, DbError> {
        let existing: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT data FROM sync_docs WHERE doc_kind = ?",
                [kind.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(blob) = existing {
            return Ok(blob);
        }
        // No document yet (fresh install): create one from the shared deterministic
        // genesis and persist it, so every device's document has the same root.
        let blob = sync::empty_blob(kind).map_err(|e| DbError::Validation {
            message: e.to_string(),
        })?;
        self.conn.execute(
            "INSERT OR IGNORE INTO sync_docs (doc_kind, data) VALUES (?, ?)",
            rusqlite::params![kind.as_str(), &blob],
        )?;
        Ok(blob)
    }

    fn handle_save_sync_doc(&self, kind: DocKind, blob: Vec<u8>) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO sync_docs (doc_kind, data) VALUES (?1, ?2) \
             ON CONFLICT(doc_kind) DO UPDATE SET data = excluded.data",
            rusqlite::params![kind.as_str(), &blob],
        )?;
        Ok(())
    }

    // Typed wrappers so a caller can't pair the wrong view type with a `DocKind`
    fn read_contacts_view(&self) -> Result<ContactsDoc, DbError> {
        self.read_sync_view(DocKind::Contacts)
    }

    fn mutate_contacts_view(&self, f: impl FnOnce(&mut ContactsDoc)) -> Result<(), DbError> {
        self.mutate_sync_view(DocKind::Contacts, f)
    }

    fn read_remote_fr_view(&self) -> Result<RemoteFrDoc, DbError> {
        self.read_sync_view(DocKind::RemoteFilerequests)
    }

    fn mutate_remote_fr_view(&self, f: impl FnOnce(&mut RemoteFrDoc)) -> Result<(), DbError> {
        self.mutate_sync_view(DocKind::RemoteFilerequests, f)
    }

    /// Resolve a remote filerequest's `contact_id` from the synced document
    fn remote_fr_contact_id(
        &self,
        target_filerequest_id: &str,
    ) -> Result<Option<ContactId>, DbError> {
        Ok(self
            .read_remote_fr_view()?
            .remote_filerequests
            .get(&FylesId(target_filerequest_id.to_string()))
            .map(|entry| entry.contact_id.clone()))
    }

    fn read_sync_view<T: autosurgeon::Hydrate>(&self, kind: DocKind) -> Result<T, DbError> {
        let blob = self.handle_get_sync_doc(kind)?;
        let doc = sync::load(&blob).map_err(sync_db_err)?;
        sync::hydrate(&doc).map_err(sync_db_err)
    }

    /// Reconcile into the loaded (genesis-descended) document, never a fresh one — that
    /// shared root is what makes devices converge on sync.
    fn mutate_sync_view<T, F, R>(&self, kind: DocKind, f: F) -> Result<R, DbError>
    where
        T: autosurgeon::Hydrate + autosurgeon::Reconcile,
        F: FnOnce(&mut T) -> R,
    {
        let blob = self.handle_get_sync_doc(kind)?;
        let mut doc = sync::load(&blob).map_err(sync_db_err)?;
        let mut view: T = sync::hydrate(&doc).map_err(sync_db_err)?;
        let outcome = f(&mut view);
        sync::reconcile(&mut doc, &view).map_err(sync_db_err)?;
        let new_blob = doc.save();
        self.handle_save_sync_doc(kind, new_blob)?;
        Ok(outcome)
    }

    fn handle_mutate_devices_doc(
        &self,
        f: Box<dyn for<'a> FnOnce(&'a mut DevicesDoc) -> DeviceChange + Send>,
    ) -> Result<DeviceChange, DbError> {
        self.mutate_sync_view(DocKind::Devices, f)
    }

    fn handle_merge_sync_doc(
        &self,
        kind: DocKind,
        mut incoming: Box<AutoCommit>,
    ) -> Result<(), DbError> {
        let blob = self.handle_get_sync_doc(kind)?;
        let mut stored = sync::load(&blob).map_err(sync_db_err)?;
        sync::merge(&mut stored, &mut incoming).map_err(sync_db_err)?;
        self.handle_save_sync_doc(kind, stored.save())
    }
}

/// Map a `core::sync` document error into a `DbError`.
fn sync_db_err(e: sync::SyncDocError) -> DbError {
    DbError::DataConversion {
        message: format!("sync document error: {e}"),
    }
}

/// Build a domain `RemoteFilerequest` from a synced document entry.
fn remote_fr_from_entry(id: FylesId, entry: &RemoteFrEntry) -> RemoteFilerequest {
    RemoteFilerequest {
        id,
        peer_id: entry.peer_id_hint.clone(),
        filerequest_id: entry.filerequest_id.clone(),
        contact_id: entry.contact_id.clone(),
        name: entry.name.clone(),
    }
}
