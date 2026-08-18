use std::collections::HashMap;

use crypto::ContactKeys;
use tap::Pipe;
use tracing::{Instrument, Span, debug, error, info, instrument, trace, warn};

use crate::core::brain::GigaBrain;
use crate::core::brain::action::BrainAction;
use crate::core::brain::action_p2p::NodeInfo;
#[cfg(any(test, feature = "test-support"))]
use crate::core::brain::action_test::TestAction;
use crate::core::brain::stale_cleanup;
use crate::core::brain::types::CreationResult;
use crate::core::brain::types::{ContactShareChallenge, SelfContactInviteChallenge};
use crate::core::db::DbError;
use crate::core::domain_models::{
    ContactId, FilerequestLocation, FylesId, InProgressSendStatus, PeerIdWrapper, ReceiveStatus,
    ReceivedFile, SelfContact, SelfContactInvite, SendStatus,
};
use crate::core::filerequest_drive_handler::{FilerequestDriveHandler, RenameFilerequestError};
use crate::core::notification::{FileSender, Notification};
use crate::core::p2p::FileToSend;
use crate::core::sync::LocalDocWrite;
use crate::io_controller::{FileNamespace, OwnershipOutcome};
use crate::library::util::error_handling::AutoMapError;
use crate::library::util::util::{TimeoutLock, generate_byte_challenge};
use crate::library::wire::api::ServerMessage;

use super::action_client::ClientAction;
use super::action_p2p::{NetworkNodeAction, PubkeyFetchReceiveRequest, SyncReceiveRequest};
use super::error::FilerequestError;
use super::types::FilerequestResult;
use crate::core::sync::{
    self, DocKind, SyncId, add_or_update_owned_device, is_owned_device, list_owned_devices,
    remove_owned_device, sync_doc_err,
};

pub(crate) struct SyncMessage {
    pub(crate) sync_id: SyncId,
    pub(crate) message: Option<Vec<u8>>,
}

impl GigaBrain {
    #[instrument(skip(self), level = "trace")]
    pub(super) async fn handle_action(&self, action: BrainAction) {
        #[cfg(any(test, feature = "test-support"))]
        let action = {
            use tracing::debug;

            use crate::library::util::util::TimeoutLock;
            let mut interceptor_guard = self.action_interceptor.timeout_lock().await;
            if let Some(interceptor) = &mut *interceptor_guard {
                if let Err(action) = interceptor.action_sender.send(action).await {
                    debug!(
                        "Action interceptor receiver dropped when trying to send, stopping intercepting"
                    );
                    *interceptor_guard = None;
                    action.0
                } else {
                    match interceptor.action_receiver.recv().await {
                        None => {
                            debug!(
                                "Action interceptor receiver dropped when trying to receive, stopping intercepting"
                            );
                            *interceptor_guard = None;
                            return;
                        }
                        Some(Some(action)) => action,
                        Some(None) => return,
                    }
                }
            } else {
                action
            }
        };
        match action {
            BrainAction::Client(action) => self.handle_client_action(action).await,
            BrainAction::NetworkNode(action) => self.handle_p2p_action(action).await,
            #[cfg(any(test, feature = "test-support"))]
            BrainAction::Test(action) => match action {
                TestAction::RegisterActionInterceptor(request) => {
                    let mut in_place_interceptor = self.action_interceptor.timeout_lock().await;
                    if in_place_interceptor.is_some() {
                        panic!("Action interceptor already set, cannot set a new one");
                    }
                    debug!("Starting to have actions intercepted");
                    *in_place_interceptor = Some(request.request);
                    if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                        sender.send(()).unwrap();
                    }
                }
            },
        }
    }

    // #[instrument(skip_all, fields(action = ?action))]
    #[instrument(skip_all)]
    async fn handle_client_action(&self, action: ClientAction) {
        match action {
            ClientAction::CreateFilerequest(create_filerequest) => {
                // Filesystem first, then DB: generate the id, atomically create
                // and claim the directory, and only persist the row once that
                // succeeds. Nothing to roll back.
                let id = FylesId::new();
                let title = create_filerequest.request.title.clone();
                let dir_ns = || FileNamespace::Namespace {
                    namespace: title.clone(),
                    child: None,
                };

                let persist = async {
                    self.db
                        .create_filerequest(&id, &create_filerequest.request)
                        .await
                        .auto_map_err()
                        .map(|()| id.clone())
                };

                let reply: CreationResult = match self
                    .host_controller
                    .ensure_exists_and_is_owned(dir_ns(), &id.0, false)
                    .await
                {
                    Ok(OwnershipOutcome::Owned) | Ok(OwnershipOutcome::Adopted) => persist.await,
                    Ok(OwnershipOutcome::Conflict { other_id }) => {
                        // Look the owner up only to decide ghost-vs-live
                        match self.db.get_filerequest(&FylesId(other_id.clone())).await {
                            // Live owner: hard block, no row created.
                            Ok(_) => Err(FilerequestError::FolderClaimed {
                                folder: title.clone(),
                            }),
                            // Ghost marker (owner row is gone): reclaim, then persist.
                            // Only a genuine "not found" justifies reclaiming — any
                            // other DB error must surface, not silently steal the dir.
                            Err(DbError::NotFound { .. }) => {
                                info!("Reclaiming directory '{title}' from stale owner {other_id}");
                                match self
                                    .host_controller
                                    .ensure_exists_and_is_owned(dir_ns(), &id.0, true)
                                    .await
                                {
                                    Ok(_) => persist.await,
                                    Err(e) => Err(FilerequestError::HostError { source: e }),
                                }
                            }
                            // A real DB error looking up the owner: surface it.
                            Err(e) => Err(FilerequestError::DatabaseError(e)),
                        }
                    }
                    Err(e) => Err(FilerequestError::HostError { source: e }),
                };

                if let Some(sender) = create_filerequest
                    .response_sender
                    .timeout_lock()
                    .await
                    .take()
                {
                    let _ = sender.send(reply);
                }
            }
            ClientAction::UpdateFilerequest(update_filerequest) => {
                let filerequest = match self
                    .db
                    .get_filerequest(&update_filerequest.request.id)
                    .await
                {
                    Err(e) => {
                        if let Some(sender) = update_filerequest
                            .response_sender
                            .timeout_lock()
                            .await
                            .take()
                        {
                            let _ = sender.send(Err(e.into()));
                        }
                        return;
                    }
                    Ok(filerequest) => filerequest,
                };
                if filerequest.title != update_filerequest.request.title {
                    let new_title = update_filerequest.request.title.clone();

                    let mut filerequest_handler =
                        FilerequestDriveHandler::new(self.host_controller.clone(), filerequest);
                    // The rename atomically refuses to overwrite an existing
                    // directory and reports it as `DestinationOccupied` — the
                    // same conflict path creation uses.
                    match filerequest_handler
                        .rename_filerquest(&update_filerequest.request.title)
                        .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            let err = match e {
                                RenameFilerequestError::DestinationOccupied => {
                                    FilerequestError::FolderClaimed { folder: new_title }
                                }
                                RenameFilerequestError::Failed => FilerequestError::GenericError {
                                    msg: "Unable to rename Filerequest because directory cannot be renamed".into(),
                                    source: None,
                                },
                            };
                            if let Some(sender) = update_filerequest
                                .response_sender
                                .timeout_lock()
                                .await
                                .take()
                            {
                                let _ = sender.send(Err(err));
                            }
                            return;
                        }
                    }
                }
                let result = self
                    .db
                    .update_filerequest(&update_filerequest.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = update_filerequest
                    .response_sender
                    .timeout_lock()
                    .await
                    .take()
                {
                    let _ = sender.send(result);
                }
            }
            ClientAction::DeleteFilerequest(delete_filerequest) => {
                // Filesystem first, then DB. Remove the directory (idempotent —
                // an already-missing directory counts as success) and only delete
                // the row once that succeeds. Nothing to roll back.
                let result = match self.db.get_filerequest(&delete_filerequest.request).await {
                    Ok(fr) => {
                        match self
                            .host_controller
                            .remove_filerequest_resource(FileNamespace::Namespace {
                                namespace: fr.title,
                                child: None,
                            })
                            .await
                        {
                            Ok(()) => self
                                .db
                                .delete_filerequest(&delete_filerequest.request)
                                .await
                                .auto_map_err(),
                            Err(e) => {
                                error!("Failed to remove filerequest directory on delete: {e:?}");
                                Err(FilerequestError::HostError { source: e })
                            }
                        }
                    }
                    // No such row: deleting it is a no-op success.
                    Err(_) => Ok(()),
                };
                if let Some(sender) = delete_filerequest
                    .response_sender
                    .timeout_lock()
                    .await
                    .take()
                {
                    let _ = sender.send(result);
                }
            }
            ClientAction::ReadFilerequest(read_filerequest) => {
                let result = self
                    .db
                    .get_filerequest(&read_filerequest.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = read_filerequest.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::ListFilerequests(list_filerequests) => {
                let result = self.db.get_filerequests().await.auto_map_err();
                if let Some(sender) = list_filerequests
                    .response_sender
                    .timeout_lock()
                    .await
                    .take()
                {
                    let _ = sender.send(result);
                }
            }

            // Contact operations
            ClientAction::GetContactName(get_contact_name) => {
                let result = self
                    .db
                    .get_contact_name(&get_contact_name.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = get_contact_name.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetContactNames(get_contact_names) => {
                let result = self
                    .db
                    .get_contact_names(&get_contact_names.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = get_contact_names
                    .response_sender
                    .timeout_lock()
                    .await
                    .take()
                {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetContact(get_contact) => {
                let result = self
                    .db
                    .get_contact(&get_contact.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = get_contact.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::ListContacts(list_contacts) => {
                let result = self.db.get_contacts().await.auto_map_err();
                if let Some(sender) = list_contacts.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::UpdateContact(update_contact) => {
                let result = self.db.update_contact(&update_contact.request).await;
                match result {
                    Ok(write) => {
                        self.announce(write);
                        if let Some(sender) =
                            update_contact.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) =
                            update_contact.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Err(e).auto_map_err());
                        }
                    }
                }
            }
            ClientAction::DeleteContact(delete_contact) => {
                match self.db.delete_contact(&delete_contact.request).await {
                    Ok(write) => {
                        self.announce(write);
                        if let Some(sender) =
                            delete_contact.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) =
                            delete_contact.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Err(e).auto_map_err());
                        }
                    }
                }
            }
            ClientAction::Shutdown(request) => {
                // Answer before shutting down: the wait would outlive the response channel.
                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(Ok(true));
                }
                self.shutdown_workloads().await;
            }
            ClientAction::ClientHeartbeat { foreground } => {
                self.observe_client_heartbeat(foreground).await;
            }
            ClientAction::GetNodeStatus(request) => {
                let result = self
                    .network
                    .read()
                    .await
                    .get_node_info()
                    .await
                    .auto_map_err();
                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::CreateRemoteFilerequest(req) => {
                match self.db.create_remote_filerequest(&req.request).await {
                    Ok((id, write)) => {
                        self.announce(write);
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Ok(id));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(e).auto_map_err());
                        }
                    }
                }
            }
            ClientAction::GetRemoteFilerequest(req) => {
                let result = self
                    .db
                    .get_remote_filerequest(&req.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetRemoteFilerequestsByContact(req) => {
                let result = self
                    .db
                    .get_remote_filerequests_by_contact(&req.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetAllRemoteFilerequests(req) => {
                let result = self.db.get_all_remote_filerequests().await.auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::DeleteRemoteFilerequest(req) => {
                let filerequest = match self.db.get_remote_filerequest(&req.request).await {
                    Err(e) => {
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(e.into()));
                        }
                        return;
                    }
                    Ok(it) => it,
                };
                let result: Result<bool, FilerequestError> = self
                    .network
                    .read()
                    .await
                    .cancel_files_for_remote_filerequest(
                        filerequest.filerequest_id,
                        filerequest.peer_id,
                    )
                    .await
                    .auto_map_err();
                let result: Result<_, FilerequestError> = match result {
                    Err(e) => Err(e),
                    Ok(deleted_request) => {
                        debug!(
                            "Cancelled files before deleting remote filerequest: {deleted_request}"
                        );
                        match self.db.delete_remote_filerequest(&req.request).await {
                            Ok(write) => {
                                self.announce(write);
                                Ok(true)
                            }
                            Err(e) => Err(e).auto_map_err(),
                        }
                    }
                };
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::UpdateRemoteFilerequest(req) => {
                match self
                    .db
                    .update_remote_filerequest(req.request.0, req.request.1)
                    .await
                {
                    Ok(write) => {
                        self.announce(write);
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(e).auto_map_err());
                        }
                    }
                }
            }
            ClientAction::CreatePendingFiles(req) => {
                // correlate with other entities
                let filerequest = match self
                    .db
                    .get_remote_filerequest(&req.request.target_filerequest_id)
                    .await
                    .inspect_err(|e| {
                        warn!("Failed to get remote filerequest: {:?}", e);
                    }) {
                    Ok(it) => it,
                    Err(e) => {
                        warn!("Failed to get remote filerequest: {:?}", e);
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(FilerequestError::InputError(format!(
                                "No filerequest known with id {}",
                                &req.request.target_filerequest_id
                            ))));
                        }
                        return;
                    }
                };

                // store files in db, get ids
                let pending_file_id_res = self
                    .db
                    .create_pending_files(&req.request)
                    .await
                    .auto_map_err();
                // send response to client

                debug!("Created pending files with IDs {pending_file_id_res:?}");

                if let Ok(pending_file_ids) = pending_file_id_res.as_ref() {
                    let file_paths = req.request.file_infos.clone();
                    let db_ids_and_file_paths = pending_file_ids.iter().zip(file_paths.iter());
                    // let peer = db.get_peer(&peer_id).await.expect("Peer to be found");
                    let filerequest_id = filerequest.filerequest_id;
                    let files_to_send = db_ids_and_file_paths
                        .map(|(id, file_info)| FileToSend {
                            id: id.clone(),
                            contact_id: filerequest.contact_id.clone(),
                            peer_id: filerequest.peer_id.clone(),
                            filerequest_id: filerequest_id.clone(),
                            file_path: file_info.path.clone(),
                            retry_count: 0,
                            status: SendStatus::Pending,
                        })
                        .collect::<Vec<_>>();
                    self.network
                        .read()
                        .await
                        .send_files(files_to_send)
                        .await
                        .expect("File to be sendable");
                }

                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(pending_file_id_res);
                }
            }
            ClientAction::GetPendingFile(req) => {
                let result = self.db.get_pending_file(&req.request).await.auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetPendingFiles(req) => {
                let result = self.db.get_pending_files(&req.request).await.auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetAllPendingFiles(req) => {
                let result = self.db.get_all_pending_files().await.auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::DeletePendingFile(req) => {
                debug!("Delete pending file {:?}", req.request);
                // First check if file is still pending in DB
                let pending_file = match self.db.get_pending_file(&req.request).await {
                    Ok(file) => file,
                    Err(e) => {
                        let result = Err(e).auto_map_err();
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(result);
                        }
                        return;
                    }
                };

                match pending_file.status {
                    SendStatus::Pending | SendStatus::InProgress(_) => {
                        // File is still being processed, must cancel in p2p node first
                        match self
                            .network
                            .read()
                            .await
                            .cancel_file(req.request.clone())
                            .await
                        {
                            Ok(true) => {
                                // Successfully cancelled in p2p node, now safe to delete from DB
                                let result = self
                                    .db
                                    .delete_pending_file(&req.request)
                                    .await
                                    .auto_map_err();
                                if let Some(sender) =
                                    req.response_sender.timeout_lock().await.take()
                                {
                                    let _ = sender.send(result);
                                }
                            }
                            Ok(false) => {
                                error!("File exists in DB but not in P2P node");
                                // P2P node couldn't find the file to cancel - this is an error state
                                let result = Err(DbError::Validation {
                                    message: "File exists in DB but not in P2P node".into(),
                                })
                                .auto_map_err();
                                // We'll delete it anyway but report the error
                                let _ = self.db.delete_pending_file(&req.request).await;

                                if let Some(sender) =
                                    req.response_sender.timeout_lock().await.take()
                                {
                                    let _ = sender.send(result);
                                }
                            }
                            Err(e) => {
                                // P2P cancellation failed, cannot proceed with DB deletion
                                let result = Err(DbError::Validation {
                                    message: format!("Failed to cancel file transfer: {}", e),
                                })
                                .auto_map_err();
                                if let Some(sender) =
                                    req.response_sender.timeout_lock().await.take()
                                {
                                    let _ = sender.send(result);
                                }
                            }
                        }
                    }
                    _ => {
                        // File is in a terminal state (Sent/Failed/Rejected), safe to delete from DB
                        let result = self
                            .db
                            .delete_pending_file(&req.request)
                            .await
                            .auto_map_err();
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(result);
                        }
                    }
                }
            }
            ClientAction::GetNodePeerId(req) => {
                // Extract peer ID from node keys
                let send_result = self.db.get_node_keys().await;
                match send_result {
                    Ok(keys) => {
                        let peer_id = self
                            .network
                            .read()
                            .await
                            .display_keypair(&keys.node_key_pair)
                            .map_err(|e| FilerequestError::GenericError {
                                msg: "Failed to extract peer Id".into(),
                                source: Some(e),
                            });
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(peer_id);
                        }
                    }
                    Err(e) => {
                        let result = Err(e).auto_map_err();
                        if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(result);
                        }
                    }
                }
            }
            ClientAction::ListReceivedFilesForRequest(req) => {
                let result = self
                    .db
                    .list_received_files(&req.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::DeleteReceivedFile(req) => {
                debug!("Deleting received file with ID {:?}", req.request);
                let result = self
                    .db
                    .delete_received_file(&req.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::ResolveFilerequestLocation(req) => {
                // Resolve the sanitized on-disk folder name for a filerequest,
                // reusing the same logic that created the directory (incl. the
                // empty-title -> id fallback) so the result matches disk exactly.
                let result = match self.db.get_filerequest(&req.request).await {
                    Ok(filerequest) => {
                        let handler =
                            FilerequestDriveHandler::new(self.host_controller.clone(), filerequest);
                        let relative = handler.safe_dir_name().to_string();
                        // Either-or: on a directly-accessible filesystem (desktop)
                        // hand the UI the absolute path, so it need not know where
                        // the daemon was pointed; otherwise the relative path for
                        // the client to compose (scoped-storage hosts).
                        let location = match self.host_controller.drop_dir_absolute_path() {
                            Some(base) => FilerequestLocation::Absolute(
                                std::path::Path::new(&base)
                                    .join(&relative)
                                    .to_string_lossy()
                                    .into_owned(),
                            ),
                            None => FilerequestLocation::Relative(relative),
                        };
                        Ok(location)
                    }
                    Err(e) => Err(e.into()),
                };
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::UpdateSelfContactName(req) => {
                let result = self
                    .db
                    .update_self_contact_name(req.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = req.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::WaitForReady(brain_request) => {
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    // If we are not ready yet, queue the request
                    // to be answered when the node is ready
                    match self
                        .ephemeral_data
                        .timeout_lock()
                        .await
                        .queued_wait_for_ready_requests
                    {
                        Some(ref mut q) => {
                            let _dropping_causes_expected_error = q.push(sender);
                        }
                        None => {
                            let sucessfully_started = self
                                .ephemeral_data
                                .timeout_lock()
                                .await
                                .successfully_started;
                            if let Some(sender) =
                                brain_request.response_sender.timeout_lock().await.take()
                            {
                                let _ = sender.send(sucessfully_started);
                            }
                        }
                    };
                }
            }
            ClientAction::GetFullSelfContact(brain_request) => {
                let invite = self.build_self_contact_invite().await;
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(invite);
                }
            }
            ClientAction::UpdateIdentity(brain_request) => {
                let SelfContactInvite {
                    contact,
                    inviter_peer_id,
                    inviter_device_name,
                } = brain_request.request;
                let result = self
                    .adopt_shared_identity(contact, inviter_peer_id, Some(inviter_device_name))
                    .await
                    .auto_map_err();
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::GetSelfContactDisplay(brain_request) => {
                let self_contact = self.db.get_self_contact_for_display().await;
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(self_contact.auto_map_err());
                }
            }
            ClientAction::SharePublicSelfContact(brain_request) => {
                let result = self
                    .db
                    .get_sharable_public_self_contact()
                    .await
                    .auto_map_err();
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::RegisterContact(brain_request) => {
                match self
                    .db
                    .update_contact_projection(brain_request.request)
                    .await
                {
                    Ok(write) => {
                        self.announce(write);
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Err(e).auto_map_err());
                        }
                    }
                }
            }
            ClientAction::RegisterSelfContactInviteChallenge(brain_request) => {
                let challenge: SelfContactInviteChallenge = generate_byte_challenge().into();
                self.ephemeral_data
                    .timeout_lock()
                    .await
                    .self_contact_invite_challenges
                    .insert(challenge.clone());
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(challenge);
                }
            }
            ClientAction::UnregisterSelfContactInviteChallenge(brain_request) => {
                self.ephemeral_data
                    .timeout_lock()
                    .await
                    .self_contact_invite_challenges
                    .remove(&brain_request.request);
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(());
                }
            }
            ClientAction::UseSelfContactInviteChallenge(brain_request) => {
                let (challenge, peer_id_wrapper) = brain_request.request;
                self.network
                    .read()
                    .await
                    .use_self_contact_invite(challenge, peer_id_wrapper)
                    .await;
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(());
                }
            }
            ClientAction::RenameThisDevice(brain_request) => {
                let name = brain_request.request;
                // We may only rename our *own* device entry; resolve our own peer id.
                let result: FilerequestResult<LocalDocWrite> = async {
                    let info = self.network.read().await.get_node_info().await?;
                    Ok(add_or_update_owned_device(self.db.as_ref(), info.peer_id, name).await?)
                }
                .await;
                match result {
                    Ok(write) => {
                        self.announce(write);
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Err(e));
                        }
                    }
                }
            }
            ClientAction::RemoveOwnedDevice(brain_request) => {
                match remove_owned_device(self.db.as_ref(), &brain_request.request).await {
                    Ok(write) => {
                        self.announce(write);
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(Err(e).auto_map_err());
                        }
                    }
                }
            }
            ClientAction::ListOwnedDevices(brain_request) => {
                let result = list_owned_devices(self.db.as_ref()).await.auto_map_err();
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            ClientAction::RegisterContactShareChallenge(brain_request) => {
                let challenge: ContactShareChallenge = generate_byte_challenge().into();
                self.ephemeral_data
                    .timeout_lock()
                    .await
                    .contact_share_challenges
                    .insert(challenge.clone());
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(challenge);
                }
            }
            ClientAction::UnregisterContactShareChallenge(brain_request) => {
                self.ephemeral_data
                    .timeout_lock()
                    .await
                    .contact_share_challenges
                    .remove(&brain_request.request);
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(());
                }
            }
            ClientAction::UseContactShareChallenge(brain_request) => {
                let (challenge, peer_id_wrapper) = brain_request.request;
                self.network
                    .read()
                    .await
                    .use_contact_share_challenge(challenge, peer_id_wrapper)
                    .await;
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(());
                }
            }
            ClientAction::BackupData(request) => {
                let backup_dir = match self.host_controller.get_path_to_db_backup_file().await {
                    Ok(path) => path,
                    Err(e) => {
                        error!("Could not obtain backup drop location from host: {e:?}");
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(FilerequestError::HostError { source: e }));
                        }
                        return;
                    }
                };

                match self
                    .db
                    .backup_database(backup_dir, self.internal_data_dir.clone())
                    .await
                {
                    Err(e) => {
                        error!("Error during database backup: {e:?}");
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(e.into()));
                        }
                        return;
                    }
                    Ok(()) => {
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Ok(()));
                        }
                    }
                };
            }
            ClientAction::RestoreData(request) => {
                let backup_path = request.request;
                let backup_file = match self
                    .host_controller
                    .access_file_for_reading(backup_path)
                    .await
                {
                    Err(e) => {
                        error!("Could not access backup file for reading: {e:?}");
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(FilerequestError::HostError { source: e }));
                        }
                        return;
                    }
                    Ok(f) => f,
                };
                match self.db.restore_database(backup_file.file).await {
                    Err(e) => {
                        error!("Error during database restore: {e:?}");
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Err(e.into()));
                        }
                    }
                    Ok(()) => {
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Ok(()));
                        }
                        // The restored DB may reference directories that don't
                        // exist (or aren't claimed) on this device — re-establish
                        // ownership idempotently.
                        stale_cleanup::reconcile_filerequest_dirs(
                            self.db.clone(),
                            self.host_controller.clone(),
                        )
                        .await;
                        self.client_push_sender
                            .send(ServerMessage::database_restored())
                            .await
                            .expect("Sending to work");
                    }
                };
            }
            ClientAction::Settings(request) => {
                // Opaque relay: forward the serialized request to the network node and
                // return its serialized response. The network node interprets the bytes
                // and owns any persistence (via NetworkNodeAction::PersistSettings).
                let result = self
                    .network
                    .read()
                    .await
                    .process_settings_request(&request.request)
                    .await
                    .auto_map_err();
                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
        }
    }

    // #[instrument(skip_all, fields(action = ?action))]
    #[instrument(skip_all)]
    async fn handle_p2p_action(&self, action: NetworkNodeAction) {
        match action {
            NetworkNodeAction::RequestFileDrop(request_file_drop, span) => {
                Span::current().follows_from(span.clone());
                async move {
                    match self.db
                        .get_filerequest(&request_file_drop.request.filerequest_id)
                        .in_current_span()
                        .await
                    {
                        Ok(filerequest) => {
                            if !filerequest.is_active {
                                warn!("Filerequest is not active");
                                if let Some(sender) =
                                    request_file_drop.response_sender.timeout_lock().await.take()
                                {
                                    let _ = sender.send(None);
                                }
                            } else {
                                let is_accessible = filerequest
                                    .access
                                    .is_accessible_by(request_file_drop.request.contact_id.clone()).pipe(async |is_accessible| {
                                        if !is_accessible
                                            && let Some(ref maybe_me) = request_file_drop.request.contact_id {
                                                let is_me = self.db.get_self_contact().await.ok().map(|sc| sc.id == *maybe_me).unwrap_or(false);
                                                if is_me {
                                                    trace!("Filerequest is accessible because contact is self");
                                                    return true;
                                                }
                                            }
                                        is_accessible
                                    }).await;
                                if !is_accessible {
                                    warn!(
                                        "RequestFileDrop is not accessible by: {:?}",
                                        request_file_drop.request.contact_id
                                    );
                                }
                                if let Some(sender) =
                                    request_file_drop.response_sender.timeout_lock().await.take()
                                {
                                    let handler = is_accessible.then(|| {
                                        FilerequestDriveHandler::new(
                                            self.host_controller.clone(),
                                            filerequest,
                                        )
                                    });
                                    let _ = sender.send(handler);
                                };
                            };
                        }
                        Err(_) => {
                            error!(
                                "Filerequest {} not found",
                                request_file_drop.request.filerequest_id
                            );
                            if let Some(sender) =
                                request_file_drop.response_sender.timeout_lock().await.take()
                            {
                                let _ = sender.send(None);
                            }
                        }
                    }
                }
                .instrument(span)
                .await;
            }
            NetworkNodeAction::RequestFileTransferContinuation(request) => {
                let transfer_id = &request.request.transfer_id;
                // Look up the persisted received file to recover file_name and file_size_bytes
                let received_file = self.db.get_received_file_by_transfer_id(transfer_id).await;
                let filerequest_result = self
                    .db
                    .get_filerequest(&request.request.filerequest_id)
                    .await;
                let response = match (filerequest_result, received_file) {
                    (Ok(filerequest), Ok(rf)) => {
                        if !filerequest.is_active {
                            warn!("Filerequest is not active for transfer continuation");
                            None
                        } else if !filerequest
                            .access
                            .is_accessible_by(request.request.contact_id.clone())
                        {
                            warn!(
                                "Transfer continuation not accessible by: {:?}",
                                request.request.contact_id
                            );
                            None
                        } else if rf.peer_id != request.request.peer_id {
                            warn!(
                                "Peer {} tried to continue transfer owned by peer {}",
                                request.request.peer_id, rf.peer_id
                            );
                            None
                        } else if rf.contact_id != request.request.contact_id {
                            warn!(
                                "Peer {} tried to continue transfer with mismatched contact_id (expected {:?}, got {:?})",
                                request.request.peer_id, rf.contact_id, request.request.contact_id
                            );
                            None
                        } else {
                            let handler = FilerequestDriveHandler::new(
                                self.host_controller.clone(),
                                filerequest,
                            );
                            Some(super::action_p2p::FilerequestContinueResponse {
                                drive_handler: handler,
                                file_name: rf.file_name,
                                file_size_bytes: rf.file_size_bytes,
                                started_at_ms: rf.started_at_ms,
                            })
                        }
                    }
                    (Err(e), _) => {
                        error!(
                            "Filerequest {} not found for transfer continuation: {e:?}",
                            request.request.filerequest_id
                        );
                        None
                    }
                    (_, Err(e)) => {
                        warn!(
                            "Received file with transfer_id {transfer_id} not found in DB (stale .part file?): {e:?}"
                        );
                        None
                    }
                };
                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(response);
                }
            }
            NetworkNodeAction::GetNodeInfo(request) => {
                let result = self.db.get_node_keys().await;
                let persisted_settings = self.db.get_settings().await.unwrap_or_else(|e| {
                    warn!("Failed to read persisted settings during GetNodeInfo: {e}");
                    Vec::new()
                });

                match result {
                    Ok(node_info) => {
                        if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                            let _ = sender.send(Ok((node_info, persisted_settings)));
                        }
                    }
                    Err(db_err) => match db_err {
                        DbError::DataNotYetInitialized => {
                            let node_info = NodeInfo {
                                node_key_pair: self
                                    .network
                                    .read()
                                    .await
                                    .generate_keypair()
                                    .expect("Key generation failed. Cancel execution."),
                                self_contact_id: ContactId::new(),
                                self_contact_keys: ContactKeys::new(),
                            };
                            self.db.store_node_keys(node_info.clone()).await.expect("Storing node keys failed. This is essential for operation. Aborting.");
                            if let Some(sender) =
                                request.response_sender.timeout_lock().await.take()
                            {
                                let _ = sender.send(Ok((node_info, persisted_settings)));
                            }
                        }
                        DbError::SigningKeysMissing => {
                            // The node keypair (PeerId) and contact id exist but the signing keys
                            // are absent; regenerate them in place, keeping the other identity parts.
                            let (node_key_pair, self_contact_id) = self
                                .db
                                .get_node_and_self_contact_id()
                                .await
                                .expect("Node keypair and contact id must be readable when signing keys are missing. Aborting.");
                            let self_contact_keys = ContactKeys::new();
                            self.db
                                .store_signing_keys(self_contact_keys.clone())
                                .await
                                .expect("Storing regenerated signing keys failed. This is essential for operation. Aborting.");
                            let node_info = NodeInfo {
                                node_key_pair,
                                self_contact_id,
                                self_contact_keys,
                            };
                            if let Some(sender) =
                                request.response_sender.timeout_lock().await.take()
                            {
                                let _ = sender.send(Ok((node_info, persisted_settings)));
                            }
                        }
                        e => {
                            panic!(
                                "Problem querying cryptographic identity of the node. Aborting: {e:?}"
                            );
                        }
                    },
                };
            }
            NetworkNodeAction::PersistSettings(request) => {
                // Store-only: the network node owns settings persistence and asks us to
                // durably store an opaque blob. Awaitable so the network node knows the
                // write landed before reporting success.
                let result = self
                    .db
                    .store_settings(request.request.clone())
                    .await
                    .auto_map_err();
                if let Err(e) = &result {
                    error!("Failed to persist settings: {e}");
                }
                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            NetworkNodeAction::Ready => {
                let remote_file_requests_list = self
                    .db
                    .get_all_remote_filerequests()
                    .await
                    .expect("Remote file requests to be queriable");

                let remote_file_requests = remote_file_requests_list
                    .iter()
                    .map(|filerequest| (filerequest.id.clone(), filerequest))
                    .collect::<HashMap<_, _>>();

                let mut pending_files_and_filerequest_result = self
                    .db
                    .get_all_pending_files()
                    .await
                    .expect("Pending files to be queriable")
                    .into_iter()
                    .filter(|file| {
                        let keep =
                            matches!(file.status, SendStatus::Pending | SendStatus::InProgress(_));
                        if keep {
                            trace!(?file, "Kept file known at startup as it was in a continuable state");
                        } else {
                            trace!(?file, "Dropped file known at startup, as it was not in a continuable state");
                        }
                        keep
                    })
                    .filter_map(|file| {
                        let option = remote_file_requests.get(&file.target_filerequest_id);
                        if option.is_none() {
                            warn!("Pending file {} has no remote filerequest", file.id);
                        }
                        option.map(|remote_fr| (file.clone(), remote_fr))
                    })
                    .collect::<Vec<_>>();

                for (file, _) in &mut pending_files_and_filerequest_result {
                    if let SendStatus::InProgress(ref mut in_progress) = file.status {
                        match in_progress.status {
                            InProgressSendStatus::Prepared => {}
                            InProgressSendStatus::Sending => {
                                warn!(
                                    "File with ID {} is in 'Sending' status on startup, resetting to 'Interrupted'",
                                    file.id
                                );
                                in_progress.status = InProgressSendStatus::Interrupted;
                                self.db
                                    .handle_update_pending_file_status(&file.id, &file.status, None)
                                    .await
                                    .expect("Pending file status to be updatable");
                            }
                            InProgressSendStatus::Interrupted => {}
                            InProgressSendStatus::PendingSent => {}
                        }
                    }
                }

                let files_to_send = pending_files_and_filerequest_result
                    .into_iter()
                    .map(|(pending_file, remote_fr)| FileToSend {
                        id: pending_file.id.clone(),
                        peer_id: remote_fr.peer_id.clone(),
                        filerequest_id: remote_fr.filerequest_id.clone(),
                        file_path: pending_file.file_path.clone(),
                        contact_id: remote_fr.contact_id.clone(),
                        retry_count: pending_file.retry_count,
                        status: pending_file.status,
                    })
                    .collect::<Vec<_>>();

                let mut ephemeral_data = self.ephemeral().await;
                ephemeral_data.successfully_started = true;
                if let Some(senders) = ephemeral_data.queued_wait_for_ready_requests.take() {
                    senders.into_iter().for_each(|sender| {
                        let _ = sender.send(true);
                    });
                }

                debug!(?files_to_send, "Initial files to send");

                match self
                    .network
                    .read()
                    .await
                    .initial_files_to_send(files_to_send)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Failed to send initial files to P2P node: {:?}", e);
                    }
                };

                // Ensure this device appears in the synced owned-device list.
                self.ensure_own_device_registered().await;
            }
            NetworkNodeAction::FileSent { pending_file_id } => {
                info!(?pending_file_id, "File sent");
                let status = SendStatus::Sent;
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&pending_file_id, &status, None)
                    .await
                    .expect("Pending file status to be updated");
                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &pending_file_id,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
                self.try_cleanup_source_file(&pending_file_id).await;
            }
            NetworkNodeAction::FileSending {
                pending_file_id,
                status,
            } => {
                trace!("File sending");
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&pending_file_id, &status, None)
                    .await
                    .expect("Pending file status to be updated");
                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &pending_file_id,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
            }
            NetworkNodeAction::FileSendReset {
                pending_file_id,
                status,
                retry_count,
                reason,
            } => {
                warn!(
                    ?pending_file_id,
                    ?retry_count,
                    ?reason,
                    "File send is being reset"
                );
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&pending_file_id, &status, retry_count)
                    .await
                    .expect("Pending file status to be updated");

                if let Some(r) = reason {
                    let _ = self.db.add_interruption_reason(&pending_file_id, r).await;
                }

                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &pending_file_id,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
            }
            NetworkNodeAction::FileRejected { pending_file_id } => {
                warn!(
                    "File transfer rejected by peer. Pending file ID: {:?}",
                    pending_file_id
                );
                let status = SendStatus::Rejected;
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&pending_file_id, &status, None)
                    .await
                    .expect("Pending file status to be updated");
                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &pending_file_id,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
                self.try_cleanup_source_file(&pending_file_id).await;
            }
            NetworkNodeAction::FileFailed { pending_file_id } => {
                error!(
                    "File transfer failed for pending file ID: {:?}",
                    pending_file_id
                );
                let status = SendStatus::Failed;
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&pending_file_id, &status, None)
                    .await
                    .expect("Pending file status to be updated");
                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &pending_file_id,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
                self.try_cleanup_source_file(&pending_file_id).await;
            }
            NetworkNodeAction::FileMissing { pending_file_id } => {
                error!("File missing for pending file ID: {:?}", pending_file_id);
                let status = SendStatus::Failed;
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&pending_file_id, &status, None)
                    .await
                    .expect("Pending file status to be updated");
                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &pending_file_id,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
                self.try_cleanup_source_file(&pending_file_id).await;
            }
            NetworkNodeAction::StoreReceivedFile(request) => {
                let completed = &request.request;

                let result = self
                    .db
                    .complete_received_file(completed)
                    .await
                    .auto_map_err();

                if let Err(ref e) = result {
                    error!("Failed to complete received file: {e:?}");
                }

                // Fetch the full record so we can send a notification and push status
                let received = self
                    .db
                    .get_received_file_by_transfer_id(&completed.transfer_id)
                    .await;

                if let Ok(ref rf) = received {
                    let _ = self
                        .client_push_sender
                        .send(ServerMessage::received_file_status_changed(rf))
                        .await;
                }

                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }

                let Ok(ref rf) = received else {
                    error!("Failed to fetch completed received file for notification");
                    return;
                };

                let self_contact_id = match self.db.get_self_contact().await {
                    Ok(self_contact) => Some(self_contact.id),
                    Err(e) => {
                        error!("Could not retrieve self contact: {e:?}");
                        None
                    }
                };

                let sender = match rf.contact_id {
                    // The file was sent from another of the user's own devices.
                    Some(ref id) if self_contact_id.as_ref() == Some(id) => FileSender::SelfDevice,
                    Some(ref id) => match self.db.get_contact_name(id).await {
                        Ok(name) => FileSender::Contact(name),
                        Err(e) => {
                            error!("Could not retrieve contact name for {id}: {e:?}");
                            FileSender::Unknown
                        }
                    },
                    None => FileSender::Unknown,
                };

                let filerequest_name = match self.db.get_filerequest(&rf.filerequest_id).await {
                    Ok(fr) => fr.title,
                    Err(e) => {
                        error!(
                            "Unable to retrieve filerequest of id {}: {e:?}",
                            rf.filerequest_id
                        );
                        return;
                    }
                };
                self.host_controller
                    .send_notification(Notification::FileReceived {
                        sender,
                        filerequest_name,
                        file_name: rf.file_name.clone(),
                        file_size: rf.file_size_bytes,
                    });
            }
            NetworkNodeAction::CreateIncomingFile(request) => {
                let result = self
                    .db
                    .create_incoming_file(&request.request)
                    .await
                    .auto_map_err();

                if let Err(ref e) = result {
                    error!("Failed to create incoming file: {e:?}");
                } else {
                    // Build a ReceivedFile for the status push
                    let rf = ReceivedFile {
                        id: result.as_ref().unwrap().clone(),
                        filerequest_id: request.request.filerequest_id.clone(),
                        transfer_id: Some(request.request.transfer_id.clone()),
                        contact_id: request.request.contact_id.clone(),
                        peer_id: request.request.peer_id.clone(),
                        file_name: request.request.file_name.clone(),
                        persisted_as_name: request.request.persisted_as_name.clone(),
                        file_path: None,
                        file_size_bytes: request.request.file_size_bytes,
                        progress_bytes: 0,
                        status: ReceiveStatus::Receiving,
                        started_at_ms: request.request.started_at_ms,
                        received_at_ms: None,
                    };
                    let _ = self
                        .client_push_sender
                        .send(ServerMessage::received_file_status_changed(&rf))
                        .await;
                }

                if let Some(sender) = request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(result);
                }
            }
            NetworkNodeAction::UpdateReceivedFileStatus {
                transfer_id,
                status,
                progress_bytes,
            } => {
                let db_result = self
                    .db
                    .update_received_file_status(&transfer_id, &status, progress_bytes)
                    .await;
                if let Err(ref e) = db_result {
                    error!("Failed to update received file status: {e:?}");
                }
                // Push the updated status to frontend — fetch the full record for the message
                if let Ok(rf) = self.db.get_received_file_by_transfer_id(&transfer_id).await {
                    let _ = self
                        .client_push_sender
                        .send(ServerMessage::received_file_status_changed(&rf))
                        .await;
                }
            }
            NetworkNodeAction::DeleteReceivedFile { transfer_id } => {
                // Look up by transfer_id to get the row id, then delete
                if let Ok(rf) = self.db.get_received_file_by_transfer_id(&transfer_id).await {
                    if let Err(e) = self.db.delete_received_file(&rf.id).await {
                        error!("Failed to delete received file {}: {e:?}", rf.id);
                    }
                } else {
                    error!(
                        "Failed to find received file with transfer_id {transfer_id} for deletion"
                    );
                }
            }
            NetworkNodeAction::FileIoError { file, error } => {
                error!("File IO error for file {:?}: {:?}", file, error);
                let status = SendStatus::Failed;
                let target_filerequest_id = self
                    .db
                    .handle_update_pending_file_status(&file, &status, None)
                    .await
                    .expect("Pending file status to be updated");
                let _ = self
                    .client_push_sender
                    .send(ServerMessage::pending_file_status_changed(
                        &file,
                        &status,
                        &target_filerequest_id,
                    ))
                    .await;
                self.try_cleanup_source_file(&file).await;
            }
            NetworkNodeAction::GetContactPublicKeys(brain_request) => {
                let keys = match self
                    .db
                    .get_contact_public_keys(brain_request.request.clone())
                    .await
                    .ok()
                    .flatten()
                {
                    Some(keys) => Some(keys),
                    // Check if maybe the one talking to us is ourselves (from another device we own)
                    None => self
                        .db
                        .get_self_contact()
                        .await
                        .ok()
                        .and_then(|self_contact| {
                            if self_contact.id == brain_request.request {
                                Some(self_contact.keys.public)
                            } else {
                                None
                            }
                        }),
                };
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(keys);
                }
            }
            NetworkNodeAction::GetContactsPublicKeys(brain_request) => {
                let PubkeyFetchReceiveRequest {
                    contact_id,
                    wanted_contact_ids,
                } = &brain_request.request;
                let self_id = self.db.get_self_contact_id().await.ok();
                let keys = if self_id.as_ref() != Some(contact_id) {
                    warn!("Declining pubkey fetch from a peer that isn't one of our devices");
                    None
                } else {
                    Some(
                        self.db
                            .get_contacts_public_keys(wanted_contact_ids.clone())
                            .await
                            .unwrap_or_default(),
                    )
                };
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(keys);
                }
            }
            NetworkNodeAction::IsContactKnown(brain_request) => {
                let contact_name = self.db.get_contact_name(&brain_request.request).await;
                let is_known = match contact_name {
                    Ok(_) => {
                        // if we know the name, we know the contact
                        Ok(true)
                    }
                    Err(DbError::NotFound { .. }) => {
                        // We don't know that contact
                        Ok(false)
                    }
                    Err(e) => {
                        error!(?e, "Error when querying contact name");
                        Err("Could not determine whether contact is known due to DB error".into())
                    }
                };
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(is_known);
                }
            }
            NetworkNodeAction::OpenFileForReading(brain_request) => {
                let file = self
                    .host_controller
                    .access_file_for_reading(brain_request.request.uri)
                    .await;
                if let Err(ref e) = file {
                    error!("Could not open file for reading: {e}");
                    let status = SendStatus::Failed;
                    let target_filerequest_id = self
                        .db
                        .handle_update_pending_file_status(&brain_request.request.id, &status, None)
                        .await
                        .expect("Pending file status to be updated");
                    let _ = self
                        .client_push_sender
                        .send(ServerMessage::pending_file_status_changed(
                            &brain_request.request.id,
                            &status,
                            &target_filerequest_id,
                        ))
                        .await;
                    self.try_cleanup_source_file(&brain_request.request.id)
                        .await;
                }
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(file.map_err(|e| {
                        error!("Failed to access file for reading: {:?}", e);
                    }));
                }
            }
            NetworkNodeAction::ValidateSelfContactInviteChallenge(brain_request) => {
                match self
                    .ephemeral_data
                    .timeout_lock()
                    .await
                    .self_contact_invite_challenges
                    .remove(&brain_request.request)
                {
                    false => {
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(None);
                        }
                    }
                    true => {
                        let self_contact = self
                            .db
                            .get_self_contact()
                            .await
                            .inspect_err(|e| {
                                error!("Failed to get self contact from DB: {:?}", e);
                            })
                            .ok();
                        if let Some(sender) =
                            brain_request.response_sender.timeout_lock().await.take()
                        {
                            let _ = sender.send(self_contact);
                        }
                    }
                }
            }
            NetworkNodeAction::UpdateIdentity(new_identity, inviter_peer) => {
                // The peers are already connected here, so sync runs immediately and the inviter's
                // entry arrives via the Devices doc — no need to pre-register it (unlike the
                // out-of-band QR path, which may be offline; see `ClientAction::UpdateIdentity`).
                match self
                    .adopt_shared_identity(new_identity, inviter_peer, None)
                    .await
                {
                    Ok(()) => {
                        self.client_push_sender
                            .send(ServerMessage::received_self_contact_invite_over_network())
                            .await
                            .expect("Sending to work");
                    }
                    Err(e) => error!("Failed to update identity in DB: {e:?}"),
                }
            }
            NetworkNodeAction::AnsweredSelfContactInvite => {
                self.client_push_sender
                    .send(ServerMessage::self_contact_invite_accepted_over_network())
                    .await
                    .expect("sending to work");
            }
            NetworkNodeAction::RejectedSelfContactInvite => {
                // Nothing for now
            }
            NetworkNodeAction::SelfContactInviteGotRejected => {
                // Nothing for now
            }
            NetworkNodeAction::ValidateContactShareChallenge(brain_request) => match self
                .ephemeral_data
                .timeout_lock()
                .await
                .contact_share_challenges
                .contains(&brain_request.request)
            {
                false => {
                    if let Some(sender) = brain_request.response_sender.timeout_lock().await.take()
                    {
                        let _ = sender.send(None);
                    }
                }
                true => {
                    let self_contact = self
                        .db
                        .get_sharable_public_self_contact()
                        .await
                        .inspect_err(|e| {
                            error!("Failed to get contact from DB: {:?}", e);
                        })
                        .ok();
                    if let Some(sender) = brain_request.response_sender.timeout_lock().await.take()
                    {
                        let _ = sender.send(self_contact);
                    }
                }
            },
            NetworkNodeAction::CreateContact(contact) => {
                match self.db.update_contact_projection(contact).await {
                    Ok(write) => {
                        self.announce(write);
                        self.client_push_sender
                            .send(ServerMessage::contact_share_accepted_over_network())
                            .await
                            .expect("Sending to work");
                    }
                    Err(e) => {
                        error!("Failed to create contact in DB: {:?}", e);
                    }
                };
            }
            NetworkNodeAction::AnsweredContactShare => {
                // Do nothing
            }
            NetworkNodeAction::ContactShareGotRejected => {
                // Do nothing
            }
            NetworkNodeAction::RejectedContactShare => {
                // DoNothing
            }
            NetworkNodeAction::SyncReceive(brain_request) => {
                let SyncReceiveRequest {
                    contact_id,
                    peer,
                    wire,
                } = brain_request.request;
                let reply = self.handle_sync_receive(contact_id, peer, wire).await;
                if let Some(sender) = brain_request.response_sender.timeout_lock().await.take() {
                    let _ = sender.send(reply);
                }
            }
            NetworkNodeAction::SyncReplyReceived { peer, reply } => {
                self.handle_sync_reply(peer, reply).await;
            }
            NetworkNodeAction::SyncOnPeerConnect(peer_id) => {
                // Push only to devices on our owned-device list. The accept side
                // independently requires the session to prove it carries our contact id
                // (i.e. the peer is one of our own devices too).
                match is_owned_device(self.db.as_ref(), &peer_id).await {
                    Ok(true) => self.initiate_sync(&peer_id).await,
                    Ok(false) => {}
                    Err(e) => error!("Failed to check owned-device status: {e:?}"),
                }
            }
            NetworkNodeAction::SyncDocWithPeer { peer, kind } => {
                self.initiate_sync_if_dirty(&peer, kind).await;
            }
            NetworkNodeAction::SyncTransportFailed { peer } => {
                // Drop the peer's exchange slots so the next connect re-syncs from scratch.
                self.ephemeral_data
                    .timeout_lock()
                    .await
                    .sync_slots
                    .retain(|(p, _), _| p != &peer);
            }
            NetworkNodeAction::StoreFetchedContactKeys(response) => {
                self.handle_store_fetched_contact_keys(response).await;
            }
        }
    }


    /// Adopt a shared identity (from a network invite or an out-of-band QR): switch our
    /// contact id/keys, reset the old identity's owned-device list, re-register ourselves,
    /// and initiate a sync with the inviter. When `inviter_device_name` is given, the inviter is
    /// also recorded as an owned device up front — required for the offline QR case, where sync
    /// can only happen on a later connect (gated on `is_owned_device`).
    async fn adopt_shared_identity(
        &self,
        identity: SelfContact,
        inviter_peer: PeerIdWrapper,
        inviter_device_name: Option<String>,
    ) -> Result<(), DbError> {
        self.db.update_identity(identity.clone()).await?;
        self.network
            .read()
            .await
            .update_identity(identity.id, identity.keys)
            .await;
        // Reset the (old identity's) device list atomically so it can't race a concurrent write.
        if let Err(e) = self
            .db
            .mutate_sync_doc(
                DocKind::Devices,
                Box::new(|_old| sync::empty_blob(DocKind::Devices).map_err(sync_doc_err)),
            )
            .await
        {
            error!("Failed to reset devices doc on identity change: {e:?}");
        }
        self.ensure_own_device_registered().await;
        if let Some(name) = inviter_device_name {
            // Not announced: the full `initiate_sync` below covers every document for this peer,
            // so announcing the devices document separately would only duplicate it.
            match add_or_update_owned_device(self.db.as_ref(), inviter_peer.clone(), name).await {
                Ok(_write) => {}
                Err(e) => error!("Failed to register inviter as an owned device: {e:?}"),
            }
        }
        self.initiate_sync(&inviter_peer).await;
        Ok(())
    }

    async fn build_self_contact_invite(&self) -> FilerequestResult<SelfContactInvite> {
        let contact = self.db.get_self_contact().await?;
        let info = self.network.read().await.get_node_info().await?;
        let peer_id = info.peer_id;
        let inviter_device_name = list_owned_devices(self.db.as_ref())
            .await?
            .into_iter()
            .find(|(p, _)| *p == peer_id)
            .map(|(_, name)| name)
            .unwrap_or_default();
        Ok(SelfContactInvite {
            contact,
            inviter_peer_id: peer_id,
            inviter_device_name,
        })
    }

    async fn try_cleanup_source_file(&self, pending_file_id: &FylesId) {
        if let Ok(pending_file) = self.db.get_pending_file(pending_file_id).await
            && let Ok(count) = self
                .db
                .count_non_terminal_pending_files_for_path(pending_file_id)
                .await
            && count == 0
        {
            let _ = self
                .host_controller
                .remove_source_file_if_temporary(&pending_file.file_path)
                .await;
        }
    }
}

