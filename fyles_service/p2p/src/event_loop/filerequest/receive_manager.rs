use super::PARALLEL_CHUNKS;
use crate::data_structures::idle_expiry_map::{IdleEntry, IdleExpiryMap};
use crate::types::{DataChunk, FileRequest, FileResponse};
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::{
    FilerequestAccessRequest, FilerequestContinueRequest, FilerequestContinueResponse,
    NetworkNodeAction,
};
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{
    CompleteReceivedFile, ContactId, CreateIncomingFile, FylesId, ReceiveStatus,
};
use fyles_core::core::filerequest_drive_handler::FilerequestDriveHandler;
use fyles_core::library::util::duration_ext::DurationExt;
use fyles_core::library::util::epoch::unix_epoch_millis;
use fyles_core::library::util::part_file::get_part_file_name;
use libp2p::request_response::InboundRequestId;
use libp2p::PeerId;
use std::fmt::{Debug, Formatter};
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, trace, warn, Span};

#[derive(Debug)]
pub struct ReceiveManager {
    chunks_in_flight: Vec<Option<DataChunk>>,
    earliest_not_received: u32,
    declared_file_size_bytes: u64,
    byte_progress: u64,
    file: tokio::fs::File,
    file_name: String,
    transfer_id: FylesId,
    filerequest_id: FylesId,
    drive_handler: FilerequestDriveHandler,
    contact_id: Option<ContactId>,
    started_at_ms: i64,
}

enum ReceiverRes {
    Received,
    ProtocolViolation,
    IoError,
}

impl ReceiveManager {
    fn new(
        drive_handler: FilerequestDriveHandler,
        file: tokio::fs::File,
        target_file_size: u64,
        file_name: String,
        transfer_id: FylesId,
        filerequest_id: FylesId,
        contact_id: Option<ContactId>,
        started_at_ms: i64,
    ) -> Self {
        Self {
            chunks_in_flight: vec![None; PARALLEL_CHUNKS as usize],
            earliest_not_received: 0,
            declared_file_size_bytes: target_file_size,
            byte_progress: 0,
            file_name,
            file,
            drive_handler,
            transfer_id,
            filerequest_id,
            contact_id,
            started_at_ms,
        }
    }

    fn reset(&mut self) {
        for slot in &mut self.chunks_in_flight {
            *slot = None;
        }
        self.earliest_not_received = 0
    }

    async fn store(&mut self, chunk: DataChunk) -> ReceiverRes {
        // let chunk_size = chunk.data.len() as u64;
        // if chunk_size + self.byte_progress > self.declared_file_size_bytes {
        //     return ReceiverRes::ProtocolViolation;
        // }
        if chunk.idx >= self.earliest_not_received + PARALLEL_CHUNKS {
            return ReceiverRes::ProtocolViolation;
        }

        if chunk.idx < self.earliest_not_received {
            trace!("Received chunk that we already saw, we just accept it again");
            return ReceiverRes::Received;
        }
        let real_index = (chunk.idx % PARALLEL_CHUNKS) as usize;
        assert!(
            real_index < self.chunks_in_flight.len(),
            "Real index out of bounds"
        );
        self.chunks_in_flight[real_index] = Some(chunk);
        if real_index == (self.earliest_not_received % PARALLEL_CHUNKS) as usize {
            while let Some(chunk) = self.chunks_in_flight
                [(self.earliest_not_received % PARALLEL_CHUNKS) as usize]
                .take()
            {
                let chunk_size = chunk.data.len() as u64;
                if chunk_size + self.byte_progress > self.declared_file_size_bytes {
                    return ReceiverRes::ProtocolViolation;
                }
                self.byte_progress += chunk_size;
                let Ok(_) = self.file.write_all(&chunk.data).await else {
                    return ReceiverRes::IoError;
                };
                self.earliest_not_received += 1;
            }
        }
        ReceiverRes::Received
    }
}

pub struct FilerequestReceiver<F: FnMut(ReceiveManager) = fn(ReceiveManager)> {
    receiving_files: IdleExpiryMap<PeerId, ReceiveManager, F>,
    brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>,
}

impl Debug for FilerequestReceiver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilerequestReceiver")
            .field("receiving_files", &self.receiving_files)
            .finish()
    }
}

impl FilerequestReceiver {
    pub fn new(brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>) -> Self {
        Self {
            receiving_files: IdleExpiryMap::new(30.seconds(), 35.seconds(), |mut expired| {
                tokio::spawn(async move {
                    let _ = expired.file.flush().await;
                });
            }),
            brain_action_sender,
        }
    }
}

impl FilerequestReceiver {
    pub async fn respond_to(
        &mut self,
        node_id: PeerId,
        contact_id: Option<ContactId>,
        request: FileRequest,
        request_id: InboundRequestId,
    ) -> FileResponse {
        let mut entry = self.receiving_files.entry(node_id);
        match request {
            FileRequest::NewTransfer {
                filerequest_id,
                file_name,
                file_size_bytes,
                transfer_uuid,
            } => {
                let (request, response) = BrainRequest::with_receiver(FilerequestAccessRequest {
                    filerequest_id: filerequest_id.clone(),
                    contact_id: contact_id.clone(),
                });
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(
                        NetworkNodeAction::RequestFileDrop(request, Span::current()),
                    ))
                    .await
                    .expect("Sending to work");

                let Some(drive_handler) = response.await.expect("Sender not to be dropped") else {
                    return FileResponse::RejectNewTransfer;
                };

                let started_at_ms = match unix_epoch_millis() {
                    Ok(ms) => ms,
                    Err(e) => {
                        error!(peer=?node_id, error=?e, "System clock error");
                        return FileResponse::InternalError;
                    }
                };

                let part_file_name =
                    get_part_file_name(started_at_ms, &transfer_uuid.0, &node_id.to_base58(), &contact_id);

                let file = match drive_handler
                    .get_partial_file_for_writing(&part_file_name)
                    .await
                {
                    Ok(file) => file,
                    Err(e) => {
                        error!(error=?e, "Error opening exact file for writing");
                        return FileResponse::InternalError;
                    }
                };

                // Persist the in-progress transfer so the receiver can resume after restart
                let (create_req, create_resp) = BrainRequest::with_receiver(CreateIncomingFile {
                    filerequest_id: filerequest_id.clone(),
                    transfer_id: transfer_uuid.clone(),
                    contact_id: contact_id.clone(),
                    peer_id: node_id.to_base58(),
                    file_name: file_name.clone(),
                    file_size_bytes,
                    started_at_ms,
                });
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(
                        NetworkNodeAction::CreateIncomingFile(create_req),
                    ))
                    .await
                    .expect("Sending to work");
                if let Err(e) = create_resp.await.expect("Sender not to be dropped") {
                    error!(peer=?node_id, "Failed to persist receiving transfer: {e:?}");
                    return FileResponse::InternalError;
                }

                let receive_manager = ReceiveManager::new(
                    drive_handler,
                    file,
                    file_size_bytes,
                    file_name.clone(),
                    transfer_uuid.clone(),
                    filerequest_id,
                    contact_id,
                    started_at_ms,
                );
                if let Some(before) = entry.insert(receive_manager) {
                    warn!(peer=?node_id, old_file_name=before.file_name, new_file_name=file_name, "Replaced previous receiving file with new transfer");
                }
                FileResponse::AcceptNewTransfer
            }
            FileRequest::ContinueTransfer {
                filerequest_id,
                transfer_uuid,
            } => {
                if let IdleEntry::Occupied(ref mut entry) = entry {
                    if let Some(entry) = entry.get_mut() {
                        if entry.transfer_id == transfer_uuid
                            && entry.filerequest_id == filerequest_id
                            && entry.contact_id == contact_id
                        {
                            debug!("Transfer to be continued is still in memory");
                            entry.reset();
                            return FileResponse::AcceptContinueTransfer {
                                offset: entry.byte_progress,
                            };
                        }
                    }
                }

                let (request, response) = BrainRequest::with_receiver(FilerequestContinueRequest {
                    filerequest_id: filerequest_id.clone(),
                    contact_id: contact_id.clone(),
                    peer_id: node_id.to_base58(),
                    transfer_id: transfer_uuid.clone(),
                });

                self.brain_action_sender
                    .send(BrainAction::NetworkNode(
                        NetworkNodeAction::RequestFileTransferContinuation(request),
                    ))
                    .await
                    .expect("Sending to work");

                let Some(FilerequestContinueResponse {
                    drive_handler,
                    file_name,
                    file_size_bytes,
                    started_at_ms,
                }) = response.await.expect("Sender not to be dropped")
                else {
                    return FileResponse::RestartContinueTransfer;
                };

                let part_file_name =
                    get_part_file_name(started_at_ms, &transfer_uuid.0, &node_id.to_base58(), &contact_id);

                let file = match drive_handler
                    .get_partial_file_for_writing(&part_file_name)
                    .await
                {
                    Ok(file) => file,
                    Err(e) => {
                        error!(peer=?node_id, error=?e, "Could not open file to continue transfer");
                        let _ = self
                            .brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id: transfer_uuid,
                                    status: ReceiveStatus::Failed,
                                    progress_bytes: 0,
                                },
                            ))
                            .await;
                        return FileResponse::RestartContinueTransfer;
                    }
                };

                let continue_at = match file.metadata().await {
                    Ok(continue_at) => continue_at.len(),
                    Err(e) => {
                        error!(error=?e, "Error getting metadata");
                        return FileResponse::InternalError;
                    }
                };

                let mut receive_manager = ReceiveManager::new(
                    drive_handler,
                    file,
                    file_size_bytes,
                    file_name.clone(),
                    transfer_uuid.clone(),
                    filerequest_id,
                    contact_id,
                    started_at_ms,
                );
                receive_manager.byte_progress = continue_at;

                if let Some(before) = entry.insert(receive_manager) {
                    warn!(peer=?node_id, old_file_name=before.file_name, new_file_name=file_name, "Replaced previous receiving file with continued transfer");
                }
                info!(?continue_at, "Ready to continue receiving file");
                FileResponse::AcceptContinueTransfer {
                    offset: continue_at,
                }
            }
            FileRequest::AbortTransfer { transfer_uuid } => {
                let Some(receive_manager) = self.receiving_files.remove(&node_id) else {
                    warn!(peer=?node_id, "Attempted to abort transfer that we don't know");
                    return FileResponse::ProtocolViolation;
                };
                let effective_transfer_id = if let Some(transfer_id) = transfer_uuid {
                    if transfer_id != receive_manager.transfer_id {
                        warn!(peer=?node_id, "Peer requested transfer abort with ID that does not match the current transfer");
                        None
                    } else {
                        Some(transfer_id)
                    }
                } else {
                    warn!(peer=?node_id, "Peer request transfer abort without a transfer id. Assuming current transfer");
                    Some(receive_manager.transfer_id)
                };
                let Some(verified_transfer_id) = effective_transfer_id else {
                    return FileResponse::ProtocolViolation;
                };
                let part_file_name = get_part_file_name(
                    receive_manager.started_at_ms,
                    &verified_transfer_id.0,
                    &node_id.to_base58(),
                    &contact_id,
                );
                match receive_manager
                    .drive_handler
                    .remove_partial_file(&part_file_name)
                    .await
                {
                    Ok(()) => {
                        // Mark the received file as failed in the DB
                        self.brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id: verified_transfer_id,
                                    status: ReceiveStatus::Failed,
                                    progress_bytes: receive_manager.byte_progress,
                                },
                            ))
                            .await
                            .expect("Sending to work");
                        FileResponse::ConfirmAbort
                    }
                    Err(_) => {
                        error!("Error while trying to deleted file for aborted transfer");
                        FileResponse::InternalError
                    }
                }
            }
            FileRequest::Chunk(chunk) => {
                let Some(receive_manager) = self.receiving_files.get_mut(&node_id) else {
                    warn!(peer=?node_id, "Received file chunk from peer for which no transfer is ongoing");
                    return FileResponse::ProtocolViolation;
                };

                if chunk.transfer_uuid != receive_manager.transfer_id {
                    warn!(peer=?node_id, "Peer sent chunk with transfer id that does not match the current transfer");
                    self.receiving_files.remove(&node_id);
                    return FileResponse::ProtocolViolation;
                }

                if contact_id != receive_manager.contact_id {
                    warn!(
                        "Contact ID does not match, is this an attack? Responding with something random"
                    );
                    self.receiving_files.remove(&node_id);
                    // TODO: Notification
                    return FileResponse::random();
                }

                match receive_manager.store(chunk).await {
                    ReceiverRes::Received => {
                        let _ = self
                            .brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id: receive_manager.transfer_id.clone(),
                                    status: ReceiveStatus::Receiving,
                                    progress_bytes: receive_manager.byte_progress,
                                },
                            ))
                            .await;
                        FileResponse::ConfirmChunk
                    }
                    ReceiverRes::ProtocolViolation => {
                        warn!(peer=?node_id, ?request_id, "Protocol violation in received chunk");
                        self.receiving_files.remove(&node_id);
                        FileResponse::ProtocolViolation
                    }
                    ReceiverRes::IoError => {
                        warn!(peer=?node_id, "I/O error");
                        let transfer_id = receive_manager.transfer_id.clone();
                        let progress = receive_manager.byte_progress;
                        self.receiving_files.remove(&node_id);
                        let _ = self
                            .brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id,
                                    status: ReceiveStatus::Failed,
                                    progress_bytes: progress,
                                },
                            ))
                            .await;
                        FileResponse::InternalError
                    }
                }
            }
            FileRequest::Done { transfer_uuid } => {
                let IdleEntry::Occupied(mut receive_manager) = self.receiving_files.entry(node_id)
                else {
                    warn!(peer=?node_id, "Peer said we are done but there is no active transfer");
                    return FileResponse::ProtocolViolation;
                };

                let Some(receive_manager_real) = receive_manager.get_mut() else {
                    error!("Idle entry should not expire that quickly");
                    return FileResponse::InternalError;
                };

                if receive_manager_real.transfer_id != transfer_uuid {
                    warn!(peer=?node_id, "Peer claimed done with transfer id that does not match current transfer");
                    receive_manager.destruct();
                    return FileResponse::ProtocolViolation;
                }

                if receive_manager_real.byte_progress
                    != receive_manager_real.declared_file_size_bytes
                {
                    warn!(peer=?node_id, "Received bytes {} do not match declared bytes {}", receive_manager_real.byte_progress, receive_manager_real.declared_file_size_bytes);
                    return FileResponse::ProtocolViolation;
                }

                let part_file_name = get_part_file_name(
                    receive_manager_real.started_at_ms,
                    &transfer_uuid.0,
                    &node_id.to_base58(),
                    &contact_id,
                );

                match receive_manager_real
                    .drive_handler
                    .finalize_partial_file(&part_file_name, &receive_manager_real.file_name)
                    .await
                {
                    Ok(path) => {
                        let (request, response) =
                            BrainRequest::with_receiver(CompleteReceivedFile {
                                transfer_id: receive_manager_real.transfer_id.clone(),
                                file_path: path,
                                received_at_ms: match unix_epoch_millis() {
                                    Ok(ms) => ms,
                                    Err(e) => {
                                        error!(error=?e, "System clock error");
                                        return FileResponse::InternalError;
                                    }
                                },
                            });
                        self.brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::StoreReceivedFile(request),
                            ))
                            .await
                            .expect("Sending to work");
                        match response.await.expect("Sender not to be dropped") {
                            Ok(_id) => FileResponse::ConfirmDone,
                            Err(e) => {
                                error!(peer=?node_id, "Error completing received file: {e:?}");
                                FileResponse::InternalError
                            }
                        }
                    }
                    Err(e) => {
                        error!(error=?e, "Internal error upon transfer completion");
                        // TODO: Look at the error in detail. It may be that retrying makes no sense, for example when the filename doesn't sanitize
                        FileResponse::InternalError
                    }
                }
            }
            FileRequest::ProtocolViolation { transfer_uuid } => {
                warn!(peer=?node_id, transfer_id=?transfer_uuid, state=?self.receiving_files, "Peer reported protocol error on our side");
                self.receiving_files.remove(&node_id);
                FileResponse::AcknowledgeProtocolViolation
            }
        }
    }
}
