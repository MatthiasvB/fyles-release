use super::PARALLEL_CHUNKS;
use super::stream_receiver::StreamSlot;
use crate::CHUNK_SIZE;
use crate::constants::{RECEIVE_IDLE_SLACK, RECEIVE_IDLE_TIMEOUT};
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
use fyles_core::library::util::epoch::unix_epoch_millis;
use fyles_core::library::util::part_file::get_part_file_name;
use libp2p::PeerId;
use libp2p::request_response::InboundRequestId;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use tokio::io::AsyncWriteExt;
use tracing::{Span, debug, error, info, warn};

struct ChunkAssembler {
    byte_progress: u64,
    declared: u64,
    buffered: BTreeMap<u64, Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
enum ChunkViolation {
    BeyondDeclaredSize,
    BeyondWindow,
    TooManyBuffered,
}

impl ChunkAssembler {
    const WINDOW_BYTES: u64 = PARALLEL_CHUNKS as u64 * CHUNK_SIZE as u64;
    /// Room for a full window of the current attempt plus one of late chunks from the previous one.
    const MAX_BUFFERED: usize = 2 * PARALLEL_CHUNKS as usize;

    fn starting(declared: u64) -> Self {
        Self {
            byte_progress: 0,
            declared,
            buffered: BTreeMap::new(),
        }
    }

    fn resuming_at(declared: u64, byte_progress: u64) -> Option<Self> {
        (byte_progress <= declared).then(|| Self {
            byte_progress,
            declared,
            buffered: BTreeMap::new(),
        })
    }

    fn is_complete(&self) -> bool {
        if self.byte_progress > self.declared {
            error!(assembler = ?self, "Received more bytes than declared");
        }
        self.byte_progress >= self.declared
    }

    /// The returned runs are to be appended in order.
    fn accept(&mut self, offset: u64, data: Vec<u8>) -> Result<Vec<Vec<u8>>, ChunkViolation> {
        let Some(end) = offset
            .checked_add(data.len() as u64)
            .filter(|end| *end <= self.declared)
        else {
            return Err(ChunkViolation::BeyondDeclaredSize);
        };
        if end <= self.byte_progress {
            return Ok(Vec::new());
        }
        if offset >= self.byte_progress.saturating_add(Self::WINDOW_BYTES) {
            return Err(ChunkViolation::BeyondWindow);
        }
        if self.buffered.len() >= Self::MAX_BUFFERED && !self.buffered.contains_key(&offset) {
            return Err(ChunkViolation::TooManyBuffered);
        }
        self.buffered.insert(offset, data);

        let mut runs = Vec::new();
        while let Some(entry) = self.buffered.first_entry()
            && *entry.key() <= self.byte_progress
        {
            let start = *entry.key();
            let mut data = entry.remove();
            let already_written = (self.byte_progress - start) as usize;
            if already_written < data.len() {
                let fresh = data.split_off(already_written);
                self.byte_progress += fresh.len() as u64;
                runs.push(fresh);
            }
        }
        Ok(runs)
    }
}

impl Debug for ChunkAssembler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkAssembler")
            .field("byte_progress", &self.byte_progress)
            .field("declared", &self.declared)
            .field(
                "buffered",
                &self
                    .buffered
                    .iter()
                    .map(|(offset, data)| (*offset, data.len()))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
pub struct ReceiveManager {
    assembler: ChunkAssembler,
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
    #[allow(clippy::too_many_arguments)]
    fn new(
        drive_handler: FilerequestDriveHandler,
        file: tokio::fs::File,
        assembler: ChunkAssembler,
        file_name: String,
        transfer_id: FylesId,
        filerequest_id: FylesId,
        contact_id: Option<ContactId>,
        started_at_ms: i64,
    ) -> Self {
        Self {
            assembler,
            file_name,
            file,
            drive_handler,
            transfer_id,
            filerequest_id,
            contact_id,
            started_at_ms,
        }
    }

    fn byte_progress(&self) -> u64 {
        self.assembler.byte_progress
    }

    async fn store(&mut self, chunk: DataChunk) -> ReceiverRes {
        let runs = match self.assembler.accept(chunk.offset, chunk.data) {
            Ok(runs) => runs,
            Err(violation) => {
                warn!(
                    ?violation,
                    offset = chunk.offset,
                    "Chunk does not fit the transfer"
                );
                return ReceiverRes::ProtocolViolation;
            }
        };
        for run in runs {
            if self.file.write_all(&run).await.is_err() {
                return ReceiverRes::IoError;
            }
        }
        ReceiverRes::Received
    }
}

/// Moves a complete part file into place and records it. `false` when either fails, which leaves
/// the sender free to try again.
pub(super) async fn finalize_received(
    drive_handler: &FilerequestDriveHandler,
    part_file_name: &str,
    file_name: &str,
    transfer_id: FylesId,
    brain_action_sender: &tokio::sync::mpsc::Sender<BrainAction>,
) -> bool {
    let persisted_as_name = match drive_handler
        .finalize_partial_file(part_file_name, file_name)
        .await
    {
        Ok(persisted_as_name) => persisted_as_name,
        Err(e) => {
            error!(error=?e, "Internal error upon transfer completion");
            // TODO: Look at the error in detail. It may be that retrying makes no sense, for example when the filename doesn't sanitize
            return false;
        }
    };
    let received_at_ms = match unix_epoch_millis() {
        Ok(ms) => ms,
        Err(e) => {
            error!(error=?e, "System clock error");
            return false;
        }
    };
    let (request, response) = BrainRequest::with_receiver(CompleteReceivedFile {
        transfer_id,
        file_path: persisted_as_name.clone(),
        persisted_as_name,
        received_at_ms,
    });
    brain_action_sender
        .send(BrainAction::NetworkNode(
            NetworkNodeAction::StoreReceivedFile(request),
        ))
        .await
        .expect("Sending to work");
    match response.await.expect("Sender not to be dropped") {
        Ok(_id) => true,
        Err(e) => {
            error!("Error completing received file: {e:?}");
            false
        }
    }
}

/// A peer has at most one transfer, in `receiving_files` or in `streams`, and every offer goes in
/// through [`FilerequestReceiver::clear_stream`] to keep it so. Two maps because a stream must
/// not idle-expire while its task, which never touches the map, is busy. They become one when the
/// chunked protocol is retired.
pub struct FilerequestReceiver<F: FnMut(ReceiveManager) = fn(ReceiveManager)> {
    pub(super) receiving_files: IdleExpiryMap<PeerId, ReceiveManager, F>,
    pub(super) streams: HashMap<PeerId, StreamSlot>,
    pub(super) brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>,
}

impl Debug for FilerequestReceiver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilerequestReceiver")
            .field("receiving_files", &self.receiving_files)
            .field("streams", &self.streams.len())
            .finish()
    }
}

impl FilerequestReceiver {
    pub fn new(brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>) -> Self {
        Self {
            receiving_files: IdleExpiryMap::new(
                RECEIVE_IDLE_TIMEOUT,
                RECEIVE_IDLE_SLACK,
                |mut expired| {
                    tokio::spawn(async move {
                        let _ = expired.file.flush().await;
                    });
                },
            ),
            streams: HashMap::new(),
            brain_action_sender,
        }
    }
}

/// `None` once the entry has expired, since `entry()` has already destructed it.
fn current_file_name<F: FnMut(ReceiveManager)>(
    entry: &mut IdleEntry<'_, PeerId, ReceiveManager, F>,
) -> Option<String> {
    match entry {
        IdleEntry::Occupied(occupied) => occupied.get().map(|it| it.file_name.clone()),
        IdleEntry::Vacant(_) => None,
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

                let part_file_name = get_part_file_name(
                    started_at_ms,
                    &transfer_uuid.0,
                    &node_id.to_base58(),
                    &contact_id,
                );

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
                    // Best estimate until the file lands; overwritten with the real
                    // on-disk basename on completion.
                    persisted_as_name: file_name.clone(),
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
                    ChunkAssembler::starting(file_size_bytes),
                    file_name.clone(),
                    transfer_uuid.clone(),
                    filerequest_id,
                    contact_id,
                    started_at_ms,
                );
                self.clear_stream(node_id);
                let mut entry = self.receiving_files.entry(node_id);
                // Read before replacing: `insert_destruct` destructs the displaced transfer rather
                // than handing it back, so this is the only chance to name it in the log.
                let displaced = current_file_name(&mut entry);
                if entry.insert_destruct(receive_manager) {
                    warn!(peer=?node_id, old_file_name=?displaced, new_file_name=file_name, "Replaced previous receiving file with new transfer");
                }
                FileResponse::AcceptNewTransfer
            }
            FileRequest::ContinueTransfer {
                filerequest_id,
                transfer_uuid,
            } => {
                if let IdleEntry::Occupied(ref mut entry) = entry
                    && let Some(entry) = entry.get_mut()
                    && entry.transfer_id == transfer_uuid
                    && entry.filerequest_id == filerequest_id
                    && entry.contact_id == contact_id
                {
                    debug!("Transfer to be continued is still in memory");
                    return FileResponse::AcceptContinueTransfer {
                        offset: entry.byte_progress(),
                    };
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

                let part_file_name = get_part_file_name(
                    started_at_ms,
                    &transfer_uuid.0,
                    &node_id.to_base58(),
                    &contact_id,
                );

                let file = match drive_handler
                    .get_partial_file_for_writing(&part_file_name)
                    .await
                {
                    Ok(file) => file,
                    Err(e) => {
                        error!(peer=?node_id, error=?e, "Could not open file to continue transfer");
                        self.brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id: transfer_uuid,
                                    status: ReceiveStatus::Failed,
                                    progress_bytes: 0,
                                },
                            ))
                            .await
                            .expect("Sending to work");
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

                let Some(assembler) = ChunkAssembler::resuming_at(file_size_bytes, continue_at)
                else {
                    error!(peer=?node_id, continue_at, file_size_bytes, "Partial file is longer than the transfer, starting over");
                    drop(file);
                    if let Err(e) = drive_handler.remove_partial_file(&part_file_name).await {
                        error!(error=?e, "Could not remove the oversized partial file");
                    }
                    self.brain_action_sender
                        .send(BrainAction::NetworkNode(
                            NetworkNodeAction::UpdateReceivedFileStatus {
                                transfer_id: transfer_uuid,
                                status: ReceiveStatus::Failed,
                                progress_bytes: 0,
                            },
                        ))
                        .await
                        .expect("Sending to work");
                    return FileResponse::RestartContinueTransfer;
                };

                let receive_manager = ReceiveManager::new(
                    drive_handler,
                    file,
                    assembler,
                    file_name.clone(),
                    transfer_uuid.clone(),
                    filerequest_id,
                    contact_id,
                    started_at_ms,
                );

                self.clear_stream(node_id);
                let mut entry = self.receiving_files.entry(node_id);
                let displaced = current_file_name(&mut entry);
                if entry.insert_destruct(receive_manager) {
                    warn!(peer=?node_id, old_file_name=?displaced, new_file_name=file_name, "Replaced previous receiving file with continued transfer");
                }
                info!(?continue_at, "Ready to continue receiving file");
                FileResponse::AcceptContinueTransfer {
                    offset: continue_at,
                }
            }
            FileRequest::AbortTransfer { transfer_uuid } => {
                // The brain verifies peer and contact and drops the named transfer from storage,
                // whether or not it is the one in memory, so an abort naming one we have moved on
                // from leaves the current one alone.
                let mut maybe_receiving = self.receiving_files.entry(node_id);
                let current = match &mut maybe_receiving {
                    IdleEntry::Occupied(occupied) => occupied
                        .get()
                        .filter(|it| it.contact_id == contact_id)
                        .map(|it| it.transfer_id.clone()),
                    IdleEntry::Vacant(_) => None,
                };
                if transfer_uuid.is_none() {
                    warn!(peer=?node_id, "Peer request transfer abort without a transfer id. Assuming current transfer");
                }
                let Some(withdrawn) = transfer_uuid.or_else(|| current.clone()) else {
                    warn!(peer=?node_id, "Attempted to abort transfer that we don't know");
                    return FileResponse::ProtocolViolation;
                };
                if current.as_ref() == Some(&withdrawn) {
                    // Closes the partial file before the brain removes it.
                    drop(maybe_receiving.remove());
                }
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(
                        NetworkNodeAction::DropIncomingTransfer {
                            transfer_id: withdrawn,
                            peer_id: node_id.to_base58(),
                            contact_id,
                        },
                    ))
                    .await
                    .expect("Sending to work");
                FileResponse::ConfirmAbort
            }
            FileRequest::Chunk(chunk) => {
                let Some(receive_manager) = self.receiving_files.get_mut(&node_id) else {
                    warn!(peer=?node_id, "Received file chunk from peer for which no transfer is ongoing");
                    return FileResponse::ProtocolViolation;
                };

                if chunk.transfer_uuid != receive_manager.transfer_id {
                    warn!(peer=?node_id, "Peer sent chunk with transfer id that does not match the current transfer");
                    self.receiving_files.destruct(&node_id);
                    return FileResponse::ProtocolViolation;
                }

                if contact_id != receive_manager.contact_id {
                    warn!(
                        "Contact ID does not match, is this an attack? Responding with something random"
                    );
                    self.receiving_files.destruct(&node_id);
                    // TODO: Notification
                    return FileResponse::random();
                }

                match receive_manager.store(chunk).await {
                    ReceiverRes::Received => {
                        self.brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id: receive_manager.transfer_id.clone(),
                                    status: ReceiveStatus::Receiving,
                                    progress_bytes: receive_manager.byte_progress(),
                                },
                            ))
                            .await
                            .expect("Sending to work");
                        FileResponse::ConfirmChunk
                    }
                    ReceiverRes::ProtocolViolation => {
                        warn!(peer=?node_id, ?request_id, "Protocol violation in received chunk");
                        self.receiving_files.destruct(&node_id);
                        FileResponse::ProtocolViolation
                    }
                    ReceiverRes::IoError => {
                        warn!(peer=?node_id, "I/O error");
                        let transfer_id = receive_manager.transfer_id.clone();
                        let progress = receive_manager.byte_progress();
                        self.receiving_files.destruct(&node_id);
                        self.brain_action_sender
                            .send(BrainAction::NetworkNode(
                                NetworkNodeAction::UpdateReceivedFileStatus {
                                    transfer_id,
                                    status: ReceiveStatus::Failed,
                                    progress_bytes: progress,
                                },
                            ))
                            .await
                            .expect("Sending to work");
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

                if !receive_manager_real.assembler.is_complete() {
                    warn!(peer=?node_id, assembler=?receive_manager_real.assembler, "Peer said we are done, but bytes are missing");
                    return FileResponse::ProtocolViolation;
                }

                if let Err(e) = receive_manager_real.file.flush().await {
                    error!(error=?e, "Could not flush the received file");
                    return FileResponse::InternalError;
                }

                let part_file_name = get_part_file_name(
                    receive_manager_real.started_at_ms,
                    &transfer_uuid.0,
                    &node_id.to_base58(),
                    &contact_id,
                );

                let response = if finalize_received(
                    &receive_manager_real.drive_handler,
                    &part_file_name,
                    &receive_manager_real.file_name,
                    receive_manager_real.transfer_id.clone(),
                    &self.brain_action_sender,
                )
                .await
                {
                    FileResponse::ConfirmDone
                } else {
                    FileResponse::InternalError
                };
                if matches!(response, FileResponse::ConfirmDone) {
                    receive_manager.destruct();
                }
                response
            }
            FileRequest::ProtocolViolation { transfer_uuid } => {
                warn!(peer=?node_id, transfer_id=?transfer_uuid, state=?self.receiving_files, "Peer reported protocol error on our side");
                let mut maybe_receiving = self.receiving_files.entry(node_id);
                let names_another = match (&transfer_uuid, &mut maybe_receiving) {
                    (Some(transfer_id), IdleEntry::Occupied(occupied)) => occupied
                        .get()
                        .is_some_and(|it| it.transfer_id != *transfer_id),
                    _ => false,
                };
                if !names_another {
                    maybe_receiving.destruct();
                }
                FileResponse::AcknowledgeProtocolViolation
            }
        }
    }
}

#[cfg(test)]
mod chunk_assembler_tests {
    use super::*;

    fn bytes(range: std::ops::Range<u8>) -> Vec<u8> {
        range.collect()
    }

    fn written(runs: Vec<Vec<u8>>) -> Vec<u8> {
        runs.concat()
    }

    #[test]
    fn chunks_in_order_are_written_straight_away() {
        let mut assembler = ChunkAssembler::starting(20);

        assert_eq!(
            written(assembler.accept(0, bytes(0..10)).unwrap()),
            bytes(0..10)
        );
        assert_eq!(
            written(assembler.accept(10, bytes(10..20)).unwrap()),
            bytes(10..20)
        );
        assert!(assembler.is_complete());
    }

    #[test]
    fn a_chunk_ahead_waits_for_the_gap_before_it() {
        let mut assembler = ChunkAssembler::starting(20);

        assert!(assembler.accept(10, bytes(10..20)).unwrap().is_empty());
        assert_eq!(
            written(assembler.accept(0, bytes(0..10)).unwrap()),
            bytes(0..20)
        );
        assert!(assembler.is_complete());
    }

    #[test]
    fn a_duplicate_is_accepted_without_writing_anything() {
        let mut assembler = ChunkAssembler::starting(20);
        assembler.accept(0, bytes(0..10)).unwrap();

        assert!(assembler.accept(0, bytes(0..10)).unwrap().is_empty());
        assert_eq!(assembler.byte_progress, 10);
    }

    #[test]
    fn a_late_chunk_of_an_earlier_attempt_only_adds_what_is_missing() {
        let mut assembler = ChunkAssembler::resuming_at(30, 10).expect("10 is within 30");

        let late = assembler.accept(5, bytes(5..15)).unwrap();
        let resumed = assembler.accept(10, bytes(10..20)).unwrap();
        let rest = assembler.accept(20, bytes(20..30)).unwrap();

        assert_eq!(written([late, resumed, rest].concat()), bytes(10..30));
        assert!(assembler.is_complete());
    }

    #[test]
    fn a_chunk_past_the_declared_size_is_a_violation() {
        let mut assembler = ChunkAssembler::starting(30);

        assert_eq!(
            assembler.accept(25, bytes(0..10)),
            Err(ChunkViolation::BeyondDeclaredSize)
        );
    }

    #[test]
    fn progress_past_the_declared_size_cannot_be_resumed() {
        assert!(ChunkAssembler::resuming_at(30, 31).is_none());
        assert!(
            ChunkAssembler::resuming_at(30, 30)
                .expect("a finished transfer to resume")
                .is_complete()
        );
    }

    #[test]
    fn an_offset_that_overflows_is_a_violation() {
        let mut assembler = ChunkAssembler::starting(u64::MAX);

        assert_eq!(
            assembler.accept(u64::MAX, bytes(0..2)),
            Err(ChunkViolation::BeyondDeclaredSize)
        );
    }

    #[test]
    fn a_chunk_beyond_the_window_is_a_violation() {
        let mut assembler = ChunkAssembler::starting(u64::MAX);

        assert_eq!(
            assembler.accept(ChunkAssembler::WINDOW_BYTES, bytes(0..1)),
            Err(ChunkViolation::BeyondWindow)
        );
    }

    #[test]
    fn buffering_is_bounded() {
        let mut assembler = ChunkAssembler::starting(u64::MAX);
        for offset in 1..=ChunkAssembler::MAX_BUFFERED as u64 {
            assembler.accept(offset * 2, bytes(0..1)).unwrap();
        }

        assert_eq!(
            assembler.accept(1, bytes(0..1)),
            Err(ChunkViolation::TooManyBuffered)
        );
    }
}
