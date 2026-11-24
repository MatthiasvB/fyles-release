use crate::constants::{LEGACY_RECHECK, STALL_ERROR_AFTER};
#[cfg(feature = "debug-server")]
use crate::debug::{NoContent, SerializeDebug};
use crate::event_loop::filerequest::send_decision::{
    self, ChunkStep, Opening, Party, Response, Verdict,
};
use crate::event_loop::filerequest::send_manager::{
    ContactPin, ControlRequest, DataPath, Request, SendDataMut, SendSlot,
};
use crate::file_stream::send::AttemptOutcome;
use crate::send_receive_traits::response_receiver::ResponseReceiver;
use crate::send_receive_traits::session_send_receive::{ReceiveError, SessionSendError};
use crate::sessions::SessionEstablishmentError;
use crate::source::{Source, SourceStamp};
use crate::types::{
    AttemptGrant, AttemptId, FileRequest, FileRequestV2, FileResponse, FileResponseV2,
};
use async_trait::async_trait;
use crypto::{DeSerCryptError, SerCryptError};
use fyles_core::core::brain::action_p2p::OpenFileForReadingRequest;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{ContactId, InProgress, InProgressSendStatus, TransferData};
use fyles_core::core::{
    brain::{action::BrainAction, action_p2p::NetworkNodeAction},
    domain_models::{FylesId, PendingFile, SendStatus},
};
use fyles_core::library::util::util::{TimeoutLock, TimeoutRwLock};
use libp2p::PeerId;
use libp2p::request_response::OutboundFailure;
use std::collections::VecDeque;
use std::collections::hash_map::OccupiedEntry;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::instrument::Instrumented;
use tracing::{Instrument, Span, debug, debug_span, error, info, trace, warn};

pub enum FileTrackerReceivePayload {
    Response(FileResponse),
    Timeout,
    SessionSendError(SessionSendError<SerCryptError, SessionEstablishmentError>),
    TransportSendError,
}

/// Terminal outcome for a file that is leaving the tracker.
///
/// Every file that leaves the tracker (except brain-initiated cancels) must
/// produce exactly one of these outcomes, which maps 1:1 to a brain notification.
#[derive(Debug)]
pub enum FileOutcome {
    /// Transfer completed successfully → `FileSent`
    Sent,
    /// Transfer failed after exceeding retry limit → `FileFailed`
    Failed,
    /// Source file could not be opened / metadata unreadable → `FileMissing`
    Missing,
    /// Peer semantically rejected the file → `FileRejected`
    Rejected,
    /// I/O error reading the file (potentially user-fixable) → `FileIoError`
    IoError(std::io::Error),
}

#[async_trait]
pub trait FileTracker:
    Send
    + Sync
    + ResponseReceiver<
        FileResponse,
        NodeId = PeerId,
        RequestId = usize,
        DecryptError = DeSerCryptError,
        TransportError = OutboundFailure,
        EncryptError = SerCryptError,
        GetSessionError = Arc<SessionEstablishmentError>,
    > + ResponseReceiver<
        FileResponseV2,
        NodeId = PeerId,
        RequestId = usize,
        DecryptError = DeSerCryptError,
        TransportError = OutboundFailure,
        EncryptError = SerCryptError,
        GetSessionError = Arc<SessionEstablishmentError>,
    >
{
    async fn add_pending_file(&self, peer: PeerId, file: PendingFile);

    async fn on_attempt_finished(&self, peer: PeerId, attempt: AttemptId, outcome: AttemptOutcome);

    async fn trigger_interaction_with(&self, peer: PeerId);

    async fn has_pending_files_for(&self, peer: &PeerId) -> bool;

    async fn cancel_file(&self, file_id: FylesId) -> Option<PeerId>;

    /// Returns the peer id for which a file was cancelled, if any
    async fn cancel_file_by_target(
        &self,
        target_filerequest_id: FylesId,
        peer_id: PeerId,
    ) -> Option<PeerId>;

    #[cfg(feature = "debug-server")]
    async fn get_debug_blob(&self) -> Box<dyn SerializeDebug + Send>;
}

#[async_trait]
pub trait FileTrackerPlugin {
    async fn pending_file_added_for_peer(&self, peer: PeerId, after_reset: bool);

    async fn pending_file_removed_for_peer(
        &mut self,
        peer: &PeerId,
        has_pending_files: bool,
        has_sending_files: bool,
    );

    async fn on_send_file_to_peer(&mut self, peer: PeerId);

    async fn interrupt_sending_to_peer(&mut self, peer: PeerId);

    async fn done_sending_to_peer(&mut self, peer: PeerId);

    #[cfg(feature = "debug-server")]
    async fn get_debug_blob(&self) -> Box<dyn SerializeDebug + Send>;
}

pub struct NoOpFileTrackerPlugin;

#[async_trait]
impl FileTrackerPlugin for NoOpFileTrackerPlugin {
    async fn pending_file_added_for_peer(&self, _peer: PeerId, _after_reset: bool) {
        // do nothing
    }

    async fn pending_file_removed_for_peer(
        &mut self,
        _peer: &PeerId,
        _has_pending_files: bool,
        _has_sending_files: bool,
    ) {
        // do nothing
    }

    async fn on_send_file_to_peer(&mut self, _peer: PeerId) {
        // do nothing
    }

    async fn interrupt_sending_to_peer(&mut self, _peer: PeerId) {
        // do nothing
    }

    async fn done_sending_to_peer(&mut self, _peer: PeerId) {
        // do nothing
    }

    #[cfg(feature = "debug-server")]
    async fn get_debug_blob(&self) -> Box<dyn SerializeDebug + Send> {
        Box::new(NoContent)
    }
}

pub struct PeerFiles {
    pending: VecDeque<PendingFile>,
    sending: Option<SendSlot<usize>>,
    wire: PeerWire,
    span: Span,
}

impl Default for PeerFiles {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            sending: None,
            wire: PeerWire::Streams,
            span: debug_span!(parent: None, "Peer send"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    Chunks,
    Streams,
}

/// A peer that lacks the stream protocol is served chunks for a while, then asked again, so an
/// update on its side is noticed.
enum PeerWire {
    Streams,
    ChunksUntil(Instant),
}

impl PeerWire {
    fn current(&self) -> Wire {
        match self {
            PeerWire::ChunksUntil(until) if Instant::now() < *until => Wire::Chunks,
            _ => Wire::Streams,
        }
    }
}

#[derive(Debug)]
pub enum WireRequest {
    Chunks(FileRequest),
    Streams(FileRequestV2),
}

impl From<FileRequest> for WireRequest {
    fn from(request: FileRequest) -> Self {
        WireRequest::Chunks(request)
    }
}

impl From<FileRequestV2> for WireRequest {
    fn from(request: FileRequestV2) -> Self {
        WireRequest::Streams(request)
    }
}

fn abort_over(wire: Wire, transfer_uuid: Option<FylesId>) -> WireRequest {
    match wire {
        Wire::Chunks => FileRequest::AbortTransfer { transfer_uuid }.into(),
        Wire::Streams => FileRequestV2::AbortTransfer { transfer_uuid }.into(),
    }
}

fn offer_over(
    wire: Wire,
    filerequest_id: FylesId,
    file_name: String,
    transfer_data: &TransferData,
) -> WireRequest {
    let file_size_bytes = transfer_data.file_size_bytes;
    let transfer_uuid = transfer_data.transfer_id.clone();
    match wire {
        Wire::Chunks => FileRequest::NewTransfer {
            filerequest_id,
            file_name,
            file_size_bytes,
            transfer_uuid,
        }
        .into(),
        Wire::Streams => FileRequestV2::NewTransfer {
            filerequest_id,
            file_name,
            file_size_bytes,
            transfer_uuid,
        }
        .into(),
    }
}

fn continue_over(wire: Wire, filerequest_id: FylesId, transfer_uuid: FylesId) -> WireRequest {
    match wire {
        Wire::Chunks => FileRequest::ContinueTransfer {
            filerequest_id,
            transfer_uuid,
        }
        .into(),
        Wire::Streams => FileRequestV2::ContinueTransfer {
            filerequest_id,
            transfer_uuid,
        }
        .into(),
    }
}

fn slot_over(
    wire: Wire,
    pending_file: PendingFile,
    transfer_id: FylesId,
    source: Source,
    pin: ContactPin,
) -> SendSlot<usize> {
    match wire {
        Wire::Chunks => SendSlot::chunked(pending_file, transfer_id, source, pin),
        Wire::Streams => SendSlot::streamed(pending_file, transfer_id, source, pin),
    }
}

fn violation_over(wire: Wire, transfer_uuid: Option<FylesId>) -> WireRequest {
    match wire {
        Wire::Chunks => FileRequest::ProtocolViolation { transfer_uuid }.into(),
        Wire::Streams => FileRequestV2::ProtocolViolation { transfer_uuid }.into(),
    }
}

fn wire_of(sending: &SendSlot<usize>) -> Wire {
    match sending.path() {
        DataPath::Chunked(_) => Wire::Chunks,
        DataPath::Stream(_) => Wire::Streams,
    }
}

/// What a stream attempt needs once the receiver has admitted it.
pub struct AttemptStart {
    pub grant: AttemptGrant,
    pub source: Source,
    pub offset: u64,
    pub progress: Arc<AtomicU64>,
    pub cancel: CancellationToken,
    pub pending_file_id: FylesId,
    pub transfer_data: TransferData,
}

pub type StartAttemptFn = Box<dyn Fn(PeerId, AttemptStart) + Send + Sync>;

/// Work that has to wait until the `files` lock is released. The plugin lock in particular is
/// taken only afterwards, since `provider_query_completed` takes the two the other way round.
enum AfterUnlock {
    SendChunks {
        connected: Option<ContactId>,
    },
    Interrupted {
        pending_file_id: FylesId,
        status: SendStatus,
        reason: &'static str,
    },
    ReportAndSendChunks {
        pending_file_id: FylesId,
        status: SendStatus,
        connected: Option<ContactId>,
    },
    StartAttempt(Box<AttemptStart>),
}

/// Takes the send slot out of `peer`'s entry, together with what `request_id` asked of it. `None`
/// when the current transfer never sent that request, for example because an earlier attempt did.
fn claim_for_current_transfer(
    files: &mut HashMap<PeerId, PeerFiles>,
    peer: PeerId,
    request_id: Result<usize, ()>,
) -> Option<(
    OccupiedEntry<'_, PeerId, PeerFiles>,
    SendSlot<usize>,
    Request,
)> {
    let Ok(request_id) = request_id else {
        return None;
    };
    let Entry::Occupied(mut entry) = files.entry(peer) else {
        return None;
    };
    let peer_files = entry.get_mut();
    let mut sending = peer_files.sending.take()?;
    let Some(request) = sending.claim(&request_id) else {
        peer_files.sending = Some(sending);
        return None;
    };
    Some((entry, sending, request))
}

pub type SendFn = Box<
    dyn Fn(
            PeerId,
            Option<ContactId>,
            WireRequest,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            usize,
                            SessionSendError<SerCryptError, Arc<SessionEstablishmentError>>,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

pub type SpanSendFn = Box<
    dyn Fn(
            PeerId,
            Option<ContactId>,
            WireRequest,
            Span,
        ) -> Instrumented<
            Pin<
                Box<
                    dyn Future<
                            Output = Result<
                                usize,
                                SessionSendError<SerCryptError, Arc<SessionEstablishmentError>>,
                            >,
                        > + Send,
                >,
            >,
        > + Send
        + Sync,
>;

/// Tracks files being sent to peers, managing the lifecycle of pending → sending → done.
///
/// # Lock ordering
///
/// When a `FileTrackerPlugin` (e.g. `KademliaFileTrackerPlugin`) introduces its own
/// locks, all code must acquire locks in the following order to prevent deadlocks:
///
/// **`files` → `plugin` → (plugin-internal locks, e.g. `query_map`)**
///
/// Most `CoreFileTrackerRef` methods are called while the caller holds the `files`
/// write lock and then acquire `plugin` internally, establishing this order.
/// Any code that acquires `plugin` first (e.g. `KademliaExt` methods) must **not**
/// subsequently acquire `files` while still holding `plugin`.
pub struct CoreFileTracker<P: FileTrackerPlugin = NoOpFileTrackerPlugin> {
    files: Arc<RwLock<HashMap<PeerId, PeerFiles>>>,
    brain_action_sender: mpsc::Sender<BrainAction>,
    send: SendFn,
    send_idempotent: SendFn,
    start_attempt: StartAttemptFn,
    pub plugin: Arc<Mutex<P>>,
}

/// Put the thing back. Otherwise attackers can permanently stuck this app. Someone passing as the
/// peer may delay a transfer, but must neither wedge it nor decide its outcome.
fn put_back(entry: &mut OccupiedEntry<'_, PeerId, PeerFiles>, sending: SendSlot<usize>) {
    entry.get_mut().sending = Some(sending);
}

macro_rules! light_ref {
    ($tracker:expr) => {{
        CoreFileTrackerRef {
            brain_action_sender: &$tracker.brain_action_sender,
            send: &$tracker.send,
            send_idempotent: &$tracker.send_idempotent,
            plugin: &$tracker.plugin,
        }
    }};
}

impl<P: FileTrackerPlugin> CoreFileTracker<P> {
    #[allow(clippy::type_complexity)]
    fn send_instrumented(
        &self,
        peer_id: PeerId,
        contact_id: Option<ContactId>,
        request: WireRequest,
        span: Span,
        path: Option<&str>,
    ) -> Instrumented<
        Pin<
            Box<
                dyn Future<
                        Output = Result<
                            usize,
                            SessionSendError<SerCryptError, Arc<SessionEstablishmentError>>,
                        >,
                    > + Send,
            >,
        >,
    > {
        let future = span.in_scope(|| {
            debug!(peer=?peer_id, contact_id=?contact_id, ?request, ?path, "Sending filerequest message");
            (self.send)(peer_id, contact_id, request)
        });
        future.instrument(span)
    }

    #[allow(clippy::type_complexity)]
    fn send_idempotent_instrumented(
        &self,
        peer_id: PeerId,
        contact_id: Option<ContactId>,
        request: WireRequest,
        span: Span,
        path: Option<&str>,
    ) -> Instrumented<
        Pin<
            Box<
                dyn Future<
                        Output = Result<
                            usize,
                            SessionSendError<SerCryptError, Arc<SessionEstablishmentError>>,
                        >,
                    > + Send,
            >,
        >,
    > {
        let future = span.in_scope(|| {
            debug!(peer=?peer_id, contact_id=?contact_id, ?request, ?path, "Sending filerequest message (idempotent)");
            (self.send_idempotent)(peer_id, contact_id, request)
        });
        future.instrument(span)
    }
}

struct CoreFileTrackerRef<'a, P: FileTrackerPlugin = NoOpFileTrackerPlugin> {
    brain_action_sender: &'a mpsc::Sender<BrainAction>,
    send: &'a SendFn,
    #[allow(dead_code)] // may be needed in the future
    send_idempotent: &'a SendFn,
    plugin: &'a Arc<Mutex<P>>,
}

impl<'a, P: FileTrackerPlugin + Send> CoreFileTrackerRef<'a, P> {
    async fn send_next(
        &mut self,
        node_id: PeerId,
        mut entry: OccupiedEntry<'a, PeerId, PeerFiles>,
    ) {
        let span = entry.get().span.clone();
        async move {
            debug!("Sending next file");
            let peer_files = entry.get_mut();
            if peer_files.sending.is_some() {
                error!("Attempting to send next file for peer that is already in sending state")
            }
            let Some(mut next_to_send) = peer_files.pending.pop_front() else {
                debug!(peer=?node_id, "No more files to send");
                entry.remove();
                self.plugin
                    .timeout_lock()
                    .await
                    .done_sending_to_peer(node_id)
                    .await;
                return;
            };

            let Ok(opening) = send_decision::opening(&next_to_send.status) else {
                error!(peer=?node_id, file=?next_to_send, "Pending file has a status it cannot be sent with, failing it");
                self.complete_file(entry, next_to_send.id.clone(), FileOutcome::Failed).await;
                return;
            };

            let pin = match self.starting_pin(&next_to_send.contact_id).await {
                Ok(pin) => pin,
                Err(message) => {
                    error!(peer=?node_id, message, "Could not tell whether we hold the target's keys, setting back to pending");
                    peer_files.pending.push_front(next_to_send);
                    return;
                }
            };

            let (request, response) = BrainRequest::with_receiver(OpenFileForReadingRequest {
                uri: next_to_send.file_path.clone().into(),
                id: next_to_send.id.clone(),
                byte_offset: opening.offset(),
            });
            self.brain_action_sender
                .send(BrainAction::NetworkNode(
                    NetworkNodeAction::OpenFileForReading(request),
                ))
                .await
                .expect("Sending to work");
            let Ok(file) = response.await.expect("Sender not to be dropped") else {
                error!(peer=?node_id, file=?next_to_send, "Could not open file for reading");
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(NetworkNodeAction::FileMissing {
                        pending_file_id: next_to_send.id.clone(),
                    }))
                    .await
                    .expect("Sending to work");
                Box::pin(self.send_next(node_id, entry)).await;
                return;
            };
            let source = match Source::open(file.file).await {
                Err(e) => {
                    error!(peer=?node_id, file=?next_to_send, error=?e, "Could not inspect the source file");
                    self.brain_action_sender
                        .send(BrainAction::NetworkNode(NetworkNodeAction::FileMissing {
                            pending_file_id: next_to_send.id.clone(),
                        }))
                        .await
                        .expect("Sending to work");
                    Box::pin(self.send_next(node_id, entry)).await;
                    return;
                }
                Ok(source) => source,
            };
            let stamp = source.stamp();
            let wire = peer_files.wire.current();
            let opening_request = opening.request();

            trace!(status = ?next_to_send.status, ?wire, "Next to send status");

            let filerequest_id = next_to_send.target_filerequest_id.clone();
            let (request, transfer_data) = match opening {
                Opening::Offer => {
                    let transfer_data = TransferData {
                        progress_bytes: 0,
                        file_size_bytes: stamp.size_bytes,
                        source_modified_ms: stamp.modified_ms,
                        transfer_id: FylesId::default(),
                    };
                    let offer = offer_over(
                        wire,
                        filerequest_id,
                        file.file_name,
                        &transfer_data,
                    );
                    (offer, transfer_data)
                }
                Opening::Continue(transfer_data) => {
                    if stamp != SourceStamp::recorded_in(&transfer_data) {
                        info!(peer=?node_id, file=?next_to_send, ?stamp, "Source file changed since the interrupted transfer, starting over");
                        drop(source);
                        self.restart_as_new(
                            entry,
                            next_to_send,
                            transfer_data.transfer_id,
                            wire,
                            "Source file changed since the interrupted transfer",
                        )
                        .await;
                        return;
                    }
                    let continuation =
                        continue_over(wire, filerequest_id, transfer_data.transfer_id.clone());
                    (continuation, transfer_data)
                }
            };
            let Ok(request_id) = (self.send)(node_id, Some(next_to_send.contact_id.clone()), request).await else {
                warn!(peer=?node_id, ?opening_request, "Error trying to send, setting back to pending");
                peer_files.pending.push_front(next_to_send);
                return;
            };
            let transfer_id = transfer_data.transfer_id.clone();
            next_to_send.status = InProgress {
                status: InProgressSendStatus::Prepared,
                transfer_data,
            }
            .into();
            let mut slot = slot_over(wire, next_to_send, transfer_id, source, pin);
            slot.await_response(request_id, opening_request);
            debug!(peer=?node_id, pending_file=?slot.pending_file, "Sent the opening request, setting `sending`");
            peer_files.sending = Some(slot);

            self.plugin
                .timeout_lock()
                .await
                .on_send_file_to_peer(node_id)
                .await;
        }.instrument(span).await
    }

    async fn starting_pin(&self, target: &ContactId) -> Result<ContactPin, String> {
        let (request, response) = BrainRequest::with_receiver(target.clone());
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::HoldsContactKeys(request),
            ))
            .await
            .expect("Sending to work");
        Ok(match response.await.expect("Sender not to be dropped")? {
            true => ContactPin::Pinned,
            false => ContactPin::Unpinned,
        })
    }

    async fn restart_as_new(
        &mut self,
        mut entry: OccupiedEntry<'a, PeerId, PeerFiles>,
        mut file: PendingFile,
        abandoned: FylesId,
        wire: Wire,
        reason: &str,
    ) {
        let node_id = *entry.key();
        let _ = (self.send)(
            node_id,
            Some(file.contact_id.clone()),
            abort_over(wire, Some(abandoned)),
        )
        .await;
        file.status = SendStatus::Pending;
        self.brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::FileSendReset {
                pending_file_id: file.id.clone(),
                status: SendStatus::Pending,
                retry_count: None,
                reason: Some(reason.into()),
            }))
            .await
            .expect("Sending to work");
        entry.get_mut().pending.push_front(file);
        // Not awaiting the confirmation is safe: the abort names the abandoned transfer, so in
        // whichever order the receiver sees the two, it cannot take down the new one.
        Box::pin(self.send_next(node_id, entry)).await;
    }

    /// Terminally complete a file: notify the brain, then advance to the next file.
    ///
    /// The sending slot must already be cleared (or the file was never in it, e.g.
    /// popped from pending in `send_next`). This method sends exactly one brain
    /// notification and then calls `send_next` for any remaining pending files.
    async fn complete_file(
        &mut self,
        entry: OccupiedEntry<'a, PeerId, PeerFiles>,
        file_id: FylesId,
        outcome: FileOutcome,
    ) {
        let node_id = *entry.key();
        let span = entry.get().span.clone();
        async move {
            debug!(file_id=?file_id, ?outcome, "File completed");
            let brain_action = match outcome {
                FileOutcome::Sent => NetworkNodeAction::FileSent {
                    pending_file_id: file_id,
                },
                FileOutcome::Failed => NetworkNodeAction::FileFailed {
                    pending_file_id: file_id,
                },
                FileOutcome::Missing => NetworkNodeAction::FileMissing {
                    pending_file_id: file_id,
                },
                FileOutcome::Rejected => NetworkNodeAction::FileRejected {
                    pending_file_id: file_id,
                },
                FileOutcome::IoError(error) => NetworkNodeAction::FileIoError {
                    file: file_id,
                    error,
                },
            };
            self.brain_action_sender
                .send(BrainAction::NetworkNode(brain_action))
                .await
                .expect("Sending to work");
            Box::pin(self.send_next(node_id, entry)).await;
        }
        .instrument(span)
        .await
    }

    /// Reset the sending file back to pending and stop sending (do NOT call `send_next`).
    ///
    /// Use this when we want to add delay before retrying, e.g. after a remote
    /// `InternalError`. The file is pushed back into the pending queue, the brain
    /// is notified with `FileSendReset`, and the plugin is told to interrupt and
    /// re-schedule (e.g. start a new kademlia query, adding natural delay).
    async fn reset_and_stop(
        &mut self,
        mut entry: OccupiedEntry<'a, PeerId, PeerFiles>,
        mut sending: SendSlot<usize>,
        reason: String,
    ) {
        let span = entry.get().span.clone();
        let peer = *entry.key();
        async move {
            let reset_status = send_decision::reset_status(&mut sending);
            debug!(new_status=?reset_status, "reset_and_stop: resetting in brain, interrupting plugin");
            sending.pending_file.status = reset_status.clone();
            self
                .brain_action_sender
                .send(BrainAction::NetworkNode(NetworkNodeAction::FileSendReset {
                    pending_file_id: sending.pending_file.id.clone(),
                    status: reset_status,
                    retry_count: Some(sending.pending_file.retry_count),
                    reason: Some(reason),
                }))
                .await
                .expect("Sending to work");
            entry.get_mut().pending.push_back(sending.pending_file);
            let mut plugin = self.plugin.timeout_lock().await;
            plugin.interrupt_sending_to_peer(peer).await;
            plugin.pending_file_added_for_peer(peer, true).await;
        }.instrument(span).await
    }

    /// Counts the violation and fails the file once the count passes a limit that grows with the
    /// file's size. Only a violation the authenticated peer could cause may come here, or anyone on
    /// the path could fail the transfer.
    async fn count_violation(
        &mut self,
        entry: OccupiedEntry<'a, PeerId, PeerFiles>,
        mut sending: SendSlot<usize>,
        reason: String,
    ) {
        sending.pending_file.retry_count += 1;
        let span = entry.get().span.clone();
        let file_size = sending.get_file_size_bytes();
        let gigabyte = 1_000_000_000;
        let large_file_extra_errors = file_size / gigabyte;
        let max_allowed_failures = 1 + large_file_extra_errors as usize;
        async move {
            if sending.pending_file.retry_count > 1 {
                warn!(pending_file=?sending.pending_file, retry_count = sending.pending_file.retry_count, ?max_allowed_failures, "Repeated failure");
            }
            if sending.pending_file.retry_count > max_allowed_failures {
                debug!(
                    retry_count = sending.pending_file.retry_count,
                    "Max retries exceeded, completing as failed"
                );
                let file_id = sending.pending_file.id.clone();
                // sending is dropped here — the file is terminal
                self.complete_file(entry, file_id, FileOutcome::Failed).await;
            } else {
                self.reset_and_stop(entry, sending, reason).await;
            }
        }
        .instrument(span)
        .await;
    }
}

pub fn new_core_file_tracker(
    brain_action_sender: mpsc::Sender<BrainAction>,
    send: SendFn,
    send_idempotent: SendFn,
    start_attempt: StartAttemptFn,
) -> CoreFileTracker<NoOpFileTrackerPlugin> {
    CoreFileTracker {
        files: Default::default(),
        brain_action_sender,
        send,
        send_idempotent,
        start_attempt,
        plugin: Mutex::new(NoOpFileTrackerPlugin {}).into(),
    }
}

impl<P: FileTrackerPlugin + Send> CoreFileTracker<P> {
    pub fn with_plugin(
        brain_action_sender: mpsc::Sender<BrainAction>,
        send: SendFn,
        send_idempotent: SendFn,
        start_attempt: StartAttemptFn,
        plugin: P,
    ) -> Self {
        Self {
            files: Default::default(),
            brain_action_sender,
            send,
            send_idempotent,
            start_attempt,
            plugin: Mutex::new(plugin).into(),
        }
    }

    pub async fn has_pending_files_for(&self, peer: &PeerId) -> bool {
        if let Some(peer_files) = self.files.timeout_read().await.get(peer) {
            !peer_files.pending.is_empty()
        } else {
            false
        }
    }

    /// Sends chunks until the window is full, the last chunk is out, or the transfer stops.
    async fn send_chunks_loop(&self, node_id: PeerId, connected_contact_id: &Option<ContactId>) {
        loop {
            let mut files = self.files.timeout_write().await;
            let Entry::Occupied(mut entry) = files.entry(node_id) else {
                // Could be that a parallel receive thread has finished the transfer, so this
                // is a valid case
                break;
            };
            let span = entry.get().span.clone();
            let Some(mut sending) = entry.get_mut().sending.take() else {
                span.in_scope(|| trace!("No send data, exiting send loop"));
                break;
            };
            let Some(SendDataMut { path, pending_file }) =
                sending.advance_for(connected_contact_id)
            else {
                warn!(expected=?sending.pending_file.contact_id, got=?connected_contact_id, "Incorrect contact id for file send in loop. Aborting.");
                light_ref!(self)
                    .reset_and_stop(entry, sending, "Contact verification failed".into())
                    .await;
                return;
            };
            let PendingFile {
                status,
                file_path,
                contact_id,
                ..
            } = pending_file;
            let (
                Some(send_manager),
                SendStatus::InProgress(InProgress {
                    status: phase @ InProgressSendStatus::Sending,
                    ..
                }),
            ) = (path.chunked_mut(), status)
            else {
                span.in_scope(|| trace!("Not sending chunks, exiting send loop"));
                put_back(&mut entry, sending);
                break;
            };
            let res = send_manager
                .send_next_chunk(|chunk| {
                    self.send_idempotent_instrumented(
                        node_id,
                        Some(contact_id.clone()),
                        FileRequest::Chunk(chunk).into(),
                        span.clone(),
                        Some(file_path.as_str()),
                    )
                })
                .await;
            let verdict = match send_decision::on_chunk_send(res) {
                ChunkStep::Wait => {
                    span.in_scope(|| trace!("File reader not ready"));
                    put_back(&mut entry, sending);
                    break;
                }
                ChunkStep::Continue => {
                    span.in_scope(|| trace!(peer=?node_id, "Sent chunk"));
                    put_back(&mut entry, sending);
                    continue;
                }
                ChunkStep::SendDone => {
                    span.in_scope(|| debug!("File reader is done"));
                    let done = FileRequest::Done {
                        transfer_uuid: send_manager.transfer_id.clone(),
                    };
                    match self
                        .send_instrumented(
                            node_id,
                            Some(contact_id.clone()),
                            done.into(),
                            span.clone(),
                            Some(file_path.as_str()),
                        )
                        .await
                    {
                        Ok(request_id) => {
                            send_manager.await_response(request_id, ControlRequest::Done);
                            *phase = InProgressSendStatus::PendingSent;
                            put_back(&mut entry, sending);
                            break;
                        }
                        Err(_) => Verdict::SetBack("Could not send done".into()),
                    }
                }
                ChunkStep::Stop(verdict) => verdict,
            };
            let after = self
                .carry_out(entry, sending, verdict)
                .instrument(span.clone())
                .await;
            drop(files);
            if let Some(after) = after {
                self.after_unlock(node_id, after).instrument(span).await;
            }
            break;
        }
    }

    async fn receive(
        &self,
        node_id: PeerId,
        contact_id: Option<ContactId>,
        request_id: Result<usize, ()>,
        response: Response,
    ) {
        trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
        let mut files = self.files.timeout_write().await;
        let Some((entry, mut sending, request)) =
            claim_for_current_transfer(&mut files, node_id, request_id)
        else {
            self.on_unclaimed(files, node_id, request_id, response)
                .await;
            return;
        };
        let span = entry.get().span.clone();
        let verdict = span
            .in_scope(|| send_decision::on_response(&mut sending, &contact_id, request, response));
        let after = self
            .carry_out(entry, sending, verdict)
            .instrument(span.clone())
            .await;
        drop(files);
        if let Some(after) = after {
            self.after_unlock(node_id, after).instrument(span).await;
        }
    }

    async fn on_unclaimed(
        &self,
        mut files: RwLockWriteGuard<'_, HashMap<PeerId, PeerFiles>>,
        node_id: PeerId,
        request_id: Result<usize, ()>,
        response: Response,
    ) {
        match response {
            Response::ConfirmAbort => {
                let Entry::Occupied(entry) = files.entry(node_id) else {
                    debug!(peer=?node_id, "Abort confirmed, but we don't know for what");
                    return;
                };
                entry.get().span.in_scope(|| {
                    debug!(peer=?node_id, ?request_id, "Abort confirmed");
                });
                if entry.get().sending.is_none() {
                    light_ref!(self).send_next(node_id, entry).await;
                } else {
                    debug!(peer=?node_id, "Abort confirmed while a newer transfer is under way");
                }
            }
            Response::AcknowledgeViolation => {
                debug!(peer=?node_id, "Other end acknowledged protocol violation");
            }
            response => debug!(
                peer=?node_id,
                ?request_id,
                ?response,
                "Response to a request the current transfer did not send, ignoring"
            ),
        }
    }

    /// Carries out a verdict while the `files` lock is held, and hands back what has to wait until
    /// the lock is released.
    async fn carry_out(
        &self,
        mut entry: OccupiedEntry<'_, PeerId, PeerFiles>,
        sending: SendSlot<usize>,
        verdict: Verdict,
    ) -> Option<AfterUnlock> {
        match verdict {
            Verdict::Complete(outcome) => {
                light_ref!(self)
                    .complete_file(entry, sending.pending_file.id, outcome)
                    .await;
                None
            }
            Verdict::SetBack(reason) => {
                light_ref!(self)
                    .reset_and_stop(entry, sending, reason)
                    .await;
                None
            }
            Verdict::Violation(Party::Us, reason) => {
                light_ref!(self)
                    .count_violation(entry, sending, reason.into())
                    .await;
                None
            }
            Verdict::Violation(Party::Peer, reason) => {
                self.violated_by_peer(entry, sending, reason).await;
                None
            }
            Verdict::RestartAsNew(reason) => {
                let abandoned = sending.get_transfer_id().clone();
                let wire = wire_of(&sending);
                light_ref!(self)
                    .restart_as_new(entry, sending.pending_file, abandoned, wire, reason)
                    .await;
                None
            }
            Verdict::AbortAndComplete(outcome) => {
                let peer = *entry.key();
                let span = entry.get().span.clone();
                let abort = abort_over(wire_of(&sending), Some(sending.get_transfer_id().clone()));
                let _ = self
                    .send_instrumented(
                        peer,
                        Some(sending.pending_file.contact_id.clone()),
                        abort,
                        span,
                        Some(sending.pending_file.file_path.as_str()),
                    )
                    .await;
                light_ref!(self)
                    .complete_file(entry, sending.pending_file.id, outcome)
                    .await;
                None
            }
            Verdict::Interrupt(reason) => {
                let mut sending = sending;
                let status = send_decision::reset_status(&mut sending);
                let pending_file_id = sending.pending_file.id.clone();
                sending.pending_file.status = status.clone();
                entry.get_mut().pending.push_back(sending.pending_file);
                Some(AfterUnlock::Interrupted {
                    pending_file_id,
                    status,
                    reason,
                })
            }
            Verdict::FallBackToChunks => {
                self.fall_back_to_chunks(entry, sending).await;
                None
            }
            Verdict::SendChunks { connected } => {
                put_back(&mut entry, sending);
                Some(AfterUnlock::SendChunks { connected })
            }
            Verdict::ChunkConfirmed { connected } => {
                let pending_file_id = sending.pending_file.id.clone();
                let status = sending.pending_file.status.clone();
                put_back(&mut entry, sending);
                Some(AfterUnlock::ReportAndSendChunks {
                    pending_file_id,
                    status,
                    connected,
                })
            }
            Verdict::StartAttempt(start) => {
                put_back(&mut entry, sending);
                Some(AfterUnlock::StartAttempt(start))
            }
        }
    }

    async fn after_unlock(&self, peer: PeerId, after: AfterUnlock) {
        match after {
            AfterUnlock::SendChunks { connected } => {
                Box::pin(self.send_chunks_loop(peer, &connected)).await
            }
            AfterUnlock::Interrupted {
                pending_file_id,
                status,
                reason,
            } => {
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(NetworkNodeAction::FileSendReset {
                        pending_file_id,
                        status,
                        retry_count: None,
                        reason: Some(format!("Send interrupted: {reason}")),
                    }))
                    .await
                    .expect("Sending to work");
                self.plugin
                    .timeout_lock()
                    .await
                    .interrupt_sending_to_peer(peer)
                    .await;
            }
            AfterUnlock::ReportAndSendChunks {
                pending_file_id,
                status,
                connected,
            } => {
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(NetworkNodeAction::FileSending {
                        pending_file_id,
                        status,
                    }))
                    .await
                    .expect("Sending to work");
                Box::pin(self.send_chunks_loop(peer, &connected)).await;
            }
            AfterUnlock::StartAttempt(start) => {
                debug!(
                    ?peer,
                    offset = start.offset,
                    "Receiver admitted a stream attempt"
                );
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(NetworkNodeAction::FileSending {
                        pending_file_id: start.pending_file_id.clone(),
                        status: SendStatus::InProgress(InProgress {
                            status: InProgressSendStatus::Sending,
                            transfer_data: start.transfer_data.clone(),
                        }),
                    }))
                    .await
                    .expect("Sending to work");
                (self.start_attempt)(peer, *start);
            }
        }
    }

    async fn violated_by_peer(
        &self,
        entry: OccupiedEntry<'_, PeerId, PeerFiles>,
        sending: SendSlot<usize>,
        reason: &str,
    ) {
        let peer = *entry.key();
        let span = entry.get().span.clone();
        span.in_scope(|| warn!(reason, "Resetting on protocol violation"));
        // Named, so the peer drops this transfer rather than every transfer with us.
        let violation = violation_over(wire_of(&sending), Some(sending.get_transfer_id().clone()));
        let contact_id = Some(sending.pending_file.contact_id.clone());
        let path = sending.pending_file.file_path.clone();
        light_ref!(self)
            .count_violation(entry, sending, reason.into())
            .await;
        let _ = self
            .send_instrumented(peer, contact_id, violation, span, Some(&path))
            .await;
    }

    async fn fall_back_to_chunks(
        &self,
        mut entry: OccupiedEntry<'_, PeerId, PeerFiles>,
        mut sending: SendSlot<usize>,
    ) {
        let peer = *entry.key();
        let peer_files = entry.get_mut();
        peer_files.wire = PeerWire::ChunksUntil(Instant::now() + LEGACY_RECHECK);
        sending.pending_file.status = send_decision::reset_status(&mut sending);
        peer_files.pending.push_front(sending.pending_file);
        light_ref!(self).send_next(peer, entry).await;
    }

    pub async fn get_peers_with_pending_files(&self) -> Vec<PeerId> {
        self.files.timeout_read().await.keys().cloned().collect()
    }
}

#[async_trait]
impl<P: FileTrackerPlugin + Send + Sync> ResponseReceiver<FileResponse> for CoreFileTracker<P> {
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        node_id: PeerId,
        contact_id: Option<ContactId>,
        request_id: Result<usize, ()>,
        response: FileResponse,
    ) {
        self.receive(node_id, contact_id, request_id, response.into())
            .await
    }

    async fn handle_receive_error(
        &self,
        node_id: PeerId,
        request_id: Result<usize, ()>,
        error: ReceiveError<
            OutboundFailure,
            DeSerCryptError,
            SerCryptError,
            Arc<SessionEstablishmentError>,
        >,
    ) {
        warn!(peer=?node_id, ?error, ?request_id, "Error in filerequest protocol");
        let mut files = self.files.timeout_write().await;
        let Some((entry, sending, _request)) =
            claim_for_current_transfer(&mut files, node_id, request_id)
        else {
            debug!(peer=?node_id, ?error, ?request_id, "The failed request does not belong to the current transfer, ignoring");
            return;
        };
        let span = entry.get().span.clone();
        let verdict = span.in_scope(|| send_decision::on_receive_error(&sending, error));
        let after = self
            .carry_out(entry, sending, verdict)
            .instrument(span.clone())
            .await;
        drop(files);
        if let Some(after) = after {
            self.after_unlock(node_id, after).instrument(span).await;
        }
    }
}

#[async_trait]
impl<P: FileTrackerPlugin + Send + Sync> ResponseReceiver<FileResponseV2> for CoreFileTracker<P> {
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        node_id: PeerId,
        contact_id: Option<ContactId>,
        request_id: Result<usize, ()>,
        response: FileResponseV2,
    ) {
        self.receive(node_id, contact_id, request_id, response.into())
            .await
    }

    async fn handle_receive_error(
        &self,
        node_id: PeerId,
        request_id: Result<usize, ()>,
        error: ReceiveError<
            OutboundFailure,
            DeSerCryptError,
            SerCryptError,
            Arc<SessionEstablishmentError>,
        >,
    ) {
        ResponseReceiver::<FileResponse>::handle_receive_error(self, node_id, request_id, error)
            .await
    }
}

#[async_trait]
impl<P: FileTrackerPlugin + Send + Sync> FileTracker for CoreFileTracker<P> {
    async fn on_attempt_finished(&self, peer: PeerId, attempt: AttemptId, outcome: AttemptOutcome) {
        let mut files = self.files.timeout_write().await;
        let Entry::Occupied(mut entry) = files.entry(peer) else {
            debug!(
                ?peer,
                ?outcome,
                "Attempt finished for a peer we no longer send to"
            );
            return;
        };
        let Some(sending) = entry
            .get_mut()
            .sending
            .take_if(|it| it.running_attempt().is_some_and(|it| it.attempt == attempt))
        else {
            debug!(
                ?peer,
                ?outcome,
                "Attempt finished that is no longer current"
            );
            return;
        };
        let span = entry.get().span.clone();
        span.in_scope(|| debug!(?peer, ?outcome, "Stream attempt finished"));
        let verdict = send_decision::on_attempt_outcome(outcome);
        let after = self
            .carry_out(entry, sending, verdict)
            .instrument(span.clone())
            .await;
        drop(files);
        if let Some(after) = after {
            self.after_unlock(peer, after).instrument(span).await;
        }
    }

    async fn add_pending_file(&self, peer: PeerId, file: PendingFile) {
        let mut files = self.files.timeout_write().await;
        match file.status {
            SendStatus::Pending
            | SendStatus::InProgress(InProgress {
                status: InProgressSendStatus::Interrupted,
                ..
            }) => {
                let peer_files = files.entry(peer).or_default();
                peer_files.span.in_scope(|| {
                    info!(?peer, ?file, "Added file for sending");
                });
                peer_files.pending.push_back(file);
                drop(files);
                self.plugin
                    .timeout_lock()
                    .await
                    .pending_file_added_for_peer(peer, false)
                    .await;
            }
            _ => {
                error!(
                    ?file,
                    "Trying to add a pending file with invalid status, rejecting"
                );
            }
        }
    }

    async fn trigger_interaction_with(&self, peer: PeerId) {
        let mut files = self.files.timeout_write().await;
        trace!(
            "Triggering interaction with peer with these files: {:?}",
            files
                .values()
                .map(|it| (&it.pending, it.sending.is_some()))
                .collect::<Vec<_>>()
        );
        let Entry::Occupied(mut peer_files) = files.entry(peer) else {
            trace!(?peer, "No files for peer");
            return;
        };
        if let Some(sending) = peer_files.get_mut().sending.as_mut() {
            // This early return is what a wedged slot hides behind: while `sending` is occupied,
            // every future interaction with this peer is refused. A slot that is genuinely in
            // flight either advances or has its request time out well inside STALL_ERROR_AFTER, so
            // crossing that threshold is never a transient condition — it means the send path lost
            // track of the slot and left it occupied with nothing outstanding. Report it loudly
            // and leave it alone: repairing it here would hide the defect that caused it.
            if sending.should_report_stall(STALL_ERROR_AFTER) {
                error!(
                    ?peer,
                    transfer = ?sending.get_transfer_id(),
                    stalled_for = ?sending.stalled_for(),
                    held_for = ?sending.held_for(),
                    "Send slot has made no progress and is blocking every send to this peer. \
                     This is a bug in the send path, not a transient failure."
                );
            }
            peer_files.get().span.in_scope(|| {
                trace!(?peer, "Already sending files to peer");
            });
            return;
        } else {
            debug!(?peer, "Ready to interact with (send files to) peer");
        }

        light_ref!(self).send_next(peer, peer_files).await;
    }

    async fn has_pending_files_for(&self, peer: &PeerId) -> bool {
        if let Some(peer_files) = self.files.timeout_read().await.get(peer) {
            !peer_files.pending.is_empty()
        } else {
            false
        }
    }

    async fn cancel_file(&self, file_id: FylesId) -> Option<PeerId> {
        let mut files = self.files.timeout_write().await;
        // Find which peer has this file
        let peer = files.iter().find_map(|(peer, pf)| {
            let in_sending = pf
                .sending
                .as_ref()
                .is_some_and(|s| s.pending_file.id == file_id);
            let in_pending = pf.pending.iter().any(|p| p.id == file_id);
            (in_sending || in_pending).then_some(*peer)
        });
        let peer = peer?;
        let Entry::Occupied(mut entry) = files.entry(peer) else {
            return None;
        };

        let peer_files = entry.get_mut();

        // If in sending slot: take it, abort with peer, then send_next
        if peer_files
            .sending
            .as_ref()
            .is_some_and(|s| s.pending_file.id == file_id)
        {
            let sending = peer_files.sending.take().unwrap();
            let _ = self
                .send_instrumented(
                    peer,
                    Some(sending.pending_file.contact_id.clone()),
                    abort_over(wire_of(&sending), Some(sending.get_transfer_id().clone())),
                    peer_files.span.clone(),
                    peer_files
                        .sending
                        .as_ref()
                        .map(|it| it.pending_file.file_path.as_ref()),
                )
                .await;
            self.plugin
                .timeout_lock()
                .await
                .pending_file_removed_for_peer(&peer, !peer_files.pending.is_empty(), false)
                .await;
            // No brain notification — cancel is brain-initiated
            light_ref!(self).send_next(peer, entry).await;
            return Some(peer);
        }

        // If in pending queue: just remove it
        let len_before = peer_files.pending.len();
        peer_files.pending.retain(|it| it.id != file_id);
        if len_before != peer_files.pending.len() {
            self.plugin
                .timeout_lock()
                .await
                .pending_file_removed_for_peer(
                    &peer,
                    !peer_files.pending.is_empty(),
                    peer_files.sending.is_some(),
                )
                .await;
            return Some(peer);
        }
        None
    }

    // TODO: It should be possible to call this with the ID of the peer that this filerequest belongs to
    async fn cancel_file_by_target(
        &self,
        target_filerequest_id: FylesId,
        peer_id: PeerId,
    ) -> Option<PeerId> {
        let mut files = self.files.timeout_write().await;
        let Entry::Occupied(mut peer_files) = files.entry(peer_id) else {
            return None;
        };

        // If in sending slot: take it, abort with peer, then send_next
        if peer_files
            .get()
            .sending
            .as_ref()
            .is_some_and(|s| s.pending_file.target_filerequest_id == target_filerequest_id)
        {
            // Can unwrap because presence was just checked
            let sending = peer_files.get_mut().sending.take().unwrap();
            let _ = self
                .send_instrumented(
                    peer_id,
                    Some(sending.pending_file.contact_id.clone()),
                    abort_over(wire_of(&sending), Some(sending.get_transfer_id().clone())),
                    peer_files.get().span.clone(),
                    peer_files
                        .get()
                        .sending
                        .as_ref()
                        .map(|it| it.pending_file.file_path.as_ref()),
                )
                .await;
            self.plugin
                .timeout_lock()
                .await
                .pending_file_removed_for_peer(
                    &peer_id,
                    !peer_files.get().pending.is_empty(),
                    false,
                )
                .await;
            // No brain notification — cancel is brain-initiated
            light_ref!(self).send_next(peer_id, peer_files).await;
            return Some(peer_id);
        }

        // If in pending queue: just remove it
        let len_before = peer_files.get().pending.len();
        peer_files
            .get_mut()
            .pending
            .retain(|it| it.target_filerequest_id != target_filerequest_id);
        if len_before != peer_files.get().pending.len() {
            self.plugin
                .timeout_lock()
                .await
                .pending_file_removed_for_peer(
                    &peer_id,
                    !peer_files.get().pending.is_empty(),
                    peer_files.get().sending.is_some(),
                )
                .await;
            return Some(peer_id);
        }
        None
    }

    #[cfg(feature = "debug-server")]
    async fn get_debug_blob(&self) -> Box<dyn SerializeDebug + Send> {
        self.plugin.timeout_lock().await.get_debug_blob().await
    }
}
