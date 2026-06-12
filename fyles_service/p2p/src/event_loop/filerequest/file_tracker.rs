use crate::crypto::SessionEstablishmentError;
use crate::event_loop::filerequest::send_manager::{
    LockedSendData, SendDataMut, SendRes,
};
use crate::send_receive_traits::response_receiver::ResponseReceiver;
use crate::send_receive_traits::session_send_receive::{
    LocalReceiveError, ReceiveError, SessionSendError,
};
use crate::types::{FileRequest, FileResponse};
use async_trait::async_trait;
use crypto::{DeSerCryptError, SerCryptError};
use fyles_core::core::brain::action_p2p::OpenFileForReadingRequest;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::{ContactId, InProgress, InProgressSendStatus, TransferData};
use fyles_core::core::{
    brain::{action::BrainAction, action_p2p::NetworkNodeAction},
    domain_models::{FylesId, PendingFile, SendStatus},
};
use fyles_core::library::util::util::TimeoutLock;
use libp2p::request_response::OutboundFailure;
use libp2p::{Multiaddr, PeerId};
use std::collections::VecDeque;
use std::collections::hash_map::OccupiedEntry;
use std::pin::Pin;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock, mpsc};
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
    >
{
    async fn add_pending_file(&self, peer: PeerId, file: PendingFile);

    async fn trigger_interaction_with(&self, peer: PeerId);

    async fn has_pending_files_for(&self, peer: &PeerId) -> bool;

    async fn has_sending_files_for(&self, peer: &PeerId) -> bool;

    async fn cancel_file(&self, file_id: FylesId) -> Option<PeerId>;

    /// Returns the peer id for which a file was cancelled, if any
    async fn cancel_file_by_target(
        &self,
        target_filerequest_id: FylesId,
        peer_id: PeerId,
    ) -> Option<PeerId>;

    /// Reset any active sending state for a peer (e.g., on connection loss).
    /// Files in sending state are moved back to the pending queue.
    async fn reset_for_disconnected_peer(&self, peer: PeerId);

    async fn handle_outgoing_connection_error(&self, peer: PeerId, address: &Multiaddr);

    async fn handle_non_correlatable_error(&self, peer: PeerId);
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

    async fn outgoing_connection_error(&mut self, peer: &PeerId, multiaddr: &Multiaddr);

    async fn non_correlatable_outgoing_connection_error(&mut self, peer: &PeerId);

    async fn on_send_file_to_peer(&mut self, peer: PeerId);

    async fn interrupt_sending_to_peer(&mut self, peer: PeerId);

    async fn done_sending_to_peer(&mut self, peer: PeerId);
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

    async fn outgoing_connection_error(&mut self, _peer: &PeerId, _multiaddr: &Multiaddr) {
        // do nothing
    }

    async fn non_correlatable_outgoing_connection_error(&mut self, _peer: &PeerId) {
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
}

pub struct PeerFiles {
    pending: VecDeque<PendingFile>,
    sending: Option<LockedSendData<usize>>,
    span: Span,
}

impl Default for PeerFiles {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            sending: None,
            span: debug_span!(parent: None, "Peer send"),
        }
    }
}

pub type SendFn = Box<
    dyn Fn(
            PeerId,
            Option<ContactId>,
            FileRequest,
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
            FileRequest,
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
    pub plugin: Arc<Mutex<P>>,
}

#[derive(Debug)]
pub enum ContactVerificationResult {
    Verified,
    ContactIsUnknown,
    VerificationFailed,
    InternalError,
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
    fn send_instrumented(
        &self,
        peer_id: PeerId,
        contact_id: Option<ContactId>,
        request: FileRequest,
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

    fn send_idempotent_instrumented(
        &self,
        peer_id: PeerId,
        contact_id: Option<ContactId>,
        request: FileRequest,
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

    async fn verify_target_contact_id(
        &self,
        target_node_id: PeerId,
        target_contact_id: &ContactId,
        connected_contact_id: &Option<ContactId>,
    ) -> ContactVerificationResult
    where
        P: Send,
    {
        let target_filerequest_contact_id = target_contact_id;
        let (request, response) =
            BrainRequest::with_receiver(target_filerequest_contact_id.clone());
        self.brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::IsContactKnown(
                request,
            )))
            .await
            .expect("Sending to work");
        let target_contact_is_known_res = response.await.expect("Sender not dropped");

        match target_contact_is_known_res {
            Err(message) => {
                error!(peer=?target_node_id, message, "Error while trying to verify contact. Sending internal error");
                self.reset_all_on_internal_error_for(target_node_id).await;
                ContactVerificationResult::InternalError
            }
            Ok(target_contact_is_known) => {
                if target_contact_is_known {
                    trace!("Target contact is known, so MUST verify that IDs match");
                    if !connected_contact_id
                        .as_ref()
                        .is_some_and(|id| id == target_filerequest_contact_id)
                    {
                        match connected_contact_id {
                            Some(evil_id) => {
                                // This means that either somebody we know is impersonating the peer behind that libp2p PeerId, or that peer has since change its contact id and the filerequest has become invalid.
                                warn!(peer=?target_node_id, filerequest_target_contact_id=?target_filerequest_contact_id, reached_contact_id=?evil_id, "Tried to dial a peer to send a file, but reached a contact that does not belong to the target filerequest.")
                            }
                            None => {
                                // This means that either somebody we don't know is impersonating the peer behind that libp2p PeerId, or that peer has since change its contact id and the filerequest has become invalid.
                                warn!(peer=?target_node_id, filerequest_target_contact_id=?target_filerequest_contact_id, "We know the contact behind this peer id, but we don't know the contact we are talking to. So we do not send that file.")
                            }
                        }
                        ContactVerificationResult::VerificationFailed
                    } else {
                        trace!("Contact was verified");
                        ContactVerificationResult::Verified
                    }
                } else {
                    // Since we don't know the target contact, we can't verify him. So we just send a file and hope that libp2p's authentication isn't broken
                    trace!("Target contact is not known, so we send file blindly");
                    ContactVerificationResult::ContactIsUnknown
                }
            }
        }
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

            let progress = match &next_to_send.status {
                SendStatus::Pending => 0,
                SendStatus::InProgress(InProgress { transfer_data, .. }) => {
                    transfer_data.progress_bytes
                }
                _ => {
                    error!(peer=?node_id, file=?next_to_send, "Trying to get progress for a file with a nonsense status");
                    self.complete_file(entry, next_to_send.id.clone(), FileOutcome::Failed).await;
                    return;
                }
            };

            let (request, response) = BrainRequest::with_receiver(OpenFileForReadingRequest {
                uri: next_to_send.file_path.clone().into(),
                id: next_to_send.id.clone(),
                byte_offset: progress,
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
            let file_size_bytes = match file.file.metadata().await {
                Err(e) => {
                    error!(peer=?node_id, file=?next_to_send, error=?e, "Could not get filesize");
                    self.brain_action_sender
                        .send(BrainAction::NetworkNode(NetworkNodeAction::FileMissing {
                            pending_file_id: next_to_send.id.clone(),
                        }))
                        .await
                        .expect("Sending to work");
                    Box::pin(self.send_next(node_id, entry)).await;
                    return;
                }
                Ok(metadata) => metadata.len(),
            };

            trace!(status = ?next_to_send.status, "Next to send status");

            match next_to_send.status.clone() {
                SendStatus::Pending => {
                    let transfer_uuid: FylesId = Default::default();

                    if let Ok(_id) = (self.send)(
                        node_id,
                        Some(next_to_send.contact_id.clone()),
                        FileRequest::NewTransfer {
                            filerequest_id: next_to_send.target_filerequest_id.clone(),
                            file_name: file.file_name,
                            file_size_bytes,
                            transfer_uuid: transfer_uuid.clone(),
                        },
                    )
                        .await
                    {
                        next_to_send.status = InProgress {
                            status: InProgressSendStatus::Prepared,
                            transfer_data: TransferData {
                                progress_bytes: 0,
                                file_size_bytes,
                                transfer_id: transfer_uuid.clone(),
                            },
                        }
                            .into();
                        let send_data =
                            LockedSendData::new(next_to_send.clone(), transfer_uuid.clone(), file_size_bytes, file.file);
                        trace!("Setting next file into the sending slot");
                        peer_files.sending = Some(send_data);
                    } else {
                        warn!(peer=?node_id, "Error trying to send, setting back to pending");
                        peer_files.pending.push_front(next_to_send);
                        return;
                    }
                }
                SendStatus::InProgress(
                    mut in_progress @ InProgress {
                        status: InProgressSendStatus::Interrupted,
                        ..
                    },
                ) => {
                    if let Ok(_id) = (self.send)(
                        node_id,
                        Some(next_to_send.contact_id.clone()),
                        FileRequest::ContinueTransfer {
                            filerequest_id: next_to_send.target_filerequest_id.clone(),
                            transfer_uuid: in_progress.transfer_data.transfer_id.clone(),
                        },
                    )
                        .await
                    {
                        let mut send_data = LockedSendData::new(
                            next_to_send.clone(),
                            in_progress.transfer_data.transfer_id.clone(),
                            file_size_bytes,
                            file.file,
                        );
                        in_progress.status = InProgressSendStatus::Prepared;
                        send_data.pending_file.status = in_progress.into();
                        info!(peer=?node_id, pending_file=?send_data.pending_file, "Sent continuation request, setting `sending`");
                        peer_files.sending = Some(send_data);
                    }
                }
                SendStatus::Unknown(ref status) => {
                    warn!(peer=?node_id, file=?next_to_send, "Pending file has unknown status \"{status}\", won't send this");
                    self.brain_action_sender
                        .send(BrainAction::NetworkNode(NetworkNodeAction::FileFailed {
                            pending_file_id: next_to_send.id.clone(),
                        }))
                        .await
                        .expect("Sending to work");
                    Box::pin(self.send_next(node_id, entry)).await;
                    return;
                }
                _ => {
                    error!(peer=?node_id, file=?next_to_send, "Pending file has status that is neither sending nor interrupted. Dropping it");
                    self.brain_action_sender
                        .send(BrainAction::NetworkNode(NetworkNodeAction::FileFailed {
                            pending_file_id: next_to_send.id.clone(),
                        }))
                        .await
                        .expect("Sending to work");
                    Box::pin(self.send_next(node_id, entry)).await;
                    return;
                }
            }
            if !matches!(next_to_send.status, SendStatus::InProgress(InProgress{ status: InProgressSendStatus::Prepared | InProgressSendStatus::PendingSent, ..})) {
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(NetworkNodeAction::FileSending {
                        pending_file_id: next_to_send.id,
                        status: next_to_send.status,
                    }))
                    .await
                    .expect("Sending to work");
            }

            self.plugin
                .timeout_lock()
                .await
                .on_send_file_to_peer(node_id)
                .await;
        }.instrument(span).await
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
            let _ = self
                .brain_action_sender
                .send(BrainAction::NetworkNode(brain_action))
                .await;
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
        mut sending: LockedSendData<usize>,
        reason: String,
    ) {
        let span = entry.get().span.clone();
        let peer = *entry.key();
        async move {
            let reset_status = Self::compute_reset_status(&mut sending);
            debug!(new_status=?reset_status, "reset_and_stop: resetting in brain, interrupting plugin");
            sending.pending_file.status = reset_status.clone();
            let _ = self
                .brain_action_sender
                .send(BrainAction::NetworkNode(NetworkNodeAction::FileSendReset {
                    pending_file_id: sending.pending_file.id.clone(),
                    status: reset_status,
                    retry_count: Some(sending.pending_file.retry_count),
                    reason: Some(reason),
                }))
                .await;
            entry.get_mut().pending.push_back(sending.pending_file);
            let mut plugin = self.plugin.timeout_lock().await;
            plugin.interrupt_sending_to_peer(peer).await;
            plugin.pending_file_added_for_peer(peer, true).await;
        }.instrument(span).await
    }

    /// Reset the sending file back to pending, respecting retry limits.
    ///
    /// If the retry count exceeds the threshold (based on file size), the file
    /// is terminally completed as `Failed` and we advance to the next file.
    /// Otherwise it is pushed back to pending, the brain is notified with
    /// `FileSendReset`, and the plugin is told to interrupt and re-schedule.
    async fn reset_and_retry(
        &mut self,
        entry: OccupiedEntry<'a, PeerId, PeerFiles>,
        sending: LockedSendData<usize>,
        reason: String,
    ) {
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

    /// Compute the appropriate reset status for a file being pushed back to pending.
    /// Prepared → Pending (ephemeral, brain never knew), everything else → Interrupted.
    fn compute_reset_status(sending: &mut LockedSendData<usize>) -> SendStatus {
        match sending.pending_file.status {
            SendStatus::InProgress(InProgress {
                status: InProgressSendStatus::Prepared,
                ref transfer_data,
                ..
            }) => {
                if transfer_data.progress_bytes > 0 {
                    // Was a ContinueTransfer — preserve progress
                    SendStatus::InProgress(InProgress {
                        status: InProgressSendStatus::Interrupted,
                        transfer_data: transfer_data.clone(),
                    })
                } else {
                    SendStatus::Pending
                }
            }
            SendStatus::InProgress(InProgress {
                ref transfer_data,
                ..
            }) => SendStatus::InProgress(InProgress {
                status: InProgressSendStatus::Interrupted,
                transfer_data: transfer_data.clone(),
            }),
            _ => SendStatus::Pending,
        }
    }

    /// Reset any active sending state for a peer (e.g. on connection loss or protocol violation).
    ///
    /// Takes the sending slot, pushes back to pending with appropriate status,
    /// notifies brain and plugin. Returns the contact_id if there was a sending file.
    async fn reset_all(&mut self, entry: Entry<'a, PeerId, PeerFiles>) -> Option<ContactId> {
        if let Entry::Occupied(peer_files) = entry {
            peer_files.get().span.in_scope(|| {
                debug!("Resetting");
            });
            self.reset_all_with_occupied(peer_files).await
        } else {
            None
        }
    }

    async fn reset_all_with_occupied(
        &mut self,
        mut entry: OccupiedEntry<'a, PeerId, PeerFiles>,
    ) -> Option<ContactId> {
        let span = entry.get().span.clone();
        async move {
            let peer_files = entry.get_mut();
            let Some(sending) = peer_files.sending.take() else {
                trace!("No sending state - nothing to reset");
                return None;
            };
            let contact_id = sending.pending_file.contact_id.clone();
            self.reset_and_stop(entry, sending, "Peer disconnected while sending".into())
                .await;
            Some(contact_id)
        }
        .instrument(span)
        .await
    }
}

pub fn new_core_file_tracker(
    brain_action_sender: mpsc::Sender<BrainAction>,
    send: SendFn,
    send_idempotent: SendFn,
) -> CoreFileTracker<NoOpFileTrackerPlugin> {
    CoreFileTracker {
        files: Default::default(),
        brain_action_sender,
        send,
        send_idempotent,
        plugin: Mutex::new(NoOpFileTrackerPlugin {}).into(),
    }
}

impl<P: FileTrackerPlugin + Send> CoreFileTracker<P> {
    pub fn with_plugin(
        brain_action_sender: mpsc::Sender<BrainAction>,
        send: SendFn,
        send_idempotent: SendFn,
        plugin: P,
    ) -> Self {
        Self {
            files: Default::default(),
            brain_action_sender,
            send,
            send_idempotent,
            plugin: Mutex::new(plugin).into(),
        }
    }

    pub async fn has_pending_files_for(&self, peer: &PeerId) -> bool {
        if let Some(peer_files) = self.files.read().await.get(peer) {
            !peer_files.pending.is_empty()
        } else {
            false
        }
    }

    /// Common loop that sends file chunks to a peer until the send manager
    /// reports not-ready, done, an I/O error, or an interrupt.
    /// Re-acquires the write lock each iteration to avoid holding it across await points.
    /// Handles `send_next_after_error` internally.
    async fn send_chunks_loop(&self, node_id: PeerId, connected_contact_id: &Option<ContactId>) {
        loop {
            let mut files = self.files.write().await;
            let raw_entry = files.entry(node_id);
            let Entry::Occupied(mut entry) = raw_entry else {
                // Could be that a parallel receive thread has finished the transfer, so this
                // is a valid case
                break;
            };
            let peer_files = entry.get_mut();
            let Some(sd) = &mut peer_files.sending else {
                peer_files.span.in_scope(|| {
                    trace!("No send data, exiting send loop");
                });
                break;
            };
            let Some(sending) = sd.unlock_mut(connected_contact_id) else {
                warn!(expected=?sd.pending_file.contact_id, got=?connected_contact_id, "Incorrect contact id for file send in loop. Aborting.");
                return;
            };

            let PendingFile {
                status,
                file_path,
                id,
                contact_id,
                ..
            } = sending.pending_file;
            let span = peer_files.span.clone();
            match sending
                .send_manager
                .send_next_chunk(|chunk| {
                    self.send_idempotent_instrumented(
                        node_id,
                        Some(contact_id.clone()),
                        FileRequest::Chunk(chunk),
                        span,
                        Some(file_path.as_str()),
                    )
                })
                .await
            {
                SendRes::NotReady => {
                    peer_files.span.in_scope(|| {
                        trace!("File reader not ready");
                    });
                    break;
                }
                SendRes::Sent(_id) => {
                    peer_files.span.in_scope(|| {
                        trace!(peer=?node_id, "Sent chunk");
                    });
                }
                SendRes::Done => {
                    peer_files.span.in_scope(|| {
                        debug!("File reader is done");
                    });
                    if let Ok(_id) = self
                        .send_instrumented(
                            node_id,
                            Some(contact_id.clone()),
                            FileRequest::Done {
                                transfer_uuid: sending.send_manager.transfer_id.clone(),
                            },
                            peer_files.span.clone(),
                            Some(file_path.as_str()),
                        )
                        .await
                    {
                        if let SendStatus::InProgress(InProgress {
                            status: status @ InProgressSendStatus::Sending,
                            ..
                        }) = status
                        {
                            peer_files.span.in_scope(|| {
                                trace!("Setting status to pending sent");
                            });
                            *status = InProgressSendStatus::PendingSent;
                        } else {
                            peer_files.span.in_scope(|| {
                                error!(peer=?node_id, ?status, "File in unexpected send status after being done sending")
                            });
                        }
                    } else {
                        if status.transfer_data().is_none() {
                            peer_files.span.in_scope(|| {
                                error!(peer=?node_id, "File in unexpected send state after failing to send request");
                            });
                            return;
                        };
                        peer_files.span.in_scope(|| {
                            warn!(peer=?node_id, "Resetting because sending failed after receiving response");
                        });
                        light_ref!(self).reset_all_with_occupied(entry).await;
                    }
                    break;
                }
                SendRes::IoError(e) => {
                    peer_files.span.in_scope(|| {
                        error!(peer=?node_id, "Error while reading file {}: {e}", file_path);
                    });
                    let transfer_id = sending.send_manager.transfer_id.clone();
                    let contact_id = contact_id.clone();
                    let file_id = id.clone();
                    let io_error = e;
                    // Take the sending slot and abort with the peer
                    peer_files.sending = None;
                    let _ = self
                        .send_instrumented(
                            node_id,
                            Some(contact_id),
                            FileRequest::AbortTransfer {
                                transfer_uuid: Some(transfer_id),
                            },
                            peer_files.span.clone(),
                            peer_files
                                .sending
                                .as_ref()
                                .map(|it| it.pending_file.file_path.as_str()),
                        )
                        .await;
                    // Notify brain and advance to next file
                    light_ref!(self)
                        .complete_file(
                            entry,
                            file_id,
                            FileOutcome::IoError(io_error),
                        )
                        .await;
                    break;
                }
                SendRes::Interrupt => {
                    // Do not increase retry count. This is likely due to a network issue.
                    peer_files.span.in_scope(|| {
                        warn!(peer=?node_id, "Interrupting transfer because file tracker could not send, going to push back into pending files");
                    });
                    let Some(transfer_data) = status.transfer_data() else {
                        peer_files.span.in_scope(|| {
                            error!(peer=?node_id, "File in unexpected send state after getting interrupt response from send manager");
                        });
                        return;
                    };
                    let pending_file_id = id.clone();
                    let interrupted_status: SendStatus = InProgress {
                        status: InProgressSendStatus::Interrupted,
                        transfer_data: transfer_data.clone(),
                    }
                    .into();

                    // Push the sending file back to pending before releasing the lock
                    if let Some(mut sending) = peer_files.sending.take() {
                        sending.pending_file.status = interrupted_status.clone();
                        peer_files.pending.push_back(sending.pending_file);
                    }

                    // Drop the files write lock BEFORE acquiring the plugin lock
                    // to avoid lock ordering inversion with provider_query_completed
                    // (which acquires plugin → files).
                    drop(files);

                    let _ = self
                        .brain_action_sender
                        .send(BrainAction::NetworkNode(NetworkNodeAction::FileSendReset {
                            pending_file_id,
                            status: interrupted_status,
                            retry_count: None,
                            reason: Some(
                                "Send interrupted: file tracker could not send to peer".into(),
                            ),
                        }))
                        .await;
                    self.plugin
                        .timeout_lock()
                        .await
                        .interrupt_sending_to_peer(node_id)
                        .await;

                    break;
                }
            }
        }
    }

    async fn reset_all_on_internal_error_for(&self, peer: PeerId) {
        let mut files = self.files.write().await;
        let entry = files.entry(peer);
        self.reset_all_on_internal_error_for_entry(peer, entry)
            .await;
    }

    async fn reset_all_on_internal_error_for_entry(
        &self,
        peer: PeerId,
        mut entry: Entry<'_, PeerId, PeerFiles>,
    ) {
        let (transfer_uuid, contact_id, span) = match &mut entry {
            Entry::Vacant(_) => (None, None, None),
            Entry::Occupied(peer_files) => {
                let peer_files = peer_files.get_mut();
                let span = peer_files.span.clone();
                peer_files.span.in_scope(|| {
                    warn!("Resetting on internal error, sending abort transfer");
                    if let Some(ref mut sending) = peer_files.sending {
                        sending.pending_file.retry_count += 1;
                        warn!(
                            retry_count = sending.pending_file.retry_count,
                            "Increased retry count"
                        );
                        (
                            Some(sending.get_transfer_id().clone()),
                            Some(sending.pending_file.contact_id.clone()),
                            Some(span),
                        )
                    } else {
                        (None, None, Some(span))
                    }
                })
            }
        };
        let path = match &entry {
            Entry::Occupied(occupied_entry) => occupied_entry
                .get()
                .sending
                .as_ref()
                .map(|it| it.pending_file.file_path.clone()),
            Entry::Vacant(_) => None,
        };
        light_ref!(self).reset_all(entry).await;
        if let Some(span) = span {
            let _ = self
                .send_instrumented(
                    peer,
                    contact_id,
                    FileRequest::AbortTransfer { transfer_uuid },
                    span,
                    path.as_deref(),
                )
                .await;
        } else {
            let _ = (self.send)(
                peer,
                contact_id,
                FileRequest::AbortTransfer { transfer_uuid },
            )
            .await;
        }
    }

    async fn reset_all_on_protocol_violation_for(&self, peer: PeerId) {
        let mut files = self.files.write().await;
        let mut span = None;
        let mut path = None;
        if let Entry::Occupied(mut entry) = files.entry(peer) {
            span = Some(entry.get().span.clone());
            path = entry
                .get()
                .sending
                .as_ref()
                .map(|it| it.pending_file.file_path.clone());
            entry.get().span.in_scope(|| {
                warn!("Resetting on protocol violation");
            });
            if let Some(ref mut sending) = entry.get_mut().sending {
                sending.pending_file.retry_count += 1;
                warn!(
                    retry_count = sending.pending_file.retry_count,
                    "Increased retry count"
                );
            }
        }
        let contact_id = light_ref!(self).reset_all(files.entry(peer)).await;
        if let Some(span) = span {
            let _ = self
                .send_instrumented(
                    peer,
                    contact_id,
                    FileRequest::ProtocolViolation {
                        transfer_uuid: None,
                    },
                    span,
                    path.as_deref(),
                )
                .await;
        } else {
            let _ = (self.send)(
                peer,
                contact_id,
                FileRequest::ProtocolViolation {
                    transfer_uuid: None,
                },
            )
            .await;
        }
    }

    pub async fn get_peers_with_pending_files(&self) -> Vec<PeerId> {
        self.files.read().await.keys().cloned().collect()
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
        trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
        match response {
            FileResponse::AcceptNewTransfer | FileResponse::RestartContinueTransfer => {
                {
                    let mut files = self.files.write().await;
                    let Entry::Occupied(mut entry) = files.entry(node_id) else {
                        trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                        warn!(peer=?node_id, "Accepted transfer that was never offered, sending protocol violation");
                        let _ = (self.send)(
                            node_id,
                            None,
                            FileRequest::ProtocolViolation {
                                transfer_uuid: None,
                            },
                        )
                        .await;
                        return;
                    };

                    let peer_files = entry.get_mut();
                    peer_files.span.in_scope(|| {
                        debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    });
                    let Some(LockedSendData {
                        pending_file,
                        must_verify_contact_id,
                        ..
                    }) = &mut peer_files.sending
                    else {
                        peer_files.span.in_scope(|| {
                            warn!(peer=?node_id, "Accepted transfer that was never offered");
                        });
                        drop(files);
                        self.reset_all_on_protocol_violation_for(node_id).await;
                        return;
                    };

                    match self
                        .verify_target_contact_id(node_id, &pending_file.contact_id, &contact_id)
                        .await
                    {
                        ContactVerificationResult::ContactIsUnknown => {
                            *must_verify_contact_id = false;
                        }
                        ContactVerificationResult::Verified => {},
                        ContactVerificationResult::VerificationFailed => {
                        light_ref!(self).reset_all(Entry::Occupied(entry)).await;
                            return;
                        }
                        ContactVerificationResult::InternalError => {
                            drop(files);
                            self.reset_all_on_internal_error_for(node_id).await;
                            return;
                        }
                    };

                    let SendStatus::InProgress(
                        ref mut in_progress @ InProgress {
                            status: InProgressSendStatus::Prepared,
                            ..
                        },
                    ) = pending_file.status
                    else {
                        peer_files.span.in_scope(|| {
                            warn!(peer=?node_id, "Accepted transfer that was never offered");
                        });
                        drop(files);
                        self.reset_all_on_protocol_violation_for(node_id).await;
                        return;
                    };

                    in_progress.status = InProgressSendStatus::Sending;
                }
                self.send_chunks_loop(node_id, &contact_id).await;
            }
            FileResponse::RejectNewTransfer => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    warn!(peer=?node_id, "Rejected transfer that was never offered");
                    let _ = (self.send)(
                        node_id,
                        None,
                        FileRequest::ProtocolViolation {
                            transfer_uuid: None,
                        },
                    )
                    .await;
                    return;
                };
                let peer_files = entry.get_mut();
                peer_files.span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });

                let Some(mut locked_send_data) = peer_files.sending.take() else {
                    peer_files.span.in_scope(|| {
                        warn!(peer=?node_id, "Rejected transfer that was never offered, resetting with protocol violation");
                    });
                    drop(files);
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };

                match self
                    .verify_target_contact_id(
                        node_id,
                        &locked_send_data.pending_file.contact_id,
                        &contact_id,
                    )
                    .await
                {
                    ContactVerificationResult::ContactIsUnknown => {
                        locked_send_data.must_verify_contact_id = false;
                    }
                    ContactVerificationResult::Verified => {},
                    ContactVerificationResult::VerificationFailed => {
                        // put the thing back. Otherwise attackers can permanently stuck this app
                    peer_files.sending = Some(locked_send_data);
                        light_ref!(self).reset_all(Entry::Occupied(entry)).await;
                        return;
                    }
                    ContactVerificationResult::InternalError => {
                        // put the thing back. Otherwise attackers can permanently stuck this app
                    peer_files.sending = Some(locked_send_data);
                        drop(files);
                        self.reset_all_on_internal_error_for(node_id).await;
                        return;
                    }
                };

                light_ref!(self)
                    .complete_file(
                        entry,
                        locked_send_data.pending_file.id,
                        FileOutcome::Rejected,
                    )
                    .await;
            }
            FileResponse::AcceptContinueTransfer { offset } => {
                debug!(?offset, "Accepted to continue transfer");
                let mut files = self.files.write().await;
                let raw_entry = files.entry(node_id);
                let Entry::Occupied(mut entry) = raw_entry else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    warn!(peer=?node_id, "Accepted transfer that was never offered");
                    let _ = (self.send)(
                        node_id,
                        None,
                        FileRequest::ProtocolViolation {
                            transfer_uuid: None,
                        },
                    )
                    .await;
                    return;
                };
                let peer_files = entry.get_mut();
                peer_files.span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let Some(locked_send_data) = &mut peer_files.sending else {
                    peer_files.span.in_scope(|| {
                        warn!(peer=?node_id, "Accepted transfer that was never offered");
                    });
                    drop(files);
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };

                match self
                    .verify_target_contact_id(
                        node_id,
                        &locked_send_data.pending_file.contact_id,
                        &contact_id,
                    )
                    .await
                {
                    ContactVerificationResult::ContactIsUnknown => {
                        locked_send_data.must_verify_contact_id = false;
                    }
                    ContactVerificationResult::Verified => {},
                    ContactVerificationResult::VerificationFailed => {
                        light_ref!(self).reset_all(Entry::Occupied(entry)).await;
                        return
                    }
                    ContactVerificationResult::InternalError => {
                        drop(files);
                        self.reset_all_on_internal_error_for(node_id).await;
                        return
                    }
                };

                let SendStatus::InProgress(InProgress {
                    status: status @ InProgressSendStatus::Prepared,
                    transfer_data,
                }) = &mut locked_send_data.pending_file.status
                else {
                    warn!(
                        status=?locked_send_data.pending_file.status,
                        "Sending file in unexpected state to continue transfer"
                    );
                    drop(files);
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };
                *status = InProgressSendStatus::Sending;
                transfer_data.progress_bytes = offset;
                locked_send_data
                    .unlock_mut(&contact_id)
                    .expect("Must unlock, is already verified")
                    .send_manager
                    .set_progress(offset)
                    .await;

                drop(files);

                self.send_chunks_loop(node_id, &contact_id).await;
            }
            FileResponse::ConfirmChunk => {
                {
                    let mut files = self.files.write().await;
                    let Entry::Occupied(mut entry) = files.entry(node_id) else {
                        trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                        warn!(peer=?node_id, "Accepted chunk that wasn't sent (no entry)");
                        let _ = (self.send)(
                            node_id,
                            None,
                            FileRequest::ProtocolViolation {
                                transfer_uuid: None,
                            },
                        )
                        .await;
                        return;
                    };
                    let peer_files = &mut entry.get_mut();
                    peer_files.span.in_scope(|| {
                        debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    });
                    let Ok(request_id) = request_id else {
                        peer_files.span.in_scope(|| {
                            error!(peer=?node_id, "Internal error tracking request ids");
                        });
                        drop(files);
                        self.reset_all_on_internal_error_for(node_id).await;
                        return;
                    };

                    let opt = match &mut peer_files.sending {
                        None => None,
                        Some(locked_send_data) => {
                            let Some(unlocked) = locked_send_data.unlock_mut(&contact_id) else {
                                warn!(peer=?node_id, expected=?locked_send_data.pending_file.contact_id, got=?contact_id, "Unable to unlock send manager. Are we under attack here by this peer?");
                                light_ref!(self).reset_all(Entry::Occupied(entry)).await;
                                return;
                            };
                            Some(unlocked)
                        }
                    };
                    let Some(SendDataMut {
                        send_manager: sending,
                        pending_file:
                            PendingFile {
                                status,
                                id,
                                ..
                            },
                    }) = opt
                    else {
                        peer_files.span.in_scope(|| {
                            warn!(peer=?node_id, "Accepted chunk that wasn't sent (no send data), sending protocol violation");
                        });
                        drop(files);
                        self.reset_all_on_protocol_violation_for(node_id).await;
                        return;
                    };

                    let SendStatus::InProgress(InProgress {
                        status: InProgressSendStatus::Sending,
                        transfer_data,
                    }) = status
                    else {
                        peer_files.span.in_scope(|| {
                            warn!(peer=?node_id, ?status, "Accepted chunk that wasn't sent (not in status sending)");
                        });
                        drop(files);
                        self.reset_all_on_protocol_violation_for(node_id).await;
                        return;
                    };

                    peer_files.span.in_scope(|| {
                        trace!(request_id, "Chunk confirmed");
                    });

                    sending.receive_ack(request_id);

                    transfer_data.progress_bytes = sending.progress_bytes();
                    let _ = self
                        .brain_action_sender
                        .send(BrainAction::NetworkNode(NetworkNodeAction::FileSending {
                            pending_file_id: id.clone(),
                            status: SendStatus::InProgress(InProgress {
                                status: InProgressSendStatus::Sending,
                                transfer_data: transfer_data.clone(),
                            }),
                        }))
                        .await;
                }

                self.send_chunks_loop(node_id, &contact_id).await;
            }
            FileResponse::RejectChunk => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    warn!(peer=?node_id, "Rejected chunk that wasn't sent");
                    let _ = (self.send)(
                        node_id,
                        None,
                        FileRequest::ProtocolViolation {
                            transfer_uuid: None,
                        },
                    )
                    .await;
                    return;
                };
                let peer_files = entry.get_mut();
                peer_files.span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let opt = peer_files.sending.take();
                let Some(LockedSendData {
                    pending_file:
                        pending_file @ PendingFile {
                            status:
                                SendStatus::InProgress(InProgress {
                                    status: InProgressSendStatus::Sending,
                                    ..
                                }),
                            ..
                        },
                    ..
                }) = opt
                else {
                    warn!(peer=?node_id, "Rejected chunk that wasn't sent");
                    drop(files);
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };

                warn!(peer=?node_id, "Rejected file chunk");
                let file_id = pending_file.id.clone();
                light_ref!(self)
                    .complete_file(entry, file_id, FileOutcome::Rejected)
                    .await;
            }
            FileResponse::ConfirmAbort => {
                let mut files = self.files.write().await;
                let Entry::Occupied(entry) = files.entry(node_id) else {
                    debug!(peer=?node_id, "Abort confirmed, but we don't know for what");
                    return;
                };
                entry.get().span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                if entry.get().sending.is_none() {
                    light_ref!(self).send_next(node_id, entry).await;
                } else {
                    error!("Received AbortConfirmed but we still have sending state");
                }
            }
            FileResponse::ConfirmDone => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    warn!(peer=?node_id, "Accepted done that wasn't sent");
                    let _ = (self.send)(
                        node_id,
                        None,
                        FileRequest::ProtocolViolation {
                            transfer_uuid: None,
                        },
                    )
                    .await;
                    return;
                };
                let peer_files = entry.get_mut();
                peer_files.span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let opt = &peer_files.sending;
                let Some(LockedSendData {
                    pending_file:
                        pending_file @ PendingFile {
                            status:
                                SendStatus::InProgress(InProgress {
                                    status: InProgressSendStatus::PendingSent,
                                    ..
                                }),
                            ..
                        },
                    ..
                }) = opt
                else {
                    warn!(peer=?node_id, "Accepted done that wasn't sent");
                    drop(files);
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };

                debug!(peer=?node_id, "Accepted file transfer done");
                let file_id = pending_file.id.clone();
                peer_files.sending = None;
                light_ref!(self)
                    .complete_file(entry, file_id, FileOutcome::Sent)
                    .await;
            }
            FileResponse::RejectDone => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    warn!(peer=?node_id, "Rejected done that wasn't sent");
                    let _ = (self.send)(
                        node_id,
                        None,
                        FileRequest::ProtocolViolation {
                            transfer_uuid: None,
                        },
                    )
                    .await;
                    return;
                };
                entry.get().span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let send_data = &mut entry.get_mut().sending;
                let Some((
                    done,
                    SendStatus::InProgress(InProgress {
                        status: InProgressSendStatus::PendingSent,
                        ..
                    }),
                )) = send_data.as_ref().map(|it| (it, &it.pending_file.status))
                else {
                    warn!(peer=?node_id, "Rejected done that wasn't sent");
                    drop(files);
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };

                let file_id = done.pending_file.id.clone();
                *send_data = None;
                light_ref!(self)
                    .complete_file(entry, file_id, FileOutcome::Rejected)
                    .await;
            }
            FileResponse::AcknowledgeProtocolViolation => {
                debug!(peer=?node_id, "Other end acknowledged protocol violation");
            }
            FileResponse::ProtocolViolation => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    error!(peer=?node_id, "Peer reports that we violated the filerequest protocol, but we have no state for them");
                    return;
                };
                entry.get().span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let Some(mut sending) = entry.get_mut().sending.take() else {
                    error!(peer=?node_id, "Peer reports that we violated the filerequest protocol, but we have no sending state for them");
                    return;
                };
                sending.pending_file.retry_count += 1;
                error!(peer=?node_id, sending_file=sending.pending_file.file_path, retry_count = sending.pending_file.retry_count, "Peer reports that we violated the filerequest protocol while sending, increased retry count");
                light_ref!(self)
                    .reset_and_retry(entry, sending, "Peer reported protocol violation".into())
                    .await;
            }
            FileResponse::InternalError => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    error!(peer=?node_id, "Peer reported internal error, but we have no state for them");
                    return;
                };
                let peer_files = entry.get_mut();
                peer_files.span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let Some(mut sending) = peer_files.sending.take() else {
                    error!(peer=?node_id, "Peer reported internal error, but we have no send state for them");
                    return;
                };
                warn!(peer=?node_id, "Peer reported an internal error");
                sending.pending_file.retry_count += 1;
                // Don't continue sending — reset_and_stop lets kademlia requery add natural delay
                light_ref!(self)
                    .reset_and_stop(entry, sending, "Peer reported internal error".into())
                    .await;
            }
            FileResponse::RejectContinueTransfer => {
                let mut files = self.files.write().await;
                let Entry::Occupied(mut entry) = files.entry(node_id) else {
                    trace!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                    error!(peer=?node_id, "Peer rejected continue transfer but we have no send state for them");
                    return;
                };
                entry.get().span.in_scope(|| {
                    debug!(peer=?node_id, ?response, ?request_id, "Got filerequest response");
                });
                let Some(locked_send_data) = entry.get_mut().sending.take() else {
                    error!(peer=?node_id, "Peer rejected continue transfer which we didn't send");
                    self.reset_all_on_protocol_violation_for(node_id).await;
                    return;
                };

                match self
                    .verify_target_contact_id(
                        node_id,
                        &locked_send_data.pending_file.contact_id,
                        &contact_id,
                    )
                    .await
                {
                    ContactVerificationResult::ContactIsUnknown => true,
                    ContactVerificationResult::Verified => true,
                    ContactVerificationResult::VerificationFailed => {
                        // put the thing back. Otherwise attackers can permanently stuck this app
                        entry.get_mut().sending = Some(locked_send_data);
                        light_ref!(self).reset_all(Entry::Occupied(entry)).await;
                        return
                    }
                    ContactVerificationResult::InternalError => {
                        // put the thing back. Otherwise attackers can permanently stuck this app
                        entry.get_mut().sending = Some(locked_send_data);
                        drop(files);
                        self.reset_all_on_internal_error_for(node_id).await;
                        return
                    }
                };

                warn!(peer=?node_id, "Peer rejected continue transfer");
                let file_id = locked_send_data.pending_file.id;
                light_ref!(self)
                    .complete_file(entry, file_id, FileOutcome::Rejected)
                    .await;
            }
        }
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
        let mut files = self.files.write().await;
        let Entry::Occupied(mut entry) = files.entry(node_id) else {
            error!(peer=?node_id, ?error, "Received a receive error while not in sending state");
            return;
        };
        let span = entry.get().span.clone();
        async move {
            let Some(sending) = entry.get_mut().sending.take() else {
                error!(peer=?node_id, ?error, ?request_id, "Received a receive error while not in sending state");
                return;
            };
            match error {
                ReceiveError::RemoteError(_) => {
                    // Can't fix this. Increase retry count and push back
                    light_ref!(self)
                        .reset_and_retry(entry, sending, format!("Remote error: {error:?}"))
                        .await;
                }
                ReceiveError::LocalError(local_error) => {
                    match local_error {
                        LocalReceiveError::Timeout => {
                            // Just push back, do not increase retry count
                            warn!(peer=?node_id, ?request_id, "Resetting due to timeout");
                            light_ref!(self)
                                .reset_and_stop(entry, sending, "Request timeout".into())
                                .await;
                        }
                        LocalReceiveError::TransportError(transport_error) => {
                            match transport_error {
                                OutboundFailure::DialFailure
                                | OutboundFailure::Timeout
                                | OutboundFailure::Io(_)
                                | OutboundFailure::ConnectionClosed => {
                                    // Just push back, do not increase retry count
                                    warn!(peer=?node_id, ?request_id, error=?transport_error, "Resetting due to outbound failure");
                                    light_ref!(self)
                                        .reset_and_stop(entry, sending, format!("Transport error: {transport_error:?}"))
                                        .await;
                                }
                                OutboundFailure::UnsupportedProtocols => {
                                    // File rejected — terminal
                                    let file_id = sending.pending_file.id.clone();
                                    light_ref!(self)
                                        .complete_file(entry, file_id, FileOutcome::Rejected)
                                        .await;
                                }
                            }
                        }
                        LocalReceiveError::MissingSession | LocalReceiveError::DecryptError(_) => {
                            // Can only occur on persistent problem with session management. Increase retry count and push back
                            light_ref!(self)
                                .reset_and_retry(entry, sending, format!("Session/decrypt error: {local_error:?}"))
                                .await;
                        }
                        LocalReceiveError::ResendError(_) => {
                            // Can only occur on persistent problem with session management. Increase retry count and push back
                            light_ref!(self)
                                .reset_and_retry(entry, sending, format!("Resend error: {local_error:?}"))
                                .await;
                        }
                    }
                }
            }
        }.instrument(span).await;
    }
}

#[async_trait]
impl<P: FileTrackerPlugin + Send + Sync> FileTracker for CoreFileTracker<P> {
    async fn add_pending_file(&self, peer: PeerId, file: PendingFile) {
        let mut files = self.files.write().await;
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
        let mut files = self.files.write().await;
        trace!(
            "Triggering interaction with peer with these files: {:?}",
            files
                .values()
                .map(|it| (&it.pending, it.sending.is_some()))
                .collect::<Vec<_>>()
        );
        let Entry::Occupied(peer_files) = files.entry(peer) else {
            trace!(?peer, "No files for peer");
            return;
        };
        if peer_files.get().sending.is_some() {
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
        if let Some(peer_files) = self.files.read().await.get(peer) {
            !peer_files.pending.is_empty()
        } else {
            false
        }
    }

    async fn has_sending_files_for(&self, peer: &PeerId) -> bool {
        if let Some(peer_files) = self.files.read().await.get(peer) {
            peer_files.sending.is_some()
        } else {
            false
        }
    }

    async fn cancel_file(&self, file_id: FylesId) -> Option<PeerId> {
        let mut files = self.files.write().await;
        // Find which peer has this file
        let peer = files.iter().find_map(|(peer, pf)| {
            let in_sending = pf
                .sending
                .as_ref()
                .is_some_and(|s| s.pending_file.id == file_id);
            let in_pending = pf.pending.iter().any(|p| p.id == file_id);
            (in_sending || in_pending).then_some(*peer)
        });
        let Some(peer) = peer else { return None };
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
                    FileRequest::AbortTransfer {
                        transfer_uuid: Some(sending.get_transfer_id().clone()),
                    },
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
        let mut files = self.files.write().await;
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
                    FileRequest::AbortTransfer {
                        transfer_uuid: Some(sending.get_transfer_id().clone()),
                    },
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
                .pending_file_removed_for_peer(&peer_id, !peer_files.get().pending.is_empty(), false)
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

    async fn reset_for_disconnected_peer(&self, peer: PeerId) {
        if !self.has_sending_files_for(&peer).await {
            return;
        }
        let mut files = self.files.write().await;
        let entry = files.entry(peer);
        match entry {
            Entry::Occupied(ref occupied_entry) => occupied_entry.get().span.in_scope(|| {
                warn!(?peer, "Connection closed while sending file — resetting");
            }),
            Entry::Vacant(_) => {
                warn!(?peer, "Connection closed while sending file — resetting");
            }
        }
        light_ref!(self).reset_all(entry).await;
    }

    async fn handle_outgoing_connection_error(&self, peer: PeerId, address: &Multiaddr) {
        self.plugin
            .timeout_lock()
            .await
            .outgoing_connection_error(&peer, address)
            .await;
    }

    async fn handle_non_correlatable_error(&self, peer: PeerId) {
        self.plugin
            .timeout_lock()
            .await
            .non_correlatable_outgoing_connection_error(&peer)
            .await;
    }
}
