use std::sync::Arc;

use async_trait::async_trait;
use futures::{AsyncReadExt, StreamExt};
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::{
    FilerequestAccessRequest, FilerequestContinueRequest, FilerequestContinueResponse,
    NetworkNodeAction,
};
use fyles_core::core::brain::types::{BrainRequest, FilerequestResult};
use fyles_core::core::domain_models::{ContactId, CreateIncomingFile, FylesId, ReceiveStatus};
use fyles_core::core::filerequest_drive_handler::FilerequestDriveHandler;
use fyles_core::library::util::epoch::unix_epoch_millis;
use fyles_core::library::util::part_file::get_part_file_name;
use fyles_core::library::util::util::TimeoutLock;
use libp2p::{PeerId, Stream, StreamProtocol};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, error, info, warn};

use super::receive_manager::{FilerequestReceiver, finalize_received};
use crate::constants::{ATTEMPT_OPEN_TIMEOUT, FILE_DATA_PROTOCOL, TASK_STOP_TIMEOUT};
use crate::event_loop::LocalNetworkSwarm;
use crate::file_stream::receive::{
    ReceiveExit, ReceiverEnds, ReceivingFile, Unfinished, await_begin, receive,
};
use crate::types::{AttemptGrant, AttemptId, FileRequestV2, FileResponseV2};

pub(super) struct StreamSlot {
    meta: Arc<IncomingMeta>,
    stage: Stage,
}

enum Stage {
    Reserved(Reservation),
    Streaming(StreamingTransfer),
}

pub(super) struct Reservation {
    grant: AttemptGrant,
    file: ReceivingFile,
}

struct IncomingMeta {
    transfer_id: FylesId,
    filerequest_id: FylesId,
    contact_id: Option<ContactId>,
    file_name: String,
    started_at_ms: i64,
    drive_handler: FilerequestDriveHandler,
}

pub(super) struct StreamingTransfer {
    attempt: AttemptId,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

struct StillRunning;

/// Never call it with the receiver locked, since the task takes that lock once more before it ends.
async fn stop(slot: StreamSlot) -> Result<(), StillRunning> {
    match slot.stage {
        Stage::Reserved(_) => Ok(()),
        Stage::Streaming(streaming) => {
            streaming.cancel.cancel();
            match timeout(TASK_STOP_TIMEOUT, streaming.task).await {
                Ok(_ended) => Ok(()),
                Err(_) => Err(StillRunning),
            }
        }
    }
}

fn part_file_name(meta: &IncomingMeta, peer: &PeerId) -> String {
    get_part_file_name(
        meta.started_at_ms,
        &meta.transfer_id.0,
        &peer.to_base58(),
        &meta.contact_id,
    )
}

/// The brain round trips the stream control handlers make.
#[async_trait]
pub trait ReceiverPorts: Send + Sync {
    async fn request_file_drop(
        &self,
        filerequest_id: FylesId,
        contact_id: Option<ContactId>,
    ) -> Option<FilerequestDriveHandler>;

    async fn create_incoming(&self, file: CreateIncomingFile) -> FilerequestResult<FylesId>;

    async fn continuation(
        &self,
        request: FilerequestContinueRequest,
    ) -> Option<FilerequestContinueResponse>;

    async fn mark_failed(&self, transfer_id: FylesId);

    async fn drop_incoming(
        &self,
        transfer_id: FylesId,
        peer: PeerId,
        contact_id: Option<ContactId>,
    );
}

pub struct BrainPorts<'a>(pub &'a Sender<BrainAction>);

#[async_trait]
impl ReceiverPorts for BrainPorts<'_> {
    async fn request_file_drop(
        &self,
        filerequest_id: FylesId,
        contact_id: Option<ContactId>,
    ) -> Option<FilerequestDriveHandler> {
        let (request, response) = BrainRequest::with_receiver(FilerequestAccessRequest {
            filerequest_id,
            contact_id,
        });
        self.0
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::RequestFileDrop(request, Span::current()),
            ))
            .await
            .expect("Sending to work");
        response.await.expect("Sender not to be dropped")
    }

    async fn create_incoming(&self, file: CreateIncomingFile) -> FilerequestResult<FylesId> {
        let (request, response) = BrainRequest::with_receiver(file);
        self.0
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::CreateIncomingFile(request),
            ))
            .await
            .expect("Sending to work");
        response.await.expect("Sender not to be dropped")
    }

    async fn continuation(
        &self,
        request: FilerequestContinueRequest,
    ) -> Option<FilerequestContinueResponse> {
        let (request, response) = BrainRequest::with_receiver(request);
        self.0
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::RequestFileTransferContinuation(request),
            ))
            .await
            .expect("Sending to work");
        response.await.expect("Sender not to be dropped")
    }

    async fn mark_failed(&self, transfer_id: FylesId) {
        self.0
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::UpdateReceivedFileStatus {
                    transfer_id,
                    status: ReceiveStatus::Failed,
                    progress_bytes: 0,
                },
            ))
            .await
            .expect("Sending to work");
    }

    async fn drop_incoming(
        &self,
        transfer_id: FylesId,
        peer: PeerId,
        contact_id: Option<ContactId>,
    ) {
        self.0
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::DropIncomingTransfer {
                    transfer_id,
                    peer_id: peer.to_base58(),
                    contact_id,
                },
            ))
            .await
            .expect("Sending to work");
    }
}

impl FilerequestReceiver {
    /// A cleared transfer has its own part file, so nothing waits for its task to stop.
    pub(super) fn clear_stream(&mut self, peer: PeerId) {
        if let Some(StreamSlot {
            meta,
            stage: Stage::Streaming(streaming),
        }) = self.streams.remove(&peer)
        {
            warn!(?peer, transfer_id=?meta.transfer_id, "Stopping a running transfer");
            streaming.cancel.cancel();
        }
    }

    /// A grant the sender never opens a stream for is dropped after [`ATTEMPT_OPEN_TIMEOUT`],
    /// closing its part file. The sender resumes it with a fresh grant.
    async fn reserve(
        this: &Arc<Mutex<Self>>,
        peer: PeerId,
        meta: IncomingMeta,
        reservation: Reservation,
    ) {
        let attempt = reservation.grant.attempt;
        {
            let mut me = this.timeout_lock().await;
            me.clear_stream(peer);
            me.receiving_files.destruct(&peer);
            me.streams.insert(
                peer,
                StreamSlot {
                    meta: Arc::new(meta),
                    stage: Stage::Reserved(reservation),
                },
            );
        }
        let this = this.clone();
        tokio::spawn(
            async move {
                sleep(ATTEMPT_OPEN_TIMEOUT).await;
                if this
                    .timeout_lock()
                    .await
                    .take_reservation(peer, attempt)
                    .is_some()
                {
                    debug!(?peer, "No data stream arrived for the grant, dropping it");
                }
            }
            .in_current_span(),
        );
    }

    fn take_reservation(
        &mut self,
        peer: PeerId,
        attempt: AttemptId,
    ) -> Option<(Arc<IncomingMeta>, Reservation)> {
        match self.take_stream_if(peer, |slot| {
            matches!(&slot.stage, Stage::Reserved(reservation) if reservation.grant.attempt == attempt)
        }) {
            Some(StreamSlot {
                meta,
                stage: Stage::Reserved(reservation),
            }) => Some((meta, reservation)),
            _ => None,
        }
    }

    fn take_stream_if(
        &mut self,
        peer: PeerId,
        matches: impl FnOnce(&StreamSlot) -> bool,
    ) -> Option<StreamSlot> {
        if self.streams.get(&peer).is_some_and(matches) {
            self.streams.remove(&peer)
        } else {
            None
        }
    }

    pub async fn respond_to_v2(
        this: &Arc<Mutex<Self>>,
        ports: &impl ReceiverPorts,
        node_id: PeerId,
        contact_id: Option<ContactId>,
        request: FileRequestV2,
    ) -> FileResponseV2 {
        match request {
            FileRequestV2::NewTransfer {
                filerequest_id,
                file_name,
                file_size_bytes,
                transfer_uuid,
            } => {
                let Some(drive_handler) = ports
                    .request_file_drop(filerequest_id.clone(), contact_id.clone())
                    .await
                else {
                    return FileResponseV2::RejectNewTransfer;
                };
                let started_at_ms = match unix_epoch_millis() {
                    Ok(ms) => ms,
                    Err(e) => {
                        error!(peer=?node_id, error=?e, "System clock error");
                        return FileResponseV2::InternalError;
                    }
                };
                let meta = IncomingMeta {
                    transfer_id: transfer_uuid,
                    filerequest_id,
                    contact_id,
                    file_name,
                    started_at_ms,
                    drive_handler,
                };
                let file = match meta
                    .drive_handler
                    .get_partial_file_for_writing(&part_file_name(&meta, &node_id))
                    .await
                {
                    Ok(file) => file,
                    Err(e) => {
                        error!(error=?e, "Error opening exact file for writing");
                        return FileResponseV2::InternalError;
                    }
                };
                let created = ports
                    .create_incoming(CreateIncomingFile {
                        filerequest_id: meta.filerequest_id.clone(),
                        transfer_id: meta.transfer_id.clone(),
                        contact_id: meta.contact_id.clone(),
                        peer_id: node_id.to_base58(),
                        file_name: meta.file_name.clone(),
                        persisted_as_name: meta.file_name.clone(),
                        file_size_bytes,
                        started_at_ms,
                    })
                    .await;
                if let Err(e) = created {
                    error!(peer=?node_id, "Failed to persist receiving transfer: {e:?}");
                    return FileResponseV2::InternalError;
                }
                let grant = AttemptGrant::mint();
                Self::reserve(
                    this,
                    node_id,
                    meta,
                    Reservation {
                        grant: grant.clone(),
                        file: ReceivingFile::starting(file, file_size_bytes),
                    },
                )
                .await;
                FileResponseV2::AcceptNewTransfer { grant }
            }
            FileRequestV2::ContinueTransfer {
                filerequest_id,
                transfer_uuid,
            } => {
                let current = this.timeout_lock().await.take_stream_if(node_id, |slot| {
                    slot.meta.transfer_id == transfer_uuid
                        && slot.meta.filerequest_id == filerequest_id
                        && slot.meta.contact_id == contact_id
                });
                if let Some(slot) = current
                    && stop(slot).await.is_err()
                {
                    warn!(peer=?node_id, "The previous attempt did not stop in time");
                    return FileResponseV2::InternalError;
                }
                Self::continue_from_storage(
                    this,
                    ports,
                    node_id,
                    contact_id,
                    filerequest_id,
                    transfer_uuid,
                )
                .await
            }
            FileRequestV2::AbortTransfer { transfer_uuid } => {
                let current = this.timeout_lock().await.take_stream_if(node_id, |slot| {
                    slot.meta.contact_id == contact_id
                        && transfer_uuid
                            .as_ref()
                            .is_none_or(|named| *named == slot.meta.transfer_id)
                });
                let current_transfer = current.as_ref().map(|slot| slot.meta.transfer_id.clone());
                if let Some(slot) = current
                    && stop(slot).await.is_err()
                {
                    warn!(peer=?node_id, "The aborted attempt did not stop in time");
                }
                if transfer_uuid.is_none() {
                    warn!(peer=?node_id, "Peer request transfer abort without a transfer id. Assuming current transfer");
                }
                let Some(transfer_id) = transfer_uuid.or(current_transfer) else {
                    warn!(peer=?node_id, "Attempted to abort transfer that we don't know");
                    return FileResponseV2::ProtocolViolation;
                };
                ports.drop_incoming(transfer_id, node_id, contact_id).await;
                FileResponseV2::ConfirmAbort
            }
            FileRequestV2::ProtocolViolation { transfer_uuid } => {
                warn!(peer=?node_id, transfer_id=?transfer_uuid, "Peer reported protocol error on our side");
                let mut me = this.timeout_lock().await;
                let names_another = transfer_uuid.as_ref().is_some_and(|named| {
                    me.streams
                        .get(&node_id)
                        .is_some_and(|slot| slot.meta.transfer_id != *named)
                });
                if !names_another {
                    me.clear_stream(node_id);
                }
                FileResponseV2::AcknowledgeProtocolViolation
            }
        }
    }

    /// No task writes to the transfer's part file any more, so its length is what was received.
    async fn continue_from_storage(
        this: &Arc<Mutex<Self>>,
        ports: &impl ReceiverPorts,
        node_id: PeerId,
        contact_id: Option<ContactId>,
        filerequest_id: FylesId,
        transfer_uuid: FylesId,
    ) -> FileResponseV2 {
        let Some(FilerequestContinueResponse {
            drive_handler,
            file_name,
            file_size_bytes,
            started_at_ms,
        }) = ports
            .continuation(FilerequestContinueRequest {
                filerequest_id: filerequest_id.clone(),
                contact_id: contact_id.clone(),
                peer_id: node_id.to_base58(),
                transfer_id: transfer_uuid.clone(),
            })
            .await
        else {
            return FileResponseV2::RestartContinueTransfer;
        };
        let meta = IncomingMeta {
            transfer_id: transfer_uuid,
            filerequest_id,
            contact_id,
            file_name,
            started_at_ms,
            drive_handler,
        };
        let part_file_name = part_file_name(&meta, &node_id);
        let fail = async || {
            ports.mark_failed(meta.transfer_id.clone()).await;
            FileResponseV2::RestartContinueTransfer
        };
        let file = match meta
            .drive_handler
            .get_partial_file_for_writing(&part_file_name)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                error!(peer=?node_id, error=?e, "Could not open file to continue transfer");
                return fail().await;
            }
        };
        let continue_at = match file.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                error!(error=?e, "Error getting metadata");
                return FileResponseV2::InternalError;
            }
        };
        let Some(file) = ReceivingFile::resuming(file, continue_at, file_size_bytes) else {
            error!(peer=?node_id, continue_at, file_size_bytes, "Partial file is longer than the transfer, starting over");
            if let Err(e) = meta
                .drive_handler
                .remove_partial_file(&part_file_name)
                .await
            {
                error!(error=?e, "Could not remove the oversized partial file");
            }
            return fail().await;
        };
        let grant = AttemptGrant::mint();
        info!(?continue_at, "Ready to continue receiving file");
        Self::reserve(
            this,
            node_id,
            meta,
            Reservation {
                grant: grant.clone(),
                file,
            },
        )
        .await;
        FileResponseV2::AcceptContinueTransfer {
            offset: continue_at,
            grant,
        }
    }
}

/// Accepts this swarm's inbound data streams until its behaviour is dropped, which ends the
/// acceptor along with the swarm it was registered on.
pub fn spawn_file_stream_acceptor(
    swarm: &mut impl LocalNetworkSwarm,
    receiver: Arc<Mutex<FilerequestReceiver>>,
    shutdown: CancellationToken,
) {
    match swarm
        .file_stream_control()
        .accept(StreamProtocol::new(FILE_DATA_PROTOCOL))
    {
        Ok(incoming) => {
            tokio::spawn(accept_file_streams(receiver, incoming, shutdown).in_current_span());
        }
        Err(error) => error!(?error, "Cannot accept file data streams"),
    }
}

/// Hands each inbound data stream to [`bind`].
async fn accept_file_streams(
    receiver: Arc<Mutex<FilerequestReceiver>>,
    mut incoming: libp2p_stream::IncomingStreams,
    shutdown: CancellationToken,
) {
    loop {
        let (peer, stream) = tokio::select! {
            _ = shutdown.cancelled() => return,
            next = incoming.next() => match next {
                Some(next) => next,
                None => return,
            },
        };
        let receiver = receiver.clone();
        tokio::spawn(bind(receiver, peer, stream).in_current_span());
    }
}

/// Until its `Begin` authenticates, a stream has proven nothing and must leave the reservation
/// it names untouched, or anyone who saw the attempt id could take the transfer over.
async fn bind(receiver: Arc<Mutex<FilerequestReceiver>>, peer: PeerId, mut stream: Stream) {
    let mut id = [0u8; 16];
    let Ok(Ok(())) = timeout(ATTEMPT_OPEN_TIMEOUT, stream.read_exact(&mut id)).await else {
        debug!(?peer, "Data stream did not name its attempt");
        return;
    };
    let attempt = AttemptId::from_bytes(id);
    let grant = {
        let me = receiver.timeout_lock().await;
        match me.streams.get(&peer).map(|slot| &slot.stage) {
            Some(Stage::Reserved(reservation)) if reservation.grant.attempt == attempt => {
                reservation.grant.clone()
            }
            _ => {
                debug!(?peer, "Data stream names no reserved attempt");
                return;
            }
        }
    };
    let mut ends = ReceiverEnds::new(stream, &grant);
    if let Err(error) = await_begin(&mut ends).await {
        warn!(
            ?peer,
            ?error,
            "Data stream did not begin with a valid frame"
        );
        return;
    }
    let mut me = receiver.timeout_lock().await;
    let Some((meta, reservation)) = me.take_reservation(peer, attempt) else {
        debug!(?peer, "The reservation went away while the stream began");
        return;
    };
    let cancel = CancellationToken::new();
    let streaming = StreamingTransfer {
        attempt,
        cancel: cancel.clone(),
        task: tokio::spawn(
            run_receive(
                receiver.clone(),
                peer,
                ends,
                meta.clone(),
                reservation,
                me.brain_action_sender.clone(),
                cancel,
            )
            .in_current_span(),
        ),
    };
    me.streams.insert(
        peer,
        StreamSlot {
            meta,
            stage: Stage::Streaming(streaming),
        },
    );
}

async fn run_receive(
    receiver: Arc<Mutex<FilerequestReceiver>>,
    peer: PeerId,
    mut ends: ReceiverEnds<Stream>,
    meta: Arc<IncomingMeta>,
    reservation: Reservation,
    brain: Sender<BrainAction>,
    cancel: CancellationToken,
) {
    let Reservation { grant, mut file } = reservation;
    let part_file_name = part_file_name(&meta, &peer);
    let report = |progress_bytes| {
        // Progress is superseded by the next report, so a full channel may drop it.
        let _ = brain.try_send(BrainAction::NetworkNode(
            NetworkNodeAction::UpdateReceivedFileStatus {
                transfer_id: meta.transfer_id.clone(),
                status: ReceiveStatus::Receiving,
                progress_bytes,
            },
        ));
    };
    let finalize = async || {
        finalize_received(
            &meta.drive_handler,
            &part_file_name,
            &meta.file_name,
            meta.transfer_id.clone(),
            &brain,
        )
        .await
    };
    let exit = receive(&mut ends, &mut file, report, finalize, &cancel).await;
    debug!(?peer, ?exit, written = file.written(), "Data stream ended");
    let settled = match exit {
        ReceiveExit::Unfinished(Unfinished::Interrupted) => Some(ReceiveStatus::Receiving),
        ReceiveExit::Unfinished(Unfinished::IoError) => Some(ReceiveStatus::Failed),
        ReceiveExit::Committed
        | ReceiveExit::FinalizeFailed
        | ReceiveExit::Unfinished(Unfinished::Violation | Unfinished::Cancelled) => None,
    };
    if let Some(status) = settled {
        brain
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::UpdateReceivedFileStatus {
                    transfer_id: meta.transfer_id.clone(),
                    status,
                    progress_bytes: file.written(),
                },
            ))
            .await
            .expect("Sending to work");
    }
    // Whoever stops a task takes its entry out first, so this finds nothing then.
    receiver.timeout_lock().await.take_stream_if(
        peer,
        |slot| matches!(&slot.stage, Stage::Streaming(streaming) if streaming.attempt == grant.attempt),
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fyles_core::core::brain::error::FilerequestError;
    use fyles_core::core::domain_models::{Filerequest, FilerequestAccess};
    use fyles_core::io_controller::test::TestHostController;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc::channel;

    use super::*;

    const STARTED_AT_MS: i64 = 1_000;

    fn inbox(host: Arc<TestHostController>) -> FilerequestDriveHandler {
        FilerequestDriveHandler::new(
            host,
            Filerequest {
                id: FylesId::new(),
                title: "inbox".into(),
                description: "".into(),
                access: FilerequestAccess::Public,
                is_active: true,
            },
        )
    }

    fn reservation() -> (IncomingMeta, Reservation) {
        let file = tempfile::tempfile().expect("a temp file to be creatable");
        (
            IncomingMeta {
                transfer_id: FylesId::new(),
                filerequest_id: FylesId::new(),
                contact_id: None,
                file_name: "file".into(),
                started_at_ms: 0,
                drive_handler: inbox(Arc::new(TestHostController::new(None))),
            },
            Reservation {
                grant: AttemptGrant::mint(),
                file: ReceivingFile::starting(File::from_std(file), 1),
            },
        )
    }

    #[derive(Default)]
    struct Calls {
        failed: Vec<FylesId>,
        dropped: Vec<FylesId>,
    }

    struct FakePorts {
        host: Arc<TestHostController>,
        admits_drop: bool,
        create_fails: bool,
        stored_size: Option<u64>,
        calls: std::sync::Mutex<Calls>,
    }

    impl FakePorts {
        fn new() -> Self {
            Self {
                host: Arc::new(TestHostController::new(None)),
                admits_drop: true,
                create_fails: false,
                stored_size: None,
                calls: Default::default(),
            }
        }

        async fn write_part_file(&self, transfer: &FylesId, peer: &PeerId, bytes: &[u8]) {
            let name = get_part_file_name(STARTED_AT_MS, &transfer.0, &peer.to_base58(), &None);
            let mut file = inbox(self.host.clone())
                .get_partial_file_for_writing(&name)
                .await
                .expect("the part file to be writable");
            file.write_all(bytes)
                .await
                .expect("the part file to take bytes");
            file.flush().await.expect("the part file to flush");
        }
    }

    #[async_trait]
    impl ReceiverPorts for FakePorts {
        async fn request_file_drop(
            &self,
            _filerequest_id: FylesId,
            _contact_id: Option<ContactId>,
        ) -> Option<FilerequestDriveHandler> {
            self.admits_drop.then(|| inbox(self.host.clone()))
        }

        async fn create_incoming(&self, _file: CreateIncomingFile) -> FilerequestResult<FylesId> {
            if self.create_fails {
                Err(FilerequestError::InputError("refused".into()))
            } else {
                Ok(FylesId::new())
            }
        }

        async fn continuation(
            &self,
            _request: FilerequestContinueRequest,
        ) -> Option<FilerequestContinueResponse> {
            self.stored_size
                .map(|file_size_bytes| FilerequestContinueResponse {
                    drive_handler: inbox(self.host.clone()),
                    file_name: "file".into(),
                    file_size_bytes,
                    started_at_ms: STARTED_AT_MS,
                })
        }

        async fn mark_failed(&self, transfer_id: FylesId) {
            self.calls.lock().unwrap().failed.push(transfer_id);
        }

        async fn drop_incoming(
            &self,
            transfer_id: FylesId,
            _peer: PeerId,
            _contact_id: Option<ContactId>,
        ) {
            self.calls.lock().unwrap().dropped.push(transfer_id);
        }
    }

    fn receiver() -> Arc<Mutex<FilerequestReceiver>> {
        Arc::new(Mutex::new(FilerequestReceiver::new(channel(1).0)))
    }

    fn offer(transfer: &FylesId) -> FileRequestV2 {
        FileRequestV2::NewTransfer {
            filerequest_id: FylesId::new(),
            file_name: "file".into(),
            file_size_bytes: 10,
            transfer_uuid: transfer.clone(),
        }
    }

    fn continuation(transfer: &FylesId) -> FileRequestV2 {
        FileRequestV2::ContinueTransfer {
            filerequest_id: FylesId::new(),
            transfer_uuid: transfer.clone(),
        }
    }

    async fn reserved_attempt(
        receiver: &Arc<Mutex<FilerequestReceiver>>,
        peer: &PeerId,
    ) -> Option<AttemptId> {
        match receiver
            .lock()
            .await
            .streams
            .get(peer)
            .map(|slot| &slot.stage)
        {
            Some(Stage::Reserved(reservation)) => Some(reservation.grant.attempt),
            _ => None,
        }
    }

    #[tokio::test]
    async fn a_refused_drop_rejects_without_reserving() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let ports = FakePorts {
            admits_drop: false,
            ..FakePorts::new()
        };
        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            offer(&FylesId::new()),
        )
        .await;
        assert!(matches!(response, FileResponseV2::RejectNewTransfer));
        assert_eq!(reserved_attempt(&receiver, &peer).await, None);
    }

    #[tokio::test]
    async fn an_unrecorded_offer_is_an_internal_error_without_reserving() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let ports = FakePorts {
            create_fails: true,
            ..FakePorts::new()
        };
        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            offer(&FylesId::new()),
        )
        .await;
        assert!(matches!(response, FileResponseV2::InternalError));
        assert_eq!(reserved_attempt(&receiver, &peer).await, None);
    }

    #[tokio::test]
    async fn an_accepted_offer_reserves_the_grant_it_hands_out() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &FakePorts::new(),
            peer,
            None,
            offer(&FylesId::new()),
        )
        .await;
        let FileResponseV2::AcceptNewTransfer { grant } = response else {
            panic!("the offer to be accepted, got {response:?}");
        };
        assert_eq!(
            reserved_attempt(&receiver, &peer).await,
            Some(grant.attempt)
        );
    }

    #[tokio::test]
    async fn a_continuation_nothing_was_stored_for_restarts() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let ports = FakePorts::new();
        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            continuation(&FylesId::new()),
        )
        .await;
        assert!(matches!(response, FileResponseV2::RestartContinueTransfer));
        assert!(ports.calls.lock().unwrap().failed.is_empty());
    }

    #[tokio::test]
    async fn a_partial_file_longer_than_the_transfer_restarts_it() {
        let (receiver, peer, transfer) = (receiver(), PeerId::random(), FylesId::new());
        let ports = FakePorts {
            stored_size: Some(10),
            ..FakePorts::new()
        };
        ports.write_part_file(&transfer, &peer, &[1; 20]).await;

        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            continuation(&transfer),
        )
        .await;

        assert!(matches!(response, FileResponseV2::RestartContinueTransfer));
        assert_eq!(ports.calls.lock().unwrap().failed, vec![transfer]);
        assert_eq!(reserved_attempt(&receiver, &peer).await, None);
    }

    #[tokio::test]
    async fn a_partial_file_continues_where_it_ends() {
        let (receiver, peer, transfer) = (receiver(), PeerId::random(), FylesId::new());
        let ports = FakePorts {
            stored_size: Some(10),
            ..FakePorts::new()
        };
        ports.write_part_file(&transfer, &peer, &[1; 4]).await;

        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            continuation(&transfer),
        )
        .await;

        let FileResponseV2::AcceptContinueTransfer { offset, grant } = response else {
            panic!("the continuation to be accepted, got {response:?}");
        };
        assert_eq!(offset, 4);
        assert_eq!(
            reserved_attempt(&receiver, &peer).await,
            Some(grant.attempt)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_running_attempt_that_will_not_stop_is_an_internal_error() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let (meta, _) = reservation();
        let (transfer, filerequest) = (meta.transfer_id.clone(), meta.filerequest_id.clone());
        receiver.lock().await.streams.insert(
            peer,
            StreamSlot {
                meta: Arc::new(meta),
                stage: Stage::Streaming(StreamingTransfer {
                    attempt: AttemptGrant::mint().attempt,
                    cancel: CancellationToken::new(),
                    task: tokio::spawn(std::future::pending()),
                }),
            },
        );

        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &FakePorts::new(),
            peer,
            None,
            FileRequestV2::ContinueTransfer {
                filerequest_id: filerequest,
                transfer_uuid: transfer,
            },
        )
        .await;

        assert!(matches!(response, FileResponseV2::InternalError));
    }

    #[tokio::test]
    async fn an_abort_naming_nothing_while_nothing_runs_is_a_violation() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let ports = FakePorts::new();
        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            FileRequestV2::AbortTransfer {
                transfer_uuid: None,
            },
        )
        .await;
        assert!(matches!(response, FileResponseV2::ProtocolViolation));
        assert!(ports.calls.lock().unwrap().dropped.is_empty());
    }

    #[tokio::test]
    async fn a_named_abort_drops_the_transfer() {
        let (receiver, peer, transfer) = (receiver(), PeerId::random(), FylesId::new());
        let ports = FakePorts::new();
        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &ports,
            peer,
            None,
            FileRequestV2::AbortTransfer {
                transfer_uuid: Some(transfer.clone()),
            },
        )
        .await;
        assert!(matches!(response, FileResponseV2::ConfirmAbort));
        assert_eq!(ports.calls.lock().unwrap().dropped, vec![transfer]);
    }

    #[tokio::test]
    async fn a_violation_naming_another_transfer_leaves_the_current_one() {
        let (receiver, peer) = (receiver(), PeerId::random());
        let (meta, reserved) = reservation();
        let attempt = reserved.grant.attempt;
        FilerequestReceiver::reserve(&receiver, peer, meta, reserved).await;

        let response = FilerequestReceiver::respond_to_v2(
            &receiver,
            &FakePorts::new(),
            peer,
            None,
            FileRequestV2::ProtocolViolation {
                transfer_uuid: Some(FylesId::new()),
            },
        )
        .await;

        assert!(matches!(
            response,
            FileResponseV2::AcknowledgeProtocolViolation
        ));
        assert_eq!(reserved_attempt(&receiver, &peer).await, Some(attempt));
    }

    #[tokio::test(start_paused = true)]
    async fn an_unopened_grant_expires_on_its_own_timer() {
        let receiver = Arc::new(Mutex::new(FilerequestReceiver::new(channel(1).0)));
        let peer = PeerId::random();
        let (meta, older) = reservation();
        FilerequestReceiver::reserve(&receiver, peer, meta, older).await;
        sleep(ATTEMPT_OPEN_TIMEOUT / 2).await;
        let (meta, newer) = reservation();
        let newer_attempt = newer.grant.attempt;
        FilerequestReceiver::reserve(&receiver, peer, meta, newer).await;

        sleep(ATTEMPT_OPEN_TIMEOUT / 2 + Duration::from_millis(1)).await;
        assert!(
            matches!(receiver.lock().await.streams.get(&peer).map(|slot| &slot.stage), Some(Stage::Reserved(r)) if r.grant.attempt == newer_attempt),
            "the earlier grant's timer must leave the newer grant alone"
        );

        sleep(ATTEMPT_OPEN_TIMEOUT / 2).await;
        assert!(!receiver.lock().await.streams.contains_key(&peer));
    }
}
