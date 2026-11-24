use std::sync::Arc;

use crypto::{DeSerCryptError, SerCryptError};
use fyles_core::core::domain_models::{
    ContactId, InProgress, InProgressSendStatus, PendingFile, SendStatus, TransferData,
};
use libp2p::request_response::OutboundFailure;
use tracing::{info, warn};

use super::file_tracker::{AttemptStart, FileOutcome};
use super::send_manager::{
    ControlRequest, DataPath, OpeningRequest, Request, SendDataMut, SendRes, SendSlot,
    StreamAttempt, StreamPath, StreamPhase,
};
use crate::file_stream::codec::Failure;
use crate::file_stream::send::AttemptOutcome;
use crate::send_receive_traits::session_send_receive::{LocalReceiveError, ReceiveError};
use crate::sessions::SessionEstablishmentError;
use crate::types::{AttemptGrant, FileResponse, FileResponseV2};

pub type FileReceiveError =
    ReceiveError<OutboundFailure, DeSerCryptError, SerCryptError, Arc<SessionEstablishmentError>>;

/// A response on either wire version, as the send slot sees it.
#[derive(Debug)]
pub enum Response {
    Accepted(Accepted, DataGrant),
    RejectNew,
    RestartContinue,
    RejectContinue,
    ConfirmChunk,
    RejectChunk,
    ConfirmDone,
    RejectDone,
    ConfirmAbort,
    AcknowledgeViolation,
    Violation,
    InternalError,
}

#[derive(Debug, Clone, Copy)]
pub enum Accepted {
    New,
    Continue { offset: u64 },
}

#[derive(Debug)]
pub enum DataGrant {
    Chunks,
    Stream(AttemptGrant),
}

impl From<FileResponse> for Response {
    fn from(response: FileResponse) -> Self {
        match response {
            FileResponse::AcceptNewTransfer => Response::Accepted(Accepted::New, DataGrant::Chunks),
            FileResponse::AcceptContinueTransfer { offset } => {
                Response::Accepted(Accepted::Continue { offset }, DataGrant::Chunks)
            }
            FileResponse::RejectNewTransfer => Response::RejectNew,
            FileResponse::RestartContinueTransfer => Response::RestartContinue,
            FileResponse::RejectContinueTransfer => Response::RejectContinue,
            FileResponse::ConfirmChunk => Response::ConfirmChunk,
            FileResponse::RejectChunk => Response::RejectChunk,
            FileResponse::ConfirmDone => Response::ConfirmDone,
            FileResponse::RejectDone => Response::RejectDone,
            FileResponse::ConfirmAbort => Response::ConfirmAbort,
            FileResponse::AcknowledgeProtocolViolation => Response::AcknowledgeViolation,
            FileResponse::ProtocolViolation => Response::Violation,
            FileResponse::InternalError => Response::InternalError,
        }
    }
}

impl From<FileResponseV2> for Response {
    fn from(response: FileResponseV2) -> Self {
        match response {
            FileResponseV2::AcceptNewTransfer { grant } => {
                Response::Accepted(Accepted::New, DataGrant::Stream(grant))
            }
            FileResponseV2::AcceptContinueTransfer { offset, grant } => {
                Response::Accepted(Accepted::Continue { offset }, DataGrant::Stream(grant))
            }
            FileResponseV2::RejectNewTransfer => Response::RejectNew,
            FileResponseV2::RestartContinueTransfer => Response::RestartContinue,
            FileResponseV2::RejectContinueTransfer => Response::RejectContinue,
            FileResponseV2::ConfirmAbort => Response::ConfirmAbort,
            FileResponseV2::AcknowledgeProtocolViolation => Response::AcknowledgeViolation,
            FileResponseV2::ProtocolViolation => Response::Violation,
            FileResponseV2::InternalError => Response::InternalError,
        }
    }
}

/// Who broke the protocol. The peer is told about its own violations. Ours it reported itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Party {
    Us,
    Peer,
}

pub enum Verdict {
    Complete(FileOutcome),
    AbortAndComplete(FileOutcome),
    SetBack(String),
    Interrupt(&'static str),
    Violation(Party, &'static str),
    RestartAsNew(&'static str),
    FallBackToChunks,
    SendChunks { connected: Option<ContactId> },
    ChunkConfirmed { connected: Option<ContactId> },
    StartAttempt(Box<AttemptStart>),
}

pub enum ChunkStep {
    Wait,
    Continue,
    SendDone,
    Stop(Verdict),
}

/// Decides what a claimed response means for its slot. Only a session the slot admits may change
/// it, so an impostor can delay the transfer but never decide it.
pub fn on_response(
    slot: &mut SendSlot<usize>,
    connected: &Option<ContactId>,
    request: Request,
    response: Response,
) -> Verdict {
    let Some(SendDataMut { path, pending_file }) = slot.advance_for(connected) else {
        warn!(expected=?slot.pending_file.contact_id, got=?connected, "Response from a session that may not advance the transfer");
        return Verdict::SetBack("Contact verification failed".into());
    };
    match response {
        Response::Violation => Verdict::Violation(Party::Us, "Peer reported protocol violation"),
        Response::InternalError => Verdict::SetBack("Peer reported internal error".into()),
        Response::ConfirmAbort | Response::AcknowledgeViolation => mismatch(
            "Response answers a request it cannot belong to",
            &request,
            &pending_file.status,
        ),
        Response::RejectNew => answer_to(
            &request,
            ControlRequest::Offer,
            &pending_file.status,
            Verdict::Complete(FileOutcome::Rejected),
        ),
        Response::RestartContinue => answer_to(
            &request,
            ControlRequest::Continue,
            &pending_file.status,
            Verdict::RestartAsNew("Receiver could not continue the transfer"),
        ),
        Response::RejectContinue => answer_to(
            &request,
            ControlRequest::Continue,
            &pending_file.status,
            Verdict::Complete(FileOutcome::Rejected),
        ),
        Response::Accepted(accepted, data) => {
            accept(path, pending_file, connected, request, accepted, data)
        }
        Response::ConfirmChunk => confirm_chunk(path, pending_file, connected, request),
        Response::RejectChunk => match (&request, &pending_file.status) {
            (
                Request::Chunk(_),
                SendStatus::InProgress(InProgress {
                    status: InProgressSendStatus::Sending,
                    ..
                }),
            ) => Verdict::Complete(FileOutcome::Rejected),
            _ => mismatch(
                "Rejected chunk that wasn't sent",
                &request,
                &pending_file.status,
            ),
        },
        Response::ConfirmDone => done(&request, &pending_file.status, FileOutcome::Sent),
        Response::RejectDone => done(&request, &pending_file.status, FileOutcome::Rejected),
    }
}

/// What the next file in line asks the receiver for.
pub enum Opening {
    Offer,
    Continue(TransferData),
}

#[derive(Debug)]
pub struct Unsendable;

impl Opening {
    pub fn offset(&self) -> u64 {
        match self {
            Opening::Offer => 0,
            Opening::Continue(transfer_data) => transfer_data.progress_bytes,
        }
    }

    pub fn request(&self) -> OpeningRequest {
        match self {
            Opening::Offer => OpeningRequest::Offer,
            Opening::Continue(_) => OpeningRequest::Continue,
        }
    }
}

/// Only a file that never started, or whose transfer was interrupted, waits in line. Any other
/// status there means the queue lost track of it.
pub fn opening(status: &SendStatus) -> Result<Opening, Unsendable> {
    match status {
        SendStatus::Pending => Ok(Opening::Offer),
        SendStatus::InProgress(InProgress {
            status: InProgressSendStatus::Interrupted,
            transfer_data,
        }) => Ok(Opening::Continue(transfer_data.clone())),
        _ => Err(Unsendable),
    }
}

pub fn on_attempt_outcome(outcome: AttemptOutcome) -> Verdict {
    match outcome {
        AttemptOutcome::Committed => Verdict::Complete(FileOutcome::Sent),
        AttemptOutcome::ReceiverFailed(Failure::Violation) => {
            Verdict::Violation(Party::Us, "Receiver reported a protocol violation")
        }
        AttemptOutcome::ProtocolViolation => {
            Verdict::Violation(Party::Peer, "Receiver broke the stream protocol")
        }
        AttemptOutcome::ReceiverFailed(Failure::Internal) => {
            Verdict::SetBack("Receiver failed internally".into())
        }
        AttemptOutcome::Unauthentic => Verdict::SetBack("Stream frame did not authenticate".into()),
        AttemptOutcome::Interrupted(reason) => Verdict::Interrupt(reason),
        AttemptOutcome::SourceChanged => Verdict::RestartAsNew("Source file changed while sending"),
        AttemptOutcome::SourceIo(error) => Verdict::AbortAndComplete(FileOutcome::IoError(error)),
    }
}

/// None of these errors proves anything about the peer. The remote error travels unencrypted,
/// anyone on the path can send what fails to decrypt, and the rest is transport or our own
/// trouble. So none of them counts toward failing the file.
pub fn on_receive_error(slot: &SendSlot<usize>, error: FileReceiveError) -> Verdict {
    match error {
        ReceiveError::RemoteError(error) => Verdict::SetBack(format!("Remote error: {error:?}")),
        ReceiveError::LocalError(LocalReceiveError::Timeout) => {
            Verdict::SetBack("Request timeout".into())
        }
        ReceiveError::LocalError(LocalReceiveError::TransportError(error)) => match error {
            OutboundFailure::UnsupportedProtocols => match slot.path() {
                DataPath::Stream(_) => {
                    info!(
                        "Peer does not speak the stream protocol, offering the file over the chunked one"
                    );
                    Verdict::FallBackToChunks
                }
                DataPath::Chunked(_) => Verdict::Complete(FileOutcome::Rejected),
            },
            error @ (OutboundFailure::DialFailure
            | OutboundFailure::Timeout
            | OutboundFailure::Io(_)
            | OutboundFailure::ConnectionClosed) => {
                Verdict::SetBack(format!("Transport error: {error:?}"))
            }
        },
        ReceiveError::LocalError(
            error @ (LocalReceiveError::MissingSession | LocalReceiveError::DecryptError(_)),
        ) => Verdict::SetBack(format!("Session/decrypt error: {error:?}")),
        ReceiveError::LocalError(error @ LocalReceiveError::ResendError(_)) => {
            Verdict::SetBack(format!("Resend error: {error:?}"))
        }
    }
}

pub fn on_chunk_send(res: SendRes<usize>) -> ChunkStep {
    match res {
        SendRes::NotReady => ChunkStep::Wait,
        SendRes::Sent(_) => ChunkStep::Continue,
        SendRes::Done => ChunkStep::SendDone,
        SendRes::IoError(error) => {
            ChunkStep::Stop(Verdict::AbortAndComplete(FileOutcome::IoError(error)))
        }
        SendRes::SourceChanged => {
            ChunkStep::Stop(Verdict::RestartAsNew("Source file changed while sending"))
        }
        SendRes::Interrupt => {
            ChunkStep::Stop(Verdict::Interrupt("file tracker could not send to peer"))
        }
    }
}

/// Compute the appropriate reset status for a file being pushed back to pending.
/// Prepared → Pending (ephemeral, brain never knew), everything else → Interrupted.
pub fn reset_status(slot: &mut SendSlot<usize>) -> SendStatus {
    let streamed = slot.running_attempt().map(StreamAttempt::progress_bytes);
    if let (Some(progress_bytes), SendStatus::InProgress(InProgress { transfer_data, .. })) =
        (streamed, &mut slot.pending_file.status)
    {
        transfer_data.progress_bytes = progress_bytes;
    }
    match slot.pending_file.status {
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
            ref transfer_data, ..
        }) => SendStatus::InProgress(InProgress {
            status: InProgressSendStatus::Interrupted,
            transfer_data: transfer_data.clone(),
        }),
        _ => SendStatus::Pending,
    }
}

fn mismatch(reason: &'static str, request: &Request, status: &SendStatus) -> Verdict {
    warn!(?request, ?status, reason);
    Verdict::Violation(Party::Peer, reason)
}

fn answer_to(
    request: &Request,
    expected: ControlRequest,
    status: &SendStatus,
    verdict: Verdict,
) -> Verdict {
    if *request == Request::Control(expected) {
        verdict
    } else {
        mismatch("Response does not answer its request", request, status)
    }
}

fn done(request: &Request, status: &SendStatus, outcome: FileOutcome) -> Verdict {
    match (request, status) {
        (
            Request::Control(ControlRequest::Done),
            SendStatus::InProgress(InProgress {
                status: InProgressSendStatus::PendingSent,
                ..
            }),
        ) => Verdict::Complete(outcome),
        _ => mismatch("Answer to done that wasn't sent", request, status),
    }
}

fn accept(
    path: &mut DataPath<usize>,
    pending_file: &mut PendingFile,
    connected: &Option<ContactId>,
    request: Request,
    accepted: Accepted,
    data: DataGrant,
) -> Verdict {
    let (expected, offset) = match accepted {
        Accepted::New => (OpeningRequest::Offer, 0),
        Accepted::Continue { offset } => (OpeningRequest::Continue, offset),
    };
    if request != Request::Control(expected.into()) {
        return mismatch(
            "Accept does not answer its request",
            &request,
            &pending_file.status,
        );
    }
    let SendStatus::InProgress(InProgress {
        status: status @ InProgressSendStatus::Prepared,
        transfer_data,
    }) = &mut pending_file.status
    else {
        return mismatch(
            "Accepted a transfer that is not waiting for acceptance",
            &request,
            &pending_file.status,
        );
    };
    if offset > transfer_data.file_size_bytes {
        warn!(
            offset,
            size = transfer_data.file_size_bytes,
            "Receiver wants to continue past the end"
        );
        return Verdict::Violation(Party::Peer, "Receiver wants to continue past the end");
    }
    match (data, path) {
        (DataGrant::Chunks, DataPath::Chunked(send_manager)) => {
            send_manager.resume_at(offset);
            *status = InProgressSendStatus::Sending;
            transfer_data.progress_bytes = offset;
            Verdict::SendChunks {
                connected: connected.clone(),
            }
        }
        (DataGrant::Stream(grant), DataPath::Stream(StreamPath { phase, .. })) => {
            let attempt = StreamAttempt::new(grant.attempt, offset);
            let progress = attempt.progress.clone();
            let cancel = attempt.cancel_token();
            match std::mem::replace(phase, StreamPhase::Running(attempt)) {
                StreamPhase::Negotiating(source) => {
                    *status = InProgressSendStatus::Sending;
                    transfer_data.progress_bytes = offset;
                    Verdict::StartAttempt(Box::new(AttemptStart {
                        grant,
                        source: *source,
                        offset,
                        progress,
                        cancel,
                        pending_file_id: pending_file.id.clone(),
                        transfer_data: transfer_data.clone(),
                    }))
                }
                StreamPhase::Running(running) => {
                    *phase = StreamPhase::Running(running);
                    warn!("Stream accepted for an attempt that is already running");
                    Verdict::Violation(
                        Party::Peer,
                        "Stream accepted for an attempt that is already running",
                    )
                }
            }
        }
        (data, _) => {
            warn!(?data, "Accept does not fit the transfer's data path");
            Verdict::Violation(Party::Peer, "Accept does not fit the transfer's data path")
        }
    }
}

fn confirm_chunk(
    path: &mut DataPath<usize>,
    pending_file: &mut PendingFile,
    connected: &Option<ContactId>,
    request: Request,
) -> Verdict {
    let (
        Request::Chunk(ticket),
        Some(send_manager),
        SendStatus::InProgress(InProgress {
            status: InProgressSendStatus::Sending,
            transfer_data,
        }),
    ) = (request, path.chunked_mut(), &mut pending_file.status)
    else {
        warn!(status=?pending_file.status, "Confirmed a chunk that wasn't sent");
        return Verdict::Violation(Party::Peer, "Confirmed a chunk that wasn't sent");
    };
    send_manager.receive_ack(ticket);
    transfer_data.progress_bytes = send_manager.progress_bytes();
    Verdict::ChunkConfirmed {
        connected: connected.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use fyles_core::core::domain_models::{FylesId, TransferData};

    use super::*;
    use crate::event_loop::filerequest::file_tracker::Wire;
    use crate::event_loop::filerequest::send_manager::ContactPin;
    use crate::send_receive_traits::session_send_receive::RemoteReceiveError;
    use crate::source::Source;

    const SIZE: u64 = 100;

    fn target() -> ContactId {
        ContactId::for_test(3)
    }

    fn stranger() -> ContactId {
        ContactId::for_test(119)
    }

    async fn new_slot(
        wire: Wire,
        pin: ContactPin,
        status: InProgressSendStatus,
    ) -> SendSlot<usize> {
        let mut file = tempfile::NamedTempFile::new().expect("a temp file to be creatable");
        file.write_all(&[7; SIZE as usize])
            .expect("the temp file to be writable");
        let source = Source::open(
            tokio::fs::File::open(file.path())
                .await
                .expect("the temp file to be openable"),
        )
        .await
        .expect("the temp file to be inspectable");
        let transfer_id = FylesId("transfer".into());
        let pending_file = PendingFile {
            id: FylesId("file".into()),
            contact_id: target(),
            file_path: file.path().to_string_lossy().into_owned(),
            retry_count: 0,
            target_filerequest_id: FylesId("fr".into()),
            status: InProgress {
                status,
                transfer_data: TransferData {
                    progress_bytes: 0,
                    file_size_bytes: SIZE,
                    source_modified_ms: 0,
                    transfer_id: transfer_id.clone(),
                },
            }
            .into(),
            display_name: None,
            interruption_reasons: Vec::new(),
        };
        match wire {
            Wire::Chunks => SendSlot::chunked(pending_file, transfer_id, source, pin),
            Wire::Streams => SendSlot::streamed(pending_file, transfer_id, source, pin),
        }
    }

    fn offer() -> Request {
        Request::Control(ControlRequest::Offer)
    }

    fn continuation() -> Request {
        Request::Control(ControlRequest::Continue)
    }

    fn done() -> Request {
        Request::Control(ControlRequest::Done)
    }

    async fn chunk(slot: &mut SendSlot<usize>) -> Request {
        let SendDataMut { path, .. } = slot
            .advance_for(&Some(target()))
            .expect("the target to be admitted");
        let send_manager = path.chunked_mut().expect("a chunked slot");
        let SendRes::Sent(id) = send_manager.send_next_chunk(async |_| Ok(9)).await else {
            panic!("the chunk to be sent");
        };
        slot.claim(&id).expect("the chunk to be claimable")
    }

    fn decide(slot: &mut SendSlot<usize>, request: Request, response: Response) -> Verdict {
        on_response(slot, &Some(target()), request, response)
    }

    fn progress(slot: &SendSlot<usize>) -> (InProgressSendStatus, u64) {
        let SendStatus::InProgress(in_progress) = &slot.pending_file.status else {
            panic!("the file to be in progress");
        };
        (
            in_progress.status.clone(),
            in_progress.transfer_data.progress_bytes,
        )
    }

    fn every_response() -> Vec<Response> {
        vec![
            Response::Accepted(Accepted::New, DataGrant::Chunks),
            Response::Accepted(
                Accepted::Continue { offset: 1 },
                DataGrant::Stream(AttemptGrant::mint()),
            ),
            Response::RejectNew,
            Response::RestartContinue,
            Response::RejectContinue,
            Response::ConfirmChunk,
            Response::RejectChunk,
            Response::ConfirmDone,
            Response::RejectDone,
            Response::ConfirmAbort,
            Response::AcknowledgeViolation,
            Response::Violation,
            Response::InternalError,
        ]
    }

    #[tokio::test]
    async fn a_pinned_slot_lets_no_other_session_decide_anything() {
        for connected in [None, Some(stranger())] {
            for response in every_response() {
                let mut slot = new_slot(
                    Wire::Streams,
                    ContactPin::Pinned,
                    InProgressSendStatus::Prepared,
                )
                .await;
                let verdict = on_response(&mut slot, &connected, offer(), response);
                assert!(matches!(verdict, Verdict::SetBack(_)));
                assert_eq!(progress(&slot).0, InProgressSendStatus::Prepared);
            }
        }
    }

    #[tokio::test]
    async fn an_unpinned_slot_lets_an_unauthenticated_session_decide() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Unpinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        let verdict = on_response(&mut slot, &None, offer(), Response::RejectNew);
        assert!(matches!(verdict, Verdict::Complete(FileOutcome::Rejected)));
    }

    #[tokio::test]
    async fn rejections_and_restarts_answer_their_own_request() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        assert!(matches!(
            decide(&mut slot, offer(), Response::RejectNew),
            Verdict::Complete(FileOutcome::Rejected)
        ));
        assert!(matches!(
            decide(&mut slot, continuation(), Response::RejectContinue),
            Verdict::Complete(FileOutcome::Rejected)
        ));
        assert!(matches!(
            decide(&mut slot, continuation(), Response::RestartContinue),
            Verdict::RestartAsNew(_)
        ));
        for (request, response) in [
            (continuation(), Response::RejectNew),
            (offer(), Response::RejectContinue),
            (offer(), Response::RestartContinue),
        ] {
            assert!(matches!(
                decide(&mut slot, request, response),
                Verdict::Violation(Party::Peer, _)
            ));
        }
    }

    #[tokio::test]
    async fn the_peer_blaming_us_counts_and_its_internal_error_does_not() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Sending,
        )
        .await;
        assert!(matches!(
            decide(&mut slot, offer(), Response::Violation),
            Verdict::Violation(Party::Us, _)
        ));
        assert!(matches!(
            decide(&mut slot, offer(), Response::InternalError),
            Verdict::SetBack(_)
        ));
    }

    #[tokio::test]
    async fn an_answer_to_an_unregistered_request_is_a_violation_once_claimed() {
        for response in [Response::ConfirmAbort, Response::AcknowledgeViolation] {
            let mut slot = new_slot(
                Wire::Chunks,
                ContactPin::Pinned,
                InProgressSendStatus::Prepared,
            )
            .await;
            assert!(matches!(
                decide(&mut slot, offer(), response),
                Verdict::Violation(Party::Peer, _)
            ));
        }
    }

    #[tokio::test]
    async fn an_accepted_offer_sends_chunks_from_the_start() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        let verdict = decide(
            &mut slot,
            offer(),
            Response::Accepted(Accepted::New, DataGrant::Chunks),
        );
        assert!(matches!(verdict, Verdict::SendChunks { .. }));
        assert_eq!(progress(&slot), (InProgressSendStatus::Sending, 0));
    }

    #[tokio::test]
    async fn an_accepted_continuation_resumes_chunks_at_the_offset() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        let verdict = decide(
            &mut slot,
            continuation(),
            Response::Accepted(Accepted::Continue { offset: 40 }, DataGrant::Chunks),
        );
        assert!(matches!(verdict, Verdict::SendChunks { .. }));
        assert_eq!(progress(&slot), (InProgressSendStatus::Sending, 40));
    }

    #[tokio::test]
    async fn an_accepted_stream_starts_its_attempt_at_the_offset() {
        let mut slot = new_slot(
            Wire::Streams,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        let grant = AttemptGrant::mint();
        let verdict = decide(
            &mut slot,
            continuation(),
            Response::Accepted(
                Accepted::Continue { offset: 40 },
                DataGrant::Stream(grant.clone()),
            ),
        );
        let Verdict::StartAttempt(start) = verdict else {
            panic!("the attempt to start");
        };
        assert_eq!((start.grant, start.offset), (grant.clone(), 40));
        assert_eq!(progress(&slot), (InProgressSendStatus::Sending, 40));
        assert_eq!(
            slot.running_attempt().map(|it| it.attempt),
            Some(grant.attempt)
        );
    }

    #[tokio::test]
    async fn an_accept_for_the_other_data_path_is_a_violation() {
        for (wire, data) in [
            (Wire::Chunks, DataGrant::Stream(AttemptGrant::mint())),
            (Wire::Streams, DataGrant::Chunks),
        ] {
            let mut slot = new_slot(wire, ContactPin::Pinned, InProgressSendStatus::Prepared).await;
            let verdict = decide(&mut slot, offer(), Response::Accepted(Accepted::New, data));
            assert!(matches!(verdict, Verdict::Violation(Party::Peer, _)));
            assert_eq!(progress(&slot).0, InProgressSendStatus::Prepared);
        }
    }

    #[tokio::test]
    async fn a_continuation_past_the_end_is_a_violation() {
        for wire in [Wire::Chunks, Wire::Streams] {
            let data = match wire {
                Wire::Chunks => DataGrant::Chunks,
                Wire::Streams => DataGrant::Stream(AttemptGrant::mint()),
            };
            let mut slot = new_slot(wire, ContactPin::Pinned, InProgressSendStatus::Prepared).await;
            let verdict = decide(
                &mut slot,
                continuation(),
                Response::Accepted(Accepted::Continue { offset: SIZE + 1 }, data),
            );
            assert!(matches!(verdict, Verdict::Violation(Party::Peer, _)));
        }
    }

    #[tokio::test]
    async fn an_accept_that_answers_the_wrong_request_or_comes_late_is_a_violation() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        let verdict = decide(
            &mut slot,
            continuation(),
            Response::Accepted(Accepted::New, DataGrant::Chunks),
        );
        assert!(matches!(verdict, Verdict::Violation(Party::Peer, _)));

        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Sending,
        )
        .await;
        let verdict = decide(
            &mut slot,
            offer(),
            Response::Accepted(Accepted::New, DataGrant::Chunks),
        );
        assert!(matches!(verdict, Verdict::Violation(Party::Peer, _)));
    }

    #[tokio::test]
    async fn a_confirmed_chunk_advances_the_progress() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Sending,
        )
        .await;
        let request = chunk(&mut slot).await;
        let verdict = decide(&mut slot, request, Response::ConfirmChunk);
        assert!(matches!(verdict, Verdict::ChunkConfirmed { .. }));
        assert_eq!(progress(&slot), (InProgressSendStatus::Sending, SIZE));
    }

    #[tokio::test]
    async fn a_chunk_answer_to_a_control_request_is_a_violation() {
        for response in [Response::ConfirmChunk, Response::RejectChunk] {
            let mut slot = new_slot(
                Wire::Chunks,
                ContactPin::Pinned,
                InProgressSendStatus::Sending,
            )
            .await;
            assert!(matches!(
                decide(&mut slot, offer(), response),
                Verdict::Violation(Party::Peer, _)
            ));
        }
    }

    #[tokio::test]
    async fn a_rejected_chunk_rejects_the_file() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Sending,
        )
        .await;
        let request = chunk(&mut slot).await;
        assert!(matches!(
            decide(&mut slot, request, Response::RejectChunk),
            Verdict::Complete(FileOutcome::Rejected)
        ));
    }

    #[tokio::test]
    async fn done_settles_the_file_only_once_it_was_sent() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::PendingSent,
        )
        .await;
        assert!(matches!(
            decide(&mut slot, done(), Response::ConfirmDone),
            Verdict::Complete(FileOutcome::Sent)
        ));
        assert!(matches!(
            decide(&mut slot, done(), Response::RejectDone),
            Verdict::Complete(FileOutcome::Rejected)
        ));

        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Sending,
        )
        .await;
        assert!(matches!(
            decide(&mut slot, done(), Response::ConfirmDone),
            Verdict::Violation(Party::Peer, _)
        ));
    }

    #[test]
    fn attempt_outcomes_count_only_authenticated_violations() {
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::Committed),
            Verdict::Complete(FileOutcome::Sent)
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::ReceiverFailed(Failure::Violation)),
            Verdict::Violation(Party::Us, _)
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::ProtocolViolation),
            Verdict::Violation(Party::Peer, _)
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::ReceiverFailed(Failure::Internal)),
            Verdict::SetBack(_)
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::Unauthentic),
            Verdict::SetBack(_)
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::Interrupted("gone")),
            Verdict::Interrupt("gone")
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::SourceChanged),
            Verdict::RestartAsNew(_)
        ));
        assert!(matches!(
            on_attempt_outcome(AttemptOutcome::SourceIo(std::io::Error::other("disk"))),
            Verdict::AbortAndComplete(FileOutcome::IoError(_))
        ));
    }

    #[tokio::test]
    async fn an_interrupted_stream_keeps_the_progress_its_attempt_reached() {
        let mut slot = new_slot(
            Wire::Streams,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        let Verdict::StartAttempt(start) = decide(
            &mut slot,
            offer(),
            Response::Accepted(Accepted::New, DataGrant::Stream(AttemptGrant::mint())),
        ) else {
            panic!("the attempt to start");
        };
        start
            .progress
            .store(70, std::sync::atomic::Ordering::Relaxed);

        let SendStatus::InProgress(in_progress) = reset_status(&mut slot) else {
            panic!("the file to stay in progress");
        };
        assert_eq!(in_progress.status, InProgressSendStatus::Interrupted);
        assert_eq!(in_progress.transfer_data.progress_bytes, 70);
    }

    #[tokio::test]
    async fn an_unadmitted_offer_starts_over_and_an_unadmitted_continuation_does_not() {
        let mut slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        assert_eq!(reset_status(&mut slot), SendStatus::Pending);

        let SendStatus::InProgress(in_progress) = &mut slot.pending_file.status else {
            panic!("the file to be in progress");
        };
        in_progress.transfer_data.progress_bytes = 40;
        let SendStatus::InProgress(reset) = reset_status(&mut slot) else {
            panic!("a continuation to keep its progress");
        };
        assert_eq!(reset.status, InProgressSendStatus::Interrupted);
        assert_eq!(reset.transfer_data.progress_bytes, 40);
    }

    #[tokio::test]
    async fn a_peer_without_the_stream_protocol_gets_chunks_and_one_without_either_rejects() {
        let unsupported = || {
            ReceiveError::LocalError(LocalReceiveError::TransportError(
                OutboundFailure::UnsupportedProtocols,
            ))
        };
        let slot = new_slot(
            Wire::Streams,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        assert!(matches!(
            on_receive_error(&slot, unsupported()),
            Verdict::FallBackToChunks
        ));
        let slot = new_slot(
            Wire::Chunks,
            ContactPin::Pinned,
            InProgressSendStatus::Prepared,
        )
        .await;
        assert!(matches!(
            on_receive_error(&slot, unsupported()),
            Verdict::Complete(FileOutcome::Rejected)
        ));
    }

    #[tokio::test]
    async fn no_receive_error_counts() {
        let slot = new_slot(
            Wire::Streams,
            ContactPin::Pinned,
            InProgressSendStatus::Sending,
        )
        .await;
        let errors: [FileReceiveError; 4] = [
            ReceiveError::RemoteError(RemoteReceiveError::CannotDecrypt),
            ReceiveError::LocalError(LocalReceiveError::Timeout),
            ReceiveError::LocalError(LocalReceiveError::TransportError(
                OutboundFailure::ConnectionClosed,
            )),
            ReceiveError::LocalError(LocalReceiveError::MissingSession),
        ];
        for error in errors {
            assert!(matches!(
                on_receive_error(&slot, error),
                Verdict::SetBack(_)
            ));
        }
    }

    #[test]
    fn a_chunk_send_that_cannot_go_on_stops_with_a_verdict() {
        assert!(matches!(on_chunk_send(SendRes::NotReady), ChunkStep::Wait));
        assert!(matches!(
            on_chunk_send(SendRes::Sent(1)),
            ChunkStep::Continue
        ));
        assert!(matches!(on_chunk_send(SendRes::Done), ChunkStep::SendDone));
        assert!(matches!(
            on_chunk_send(SendRes::IoError(std::io::Error::other("disk"))),
            ChunkStep::Stop(Verdict::AbortAndComplete(FileOutcome::IoError(_)))
        ));
        assert!(matches!(
            on_chunk_send(SendRes::SourceChanged),
            ChunkStep::Stop(Verdict::RestartAsNew(_))
        ));
        assert!(matches!(
            on_chunk_send(SendRes::Interrupt),
            ChunkStep::Stop(Verdict::Interrupt(_))
        ));
    }

    fn in_progress(status: InProgressSendStatus) -> SendStatus {
        InProgress {
            status,
            transfer_data: TransferData {
                progress_bytes: 40,
                file_size_bytes: SIZE,
                source_modified_ms: 0,
                transfer_id: FylesId("transfer".into()),
            },
        }
        .into()
    }

    #[test]
    fn a_pending_file_is_offered_from_the_start() {
        let opening = opening(&SendStatus::Pending).expect("a pending file to be sendable");
        assert!(matches!(opening, Opening::Offer));
        assert_eq!(opening.offset(), 0);
        assert_eq!(opening.request(), OpeningRequest::Offer);
    }

    #[test]
    fn an_interrupted_file_continues_where_it_stopped() {
        let opening = opening(&in_progress(InProgressSendStatus::Interrupted))
            .expect("an interrupted file to be sendable");
        assert!(matches!(opening, Opening::Continue(_)));
        assert_eq!(opening.offset(), 40);
        assert_eq!(opening.request(), OpeningRequest::Continue);
    }

    #[test]
    fn no_other_status_can_be_sent() {
        for status in [
            in_progress(InProgressSendStatus::Prepared),
            in_progress(InProgressSendStatus::Sending),
            in_progress(InProgressSendStatus::PendingSent),
            SendStatus::Sent,
            SendStatus::Rejected,
            SendStatus::Failed,
            SendStatus::Unknown("from a newer version".into()),
        ] {
            assert!(opening(&status).is_err(), "{status:?}");
        }
    }
}
