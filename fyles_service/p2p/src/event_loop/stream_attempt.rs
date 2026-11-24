use std::sync::Arc;

use crypto::SerCryptError;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::domain_models::{ContactId, InProgress, InProgressSendStatus, SendStatus};
use libp2p::{PeerId, StreamProtocol};
use tracing::debug;

use crate::constants::FILE_DATA_PROTOCOL;
use crate::event_loop::filerequest::file_tracker::{AttemptStart, FileTracker, WireRequest};
use crate::event_loop::with_swarm::WithSwarm;
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::file_stream::send::{AttemptOutcome, run_send_attempt};
use crate::send_receive_traits::session_send_receive::{SessionSend, SessionSendError};
use crate::sessions::SessionEstablishmentError;

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    pub async fn send_wire_request(
        &self,
        peer: PeerId,
        contact_id: Option<ContactId>,
        request: WireRequest,
        idempotent: bool,
    ) -> Result<usize, SessionSendError<SerCryptError, Arc<SessionEstablishmentError>>> {
        match (request, idempotent) {
            (WireRequest::Chunks(request), false) => {
                self.send_request(peer, contact_id, request).await
            }
            (WireRequest::Chunks(request), true) => {
                self.send_request_idempotent(peer, contact_id, request)
                    .await
            }
            (WireRequest::Streams(request), false) => {
                self.send_request(peer, contact_id, request).await
            }
            (WireRequest::Streams(request), true) => {
                self.send_request_idempotent(peer, contact_id, request)
                    .await
            }
        }
    }

    pub async fn run_stream_attempt(&self, peer: PeerId, start: AttemptStart) {
        let AttemptStart {
            grant,
            source,
            offset,
            progress,
            cancel,
            pending_file_id,
            transfer_data,
        } = start;
        let opened = match self
            .with_swarm_res(|swarm| swarm.file_stream_control())
            .await
        {
            Err(_) => Err("the swarm went away"),
            Ok(mut control) => tokio::select! {
                _ = cancel.cancelled() => return,
                opened = control.open_stream(peer, StreamProtocol::new(FILE_DATA_PROTOCOL)) => {
                    opened.map_err(|error| {
                        debug!(?peer, ?error, "Could not open a data stream");
                        "could not open a data stream"
                    })
                }
            },
        };
        let outcome = match opened {
            Err(reason) => Some(AttemptOutcome::Interrupted(reason)),
            Ok(stream) => {
                let brain = &self.brain_action_sender;
                let report = |progress_bytes| {
                    let mut transfer_data = transfer_data.clone();
                    transfer_data.progress_bytes = progress_bytes;
                    // Progress is superseded by the next report, so a full channel may drop it.
                    let _ =
                        brain.try_send(BrainAction::NetworkNode(NetworkNodeAction::FileSending {
                            pending_file_id: pending_file_id.clone(),
                            status: SendStatus::InProgress(InProgress {
                                status: InProgressSendStatus::Sending,
                                transfer_data,
                            }),
                        }));
                };
                run_send_attempt(stream, &grant, source, offset, &progress, report, &cancel).await
            }
        };
        if let Some(outcome) = outcome {
            self.file_tracker
                .on_attempt_finished(peer, grant.attempt, outcome)
                .await;
        }
    }
}
