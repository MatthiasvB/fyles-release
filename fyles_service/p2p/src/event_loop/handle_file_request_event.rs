
use crypto::Encrypted;
use libp2p::request_response::{self, Event};
use tracing::{debug, trace, warn};


use crate::send_receive_traits::session_responder::SessionResponder;
use crate::send_receive_traits::session_send_receive::{
    LocalReceiveError, ReceiveError, ReceivePayload, RemoteReceiveError,
};
use crate::send_receive_traits::session_send_receive::SessionReceive;
use crate::{
    behaviour::SessionErrorResponse,
    event_loop::{
        with_swarm::WithSwarm, FileTracker, LocalNetworkSwarm, RefCountEventLoopData,
    },
    types::{FileRequest, FileResponse},
};

pub type FilerequestEvent =
    Event<Encrypted<FileRequest>, Result<Encrypted<FileResponse>, SessionErrorResponse>>;


impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    pub(super) fn handle_filerequest_event(self, event: FilerequestEvent) {
        match event {
            request_response::Event::OutboundFailure {
                peer,
                error,
                request_id,
                ..
            } => {
                debug!("Outbound request failed to peer {peer} with error: {error}");
                match &error {
                    request_response::OutboundFailure::Timeout => {
                        debug!("Outbound request timed out");
                    }
                    request_response::OutboundFailure::ConnectionClosed => {
                        debug!("Outbound request connection closed");
                    }
                    request_response::OutboundFailure::DialFailure => {
                        debug!("Dial Failure");
                    }
                    request_response::OutboundFailure::UnsupportedProtocols => {
                        debug!("Unsupported protocols");
                    }
                    request_response::OutboundFailure::Io(e) => {
                        debug!("IO error: {e}");
                    }
                }
                tokio::spawn(async move {
                    self.receive_encrypted_response(
                        peer,
                        request_id,
                        ReceivePayload::<FileResponse, _, _, _, _>::ReceiveError(
                            ReceiveError::LocalError(LocalReceiveError::TransportError(error)),
                        ),
                        &self.filerequest_request_registry,
                    )
                    .await
                });
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                warn!("Inbound request failed from peer {peer} with error: {error}");
            }
            request_response::Event::ResponseSent { peer, .. } => {
                trace!("Response sent to peer {peer}");
            }
            request_response::Event::Message { message, peer, .. } => match message {
                request_response::Message::Request {
                    request, channel, request_id, ..
                } => {
                    tokio::spawn(async move {
                        if let Some(response) = self.decrypt_respond(peer, request, request_id).await {
                            self.with_swarm(move |swarm| {
                                if swarm.get_filerequest_behaviour().send_response(
                                    channel,
                                    response.map_err(|e| {
                                        warn!(error=?e, "Error in filerequest behaviour");
                                        SessionErrorResponse::CryptError
                                    }),
                                ).is_err() {
                                    warn!(?peer, "Unable to send response to peer");
                                };
                            })
                        } else {
                            trace!("Not sending a response");
                        }
                    });
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    tokio::spawn(async move {
                        let payload = match response {
                            Ok(response) => ReceivePayload::Response(response),
                            Err(e) => match e {
                                SessionErrorResponse::MissingSession => {
                                    ReceivePayload::ReceiveError(ReceiveError::RemoteError(
                                        RemoteReceiveError::MissingSession,
                                    ))
                                }
                                SessionErrorResponse::CryptError => ReceivePayload::ReceiveError(
                                    ReceiveError::RemoteError(RemoteReceiveError::CannotDecrypt),
                                ),
                            },
                        };
                        self.receive_encrypted_response(
                            peer,
                            request_id,
                            payload,
                            &self.filerequest_request_registry,
                        )
                        .await
                    });
                }
            },
        }
    }
}
