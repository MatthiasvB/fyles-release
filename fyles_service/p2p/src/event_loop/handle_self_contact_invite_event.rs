use crypto::Encrypted;
use libp2p::request_response::{self, Event};
use tracing::{debug, error, instrument, trace, warn, Instrument};

use fyles_core::core::{
        brain::{
            action::BrainAction,
            action_p2p::NetworkNodeAction,
            types::SelfContactInviteChallenge,
        },
        domain_models::SelfContact,
    };

use crate::{
    behaviour::SessionErrorResponse,
    event_loop::{
        with_swarm::WithSwarm, FileTracker, LocalNetworkSwarm, RefCountEventLoopData,
    },
    send_receive_traits::{
        session_responder::SessionResponder,
        session_send_receive::{
            ReceiveError, ReceivePayload, RemoteReceiveError, SessionReceive,
        },
    },
};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    #[instrument(skip_all)]
    pub fn handle_self_contact_invite_event(
        self,
        event: Event<
            Encrypted<SelfContactInviteChallenge>,
            Result<Encrypted<SelfContact>, SessionErrorResponse>,
        >,
    ) {
        tokio::spawn(async move {
            match event {
                Event::Message {
                    peer,
                    message,
                    ..
                } => {
                    match message {
                        request_response::Message::Request {
                            request,
                            channel,
                            request_id,
                            ..
                        } => {
                            debug!("Received self contact invite request from {peer}");
                            tokio::spawn(async move {
                                if let Some(response) = self.decrypt_respond(peer, request, request_id).await {
                                    self.with_swarm(move |swarm| {
                                        if swarm.get_self_contact_invite_behaviour().send_response(
                                            channel,
                                            response.map_err(|e| {
                                                warn!(error=?e, "Error in self contact invite behaviour");
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
                            response,
                            request_id,
                        } => {
                            trace!("Received self contact invite response from {peer}");
                            let payload = match response {
                                        Ok(response) => ReceivePayload::Response(response),
                                        Err(e) => match e {
                                            SessionErrorResponse::MissingSession => {
                                                ReceivePayload::ReceiveError(ReceiveError::RemoteError(
                                                    RemoteReceiveError::MissingSession,
                                                ))
                                            }
                                            SessionErrorResponse::CryptError => {
                                                ReceivePayload::ReceiveError(ReceiveError::RemoteError(
                                                    RemoteReceiveError::CannotDecrypt,
                                                ))
                                            }
                                        },
                                    };
                            self.receive_encrypted_response(peer, request_id, payload, &self.self_contact_invite_challenge_request_registry).await;
                        }
                    }
                }
                Event::OutboundFailure {
                    peer,
                    connection_id,
                    request_id,
                    error,
                } => {
                    error!(
                        "Outbound failure: {error:?} for peer {peer} with connection {connection_id} and request {request_id}"
                    );
                    error!("Sending got rejected action");
                    // Not the exact right action, but good enough for now
                    self.brain_action_sender.send(BrainAction::NetworkNode(NetworkNodeAction::SelfContactInviteGotRejected)).await.expect("Sending to work");
                    error!("Sent got rejected action");
                }
                Event::InboundFailure {
                    peer,
                    connection_id,
                    request_id,
                    error,
                } => {
                    error!(
                        "Inbound failure: {error:?} for peer {peer} with connection {connection_id} and request {request_id}"
                    );
                    // Not the exact right action, but good enough for now
                    self.brain_action_sender.send(BrainAction::NetworkNode(NetworkNodeAction::SelfContactInviteGotRejected)).await.expect("Sending to work");
                }
                Event::ResponseSent {
                    peer,
                    ..
                } => {
                    trace!("Response sent to {peer:?}");
                }
            }
        }.in_current_span());
    }
}
