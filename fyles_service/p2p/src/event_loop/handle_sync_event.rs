use crypto::Encrypted;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::sync::{SyncRequest, SyncResponse};
use libp2p::request_response::{self, Event};
use tracing::{Instrument, debug, error, instrument, trace, warn};

use crate::{
    behaviour::SessionErrorResponse,
    event_loop::{FileTracker, LocalNetworkSwarm, RefCountEventLoopData, with_swarm::WithSwarm},
    send_receive_traits::{
        session_responder::SessionResponder,
        session_send_receive::{ReceiveError, ReceivePayload, RemoteReceiveError, SessionReceive},
    },
    types::Wrap,
};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    async fn drop_sync_state(&self, peer: libp2p::PeerId) {
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::SyncTransportFailed { peer: peer.wrap() },
            ))
            .await
            .expect("Sending to work");
    }

    #[instrument(skip_all)]
    pub fn handle_sync_event(
        self,
        event: Event<Encrypted<SyncRequest>, Result<Encrypted<SyncResponse>, SessionErrorResponse>>,
    ) {
        tokio::spawn(
            async move {
                match event {
                    Event::Message { peer, message, .. } => match message {
                        request_response::Message::Request {
                            request,
                            channel,
                            request_id,
                            ..
                        } => {
                            debug!("Received sync request from {peer}");
                            tokio::spawn(async move {
                                if let Some(response) =
                                    self.decrypt_respond(peer, request, request_id).await
                                {
                                    self.with_swarm(move |swarm| {
                                        if swarm
                                            .get_sync_behaviour()
                                            .send_response(
                                                channel,
                                                response.map_err(|e| {
                                                    warn!(error=?e, "Error in sync behaviour");
                                                    SessionErrorResponse::from(e)
                                                }),
                                            )
                                            .is_err()
                                        {
                                            warn!(?peer, "Unable to send sync response to peer");
                                        };
                                    })
                                } else {
                                    trace!("Not sending a sync response");
                                }
                            }.in_current_span());
                        }
                        request_response::Message::Response {
                            response,
                            request_id,
                        } => {
                            trace!("Received sync response from {peer}");
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
                            self.receive_encrypted_response(
                                peer,
                                request_id,
                                payload,
                                &self.sync_request_registry,
                            )
                            .await;
                        }
                    },
                    Event::OutboundFailure {
                        peer,
                        request_id,
                        error,
                        ..
                    } => {
                        warn!("Sync outbound failure: {error:?} for {peer} request {request_id}");
                        self.drop_sync_state(peer).await;
                    }
                    Event::InboundFailure {
                        peer,
                        request_id,
                        error,
                        ..
                    } => {
                        error!("Sync inbound failure: {error:?} for {peer} request {request_id}");
                        self.drop_sync_state(peer).await;
                    }
                    Event::ResponseSent { peer, .. } => {
                        trace!("Sync response sent to {peer:?}");
                    }
                }
            }
            .in_current_span(),
        );
    }
}
