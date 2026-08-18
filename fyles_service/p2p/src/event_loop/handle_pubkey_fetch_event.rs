use crypto::Encrypted;
use fyles_core::core::brain::types::{PubkeyFetchRequest, PubkeyFetchResponse};
use libp2p::request_response::{self, Event};
use tracing::{Instrument, debug, error, instrument, trace, warn};

use crate::{
    behaviour::SessionErrorResponse,
    event_loop::{FileTracker, LocalNetworkSwarm, RefCountEventLoopData, with_swarm::WithSwarm},
    send_receive_traits::{
        session_responder::SessionResponder,
        session_send_receive::{ReceiveError, ReceivePayload, RemoteReceiveError, SessionReceive},
    },
};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    #[instrument(skip_all)]
    pub fn handle_pubkey_fetch_event(
        self,
        event: Event<
            Encrypted<PubkeyFetchRequest>,
            Result<Encrypted<PubkeyFetchResponse>, SessionErrorResponse>,
        >,
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
                            debug!("Received pubkey fetch request from {peer}");
                            tokio::spawn(async move {
                                if let Some(response) =
                                    self.decrypt_respond(peer, request, request_id).await
                                {
                                    self.with_swarm(move |swarm| {
                                        if swarm
                                            .get_pubkey_fetch_behaviour()
                                            .send_response(
                                                channel,
                                                response.map_err(|e| {
                                                    warn!(error=?e, "Error in pubkey fetch behaviour");
                                                    SessionErrorResponse::from(e)
                                                }),
                                            )
                                            .is_err()
                                        {
                                            warn!(?peer, "Unable to send pubkey fetch response to peer");
                                        };
                                    })
                                } else {
                                    trace!("Not sending a pubkey fetch response");
                                }
                            }.in_current_span());
                        }
                        request_response::Message::Response {
                            response,
                            request_id,
                        } => {
                            trace!("Received pubkey fetch response from {peer}");
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
                                &self.pubkey_fetch_request_registry,
                            )
                            .await;
                        }
                    },
                    Event::OutboundFailure {
                        peer, request_id, error, ..
                    } => {
                        // Stateless and non-fatal: the still-keyless contact is re-fetched on
                        // the next Contacts sync convergence (fetch_missing_contact_keys).
                        warn!("Pubkey fetch outbound failure: {error:?} for {peer} request {request_id}");
                    }
                    Event::InboundFailure {
                        peer, request_id, error, ..
                    } => {
                        error!("Pubkey fetch inbound failure: {error:?} for {peer} request {request_id}");
                    }
                    Event::ResponseSent { peer, .. } => {
                        trace!("Pubkey fetch response sent to {peer:?}");
                    }
                }
            }
            .in_current_span(),
        );
    }
}
