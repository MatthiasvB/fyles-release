use libp2p::request_response::{self, Event};
use tracing::{Instrument, error, instrument, trace, warn};

use crypto::session::{
    AuthWireRequest, AuthWireResponse, KeyExchangeResponseMessage, SessionInitiationRequest,
};

use crate::{
    event_loop::{FileTracker, LocalNetworkSwarm, RefCountEventLoopData, with_swarm::WithSwarm},
    sessions::ResponderSessionEstablishmentError,
};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    #[allow(clippy::type_complexity)]
    #[instrument(skip_all)]
    pub fn handle_session_establishment_event(
        self,
        event: Event<
            AuthWireRequest<Box<SessionInitiationRequest>>,
            Result<
                AuthWireResponse<Box<KeyExchangeResponseMessage>>,
                ResponderSessionEstablishmentError,
            >,
        >,
    ) {
        tokio::spawn(
            async move {
                match event {
                    Event::Message { peer, message, .. } => match message {
                        request_response::Message::Request {
                            request, channel, ..
                        } => {
                            let res = self
                                .session_manager
                                .handle_session_establishment_request(peer, request)
                                .await;
                            self.inner_event_sender.with_swarm(move |swarm| {
                                let _ = swarm
                                    .get_session_establishment_behaviour()
                                    .send_response(channel, res);
                            });
                        }
                        request_response::Message::Response { response, .. } => {
                            self.session_manager
                                .handle_session_establishment_response(peer, response)
                                .await;
                        }
                    },
                    Event::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    } => {
                        error!(
                            ?error,
                            ?peer,
                            ?connection_id,
                            ?request_id,
                            "Session establishment outbound failure"
                        );
                        self.session_manager
                            .handle_session_establishment_failure(peer)
                            .await;
                    }
                    Event::InboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    } => {
                        error!(
                            ?error,
                            ?peer,
                            ?connection_id,
                            ?request_id,
                            "Session establishment inbound failure"
                        );
                        self.session_manager
                            .handle_session_establishment_failure(peer)
                            .await;
                    }
                    Event::ResponseSent { peer, .. } => {
                        trace!(?peer, "Response sent");
                    }
                }
            }
            .in_current_span(),
        );
    }
}
