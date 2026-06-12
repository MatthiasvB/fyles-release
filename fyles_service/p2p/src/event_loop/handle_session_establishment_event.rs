
use libp2p::request_response::{self, Event};
use tracing::{error, instrument, trace, warn, Instrument};


use crate::{
    crypto::{
        AuthWireRequest, AuthWireResponse, KeyExchangeResponseMessage,
        ResponderSessionEstablishmentError, SessionInitiationRequest,
    },
    event_loop::{
        with_swarm::WithSwarm, FileTracker, LocalNetworkSwarm, RefCountEventLoopData,
    },
};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
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
                        self.session_manager.handle_session_establishment_failure(peer).await;
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
                        self.session_manager.handle_session_establishment_failure(peer).await;
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
