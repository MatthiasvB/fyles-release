
use crypto::Encrypted;
#[cfg(test)]
use fyles_core::library::util::duration_ext::DurationExt;
use libp2p::identity::Keypair;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{mdns, request_response, StreamProtocol};
use request_response::ProtocolSupport;
use serde::{Deserialize, Serialize};

use fyles_core::core::brain::types::{
    ContactShareChallenge, SelfContactInviteChallenge,
};
use fyles_core::core::domain_models::{Contact, SelfContact};

use crate::crypto::{
    AuthWireRequest, AuthWireResponse, KeyExchangeResponseMessage,
    ResponderSessionEstablishmentError, SessionInitiationRequest,
};
use crate::types::{FileRequest, FileResponse};

#[derive(Debug, Serialize, Deserialize)]
pub enum SessionErrorResponse {
    MissingSession,
    CryptError,
}

#[derive(NetworkBehaviour)]
pub struct CoreBehaviour {
    pub filerequest: FilerequestBehaviour,
    pub self_contact_invite: SelfContactInviteBehaviour,
    pub contact_share: ContactShareBehaviour,
    pub session_establishment: SessionEstablishmentBehaviour,
}

pub type FilerequestBehaviour = request_response::cbor::Behaviour<
    Encrypted<FileRequest>,
    Result<Encrypted<FileResponse>, SessionErrorResponse>,
>;

pub type SelfContactInviteBehaviour = request_response::cbor::Behaviour<
    Encrypted<SelfContactInviteChallenge>,
    Result<Encrypted<SelfContact>, SessionErrorResponse>,
>;

pub type ContactShareBehaviour = request_response::cbor::Behaviour<
    Encrypted<ContactShareChallenge>,
    Result<Encrypted<Contact>, SessionErrorResponse>,
>;

pub type SessionEstablishmentBehaviour = request_response::cbor::Behaviour<
    AuthWireRequest<Box<SessionInitiationRequest>>,
    Result<AuthWireResponse<Box<KeyExchangeResponseMessage>>, ResponderSessionEstablishmentError>,
>;

#[derive(NetworkBehaviour)]
pub struct LocalNetworkBehaviour {
    pub core: CoreBehaviour,
    pub mdns: mdns::tokio::Behaviour,
}

impl Default for CoreBehaviour {
    fn default() -> Self {
        Self::new()
    }
}

impl CoreBehaviour {
    pub fn new() -> Self {
        let request_response_config =
            request_response::Config::default().with_relay_for_requests(false);

        #[cfg(test)]
        let request_response_config = request_response_config
            .with_request_timeout(500.millis())
            .with_relay_for_requests(false);

        Self {
            filerequest: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/file-push/0.0.4"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            self_contact_invite: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/fyles/self-contact-invite/0.0.1"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            contact_share: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/fyles/contact-invite/0.0.1"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            session_establishment: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/session-establishment/0.0.2"),
                    ProtocolSupport::Full,
                )],
                request_response_config,
            ),
        }
    }
}

impl LocalNetworkBehaviour {
    pub fn new(key: &Keypair) -> Self {
        Self {
            core: CoreBehaviour::new(),
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())
                .expect("mdns build behaviour is hardly runtime dependend and should always work (per platform, at least)"),
        }
    }
}
