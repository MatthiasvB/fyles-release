#![expect(
    unreachable_code,
    reason = "NetworkBehaviour derive forwards libp2p_stream's Infallible handler events"
)]

use crypto::Encrypted;
use libp2p::identity::Keypair;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{StreamProtocol, mdns, request_response};
use request_response::ProtocolSupport;
use serde::{Deserialize, Serialize};

use fyles_core::core::brain::types::{
    ContactShareChallenge, PubkeyFetchRequest, PubkeyFetchResponse, SelfContactInviteChallenge,
    SignedContactShare,
};
use fyles_core::core::domain_models::SelfContact;
use fyles_core::core::sync::{SyncRequest, SyncResponse};

#[cfg(test)]
use crate::constants::BEHAVIOUR_REQUEST_TIMEOUT;
use crate::constants::{FILE_PUSH_V2_PROTOCOL, SESSION_ESTABLISHMENT_PROTOCOL};
use crate::sessions::ResponderSessionEstablishmentError;
use crate::types::{FileRequest, FileRequestV2, FileResponse, FileResponseV2};
use crypto::session::{
    AuthWireRequest, AuthWireResponse, KeyExchangeResponseMessage, SessionInitiationRequest,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum SessionErrorResponse {
    MissingSession,
    CryptError,
}

#[derive(NetworkBehaviour)]
pub struct CoreBehaviour {
    pub filerequest: FilerequestBehaviour,
    pub filerequest_v2: FilerequestV2Behaviour,
    pub file_stream: libp2p_stream::Behaviour,
    pub self_contact_invite: SelfContactInviteBehaviour,
    pub contact_share: ContactShareBehaviour,
    pub session_establishment: SessionEstablishmentBehaviour,
    pub sync: SyncBehaviour,
    pub pubkey_fetch: PubkeyFetchBehaviour,
}

pub type FilerequestBehaviour = request_response::cbor::Behaviour<
    Encrypted<FileRequest>,
    Result<Encrypted<FileResponse>, SessionErrorResponse>,
>;

pub type FilerequestV2Behaviour = request_response::cbor::Behaviour<
    Encrypted<FileRequestV2>,
    Result<Encrypted<FileResponseV2>, SessionErrorResponse>,
>;

pub type SelfContactInviteBehaviour = request_response::cbor::Behaviour<
    Encrypted<SelfContactInviteChallenge>,
    Result<Encrypted<SelfContact>, SessionErrorResponse>,
>;

pub type ContactShareBehaviour = request_response::cbor::Behaviour<
    Encrypted<ContactShareChallenge>,
    Result<Encrypted<SignedContactShare>, SessionErrorResponse>,
>;

pub type SessionEstablishmentBehaviour = request_response::cbor::Behaviour<
    AuthWireRequest<Box<SessionInitiationRequest>>,
    Result<AuthWireResponse<Box<KeyExchangeResponseMessage>>, ResponderSessionEstablishmentError>,
>;

pub type SyncBehaviour = request_response::cbor::Behaviour<
    Encrypted<SyncRequest>,
    Result<Encrypted<SyncResponse>, SessionErrorResponse>,
>;

pub type PubkeyFetchBehaviour = request_response::cbor::Behaviour<
    Encrypted<PubkeyFetchRequest>,
    Result<Encrypted<PubkeyFetchResponse>, SessionErrorResponse>,
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
            .with_request_timeout(BEHAVIOUR_REQUEST_TIMEOUT)
            .with_relay_for_requests(false);

        Self {
            filerequest: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/file-push/0.2.0"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            filerequest_v2: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new(FILE_PUSH_V2_PROTOCOL),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            file_stream: libp2p_stream::Behaviour::with_config(
                libp2p_stream::Config::default()
                    .with_relayed_connections(false)
                    .with_dialing(false),
            ),
            self_contact_invite: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/fyles/self-contact-invite/0.1.0"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            contact_share: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/fyles/contact-invite/0.1.0"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            session_establishment: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new(SESSION_ESTABLISHMENT_PROTOCOL),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            sync: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/fyles/sync/0.1.0"),
                    ProtocolSupport::Full,
                )],
                request_response_config.clone(),
            ),
            pubkey_fetch: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/fyles/pubkey-fetch/0.1.0"),
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

    /// A node from before the stream protocol: it neither offers nor answers `/file-push/0.3.0`.
    #[cfg(test)]
    pub fn chunks_only(key: &Keypair) -> Self {
        let mut behaviour = Self::new(key);
        behaviour.core.filerequest_v2 = request_response::cbor::Behaviour::new(
            std::iter::empty(),
            request_response::Config::default().with_request_timeout(BEHAVIOUR_REQUEST_TIMEOUT),
        );
        behaviour
    }
}

#[cfg(test)]
mod wire_size_tests {
    use super::*;
    use crate::CHUNK_SIZE;
    use crate::types::DataChunk;
    use crypto::ContactKeys;
    use crypto::session::{SessionInitiationRequest, SessionInitiatorSecrets};
    use fyles_core::core::domain_models::FylesId;
    use libp2p::request_response::Codec;
    use serde::de::DeserializeOwned;

    async fn request_wire_len<Req>(request: Req) -> usize
    where
        Req: Serialize + DeserializeOwned + Send + 'static,
    {
        let mut codec = request_response::cbor::codec::Codec::<Req, ()>::default();
        let mut wire = futures::io::Cursor::new(Vec::new());
        codec
            .write_request(&StreamProtocol::new("/test"), &mut wire, request)
            .await
            .expect("the request to encode");
        wire.into_inner().len()
    }

    #[tokio::test]
    async fn an_encrypted_chunk_costs_little_more_than_its_data() {
        let request = Encrypted::new_plain(&FileRequest::Chunk(DataChunk {
            transfer_uuid: FylesId::new(),
            offset: 0,
            data: vec![0xAB; CHUNK_SIZE],
        }));
        let len = request_wire_len(request).await;
        assert!(len < CHUNK_SIZE + 256, "{len} bytes on the wire");
    }

    #[tokio::test]
    async fn a_session_request_carries_its_key_and_signature_as_bytes() {
        let mut keys = ContactKeys::new();
        let id = keys.public.id();
        let request =
            SessionInitiationRequest::new(Box::new(SessionInitiatorSecrets::new().public), None)
                .sign(&mut keys.private, id)
                .expect("the request to sign");
        // An ML-KEM-1024 key and an ML-DSA-87 signature, plus a few hundred bytes of the rest.
        let len = request_wire_len(request).await;
        assert!(len < 1568 + 4627 + 512, "{len} bytes on the wire");
    }
}
