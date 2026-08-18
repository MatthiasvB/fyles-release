use crate::crypto::SessionEstablishmentError;
use crate::event_loop::filerequest::file_tracker::FileTracker;
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::send_receive_traits::response_receiver::ResponseReceiver;
use crate::send_receive_traits::session_send_receive::ReceiveError;
use crate::types::FileResponse;
use crate::types::Wrap;
use async_trait::async_trait;
use crypto::{DeSerCryptError, SerCryptError};
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::PubkeyFetchResponse;
use fyles_core::core::domain_models::{Contact, ContactId, SelfContact};
use fyles_core::core::sync::SyncResponse;
use libp2p::PeerId;
use libp2p::request_response::OutboundFailure;
use std::sync::Arc;
use tracing::{trace, warn};

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> ResponseReceiver<FileResponse>
    for RefCountEventLoopData<T, S>
{
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<ContactId>,
        request_id: Result<Self::RequestId, ()>,
        response: FileResponse,
    ) {
        trace!(peer=?node_id, ?response, "Received response");
        self.file_tracker
            .receive_response(node_id, contact_id, request_id, response)
            .await
    }

    async fn handle_receive_error(
        &self,
        node_id: Self::NodeId,
        request_id: Result<Self::RequestId, ()>,
        error: ReceiveError<
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
    ) {
        self.file_tracker
            .handle_receive_error(node_id, request_id, error)
            .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> ResponseReceiver<SelfContact>
    for RefCountEventLoopData<T, S>
{
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        node_id: Self::NodeId,
        _contact_id: Option<ContactId>,
        _request_id: Result<Self::RequestId, ()>,
        response: SelfContact,
    ) {
        self.brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::UpdateIdentity(
                response,
                node_id.wrap(),
            )))
            .await
            .expect("Sending to work");
    }

    async fn handle_receive_error(
        &self,
        _node_id: Self::NodeId,
        _request_id: Result<Self::RequestId, ()>,
        _error: ReceiveError<
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
    ) {
        // Technically, "rejected" is only the case on timeout, but this is convenience only and exact
        // error handling is not that important
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::SelfContactInviteGotRejected,
            ))
            .await
            .expect("Sending to work");
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> ResponseReceiver<SyncResponse>
    for RefCountEventLoopData<T, S>
{
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        node_id: Self::NodeId,
        _contact_id: Option<ContactId>,
        _request_id: Result<Self::RequestId, ()>,
        response: SyncResponse,
    ) {
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::SyncReplyReceived {
                    peer: node_id.wrap(),
                    reply: response,
                },
            ))
            .await
            .expect("Sending to work");
    }

    async fn handle_receive_error(
        &self,
        node_id: Self::NodeId,
        _request_id: Result<Self::RequestId, ()>,
        error: ReceiveError<
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
    ) {
        warn!(peer=?node_id, ?error, "Error receiving sync response");
        // The exchange failed; drop any in-flight sync state so a later attempt starts fresh.
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::SyncTransportFailed {
                    peer: node_id.wrap(),
                },
            ))
            .await
            .expect("Sending to work");
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> ResponseReceiver<PubkeyFetchResponse>
    for RefCountEventLoopData<T, S>
{
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        _node_id: Self::NodeId,
        _contact_id: Option<ContactId>,
        _request_id: Result<Self::RequestId, ()>,
        response: PubkeyFetchResponse,
    ) {
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::StoreFetchedContactKeys(response),
            ))
            .await
            .expect("Sending to work");
    }

    async fn handle_receive_error(
        &self,
        node_id: Self::NodeId,
        _request_id: Result<Self::RequestId, ()>,
        error: ReceiveError<
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
    ) {
        // Non-fatal: the contact stays keyless and is re-fetched on the next Contacts sync
        // convergence (fetch_missing_contact_keys re-checks the cache each time).
        warn!(peer=?node_id, ?error, "Error receiving pubkey fetch response");
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> ResponseReceiver<Contact>
    for RefCountEventLoopData<T, S>
{
    type DecryptError = DeSerCryptError;
    type TransportError = OutboundFailure;
    type EncryptError = SerCryptError;
    type GetSessionError = Arc<SessionEstablishmentError>;
    type NodeId = PeerId;
    type RequestId = usize;

    async fn receive_response(
        &self,
        node_id: Self::NodeId,
        _contact_id: Option<ContactId>,
        _request_id: Result<Self::RequestId, ()>,
        response: Contact,
    ) {
        trace!("Received contact share response from {node_id:?}");
        self.brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::CreateContact(
                response,
            )))
            .await
            .expect("Sending to work");
    }

    async fn handle_receive_error(
        &self,
        node_id: Self::NodeId,
        _request_id: Result<Self::RequestId, ()>,
        error: ReceiveError<
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
    ) {
        warn!(peer=?node_id, ?error, "Error receiving contact share response");
    }
}
