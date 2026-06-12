use crate::crypto::Session;
use crate::event_loop::filerequest::file_tracker::FileTracker;
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::send_receive_traits::request_registry::{
    RemovedRequest, RequestForResend, RequestForResendAfterSessionError, RequestRegistry,
};
use crate::types::FileRequest;
use crate::utils::W;
use async_trait::async_trait;
use fyles_core::core::brain::types::{ContactShareChallenge, SelfContactInviteChallenge};
use fyles_core::core::domain_models::ContactId;
use libp2p::request_response::OutboundRequestId;
use libp2p::PeerId;
use std::sync::Arc;

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestRegistry<FileRequest>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type Session = W<Arc<Session>>;
    type ContactId = ContactId;
    type ExternalRequestId = usize;
    type NodeId = PeerId;

    async fn register_request_id(
        &self,
        request_id: Self::RequestId,
        node_id: Self::NodeId,
    ) -> Self::ExternalRequestId {
        self.filerequest_request_registry
            .register_request_id(request_id, node_id)
            .await
    }

    async fn register_request(
        &self,
        session: Self::Session,
        id: Self::RequestId,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: FileRequest,
        idempotent: bool,
    ) -> Self::ExternalRequestId {
        self.filerequest_request_registry
            .register_request(session, id, node_id, contact_id, request, idempotent)
            .await
    }

    async fn remove_request(
        &self,
        id: Self::RequestId,
    ) -> Option<RemovedRequest<Self::ExternalRequestId, FileRequest>> {
        self.filerequest_request_registry.remove_request(id).await
    }

    async fn get_request_for_resend(
        &self,
        id: Self::RequestId,
    ) -> Option<RequestForResend<FileRequest, Self::ContactId>> {
        self.filerequest_request_registry
            .get_request_for_resend(id)
            .await
    }

    async fn get_request_for_resend_after_session_error(
        &self,
        current_session: Option<Self::Session>,
        id: Self::RequestId,
    ) -> Option<RequestForResendAfterSessionError<FileRequest, Self::ContactId>> {
        self.filerequest_request_registry
            .get_request_for_resend_after_session_error(current_session, id)
            .await
    }

    async fn check_request_metadata(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<(usize, Self::ExternalRequestId)> {
        self.filerequest_request_registry
            .check_request_metadata(request_id)
            .await
    }

    async fn update_request_id(
        &self,
        old_request_id: &Self::RequestId,
        new_request_id: Self::RequestId,
    ) -> Result<Self::ExternalRequestId, ()> {
        self.filerequest_request_registry
            .update_request_id(old_request_id, new_request_id)
            .await
    }

    async fn get_external_request_id(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<Self::ExternalRequestId> {
        self.filerequest_request_registry
            .get_external_request_id(request_id)
            .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestRegistry<ContactShareChallenge>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type Session = W<Arc<Session>>;
    type ContactId = ContactId;
    type ExternalRequestId = usize;
    type NodeId = PeerId;

    async fn register_request_id(
        &self,
        request_id: Self::RequestId,
        node_id: Self::NodeId,
    ) -> Self::ExternalRequestId {
        self.filerequest_request_registry
            .register_request_id(request_id, node_id)
            .await
    }

    async fn register_request(
        &self,
        session: Self::Session,
        id: Self::RequestId,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: ContactShareChallenge,
        idempotent: bool,
    ) -> Self::ExternalRequestId {
        self.contact_invite_challenge_request_registry
            .register_request(session, id, node_id, contact_id, request, idempotent)
            .await
    }

    async fn remove_request(
        &self,
        id: Self::RequestId,
    ) -> Option<RemovedRequest<Self::ExternalRequestId, ContactShareChallenge>> {
        self.contact_invite_challenge_request_registry
            .remove_request(id)
            .await
    }

    async fn get_request_for_resend(
        &self,
        id: Self::RequestId,
    ) -> Option<RequestForResend<ContactShareChallenge, Self::ContactId>> {
        self.contact_invite_challenge_request_registry
            .get_request_for_resend(id)
            .await
    }

    async fn get_request_for_resend_after_session_error(
        &self,
        current_session: Option<Self::Session>,
        id: Self::RequestId,
    ) -> Option<RequestForResendAfterSessionError<ContactShareChallenge, Self::ContactId>> {
        self.contact_invite_challenge_request_registry
            .get_request_for_resend_after_session_error(current_session, id)
            .await
    }

    async fn check_request_metadata(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<(usize, Self::ExternalRequestId)> {
        self.contact_invite_challenge_request_registry
            .check_request_metadata(request_id)
            .await
    }

    async fn update_request_id(
        &self,
        old_request_id: &Self::RequestId,
        new_request_id: Self::RequestId,
    ) -> Result<Self::ExternalRequestId, ()> {
        self.contact_invite_challenge_request_registry
            .update_request_id(old_request_id, new_request_id)
            .await
    }

    async fn get_external_request_id(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<Self::ExternalRequestId> {
        self.contact_invite_challenge_request_registry
            .get_external_request_id(request_id)
            .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestRegistry<SelfContactInviteChallenge>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type Session = W<Arc<Session>>;
    type ContactId = ContactId;
    type NodeId = PeerId;
    type ExternalRequestId = usize;

    async fn register_request_id(
        &self,
        request_id: Self::RequestId,
        node_id: Self::NodeId,
    ) -> Self::ExternalRequestId {
        self.self_contact_invite_challenge_request_registry
            .register_request_id(request_id, node_id)
            .await
    }

    async fn register_request(
        &self,
        session: Self::Session,
        id: Self::RequestId,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: SelfContactInviteChallenge,
        idempotent: bool,
    ) -> Self::ExternalRequestId {
        self.self_contact_invite_challenge_request_registry
            .register_request(session, id, node_id, contact_id, request, idempotent)
            .await
    }

    async fn remove_request(
        &self,
        id: Self::RequestId,
    ) -> Option<RemovedRequest<Self::ExternalRequestId, SelfContactInviteChallenge>> {
        self.self_contact_invite_challenge_request_registry
            .remove_request(id)
            .await
    }

    async fn get_request_for_resend(
        &self,
        id: Self::RequestId,
    ) -> Option<RequestForResend<SelfContactInviteChallenge, Self::ContactId>> {
        self.self_contact_invite_challenge_request_registry
            .get_request_for_resend(id)
            .await
    }

    async fn get_request_for_resend_after_session_error(
        &self,
        current_session: Option<Self::Session>,
        id: Self::RequestId,
    ) -> Option<RequestForResendAfterSessionError<SelfContactInviteChallenge, Self::ContactId>>
    {
        self.self_contact_invite_challenge_request_registry
            .get_request_for_resend_after_session_error(current_session, id)
            .await
    }

    async fn check_request_metadata(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<(usize, Self::ExternalRequestId)> {
        self.self_contact_invite_challenge_request_registry
            .check_request_metadata(request_id)
            .await
    }

    async fn update_request_id(
        &self,
        old_request_id: &Self::RequestId,
        new_request_id: Self::RequestId,
    ) -> Result<Self::ExternalRequestId, ()> {
        self.self_contact_invite_challenge_request_registry
            .update_request_id(old_request_id, new_request_id)
            .await
    }

    async fn get_external_request_id(
        &self,
        request_id: &Self::RequestId,
    ) -> Option<Self::ExternalRequestId> {
        self.self_contact_invite_challenge_request_registry
            .get_external_request_id(request_id)
            .await
    }
}
