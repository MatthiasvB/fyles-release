use crate::event_loop::filerequest::file_tracker::FileTracker;
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::send_receive_traits::request_receiver::RequestReceiver;
use crate::types::{FileRequest, FileResponse};
use async_trait::async_trait;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::{
    NetworkNodeAction, PubkeyFetchReceiveRequest, SyncReceiveRequest,
};
use fyles_core::core::brain::types::{
    BrainRequest, ContactShareChallenge, FetchedContactKeys, PubkeyFetchRequest,
    PubkeyFetchResponse, SelfContactInviteChallenge,
};
use fyles_core::core::domain_models::{Contact, ContactId, SelfContact};
use fyles_core::core::sync::{SyncRequest, SyncResponse};
use fyles_core::library::util::util::TimeoutLock;
use libp2p::PeerId;
use libp2p::request_response::InboundRequestId;
use tracing::{debug, warn};

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestReceiver<SelfContactInviteChallenge, SelfContact>
    for RefCountEventLoopData<T, S>
{
    type NodeId = PeerId;
    type ContactId = ContactId;
    type InboundRequestId = InboundRequestId;

    async fn respond_to(
        &self,
        node_id: Self::NodeId,
        _contact_id: Option<Self::ContactId>,
        request: SelfContactInviteChallenge,
        _request_id: Self::InboundRequestId,
    ) -> Option<SelfContact> {
        let (request, response) = BrainRequest::with_receiver(request);
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::ValidateSelfContactInviteChallenge(request),
            ))
            .await
            .expect("Sending to work");
        if let Some(self_contact) = response.await.expect("Receiving to work") {
            debug!("Valid self contact invite challenge from {node_id:?}. Sending self contact");
            self.brain_action_sender
                .send(BrainAction::NetworkNode(
                    NetworkNodeAction::AnsweredSelfContactInvite,
                ))
                .await
                .expect("Sending to work");
            return Some(self_contact);
        } else {
            warn!("Invalid self contact invite challenge from {node_id:?}");
            self.brain_action_sender
                .send(BrainAction::NetworkNode(
                    NetworkNodeAction::RejectedSelfContactInvite,
                ))
                .await
                .expect("Sending to work");
            // Do _not_ send a response to make brute forcing harder
            return None;
        }
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestReceiver<ContactShareChallenge, Contact>
    for RefCountEventLoopData<T, S>
{
    type NodeId = PeerId;
    type ContactId = ContactId;
    type InboundRequestId = InboundRequestId;

    async fn respond_to(
        &self,
        _node_id: Self::NodeId,
        _contact_id: Option<Self::ContactId>,
        request: ContactShareChallenge,
        _request_id: Self::InboundRequestId,
    ) -> Option<Contact> {
        let (request, response) = BrainRequest::with_receiver(request);
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::ValidateContactShareChallenge(request),
            ))
            .await
            .expect("Sending to work");

        response.await.expect("Receiving to work")
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestReceiver<SyncRequest, SyncResponse>
    for RefCountEventLoopData<T, S>
{
    type NodeId = PeerId;
    type ContactId = ContactId;
    type InboundRequestId = InboundRequestId;

    async fn respond_to(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: SyncRequest,
        _request_id: Self::InboundRequestId,
    ) -> Option<SyncResponse> {
        // Only forward when the session is authenticated (carries some contact id); the
        // brain then verifies that id is ours before merging.
        let Some(contact_id) = contact_id else {
            warn!(peer=?node_id, "Declining sync from unauthenticated peer");
            return None;
        };
        let (brain_request, response) = BrainRequest::with_receiver(SyncReceiveRequest {
            contact_id,
            peer: node_id.to_string().into(),
            wire: request,
        });
        self.brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::SyncReceive(
                brain_request,
            )))
            .await
            .expect("Sending to work");
        response.await.expect("Receiving to work")
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestReceiver<PubkeyFetchRequest, PubkeyFetchResponse>
    for RefCountEventLoopData<T, S>
{
    type NodeId = PeerId;
    type ContactId = ContactId;
    type InboundRequestId = InboundRequestId;

    async fn respond_to(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: PubkeyFetchRequest,
        _request_id: Self::InboundRequestId,
    ) -> Option<PubkeyFetchResponse> {
        // Only forward when the session is authenticated (carries some contact id); the brain
        // then verifies that id is our own before serving any keys.
        let Some(contact_id) = contact_id else {
            warn!(peer=?node_id, "Declining pubkey fetch from unauthenticated peer");
            return None;
        };
        let (brain_request, response) = BrainRequest::with_receiver(PubkeyFetchReceiveRequest {
            contact_id,
            wanted_contact_ids: request.contact_ids,
        });
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::GetContactsPublicKeys(brain_request),
            ))
            .await
            .expect("Sending to work");
        let found = response.await.expect("Receiving to work")?;
        Some(PubkeyFetchResponse {
            keys: found
                .into_iter()
                .map(|(contact_id, keys)| FetchedContactKeys { contact_id, keys })
                .collect(),
        })
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm> RequestReceiver<FileRequest, FileResponse>
    for RefCountEventLoopData<T, S>
{
    type NodeId = PeerId;
    type ContactId = ContactId;
    type InboundRequestId = InboundRequestId;

    async fn respond_to(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: FileRequest,
        request_id: InboundRequestId,
    ) -> Option<FileResponse> {
        debug!(peer=?node_id, ?request, ?request_id, "Got filerequest request");
        let response = self
            .receive_manager
            .timeout_lock()
            .await
            .respond_to(node_id, contact_id, request, request_id)
            .await;
        debug!(peer=?node_id, ?request_id, ?response, "Responding");
        Some(response)
    }
}
