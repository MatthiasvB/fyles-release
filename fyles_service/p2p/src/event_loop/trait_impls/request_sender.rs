use crate::event_loop::filerequest::file_tracker::FileTracker;
use crate::event_loop::with_swarm::WithSwarm;
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::send_receive_traits::request_sender::RequestSender;
use crate::types::FileRequest;
use async_trait::async_trait;
use crypto::Encrypted;
use fyles_core::core::brain::types::{ContactShareChallenge, SelfContactInviteChallenge};
use libp2p::request_response::OutboundRequestId;
use libp2p::PeerId;

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> RequestSender<Encrypted<FileRequest>>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type NodeId = PeerId;

    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: Encrypted<FileRequest>,
    ) -> Self::RequestId {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_filerequest_behaviour()
                .send_request(&node, request)
        })
        .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> RequestSender<Encrypted<ContactShareChallenge>>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type NodeId = PeerId;

    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: Encrypted<ContactShareChallenge>,
    ) -> Self::RequestId {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_contact_share_behaviour()
                .send_request(&node, request)
        })
        .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static>
    RequestSender<Encrypted<SelfContactInviteChallenge>> for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type NodeId = PeerId;

    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: Encrypted<SelfContactInviteChallenge>,
    ) -> Self::RequestId {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_self_contact_invite_behaviour()
                .send_request(&node, request)
        })
        .await
    }
}
