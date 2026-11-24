use crate::event_loop::filerequest::file_tracker::FileTracker;
use crate::event_loop::with_swarm::{SwarmWorkDiscarded, WithSwarm};
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::send_receive_traits::request_sender::RequestSender;
use crate::types::{FileRequest, FileRequestV2};
use async_trait::async_trait;
use crypto::Encrypted;
use fyles_core::core::brain::types::{
    ContactShareChallenge, PubkeyFetchRequest, SelfContactInviteChallenge,
};
use fyles_core::core::sync::SyncRequest;
use libp2p::PeerId;
use libp2p::request_response::OutboundRequestId;

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
    ) -> Result<Self::RequestId, SwarmWorkDiscarded> {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_filerequest_behaviour()
                .send_request(&node, request)
        })
        .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> RequestSender<Encrypted<FileRequestV2>>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type NodeId = PeerId;

    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: Encrypted<FileRequestV2>,
    ) -> Result<Self::RequestId, SwarmWorkDiscarded> {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_filerequest_v2_behaviour()
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
    ) -> Result<Self::RequestId, SwarmWorkDiscarded> {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_contact_share_behaviour()
                .send_request(&node, request)
        })
        .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> RequestSender<Encrypted<SyncRequest>>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type NodeId = PeerId;

    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: Encrypted<SyncRequest>,
    ) -> Result<Self::RequestId, SwarmWorkDiscarded> {
        self.with_swarm_res(move |swarm| swarm.get_sync_behaviour().send_request(&node, request))
            .await
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> RequestSender<Encrypted<PubkeyFetchRequest>>
    for RefCountEventLoopData<T, S>
{
    type RequestId = OutboundRequestId;
    type NodeId = PeerId;

    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: Encrypted<PubkeyFetchRequest>,
    ) -> Result<Self::RequestId, SwarmWorkDiscarded> {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_pubkey_fetch_behaviour()
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
    ) -> Result<Self::RequestId, SwarmWorkDiscarded> {
        self.with_swarm_res(move |swarm| {
            swarm
                .get_self_contact_invite_behaviour()
                .send_request(&node, request)
        })
        .await
    }
}
