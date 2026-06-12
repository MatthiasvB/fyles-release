use crate::crypto::{Session, SessionEstablishmentError};
use crate::event_loop::filerequest::file_tracker::FileTracker;
use crate::event_loop::{LocalNetworkSwarm, RefCountEventLoopData};
use crate::send_receive_traits::get_session::GetSession;
use crate::utils::W;
use async_trait::async_trait;
use fyles_core::core::domain_models::ContactId;
use libp2p::PeerId;
use std::sync::Arc;

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> GetSession for RefCountEventLoopData<T, S> {
    type Session = W<Arc<Session>>;
    type NodeId = PeerId;
    type ContactId = ContactId;
    type Error = SessionEstablishmentError;

    async fn get_or_establish_session(
        &self,
        node_id: Self::NodeId,
        for_contact_id: Option<Self::ContactId>,
    ) -> Result<Self::Session, Arc<Self::Error>> {
        self.session_manager
            .get_or_establish_session(node_id, for_contact_id)
            .await
    }

    async fn get_established_session(
        &self,
        node_id: Self::NodeId,
        use_for_send: bool,
    ) -> Option<Self::Session> {
        self.session_manager
            .get_established_session(node_id, use_for_send)
            .await
    }

    async fn get_recent_receive_sessions(&self, node_id: &Self::NodeId) -> Vec<Self::Session> {
        self.session_manager
            .get_recent_receive_sessions(node_id)
            .await
    }

    async fn clear_session(&self, node_id: Self::NodeId) {
        self.session_manager.clear_session(node_id).await
    }
}
