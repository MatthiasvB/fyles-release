use crate::event_loop::with_swarm::SwarmWorkDiscarded;
use async_trait::async_trait;

#[async_trait]
pub trait RequestSender<R> {
    type RequestId;
    type NodeId;
    async fn send_encrypted_request(
        &self,
        node: Self::NodeId,
        request: R,
    ) -> Result<Self::RequestId, SwarmWorkDiscarded>;
}
