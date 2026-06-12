use async_trait::async_trait;

#[async_trait]
pub trait RequestSender<R> {
    type RequestId;
    type NodeId;
    async fn send_encrypted_request(&self, node: Self::NodeId, request: R) -> Self::RequestId;
}
