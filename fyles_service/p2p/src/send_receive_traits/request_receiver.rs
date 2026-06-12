use async_trait::async_trait;

#[async_trait]
pub trait RequestReceiver<Req, Res> {
    type NodeId;
    type ContactId;
    type InboundRequestId;
    /// This function returns an Option to indicate whether a response should be sent.
    /// In some scenarios, we may silently not respond, for example if we suspect the request
    /// was sent by an attacker
    async fn respond_to(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Req,
        request_id: Self::InboundRequestId,
    ) -> Option<Res>;
}
