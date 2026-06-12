use crate::send_receive_traits::session_send_receive::ReceiveError;
use async_trait::async_trait;
use fyles_core::core::domain_models::ContactId;
use std::error;

#[async_trait]
pub trait ResponseReceiver<R> {
    type DecryptError: error::Error;
    type TransportError: error::Error;
    type EncryptError: error::Error;
    type GetSessionError: error::Error;
    type NodeId;
    type RequestId;
    async fn receive_response(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<ContactId>,
        request_id: Result<Self::RequestId, ()>,
        response: R,
    );
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
    );
}

