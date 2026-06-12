use async_trait::async_trait;

pub mod api {
    tonic::include_proto!("api");
    tonic::include_proto!("nodestatus");
}

#[async_trait]
pub trait RunnableFilerequestServer: api::file_request_service_server::FileRequestService {
    async fn run(self: Box<Self>);
}
