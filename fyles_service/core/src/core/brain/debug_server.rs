use crate::core::brain::{GigaBrain, action::BrainAction, types::BrainRequest};
use axum::{Router, extract::State, routing::get};
use erased_serde::Serialize;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub enum DebugServerAction {
    GetDebugState(BrainRequest<(), Box<dyn Serialize + Send>>),
}

pub(crate) async fn run_debug_server(sender: Sender<BrainAction>) {
    let app = Router::new().route("/", get(handle_get_debug_state).with_state(sender.clone()));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:6754").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handle_get_debug_state(State(sender): State<Sender<BrainAction>>) -> String {
    let (request, response) = BrainRequest::with_receiver(());
    sender
        .send(BrainAction::DebugServer(DebugServerAction::GetDebugState(
            request,
        )))
        .await
        .expect("Sending to work");
    let response = &*response.await.expect("Sender not to be dropped");
    serde_json::to_string(response).expect("Serialization to work")
}

impl GigaBrain {
    pub(crate) async fn handle_debug_server_action(&self, action: DebugServerAction) {
        match action {
            DebugServerAction::GetDebugState(brain_request) => {
                let network = self.network.read().await;
                let p2p_blob = network.get_debug_blob().await;
                if let Some(sender) = brain_request.response_sender.lock().await.take()
                    && sender.send(p2p_blob).is_err()
                {
                    panic!("Expect sending to work");
                }
            }
        }
    }
}
