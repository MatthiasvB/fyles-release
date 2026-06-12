use async_trait::async_trait;
use crypto::ContactKeys;
use std::sync::{Arc, Mutex};

use crate::{
    core::{
        brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
        domain_models::{ContactId, FylesId, PeerIdWrapper},
        p2p::{FileToSend, NetworkNode, NodeStatusInfo, P2pError, P2pResult, Runnable},
    },
    library::util::{duration_ext::DurationExt, util::generate_random_bytes},
};

#[derive(Clone)]
pub struct MockP2pNode {
    send_file_calls: Arc<Mutex<Vec<Vec<FileToSend>>>>,
    get_node_info_calls: Arc<Mutex<usize>>,

    // Configurable responses
    node_info: Arc<Mutex<NodeStatusInfo>>,
    should_fail: Arc<Mutex<bool>>,

    // Add slower responses to simulate network lag if needed
    response_delay_ms: Arc<Mutex<u64>>,
}

impl MockP2pNode {
    pub fn new(node_info: NodeStatusInfo) -> Self {
        Self {
            send_file_calls: Arc::new(Mutex::new(Vec::new())),
            get_node_info_calls: Arc::new(Mutex::new(0)),
            node_info: Arc::new(Mutex::new(node_info)),
            should_fail: Arc::new(Mutex::new(false)),
            response_delay_ms: Arc::new(Mutex::new(0)),
        }
    }

    async fn maybe_delay(&self) {
        let delay_ms = *self.response_delay_ms.lock().unwrap();
        if delay_ms > 0 {
            tokio::time::sleep(delay_ms.millis()).await;
        }
    }
}

#[async_trait]
impl Runnable for MockP2pNode {
    async fn run(&self) {
        // Mock implementation does nothing
    }
}

#[async_trait]
impl NetworkNode for MockP2pNode {
    fn display_keypair(
        &self,
        binary: &Vec<u8>,
    ) -> Result<String, Box<dyn std::error::Error + std::marker::Send + Sync>> {
        Ok(String::from_utf8_lossy(binary).into_owned())
    }

    async fn send_files(&self, files_to_send: Vec<FileToSend>) -> P2pResult<()> {
        self.maybe_delay().await;

        if *self.should_fail.lock().unwrap() {
            return Err(P2pError::NetworkError {
                msg: "Mock failure".into(),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, "Mock error")),
            });
        }

        self.send_file_calls.lock().unwrap().push(files_to_send);
        Ok(())
    }

    async fn initial_files_to_send(&self, files: Vec<FileToSend>) -> P2pResult<()> {
        self.send_files(files).await
    }

    async fn get_node_info(&self) -> P2pResult<NodeStatusInfo> {
        self.maybe_delay().await;

        if *self.should_fail.lock().unwrap() {
            return Err(P2pError::NetworkError {
                msg: "Mock failure".into(),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, "Mock error")),
            });
        }

        // First increment the call counter
        {
            let mut calls = self.get_node_info_calls.lock().unwrap();
            *calls += 1;
        }

        // Then get the node info
        let node_info = self.node_info.lock().unwrap().clone();

        // Make sure we complete all operations within a reasonable time
        Ok(node_info)
    }

    async fn cancel_file(&self, _: FylesId) -> P2pResult<bool> {
        self.maybe_delay().await;

        if *self.should_fail.lock().unwrap() {
            return Err(P2pError::NetworkError {
                msg: "Mock failure".into(),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, "Mock error")),
            });
        }

        Ok(true) // Mock always succeeds in cancelling files for now
        // TODO: Could add more sophisticated mocking if needed for testing edge cases
    }

    async fn cancel_files_for_remote_filerequest(&self, _: FylesId, _peer_id: PeerIdWrapper) -> P2pResult<bool> {
        self.maybe_delay().await;

        if *self.should_fail.lock().unwrap() {
            return Err(P2pError::NetworkError {
                msg: "Mock failure".into(),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, "Mock error")),
            });
        }

        Ok(true) // Mock always succeeds in cancelling files for now
    }

    fn generate_keypair(&self) -> Result<Vec<u8>, crate::core::p2p::KeypairGenerationError> {
        Ok(generate_random_bytes(32))
    }

    async fn update_identity(&self, _: ContactId, _: ContactKeys) {
        // Mock implementation does not need to do anything
        // In a real implementation, this would update the node's identity
        // in the underlying P2P network
    }

    async fn use_self_contact_invite(&self, _: SelfContactInviteChallenge, _: PeerIdWrapper) -> () {
        // Convenience function which the mock doesn't currently support
    }

    async fn use_contact_share_challenge(&self, _: ContactShareChallenge, _: PeerIdWrapper) -> () {
        // Convenience function which the mock doesn't currently support
    }

    async fn apply_settings(&self, _settings: &[u8]) -> P2pResult<()> {
        Ok(())
    }
}
