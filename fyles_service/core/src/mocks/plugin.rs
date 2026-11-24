use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::core::brain::plugin::BrainPlugin;
use crate::core::brain::types::FilerequestResult;
use crate::core::p2p::NodeConfig;
use crate::core::settings::{SettingsMutation, SettingsView};
use crate::io_controller::HostState;

#[derive(Clone, Default)]
pub struct MockBrainPlugin {
    started_with: Arc<Mutex<Vec<HostState>>>,
    host_states: Arc<Mutex<Vec<HostState>>>,
    mutations: Arc<Mutex<Vec<Vec<u8>>>>,
    view: Arc<Mutex<Vec<u8>>>,
}

impl MockBrainPlugin {
    pub fn set_view(&self, view: Vec<u8>) {
        *self.view.lock().unwrap() = view;
    }

    /// More than one entry is a bug: `start` runs once.
    pub fn started_with(&self) -> Vec<HostState> {
        self.started_with.lock().unwrap().clone()
    }

    pub fn host_states(&self) -> Vec<HostState> {
        self.host_states.lock().unwrap().clone()
    }

    pub fn mutations(&self) -> Vec<Vec<u8>> {
        self.mutations.lock().unwrap().clone()
    }
}

#[async_trait]
impl BrainPlugin for MockBrainPlugin {
    async fn start(&mut self, host_state: HostState) {
        self.started_with.lock().unwrap().push(host_state);
    }

    async fn handle_host_state_changed(&mut self, state: HostState) {
        self.host_states.lock().unwrap().push(state);
    }

    async fn node_config(&self) -> NodeConfig {
        NodeConfig(Vec::new())
    }

    async fn handle_settings_query(&self) -> FilerequestResult<SettingsView> {
        Ok(SettingsView(self.view.lock().unwrap().clone()))
    }

    async fn handle_settings_mutation(
        &mut self,
        request: SettingsMutation,
    ) -> FilerequestResult<SettingsView> {
        self.mutations.lock().unwrap().push(request.0);
        Ok(SettingsView(self.view.lock().unwrap().clone()))
    }
}
