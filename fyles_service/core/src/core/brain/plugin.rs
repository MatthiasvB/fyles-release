use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::types::FilerequestResult;
use crate::core::p2p::NodeConfig;
use crate::core::settings::{SettingsMutation, SettingsView};
use crate::io_controller::HostState;

pub type BrainPluginRef = Arc<RwLock<dyn BrainPlugin>>;

/// Behaviour the brain cannot carry itself, because it turns on a schema `core` must stay
/// ignorant of. See ADR-0008.
#[async_trait]
pub trait BrainPlugin: Send + Sync + 'static {
    /// Runs after the action loop is live and before the network node starts, so a plugin may use
    /// its sender here and [`Self::node_config`] is already right when the node first asks.
    async fn start(&mut self, host_state: HostState);

    async fn handle_host_state_changed(&mut self, state: HostState);

    async fn node_config(&self) -> NodeConfig;

    async fn handle_settings_query(&self) -> FilerequestResult<SettingsView>;

    async fn handle_settings_mutation(
        &mut self,
        request: SettingsMutation,
    ) -> FilerequestResult<SettingsView>;
}

/// Written out rather than defaulted on the trait, so a new hook is a compile error here rather
/// than a silent no-op.
pub struct NoOpBrainPlugin;

#[async_trait]
impl BrainPlugin for NoOpBrainPlugin {
    async fn start(&mut self, _host_state: HostState) {}

    async fn handle_host_state_changed(&mut self, _state: HostState) {}

    async fn node_config(&self) -> NodeConfig {
        NodeConfig(Vec::new())
    }

    /// The frontend renders empty as "this build has no settings to configure".
    async fn handle_settings_query(&self) -> FilerequestResult<SettingsView> {
        Ok(SettingsView(Vec::new()))
    }

    async fn handle_settings_mutation(
        &mut self,
        _request: SettingsMutation,
    ) -> FilerequestResult<SettingsView> {
        Ok(SettingsView(Vec::new()))
    }
}
