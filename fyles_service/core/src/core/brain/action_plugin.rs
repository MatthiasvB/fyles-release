use super::types::{BrainRequest, FilerequestResult};
use crate::core::p2p::NodeConfig;
use crate::core::settings::{SettingsBlob, SettingsView};

/// A plugin holds nothing but a `Sender<BrainAction>`, so this is its entire reach.
#[derive(Debug)]
pub enum PluginAction {
    LoadSettings(BrainRequest<(), FilerequestResult<SettingsBlob>>),
    StoreSettings(BrainRequest<SettingsBlob, FilerequestResult<()>>),
    RestartNetwork(NodeConfig),
    PushSettingsView(SettingsView),
}
