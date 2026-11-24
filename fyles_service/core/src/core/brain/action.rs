#[cfg(any(test, feature = "test-support"))]
use crate::core::brain::action_test::TestAction;
#[cfg(feature = "debug-server")]
use crate::core::brain::debug_server::DebugServerAction;

use super::{
    action_client::ClientAction, action_p2p::NetworkNodeAction, action_plugin::PluginAction,
};

#[allow(unused)]
#[derive(Debug)]
pub enum BrainAction {
    Client(ClientAction),
    NetworkNode(NetworkNodeAction),
    Plugin(PluginAction),
    #[cfg(any(test, feature = "test-support"))]
    Test(TestAction),
    #[cfg(feature = "debug-server")]
    DebugServer(DebugServerAction),
}
