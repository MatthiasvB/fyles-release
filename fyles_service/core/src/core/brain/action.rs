#[cfg(any(test, feature = "test-support"))]
use crate::core::brain::action_test::TestAction;

use super::{action_client::ClientAction, action_p2p::NetworkNodeAction};

#[allow(unused)]
#[derive(Debug)]
pub enum BrainAction {
    Client(ClientAction),
    NetworkNode(NetworkNodeAction),
    #[cfg(any(test, feature = "test-support"))]
    Test(TestAction),
}
