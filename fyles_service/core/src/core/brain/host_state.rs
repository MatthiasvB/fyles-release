use tokio::sync::watch;

use crate::core::brain::GigaBrain;
use crate::io_controller::HostState;

impl GigaBrain {
    /// Changes only. `Brain::run` gave the starting reading to `BrainPlugin::start`, and
    /// subscribed this receiver beforehand so a change in between is still delivered.
    pub(super) async fn watch_host_state(&self, mut host_state: watch::Receiver<HostState>) {
        while host_state.changed().await.is_ok() {
            let state = *host_state.borrow_and_update();
            self.plugin
                .write()
                .await
                .handle_host_state_changed(state)
                .await;
        }
    }
}
