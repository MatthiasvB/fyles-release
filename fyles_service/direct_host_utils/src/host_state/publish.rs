use fyles_core::io_controller::{HostSignal, HostState, NetworkStatus, PowerSource};
use tokio::sync::watch;
use tracing::debug;

/// The two watchers own a field each, so neither can clobber the other's.
pub(super) fn power(sender: &watch::Sender<HostState>, power: HostSignal<PowerSource>) {
    if sender.send_if_modified(|state| {
        let changed = state.power != power;
        state.power = power;
        changed
    }) {
        debug!("Power reading: {power:?}");
    }
}

pub(super) fn network(sender: &watch::Sender<HostState>, network: HostSignal<NetworkStatus>) {
    if sender.send_if_modified(|state| {
        let changed = state.network != network;
        state.network = network;
        changed
    }) {
        debug!("Network reading: {network:?}");
    }
}
