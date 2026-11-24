//! A [`HostState`] for a host that reports its readings rather than being asked for them.
//!
//! They arrive as integer codes, decoded here so every reporter agrees on what they mean.

use std::sync::OnceLock;

use tokio::sync::watch;
use tracing::debug;

use super::{HostSignal, HostState, NetworkCost, NetworkStatus, PowerSource};

const SIGNAL_UNSUPPORTED: i32 = 0;
const SIGNAL_UNKNOWN: i32 = 1;
const POWER_EXTERNAL: i32 = 2;
const POWER_BATTERY: i32 = 3;
const NETWORK_UNMETERED: i32 = 2;
const NETWORK_METERED: i32 = 3;
const NETWORK_ABSENT: i32 = 4;

static HOST_STATE: OnceLock<watch::Sender<HostState>> = OnceLock::new();

/// Both ends initialize it, so the reporter may push before the controller exists and vice versa.
fn channel() -> &'static watch::Sender<HostState> {
    HOST_STATE.get_or_init(|| watch::channel(HostState::UNKNOWN).0)
}

pub fn subscribe() -> watch::Receiver<HostState> {
    channel().subscribe()
}

pub fn report(power: i32, network: i32) {
    let state = HostState {
        power: decode_power(power),
        network: decode_network(network),
    };
    if channel().send_if_modified(|current| {
        let changed = *current != state;
        *current = state;
        changed
    }) {
        debug!("Host state reported: {state:?}");
    }
}

/// An unrecognised code decodes to `Unknown` rather than `Unsupported`: a reading this build does
/// not know must fail toward the answer that claims least.
fn decode_power(code: i32) -> HostSignal<PowerSource> {
    match code {
        SIGNAL_UNSUPPORTED => HostSignal::Unsupported,
        SIGNAL_UNKNOWN => HostSignal::Unknown,
        POWER_EXTERNAL => HostSignal::Known(PowerSource::External),
        POWER_BATTERY => HostSignal::Known(PowerSource::Battery),
        _ => HostSignal::Unknown,
    }
}

fn decode_network(code: i32) -> HostSignal<NetworkStatus> {
    match code {
        SIGNAL_UNSUPPORTED => HostSignal::Unsupported,
        SIGNAL_UNKNOWN => HostSignal::Unknown,
        NETWORK_UNMETERED => HostSignal::Known(NetworkStatus::Present(NetworkCost::Unmetered)),
        NETWORK_METERED => HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered)),
        NETWORK_ABSENT => HostSignal::Known(NetworkStatus::Absent),
        _ => HostSignal::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_codes_decode_to_their_readings() {
        assert_eq!(
            decode_power(POWER_EXTERNAL),
            HostSignal::Known(PowerSource::External)
        );
        assert_eq!(
            decode_power(POWER_BATTERY),
            HostSignal::Known(PowerSource::Battery)
        );
        assert_eq!(decode_power(SIGNAL_UNKNOWN), HostSignal::Unknown);
        assert_eq!(decode_power(SIGNAL_UNSUPPORTED), HostSignal::Unsupported);

        assert_eq!(
            decode_network(NETWORK_UNMETERED),
            HostSignal::Known(NetworkStatus::Present(NetworkCost::Unmetered))
        );
        assert_eq!(
            decode_network(NETWORK_METERED),
            HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered))
        );
        assert_eq!(
            decode_network(NETWORK_ABSENT),
            HostSignal::Known(NetworkStatus::Absent)
        );
        assert_eq!(decode_network(SIGNAL_UNKNOWN), HostSignal::Unknown);
        assert_eq!(decode_network(SIGNAL_UNSUPPORTED), HostSignal::Unsupported);
    }

    #[test]
    fn an_unrecognised_code_can_still_block() {
        for code in [-1, 5, i32::MAX] {
            assert_eq!(decode_power(code), HostSignal::Unknown);
            assert_eq!(decode_network(code), HostSignal::Unknown);
        }
    }

    /// Both halves live in one test because the channel is a process-wide static and separate
    /// tests would depend on their order.
    ///
    /// The opening readings are delivered asynchronously, so a subscriber can arrive before
    /// either lands. A push then has to survive having had nobody to deliver it to.
    #[test]
    fn the_state_is_unknown_until_the_first_report_and_then_keeps_it() {
        assert_eq!(*subscribe().borrow(), HostState::UNKNOWN);

        report(POWER_BATTERY, NETWORK_METERED);

        let received = *subscribe().borrow();

        assert_eq!(
            received,
            HostState {
                power: HostSignal::Known(PowerSource::Battery),
                network: HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered)),
            }
        );
        assert_ne!(received, HostState::UNSUPPORTED);
    }
}
