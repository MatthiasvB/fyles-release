use std::sync::{Mutex, PoisonError};

use fyles_core::io_controller::{HostSignal, HostState, NetworkCost, NetworkStatus, PowerSource};
use tokio::sync::watch;
use tracing::warn;
use windows::Foundation::EventHandler;
use windows::Networking::Connectivity::{
    ConnectionCost, NetworkCostType, NetworkInformation, NetworkStatusChangedEventHandler,
};
use windows::System::Power::{BatteryStatus, PowerManager};
use windows::Win32::System::Com::CoIncrementMTAUsage;
use windows::core::{IInspectable, Result as WindowsResult};

use super::publish;

static POWER_REFRESH: Mutex<()> = Mutex::new(());
static NETWORK_REFRESH: Mutex<()> = Mutex::new(());

/// Runs once per process. Neither the event registrations nor the MTA usage count is ever undone,
/// so a second call would double both. [`super::subscribe`] holds this to one call.
pub(super) fn spawn(sender: watch::Sender<HostState>) {
    // WinRT statics and the pool threads their callbacks arrive on need the process MTA to exist.
    // Nothing decrements the usage count, because the watchers run until exit.
    if let Err(e) = unsafe { CoIncrementMTAUsage() } {
        warn!("Cannot ensure the COM multi-threaded apartment: {e}");
        return;
    }

    watch_power(&sender);
    watch_network(&sender);
}

fn watch_power(sender: &watch::Sender<HostState>) {
    if let Err(e) = register_power(sender) {
        warn!("Cannot read the power state: {e}");
        refresh_power(sender, || HostSignal::Unsupported);
    }
}

fn register_power(sender: &watch::Sender<HostState>) -> WindowsResult<()> {
    let on_change = {
        let sender = sender.clone();
        EventHandler::<IInspectable>::new(move |_, _| {
            refresh_power(&sender, power_reading);
            Ok(())
        })
    };

    PowerManager::BatteryStatusChanged(&on_change)?;
    refresh_power(sender, power_reading);
    Ok(())
}

fn power_reading() -> HostSignal<PowerSource> {
    match PowerManager::BatteryStatus() {
        Ok(battery) => power_source(battery),
        Err(e) => {
            warn!("Cannot read the battery status: {e}");
            HostSignal::Unknown
        }
    }
}

fn power_source(battery: BatteryStatus) -> HostSignal<PowerSource> {
    match battery {
        BatteryStatus::Discharging => HostSignal::Known(PowerSource::Battery),
        BatteryStatus::NotPresent | BatteryStatus::Charging | BatteryStatus::Idle => {
            HostSignal::Known(PowerSource::External)
        }
        battery => {
            warn!("Unrecognised battery status {battery:?}");
            HostSignal::Unknown
        }
    }
}

fn watch_network(sender: &watch::Sender<HostState>) {
    if let Err(e) = register_network(sender) {
        warn!("Cannot read the network state: {e}");
        refresh_network(sender, || HostSignal::Unsupported);
    }
}

fn register_network(sender: &watch::Sender<HostState>) -> WindowsResult<()> {
    let on_change = {
        let sender = sender.clone();
        NetworkStatusChangedEventHandler::new(move |_| {
            refresh_network(&sender, network_reading);
            Ok(())
        })
    };

    NetworkInformation::NetworkStatusChanged(&on_change)?;
    refresh_network(sender, network_reading);
    Ok(())
}

fn network_reading() -> HostSignal<NetworkStatus> {
    let profile = match NetworkInformation::GetInternetConnectionProfile() {
        Ok(profile) => profile,
        // WinRT answers with null when it has no profile to offer, and windows-rs projects a null
        // interface as an empty error whose HRESULT is still a success. That shape alone means
        // "no network", and a real failure carries a failed `HRESULT`.
        Err(e) if e.code().is_ok() => return HostSignal::Known(NetworkStatus::Absent),
        Err(e) => {
            warn!("Cannot read the internet connection profile: {e}");
            return HostSignal::Unknown;
        }
    };

    match profile.GetConnectionCost() {
        Ok(cost) => network_cost(&cost),
        Err(e) => {
            warn!("Cannot read the connection cost: {e}");
            HostSignal::Unknown
        }
    }
}

fn network_cost(cost: &ConnectionCost) -> HostSignal<NetworkStatus> {
    let (kind, roaming, over_limit) =
        match (cost.NetworkCostType(), cost.Roaming(), cost.OverDataLimit()) {
            (Ok(kind), Ok(roaming), Ok(over_limit)) => (kind, roaming, over_limit),
            (kind, roaming, over_limit) => {
                warn!("Cannot read the connection cost: {kind:?}, {roaming:?}, {over_limit:?}");
                return HostSignal::Unknown;
            }
        };

    // This one lives on a later interface than the rest, and missing it is no reason to distrust
    // the readings that did arrive.
    let background_restricted = cost.BackgroundDataUsageRestricted().unwrap_or(false);

    network_status(kind, roaming, over_limit, background_restricted)
}

fn network_status(
    kind: NetworkCostType,
    roaming: bool,
    over_limit: bool,
    background_restricted: bool,
) -> HostSignal<NetworkStatus> {
    // Whatever the link costs nominally, Windows says background traffic is unwelcome on it once
    // it roams, passes its cap, or the user restricts it. Reprovides are background traffic.
    if roaming || over_limit || background_restricted {
        return HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered));
    }

    match kind {
        NetworkCostType::Unrestricted => {
            HostSignal::Known(NetworkStatus::Present(NetworkCost::Unmetered))
        }
        NetworkCostType::Fixed | NetworkCostType::Variable => {
            HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered))
        }
        NetworkCostType::Unknown => HostSignal::Unknown,
        kind => {
            warn!("Unrecognised network cost type {kind:?}");
            HostSignal::Unknown
        }
    }
}

/// Reading and publishing are one step under the lock. Callbacks arrive on pool threads, so a
/// callback that landed between another refresh's read and its write would otherwise be undone by
/// the older reading.
fn refresh_power(
    sender: &watch::Sender<HostState>,
    read: impl FnOnce() -> HostSignal<PowerSource>,
) {
    let _guard = POWER_REFRESH.lock().unwrap_or_else(PoisonError::into_inner);
    publish::power(sender, read());
}

fn refresh_network(
    sender: &watch::Sender<HostState>,
    read: impl FnOnce() -> HostSignal<NetworkStatus>,
) {
    let _guard = NETWORK_REFRESH
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    publish::network(sender, read());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn present(cost: NetworkCost) -> HostSignal<NetworkStatus> {
        HostSignal::Known(NetworkStatus::Present(cost))
    }

    fn unconstrained(kind: NetworkCostType) -> HostSignal<NetworkStatus> {
        network_status(kind, false, false, false)
    }

    #[test]
    fn a_machine_without_a_battery_runs_on_external_power() {
        assert_eq!(
            power_source(BatteryStatus::NotPresent),
            HostSignal::Known(PowerSource::External)
        );
    }

    #[test]
    fn only_a_discharging_battery_is_battery_power() {
        assert_eq!(
            power_source(BatteryStatus::Discharging),
            HostSignal::Known(PowerSource::Battery)
        );
        for battery in [BatteryStatus::Charging, BatteryStatus::Idle] {
            assert_eq!(
                power_source(battery),
                HostSignal::Known(PowerSource::External)
            );
        }
    }

    #[test]
    fn an_unrecognised_battery_status_answers_nothing() {
        assert_eq!(power_source(BatteryStatus(42)), HostSignal::Unknown);
    }

    #[test]
    fn cost_type_decides_an_unconstrained_link() {
        assert_eq!(
            unconstrained(NetworkCostType::Unrestricted),
            present(NetworkCost::Unmetered)
        );
        for kind in [NetworkCostType::Fixed, NetworkCostType::Variable] {
            assert_eq!(unconstrained(kind), present(NetworkCost::Metered));
        }
    }

    #[test]
    fn any_constraint_meters_a_free_link() {
        for constraint in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let (roaming, over_limit, background_restricted) = constraint;
            assert_eq!(
                network_status(
                    NetworkCostType::Unrestricted,
                    roaming,
                    over_limit,
                    background_restricted
                ),
                present(NetworkCost::Metered)
            );
        }
    }

    /// A constraint outranks a cost the OS could not classify, because it is an answer where the
    /// cost type is the absence of one.
    #[test]
    fn a_constraint_answers_even_when_the_cost_type_does_not() {
        assert_eq!(
            network_status(NetworkCostType::Unknown, false, false, true),
            present(NetworkCost::Metered)
        );
    }

    #[test]
    fn an_unclassified_cost_answers_nothing() {
        assert_eq!(unconstrained(NetworkCostType::Unknown), HostSignal::Unknown);
        assert_eq!(unconstrained(NetworkCostType(42)), HostSignal::Unknown);
    }

    /// A link Windows cannot price is still a link, so it never reads as having no network.
    #[test]
    fn an_unclassified_cost_is_not_an_absent_network() {
        assert_ne!(
            unconstrained(NetworkCostType::Unknown),
            HostSignal::Known(NetworkStatus::Absent)
        );
    }
}
