use std::ffi::{c_int, c_void};
use std::sync::OnceLock;

use block2::{Block, RcBlock};
use dispatch2::{DispatchQueue, DispatchQueueAttr, DispatchRetained};
use fyles_core::io_controller::{HostSignal, HostState, NetworkCost, NetworkStatus, PowerSource};
use objc2_io_kit::{
    IOPSCopyPowerSourcesInfo, IOPSGetProvidingPowerSourceType, kIOPMACPowerKey,
    kIOPMBatteryPowerKey, kIOPMUPSPowerKey, kIOPSNotifyPowerSource,
};
use tokio::sync::watch;
use tracing::warn;

use super::publish;

const NOTIFY_STATUS_OK: u32 = 0;

const PATH_STATUS_INVALID: c_int = 0;
const PATH_STATUS_SATISFIED: c_int = 1;
const PATH_STATUS_UNSATISFIED: c_int = 2;
const PATH_STATUS_SATISFIABLE: c_int = 3;

/// Opaque Network.framework objects. Distinct types so a monitor cannot be passed where a path
/// belongs, which `*mut c_void` would have accepted.
enum NwPathMonitor {}
enum NwPath {}

#[link(name = "Network", kind = "framework")]
unsafe extern "C" {
    fn nw_path_monitor_create() -> *mut NwPathMonitor;
    fn nw_path_monitor_set_queue(monitor: *mut NwPathMonitor, queue: &DispatchQueue);
    // The handler's parameter stays `c_void` because a block argument has to carry an Objective-C
    // encoding, which an opaque type has none of. It is cast back at the one place it arrives.
    fn nw_path_monitor_set_update_handler(
        monitor: *mut NwPathMonitor,
        handler: &Block<dyn Fn(*mut c_void)>,
    );
    fn nw_path_monitor_start(monitor: *mut NwPathMonitor);
    fn nw_path_get_status(path: *mut NwPath) -> c_int;
    fn nw_path_is_expensive(path: *mut NwPath) -> bool;
    fn nw_path_is_constrained(path: *mut NwPath) -> bool;
}

unsafe extern "C" {
    fn notify_register_dispatch(
        name: *const c_void,
        out_token: *mut c_int,
        queue: &DispatchQueue,
        handler: &Block<dyn Fn(c_int)>,
    ) -> u32;
}

/// Serial, and that is load-bearing rather than incidental. Each block reads and publishes within
/// its own execution and executions do not overlap, so the block that publishes last is the block
/// that read last. A concurrent queue would let an older reading land after a newer one.
fn serial_queue() -> &'static DispatchQueue {
    static QUEUE: OnceLock<DispatchRetained<DispatchQueue>> = OnceLock::new();

    QUEUE.get_or_init(|| DispatchQueue::new("app.fyles.host-state", DispatchQueueAttr::SERIAL))
}

/// Runs once per process. Neither registration is ever undone and the path monitor is deliberately
/// never released, so a second call would leak a monitor and double the callbacks.
/// [`super::subscribe`] holds this to one call.
pub(super) fn spawn(sender: watch::Sender<HostState>) {
    watch_power(&sender);
    watch_network(&sender);
}

fn watch_power(sender: &watch::Sender<HostState>) {
    let on_change = {
        let sender = sender.clone();
        RcBlock::new(move |_token: c_int| publish::power(&sender, power_reading()))
    };

    let mut token = 0;
    let status = unsafe {
        notify_register_dispatch(
            kIOPSNotifyPowerSource.as_ptr().cast(),
            &mut token,
            serial_queue(),
            &on_change,
        )
    };

    if status != NOTIFY_STATUS_OK {
        warn!("Cannot watch the power source, notify status {status}");
        publish::power(sender, HostSignal::Unsupported);
        return;
    }

    // Enqueued rather than read here, so the first reading is ordered against the callbacks
    // instead of racing them.
    let sender = sender.clone();
    serial_queue().exec_async(move || publish::power(&sender, power_reading()));
}

fn power_reading() -> HostSignal<PowerSource> {
    let Some(snapshot) = IOPSCopyPowerSourcesInfo() else {
        warn!("IOKit has no power source snapshot");
        return HostSignal::Unknown;
    };

    let providing = unsafe { IOPSGetProvidingPowerSourceType(Some(&snapshot)) };
    let Some(providing) = providing else {
        warn!("IOKit names no providing power source");
        return HostSignal::Unknown;
    };

    power_source(&providing.to_string())
}

fn power_source(providing: &str) -> HostSignal<PowerSource> {
    let providing = providing.as_bytes();

    if providing == kIOPMACPowerKey.to_bytes() {
        HostSignal::Known(PowerSource::External)
    } else if providing == kIOPMBatteryPowerKey.to_bytes()
        // Only ever the providing source once mains is lost, so the machine is running down a
        // store of charge just as it would on its own battery.
        || providing == kIOPMUPSPowerKey.to_bytes()
    {
        HostSignal::Known(PowerSource::Battery)
    } else {
        warn!("Unrecognised power source {:?}", providing.escape_ascii());
        HostSignal::Unknown
    }
}

fn watch_network(sender: &watch::Sender<HostState>) {
    let monitor = unsafe { nw_path_monitor_create() };
    if monitor.is_null() {
        warn!("Cannot create a network path monitor");
        publish::network(sender, HostSignal::Unsupported);
        return;
    }

    let on_change = {
        let sender = sender.clone();
        RcBlock::new(move |path: *mut c_void| {
            publish::network(&sender, network_reading(path.cast()))
        })
    };

    // Both must be set before the monitor starts, and starting delivers the current path, so this
    // half needs no separate first reading.
    unsafe {
        nw_path_monitor_set_queue(monitor, serial_queue());
        nw_path_monitor_set_update_handler(monitor, &on_change);
        nw_path_monitor_start(monitor);
    }
}

fn network_reading(path: *mut NwPath) -> HostSignal<NetworkStatus> {
    let raw = unsafe { nw_path_get_status(path) };
    let cost = || {
        network_cost(unsafe { nw_path_is_expensive(path) }, unsafe {
            nw_path_is_constrained(path)
        })
    };

    let Some(reading) = path_reading(raw, cost) else {
        warn!("Unrecognised network path status {raw}");
        return HostSignal::Unknown;
    };

    network_status(reading)
}

/// A cost only exists where there is a link to have one, so it rides on the one variant that has
/// a link and is never read for the others.
enum PathReading {
    Invalid,
    Satisfied(NetworkCost),
    Unsatisfied,
    RequiresConnection,
}

fn path_reading(raw: c_int, cost: impl FnOnce() -> NetworkCost) -> Option<PathReading> {
    match raw {
        PATH_STATUS_INVALID => Some(PathReading::Invalid),
        PATH_STATUS_SATISFIED => Some(PathReading::Satisfied(cost())),
        PATH_STATUS_UNSATISFIED => Some(PathReading::Unsatisfied),
        PATH_STATUS_SATISFIABLE => Some(PathReading::RequiresConnection),
        _ => None,
    }
}

fn network_cost(expensive: bool, constrained: bool) -> NetworkCost {
    if expensive || constrained {
        NetworkCost::Metered
    } else {
        NetworkCost::Unmetered
    }
}

fn network_status(reading: PathReading) -> HostSignal<NetworkStatus> {
    match reading {
        PathReading::Satisfied(cost) => HostSignal::Known(NetworkStatus::Present(cost)),
        PathReading::Unsatisfied => HostSignal::Known(NetworkStatus::Absent),
        // A connection attempt could make this path work, and ours might be the attempt that does
        // it. Calling it absent would forbid the traffic that would have proved it usable, with no
        // setting to escape that, so let strictness rule on it instead.
        PathReading::RequiresConnection | PathReading::Invalid => HostSignal::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ac_power() -> String {
        kIOPMACPowerKey.to_str().unwrap().to_owned()
    }

    #[test]
    fn mains_power_is_external_power() {
        assert_eq!(
            power_source(&ac_power()),
            HostSignal::Known(PowerSource::External)
        );
    }

    /// A UPS only becomes the providing source once mains is lost, which is the state the setting
    /// exists to protect against. While the mains is up a Mac on a UPS still reports AC power.
    #[test]
    fn a_ups_is_battery_power() {
        for providing in [kIOPMBatteryPowerKey, kIOPMUPSPowerKey] {
            assert_eq!(
                power_source(providing.to_str().unwrap()),
                HostSignal::Known(PowerSource::Battery)
            );
        }
    }

    #[test]
    fn an_unrecognised_power_source_answers_nothing() {
        assert_eq!(power_source("Solar Power"), HostSignal::Unknown);
    }

    #[test]
    fn only_an_unconstrained_link_is_unmetered() {
        assert_eq!(network_cost(false, false), NetworkCost::Unmetered);
    }

    #[test]
    fn an_expensive_or_constrained_link_is_metered() {
        for (expensive, constrained) in [(true, false), (false, true), (true, true)] {
            assert_eq!(network_cost(expensive, constrained), NetworkCost::Metered);
        }
    }

    #[test]
    fn a_satisfied_path_carries_its_cost() {
        for cost in [NetworkCost::Unmetered, NetworkCost::Metered] {
            assert_eq!(
                network_status(PathReading::Satisfied(cost)),
                HostSignal::Known(NetworkStatus::Present(cost))
            );
        }
    }

    #[test]
    fn an_unreachable_path_is_an_absent_network() {
        assert_eq!(
            network_status(PathReading::Unsatisfied),
            HostSignal::Known(NetworkStatus::Absent)
        );
    }

    /// Neither "there is a network" nor "there is none": a connection attempt decides, and ours
    /// may be it. `Absent` is the one reading no setting can override, so it would trap a machine
    /// whose only route is an on-demand VPN.
    #[test]
    fn a_path_that_can_still_be_activated_answers_nothing() {
        assert_eq!(
            network_status(PathReading::RequiresConnection),
            HostSignal::Unknown
        );
    }

    #[test]
    fn an_invalid_path_answers_nothing() {
        assert_eq!(network_status(PathReading::Invalid), HostSignal::Unknown);
    }

    #[test]
    fn an_unrecognised_path_status_answers_nothing() {
        assert!(path_reading(42, || NetworkCost::Unmetered).is_none());
    }

    /// The cost is two more calls into Network.framework, so it is only worth asking for where the
    /// answer has somewhere to live.
    #[test]
    fn cost_is_only_read_for_a_satisfied_path() {
        for raw in [
            PATH_STATUS_INVALID,
            PATH_STATUS_UNSATISFIED,
            PATH_STATUS_SATISFIABLE,
        ] {
            path_reading(raw, || panic!("cost read for status {raw}"));
        }
    }
}
