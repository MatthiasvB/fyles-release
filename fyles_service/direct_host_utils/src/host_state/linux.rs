use futures::{Stream, StreamExt, join, stream};
use fyles_core::io_controller::{HostSignal, HostState, NetworkCost, NetworkStatus, PowerSource};
use tokio::sync::watch;
use tracing::warn;
use zbus::proxy::CacheProperties;
use zbus::zvariant::OwnedObjectPath;
use zbus::{Connection, fdo::PropertiesProxy};

use super::publish;

const UPOWER_SERVICE: &str = "org.freedesktop.UPower";
const UPOWER_PATH: &str = "/org/freedesktop/UPower";
const UPOWER_INTERFACE: &str = "org.freedesktop.UPower";
const NM_SERVICE: &str = "org.freedesktop.NetworkManager";
const NM_PATH: &str = "/org/freedesktop/NetworkManager";
const NM_INTERFACE: &str = "org.freedesktop.NetworkManager";

/// NetworkManager reports this when it has no primary connection, which means the default route
/// is gone *or* runs over a device NetworkManager does not manage. `Absent` therefore means "no
/// route NetworkManager knows about", and a machine whose uplink is configured outside
/// NetworkManager while NetworkManager still runs would read as absent while online.
const NO_PRIMARY_CONNECTION: &str = "/";

/// `NMMetered`. A guessed value is NetworkManager inferring from the device type rather than
/// reading an explicit setting, and is treated like the definite value it guesses. `UNKNOWN` and
/// any future value stay unknown so `strict_when_unknown` decides, where NetworkManager's own
/// advice to consumers is to read everything but `YES`/`GUESS_YES` as unmetered.
const METERED_YES: u32 = 1;
const METERED_NO: u32 = 2;
const METERED_GUESS_YES: u32 = 3;
const METERED_GUESS_NO: u32 = 4;

#[zbus::proxy(
    interface = "org.freedesktop.UPower",
    default_service = "org.freedesktop.UPower",
    default_path = "/org/freedesktop/UPower"
)]
trait UPower {
    #[zbus(property)]
    fn on_battery(&self) -> zbus::Result<bool>;
}

#[zbus::proxy(
    interface = "org.freedesktop.NetworkManager",
    default_service = "org.freedesktop.NetworkManager",
    default_path = "/org/freedesktop/NetworkManager"
)]
trait NetworkManager {
    #[zbus(property)]
    fn metered(&self) -> zbus::Result<u32>;

    #[zbus(property)]
    fn primary_connection(&self) -> zbus::Result<OwnedObjectPath>;
}

pub(super) fn spawn(sender: watch::Sender<HostState>) {
    tokio::spawn(async move {
        let connection = match Connection::system().await {
            Ok(connection) => connection,
            Err(e) => {
                // Nothing to subscribe to, so both readings stay unsupported for good.
                warn!("No D-Bus system bus, so the run conditions cannot be enforced: {e}");
                return;
            }
        };
        join!(
            watch_power(&connection, &sender),
            watch_network(&connection, &sender)
        );
    });
}

/// Whatever ends the watch — a refused proxy, a dropped bus — the reading is no longer being
/// observed, so publishing here rather than at each exit keeps a stale value from outliving the
/// watcher that produced it.
async fn watch_power(connection: &Connection, sender: &watch::Sender<HostState>) {
    if let Err(e) = power_readings(connection, sender).await {
        warn!("Cannot read UPower: {e}");
    }
    publish::power(sender, HostSignal::Unsupported);
}

async fn power_readings(
    connection: &Connection,
    sender: &watch::Sender<HostState>,
) -> zbus::Result<()> {
    let proxy = UPowerProxy::builder(connection)
        .cache_properties(CacheProperties::No)
        .build()
        .await?;
    let mut wakes = wake_stream(connection, UPOWER_SERVICE, UPOWER_PATH, UPOWER_INTERFACE).await?;

    loop {
        publish::power(sender, read_power(&proxy).await);
        if wakes.next().await.is_none() {
            return Ok(());
        }
    }
}

async fn watch_network(connection: &Connection, sender: &watch::Sender<HostState>) {
    if let Err(e) = network_readings(connection, sender).await {
        warn!("Cannot read NetworkManager: {e}");
    }
    publish::network(sender, HostSignal::Unsupported);
}

async fn network_readings(
    connection: &Connection,
    sender: &watch::Sender<HostState>,
) -> zbus::Result<()> {
    let proxy = NetworkManagerProxy::builder(connection)
        .cache_properties(CacheProperties::No)
        .build()
        .await?;
    let mut wakes = wake_stream(connection, NM_SERVICE, NM_PATH, NM_INTERFACE).await?;

    loop {
        publish::network(sender, read_network(&proxy).await);
        if wakes.next().await.is_none() {
            return Ok(());
        }
    }
}

/// Signals rather than zbus's property cache, so a service that restarts cannot leave a reading
/// frozen at its last value. Losing the name is itself a wake, and the read it triggers fails and
/// reports the signal unavailable.
async fn wake_stream(
    connection: &Connection,
    service: &'static str,
    path: &'static str,
    interface: &'static str,
) -> zbus::Result<impl Stream<Item = ()> + Unpin> {
    let properties = PropertiesProxy::builder(connection)
        .destination(service)?
        .path(path)?
        .cache_properties(CacheProperties::No)
        .build()
        .await?;
    // Filtered on the signal's interface argument, so another interface on the same object cannot
    // wake a read that could not tell us anything new.
    let changed = properties
        .receive_properties_changed_with_args(&[(0, interface)])
        .await?;
    let owner = properties.inner().receive_owner_changed().await?;
    Ok(Box::pin(stream::select(
        changed.map(|_| ()),
        owner.map(|_| ()),
    )))
}

/// A failed read is reported as unsupported, which is what an absent service produces and what a
/// service that vanished mid-call amounts to. A transient failure therefore leaves the condition
/// unenforced until the next wake, so the error is logged rather than swallowed.
async fn read_power(proxy: &UPowerProxy<'_>) -> HostSignal<PowerSource> {
    match proxy.on_battery().await {
        Ok(true) => HostSignal::Known(PowerSource::Battery),
        Ok(false) => HostSignal::Known(PowerSource::External),
        Err(e) => {
            warn!("Cannot read UPower.OnBattery, so power is unavailable: {e}");
            HostSignal::Unsupported
        }
    }
}

async fn read_network(proxy: &NetworkManagerProxy<'_>) -> HostSignal<NetworkStatus> {
    let primary = proxy.primary_connection().await;
    let metered = proxy.metered().await;
    match (primary, metered) {
        (Ok(primary), Ok(metered)) => network_status(primary.as_str(), metered),
        (Err(e), _) | (_, Err(e)) => {
            warn!("Cannot read NetworkManager, so the network is unavailable: {e}");
            HostSignal::Unsupported
        }
    }
}

fn network_status(primary_connection: &str, metered: u32) -> HostSignal<NetworkStatus> {
    if primary_connection == NO_PRIMARY_CONNECTION {
        return HostSignal::Known(NetworkStatus::Absent);
    }
    match metered {
        METERED_YES | METERED_GUESS_YES => {
            HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered))
        }
        METERED_NO | METERED_GUESS_NO => {
            HostSignal::Known(NetworkStatus::Present(NetworkCost::Unmetered))
        }
        _ => HostSignal::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A_CONNECTION: &str = "/org/freedesktop/NetworkManager/ActiveConnection/1";

    #[test]
    fn no_primary_connection_beats_every_cost() {
        for metered in [
            METERED_YES,
            METERED_NO,
            METERED_GUESS_YES,
            METERED_GUESS_NO,
            0,
            99,
        ] {
            assert_eq!(
                network_status(NO_PRIMARY_CONNECTION, metered),
                HostSignal::Known(NetworkStatus::Absent)
            );
        }
    }

    #[test]
    fn a_guessed_cost_counts_as_the_cost_it_guessed() {
        for metered in [METERED_YES, METERED_GUESS_YES] {
            assert_eq!(
                network_status(A_CONNECTION, metered),
                HostSignal::Known(NetworkStatus::Present(NetworkCost::Metered))
            );
        }
        for metered in [METERED_NO, METERED_GUESS_NO] {
            assert_eq!(
                network_status(A_CONNECTION, metered),
                HostSignal::Known(NetworkStatus::Present(NetworkCost::Unmetered))
            );
        }
    }

    /// Unknown rather than unsupported: NetworkManager is there and has no answer, which is what
    /// the strictness setting exists to govern.
    #[test]
    fn an_unreadable_cost_is_unknown() {
        for metered in [0, 5, u32::MAX] {
            assert_eq!(network_status(A_CONNECTION, metered), HostSignal::Unknown);
        }
    }
}
