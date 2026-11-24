use std::sync::OnceLock;

use fyles_core::io_controller::HostState;
use tokio::sync::watch;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
mod publish;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux::spawn as spawn_watchers;
#[cfg(target_os = "macos")]
use macos::spawn as spawn_watchers;
#[cfg(target_os = "windows")]
use windows::spawn as spawn_watchers;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
const INITIAL: HostState = HostState::UNKNOWN;

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
const INITIAL: HostState = HostState::UNSUPPORTED;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn spawn_watchers(_sender: watch::Sender<HostState>) {}

static HOST_STATE: OnceLock<watch::Sender<HostState>> = OnceLock::new();

/// Starting the platform watchers is a side effect of the first call. They then run until exit.
pub fn subscribe() -> watch::Receiver<HostState> {
    HOST_STATE
        .get_or_init(|| {
            let sender = watch::channel(INITIAL).0;
            spawn_watchers(sender.clone());
            sender
        })
        .subscribe()
}

/// For a build with no consumer for the readings, which should not be starting watchers.
pub fn unsupported() -> watch::Receiver<HostState> {
    static CHANNEL: OnceLock<watch::Sender<HostState>> = OnceLock::new();

    CHANNEL
        .get_or_init(|| watch::channel(HostState::UNSUPPORTED).0)
        .subscribe()
}
