use crate::core::brain::{
    action::BrainAction, BrainApiServer, BrainClientPushSender, BrainDb, BrainHostController,
    BrainNetwork,
};
use core::brain::Brain;
use std::path::PathBuf;
use tracing::debug;
use ::tracing::Instrument;

use tokio::sync::mpsc::Receiver;

pub mod core;
pub mod io_controller;
pub mod library;
#[cfg(any(test, feature = "test-support"))]
pub mod mocks;

#[derive(Clone)]
pub enum Endpoint {
    Tcp { host: String, port: Option<u16> },
    Uds { path: String },
}

impl Endpoint {
    pub fn parse(raw: &str) -> Result<Self, String> {
        if let Some(rest) = raw.strip_prefix("uds:") {
            #[cfg(unix)]
            {
                if rest.is_empty() {
                    return Err("uds path empty".into());
                }
                return Ok(Endpoint::Uds {
                    path: rest.to_string(),
                });
            }
            #[cfg(not(unix))]
            return Err("UDS endpoints not supported on this platform".into());
        }
        if let Some(rest) = raw.strip_prefix("tcp:") {
            let mut parts = rest.splitn(2, ':');
            let host = parts.next().ok_or("missing host")?;
            let port_str = parts.next();
            let port: Option<u16> = port_str
                .map(|p| p.parse().map_err(|e| format!("invalid port: {e}")))
                .transpose()?;
            return Ok(Endpoint::Tcp {
                host: host.to_string(),
                port,
            });
        }
        Err("endpoint must start with 'tcp:' or 'uds:'".into())
    }

    pub fn display(&self) -> String {
        match self {
            Endpoint::Tcp { host, port } => format!(
                "tcp:{host}:{}",
                port.map(|p| p.to_string()).unwrap_or("-auto-".into())
            ),
            Endpoint::Uds { path } => format!("uds:{path}"),
        }
    }
}

pub fn entrypoint(
    internal_data_dir: PathBuf,
    db_factory: impl AsyncFn() -> BrainDb,
    host_controller_factory: impl AsyncFn() -> BrainHostController,
    p2p_factory: impl AsyncFn() -> BrainNetwork,
    api_server_factory: impl AsyncFnOnce() -> BrainApiServer,
    brain_receiver: Receiver<BrainAction>,
    client_push_sender: BrainClientPushSender,
) {
    debug!("Running entrypoint");

    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create async runtime");

    // let async_runtime = tokio::runtime::Builder::new_current_thread()
    //     .enable_all()
    //     .build()
    //     .expect("Failed to create async runtime");

    debug!("Blocking on async workload");
    async_runtime.block_on(
        execute(
            internal_data_dir,
            db_factory,
            host_controller_factory,
            p2p_factory,
            api_server_factory,
            brain_receiver,
            client_push_sender,
        )
        .in_current_span(),
    );
}

async fn execute(
    internal_data_dir: PathBuf,
    db_factory: impl AsyncFn() -> BrainDb,
    host_controller_factory: impl AsyncFn() -> BrainHostController,
    p2p_factory: impl AsyncFn() -> BrainNetwork,
    api_server_factory: impl AsyncFnOnce() -> BrainApiServer,
    brain_receiver: Receiver<BrainAction>,
    client_push_sender: BrainClientPushSender,
) -> () {
    Brain::run(
        internal_data_dir,
        db_factory,
        host_controller_factory,
        p2p_factory,
        api_server_factory,
        brain_receiver,
        client_push_sender,
    )
    .await
    .0
    .await
    .unwrap();
}
