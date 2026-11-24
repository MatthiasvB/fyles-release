use crate::core::brain::plugin::BrainPluginRef;
use crate::core::brain::{
    BrainApiServer, BrainDb, BrainHostController, BrainNetwork, GenerationEnd, NodeSettings,
    action::BrainAction,
};
use crate::library::wire::api::ServerMessage;
use ::tracing::Instrument;
use core::brain::Brain;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

pub mod constants;
pub mod core;
#[cfg(test)]
mod generation_tests;
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
    settings: NodeSettings,
    db_factory: impl AsyncFn() -> BrainDb,
    host_controller_factory: impl AsyncFn() -> BrainHostController,
    network_factory: impl AsyncFn(Sender<BrainAction>, CancellationToken) -> BrainNetwork,
    plugin_factory: impl AsyncFn(Sender<BrainAction>) -> BrainPluginRef,
    api_server_factory: impl AsyncFn(
        Sender<BrainAction>,
        Receiver<ServerMessage>,
        CancellationToken,
    ) -> BrainApiServer,
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
        run_generations(
            settings,
            db_factory,
            host_controller_factory,
            network_factory,
            plugin_factory,
            api_server_factory,
        )
        .in_current_span(),
    );
}

async fn run_generations(
    settings: NodeSettings,
    db_factory: impl AsyncFn() -> BrainDb,
    host_controller_factory: impl AsyncFn() -> BrainHostController,
    network_factory: impl AsyncFn(Sender<BrainAction>, CancellationToken) -> BrainNetwork,
    plugin_factory: impl AsyncFn(Sender<BrainAction>) -> BrainPluginRef,
    api_server_factory: impl AsyncFn(
        Sender<BrainAction>,
        Receiver<ServerMessage>,
        CancellationToken,
    ) -> BrainApiServer,
) {
    loop {
        let (brain_sender, brain_receiver) = mpsc::channel(16);
        let (client_push_sender, client_push_receiver) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let (generation, _) = Brain::run(
            settings.clone(),
            &db_factory,
            &host_controller_factory,
            async || network_factory(brain_sender.clone(), shutdown.clone()).await,
            async || plugin_factory(brain_sender.clone()).await,
            async || {
                api_server_factory(brain_sender.clone(), client_push_receiver, shutdown.clone())
                    .await
            },
            brain_receiver,
            brain_sender.clone(),
            client_push_sender,
            shutdown.clone(),
        )
        .await;
        match generation.await.expect("the brain loop not to panic") {
            GenerationEnd::Stop => return,
            GenerationEnd::Restart => info!("Restarting the brain"),
        }
    }
}
