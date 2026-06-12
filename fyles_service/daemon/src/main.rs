#![cfg_attr(
    all(target_os = "windows", not(debug_assertions), not(feature = "console"),),
    windows_subsystem = "windows"
)]

use clap::Parser;
use libp2p::identity::Keypair;
#[cfg(feature = "otel-jaeger")]
use std::time::UNIX_EPOCH;
use std::{path::PathBuf, sync::Arc, time::SystemTime};
use tokio::sync::mpsc;
use ::tracing::info;

use fyles_core::{
    core::{
        api_server::ApiServer,
        brain::{BrainApiServer, BrainDb, BrainNetwork},
    },
    entrypoint,
    library::sqlite::{Sqlite, SqliteConfig},
    Endpoint,
};
use p2p::{event_loop::LocalEventLoop, types::Config, P2pClient};

use direct_host_utils::{
    direct_fs_controller::DirectFsController,
    tracing::{init_tracing, TracingConfig},
};

struct DaemonConfig {
    config: Config,
    #[cfg(feature = "otel-jaeger")]
    otel_endpoint: String,
}

fn main() {
    let args = Args::parse();
    let output_dir = PathBuf::from(&args.output_dir);
    let config: DaemonConfig = args.into();

    #[cfg(not(feature = "otel-jaeger"))]
    let _provider = init_tracing(
        "filerequest-node".into(),
        None,
        TracingConfig {
            with_target: true,
            with_line_number: true,
            max_events_per_span: None,
        },
    );

    let start_date = SystemTime::now();
    #[cfg(feature = "otel-jaeger")]
    let _provider = init_tracing(
        format!(
            "filerequest-node--{}",
            start_date.duration_since(UNIX_EPOCH).unwrap().as_millis()
        ),
        Some(&config.otel_endpoint),
        TracingConfig {
            with_target: true,
            with_line_number: true,
            max_events_per_span: None,
        },
    );

    info!(
        "filerequest v{} starting with instance id {start_date:?}",
        env!("CARGO_PKG_VERSION")
    );

    let endpoint = Endpoint::parse(&config.config.endpoint).unwrap();

    let (brain_sender, brain_receiver) = mpsc::channel(16);
    let brain_sender_clone = brain_sender.clone();
    let (client_sender, client_receiver) = mpsc::channel(8);

    let db_path = config.config.db_path.clone();
    entrypoint(
        config.config.internal_data_dir.clone(),
        async || {
            Arc::new(Sqlite::with_config(SqliteConfig {
                path: db_path.clone(),
            })) as BrainDb
        },
        async || {
            DirectFsController::new(output_dir.clone())
                .await
                .expect("I/O Controller to initialize fine")
                .dynamic()
        },
        async || {
            Arc::new(
                P2pClient::new(
                    brain_sender.clone(),
                    Arc::new(move |keypair: Keypair, _config: &[u8]| {
                        LocalEventLoop::swarm_factory(keypair)
                    }),
                )
                .await
                .expect("Creating P2p Client to work"),
            ) as BrainNetwork
        },
        async || {
            Box::new(ApiServer::new(
                brain_sender_clone.clone(),
                endpoint.clone(),
                config
                    .config
                    .db_path
                    .parent()
                    .expect("DB dir to have a parent that can be used as internal data dir")
                    .into(),
                client_receiver,
            )) as BrainApiServer
        },
        brain_receiver,
        client_sender,
    );
}

impl From<Args> for DaemonConfig {
    fn from(args: Args) -> Self {
        let db_path = args
            .db_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("filerequest.db"));
        let internal_data_dir = args
            .internal_data_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| db_path.parent().expect("parent of db_path to exist").into());
        if internal_data_dir.exists() && !internal_data_dir.is_dir() {
            panic!(
                "Cannot operate on internal data dir {}: It exists but is not a directory!",
                internal_data_dir.to_string_lossy()
            );
        }
        DaemonConfig {
            config: Config {
                db_path,
                internal_data_dir,
                endpoint: args.endpoint,
            },
            #[cfg(feature = "otel-jaeger")]
            otel_endpoint: args.otel_endpoint.unwrap_or_default(),
        }
    }
}

#[derive(Parser)]
#[clap(version, author, about)]
#[repr(C)]
pub struct Args {
    /// Path to the SQLite database file
    #[clap(short, long, env = "DB_PATH")]
    db_path: Option<String>,

    /// Path used for SQlite backups
    #[clap(
        short,
        long,
        env = "INTERNAL_DATA_DIR",
        long_help = r#"Path under which internal data can freely be created and stored.

Must be a directory.

If no values is set, falls back to the parent directory of the db_path."#
    )]
    internal_data_dir: Option<String>,

    /// Listening endpoint (scheme required):
    ///   tcp:HOST:PORT        e.g. tcp:0.0.0.0:50051
    ///   uds:/path/socket     (unix only) e.g. uds:/tmp/fyles.sock
    #[clap(
        long,
        short = 'e',
        env = "ENDPOINT",
        default_value = "tcp:127.0.0.1",
        long_help = r#"Listening endpoint for the API server.

Forms:
  tcp:HOST:PORT          (TCP listener, e.g. tcp:127.0.0.1:50051 or tcp:127.0.0.1 for auto port)
  uds:/path/to/socket    (Unix only, e.g. uds:/tmp/fyles.sock)
"#
    )]
    endpoint: String,

    /// Directory for storing received files
    #[clap(short = 'o', long, default_value = ".", env = "OUTPUT_DIR")]
    output_dir: String,

    #[cfg(feature = "otel-jaeger")]
    /// OTLP/HTTP endpoint for Jaeger or Collector
    #[arg(
        long,
        env = "OTEL_EXPORTER_OTLP_ENDPOINT",
        help = "e.g. http://m-mac:4318/v1/traces"
    )]
    otel_endpoint: Option<String>,
}
