#![cfg_attr(
    all(target_os = "windows", not(debug_assertions), not(feature = "console"),),
    windows_subsystem = "windows"
)]

use ::tracing::info;
use clap::Parser;
use libp2p::identity::Keypair;
#[cfg(feature = "otel-jaeger")]
use std::time::UNIX_EPOCH;
use std::{path::PathBuf, sync::Arc, time::SystemTime};
use tokio::sync::RwLock;

use fyles_core::core::brain::plugin::{BrainPluginRef, NoOpBrainPlugin};
use fyles_core::library::sqlite::SqlitePath;
use fyles_core::{
    Endpoint,
    core::{
        api_server::ApiServer,
        brain::{BrainApiServer, BrainDb, BrainNetwork},
    },
    entrypoint,
    library::sqlite::{Sqlite, SqliteConfig},
};
use p2p::{P2pClient, event_loop::LocalEventLoop, types::Config};

use direct_host_utils::{
    direct_fs_controller::DirectFsController,
    host_state,
    tracing::{TracingConfig, init_tracing},
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

    let db_path = config.config.db_path.clone();
    entrypoint(
        config.config.internal_data_dir.clone(),
        async || {
            Sqlite::spawn(SqliteConfig {
                path: SqlitePath::File(db_path.clone()),
            }) as BrainDb
        },
        async || {
            // the plain daemon does not require host state awareness
            DirectFsController::new(output_dir.clone(), host_state::unsupported())
                .await
                .expect("I/O Controller to initialize fine")
                .dynamic()
        },
        async |brain_sender, shutdown| {
            Arc::new(RwLock::new(
                P2pClient::new(
                    brain_sender,
                    shutdown,
                    Arc::new(move |keypair: Keypair, _config: &[u8]| {
                        LocalEventLoop::swarm_factory(keypair)
                    }),
                )
                .await
                .expect("Creating P2p Client to work"),
            )) as BrainNetwork
        },
        async |_| Arc::new(RwLock::new(NoOpBrainPlugin)) as BrainPluginRef,
        async |brain_sender, client_push_receiver, shutdown| {
            Box::new(ApiServer::new(
                brain_sender,
                endpoint.clone(),
                config
                    .config
                    .db_path
                    .parent()
                    .expect("DB dir to have a parent that can be used as internal data dir")
                    .into(),
                client_push_receiver,
                shutdown,
            )) as BrainApiServer
        },
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
    ///   uds:/path/socket     (unix only) e.g. uds:$XDG_RUNTIME_DIR/fyles.sock
    #[clap(
        long,
        short = 'e',
        env = "ENDPOINT",
        default_value = "tcp:127.0.0.1",
        long_help = r#"Listening endpoint for the API server.

Forms:
  tcp:HOST:PORT          (TCP listener, e.g. tcp:127.0.0.1:50051 or tcp:127.0.0.1 for auto port)
  uds:/path/to/socket    (Unix only, e.g. uds:$XDG_RUNTIME_DIR/fyles.sock)
"#
    )]
    endpoint: String,

    /// Directory for storing received files (the "drop dir"). This is the sole
    /// source of truth for where files land on desktop: it is chosen here at
    /// launch, held in memory by the DirectFsController, and surfaced to the UI
    /// via `HostController::drop_dir_absolute_path()` — it is never stored in the
    /// database. (On mobile the equivalent is the platform's scoped storage:
    /// Android's persisted SAF tree, iOS's Documents/Drop.)
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
