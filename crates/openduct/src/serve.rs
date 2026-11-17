// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct serve`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, ValueEnum};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{FileStore, FileStoreConfig, FsyncPolicy, MemoryStore, Store};

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Args)]
pub(crate) struct ServeArgs {
    /// Address to listen on. The default accepts only this machine; use 0.0.0.0:9092 (with --advertise) to accept
    /// others.
    #[arg(long, env = "OPENDUCT_LISTEN", default_value = "127.0.0.1:9092")]
    pub(crate) listen: SocketAddr,
    /// host:port clients are told to connect to. Kafka clients ask the broker for its address and reconnect there,
    /// so this must be reachable by them. Defaults to the listen address.
    #[arg(long, env = "OPENDUCT_ADVERTISE")]
    advertise: Option<String>,
    /// Directory for the durable log. Created if missing; only one broker may use it at a time.
    #[arg(
        long,
        env = "OPENDUCT_DATA_DIR",
        default_value = "openduct-data",
        conflicts_with = "memory"
    )]
    pub(crate) data_dir: PathBuf,
    /// Keep everything in memory: fast, and lost when the broker stops.
    #[arg(long, env = "OPENDUCT_MEMORY")]
    pub(crate) memory: bool,
    /// Where OpenDuct keeps its own log on disk, as OpenTelemetry log records (OTLP JSON Lines, a file a day), beside
    /// what it writes to stderr. Default: the data directory's `logs` folder (none with --memory); `none`: no file.
    #[arg(long, env = "OPENDUCT_LOG_DIR")]
    log_dir: Option<PathBuf>,
    /// How many days of log files are kept.
    #[arg(long, env = "OPENDUCT_LOG_KEEP_DAYS", default_value_t = 14, value_parser = clap::value_parser!(u32).range(1..=3650))]
    log_keep_days: u32,
    /// When a write reaches the disk. `always`: before it is acknowledged (no acknowledged write is lost on power
    /// loss). `never`: left to the operating system (faster; a crash of the machine can lose recent writes).
    #[arg(long, env = "OPENDUCT_FSYNC", value_enum, default_value_t = Fsync::Always)]
    fsync: Fsync,
    /// Size at which a log segment is closed and a new one started, in MiB.
    #[arg(long, env = "OPENDUCT_SEGMENT_MIB", default_value_t = 64, value_parser = clap::value_parser!(u64).range(1..=4096))]
    segment_mib: u64,
    /// This broker's id, reported to clients; in a cluster, its id among --brokers.
    #[arg(long, env = "OPENDUCT_NODE_ID", default_value_t = 0)]
    node_id: i32,
    /// The cluster id, reported to clients.
    #[arg(long, env = "OPENDUCT_CLUSTER_ID", default_value = "openduct")]
    cluster_id: String,
    /// The largest request accepted, in MiB.
    #[arg(long, env = "OPENDUCT_MAX_REQUEST_MIB", default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..=1024))]
    max_request_mib: u64,
    /// The most request bytes held at once across every connection, in MiB; at least --max-request-mib. When it is
    /// spent, reading waits: the broker's memory for requests never grows past it.
    #[arg(long, env = "OPENDUCT_MAX_REQUEST_MEMORY_MIB", default_value_t = 256, value_parser = clap::value_parser!(u64).range(1..=65_536))]
    max_request_memory_mib: u64,
    /// A request's body must arrive within this many seconds once its size has.
    #[arg(long, env = "OPENDUCT_REQUEST_READ_TIMEOUT_SECS", default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    request_read_timeout_secs: u64,
    /// The most bytes one Fetch answer carries, in MiB, whatever the client asks.
    #[arg(long, env = "OPENDUCT_MAX_FETCH_MIB", default_value_t = 55, value_parser = clap::value_parser!(u64).range(1..=1024))]
    max_fetch_mib: u64,
    /// The most client connections at once.
    #[arg(long, env = "OPENDUCT_MAX_CONNECTIONS", default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..=1_000_000))]
    max_connections: u64,
    /// Close a connection that sends nothing for this many seconds.
    #[arg(long, env = "OPENDUCT_IDLE_TIMEOUT_SECS", default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..))]
    idle_timeout_secs: u64,
    /// Create a topic when a client writes to or asks for one that does not exist.
    #[arg(long, env = "OPENDUCT_AUTO_CREATE_TOPICS")]
    auto_create_topics: bool,
    /// Partitions of a topic created automatically or without a partition count.
    #[arg(long, env = "OPENDUCT_DEFAULT_PARTITIONS", default_value_t = 1, value_parser = clap::value_parser!(i32).range(1..=10_000))]
    default_partitions: i32,
    /// How long a new consumer group waits for more members before its first assignment, in milliseconds.
    #[arg(
        long,
        env = "OPENDUCT_GROUP_INITIAL_REBALANCE_DELAY_MS",
        default_value_t = 3_000
    )]
    group_initial_rebalance_delay_ms: u64,
    /// How often expired data is removed, in seconds.
    #[arg(long, env = "OPENDUCT_RETENTION_CHECK_SECS", default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    retention_check_secs: u64,
    /// Log format: `text` for people, `json` for log collectors. Detail is set with RUST_LOG (default `info`).
    #[arg(long, env = "OPENDUCT_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    pub(crate) log_format: LogFormat,
    #[command(flatten)]
    pub(crate) cluster: crate::cluster::ClusterArgs,
    #[command(flatten)]
    security: crate::security_args::SecurityArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Fsync {
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum LogFormat {
    Text,
    Json,
}

impl ServeArgs {
    /// Whether this is the first node (or the only one): what runs once in a cluster runs here.
    pub(crate) fn first_node(&self) -> bool {
        self.node_id <= 1
    }

    /// Where the log's files go, if anywhere.
    pub(crate) fn log_files(&self) -> Option<crate::otel_log::LogFiles> {
        let dir = match &self.log_dir {
            Some(d) if d.as_os_str() == "none" || d.as_os_str().is_empty() => return None,
            Some(d) => d.clone(),
            None if self.memory => return None,
            None => self.data_dir.join("logs"),
        };
        Some(crate::otel_log::LogFiles {
            dir,
            keep_days: self.log_keep_days,
            instance: format!("node-{}", self.node_id),
        })
    }
}

pub(crate) async fn run(args: ServeArgs) -> Result<(), String> {
    init_logging(args.log_format, args.log_files());
    run_until(args, stopped(), |_| {}).await
}

/// Runs the broker until `stop` completes; `ready` is told the address it listens on, once it does.
pub(crate) async fn run_until(
    args: ServeArgs,
    stop: impl Future<Output = ()>,
    ready: impl FnOnce(SocketAddr),
) -> Result<(), String> {
    let config = broker_config(&args)?;
    let store_config = FileStoreConfig {
        segment_bytes: args.segment_mib * MIB,
        fsync: match args.fsync {
            Fsync::Always => FsyncPolicy::Always,
            Fsync::Never => FsyncPolicy::Never,
        },
    };
    let mut replica = None;
    let store: Arc<dyn Store> = if args.cluster.replicated() {
        if args.memory {
            return Err(
                "a broker of a cluster keeps its log on disk: --memory and --brokers do not go together"
                    .to_owned(),
            );
        }
        // The replicated log is flushed before anything is acknowledged, whatever --fsync says.
        let kafka = format!("{}:{}", config.advertised_host, config.advertised_port);
        let r = crate::cluster::start(
            &args.cluster,
            args.node_id,
            &args.cluster_id,
            &kafka,
            &args.data_dir,
            store_config,
        )
        .await?;
        replica = Some(Arc::clone(&r));
        r
    } else if args.memory {
        tracing::warn!("running in memory: nothing is kept when the broker stops");
        Arc::new(MemoryStore::new())
    } else {
        let dir = args.data_dir.clone();
        let opened = tokio::task::spawn_blocking(move || FileStore::open(dir, store_config))
            .await
            .map_err(|e| format!("cannot open the data directory: {e}"))?;
        let store = opened.map_err(|e| format!("cannot open the log: {e}"))?;
        tracing::info!(data_dir = %args.data_dir.display(), fsync = ?args.fsync, "log opened");
        Arc::new(store)
    };
    let broker = Broker::start(config, store).await.map_err(|e| e.to_string())?;
    ready(broker.local_addr());
    stop.await;
    tracing::info!("stop requested");
    broker.shutdown().await;
    if let Some(replica) = replica {
        replica.shutdown().await;
    }
    tracing::info!("OpenDuct stopped");
    Ok(())
}

fn broker_config(args: &ServeArgs) -> Result<BrokerConfig, String> {
    let mut config = BrokerConfig::new(args.listen);
    match &args.advertise {
        Some(advertise) => {
            let (host, port) = split_host_port(advertise)?;
            config.advertised_host = host;
            config.advertised_port = port;
        }
        None if args.listen.ip().is_unspecified() => {
            return Err(format!(
                "listening on {} needs --advertise host:port: clients are told where to reconnect, and {} is not \
                 an address they can reach",
                args.listen,
                args.listen.ip()
            ));
        }
        None => {}
    }
    config.node_id = args.node_id;
    config.cluster_id = args.cluster_id.clone();
    config.max_request_bytes = usize::try_from(args.max_request_mib * MIB).map_err(|e| e.to_string())?;
    config.max_request_memory =
        usize::try_from(args.max_request_memory_mib * MIB).map_err(|e| e.to_string())?;
    config.request_read_timeout = Duration::from_secs(args.request_read_timeout_secs);
    config.max_fetch_bytes = usize::try_from(args.max_fetch_mib * MIB).map_err(|e| e.to_string())?;
    config.max_connections = usize::try_from(args.max_connections).map_err(|e| e.to_string())?;
    config.idle_timeout = Duration::from_secs(args.idle_timeout_secs);
    config.auto_create_topics = args.auto_create_topics;
    config.default_partitions = args.default_partitions;
    config.group_initial_rebalance_delay = Duration::from_millis(args.group_initial_rebalance_delay_ms);
    config.retention_check_interval = Duration::from_secs(args.retention_check_secs);
    config.security = args.security.build()?;
    Ok(config)
}

/// `host:port`, `[v6]:port` included.
fn split_host_port(text: &str) -> Result<(String, i32), String> {
    let bad = || format!("--advertise wants host:port, not {text:?}");
    let (host, port) = text.rsplit_once(':').ok_or_else(bad)?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let port: u16 = port.parse().map_err(|_| bad())?;
    if host.is_empty() || port == 0 {
        return Err(bad());
    }
    Ok((host.to_owned(), i32::from(port)))
}

/// The log: to stderr (text or JSON), and into `files` as OpenTelemetry records when there are any.
pub(crate) fn init_logging(format: LogFormat, files: Option<crate::otel_log::LogFiles>) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stderr = match format {
        LogFormat::Text => tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_writer(std::io::stderr)
            .boxed(),
    };
    let file = files.as_ref().and_then(crate::otel_log::OtlpFile::start);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(stderr)
        .with(file)
        .try_init();
}

/// Resolves on Ctrl-C, or on SIGTERM where there is one (what container runtimes and service managers send).
pub(crate) async fn stopped() {
    let interrupt = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "cannot watch for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot watch for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::split_host_port;

    #[test]
    fn advertised_addresses_are_host_and_port() {
        assert_eq!(
            split_host_port("broker.local:9092"),
            Ok(("broker.local".to_owned(), 9092))
        );
        assert_eq!(split_host_port("[::1]:19092"), Ok(("::1".to_owned(), 19092)));
        assert!(split_host_port("broker.local").is_err());
        assert!(split_host_port(":9092").is_err());
        assert!(split_host_port("host:0").is_err());
        assert!(split_host_port("host:99999").is_err());
    }
}
