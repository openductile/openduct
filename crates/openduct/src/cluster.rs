// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct serve` as one broker of a cluster: the brokers, where this one listens for the others, and the secret
//! they share.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use openduct_raft::{
    Broker, ClusterAuth, Replica, ReplicaConfig, ReplicationTls, S3, S3Settings, TierConfig,
};
use openduct_storage::FileStoreConfig;

#[derive(Debug, Args)]
pub(crate) struct ClusterArgs {
    /// Every broker of the cluster, this one included, comma-separated: `id=replication-host:port@client-host:port`.
    /// Set, the broker keeps its log replicated with the others: a write is acknowledged once most brokers hold it,
    /// and the cluster carries on while most brokers are up. --node-id names this broker among them. The broker with
    /// the lowest id starts a new cluster.
    #[arg(long, env = "OPENDUCT_BROKERS", value_delimiter = ',')]
    brokers: Vec<String>,
    /// Where this broker listens for the other brokers. Keep it on a private network: brokers prove they belong
    /// with the cluster secret, which travels as written.
    #[arg(long, env = "OPENDUCT_REPLICATION_LISTEN", default_value = "127.0.0.1:9093")]
    replication_listen: SocketAddr,
    /// A file holding the secret every broker of the cluster shares: at least 16 characters; needed when there is more
    /// than one broker. Never given in the environment.
    #[arg(long, env = "OPENDUCT_CLUSTER_SECRET_FILE")]
    cluster_secret_file: Option<PathBuf>,
    /// How many brokers hold each partition of a new topic (at most every broker). Each partition is led by one of
    /// them, and leaders spread over the brokers.
    #[arg(long, env = "OPENDUCT_REPLICATION_FACTOR", default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..=7))]
    replication_factor: u64,
    /// For recovery, by a person's decision: where most copies of a partition were lost, take the copies that kept
    /// their log as they are. Give the recovery a name of your choosing (a date will do) and start every broker
    /// with it. A broker that lost its copy sets it aside and is caught up from the others by itself; it votes for
    /// no one until it is, so with most copies lost a partition waits. With this, the brokers being caught up
    /// vote, and a copy that kept its log leads. A write only the lost copies held is gone. The name holds for one
    /// recovery: once every partition has a leader it is written down as done and does nothing from then on, so
    /// it may stay in the settings - a later loss needs a new name.
    #[arg(long, env = "OPENDUCT_ACCEPT_SURVIVING_COPIES", value_name = "NAME")]
    accept_surviving_copies: Option<String>,
    /// For recovery, by a person's decision, when NO copy of a partition kept its log: start that partition again
    /// empty. The stronger of the two recovery words, and it includes the one above. Give it a name and start
    /// every broker with it. Where a copy that kept its log can be reached, it leads within moments and the others
    /// are caught up from it, as above. A partition that has had no leader for thirty seconds is started again
    /// from nothing by the copies being rebuilt: what it held is gone. So is what a copy that kept its log but
    /// could not be reached in that time alone held - have every broker up before giving this. What was on each
    /// disk stays in its set-aside directory. The name holds for one recovery, as above.
    #[arg(
        long,
        env = "OPENDUCT_START_EMPTY_WHERE_NO_COPY_SURVIVED",
        value_name = "NAME"
    )]
    start_empty_where_no_copy_survived: Option<String>,
    /// TLS between brokers: this broker's certificate (PEM, its chain leaf first). With --replication-tls-key and
    /// --replication-tls-ca, every connection between brokers is encrypted and both ends must show a certificate
    /// the cluster's authority signed, naming their replication host.
    #[arg(long, env = "OPENDUCT_REPLICATION_TLS_CERT", requires_all = ["replication_tls_key", "replication_tls_ca"])]
    replication_tls_cert: Option<PathBuf>,
    /// TLS between brokers: this broker's private key (PEM).
    #[arg(long, env = "OPENDUCT_REPLICATION_TLS_KEY", requires = "replication_tls_cert")]
    replication_tls_key: Option<PathBuf>,
    /// TLS between brokers: the certificate of the authority that signs every broker's (PEM).
    #[arg(long, env = "OPENDUCT_REPLICATION_TLS_CA", requires = "replication_tls_cert")]
    replication_tls_ca: Option<PathBuf>,
    /// Tiered storage: the S3 bucket closed segments are copied to. Set, each partition's leader stores its closed
    /// segments there and the local disk keeps only the recent ones (--tier-local-retention-mib); reads of older
    /// offsets come back from the bucket. Its keys from --tier-s3-access-key-file and --tier-s3-secret-key-file, or
    /// else the role it runs with
    /// (EKS web identity, the ECS or EKS container endpoint, the EC2 instance). Every broker must use the same
    /// --segment-mib.
    #[arg(long, env = "OPENDUCT_TIER_S3_BUCKET")]
    tier_s3_bucket: Option<String>,
    /// Tiered storage: make the bucket at start when it is not there yet - for an S3-compatible store of your own
    /// (a bucket that is there already is left as it is). On AWS, make the bucket beforehand, with its own settings.
    #[arg(long, env = "OPENDUCT_TIER_S3_CREATE_BUCKET", requires = "tier_s3_bucket")]
    tier_s3_create_bucket: bool,
    /// Tiered storage: a file holding the bucket's access key id - for a store of your own, or AWS without a role.
    /// Never given in the environment.
    #[arg(long, env = "OPENDUCT_TIER_S3_ACCESS_KEY_FILE", requires_all = ["tier_s3_bucket", "tier_s3_secret_key_file"])]
    tier_s3_access_key_file: Option<PathBuf>,
    /// Tiered storage: a file holding the bucket's secret access key, with --tier-s3-access-key-file.
    #[arg(
        long,
        env = "OPENDUCT_TIER_S3_SECRET_KEY_FILE",
        requires = "tier_s3_access_key_file"
    )]
    tier_s3_secret_key_file: Option<PathBuf>,
    /// Tiered storage: the bucket's region.
    #[arg(long, env = "OPENDUCT_TIER_S3_REGION", default_value = "us-east-1")]
    tier_s3_region: String,
    /// Tiered storage: an S3-compatible store's address (MinIO and others); none for AWS itself.
    #[arg(long, env = "OPENDUCT_TIER_S3_ENDPOINT")]
    tier_s3_endpoint: Option<String>,
    /// Tiered storage: what object keys start with; by default `openduct/<cluster id>`.
    #[arg(long, env = "OPENDUCT_TIER_PREFIX")]
    tier_prefix: Option<String>,
    /// Tiered storage: local copies of stored segments each partition keeps, in MiB.
    #[arg(long, env = "OPENDUCT_TIER_LOCAL_RETENTION_MIB", default_value_t = 1024)]
    tier_local_retention_mib: u64,
    /// Tiered storage: local copies of stored segments are dropped once their newest record is this many hours old.
    #[arg(long, env = "OPENDUCT_TIER_LOCAL_RETENTION_HOURS")]
    tier_local_retention_hours: Option<u64>,
    /// Tiered storage: how often each leader looks for segments to store, in seconds.
    #[arg(long, env = "OPENDUCT_TIER_EVERY_SECS", default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    tier_every_secs: u64,
}

impl ClusterArgs {
    pub(crate) fn replicated(&self) -> bool {
        !self.brokers.is_empty()
    }

    /// The secret the brokers share, when this broker is one of a cluster and has one: the studio's keys are kept
    /// in the cluster under it, so every node uses the same ones.
    pub(crate) fn shared_secret(&self) -> Result<Option<String>, String> {
        if !self.replicated() {
            return Ok(None);
        }
        let secret = secret(self.cluster_secret_file.as_deref())?;
        Ok((secret.chars().count() >= 16).then_some(secret))
    }
}

/// `id=replication-host:port@client-host:port`, one per broker.
fn parse_brokers(entries: &[String]) -> Result<BTreeMap<u64, Broker>, String> {
    let mut out = BTreeMap::new();
    for entry in entries.iter().map(|e| e.trim()).filter(|e| !e.is_empty()) {
        let bad = || format!("--brokers wants id=replication-host:port@client-host:port, not {entry:?}");
        let (id, addresses) = entry.split_once('=').ok_or_else(bad)?;
        let (raft, kafka) = addresses.split_once('@').ok_or_else(bad)?;
        let id: u64 = id.trim().parse().map_err(|_| bad())?;
        if id == 0 || i32::try_from(id).is_err() {
            return Err(format!("broker ids are 1 to 2147483647, not {id}"));
        }
        for address in [raft, kafka] {
            let port = address.rsplit_once(':').and_then(|(_, p)| p.parse::<u16>().ok());
            if !port.is_some_and(|p| p != 0) {
                return Err(bad());
            }
        }
        let broker = Broker {
            raft: raft.to_owned(),
            kafka: kafka.to_owned(),
        };
        if out.insert(id, broker).is_some() {
            return Err(format!("broker {id} is listed twice in --brokers"));
        }
    }
    Ok(out)
}

/// A key of the bucket's, from its file (trimmed).
fn key_file(path: &Path) -> Result<String, String> {
    let key = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read the tiered storage key file {}: {e}", path.display()))?;
    let key = key.trim().to_owned();
    if key.is_empty() {
        return Err(format!(
            "{} is empty: it holds one of the bucket's keys",
            path.display()
        ));
    }
    Ok(key)
}

/// RETIRE after 0.1: a secret once read from the environment, still set there, stops OpenDuct with where it goes now -
/// rather than run without it (the studio's keys would change, and its credentials could not be opened). Only whether
/// the variable is set is looked at, never its value.
pub(crate) fn refuse_old(variable: &str, instead: &str) -> Result<(), String> {
    if std::env::var_os(variable).is_some_and(|v| !v.is_empty()) {
        return Err(format!(
            "{variable} is set, but secrets are no longer read from the environment: put it in a file only OpenDuct's \
             user may read and give {instead}, then unset {variable}"
        ));
    }
    Ok(())
}

fn secret(file: Option<&Path>) -> Result<String, String> {
    if file.is_none() {
        refuse_old("OPENDUCT_CLUSTER_SECRET", "--cluster-secret-file")?;
    }
    let secret = match file {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read the cluster secret file {}: {e}", path.display()))?,
        None => String::new(),
    };
    Ok(secret.trim().to_owned())
}

/// Opens this broker's replicated store. `kafka` is the address the broker tells clients (its own entry in
/// --brokers must name the same one).
pub(crate) async fn start(
    args: &ClusterArgs,
    node_id: i32,
    cluster: &str,
    kafka: &str,
    data_dir: &Path,
    store: FileStoreConfig,
) -> Result<Arc<Replica>, String> {
    let brokers = parse_brokers(&args.brokers)?;
    let id = u64::try_from(node_id).map_err(|_| format!("--node-id {node_id} is not a broker id"))?;
    let me = brokers
        .get(&id)
        .ok_or_else(|| format!("--node-id {node_id} is not among --brokers"))?;
    if me.kafka != kafka {
        return Err(format!(
            "--brokers gives broker {id} the client address {}, but it tells clients {kafka} (--advertise)",
            me.kafka
        ));
    }
    let secret = secret(args.cluster_secret_file.as_deref())?;
    if brokers.len() > 1 && secret.chars().count() < 16 {
        return Err(
            "brokers of a cluster share a secret of at least 16 characters: give its file with --cluster-secret-file"
                .to_owned(),
        );
    }
    let tls = match (
        &args.replication_tls_cert,
        &args.replication_tls_key,
        &args.replication_tls_ca,
    ) {
        (Some(cert), Some(key), Some(ca)) => Some(ReplicationTls::from_pem_files(cert, key, ca)?),
        _ => {
            tracing::warn!("replication without TLS: keep the replication port on a private network");
            None
        }
    };
    let mut config = ReplicaConfig::new(
        id,
        data_dir,
        args.replication_listen,
        brokers,
        ClusterAuth {
            cluster: cluster.to_owned(),
            secret,
            tls,
        },
    );
    config.store = store;
    if let Some(bucket) = &args.tier_s3_bucket {
        if args.tier_s3_access_key_file.is_none() {
            refuse_old("AWS_ACCESS_KEY_ID", "--tier-s3-access-key-file")?;
            refuse_old("AWS_SECRET_ACCESS_KEY", "--tier-s3-secret-key-file")?;
        }
        let s3 = S3::new(S3Settings {
            bucket: bucket.clone(),
            region: args.tier_s3_region.clone(),
            endpoint: args.tier_s3_endpoint.clone(),
            keys: match (&args.tier_s3_access_key_file, &args.tier_s3_secret_key_file) {
                (Some(access), Some(secret)) => Some((key_file(access)?, key_file(secret)?)),
                _ => None,
            },
        })?;
        if args.tier_s3_create_bucket {
            s3.create_bucket()
                .await
                .map_err(|e| format!("cannot make the bucket {bucket}: {e}"))?;
        }
        config.tier = Some(TierConfig {
            s3: Arc::new(s3),
            prefix: args
                .tier_prefix
                .clone()
                .unwrap_or_else(|| format!("openduct/{cluster}")),
            local_retention_bytes: args.tier_local_retention_mib.saturating_mul(1024 * 1024),
            local_retention_ms: args
                .tier_local_retention_hours
                .map(|h| i64::try_from(h.saturating_mul(3_600_000)).unwrap_or(i64::MAX)),
            every: std::time::Duration::from_secs(args.tier_every_secs),
        });
        tracing::info!(bucket, "tiered storage on: closed segments go to object storage");
    }
    config.replication_factor = usize::try_from(args.replication_factor).unwrap_or(3);
    config.accept_surviving_copies = args.accept_surviving_copies.clone();
    config.start_empty_where_none_survived = args.start_empty_where_no_copy_survived.clone();
    let replica = Replica::start(config)
        .await
        .map_err(|e| format!("cannot start replication: {e}"))?;
    tracing::info!(
        node_id,
        replication = %args.replication_listen,
        "replicated with the other brokers"
    );
    Ok(Arc::new(replica))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brokers_are_listed_with_both_their_addresses() {
        let parsed = parse_brokers(&[
            "1=10.0.0.1:9093@b1.internal:9092".to_owned(),
            " 2=10.0.0.2:9093@b2.internal:9092".to_owned(),
        ])
        .unwrap();
        assert_eq!(parsed[&2].raft, "10.0.0.2:9093");
        assert_eq!(parsed[&2].kafka, "b2.internal:9092");
        for bad in [
            "1=10.0.0.1:9093",
            "x=a:1@b:2",
            "0=a:1@b:2",
            "1=a@b:2",
            "1=a:1@b:0",
        ] {
            assert!(parse_brokers(&[bad.to_owned()]).is_err(), "{bad}");
        }
        assert!(parse_brokers(&["1=a:1@b:2".to_owned(), "1=c:1@d:2".to_owned()]).is_err());
    }
}
