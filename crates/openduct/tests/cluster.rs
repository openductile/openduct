// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Three `openduct serve` processes as one cluster, used by an independent Kafka client (rskafka, which OpenDuct did
//! not write): it finds each partition's leader on its own; partition leaders are spread over the brokers; the
//! process leading the partition being written is killed mid-stream, and the client carries on with the broker
//! elected in its place; the killed broker comes back and holds its share again.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kafka_protocol::messages::MetadataRequest;
use openduct_broker::client::Client;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::record::Record;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");
const SECRET: &str = "a-cluster-secret-for-tests";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Cluster {
    root: tempfile::TempDir,
    kafka: BTreeMap<u64, u16>,
    replication: BTreeMap<u64, u16>,
    processes: BTreeMap<u64, Child>,
    /// Certificate files when the brokers speak TLS to each other: the authority's, and per broker its own.
    tls: Option<std::path::PathBuf>,
    /// Settings for every broker's environment (tiered storage, say).
    env: Vec<(String, String)>,
}

impl Cluster {
    fn new() -> Self {
        let ids = [1u64, 2, 3];
        Self {
            root: tempfile::tempdir().unwrap(),
            kafka: ids.iter().map(|i| (*i, free_port())).collect(),
            replication: ids.iter().map(|i| (*i, free_port())).collect(),
            processes: BTreeMap::new(),
            tls: None,
            env: Vec::new(),
        }
    }

    fn brokers(&self) -> String {
        self.kafka
            .keys()
            .map(|id| {
                format!(
                    "{id}=127.0.0.1:{}@127.0.0.1:{}",
                    self.replication[id], self.kafka[id]
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn bootstrap(&self) -> Vec<String> {
        self.kafka.values().map(|p| format!("127.0.0.1:{p}")).collect()
    }

    /// Writes an authority and a certificate per broker (for 127.0.0.1, usable both ways) into the cluster's
    /// directory; brokers started from here on speak TLS to each other.
    fn use_tls(&mut self) {
        use rcgen::{
            BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        };
        let dir = self.root.path().join("tls");
        std::fs::create_dir_all(&dir).unwrap();
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().unwrap()).unwrap();
        std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
        for id in self.kafka.keys() {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let cert = params.signed_by(&key, &*ca).unwrap();
            std::fs::write(dir.join(format!("{id}.pem")), cert.pem()).unwrap();
            std::fs::write(dir.join(format!("{id}.key")), key.serialize_pem()).unwrap();
        }
        self.tls = Some(dir);
    }

    /// The cluster secret, in a file: never in the environment.
    fn secret_file(&self) -> std::path::PathBuf {
        let path = self.root.path().join("cluster-secret");
        if !path.exists() {
            std::fs::write(&path, SECRET).unwrap();
        }
        path
    }

    fn start(&mut self, id: u64) {
        let mut command = Command::new(OPENDUCT);
        command.envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        if let Some(dir) = &self.tls {
            command
                .env("OPENDUCT_REPLICATION_TLS_CERT", dir.join(format!("{id}.pem")))
                .env("OPENDUCT_REPLICATION_TLS_KEY", dir.join(format!("{id}.key")))
                .env("OPENDUCT_REPLICATION_TLS_CA", dir.join("ca.pem"));
        }
        command
            .args(["serve", "--listen"])
            .arg(format!("127.0.0.1:{}", self.kafka[&id]))
            .arg("--data-dir")
            .arg(self.root.path().join(format!("broker-{id}")))
            .args(["--node-id", &id.to_string(), "--cluster-id", "test-cluster"])
            .args(["--brokers", &self.brokers()])
            .arg("--replication-listen")
            .arg(format!("127.0.0.1:{}", self.replication[&id]))
            .arg("--cluster-secret-file")
            .arg(self.secret_file())
            .stdout(Stdio::null());
        // OPENDUCT_TEST_LOG_DIR keeps each broker's log (at info) for a failing run.
        match std::env::var("OPENDUCT_TEST_LOG_DIR") {
            Ok(dir) => {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(std::path::Path::new(&dir).join(format!(
                        "{}-broker-{id}.log",
                        self.root.path().file_name().and_then(|n| n.to_str()).unwrap_or("cluster")
                    )))
                    .unwrap();
                command.env("RUST_LOG", "info").stderr(file);
            }
            Err(_) => {
                command.env("RUST_LOG", "warn").stderr(Stdio::null());
            }
        }
        let child = command.spawn().unwrap();
        self.processes.insert(id, child);
    }

    /// A broker new to the cluster: given ports, then started with every broker listed - it joins.
    fn add(&mut self, id: u64) {
        self.kafka.insert(id, free_port());
        self.replication.insert(id, free_port());
        self.start(id);
    }

    fn kill(&mut self, id: u64) {
        if let Some(mut child) = self.processes.remove(&id) {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }

    /// The broker every running broker names as the leader - one that is running: until their election timeout,
    /// the others still name a leader that has just died.
    /// The leaders of `topic`'s partitions every running broker names - all of them running.
    async fn partition_leaders(&self, topic: &str, partitions: usize) -> Vec<u64> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut seen = Vec::new();
            for id in self.processes.keys() {
                seen.push(partition_leaders(self.kafka[id], topic).await);
            }
            if let Some(Some(first)) = seen.first()
                && first.len() == partitions
                && seen.iter().all(|s| s.as_ref() == Some(first))
                && first
                    .iter()
                    .all(|l| u64::try_from(*l).is_ok_and(|l| self.processes.contains_key(&l)))
            {
                return first.iter().map(|l| u64::try_from(*l).unwrap()).collect();
            }
            assert!(
                Instant::now() < deadline,
                "the brokers never agreed on {topic}'s leaders: {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn leader(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut seen = Vec::new();
            for id in self.processes.keys() {
                seen.push(controller(self.kafka[id]).await);
            }
            if let Some(Some(l)) = seen.first()
                && *l > 0
                && seen.iter().all(|s| *s == Some(*l))
                && self.processes.contains_key(&u64::try_from(*l).unwrap())
            {
                return u64::try_from(*l).unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "the brokers never agreed on a leader: {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.processes.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// The coordinator as one broker's metadata names it.
async fn controller(port: u16) -> Option<i32> {
    let mut c = Client::connect(&format!("127.0.0.1:{port}"), Duration::from_secs(1))
        .await
        .ok()?;
    let res = c.send(&MetadataRequest::default(), 9).await.ok()?;
    Some(res.controller_id.0)
}

/// Each partition's leader of `topic`, as one broker's metadata names them.
async fn partition_leaders(port: u16, topic: &str) -> Option<Vec<i32>> {
    let mut c = Client::connect(&format!("127.0.0.1:{port}"), Duration::from_secs(1))
        .await
        .ok()?;
    // A null topic list asks for every topic (an empty one, for none).
    let res = c
        .send(&MetadataRequest::default().with_topics(None), 9)
        .await
        .ok()?;
    let t = res
        .topics
        .iter()
        .find(|t| t.name.as_ref().is_some_and(|n| n.0.as_str() == topic))?;
    let mut parts: Vec<(i32, i32)> = t
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.leader_id.0))
        .collect();
    parts.sort_unstable();
    Some(parts.into_iter().map(|(_, l)| l).collect())
}

fn record(value: String) -> Record {
    Record {
        key: None,
        value: Some(value.into_bytes()),
        headers: Default::default(),
        timestamp: chrono::Utc::now(),
    }
}

async fn partition(bootstrap: Vec<String>) -> PartitionClient {
    let client = ClientBuilder::new(bootstrap).build().await.unwrap();
    client
        .partition_client("events", 0, UnknownTopicHandling::Retry)
        .await
        .unwrap()
}

/// Produces `values`, one request each, until every one is acknowledged.
async fn produce(p: &PartitionClient, values: impl Iterator<Item = String>) {
    for v in values {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match p
                .produce(vec![record(v.clone())], Compression::NoCompression)
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    assert!(Instant::now() < deadline, "{v} was never acknowledged: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn read_all(p: &PartitionClient) -> Vec<String> {
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        let (records, high_watermark) = p.fetch_records(offset, 1..1_000_000, 500).await.unwrap();
        for r in records {
            out.push(String::from_utf8(r.record.value.unwrap()).unwrap());
            offset = r.offset + 1;
        }
        if offset >= high_watermark {
            return out;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kafka_client_carries_on_when_the_leader_s_process_is_killed() {
    let mut cluster = Cluster::new();
    for id in [1, 2, 3] {
        cluster.start(id);
    }
    cluster.leader().await;

    let client = ClientBuilder::new(cluster.bootstrap()).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("events", 3, 3, 5_000)
        .await
        .unwrap();
    // Each partition is led by another broker - once the first elections' winners have handed back to each
    // partition's first broker (every second; a loaded runner takes a few).
    let deadline = Instant::now() + Duration::from_secs(30);
    let leaders = loop {
        let leaders = cluster.partition_leaders("events", 3).await;
        let distinct: std::collections::BTreeSet<u64> = leaders.iter().copied().collect();
        if distinct.len() == 3 {
            break leaders;
        }
        assert!(Instant::now() < deadline, "partition leaders {leaders:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let first = leaders[0];
    let p = partition(cluster.bootstrap()).await;
    produce(&p, (0..20).map(|i| format!("before-{i}"))).await;

    // The process leading partition 0 dies; the client finds its new leader and goes on, nothing lost or reordered.
    cluster.kill(first);
    let second = cluster.partition_leaders("events", 3).await[0];
    assert_ne!(second, first);
    produce(&p, (0..10).map(|i| format!("after-{i}"))).await;
    let expected: Vec<String> = (0..20)
        .map(|i| format!("before-{i}"))
        .chain((0..10).map(|i| format!("after-{i}")))
        .collect();
    assert_eq!(read_all(&p).await, expected);

    // The killed broker comes back and catches up: with it, the cluster survives losing another broker.
    cluster.start(first);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let other = *cluster
        .processes
        .keys()
        .find(|id| **id != first && **id != second)
        .unwrap();
    cluster.kill(other);
    produce(&p, (0..5).map(|i| format!("later-{i}"))).await;
    let p = partition(cluster.bootstrap()).await;
    let all = read_all(&p).await;
    assert_eq!(all.len(), 35);
    assert_eq!(all.last().map(String::as_str), Some("later-4"));
}

/// `openduct <args>` against one broker, off the async threads: its standard output, or its standard error.
async fn cli(port: u16, args: &[&str]) -> Result<String, String> {
    let mut all: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
    all.extend(["--bootstrap".to_owned(), format!("127.0.0.1:{port}")]);
    tokio::task::spawn_blocking(move || {
        let out = Command::new(OPENDUCT).args(&all).output().unwrap();
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).into_owned())
        }
    })
    .await
    .unwrap()
}

async fn broker_count(port: u16) -> usize {
    let Ok(mut c) = Client::connect(&format!("127.0.0.1:{port}"), Duration::from_secs(1)).await else {
        return 0;
    };
    c.send(&MetadataRequest::default(), 9)
        .await
        .map_or(0, |res| res.brokers.len())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fourth_broker_joins_takes_its_share_and_another_one_leaves() {
    let mut cluster = Cluster::new();
    for id in [1, 2, 3] {
        cluster.start(id);
    }
    cluster.leader().await;
    let client = ClientBuilder::new(cluster.bootstrap()).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("events", 6, 3, 5_000)
        .await
        .unwrap();
    let p = partition(cluster.bootstrap()).await;
    produce(&p, (0..20).map(|i| format!("m-{i}"))).await;

    // Broker 4 starts with every broker listed: it joins, and every broker's metadata lists it.
    cluster.add(4);
    let deadline = Instant::now() + Duration::from_secs(30);
    while broker_count(cluster.kafka[&2]).await != 4 {
        assert!(Instant::now() < deadline, "broker 4 never joined");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // A rebalance gives it its share of the copies and of the leaders.
    let out = cli(cluster.kafka[&2], &["cluster", "rebalance"]).await.unwrap();
    assert!(out.contains("moved"), "{out}");
    let status = cli(cluster.kafka[&3], &["cluster", "status"]).await.unwrap();
    let line = status
        .lines()
        .find(|l| l.starts_with("broker 4 "))
        .unwrap()
        .to_owned();
    assert!(!line.contains("holds 0 partitions"), "{status}");

    // Broker 1 leaves: its partitions move to the others first; then it can be stopped.
    let out = cli(cluster.kafka[&2], &["cluster", "remove-broker", "1"])
        .await
        .unwrap();
    assert!(out.contains("broker 1 left the cluster"), "{out}");
    cluster.kill(1);
    let rest: Vec<String> = [2u64, 3, 4]
        .iter()
        .map(|id| format!("127.0.0.1:{}", cluster.kafka[id]))
        .collect();
    let p = partition(rest).await;
    produce(&p, (20..25).map(|i| format!("m-{i}"))).await;
    let expected: Vec<String> = (0..25).map(|i| format!("m-{i}")).collect();
    assert_eq!(read_all(&p).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brokers_that_speak_tls_to_each_other_take_writes_and_survive_a_kill() {
    let mut cluster = Cluster::new();
    cluster.use_tls();
    for id in [1, 2, 3] {
        cluster.start(id);
    }
    cluster.leader().await;
    let client = ClientBuilder::new(cluster.bootstrap()).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("events", 3, 3, 5_000)
        .await
        .unwrap();
    let first = cluster.partition_leaders("events", 3).await[0];
    let p = partition(cluster.bootstrap()).await;
    produce(&p, (0..10).map(|i| format!("m-{i}"))).await;
    cluster.kill(first);
    produce(&p, (10..15).map(|i| format!("m-{i}"))).await;
    let expected: Vec<String> = (0..15).map(|i| format!("m-{i}")).collect();
    assert_eq!(read_all(&p).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_records_come_back_from_object_storage_once_the_disk_has_let_them_go() {
    let (Ok(endpoint), Ok(access), Ok(secret)) = (
        std::env::var("OPENDUCT_TEST_S3"),
        std::env::var("OPENDUCT_TEST_S3_ACCESS"),
        std::env::var("OPENDUCT_TEST_S3_SECRET"),
    ) else {
        eprintln!(
            "skipped: set OPENDUCT_TEST_S3, OPENDUCT_TEST_S3_ACCESS and OPENDUCT_TEST_S3_SECRET (cargo xtask services up)"
        );
        return;
    };
    let bucket = format!("openduct-serve-tier-{}", std::process::id());
    openduct_raft::S3::new(openduct_raft::S3Settings {
        bucket: bucket.clone(),
        region: "us-east-1".to_owned(),
        endpoint: Some(endpoint.clone()),
        keys: Some((access.clone(), secret.clone())),
    })
    .unwrap()
    .create_bucket()
    .await
    .unwrap();
    let mut cluster = Cluster::new();
    // The bucket's keys, each in a file: never in the environment.
    for (flag, name, key) in [
        ("OPENDUCT_TIER_S3_ACCESS_KEY_FILE", "s3-access-key", &access),
        ("OPENDUCT_TIER_S3_SECRET_KEY_FILE", "s3-secret-key", &secret),
    ] {
        let path = cluster.root.path().join(name);
        std::fs::write(&path, key).unwrap();
        cluster
            .env
            .push((flag.to_owned(), path.to_str().unwrap().to_owned()));
    }
    for (k, v) in [
        ("OPENDUCT_TIER_S3_BUCKET", bucket.as_str()),
        ("OPENDUCT_TIER_S3_ENDPOINT", endpoint.as_str()),
        ("OPENDUCT_TIER_LOCAL_RETENTION_MIB", "0"),
        ("OPENDUCT_TIER_EVERY_SECS", "1"),
        ("OPENDUCT_SEGMENT_MIB", "1"),
    ] {
        cluster.env.push((k.to_owned(), v.to_owned()));
    }
    for id in [1, 2, 3] {
        cluster.start(id);
    }
    cluster.leader().await;
    let client = ClientBuilder::new(cluster.bootstrap()).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("events", 1, 3, 5_000)
        .await
        .unwrap();
    let p = partition(cluster.bootstrap()).await;
    // About 3 MB in 1 KiB records: three closed 1 MiB segments, stored, then dropped from every disk.
    let pad = "x".repeat(1000);
    for chunk in 0..30 {
        let records: Vec<Record> = (0..100)
            .map(|i| record(format!("{:05}{pad}", chunk * 100 + i)))
            .collect();
        p.produce(records, Compression::NoCompression).await.unwrap();
    }
    let leader = cluster.partition_leaders("events", 1).await[0];
    let data = cluster
        .root
        .path()
        .join(format!("broker-{leader}"))
        .join("partitions");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let local: u64 = walk(&data)
            .iter()
            .filter(|p| p.extension().is_some_and(|e| e == "log"))
            .filter_map(|p| p.metadata().ok())
            .map(|m| m.len())
            .sum();
        if local < 2 * 1024 * 1024 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the leader still holds {local} bytes of segments"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let all = read_all(&p).await;
    assert_eq!(all.len(), 3000);
    assert!(
        all.iter()
            .enumerate()
            .all(|(i, v)| v.starts_with(&format!("{i:05}")))
    );
}

/// Every file under `dir`.
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}
