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
    /// The port each broker serves the schema registry on, for the brokers that do.
    registry: BTreeMap<u64, u16>,
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
            registry: BTreeMap::new(),
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
        if let Some(port) = self.registry.get(&id) {
            command.env("OPENDUCT_REGISTRY_LISTEN", format!("127.0.0.1:{port}"));
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

/// One request to a broker's schema registry: the status and the body, or `None` while it does not answer.
async fn registry(
    port: u16,
    method: &str,
    target: &str,
    body: Option<serde_json::Value>,
) -> Option<(u16, serde_json::Value)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let asked = async {
        let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.ok()?;
        socket.write_all(request.as_bytes()).await.ok()?;
        let mut answer = Vec::new();
        socket.read_to_end(&mut answer).await.ok()?;
        let answer = String::from_utf8(answer).ok()?;
        let (head, body) = answer.split_once("\r\n\r\n")?;
        let status = head.split(' ').nth(1)?.parse().ok()?;
        Some((
            status,
            serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
        ))
    };
    tokio::time::timeout(Duration::from_secs(40), asked)
        .await
        .ok()
        .flatten()
}

/// Asks until the registry gives this status (it starts once the cluster has formed, and a write waits out an
/// election): the body it gave.
async fn registry_until(
    port: u16,
    method: &str,
    target: &str,
    body: Option<serde_json::Value>,
    want: u16,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let got = registry(port, method, target, body.clone()).await;
        if let Some((status, answer)) = &got
            && *status == want
        {
            return answer.clone();
        }
        assert!(
            Instant::now() < deadline,
            "{method} {target} on port {port}: {got:?}, not {want}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_broker_serves_one_schema_registry_and_it_outlives_the_broker_that_led_its_topic() {
    use serde_json::json;
    let mut cluster = Cluster::new();
    for id in [1, 2, 3] {
        cluster.registry.insert(id, free_port());
        cluster.start(id);
    }
    cluster.leader().await;
    let port = |cluster: &Cluster, id: u64| cluster.registry[&id];
    let order = |fields: &str| json!({ "schema": format!(r#"{{"type":"record","name":"Order","fields":[{fields}]}}"#) });
    let id_only = r#"{"name":"id","type":"long"}"#;
    let with_note = r#"{"name":"id","type":"long"},{"name":"note","type":"string","default":""}"#;

    // Registered through broker 1; read through 2 and 3, which read it from the topic.
    let made = registry_until(
        port(&cluster, 1),
        "POST",
        "/subjects/orders-value/versions",
        Some(order(id_only)),
        200,
    )
    .await;
    assert_eq!(made, json!({ "id": 1 }));
    for id in [2, 3] {
        let seen = registry_until(port(&cluster, id), "GET", "/schemas/ids/1", None, 200).await;
        assert_eq!(seen, order(id_only), "broker {id}");
    }
    // A level set through broker 3 holds on broker 2: a breaking schema is refused there.
    registry_until(
        port(&cluster, 3),
        "PUT",
        "/config/orders-value",
        Some(json!({ "compatibility": "FULL" })),
        200,
    )
    .await;
    registry_until(port(&cluster, 2), "GET", "/config/orders-value", None, 200).await;
    let breaking = order(r#"{"name":"id","type":"long"},{"name":"total","type":"double"}"#);
    let refused = registry(
        port(&cluster, 2),
        "POST",
        "/subjects/orders-value/versions",
        Some(breaking),
    )
    .await;
    assert_eq!(refused.map(|(status, _)| status), Some(409));

    // The process that leads the registry's topic dies. The other two elect a leader among them and go on:
    // a new version is registered through one and read through the other.
    let leader = cluster.partition_leaders("_schemas", 1).await[0];
    cluster.kill(leader);
    let left: Vec<u64> = cluster.processes.keys().copied().collect();
    let made = registry_until(
        port(&cluster, left[0]),
        "POST",
        "/subjects/orders-value/versions",
        Some(order(with_note)),
        200,
    )
    .await;
    assert_eq!(made, json!({ "id": 2 }));
    let versions = registry_until(
        port(&cluster, left[1]),
        "GET",
        "/subjects/orders-value/versions/2",
        None,
        200,
    )
    .await;
    assert_eq!(versions["id"], json!(2));

    // The killed broker comes back, reads the topic from its own copy and what it missed, and serves it all.
    cluster.start(leader);
    let seen = registry_until(
        port(&cluster, leader),
        "GET",
        "/subjects/orders-value/versions/2",
        None,
        200,
    )
    .await;
    assert_eq!(seen["version"], json!(2));
    let level = registry_until(port(&cluster, leader), "GET", "/config/orders-value", None, 200).await;
    assert_eq!(level, json!({ "compatibilityLevel": "FULL" }));
}

/// A transaction's records for one partition: one batch of one value, marked transactional.
fn transactional_batch(producer_id: i64, epoch: i16, sequence: i32, value: &str) -> bytes::Bytes {
    use kafka_protocol::records::{
        Compression as NoCompression, Record as WireRecord, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    };
    let records = [WireRecord {
        transactional: true,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: 0,
        producer_id,
        producer_epoch: epoch,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence,
        timestamp: 1_700_000_000_000,
        key: None,
        value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
        headers: Default::default(),
    }];
    let mut buf = bytes::BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: NoCompression::None,
        },
    )
    .unwrap();
    buf.freeze()
}

/// The transactions of one transactional id, as a client drives them across a cluster's brokers.
struct Transactor<'a> {
    cluster: &'a Cluster,
    id: &'static str,
    producer_id: i64,
    epoch: i16,
}

/// Errors a client answers by looking for the coordinator again and trying once more.
const TRY_AGAIN: [i16; 4] = [14, 15, 16, 51];

impl Transactor<'_> {
    async fn connect(&self, broker: u64) -> Option<Client> {
        Client::connect(
            &format!("127.0.0.1:{}", self.cluster.kafka[&broker]),
            Duration::from_secs(2),
        )
        .await
        .ok()
    }

    /// The broker coordinating the id, as a running broker names it - and it is running.
    async fn coordinator(&self) -> u64 {
        use kafka_protocol::messages::FindCoordinatorRequest;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            for broker in self.cluster.processes.keys() {
                let Some(mut c) = self.connect(*broker).await else {
                    continue;
                };
                let request = FindCoordinatorRequest::default()
                    .with_key(kafka_protocol::protocol::StrBytes::from_static_str(self.id))
                    .with_key_type(1);
                if let Ok(found) = c.send(&request, 2).await
                    && found.error_code == 0
                    && let Ok(node) = u64::try_from(found.node_id.0)
                    && self.cluster.processes.contains_key(&node)
                {
                    return node;
                }
            }
            assert!(Instant::now() < deadline, "no broker coordinates {}", self.id);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// One request to the coordinator, tried again while there is none or it is moving: what it answered.
    async fn ask<R, F>(&self, version: i16, request: &R, code: F) -> R::Response
    where
        R: kafka_protocol::protocol::Request,
        F: Fn(&R::Response) -> i16,
    {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let coordinator = self.coordinator().await;
            if let Some(mut c) = self.connect(coordinator).await
                && let Ok(answer) = c.send(request, version).await
                && !TRY_AGAIN.contains(&code(&answer))
            {
                return answer;
            }
            assert!(
                Instant::now() < deadline,
                "the coordinator of {} never answered",
                self.id
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn init(&mut self) {
        use kafka_protocol::messages::{InitProducerIdRequest, ProducerId, TransactionalId};
        let request = InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(
                kafka_protocol::protocol::StrBytes::from_static_str(self.id),
            )))
            .with_transaction_timeout_ms(60_000)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
        let answer = self.ask(2, &request, |a| a.error_code).await;
        assert_eq!(answer.error_code, 0);
        (self.producer_id, self.epoch) = (answer.producer_id.0, answer.producer_epoch);
    }

    /// Adds the topic's three partitions to the transaction, and writes one value to each at its leader.
    async fn write(&self, topic: &str, sequence: i32, value: &str) {
        use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
        use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
        use kafka_protocol::messages::{
            AddPartitionsToTxnRequest, ProduceRequest, ProducerId, TopicName, TransactionalId,
        };
        use kafka_protocol::protocol::StrBytes;
        let name = TopicName(StrBytes::from_string(topic.to_owned()));
        let id = TransactionalId(StrBytes::from_static_str(self.id));
        let request = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(id.clone())
            .with_v3_and_below_producer_id(ProducerId(self.producer_id))
            .with_v3_and_below_producer_epoch(self.epoch)
            .with_v3_and_below_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(name.clone())
                    .with_partitions(vec![0, 1, 2]),
            ]);
        let added = self
            .ask(1, &request, |a| {
                a.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code
            })
            .await;
        for p in &added.results_by_topic_v3_and_below[0].results_by_partition {
            assert_eq!(p.partition_error_code, 0, "partition {}", p.partition_index);
        }
        let leaders = self.cluster.partition_leaders(topic, 3).await;
        for (partition, leader) in leaders.iter().enumerate() {
            let partition = i32::try_from(partition).unwrap();
            let batch = transactional_batch(
                self.producer_id,
                self.epoch,
                sequence,
                &format!("{value}-{partition}"),
            );
            let request = ProduceRequest::default()
                .with_transactional_id(Some(id.clone()))
                .with_acks(-1)
                .with_timeout_ms(10_000)
                .with_topic_data(vec![
                    TopicProduceData::default()
                        .with_name(name.clone())
                        .with_partition_data(vec![
                            PartitionProduceData::default()
                                .with_index(partition)
                                .with_records(Some(batch)),
                        ]),
                ]);
            let mut c = self.connect(*leader).await.unwrap();
            let answer = c.send(&request, 9).await.unwrap();
            assert_eq!(
                answer.responses[0].partition_responses[0].error_code, 0,
                "partition {partition}"
            );
        }
    }

    async fn end(&self, commit: bool) -> i16 {
        use kafka_protocol::messages::{EndTxnRequest, ProducerId, TransactionalId};
        let request = EndTxnRequest::default()
            .with_transactional_id(TransactionalId(
                kafka_protocol::protocol::StrBytes::from_static_str(self.id),
            ))
            .with_producer_id(ProducerId(self.producer_id))
            .with_producer_epoch(self.epoch)
            .with_committed(commit);
        self.ask(1, &request, |a| a.error_code).await.error_code
    }

    /// What a consumer of committed data reads from each of the topic's three partitions, at its leader.
    async fn committed(&self, topic: &str) -> Vec<Vec<String>> {
        use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
        use kafka_protocol::messages::{BrokerId, FetchRequest, TopicName};
        use kafka_protocol::records::RecordBatchDecoder;
        let leaders = self.cluster.partition_leaders(topic, 3).await;
        let mut out = Vec::new();
        for (partition, leader) in leaders.iter().enumerate() {
            let request = FetchRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_max_wait_ms(0)
                .with_min_bytes(0)
                .with_max_bytes(1 << 20)
                .with_isolation_level(1)
                .with_topics(vec![
                    FetchTopic::default()
                        .with_topic(TopicName(kafka_protocol::protocol::StrBytes::from_string(
                            topic.to_owned(),
                        )))
                        .with_partitions(vec![
                            FetchPartition::default()
                                .with_partition(i32::try_from(partition).unwrap())
                                .with_fetch_offset(0)
                                .with_partition_max_bytes(1 << 20),
                        ]),
                ]);
            let mut c = self.connect(*leader).await.unwrap();
            let answer = c.send(&request, 11).await.unwrap();
            let part = &answer.responses[0].partitions[0];
            assert_eq!(part.error_code, 0);
            let aborted: Vec<(i64, i64)> = part
                .aborted_transactions
                .iter()
                .flatten()
                .map(|t| (t.producer_id.0, t.first_offset))
                .collect();
            let (mut values, mut skipping) = (Vec::new(), false);
            let mut batches = part.records.clone().unwrap_or_default();
            while !batches.is_empty() {
                for r in RecordBatchDecoder::decode(&mut batches).unwrap().records {
                    if aborted.contains(&(r.producer_id, r.offset)) {
                        skipping = true;
                    }
                    if r.control {
                        skipping = false;
                    } else if !skipping {
                        values.push(String::from_utf8(r.value.unwrap().to_vec()).unwrap());
                    }
                }
            }
            out.push(values);
        }
        out
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_spans_three_brokers_and_is_finished_by_another_when_its_coordinator_is_killed() {
    let mut cluster = Cluster::new();
    for id in [1, 2, 3] {
        cluster.start(id);
    }
    cluster.leader().await;
    let client = ClientBuilder::new(cluster.bootstrap()).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("ledger", 3, 3, 5_000)
        .await
        .unwrap();
    drop(client);
    cluster.partition_leaders("ledger", 3).await;

    let coordinator;
    {
        let mut t = Transactor {
            cluster: &cluster,
            id: "bookkeeper",
            producer_id: -1,
            epoch: -1,
        };
        t.init().await;
        assert_eq!(t.epoch, 0);
        // The cluster's transactional ids are shared out over its brokers: not one broker coordinates them all.
        let mut coordinators = std::collections::BTreeSet::new();
        for id in [
            "a", "b", "c", "d", "e", "f", "g", "h", "payments", "orders", "ledger", "billing",
        ] {
            let other = Transactor {
                cluster: &cluster,
                id,
                producer_id: -1,
                epoch: -1,
            };
            coordinators.insert(other.coordinator().await);
        }
        assert!(
            coordinators.len() > 1,
            "every id is coordinated by {coordinators:?}"
        );
        // Written to three partitions; none of it is read until the transaction is committed - then all of it.
        t.write("ledger", 0, "first").await;
        assert_eq!(t.committed("ledger").await, vec![Vec::<String>::new(); 3]);
        assert_eq!(t.end(true).await, 0);
        assert_eq!(
            t.committed("ledger").await,
            [["first-0"], ["first-1"], ["first-2"]]
        );
        // One aborted: never read.
        t.write("ledger", 1, "dropped").await;
        assert_eq!(t.end(false).await, 0);
        assert_eq!(
            t.committed("ledger").await,
            [["first-0"], ["first-1"], ["first-2"]]
        );
        // Another is open when the coordinator's process is killed.
        t.write("ledger", 2, "second").await;
        coordinator = t.coordinator().await;
    }
    cluster.kill(coordinator);
    let mut t = Transactor {
        cluster: &cluster,
        id: "bookkeeper",
        producer_id: -1,
        epoch: -1,
    };
    // Another broker coordinates the id now, and knows what the first one knew: the producer comes back, is
    // given the next epoch, and what it had left open is aborted - on every partition, wherever it is led now.
    assert_ne!(t.coordinator().await, coordinator);
    t.init().await;
    assert_eq!(t.epoch, 1, "the id's producer and epoch outlived the coordinator");
    assert_eq!(
        t.committed("ledger").await,
        [["first-0"], ["first-1"], ["first-2"]]
    );
    t.write("ledger", 0, "third").await;
    assert_eq!(t.end(true).await, 0);
    assert_eq!(
        t.committed("ledger").await,
        [
            ["first-0", "third-0"],
            ["first-1", "third-1"],
            ["first-2", "third-2"]
        ]
    );
}
