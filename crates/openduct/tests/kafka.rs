// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The kafka connector as a person runs it: topics of another cluster read into OpenDuct and written back out.
//! Against another OpenDuct always; against a real Kafka cluster when `OPENDUCT_TEST_KAFKA` names one
//! (`cargo xtask services up` starts one), with an independent Kafka client (rskafka) producing what is read; and
//! signed in with SCRAM and PLAIN over TLS when `OPENDUCT_TEST_KAFKA_SASL` names its SASL listener.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hmac::{KeyInit, Mac};
use kafka_protocol::messages::alter_user_scram_credentials_request::ScramCredentialUpsertion;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::{AlterUserScramCredentialsRequest, CreateTopicsRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use openduct_broker::client::Client;
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression as RsCompression, UnknownTopicHandling};
use rskafka::record::Record as RsRecord;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

async fn broker() -> (Broker, String) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

fn openduct(addr: &str, args: &[&str]) -> String {
    let out = Command::new(OPENDUCT)
        .args(args)
        .args(["--bootstrap", addr])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A name no earlier run used: the Kafka cluster keeps topics and groups between runs.
fn run_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Tests run at once, and a clock may count only microseconds: a count keeps their names apart.
    static COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("t{:x}{n}", nanos % (1 << 40))
}

/// A running pipeline, with its log lines as they come.
struct Running {
    child: Child,
    log: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Running {
    fn start(addr: &str, file: &std::path::Path, env: &[(&str, &str)]) -> Self {
        let mut child = Command::new(OPENDUCT)
            .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", addr])
            .args(secrets_args(file, env))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, log) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = tx.send(line);
            }
        });
        Self {
            child,
            log,
            seen: Vec::new(),
        }
    }

    fn wait_for_log(&mut self, text: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some(line) = self.seen.iter().find(|l| l.contains(text)) {
                return line.clone();
            }
            if let Ok(line) = self.log.recv_timeout(Duration::from_millis(200)) {
                self.seen.push(line);
            }
        }
        panic!("no log line with {text:?}: {:?}", self.seen);
    }

    fn stop(mut self) -> String {
        Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        let status = self.child.wait().unwrap();
        while let Ok(line) = self.log.recv_timeout(Duration::from_secs(2)) {
            self.seen.push(line);
        }
        let log = self.seen.join("\n");
        assert!(
            status.success(),
            "the pipeline had stopped by itself ({status}):\n{log}"
        );
        log
    }

    /// Wait for it to stop by itself; whether it succeeded, and its log.
    fn ended(mut self) -> (bool, String) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "it did not stop: {:?}", self.seen);
            std::thread::sleep(Duration::from_millis(100));
        };
        while let Ok(line) = self.log.recv_timeout(Duration::from_secs(1)) {
            self.seen.push(line);
        }
        (status.success(), self.seen.join("\n"))
    }
}

/// The messages of a topic as `openduct read` shows them, once there are `want` (or 30 seconds have passed).
fn wait_for(addr: &str, topic: &str, want: usize) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let messages: Vec<serde_json::Value> = openduct(addr, &["read", topic, "--format", "json"])
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        if messages.len() >= want || Instant::now() > deadline {
            return messages;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

fn header<'a>(message: &'a serde_json::Value, name: &str) -> &'a str {
    message["headers"][name].as_str().unwrap_or_default()
}

/// Where each message came from in the other cluster: (partition, offset), from the headers the source adds.
fn origins(messages: &[serde_json::Value]) -> Vec<(String, String)> {
    messages
        .iter()
        .map(|m| {
            (
                header(m, "kafka-partition").to_owned(),
                header(m, "kafka-offset").to_owned(),
            )
        })
        .collect()
}

fn write(dir: &tempfile::TempDir, name: &str, text: &str) -> std::path::PathBuf {
    let file = dir.path().join(name);
    std::fs::write(&file, text).unwrap();
    file
}

#[tokio::test(flavor = "multi_thread")]
async fn another_openduct_is_read_and_written_as_a_kafka_cluster() {
    let (remote, there) = broker().await;
    let (local, here) = broker().await;
    let (t, h) = (there.clone(), here.clone());
    tokio::task::spawn_blocking(move || {
        openduct(&t, &["topic", "create", "orders", "--partitions", "3"]);
        openduct(&t, &["topic", "create", "returned", "--partitions", "3"]);
        openduct(&h, &["topic", "create", "copied"]);
        for n in 1..=4 {
            openduct(&t, &["send", "orders", "--key", &format!("order-{n}"), &format!(r#"{{"n":{n}}}"#)]);
        }
        let dir = tempfile::tempdir().unwrap();
        let inward = write(
            &dir,
            "in.yaml",
            &format!("name: orders-in\nfrom:\n  kafka:\n    brokers: {t}\n    topics: orders\nto:\n  topic: copied\n"),
        );
        let mut running = Running::start(&h, &inward, &[]);
        running.wait_for_log(&format!("reading kafka orders on {t} as group openduct-orders-in"));
        let got = wait_for(&h, "copied", 4);
        let log = running.stop();
        assert_eq!(got.len(), 4, "{got:?}\n{log}");
        let first = got.iter().find(|m| m["key"] == "order-1").unwrap();
        assert_eq!(first["value"], r#"{"n":1}"#);
        assert_eq!(header(first, "kafka-topic"), "orders");

        // What arrives while it is stopped comes after; what it read before does not come again.
        for n in 5..=6 {
            openduct(&t, &["send", "orders", "--key", &format!("order-{n}"), &format!(r#"{{"n":{n}}}"#)]);
        }
        let running = Running::start(&h, &inward, &[]);
        wait_for(&h, "copied", 6);
        std::thread::sleep(Duration::from_secs(2));
        let log = running.stop();
        let got = wait_for(&h, "copied", 6);
        assert_eq!(got.len(), 6, "nothing twice: {got:?}\n{log}");
        assert_eq!(origins(&got).into_iter().collect::<BTreeSet<_>>().len(), 6);

        // Out again, compressed: each key lands on the partition it has in the topic it came from.
        let outward = write(
            &dir,
            "out.yaml",
            &format!(
                "name: orders-out\nfrom:\n  topic: copied\n  start: earliest\nto:\n  kafka:\n    brokers: {t}\n    topic: returned\n    compression: lz4\n"
            ),
        );
        let mut running = Running::start(&h, &outward, &[]);
        running.wait_for_log("writing to kafka returned");
        let back = wait_for(&t, "returned", 6);
        let log = running.stop();
        assert_eq!(back.len(), 6, "{back:?}\n{log}");
        let orders = wait_for(&t, "orders", 6);
        for message in &back {
            let source = orders.iter().find(|o| o["key"] == message["key"]).unwrap();
            assert_eq!(message["partition"], source["partition"], "{message}");
            assert_eq!(message["value"], source["value"]);
        }
    })
    .await
    .unwrap();
    remote.shutdown().await;
    local.shutdown().await;
}

#[test]
fn settings_that_cannot_work_are_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    let check = |name: &str, from: &str| {
        let file = write(
            &dir,
            &format!("{name}.yaml"),
            &format!("name: {name}\nfrom:\n  kafka:\n{from}to:\n  topic: copied\n"),
        );
        let out = Command::new(OPENDUCT)
            .args(["pipeline", "check", file.to_str().unwrap()])
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), text)
    };
    let (ok, text) = check(
        "good",
        "    brokers: [b-1.example.com:9096, b-2.example.com:9096]\n    topics: orders, payments\n    tls: true\n    sasl: scram-sha-512\n    username: secret:KAFKA_USER\n    password: secret:KAFKA_PASSWORD\n",
    );
    assert!(ok, "{text}");
    assert!(
        text.contains(
            "it reads kafka orders, payments on b-1.example.com:9096 and 1 more as group openduct-good and writes copied"
        ),
        "{text}"
    );
    for (name, from, why) in [
        (
            "plain",
            "    brokers: b:9092\n    topics: t\n    sasl: plain\n    username: u\n    password: p\n",
            "plain sends the password as it is",
        ),
        (
            "scheme",
            "    brokers: SASL_SSL://b:9096\n    topics: t\n",
            "how to connect goes in tls and sasl",
        ),
        (
            "topic",
            "    brokers: b:9092\n    topics: orders/eu\n",
            "is not a Kafka topic name",
        ),
        (
            "user",
            "    brokers: b:9092\n    topics: t\n    username: u\n",
            "give sasl too",
        ),
        (
            "mech",
            "    brokers: b:9092\n    topics: t\n    sasl: gssapi\n    username: u\n    password: p\n",
            "is plain, scram-sha-256 or scram-sha-512",
        ),
        (
            "ca",
            "    brokers: b:9092\n    topics: t\n    ca: file:/x.pem\n",
            "is for tls: true",
        ),
        (
            "start",
            "    brokers: b:9092\n    topics: t\n    start: middle\n",
            "is one of earliest, latest",
        ),
        ("none", "    topics: t\n", "one or more host:port"),
    ] {
        let (ok, text) = check(name, from);
        assert!(!ok && text.contains(why), "{name}: {text}");
    }
}

// ------------------------------------------------------------------------------------------------ a real cluster

fn kafka() -> Option<String> {
    std::env::var("OPENDUCT_TEST_KAFKA").ok()
}

async fn create_topic(kafka: &str, name: &str, partitions: i32, configs: &[(&str, &str)]) {
    let mut client = Client::connect(kafka, Duration::from_secs(10)).await.unwrap();
    let topic = CreatableTopic::default()
        .with_name(TopicName(StrBytes::from_string(name.to_owned())))
        .with_num_partitions(partitions)
        .with_replication_factor(-1)
        .with_configs(
            configs
                .iter()
                .map(|(k, v)| {
                    CreatableTopicConfig::default()
                        .with_name(StrBytes::from_string((*k).to_owned()))
                        .with_value(Some(StrBytes::from_string((*v).to_owned())))
                })
                .collect(),
        );
    let answer = client
        .send(
            &CreateTopicsRequest::default()
                .with_topics(vec![topic])
                .with_timeout_ms(30_000),
            7,
        )
        .await
        .unwrap();
    assert_eq!(answer.topics[0].error_code, 0, "{answer:?}");
}

fn reading(key: &str, n: usize) -> RsRecord {
    RsRecord {
        key: Some(key.as_bytes().to_vec()),
        value: Some(format!(r#"{{"reading":{n}}}"#).into_bytes()),
        headers: BTreeMap::from([("plant".to_owned(), b"plant-4".to_vec())]),
        timestamp: chrono::Utc::now(),
    }
}

/// Produce with rskafka, a Kafka client OpenDuct did not write: `count` messages to each partition, each partition
/// compressed its own way.
async fn produce(kafka: &str, topic: &str, count: usize, first: usize) {
    let client = ClientBuilder::new(vec![kafka.to_owned()]).build().await.unwrap();
    for (partition, compression) in [
        RsCompression::Gzip,
        RsCompression::Snappy,
        RsCompression::NoCompression,
    ]
    .into_iter()
    .enumerate()
    {
        let p = client
            .partition_client(
                topic,
                i32::try_from(partition).unwrap(),
                UnknownTopicHandling::Retry,
            )
            .await
            .unwrap();
        let records = (0..count)
            .map(|i| reading(&format!("dev-{partition}"), first + partition * 1000 + i))
            .collect();
        p.produce(records, compression).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kafka_cluster_is_read_by_a_group_and_written_by_key() {
    let Some(kafka) = kafka() else {
        eprintln!("skipped: OPENDUCT_TEST_KAFKA is not set (cargo xtask services up)");
        return;
    };
    let run = run_name();
    let (input, output) = (format!("{run}-in"), format!("{run}-out"));
    create_topic(&kafka, &input, 3, &[]).await;
    create_topic(&kafka, &output, 3, &[]).await;
    create_topic(
        &kafka,
        &format!("{run}-small"),
        1,
        &[("max.message.bytes", "2000")],
    )
    .await;
    produce(&kafka, &input, 10, 0).await;
    let (local, here) = broker().await;
    let rt = tokio::runtime::Handle::current();
    let (k, h, r) = (kafka.clone(), here.clone(), run.clone());
    tokio::task::spawn_blocking(move || {
        for topic in ["readings", "refused", "keyed"] {
            openduct(&h, &["topic", "create", topic]);
        }
        let dir = tempfile::tempdir().unwrap();
        let inward = write(
            &dir,
            "in.yaml",
            &format!("name: {r}-in\nfrom:\n  kafka:\n    brokers: {k}\n    topics: {input}\n    group: {r}-group\nto:\n  topic: readings\n"),
        );
        let running = Running::start(&h, &inward, &[]);
        let got = wait_for(&h, "readings", 30);
        let log = running.stop();
        assert_eq!(got.len(), 30, "{log}");
        let first = got.iter().find(|m| m["value"] == r#"{"reading":1000}"#).unwrap();
        assert_eq!(first["key"], "dev-1");
        assert_eq!(header(first, "plant"), "plant-4");
        assert_eq!(header(first, "kafka-partition"), "1");

        // The group's offsets, committed in the cluster: everything read, nothing more.
        let committed = rt.block_on(async {
            let mut client = Client::connect(&k, Duration::from_secs(10)).await.unwrap();
            client.committed(&format!("{r}-group"), &input, &[0, 1, 2]).await.unwrap()
        });
        assert_eq!(committed, vec![(0, 10), (1, 10), (2, 10)], "{log}");

        // Two copies share the partitions; together they read each message once.
        rt.block_on(produce(&k, &input, 4, 100));
        let mut one = Running::start(&h, &inward, &[]);
        one.wait_for_log("reading kafka");
        let got = wait_for(&h, "readings", 42);
        assert_eq!(got.len(), 42);
        let two = Running::start(&h, &inward, &[]);
        std::thread::sleep(Duration::from_secs(4));
        rt.block_on(produce(&k, &input, 5, 200));
        wait_for(&h, "readings", 57);
        std::thread::sleep(Duration::from_secs(2));
        let (log_one, log_two) = (one.stop(), two.stop());
        let got = wait_for(&h, "readings", 57);
        let unique: BTreeSet<_> = origins(&got).into_iter().collect();
        assert_eq!((got.len(), unique.len()), (57, 57), "{log_one}\n---\n{log_two}");
        assert!(log_two.contains("joined the Kafka group"), "{log_two}");

        // Written out by key, as Kafka's own clients place keys (values from Kafka's default partitioner).
        let placed = [("sensor-1", 1), ("order-42", 0), ("ab", 2), ("a", 1)];
        for (key, _) in placed {
            openduct(&h, &["send", "keyed", "--key", key, &format!(r#"{{"for":"{key}"}}"#)]);
        }
        let outward = write(
            &dir,
            "out.yaml",
            &format!(
                "name: {r}-out\nfrom:\n  topic: keyed\n  start: earliest\nto:\n  kafka:\n    brokers: {k}\n    topic: {output}\n    compression: zstd\n"
            ),
        );
        let running = Running::start(&h, &outward, &[]);
        let back = wait_for(&k, &output, 4);
        let log = running.stop();
        assert_eq!(back.len(), 4, "{log}");
        for (key, partition) in placed {
            let message = back.iter().find(|m| m["key"] == key).unwrap();
            assert_eq!(message["partition"], partition, "{key}");
        }

        // A message larger than the topic takes is set aside with the cluster's reason; the rest go.
        openduct(&h, &["topic", "create", "mixed"]);
        openduct(&h, &["send", "mixed", "--key", "small-1", r#"{"ok":1}"#]);
        openduct(&h, &["send", "mixed", "--key", "large", &format!(r#"{{"blob":"{}"}}"#, "x".repeat(5_000))]);
        openduct(&h, &["send", "mixed", "--key", "small-2", r#"{"ok":2}"#]);
        let small = write(
            &dir,
            "small.yaml",
            &format!(
                "name: {r}-small\nfrom:\n  topic: mixed\n  start: earliest\nto:\n  kafka:\n    brokers: {k}\n    topic: {r}-small\non_error:\n  topic: refused\n"
            ),
        );
        let running = Running::start(&h, &small, &[]);
        let refused = wait_for(&h, "refused", 1);
        let kept = wait_for(&k, &format!("{r}-small"), 2);
        let log = running.stop();
        assert_eq!(refused.len(), 1, "{log}");
        assert!(refused[0].to_string().contains("larger than Kafka topic"), "{}", refused[0]);
        assert_eq!(kept.len(), 2, "{kept:?}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

// ------------------------------------------------------------------------------------------------ SASL over TLS

struct Certificates {
    ca: String,
    server: (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
}

/// A CA and a certificate for 127.0.0.1 signed by it.
fn certificates() -> Certificates {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    };
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().unwrap()).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let mut server = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
    server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server = server.signed_by(&server_key, &*ca).unwrap();
    Certificates {
        ca: ca.pem(),
        server: (
            vec![server.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        ),
    }
}

/// TLS on 127.0.0.1:19094 - the address the cluster's SASL listener gives for itself - in front of that listener.
async fn tls_in_front(listener: String, certs: &Certificates) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs.server.0.clone(), certs.server.1.clone_key())
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let socket = tokio::net::TcpListener::bind("127.0.0.1:19094").await.unwrap();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = socket.accept().await {
            let (acceptor, listener) = (acceptor.clone(), listener.clone());
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let Ok(mut kafka) = tokio::net::TcpStream::connect(&listener).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut kafka).await;
            });
        }
    });
}

fn salted(mechanism: i8, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        if mechanism == 1 {
            let mut m = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
            m.update(data);
            m.finalize().into_bytes().to_vec()
        } else {
            let mut m = hmac::Hmac::<sha2::Sha512>::new_from_slice(key).unwrap();
            m.update(data);
            m.finalize().into_bytes().to_vec()
        }
    };
    let mut u = mac(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut out = u.clone();
    for _ in 1..iterations {
        u = mac(password, &u);
        out.iter_mut().zip(&u).for_each(|(o, x)| *o ^= x);
    }
    out
}

/// The user's SCRAM-SHA-256 and SCRAM-SHA-512 credentials, set in the cluster (as an administrator does).
async fn scram_user(kafka: &str, user: &str, password: &str) {
    let mut client = Client::connect(kafka, Duration::from_secs(10)).await.unwrap();
    for mechanism in [1i8, 2] {
        let salt = format!("openduct-salt-{mechanism}").into_bytes();
        let upsertion = ScramCredentialUpsertion::default()
            .with_name(StrBytes::from_string(user.to_owned()))
            .with_mechanism(mechanism)
            .with_iterations(4096)
            .with_salted_password(Bytes::from(salted(mechanism, password.as_bytes(), &salt, 4096)))
            .with_salt(Bytes::from(salt));
        let answer = client
            .send(
                &AlterUserScramCredentialsRequest::default().with_upsertions(vec![upsertion]),
                0,
            )
            .await
            .unwrap();
        assert!(answer.results.iter().all(|r| r.error_code == 0), "{answer:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scram_and_plain_sign_in_over_tls_and_a_wrong_password_or_ca_stops_with_the_reason() {
    let (Some(kafka), Ok(sasl), Ok(user), Ok(password)) = (
        kafka(),
        std::env::var("OPENDUCT_TEST_KAFKA_SASL"),
        std::env::var("OPENDUCT_TEST_KAFKA_USER"),
        std::env::var("OPENDUCT_TEST_KAFKA_PASSWORD"),
    ) else {
        eprintln!("skipped: OPENDUCT_TEST_KAFKA_SASL is not set (cargo xtask services up)");
        return;
    };
    let run = run_name();
    let topic = format!("{run}-secure");
    create_topic(&kafka, &topic, 1, &[]).await;
    scram_user(&kafka, &user, &password).await;
    let certs = certificates();
    tls_in_front(sasl, &certs).await;
    let other_ca = certificates().ca;
    let (local, here) = broker().await;
    let (k, h) = (kafka.clone(), here.clone());
    tokio::task::spawn_blocking(move || {
        openduct(&k, &["send", &topic, "--key", "door-3", r#"{"open":true}"#, r#"{"open":false}"#]);
        let dir = tempfile::tempdir().unwrap();
        let ca = write(&dir, "ca.pem", &certs.ca);
        let wrong_ca = write(&dir, "other-ca.pem", &other_ca);
        let pipeline = |name: &str, sasl: &str, ca: &std::path::Path| {
            write(
                &dir,
                &format!("{name}.yaml"),
                &format!(
                    "name: {run}-{name}\nfrom:\n  kafka:\n    brokers: 127.0.0.1:19094\n    topics: {topic}\n    tls: true\n    ca: file:{}\n    sasl: {sasl}\n    username: {user}\n    password: secret:KAFKA_PASSWORD\nto:\n  topic: {name}\n",
                    ca.display()
                ),
            )
        };
        for (name, sasl) in [("sha512", "scram-sha-512"), ("sha256", "scram-sha-256"), ("plain", "plain")] {
            openduct(&h, &["topic", "create", name]);
            let running = Running::start(&h, &pipeline(name, sasl, &ca), &[("KAFKA_PASSWORD", &password)]);
            let got = wait_for(&h, name, 2);
            let log = running.stop();
            assert_eq!(got.len(), 2, "{sasl}: {log}");
            assert_eq!(got[0]["key"], "door-3");
        }

        let wrong = "not-the-password-4471";
        let (ok, log) = Running::start(&h, &pipeline("sha512", "scram-sha-512", &ca), &[("KAFKA_PASSWORD", wrong)]).ended();
        assert!(!ok && log.contains("refused the username or password"), "{log}");
        assert!(!log.contains(wrong), "the password is never shown: {log}");

        let (ok, log) = Running::start(&h, &pipeline("sha512", "scram-sha-512", &wrong_ca), &[("KAFKA_PASSWORD", &password)]).ended();
        assert!(!ok && log.contains("certificate was not accepted"), "{log}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

/// The secrets a pipeline names, in a secrets file beside it that only its owner may read: `--secrets-file <it>`.
fn secrets_args(file: &std::path::Path, secrets: &[(&str, &str)]) -> Vec<String> {
    let path = file.with_extension("secrets");
    let text: String = secrets
        .iter()
        .map(|(k, v)| {
            let v = v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
            format!("{k}=\"{v}\"\n")
        })
        .collect();
    std::fs::write(&path, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    vec!["--secrets-file".to_owned(), path.to_str().unwrap().to_owned()]
}
