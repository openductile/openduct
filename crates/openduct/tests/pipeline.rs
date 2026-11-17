// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct pipeline run` against a real broker: the clean-orders example end to end, a restart that carries on
//! without repeating anything, a stop at a bad message, and two copies sharing one topic.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");
const EXAMPLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/pipelines/clean-orders"
);

async fn broker() -> (Broker, String) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

fn openduct(addr: &str, args: &[&str], stdin: &str) -> (bool, String, String) {
    let mut child = Command::new(OPENDUCT)
        .args(args)
        .args(["--bootstrap", addr])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn start_pipeline(addr: &str, file: &std::path::Path) -> Child {
    Command::new(OPENDUCT)
        .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", addr])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// SIGTERM, as a service manager stops it, then everything it wrote on standard error.
fn stop(mut child: Child) -> (bool, String) {
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    let status = child.wait().unwrap();
    let mut err = String::new();
    child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    (status.success(), err)
}

/// The messages of a topic as JSON lines, once there are at least `want` of them.
fn wait_for(addr: &str, topic: &str, want: usize) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, out, _) = openduct(addr, &["read", topic, "--format", "json"], "");
        let lines: Vec<serde_json::Value> = out.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        if lines.len() >= want || Instant::now() > deadline {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn values(lines: &[serde_json::Value]) -> BTreeSet<String> {
    lines
        .iter()
        .map(|l| l["value"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_clean_orders_example_runs_end_to_end_and_carries_on_after_a_restart() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        for (topic, partitions) in [("orders", "2"), ("orders-clean", "2"), ("orders-rejected", "1")] {
            assert!(openduct(&a, &["topic", "create", topic, "--partitions", partitions], "").0);
        }
        let input = std::fs::read_to_string(format!("{EXAMPLE}/input.jsonl")).unwrap();
        assert!(openduct(&a, &["send", "orders"], &input).0);
        let file = std::path::Path::new(EXAMPLE).join("pipeline.yaml");

        let running = start_pipeline(&a, &file);
        let written = wait_for(&a, "orders-clean", 3);
        let rejected = wait_for(&a, "orders-rejected", 1);
        let (ok, err) = stop(running);
        assert!(ok, "{err}");
        assert!(err.contains("read 5, wrote 3, set aside 1"), "{err}");

        // What was written is what the example's expected.jsonl says, value for value and key for key.
        let expected: Vec<serde_json::Value> = std::fs::read_to_string(format!("{EXAMPLE}/expected.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .filter(|l| l.get("value").is_some())
            .collect();
        assert_eq!(written.len(), expected.len());
        for want in &expected {
            let found = written.iter().find(|w| w["key"] == want["key"]).unwrap();
            let value: serde_json::Value = serde_json::from_str(found["value"].as_str().unwrap()).unwrap();
            assert_eq!(value, want["value"], "{}", want["key"]);
        }
        // The order that could not be read is set aside unchanged, with why, by whom and from where.
        assert_eq!(rejected.len(), 1);
        let headers = &rejected[0]["headers"];
        assert_eq!(headers["openduct-error"], "map.items: double: given text that is not a number");
        assert_eq!(headers["openduct-pipeline"], "clean-orders");
        assert!(headers["openduct-source"].as_str().unwrap().starts_with("orders/"));
        assert!(rejected[0]["value"].as_str().unwrap().contains("\"A-103\""));

        // A restart carries on from what was committed: one new order, one new message, nothing repeated.
        let next = r#"{"id":"A-105","status":"new","paid":true,"created_at":"2026-11-15 10:00:00","customer":{"name":" Mary Jackson"},"lines":[{"sku":"z","qty":1,"price":9.5}]}"#;
        assert!(openduct(&a, &["send", "orders", next], "").0);
        let running = start_pipeline(&a, &file);
        let written = wait_for(&a, "orders-clean", 4);
        let (ok, err) = stop(running);
        assert!(ok, "{err}");
        assert_eq!(written.len(), 4, "every order once: {:?}", values(&written));
        assert!(err.contains("read 1, wrote 1, set aside 0"), "{err}");
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_an_error_topic_the_pipeline_stops_at_the_bad_message() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        for topic in ["in", "out"] {
            openduct(&a, &["topic", "create", topic], "");
        }
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("strict.yaml");
        std::fs::write(
            &file,
            "name: strict\nfrom:\n  topic: in\n  start: earliest\nmap:\n  double: n * 2\nto:\n  topic: out\n",
        )
        .unwrap();
        openduct(&a, &["send", "in"], "{\"n\": 1}\n{\"n\": \"lots\"}\n{\"n\": 3}\n");
        let (ok, _, err) = openduct(&a, &["pipeline", "run", file.to_str().unwrap()], "");
        assert!(!ok, "it stops");
        assert!(
            err.contains("stopped at in/0/1: map.double: cannot multiply text and a number"),
            "{err}"
        );
        let (_, out, _) = openduct(&a, &["read", "out"], "");
        assert_eq!(
            out, "{\"double\":2}\n",
            "what came before the bad message was written, and nothing after"
        );
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn two_copies_share_the_partitions_and_handle_every_message_once() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "jobs", "--partitions", "4"], "");
        openduct(&a, &["topic", "create", "done", "--partitions", "4"], "");
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("jobs.yaml");
        std::fs::write(&file, "name: jobs\nfrom:\n  topic: jobs\n  start: earliest\nmap:\n  job: id\nkey: job\nto:\n  topic: done\n").unwrap();
        let first = start_pipeline(&a, &file);
        std::thread::sleep(Duration::from_millis(500));
        let second = start_pipeline(&a, &file);
        // Let the group settle: the first copy learns of the second at its next heartbeat and gives up half.
        std::thread::sleep(Duration::from_secs(5));
        for i in 0..40 {
            openduct(&a, &["send", "jobs", &format!("{{\"id\": \"job-{i}\"}}"), "--key", &format!("job-{i}")], "");
        }
        let done = wait_for(&a, "done", 40);
        let (ok1, err1) = stop(first);
        let (ok2, err2) = stop(second);
        assert!(ok1 && ok2, "{err1}\n{err2}");
        assert_eq!(done.len(), 40, "each job once");
        let keys: BTreeSet<&str> = done.iter().map(|d| d["key"].as_str().unwrap()).collect();
        assert_eq!(keys.len(), 40);
        // Each copy ended with a share of the partitions, and together they had all four.
        let last_share = |err: &str| -> BTreeSet<i32> {
            let line = err.lines().rfind(|l| l.contains("reading jobs partitions")).unwrap().to_owned();
            let list = &line[line.find('[').unwrap() + 1..line.find(']').unwrap()];
            list.split(", ").filter(|s| !s.is_empty()).map(|p| p.parse().unwrap()).collect()
        };
        let (s1, s2) = (last_share(&err1), last_share(&err2));
        assert!(!s1.is_empty() && !s2.is_empty(), "both copies worked: {s1:?} {s2:?}");
        assert!(s1.is_disjoint(&s2));
        assert_eq!(s1.union(&s2).copied().collect::<BTreeSet<_>>(), (0..4).collect());
        let read = |err: &str| -> u64 {
            let line = err.lines().rfind(|l| l.contains(": read ")).unwrap().to_owned();
            line.split("read ").nth(1).unwrap().split(',').next().unwrap().parse().unwrap()
        };
        assert_eq!(read(&err1) + read(&err2), 40, "{err1}\n{err2}");
    })
    .await
    .unwrap();
    broker.shutdown().await;
}
