// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct send` and `openduct read` as a person runs them: the real binary, against a broker on a free port.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

async fn broker() -> (Broker, String) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

/// Run `openduct <args> --bootstrap <addr>`, with `stdin` as its input, off the async threads.
async fn openduct(addr: &str, args: &[&str], stdin: &str) -> Output {
    let mut all: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
    all.push("--bootstrap".into());
    all.push(addr.to_owned());
    let input = stdin.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut child = Command::new(OPENDUCT)
            .args(&all)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap()
}

fn out(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn err(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test]
async fn messages_sent_on_the_command_line_are_read_back_with_their_key_and_headers() {
    let (broker, addr) = broker().await;
    assert!(
        openduct(&addr, &["topic", "create", "sensors", "--partitions", "3"], "")
            .await
            .status
            .success()
    );
    let sent = openduct(
        &addr,
        &[
            "send",
            "sensors",
            r#"{"temp_c": 21.4}"#,
            r#"{"temp_c": 21.9}"#,
            "--key",
            "sensor-1",
            "--header",
            "unit=celsius",
        ],
        "",
    )
    .await;
    assert!(sent.status.success(), "{}", err(&sent));
    assert_eq!(out(&sent), "sent 2 messages to sensors\n");

    let read = openduct(&addr, &["read", "sensors", "--format", "json"], "").await;
    assert!(read.status.success(), "{}", err(&read));
    let lines: Vec<serde_json::Value> = out(&read)
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    // The key chooses the partition as Kafka clients do: "sensor-1" is partition 1 of 3.
    assert_eq!(lines[0]["partition"], 1);
    assert_eq!(lines[0]["offset"], 0);
    assert_eq!(lines[1]["offset"], 1);
    assert_eq!(lines[0]["key"], "sensor-1");
    assert_eq!(lines[0]["headers"]["unit"], "celsius");
    assert_eq!(lines[0]["value"], r#"{"temp_c": 21.4}"#);
    assert_eq!(lines[1]["value"], r#"{"temp_c": 21.9}"#);
    assert!(lines[0]["timestamp"].as_str().unwrap().ends_with('Z'));
    broker.shutdown().await;
}

#[tokio::test]
async fn standard_input_is_one_message_per_line_and_read_prints_values() {
    let (broker, addr) = broker().await;
    openduct(&addr, &["topic", "create", "log"], "").await;
    let sent = openduct(&addr, &["send", "log"], "first\n\nsecond\r\nthird\n").await;
    assert_eq!(
        out(&sent),
        "sent 3 messages to log\n",
        "the blank line is skipped: {}",
        err(&sent)
    );
    assert_eq!(
        out(&openduct(&addr, &["read", "log"], "").await),
        "first\nsecond\nthird\n"
    );
    assert_eq!(
        out(&openduct(&addr, &["read", "log", "--max", "2"], "").await),
        "first\nsecond\n"
    );
    assert_eq!(
        out(&openduct(&addr, &["read", "log", "--from", "2"], "").await),
        "third\n"
    );
    assert_eq!(
        out(&openduct(&addr, &["read", "log", "--from", "latest"], "").await),
        "",
        "only new ones"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn a_group_carries_on_where_it_got_to() {
    let (broker, addr) = broker().await;
    openduct(&addr, &["topic", "create", "jobs", "--partitions", "2"], "").await;
    openduct(&addr, &["send", "jobs"], "a\nb\nc\nd\n").await;
    let first = openduct(&addr, &["read", "jobs", "--group", "worker", "--max", "1"], "").await;
    assert_eq!(out(&first).lines().count(), 1, "{}", err(&first));
    let rest = openduct(&addr, &["read", "jobs", "--group", "worker"], "").await;
    let mut seen: Vec<String> = out(&first)
        .lines()
        .chain(out(&rest).lines())
        .map(str::to_owned)
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        ["a", "b", "c", "d"],
        "every message once, across the two reads"
    );
    assert_eq!(
        out(&openduct(&addr, &["read", "jobs", "--group", "worker"], "").await),
        "",
        "nothing left"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn follow_waits_for_a_message_that_has_not_been_sent_yet() {
    let (broker, addr) = broker().await;
    openduct(&addr, &["topic", "create", "alerts"], "").await;
    let started = Instant::now();
    let waiting = {
        let addr = addr.clone();
        tokio::spawn(async move {
            openduct(
                &addr,
                &["read", "alerts", "--follow", "--from", "latest", "--max", "1"],
                "",
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(700)).await;
    openduct(&addr, &["send", "alerts", "door open"], "").await;
    let read = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out(&read), "door open\n", "{}", err(&read));
    assert!(started.elapsed() < Duration::from_secs(8));
    broker.shutdown().await;
}

#[tokio::test]
async fn mistakes_are_explained() {
    let (broker, addr) = broker().await;
    let missing = openduct(&addr, &["read", "nowhere"], "").await;
    assert!(!missing.status.success());
    assert_eq!(
        err(&missing),
        "openduct: cannot read nowhere: topic \"nowhere\" does not exist\n"
    );
    openduct(&addr, &["topic", "create", "one"], "").await;
    let partition = openduct(&addr, &["send", "one", "x", "--partition", "4"], "").await;
    assert_eq!(err(&partition), "openduct: one has partitions 0 to 0, not 4\n");
    let header = openduct(&addr, &["send", "one", "x", "--header", "novalue"], "").await;
    assert!(
        err(&header).contains("a header is written name=value"),
        "{}",
        err(&header)
    );
    let empty = openduct(&addr, &["read", "one", "--from", "7"], "").await;
    assert_eq!(
        err(&empty),
        "openduct: offset 7 is past the end of partition 0 of one, which is empty\n"
    );
    openduct(&addr, &["send", "one", "a", "b"], "").await;
    let past = openduct(&addr, &["read", "one", "--from", "5"], "").await;
    assert_eq!(
        err(&past),
        "openduct: offset 5 is outside partition 0 of one, which holds offsets 0 to 1\n"
    );
    assert_eq!(
        out(&openduct(&addr, &["read", "one", "--from", "1"], "").await),
        "b\n"
    );
    broker.shutdown().await;
}
