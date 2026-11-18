// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct serve --metrics-listen`: the broker's metrics for Prometheus, from the program itself.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A broker in memory, with `extra` arguments; stopped when dropped.
struct Serving(Child);

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn serve(kafka: u16, extra: &[&str]) -> Serving {
    let child = Command::new(OPENDUCT)
        .args(["serve", "--memory", "--listen", &format!("127.0.0.1:{kafka}")])
        .args(extra)
        .env("OPENDUCT_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    Serving(child)
}

/// The whole answer to `GET /metrics` (status line, headers and body), once the endpoint answers.
fn get(port: u16, authorization: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let auth = authorization
                .map(|a| format!("Authorization: {a}\r\n"))
                .unwrap_or_default();
            s.write_all(format!("GET /metrics HTTP/1.1\r\nHost: test\r\n{auth}\r\n").as_bytes())
                .unwrap();
            let mut answer = String::new();
            s.read_to_string(&mut answer).unwrap();
            return answer;
        }
        assert!(Instant::now() < deadline, "the metrics endpoint did not answer");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn openduct(kafka: u16, args: &[&str], stdin: &str) -> String {
    let mut child = Command::new(OPENDUCT)
        .args(args)
        .args(["--bootstrap", &format!("127.0.0.1:{kafka}")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The value of the first line starting `series ` (a series and its labels, exactly).
fn value(answer: &str, series: &str) -> Option<f64> {
    answer
        .lines()
        .find_map(|l| l.strip_prefix(series).and_then(|rest| rest.strip_prefix(' ')))
        .and_then(|v| v.trim().parse().ok())
}

#[test]
fn requests_offsets_and_a_groups_lag_are_measured() {
    let (kafka, metrics) = (free_port(), free_port());
    let _broker = serve(kafka, &["--metrics-listen", &format!("127.0.0.1:{metrics}")]);
    get(metrics, None);
    openduct(kafka, &["topic", "create", "jobs"], "");
    // A second topic, so families with a line per partition have several (and must stay grouped).
    openduct(kafka, &["topic", "create", "audit", "--partitions", "2"], "");
    openduct(kafka, &["send", "jobs"], "a\nb\nc\n");
    assert_eq!(
        openduct(kafka, &["read", "jobs", "--group", "worker", "--max", "1"], "")
            .lines()
            .count(),
        1
    );

    let answer = get(metrics, None);
    assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    assert!(
        answer.contains("Content-Type: text/plain; version=0.0.4"),
        "{answer}"
    );
    assert!(value(&answer, "openduct_requests_total{api=\"Produce\"}").unwrap() >= 1.0);
    assert!(value(&answer, "openduct_produced_bytes_total").unwrap() > 0.0);
    assert!(value(&answer, "openduct_connections_accepted_total").unwrap() >= 3.0);
    assert_eq!(
        value(
            &answer,
            "openduct_partition_log_end_offset{topic=\"jobs\",partition=\"0\"}"
        ),
        Some(3.0)
    );
    assert_eq!(
        value(
            &answer,
            "openduct_consumer_group_lag{group=\"worker\",topic=\"jobs\",partition=\"0\"}"
        ),
        Some(2.0),
        "one message read of three: {answer}"
    );
    assert_eq!(value(&answer, "openduct_partitions_without_leader"), Some(0.0));
    let produce_count = value(
        &answer,
        "openduct_request_duration_seconds_count{api=\"Produce\"}",
    )
    .unwrap();
    assert_eq!(
        value(
            &answer,
            "openduct_request_duration_seconds_bucket{api=\"Produce\",le=\"+Inf\"}"
        ),
        Some(produce_count)
    );
    assert!(
        !answer.contains("openduct_workflow_runs_total"),
        "workflows are counted only beside the studio"
    );
    families_are_whole(&answer);
}

/// The text format's rule: a family's samples come together, after its HELP and TYPE, and it never comes back.
fn families_are_whole(answer: &str) {
    let body = answer.split("\r\n\r\n").nth(1).unwrap();
    let mut done: Vec<String> = Vec::new();
    let mut current = String::new();
    for line in body.lines().filter(|l| !l.is_empty()) {
        let family = if let Some(rest) = line
            .strip_prefix("# HELP ")
            .or_else(|| line.strip_prefix("# TYPE "))
        {
            rest.split(' ').next().unwrap().to_owned()
        } else {
            let name = line.split(['{', ' ']).next().unwrap();
            ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| name.strip_suffix(s))
                .filter(|base| body.contains(&format!("# TYPE {base} histogram")))
                .unwrap_or(name)
                .to_owned()
        };
        if family != current {
            assert!(
                !done.contains(&family),
                "{family} comes back after another family:\n{body}"
            );
            if !current.is_empty() {
                done.push(std::mem::take(&mut current));
            }
            current = family;
        }
    }
}

#[test]
fn beyond_this_machine_a_token_is_asked_and_checked() {
    let (kafka, metrics) = (free_port(), free_port());
    let out = Command::new(OPENDUCT)
        .args(["serve", "--memory", "--listen", &format!("127.0.0.1:{kafka}")])
        .args(["--metrics-listen", &format!("0.0.0.0:{metrics}")])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("give --metrics-token-file"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("metrics.token");
    std::fs::write(&file, "a-token-of-sixteen-or-more\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let (kafka, metrics) = (free_port(), free_port());
    let _broker = serve(
        kafka,
        &[
            "--metrics-listen",
            &format!("127.0.0.1:{metrics}"),
            "--metrics-token-file",
            file.to_str().unwrap(),
        ],
    );
    assert!(get(metrics, None).starts_with("HTTP/1.1 401"));
    assert!(get(metrics, Some("Bearer not-the-token-at-all")).starts_with("HTTP/1.1 401"));
    let answer = get(metrics, Some("Bearer a-token-of-sixteen-or-more"));
    assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    assert!(
        !answer.contains("a-token-of-sixteen-or-more"),
        "the token is never shown"
    );
}
