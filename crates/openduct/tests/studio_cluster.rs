// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Three `openduct start` nodes as one cluster: an account made on one signs in on another, a session made by one is
//! good on the others, a credential sealed by one is opened by another, webhook events sent to any node all run, and
//! a schedule runs each time once - not once on every node.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use studio::{JSON, Running, call, http, owner, start_with, until};

const SECRET: &str = "the test cluster's shared secret";
const TOKEN: &str = "cluster-token-0123456789";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Three nodes, started together (one alone cannot elect a leader of three).
fn cluster(dirs: &[tempfile::TempDir]) -> Vec<Running> {
    let ports: Vec<(u16, u16)> = (0..3).map(|_| (free_port(), free_port())).collect();
    let brokers = ports
        .iter()
        .enumerate()
        .map(|(i, (kafka, replication))| format!("{}=127.0.0.1:{replication}@127.0.0.1:{kafka}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    // The cluster secret, in a file: never in the environment.
    let secret_dir = tempfile::tempdir().unwrap();
    let secret = secret_dir.path().join("cluster-secret");
    std::fs::write(&secret, SECRET).unwrap();
    let starting: Vec<_> = ports
        .iter()
        .enumerate()
        .map(|(i, (kafka, replication))| {
            let data = dirs[i].path().to_owned();
            let (brokers, kafka, replication) = (brokers.clone(), *kafka, *replication);
            let secret = secret.clone();
            std::thread::spawn(move || {
                start_with(
                    &data,
                    &[
                        "--listen",
                        &format!("127.0.0.1:{kafka}"),
                        "--node-id",
                        &(i + 1).to_string(),
                        "--brokers",
                        &brokers,
                        "--replication-listen",
                        &format!("127.0.0.1:{replication}"),
                        "--studio-listen",
                        "127.0.0.1:0",
                        "--allow-private-network",
                    ],
                    &[("OPENDUCT_CLUSTER_SECRET_FILE", secret.to_str().unwrap())],
                )
            })
        })
        .collect();
    // A node that stops says so at once: the others then wait for a quorum that will not come.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    while !starting.iter().all(std::thread::JoinHandle::is_finished) {
        assert!(std::time::Instant::now() < deadline, "the cluster did not start");
        if starting.iter().any(std::thread::JoinHandle::is_finished)
            && !starting.iter().all(std::thread::JoinHandle::is_finished)
        {
            // One finished: if it failed, stop waiting.
            for (i, s) in starting.iter().enumerate() {
                if s.is_finished()
                    && std::fs::read_to_string(dirs[i].path().join("openduct.log"))
                        .is_ok_and(|l| l.contains("openduct: "))
                {
                    panic!(
                        "node {} stopped: {}",
                        i + 1,
                        std::fs::read_to_string(dirs[i].path().join("openduct.log"))
                            .unwrap_or_default()
                            .lines()
                            .last()
                            .unwrap_or_default()
                    );
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    starting.into_iter().map(|s| s.join().unwrap()).collect()
}

/// A server that answers 200 and keeps the Authorization header of every request.
fn recorder() -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut got = Vec::new();
            let mut buf = [0u8; 8192];
            while let Ok(n) = stream.read(&mut buf) {
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got);
                if let Some((head, rest)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if rest.len() >= length {
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&got).into_owned();
            let auth = text
                .lines()
                .find_map(|l| {
                    l.strip_prefix("authorization: ")
                        .or_else(|| l.strip_prefix("Authorization: "))
                })
                .unwrap_or("")
                .to_owned();
            kept.lock().unwrap().push(auth);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}");
        }
    });
    (port, seen)
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None).body["runs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[test]
fn three_nodes_share_accounts_sessions_credentials_and_runs_and_a_schedule_runs_each_time_once() {
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = cluster(&dirs);
    let studios: Vec<String> = nodes.iter().map(|n| n.studio.clone()).collect();

    // The account made on the first node; its session is good on the others, and it signs in on them too.
    let signed = owner(&studios[0]);
    for studio in &studios[1..] {
        until(30, "the session good on another node", || {
            call(studio, &signed, "GET", "/api/session", None).status == 200
        });
    }
    let password = json!({ "email": "ada@example.com", "password": "correct horse battery" }).to_string();
    let elsewhere = http(&studios[2], "POST", "/api/sign-in", &[JSON], Some(&password));
    assert_eq!(elsewhere.status, 200, "{}", elsewhere.body);

    // A credential sealed by the first node is opened by the second: a preview there sends its GET for real.
    let (port, seen) = recorder();
    let made = call(
        &studios[0],
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "api", "kind": "bearer", "fields": { "token": TOKEN } }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let get = json!({ "name": "Read", "steps": [{ "name": "call", "http": { "method": "GET", "url": format!("http://127.0.0.1:{port}/read"), "credential": "api" } }] });
    let reader = call(
        &studios[0],
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": get }).to_string()),
    );
    let reader = reader.body["id"].as_str().unwrap().to_owned();
    until(30, "a preview on another node that opens the credential", || {
        let shown = call(
            &studios[1],
            &signed,
            "POST",
            &format!("/api/workflows/{reader}/preview"),
            Some(r#"{"input": {}}"#),
        );
        shown.body["run"]["status"] == "done"
    });
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|a| a == &format!("Bearer {TOKEN}")),
        "{:?}",
        seen.lock().unwrap()
    );

    // Webhook events sent to every node all run, whichever node runs each, each with the credential opened.
    seen.lock().unwrap().clear();
    let post = json!({ "name": "Forward", "trigger": "webhook", "steps": [{ "name": "call", "http": { "method": "POST", "url": format!("http://127.0.0.1:{port}/in"), "credential": "api", "json": { "n": "{{ value.n }}" } } }] });
    let forward = call(
        &studios[0],
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": post }).to_string()),
    );
    let hook = forward.body["hook"].as_str().unwrap().to_owned();
    let forward = forward.body["id"].as_str().unwrap().to_owned();
    call(
        &studios[0],
        &signed,
        "POST",
        &format!("/api/workflows/{forward}/active"),
        Some(r#"{"active": true}"#),
    );
    for n in 0..9 {
        let studio = &studios[n % 3];
        until(30, "a webhook taken", || {
            http(
                studio,
                "POST",
                &hook,
                &[JSON],
                Some(&json!({ "n": n }).to_string()),
            )
            .status
                == 202
        });
    }
    until(60, "every event run", || seen.lock().unwrap().len() >= 9);
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .all(|a| a == &format!("Bearer {TOKEN}")),
        "{:?}",
        seen.lock().unwrap()
    );

    // A schedule every 2 seconds, on in all three nodes: each time runs once.
    let tick = json!({ "name": "Tick", "trigger": { "schedule": { "every": "2s" } }, "steps": [{ "name": "now", "set": { "at": "trigger.at" } }] });
    let ticker = call(
        &studios[0],
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": tick }).to_string()),
    );
    let ticker = ticker.body["id"].as_str().unwrap().to_owned();
    call(
        &studios[0],
        &signed,
        "POST",
        &format!("/api/workflows/{ticker}/active"),
        Some(r#"{"active": true}"#),
    );
    until(60, "five scheduled runs", || {
        runs(&studios[2], &signed, &ticker).len() >= 5
    });
    std::thread::sleep(std::time::Duration::from_secs(3));
    let times: Vec<String> = runs(&studios[2], &signed, &ticker)
        .iter()
        .map(|r| r["trigger"]["at"].as_str().unwrap_or_default().to_owned())
        .collect();
    let distinct: BTreeSet<&String> = times.iter().collect();
    assert_eq!(
        distinct.len(),
        times.len(),
        "each time ran once, not once on every node: {times:?}"
    );

    // The first node killed: the other two carry on - its sessions, its events' runs, the schedule.
    nodes[0].child.kill().unwrap();
    let _ = nodes[0].child.wait();
    let alive = &studios[1..];
    for studio in alive {
        until(60, "the session good after a node was lost", || {
            call(studio, &signed, "GET", "/api/session", None).status == 200
        });
    }
    let before = seen.lock().unwrap().len();
    for n in 0..6 {
        let studio = &alive[n % 2];
        until(60, "a webhook taken after a node was lost", || {
            http(
                studio,
                "POST",
                &hook,
                &[JSON],
                Some(&json!({ "n": 100 + n }).to_string()),
            )
            .status
                == 202
        });
    }
    // The lost node's partitions go to the others once its session in the group runs out (30 s).
    until(120, "every event run after a node was lost", || {
        seen.lock().unwrap().len() >= before + 6
    });
    let counted = |studio: &str| {
        call(
            studio,
            &signed,
            "GET",
            &format!("/api/workflows/{ticker}/metrics"),
            None,
        )
        .body["metrics"]["runs"]
            .as_u64()
            .unwrap_or(0)
    };
    let ran = counted(&studios[1]);
    until(120, "the schedule going on after a node was lost", || {
        counted(&studios[1]) >= ran + 3
    });
    let times: Vec<String> = runs(&studios[1], &signed, &ticker)
        .iter()
        .map(|r| r["trigger"]["at"].as_str().unwrap_or_default().to_owned())
        .collect();
    let distinct: BTreeSet<&String> = times.iter().collect();
    assert_eq!(
        distinct.len(),
        times.len(),
        "each time ran once, across the loss too: {times:?}"
    );
    drop(nodes);
}
