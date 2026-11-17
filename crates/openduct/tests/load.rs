// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! How many workflows one node holds: for each size (100, 500 and 1,000 unless `OPENDUCT_LOAD` says, as
//! `OPENDUCT_LOAD=100,2000`), a fresh `openduct start` with that many workflows on, all reading one topic. It measures
//! how long they take to be on and running, what an idle node then costs (memory, CPU, connections), and how many
//! runs a second it gets through a burst of records - every workflow running each record.
//!
//! Not run with the others - it takes minutes and wants a release build:
//!
//! ```sh
//! cargo test --release -p openduct --features studio --test load -- --ignored --nocapture
//! ```
//!
//! `OPENDUCT_LOAD_SAMPLE=<file>` also records (macOS `sample`) where the node's time goes while idle and in each
//! burst, to `<file>.idle.<workflows>` and `<file>.<workflows>`.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::print_stdout,
    clippy::cast_precision_loss
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::process::Command;
use std::time::{Duration, Instant};

use openduct_broker::client::{LATEST, Routed};
use serde_json::json;
use studio::{OPENDUCT, call, owner, start, stop, workspace_topic};

/// Records in the burst: each workflow runs each.
const BURST: usize = 20;

#[derive(Debug)]
struct Measured {
    workflows: usize,
    turned_on: Duration,
    running: Duration,
    memory_mib: f64,
    idle_cpu: f64,
    connections: usize,
    runs: usize,
    burst: Duration,
}

/// Kilobytes of memory the process holds, and its CPU time so far in seconds.
fn usage(pid: u32) -> (u64, f64) {
    let out = Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    let rss = parts.next().unwrap().parse().unwrap();
    // [hh:]mm:ss.ss
    let cpu = parts
        .next()
        .unwrap()
        .split(':')
        .fold(0.0, |total, part| total * 60.0 + part.parse::<f64>().unwrap());
    (rss, cpu)
}

/// The connections open to the broker's port, from this process's own side.
fn connections(pid: u32, broker: &str) -> usize {
    let out = Command::new("lsof")
        .args(["-nP", "-a", "-p", &pid.to_string(), "-iTCP", "-sTCP:ESTABLISHED"])
        .output()
        .unwrap();
    let to = format!("->{broker}");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains(&to))
        .count()
}

/// Runs kept so far: the end of each workflow's history.
async fn kept(client: &mut Routed, ids: &[String]) -> usize {
    let mut total = 0;
    for id in ids {
        let topic = format!("__openduct.flow.{id}.runs");
        total += client
            .list_offset(&topic, 0, LATEST)
            .await
            .map_or(0, |end| usize::try_from(end).unwrap_or(0));
    }
    total
}

fn until_kept(
    runtime: &tokio::runtime::Runtime,
    broker: &str,
    ids: &[String],
    want: usize,
    within: Duration,
) {
    let begun = Instant::now();
    runtime.block_on(async {
        let mut client = Routed::connect(broker, Duration::from_secs(10)).await.unwrap();
        loop {
            let have = kept(&mut client, ids).await;
            if have >= want {
                return;
            }
            assert!(
                begun.elapsed() < within,
                "{have} of {want} runs kept after {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}

fn send(broker: &str, topic: &str, values: &[String]) {
    let mut args: Vec<&str> = vec!["send", topic];
    args.extend(values.iter().map(String::as_str));
    args.extend(["--bootstrap", broker]);
    let sent = Command::new(OPENDUCT).args(&args).output().unwrap();
    assert!(sent.status.success(), "{}", String::from_utf8_lossy(&sent.stderr));
}

fn measure(workflows: usize, runtime: &tokio::runtime::Runtime) -> Measured {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let pid = running.child.id();
    let topic = workspace_topic(&studio, &signed, "load");
    let made = Command::new(OPENDUCT)
        .args([
            "topic",
            "create",
            &topic,
            "--partitions",
            "4",
            "--bootstrap",
            &running.broker,
        ])
        .output()
        .unwrap();
    assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));

    // Made and turned on, one after another, as a person or a script would.
    let begun = Instant::now();
    let mut ids = Vec::with_capacity(workflows);
    for n in 0..workflows {
        let definition = json!({
            "name": format!("Load {n}"),
            "trigger": { "topic": { "name": "load", "start": "earliest" } },
            "steps": [{ "name": "total", "set": { "total": "value.qty * value.price", "big": "value.qty * value.price > 100" } }],
        });
        let made = call(
            &studio,
            &signed,
            "POST",
            "/api/workflows",
            Some(&json!({ "definition": definition }).to_string()),
        );
        assert_eq!(made.status, 201, "{}", made.body);
        let id = made.body["id"].as_str().unwrap().to_owned();
        let on = call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/active"),
            Some(r#"{"active": true}"#),
        );
        assert_eq!(on.status, 200, "{}", on.body);
        ids.push(id);
    }
    let turned_on = begun.elapsed();

    // Running: every workflow has run the first record.
    send(&running.broker, &topic, &[r#"{"qty": 1, "price": 1}"#.to_owned()]);
    until_kept(
        runtime,
        &running.broker,
        &ids,
        workflows,
        Duration::from_secs(900),
    );
    let running_after = begun.elapsed();

    // Idle: what the node costs with them all on and nothing arriving.
    std::thread::sleep(Duration::from_secs(5));
    let (_, cpu_before) = usage(pid);
    let idle_sample = std::env::var("OPENDUCT_LOAD_SAMPLE").ok().map(|file| {
        Command::new("sample")
            .args([
                &pid.to_string(),
                "8",
                "-mayDie",
                "-file",
                &format!("{file}.idle.{workflows}"),
            ])
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap()
    });
    std::thread::sleep(Duration::from_secs(10));
    if let Some(mut sampling) = idle_sample {
        let _ = sampling.wait();
    }
    let (rss, cpu_after) = usage(pid);
    let idle_cpu = (cpu_after - cpu_before) / 10.0 * 100.0;
    let open = connections(pid, &running.broker);

    // A burst: each workflow runs each record.
    let values: Vec<String> = (0..BURST)
        .map(|n| json!({ "qty": n + 1, "price": 7 }).to_string())
        .collect();
    let burst_begun = Instant::now();
    send(&running.broker, &topic, &values);
    // Where the node's time goes in the burst, when asked (macOS `sample`): OPENDUCT_LOAD_SAMPLE=<file>.
    let sampling = std::env::var("OPENDUCT_LOAD_SAMPLE").ok().map(|file| {
        Command::new("sample")
            .args([
                &pid.to_string(),
                "5",
                "-mayDie",
                "-file",
                &format!("{file}.{workflows}"),
            ])
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap()
    });
    let runs = workflows * BURST;
    until_kept(
        runtime,
        &running.broker,
        &ids,
        workflows * (BURST + 1),
        Duration::from_secs(1800),
    );
    let burst = burst_begun.elapsed();
    if let Some(mut sampling) = sampling {
        let _ = sampling.wait();
    }
    stop(running);
    Measured {
        workflows,
        turned_on,
        running: running_after,
        memory_mib: rss as f64 / 1024.0,
        idle_cpu,
        connections: open,
        runs,
        burst,
    }
}

#[test]
#[ignore = "minutes long: run it on purpose, in a release build"]
fn how_many_workflows_a_node_holds() {
    let sizes: Vec<usize> = std::env::var("OPENDUCT_LOAD")
        .unwrap_or_else(|_| "100,500,1000".to_owned())
        .split(',')
        .map(|n| n.trim().parse().unwrap())
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut rows = Vec::new();
    for size in sizes {
        let row = measure(size, &runtime);
        println!("{row:?}");
        rows.push(row);
    }
    println!();
    println!(
        "| Workflows | Turned on | All running | Memory | Idle CPU | Connections | Burst runs | Took | Runs a second |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for r in &rows {
        println!(
            "| {} | {:.1} s | {:.1} s | {:.0} MiB | {:.1}% | {} | {} | {:.1} s | {:.0} |",
            r.workflows,
            r.turned_on.as_secs_f64(),
            r.running.as_secs_f64(),
            r.memory_mib,
            r.idle_cpu,
            r.connections,
            r.runs,
            r.burst.as_secs_f64(),
            r.runs as f64 / r.burst.as_secs_f64()
        );
    }
}
