// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Pipelines in `openduct start`: written and checked in the studio, tried over sample messages, turned on - their
//! topics made - and run inside OpenDuct, their counts shown; turned off, they stop.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", feature = "pipelines", unix))]

mod studio;

use std::process::Command;

use serde_json::{Value, json};
use studio::{OPENDUCT, call, owner, start, start_with, stop, until, workspace_topic};

const SOURCE: &str = "name: orders-clean
from:
  topic: orders
  start: earliest
where: status != 'test'
map:
  order_id: id
  total: qty * price
key: order_id
to:
  topic: orders-clean
on_error:
  topic: orders-rejected
";

fn read(broker: &str, topic: &str) -> Vec<Value> {
    let out = Command::new(OPENDUCT)
        .args([
            "read",
            topic,
            "--from",
            "earliest",
            "--format",
            "json",
            "--timeout-secs",
            "3",
            "--bootstrap",
            broker,
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

#[test]
fn a_pipeline_written_in_the_studio_runs_inside_openduct_and_shows_its_counts() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);

    // Checked as written: a mistake is said with where it is.
    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines/check",
        Some(&json!({ "source": "name: x\nfrom:\n  topic: a\n" }).to_string()),
    );
    assert_eq!(wrong.status, 422, "{}", wrong.body);
    let right = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines/check",
        Some(&json!({ "source": SOURCE }).to_string()),
    );
    assert_eq!(right.status, 200, "{}", right.body);
    assert_eq!(right.body["from"], "orders");
    assert_eq!(right.body["to"], "orders-clean");

    // Tried over sample messages: what it would write, and what would fail.
    let tried = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines/test",
        Some(&json!({ "source": SOURCE, "input": "{\"id\":\"A-1\",\"status\":\"paid\",\"qty\":2,\"price\":5}\n{\"id\":\"T-1\",\"status\":\"test\",\"qty\":1,\"price\":1}\n" }).to_string()),
    );
    assert_eq!(tried.status, 200, "{}", tried.body);
    let lines = tried.body["lines"].as_array().unwrap();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["value"]["total"], 10);

    // Kept, then turned on: its topics are made, and it runs.
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines",
        Some(&json!({ "source": SOURCE }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    let twin = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines",
        Some(&json!({ "source": SOURCE }).to_string()),
    );
    assert_eq!(twin.status, 409, "{}", twin.body);
    let on = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/pipelines/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);

    // Its topics are its workspace's own: `orders` is w.<workspace>.orders on the broker.
    let orders = workspace_topic(&studio, &signed, "orders");
    let clean = workspace_topic(&studio, &signed, "orders-clean");
    let sent = Command::new(OPENDUCT)
        .args([
            "send",
            &orders,
            r#"{"id": "A-1", "status": "paid", "qty": 2, "price": 5}"#,
            r#"{"id": "T-1", "status": "test", "qty": 1, "price": 1}"#,
            r#"{"id": "A-2", "status": "paid", "qty": 3, "price": 4}"#,
            "--bootstrap",
            &running.broker,
        ])
        .output()
        .unwrap();
    assert!(sent.status.success(), "{}", String::from_utf8_lossy(&sent.stderr));
    until(60, "the cleaned orders in orders-clean", || {
        read(&running.broker, &clean).len() == 2
    });
    let out = read(&running.broker, &clean);
    let totals: Vec<i64> = out
        .iter()
        .map(|m| {
            // `read --format json` gives the value as it was written: here, JSON text.
            let value: Value = m["value"]
                .as_str()
                .map_or_else(|| m["value"].clone(), |t| serde_json::from_str(t).unwrap());
            value["total"].as_i64().unwrap()
        })
        .collect();
    assert_eq!(totals, [10, 12]);
    until(30, "its counts shown", || {
        let got = call(&studio, &signed, "GET", &format!("/api/pipelines/{id}"), None);
        got.body["read"] == 3 && got.body["written"] == 2 && got.body["running"] == true
    });

    // Another workspace's `orders`, and the broker's own plain `orders`, are not its: what goes there is not read.
    let other = Command::new(OPENDUCT)
        .args([
            "send",
            "orders",
            r#"{"id": "Z-9", "status": "paid", "qty": 1, "price": 1}"#,
            "--bootstrap",
            &running.broker,
        ])
        .output()
        .unwrap();
    let _ = other;
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(read(&running.broker, &clean).len(), 2);

    // Turned off: it stops running.
    let off = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/pipelines/{id}/active"),
        Some(r#"{"active": false}"#),
    );
    assert_eq!(off.status, 200, "{}", off.body);
    let listed = call(&studio, &signed, "GET", "/api/pipelines", None);
    assert_eq!(listed.body["pipelines"][0]["running"], false, "{}", listed.body);
    stop(running);
}

/// A web server that answers `[{"id": 1}]` only to `Authorization: Bearer s3cret`, and 401 otherwise.
fn guarded_server() -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut seen = Vec::new();
            let mut buf = [0u8; 4096];
            while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => seen.extend_from_slice(&buf[..n]),
                }
            }
            let asked = String::from_utf8_lossy(&seen).to_ascii_lowercase();
            let answer = if asked.contains("authorization: bearer s3cret") {
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\nconnection: close\r\n\r\n[{\"id\": 1}]"
            } else {
                "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            };
            let _ = stream.write_all(answer.as_bytes());
        }
    });
    addr
}

#[test]
fn a_studio_pipeline_s_secrets_come_from_its_workspace_s_credentials_never_the_environment() {
    let data = tempfile::tempdir().unwrap();
    // The guarded server is on this machine: pipelines may reach it only when OpenDuct is told they may.
    let running = start_with(
        data.path(),
        &[
            "--listen",
            "127.0.0.1:0",
            "--studio-listen",
            "127.0.0.1:0",
            "--allow-private-network",
        ],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let server = guarded_server();

    // The environment, and files on the server, are refused - with where the secret goes instead.
    let env = format!(
        "name: shop-in\nfrom:\n  http:\n    url: http://{server}/orders\n    headers: {{ Authorization: env:SHOP_TOKEN }}\nto:\n  topic: shop-orders\n"
    );
    let refused = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines/check",
        Some(&json!({ "source": env }).to_string()),
    );
    assert_eq!(refused.status, 422, "{}", refused.body);
    assert!(
        refused.body.to_string().contains("Credentials"),
        "{}",
        refused.body
    );
    let file = env.replace("env:SHOP_TOKEN", "file:/etc/hosts");
    let refused = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines",
        Some(&json!({ "source": file }).to_string()),
    );
    assert_eq!(refused.status, 422, "{}", refused.body);

    // Named, it is checked against the workspace's credentials as it is written.
    let source = env.replace("env:SHOP_TOKEN", "secret:shop.value");
    let checked = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines/check",
        Some(&json!({ "source": source }).to_string()),
    );
    assert_eq!(checked.status, 200, "{}", checked.body);
    assert_eq!(checked.body["missing"], json!(["shop.value"]));

    // Kept and turned on before the credential is there: it says what to add, and waits.
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines",
        Some(&json!({ "source": source }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    let on = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/pipelines/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);
    until(30, "it says to add the credential", || {
        let got = call(&studio, &signed, "GET", &format!("/api/pipelines/{id}"), None);
        got.body["error"]
            .as_str()
            .is_some_and(|e| e.contains("add it in Credentials"))
    });

    // The credential added in the studio: the pipeline takes it up and reads what the token opens.
    let added = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "shop", "kind": "header", "fields": { "name": "Authorization", "value": "Bearer s3cret" } }).to_string()),
    );
    assert_eq!(added.status, 201, "{}", added.body);
    let checked = call(
        &studio,
        &signed,
        "POST",
        "/api/pipelines/check",
        Some(&json!({ "source": source }).to_string()),
    );
    assert_eq!(checked.body["missing"], json!([]));
    let topic = workspace_topic(&studio, &signed, "shop-orders");
    until(
        60,
        "the guarded orders read with the workspace's credential",
        || !read(&running.broker, &topic).is_empty(),
    );
    stop(running);
}

#[test]
fn a_studio_pipeline_reaches_this_machine_s_own_network_only_when_allowed() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let server = guarded_server();
    let added = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "shop", "kind": "header", "fields": { "name": "Authorization", "value": "Bearer s3cret" } }).to_string()),
    );
    assert_eq!(added.status, 201, "{}", added.body);

    // An address on this machine, a name for one (localhost), and the cloud's metadata service: each refused, with
    // the reason - and nothing is read.
    for (n, url) in [
        format!("http://{server}/orders"),
        format!("http://localhost:{}/orders", server.rsplit_once(':').unwrap().1),
        "http://169.254.169.254/latest/meta-data/".to_owned(),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("near-{n}");
        let source = format!(
            "name: {name}\nfrom:\n  http:\n    url: {url}\n    headers: {{ Authorization: secret:shop.value }}\nto:\n  topic: {name}\n"
        );
        let made = call(
            &studio,
            &signed,
            "POST",
            "/api/pipelines",
            Some(&json!({ "source": source }).to_string()),
        );
        assert_eq!(made.status, 201, "{}", made.body);
        let id = made.body["id"].as_str().unwrap().to_owned();
        let on = call(
            &studio,
            &signed,
            "POST",
            &format!("/api/pipelines/{id}/active"),
            Some(r#"{"active": true}"#),
        );
        assert_eq!(on.status, 200, "{}", on.body);
        until(30, "it says why it may not reach the address", || {
            let got = call(&studio, &signed, "GET", &format!("/api/pipelines/{id}"), None);
            got.body["error"]
                .as_str()
                .is_some_and(|e| e.contains("--allow-private-network"))
        });
        let topic = workspace_topic(&studio, &signed, &name);
        assert!(read(&running.broker, &topic).is_empty(), "{url} was reached");
    }
    stop(running);
}
