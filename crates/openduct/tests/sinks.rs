// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Sinks against the real systems: `to: s3:` with an S3-compatible store and `to: elasticsearch:`. Each runs when
//! its service is given (`cargo xtask services up` starts both):
//!
//! ```text
//! OPENDUCT_TEST_S3=http://127.0.0.1:19900 OPENDUCT_TEST_S3_ACCESS=... OPENDUCT_TEST_S3_SECRET=...
//! OPENDUCT_TEST_ELASTICSEARCH=http://127.0.0.1:19920
//! ```

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{KeyInit, Mac};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;
use sha2::{Digest, Sha256};

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

async fn broker() -> (Broker, String) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

fn openduct(addr: &str, args: &[&str], stdin: &str) -> String {
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
    assert!(
        out.status.success(),
        "openduct {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn start(addr: &str, file: &std::path::Path, env: &[(&str, &str)]) -> Child {
    let mut command = Command::new(OPENDUCT);
    command
        .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", addr])
        .args(secrets_args(file, env))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn stop(mut child: Child) -> String {
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    let status = child.wait().unwrap();
    let mut err = String::new();
    child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    assert!(status.success(), "{err}");
    err
}

/// A plain-HTTP request; the status and the body.
fn http(at: &str, method: &str, path: &str, headers: &[(String, String)], body: &[u8]) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(at).unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {at}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).unwrap();
    let split = answer.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&answer[..split]).into_owned();
    let mut body = answer[split + 4..].to_vec();
    if head.to_lowercase().contains("transfer-encoding: chunked") {
        body = unchunk(&body);
    }
    (head.split_whitespace().nth(1).unwrap().parse().unwrap(), body)
}

fn unchunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = body.windows(2).position(|w| w == b"\r\n").unwrap();
        let size = usize::from_str_radix(std::str::from_utf8(&body[..line_end]).unwrap().trim(), 16).unwrap();
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&body[line_end + 2..line_end + 2 + size]);
        body = &body[line_end + 2 + size + 2..];
    }
}

// ------------------------------------------------------------------------------------------------------ S3

/// An S3 request signed here, independently of the connector's own signer, so each checks the other.
fn s3(
    endpoint: &str,
    access: &str,
    secret: &str,
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<u8>) {
    let host = endpoint.trim_start_matches("http://");
    let now = chrono::Utc::now();
    let (stamp, date) = (
        now.format("%Y%m%dT%H%M%SZ").to_string(),
        now.format("%Y%m%d").to_string(),
    );
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let mac = |k: &[u8], d: &str| {
        let mut m = hmac::Hmac::<Sha256>::new_from_slice(k).unwrap();
        m.update(d.as_bytes());
        m.finalize().into_bytes().to_vec()
    };
    let enc = |s: &str| {
        s.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                    (b as char).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect::<String>()
    };
    let mut q: Vec<(String, String)> = query.iter().map(|(k, v)| (enc(k), enc(v))).collect();
    q.sort();
    let canonical_query = q
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let payload = hex(&Sha256::digest(body));
    let canonical = format!(
        "{method}\n{path}\n{canonical_query}\nhost:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}"
    );
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let key = mac(
        &mac(
            &mac(&mac(format!("AWS4{secret}").as_bytes(), &date), "us-east-1"),
            "s3",
        ),
        "aws4_request",
    );
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
        hex(&mac(&key, &to_sign))
    );
    let target = if canonical_query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{canonical_query}")
    };
    http(
        host,
        method,
        &target,
        &[
            ("x-amz-content-sha256".into(), payload),
            ("x-amz-date".into(), stamp),
            ("Authorization".into(), auth),
        ],
        body,
    )
}

fn keys_in(listing: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(listing)
        .split("<Key>")
        .skip(1)
        .map(|k| k.split("</Key>").next().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_s3_sink_writes_gzip_objects_and_commits_only_what_is_stored() {
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
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        let bucket = format!("openduct-test-{}", std::process::id());
        let (status, _) = s3(&endpoint, &access, &secret, "PUT", &format!("/{bucket}"), &[], b"");
        assert_eq!(status, 200, "the test bucket");
        openduct(&a, &["topic", "create", "events", "--partitions", "2"], "");
        let input: String = (0..10).map(|i| format!("{{\"n\": {i}}}\n")).collect();
        openduct(&a, &["send", "events"], &input);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("lake.yaml");
        std::fs::write(
            &file,
            format!(
                "name: lake\nfrom:\n  topic: events\n  start: earliest\nto:\n  s3:\n    endpoint: {endpoint}\n    bucket: {bucket}\n    prefix: events/\n    access_key: secret:OPENDUCT_TEST_ACCESS\n    secret_key: secret:OPENDUCT_TEST_SECRET\n    every: 1s\n"
            ),
        )
        .unwrap();
        let env = [("OPENDUCT_TEST_ACCESS", access.as_str()), ("OPENDUCT_TEST_SECRET", secret.as_str())];
        let running = start(&a, &file, &env);
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut keys = Vec::new();
        while keys.is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(300));
            let (_, listing) = s3(&endpoint, &access, &secret, "GET", &format!("/{bucket}"), &[("list-type", "2"), ("prefix", "events/")], b"");
            keys = keys_in(&listing);
        }
        let log = stop(running);
        assert!(log.contains("read 10, wrote 10"), "{log}");
        assert_eq!(keys.len(), 1, "one object for the batch: {keys:?}");
        assert!(keys[0].starts_with("events/") && keys[0].ends_with(".jsonl.gz"), "{keys:?}");
        let (status, object) = s3(&endpoint, &access, &secret, "GET", &format!("/{bucket}/{}", keys[0]), &[], b"");
        assert_eq!(status, 200);
        let mut text = String::new();
        flate2::read::GzDecoder::new(object.as_slice()).read_to_string(&mut text).unwrap();
        let mut numbers: Vec<i64> = text.lines().map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["n"].as_i64().unwrap()).collect();
        numbers.sort_unstable();
        assert_eq!(numbers, (0..10).collect::<Vec<_>>(), "every message, once");

        // Committed once stored: a restart writes no second object.
        let running = start(&a, &file, &env);
        std::thread::sleep(Duration::from_secs(3));
        let log = stop(running);
        assert!(log.contains("read 0, wrote 0"), "{log}");
        let (_, listing) = s3(&endpoint, &access, &secret, "GET", &format!("/{bucket}"), &[("list-type", "2")], b"");
        assert_eq!(keys_in(&listing).len(), 1);
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

// ------------------------------------------------------------------------------------------------------ Elasticsearch

#[tokio::test(flavor = "multi_thread")]
async fn an_elasticsearch_sink_indexes_by_key_and_sets_refused_documents_aside() {
    let Ok(url) = std::env::var("OPENDUCT_TEST_ELASTICSEARCH") else {
        eprintln!("skipped: set OPENDUCT_TEST_ELASTICSEARCH (cargo xtask services up)");
        return;
    };
    let at = url.trim_start_matches("http://").to_owned();
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        let index = format!("openduct-test-{}", std::process::id());
        openduct(&a, &["topic", "create", "docs"], "");
        openduct(&a, &["topic", "create", "docs-refused"], "");
        // Two documents, one sent again with a new total (it replaces, by key), one that is not an object, and one
        // whose field contradicts the index's mapping.
        let input = "{\"id\": \"a\", \"total\": 1}\n{\"id\": \"b\", \"total\": 2}\n{\"id\": \"a\", \"total\": 3}\n[1, 2]\n{\"id\": \"c\", \"total\": \"lots\"}\n";
        openduct(&a, &["send", "docs"], input);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("search.yaml");
        std::fs::write(
            &file,
            format!(
                "name: search\nfrom:\n  topic: docs\n  start: earliest\nkey: \"type(value) == map ? id : null\"\nto:\n  elasticsearch:\n    url: {url}\n    index: {index}\non_error:\n  topic: docs-refused\n"
            ),
        )
        .unwrap();
        let running = start(&a, &file, &[]);
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut refused = String::new();
        while refused.lines().count() < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(300));
            refused = openduct(&a, &["read", "docs-refused", "--format", "json"], "");
        }
        let log = stop(running);
        assert!(log.contains("read 5, wrote 3, set aside 2"), "{log}");
        http(&at, "POST", &format!("/{index}/_refresh"), &[], b"");
        let (_, count) = http(&at, "GET", &format!("/{index}/_count"), &[], b"");
        let count: serde_json::Value = serde_json::from_slice(&count).unwrap();
        assert_eq!(count["count"], 2, "a and b: the second a replaced the first");
        let (_, doc) = http(&at, "GET", &format!("/{index}/_doc/a"), &[], b"");
        let doc: serde_json::Value = serde_json::from_slice(&doc).unwrap();
        assert_eq!(doc["_source"]["total"], 3);
        let reasons: Vec<String> = refused
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["headers"]["openduct-error"].as_str().unwrap().to_owned())
            .collect();
        assert!(reasons.iter().any(|r| r.contains("not a JSON object")), "{reasons:?}");
        assert!(reasons.iter().any(|r| r.contains("Elasticsearch refused the document")), "{reasons:?}");
        http(&at, "DELETE", &format!("/{index}"), &[], b"");
    })
    .await
    .unwrap();
    broker.shutdown().await;
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
