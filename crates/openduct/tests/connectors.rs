// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Connectors as a person runs them: `openduct pipeline run` with `from: http:` against a test server, and with
//! `from: webhook:` receiving deliveries.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use hmac::{KeyInit, Mac};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;
use sha2::Sha256;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

async fn broker() -> (Broker, String) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

fn openduct(addr: &str, args: &[&str]) -> (bool, String, String) {
    let out = Command::new(OPENDUCT)
        .args(args)
        .args(["--bootstrap", addr])
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// A running pipeline, with its log lines as they come.
struct Running {
    child: Child,
    log: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Running {
    fn start(addr: &str, file: &std::path::Path) -> Self {
        let mut child = Command::new(OPENDUCT)
            .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", addr])
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

    /// The first log line holding `text`, waiting for it.
    fn wait_for_log(&mut self, text: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
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
        assert!(
            status.success(),
            "the pipeline stopped with {status}: {:?}",
            self.seen
        );
        self.seen.join("\n")
    }
}

fn wait_for_values(addr: &str, topic: &str, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, out, _) = openduct(addr, &["read", topic]);
        let lines: Vec<String> = out.lines().map(str::to_owned).collect();
        if lines.len() >= want || Instant::now() > deadline {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// A tiny API: `GET /orders?since=N` answers the orders after N, two to a page, with a link to the next page.
fn order_api(orders: Arc<Mutex<Vec<u32>>>, asked: Arc<Mutex<Vec<String>>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut request = String::new();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            reader.read_line(&mut request).unwrap();
            let path = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
            asked.lock().unwrap().push(path.clone());
            let since: u32 = path
                .split("since=")
                .nth(1)
                .and_then(|s| s.split('&').next())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let after: Vec<u32> = orders
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|o| *o > since)
                .collect();
            let page: Vec<u32> = after.iter().copied().take(2).collect();
            let items: Vec<String> = page
                .iter()
                .map(|o| format!(r#"{{"seq":{o},"id":"order-{o}"}}"#))
                .collect();
            let next = match (after.len() > 2, page.last()) {
                (true, Some(last)) => format!(r#""/orders?since={last}""#),
                _ => "null".to_owned(),
            };
            let body = format!(r#"{{"items":[{}],"next":{next}}}"#, items.join(","));
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    addr
}

/// Asked to stop while it is still connecting - to a broker that takes the connection and never answers - a pipeline
/// stops at once, and cleanly, rather than being killed or waiting out its connection's timeout.
#[test]
fn a_pipeline_asked_to_stop_while_it_connects_stops_cleanly() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = silent.local_addr().unwrap().to_string();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("copy.yaml");
    std::fs::write(&file, "name: copy\nfrom:\n  topic: a\nto:\n  topic: b\n").unwrap();
    let running = Running::start(&addr, &file);
    // Its connection arriving means it is running and waiting on the broker (a fixed sleep is not enough: a newly
    // built program can take a while to start, as on a fresh CI machine).
    let (connection, _) = silent.accept().unwrap();
    let asked = Instant::now();
    running.stop();
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "it took {:?} to stop",
        asked.elapsed()
    );
    drop(connection);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_http_source_reads_every_page_and_carries_on_from_its_position() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "orders"]);
        let orders = Arc::new(Mutex::new(vec![1, 2, 3, 4, 5]));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let api = order_api(Arc::clone(&orders), Arc::clone(&asked));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("poll.yaml");
        std::fs::write(
            &file,
            format!(
                "name: poll\nfrom:\n  http:\n    url: http://{api}/orders?since={{position}}\n    every: 300ms\n    records: value.items\n    position: math.greatest(records.map(r, r.seq))\n    next: value.next\n    start: \"0\"\nmap:\n  id: id\nkey: id\nto:\n  topic: orders\n"
            ),
        )
        .unwrap();
        let running = Running::start(&a, &file);
        let got = wait_for_values(&a, "orders", 5);
        // The next poll finds nothing new: an empty page keeps the position and the pipeline carries on.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !asked.lock().unwrap().iter().any(|p| p == "/orders?since=5") && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        std::thread::sleep(Duration::from_millis(200));
        let log = running.stop();
        assert!(log.contains("reading http GET http://"), "{log}");
        let mut ids: Vec<String> = got.clone();
        ids.sort();
        assert_eq!(ids.len(), 5, "every page of the first poll: {got:?}");
        assert!(asked.lock().unwrap().iter().any(|p| p == "/orders?since=2"), "it followed the next link: {:?}", asked.lock().unwrap());

        // Two new orders, then a restart: it asks from the saved position and writes only the new ones.
        orders.lock().unwrap().extend([6, 7]);
        asked.lock().unwrap().clear();
        let running = Running::start(&a, &file);
        let got = wait_for_values(&a, "orders", 7);
        running.stop();
        assert_eq!(got.len(), 7, "nothing repeated: {got:?}");
        assert_eq!(asked.lock().unwrap().first().map(String::as_str), Some("/orders?since=5"));
        assert!(got.iter().any(|v| v.contains("order-7")));
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

/// POST `body` to the webhook, signed with `secret` if one is given; the status code answered.
fn post(at: &str, path: &str, body: &str, secret: Option<&str>) -> u16 {
    let mut stream = TcpStream::connect(at).unwrap();
    let signature = secret.map(|s| {
        let mut mac = hmac::Hmac::<Sha256>::new_from_slice(s.as_bytes()).unwrap();
        mac.update(body.as_bytes());
        let bytes = mac.finalize().into_bytes();
        format!(
            "X-Signature: sha256={}\r\n",
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        )
    });
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        signature.unwrap_or_default(),
        body.len()
    )
    .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    answer
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_webhook_accepts_a_signed_delivery_once_it_is_stored_and_refuses_the_rest() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "payments"]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hooks.yaml");
        std::fs::write(
            &file,
            "name: hooks\nfrom:\n  webhook:\n    listen: 127.0.0.1:0\n    path: /hooks/payments\n    secret: secret:OPENDUCT_TEST_HOOK_SECRET\n    max_body_kib: 1\nmap:\n  payment: id\n  cents: amount\nkey: payment\nto:\n  topic: payments\n",
        )
        .unwrap();
        let mut child = Command::new(OPENDUCT);
        let mut running = {
            let mut c = child
                .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", &a])
                .args(secrets_args(&file, &[("OPENDUCT_TEST_HOOK_SECRET", "s3cret")]))
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let stderr = c.stderr.take().unwrap();
            let (tx, log) = mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    let _ = tx.send(line);
                }
            });
            Running { child: c, log, seen: Vec::new() }
        };
        let line = running.wait_for_log("reading webhook POST http://");
        let at = line.split("http://").nth(1).unwrap().split('/').next().unwrap().to_owned();
        assert!(line.contains("(signed)"), "{line}");

        assert_eq!(post(&at, "/hooks/payments", r#"{"id":"pay-1","amount":1999}"#, Some("s3cret")), 202);
        // 202 means stored: it is already in the topic.
        let (_, stored, _) = openduct(&a, &["read", "payments"]);
        assert_eq!(stored, "{\"payment\":\"pay-1\",\"cents\":1999}\n");
        assert_eq!(post(&at, "/hooks/payments", r#"{"id":"pay-2"}"#, Some("wrong")), 401, "a forged signature");
        assert_eq!(post(&at, "/hooks/payments", r#"{"id":"pay-3"}"#, None), 401, "no signature");
        assert_eq!(post(&at, "/hooks/payments", "not json", Some("s3cret")), 400);
        assert_eq!(post(&at, "/elsewhere", "{}", Some("s3cret")), 404);
        assert_eq!(post(&at, "/hooks/payments", &format!(r#"{{"pad":"{}"}}"#, "x".repeat(2000)), Some("s3cret")), 413);
        let log = running.stop();
        let (_, stored, _) = openduct(&a, &["read", "payments"]);
        assert_eq!(stored.lines().count(), 1, "only the genuine delivery: {stored}");
        assert!(log.contains("read 1, wrote 1, set aside 0"), "{log}");
        assert!(!log.contains("s3cret"), "the secret never appears in the log");
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
