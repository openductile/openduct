// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The servicenow connector as a person runs it, against a stand-in for a ServiceNow instance's Table API that keeps
//! to its documented behaviour: basic and OAuth sign-in, encoded queries with `^NQ` and `ORDERBY`, `sysparm_limit`
//! and `sysparm_fields`, inserts, updates and lookups, and a data policy that refuses a record. A real instance is
//! used once one is given for tests.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
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

#[allow(dead_code)]
fn change(message: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(message["value"].as_str().unwrap()).unwrap()
}

/// What the stand-in instance holds and what it was asked.
#[derive(Default)]
struct Instance {
    /// Incidents: sys_id, sys_updated_on, fields.
    incidents: Vec<(String, String, serde_json::Value)>,
    tokens: Vec<String>,
    queries: Vec<String>,
    writes: Vec<String>,
}

fn respond(stream: &mut std::net::TcpStream, status: &str, body: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn decode(text: &str) -> String {
    let mut out = Vec::new();
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            out.push(u8::from_str_radix(&text[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(if b[i] == b'+' { b' ' } else { b[i] });
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn param(path: &str, name: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    query
        .split('&')
        .find_map(|p| p.strip_prefix(&format!("{name}=")).map(decode))
}

/// Whether a record meets one group of an encoded query (`a=b^c>d`), the way the stand-in needs it.
fn meets(record: &(String, String, serde_json::Value), group: &str) -> bool {
    group
        .split('^')
        .filter(|c| !c.is_empty() && !c.starts_with("ORDERBY"))
        .all(|c| {
            let (field, op, value) = if let Some((f, v)) = c.split_once('>') {
                (f, '>', v)
            } else if let Some((f, v)) = c.split_once('=') {
                (f, '=', v)
            } else {
                return true;
            };
            let actual = match field {
                "sys_id" => record.0.clone(),
                "sys_updated_on" => record.1.clone(),
                other => record
                    .2
                    .get(other)
                    .map(|v| v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string()))
                    .unwrap_or_default(),
            };
            if op == '>' {
                actual.as_str() > value
            } else {
                actual == value
            }
        })
}

fn servicenow(instance: Arc<Mutex<Instance>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let mut words = first.split_whitespace();
            let (method, path) = (
                words.next().unwrap_or("").to_owned(),
                words.next().unwrap_or("").to_owned(),
            );
            let (mut length, mut auth) = (0usize, String::new());
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                let (name, value) = line.split_once(':').unwrap_or((line, ""));
                match name.to_ascii_lowercase().as_str() {
                    "content-length" => length = value.trim().parse().unwrap_or(0),
                    "authorization" => auth = value.trim().to_owned(),
                    _ => {}
                }
            }
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).unwrap();
            let body = String::from_utf8_lossy(&body).into_owned();
            let mut inst = instance.lock().unwrap();
            if path == "/oauth_token.do" {
                let field = |n: &str| {
                    body.split('&')
                        .find_map(|p| p.strip_prefix(&format!("{n}=")).map(decode))
                };
                let ok = field("client_id").as_deref() == Some("app")
                    && field("client_secret").as_deref() == Some("app-secret")
                    && (field("grant_type").as_deref() == Some("client_credentials")
                        || (field("username").as_deref() == Some("integration")
                            && field("password").as_deref() == Some("sn-secret")));
                if ok {
                    let token = format!("tok-{}", inst.tokens.len() + 1);
                    inst.tokens.push(token.clone());
                    respond(
                        &mut stream,
                        "200 OK",
                        &format!(r#"{{"access_token":"{token}","token_type":"Bearer","expires_in":1799}}"#),
                    );
                } else {
                    respond(
                        &mut stream,
                        "401 Unauthorized",
                        r#"{"error":"access_denied","error_description":"access_denied"}"#,
                    );
                }
                continue;
            }
            let basic = format!("Basic {}", STANDARD.encode("integration:sn-secret"));
            let bearer_ok = auth
                .strip_prefix("Bearer ")
                .is_some_and(|t| inst.tokens.iter().any(|k| k == t));
            if auth != basic && !bearer_ok {
                respond(
                    &mut stream,
                    "401 Unauthorized",
                    r#"{"error":{"message":"User Not Authenticated","detail":"Required to provide Auth information"},"status":"failure"}"#,
                );
                continue;
            }
            let table_path = path.split('?').next().unwrap_or("").to_owned();
            if method == "GET" && table_path == "/api/now/table/incident" {
                let query = param(&path, "sysparm_query").unwrap_or_default();
                inst.queries.push(query.clone());
                let limit: usize = param(&path, "sysparm_limit")
                    .and_then(|l| l.parse().ok())
                    .unwrap_or(10_000);
                let fields: Option<Vec<String>> =
                    param(&path, "sysparm_fields").map(|f| f.split(',').map(str::to_owned).collect());
                let groups: Vec<&str> = query.split("^NQ").collect();
                let mut rows: Vec<_> = inst
                    .incidents
                    .iter()
                    .filter(|r| groups.iter().any(|g| meets(r, g)))
                    .cloned()
                    .collect();
                rows.sort_by(|a, b| (&a.1, &a.0).cmp(&(&b.1, &b.0)));
                rows.truncate(limit);
                let result: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|(id, updated, f)| {
                        let mut all = f.clone();
                        all["sys_id"] = serde_json::json!(id);
                        all["sys_updated_on"] = serde_json::json!(updated);
                        match &fields {
                            Some(want) => serde_json::Value::Object(
                                want.iter()
                                    .filter_map(|w| all.get(w).map(|v| (w.clone(), v.clone())))
                                    .collect(),
                            ),
                            None => all,
                        }
                    })
                    .collect();
                respond(
                    &mut stream,
                    "200 OK",
                    &serde_json::json!({ "result": result }).to_string(),
                );
            } else if (method == "POST" && table_path == "/api/now/table/incident")
                || (method == "PATCH" && table_path.starts_with("/api/now/table/incident/"))
            {
                let record: serde_json::Value = serde_json::from_str(&body).unwrap();
                if method == "POST" && record.get("short_description").is_none() {
                    respond(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":{"message":"Operation Failed","detail":"Data Policy Exception: The following fields are mandatory: Short description"},"status":"failure"}"#,
                    );
                    continue;
                }
                inst.writes.push(format!("{method} {table_path}"));
                let n = inst.incidents.len() + 1;
                let id = match table_path.rsplit('/').next() {
                    Some(id) if method == "PATCH" => id.to_owned(),
                    _ => format!("{n:032x}"),
                };
                let updated = "2026-11-15 10:00:00".to_owned();
                if let Some(existing) = inst.incidents.iter_mut().find(|r| r.0 == id) {
                    for (k, v) in record.as_object().unwrap() {
                        existing.2[k] = v.clone();
                    }
                    existing.1 = updated;
                } else {
                    inst.incidents.push((id.clone(), updated, record));
                }
                respond(
                    &mut stream,
                    if method == "POST" { "201 Created" } else { "200 OK" },
                    &format!(r#"{{"result":{{"sys_id":"{id}"}}}}"#),
                );
            } else {
                respond(
                    &mut stream,
                    "400 Bad Request",
                    r#"{"error":{"message":"Invalid table","detail":""},"status":"failure"}"#,
                );
            }
        }
    });
    base
}

fn file(dir: &tempfile::TempDir, name: &str, text: &str) -> std::path::PathBuf {
    let f = dir.path().join(format!("{name}.yaml"));
    std::fs::write(&f, text).unwrap();
    f
}

fn incident(n: u32, updated: &str, active: bool) -> (String, String, serde_json::Value) {
    (
        format!("{n:032x}"),
        updated.to_owned(),
        serde_json::json!({ "number": format!("INC{n:07}"), "short_description": format!("issue {n}"), "active": active.to_string() }),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn incidents_are_read_as_they_change_without_skipping_or_repeating() {
    let instance = Arc::new(Mutex::new(Instance::default()));
    instance.lock().unwrap().incidents = vec![
        incident(1, "2026-11-15 09:00:00", true),
        incident(2, "2026-11-15 09:05:00", true),
        incident(3, "2026-11-15 09:05:00", false),
    ];
    let url = servicenow(Arc::clone(&instance));
    let (local, here) = broker().await;
    let (h, inst) = (here.clone(), Arc::clone(&instance));
    tokio::task::spawn_blocking(move || {
        for t in ["incidents", "active"] {
            openduct(&h, &["topic", "create", t]);
        }
        let dir = tempfile::tempdir().unwrap();
        let sign_in = format!("    instance: {url}\n    username: integration\n    password: secret:SN_PASSWORD\n");
        let f = file(&dir, "all", &format!("name: incidents-in\nfrom:\n  servicenow:\n{sign_in}    table: incident\n    every: 5s\nto:\n  topic: incidents\n"));
        let env = [("SN_PASSWORD", "sn-secret")];
        let running = Running::start(&h, &f, &env);
        let got = wait_for(&h, "incidents", 3);
        let log = running.stop();
        assert_eq!(got.len(), 3, "{log}");
        let first = change(&got[0]);
        assert_eq!((first["op"].as_str(), first["after"]["number"].as_str()), (Some("upsert"), Some("INC0000001")));
        assert_eq!(got[0]["key"], format!("incident/{:032x}", 1));
        {
            let mut i = inst.lock().unwrap();
            i.incidents.push(incident(4, "2026-11-15 09:05:00", true));
            i.incidents.push(incident(5, "2026-11-15 09:10:00", true));
        }
        let running = Running::start(&h, &f, &env);
        wait_for(&h, "incidents", 5);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        let got = wait_for(&h, "incidents", 5);
        let numbers: Vec<String> = got.iter().map(|m| change(m)["after"]["number"].as_str().unwrap().to_owned()).collect();
        assert_eq!(numbers, ["INC0000001", "INC0000002", "INC0000003", "INC0000004", "INC0000005"], "{log}");
        let id3 = format!("{:032x}", 3);
        assert!(
            inst.lock().unwrap().queries.iter().any(|q| q.contains(&format!("sys_updated_on>2026-11-15 09:05:00^NQsys_updated_on=2026-11-15 09:05:00^sys_id>{id3}"))),
            "{:?}",
            inst.lock().unwrap().queries
        );

        // Only active ones, and only some fields: the pipeline's conditions hold in both halves of the position.
        let f = file(&dir, "active", &format!("name: active-in\nfrom:\n  servicenow:\n{sign_in}    table: incident\n    query: active=true\n    fields: [number, short_description]\nto:\n  topic: active\n"));
        let running = Running::start(&h, &f, &env);
        let got = wait_for(&h, "active", 4);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        let got_all = wait_for(&h, "active", 4);
        assert_eq!((got.len(), got_all.len()), (4, 4), "{log}");
        let one = change(&got_all[0]);
        assert!(one["after"].get("active").is_none(), "only the fields asked for: {one}");
        assert!(one["after"]["sys_id"].is_string());
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn incidents_are_upserted_with_oauth_and_a_refused_one_is_set_aside() {
    let instance = Arc::new(Mutex::new(Instance::default()));
    let url = servicenow(Arc::clone(&instance));
    let (local, here) = broker().await;
    let (h, inst) = (here.clone(), Arc::clone(&instance));
    tokio::task::spawn_blocking(move || {
        for t in ["alerts", "alerts-refused"] {
            openduct(&h, &["topic", "create", t]);
        }
        openduct(&h, &["send", "alerts", r#"{"correlation_id":"alarm-7","short_description":"Press 7 overheating","urgency":"1"}"#]);
        openduct(&h, &["send", "alerts", r#"{"correlation_id":"alarm-7","short_description":"Press 7 overheating","urgency":"2"}"#]);
        openduct(&h, &["send", "alerts", r#"{"correlation_id":"alarm-9"}"#]);
        openduct(&h, &["send", "alerts", r#"["not", "a", "record"]"#]);
        let dir = tempfile::tempdir().unwrap();
        let f = file(
            &dir,
            "out",
            &format!(
                "name: alerts-out\nfrom:\n  topic: alerts\n  start: earliest\nto:\n  servicenow:\n    instance: {url}\n    client_id: app\n    client_secret: secret:SN_APP_SECRET\n    username: integration\n    password: secret:SN_PASSWORD\n    table: incident\n    mode: upsert\n    match: correlation_id\non_error:\n  topic: alerts-refused\n"
            ),
        );
        let running = Running::start(&h, &f, &[("SN_APP_SECRET", "app-secret"), ("SN_PASSWORD", "sn-secret")]);
        let refused = wait_for(&h, "alerts-refused", 2);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        assert!(log.contains("read 4, wrote 2, set aside 2"), "{log}");
        let reasons: Vec<String> = refused.iter().map(|m| m.to_string()).collect();
        assert!(reasons.iter().any(|r| r.contains("mandatory: Short description")), "{reasons:?}");
        assert!(reasons.iter().any(|r| r.contains("not a JSON object")), "{reasons:?}");
        let i = inst.lock().unwrap();
        // The first made the incident; the second, with the same correlation id, changed it.
        assert_eq!(i.writes.len(), 2, "{:?}", i.writes);
        assert!(i.writes[0].starts_with("POST") && i.writes[1].starts_with("PATCH"), "{:?}", i.writes);
        assert_eq!(i.incidents.len(), 1);
        assert_eq!(i.incidents[0].2["urgency"], "2");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_password_stops_with_the_reason() {
    let instance = Arc::new(Mutex::new(Instance::default()));
    let url = servicenow(Arc::clone(&instance));
    let (local, here) = broker().await;
    let h = here.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "nothing"]);
        let dir = tempfile::tempdir().unwrap();
        let wrong = "not-the-password-6613";
        let f = file(&dir, "wrong", &format!("name: wrong-in\nfrom:\n  servicenow:\n    instance: {url}\n    username: integration\n    password: secret:SN_PASSWORD\n    table: incident\nto:\n  topic: nothing\n"));
        let (ok, log) = Running::start(&h, &f, &[("SN_PASSWORD", wrong)]).ended();
        assert!(!ok && log.contains("ServiceNow refused the username or password"), "{log}");
        assert!(!log.contains(wrong), "the password is never shown: {log}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[test]
fn settings_that_cannot_work_are_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    let check = |name: &str, side: &str, body: &str| {
        let text = if side == "from" {
            format!("name: {name}\nfrom:\n  servicenow:\n{body}to:\n  topic: t\n")
        } else {
            format!("name: {name}\nfrom:\n  topic: t\nto:\n  servicenow:\n{body}")
        };
        let f = file(&dir, name, &text);
        let out = Command::new(OPENDUCT)
            .args(["pipeline", "check", f.to_str().unwrap()])
            .output()
            .unwrap();
        (
            out.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    };
    let sign_in = "    instance: https://acme.service-now.com\n    username: secret:SN_USER\n    password: secret:SN_PASSWORD\n";
    let (ok, text) = check(
        "incidents",
        "from",
        &format!("{sign_in}    table: incident\n    query: active=true^priority=1\n"),
    );
    assert!(
        ok && text.contains("reads servicenow incident on acme.service-now.com every 60s"),
        "{text}"
    );
    let (ok, text) = check(
        "alerts",
        "to",
        &format!("{sign_in}    table: incident\n    mode: upsert\n    match: correlation_id\n"),
    );
    assert!(
        ok && text.contains("writes servicenow incident on acme.service-now.com (upsert by correlation_id)"),
        "{text}"
    );
    for (name, side, body, why) in [
        (
            "http",
            "from",
            "    instance: http://acme.service-now.com\n    username: u\n    password: p\n    table: incident\n",
            "is the instance's address",
        ),
        (
            "half",
            "from",
            "    instance: https://acme.service-now.com\n    username: u\n    table: incident\n",
            "give username and password",
        ),
        (
            "table",
            "from",
            &format!("{sign_in}    table: Incident\n"),
            "is a table's name",
        ),
        (
            "order",
            "from",
            &format!("{sign_in}    table: incident\n    query: active=true^ORDERBYnumber\n"),
            "conditions only",
        ),
        (
            "nomatch",
            "to",
            &format!("{sign_in}    table: incident\n    mode: upsert\n"),
            "match: is required to upsert",
        ),
    ] {
        let (ok, text) = check(name, side, body);
        assert!(!ok && text.contains(why), "{name}: {text}");
    }
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
