// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The salesforce connector as a person runs it, against a stand-in for Salesforce's REST API that keeps to its
//! documented behaviour: OAuth 2.0 sign-in (client credentials, and a JWT whose signature it checks), describe,
//! `queryAll` with SOQL positions and `LIMIT`, sObject Collections upserts with per-record errors, and sessions
//! that end. A real org is used once one is given for tests.

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
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

/// A key made for these tests only: it signs nothing anywhere else.
const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCTCNDQTyB5Vl1s\nJR9/C2/p/o7FppUwOfMNV8pJFHTbL1wLwzBM3a9HYguhiTmM06mCpmQ+cXSysOik\nMRaSC+KmytOsKh36shKe5YHBofAIgYwXfrntYaPj90jl7f1IckIYKgKFLXyjzrhY\n0vZNr03Gc4JxB5XADr5YzXhIXovVp9kFKy7XBPeNoBGlOR1nOgGizh5WQDS6meVy\nWVddfKWaECTsOMJqqW0GQaB9ax+H31Wo1aYA/TprHFuDU4aSpkxbi6uDGZmtrYhy\nyDxJAAH+pK5vYQNSXM+8z5AE29Q/hEL3OslUND85GqjrZ4qKBmrCRTvJMW0uzQpx\nSJLGnn43AgMBAAECggEAAy8PBLO5CoQGdkwj71hTKrn1R0NEnfcqYfKxmODY9u70\nfRIMfpZ+TLQSW4PEu7MJoRIr+KG7aSwI1nqtMRuDccvRqnS2IJY+/IgQKXNxkWEj\n3TkLfoOEheHZzkDJdl+xOkBHg+nadfbsaacWBH7XuoTPkyr+byHGJyxWfuHJq5gC\ncl9cIm+NCgOTz0vAGRvS/gxiGq0EZ8WuggZNYeDSUupuW5Xsd/lXhEIXQZ+A9/NC\nDPr0IIpfOmw6RkrdN4NW4OffhY+qj9WTWdNcJ95iWWD3MVMJQoL3ozql5/zRBL0/\naD/s0fQUhuZsD/33pOlToxNK8zjxyoIj4h/4ZviGgQKBgQDIrJqzVlGQHrQkNUNx\npxobv/RTnFqCq03zRNz0O2tp4YHqiOmN5wR5tzDLWPlO4kqD/C+VILiUGR58B+D7\nNQoGi5k8HtHM1iF9DI+vsYXp3t40b9J20C3R7X7bQ2Vs7rpfk00UY2AQ8mYZKhf9\nUZff363lCs6KxkBiErtYTstZpwKBgQC7kl2ZkLZilLu72gfmXTPDZMRwDqQJjI4U\ni7TG/8FsHTMo3djGu4VK+Pz03P3Qlb7zdyaguMWdCXNx1CgR3zvfEDt6W4cJTdxy\nc9e9o0kbnUsQUJzqRmlz1SgXxyUVqCv/GfeJJZUJQj6mfYRfZusyTIkaZiHW9jtU\nTDfJXwso8QKBgQC5mNHXjb0P4yXO2yhOhDMkeSt67P/VHKSSmx0UEaStn7CtfwUr\nhf9gpypM2DR0Xdx3J5XGgiXUa0sDgbH2BD8KgLAvp314yo7cffKilzh4bfTrYISx\n+KZ4IwSRvEqhLkZXQQAoHCTsZ5RAL+tSEdW6a8EENjOCrAs96X82FGIEvwKBgCF9\nN3L0kGu3bHFUszZhC53mPuBW7Ni7nJ50dxlUjn80E29HdK2+y0SggeFiF3sHUWl8\nq4vkkXvBNThsjZ7rQXBJIKVu9AQoP6S7pBg6sBHbGozWJ734RlmlBByCtM9SEFDi\nnPyClkJp7m+hV2IFKRb47T2mYIJVwjABz7AHZSGxAoGAblfTIRecygU57lnU5UyN\n6Z8eLjaBtXG0QzfmsRuq5aeIq06Kt/Ffd2b4gEJzMSbA4dibSnGPiAv3FP0yLQH0\nCDIdpv95g1URSVZeMl8cYt3cNC+dC7zO/wnfsmMNGEblq/LXWMpmNeX37Yyh2d9w\n/bc6T5JQRn2hs40ZTWiB23A=\n-----END PRIVATE KEY-----";
/// Its public half (PKCS #1), as the connected app in the stand-in knows it.
const TEST_PUBLIC: &str = "MIIBCgKCAQEAkwjQ0E8geVZdbCUffwtv6f6OxaaVMDnzDVfKSRR02y9cC8MwTN2vR2ILoYk5jNOpgqZkPnF0srDopDEWkgvipsrTrCod+rISnuWBwaHwCIGMF3657WGj4/dI5e39SHJCGCoChS18o864WNL2Ta9NxnOCcQeVwA6+WM14SF6L1afZBSsu1wT3jaARpTkdZzoBos4eVkA0upnlcllXXXylmhAk7DjCaqltBkGgfWsfh99VqNWmAP06axxbg1OGkqZMW4urgxmZra2Icsg8SQAB/qSub2EDUlzPvM+QBNvUP4RC9zrJVDQ/ORqo62eKigZqwkU7yTFtLs0KcUiSxp5+NwIDAQAB";

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

fn change(message: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(message["value"].as_str().unwrap()).unwrap()
}

/// What the stand-in org holds and what it was asked.
#[derive(Default)]
struct Org {
    /// Accounts: (SystemModstamp, Id, Name, deleted).
    accounts: Vec<(String, String, String, bool)>,
    contacts: Vec<serde_json::Value>,
    tokens: Vec<String>,
    /// The next API call with a good token is told the session has ended.
    expire: bool,
    sign_ins: Vec<String>,
    queries: Vec<String>,
    port: u16,
    /// The one refresh token that works now; each refresh replaces it (rotation), and an old one is refused.
    refresh: String,
    /// The refresh tokens asked with, in order.
    refreshes: Vec<String>,
}

fn respond(stream: &mut std::net::TcpStream, status: &str, body: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn form_value(form: &str, name: &str) -> Option<String> {
    form.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == name).then(|| percent_decode(v))
    })
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                out.push(u8::from_str_radix(&text[i + 1..i + 3], 16).unwrap());
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

/// Whether a JWT bearer assertion is signed by the test key and is for this connected app, user and audience.
fn jwt_ok(assertion: &str, audience: &str) -> bool {
    let parts: Vec<&str> = assertion.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    let public = STANDARD.decode(TEST_PUBLIC).unwrap();
    let key = ring::signature::UnparsedPublicKey::new(&ring::signature::RSA_PKCS1_2048_8192_SHA256, public);
    let signed = format!("{}.{}", parts[0], parts[1]);
    let Ok(signature) = URL_SAFE_NO_PAD.decode(parts[2]) else {
        return false;
    };
    if key.verify(signed.as_bytes(), &signature).is_err() {
        return false;
    }
    let claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    claims["iss"] == "test-client" && claims["sub"] == "integration@acme.test" && claims["aud"] == audience
}

/// The stand-in org, on a port of its own; it serves until the test ends.
fn salesforce(org: Arc<Mutex<Org>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    org.lock().unwrap().port = port;
    let base = format!("http://127.0.0.1:{port}");
    let b = base.clone();
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
            let (mut length, mut bearer) = (0usize, String::new());
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
                    "authorization" => bearer = value.trim().trim_start_matches("Bearer ").to_owned(),
                    _ => {}
                }
            }
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).unwrap();
            let body = String::from_utf8_lossy(&body).into_owned();
            let mut org = org.lock().unwrap();
            if path == "/services/oauth2/token" {
                let grant = form_value(&body, "grant_type").unwrap_or_default();
                org.sign_ins.push(grant.clone());
                let ok = match grant.as_str() {
                    "client_credentials" => {
                        form_value(&body, "client_id").as_deref() == Some("test-client")
                            && form_value(&body, "client_secret").as_deref() == Some("test-secret")
                    }
                    "urn:ietf:params:oauth:grant-type:jwt-bearer" => {
                        jwt_ok(&form_value(&body, "assertion").unwrap_or_default(), &b)
                    }
                    "refresh_token" => {
                        let given = form_value(&body, "refresh_token").unwrap_or_default();
                        org.refreshes.push(given.clone());
                        if given.is_empty() || given != org.refresh {
                            respond(
                                &mut stream,
                                "400 Bad Request",
                                r#"{"error":"invalid_grant","error_description":"expired access/refresh token"}"#,
                            );
                            continue;
                        }
                        form_value(&body, "client_id").as_deref() == Some("test-client")
                    }
                    _ => false,
                };
                if ok {
                    let token = format!("tok-{}", org.tokens.len() + 1);
                    org.tokens.push(token.clone());
                    let rotated = if grant == "refresh_token" {
                        org.refresh = format!("rt-{}", org.refreshes.len());
                        format!(r#","refresh_token":"{}""#, org.refresh)
                    } else {
                        String::new()
                    };
                    respond(
                        &mut stream,
                        "200 OK",
                        &format!(
                            r#"{{"access_token":"{token}","instance_url":"{b}","token_type":"Bearer"{rotated}}}"#
                        ),
                    );
                } else {
                    respond(
                        &mut stream,
                        "400 Bad Request",
                        r#"{"error":"invalid_client","error_description":"invalid client credentials"}"#,
                    );
                }
                continue;
            }
            if !org.tokens.contains(&bearer) || org.expire {
                org.expire = false;
                org.tokens.retain(|t| *t != bearer);
                respond(
                    &mut stream,
                    "401 Unauthorized",
                    r#"[{"message":"Session expired or invalid","errorCode":"INVALID_SESSION_ID"}]"#,
                );
                continue;
            }
            if path == "/services/data/v64.0/sobjects/Account/describe" {
                let fields = [
                    "Id:id",
                    "Name:string",
                    "SystemModstamp:datetime",
                    "IsDeleted:boolean",
                    "Logo:base64",
                ]
                .map(|f| {
                    let (n, t) = f.split_once(':').unwrap();
                    format!(r#"{{"name":"{n}","type":"{t}"}}"#)
                });
                respond(
                    &mut stream,
                    "200 OK",
                    &format!(r#"{{"name":"Account","fields":[{}]}}"#, fields.join(",")),
                );
            } else if let Some(q) = path.strip_prefix("/services/data/v64.0/queryAll?q=") {
                let soql = percent_decode(q);
                org.queries.push(soql.clone());
                assert!(
                    !soql.contains("Logo"),
                    "files' contents are not asked for: {soql}"
                );
                // The position, from `SystemModstamp > T OR (SystemModstamp = T AND Id > 'X')`.
                let after = soql.split("SystemModstamp > ").nth(1).map(|rest| {
                    let t = rest.split(' ').next().unwrap().trim_end_matches('Z').to_owned();
                    let id = rest
                        .split("Id > '")
                        .nth(1)
                        .unwrap()
                        .split('\'')
                        .next()
                        .unwrap()
                        .to_owned();
                    (t, id)
                });
                let limit: usize = soql.rsplit("LIMIT ").next().unwrap().trim().parse().unwrap();
                let mut rows: Vec<_> = org
                    .accounts
                    .iter()
                    .filter(|(s, id, _, _)| {
                        after
                            .as_ref()
                            .is_none_or(|(t, x)| (&s[..19], id.as_str()) > (t.as_str(), x.as_str()))
                    })
                    .cloned()
                    .collect();
                rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
                rows.truncate(limit);
                let records: Vec<String> = rows
                    .iter()
                    .map(|(s, id, name, deleted)| {
                        format!(
                            r#"{{"attributes":{{"type":"Account","url":"/services/data/v64.0/sobjects/Account/{id}"}},"Id":"{id}","Name":"{name}","SystemModstamp":"{s}","IsDeleted":{deleted}}}"#
                        )
                    })
                    .collect();
                respond(
                    &mut stream,
                    "200 OK",
                    &format!(
                        r#"{{"totalSize":{},"done":true,"records":[{}]}}"#,
                        records.len(),
                        records.join(",")
                    ),
                );
            } else if method == "PATCH" && path == "/services/data/v64.0/composite/sobjects/Contact/Email" {
                let request: serde_json::Value = serde_json::from_str(&body).unwrap();
                assert_eq!(request["allOrNone"], false);
                let results: Vec<String> = request["records"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| {
                        assert_eq!(r["attributes"]["type"], "Contact");
                        if r.get("LastName").is_none() {
                            r#"{"success":false,"errors":[{"statusCode":"REQUIRED_FIELD_MISSING","message":"Required fields are missing: [LastName]","fields":["LastName"]}]}"#.to_owned()
                        } else {
                            org.contacts.push(r.clone());
                            r#"{"id":"003Hp00001AbCdEIAV","success":true,"errors":[],"created":true}"#.to_owned()
                        }
                    })
                    .collect();
                respond(&mut stream, "200 OK", &format!("[{}]", results.join(",")));
            } else {
                respond(
                    &mut stream,
                    "404 Not Found",
                    r#"[{"message":"The requested resource does not exist","errorCode":"NOT_FOUND"}]"#,
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

#[tokio::test(flavor = "multi_thread")]
async fn accounts_are_read_as_they_change_without_skipping_or_repeating_and_through_an_ended_session() {
    let org = Arc::new(Mutex::new(Org::default()));
    {
        let mut o = org.lock().unwrap();
        o.accounts = vec![
            (
                "2026-11-15T09:00:00.000+0000".into(),
                "001A00000000001AAA".into(),
                "Acme".into(),
                false,
            ),
            (
                "2026-11-15T09:05:00.000+0000".into(),
                "001A00000000002AAA".into(),
                "Globex".into(),
                false,
            ),
            (
                "2026-11-15T09:05:00.000+0000".into(),
                "001A00000000003AAA".into(),
                "Initech".into(),
                false,
            ),
        ];
    }
    let url = salesforce(Arc::clone(&org));
    let (local, here) = broker().await;
    let (h, o) = (here.clone(), Arc::clone(&org));
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "accounts"]);
        let dir = tempfile::tempdir().unwrap();
        let f = file(
            &dir,
            "accounts",
            &format!(
                "name: accounts-in\nfrom:\n  salesforce:\n    login_url: {url}\n    client_id: test-client\n    client_secret: secret:SF_SECRET\n    object: Account\n    every: 5s\nto:\n  topic: accounts\n"
            ),
        );
        let env = [("SF_SECRET", "test-secret")];
        let running = Running::start(&h, &f, &env);
        let got = wait_for(&h, "accounts", 3);
        let log = running.stop();
        assert_eq!(got.len(), 3, "{log}");
        let first = change(&got[0]);
        assert_eq!(first["op"], "upsert");
        assert_eq!(first["after"]["Name"], "Acme");
        assert!(first["after"].get("attributes").is_none());
        assert_eq!(got[0]["key"], "Account/001A00000000001AAA");

        // While it is stopped: a record changed at the same second as the last one read (after it by id), a
        // deletion, and a session that has ended.
        {
            let mut o = o.lock().unwrap();
            o.accounts.push(("2026-11-15T09:05:00.000+0000".into(), "001A00000000004AAA".into(), "Umbrella".into(), false));
            o.accounts.push(("2026-11-15T09:10:00.000+0000".into(), "001A00000000001AAA".into(), "Acme".into(), true));
            o.accounts.retain(|a| !(a.1 == "001A00000000001AAA" && !a.3));
            o.expire = true;
        }
        let running = Running::start(&h, &f, &env);
        wait_for(&h, "accounts", 5);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        let got = wait_for(&h, "accounts", 5);
        assert_eq!(got.len(), 5, "nothing skipped, nothing twice: {log}");
        assert_eq!(change(&got[3])["after"]["Name"], "Umbrella");
        let deleted = change(&got[4]);
        assert_eq!((deleted["op"].as_str(), deleted["before"]["Name"].as_str()), (Some("delete"), Some("Acme")));
        let o = o.lock().unwrap();
        assert!(o.sign_ins.len() >= 3, "signed in again after the session ended: {:?}", o.sign_ins);
        assert!(o.queries.iter().any(|q| q.contains("SystemModstamp > 2026-11-15T09:05:00Z OR (SystemModstamp = 2026-11-15T09:05:00Z AND Id > '001A00000000003AAA')")), "{:?}", o.queries);
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn contacts_are_upserted_by_external_id_and_a_refused_one_is_set_aside() {
    let org = Arc::new(Mutex::new(Org::default()));
    let url = salesforce(Arc::clone(&org));
    let (local, here) = broker().await;
    let (h, o) = (here.clone(), Arc::clone(&org));
    tokio::task::spawn_blocking(move || {
        for t in ["people", "people-refused"] {
            openduct(&h, &["topic", "create", t]);
        }
        openduct(&h, &["send", "people", r#"{"Email":"ada@acme.test","LastName":"Lovelace","FirstName":"Ada"}"#]);
        openduct(&h, &["send", "people", r#"{"Email":"no-name@acme.test"}"#]);
        openduct(&h, &["send", "people", r#"{"Email":"alan@acme.test","LastName":"Turing"}"#]);
        openduct(&h, &["send", "people", r#"{"LastName":"No email"}"#]);
        let dir = tempfile::tempdir().unwrap();
        let f = file(
            &dir,
            "contacts",
            &format!(
                "name: contacts-out\nfrom:\n  topic: people\n  start: earliest\nto:\n  salesforce:\n    login_url: {url}\n    client_id: test-client\n    client_secret: secret:SF_SECRET\n    object: Contact\n    external_id: Email\non_error:\n  topic: people-refused\n"
            ),
        );
        let running = Running::start(&h, &f, &[("SF_SECRET", "test-secret")]);
        let refused = wait_for(&h, "people-refused", 2);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        assert!(log.contains("read 4, wrote 2, set aside 2"), "{log}");
        let reasons: Vec<String> = refused.iter().map(|m| m.to_string()).collect();
        assert!(reasons.iter().any(|r| r.contains("REQUIRED_FIELD_MISSING")), "{reasons:?}");
        assert!(reasons.iter().any(|r| r.contains("has no Email")), "{reasons:?}");
        let o = o.lock().unwrap();
        let emails: Vec<&str> = o.contacts.iter().map(|c| c["Email"].as_str().unwrap()).collect();
        assert_eq!(emails, ["ada@acme.test", "alan@acme.test"]);
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_jwt_signs_in_and_a_wrong_secret_stops_with_the_reason() {
    let org = Arc::new(Mutex::new(Org::default()));
    org.lock().unwrap().accounts.push((
        "2026-11-15T09:00:00.000+0000".into(),
        "001A00000000001AAA".into(),
        "Acme".into(),
        false,
    ));
    let url = salesforce(Arc::clone(&org));
    let (local, here) = broker().await;
    let (h, o) = (here.clone(), Arc::clone(&org));
    tokio::task::spawn_blocking(move || {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("sf.key");
        std::fs::write(&key, TEST_KEY).unwrap();
        openduct(&h, &["topic", "create", "jwt-accounts"]);
        let f = file(
            &dir,
            "jwt",
            &format!(
                "name: jwt-in\nfrom:\n  salesforce:\n    login_url: {url}\n    client_id: test-client\n    username: integration@acme.test\n    private_key: file:{}\n    object: Account\nto:\n  topic: jwt-accounts\n",
                key.display()
            ),
        );
        let running = Running::start(&h, &f, &[]);
        let got = wait_for(&h, "jwt-accounts", 1);
        let log = running.stop();
        assert_eq!(got.len(), 1, "{log}");
        assert!(o.lock().unwrap().sign_ins.iter().all(|g| g.contains("jwt-bearer")));

        let wrong = "not-the-secret-2268";
        let f = file(
            &dir,
            "wrong",
            &format!(
                "name: wrong-in\nfrom:\n  salesforce:\n    login_url: {url}\n    client_id: test-client\n    client_secret: secret:SF_SECRET\n    object: Account\nto:\n  topic: jwt-accounts\n"
            ),
        );
        let (ok, log) = Running::start(&h, &f, &[("SF_SECRET", wrong)]).ended();
        assert!(!ok && log.contains("Salesforce refused signing in: invalid client credentials"), "{log}");
        assert!(!log.contains(wrong), "the secret is never shown: {log}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

/// Salesforce rotates refresh tokens (it requires rotation of External Client Apps): each refresh answers the next
/// one and the one used stops working. A running pipeline keeps the next one, so a session that ends signs in again;
/// started again with the token it began with - used up - it stops and says why, showing no secret.
#[tokio::test(flavor = "multi_thread")]
async fn a_rotated_refresh_token_is_kept_while_running_and_a_used_one_is_explained() {
    let org = Arc::new(Mutex::new(Org::default()));
    {
        let mut o = org.lock().unwrap();
        o.refresh = "rt-0".into();
        o.accounts.push((
            "2026-11-15T09:00:00.000+0000".into(),
            "001A00000000001AAA".into(),
            "Acme".into(),
            false,
        ));
    }
    let url = salesforce(Arc::clone(&org));
    let (local, here) = broker().await;
    let (h, o) = (here.clone(), Arc::clone(&org));
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "rotated-accounts"]);
        let dir = tempfile::tempdir().unwrap();
        let f = file(
            &dir,
            "rotated",
            &format!(
                "name: rotated-in\nfrom:\n  salesforce:\n    login_url: {url}\n    client_id: test-client\n    refresh_token: secret:SF_REFRESH\n    object: Account\n    every: 5s\nto:\n  topic: rotated-accounts\n"
            ),
        );
        let env = [("SF_REFRESH", "rt-0")];
        let running = Running::start(&h, &f, &env);
        wait_for(&h, "rotated-accounts", 1);
        // The session ends while it runs; a second account is waiting.
        {
            let mut o = o.lock().unwrap();
            o.accounts.push((
                "2026-11-15T09:05:00.000+0000".into(),
                "001A00000000002AAA".into(),
                "Globex".into(),
                false,
            ));
            o.expire = true;
        }
        let got = wait_for(&h, "rotated-accounts", 2);
        let log = running.stop();
        assert_eq!(got.len(), 2, "{log}");
        assert_eq!(
            o.lock().unwrap().refreshes,
            ["rt-0", "rt-1"],
            "the second sign-in asks with the token the first was given"
        );

        let (ok, log) = Running::start(&h, &f, &env).ended();
        assert!(!ok && log.contains("Salesforce refused the refresh token"), "{log}");
        assert!(log.contains("client credentials"), "it says what to use instead: {log}");
        assert!(!log.contains("rt-0") && !log.contains("rt-2"), "no refresh token is shown: {log}");
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
            format!("name: {name}\nfrom:\n  salesforce:\n{body}to:\n  topic: t\n")
        } else {
            format!("name: {name}\nfrom:\n  topic: t\nto:\n  salesforce:\n{body}")
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
    let sign_in = "    login_url: https://acme.my.salesforce.com\n    client_id: secret:SF_CLIENT_ID\n    client_secret: secret:SF_CLIENT_SECRET\n";
    let (ok, text) = check(
        "accounts",
        "from",
        &format!("{sign_in}    object: Account\n    fields: [Id, Name, Industry]\n"),
    );
    assert!(
        ok && text.contains("reads salesforce Account on acme.my.salesforce.com every 60s"),
        "{text}"
    );
    let (ok, text) = check(
        "contacts",
        "to",
        &format!("{sign_in}    object: Contact\n    external_id: Email\n"),
    );
    assert!(
        ok && text.contains("writes salesforce Contact on acme.my.salesforce.com (upsert by Email)"),
        "{text}"
    );
    for (name, side, body, why) in [
        (
            "http",
            "from",
            "    login_url: http://acme.my.salesforce.com\n    client_id: x\n    client_secret: y\n    object: Account\n",
            "is https://",
        ),
        (
            "nosecret",
            "from",
            "    login_url: https://acme.my.salesforce.com\n    client_id: x\n    object: Account\n",
            "client_secret: is required",
        ),
        (
            "object",
            "from",
            &format!("{sign_in}    object: \"Account; DELETE\"\n"),
            "API name",
        ),
        (
            "noext",
            "to",
            &format!("{sign_in}    object: Contact\n"),
            "external_id: is required to upsert",
        ),
        (
            "often",
            "from",
            &format!("{sign_in}    object: Account\n    every: 1s\n"),
            "at least 5s",
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
