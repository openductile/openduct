// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The snowflake connector as a person runs it, against a stand-in for Snowflake's SQL API that keeps to its
//! documented behaviour: key-pair sign-in checked against the key's fingerprint, statements still running answered
//! 202 and asked after, answers in gzip partitions, values as `jsonv2` writes them, refusals with SQL states, and a
//! statement sent again with its request id not run twice. A real account is used once one is given for tests.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

/// A key made for these tests only.
const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCTCNDQTyB5Vl1s\nJR9/C2/p/o7FppUwOfMNV8pJFHTbL1wLwzBM3a9HYguhiTmM06mCpmQ+cXSysOik\nMRaSC+KmytOsKh36shKe5YHBofAIgYwXfrntYaPj90jl7f1IckIYKgKFLXyjzrhY\n0vZNr03Gc4JxB5XADr5YzXhIXovVp9kFKy7XBPeNoBGlOR1nOgGizh5WQDS6meVy\nWVddfKWaECTsOMJqqW0GQaB9ax+H31Wo1aYA/TprHFuDU4aSpkxbi6uDGZmtrYhy\nyDxJAAH+pK5vYQNSXM+8z5AE29Q/hEL3OslUND85GqjrZ4qKBmrCRTvJMW0uzQpx\nSJLGnn43AgMBAAECggEAAy8PBLO5CoQGdkwj71hTKrn1R0NEnfcqYfKxmODY9u70\nfRIMfpZ+TLQSW4PEu7MJoRIr+KG7aSwI1nqtMRuDccvRqnS2IJY+/IgQKXNxkWEj\n3TkLfoOEheHZzkDJdl+xOkBHg+nadfbsaacWBH7XuoTPkyr+byHGJyxWfuHJq5gC\ncl9cIm+NCgOTz0vAGRvS/gxiGq0EZ8WuggZNYeDSUupuW5Xsd/lXhEIXQZ+A9/NC\nDPr0IIpfOmw6RkrdN4NW4OffhY+qj9WTWdNcJ95iWWD3MVMJQoL3ozql5/zRBL0/\naD/s0fQUhuZsD/33pOlToxNK8zjxyoIj4h/4ZviGgQKBgQDIrJqzVlGQHrQkNUNx\npxobv/RTnFqCq03zRNz0O2tp4YHqiOmN5wR5tzDLWPlO4kqD/C+VILiUGR58B+D7\nNQoGi5k8HtHM1iF9DI+vsYXp3t40b9J20C3R7X7bQ2Vs7rpfk00UY2AQ8mYZKhf9\nUZff363lCs6KxkBiErtYTstZpwKBgQC7kl2ZkLZilLu72gfmXTPDZMRwDqQJjI4U\ni7TG/8FsHTMo3djGu4VK+Pz03P3Qlb7zdyaguMWdCXNx1CgR3zvfEDt6W4cJTdxy\nc9e9o0kbnUsQUJzqRmlz1SgXxyUVqCv/GfeJJZUJQj6mfYRfZusyTIkaZiHW9jtU\nTDfJXwso8QKBgQC5mNHXjb0P4yXO2yhOhDMkeSt67P/VHKSSmx0UEaStn7CtfwUr\nhf9gpypM2DR0Xdx3J5XGgiXUa0sDgbH2BD8KgLAvp314yo7cffKilzh4bfTrYISx\n+KZ4IwSRvEqhLkZXQQAoHCTsZ5RAL+tSEdW6a8EENjOCrAs96X82FGIEvwKBgCF9\nN3L0kGu3bHFUszZhC53mPuBW7Ni7nJ50dxlUjn80E29HdK2+y0SggeFiF3sHUWl8\nq4vkkXvBNThsjZ7rQXBJIKVu9AQoP6S7pBg6sBHbGozWJ734RlmlBByCtM9SEFDi\nnPyClkJp7m+hV2IFKRb47T2mYIJVwjABz7AHZSGxAoGAblfTIRecygU57lnU5UyN\n6Z8eLjaBtXG0QzfmsRuq5aeIq06Kt/Ffd2b4gEJzMSbA4dibSnGPiAv3FP0yLQH0\nCDIdpv95g1URSVZeMl8cYt3cNC+dC7zO/wnfsmMNGEblq/LXWMpmNeX37Yyh2d9w\n/bc6T5JQRn2hs40ZTWiB23A=\n-----END PRIVATE KEY-----";
const TEST_PUBLIC: &str = "MIIBCgKCAQEAkwjQ0E8geVZdbCUffwtv6f6OxaaVMDnzDVfKSRR02y9cC8MwTN2vR2ILoYk5jNOpgqZkPnF0srDopDEWkgvipsrTrCod+rISnuWBwaHwCIGMF3657WGj4/dI5e39SHJCGCoChS18o864WNL2Ta9NxnOCcQeVwA6+WM14SF6L1afZBSsu1wT3jaARpTkdZzoBos4eVkA0upnlcllXXXylmhAk7DjCaqltBkGgfWsfh99VqNWmAP06axxbg1OGkqZMW4urgxmZra2Icsg8SQAB/qSub2EDUlzPvM+QBNvUP4RC9zrJVDQ/ORqo62eKigZqwkU7yTFtLs0KcUiSxp5+NwIDAQAB";
/// The test key's fingerprint, as `openssl rsa -pubout -outform DER | openssl dgst -sha256 -binary | base64` gives it.
const FINGERPRINT: &str = "SHA256:pXkcAsNvmfU0I8qzxV35yRPnZvVdbs7JK0fuOW2a8bk=";
const TOKEN: &str = "pat-7731-for-tests";

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

type Row = BTreeMap<String, Option<String>>;

/// What the stand-in account holds and what it was asked.
#[derive(Default)]
struct Account {
    /// ORDERS, its values as `jsonv2` writes them.
    rows: Vec<Row>,
    /// Each statement run, with what it bound.
    statements: Vec<(String, Vec<String>)>,
    /// Request ids, and whether each was sent as a retry.
    requests: Vec<(String, bool)>,
    answers: BTreeMap<String, String>,
    running: BTreeMap<String, String>,
    /// Answer the first query for rows with 202 (still running).
    first_query_waits: bool,
    /// Run the next successful INSERT and lose its answer.
    lose_next_insert: bool,
}

const COLUMNS: &[(&str, &str, u64, u64)] = &[
    ("ORDER_ID", "fixed", 38, 0),
    ("CUSTOMER", "text", 0, 0),
    ("TOTAL", "fixed", 10, 2),
    ("STATUS", "text", 0, 0),
    ("UPDATED_AT", "timestamp_ntz", 0, 9),
    ("DETAILS", "variant", 0, 0),
];

fn row_type(names: &[&str]) -> serde_json::Value {
    names
        .iter()
        .map(|n| {
            let (name, kind, precision, scale) = COLUMNS.iter().find(|c| c.0 == *n).unwrap();
            serde_json::json!({ "name": name, "type": kind, "precision": precision, "scale": scale, "nullable": true })
        })
        .collect()
}

fn epoch(iso: &str) -> Option<String> {
    let t = chrono::NaiveDateTime::parse_from_str(iso.trim_end_matches('Z'), "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    let utc = t.and_utc();
    Some(format!("{}.{:09}", utc.timestamp(), utc.timestamp_subsec_nanos()))
}

fn order(id: u32, customer: &str, total: &str, status: &str, at: &str, details: Option<&str>) -> Row {
    [
        ("ORDER_ID", Some(id.to_string())),
        ("CUSTOMER", Some(customer.to_owned())),
        ("TOTAL", Some(total.to_owned())),
        ("STATUS", Some(status.to_owned())),
        ("UPDATED_AT", epoch(at)),
        ("DETAILS", details.map(str::to_owned)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

/// A value as the column stores it, or the refusal Snowflake gives.
fn stored(column: &str, text: &str) -> Result<String, (String, String)> {
    let kind = COLUMNS.iter().find(|c| c.0 == column).unwrap();
    match kind.1 {
        "fixed" => text
            .parse::<f64>()
            .map(|n| format!("{n:.prec$}", prec = kind.3 as usize))
            .map_err(|_| {
                (
                    "22018".to_owned(),
                    format!("Numeric value '{text}' is not recognized"),
                )
            }),
        "timestamp_ntz" => epoch(text).ok_or((
            "22007".to_owned(),
            format!("Timestamp '{text}' is not recognized"),
        )),
        "variant" => serde_json::from_str::<serde_json::Value>(text)
            .map(|v| v.to_string())
            .map_err(|_| ("22000".to_owned(), format!("Error parsing JSON: {text}"))),
        _ => Ok(text.to_owned()),
    }
}

/// `(?, NULL), (?, ?)` with its binds: each row's values in order.
fn tuples(text: &str, binds: &[String]) -> Vec<Vec<Option<String>>> {
    let mut next = binds.iter();
    text.trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split("), (")
        .map(|t| {
            t.split(", ")
                .map(|v| if v == "?" { next.next().cloned() } else { None })
                .collect()
        })
        .collect()
}

fn unquote(name: &str) -> String {
    name.trim().trim_matches('"').to_owned()
}

fn refusal(state: &str, words: &str) -> (u16, String) {
    (
        422,
        serde_json::json!({ "code": "100038", "sqlState": state, "message": words, "statementHandle": "01bf0000-0000-4000-8000-00000000e422" })
            .to_string(),
    )
}

fn dml(n: usize) -> (u16, String) {
    let meta = serde_json::json!({ "numRows": 1, "format": "jsonv2", "rowType": [{"name": "number of rows", "type": "fixed", "scale": 0, "precision": 19}], "partitionInfo": [{"rowCount": 1}] });
    (200, serde_json::json!({ "resultSetMetaData": meta, "data": [[n.to_string()]], "statementHandle": "01bf0000-0000-4000-8000-00000000d001" }).to_string())
}

/// Write rows: each `(names, values)`; all or none, as one statement is.
fn write(
    account: &mut Account,
    names: &[String],
    rows: Vec<Vec<Option<String>>>,
    keys: &[String],
) -> (u16, String) {
    let mut ready = Vec::new();
    for values in rows {
        let mut row = Row::new();
        for (name, value) in names.iter().zip(values) {
            let v = match value {
                Some(text) => match stored(name, &text) {
                    Ok(v) => Some(v),
                    Err((state, words)) => return refusal(&state, &words),
                },
                None => None,
            };
            row.insert(name.clone(), v);
        }
        ready.push(row);
    }
    let n = ready.len();
    for row in ready {
        let existing = account
            .rows
            .iter_mut()
            .find(|r| !keys.is_empty() && keys.iter().all(|k| r.get(k) == row.get(k)));
        match existing {
            Some(r) => r.extend(row),
            None => account.rows.push(row),
        }
    }
    dml(n)
}

/// The answer to a query, in two partitions when it is longer than two rows.
fn answer(account: &mut Account, names: &[&str], rows: Vec<Vec<Option<String>>>) -> serde_json::Value {
    let handle = format!(
        "01bf{:04x}-0000-4000-8000-00000000a001",
        account.running.len() + account.answers.len()
    );
    let (first, rest) = rows.split_at(rows.len().min(2));
    let mut partitions = vec![serde_json::json!({ "rowCount": first.len() })];
    if !rest.is_empty() {
        partitions.push(serde_json::json!({ "rowCount": rest.len() }));
        account.running.insert(
            format!("{handle}?partition=1"),
            serde_json::json!({ "data": rest }).to_string(),
        );
    }
    let meta = serde_json::json!({ "numRows": rows.len(), "format": "jsonv2", "rowType": row_type(names), "partitionInfo": partitions });
    serde_json::json!({ "resultSetMetaData": meta, "data": first, "statementHandle": handle })
}

fn statement(account: &mut Account, text: &str, binds: &[String]) -> (u16, String) {
    let all: Vec<&str> = COLUMNS.iter().map(|c| c.0).collect();
    if text.contains("ORDRES") {
        return refusal(
            "42S02",
            "SQL compilation error:\nTable 'ORDRES' does not exist or not authorized.",
        );
    }
    if text == "SELECT * FROM ORDERS LIMIT 0" {
        let meta = serde_json::json!({ "numRows": 0, "format": "jsonv2", "rowType": row_type(&all), "partitionInfo": [] });
        return (
            200,
            serde_json::json!({ "resultSetMetaData": meta, "data": [] }).to_string(),
        );
    }
    if let Some(rest) = text.strip_prefix("INSERT INTO ORDERS (") {
        let (names, rest) = rest.split_once(") SELECT ").unwrap();
        let names: Vec<String> = names.split(", ").map(unquote).collect();
        let values = rest.split_once(" FROM VALUES ").unwrap().1;
        return write(account, &names, tuples(values, binds), &[]);
    }
    if let Some(rest) = text.strip_prefix("MERGE INTO ORDERS AS t USING (SELECT ") {
        let (select, rest) = rest.split_once(" FROM VALUES ").unwrap();
        let names: Vec<String> = select
            .split(", ")
            .map(|s| unquote(s.rsplit_once(" AS ").unwrap().1))
            .collect();
        let (values, rest) = rest.split_once(") AS s ON ").unwrap();
        let on = rest.split_once(" WHEN ").unwrap().0;
        let keys: Vec<String> = on
            .split(" AND ")
            .map(|c| unquote(c.split_once(" = ").unwrap().0.trim_start_matches("t.")))
            .collect();
        assert!(rest.contains("WHEN NOT MATCHED THEN INSERT"), "{text}");
        return write(account, &names, tuples(values, binds), &keys);
    }
    if let Some(rest) = text.strip_prefix("SELECT ") {
        let (select, rest) = rest.split_once(" FROM ORDERS WHERE ").unwrap();
        assert!(
            rest.contains("\"UPDATED_AT\" IS NOT NULL")
                && rest.contains("ORDER BY \"UPDATED_AT\", \"ORDER_ID\" LIMIT 1000"),
            "{text}"
        );
        let names: Vec<String> = if select == "*" {
            all.iter().map(|s| (*s).to_owned()).collect()
        } else {
            select.split(", ").map(unquote).collect()
        };
        let at = |r: &Row| {
            let t = r["UPDATED_AT"].clone().unwrap_or_default();
            let (s, n) = t.split_once('.').unwrap_or((&t, "0"));
            (s.parse::<i64>().unwrap_or(0), n.parse::<u64>().unwrap_or(0))
        };
        let id = |r: &Row| {
            r["ORDER_ID"]
                .clone()
                .unwrap_or_default()
                .parse::<i64>()
                .unwrap_or(0)
        };
        let since = binds.first().map(|b| {
            let e = epoch(b).unwrap();
            let (s, n) = e.split_once('.').unwrap();
            (s.parse::<i64>().unwrap(), n.parse::<u64>().unwrap())
        });
        let after_id: Option<i64> = binds.get(2).map(|b| b.parse().unwrap());
        let mut rows: Vec<Row> = account
            .rows
            .iter()
            .filter(|r| r["UPDATED_AT"].is_some() && r["ORDER_ID"].is_some())
            .filter(|r| match (since, after_id) {
                (Some(t), Some(i)) => at(r) > t || (at(r) == t && id(r) > i),
                _ => true,
            })
            .filter(|r| !text.contains("(STATUS = 'paid')") || r["STATUS"].as_deref() == Some("paid"))
            .cloned()
            .collect();
        rows.sort_by_key(|r| (at(r), id(r)));
        let data: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|r| names.iter().map(|n| r.get(n).cloned().flatten()).collect())
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let body = answer(account, &names, data);
        if account.first_query_waits {
            account.first_query_waits = false;
            let handle = body["statementHandle"].as_str().unwrap().to_owned();
            account.running.insert(handle.clone(), body.to_string());
            let waiting = serde_json::json!({ "code": "333334", "message": "Asynchronous execution in progress. Use provided query id to perform query monitoring and management.", "statementHandle": handle, "statementStatusUrl": format!("/api/v2/statements/{handle}") });
            return (202, waiting.to_string());
        }
        return (200, body.to_string());
    }
    panic!("the stand-in does not know {text}");
}

fn signed_in(auth: &str, kind: Option<&str>) -> bool {
    let Some(token) = auth.strip_prefix("Bearer ") else {
        return false;
    };
    if token == TOKEN {
        return kind.is_none();
    }
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 || kind != Some("KEYPAIR_JWT") {
        return false;
    }
    let public = STANDARD.decode(TEST_PUBLIC).unwrap();
    let key = ring::signature::UnparsedPublicKey::new(&ring::signature::RSA_PKCS1_2048_8192_SHA256, public);
    let signature = URL_SAFE_NO_PAD.decode(parts[2]).unwrap_or_default();
    if key
        .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
        .is_err()
    {
        return false;
    }
    let claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (iat, exp) = (claims["iat"].as_u64().unwrap(), claims["exp"].as_u64().unwrap());
    claims["iss"] == format!("ACME-ANALYTICS.LOADER.{FINGERPRINT}")
        && claims["sub"] == "ACME-ANALYTICS.LOADER"
        && exp > now
        && exp - iat <= 3600
}

fn respond(stream: &mut std::net::TcpStream, status: u16, body: &[u8], zipped: bool) {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        401 => "Unauthorized",
        422 => "Unprocessable Entity",
        _ => "Not Found",
    };
    let encoding = if zipped { "Content-Encoding: gzip\r\n" } else { "" };
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n{encoding}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(body);
}

fn snowflake(account: Arc<Mutex<Account>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            if reader.read_line(&mut first).is_err() {
                continue;
            }
            let mut words = first.split_whitespace();
            let (method, path) = (
                words.next().unwrap_or("").to_owned(),
                words.next().unwrap_or("").to_owned(),
            );
            let (mut length, mut auth, mut kind) = (0usize, String::new(), None);
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
                    "x-snowflake-authorization-token-type" => kind = Some(value.trim().to_owned()),
                    _ => {}
                }
            }
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).unwrap();
            if !signed_in(&auth, kind.as_deref()) {
                respond(
                    &mut stream,
                    401,
                    br#"{"code":"390144","message":"JWT token is invalid."}"#,
                    false,
                );
                continue;
            }
            let mut account = account.lock().unwrap();
            if method == "GET" {
                let handle = path.trim_start_matches("/api/v2/statements/").to_owned();
                match account.running.remove(&handle) {
                    Some(answer) if handle.contains("partition=") => {
                        let mut zipped =
                            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                        zipped.write_all(answer.as_bytes()).unwrap();
                        respond(&mut stream, 200, &zipped.finish().unwrap(), true);
                    }
                    Some(answer) => respond(&mut stream, 200, answer.as_bytes(), false),
                    None => respond(&mut stream, 404, b"{}", false),
                }
                continue;
            }
            let query = path
                .split_once('?')
                .map(|(_, q)| q.to_owned())
                .unwrap_or_default();
            let id = query
                .split('&')
                .find_map(|p| p.strip_prefix("requestId="))
                .unwrap_or_default()
                .to_owned();
            let retry = query.split('&').any(|p| p == "retry=true");
            account.requests.push((id.clone(), retry));
            if retry && let Some(answer) = account.answers.get(&id).cloned() {
                respond(&mut stream, 200, answer.as_bytes(), false);
                continue;
            }
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                (request["warehouse"].as_str(), request["database"].as_str()),
                (Some("LOADING"), Some("SHOP"))
            );
            let text = request["statement"].as_str().unwrap().to_owned();
            let binds: Vec<String> = (1..)
                .map_while(|n| request["bindings"].get(n.to_string()))
                .map(|b| {
                    assert_eq!(b["type"], "TEXT");
                    b["value"].as_str().unwrap().to_owned()
                })
                .collect();
            account.statements.push((text.clone(), binds.clone()));
            let (status, answer) = statement(&mut account, &text, &binds);
            if status == 200 {
                account.answers.insert(id, answer.clone());
                if text.starts_with("INSERT") && account.lose_next_insert {
                    account.lose_next_insert = false;
                    continue; // run, and the answer lost on the way
                }
            }
            respond(&mut stream, status, answer.as_bytes(), false);
        }
    });
    base
}

fn file(dir: &tempfile::TempDir, name: &str, text: &str) -> std::path::PathBuf {
    let f = dir.path().join(format!("{name}.yaml"));
    std::fs::write(&f, text).unwrap();
    f
}

fn sign_in(url: &str, how: &str) -> String {
    let who = match how {
        "key" => "    user: loader\n    private_key: secret:SF_KEY\n",
        _ => "    token: secret:SF_TOKEN\n",
    };
    format!(
        "    account: acme-analytics\n    url: {url}\n{who}    warehouse: LOADING\n    database: SHOP\n    schema: PUBLIC\n"
    )
}

fn env() -> Vec<(&'static str, &'static str)> {
    vec![("SF_KEY", TEST_KEY), ("SF_TOKEN", TOKEN)]
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_are_read_in_order_without_skipping_or_repeating() {
    let account = Arc::new(Mutex::new(Account {
        rows: vec![
            order(
                1,
                "Ada",
                "48.50",
                "paid",
                "2026-11-15T09:00:00",
                Some("{\n  \"lines\": 2\n}"),
            ),
            order(2, "Bo", "12.00", "pending", "2026-11-15T09:05:00", None),
            order(
                3,
                "Cy",
                "7.25",
                "paid",
                "2026-11-15T09:05:00",
                Some("{\"gift\":true}"),
            ),
        ],
        first_query_waits: true,
        ..Account::default()
    }));
    let url = snowflake(Arc::clone(&account));
    let (local, here) = broker().await;
    let (h, acct) = (here.clone(), Arc::clone(&account));
    tokio::task::spawn_blocking(move || {
        for t in ["orders", "paid"] {
            openduct(&h, &["topic", "create", t]);
        }
        let dir = tempfile::tempdir().unwrap();
        let f = file(&dir, "all", &format!("name: orders-in\nfrom:\n  snowflake:\n{}    table: ORDERS\n    track: updated_at\n    id: order_id\n    every: 5s\nto:\n  topic: orders\n", sign_in(&url, "key")));
        let running = Running::start(&h, &f, &env());
        let got = wait_for(&h, "orders", 3);
        let log = running.stop();
        assert_eq!(got.len(), 3, "{log}");
        let text = got[0]["value"].as_str().unwrap();
        assert!(text.contains(r#""ORDER_ID":1,"CUSTOMER":"Ada","TOTAL":48.50,"#), "{text}");
        assert!(text.contains(r#""UPDATED_AT":"2026-11-15T09:00:00","DETAILS":{"lines":2}"#), "{text}");
        assert_eq!(got[0]["key"], "ORDERS/1");
        {
            let mut a = acct.lock().unwrap();
            a.rows.push(order(4, "Di", "3.00", "paid", "2026-11-15T09:05:00", None));
            a.rows.push(order(5, "Ed", "9.99", "pending", "2026-11-15T09:10:00", None));
        }
        let running = Running::start(&h, &f, &env());
        wait_for(&h, "orders", 5);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        let got = wait_for(&h, "orders", 5);
        let ids: Vec<i64> = got.iter().map(|m| change(m)["after"]["ORDER_ID"].as_i64().unwrap()).collect();
        assert_eq!(ids, [1, 2, 3, 4, 5], "{log}");
        assert!(
            acct.lock().unwrap().statements.iter().any(|(_, b)| b == &["2026-11-15T09:05:00", "2026-11-15T09:05:00", "3"]),
            "the position is bound, not written into the statement"
        );

        // Only paid orders, and only some columns: the column to order by is read too.
        let f = file(&dir, "paid", &format!("name: paid-in\nfrom:\n  snowflake:\n{}    table: ORDERS\n    columns: [order_id, status]\n    track: UPDATED_AT\n    id: ORDER_ID\n    where: STATUS = 'paid'\nto:\n  topic: paid\n", sign_in(&url, "token")));
        let running = Running::start(&h, &f, &env());
        wait_for(&h, "paid", 3);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        let got = wait_for(&h, "paid", 3);
        let ids: Vec<i64> = got.iter().map(|m| change(m)["after"]["ORDER_ID"].as_i64().unwrap()).collect();
        assert_eq!(ids, [1, 3, 4], "{log}");
        let after = change(&got[0])["after"].clone();
        assert_eq!(after.as_object().unwrap().len(), 3, "{after}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_are_merged_and_a_refused_one_is_set_aside() {
    let account = Arc::new(Mutex::new(Account {
        rows: vec![order(7, "Ada L", "40.00", "pending", "2026-11-15T08:00:00", None)],
        ..Account::default()
    }));
    let url = snowflake(Arc::clone(&account));
    let (local, here) = broker().await;
    let (h, acct) = (here.clone(), Arc::clone(&account));
    tokio::task::spawn_blocking(move || {
        for t in ["changes", "changes-refused"] {
            openduct(&h, &["topic", "create", t]);
        }
        for m in [
            r#"{"order_id":7,"customer":"Ada","total":"48.5","status":"paid","details":{"lines":2}}"#,
            r#"{"order_id":7,"status":"shipped","updated_at":"2026-11-15T10:00:00Z"}"#,
            r#"{"order_id":8,"total":"abc"}"#,
            r#"["not", "a", "row"]"#,
            r#"{"order_id":9,"colour":"red"}"#,
            r#"{"customer":"Bo"}"#,
        ] {
            openduct(&h, &["send", "changes", m]);
        }
        let dir = tempfile::tempdir().unwrap();
        let f = file(&dir, "out", &format!("name: changes-out\nfrom:\n  topic: changes\n  start: earliest\nto:\n  snowflake:\n{}    table: ORDERS\n    mode: merge\n    match: order_id\non_error:\n  topic: changes-refused\n", sign_in(&url, "token")));
        let running = Running::start(&h, &f, &env());
        let refused = wait_for(&h, "changes-refused", 4);
        std::thread::sleep(Duration::from_secs(1));
        let log = running.stop();
        assert!(log.contains("read 6, wrote 2, set aside 4"), "{log}");
        let reasons: Vec<String> = refused.iter().map(|m| m.to_string()).collect();
        for why in ["Numeric value 'abc' is not recognized", "not a JSON object", "has no column colour", "has no order_id, which it is matched by"] {
            assert!(reasons.iter().any(|r| r.contains(why)), "{why}: {reasons:?}");
        }
        let a = acct.lock().unwrap();
        assert_eq!(a.rows.len(), 1, "{:?}", a.rows);
        let r = &a.rows[0];
        assert_eq!(
            (r["CUSTOMER"].as_deref(), r["TOTAL"].as_deref(), r["STATUS"].as_deref(), r["DETAILS"].as_deref()),
            (Some("Ada"), Some("48.50"), Some("shipped"), Some(r#"{"lines":2}"#))
        );
        assert_eq!(r["UPDATED_AT"], epoch("2026-11-15T10:00:00"));
        assert!(a.statements.iter().any(|(s, _)| s.contains("WHEN MATCHED THEN UPDATE SET t.\"CUSTOMER\" = s.\"CUSTOMER\"")));
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_are_inserted_once_when_an_answer_is_lost() {
    let account = Arc::new(Mutex::new(Account {
        lose_next_insert: true,
        ..Account::default()
    }));
    let url = snowflake(Arc::clone(&account));
    let (local, here) = broker().await;
    let (h, acct) = (here.clone(), Arc::clone(&account));
    tokio::task::spawn_blocking(move || {
        for t in ["new-orders", "new-orders-refused"] {
            openduct(&h, &["topic", "create", t]);
        }
        for m in [
            r#"{"order_id":11,"customer":"Ada","total":10}"#,
            r#"{"order_id":12,"customer":"Bo","total":"ten"}"#,
            r#"{"order_id":13,"customer":"Cy","total":30.5}"#,
        ] {
            openduct(&h, &["send", "new-orders", m]);
        }
        let dir = tempfile::tempdir().unwrap();
        let f = file(&dir, "in", &format!("name: new-orders-out\nfrom:\n  topic: new-orders\n  start: earliest\nto:\n  snowflake:\n{}    table: ORDERS\non_error:\n  topic: new-orders-refused\n", sign_in(&url, "key")));
        let running = Running::start(&h, &f, &env());
        wait_for(&h, "new-orders-refused", 1);
        std::thread::sleep(Duration::from_secs(2));
        let log = running.stop();
        assert!(log.contains("read 3, wrote 2, set aside 1"), "{log}");
        let a = acct.lock().unwrap();
        let ids: Vec<&str> = a.rows.iter().map(|r| r["ORDER_ID"].as_deref().unwrap()).collect();
        assert_eq!(ids, ["11", "13"], "each row once, though an answer was lost");
        assert!(a.requests.iter().any(|(_, retry)| *retry), "sent again as a retry: {:?}", a.requests);
        // The three went as one statement; when it was refused, each went alone.
        assert!(a.statements.iter().any(|(s, b)| s.starts_with("INSERT") && b.len() == 9), "{:?}", a.statements);
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_table_or_a_wrong_token_stops_with_the_reason() {
    let url = snowflake(Arc::new(Mutex::new(Account::default())));
    let (local, here) = broker().await;
    let h = here.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "t"]);
        openduct(&h, &["send", "t", r#"{"order_id":1}"#]);
        let dir = tempfile::tempdir().unwrap();
        let f = file(&dir, "missing", &format!("name: missing-out\nfrom:\n  topic: t\n  start: earliest\nto:\n  snowflake:\n{}    table: ORDRES\n", sign_in(&url, "key")));
        let (ok, log) = Running::start(&h, &f, &env()).ended();
        assert!(!ok && log.contains("Table 'ORDRES' does not exist or not authorized"), "{log}");
        let wrong = "pat-not-the-token-4410";
        let f = file(&dir, "wrong", &format!("name: wrong-in\nfrom:\n  snowflake:\n{}    table: ORDERS\n    track: UPDATED_AT\nto:\n  topic: t\n", sign_in(&url, "token")));
        let (ok, log) = Running::start(&h, &f, &[("SF_TOKEN", wrong)]).ended();
        assert!(!ok && log.contains("Snowflake refused the sign-in: JWT token is invalid."), "{log}");
        assert!(!log.contains(wrong), "the token is never shown: {log}");
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
            format!("name: {name}\nfrom:\n  snowflake:\n{body}to:\n  topic: t\n")
        } else {
            format!("name: {name}\nfrom:\n  topic: t\nto:\n  snowflake:\n{body}")
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
    let key = "    account: myorg-myaccount\n    user: LOADER\n    private_key: secret:SF_KEY\n";
    let (ok, text) = check(
        "orders",
        "from",
        &format!("{key}    table: SHOP.PUBLIC.ORDERS\n    track: UPDATED_AT\n    id: ORDER_ID\n"),
    );
    assert!(
        ok && text.contains(
            "reads snowflake SHOP.PUBLIC.ORDERS on myorg-myaccount.snowflakecomputing.com every 60s"
        ),
        "{text}"
    );
    let (ok, text) = check(
        "loads",
        "to",
        &format!("{key}    table: ORDERS\n    mode: merge\n    match: [ORDER_ID]\n"),
    );
    assert!(
        ok && text.contains(
            "writes snowflake ORDERS on myorg-myaccount.snowflakecomputing.com (merge by ORDER_ID)"
        ),
        "{text}"
    );
    for (name, side, body, why) in [
        ("account", "to", "    account: my org\n    token: t\n    table: ORDERS\n".to_owned(), "is the account identifier"),
        ("neither", "to", "    account: myorg-myaccount\n    user: LOADER\n    table: ORDERS\n".to_owned(), "takes a key pair or a token"),
        ("both", "to", "    account: myorg-myaccount\n    user: LOADER\n    private_key: secret:K\n    token: secret:T\n    table: ORDERS\n".to_owned(), "give one of them"),
        ("nouser", "to", "    account: myorg-myaccount\n    private_key: secret:K\n    table: ORDERS\n".to_owned(), "is required with private_key"),
        ("table", "to", format!("{key}    table: ORDERS;DROP\n"), "is a table's name"),
        ("nomatch", "to", format!("{key}    table: ORDERS\n    mode: merge\n"), "is required to merge"),
        ("onlymatch", "to", format!("{key}    table: ORDERS\n    match: ORDER_ID\n"), "is for mode: merge"),
        ("notrack", "from", format!("{key}    table: ORDERS\n"), "track: is required"),
        ("order", "from", format!("{key}    table: ORDERS\n    track: UPDATED_AT\n    where: STATUS = 'paid' ORDER BY 1\n"), "conditions only"),
        ("http", "to", format!("{key}    url: http://acme.example\n    table: ORDERS\n"), "is the account's address"),
    ] {
        let (ok, text) = check(name, side, &body);
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
