// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Stand-ins for the vector databases, on 127.0.0.1: each speaks its database's API as its vendor documents it (the
//! paths, the bodies, the answers) and keeps what it is sent in memory, so the steps' requests are checked and their
//! results come from what they wrote. No real account is reached.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

pub(crate) mod native;
pub(crate) mod web;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::{Credential, Effects, Event, Live, Outcome, RunRecord, Secrets, Workflow, run};
use serde_json::{Value as Json, json};

/// One request a stand-in was sent.
#[derive(Debug, Clone)]
pub(crate) struct Asked {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Json,
}

/// What answers a request: its status and body.
pub(crate) type Handler = Box<dyn FnMut(&Asked) -> (u16, Json) + Send>;

/// A stand-in's address, and every request it was sent.
pub(crate) struct Server {
    pub(crate) base: String,
    pub(crate) asked: Arc<Mutex<Vec<Asked>>>,
}

impl Server {
    pub(crate) fn asked(&self) -> Vec<Asked> {
        self.asked.lock().unwrap().clone()
    }

    pub(crate) fn count(&self) -> usize {
        self.asked.lock().unwrap().len()
    }

    /// The requests to a path that begins with `prefix`, by `method`.
    pub(crate) fn to(&self, method: &str, prefix: &str) -> Vec<Asked> {
        self.asked()
            .into_iter()
            .filter(|a| a.method == method && a.path.starts_with(prefix))
            .collect()
    }
}

/// An HTTP stand-in: `make` is given its base address and gives the handler.
pub(crate) fn serve(make: impl FnOnce(String) -> Handler) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let mut handler = make(base.clone());
    let asked: Arc<Mutex<Vec<Asked>>> = Arc::default();
    let kept = Arc::clone(&asked);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            if reader.read_line(&mut first).unwrap_or(0) == 0 {
                continue;
            }
            let mut parts = first.split_whitespace();
            let method = parts.next().unwrap_or_default().to_owned();
            let path = parts.next().unwrap_or_default().to_owned();
            let mut headers = BTreeMap::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line.trim().is_empty() {
                    break;
                }
                if let Some((k, v)) = line.split_once(':') {
                    headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
                }
            }
            let length: usize = headers
                .get("content-length")
                .and_then(|l| l.parse().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).unwrap();
            let asked = Asked {
                method,
                path,
                headers,
                body: serde_json::from_slice(&body).unwrap_or(Json::Null),
            };
            let (status, answer) = handler(&asked);
            kept.lock().unwrap().push(asked);
            let text = answer.to_string();
            let mut out = stream;
            let _ = out.write_all(
                format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                    text.len()
                )
                .as_bytes(),
            );
        }
    });
    Server { base, asked }
}

#[derive(Debug)]
pub(crate) struct Kept(pub BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

pub(crate) fn credential(kind: &str, fields: &[(&str, &str)]) -> Credential {
    Credential {
        kind: kind.to_owned(),
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
    }
}

/// Real effects with the credential `vs`, this machine's network allowed (the stand-ins are on it).
pub(crate) fn live(vs: Credential) -> Live {
    Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(Arc::new(Kept([("vs".to_owned(), vs)].into())))
}

pub(crate) async fn go(workflow: &str, input: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap_or_else(|e| panic!("{e}\n{workflow}"));
    let event = Event {
        value: Value::parse_json(&input.to_string()).unwrap(),
        details: Value::Null,
    };
    run(&workflow, &event, effects).await
}

pub(crate) fn result(record: &RunRecord, step: &str) -> Json {
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
    let found = record.steps.iter().find(|s| s.name == step).unwrap();
    serde_json::from_str(&found.result.to_json()).unwrap()
}

pub(crate) fn failed(record: &RunRecord) -> String {
    match &record.outcome {
        Outcome::Failed { error, .. } => error.clone(),
        other => panic!("not failed: {other:?}"),
    }
}

pub(crate) fn cosine(a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let n = |v: &[f64]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
    dot / (n(a) * n(b))
}

pub(crate) fn numbers(v: &Json) -> Vec<f64> {
    v.as_array()
        .map(|a| a.iter().filter_map(Json::as_f64).collect())
        .unwrap_or_default()
}

/// Whether a JSON value is, or (a list) holds, `want`.
pub(crate) fn is_or_holds(have: Option<&Json>, want: &Json) -> bool {
    match have {
        Some(Json::Array(items)) => items.iter().any(|i| same(i, want)),
        Some(v) => same(v, want),
        None => false,
    }
}

pub(crate) fn same(a: &Json, b: &Json) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// The value at a dotted path of a record.
pub(crate) fn at<'a>(record: &'a Json, path: &str) -> Option<&'a Json> {
    path.split('.').try_fold(record, |r, k| r.get(k))
}

pub(crate) const ADD: &str = "name: keep\nsteps:\n  - name: store\n    vector_store:\n      action: add\n      store: {store}\n      credential: vs\n      collection: {collection}\n      create: true\n      pieces: value.pieces\n{more}";
pub(crate) const SEARCH: &str = "name: find\nsteps:\n  - name: find\n    vector_store:\n      action: search\n      store: {store}\n      credential: vs\n      collection: {collection}\n      vector: value.v\n      top_k: 2\n{more}";
pub(crate) const DELETE: &str = "name: forget\nsteps:\n  - name: forget\n    vector_store:\n      action: delete\n      store: {store}\n      credential: vs\n      collection: {collection}\n      source: value.source\n{more}";

pub(crate) fn workflow(form: &str, store: &str, collection: &str, more: &str) -> String {
    form.replace("{store}", store)
        .replace("{collection}", collection)
        .replace("{more}", more)
}

/// Three pieces of two documents: each with its vector, its team, its tags.
pub(crate) fn pieces() -> Json {
    json!([
        { "source": "handbook", "position": 0, "text": "Money back on any return within 14 days.",
          "metadata": { "team": "support", "tags": ["faq", "money"], "year": 2024 }, "embedding": [1.0, 0.0, 0.0] },
        { "source": "handbook", "position": 1, "text": "Parcels ship in two working days.",
          "metadata": { "team": "support", "tags": ["faq"], "year": 2024 }, "embedding": [0.0, 1.0, 0.0] },
        { "source": "accounts", "position": 0, "text": "Reset a forgotten password from the sign in page.",
          "metadata": { "team": "it", "tags": ["faq"], "year": 2025 }, "embedding": [0.0, 0.0, 1.0] },
    ])
}

/// The steps on a store, end to end: pieces added (the collection made), found (nearest first, by a filter on a
/// field and on a list), a document added again with fewer pieces (the old one removed), a document deleted. `removes`:
/// whether the store says how many it removed.
pub(crate) async fn scenario(store: &str, collection: &str, vs: Credential, removes: bool) {
    let fx = live(vs);
    let added = result(
        &go(
            &workflow(ADD, store, collection, ""),
            json!({ "pieces": pieces() }),
            &fx,
        )
        .await,
        "store",
    );
    assert_eq!(
        (added["added"].clone(), added["created"].clone()),
        (json!(3), json!(true)),
        "{added}"
    );
    assert_eq!(added["store"], store);
    assert_eq!(added["sources"], json!(["accounts", "handbook"]));
    assert_eq!(added["dimensions"], 3);

    let search = |more: &str| workflow(SEARCH, store, collection, more);
    let found = result(
        &go(&search(""), json!({ "v": [0.9, 0.1, 0.0] }), &fx).await,
        "find",
    );
    assert_eq!(found["count"], 2, "{found}");
    let first = &found["matches"][0];
    assert_eq!(
        (first["id"].clone(), first["source"].clone()),
        (json!("handbook#0"), json!("handbook")),
        "{found}"
    );
    assert_eq!(
        first["metadata"],
        json!({ "team": "support", "tags": ["faq", "money"], "year": 2024 }),
        "{found}"
    );
    assert_eq!(first["position"], 0);
    assert!(first["text"].as_str().unwrap().contains("14 days"));
    let score = first["score"].as_f64().unwrap();
    assert!(
        (score - cosine(&[0.9, 0.1, 0.0], &[1.0, 0.0, 0.0])).abs() < 1e-4,
        "the cosine similarity: {found}"
    );
    assert!(score > found["matches"][1]["score"].as_f64().unwrap());

    let it = result(
        &go(
            &search("      filter: { team: it }\n"),
            json!({ "v": [1.0, 0.0, 0.0] }),
            &fx,
        )
        .await,
        "find",
    );
    assert_eq!(it["count"], 1, "only the team's piece, however far: {it}");
    assert_eq!(it["matches"][0]["source"], "accounts");
    let money = result(
        &go(
            &search("      filter: { tags: [money] }\n"),
            json!({ "v": [0.0, 1.0, 0.0] }),
            &fx,
        )
        .await,
        "find",
    );
    assert_eq!(money["count"], 1, "a list field holding the value: {money}");
    assert_eq!(money["matches"][0]["id"], "handbook#0");

    // The handbook again, its shipping piece gone: that piece is removed.
    let shorter = json!({ "pieces": [pieces()[0].clone()] });
    let again = result(
        &go(&workflow(ADD, store, collection, ""), shorter, &fx).await,
        "store",
    );
    assert_eq!(
        (again["added"].clone(), again["created"].clone()),
        (json!(1), json!(false)),
        "{again}"
    );
    if removes {
        assert_eq!(again["removed"], 1, "{again}");
    }
    let left = result(
        &go(&search(""), json!({ "v": [0.0, 1.0, 0.0] }), &fx).await,
        "find",
    );
    let ids: Vec<&str> = left["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(!ids.contains(&"handbook#1"), "removed: {left}");

    let gone = result(
        &go(
            &workflow(DELETE, store, collection, ""),
            json!({ "source": "accounts" }),
            &fx,
        )
        .await,
        "forget",
    );
    if removes {
        assert_eq!(gone["deleted"], 1, "{gone}");
    }
    let last = result(
        &go(&search(""), json!({ "v": [0.0, 0.0, 1.0] }), &fx).await,
        "find",
    );
    assert_eq!(last["count"], 1, "{last}");
    assert_eq!(last["matches"][0]["id"], "handbook#0");
}

/// A test run sends nothing and shows its first request; a preview searches for real and only shows a write.
pub(crate) async fn test_run_and_preview(
    store: &str,
    collection: &str,
    vs: Credential,
    sent: &dyn Fn() -> usize,
) {
    let masked = openduct_flow::DryRun {
        credentials: [("vs".to_owned(), vs.masked())].into(),
        ..openduct_flow::DryRun::default()
    };
    let before = sent();
    let shown = result(
        &go(
            &workflow(ADD, store, collection, ""),
            json!({ "pieces": pieces() }),
            &masked,
        )
        .await,
        "store",
    );
    assert_eq!(shown["test"], "not sent", "{shown}");
    assert_eq!(shown["added"], 3, "{shown}");
    let shown = result(
        &go(
            &workflow(SEARCH, store, collection, ""),
            json!({ "v": [1.0, 0.0, 0.0] }),
            &masked,
        )
        .await,
        "find",
    );
    assert_eq!(
        (shown["test"].clone(), shown["matches"].clone()),
        (json!("not sent"), json!([])),
        "{shown}"
    );
    assert_eq!(sent(), before, "a test run sent nothing");

    let preview = openduct_flow::Preview::new(Arc::new(live(vs)));
    let found = result(
        &go(
            &workflow(SEARCH, store, collection, ""),
            json!({ "v": [1.0, 0.0, 0.0] }),
            &preview,
        )
        .await,
        "find",
    );
    assert!(
        found["count"].as_u64().unwrap() >= 1,
        "a preview searches: {found}"
    );
    let more = json!({ "pieces": [{ "source": "new", "text": "Kept only in a real run.", "embedding": [0.5, 0.5, 0.0] }] });
    let shown = result(
        &go(&workflow(ADD, store, collection, ""), more, &preview).await,
        "store",
    );
    // A web store's write is a request not sent; Redis's and MongoDB's, a command not written (the storage steps').
    assert!(
        shown["test"] == "not sent" || shown["test"] == "not written",
        "a preview shows a write: {shown}"
    );
    let found = result(
        &go(
            &workflow(SEARCH, store, collection, ""),
            json!({ "v": [0.5, 0.5, 0.0] }),
            &preview,
        )
        .await,
        "find",
    );
    assert!(
        found["matches"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["source"] != "new"),
        "nothing was written: {found}"
    );
}
