// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The retrieval steps end to end: documents split, kept with their vectors, found again by a question (with a
//! filter, reranked), answered from with every passage shown, replaced, deleted - and in a test run and a preview.
//!
//! The model is a stand-in that speaks the standard API over HTTP (one dimension for each of three subjects, so the
//! nearest piece is known; a chat answer that cites passage 1; a rerank that reverses). The database is, by default, a
//! stand-in too: it reads the statements the steps send and keeps their pieces in memory, so the SQL and its
//! parameters are checked here on every run. With OPENDUCT_TEST_POSTGRES (`cargo xtask services up`), the same
//! scenario runs against PostgreSQL with pgvector as well.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::{
    Credential, DryRun, Effects, Event, HttpRequest, Live, Outcome, Preview, RunRecord, Secrets, SqlRequest,
    Workflow, run,
};
use serde_json::{Value as Json, json};

type Pending<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

// --- the stand-in model ---

const SUBJECTS: [&[&str]; 3] = [
    &["refund", "money", "return"],
    &["ship", "deliver", "parcel"],
    &["password", "sign in", "account"],
];

/// A text's vector: how often it names each subject (never all zero).
fn vector(text: &str) -> Vec<f64> {
    let text = text.to_lowercase();
    SUBJECTS
        .iter()
        .map(|words| 0.01 + words.iter().map(|w| text.matches(w).count()).sum::<usize>() as f64)
        .collect()
}

const ANSWER: &str = "Within 14 days, to the card that paid [1].";

/// A model API in the standard form on 127.0.0.1, answering until the test ends; counts what it is asked.
fn stand_in(asked: Arc<AtomicUsize>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut got = Vec::new();
            let mut buf = vec![0u8; 65536];
            let (head, body) = loop {
                let n = stream.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break (String::new(), String::new());
                }
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).into_owned();
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
                        break (head.to_owned(), rest.to_owned());
                    }
                }
            };
            asked.fetch_add(1, Ordering::SeqCst);
            let asked: Json = serde_json::from_str(&body).unwrap_or(Json::Null);
            let (status, answer) = if head.starts_with("POST /v1/embeddings") {
                let inputs: Vec<String> = asked["input"]
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .map(|i| i.as_str().unwrap_or_default().to_owned())
                            .collect()
                    })
                    .unwrap_or_else(|| vec![asked["input"].as_str().unwrap_or_default().to_owned()]);
                let data: Vec<Json> = inputs
                    .iter()
                    .enumerate()
                    .map(|(i, t)| json!({ "object": "embedding", "index": i, "embedding": vector(t) }))
                    .collect();
                (
                    200,
                    json!({ "object": "list", "data": data, "usage": { "prompt_tokens": 4, "total_tokens": 4 } }),
                )
            } else if head.starts_with("POST /v1/rerank") {
                let n = asked["documents"].as_array().map_or(0, Vec::len);
                let top = asked["top_n"].as_u64().unwrap_or(n as u64) as usize;
                let results: Vec<Json> = (0..n)
                    .rev()
                    .take(top)
                    .enumerate()
                    .map(
                        |(rank, index)| json!({ "index": index, "relevance_score": 0.9 - rank as f64 * 0.1 }),
                    )
                    .collect();
                (200, json!({ "results": results }))
            } else if head.starts_with("POST /v1/chat/completions") {
                (
                    200,
                    json!({
                        "model": asked["model"],
                        "choices": [{ "index": 0, "message": { "role": "assistant", "content": ANSWER }, "finish_reason": "stop" }],
                        "usage": { "prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40 },
                    }),
                )
            } else {
                (404, json!({ "error": { "message": "no such route" } }))
            };
            let answer = answer.to_string();
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                    answer.len()
                )
                .as_bytes(),
            );
        }
    });
    base
}

// --- the stand-in database ---

#[derive(Debug, Clone)]
struct Row {
    id: String,
    source: String,
    position: i64,
    content: String,
    metadata: Json,
    embedding: Vec<f64>,
}

/// What effects answer when SQL run to read only would write (a preview's, as the database says it).
const WRITES: &str = "this SQL writes, and this run only reads";

#[derive(Debug, Default)]
struct Table {
    made: bool,
    rows: Vec<Row>,
}

fn holds(metadata: &Json, filter: &Json) -> bool {
    match (metadata, filter) {
        (Json::Object(m), Json::Object(f)) => {
            f.iter().all(|(k, v)| m.get(k).is_some_and(|have| holds(have, v)))
        }
        (Json::Array(have), Json::Array(want)) => want.iter().all(|w| have.iter().any(|h| holds(h, w))),
        (a, b) => a == b,
    }
}

fn distance(op: &str, a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    match op {
        "<=>" => {
            let norm = |v: &[f64]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
            1.0 - dot / (norm(a) * norm(b))
        }
        "<#>" => -dot,
        _ => a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum::<f64>().sqrt(),
    }
}

fn param(r: &SqlRequest, i: usize) -> Json {
    serde_json::from_str(r.params[i].as_deref().unwrap()).unwrap()
}

impl Table {
    fn answer(&mut self, r: &SqlRequest) -> Result<Json, String> {
        let sql = r.sql.as_str();
        let writes = !sql.starts_with("SELECT");
        if writes && r.read_only {
            return Err(WRITES.to_owned());
        }
        let done = |rows: Vec<Json>, command: String| json!({ "rows": rows, "count": rows.len(), "columns": [], "command": command });
        if sql.starts_with("SELECT to_regclass") {
            let name = self.made.then(|| r.params[0].clone().unwrap());
            return Ok(done(
                vec![json!({ "kept_table": name, "kept_index": name })],
                "SELECT 1".to_owned(),
            ));
        }
        if sql.starts_with("CREATE TABLE") {
            self.made = true;
        }
        if sql.starts_with("CREATE") {
            return Ok(done(vec![], "CREATE".to_owned()));
        }
        if !self.made {
            return Err("relation does not exist".to_owned());
        }
        if sql.starts_with("INSERT INTO") {
            let pieces = param(r, 0);
            let pieces = pieces.as_array().unwrap();
            for p in pieces {
                let text = p["embedding"].as_str().unwrap();
                let row = Row {
                    id: p["id"].as_str().unwrap().to_owned(),
                    source: p["source"].as_str().unwrap().to_owned(),
                    position: p["position"].as_i64().unwrap(),
                    content: p["content"].as_str().unwrap().to_owned(),
                    metadata: p["metadata"].clone(),
                    embedding: serde_json::from_str(text).unwrap(),
                };
                self.rows.retain(|old| old.id != row.id);
                self.rows.push(row);
            }
            return Ok(done(vec![], format!("INSERT 0 {}", pieces.len())));
        }
        if sql.starts_with("DELETE FROM") {
            let sources = param(r, 0);
            let keep: Option<Json> = sql.contains("NOT IN").then(|| param(r, 1));
            let before = self.rows.len();
            self.rows.retain(|row| {
                let named = sources
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|s| s == row.source.as_str());
                let kept = keep
                    .as_ref()
                    .is_some_and(|ids| ids.as_array().unwrap().iter().any(|i| i == row.id.as_str()));
                !named || kept
            });
            return Ok(done(vec![], format!("DELETE {}", before - self.rows.len())));
        }
        if sql.starts_with("SELECT id, source") {
            let query: Vec<f64> = serde_json::from_str(r.params[0].as_deref().unwrap()).unwrap();
            let (filter, limit) = if sql.contains("@>") {
                (Some(param(r, 1)), param(r, 2))
            } else {
                (None, param(r, 1))
            };
            let op = ["<=>", "<->", "<#>"]
                .into_iter()
                .find(|op| sql.contains(op))
                .unwrap();
            let mut found: Vec<(f64, &Row)> = self
                .rows
                .iter()
                .filter(|row| filter.as_ref().is_none_or(|f| holds(&row.metadata, f)))
                .map(|row| (distance(op, &query, &row.embedding), row))
                .collect();
            found.sort_by(|a, b| a.0.total_cmp(&b.0));
            let rows = found
                .into_iter()
                .take(limit.as_u64().unwrap() as usize)
                .map(|(d, row)| {
                    json!({ "id": row.id, "source": row.source, "position": row.position, "content": row.content,
                            "metadata": row.metadata, "distance": d })
                })
                .collect();
            return Ok(done(rows, "SELECT".to_owned()));
        }
        Err(format!("the stand-in database does not read {sql}"))
    }
}

/// Effects that reach the stand-in model for real and the stand-in database in memory; every statement kept.
#[derive(Debug)]
struct Stand {
    live: Live,
    table: Mutex<Table>,
    statements: Mutex<Vec<SqlRequest>>,
}

impl Effects for Stand {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        self.live.http(request)
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no topics here".to_owned()) })
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        self.live.credential(name)
    }

    fn file<'a>(&'a self, _: &'a str) -> Pending<'a> {
        Box::pin(async { Err("no files here".to_owned()) })
    }

    fn sql<'a>(&'a self, request: SqlRequest) -> Pending<'a> {
        self.statements.lock().unwrap().push(request.clone());
        let answer = self.table.lock().unwrap().answer(&request);
        Box::pin(async move { answer.map(|j| Value::from_json(&j)) })
    }
}

// --- the scenario ---

fn live(credentials: Vec<(&str, Credential)>) -> Live {
    let kept = Kept(credentials.into_iter().map(|(n, c)| (n.to_owned(), c)).collect());
    Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(Arc::new(kept))
}

fn model_credential(base: String) -> Credential {
    Credential {
        kind: "ai".to_owned(),
        fields: [
            ("base_url".to_owned(), base),
            ("model".to_owned(), "stand-in-chat".to_owned()),
            ("embedding_model".to_owned(), "stand-in-vectors".to_owned()),
        ]
        .into(),
    }
}

async fn go(workflow: &str, input: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap_or_else(|e| panic!("{e}\n{workflow}"));
    let event = Event {
        value: Value::parse_json(&input.to_string()).unwrap(),
        details: Value::Null,
    };
    run(&workflow, &event, effects).await
}

fn result(record: &RunRecord, step: &str) -> Json {
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
    let found = record.steps.iter().find(|s| s.name == step).unwrap();
    serde_json::from_str(&found.result.to_json()).unwrap()
}

fn keep(table: &str) -> String {
    format!(
        r#"
name: keep
steps:
  - name: split
    split_text:
      text: "{{{{ value.text }}}}"
      source: "{{{{ value.source }}}}"
      by: markdown
      size: 200
      metadata: {{ team: "{{{{ value.team }}}}" }}
  - name: store
    vector_store:
      action: add
      credential: db
      table: {table}
      create: true
      embed_with: model
      pieces: steps.split.pieces
"#
    )
}

fn search(table: &str, more: &str) -> String {
    format!(
        "name: search\nsteps:\n  - name: find\n    vector_store:\n      action: search\n      credential: db\n      table: {table}\n      embed_with: model\n      query: \"{{{{ value.q }}}}\"\n{more}"
    )
}

const HANDBOOK: &str = "# Refunds\n\nMoney back on any return within 14 days, to the card that paid.\n\n# Shipping\n\nParcels ship in two working days, and every parcel is tracked.";

/// Everything the steps do, on `effects` (the stand-in database, or a real one), in table `table`.
async fn scenario(effects: Arc<dyn Effects>, table: &str, asked: &AtomicUsize) {
    let fx: &dyn Effects = effects.as_ref();
    // Two documents kept: the handbook in two pieces (one a heading each), the accounts page in one.
    let kept = go(
        &keep(table),
        json!({ "source": "handbook", "team": "support", "text": HANDBOOK }),
        fx,
    )
    .await;
    let split = result(&kept, "split");
    assert_eq!(split["count"], 2, "{split}");
    assert_eq!(split["pieces"][0]["id"], "handbook#0");
    assert_eq!(
        split["pieces"][1]["metadata"],
        json!({ "team": "support", "headings": ["Shipping"] })
    );
    let store = result(&kept, "store");
    assert_eq!(
        (store["added"].clone(), store["removed"].clone()),
        (json!(2), json!(0)),
        "{store}"
    );
    assert_eq!(store["dimensions"], 3);
    assert_eq!(store["sources"], json!(["handbook"]));
    let accounts = json!({ "source": "accounts", "team": "it", "text": "# Passwords\n\nReset a forgotten password from the sign in page." });
    assert_eq!(
        result(&go(&keep(table), accounts, fx).await, "store")["created"],
        false,
        "made once"
    );

    // The nearest piece first, with its score; a filter keeps to one team's.
    let found = result(
        &go(
            &search(table, "      top_k: 2\n"),
            json!({ "q": "How long do I have for a refund?" }),
            fx,
        )
        .await,
        "find",
    );
    assert_eq!(found["count"], 2, "{found}");
    let first = &found["matches"][0];
    assert_eq!(
        (first["id"].clone(), first["source"].clone()),
        (json!("handbook#0"), json!("handbook"))
    );
    assert!(first["text"].as_str().unwrap().contains("14 days"));
    assert!(
        first["score"].as_f64().unwrap() > 0.9
            && first["score"].as_f64().unwrap() > found["matches"][1]["score"].as_f64().unwrap()
    );
    let it = result(
        &go(
            &search(table, "      filter: { team: it }\n"),
            json!({ "q": "refund" }),
            fx,
        )
        .await,
        "find",
    );
    assert_eq!(it["count"], 1, "only the team's piece, however far: {it}");
    assert_eq!(it["matches"][0]["source"], "accounts");
    let least = result(
        &go(
            &search(table, "      min_score: 0.9\n"),
            json!({ "q": "parcel" }),
            fx,
        )
        .await,
        "find",
    );
    assert_eq!(least["count"], 1, "{least}");

    // Reranked: the stand-in reverses what it is given, and both scores are kept.
    let reranked = result(
        &go(
            &search(
                table,
                "      top_k: 1\n      rerank: { model: stand-in-ranker, candidates: 3 }\n",
            ),
            json!({ "q": "refund" }),
            fx,
        )
        .await,
        "find",
    );
    assert_eq!(reranked["count"], 1, "{reranked}");
    assert_ne!(
        reranked["matches"][0]["id"], "handbook#0",
        "the rerank model's order: {reranked}"
    );
    assert_eq!(reranked["matches"][0]["score"], 0.9);
    assert!(reranked["matches"][0]["vector_score"].is_number());

    // An answer from the documents, with each passage it was given.
    let ask = format!(
        "name: ask\nsteps:\n  - name: answer\n    ask_documents:\n      credential: db\n      table: {table}\n      embed_with: model\n      question: \"{{{{ value.q }}}}\"\n      top_k: 2\n      instructions: Answer in one sentence.\n"
    );
    let answer = result(
        &go(&ask, json!({ "q": "How long do I have for a refund?" }), fx).await,
        "answer",
    );
    assert_eq!(answer["text"], ANSWER);
    assert_eq!(answer["answered"], true);
    assert_eq!(answer["cited"], json!([1]));
    let sources = answer["sources"].as_array().unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(
        (sources[0]["n"].clone(), sources[0]["source"].clone()),
        (json!(1), json!("handbook"))
    );
    assert_eq!(
        (sources[0]["cited"].clone(), sources[1]["cited"].clone()),
        (json!(true), json!(false))
    );
    assert!(sources[0]["score"].is_number() && sources[0]["used"] == true);
    let transcript = answer["transcript"].as_array().unwrap();
    let system = transcript[0]["content"].as_str().unwrap();
    assert!(
        system.contains("from nothing else") && system.ends_with("Answer in one sentence."),
        "{system}"
    );
    let user = transcript[1]["content"].as_str().unwrap();
    assert!(
        user.starts_with("Passages:\n\n[1] From: handbook\n# Refunds"),
        "{user}"
    );
    assert!(
        user.ends_with("Question: How long do I have for a refund?"),
        "{user}"
    );
    assert_eq!(answer["usage"]["total_tokens"], 40);
    assert_eq!(answer["metrics"]["found"], 2);

    // The handbook kept again without its shipping section: that piece goes.
    let shorter =
        json!({ "source": "handbook", "team": "support", "text": "# Refunds\n\nMoney back within 14 days." });
    let store = result(&go(&keep(table), shorter, fx).await, "store");
    assert_eq!(
        (store["added"].clone(), store["removed"].clone()),
        (json!(1), json!(1)),
        "{store}"
    );

    // A test run sends nothing and shows what it would; a preview reads for real and writes nothing.
    let before = asked.load(Ordering::SeqCst);
    let tested = go(
        &keep(table),
        json!({ "source": "x", "team": "t", "text": "Hello." }),
        &DryRun::default(),
    )
    .await;
    let shown = result(&tested, "store");
    assert_eq!(shown["test"], "not sent", "{shown}");
    assert_eq!(shown["added"], 1);
    let shown = result(
        &go(&ask, json!({ "q": "refund?" }), &DryRun::default()).await,
        "answer",
    );
    assert_eq!(
        (shown["test"].clone(), shown["sources"].clone()),
        (json!("not sent"), json!([])),
        "{shown}"
    );
    assert_eq!(asked.load(Ordering::SeqCst), before, "a test run asked nothing");
    let preview = Preview::new(effects.clone());
    let previewed = go(
        &keep(table),
        json!({ "source": "new", "team": "t", "text": "Return it." }),
        &preview,
    )
    .await;
    assert_eq!(
        result(&previewed, "store")["test"],
        "not written",
        "{}",
        previewed.to_json()
    );
    assert!(
        asked.load(Ordering::SeqCst) > before,
        "the preview's vectors were asked for"
    );
    let answered = go(&ask, json!({ "q": "How long do I have for a refund?" }), &preview).await;
    assert_eq!(
        result(&answered, "answer")["text"],
        ANSWER,
        "a preview answers for real"
    );

    // A document's pieces deleted; the rest stay.
    let delete = format!(
        "name: delete\nsteps:\n  - name: gone\n    vector_store: {{ action: delete, credential: db, table: {table}, source: value.source }}\n"
    );
    let gone = result(&go(&delete, json!({ "source": "handbook" }), fx).await, "gone");
    assert_eq!(gone["deleted"], 1, "{gone}");
    let left = result(
        &go(
            &search(table, "      top_k: 10\n"),
            json!({ "q": "anything" }),
            fx,
        )
        .await,
        "find",
    );
    let sources: Vec<&str> = left["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["source"].as_str().unwrap())
        .collect();
    assert_eq!(
        sources,
        ["accounts"],
        "the preview wrote nothing, the delete took the handbook"
    );
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn documents_are_split_kept_found_answered_from_and_deleted() {
    let asked = Arc::new(AtomicUsize::new(0));
    let model = model_credential(stand_in(asked.clone()));
    let db = Credential {
        kind: "postgres".to_owned(),
        fields: [
            ("url".to_owned(), "postgres://127.0.0.1:1/none".to_owned()),
            ("username".to_owned(), "u".to_owned()),
        ]
        .into(),
    };
    let stand = Arc::new(Stand {
        live: live(vec![("model", model), ("db", db)]),
        table: Mutex::new(Table::default()),
        statements: Mutex::new(Vec::new()),
    });
    runtime().block_on(scenario(stand.clone(), "kb.doc_pieces", &asked));

    // No value was ever written into the SQL: the documents' texts, sources and filters went as parameters.
    let statements = stand.statements.lock().unwrap();
    assert!(statements.len() > 10);
    for s in statements.iter() {
        for value in ["handbook", "accounts", "support", "14 days", "Refunds"] {
            assert!(!s.sql.contains(value), "{value} in {}", s.sql);
        }
        // The table is named in the SQL, quoted - or, when its being there is asked, given as a parameter too.
        let named = s.sql.contains("\"kb\".\"doc_pieces\"");
        assert!(
            named || s.sql.starts_with("CREATE EXTENSION") || s.sql.starts_with("SELECT to_regclass"),
            "{}",
            s.sql
        );
    }
}

#[test]
fn values_meant_to_break_the_sql_are_kept_as_text() {
    let asked = Arc::new(AtomicUsize::new(0));
    let stand = Arc::new(Stand {
        live: live(vec![
            ("model", model_credential(stand_in(asked))),
            (
                "db",
                Credential {
                    kind: "postgres".to_owned(),
                    fields: BTreeMap::new(),
                },
            ),
        ]),
        table: Mutex::new(Table::default()),
        statements: Mutex::new(Vec::new()),
    });
    let sneaky = "x'); DROP TABLE doc_pieces; --";
    runtime().block_on(async {
        let record = go(
            &keep("doc_pieces"),
            json!({ "source": sneaky, "team": sneaky, "text": "A refund." }),
            stand.as_ref(),
        )
        .await;
        assert_eq!(result(&record, "store")["sources"], json!([sneaky]));
        let filter = format!("{}      filter: \"{{'team': value.q}}\"\n", "");
        let found = result(
            &go(
                &search("doc_pieces", &filter),
                json!({ "q": sneaky }),
                stand.as_ref(),
            )
            .await,
            "find",
        );
        assert_eq!(found["matches"][0]["source"], sneaky);
    });
    for s in stand.statements.lock().unwrap().iter() {
        assert!(!s.sql.contains("DROP"), "{}", s.sql);
    }
    // A table's name is checked when the workflow is read: it is the one thing written into the SQL.
    let bad = keep("doc_pieces; DROP TABLE users");
    let e = Workflow::parse(&bad).unwrap_err();
    assert!(e.contains("is not a table name"), "{e}");
}

#[test]
fn mistakes_are_said_when_the_workflow_is_read_and_when_it_runs() {
    let read = |steps: &str| {
        Workflow::parse(&format!("name: t\nsteps:\n{steps}"))
            .map(|_| ())
            .unwrap_err()
    };
    for (steps, says) in [
        (
            "  - split_text: { by: words, text: x }",
            "split_text.by: characters, paragraphs",
        ),
        (
            "  - split_text: { text: x, documents: value.d }",
            "one of the two",
        ),
        (
            "  - split_text: { text: x, size: 100, overlap: 80 }",
            "overlap: 0 to half the size (50)",
        ),
        (
            "  - vector_store: { action: add, credential: db, table: t }",
            "pieces: the pieces to add",
        ),
        (
            "  - vector_store: { action: find, credential: db, table: t }",
            "add, search or delete",
        ),
        (
            "  - vector_store: { action: delete, credential: db, table: t, source: x, top_k: 3 }",
            "top_k: not a setting of vector_store's delete",
        ),
        (
            "  - vector_store: { action: search, credential: db, table: t, query: x }",
            "embed_with: the AI credential",
        ),
        (
            "  - vector_store: { action: search, credential: db, table: t, vector: value.v, rerank: { model: r } }",
            "give query, not vector",
        ),
        (
            "  - vector_store: { action: search, credential: db, table: t, vector: value.v, top_k: 500 }",
            "top_k: a whole number, 1 to 50",
        ),
        (
            "  - ask_documents: { credential: db, table: t, question: q }",
            "embed_with",
        ),
        (
            "  - ask_documents: { credential: db, table: t, embed_with: m, question: q, temperature: 5 }",
            "temperature: a number, 0 to 2",
        ),
        (
            "  - ask_documents: { credential: db, table: Docs, embed_with: m, question: q }",
            "is not a table name",
        ),
    ] {
        let e = read(steps);
        assert!(e.contains(says), "{steps}: {e}");
    }

    let asked = Arc::new(AtomicUsize::new(0));
    let stand = Stand {
        live: live(vec![
            ("model", model_credential(stand_in(asked))),
            (
                "db",
                Credential {
                    kind: "postgres".to_owned(),
                    fields: BTreeMap::new(),
                },
            ),
            (
                "mail",
                Credential {
                    kind: "smtp".to_owned(),
                    fields: BTreeMap::new(),
                },
            ),
        ]),
        table: Mutex::new(Table::default()),
        statements: Mutex::new(Vec::new()),
    };
    let failed = |record: RunRecord| match &record.outcome {
        Outcome::Failed { error, .. } => error.clone(),
        other => panic!("{other:?}: {}", record.to_json()),
    };
    runtime().block_on(async {
        let wrong = keep("t").replace("credential: db", "credential: mail");
        let e = failed(go(&wrong, json!({ "source": "s", "team": "t", "text": "x" }), &stand).await);
        assert_eq!(e, "mail is a smtp credential: a vector store takes a postgres one");
        let no_source = "name: t\nsteps:\n  - vector_store: { action: add, credential: db, table: t, embed_with: model, pieces: \"[{'text': 'hi'}]\" }\n";
        let e = failed(go(no_source, json!({}), &stand).await);
        assert!(e.contains("pieces[0].source: the document it is from"), "{e}");
        let narrow = "name: t\nsteps:\n  - vector_store: { action: add, credential: db, table: t, dimensions: 5, embed_with: model, pieces: \"[{'text': 'hi', 'source': 's'}]\" }\n";
        let e = failed(go(narrow, json!({}), &stand).await);
        assert!(e.contains("a vector of 3 numbers, where the store's have 5"), "{e}");
        let unknown = "name: t\nsteps:\n  - vector_store: { action: search, credential: db, table: never_made, vector: '[1, 2, 3]' }\n";
        let e = failed(go(unknown, json!({}), &stand).await);
        assert!(e.contains("does not exist"), "the database's own words: {e}");
    });
}

// --- against PostgreSQL with pgvector, when one is given ---

fn database() -> Option<Credential> {
    let setting = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let (Some(url), Some(username), Some(password)) = (
        setting("OPENDUCT_TEST_POSTGRES"),
        setting("OPENDUCT_TEST_POSTGRES_USER"),
        setting("OPENDUCT_TEST_POSTGRES_PASSWORD"),
    ) else {
        eprintln!("skipped: OPENDUCT_TEST_POSTGRES is not set (cargo xtask services up)");
        return None;
    };
    Some(Credential {
        kind: "postgres".to_owned(),
        fields: [
            ("url".to_owned(), url),
            ("username".to_owned(), username),
            ("password".to_owned(), password),
            ("tls".to_owned(), "disable".to_owned()),
        ]
        .into(),
    })
}

#[test]
fn the_same_against_postgresql_with_pgvector() {
    let Some(db) = database() else { return };
    let asked = Arc::new(AtomicUsize::new(0));
    let effects: Arc<dyn Effects> = Arc::new(live(vec![
        ("model", model_credential(stand_in(asked.clone()))),
        ("db", db),
    ]));
    let table = format!("retrieval_{}", std::process::id());
    runtime().block_on(async {
        scenario(effects.clone(), &table, &asked).await;
        let drop =
            format!("name: drop\nsteps:\n  - postgres: {{ credential: db, sql: 'DROP TABLE {table}' }}\n");
        assert_eq!(
            go(&drop, json!({}), effects.as_ref()).await.outcome,
            Outcome::Done
        );
    });
}
