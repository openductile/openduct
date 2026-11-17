// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `postgres` step against a real database, when one is given (`cargo xtask services up`, then
//! `eval "$(cargo xtask services env)"`); without OPENDUCT_TEST_POSTGRES each test says it was skipped and passes.
//! The database needs pgvector for the similarity search (the test service has it).
//!
//! Retrieval-augmented answers end to end: texts embedded and kept with their vectors, the nearest found for a
//! question, a model answering from them. The embeddings come from a stand-in that follows the standard API (one
//! dimension for each of three subjects), so the nearest text is known; the model is a stand-in too, or a real one
//! when OPENDUCT_TEST_AI_BASE_URL, OPENDUCT_TEST_AI_MODEL and OPENDUCT_TEST_AI_KEY_FILE name it (see live_ai.rs).

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use openduct_expr::Value;
use openduct_flow::{
    Credential, DryRun, Effects, Event, Live, Outcome, Preview, RunRecord, Secrets, Workflow, run,
};
use serde_json::{Value as Json, json};

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

/// The test database's credential, `None` when none is given.
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

/// A table name of this test's own, so tests run side by side.
fn table(what: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!(
        "flow_{what}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn effects(credentials: Vec<(&str, Credential)>, allow_private: bool) -> Live {
    let kept = Kept(credentials.into_iter().map(|(n, c)| (n.to_owned(), c)).collect());
    Live::new("127.0.0.1:1", allow_private)
        .unwrap()
        .with_secrets(Arc::new(kept))
}

fn go(workflow: &str, input: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap();
    let event = Event {
        value: Value::parse_json(&input.to_string()).unwrap(),
        details: Value::Null,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&workflow, &event, effects))
}

fn done(record: &RunRecord) {
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
}

fn result(record: &RunRecord, step: &str) -> Json {
    let found = record.steps.iter().find(|s| s.name == step).unwrap();
    serde_json::from_str(&found.result.to_json()).unwrap()
}

/// One step of SQL, with `params` (expressions).
fn sql(sql: &str, params: &str) -> String {
    format!(
        "name: sql\nsteps:\n  - name: q\n    postgres:\n      credential: db\n      sql: \"{sql}\"\n      params: {params}\n"
    )
}

#[test]
fn rows_come_back_as_their_types_and_no_value_changes_what_the_sql_does() {
    let Some(db) = database() else { return };
    let live = effects(vec![("db", db)], true);
    let t = table("typed");
    done(&go(
        &sql(
            &format!(
                "CREATE TABLE {t} (id bigint PRIMARY KEY, name text, price numeric(10,2), ok boolean, extra jsonb)"
            ),
            "[]",
        ),
        json!({}),
        &live,
    ));
    let sneaky = format!("x'); DROP TABLE {t}; --");
    let added = go(
        &sql(
            &format!("INSERT INTO {t} VALUES ($1, $2, $3, $4, $5)"),
            "[value.id, value.name, value.price, value.ok, value.extra]",
        ),
        json!({ "id": 9_007_199_254_740_993_i64, "name": sneaky, "price": 19.9, "ok": true, "extra": { "tags": ["a", "b"] } }),
        &live,
    );
    done(&added);
    assert_eq!(result(&added, "q")["command"], "INSERT 0 1");

    let read = go(
        &sql(&format!("SELECT * FROM {t} WHERE id = $1"), "[value.id]"),
        json!({ "id": 9_007_199_254_740_993_i64 }),
        &live,
    );
    done(&read);
    let q = result(&read, "q");
    assert_eq!(q["count"], 1, "{q}");
    assert_eq!(q["columns"], json!(["id", "name", "price", "ok", "extra"]));
    let row = &q["rows"][0];
    assert_eq!(row["id"].to_string(), "9007199254740993", "a big id stays exact");
    assert_eq!(
        row["name"],
        sneaky.as_str(),
        "kept as text, the table still there"
    );
    assert_eq!(row["price"], 19.9);
    assert_eq!(row["ok"], true);
    assert_eq!(row["extra"], json!({ "tags": ["a", "b"] }));
    done(&go(&sql(&format!("DROP TABLE {t}"), "[]"), json!({}), &live));
}

#[test]
fn a_preview_reads_for_real_and_only_shows_what_would_write_and_a_test_runs_nothing() {
    let Some(db) = database() else { return };
    let live = effects(vec![("db", db)], true);
    let t = table("preview");
    done(&go(
        &sql(&format!("CREATE TABLE {t} (n int)"), "[]"),
        json!({}),
        &live,
    ));
    done(&go(
        &sql(&format!("INSERT INTO {t} VALUES (1), (2)"), "[]"),
        json!({}),
        &live,
    ));

    let both = format!(
        "name: both\nsteps:\n  - name: count\n    postgres: {{ credential: db, sql: 'SELECT count(*) AS n FROM {t}' }}\n  - name: add\n    postgres: {{ credential: db, sql: 'INSERT INTO {t} VALUES ($1)', params: [value.n] }}\n"
    );
    let preview = Preview::new(Arc::new(live));
    let shown = go(&both, json!({ "n": 3 }), &preview);
    done(&shown);
    assert_eq!(result(&shown, "count")["rows"][0]["n"], 2, "read for real");
    let add = result(&shown, "add");
    assert_eq!(add["test"], "not written", "{add}");
    assert_eq!(add["params"], json!(["3"]));

    let tested = go(&both, json!({ "n": 3 }), &DryRun::default());
    done(&tested);
    assert_eq!(result(&tested, "count")["test"], "not run");

    let live = effects(vec![("db", database().unwrap())], true);
    let after = go(
        &sql(&format!("SELECT count(*) AS n FROM {t}"), "[]"),
        json!({}),
        &live,
    );
    assert_eq!(result(&after, "q")["rows"][0]["n"], 2, "nothing was written");
    done(&go(&sql(&format!("DROP TABLE {t}"), "[]"), json!({}), &live));
}

#[test]
fn a_database_on_this_machines_own_network_is_refused_unless_allowed() {
    let Some(db) = database() else { return };
    let refused = go(
        &sql("SELECT 1 AS one", "[]"),
        json!({}),
        &effects(vec![("db", db)], false),
    );
    let Outcome::Failed { error, .. } = &refused.outcome else {
        panic!("{}", refused.to_json())
    };
    assert!(error.contains("--allow-private-network"), "{error}");
}

#[test]
fn sql_with_values_written_into_it_is_refused_when_the_workflow_is_read() {
    let error = Workflow::parse(&sql("SELECT * FROM t WHERE id = '{{ value.id }}'", "[]")).unwrap_err();
    assert!(error.contains("params"), "{error}");
}

// --- retrieval-augmented answers ---

const SUBJECTS: [&[&str]; 3] = [
    &["refund", "money", "return"],
    &["ship", "deliver", "parcel"],
    &["password", "sign in", "account"],
];

/// A text's vector from the stand-in: how often it names each subject (never all zero).
fn vector(text: &str) -> Vec<f64> {
    let text = text.to_lowercase();
    SUBJECTS
        .iter()
        .map(|words| 0.01 + words.iter().map(|w| text.matches(w).count()).sum::<usize>() as f64)
        .collect()
}

/// A stand-in for a model API in the standard form: `/embeddings` gives [`vector`]s, `/chat/completions` answers
/// with the system prompt it was given (so a test sees what the model was told). It answers until the test ends.
fn stand_in() -> String {
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
            let asked: Json = serde_json::from_str(&body).unwrap_or(Json::Null);
            let answer = if head.starts_with("POST /v1/embeddings") {
                let inputs: Vec<String> = match &asked["input"] {
                    Json::String(s) => vec![s.clone()],
                    Json::Array(items) => items.iter().map(|i| i.as_str().unwrap_or_default().to_owned()).collect(),
                    _ => Vec::new(),
                };
                let data: Vec<Json> = inputs
                    .iter()
                    .enumerate()
                    .map(|(i, t)| json!({ "object": "embedding", "index": i, "embedding": vector(t) }))
                    .collect();
                json!({ "object": "list", "data": data, "model": asked["model"], "usage": { "prompt_tokens": 4, "total_tokens": 4 } })
            } else {
                let system = asked["messages"][0]["content"].as_str().unwrap_or_default().to_owned();
                json!({
                    "id": "stand-in",
                    "model": asked["model"],
                    "choices": [{ "index": 0, "message": { "role": "assistant", "content": system }, "finish_reason": "stop" }],
                    "usage": { "prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40 },
                })
            }
            .to_string();
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                    answer.len()
                )
                .as_bytes(),
            );
        }
    });
    base
}

/// A real model's credential, when the live settings name one: (credential, model).
fn real_model() -> Option<(Credential, String)> {
    let setting = |name: &str| {
        std::env::var(format!("OPENDUCT_TEST_AI_{name}"))
            .ok()
            .filter(|v| !v.trim().is_empty())
    };
    let (base_url, model, file) = (setting("BASE_URL")?, setting("MODEL")?, setting("KEY_FILE")?);
    let text = std::fs::read_to_string(file).ok()?;
    let line = text.lines().find(|l| !l.trim().is_empty())?.trim().to_owned();
    let key = line
        .split_once('=')
        .map_or(line.as_str(), |(_, v)| v)
        .trim()
        .trim_matches(['"', '\''])
        .to_owned();
    let mut fields: BTreeMap<String, String> =
        [("base_url".to_owned(), base_url), ("api_key".to_owned(), key)].into();
    for (name, field) in [
        ("API", "api"),
        ("KEY_HEADER", "key_header"),
        ("HEADERS", "headers"),
    ] {
        if let Some(value) = setting(name) {
            fields.insert(field.to_owned(), value);
        }
    }
    Some((
        Credential {
            kind: "ai".to_owned(),
            fields,
        },
        model,
    ))
}

#[test]
fn texts_kept_with_their_vectors_are_found_for_a_question_and_answered_from() {
    let Some(db) = database() else { return };
    let embeddings = Credential {
        kind: "ai".to_owned(),
        fields: [("base_url".to_owned(), stand_in())].into(),
    };
    let real = real_model();
    let (brain, model) = real
        .clone()
        .unwrap_or_else(|| (embeddings.clone(), "stand-in".to_owned()));
    let live = effects(vec![("db", db), ("vectors", embeddings), ("brain", brain)], true);
    let t = table("notes");
    done(&go(
        &sql("CREATE EXTENSION IF NOT EXISTS vector", "[]"),
        json!({}),
        &live,
    ));
    done(&go(
        &sql(
            &format!(
                "CREATE TABLE {t} (id bigserial PRIMARY KEY, title text, body text, embedding vector(3))"
            ),
            "[]",
        ),
        json!({}),
        &live,
    ));

    let keep = format!(
        "name: keep a note\nsteps:\n  - name: vec\n    embed: {{ credential: vectors, model: stand-in, input: '{{{{ value.body }}}}' }}\n  - name: keep\n    postgres:\n      credential: db\n      sql: INSERT INTO {t} (title, body, embedding) VALUES ($1, $2, $3::vector)\n      params: [value.title, value.body, steps.vec.embedding]\n"
    );
    for (title, body) in [
        (
            "Refunds",
            "Money back on any return within 14 days of delivery, to the card that paid.",
        ),
        (
            "Shipping",
            "Parcels ship in two working days; tracking comes by e-mail.",
        ),
        ("Accounts", "Reset a forgotten password from the sign in page."),
    ] {
        done(&go(&keep, json!({ "title": title, "body": body }), &live));
    }

    let answer = format!(
        "name: answer\nsteps:\n  - name: vec\n    embed: {{ credential: vectors, model: stand-in, input: '{{{{ value.question }}}}' }}\n  - name: found\n    postgres:\n      credential: db\n      sql: SELECT title, body FROM {t} ORDER BY embedding <=> $1::vector LIMIT 2\n      params: [steps.vec.embedding]\n  - name: reply\n    ai:\n      credential: brain\n      model: {model}\n      system: \"Answer in one sentence, from these notes only: {{{{ steps.found.rows.map(r, r.title + ': ' + r.body).join(' | ') }}}}\"\n      prompt: '{{{{ value.question }}}}'\n      max_tokens: 200\n"
    );
    let record = go(
        &answer,
        json!({ "question": "How long do I have to get a refund?" }),
        &live,
    );
    done(&record);
    let found = result(&record, "found");
    assert_eq!(found["count"], 2);
    assert_eq!(
        found["rows"][0]["title"], "Refunds",
        "the nearest note first: {found}"
    );
    let reply = result(&record, "reply");
    let told = reply["transcript"][0]["content"].as_str().unwrap();
    assert!(
        told.contains("within 14 days"),
        "the model was given the note: {told}"
    );
    assert!(reply["usage"]["total_tokens"].as_u64().unwrap() > 0);
    if real.is_some() {
        let text = reply["text"].as_str().unwrap();
        assert!(text.contains("14"), "a real model answered from the note: {text}");
    }
    done(&go(&sql(&format!("DROP TABLE {t}"), "[]"), json!({}), &live));
}
