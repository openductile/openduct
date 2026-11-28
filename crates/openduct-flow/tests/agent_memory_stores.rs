// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An agent's memory kept in a store of the workflow's own - a PostgreSQL table, Redis keys, an OpenDuct data table -
//! with the broker's rules: the last turns of a conversation (the window) given back, another conversation's apart,
//! forgotten when the clear condition holds, kept until its time to forget, under the workspace's own keys. The model
//! is scripted; PostgreSQL is a stand-in that reads the statements the store sends; Redis is the RESP stand-in. No
//! real model or database is asked.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod vector_support;

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::tables::{Column, ColumnType, MemoryTables, Tables};
use openduct_flow::{Credential, DryRun, Effects, HttpRequest, Live, SqlRequest};
use serde_json::{Value as Json, json};
use vector_support::{Kept, go, native, result};

type Pending<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A memory table as the stand-in database keeps it: each key's turns (JSON text) and when it is forgotten.
#[derive(Debug, Default)]
struct MemoryTable {
    made: bool,
    rows: BTreeMap<String, (String, i64)>,
}

impl MemoryTable {
    fn answer(&mut self, r: &SqlRequest) -> Result<Json, String> {
        let sql = r.sql.as_str();
        let p = |i: usize| r.params[i].clone().unwrap();
        let done = |rows: Vec<Json>, command: &str| json!({ "rows": rows, "count": rows.len(), "columns": [], "command": command });
        if sql.starts_with("SELECT to_regclass($1)") {
            return Ok(done(vec![json!({ "kept": self.made.then(|| p(0)) })], "SELECT 1"));
        }
        if sql.starts_with("CREATE TABLE IF NOT EXISTS \"agent_memory\" (key text PRIMARY KEY, turns jsonb") {
            self.made = true;
            return Ok(done(vec![], "CREATE TABLE"));
        }
        if sql.starts_with(
            "CREATE INDEX IF NOT EXISTS \"agent_memory_forget_at_idx\" ON \"agent_memory\" (forget_at)",
        ) {
            return Ok(done(vec![], "CREATE INDEX"));
        }
        if !self.made {
            return Err("relation \"agent_memory\" does not exist".to_owned());
        }
        let now = now_ms();
        if sql.contains("WHERE forget_at <= now() LIMIT 16") {
            let before = self.rows.len();
            self.rows.retain(|_, (_, at)| *at > now);
            return Ok(done(vec![], &format!("DELETE {}", before - self.rows.len())));
        }
        if sql.starts_with("INSERT INTO \"agent_memory\" (key, turns, forget_at, updated_at) VALUES ($1, $2::jsonb, now() + ($3::bigint * interval '1 millisecond')") {
            assert!(sql.ends_with("ON CONFLICT (key) DO UPDATE SET turns = EXCLUDED.turns, forget_at = EXCLUDED.forget_at, updated_at = now()"));
            let keep: i64 = p(2).parse().unwrap();
            self.rows.insert(p(0), (p(1), now + keep));
            return Ok(done(vec![], "INSERT 0 1"));
        }
        if sql == "SELECT turns::text AS turns FROM \"agent_memory\" WHERE key = $1 AND forget_at > now()" {
            let rows = self
                .rows
                .get(&p(0))
                .filter(|(_, at)| *at > now)
                .map(|(t, _)| json!({ "turns": t }));
            return Ok(done(rows.into_iter().collect(), "SELECT"));
        }
        if sql == "DELETE FROM \"agent_memory\" WHERE key = $1" {
            let n = usize::from(self.rows.remove(&p(0)).is_some());
            return Ok(done(vec![], &format!("DELETE {n}")));
        }
        Err(format!("the stand-in database does not read {sql}"))
    }
}

/// Live effects (for Redis, tables and the workspace's keys) with a scripted model and the stand-in database.
#[derive(Debug)]
struct Fx {
    live: Live,
    asked: Mutex<Vec<Json>>,
    db: Mutex<MemoryTable>,
}

impl Fx {
    fn new(live: Live) -> Self {
        Self {
            live,
            asked: Mutex::default(),
            db: Mutex::default(),
        }
    }

    /// The messages the model was sent the last time.
    fn last_messages(&self) -> Vec<String> {
        let asked = self.asked.lock().unwrap();
        asked.last().unwrap()["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["content"].as_str().unwrap().to_owned())
            .collect()
    }
}

impl Effects for Fx {
    fn http<'a>(&'a self, r: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let body: Json = serde_json::from_slice(&r.body.unwrap().1).unwrap();
            let mut asked = self.asked.lock().unwrap();
            asked.push(body);
            let answer = format!("answer {}", asked.len());
            Ok(Value::from_json(&json!({ "status": 200, "headers": {}, "body": {
                "choices": [{ "index": 0, "message": { "role": "assistant", "content": answer }, "finish_reason": "stop" }],
                "usage": { "prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7 },
            } })))
        })
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
        let answer = self.db.lock().unwrap().answer(&request);
        Box::pin(async move { answer.map(|j| Value::from_json(&j)) })
    }

    fn storage(&self) -> openduct_flow::storage::Reach<'_> {
        self.live.storage()
    }

    fn tables(&self) -> openduct_flow::tables::Access<'_> {
        self.live.tables()
    }

    fn memory_prefix(&self) -> String {
        self.live.memory_prefix()
    }
}

fn credentials(redis_port: u16) -> Arc<Kept> {
    let c = |kind: &str, fields: &[(&str, String)]| Credential {
        kind: kind.to_owned(),
        fields: fields.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect(),
    };
    Arc::new(Kept(
        [
            (
                "model".to_owned(),
                c(
                    "ai",
                    &[
                        ("base_url", "https://models.example.com/v1".to_owned()),
                        ("model", "small-1".to_owned()),
                    ],
                ),
            ),
            (
                "db".to_owned(),
                c(
                    "postgres",
                    &[
                        ("url", "postgres://db.example.com/app".to_owned()),
                        ("username", "app".to_owned()),
                    ],
                ),
            ),
            (
                "cache".to_owned(),
                c("redis", &[("url", format!("redis://127.0.0.1:{redis_port}"))]),
            ),
        ]
        .into(),
    ))
}

fn chat(store: &str) -> String {
    format!(
        "name: chat\nsteps:\n  - name: helper\n    agent:\n      credential: model\n      input: \"{{{{ value.text }}}}\"\n      memory:\n        key: \"{{{{ value.chat }}}}\"\n        window: 2\n        forget_after: 1h\n        clear: \"value.text == '/reset'\"\n{store}"
    )
}

/// The broker's rules, on a store: the window, another conversation apart, clearing.
async fn rules(fx: &Fx, store: &str) {
    let flow = chat(store);
    let say = |chat: &'static str, text: &'static str| {
        let flow = flow.clone();
        async move {
            result(
                &go(&flow, json!({ "chat": chat, "text": text }), fx).await,
                "helper",
            )
        }
    };
    assert_eq!(say("c1", "one").await["memory"]["recalled"], 0);
    assert_eq!(say("c1", "two").await["memory"]["recalled"], 2);
    let third = say("c1", "three").await;
    assert_eq!(third["memory"]["recalled"], 4, "two turns, the window: {third}");
    assert_eq!(
        fx.last_messages(),
        ["one", "answer 1", "two", "answer 2", "three"]
    );
    let fourth = say("c1", "four").await;
    assert_eq!(fourth["memory"]["recalled"], 4);
    assert_eq!(
        fx.last_messages(),
        ["two", "answer 2", "three", "answer 3", "four"]
    );
    assert_eq!(
        say("c2", "hello").await["memory"]["recalled"],
        0,
        "another conversation is its own"
    );
    let reset = say("c1", "/reset").await;
    assert_eq!(
        (
            reset["memory"]["cleared"].clone(),
            reset["memory"]["recalled"].clone()
        ),
        (json!(true), json!(0))
    );
    assert_eq!(fx.last_messages(), ["/reset"]);
    assert_eq!(
        say("c1", "again").await["memory"]["recalled"],
        2,
        "kept afresh after the reset"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_in_postgresql_keeps_the_window_clears_and_forgets_on_time() {
    let live = Live::new("127.0.0.1:1", true)
        .unwrap()
        .in_workspace("a")
        .with_secrets(credentials(1));
    let fx = Fx::new(live);
    rules(&fx, "        store: postgres\n        credential: db\n        table: agent_memory\n        create: true\n").await;
    let keys: Vec<String> = fx.db.lock().unwrap().rows.keys().cloned().collect();
    assert_eq!(
        keys,
        ["w.a.chat/helper/c1", "w.a.chat/helper/c2"],
        "the workspace's own keys"
    );
    let (turns, forget_at) = fx.db.lock().unwrap().rows["w.a.chat/helper/c1"].clone();
    let turns: Json = serde_json::from_str(&turns).unwrap();
    assert_eq!(
        turns["turns"].as_array().unwrap().len(),
        4,
        "the reset turn and the next: two turns"
    );
    assert!(
        (forget_at - now_ms() - 3_600_000).abs() < 60_000,
        "forgotten an hour after its last turn"
    );
    // A conversation past its time is not given back, and goes on the next write.
    fx.db
        .lock()
        .unwrap()
        .rows
        .get_mut("w.a.chat/helper/c2")
        .unwrap()
        .1 = now_ms() - 1;
    let flow = chat(
        "        store: postgres\n        credential: db\n        table: agent_memory\n        create: true\n",
    );
    let late = result(
        &go(&flow, json!({ "chat": "c2", "text": "late" }), &fx).await,
        "helper",
    );
    assert_eq!(late["memory"]["recalled"], 0, "{late}");
    // A test run reads nothing and keeps nothing: its memory's statements are only shown.
    let tested = result(
        &go(&flow, json!({ "chat": "c1", "text": "test" }), &DryRun::default()).await,
        "helper",
    );
    assert_eq!(tested["test"], "not sent", "{tested}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_in_redis_keeps_the_window_with_a_ttl() {
    let (port, state) = native::redis();
    let live = Live::new("127.0.0.1:1", true)
        .unwrap()
        .in_workspace("a")
        .with_secrets(credentials(port));
    let fx = Fx::new(live);
    rules(&fx, "        store: redis\n        credential: cache\n").await;
    let held = state.lock().unwrap();
    let (value, ttl) = &held.keys[&b"openduct:memory:w.a.chat/helper/c1".to_vec()];
    assert_eq!(*ttl, Some(3_600_000), "kept with a TTL of the time to forget");
    let kept: Json = serde_json::from_slice(value).unwrap();
    assert_eq!(
        kept["turns"].as_array().unwrap().len(),
        4,
        "the reset turn and the next: two turns"
    );
    assert!(
        held.keys
            .contains_key(b"openduct:memory:w.a.chat/helper/c2".as_slice())
    );
    assert!(
        held.commands.iter().any(|c| c[0] == "DEL"),
        "the reset forgot the conversation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_in_a_data_table_keeps_the_window_and_forgets_on_time() {
    let tables = Arc::new(MemoryTables::new());
    let column = |name: &str, kind| Column {
        name: name.to_owned(),
        kind,
    };
    tables
        .create(
            "agent_memory",
            vec![
                column("key", ColumnType::Text),
                column("turns", ColumnType::Json),
                column("forget_at", ColumnType::DateTime),
            ],
        )
        .unwrap();
    let live = Live::new("127.0.0.1:1", true)
        .unwrap()
        .in_workspace("a")
        .with_secrets(credentials(1))
        .with_tables(Arc::clone(&tables) as Arc<dyn Tables>);
    let fx = Fx::new(live);
    rules(&fx, "        store: table\n        table: agent_memory\n").await;
    let data = tables.read("agent_memory").await.unwrap();
    let rows: Vec<String> = data
        .rows
        .values()
        .map(|r| openduct_flow::as_text(&r.values["key"]))
        .collect();
    assert_eq!(
        rows,
        ["w.a.chat/helper/c2", "w.a.chat/helper/c1"],
        "one row a conversation"
    );
    let c1 = data
        .rows
        .values()
        .find(|r| openduct_flow::as_text(&r.values["key"]) == "w.a.chat/helper/c1")
        .unwrap();
    let Value::Time(at) = &c1.values["forget_at"] else {
        panic!("a time")
    };
    assert!((at.timestamp_millis() - now_ms() - 3_600_000).abs() < 60_000);

    // A table without the columns is said, in plain words.
    tables
        .create("notes", vec![column("text", ColumnType::Text)])
        .unwrap();
    let flow = chat("        store: table\n        table: notes\n");
    let record = go(&flow, json!({ "chat": "c1", "text": "hi" }), &fx).await;
    let e = vector_support::failed(&record);
    assert!(e.contains("key (text), turns (json) and forget_at (datetime): it lacks key (text), turns (json), forget_at (datetime)"), "{e}");
}

#[test]
fn a_memory_s_store_and_its_settings_are_checked_when_the_workflow_is_read() {
    let read = |store: &str| {
        openduct_flow::Workflow::parse(&chat(store))
            .map(|_| ())
            .unwrap_err()
    };
    assert!(read("        store: files\n").contains("memory.store: broker, postgres, redis or table"));
    assert!(read("        store: redis\n").contains("memory.credential: the Redis server's credential"));
    assert!(
        read("        store: postgres\n        credential: db\n        table: \"x; DROP\"\n")
            .contains("is not a table name")
    );
    assert!(read("        size: 3\n").contains("not a setting of memory"));
}
