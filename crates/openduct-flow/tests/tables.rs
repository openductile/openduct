// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `table` and `state` steps, and scripts given the state, run end to end on tables kept in memory: every
//! action, both kinds of condition, the run records, what a test run and a preview show instead of writing, and two
//! runs changing one row - or one state - at once.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use openduct_expr::Value;
use openduct_flow::tables::{Access, Column, ColumnType, MemoryTables, Tables};
use openduct_flow::{Credential, DryRun, Effects, Event, HttpRequest, ScriptRequest, Workflow, run};
use serde_json::{Value as Json, json};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A run's effects: tables in memory, and a stand-in for the script runner that does what the test's scripts say.
#[derive(Debug, Default)]
struct Kept {
    tables: Arc<MemoryTables>,
    /// How many times the stand-in script ran.
    scripts: AtomicU32,
    /// The first run of the script changes this value of the state behind its back, as another run would.
    meddle: Option<&'static str>,
}

impl Effects for Kept {
    fn http<'a>(&'a self, _: HttpRequest) -> Pending<'a> {
        Box::pin(async { Err("no HTTP here".to_owned()) })
    }
    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no topics here".to_owned()) })
    }
    fn credential(&self, name: &str) -> Result<Credential, String> {
        Err(format!("no credential {name}"))
    }
    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path}")) })
    }
    fn tables(&self) -> Access<'_> {
        Access::Write(self.tables.as_ref())
    }
    /// The script `count`: adds 1 to `state.count`, and answers the count it left.
    fn script<'a>(&'a self, request: ScriptRequest) -> Pending<'a> {
        Box::pin(async move {
            let n = self.scripts.fetch_add(1, Ordering::SeqCst);
            if let (Some(key), 0) = (self.meddle, n) {
                let plan: openduct_flow::tables::StatePlan<'_> = Box::new(|current| {
                    let mut next = current.clone();
                    next.insert(key.to_owned(), Value::from_json(&json!(100)));
                    Ok((Some(next), Value::Null))
                });
                self.tables.change_state("counts", plan).await?;
            }
            let mut state = request.names.get("state").cloned().unwrap_or(json!({}));
            let count = state["count"].as_i64().unwrap_or(0) + 1;
            state["count"] = json!(count);
            Ok(Value::from_json(
                &json!({ "result": { "count": count }, "logs": [format!("count {count}")], "state": state }),
            ))
        })
    }
}

fn leads() -> Arc<MemoryTables> {
    let tables = MemoryTables::new();
    let col = |name: &str, kind| Column {
        name: name.to_owned(),
        kind,
    };
    tables
        .create(
            "leads",
            vec![
                col("email", ColumnType::Text),
                col("score", ColumnType::Number),
                col("hot", ColumnType::Boolean),
                col("seen", ColumnType::DateTime),
                col("extra", ColumnType::Json),
            ],
        )
        .unwrap();
    Arc::new(tables)
}

fn event(value: Json) -> Event {
    Event {
        value: Value::from_json(&value),
        details: Value::from_json(&json!({ "trigger": "test" })),
    }
}

async fn ran(effects: &dyn Effects, yaml: &str, value: Json) -> Json {
    let workflow = Workflow::parse(yaml).unwrap();
    let record = run(&workflow, &event(value), effects).await;
    record.to_json()
}

fn done(record: &Json) -> &Json {
    assert_eq!(record["status"], "done", "{record:#}");
    record
}

fn step<'a>(record: &'a Json, name: &str) -> &'a Json {
    record["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == name)
        .map(|s| &s["result"])
        .unwrap_or_else(|| panic!("no step {name} in {record:#}"))
}

const FLOW: &str = r#"
name: Leads
steps:
  - name: add
    table:
      name: leads
      action: insert
      rows: "value.leads"
  - name: one
    table: { name: leads, action: insert, row: { email: "{{ value.extra_email }}", score: "7", hot: "yes", seen: "2026-11-27", extra: { tags: ["{{ value.tag }}"] } } }
  - name: big
    table: { name: leads, action: get, where: "row.score >= 10", sort: "-score" }
  - name: by_mail
    table: { name: leads, action: get, where: { email: "{{ value.extra_email }}" } }
  - name: some
    table:
      name: leads
      action: count
      match: any
      where: [{ column: email, op: ends_with, value: "@EXAMPLE.com" }, { column: hot, op: "=", value: "true" }]
  - name: bump
    table: { name: leads, action: update, where: "row.score < 10", set: { score: "{{ row.score + 1 }}" } }
  - name: keep
    table: { name: leads, action: upsert, key: email, row: { email: "ada@example.com", score: 99 } }
  - name: fresh
    table: { name: leads, action: upsert, key: email, row: { email: "new@example.com", score: 1 } }
  - name: drop_low
    table: { name: leads, action: delete, where: [{ column: score, op: "<=", value: "{{ value.floor }}" }] }
  - name: left
    table: { name: leads, action: get, sort: [email], limit: 2, offset: 1 }
"#;

fn sample() -> Json {
    json!({
        "leads": [
            { "email": "ada@example.com", "score": 12 },
            { "email": "sam@example.org", "score": "30", "hot": false },
            { "email": "lin@example.com" },
        ],
        "extra_email": "kim@example.net",
        "tag": "warm",
        "floor": 1,
    })
}

#[tokio::test]
async fn every_table_action_runs_and_records_what_it_did() {
    let effects = Kept {
        tables: leads(),
        ..Kept::default()
    };
    let record = ran(&effects, FLOW, sample()).await;
    let record = done(&record);
    assert_eq!(step(record, "add")["count"], 3);
    assert_eq!(
        step(record, "add")["rows"][1]["score"],
        30,
        "text read as the column's number"
    );
    assert_eq!(step(record, "add")["rows"][0]["id"], 1);
    let one = &step(record, "one")["rows"][0];
    assert_eq!((one["id"].clone(), one["hot"].clone()), (json!(4), json!(true)));
    assert_eq!(one["seen"], "2026-11-27T00:00:00Z");
    assert_eq!(one["extra"], json!({ "tags": ["warm"] }));
    let big: Vec<&Json> = step(record, "big")
        .as_array()
        .unwrap()
        .iter()
        .map(|r| &r["email"])
        .collect();
    assert_eq!(
        big,
        ["sam@example.org", "ada@example.com"],
        "score 10 and up, highest first"
    );
    assert_eq!(step(record, "by_mail").as_array().unwrap().len(), 1);
    assert_eq!(
        step(record, "some")["count"],
        3,
        "two at example.com (any case) and the hot one"
    );
    // lin (no score: null < 10 is null, not true) is left alone; kim's 7 becomes 8.
    assert_eq!(step(record, "bump")["count"], 1);
    assert_eq!(step(record, "bump")["rows"][0]["score"], 8);
    assert_eq!(
        (
            step(record, "keep")["updated"].clone(),
            step(record, "keep")["inserted"].clone()
        ),
        (json!(1), json!(0))
    );
    assert_eq!(step(record, "keep")["rows"][0]["score"], 99);
    assert_eq!(step(record, "fresh")["inserted"], 1);
    assert_eq!(step(record, "drop_low")["count"], 1, "new@ with score 1");
    let left: Vec<&Json> = step(record, "left")
        .as_array()
        .unwrap()
        .iter()
        .map(|r| &r["email"])
        .collect();
    assert_eq!(
        left,
        ["kim@example.net", "lin@example.com"],
        "by email, from the second, two"
    );
    let steps = record["steps"].as_array().unwrap();
    assert!(steps.iter().all(|s| s["kind"] == "table" && s["error"].is_null()));
    let all = effects.tables.read("leads").await.unwrap();
    assert_eq!(all.rows.len(), 4);
    assert_eq!(all.next_id, 6, "ids are never given twice");
}

#[tokio::test]
async fn a_table_step_s_mistakes_fail_its_run_in_plain_words() {
    let effects = Kept {
        tables: leads(),
        ..Kept::default()
    };
    let cases = [
        ("table: { name: nope, action: get }", "there is no table nope"),
        (
            "table: { name: leads, action: insert, row: { colour: red } }",
            "leads has no column \"colour\"",
        ),
        (
            "table: { name: leads, action: insert, row: { score: lots } }",
            "score: \"lots\" is not a number",
        ),
        (
            "table: { name: leads, action: get, where: [{ column: score, op: \">\", value: high }] }",
            "is not a number",
        ),
        (
            "table: { name: leads, action: get, sort: colour }",
            "sort: leads has no column",
        ),
        (
            "table: { name: leads, action: insert, rows: \"value\" }",
            "not a list of records",
        ),
    ];
    for (yaml, says) in cases {
        let record = ran(&effects, &format!("name: t\nsteps:\n  - {yaml}\n"), json!({})).await;
        assert_eq!(record["status"], "failed", "{yaml}");
        assert!(
            record["error"].as_str().unwrap().contains(says),
            "{yaml}: {}",
            record["error"]
        );
    }
}

#[tokio::test]
async fn a_test_run_works_a_change_out_and_writes_nothing() {
    let tables = leads();
    let yaml = "name: t\nsteps:\n  - name: add\n    table: { name: leads, action: insert, row: { email: a@b.c } }\n  \
                - name: n\n    table: { name: leads, action: count }\n  - name: s\n    state: { set: { last: \"{{ value.id }}\" } }\n";
    let test = DryRun {
        tables: Some(tables.clone() as Arc<dyn Tables>),
        ..DryRun::default()
    };
    let record = ran(&test, yaml, json!({ "id": 5 })).await;
    let record = done(&record);
    assert_eq!(step(record, "add")["test"], "not written");
    assert_eq!(step(record, "add")["rows"][0]["email"], "a@b.c");
    assert_eq!(step(record, "n")["count"], 0, "nothing was written");
    assert_eq!(step(record, "s")["last"], 5, "the state it would keep");
    assert!(tables.state("t").await.unwrap().is_empty());
    // Without tables at all (a workflow tried out on a machine), each table is an empty stand-in.
    let bare = ran(&DryRun::default(), yaml, json!({ "id": 5 })).await;
    assert_eq!(done(&bare)["steps"][0]["result"]["test"], "not written");
    // A preview reads for real and writes nothing either.
    let live: Arc<dyn Effects> = Arc::new(Kept {
        tables: tables.clone(),
        ..Kept::default()
    });
    let preview = openduct_flow::Preview::new(live);
    let record = ran(&preview, yaml, json!({ "id": 5 })).await;
    assert_eq!(step(done(&record), "add")["test"], "not written");
    assert!(tables.read("leads").await.unwrap().rows.is_empty());
}

#[tokio::test]
async fn a_workflow_keeps_its_state_between_runs() {
    let effects = Kept {
        tables: leads(),
        ..Kept::default()
    };
    let yaml = r#"
name: Seen
steps:
  - name: before
    state: { get: last_id }
  - where: "steps.before != value.id"
  - name: mark
    state: { set: { last_id: "{{ value.id }}", runs: "{{ has(state.runs) ? state.runs + 1 : 1 }}" } }
  - name: all
    state: {}
"#;
    let first = ran(&effects, yaml, json!({ "id": "A" })).await;
    assert_eq!(step(done(&first), "before"), &Json::Null);
    assert_eq!(step(&first, "all"), &json!({ "last_id": "A", "runs": 1 }));
    let again = ran(&effects, yaml, json!({ "id": "A" })).await;
    assert_eq!(again["status"], "skipped", "the same id twice: nothing new");
    let next = ran(&effects, yaml, json!({ "id": "B" })).await;
    assert_eq!(step(done(&next), "all"), &json!({ "last_id": "B", "runs": 2 }));
    let forget = ran(
        &effects,
        "name: Seen\nsteps:\n  - state: { delete: runs }\n",
        json!({}),
    )
    .await;
    assert_eq!(done(&forget)["steps"][0]["result"], json!({ "last_id": "B" }));
    let other = ran(&effects, "name: Other\nsteps:\n  - state: {}\n", json!({})).await;
    assert_eq!(
        done(&other)["steps"][0]["result"],
        json!({}),
        "each workflow keeps its own"
    );
    let clear = ran(
        &effects,
        "name: Seen\nsteps:\n  - state: { clear: true }\n",
        json!({}),
    )
    .await;
    assert_eq!(done(&clear)["steps"][0]["result"], json!({}));
}

const COUNT: &str =
    "name: Counts\nsteps:\n  - name: count\n    javascript: { code: \"count\", state: true }\n";

#[tokio::test]
async fn a_script_given_the_state_keeps_what_it_leaves_there() {
    let effects = Kept {
        tables: leads(),
        ..Kept::default()
    };
    for n in 1..=3 {
        let record = ran(&effects, COUNT, json!({})).await;
        assert_eq!(step(done(&record), "count"), &json!({ "count": n }));
        assert_eq!(record["steps"][0]["logs"], json!([format!("count {n}")]));
    }
    assert_eq!(
        effects.tables.state("counts").await.unwrap()["count"].to_json(),
        "3"
    );
    // A test run shows what it would keep, in the step's logs, and keeps nothing.
    let test = DryRun {
        tables: Some(effects.tables.clone() as Arc<dyn Tables>),
        scripts: Some(Arc::new(Counter)),
        ..DryRun::default()
    };
    let record = ran(&test, COUNT, json!({})).await;
    assert_eq!(step(done(&record), "count"), &json!({ "count": 4 }));
    let logs = record["steps"][0]["logs"][0].as_str().unwrap().to_owned();
    assert_eq!(logs, r#"state not kept (a test run): {"count":4}"#);
    assert_eq!(
        effects.tables.state("counts").await.unwrap()["count"].to_json(),
        "3"
    );
}

/// The stand-in script runner of a test run: the `count` script.
#[derive(Debug)]
struct Counter;

impl openduct_flow::Scripts for Counter {
    fn run<'a>(&'a self, request: ScriptRequest) -> Pending<'a> {
        Box::pin(async move {
            let mut state = request.names.get("state").cloned().unwrap_or(json!({}));
            let count = state["count"].as_i64().unwrap_or(0) + 1;
            state["count"] = json!(count);
            Ok(Value::from_json(
                &json!({ "result": { "count": count }, "logs": [], "state": state }),
            ))
        })
    }
}

#[tokio::test]
async fn a_script_runs_again_when_another_run_changed_the_state_meanwhile() {
    let effects = Kept {
        tables: leads(),
        meddle: Some("count"),
        ..Kept::default()
    };
    let record = ran(&effects, COUNT, json!({})).await;
    assert_eq!(
        step(done(&record), "count"),
        &json!({ "count": 101 }),
        "run again on the other run's 100"
    );
    assert_eq!(effects.scripts.load(Ordering::SeqCst), 2);
    assert_eq!(
        effects.tables.state("counts").await.unwrap()["count"].to_json(),
        "101"
    );
}

#[tokio::test]
async fn a_script_s_change_is_kept_beside_another_run_s_change_of_another_value() {
    let effects = Kept {
        tables: leads(),
        meddle: Some("other"),
        ..Kept::default()
    };
    let record = ran(&effects, COUNT, json!({})).await;
    assert_eq!(step(done(&record), "count"), &json!({ "count": 1 }));
    assert_eq!(
        effects.scripts.load(Ordering::SeqCst),
        1,
        "no need to run it again"
    );
    let state = effects.tables.state("counts").await.unwrap();
    assert_eq!(
        Value::record(state.as_ref().clone()).to_json(),
        r#"{"other":100,"count":1}"#
    );
}

#[tokio::test]
async fn runs_at_once_updating_one_row_each_count() {
    let effects = Arc::new(Kept {
        tables: leads(),
        ..Kept::default()
    });
    let seed =
        "name: Seed\nsteps:\n  - table: { name: leads, action: insert, row: { email: hits, score: 0 } }\n";
    done(&ran(effects.as_ref(), seed, json!({})).await);
    let bump = "name: Bump\nsteps:\n  - table: { name: leads, action: update, where: { email: hits }, set: { score: \"{{ row.score + 1 }}\" } }\n";
    let mut runs = Vec::new();
    for _ in 0..50 {
        let effects = Arc::clone(&effects);
        runs.push(tokio::spawn(async move {
            ran(effects.as_ref(), bump, json!({})).await
        }));
    }
    for r in runs {
        assert_eq!(r.await.unwrap()["status"], "done");
    }
    let rows = effects.tables.read("leads").await.unwrap();
    assert_eq!(rows.rows.values().next().unwrap().values["score"].to_json(), "50");
}
