// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The steps that shape a run and its data: switch, list tools, group, merge, date and time, a webhook's answer,
//! another workflow run as a step, and the trigger for another workflow's failed runs.

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

use openduct_expr::Value;
use openduct_flow::{DryRun, Event, Outcome, Trigger, Workflow, Workflows, run};
use serde_json::{Value as Json, json};

fn event(json: &str) -> Event {
    Event {
        value: Value::parse_json(json).unwrap(),
        details: Value::parse_json(r#"{"trigger": "webhook"}"#).unwrap(),
    }
}

fn go(workflow: &Workflow, event: &Event, effects: &DryRun) -> openduct_flow::RunRecord {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(workflow, event, effects))
}

fn result(record: &openduct_flow::RunRecord, step: &str) -> Json {
    let found = record.steps.iter().find(|s| s.name == step).unwrap_or_else(|| {
        panic!(
            "no step {step} in {:?}",
            record.steps.iter().map(|s| &s.name).collect::<Vec<_>>()
        )
    });
    serde_json::from_str(&found.result.to_json()).unwrap()
}

fn ran(record: &openduct_flow::RunRecord) -> Vec<&str> {
    record.steps.iter().map(|s| s.name.as_str()).collect()
}

const ROUTE: &str = r#"
name: Route by size
steps:
  - name: size
    switch:
      cases:
        - label: big
          when: value.total > 500
          steps:
            - name: big
              set: { label: "'big'" }
        - label: middling
          when: value.total > 100
          steps:
            - name: middling
              set: { label: "'middling'" }
      default:
        - name: small
          set: { label: "'small'" }
  - name: after
    set: { done: "true" }
"#;

#[test]
fn a_switch_runs_the_first_case_that_holds_and_the_default_when_none_does() {
    let w = Workflow::parse(ROUTE).unwrap();
    let test = DryRun::default();
    let big = go(&w, &event(r#"{"total": 900}"#), &test);
    assert_eq!(ran(&big), ["size", "big", "after"]);
    assert_eq!(result(&big, "size"), json!({ "case": "big" }));
    // 900 is over 100 too: only the first case that holds runs.
    let middling = go(&w, &event(r#"{"total": 200}"#), &test);
    assert_eq!(ran(&middling), ["size", "middling", "after"]);
    let small = go(&w, &event(r#"{"total": 5}"#), &test);
    assert_eq!(ran(&small), ["size", "small", "after"]);
    assert_eq!(result(&small, "size"), json!({ "case": "default" }));
}

const ORDERS: &str = r#"{"orders": [
  {"id": 1, "who": "ana", "total": 120, "at": "2026-11-02"},
  {"id": 2, "who": "bo", "total": 40, "at": "2026-11-01"},
  {"id": 3, "who": "ana", "total": 300, "at": "2026-11-03"},
  {"id": 3, "who": "ana", "total": 300, "at": "2026-11-03"},
  {"id": 4, "who": "cy", "total": 75.5, "at": "2026-11-04"}
]}"#;

#[test]
fn list_tools_keep_order_make_unique_and_cut_short() {
    let w = Workflow::parse(
        r"
name: Biggest orders
steps:
  - name: top
    list:
      items: value.orders
      filter: item.total > 50
      sort: item.total
      order: desc
      unique: item.id
      limit: 2
",
    )
    .unwrap();
    let record = go(&w, &event(ORDERS), &DryRun::default());
    let ids: Vec<i64> = result(&record, "top")
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_i64().unwrap())
        .collect();
    // Over 50: 1, 3, 3, 4; by total, greatest first: 3, 3, 1, 4; each once: 3, 1, 4; two of them.
    assert_eq!(ids, [3, 1]);
}

#[test]
fn a_group_counts_and_sums_averages_and_finds_the_least_and_greatest() {
    let w = Workflow::parse(
        r"
name: By customer
steps:
  - name: by_customer
    group:
      items: value.orders
      by: item.who
      sum: { spent: item.total }
      average: { usual: item.total }
      min: { first: item.at }
      max: { biggest: item.total }
",
    )
    .unwrap();
    let record = go(&w, &event(ORDERS), &DryRun::default());
    assert_eq!(
        result(&record, "by_customer"),
        json!([
            { "key": "ana", "count": 3, "spent": 720, "usual": 240, "first": "2026-11-02", "biggest": 300 },
            { "key": "bo", "count": 1, "spent": 40, "usual": 40, "first": "2026-11-01", "biggest": 40 },
            { "key": "cy", "count": 1, "spent": 75.5, "usual": 75.5, "first": "2026-11-04", "biggest": 75.5 },
        ])
    );
}

#[test]
fn a_merge_appends_lists_joins_records_on_a_key_and_makes_records_one() {
    let w = Workflow::parse(
        r#"
name: Merges
steps:
  - name: people
    set:
      list: "[{'id': 1, 'name': 'Ana'}, {'id': 2, 'name': 'Bo'}]"
  - name: emails
    set:
      list: "[{'id': 2, 'email': 'bo@example.com'}, {'id': 3, 'email': 'cy@example.com'}]"
  - name: all
    merge: { of: [steps.people.list, steps.emails.list] }
  - name: joined
    merge: { of: [steps.people.list, steps.emails.list], mode: key, key: id }
  - name: one
    merge: { of: ["{'a': 1, 'b': 1}", "{'b': 2}"], mode: fields }
"#,
    )
    .unwrap();
    let record = go(&w, &event("{}"), &DryRun::default());
    assert_eq!(result(&record, "all").as_array().unwrap().len(), 4);
    assert_eq!(
        result(&record, "joined"),
        json!([
            { "id": 1, "name": "Ana" },
            { "id": 2, "name": "Bo", "email": "bo@example.com" },
            { "id": 3, "email": "cy@example.com" },
        ])
    );
    assert_eq!(result(&record, "one"), json!({ "a": 1, "b": 2 }));
}

#[test]
fn a_date_is_read_moved_and_written_in_a_time_zone_and_a_form() {
    let w = Workflow::parse(
        r#"
name: Due tomorrow
steps:
  - name: due
    datetime:
      value: value.at
      add: 1d
      timezone: Asia/Kolkata
      format: "%d %b %Y, %H:%M"
  - name: plain
    datetime: { value: "'2026-11-16'", subtract: 2h }
"#,
    )
    .unwrap();
    let record = go(
        &w,
        &event(r#"{"at": "2026-11-16T09:30:00Z"}"#),
        &DryRun::default(),
    );
    let due = result(&record, "due");
    assert_eq!(due["iso"], "2026-11-17T15:00:00+05:30");
    assert_eq!(due["utc"], "2026-11-17T09:30:00Z");
    assert_eq!(due["weekday"], "Tuesday");
    assert_eq!(due["formatted"], "17 Nov 2026, 15:00");
    assert_eq!(due["date"], "2026-11-17");
    assert_eq!(result(&record, "plain")["utc"], "2026-11-15T22:00:00Z");

    let wrong = Workflow::parse(
        r#"
name: Wrong form
steps:
  - name: due
    datetime: { format: "%Q" }
"#,
    )
    .unwrap();
    let record = go(&wrong, &event("{}"), &DryRun::default());
    match &record.outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("not a time format"), "{error}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_respond_step_gives_the_status_headers_and_body_for_the_caller() {
    let w = Workflow::parse(
        r#"
name: Answer
trigger: webhook
steps:
  - name: answer
    respond:
      status: 201
      headers: { x-order: "{{ value.id }}" }
      body: { ok: true, id: "{{ value.id }}", lines: "{{ value.lines }}" }
"#,
    )
    .unwrap();
    let record = go(&w, &event(r#"{"id": 7, "lines": [1, 2]}"#), &DryRun::default());
    assert_eq!(
        result(&record, "answer"),
        json!({ "status": 201, "headers": { "x-order": "7" }, "body": { "ok": true, "id": 7, "lines": [1, 2] } })
    );
}

/// The other workflows of a workspace, as a test gives them.
#[derive(Debug)]
struct Others;

impl Workflows for Others {
    fn definition<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let found = match id {
                "double" => json!({
                    "name": "Double", "trigger": "manual",
                    "steps": [{ "name": "doubled", "set": { "total": "value.total * 2" } }],
                }),
                "again" => json!({
                    "name": "Again", "trigger": "manual",
                    "steps": [{ "name": "inner", "run_workflow": { "workflow": "again" } }],
                }),
                _ => return Err(format!("there is no workflow {id} in this workspace")),
            };
            Ok(Value::from_json(&found))
        })
    }
}

#[test]
fn another_workflow_runs_as_a_step_and_gives_back_its_result_but_not_without_end() {
    let test = DryRun {
        workflows: Some(Arc::new(Others)),
        ..DryRun::default()
    };
    let w = Workflow::parse(
        r#"
name: Uses another
steps:
  - name: double
    run_workflow: { workflow: double, input: "{'total': value.amount}" }
"#,
    )
    .unwrap();
    let record = go(&w, &event(r#"{"amount": 21}"#), &test);
    let got = result(&record, "double");
    assert_eq!(got["status"], "done");
    assert_eq!(got["result"], json!({ "total": 42 }));
    assert_eq!(got["steps"]["doubled"], json!({ "total": 42 }));

    // A workflow that runs itself stops, a few deep.
    let again =
        Workflow::parse("name: Again\nsteps:\n  - name: inner\n    run_workflow: { workflow: again }\n")
            .unwrap();
    match &go(&again, &event("{}"), &test).outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("deep"), "{error}"),
        other => panic!("{other:?}"),
    }
    // One the workspace has not.
    let missing =
        Workflow::parse("name: Missing\nsteps:\n  - name: x\n    run_workflow: { workflow: nope }\n")
            .unwrap();
    match &go(&missing, &event("{}"), &test).outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("no workflow nope"), "{error}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_failed_trigger_names_another_workflow() {
    let w = Workflow::parse("name: Tell the team\ntrigger: { failed: { workflow: orders } }\nsteps: []\n")
        .unwrap();
    assert!(matches!(&w.trigger, Trigger::Failed { workflow } if workflow == "orders"));
    let own =
        Workflow::parse("name: Orders\nid: orders\ntrigger: { failed: { workflow: orders } }\nsteps: []\n");
    assert!(own.unwrap_err().contains("not this one"));
    assert!(
        Workflow::parse("name: X\ntrigger: { failed: {} }\n")
            .unwrap_err()
            .contains("failed.workflow")
    );
}

#[test]
fn the_new_steps_say_what_is_wrong_with_them() {
    let wrong = |steps: &str| Workflow::parse(&format!("name: X\nsteps:\n{steps}")).unwrap_err();
    assert!(wrong("  - switch: { cases: [] }\n").contains("1 to 20 cases"));
    assert!(wrong("  - switch: { cases: [{ steps: [] }] }\n").contains("when"));
    assert!(wrong("  - merge: { of: [value.a], mode: key }\n").contains("joined on"));
    assert!(wrong("  - merge: { of: [value.a], mode: zip }\n").contains("append, fields or key"));
    assert!(wrong("  - list: { items: value.a, order: up }\n").contains("asc or desc"));
    assert!(wrong("  - list: { items: value.a, limit: 0 }\n").contains("at least 1"));
    assert!(
        wrong("  - group: { items: value.a, by: item.b, sum: { count: item.c } }\n")
            .contains("each group's own")
    );
    assert!(wrong("  - respond: { status: 99 }\n").contains("100 to 599"));
    assert!(wrong("  - datetime: { timezone: Mars/Olympus }\n").contains("not a time zone"));
    assert!(wrong("  - datetime: { add: soon }\n").contains("as 30m"));
    assert!(wrong("  - run_workflow: {}\n").contains("by its id"));
}
