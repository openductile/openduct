// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workflows in `openduct start`: made and turned on in the studio, started by a webhook, a schedule, a topic or a
//! person, each run in the history, failures set aside and run again once fixed - and nothing lost when the program
//! is killed in the middle of a run.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::process::Command;

use serde_json::{Value, json};
use studio::{JSON, OPENDUCT, call, http, owner, start, stop, until};

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    assert_eq!(answer.status, 200, "{}", answer.body);
    answer.body["runs"].as_array().cloned().unwrap_or_default()
}

fn statuses(runs: &[Value]) -> Vec<String> {
    let mut s: Vec<String> = runs
        .iter()
        .map(|r| r["status"].as_str().unwrap().to_owned())
        .collect();
    s.sort();
    s
}

fn make(studio: &str, signed: &[String], definition: &Value) -> String {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    let on = call(
        studio,
        signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);
    id
}

/// The workflow's webhook address: random, not its id.
fn hook_of(studio: &str, signed: &[String], id: &str) -> String {
    let got = call(studio, signed, "GET", &format!("/api/workflows/{id}"), None);
    got.body["hook"].as_str().unwrap().to_owned()
}

fn orders(check: &str) -> Value {
    json!({
        "name": "Big orders",
        "trigger": { "webhook": { "token": "t0k" } },
        "steps": [
            { "name": "order", "set": { "total": "value.qty * value.price", "id": "value.id" } },
            { "where": "steps.order.total > 100" },
            { "name": "keep", "topic": { "name": "big-orders", "key": "{{ steps.order.id }}", "value": { "id": "{{ steps.order.id }}", "total": "{{ steps.order.total }}" } } },
            { "name": "check", "set": { "share": check } },
        ],
    })
}

#[test]
fn a_webhook_runs_the_workflow_and_a_failure_is_set_aside_until_it_is_fixed_and_run_again() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let id = make(&studio, &signed, &orders("100 / value.parts"));
    assert_eq!(id.len(), 36, "a workflow's id is a UUID");
    let hook = hook_of(&studio, &signed, &id);

    let deliver = |body: &str, token: &str| {
        http(
            &studio,
            "POST",
            &format!("{hook}?token={token}"),
            &[JSON],
            Some(body),
        )
    };
    assert_eq!(
        deliver(r#"{"id": "A-1", "qty": 3, "price": 50, "parts": 4}"#, "t0k").status,
        202
    );
    assert_eq!(
        deliver(r#"{"id": "A-2", "qty": 3, "price": 50, "parts": 0}"#, "t0k").status,
        202
    );
    assert_eq!(
        deliver(r#"{"id": "A-3", "qty": 1, "price": 5, "parts": 1}"#, "t0k").status,
        202
    );
    assert_eq!(deliver("{}", "wrong").status, 401);
    let by_header = http(
        &studio,
        "POST",
        &hook_of(&studio, &signed, &id),
        &[JSON, "authorization: Bearer t0k"],
        Some(r#"{"id": "A-4", "qty": 1, "price": 1, "parts": 1}"#),
    );
    assert_eq!(by_header.status, 202);

    until(30, "four runs in the history", || {
        runs(&studio, &signed, &id).len() == 4
    });
    assert_eq!(
        statuses(&runs(&studio, &signed, &id)),
        ["done", "failed", "skipped", "skipped"]
    );
    let failed = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{id}/failed"),
        None,
    );
    let aside = failed.body["failed"].as_array().unwrap();
    assert_eq!(aside.len(), 1);
    assert_eq!(aside[0]["error"], "check: share: a division by zero");
    assert_eq!(aside[0]["event"]["id"], "A-2");

    // Fixed, and the set-aside event run again: it is done, and nothing is left aside.
    let fixed = call(
        &studio,
        &signed,
        "PUT",
        &format!("/api/workflows/{id}"),
        Some(&json!({ "definition": orders("value.parts == 0 ? 0 : 100 / value.parts") }).to_string()),
    );
    assert_eq!(fixed.status, 200, "{}", fixed.body);
    let again = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/failed/run"),
        Some("{}"),
    );
    assert_eq!((again.status, again.body["queued"].clone()), (202, json!(1)));
    until(30, "the set-aside event run again", || {
        runs(&studio, &signed, &id).len() == 5
    });
    assert_eq!(
        runs(&studio, &signed, &id)[0]["status"],
        "done",
        "the newest run first"
    );
    let left = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{id}/failed"),
        None,
    );
    assert_eq!(left.body["failed"], json!([]));

    // What the workflow wrote is in its topic - its workspace's own, w.<workspace>.big-orders - for anything that
    // reads Kafka.
    let big_orders = studio::workspace_topic(&studio, &signed, "big-orders");
    let read = Command::new(OPENDUCT)
        .args([
            "read",
            &big_orders,
            "--from",
            "earliest",
            "--max",
            "3",
            "--bootstrap",
            &running.broker,
        ])
        .output()
        .unwrap();
    let written = String::from_utf8_lossy(&read.stdout);
    assert!(written.contains(r#"{"id":"A-1","total":150}"#), "{written}");

    // Turned off: its webhook refuses deliveries.
    let off = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": false}"#),
    );
    assert_eq!(off.status, 200);
    assert_eq!(deliver("{}", "t0k").status, 503);
    stop(running);
}

#[test]
fn a_run_cut_off_by_a_crash_runs_after_the_restart() {
    let data = tempfile::tempdir().unwrap();
    let mut running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let slow = json!({
        "name": "Slow", "trigger": "webhook",
        "steps": [ { "wait": "2s" }, { "name": "done", "set": { "id": "value.id" } } ],
    });
    let id = make(&studio, &signed, &slow);
    assert_eq!(
        http(
            &studio,
            "POST",
            &hook_of(&studio, &signed, &id),
            &[JSON],
            Some(r#"{"id": 7}"#)
        )
        .status,
        202
    );
    std::thread::sleep(std::time::Duration::from_millis(800));
    // Killed in the middle of the run: no chance to finish or to say where it got to.
    running.child.kill().unwrap();
    running.child.wait().unwrap();
    drop(running);

    let running = start(data.path());
    let studio = running.studio.clone();
    let signed_in = http(
        &studio,
        "POST",
        "/api/sign-in",
        &[JSON],
        Some(r#"{"email": "ada@example.com", "password": "correct horse battery"}"#),
    );
    let signed = vec![
        format!("cookie: {}", signed_in.cookie()),
        format!("x-openduct-csrf: {}", signed_in.body["csrf"].as_str().unwrap()),
        JSON.to_owned(),
    ];
    until(40, "the run after the restart", || {
        runs(&studio, &signed, &id).iter().any(|r| r["status"] == "done")
    });
    let run = &runs(&studio, &signed, &id)[0];
    assert_eq!(run["steps"][1]["result"]["id"], 7);
    stop(running);
}

#[test]
fn a_schedule_a_topic_and_a_person_start_runs_and_a_test_run_sends_nothing() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);

    let ticking = make(
        &studio,
        &signed,
        &json!({ "name": "Tick", "trigger": { "schedule": { "every": "1s" } }, "steps": [ { "set": { "at": "value.scheduled_at" } } ] }),
    );
    until(30, "two scheduled runs", || {
        runs(&studio, &signed, &ticking).len() >= 2
    });
    assert_eq!(
        runs(&studio, &signed, &ticking)[0]["trigger"]["trigger"],
        "schedule"
    );
    call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{ticking}/active"),
        Some(r#"{"active": false}"#),
    );

    // A topic of the workspace's: w.<workspace>.readings on the broker, `readings` in the workflow.
    let readings = studio::workspace_topic(&studio, &signed, "readings");
    let made = Command::new(OPENDUCT)
        .args(["topic", "create", &readings, "--bootstrap", &running.broker])
        .output()
        .unwrap();
    assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));
    let reading = make(
        &studio,
        &signed,
        &json!({ "name": "Readings", "trigger": { "topic": { "name": "readings", "start": "earliest" } }, "steps": [ { "name": "hot", "set": { "hot": "value.c > 30" } } ] }),
    );
    let sent = Command::new(OPENDUCT)
        .args([
            "send",
            &readings,
            r#"{"c": 31}"#,
            r#"{"c": 12}"#,
            "--bootstrap",
            &running.broker,
        ])
        .output()
        .unwrap();
    assert!(sent.status.success(), "{}", String::from_utf8_lossy(&sent.stderr));
    until(30, "a run per message", || {
        runs(&studio, &signed, &reading).len() == 2
    });
    let hot: Vec<Value> = runs(&studio, &signed, &reading)
        .iter()
        .map(|r| r["steps"][0]["result"]["hot"].clone())
        .collect();
    assert!(hot.contains(&json!(true)) && hot.contains(&json!(false)));

    let by_hand = make(
        &studio,
        &signed,
        &json!({ "name": "By hand", "steps": [ { "name": "hi", "set": { "who": "value.name" } } ] }),
    );
    let queued = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{by_hand}/run"),
        Some(r#"{"input": {"name": "Grace"}}"#),
    );
    assert_eq!(queued.status, 202, "{}", queued.body);
    until(30, "the run started by hand", || {
        runs(&studio, &signed, &by_hand).len() == 1
    });
    let run = &runs(&studio, &signed, &by_hand)[0];
    assert_eq!(
        (run["trigger"]["trigger"].clone(), run["trigger"]["by"].clone()),
        (json!("manual"), json!("ada@example.com"))
    );

    let unsaved = json!({ "name": "By hand", "steps": [ { "name": "call", "http": { "method": "POST", "url": "https://example.com/{{ value.name }}" } } ] });
    let tried = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{by_hand}/test"),
        Some(&json!({ "definition": unsaved, "input": { "name": "Ada" } }).to_string()),
    );
    assert_eq!(tried.body["run"]["steps"][0]["result"]["test"], "not sent");

    // Scripts only compute, so a test runs them: each in a process of its own, started by the program.
    let scripted = json!({ "name": "Scripts", "steps": [
        { "name": "shape", "javascript": "console.log('shaping', value.name); return { upper: value.name.toUpperCase() };" },
        { "name": "count", "python": "import os\nprint(len(os.environ))\nresult = {'letters': len(steps['shape']['upper'])}" },
    ] });
    let ran = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{by_hand}/test"),
        Some(&json!({ "definition": scripted, "input": { "name": "Ada" } }).to_string()),
    );
    assert_eq!(ran.body["run"]["status"], "done", "{}", ran.body);
    assert_eq!(ran.body["run"]["steps"][0]["result"], json!({ "upper": "ADA" }));
    assert_eq!(ran.body["run"]["steps"][0]["logs"], json!(["shaping Ada"]));
    assert_eq!(ran.body["run"]["steps"][1]["result"], json!({ "letters": 3 }));
    assert_eq!(
        ran.body["run"]["steps"][1]["logs"],
        json!(["0"]),
        "a script sees none of the program's environment"
    );
    assert_eq!(
        tried.body["run"]["steps"][0]["result"]["request"]["url"],
        "https://example.com/Ada"
    );
    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/check",
        Some(r#"{"definition": {"name": "x", "steps": [{"set": {"a": "nosuch()"}}]}}"#),
    );
    assert_eq!(wrong.status, 422);
    assert!(
        wrong.body["error"].as_str().unwrap().contains("no function"),
        "{}",
        wrong.body
    );
    assert_eq!(
        http(&studio, "GET", "/api/workflows", &[], None).status,
        401,
        "signed in only"
    );
    stop(running);
}

// cron-schedules
#[test]
fn set_times_start_runs_and_the_studio_says_when_a_schedule_runs_next() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);

    // Every second, by a cron expression on a clock of its own: each run at its whole second, once.
    let ticking = make(
        &studio,
        &signed,
        &json!({ "name": "Seconds", "trigger": { "schedule": { "cron": "* * * * * *", "timezone": "Asia/Tokyo" } }, "steps": [ { "set": { "at": "value.scheduled_at" } } ] }),
    );
    until(30, "three runs at set times", || {
        runs(&studio, &signed, &ticking).len() >= 3
    });
    call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{ticking}/active"),
        Some(r#"{"active": false}"#),
    );
    let mut slots: Vec<i64> = runs(&studio, &signed, &ticking)
        .iter()
        .map(|r| r["trigger"]["slot"].as_i64().unwrap())
        .collect();
    assert!(slots.iter().all(|s| s % 1000 == 0), "{slots:?}");
    slots.sort_unstable();
    let count = slots.len();
    slots.dedup();
    assert_eq!(slots.len(), count, "a time ran twice");

    // The trigger's window asks when a schedule runs next, and is told what is wrong in plain words.
    let next = call(
        &studio,
        &signed,
        "POST",
        "/api/schedule",
        Some(r#"{"schedule": {"every": "weekday", "at": "09:00", "timezone": "Europe/Berlin"}, "count": 3}"#),
    );
    assert_eq!(next.status, 200, "{}", next.body);
    assert_eq!(next.body["says"], "every weekday at 09:00 (Europe/Berlin)");
    assert_eq!(next.body["local"].as_array().unwrap().len(), 3);
    assert!(
        next.body["local"][0].as_str().unwrap().contains("T09:00:00+0"),
        "{}",
        next.body
    );
    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/schedule",
        Some(r#"{"schedule": {"cron": "0 9 * *"}}"#),
    );
    assert_eq!(wrong.status, 422);
    assert!(
        wrong.body["error"].as_str().unwrap().contains("has 4 fields"),
        "{}",
        wrong.body
    );
    assert_eq!(
        http(&studio, "POST", "/api/schedule", &[JSON], Some("{}")).status,
        401,
        "signed in only"
    );
    stop(running);
}
