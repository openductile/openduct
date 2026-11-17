// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! In `openduct start`: a webhook whose workflow answers its caller itself (running another workflow on the way),
//! and a workflow started by another's failed runs.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use serde_json::{Value, json};
use studio::{JSON, call, http, owner, start, stop, until};

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

fn hook_of(studio: &str, signed: &[String], id: &str) -> String {
    let got = call(studio, signed, "GET", &format!("/api/workflows/{id}"), None);
    got.body["hook"].as_str().unwrap().to_owned()
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    assert_eq!(answer.status, 200, "{}", answer.body);
    answer.body["runs"].as_array().cloned().unwrap_or_default()
}

#[test]
fn a_webhook_answers_with_its_respond_step_and_a_failed_run_starts_the_workflow_watching_for_it() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);

    // A workflow another runs as a step.
    let double = make(
        &studio,
        &signed,
        &json!({
            "name": "Double it", "trigger": "manual",
            "steps": [{ "name": "doubled", "set": { "total": "value.total * 2" } }],
        }),
    );
    let answer = make(
        &studio,
        &signed,
        &json!({
            "name": "Quote",
            "trigger": { "webhook": {} },
            "steps": [
                { "name": "double", "run_workflow": { "workflow": double, "input": "{'total': value.qty * value.price}" } },
                { "name": "answer", "respond": {
                    "status": 201,
                    "headers": { "x-quote": "{{ value.id }}", "set-cookie": "studio=taken" },
                    "body": { "id": "{{ value.id }}", "total": "{{ steps.double.result.total }}" },
                } },
            ],
        }),
    );
    let hook = hook_of(&studio, &signed, &answer);
    let got = http(
        &studio,
        "POST",
        &hook,
        &[JSON],
        Some(r#"{"id": "Q-1", "qty": 3, "price": 7}"#),
    );
    assert_eq!(got.status, 201, "{}", got.body);
    assert_eq!(got.body, json!({ "id": "Q-1", "total": 42 }));
    assert!(
        got.headers.to_ascii_lowercase().contains("x-quote: q-1"),
        "{}",
        got.headers
    );
    assert!(
        !got.headers.to_ascii_lowercase().contains("studio=taken"),
        "{}",
        got.headers
    );

    // One that fails on no parts, and one that runs for each of its failures.
    let fails = make(
        &studio,
        &signed,
        &json!({
            "name": "Share it", "trigger": { "webhook": {} },
            "steps": [{ "name": "share", "set": { "each": "100 / value.parts" } }],
        }),
    );
    let tell = make(
        &studio,
        &signed,
        &json!({
            "name": "Tell the team", "trigger": { "failed": { "workflow": fails } },
            "steps": [{ "name": "note", "set": { "why": "trigger.error", "from": "trigger.workflow", "parts": "value.parts" } }],
        }),
    );
    let hook = hook_of(&studio, &signed, &fails);
    // It reads failures from when it started: send failing records until one reaches it.
    until(60, "a run of the workflow watching for failures", || {
        assert_eq!(
            http(&studio, "POST", &hook, &[JSON], Some(r#"{"parts": 0}"#)).status,
            202
        );
        std::thread::sleep(std::time::Duration::from_millis(300));
        !runs(&studio, &signed, &tell).is_empty()
    });
    let run = &runs(&studio, &signed, &tell)[0];
    assert_eq!(run["status"], "done", "{run}");
    let note = &run["steps"][0]["result"];
    assert_eq!(note["from"], fails.as_str());
    assert_eq!(note["parts"], 0);
    assert!(
        note["why"].as_str().unwrap().contains("division by zero"),
        "{note}"
    );

    // Another workspace's workflow cannot watch this one's failures: it never runs.
    let other = call(
        &studio,
        &signed,
        "POST",
        "/api/workspaces",
        Some(r#"{"name": "Someone else"}"#),
    );
    assert_eq!(other.status, 201, "{}", other.body);
    let other_id = other.body["id"].as_str().unwrap().to_owned();
    let mut elsewhere = signed.clone();
    elsewhere.push(format!("x-openduct-workspace: {other_id}"));
    let spy = make(
        &studio,
        &elsewhere,
        &json!({
            "name": "Peek", "trigger": { "failed": { "workflow": fails } },
            "steps": [{ "name": "note", "set": { "why": "trigger.error" } }],
        }),
    );
    for _ in 0..3 {
        assert_eq!(
            http(&studio, "POST", &hook, &[JSON], Some(r#"{"parts": 0}"#)).status,
            202
        );
    }
    std::thread::sleep(std::time::Duration::from_secs(5));
    assert!(
        runs(&studio, &elsewhere, &spy).is_empty(),
        "another workspace saw this one's failures"
    );

    // The Runs page: every workflow's runs in one list, each with its workflow.
    let all = call(&studio, &signed, "GET", "/api/runs", None);
    assert_eq!(all.status, 200, "{}", all.body);
    let names: std::collections::BTreeSet<&str> = all.body["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["workflow"]["name"].as_str())
        .collect();
    for name in ["Quote", "Share it", "Tell the team"] {
        assert!(names.contains(name), "{name} not in {names:?}");
    }
    // Double it ran only as a step of Quote's: it has no run of its own.
    assert!(!names.contains("Double it"), "{names:?}");
    // The log, kept as OpenTelemetry records in the data directory, read back for the installation's admin.
    let file = std::fs::read_dir(data.path().join("logs"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let text = std::fs::read_to_string(&file).unwrap();
    let first: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    let resource = &first["resourceLogs"][0];
    assert_eq!(
        resource["resource"]["attributes"][0]["value"]["stringValue"],
        "openduct"
    );
    assert!(
        resource["scopeLogs"][0]["logRecords"][0]["severityNumber"]
            .as_u64()
            .is_some()
    );
    let logs = call(&studio, &signed, "GET", "/api/logs?q=workflow+running", None);
    assert_eq!(logs.status, 200, "{}", logs.body);
    assert!(
        !logs.body["lines"].as_array().unwrap().is_empty(),
        "{}",
        logs.body
    );

    // The dashboard: the day's runs and what needs someone - Share it's failed records wait in CatchErr.
    let board = call(&studio, &signed, "GET", "/api/dashboard?range=1d", None);
    assert_eq!(board.status, 200, "{}", board.body);
    assert!(
        board.body["runs"]["failed"].as_u64().unwrap() >= 1,
        "{}",
        board.body
    );
    assert!(board.body["waiting"].as_u64().unwrap() >= 1, "{}", board.body);
    assert_eq!(board.body["health"], "red");
    let alerts = board.body["alerts"].as_array().unwrap();
    assert!(
        alerts
            .iter()
            .any(|a| a["level"] == "critical" && a["action"]["path"] == format!("/workflows/{fails}")),
        "{alerts:?}"
    );
    assert_eq!(board.body["runs"]["buckets"].as_array().unwrap().len(), 24);
    // The overview: every workspace of the person's, those needing attention first.
    let overview = call(&studio, &signed, "GET", "/api/overview?range=7d", None);
    assert_eq!(overview.status, 200, "{}", overview.body);
    assert_eq!(overview.body["workspaces"][0]["health"], "red");
    // Where alerts go: the workspace's own messengers, each checked before it is kept.
    let workspace = overview.body["workspaces"][0]["id"].as_str().unwrap().to_owned();
    let alerts_path = format!("/api/workspaces/{workspace}/alerts");
    let refused = call(
        &studio,
        &signed,
        "PUT",
        &alerts_path,
        Some(r#"{"channels": [{"kind": "telegram", "credential": "bot", "on": true}]}"#),
    );
    assert_eq!(refused.status, 422, "{}", refused.body);
    let kept = call(
        &studio,
        &signed,
        "PUT",
        &alerts_path,
        Some(r#"{"channels": [{"kind": "google_chat", "credential": "space", "on": false}]}"#),
    );
    assert_eq!(kept.status, 200, "{}", kept.body);
    let read = call(&studio, &signed, "GET", &alerts_path, None);
    assert_eq!(read.body["channels"][0]["kind"], "google_chat");
    stop(running);
}
