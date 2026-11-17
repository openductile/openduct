// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! In `openduct start`: a workflow's runs go several at once (10 unless it says), one after another when it says 1,
//! and records with the same key always in order.

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
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use studio::{JSON, OPENDUCT, call, http, owner, start, stop, until, workspace_topic};

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

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    answer.body["runs"].as_array().cloned().unwrap_or_default()
}

/// When each run finished, in milliseconds, earliest first.
fn finished(runs: &[Value]) -> Vec<i64> {
    let mut at: Vec<i64> = runs.iter().map(|r| r["at"].as_i64().unwrap()).collect();
    at.sort_unstable();
    at
}

fn hook_of(studio: &str, signed: &[String], id: &str) -> String {
    let got = call(studio, signed, "GET", &format!("/api/workflows/{id}"), None);
    got.body["hook"].as_str().unwrap().to_owned()
}

#[test]
fn runs_go_several_at_once_one_by_one_when_asked_and_a_keys_records_in_order() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);

    // A setting out of its range is refused.
    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": { "name": "Too many", "runs_at_once": 0, "trigger": { "webhook": {} }, "steps": [{ "name": "w", "wait": "1s" }] } }).to_string()),
    );
    assert_eq!(wrong.status, 422, "{}", wrong.body);

    // Eight deliveries of a two-second run, ten at once: done in about one run's time, not eight.
    let together = make(
        &studio,
        &signed,
        &json!({ "name": "Together", "trigger": { "webhook": {} }, "steps": [{ "name": "w", "wait": "2s" }] }),
    );
    let hook = hook_of(&studio, &signed, &together);
    let begun = Instant::now();
    for n in 0..8 {
        let sent = http(&studio, "POST", &hook, &[JSON], Some(&format!(r#"{{"n": {n}}}"#)));
        assert_eq!(sent.status, 202, "{}", sent.body);
    }
    until(60, "eight runs kept", || {
        runs(&studio, &signed, &together).len() == 8
    });
    let took = begun.elapsed();
    assert!(took < Duration::from_secs(12), "eight runs at once took {took:?}");
    let at = finished(&runs(&studio, &signed, &together));
    assert!(at[7] - at[0] < 4_000, "they did not overlap: {at:?}");

    // One at a time when it says so: each finished at least a run's time after the one before.
    let single = make(
        &studio,
        &signed,
        &json!({ "name": "Single file", "runs_at_once": 1, "trigger": { "webhook": {} }, "steps": [{ "name": "w", "wait": "1s" }] }),
    );
    let hook = hook_of(&studio, &signed, &single);
    for n in 0..3 {
        assert_eq!(
            http(&studio, "POST", &hook, &[JSON], Some(&format!(r#"{{"n": {n}}}"#))).status,
            202
        );
    }
    until(60, "three runs kept", || {
        runs(&studio, &signed, &single).len() == 3
    });
    let at = finished(&runs(&studio, &signed, &single));
    assert!(
        at.windows(2).all(|w| w[1] - w[0] >= 900),
        "they overlapped: {at:?}"
    );

    // A topic's records with the same key run in order, one after another; another key's alongside.
    let topic = workspace_topic(&studio, &signed, "keyed");
    let made = Command::new(OPENDUCT)
        .args(["topic", "create", &topic, "--bootstrap", &running.broker])
        .output()
        .unwrap();
    assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));
    let keyed = make(
        &studio,
        &signed,
        &json!({ "name": "Keyed", "trigger": { "topic": { "name": "keyed", "start": "earliest" } }, "steps": [{ "name": "w", "wait": "1s" }] }),
    );
    for key in ["a", "b"] {
        let sent = Command::new(OPENDUCT)
            .args([
                "send",
                &topic,
                r#"{"n": 1}"#,
                r#"{"n": 2}"#,
                r#"{"n": 3}"#,
                "--key",
                key,
                "--bootstrap",
                &running.broker,
            ])
            .output()
            .unwrap();
        assert!(sent.status.success(), "{}", String::from_utf8_lossy(&sent.stderr));
    }
    until(60, "six keyed runs kept", || {
        runs(&studio, &signed, &keyed).len() == 6
    });
    let all = runs(&studio, &signed, &keyed);
    for key in ["a", "b"] {
        let mut mine: Vec<(i64, i64)> = all
            .iter()
            .filter(|r| r["trigger"]["key"] == key)
            .map(|r| {
                (
                    r["trigger"]["offset"].as_i64().unwrap(),
                    r["at"].as_i64().unwrap(),
                )
            })
            .collect();
        mine.sort_unstable();
        assert_eq!(mine.len(), 3, "{all:?}");
        assert!(
            mine.windows(2).all(|w| w[1].1 - w[0].1 >= 900),
            "key {key}'s records did not run one after another, in order: {mine:?}"
        );
    }
    let at = finished(&all);
    assert!(
        at[5] - at[0] < 5_000,
        "the two keys did not run alongside: {at:?}"
    );
    stop(running);
}
