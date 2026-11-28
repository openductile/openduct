// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Data tables in `openduct start`: a table made in the studio from CSV, written by a workflow's runs - ten at once,
//! all changing one row - read back, its state kept, imported into, exported, re-columned and deleted; a test run
//! shows what it would write and writes nothing.

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
use studio::{call, owner, start, until};

fn rows(studio: &str, signed: &[String], query: &str) -> Value {
    let answer = call(
        studio,
        signed,
        "GET",
        &format!("/api/tables/leads/rows?{query}"),
        None,
    );
    assert_eq!(answer.status, 200, "{}", answer.body);
    answer.body
}

fn score_of(page: &Value, email: &str) -> Value {
    page["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["email"] == email)
        .map_or(Value::Null, |r| r["score"].clone())
}

#[test]
fn a_table_made_from_csv_is_written_by_runs_at_once_read_back_and_kept() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let post = |path: &str, body: &Value| call(&studio, &signed, "POST", path, Some(&body.to_string()));

    let made = post(
        "/api/tables",
        &json!({ "name": "leads", "csv": "Email,Score\r\nada@example.com,12\nsam@example.com,3\n" }),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    assert_eq!(
        made.body["columns"],
        json!([{ "name": "email", "type": "text" }, { "name": "score", "type": "number" }])
    );
    assert_eq!(made.body["rows"], 2);
    let again = post("/api/tables", &json!({ "name": "leads", "columns": [] }));
    assert_eq!(again.status, 422, "{}", again.body);

    let definition = json!({
        "name": "Score leads",
        "trigger": "manual",
        "steps": [
            { "name": "keep", "table": { "name": "leads", "action": "upsert", "key": "email",
                "row": { "email": "{{ value.email }}", "score": "{{ value.score }}" } } },
            { "name": "bump", "table": { "name": "leads", "action": "update", "where": { "email": "sam@example.com" },
                "set": { "score": "{{ row.score + 1 }}" } } },
            { "name": "runs", "state": { "set": { "runs": "{{ has(state.runs) ? state.runs + 1 : 1 }}" } } },
            { "name": "seen", "javascript": { "code": "state.seen = (state.seen || 0) + 1; return state.seen;", "state": true } },
        ],
    });
    let created = post("/api/workflows", &json!({ "definition": definition }));
    assert_eq!(created.status, 201, "{}", created.body);
    let id = created.body["id"].as_str().unwrap().to_owned();

    // A test run reads the table and works the change out: nothing is written.
    let test = post(
        &format!("/api/workflows/{id}/test"),
        &json!({ "input": { "email": "new@example.com", "score": 1 } }),
    );
    assert_eq!(test.status, 200, "{}", test.body);
    let steps = &test.body["run"]["steps"];
    assert_eq!(test.body["run"]["status"], "done", "{}", test.body);
    assert_eq!(steps[0]["result"]["test"], "not written");
    assert_eq!(steps[0]["result"]["inserted"], 1);
    assert_eq!(steps[1]["result"]["rows"][0]["score"], 4, "sam's 3, one more");
    assert_eq!(rows(&studio, &signed, "")["total"], 2);

    let on = post(&format!("/api/workflows/{id}/active"), &json!({ "active": true }));
    assert_eq!(on.status, 200, "{}", on.body);
    // Ten runs at once (a workflow runs up to ten at a time on a node), every one adding 1 to sam's score.
    for n in 0..10 {
        let queued = post(
            &format!("/api/workflows/{id}/run"),
            &json!({ "input": { "email": format!("lead{n}@example.com"), "score": n } }),
        );
        assert_eq!(queued.status, 202, "{}", queued.body);
    }
    until(60, "ten runs done", || {
        let runs = call(
            &studio,
            &signed,
            "GET",
            &format!("/api/workflows/{id}/runs"),
            None,
        );
        let all = runs.body["runs"].as_array().cloned().unwrap_or_default();
        if let Some(failed) = all.iter().find(|r| r["status"] == "failed") {
            panic!("a run failed: {failed}");
        }
        all.iter().filter(|r| r["status"] == "done").count() == 10
    });
    let page = rows(&studio, &signed, "limit=50&sort=email");
    assert_eq!(page["total"], 12);
    assert_eq!(
        score_of(&page, "sam@example.com"),
        13,
        "ten runs at once each added one: none lost"
    );
    assert_eq!(score_of(&page, "lead7@example.com"), 7);
    assert_eq!(score_of(&page, "ada@example.com"), 12);
    let filtered = rows(&studio, &signed, "where=row.score+%3E%3D+12");
    assert_eq!(filtered["matched"], 2, "{filtered}");

    let states = call(&studio, &signed, "GET", "/api/workflow-state", None);
    assert_eq!(states.status, 200);
    let kept = &states.body["states"][0];
    assert_eq!(kept["workflow"], id.as_str());
    assert_eq!(kept["name"], "Score leads");
    assert_eq!(
        kept["values"],
        json!({ "runs": 10, "seen": 10 }),
        "the state step and the script, each run"
    );

    // CSV in and out.
    let imported = post(
        "/api/tables/leads/import",
        &json!({ "csv": "email,score\nlin@example.com,1.5\n" }),
    );
    assert_eq!(imported.status, 200, "{}", imported.body);
    assert_eq!(imported.body["added"], 1);
    let wrong = post(
        "/api/tables/leads/import",
        &json!({ "csv": "email,colour\nx@example.com,red\n" }),
    );
    assert_eq!(wrong.status, 422, "{}", wrong.body);
    let exported = call(&studio, &signed, "GET", "/api/tables/leads/export?as=json", None);
    let csv = exported.body["csv"].as_str().unwrap();
    assert!(
        csv.starts_with("id,email,score,created_at,updated_at\r\n1,ada@example.com,12,"),
        "{csv}"
    );
    assert!(csv.contains(",lin@example.com,1.5,"), "{csv}");

    // A row edited by hand; columns renamed and retyped; a type a value cannot take refused, saying which row.
    let edited = call(
        &studio,
        &signed,
        "PUT",
        "/api/tables/leads/rows/1",
        Some(r#"{"values": {"score": "20"}}"#),
    );
    assert_eq!(edited.status, 200, "{}", edited.body);
    assert_eq!(edited.body["score"], 20);
    let changed = call(
        &studio,
        &signed,
        "PUT",
        "/api/tables/leads",
        Some(
            r#"{"columns": [{"name": "mail", "type": "text", "from": "email"}, {"name": "score", "type": "text"}]}"#,
        ),
    );
    assert_eq!(changed.status, 200, "{}", changed.body);
    let page = rows(&studio, &signed, "sort=id");
    assert_eq!(page["rows"][0]["mail"], "ada@example.com");
    assert_eq!(page["rows"][0]["score"], "20");
    post(
        "/api/tables/leads/rows",
        &json!({ "rows": [{ "mail": "x@example.com", "score": "lots" }] }),
    );
    let refused = call(
        &studio,
        &signed,
        "PUT",
        "/api/tables/leads",
        Some(r#"{"columns": [{"name": "mail", "type": "text"}, {"name": "score", "type": "number"}]}"#),
    );
    assert_eq!(refused.status, 422, "{}", refused.body);
    assert!(
        refused.body["error"]
            .as_str()
            .unwrap()
            .contains("is not a number"),
        "{}",
        refused.body
    );

    let gone = call(&studio, &signed, "DELETE", "/api/tables/leads", Some("{}"));
    assert_eq!(gone.status, 200, "{}", gone.body);
    let listed = call(&studio, &signed, "GET", "/api/tables", None);
    assert_eq!(listed.body["tables"], json!([]));
    let cleared = call(
        &studio,
        &signed,
        "DELETE",
        &format!("/api/workflow-state/{id}"),
        Some("{}"),
    );
    assert_eq!(cleared.status, 200);
    let states = call(&studio, &signed, "GET", "/api/workflow-state", None);
    assert_eq!(states.body["states"], json!([]));
}
