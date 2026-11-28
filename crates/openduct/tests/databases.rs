// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SQL databases in `openduct start`: a MySQL credential sealed by the studio, a `mysql` step run against a stand-in
//! that speaks MySQL's protocol (crates/openduct-flow/tests/stand_in), read-only SQL that writes refused when the
//! workflow is checked, and a workflow a table's new rows start scheduled as a database trigger.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

#[path = "../../openduct-flow/tests/stand_in/mysql.rs"]
#[allow(dead_code, unreachable_pub)]
mod mysql;

use std::sync::Arc;

use serde_json::{Value, json};
use studio::{call, owner, start_with, until};

fn orders() -> Arc<mysql::Answer> {
    use mysql::{Cell, Reply, col};
    Arc::new(|sql: &str, values: &[mysql::Val], _: &mysql::Session| {
        if sql == "SELECT id, customer, total FROM orders WHERE id = ?" {
            let mysql::Val::Int(id) = values[0] else {
                panic!("{values:?}")
            };
            return Reply::Rows(
                vec![
                    col("id", mysql::LONGLONG),
                    col("customer", mysql::VAR_STRING),
                    col("total", mysql::NEWDECIMAL),
                ],
                vec![vec![
                    Cell::Int(id),
                    Cell::Text("Zoë".into()),
                    Cell::Text("54.90".into()),
                ]],
            );
        }
        Reply::Error(1064, "42000", format!("not expected: {sql}"))
    })
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    answer.body["runs"].as_array().cloned().unwrap_or_default()
}

fn made(studio: &str, signed: &[String], definition: &Value, active: bool) -> String {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    if active {
        let on = call(
            studio,
            signed,
            "POST",
            &format!("/api/workflows/{id}/active"),
            Some(r#"{"active": true}"#),
        );
        assert_eq!(on.status, 200, "{}", on.body);
    }
    id
}

#[test]
fn a_mysql_step_runs_through_the_studio_and_a_table_s_rows_are_a_trigger() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(mysql::MySql::start(orders()));
    let data = tempfile::tempdir().unwrap();
    // The stand-in is on this machine: workflows may reach it only when OpenDuct is told they may.
    let running = start_with(
        data.path(),
        &[
            "--listen",
            "127.0.0.1:0",
            "--studio-listen",
            "127.0.0.1:0",
            "--allow-private-network",
        ],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);

    // A credential without its host is refused; with it, it is sealed and its password never shown.
    let missing = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "shop-db", "kind": "mysql", "fields": { "username": "shop" } }).to_string()),
    );
    assert_ne!(missing.status, 201);
    assert!(missing.body.to_string().contains("host"), "{}", missing.body);
    let sealed = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(
            &json!({ "name": "shop-db", "kind": "mysql", "fields": {
                "host": "127.0.0.1", "port": server.port(), "database": "shop", "username": "shop",
                "password": mysql::PASSWORD, "tls": "disable",
            } })
            .to_string(),
        ),
    );
    assert_eq!(sealed.status, 201, "{}", sealed.body);
    let listed = call(&studio, &signed, "GET", "/api/credentials", None);
    assert!(listed.body.to_string().contains("shop-db"));
    assert!(!listed.body.to_string().contains(mysql::PASSWORD));

    // SQL that writes, in a step that only reads, is refused when the workflow is checked.
    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/check",
        Some(
            &json!({ "definition": { "name": "x", "steps": [
                { "name": "q", "mysql": { "credential": "shop-db", "sql": "DELETE FROM orders", "read_only": true } },
            ] } })
            .to_string(),
        ),
    );
    assert_eq!(wrong.status, 422, "{}", wrong.body);
    assert!(wrong.body.to_string().contains("read_only"), "{}", wrong.body);

    // A step run for real, with the sealed credential.
    let id = made(
        &studio,
        &signed,
        &json!({ "name": "Order", "trigger": { "manual": {} }, "steps": [
            { "name": "order", "mysql": { "credential": "shop-db", "sql": "SELECT id, customer, total FROM orders WHERE id = ?", "params": ["value.id"], "read_only": true } },
        ] }),
        true,
    );
    let ran = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/run"),
        Some(r#"{"input": {"id": 42}}"#),
    );
    assert_eq!(ran.status, 202, "{}", ran.body);
    until(30, "the run", || !runs(&studio, &signed, &id).is_empty());
    let run = runs(&studio, &signed, &id)[0].clone();
    assert_eq!(run["status"], "done", "{run}");
    assert_eq!(
        run["steps"][0]["result"]["rows"][0],
        json!({ "id": 42, "customer": "Zoë", "total": 54.9 })
    );
    assert!(
        server
            .log()
            .contains(&"query: START TRANSACTION READ ONLY".to_owned())
    );

    // A workflow a table's new rows start is scheduled as a database trigger.
    let watched = made(
        &studio,
        &signed,
        &json!({ "name": "New orders", "trigger": { "mysql": { "credential": "shop-db", "table": "orders", "cursor": "id", "every": "30s" } },
                 "steps": [{ "name": "s", "set": { "id": "value.id" } }] }),
        true,
    );
    until(30, "the database trigger to be kept", || {
        call(&studio, &signed, "GET", "/api/triggers", None).body["triggers"]
            .as_array()
            .is_some_and(|t| {
                t.iter().any(|t| {
                    t["workflow"] == watched.as_str() && t["kind"] == "database" && t["every_ms"] == 30_000
                })
            })
    });
    let workflows = call(&studio, &signed, "GET", "/api/workflows", None);
    let shown = workflows.body["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == watched.as_str())
        .unwrap()
        .clone();
    assert_eq!(shown["trigger"], "database");
    // The credential forms the builder offers.
    let kinds = call(&studio, &signed, "GET", "/api/credential-kinds", None);
    assert_eq!(kinds.status, 200, "{}", kinds.body);
    let listed: Vec<&str> = kinds.body["kinds"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|k| k["kind"].as_str())
        .collect();
    assert!(
        listed.contains(&"mysql") && listed.contains(&"sqlserver"),
        "{listed:?}"
    );
}
