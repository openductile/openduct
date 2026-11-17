// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The studio's data model through its API: workspaces kept apart (an agency's clients), roles, versions kept once
//! by content, credentials and files of one workspace only, triggers listed, and a preview on a lookup file.

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
use studio::{JSON, call, http, owner, start, stop};

/// The signed-in headers, for one workspace.
fn within(signed: &[String], workspace: &str) -> Vec<String> {
    let mut headers = signed.to_vec();
    headers.push(format!("x-openduct-workspace: {workspace}"));
    headers
}

fn sign_in(studio: &str, email: &str, password: &str) -> Vec<String> {
    let body = json!({ "email": email, "password": password }).to_string();
    let signed = http(studio, "POST", "/api/sign-in", &[JSON], Some(&body));
    assert_eq!(signed.status, 200, "{}", signed.body);
    vec![
        format!("cookie: {}", signed.cookie()),
        format!("x-openduct-csrf: {}", signed.body["csrf"].as_str().unwrap()),
        JSON.to_owned(),
    ]
}

#[test]
fn workspaces_keep_their_clients_apart_and_roles_say_who_may_change_what() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let admin = owner(&studio);
    let session = call(&studio, &admin, "GET", "/api/session", None);
    let first = session.body["workspaces"][0]["id"].as_str().unwrap().to_owned();
    let made = call(
        &studio,
        &admin,
        "POST",
        "/api/workspaces",
        Some(r#"{"name": "Client B"}"#),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let second = made.body["id"].as_str().unwrap().to_owned();

    // A workflow and a credential in the first workspace are not in the second.
    let a = within(&admin, &first);
    let b = within(&admin, &second);
    let wf = call(
        &studio,
        &a,
        "POST",
        "/api/workflows",
        Some(r#"{"definition": {"name": "Only A", "steps": [{"set": {"x": "1"}}]}}"#),
    );
    assert_eq!(wf.status, 201, "{}", wf.body);
    let id = wf.body["id"].as_str().unwrap().to_owned();
    let cred = call(
        &studio,
        &a,
        "POST",
        "/api/credentials",
        Some(r#"{"name": "bot", "kind": "telegram", "fields": {"bot_token": "123:secret"}}"#),
    );
    assert_eq!(cred.status, 201, "{}", cred.body);
    assert!(
        !cred.body.to_string().contains("123:secret"),
        "a value is never shown"
    );
    assert_eq!(
        call(&studio, &b, "GET", "/api/workflows", None).body["workflows"],
        json!([])
    );
    assert_eq!(
        call(&studio, &b, "GET", "/api/credentials", None).body["credentials"],
        json!([])
    );
    assert_eq!(
        call(&studio, &b, "GET", &format!("/api/workflows/{id}"), None).status,
        404,
        "not by its id either"
    );

    // A viewer of the second workspace looks, and changes nothing; it does not see the first at all.
    let added = call(
        &studio,
        &b,
        "POST",
        &format!("/api/workspaces/{second}/members"),
        Some(
            r#"{"email": "viewer@example.com", "name": "Vera", "password": "a first password", "role": "viewer"}"#,
        ),
    );
    assert_eq!(added.status, 201, "{}", added.body);
    let vera = sign_in(&studio, "viewer@example.com", "a first password");
    let vera_session = call(&studio, &vera, "GET", "/api/session", None);
    assert_eq!(
        vera_session.body["workspaces"].as_array().unwrap().len(),
        1,
        "only the workspace she is in"
    );
    let vera_b = within(&vera, &second);
    assert_eq!(call(&studio, &vera_b, "GET", "/api/workflows", None).status, 200);
    let refused = call(
        &studio,
        &vera_b,
        "POST",
        "/api/workflows",
        Some(r#"{"definition": {"name": "No"}}"#),
    );
    assert_eq!(refused.status, 403, "{}", refused.body);
    assert_eq!(
        call(&studio, &within(&vera, &first), "GET", "/api/workflows", None).status,
        404
    );
    let not_admin = call(
        &studio,
        &vera,
        "POST",
        "/api/workspaces",
        Some(r#"{"name": "Mine"}"#),
    );
    assert_eq!(not_admin.status, 403);
    stop(running);
}

#[test]
fn a_change_saves_a_version_files_are_looked_up_and_a_preview_shows_every_step() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);

    let one = json!({ "name": "Prices", "trigger": "webhook", "steps": [{ "name": "item", "lookup": { "file": "/items.csv", "where": { "sku": "{{ value.sku }}" } } }] });
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": one }).to_string()),
    );
    let id = made.body["id"].as_str().unwrap().to_owned();
    let same = call(
        &studio,
        &signed,
        "PUT",
        &format!("/api/workflows/{id}"),
        Some(&json!({ "definition": one }).to_string()),
    );
    assert_eq!(same.body["version"], 1, "the same definition is no new version");
    let mut two = one.clone();
    two["steps"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "name": "keep", "topic": { "name": "priced" } }));
    let changed = call(
        &studio,
        &signed,
        "PUT",
        &format!("/api/workflows/{id}"),
        Some(&json!({ "definition": two }).to_string()),
    );
    assert_eq!(changed.body["version"], 2);
    let versions = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{id}/versions"),
        None,
    );
    let numbers: Vec<Value> = versions.body["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["number"].clone())
        .collect();
    assert_eq!(
        numbers,
        [json!(2), json!(1)],
        "newest first, each with its definition"
    );
    assert_eq!(versions.body["versions"][1]["definition"], one);

    let csv = "sku,name,price\na,Apple,1.20\nb,\"Bread, brown\",2.50\n";
    let uploaded = call(
        &studio,
        &signed,
        "POST",
        "/api/files",
        Some(&json!({ "path": "/items.csv", "content": csv }).to_string()),
    );
    assert_eq!(uploaded.status, 201, "{}", uploaded.body);
    let preview = call(
        &studio,
        &signed,
        "GET",
        "/api/files/preview?path=%2Fitems.csv",
        None,
    );
    assert_eq!(preview.body["rows"], 2);
    assert_eq!(preview.body["columns"], json!(["sku", "name", "price"]));
    assert_eq!(preview.body["first"][1]["name"], "Bread, brown");

    let shown = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/preview"),
        Some(r#"{"input": {"sku": "b"}}"#),
    );
    assert_eq!(shown.status, 200, "{}", shown.body);
    assert_eq!(shown.body["run"]["status"], "done", "{}", shown.body);
    assert_eq!(
        shown.body["run"]["steps"][0]["result"]["price"], "2.50",
        "the file was read"
    );
    assert_eq!(
        shown.body["run"]["steps"][1]["result"]["test"], "not written",
        "a preview writes nothing"
    );

    // Turned on, it has a trigger record: its webhook, at its own address.
    call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    let triggers = call(&studio, &signed, "GET", "/api/triggers", None);
    let listed = triggers.body["triggers"].as_array().unwrap();
    assert_eq!(listed.len(), 1, "{}", triggers.body);
    assert_eq!(listed[0]["kind"], "webhook");
    assert_eq!(listed[0]["hook"], changed.body["hook"]);
    assert!(
        !listed[0]["hook"].as_str().unwrap().contains(&id),
        "the address is not the workflow's id"
    );
    stop(running);
}
