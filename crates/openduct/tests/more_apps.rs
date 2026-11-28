// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The ten apps added beside the first 26 (more apps, G1), in a running studio: each is offered to the editor with
//! its actions and how it signs in, those a person signs in to at the vendor wait for the install's registration,
//! and a workflow runs one end to end - Supabase, against a stand-in project on this machine - with its credential
//! sealed, each step's result recorded, and the requests as Supabase's Data API takes them.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use studio::{call, owner, start_with, stop, until};

/// A request the stand-in project was sent: its first line, its headers (names in lowercase) and its body.
#[derive(Clone, Debug)]
struct Seen {
    line: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// A stand-in Supabase project: `POST /rest/v1/contacts` keeps a row and gives it back with its id, `GET` gives the
/// rows kept; anything without the key is refused as Supabase refuses it.
fn project(seen: Arc<Mutex<Vec<Seen>>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut got = Vec::new();
            let mut buf = [0u8; 8192];
            let (head, body) = loop {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break (String::new(), String::new()),
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                }
                let text = String::from_utf8_lossy(&got).into_owned();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break (head.to_owned(), body.to_owned());
                    }
                }
            };
            let mut lines = head.lines();
            let line = lines.next().unwrap_or_default().to_owned();
            let headers: Vec<(String, String)> = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
                .collect();
            let request = Seen { line, headers, body };
            let keyed = request.header("apikey") == Some("sb_secret_stand_in");
            let (status, answer) = if !keyed {
                (401, json!({ "message": "Invalid API key" }))
            } else if request.line.starts_with("POST /rest/v1/contacts") {
                let mut row: Value = serde_json::from_str(&request.body).unwrap_or(json!({}));
                row["id"] = json!(7);
                (201, json!([row]))
            } else if request.line.starts_with("GET /rest/v1/contacts") {
                (
                    200,
                    json!([{ "id": 7, "name": "Ann Example", "email": "ann@example.com" }]),
                )
            } else {
                (
                    404,
                    json!({ "code": "42P01", "message": "relation \"public.nothere\" does not exist" }),
                )
            };
            seen.lock().unwrap().push(request);
            let text = answer.to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
        }
    });
    addr
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let got = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    got.body["runs"].as_array().cloned().unwrap_or_default()
}

/// The new apps, each with its sign-ins and whether a person signs in at the vendor (Connect).
const ADDED: &[(&str, &[(&str, bool)])] = &[
    ("apify", &[("apify", false)]),
    ("facebook", &[("facebook", false)]),
    ("github", &[("github", false), ("github_oauth", true)]),
    ("google_docs", &[("google_docs", true)]),
    ("jira", &[("jira", false)]),
    ("linkedin", &[("linkedin", true), ("linkedin_pages", true)]),
    ("supabase", &[("supabase", false)]),
    ("wordpress", &[("wordpress", false)]),
    ("x", &[("x", true)]),
    ("youtube", &[("youtube", true)]),
];

#[test]
fn the_new_apps_are_offered_and_one_runs_in_a_workflow_end_to_end() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let at = project(Arc::clone(&seen));
    let data = tempfile::tempdir().unwrap();
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

    // The editor is offered each new app, with its actions and how it signs in.
    let listed = call(&studio, &signed, "GET", "/api/apps", None);
    assert_eq!(listed.status, 200, "{}", listed.body);
    let apps = listed.body["apps"].as_array().unwrap();
    assert!(apps.len() >= 36, "only {} apps", apps.len());
    for (id, sign_ins) in ADDED {
        let app = apps
            .iter()
            .find(|a| a["id"] == *id)
            .unwrap_or_else(|| panic!("{id} is not offered"));
        let got: Vec<(String, bool)> = app["sign_ins"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    s["id"].as_str().unwrap().to_owned(),
                    s["connect"].as_bool().unwrap(),
                )
            })
            .collect();
        let want: Vec<(String, bool)> = sign_ins.iter().map(|(s, c)| ((*s).to_owned(), *c)).collect();
        assert_eq!(got, want, "{id}");
        let actions = app["actions"].as_array().unwrap();
        assert!(actions.len() >= 3, "{id}: {actions:?}");
        assert!(
            actions.iter().any(|a| a["name"] == app["default_action"]),
            "{id}: its default action is one of its actions"
        );
        // Every action has a label, in words for the person running a business.
        assert!(
            actions.iter().all(|a| !a["label"].as_str().unwrap().is_empty()),
            "{id}"
        );
    }
    // A run of an Apify Actor spends credit: it is a write, which a preview shows and does not send.
    let apify = apps.iter().find(|a| a["id"] == "apify").unwrap();
    let run = apify["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "run_and_get_items")
        .unwrap();
    assert_eq!(run["write"], true);

    // Those a person signs in to at the vendor wait for the install's registration.
    let oauth = call(&studio, &signed, "GET", "/api/oauth/apps", None);
    assert_eq!(oauth.status, 200, "{}", oauth.body);
    for id in ["google_docs", "youtube", "linkedin", "x", "github"] {
        let row = oauth.body["apps"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["app"] == id)
            .unwrap_or_else(|| panic!("{id} is not among the app sign-ins: {}", oauth.body));
        assert_eq!(row["registered"], false, "{id}");
    }

    // A Supabase credential: the project's address and a secret key, sealed and never shown back.
    let cred = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(
            &json!({ "name": "crm", "kind": "supabase", "fields": { "project_url": format!("http://{at}"), "api_key": "sb_secret_stand_in" } })
                .to_string(),
        ),
    );
    assert_eq!(cred.status, 201, "{}", cred.body);
    assert!(!cred.body.to_string().contains("sb_secret_stand_in"));

    // A workflow keeps a row, then finds it by the email the first step gave back.
    let definition = json!({
        "name": "Keep a contact",
        "steps": [
            { "name": "add", "supabase": { "credential": "crm", "action": "add_row", "table": "contacts",
              "row": { "name": "Ann Example", "email": "ann@example.com" } } },
            { "name": "find", "supabase": { "credential": "crm", "action": "list_rows", "table": "contacts",
              "match": { "email": "{{ steps.add.email }}" }, "columns": "id,name,email" } }
        ]
    });
    let checked = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(checked.status, 201, "{}", checked.body);
    let id = checked.body["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/active"),
            Some(r#"{"active": true}"#)
        )
        .status,
        200
    );
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/run"),
            Some("{}")
        )
        .status,
        202
    );
    until(30, "the run", || {
        runs(&studio, &signed, &id)
            .first()
            .is_some_and(|r| r["status"] == "done" || r["status"] == "failed")
    });
    let run = &runs(&studio, &signed, &id)[0];
    assert_eq!(run["status"], "done", "{run}");
    assert_eq!(
        run["steps"][0]["result"],
        json!({ "id": 7, "name": "Ann Example", "email": "ann@example.com" }),
        "{run}"
    );
    assert_eq!(
        run["steps"][1]["result"],
        json!([{ "id": 7, "name": "Ann Example", "email": "ann@example.com" }]),
        "{run}"
    );
    assert!(
        !run.to_string().contains("sb_secret_stand_in"),
        "the key is in the run's record: {run}"
    );

    // What the project was sent: the key in apikey alone (a new key is no token), the row back, the condition as one.
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(seen[0].line.starts_with("POST /rest/v1/contacts "), "{seen:?}");
    assert_eq!(seen[0].header("prefer"), Some("return=representation"));
    assert_eq!(seen[0].header("authorization"), None);
    assert_eq!(
        serde_json::from_str::<Value>(&seen[0].body).unwrap(),
        json!({ "name": "Ann Example", "email": "ann@example.com" })
    );
    let line = &seen[1].line;
    assert!(line.starts_with("GET /rest/v1/contacts?"), "{line}");
    for part in [
        "select=id%2Cname%2Cemail",
        "and=%28email.eq.%22ann%40example.com%22%29",
        "limit=1000",
    ] {
        assert!(line.contains(part), "{part} not in {line}");
    }
    stop(running);
}
