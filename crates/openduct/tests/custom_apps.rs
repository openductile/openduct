// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workspace's custom apps in `openduct start` (`/api/apps/custom`): one made from an API description at an
//! address, previewed with its proposed actions, sign-ins and quality, saved, offered in the workspace's builder,
//! signed in with a credential of its own kind, run in a workflow against a stand-in of the vendor; changed,
//! exported and removed. Another workspace neither sees nor uses it; a viewer looks and changes nothing; the names
//! and bounds hold; a description on this machine's own network is refused unless the studio allows it.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use studio::{JSON, call, http, owner, start, start_with, stop, until};

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

/// A GET whose answer is not JSON: its head and its text.
fn raw_get(studio: &str, signed: &[String], path: &str) -> (String, String) {
    let mut stream = TcpStream::connect(studio).unwrap();
    let mut request = format!("GET {path} HTTP/1.1\r\nconnection: close\r\nhost: {studio}\r\n");
    for h in signed {
        request.push_str(h);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    (head.to_owned(), body.to_owned())
}

type Seen = Arc<Mutex<Vec<String>>>;

/// A stand-in of the vendor on this machine: its API description (naming this server), a task, and an MCP server.
fn vendor() -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let spec = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/openapi/tasks.yaml"),
    )
    .unwrap()
    .replace("https://api.tasks.example/v2", &format!("http://{addr}/v2"));
    let seen: Seen = Arc::default();
    let kept = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let (mut head, mut length) = (String::new(), 0);
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                if let Some(n) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = n.trim().parse().unwrap_or(0);
                }
                head.push_str(&line);
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body = String::from_utf8_lossy(&body).into_owned();
            kept.lock().unwrap().push(format!("{head}\n{body}"));
            let path = head.split(' ').nth(1).unwrap_or_default().to_owned();
            let (kind, text) = if path == "/openapi.yaml" {
                ("application/yaml", spec.clone())
            } else if path.starts_with("/v2/tasks/t1") {
                (
                    "application/json",
                    json!({ "id": "t1", "title": "Call Ann", "done": false }).to_string(),
                )
            } else if path == "/mcp" {
                let asked: Value = serde_json::from_str(&body).unwrap_or_default();
                let tools = json!({ "tools": [{ "name": "ping", "description": "Answers pong.",
                    "inputSchema": { "type": "object", "properties": {} } }] });
                (
                    "application/json",
                    json!({ "jsonrpc": "2.0", "id": asked["id"], "result": tools }).to_string(),
                )
            } else {
                ("application/json", "{}".to_owned())
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
        }
    });
    (addr, seen)
}

#[test]
fn a_custom_app_is_made_used_changed_and_kept_to_its_workspace() {
    let (vendor, seen) = vendor();
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
    let (a, b) = (within(&admin, &first), within(&admin, &second));
    let url = format!("http://{vendor}/openapi.yaml");

    // A preview: the actions proposed (ranked, the best chosen), the sign-ins (one at the vendor not allowed for a
    // custom app, so not chosen), the quality report.
    let ask = |extra: Value| {
        let mut body = json!({ "from": "openapi", "url": url, "name": "acme_tasks" });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        call(&studio, &a, "POST", "/api/apps/custom", Some(&body.to_string()))
    };
    let preview = ask(json!({}));
    assert_eq!(preview.status, 200, "{}", preview.body);
    assert_eq!(preview.body["saved"], false);
    assert_eq!(preview.body["problems"], json!([]), "{}", preview.body);
    let proposed = preview.body["made"]["proposed"].as_array().unwrap();
    assert!(
        proposed
            .iter()
            .any(|p| p["name"] == "get_task" && p["chosen"] == true),
        "{proposed:?}"
    );
    assert!(
        proposed
            .iter()
            .any(|p| p["left_out"].as_str().is_some_and(|w| w.contains("deprecated")))
    );
    let sign_ins = preview.body["made"]["sign_ins"].as_array().unwrap();
    assert_eq!(sign_ins.len(), 2, "{sign_ins:?}");
    assert_eq!(
        (sign_ins[0]["allowed"].clone(), sign_ins[0]["chosen"].clone()),
        (json!(true), json!(true))
    );
    assert_eq!(
        (sign_ins[1]["allowed"].clone(), sign_ins[1]["chosen"].clone()),
        (json!(false), json!(false))
    );
    assert!(preview.body["quality"]["score"].as_u64().unwrap() > 0);
    assert!(
        !preview.body["made"]["app_yaml"]
            .as_str()
            .unwrap()
            .contains("oauth2"),
        "the sign-in at the vendor is left out"
    );
    assert!(
        call(&studio, &a, "GET", "/api/apps/custom", None).body["apps"]
            .as_array()
            .unwrap()
            .is_empty(),
        "a preview keeps nothing"
    );

    // Saved, with the actions the person kept.
    let saved = ask(json!({ "chosen": ["get_task", "list_tasks", "create_task"], "save": true }));
    assert_eq!(saved.status, 201, "{}", saved.body);
    assert_eq!(saved.body["app"]["quality"]["actions"], 3);
    let again = ask(json!({ "save": true }));
    assert_eq!(again.status, 422, "{}", again.body);
    assert!(
        again.body["error"].as_str().unwrap().contains("already"),
        "{}",
        again.body
    );

    // Offered in its workspace's builder alone, signed in with a credential kind of its own.
    let apps = call(&studio, &a, "GET", "/api/apps", None).body;
    let mine = apps["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["id"] == "acme_tasks")
        .unwrap()
        .clone();
    assert_eq!(mine["custom"], true);
    assert_eq!(mine["actions"].as_array().unwrap().len(), 3);
    let kind = mine["sign_ins"][0]["id"].as_str().unwrap().to_owned();
    assert!(kind.starts_with("acme_tasks_") && kind != "acme_tasks", "{kind}");
    let other = call(&studio, &b, "GET", "/api/apps", None).body;
    assert!(
        !other["apps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["id"] == "acme_tasks"),
        "not in another workspace"
    );
    let kinds = call(&studio, &a, "GET", "/api/credential-kinds", None).body;
    assert!(
        kinds["kinds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k["kind"] == kind && k["custom"] == true)
    );
    let kinds_b = call(&studio, &b, "GET", "/api/credential-kinds", None).body;
    assert!(
        !kinds_b["kinds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k["kind"] == kind)
    );
    let credential =
        json!({ "name": "tasks-key", "kind": kind, "fields": { "token": "tk_live_1" } }).to_string();
    let kept = call(&studio, &a, "POST", "/api/credentials", Some(&credential));
    assert_eq!(kept.status, 201, "{}", kept.body);
    let refused = call(&studio, &b, "POST", "/api/credentials", Some(&credential));
    assert_eq!(
        refused.status, 422,
        "another workspace may not use its kind: {}",
        refused.body
    );

    // A workflow of the workspace uses it, and a preview runs its step against the vendor.
    let definition = json!({ "name": "Look up a task", "trigger": "manual", "steps": [
        { "name": "task", "acme_tasks": { "action": "get_task", "credential": "tasks-key", "task_id": "t1" } },
    ] });
    let wf = call(
        &studio,
        &a,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(wf.status, 201, "{}", wf.body);
    let id = wf.body["id"].as_str().unwrap().to_owned();
    let ran = call(
        &studio,
        &a,
        "POST",
        &format!("/api/workflows/{id}/preview"),
        Some(&json!({ "definition": definition, "input": {} }).to_string()),
    );
    assert_eq!(ran.status, 200, "{}", ran.body);
    assert!(ran.body.to_string().contains("Call Ann"), "{}", ran.body);
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|r| r.starts_with("GET /v2/tasks/t1")
                && r.to_ascii_lowercase().contains("authorization: bearer tk_live_1")),
        "the vendor was asked, signed in with the workspace's credential"
    );
    // On, and run by the node's runner as any workflow is.
    let on = call(
        &studio,
        &a,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);
    let runs = || call(&studio, &a, "GET", &format!("/api/workflows/{id}/runs"), None).body["runs"].clone();
    until(30, "the workflow runs", || {
        call(
            &studio,
            &a,
            "POST",
            &format!("/api/workflows/{id}/run"),
            Some("{}"),
        )
        .status
            == 202
    });
    until(30, "the run", || {
        runs()[0]["status"]
            .as_str()
            .is_some_and(|s| s == "done" || s == "failed")
    });
    assert_eq!(runs()[0]["status"], "done", "{}", runs());
    assert_eq!(runs()[0]["steps"][0]["result"]["title"], "Call Ann", "{}", runs());
    // Another workspace cannot name it in a step.
    let wf_b = call(
        &studio,
        &b,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(wf_b.status, 422, "{}", wf_b.body);

    // Names and bounds.
    for (name, said) in [
        ("slack", "OpenDuct carries"),
        ("http", "a step of OpenDuct's own"),
        ("Bad Name", "lowercase"),
    ] {
        let r = ask(json!({ "name": name, "save": true }));
        assert_eq!(r.status, 422, "{name}: {}", r.body);
        assert!(
            r.body["error"].as_str().unwrap().contains(said),
            "{name}: {}",
            r.body
        );
    }
    let many: String = (0..=100)
        .map(|i| format!("  a{i}: {{ label: A, method: GET, path: /a }}\n"))
        .collect();
    let big = format!(
        "app: too_big\nname: Too big\nhosts: [api.big.example]\nbase_url: https://api.big.example\nsign_in:\n  - {{ id: too_big, label: A key, fields: [{{ name: k, label: K, secret: true }}], headers: {{ X-K: \"{{{{ credential.k }}}}\" }} }}\nactions:\n{many}"
    );
    let r = call(
        &studio,
        &a,
        "POST",
        "/api/apps/custom",
        Some(&json!({ "from": "definition", "text": big, "save": true }).to_string()),
    );
    assert_eq!(r.status, 422, "{}", r.body);
    assert!(
        r.body["error"].as_str().unwrap().contains("at most 100"),
        "{}",
        r.body
    );
    let wordy = format!(
        "app: wordy\nname: Wordy\ndescription: \"{}\"\nhosts: [api.w.example]\nbase_url: https://api.w.example\nsign_in:\n  - {{ id: wordy, label: A key, fields: [{{ name: k, label: K, secret: true }}], headers: {{ X-K: \"{{{{ credential.k }}}}\" }} }}\nactions:\n  a: {{ label: A, method: GET, path: /a }}\n",
        "word ".repeat(300)
    );
    let r = call(
        &studio,
        &a,
        "POST",
        "/api/apps/custom",
        Some(&json!({ "from": "definition", "text": wordy, "save": true }).to_string()),
    );
    assert_eq!(r.status, 422, "{}", r.body);
    assert!(
        r.body["error"].as_str().unwrap().contains("description"),
        "{}",
        r.body
    );

    // A viewer looks, and changes nothing.
    let added = call(
        &studio,
        &a,
        "POST",
        &format!("/api/workspaces/{first}/members"),
        Some(
            r#"{"email": "viewer@example.com", "name": "Vera", "password": "a first password", "role": "viewer"}"#,
        ),
    );
    assert_eq!(added.status, 201, "{}", added.body);
    let vera = within(
        &sign_in(&studio, "viewer@example.com", "a first password"),
        &first,
    );
    let listed = call(&studio, &vera, "GET", "/api/apps/custom", None);
    assert_eq!(listed.status, 200, "{}", listed.body);
    assert_eq!(listed.body["apps"][0]["id"], "acme_tasks");
    let body = json!({ "from": "definition", "text": "app: x", "save": true }).to_string();
    assert_eq!(
        call(&studio, &vera, "POST", "/api/apps/custom", Some(&body)).status,
        403
    );
    assert_eq!(
        call(&studio, &vera, "DELETE", "/api/apps/custom/acme_tasks", None).status,
        403
    );

    // Changed, exported, removed.
    let one = call(&studio, &a, "GET", "/api/apps/custom/acme_tasks", None);
    assert_eq!(one.status, 200, "{}", one.body);
    let text = one.body["definition"]
        .as_str()
        .unwrap()
        .replace("Look up a task", "Find one task");
    let changed = call(
        &studio,
        &a,
        "PUT",
        "/api/apps/custom/acme_tasks",
        Some(&json!({ "definition": text }).to_string()),
    );
    assert_eq!(changed.status, 200, "{}", changed.body);
    let renamed = text.replace("app: acme_tasks", "app: acme_other");
    let r = call(
        &studio,
        &a,
        "PUT",
        "/api/apps/custom/acme_tasks",
        Some(&json!({ "definition": renamed }).to_string()),
    );
    assert_eq!(r.status, 422, "{}", r.body);
    let (head, yaml) = raw_get(&studio, &a, "/api/apps/custom/acme_tasks/app.yaml");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: application/yaml"),
        "{head}"
    );
    assert!(
        yaml.contains("Find one task") && yaml.contains("app: acme_tasks"),
        "{yaml}"
    );
    assert_eq!(
        call(&studio, &b, "GET", "/api/apps/custom/acme_tasks", None).status,
        404,
        "nor by its name"
    );
    assert_eq!(
        call(&studio, &a, "DELETE", "/api/apps/custom/acme_tasks", None).status,
        200
    );
    let apps = call(&studio, &a, "GET", "/api/apps", None).body;
    assert!(
        !apps["apps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["id"] == "acme_tasks")
    );
    assert_eq!(
        call(&studio, &a, "GET", "/api/apps/custom/acme_tasks", None).status,
        404
    );

    // From an MCP server's address: its tools as actions.
    let mcp = json!({ "from": "mcp", "url": format!("http://{vendor}/mcp"), "name": "pinger", "token": "t1", "save": true });
    let r = call(&studio, &a, "POST", "/api/apps/custom", Some(&mcp.to_string()));
    assert_eq!(r.status, 201, "{}", r.body);
    assert!(
        r.body["app"]["definition"]
            .as_str()
            .unwrap()
            .contains("mcp: ping"),
        "{}",
        r.body
    );
    stop(running);
}

#[test]
fn a_description_on_this_machine_s_own_network_is_refused_unless_allowed() {
    let (vendor, seen) = vendor();
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let body =
        json!({ "from": "openapi", "url": format!("http://{vendor}/openapi.yaml"), "name": "acme_tasks" });
    let r = call(
        &studio,
        &signed,
        "POST",
        "/api/apps/custom",
        Some(&body.to_string()),
    );
    assert_eq!(r.status, 422, "{}", r.body);
    assert!(
        r.body["error"].as_str().unwrap().contains("own network"),
        "{}",
        r.body
    );
    assert!(seen.lock().unwrap().is_empty(), "nothing was asked");
    // A description given as text is read, and nothing is reached.
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/openapi/tasks.yaml"),
    )
    .unwrap();
    let body = json!({ "from": "openapi", "text": text, "file": "tasks.yaml", "name": "acme_tasks" });
    let r = call(
        &studio,
        &signed,
        "POST",
        "/api/apps/custom",
        Some(&body.to_string()),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    // A description larger than other requests may be (1 MB) is read for a signed-in session.
    let large = format!(
        "{text}\n{}",
        "# a long description, as vendors' are\n".repeat(50_000)
    );
    assert!(large.len() > 1024 * 1024);
    let body = json!({ "from": "openapi", "text": large, "name": "acme_tasks" }).to_string();
    let r = call(&studio, &signed, "POST", "/api/apps/custom", Some(&body));
    assert_eq!(r.status, 200, "{}", r.body);
    stop(running);
}
