// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The app generator (`openduct app from-openapi`, `from-mcp`) from end to end: a description in, an app's folder
//! out that `openduct app check` passes at the bar and `openduct app test` passes with no account; a description at
//! an address reached as an HTTP step reaches one; an MCP server's tools as actions, one called for real through
//! the engine. The servers here are stand-ins on this machine.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(feature = "studio")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

fn fixture(name: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/openapi")
        .join(name)
        .display()
        .to_string()
}

fn app(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new(OPENDUCT)
        .arg("app")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// What a stand-in server was asked: each request's first line, headers and body.
type Seen = Arc<Mutex<Vec<String>>>;

/// A stand-in HTTP server on this machine: `answer(method, path, body)` gives the status, type and body.
fn serve(
    answer: impl Fn(&str, &str, &str) -> (u16, &'static str, String) + Send + 'static,
) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen: Seen = Arc::default();
    let kept = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut head = String::new();
            let mut length = 0;
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
            let first = head.lines().next().unwrap_or_default().to_owned();
            let mut parts = first.split(' ');
            let (method, path) = (parts.next().unwrap_or_default(), parts.next().unwrap_or_default());
            kept.lock().unwrap().push(format!("{head}\n{body}"));
            let (status, kind, text) = answer(method, path, &body);
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
        }
    });
    (addr, seen)
}

#[test]
fn an_app_made_from_a_description_checks_tests_and_meets_the_bar() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (ok, out) = app(
        d,
        &["from-openapi", &fixture("tasks.yaml"), "--name", "acme_tasks"],
    );
    assert!(ok, "{out}");
    assert!(
        out.contains("Made apps/acme_tasks: 10 actions, 11 recorded tests"),
        "{out}"
    );
    for file in [
        "app.yaml",
        "README.md",
        "tests/list_tasks.yaml",
        "tests/refused.yaml",
    ] {
        assert!(d.join("apps/acme_tasks").join(file).is_file(), "{file}");
    }
    // Checked, at the bar, and every recorded exchange holds with no account.
    let (ok, out) = app(d, &["check", "apps/acme_tasks", "--bar", "80"]);
    assert!(ok, "{out}");
    assert!(
        out.contains("signs in with a bearer token (acme_tasks); a sign-in at the vendor (OAuth 2.0) (acme_tasks_oauth)"),
        "{out}"
    );
    assert!(
        out.contains("10 with a recorded test, a refusal recorded"),
        "{out}"
    );
    let (ok, out) = app(d, &["test", "apps/acme_tasks"]);
    assert!(ok && out.contains("11 passed, 0 failed"), "{out}");
    // What it does not do is listed, never guessed.
    let readme = std::fs::read_to_string(d.join("apps/acme_tasks/README.md")).unwrap();
    for said in [
        "## Not done",
        "OpenID Connect",
        "file upload",
        "webhooks",
        "/oauth/callback",
        "## Tested",
    ] {
        assert!(readme.contains(said), "{said}: {readme}");
    }

    // Made again: refused over an app that is there, unless --force; a recording written by hand stays.
    let (ok, out) = app(
        d,
        &["from-openapi", &fixture("tasks.yaml"), "--name", "acme_tasks"],
    );
    assert!(!ok && out.contains("--force"), "{out}");
    let own = d.join("apps/acme_tasks/tests/my_own.yaml");
    std::fs::copy(d.join("apps/acme_tasks/tests/get_task.yaml"), &own).unwrap();
    let text = std::fs::read_to_string(&own)
        .unwrap()
        .replace("# Made by", "# Written by");
    std::fs::write(&own, text).unwrap();
    let (ok, out) = app(
        d,
        &[
            "from-openapi",
            &fixture("tasks.yaml"),
            "--name",
            "acme_tasks",
            "--force",
            "--actions",
            "3",
        ],
    );
    assert!(ok, "{out}");
    assert!(out.contains("3 actions"), "{out}");
    assert!(own.is_file(), "a recording written by hand stays");
    assert!(
        !d.join("apps/acme_tasks/tests/list_comments.yaml").exists(),
        "those made before go"
    );

    // A description that says nothing of signing in needs one given; given, its app holds.
    let (ok, out) = app(
        d,
        &[
            "from-openapi",
            &fixture("docusign-cut.yaml"),
            "--name",
            "docusign_cut",
        ],
    );
    assert!(!ok && out.contains("--sign-in bearer"), "{out}");
    let (ok, out) = app(
        d,
        &[
            "from-openapi",
            &fixture("docusign-cut.yaml"),
            "--name",
            "docusign_cut",
            "--sign-in",
            "bearer",
        ],
    );
    assert!(ok, "{out}");
    let (ok, out) = app(d, &["test", "apps/docusign_cut"]);
    assert!(ok && out.contains(" passed, 0 failed"), "{out}");

    // The bar fails an app below it: one written by hand with no outputs.
    let (ok, out) = app(d, &["new", "my_crm"]);
    assert!(ok, "{out}");
    let (ok, out) = app(d, &["check", "apps/my_crm", "--bar", "80"]);
    assert!(!ok && out.contains("is below the bar of 80"), "{out}");
    let (ok, out) = app(d, &["check", "apps/my_crm"]);
    assert!(ok && out.contains("to do: "), "without a bar it reports: {out}");
}

#[test]
fn a_description_at_an_address_is_reached_as_an_http_step_reaches_one() {
    let spec = std::fs::read_to_string(fixture("tasks.yaml")).unwrap();
    let (addr, seen) = serve(move |_, path, _| match path {
        "/openapi.yaml" => (200, "application/yaml", spec.clone()),
        _ => (404, "text/plain", "no".to_owned()),
    });
    let dir = tempfile::tempdir().unwrap();
    let url = format!("http://{addr}/openapi.yaml");
    // This machine's own network is refused unless allowed, as for an HTTP step.
    let (ok, out) = app(dir.path(), &["from-openapi", &url, "--name", "acme_tasks"]);
    assert!(!ok && out.contains("own network"), "{out}");
    assert!(seen.lock().unwrap().is_empty(), "nothing was asked");
    let (ok, out) = app(
        dir.path(),
        &[
            "from-openapi",
            &url,
            "--name",
            "acme_tasks",
            "--allow-private-network",
        ],
    );
    assert!(ok, "{out}");
    let readme = std::fs::read_to_string(dir.path().join("apps/acme_tasks/README.md")).unwrap();
    assert!(readme.contains(&url), "{readme}");
    // An address that answers with a refusal is said.
    let (ok, out) = app(
        dir.path(),
        &[
            "from-openapi",
            &format!("http://{addr}/gone.yaml"),
            "--name",
            "gone",
            "--allow-private-network",
        ],
    );
    assert!(!ok && out.contains("answered 404"), "{out}");
}

#[test]
fn an_app_made_from_an_mcp_server_calls_its_tools() {
    let tools = serde_json::json!([
        { "name": "create-issue", "title": "Open an issue", "description": "Opens an issue.",
          "inputSchema": { "type": "object", "required": ["title"], "properties": { "title": { "type": "string" } } },
          "outputSchema": { "type": "object", "properties": { "id": { "type": "string" } } } },
        { "name": "search", "description": "Finds issues.", "annotations": { "readOnlyHint": true },
          "inputSchema": { "type": "object", "properties": { "q": { "type": "string" } } } },
    ]);
    let (addr, seen) = serve(move |_, _, body| {
        let asked: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
        let result = match asked["method"].as_str() {
            Some("tools/list") => serde_json::json!({ "tools": tools }),
            Some("tools/call") => serde_json::json!({
                "content": [{ "type": "text", "text": "{\"id\": \"B-7\"}" }], "structuredContent": { "id": "B-7" },
            }),
            _ => serde_json::json!({}),
        };
        let answer = serde_json::json!({ "jsonrpc": "2.0", "id": asked["id"], "result": result });
        (200, "application/json", answer.to_string())
    });
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let token = d.join("token.env");
    std::fs::write(&token, "token=mcp_test_token\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let url = format!("http://{addr}/mcp");
    let (ok, out) = app(
        d,
        &[
            "from-mcp",
            &url,
            "--name",
            "issues_mcp",
            "--token-file",
            "token.env",
        ],
    );
    assert!(!ok && out.contains("own network"), "{out}");
    let (ok, out) = app(
        d,
        &[
            "from-mcp",
            &url,
            "--name",
            "issues_mcp",
            "--token-file",
            "token.env",
            "--allow-private-network",
        ],
    );
    assert!(ok, "{out}");
    assert!(
        seen.lock().unwrap().iter().any(|r| r
            .to_ascii_lowercase()
            .contains("authorization: bearer mcp_test_token")),
        "the server's tools are listed with the token"
    );
    let (ok, out) = app(d, &["check", "apps/issues_mcp", "--bar", "80"]);
    assert!(ok, "{out}");
    let (ok, out) = app(d, &["test", "apps/issues_mcp"]);
    assert!(ok && out.contains("3 passed, 0 failed"), "{out}");
    let definition = std::fs::read_to_string(d.join("apps/issues_mcp/app.yaml")).unwrap();
    assert!(definition.contains("mcp: create-issue"), "{definition}");

    // An action calls its tool for real, through the engine, as a workflow step does.
    std::fs::write(d.join("token.env"), "token=mcp_test_token\n").unwrap();
    let (ok, out) = app(
        d,
        &[
            "try",
            "apps/issues_mcp",
            "create_issue",
            "--credential-file",
            "token.env",
            "--input",
            "title=Bug",
            "--send",
        ],
    );
    assert!(ok, "{out}");
    assert!(out.contains("B-7"), "{out}");
    let called = seen
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.contains("\"tools/call\"") && r.contains("\"title\":\"Bug\""));
    assert!(called, "the tool was called with the input");
}
