// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workspace's MCP server in `openduct start`: its workflows offered as tools listed and called by an MCP client
//! (OpenDuct's own, as the mcp step uses it), each call a run in the workflow's history; and the server's rules -
//! a token the workspace issued, within its tools and its requests a minute, revoked with a click; both protocol
//! versions; headers that must agree with the body; no page of another site.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::sync::Arc;
use std::time::Duration;

use openduct_flow::mcp::{Reached, Server, Session, ToolAnswer, ToolInfo};
use openduct_flow::{Credential, Live};
use serde_json::{Value, json};
use studio::{JSON, call, http, owner, start_with, until};

fn make(studio: &str, signed: &[String], definition: &Value, on: bool) -> String {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    if on {
        let turned = call(
            studio,
            signed,
            "POST",
            &format!("/api/workflows/{id}/active"),
            Some(r#"{"active": true}"#),
        );
        assert_eq!(turned.status, 200, "{}", turned.body);
    }
    id
}

fn price() -> Value {
    json!({
        "name": "Price an item",
        "trigger": "manual",
        "tool": {
            "description": "The price of an item from the price list, for a quantity",
            "input": { "sku": { "type": "string", "required": true }, "qty": { "type": "integer", "required": true } },
        },
        "steps": [
            { "name": "known", "if": { "when": "value.sku == 'NOPE'", "then": [{ "stop": { "error": "no price for {{ value.sku }}" } }] } },
            { "name": "line", "set": { "sku": "value.sku", "total": "value.qty * 12" } },
        ],
    })
}

/// The client OpenDuct's mcp step uses, signed in with `token`.
fn client(url: &str, token: &str) -> (Live, Server) {
    let live = Live::new("127.0.0.1:1", true).unwrap();
    let server = Server {
        url: url.to_owned(),
        credential: Some(Credential {
            kind: "bearer".to_owned(),
            fields: [("token".to_owned(), token.to_owned())].into_iter().collect(),
        }),
        timeout: Duration::from_secs(30),
    };
    (live, server)
}

fn block<T>(f: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn tools(url: &str, token: &str) -> Vec<ToolInfo> {
    let (live, server) = client(url, token);
    block(async {
        let mut s = Session::new(&live, server);
        match s.tools().await.unwrap() {
            Reached::Answered(t) => t,
            Reached::NotSent(_) => panic!("not sent"),
        }
    })
}

fn use_tool(url: &str, token: &str, name: &str, arguments: Value) -> ToolAnswer {
    let (live, server) = client(url, token);
    block(async {
        let mut s = Session::new(&live, server);
        let answer = match s.call(name, &arguments, false).await.unwrap() {
            Reached::Answered(a) => a,
            Reached::NotSent(_) => panic!("not sent"),
        };
        assert_eq!(s.protocol(), Some("2026-07-28"));
        answer
    })
}

/// One raw MCP request: `headers` beside the content type, `body` as sent.
fn mcp(studio: &str, path: &str, headers: &[&str], body: &Value) -> studio::Answer {
    let mut all = vec![JSON];
    all.extend_from_slice(headers);
    http(studio, "POST", path, &all, Some(&body.to_string()))
}

fn modern(method: &str, params: Value) -> Value {
    let mut params = params;
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": { "name": "raw", "version": "1" },
        "io.modelcontextprotocol/clientCapabilities": {},
    });
    json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params })
}

#[test]
fn workflows_offered_as_tools_are_listed_and_called_by_an_mcp_client_each_call_a_run() {
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
    let priced = make(&studio, &signed, &price(), true);
    let slow = make(
        &studio,
        &signed,
        &json!({ "name": "Slow", "tool": { "description": "Takes its time", "timeout": "1s" }, "steps": [{ "wait": "4s" }] }),
        true,
    );
    let off = make(
        &studio,
        &signed,
        &json!({ "name": "Off", "tool": { "description": "Not on" }, "steps": [{ "set": { "a": "1" } }] }),
        false,
    );
    make(
        &studio,
        &signed,
        &json!({ "name": "Not a tool", "steps": [{ "set": { "a": "1" } }] }),
        true,
    );

    // The workspace's page: the address, every workflow offered (on or not), no token yet.
    let page = call(&studio, &signed, "GET", "/api/mcp", None);
    assert_eq!(page.status, 200, "{}", page.body);
    let path = page.body["path"].as_str().unwrap().to_owned();
    let offered: Vec<&str> = page.body["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(offered, ["price_an_item", "slow", "off"]);
    assert_eq!(page.body["tokens"], json!([]));
    assert_eq!(page.body["manage"], true);

    let issued = call(
        &studio,
        &signed,
        "POST",
        "/api/mcp/tokens",
        Some(r#"{"name": "assistant"}"#),
    );
    assert_eq!(issued.status, 201, "{}", issued.body);
    let token = issued.body["token"].as_str().unwrap().to_owned();
    assert!(token.starts_with("odmcp_"));
    let page = call(&studio, &signed, "GET", "/api/mcp", None);
    assert_eq!(page.body["tokens"][0]["name"], "assistant");
    assert!(!page.body.to_string().contains(&token), "a token is shown once");

    let url = format!("http://{studio}{path}");
    let listed = tools(&url, &token);
    let names: Vec<&str> = listed.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["price_an_item", "slow"], "only workflows that are on");
    assert_eq!(listed[0].input_schema["required"], json!(["sku", "qty"]));
    assert_eq!(listed[0].title.as_deref(), Some("Price an item"));

    let answer = use_tool(&url, &token, "price_an_item", json!({ "sku": "TOTE", "qty": 3 }));
    assert!(!answer.is_error, "{answer:?}");
    assert_eq!(answer.structured, Some(json!({ "sku": "TOTE", "total": 36 })));
    let refused = use_tool(&url, &token, "price_an_item", json!({ "sku": "NOPE", "qty": 1 }));
    assert!(refused.is_error);
    assert!(refused.text.contains("no price for NOPE"), "{}", refused.text);
    let unfit = use_tool(&url, &token, "price_an_item", json!({ "sku": 7, "qty": 1 }));
    assert_eq!(
        unfit.text,
        "the arguments do not fit: sku: a value of type string"
    );
    let late = use_tool(&url, &token, "slow", json!({}));
    assert!(
        late.is_error && late.text.contains("did not finish within 1s"),
        "{}",
        late.text
    );

    // Each call that ran is a run of the workflow, said to come from the tool and the token.
    until(20, "the runs in the history", || {
        let runs = call(
            &studio,
            &signed,
            "GET",
            &format!("/api/workflows/{priced}/runs"),
            None,
        );
        runs.body["runs"].as_array().is_some_and(|r| r.len() == 2)
    });
    let runs = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{priced}/runs"),
        None,
    );
    let run = &runs.body["runs"][0];
    assert_eq!(run["trigger"]["trigger"], "tool");
    assert_eq!(run["trigger"]["tool"], "price_an_item");
    assert_eq!(run["trigger"]["token"], "assistant");
    let statuses: Vec<&str> = runs.body["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["status"].as_str().unwrap())
        .collect();
    assert!(
        statuses.contains(&"done") && statuses.contains(&"failed"),
        "{statuses:?}"
    );

    // The builder lists another server's tools the way a run reaches them: here, this studio's own.
    let kept = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "own-tools", "kind": "bearer", "fields": { "token": token } }).to_string()),
    );
    assert_eq!(kept.status, 201, "{}", kept.body);
    let found = call(
        &studio,
        &signed,
        "POST",
        "/api/mcp/discover",
        Some(&json!({ "url": url, "credential": "own-tools" }).to_string()),
    );
    assert_eq!(found.status, 200, "{}", found.body);
    assert_eq!(found.body["tools"][0]["name"], "price_an_item");
    assert_eq!(found.body["protocol"], "2026-07-28");

    // A workflow's mcp step calls a tool the same way.
    let asking = make(
        &studio,
        &signed,
        &json!({ "name": "Ask", "steps": [{ "name": "ask", "mcp": { "url": url, "credential": "own-tools", "tool": "price_an_item", "arguments": { "sku": "{{ value.sku }}", "qty": 2 } } }] }),
        false,
    );
    let preview = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{asking}/preview"),
        Some(r#"{"input": {"sku": "CAP"}}"#),
    );
    assert_eq!(preview.status, 200, "{}", preview.body);
    assert_eq!(
        preview.body["run"]["steps"][0]["result"]["test"], "not sent",
        "a preview does not call a tool that may write"
    );
    let _ = off;
    let _ = slow;
}

#[test]
fn the_mcp_server_keeps_its_rules_whoever_calls_it() {
    let data = tempfile::tempdir().unwrap();
    let running = start_with(
        data.path(),
        &["--listen", "127.0.0.1:0", "--studio-listen", "127.0.0.1:0"],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let priced = make(&studio, &signed, &price(), true);
    make(
        &studio,
        &signed,
        &json!({ "name": "Other", "tool": { "description": "Another tool" }, "steps": [{ "set": { "a": "1" } }] }),
        true,
    );
    let path = call(&studio, &signed, "GET", "/api/mcp", None).body["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let issued = call(
        &studio,
        &signed,
        "POST",
        "/api/mcp/tokens",
        Some(&json!({ "name": "narrow", "tools": [priced], "per_minute": 7 }).to_string()),
    );
    assert_eq!(issued.status, 201, "{}", issued.body);
    let token = issued.body["token"].as_str().unwrap().to_owned();
    let id = issued.body["id"].as_str().unwrap().to_owned();
    let auth = format!("authorization: Bearer {token}");
    let list = modern("tools/list", json!({}));

    // No token, a wrong one: refused, saying how to sign in. A GET: each message is a POST.
    let none = mcp(
        &studio,
        &path,
        &["mcp-protocol-version: 2026-07-28", "mcp-method: tools/list"],
        &list,
    );
    assert_eq!(none.status, 401);
    assert!(
        none.headers
            .to_ascii_lowercase()
            .contains("www-authenticate: bearer")
    );
    let wrong = mcp(
        &studio,
        &path,
        &["authorization: Bearer odmcp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],
        &list,
    );
    assert_eq!(wrong.status, 401);
    assert_eq!(http(&studio, "GET", &path, &[&auth], None).status, 405);

    // The current protocol: headers that agree with the body, a version it speaks.
    let headers = [
        auth.as_str(),
        "mcp-protocol-version: 2026-07-28",
        "mcp-method: tools/list",
    ];
    let listed = mcp(&studio, &path, &headers, &list);
    assert_eq!(listed.status, 200, "{}", listed.body);
    assert_eq!(listed.body["result"]["resultType"], "complete");
    let names: Vec<&str> = listed.body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["price_an_item"], "only the tools the token reaches");
    let mismatch = mcp(
        &studio,
        &path,
        &[
            auth.as_str(),
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: tools/call",
        ],
        &list,
    );
    assert_eq!(
        (mismatch.status, mismatch.body["error"]["code"].as_i64()),
        (400, Some(-32020))
    );
    let mut future = list.clone();
    future["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2099-01-01");
    let unknown = mcp(
        &studio,
        &path,
        &[
            auth.as_str(),
            "mcp-protocol-version: 2099-01-01",
            "mcp-method: tools/list",
        ],
        &future,
    );
    assert_eq!(
        (unknown.status, unknown.body["error"]["code"].as_i64()),
        (400, Some(-32022))
    );
    assert_eq!(unknown.body["error"]["data"]["supported"][0], "2026-07-28");
    let other = mcp(
        &studio,
        &path,
        &[
            auth.as_str(),
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: tools/call",
            "mcp-name: other",
        ],
        &modern("tools/call", json!({ "name": "other", "arguments": {} })),
    );
    assert_eq!(
        other.body["error"]["message"], "Unknown tool: other",
        "outside the token's reach"
    );

    // An earlier protocol: the handshake, then requests that name its version.
    let hello = mcp(
        &studio,
        &path,
        &[&auth],
        &json!({ "jsonrpc": "2.0", "id": "h", "method": "initialize", "params": { "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "old", "version": "1" } } }),
    );
    assert_eq!(hello.body["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(
        hello.body["result"]["capabilities"]["tools"],
        json!({ "listChanged": false })
    );
    assert!(
        !hello.headers.to_ascii_lowercase().contains("mcp-session-id"),
        "no session: each request stands alone"
    );
    let noted = mcp(
        &studio,
        &path,
        &[&auth, "mcp-protocol-version: 2025-06-18"],
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    );
    assert_eq!(noted.status, 202);
    let called = mcp(
        &studio,
        &path,
        &[&auth, "mcp-protocol-version: 2025-06-18"],
        &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": { "name": "price_an_item", "arguments": { "sku": "CAP", "qty": 2 } } }),
    );
    assert_eq!(
        called.body["result"]["structuredContent"],
        json!({ "sku": "CAP", "total": 24 }),
        "{}",
        called.body
    );
    assert!(
        called.body["result"].get("resultType").is_none(),
        "an earlier version's shape"
    );

    // Seven requests a minute for this token (refusals before sign-in are not counted): the eighth waits.
    let limited = mcp(&studio, &path, &headers, &list);
    assert_eq!(limited.status, 429, "{}", limited.body);
    assert!(limited.headers.to_ascii_lowercase().contains("retry-after:"));

    // A page of another site never reaches it; a revoked token no longer signs in.
    let issued = call(
        &studio,
        &signed,
        "POST",
        "/api/mcp/tokens",
        Some(r#"{"name": "wide"}"#),
    );
    let wide = format!("authorization: Bearer {}", issued.body["token"].as_str().unwrap());
    let site = mcp(
        &studio,
        &path,
        &[
            &wide,
            "origin: http://attacker.example",
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: tools/list",
        ],
        &list,
    );
    assert_eq!(site.status, 403);
    let discover = mcp(
        &studio,
        &path,
        &[
            &wide,
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: server/discover",
        ],
        &modern("server/discover", json!({})),
    );
    assert_eq!(discover.body["result"]["supportedVersions"][0], "2026-07-28");
    assert_eq!(
        discover.body["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "OpenDuct"
    );
    let missing = mcp(
        &studio,
        &path,
        &[
            &wide,
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: prompts/list",
        ],
        &modern("prompts/list", json!({})),
    );
    assert_eq!(
        (missing.status, missing.body["error"]["code"].as_i64()),
        (404, Some(-32601))
    );
    let revoked = call(&studio, &signed, "DELETE", &format!("/api/mcp/tokens/{id}"), None);
    assert_eq!(revoked.status, 200, "{}", revoked.body);
    let after = mcp(
        &studio,
        &path,
        &[
            &auth,
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: tools/list",
        ],
        &list,
    );
    assert_eq!(after.status, 401);

    // Tokens are an admin's to make; a request for another workspace's server with this one's token is refused.
    let elsewhere = mcp(
        &studio,
        "/mcp/no-such-workspace",
        &[
            &wide,
            "mcp-protocol-version: 2026-07-28",
            "mcp-method: tools/list",
        ],
        &list,
    );
    assert_eq!(elsewhere.status, 401);
    let bad = call(
        &studio,
        &signed,
        "POST",
        "/api/mcp/tokens",
        Some(r#"{"name": "x", "per_minute": 0}"#),
    );
    assert_eq!(bad.status, 422);
    let _ = Arc::new(());
}
