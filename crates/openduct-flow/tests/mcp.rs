// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The MCP step against stand-in MCP servers on this machine: one of the current protocol, one of an earlier one (a
//! handshake, a session, answers as Server-Sent Events), and one that names only earlier versions - each tool called,
//! listed, refused, and never reached in a test run or by a preview's write.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::{Credential, DryRun, Effects, Event, Live, Outcome, Preview, Secrets, Workflow, run};
use serde_json::{Value as Json, json};

#[derive(Debug, Clone, Copy, PartialEq)]
enum Speaks {
    /// 2026-07-28: no handshake, the version in every request.
    Modern,
    /// 2025-06-18: `initialize`, a session, answers as events.
    Legacy,
    /// Takes the current protocol's requests, but names only 2025-11-25 as what it speaks.
    OnlyOlder,
}

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    headers: BTreeMap<String, String>,
    body: Json,
}

type Log = Arc<Mutex<Vec<Seen>>>;

fn read_request(stream: &mut std::net::TcpStream) -> Option<Seen> {
    let mut buf = vec![0u8; 65536];
    let mut got = Vec::new();
    loop {
        let n = stream.read(&mut buf).ok()?;
        if n == 0 {
            return None;
        }
        got.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&got).into_owned();
        if let Some((head, rest)) = text.split_once("\r\n\r\n") {
            let mut lines = head.lines();
            let method = lines.next()?.split(' ').next()?.to_owned();
            let headers: BTreeMap<String, String> = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
                .collect();
            let length = headers
                .get("content-length")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            if rest.len() >= length {
                return Some(Seen {
                    method,
                    headers,
                    body: serde_json::from_str(rest).unwrap_or(Json::Null),
                });
            }
        }
    }
}

fn tools_page(cursor: Option<&str>) -> Json {
    if cursor == Some("p2") {
        return json!({ "tools": [{ "name": "fail", "description": "Always fails", "inputSchema": { "type": "object" } }] });
    }
    json!({
        "tools": [
            { "name": "get_forecast", "title": "Forecast", "description": "The weather for a city",
              "inputSchema": { "type": "object", "properties": { "city": { "type": "string" }, "days": { "type": "integer" } }, "required": ["city"] } },
            { "name": "in_region", "description": "Something done in a region",
              "inputSchema": { "type": "object", "properties": { "region": { "type": "string", "x-mcp-header": "Region" } } } },
            { "name": "bad", "description": "A header on a number breaks the rules",
              "inputSchema": { "type": "object", "properties": { "n": { "type": "number", "x-mcp-header": "N" } } } },
        ],
        "nextCursor": "p2",
    })
}

fn tool_answer(name: &str, arguments: &Json) -> Json {
    match name {
        "get_forecast" => json!({
            "content": [{ "type": "text", "text": format!("Sunny in {}", arguments["city"].as_str().unwrap_or("?")) }],
            "structuredContent": { "city": arguments["city"], "days": arguments["days"], "sky": "sunny" },
        }),
        "in_region" => {
            json!({ "content": [{ "type": "text", "text": format!("done in {}", arguments["region"].as_str().unwrap_or("?")) }] })
        }
        _ => json!({ "content": [{ "type": "text", "text": "no such city" }], "isError": true }),
    }
}

/// What the stand-in answers: its status, its type, its headers, its body.
fn answer(speaks: Speaks, seen: &Seen) -> (u16, &'static str, Vec<(String, String)>, String) {
    let jsonrpc = |status: u16, message: Json| (status, "application/json", Vec::new(), message.to_string());
    if seen.headers.get("authorization").map(String::as_str) != Some("Bearer s3cret") {
        return (401, "application/json", Vec::new(), "{}".to_owned());
    }
    if seen.method == "DELETE" {
        return (200, "text/plain", Vec::new(), String::new());
    }
    let id = seen.body["id"].clone();
    let method = seen.body["method"].as_str().unwrap_or_default().to_owned();
    let params = &seen.body["params"];
    let result = |r: Json| json!({ "jsonrpc": "2.0", "id": id, "result": r });
    let error = |code: i64, message: &str, data: Json| json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message, "data": data } });
    if seen.body.get("id").is_none() {
        return (202, "text/plain", Vec::new(), String::new());
    }
    match speaks {
        Speaks::Modern => {
            let version = params["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str();
            if version != Some("2026-07-28")
                || seen.headers.get("mcp-protocol-version").map(String::as_str) != version
                || seen.headers.get("mcp-method") != Some(&method)
            {
                return jsonrpc(400, error(-32020, "Header mismatch", Json::Null));
            }
            match method.as_str() {
                "tools/list" => {
                    let mut page = tools_page(params["cursor"].as_str());
                    page["resultType"] = json!("complete");
                    jsonrpc(200, result(page))
                }
                "tools/call" => {
                    let name = params["name"].as_str().unwrap_or_default();
                    if seen.headers.get("mcp-name").map(String::as_str) != Some(name) {
                        return jsonrpc(400, error(-32020, "Mcp-Name", Json::Null));
                    }
                    let arguments = &params["arguments"];
                    if name == "in_region"
                        && seen.headers.get("mcp-param-region")
                            != arguments["region"].as_str().map(str::to_owned).as_ref()
                    {
                        return jsonrpc(
                            400,
                            error(-32020, "Header mismatch: Mcp-Param-Region", Json::Null),
                        );
                    }
                    jsonrpc(200, result(tool_answer(name, arguments)))
                }
                _ => jsonrpc(404, error(-32601, "Method not found", Json::Null)),
            }
        }
        Speaks::Legacy => {
            if method == "initialize" {
                assert_eq!(
                    params["protocolVersion"], "2025-11-25",
                    "the newest earlier version is asked for first"
                );
                let mut a = jsonrpc(
                    200,
                    result(
                        json!({ "protocolVersion": "2025-06-18", "capabilities": { "tools": {} }, "serverInfo": { "name": "stand-in", "version": "1" } }),
                    ),
                );
                a.2.push(("mcp-session-id".to_owned(), "sess-1".to_owned()));
                return a;
            }
            if seen.headers.get("mcp-session-id").map(String::as_str) != Some("sess-1")
                || seen.headers.get("mcp-protocol-version").map(String::as_str) != Some("2025-06-18")
            {
                return (400, "application/json", Vec::new(), json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32000, "message": "Bad Request: Server not initialized" } }).to_string());
            }
            let r = match method.as_str() {
                "tools/list" => result(json!({ "tools": tools_page(Some("p2"))["tools"] })),
                "tools/call" => result(tool_answer(
                    params["name"].as_str().unwrap_or_default(),
                    &params["arguments"],
                )),
                _ => error(-32601, "Method not found", Json::Null),
            };
            let progress =
                json!({ "jsonrpc": "2.0", "method": "notifications/progress", "params": { "progress": 1 } });
            (
                200,
                "text/event-stream",
                Vec::new(),
                format!(": hello\n\nevent: message\ndata: {progress}\n\nevent: message\ndata: {r}\n\n"),
            )
        }
        Speaks::OnlyOlder => {
            if params["_meta"]
                .get("io.modelcontextprotocol/protocolVersion")
                .is_some()
            {
                return jsonrpc(
                    400,
                    error(
                        -32022,
                        "Unsupported protocol version",
                        json!({ "supported": ["2025-11-25"], "requested": "2026-07-28" }),
                    ),
                );
            }
            match method.as_str() {
                "initialize" => jsonrpc(
                    200,
                    result(
                        json!({ "protocolVersion": "2025-11-25", "capabilities": { "tools": {} }, "serverInfo": { "name": "older", "version": "1" } }),
                    ),
                ),
                "tools/call"
                    if seen.headers.get("mcp-protocol-version").map(String::as_str) == Some("2025-11-25") =>
                {
                    jsonrpc(
                        200,
                        result(tool_answer(
                            params["name"].as_str().unwrap_or_default(),
                            &params["arguments"],
                        )),
                    )
                }
                _ => jsonrpc(400, error(-32600, "Bad request", Json::Null)),
            }
        }
    }
}

/// A stand-in MCP server on this machine: its address, and every request it was sent.
fn stand_in(speaks: Speaks) -> (String, Log) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&log);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let Some(seen) = read_request(&mut stream) else {
                continue;
            };
            let (status, kind, headers, body) = answer(speaks, &seen);
            kept.lock().unwrap().push(seen);
            let mut head = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n",
                body.len()
            );
            for (k, v) in headers {
                head.push_str(&format!("{k}: {v}\r\n"));
            }
            let _ = stream.write_all(format!("{head}\r\n{body}").as_bytes());
        }
    });
    (url, log)
}

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

fn bearer(token: &str) -> Credential {
    Credential {
        kind: "bearer".to_owned(),
        fields: [("token".to_owned(), token.to_owned())].into_iter().collect(),
    }
}

fn live(allow_private: bool) -> Live {
    let mut kept = BTreeMap::new();
    kept.insert("tools".to_owned(), bearer("s3cret"));
    kept.insert("wrong".to_owned(), bearer("not-it"));
    kept.insert(
        "db".to_owned(),
        Credential {
            kind: "postgres".to_owned(),
            fields: BTreeMap::new(),
        },
    );
    Live::new("127.0.0.1:1", allow_private)
        .unwrap()
        .with_secrets(Arc::new(Kept(kept)))
}

fn workflow(url: &str, step: &str) -> Workflow {
    Workflow::parse(&format!(
        "name: weather\nsteps:\n  - name: ask\n    mcp:\n      url: {url}\n{step}"
    ))
    .unwrap()
}

fn go(workflow: &Workflow, effects: &dyn Effects) -> openduct_flow::RunRecord {
    let event = Event {
        value: Value::parse_json(r#"{"city": "Lisbon"}"#).unwrap(),
        details: Value::Null,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(workflow, &event, effects))
}

fn result(record: &openduct_flow::RunRecord) -> Json {
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    serde_json::from_str(&record.steps[0].result.to_json()).unwrap()
}

fn error(record: &openduct_flow::RunRecord) -> String {
    match &record.outcome {
        Outcome::Failed { error, .. } => error.clone(),
        other => panic!("it should have failed: {other:?}"),
    }
}

const CALL: &str = "      credential: tools\n      tool: get_forecast\n      arguments: { city: '{{ value.city }}', days: 3 }\n";

#[test]
fn a_tool_of_a_current_server_is_called_with_arguments_from_the_run() {
    let (url, log) = stand_in(Speaks::Modern);
    let record = go(&workflow(&url, CALL), &live(true));
    let got = result(&record);
    assert_eq!(
        got["answer"],
        json!({ "city": "Lisbon", "days": 3, "sky": "sunny" })
    );
    assert_eq!(got["text"], "Sunny in Lisbon");
    assert_eq!(got["is_error"], false);
    assert_eq!(got["protocol"], "2026-07-28");
    assert_eq!(record.steps[0].kind, "mcp");
    let seen = log.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "one request: no handshake");
    let request = &seen[0];
    assert_eq!(request.headers["mcp-method"], "tools/call");
    assert_eq!(request.headers["mcp-name"], "get_forecast");
    assert!(request.headers["accept"].contains("text/event-stream"));
    assert_eq!(
        request.body["params"]["arguments"],
        json!({ "city": "Lisbon", "days": 3 })
    );
    assert_eq!(
        request.body["params"]["_meta"]["io.modelcontextprotocol/clientInfo"]["name"],
        "OpenDuct"
    );
}

#[test]
fn a_servers_tools_are_listed_page_after_page_leaving_out_a_tool_that_breaks_the_rules() {
    let (url, _) = stand_in(Speaks::Modern);
    let got = result(&go(&workflow(&url, "      credential: tools\n"), &live(true)));
    let names: Vec<&str> = got["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["get_forecast", "in_region", "fail"]);
    assert_eq!(got["count"], 3);
    assert_eq!(got["tools"][0]["input_schema"]["required"], json!(["city"]));
}

#[test]
fn an_argument_the_tool_wants_as_a_header_too_is_sent_so_after_reading_its_definition() {
    let (url, log) = stand_in(Speaks::Modern);
    let step = "      credential: tools\n      tool: in_region\n      arguments: { region: eu-west }\n";
    let got = result(&go(&workflow(&url, step), &live(true)));
    assert_eq!(got["text"], "done in eu-west");
    let seen = log.lock().unwrap().clone();
    let methods: Vec<&str> = seen.iter().map(|s| s.body["method"].as_str().unwrap()).collect();
    assert_eq!(methods, ["tools/call", "tools/list", "tools/list", "tools/call"]);
    assert_eq!(seen[3].headers["mcp-param-region"], "eu-west");
}

#[test]
fn a_server_of_an_earlier_protocol_is_greeted_kept_to_its_session_and_remembered() {
    let (url, log) = stand_in(Speaks::Legacy);
    let w = workflow(&url, CALL);
    let got = result(&go(&w, &live(true)));
    assert_eq!(got["answer"]["sky"], "sunny", "read from the event stream");
    assert_eq!(got["protocol"], "2025-06-18");
    let seen: Vec<Seen> = log.lock().unwrap().drain(..).collect();
    let shape: Vec<String> = seen
        .iter()
        .map(|s| {
            s.body["method"]
                .as_str()
                .map_or_else(|| s.method.clone(), str::to_owned)
        })
        .collect();
    assert_eq!(
        shape,
        [
            "tools/call",
            "initialize",
            "notifications/initialized",
            "tools/call",
            "DELETE"
        ],
        "the current protocol tried first, then the handshake; the session ended at the end"
    );
    assert_eq!(seen[3].headers["mcp-session-id"], "sess-1");
    assert_eq!(seen[4].headers["mcp-session-id"], "sess-1");
    // The next run goes straight to the handshake.
    result(&go(&w, &live(true)));
    let again: Vec<String> = log
        .lock()
        .unwrap()
        .iter()
        .map(|s| {
            s.body["method"]
                .as_str()
                .map_or_else(|| s.method.clone(), str::to_owned)
        })
        .collect();
    assert_eq!(
        again,
        ["initialize", "notifications/initialized", "tools/call", "DELETE"]
    );
    // Its tools are listed through the session too.
    let listed = result(&go(&workflow(&url, "      credential: tools\n"), &live(true)));
    assert_eq!(listed["tools"][0]["name"], "fail");
}

#[test]
fn a_server_naming_only_earlier_versions_is_spoken_to_in_one_of_them() {
    let (url, log) = stand_in(Speaks::OnlyOlder);
    let got = result(&go(&workflow(&url, CALL), &live(true)));
    assert_eq!(got["protocol"], "2025-11-25");
    assert_eq!(got["answer"]["city"], "Lisbon");
    let methods: Vec<String> = log
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.body["method"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        methods,
        [
            "tools/call",
            "initialize",
            "notifications/initialized",
            "tools/call"
        ]
    );
}

#[test]
fn a_tool_that_answers_with_an_error_fails_the_step_unless_told_not_to() {
    let (url, _) = stand_in(Speaks::Modern);
    let failing = go(
        &workflow(&url, "      credential: tools\n      tool: fail\n"),
        &live(true),
    );
    assert_eq!(error(&failing), "the tool fail failed: no such city");
    let kept = go(
        &workflow(
            &url,
            "      credential: tools\n      tool: fail\n      fail_on_error: false\n",
        ),
        &live(true),
    );
    let got = result(&kept);
    assert_eq!(got["is_error"], true);
    assert_eq!(got["answer"], "no such city");
}

#[test]
fn a_wrong_credential_an_unknown_kind_and_this_machines_own_network_are_refused() {
    let (url, log) = stand_in(Speaks::Modern);
    let wrong = go(
        &workflow(&url, "      credential: wrong\n      tool: get_forecast\n"),
        &live(true),
    );
    assert!(
        error(&wrong).contains("refused the sign-in (401)"),
        "{}",
        error(&wrong)
    );
    let db = go(
        &workflow(&url, "      credential: db\n      tool: get_forecast\n"),
        &live(true),
    );
    assert_eq!(
        error(&db),
        "db is a postgres credential: an mcp step takes a bearer, header or basic one"
    );
    let before = log.lock().unwrap().len();
    let guarded = go(&workflow(&url, CALL), &live(false));
    assert!(
        error(&guarded).contains("--allow-private-network"),
        "{}",
        error(&guarded)
    );
    assert_eq!(log.lock().unwrap().len(), before, "nothing reached it");
}

#[test]
fn a_test_run_sends_nothing_and_shows_the_call_it_would_make_without_its_secret() {
    let (url, log) = stand_in(Speaks::Modern);
    let test = DryRun {
        credentials: [("tools".to_owned(), bearer("s3cret"))].into_iter().collect(),
        ..DryRun::default()
    };
    let got = result(&go(&workflow(&url, CALL), &test));
    assert_eq!(got["test"], "not sent");
    assert_eq!(got["request"]["url"], url);
    assert_eq!(got["request"]["body"]["content"]["method"], "tools/call");
    assert_eq!(
        got["request"]["body"]["content"]["params"]["arguments"]["city"],
        "Lisbon"
    );
    assert!(!got.to_string().contains("s3cret"));
    assert!(log.lock().unwrap().is_empty());
}

#[test]
fn a_preview_lists_for_real_and_calls_only_a_tool_said_to_only_read() {
    let (url, log) = stand_in(Speaks::Modern);
    let preview = Preview::new(Arc::new(live(true)));
    let listed = result(&go(&workflow(&url, "      credential: tools\n"), &preview));
    assert_eq!(listed["count"], 3);
    let shown = result(&go(&workflow(&url, CALL), &preview));
    assert_eq!(shown["test"], "not sent");
    assert!(!shown.to_string().contains("s3cret"), "{shown}");
    let calls = |log: &Log| {
        log.lock()
            .unwrap()
            .iter()
            .filter(|s| s.body["method"] == "tools/call")
            .count()
    };
    assert_eq!(calls(&log), 0);
    let read = result(&go(
        &workflow(&url, &format!("{CALL}      read_only: true\n")),
        &preview,
    ));
    assert_eq!(read["answer"]["sky"], "sunny");
    assert_eq!(calls(&log), 1);
}

#[test]
fn a_step_written_wrong_is_found_before_it_runs() {
    let bad = |step: &str| {
        Workflow::parse(&format!("name: w\nsteps:\n  - name: ask\n    mcp:\n{step}")).unwrap_err()
    };
    assert!(bad("      tool: x\n").contains("url: the MCP server's address"));
    assert!(bad("      url: ftp://example.com\n").contains("an http or https address"));
    assert!(
        bad("      url: https://example.com/mcp\n      arguments: { a: 1 }\n")
            .contains("tool: the tool these arguments are for")
    );
    assert!(
        bad("      url: https://example.com/mcp\n      tool: x\n      arguments: [1]\n")
            .contains("arguments: a record")
    );
    assert!(bad("      url: https://example.com/mcp\n      timeout: 1h\n").contains("at most 5m"));
    assert!(bad("      url: https://example.com/mcp\n      method: GET\n").contains("not a setting of mcp"));
    assert!(bad("      url: https://example.com/mcp\n      read_only: maybe\n").contains("true or false"));
    assert!(
        Workflow::parse(
            "name: w\nsteps:\n  - mcp: { url: '{{ value.server }}', tool: '{{ value.tool }}' }\n"
        )
        .is_ok()
    );
}

#[test]
fn a_workflow_is_offered_as_a_tool_unless_its_trigger_cannot_take_a_call() {
    let w = Workflow::parse(
        "name: Price an item\ntrigger: webhook\ntool:\n  description: The price of an item\n  input:\n    sku: { type: string, required: true }\nsteps:\n  - set: { sku: value.sku }\n",
    )
    .unwrap();
    let tool = w.tool.unwrap();
    assert_eq!(tool.name, "price_an_item");
    assert_eq!(tool.input_schema()["required"], json!(["sku"]));
    assert!(Workflow::parse("name: w\nsteps: []\n").unwrap().tool.is_none());
    let topic = Workflow::parse(
        "name: w\ntrigger: { topic: { name: orders } }\ntool: { description: x }\nsteps: []\n",
    )
    .unwrap_err();
    assert!(topic.contains("not offered as a tool"), "{topic}");
    let wrong = Workflow::parse("name: w\ntool: { name: 'a b', description: x }\nsteps: []\n").unwrap_err();
    assert!(wrong.starts_with("tool: name:"), "{wrong}");
}
