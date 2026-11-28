// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workspace's MCP server, at `/mcp/<workspace>`: MCP's Streamable HTTP transport, one JSON-RPC message per POST.
//!
//! It speaks the current protocol - every request carrying its version, who asks and what it may do in `_meta`, the
//! method and the tool's name mirrored in headers, which must agree - and, for assistants that have not moved on,
//! the earlier versions, which begin with `initialize`. It keeps no session either way: each request stands alone.
//! Before any of it, the request must come with a token the workspace issued (`Authorization: Bearer`), within the
//! token's requests a minute; a browser page of another site is refused, so a name pointed at this machine cannot
//! reach a workspace's tools.

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method, Response, StatusCode};
use openduct_flow::mcp::wire::{self, LEGACY, MODERN};
use serde_json::{Value, json};

use super::calls::{self, callable, listed};
use super::tokens::{McpToken, hash, well_formed};
use crate::State;
use crate::http::{Answer, Call, host_name};

/// What the server tells a model about itself.
const INSTRUCTIONS: &str = "Each tool runs one of this workspace's workflows and answers with its result.";

fn header<'c>(call: &'c Call, name: &str) -> Option<&'c str> {
    call.headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// An answer of JSON, with these headers too.
fn answer(status: StatusCode, body: &Value, extra: &[(&'static str, String)]) -> Answer {
    let mut response = crate::http::json(status, body);
    for (name, value) in extra {
        if let Ok(v) = HeaderValue::from_str(value) {
            response.headers_mut().insert(HeaderName::from_static(name), v);
        }
    }
    response
}

/// A JSON-RPC error, as the transport says to answer it.
fn refuse(status: StatusCode, id: &Value, code: i64, message: &str, data: Option<Value>) -> Answer {
    answer(status, &wire::error(id, code, message, data), &[])
}

/// `202 Accepted`, no body: a notification, or a response, taken.
fn accepted() -> Answer {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = StatusCode::ACCEPTED;
    response
}

pub(crate) async fn endpoint(state: &State, call: &Call, workspace: &str) -> Answer {
    if call.method != Method::POST {
        // Neither a stream of the server's own messages (GET) nor a session to end (DELETE): each request stands alone.
        let mut refused = refuse(
            StatusCode::METHOD_NOT_ALLOWED,
            &Value::Null,
            wire::INVALID_REQUEST,
            "this MCP server takes each message as a POST",
            None,
        );
        refused
            .headers_mut()
            .insert(hyper::header::ALLOW, HeaderValue::from_static("POST"));
        return refused;
    }
    if let Some(origin) = header(call, "origin") {
        let host = origin.split_once("://").map_or(origin, |(_, rest)| rest);
        if !state.hosts.allows(&host_name(host)) {
            return refuse(
                StatusCode::FORBIDDEN,
                &Value::Null,
                wire::INVALID_REQUEST,
                "a request from a page of another site",
                None,
            );
        }
    }
    let token = match signed(state, call, workspace).await {
        Ok(t) => t,
        Err(answer) => return answer,
    };
    if let Err(wait) = state.mcp.allow(&token) {
        return answer(
            StatusCode::TOO_MANY_REQUESTS,
            &wire::error(
                &Value::Null,
                wire::TOO_MANY_CALLS,
                &format!(
                    "this token makes at most {} requests a minute: wait {wait}s",
                    token.per_minute
                ),
                None,
            ),
            &[("retry-after", wait.to_string())],
        );
    }
    let message: Value = match serde_json::from_slice(&call.raw) {
        Ok(m) => m,
        Err(_) => {
            return refuse(
                StatusCode::BAD_REQUEST,
                &Value::Null,
                wire::PARSE_ERROR,
                "the body is not JSON",
                None,
            );
        }
    };
    let Some(fields) = message.as_object() else {
        return refuse(
            StatusCode::BAD_REQUEST,
            &Value::Null,
            wire::INVALID_REQUEST,
            "one JSON-RPC message per request",
            None,
        );
    };
    let Some(method) = fields.get("method").and_then(Value::as_str) else {
        // A response to a request this server never sends: taken, and nothing done.
        return accepted();
    };
    let Some(id) = fields.get("id") else {
        // A notification (initialized, cancelled): nothing to answer.
        return accepted();
    };
    if !(id.is_string() || id.is_i64() || id.is_u64()) {
        return refuse(
            StatusCode::BAD_REQUEST,
            &Value::Null,
            wire::INVALID_REQUEST,
            "a request's id is a string or an integer",
            None,
        );
    }
    let params = fields.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return refuse(StatusCode::OK, id, wire::INVALID_PARAMS, "params: a record", None);
    }
    if method == "initialize" {
        return initialize(id, &params);
    }
    match params.get("_meta").and_then(|m| m.get(wire::META_VERSION)) {
        Some(version) => modern(state, call, &token, id, method, &params, version).await,
        None => legacy(state, call, &token, id, method, &params).await,
    }
}

/// The token the request signs in with, if it is one of this workspace's and not revoked; the refusal if not.
async fn signed(state: &State, call: &Call, workspace: &str) -> Result<McpToken, Answer> {
    let refused = || {
        answer(
            StatusCode::UNAUTHORIZED,
            &wire::error(
                &Value::Null,
                wire::INVALID_REQUEST,
                "sign in with a token this workspace issued: Authorization: Bearer <token>",
                None,
            ),
            &[("www-authenticate", "Bearer realm=\"OpenDuct MCP\"".to_owned())],
        )
    };
    if !state.mcp.may_try(call.peer) {
        return Err(answer(
            StatusCode::TOO_MANY_REQUESTS,
            &wire::error(
                &Value::Null,
                wire::TOO_MANY_CALLS,
                "too many wrong tokens: wait a minute",
                None,
            ),
            &[("retry-after", "60".to_owned())],
        ));
    }
    let given = header(call, "authorization")
        .and_then(|a| a.strip_prefix("Bearer ").or_else(|| a.strip_prefix("bearer ")))
        .map(str::trim)
        .unwrap_or_default();
    if !well_formed(given) {
        state.mcp.failures.failed(call.peer);
        return Err(refused());
    }
    let key = hash(given);
    if state.mcp.stale() {
        let _ = state.model.mcp_tokens.refresh().await;
    }
    let mut found = state.model.mcp_tokens.get(&key).await;
    if found.is_none() {
        // Made on another node a moment ago, perhaps.
        let _ = state.model.mcp_tokens.refresh().await;
        found = state.model.mcp_tokens.get(&key).await;
    }
    let live = state
        .model
        .workspaces
        .get(workspace)
        .await
        .is_some_and(|w| w.deleted.is_none());
    match found {
        Some(t) if t.workspace == workspace && t.revoked.is_none() && live => {
            state.mcp.failures.succeeded(call.peer);
            Ok(t)
        }
        _ => {
            state.mcp.failures.failed(call.peer);
            Err(refused())
        }
    }
}

/// The server, as MCP names it.
fn capabilities() -> Value {
    json!({ "tools": { "listChanged": false } })
}

/// The earlier protocol's handshake: the version asked for if it is one of the earlier ones this server speaks, else
/// the newest of them. No session is given: each request after stands alone.
fn initialize(id: &Value, params: &Value) -> Answer {
    let asked = params["protocolVersion"].as_str().unwrap_or_default();
    let version = if LEGACY.contains(&asked) {
        asked
    } else {
        LEGACY.first().copied().unwrap_or(wire::UNNAMED)
    };
    answer(
        StatusCode::OK,
        &wire::result(
            id,
            json!({
                "protocolVersion": version,
                "capabilities": capabilities(),
                "serverInfo": wire::implementation(),
                "instructions": INSTRUCTIONS,
            }),
        ),
        &[],
    )
}

/// A request of the current protocol: its headers checked against its body, then answered.
async fn modern(
    state: &State,
    call: &Call,
    token: &McpToken,
    id: &Value,
    method: &str,
    params: &Value,
    version: &Value,
) -> Answer {
    let mismatch = |what: &str| {
        refuse(
            StatusCode::BAD_REQUEST,
            id,
            wire::HEADER_MISMATCH,
            &format!("Header mismatch: {what}"),
            None,
        )
    };
    let version = version.as_str().unwrap_or_default();
    if header(call, "mcp-protocol-version") != Some(version) {
        return mismatch("MCP-Protocol-Version is not the version in _meta");
    }
    if version != MODERN {
        return refuse(
            StatusCode::BAD_REQUEST,
            id,
            wire::UNSUPPORTED_VERSION,
            "Unsupported protocol version",
            Some(json!({ "supported": wire::versions(), "requested": version })),
        );
    }
    if header(call, "mcp-method") != Some(method) {
        return mismatch("Mcp-Method is not the request's method");
    }
    let named = match method {
        "tools/call" | "prompts/get" => params["name"].as_str(),
        "resources/read" => params["uri"].as_str(),
        _ => None,
    };
    if let Some(named) = named
        && header(call, "mcp-name").and_then(wire::header_text).as_deref() != Some(named)
    {
        return mismatch("Mcp-Name is not the name in the request");
    }
    let meta = |key: &str| params.get("_meta").and_then(|m| m.get(key));
    if !meta(wire::META_CAPABILITIES).is_some_and(Value::is_object) {
        return refuse(
            StatusCode::BAD_REQUEST,
            id,
            wire::INVALID_PARAMS,
            "_meta: io.modelcontextprotocol/clientCapabilities is missing",
            None,
        );
    }
    let client: String = meta(wire::META_CLIENT)
        .and_then(|c| c.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .take(100)
        .collect();
    let done = match method {
        "server/discover" => Ok(json!({
            "supportedVersions": wire::versions(),
            "capabilities": capabilities(),
            "instructions": INSTRUCTIONS,
        })),
        "subscriptions/listen" => return listen(id),
        _ => answered(state, token, method, params, &client).await,
    };
    match done {
        Ok(mut result) => {
            if let Some(fields) = result.as_object_mut() {
                fields.insert("resultType".to_owned(), json!("complete"));
                let mut meta = serde_json::Map::new();
                meta.insert(wire::META_SERVER.to_owned(), wire::implementation());
                fields.insert("_meta".to_owned(), Value::Object(meta));
            }
            answer(StatusCode::OK, &wire::result(id, result), &[])
        }
        Err((code, message)) => {
            let status = if code == wire::METHOD_NOT_FOUND {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            };
            refuse(status, id, code, &message, None)
        }
    }
}

/// A request of an earlier protocol, after its handshake: its version from the header (the oldest's when none).
async fn legacy(
    state: &State,
    call: &Call,
    token: &McpToken,
    id: &Value,
    method: &str,
    params: &Value,
) -> Answer {
    let version = header(call, "mcp-protocol-version").unwrap_or(wire::UNNAMED);
    if !LEGACY.contains(&version) {
        let why = if version == MODERN {
            format!("a request of version {MODERN} carries its version in _meta")
        } else {
            "Unsupported protocol version".to_owned()
        };
        let (code, data) = if version == MODERN {
            (wire::INVALID_PARAMS, None)
        } else {
            (
                wire::UNSUPPORTED_VERSION,
                Some(json!({ "supported": wire::versions(), "requested": version })),
            )
        };
        return refuse(StatusCode::BAD_REQUEST, id, code, &why, data);
    }
    match answered(state, token, method, params, "").await {
        Ok(result) => answer(StatusCode::OK, &wire::result(id, result), &[]),
        Err((code, message)) => refuse(StatusCode::OK, id, code, &message, None),
    }
}

/// What both protocols answer alike: `ping`, `tools/list`, `tools/call`.
async fn answered(
    state: &State,
    token: &McpToken,
    method: &str,
    params: &Value,
    client: &str,
) -> Result<Value, (i64, String)> {
    match method {
        "ping" => Ok(json!({})),
        "tools/list" => {
            if params.get("cursor").is_some_and(|c| !c.is_null()) {
                return Err((
                    wire::INVALID_PARAMS,
                    "no such cursor: every tool is on the first page".to_owned(),
                ));
            }
            let tools: Vec<Value> = callable(state, token).await.iter().map(listed).collect();
            Ok(json!({ "tools": tools }))
        }
        "tools/call" => {
            let Some(name) = params["name"].as_str() else {
                return Err((wire::INVALID_PARAMS, "params.name: the tool to call".to_owned()));
            };
            let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
            if !(arguments.is_object() || arguments.is_null()) {
                return Err((wire::INVALID_PARAMS, "params.arguments: a record".to_owned()));
            }
            let tools = callable(state, token).await;
            let Some(tool) = tools.iter().find(|o| o.tool.name == name) else {
                let shown: String = name.chars().take(128).collect();
                return Err((wire::INVALID_PARAMS, format!("Unknown tool: {shown}")));
            };
            Ok(calls::call(state, tool, &arguments, token, client).await)
        }
        other => {
            let shown: String = other.chars().take(64).collect();
            Err((wire::METHOD_NOT_FOUND, format!("Method not found: {shown}")))
        }
    }
}

/// `subscriptions/listen`: this server sends no notifications, so the stream says it took none and ends at once.
fn listen(id: &Value) -> Answer {
    let subscription = json!({ "io.modelcontextprotocol/subscriptionId": id });
    let acknowledged = json!({
        "jsonrpc": "2.0", "method": "notifications/subscriptions/acknowledged",
        "params": { "_meta": subscription, "notifications": {} },
    });
    let ended = wire::result(id, json!({ "resultType": "complete", "_meta": subscription }));
    let body = format!("event: message\ndata: {acknowledged}\n\nevent: message\ndata: {ended}\n\n");
    let mut response = Response::new(Full::new(Bytes::from(body)));
    let headers = response.headers_mut();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(hyper::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}
