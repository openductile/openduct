// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MCP in the studio: each workspace's MCP server (`/mcp/<workspace>`, [`serve`]), which offers AI assistants the
//! workspace's workflows marked as tools; the tokens they sign in with ([`tokens`]); and the studio's own requests -
//! the server's address, its tools and tokens on the workspace's page, and the tools of another MCP server listed
//! for the builder's MCP step.

mod calls;
mod serve;
mod tokens;

use hyper::{Method, StatusCode};
use openduct_flow::mcp::{Reached, Server, Session};
use serde_json::{Value, json};

pub(crate) use tokens::{Limits, MCP_TOKENS, McpToken};

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, SCHEMA, new_id, now};

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (_, ["mcp", workspace]) => serve::endpoint(state, call, workspace).await,
        (&Method::GET, ["api", "mcp"]) => server(state, call).await,
        (&Method::POST, ["api", "mcp", "tokens"]) => issue(state, call).await,
        (&Method::DELETE, ["api", "mcp", "tokens", id]) => revoke(state, call, id).await,
        (&Method::POST, ["api", "mcp", "discover"]) => discover(state, call).await,
        _ => return None,
    })
}

fn shown(t: &McpToken) -> Value {
    json!({
        "id": t.id, "name": t.name, "hint": t.hint, "tools": t.tools, "per_minute": t.per_minute,
        "created": t.created, "by": t.by,
    })
}

async fn tokens_of(state: &State, workspace: &str) -> Vec<McpToken> {
    let _ = state.model.mcp_tokens.refresh().await;
    let mut out: Vec<McpToken> = state
        .model
        .mcp_tokens
        .all()
        .await
        .into_iter()
        .map(|(_, t)| t)
        .filter(|t| t.workspace == workspace && t.revoked.is_none())
        .collect();
    out.sort_by_key(|t| t.created);
    out
}

/// The workspace's MCP server: its address, the tools it offers (and those it does not, saying why), its tokens.
async fn server(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let tools: Vec<Value> = calls::offered(state, &access.workspace.id)
        .await
        .iter()
        .map(|o| {
            json!({
                "workflow": o.record.id, "workflow_name": o.record.name, "name": o.tool.name,
                "description": o.tool.description, "inputs": o.tool.inputs.len(), "active": o.active,
                "taken_by": o.taken_by,
            })
        })
        .collect();
    let tokens: Vec<Value> = tokens_of(state, &access.workspace.id)
        .await
        .iter()
        .map(shown)
        .collect();
    json(
        StatusCode::OK,
        &json!({
            "path": format!("/mcp/{}", access.workspace.id),
            "versions": openduct_flow::mcp::wire::versions(),
            "tools": tools,
            "tokens": tokens,
            "manage": access.role == Role::Admin,
        }),
    )
}

/// A new token: shown this once, kept only as its hash.
async fn issue(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let text = |name: &str| call.body.get(name).and_then(Value::as_str).unwrap_or_default();
    let name = text("name").trim().to_owned();
    if name.is_empty() || name.chars().count() > 100 {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "name: what the token is for, as the assistant that uses it (1 to 100 characters)",
        );
    }
    let per_minute = match call.body.get("per_minute") {
        None | Some(Value::Null) => tokens::PER_MINUTE,
        Some(n) => match n.as_u64().and_then(|n| u32::try_from(n).ok()) {
            Some(n) if (1..=tokens::MAX_PER_MINUTE).contains(&n) => n,
            _ => {
                return problem(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("per_minute: requests a minute, 1 to {}", tokens::MAX_PER_MINUTE),
                );
            }
        },
    };
    let tools = match call.body.get("tools") {
        None | Some(Value::Null) => None,
        Some(Value::Array(ids)) => {
            let ours: Vec<String> = calls::offered(state, &access.workspace.id)
                .await
                .into_iter()
                .map(|o| o.record.id)
                .collect();
            let mut chosen = Vec::new();
            for id in ids {
                match id.as_str() {
                    Some(id) if ours.iter().any(|o| o == id) => {
                        if !chosen.iter().any(|c| c == id) {
                            chosen.push(id.to_owned());
                        }
                    }
                    _ => {
                        return problem(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            "tools: workflows of this workspace offered as tools, by id",
                        );
                    }
                }
            }
            if chosen.is_empty() {
                return problem(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "tools: at least one tool, or none given for every tool",
                );
            }
            Some(chosen)
        }
        Some(_) => return problem(StatusCode::UNPROCESSABLE_ENTITY, "tools: a list of workflow ids"),
    };
    if tokens_of(state, &access.workspace.id).await.len() >= tokens::MAX_TOKENS {
        return problem(
            StatusCode::CONFLICT,
            &format!(
                "a workspace has at most {} tokens: revoke one first",
                tokens::MAX_TOKENS
            ),
        );
    }
    let token = match tokens::new_token() {
        Ok(t) => t,
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    let record = McpToken {
        schema: SCHEMA,
        id: new_id(),
        workspace: access.workspace.id.clone(),
        name,
        hash: tokens::hash(&token),
        hint: token
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect(),
        tools,
        per_minute,
        created: now(),
        by: access.user.email.clone(),
        revoked: None,
    };
    if let Err(e) = state.model.mcp_tokens.put(&record.hash, &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    let mut body = shown(&record);
    if let Some(fields) = body.as_object_mut() {
        fields.insert("token".to_owned(), json!(token));
    }
    json(StatusCode::CREATED, &body)
}

async fn revoke(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(mut record) = tokens_of(state, &access.workspace.id)
        .await
        .into_iter()
        .find(|t| t.id == id)
    else {
        return problem(StatusCode::NOT_FOUND, "no such token");
    };
    record.revoked = Some(now());
    if let Err(e) = state.model.mcp_tokens.put(&record.hash.clone(), &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    json(StatusCode::OK, &json!({ "revoked": id }))
}

/// The tools of another MCP server, for the builder's MCP step: reached as a run of the workspace reaches it - with
/// its credential, and not on this machine's own network unless allowed.
async fn discover(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let text = |name: &str| call.body.get(name).and_then(Value::as_str).unwrap_or_default();
    let url = text("url").trim().to_owned();
    if !(url.starts_with("https://") || url.starts_with("http://")) || url.len() > 2048 {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "url: the MCP server's address, as https://tools.example.com/mcp",
        );
    }
    let effects = match state.effects_for(&access.workspace.id) {
        Ok(e) => e,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let credential = match Some(text("credential")).filter(|c| !c.trim().is_empty()) {
        None => None,
        Some(name) => match effects.credential(name) {
            Ok(c) if matches!(c.kind.as_str(), "bearer" | "header" | "basic") => Some(c),
            Ok(c) => {
                return problem(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!(
                        "{name} is a {} credential: an MCP server takes a bearer, header or basic one",
                        c.kind
                    ),
                );
            }
            Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
        },
    };
    let server = Server {
        url,
        credential,
        timeout: std::time::Duration::from_secs(15),
    };
    let mut session = Session::new(effects.as_ref(), server);
    let listed = session.tools().await;
    let protocol = session.protocol().map(str::to_owned);
    session.close().await;
    match listed {
        Ok(Reached::Answered(tools)) => {
            let tools: Vec<Value> = tools
                .iter()
                .map(|t| json!({ "name": t.name, "title": t.title, "description": t.description, "input_schema": t.input_schema }))
                .collect();
            json(StatusCode::OK, &json!({ "tools": tools, "protocol": protocol }))
        }
        Ok(Reached::NotSent(_)) => problem(StatusCode::SERVICE_UNAVAILABLE, "the server was not asked"),
        Err(e) => problem(StatusCode::BAD_GATEWAY, &e),
    }
}
