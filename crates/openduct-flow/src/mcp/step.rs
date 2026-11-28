// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `mcp` step: a tool of an MCP server called with arguments worked out from the run - or, without a tool named,
//! the server's tools listed. Its result is what the tool answered.
//!
//! ```yaml
//! - name: weather
//!   mcp:
//!     url: https://tools.example.com/mcp
//!     credential: weather-token     # bearer, header or basic; optional
//!     tool: get_forecast
//!     arguments: { city: "{{ value.city }}", days: 3 }
//! ```

use std::time::Duration;

use openduct_expr::{Env, Value};
use serde_json::{Value as Json, json};

use super::client::{Reached, Server, Session};
use crate::model::Tree;
use crate::run::Effects;
use crate::template::Template;

/// The longest an MCP step waits for each answer: by default and at most.
pub(crate) const MCP_TIME: Duration = Duration::from_secs(30);
pub(crate) const MAX_MCP_TIME: Duration = Duration::from_secs(300);

/// An `mcp` step, as read.
#[derive(Debug, Clone)]
pub struct McpStep {
    /// The server's MCP address.
    pub url: Template,
    /// The credential it signs in with (bearer, header or basic), by name.
    pub credential: Option<String>,
    /// The tool to call; none to list the server's tools.
    pub tool: Option<Template>,
    /// The arguments: a record, its texts templates.
    pub arguments: Option<Tree>,
    pub timeout: Duration,
    /// The tool only reads: a preview calls it (otherwise a preview shows the call, not sending it).
    pub read_only: bool,
    /// A tool's answer that is an error fails the step (unless false: the answer is the result either way).
    pub fail_on_error: bool,
}

/// The step's result: the tool's answer, the server's tools, or (a test run, a preview) what would be sent.
pub(crate) async fn run(effects: &dyn Effects, step: &McpStep, env: &Env<'_>) -> Result<Value, String> {
    let url = step.url.render(env).map_err(|e| format!("url: {e}"))?;
    let credential = match &step.credential {
        None => None,
        Some(name) => {
            let c = effects.credential(name)?;
            if !matches!(c.kind.as_str(), "bearer" | "header" | "basic" | "test") {
                return Err(format!(
                    "{name} is a {} credential: an mcp step takes a bearer, header or basic one",
                    c.kind
                ));
            }
            Some(c)
        }
    };
    let server = Server {
        url,
        credential,
        timeout: step.timeout,
    };
    let mut session = Session::new(effects, server);
    let done = match &step.tool {
        None => list(&mut session).await,
        Some(tool) => call(&mut session, step, tool, env).await,
    };
    session.close().await;
    done
}

async fn list(session: &mut Session<'_>) -> Result<Value, String> {
    let tools = match session.tools().await? {
        Reached::NotSent(shown) => return Ok(shown),
        Reached::Answered(tools) => tools,
    };
    let shown: Vec<Json> = tools
        .iter()
        .map(|t| {
            json!({
                "name": t.name, "title": t.title, "description": t.description, "input_schema": t.input_schema,
            })
        })
        .collect();
    Ok(Value::from_json(&json!({
        "tools": shown, "count": shown.len(), "protocol": session.protocol(),
    })))
}

async fn call(
    session: &mut Session<'_>,
    step: &McpStep,
    tool: &Template,
    env: &Env<'_>,
) -> Result<Value, String> {
    let name = tool.render(env).map_err(|e| format!("tool: {e}"))?;
    let name = name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err("tool: the name of the tool to call".to_owned());
    }
    let arguments = match &step.arguments {
        None => json!({}),
        Some(tree) => {
            let value = crate::run::tree(tree, env).map_err(|e| format!("arguments: {e}"))?;
            match serde_json::from_str::<Json>(&value.to_json()).unwrap_or(Json::Null) {
                Json::Null => json!({}),
                record @ Json::Object(_) => record,
                _ => return Err("arguments: a record of named values".to_owned()),
            }
        }
    };
    let answer = match session.call(name, &arguments, step.read_only).await? {
        Reached::NotSent(shown) => return Ok(shown),
        Reached::Answered(answer) => answer,
    };
    if answer.is_error && step.fail_on_error {
        let said: String = answer.text.chars().take(300).collect();
        return Err(if said.trim().is_empty() {
            format!("the tool {name} failed, saying nothing more")
        } else {
            format!("the tool {name} failed: {said}")
        });
    }
    Ok(Value::from_json(&json!({
        "tool": name,
        "answer": answer.value(),
        "text": answer.text,
        "content": answer.content,
        "is_error": answer.is_error,
        "protocol": session.protocol(),
    })))
}
