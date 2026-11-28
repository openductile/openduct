// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workspace's tools - its workflows offered as tools - and a call of one: the arguments checked, the workflow's
//! run queued as any other run is (so it is in the workflow's history, and a failed one goes to CatchErr), and its
//! answer waited for, up to the tool's time.

use std::time::{Duration, Instant};

use openduct_flow::Tool;
use openduct_flow::runner::{self, Topics};
use serde_json::{Value, json};

use super::tokens::McpToken;
use crate::State;
use crate::history::{end_of, tail};
use crate::model::WorkflowRecord;
use crate::workflows::{definition_of, now_ms, queued};

/// The largest answer a call gives, in bytes as text.
pub(crate) const MAX_ANSWER: usize = 1024 * 1024;
/// The most tools a workspace offers.
const MAX_OFFERED: usize = 200;

/// A workflow offered as a tool.
#[derive(Debug, Clone)]
pub(crate) struct Offered {
    pub record: WorkflowRecord,
    pub tool: Tool,
    /// Whether it is on: only a workflow that is on is called.
    pub active: bool,
    /// The workflow whose tool already has this name, when one does: this one is then not offered.
    pub taken_by: Option<String>,
}

/// Every workflow of the workspace offered as a tool, in the order they were made; a name used twice is the first's.
pub(crate) async fn offered(state: &State, workspace: &str) -> Vec<Offered> {
    let _ = state.model.workflows.refresh().await;
    let mut records: Vec<WorkflowRecord> = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .filter(|w| w.workspace == workspace && w.deleted.is_none())
        .collect();
    records.sort_by(|a, b| (a.created, &a.id).cmp(&(b.created, &b.id)));
    let mut out: Vec<Offered> = Vec::new();
    for record in records {
        let definition = definition_of(state, &record).await;
        let Ok(workflow) = crate::custom_apps::workflow(&record.workspace, &definition, Some(&record.id))
        else {
            continue;
        };
        let Some(tool) = workflow.tool else { continue };
        let taken_by = out
            .iter()
            .find(|o| o.taken_by.is_none() && o.tool.name == tool.name)
            .map(|o| o.record.name.clone());
        out.push(Offered {
            active: record.active,
            record,
            tool,
            taken_by,
        });
        if out.len() >= MAX_OFFERED {
            break;
        }
    }
    out
}

/// The tools a token lists and calls: those of workflows that are on, within its reach, named first.
pub(crate) async fn callable(state: &State, token: &McpToken) -> Vec<Offered> {
    offered(state, &token.workspace)
        .await
        .into_iter()
        .filter(|o| o.active && o.taken_by.is_none() && token.reaches(&o.record.id))
        .collect()
}

/// A tool as `tools/list` shows it.
pub(crate) fn listed(o: &Offered) -> Value {
    json!({
        "name": o.tool.name,
        "title": o.record.name,
        "description": o.tool.description,
        "inputSchema": o.tool.input_schema(),
    })
}

/// A tool's answer as MCP gives it: its text, and the record itself when it is one.
fn answer(value: &Value) -> Value {
    let text = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if text.len() > MAX_ANSWER {
        return failed(&format!(
            "the answer is larger than {} KiB: make the workflow's last step answer less",
            MAX_ANSWER / 1024
        ));
    }
    let mut out = json!({ "content": [{ "type": "text", "text": text }], "isError": false });
    if value.is_object()
        && let Some(fields) = out.as_object_mut()
    {
        fields.insert("structuredContent".to_owned(), value.clone());
    }
    out
}

/// A tool's answer that says it failed, and why: what the model reads to try again differently.
pub(crate) fn failed(why: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": why }], "isError": true })
}

/// What a finished run answers: its last respond step's body, or else its last step's result.
fn answer_of(kept: &Value) -> Value {
    match kept["status"].as_str() {
        Some("failed") => {
            let why: String = kept["error"]
                .as_str()
                .unwrap_or("no reason given")
                .chars()
                .take(1000)
                .collect();
            return failed(&format!(
                "the workflow failed at {}: {why}",
                kept["at"].as_str().unwrap_or("a step")
            ));
        }
        Some("skipped") => {
            return json!({
                "content": [{ "type": "text", "text": format!("nothing to do: {} held it back", kept["at"].as_str().unwrap_or("a step")) }],
                "isError": false,
            });
        }
        _ => {}
    }
    let steps = kept["steps"].as_array();
    let given = steps
        .and_then(|s| {
            s.iter()
                .rev()
                .find(|s| s["kind"] == "respond" && s["error"].is_null())
        })
        .map(|s| openduct_flow::mcp::wire::at(s, "/result/body").clone())
        .or_else(|| steps.and_then(|s| s.last()).map(|s| s["result"].clone()))
        .unwrap_or(Value::Null);
    answer(&given)
}

/// Runs the tool's workflow on `arguments` and waits for its answer, up to the tool's time.
pub(crate) async fn call(
    state: &State,
    o: &Offered,
    arguments: &Value,
    token: &McpToken,
    client: &str,
) -> Value {
    if let Err(why) = o.tool.check(arguments) {
        return failed(&format!("the arguments do not fit: {why}"));
    }
    let Some(_running) = state.mcp.start(&token.workspace) else {
        return failed("too many calls are running in this workspace at once: try again in a moment");
    };
    let value = match arguments {
        Value::Null => json!({}),
        other => other.clone(),
    };
    let mut details = json!({
        "trigger": "tool", "tool": o.tool.name, "token": token.name, "at": runner::iso(now_ms()),
    });
    if !client.is_empty()
        && let Some(fields) = details.as_object_mut()
    {
        fields.insert("client".to_owned(), json!(client));
    }
    match run_and_wait(state, &o.record.id, &value, &details, o.tool.timeout).await {
        Ok((_, Some(kept))) => answer_of(&kept),
        Ok((run, None)) => failed(&format!(
            "the workflow did not finish within {}s: its run ({run}) goes on, and its history shows how it ends",
            o.tool.timeout.as_secs()
        )),
        Err(e) => {
            tracing::warn!(
                workflow = o.record.id,
                error = e,
                "an MCP call could not run its workflow"
            );
            failed("the workflow could not be started just now: try again in a moment")
        }
    }
}

/// Queues the run and reads the workflow's history until it is there: the run's id, and its record if it came in time.
async fn run_and_wait(
    state: &State,
    id: &str,
    value: &Value,
    details: &Value,
    within: Duration,
) -> Result<(String, Option<Value>), String> {
    let runs = Topics::of(id).runs;
    let (workflow, topic) = (id.to_owned(), runs.clone());
    // Where the runs end now: this call's run comes after.
    let from = state
        .with_client(|client| {
            Box::pin(async move {
                runner::prepare(client, &workflow).await?;
                end_of(client, &topic).await
            })
        })
        .await?;
    let run = queued(state, id, value, details).await?;
    let deadline = Instant::now() + within;
    let mut next = from;
    loop {
        let topic = runs.clone();
        let found = state
            .with_client(|client| Box::pin(async move { tail(client, &topic, i64::MAX / 4, next).await }))
            .await
            .unwrap_or_default();
        for r in found {
            next = next.max(r.offset + 1);
            if r.key.as_deref() == Some(run.as_bytes()) {
                let kept = r
                    .value
                    .as_ref()
                    .and_then(|v| serde_json::from_slice(v).ok())
                    .unwrap_or(Value::Null);
                return Ok((run, Some(kept)));
            }
        }
        if Instant::now() >= deadline {
            return Ok((run, None));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_answers_with_its_respond_step_its_last_step_or_why_it_failed() {
        let responded = json!({ "status": "done", "steps": [
            { "name": "a", "kind": "respond", "result": { "status": 200, "body": { "price": 12 } }, "error": null },
            { "name": "b", "kind": "set", "result": { "other": 1 }, "error": null },
        ]});
        let got = answer_of(&responded);
        assert_eq!(got["structuredContent"], json!({ "price": 12 }));
        assert_eq!(got["content"][0]["text"], r#"{"price":12}"#);
        let last = answer_of(
            &json!({ "status": "done", "steps": [{ "name": "b", "kind": "set", "result": "hello", "error": null }] }),
        );
        assert_eq!(last["content"][0]["text"], "hello");
        assert!(last.get("structuredContent").is_none());
        let bad = answer_of(
            &json!({ "status": "failed", "at": "price", "error": "no price for NOPE", "steps": [] }),
        );
        assert_eq!(bad["isError"], true);
        assert_eq!(
            bad["content"][0]["text"],
            "the workflow failed at price: no price for NOPE"
        );
        let skipped = answer_of(&json!({ "status": "skipped", "at": "where_2", "steps": [] }));
        assert_eq!(skipped["isError"], false);
        let big = answer(&json!("x".repeat(MAX_ANSWER + 1)));
        assert_eq!(big["isError"], true);
    }
}
