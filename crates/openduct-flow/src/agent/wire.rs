// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An agent's conversation on the wire. It is kept in the chat completions API's form - `system`, `user`,
//! `assistant` (with the `tool_calls` it made) and `tool` (a call's result) messages - and each request is the `ai`
//! step's own ([`crate::ai::chat_request`]: the credential's address, key, headers and model), the conversation and
//! the tools put in it. For a credential whose `api` is `messages`, the conversation and the tools are translated
//! into that shape - the system messages as its `system`, each call as a `tool_use` block, each result as a
//! `tool_result` block of a user turn - and its answer is translated back.

use std::collections::BTreeMap;
use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Value as Json, json};

use crate::apps::{Credential, put};
use crate::run::HttpRequest;

use super::ToolSpec;

/// What one request asks.
pub(crate) struct Asked<'a> {
    pub credential: &'a Credential,
    /// The model named by the step; empty: the credential's.
    pub model: &'a str,
    pub max_tokens: u64,
    pub temperature: Option<f64>,
    pub conversation: &'a [Json],
    pub tools: &'a [ToolSpec],
    /// No tool may be called this time: the model is to answer.
    pub answer_now: bool,
    /// The answer is to be JSON.
    pub json: bool,
    pub timeout: Duration,
}

/// A tool call the model made: its id (to give its result back by), the tool's name, and its arguments as JSON text.
#[derive(Debug, Clone)]
pub(crate) struct Call {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// What the model answered.
#[derive(Debug, Clone)]
pub(crate) struct Reply {
    pub text: String,
    pub calls: Vec<Call>,
    pub finish: String,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub model: Option<String>,
}

/// Whether the credential's API takes the `messages` shape.
pub(crate) fn messages_api(credential: &Credential) -> bool {
    credential
        .fields
        .get("api")
        .is_some_and(|a| a.trim() == "messages")
}

/// The request for one turn.
pub(crate) fn request(a: &Asked<'_>) -> Result<HttpRequest, String> {
    let mut fields: BTreeMap<String, Value> = BTreeMap::new();
    if !a.model.trim().is_empty() {
        fields.insert("model".to_owned(), Value::text(a.model));
    }
    fields.insert("max_tokens".to_owned(), Value::from_json(&json!(a.max_tokens)));
    if let Some(t) = a.temperature {
        fields.insert("temperature".to_owned(), Value::from_json(&json!(t)));
    }
    // The ai step's request, its one message replaced by the conversation below.
    fields.insert("prompt".to_owned(), Value::text("."));
    let mut request = crate::ai::chat_request(a.credential, &fields)?;
    let mut body: Json = request
        .body
        .as_ref()
        .and_then(|(_, bytes)| serde_json::from_slice(bytes).ok())
        .ok_or("the model's request has no body")?;
    if messages_api(a.credential) {
        let (system, turns) = to_messages(a.conversation);
        put(&mut body, "messages", Json::Array(turns));
        if let Some(fields) = body.as_object_mut() {
            fields.remove("system");
        }
        if !system.is_empty() {
            put(&mut body, "system", json!(system.join("\n\n")));
        }
        if !a.tools.is_empty() {
            let tools: Vec<Json> = a
                .tools
                .iter()
                .map(
                    |t| json!({ "name": t.name, "description": t.description, "input_schema": t.parameters }),
                )
                .collect();
            put(&mut body, "tools", Json::Array(tools));
            if a.answer_now {
                put(&mut body, "tool_choice", json!({ "type": "none" }));
            }
        }
    } else {
        put(&mut body, "messages", Json::Array(to_chat(a.conversation)));
        if !a.tools.is_empty() {
            let tools: Vec<Json> = a
                .tools
                .iter()
                .map(|t| {
                    json!({ "type": "function", "function": {
                        "name": t.name, "description": t.description, "parameters": t.parameters,
                    } })
                })
                .collect();
            put(&mut body, "tools", Json::Array(tools));
            if a.answer_now {
                put(&mut body, "tool_choice", json!("none"));
            }
        }
        if a.json {
            put(&mut body, "response_format", json!({ "type": "json_object" }));
        }
    }
    let content = body.to_string().into_bytes();
    request.body = Some(("application/json".to_owned(), content));
    request.timeout = request.timeout.min(a.timeout).max(Duration::from_secs(1));
    Ok(request)
}

/// The conversation as the chat completions API takes it: only the fields it knows.
fn to_chat(conversation: &[Json]) -> Vec<Json> {
    conversation
        .iter()
        .map(|m| {
            let role = m.get("role").and_then(Json::as_str).unwrap_or("user");
            match role {
                "assistant" => {
                    let mut out = json!({ "role": "assistant", "content": m.get("content").cloned().unwrap_or(Json::Null) });
                    if let Some(calls) = m.get("tool_calls").filter(|c| c.as_array().is_some_and(|l| !l.is_empty())) {
                        put(&mut out, "tool_calls", calls.clone());
                    }
                    out
                }
                "tool" => json!({
                    "role": "tool",
                    "tool_call_id": m.get("tool_call_id").cloned().unwrap_or(Json::Null),
                    "content": m.get("content").cloned().unwrap_or(Json::Null),
                }),
                other => json!({ "role": other, "content": m.get("content").cloned().unwrap_or(Json::Null) }),
            }
        })
        .collect()
}

/// The conversation in the `messages` shape: its system texts, and its turns.
fn to_messages(conversation: &[Json]) -> (Vec<String>, Vec<Json>) {
    let mut system = Vec::new();
    let mut turns: Vec<Json> = Vec::new();
    let text_of = |m: &Json| {
        m.get("content")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    for m in conversation {
        match m.get("role").and_then(Json::as_str).unwrap_or("user") {
            "system" => system.push(text_of(m)),
            "assistant" => {
                let mut blocks = Vec::new();
                let text = text_of(m);
                if !text.is_empty() {
                    blocks.push(json!({ "type": "text", "text": text }));
                }
                for call in m.get("tool_calls").and_then(Json::as_array).into_iter().flatten() {
                    let arguments = call
                        .pointer("/function/arguments")
                        .and_then(Json::as_str)
                        .unwrap_or("{}");
                    let input = serde_json::from_str::<Json>(arguments)
                        .ok()
                        .filter(Json::is_object)
                        .unwrap_or_else(|| json!({}));
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": call.get("id").cloned().unwrap_or(Json::Null),
                        "name": call.pointer("/function/name").cloned().unwrap_or(Json::Null),
                        "input": input,
                    }));
                }
                if blocks.is_empty() {
                    blocks.push(json!({ "type": "text", "text": "(no answer)" }));
                }
                turns.push(json!({ "role": "assistant", "content": blocks }));
            }
            "tool" => {
                let mut block = json!({
                    "type": "tool_result",
                    "tool_use_id": m.get("tool_call_id").cloned().unwrap_or(Json::Null),
                    "content": text_of(m),
                });
                if m.get("is_error") == Some(&Json::Bool(true)) {
                    put(&mut block, "is_error", Json::Bool(true));
                }
                user_block(&mut turns, block);
            }
            _ => user_block(&mut turns, json!({ "type": "text", "text": text_of(m) })),
        }
    }
    (system, turns)
}

/// A block added to the last turn when it is the user's, or as a new user turn: the shape takes turns in turn.
fn user_block(turns: &mut Vec<Json>, block: Json) {
    if let Some(last) = turns.last_mut()
        && last.get("role").and_then(Json::as_str) == Some("user")
        && let Some(Json::Array(blocks)) = last.get_mut("content")
    {
        blocks.push(block);
        return;
    }
    turns.push(json!({ "role": "user", "content": [block] }));
}

/// The model's reply, from its answer (`{status, headers, body}`), or why it refused.
pub(crate) fn reply(credential: &Credential, answer: &Json) -> Result<Reply, String> {
    let status = answer.get("status").and_then(Json::as_u64).unwrap_or(0);
    let body = answer.get("body").cloned().unwrap_or(Json::Null);
    if status >= 400 || status == 0 {
        let said = |b: &Json| -> String {
            let text = b.as_str().map_or_else(|| b.to_string(), str::to_owned);
            text.chars().take(300).collect()
        };
        let reason = body
            .pointer("/error/message")
            .or_else(|| body.get("error"))
            .or_else(|| body.get("message"))
            .map_or_else(|| said(&body), said);
        return Err(format!("the model answered {status}: {reason}"));
    }
    let model = body.get("model").and_then(Json::as_str).map(str::to_owned);
    let reply = if messages_api(credential) {
        let mut text = String::new();
        let mut calls = Vec::new();
        for (i, part) in body
            .get("content")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            match part.get("type").and_then(Json::as_str) {
                Some("text") => text.push_str(part.get("text").and_then(Json::as_str).unwrap_or_default()),
                Some("tool_use") => calls.push(Call {
                    id: part
                        .get("id")
                        .and_then(Json::as_str)
                        .map_or_else(|| format!("call_{i}"), str::to_owned),
                    name: part
                        .get("name")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    arguments: part.get("input").map_or_else(|| "{}".to_owned(), Json::to_string),
                }),
                _ => {}
            }
        }
        let finish = match body.get("stop_reason").and_then(Json::as_str) {
            Some("max_tokens") => "length",
            Some("tool_use") => "tool_calls",
            Some("refusal") => "content_filter",
            _ => "stop",
        };
        Reply {
            text,
            calls,
            finish: finish.to_owned(),
            prompt_tokens: body.pointer("/usage/input_tokens").and_then(Json::as_u64),
            completion_tokens: body.pointer("/usage/output_tokens").and_then(Json::as_u64),
            model,
        }
    } else {
        let message = body.pointer("/choices/0/message").cloned().unwrap_or(Json::Null);
        let text = match message.get("content") {
            Some(Json::String(s)) => s.clone(),
            // Some services give the text in parts.
            Some(Json::Array(parts)) => parts
                .iter()
                .filter_map(|p| p.get("text").and_then(Json::as_str))
                .collect(),
            _ => String::new(),
        };
        let calls = message
            .get("tool_calls")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(i, c)| Call {
                id: c
                    .get("id")
                    .and_then(Json::as_str)
                    .filter(|id| !id.is_empty())
                    .map_or_else(|| format!("call_{i}"), str::to_owned),
                name: c
                    .pointer("/function/name")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                arguments: match c.pointer("/function/arguments") {
                    Some(Json::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => "{}".to_owned(),
                },
            })
            .collect();
        Reply {
            text,
            calls,
            finish: body
                .pointer("/choices/0/finish_reason")
                .and_then(Json::as_str)
                .unwrap_or("stop")
                .to_owned(),
            prompt_tokens: body.pointer("/usage/prompt_tokens").and_then(Json::as_u64),
            completion_tokens: body.pointer("/usage/completion_tokens").and_then(Json::as_u64),
            model,
        }
    };
    if reply.text.trim().is_empty() && reply.calls.is_empty() {
        return Err(format!("the model gave no answer (it stopped: {})", reply.finish));
    }
    Ok(reply)
}

/// The model's reply as the conversation keeps it.
pub(crate) fn assistant(reply: &Reply) -> Json {
    let content = if reply.text.is_empty() {
        Json::Null
    } else {
        Json::String(reply.text.clone())
    };
    let mut message = json!({ "role": "assistant", "content": content });
    if !reply.calls.is_empty() {
        let calls: Vec<Json> = reply
            .calls
            .iter()
            .map(|c| json!({ "id": c.id, "type": "function", "function": { "name": c.name, "arguments": c.arguments } }))
            .collect();
        put(&mut message, "tool_calls", Json::Array(calls));
    }
    message
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn credential(api: Option<&str>) -> Credential {
        let mut fields: BTreeMap<String, String> = [
            ("base_url".to_owned(), "https://models.example.com/v1".to_owned()),
            ("api_key".to_owned(), "k".to_owned()),
            ("model".to_owned(), "m-1".to_owned()),
        ]
        .into();
        if let Some(api) = api {
            fields.insert("api".to_owned(), api.to_owned());
        }
        Credential {
            kind: "ai".to_owned(),
            fields,
        }
    }

    fn conversation() -> Vec<Json> {
        vec![
            json!({ "role": "system", "content": "You help." }),
            json!({ "role": "user", "content": "Where is A-1?" }),
            json!({ "role": "assistant", "content": null, "tool_calls": [
                { "id": "c1", "type": "function", "function": { "name": "status", "arguments": "{\"id\":\"A-1\"}" } },
                { "id": "c2", "type": "function", "function": { "name": "eta", "arguments": "{}" } },
            ] }),
            json!({ "role": "tool", "tool_call_id": "c1", "content": "{\"status\":\"sent\"}" }),
            json!({ "role": "tool", "tool_call_id": "c2", "content": "{\"error\":\"down\"}", "is_error": true }),
        ]
    }

    fn tools() -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "status".to_owned(),
            description: "An order's status".to_owned(),
            parameters: json!({ "type": "object", "properties": { "id": { "type": "string" } } }),
        }]
    }

    fn sent(r: &HttpRequest) -> Json {
        serde_json::from_slice(&r.body.as_ref().unwrap().1).unwrap()
    }

    #[test]
    fn the_standard_request_carries_the_conversation_and_the_tools_as_functions() {
        let c = credential(None);
        let (conversation, tools) = (conversation(), tools());
        let r = request(&Asked {
            credential: &c,
            model: "",
            max_tokens: 300,
            temperature: Some(0.0),
            conversation: &conversation,
            tools: &tools,
            answer_now: false,
            json: true,
            timeout: Duration::from_secs(5),
        })
        .unwrap();
        assert_eq!(r.url, "https://models.example.com/v1/chat/completions");
        assert_eq!(
            r.timeout,
            Duration::from_secs(5),
            "never longer than the agent has left"
        );
        let body = sent(&r);
        assert_eq!(body["model"], "m-1");
        assert_eq!(body["max_tokens"], 300);
        assert_eq!(body["messages"].as_array().unwrap().len(), 5);
        assert_eq!(
            body["messages"][4],
            json!({ "role": "tool", "tool_call_id": "c2", "content": "{\"error\":\"down\"}" }),
            "only the fields the standard knows"
        );
        assert_eq!(body["tools"][0]["function"]["name"], "status");
        assert_eq!(body["response_format"]["type"], "json_object");
        assert!(body.get("tool_choice").is_none());
    }

    #[test]
    fn the_messages_shape_takes_calls_as_tool_use_and_results_in_one_user_turn() {
        let c = credential(Some("messages"));
        let (conversation, tools) = (conversation(), tools());
        let r = request(&Asked {
            credential: &c,
            model: "big-2",
            max_tokens: 300,
            temperature: None,
            conversation: &conversation,
            tools: &tools,
            answer_now: true,
            json: false,
            timeout: Duration::from_secs(60),
        })
        .unwrap();
        assert_eq!(r.url, "https://models.example.com/v1/messages");
        let body = sent(&r);
        assert_eq!(body["model"], "big-2");
        assert_eq!(body["system"], "You help.");
        let turns = body["messages"].as_array().unwrap();
        assert_eq!(turns.len(), 3, "user, assistant, user: {turns:?}");
        assert_eq!(
            turns[1]["content"][0],
            json!({ "type": "tool_use", "id": "c1", "name": "status", "input": { "id": "A-1" } })
        );
        assert_eq!(turns[2]["content"][0]["type"], "tool_result");
        assert_eq!(turns[2]["content"][1]["is_error"], true);
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
        assert_eq!(body["tool_choice"], json!({ "type": "none" }));
    }

    #[test]
    fn a_reply_reads_the_same_from_either_shape() {
        let standard = json!({ "status": 200, "body": {
            "model": "m-1",
            "choices": [{ "finish_reason": "tool_calls", "message": { "content": null, "tool_calls": [
                { "id": "x", "function": { "name": "status", "arguments": "{\"id\":\"A-1\"}" } },
            ] } }],
            "usage": { "prompt_tokens": 12, "completion_tokens": 3 },
        } });
        let r = reply(&credential(None), &standard).unwrap();
        assert_eq!(
            (r.calls.len(), r.calls[0].name.as_str(), r.finish.as_str()),
            (1, "status", "tool_calls")
        );
        assert_eq!((r.prompt_tokens, r.completion_tokens), (Some(12), Some(3)));
        let other = json!({ "status": 200, "body": {
            "content": [{ "type": "text", "text": "Looking." }, { "type": "tool_use", "id": "t1", "name": "status", "input": { "id": "A-1" } }],
            "stop_reason": "tool_use",
            "usage": { "input_tokens": 20, "output_tokens": 5 },
        } });
        let r = reply(&credential(Some("messages")), &other).unwrap();
        assert_eq!(
            (r.text.as_str(), r.calls[0].arguments.as_str()),
            ("Looking.", "{\"id\":\"A-1\"}")
        );
        assert_eq!(r.finish, "tool_calls");
        let kept = assistant(&r);
        assert_eq!(kept["tool_calls"][0]["function"]["name"], "status");
        let refused = json!({ "status": 429, "body": { "error": { "message": "slow down" } } });
        assert_eq!(
            reply(&credential(None), &refused).unwrap_err(),
            "the model answered 429: slow down"
        );
        let empty = json!({ "status": 200, "body": { "choices": [{ "finish_reason": "length", "message": { "content": "" } }] } });
        assert!(
            reply(&credential(None), &empty)
                .unwrap_err()
                .contains("no answer (it stopped: length)")
        );
    }
}
