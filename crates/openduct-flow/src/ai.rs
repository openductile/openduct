// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The AI steps, in one standard form whatever the model behind them - the chat completions API's - so a workflow
//! keeps working when its model or its provider changes.
//!
//! `ai` takes the standard's chat fields - `model`, `messages` (or the shorthands `system` and `prompt`),
//! `temperature`, `top_p`, `max_tokens`, `stop`, `response_format` (`json`, `json_object`, or `{type: ...}`) - and
//! answers as the standard does: the `text`, `finish_reason`, `model` and `usage` (`prompt_tokens`,
//! `completion_tokens`, `total_tokens`), with the JSON read from the text when JSON was asked for. Beside them, a
//! `transcript` (every message sent, then the answer) and `metrics` (latency, tokens a second).
//!
//! `embed` takes `model` and `input` (a text, or a list of them), as the standard's embeddings API does, and answers
//! with the `embedding` (or `embeddings`), its `dimensions` and the `usage`.
//!
//! Where to send them is the credential's, and nothing here names a provider: its `base_url`, its `api_key`, the
//! header that carries the key (`key_header`: `authorization`, as a bearer token, unless it says otherwise), any
//! other `headers` its service asks for (a JSON record), and its `api` - `chat_completions`, the standard, or
//! `messages`, the other shape some services use (a `system` beside the messages, `max_tokens` required), into which
//! the standard request is translated and from which the answer is translated back.

use std::collections::BTreeMap;
use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Value as Json, json};

use crate::apps::{Credential, MASK, put};
use crate::run::HttpRequest;

/// The longest a model may take to answer.
const TIMEOUT: Duration = Duration::from_secs(180);

fn text(fields: &BTreeMap<String, Value>, name: &str) -> String {
    fields.get(name).map(crate::template::as_text).unwrap_or_default()
}

/// The model a step asks: the one it names, or else its credential's (`field`: `model`, or `embedding_model`). A test
/// run's stand-in for a credential the workspace does not have yet names it so, rather than failing.
fn model_of(credential: &Credential, fields: &BTreeMap<String, Value>, field: &str) -> String {
    let named = text(fields, "model");
    if !named.trim().is_empty() {
        return named;
    }
    // Not `field`: a field the credential lacks reads as nothing here, not as a hidden secret.
    let kept = credential.fields.get(field).cloned().unwrap_or_default();
    if !kept.trim().is_empty() && kept != crate::apps::MASK {
        return kept;
    }
    if credential.kind == "test" {
        return format!("the credential's {}", field.replace('_', " "));
    }
    String::new()
}

fn number(fields: &BTreeMap<String, Value>, name: &str) -> Option<f64> {
    fields
        .get(name)
        .and_then(|v| crate::template::as_text(v).trim().parse::<f64>().ok())
}

fn as_json(value: &Value) -> Json {
    serde_json::from_str(&value.to_json()).unwrap_or(Json::Null)
}

/// The credential's API shape: the standard's, or `messages`.
fn messages_api(credential: &Credential) -> bool {
    credential
        .fields
        .get("api")
        .is_some_and(|a| a.trim() == "messages")
}

/// Whether the step asks for JSON.
fn wants_json(fields: &BTreeMap<String, Value>) -> bool {
    let by_format = match fields.get("response_format").map(as_json) {
        Some(Json::String(s)) => matches!(s.as_str(), "json" | "json_object"),
        Some(Json::Object(o)) => o
            .get("type")
            .and_then(Json::as_str)
            .is_some_and(|t| t.starts_with("json")),
        _ => false,
    };
    by_format
        || fields
            .get("json")
            .is_some_and(|v| crate::template::as_text(v).trim() == "true")
}

/// The conversation, as the standard has it: `[{role, content}]`.
fn messages(fields: &BTreeMap<String, Value>) -> Result<Vec<Json>, String> {
    let mut out = Vec::new();
    let system = text(fields, "system");
    if !system.trim().is_empty() {
        out.push(json!({ "role": "system", "content": system }));
    }
    if let Some(list) = fields.get("messages") {
        let Json::Array(items) = as_json(list) else {
            return Err("messages: a list of {role, content}".to_owned());
        };
        for (i, item) in items.iter().enumerate() {
            let role = item.get("role").and_then(Json::as_str).unwrap_or_default();
            if !matches!(role, "system" | "user" | "assistant") {
                return Err(format!("messages[{i}].role: system, user or assistant"));
            }
            let content = match item.get("content") {
                Some(Json::String(s)) => s.clone(),
                Some(other) => other.to_string(),
                None => return Err(format!("messages[{i}].content: the message's text")),
            };
            out.push(json!({ "role": role, "content": content }));
        }
    }
    let prompt = text(fields, "prompt");
    if !prompt.trim().is_empty() {
        out.push(json!({ "role": "user", "content": prompt }));
    }
    if !out
        .iter()
        .any(|m| m.get("role").and_then(Json::as_str) == Some("user"))
    {
        return Err("prompt: what to ask the model (or messages with a user message)".to_owned());
    }
    Ok(out)
}

/// Where the credential's service is.
fn base(credential: &Credential) -> Result<String, String> {
    let base = credential
        .fields
        .get("base_url")
        .map(String::as_str)
        .unwrap_or_default()
        .trim();
    if base.is_empty() {
        // A test run reads no secret, and shows where the request would go.
        if credential.fields.is_empty() {
            return Ok(format!("https://{MASK}"));
        }
        return Err("the credential has no base_url: the address of the model's API".to_owned());
    }
    Ok(base.trim_end_matches('/').to_owned())
}

/// The headers the credential's service takes: its key (as `key_header` says), then its own `headers`.
fn headers(credential: &Credential) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let key = credential
        .fields
        .get("api_key")
        .cloned()
        .or_else(|| credential.fields.is_empty().then(|| MASK.to_owned()));
    if let Some(key) = key {
        let header = credential
            .fields
            .get("key_header")
            .map(|h| h.trim().to_ascii_lowercase())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "authorization".to_owned());
        let value = if header == "authorization" {
            format!("Bearer {key}")
        } else {
            key
        };
        out.push((header, value));
    }
    if let Some(extra) = credential.fields.get("headers").filter(|h| !h.trim().is_empty()) {
        let Json::Object(extra) =
            serde_json::from_str::<Json>(extra).map_err(|_| "the credential's headers are not JSON")?
        else {
            return Err("the credential's headers: a JSON record of names and values".to_owned());
        };
        for (name, value) in extra {
            let value = value.as_str().map_or_else(|| value.to_string(), str::to_owned);
            out.push((name.to_ascii_lowercase(), value));
        }
    }
    Ok(out)
}

/// The request that lists the models a credential's service offers: `GET <base_url>/models`, signed as its model's
/// requests are. A read: it changes nothing.
pub fn models_request(credential: &Credential) -> Result<HttpRequest, String> {
    Ok(HttpRequest {
        read: true,
        method: "GET".to_owned(),
        url: format!("{}/models", base(credential)?),
        headers: headers(credential)?,
        query: Vec::new(),
        body: None,
        timeout: Duration::from_secs(20),
    })
}

/// The models in a listing's answer (`{"data": [{"id": ...}, ...]}`, as most services give it): each one's id and,
/// when the service gives one, its name for people; sorted by id.
pub fn models_in(answer: &Value) -> Result<Vec<(String, Option<String>)>, String> {
    let json = as_json(answer);
    let status = json.get("status").and_then(Json::as_u64).unwrap_or(0);
    let body = json.get("body").cloned().unwrap_or(Json::Null);
    if !(200..300).contains(&status) {
        let said = body
            .pointer("/error/message")
            .or_else(|| body.get("error"))
            .or_else(|| body.get("message"))
            .map(|m| m.as_str().map_or_else(|| m.to_string(), str::to_owned))
            .unwrap_or_default();
        return Err(format!("the service answered {status} {said}").trim().to_owned());
    }
    let list = body
        .get("data")
        .or_else(|| body.get("models"))
        .and_then(Json::as_array)
        .ok_or("the service's answer lists no models")?;
    let mut out: Vec<(String, Option<String>)> = list
        .iter()
        .filter_map(|m| {
            let id = m.get("id").or_else(|| m.get("name"))?.as_str()?.to_owned();
            let name = m
                .get("display_name")
                .or_else(|| m.get("displayName"))
                .and_then(Json::as_str)
                .map(str::to_owned);
            Some((id, name))
        })
        .collect();
    out.sort();
    out.dedup_by(|a, b| a.0 == b.0);
    Ok(out)
}

/// A model asked: a read, which a preview sends (its answer is what a preview is for).
fn post(url: String, headers: Vec<(String, String)>, body: &Json, timeout: Duration) -> HttpRequest {
    HttpRequest {
        read: true,
        method: "POST".to_owned(),
        url,
        headers,
        query: Vec::new(),
        body: Some(("application/json".to_owned(), body.to_string().into_bytes())),
        timeout,
    }
}

/// The request for an `ai` step.
pub(crate) fn chat_request(
    credential: &Credential,
    fields: &BTreeMap<String, Value>,
) -> Result<HttpRequest, String> {
    let model = model_of(credential, fields, "model");
    if model.trim().is_empty() {
        return Err("model: the model to ask - in the step, or as the credential's model".to_owned());
    }
    let messages = messages(fields)?;
    let json_wanted = wants_json(fields);
    // A whole number: every number in an expression is a decimal, and the APIs take only whole ones here.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // rounded, and kept to the APIs' range
    let max_tokens = number(fields, "max_tokens").map_or(1024, |n| n.round().clamp(1.0, 1_000_000.0) as u64);
    let stop: Option<Json> = fields.get("stop").map(as_json).filter(|s| !s.is_null());
    let base = base(credential)?;
    let headers = headers(credential)?;
    if messages_api(credential) {
        // The standard's system messages are this API's `system`; the rest are its messages.
        let mut system: Vec<String> = messages
            .iter()
            .filter(|m| m.get("role").and_then(Json::as_str) == Some("system"))
            .filter_map(|m| m.get("content").and_then(Json::as_str).map(str::to_owned))
            .collect();
        if json_wanted {
            system.push("Answer with one JSON value and nothing else.".to_owned());
        }
        let turns: Vec<Json> = messages
            .into_iter()
            .filter(|m| m.get("role").and_then(Json::as_str) != Some("system"))
            .collect();
        let mut body = json!({ "model": model, "max_tokens": max_tokens, "messages": turns });
        if !system.is_empty() {
            put(&mut body, "system", json!(system.join("\n\n")));
        }
        if let Some(t) = number(fields, "temperature") {
            put(&mut body, "temperature", json!(t));
        }
        if let Some(p) = number(fields, "top_p") {
            put(&mut body, "top_p", json!(p));
        }
        if let Some(stop) = stop {
            let list = if stop.is_array() { stop } else { json!([stop]) };
            put(&mut body, "stop_sequences", list);
        }
        return Ok(post(format!("{base}/messages"), headers, &body, TIMEOUT));
    }
    let mut body = json!({ "model": model, "messages": messages, "max_tokens": max_tokens });
    if let Some(t) = number(fields, "temperature") {
        put(&mut body, "temperature", json!(t));
    }
    if let Some(p) = number(fields, "top_p") {
        put(&mut body, "top_p", json!(p));
    }
    if let Some(stop) = stop {
        put(&mut body, "stop", stop);
    }
    if json_wanted {
        put(&mut body, "response_format", json!({ "type": "json_object" }));
    }
    Ok(post(format!("{base}/chat/completions"), headers, &body, TIMEOUT))
}

/// An `ai` step's result from the answer, in the standard's form.
pub(crate) fn chat_answer(
    credential: &Credential,
    fields: &BTreeMap<String, Value>,
    body: &Json,
    ms: u64,
) -> Result<Value, String> {
    let (text, finish, prompt_tokens, completion_tokens) = if messages_api(credential) {
        let text: String = body
            .get("content")
            .and_then(Json::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter(|p| p.get("type").and_then(Json::as_str) == Some("text"))
                    .filter_map(|p| p.get("text").and_then(Json::as_str))
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        let finish = match body.get("stop_reason").and_then(Json::as_str) {
            Some("max_tokens") => "length",
            Some("tool_use") => "tool_calls",
            Some("refusal") => "content_filter",
            _ => "stop",
        };
        (
            text,
            finish.to_owned(),
            body.pointer("/usage/input_tokens").and_then(Json::as_u64),
            body.pointer("/usage/output_tokens").and_then(Json::as_u64),
        )
    } else {
        (
            body.pointer("/choices/0/message/content")
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_owned(),
            body.pointer("/choices/0/finish_reason")
                .and_then(Json::as_str)
                .unwrap_or("stop")
                .to_owned(),
            body.pointer("/usage/prompt_tokens").and_then(Json::as_u64),
            body.pointer("/usage/completion_tokens").and_then(Json::as_u64),
        )
    };
    if text.is_empty() && finish == "stop" {
        return Err("the model gave no text".to_owned());
    }
    let total = prompt_tokens.zip(completion_tokens).map(|(p, c)| p + c);
    #[allow(clippy::cast_precision_loss)] // tokens and milliseconds, far below 2^52
    let rate = completion_tokens
        .filter(|_| ms > 0)
        .map(|c| ((c as f64) * 1000.0 / ms as f64 * 10.0).round() / 10.0);
    let mut transcript = messages(fields)?;
    transcript.push(json!({ "role": "assistant", "content": text }));
    let api = if messages_api(credential) {
        "messages"
    } else {
        "chat_completions"
    };
    let mut result = json!({
        "text": text,
        "finish_reason": finish,
        "model": body.get("model").cloned().unwrap_or_else(|| json!(crate::apps::field(fields, "model"))),
        "usage": { "prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens, "total_tokens": total },
        "transcript": transcript,
        "metrics": { "latency_ms": ms, "tokens_per_second": rate, "api": api },
    });
    if wants_json(fields) {
        // Models sometimes wrap JSON in a fenced block: what is inside it is the answer.
        let inner = text
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        let parsed: Json = serde_json::from_str(inner).map_err(|_| {
            format!(
                "the model's answer is not JSON: {}",
                text.chars().take(200).collect::<String>()
            )
        })?;
        put(&mut result, "json", parsed);
    }
    Ok(Value::from_json(&result))
}

/// The request for an `embed` step.
pub(crate) fn embed_request(
    credential: &Credential,
    fields: &BTreeMap<String, Value>,
) -> Result<HttpRequest, String> {
    if messages_api(credential) {
        return Err(
            "embeddings go to a chat_completions-style API: this credential's api is messages".to_owned(),
        );
    }
    let model = model_of(credential, fields, "embedding_model");
    if model.trim().is_empty() {
        return Err(
            "model: the embedding model - in the step, or as the credential's embedding model".to_owned(),
        );
    }
    let input = fields.get("input").map(as_json).unwrap_or(Json::Null);
    let input = match input {
        Json::String(_) => input,
        Json::Array(items) if items.iter().all(Json::is_string) && !items.is_empty() => Json::Array(items),
        _ => return Err("input: a text, or a list of texts".to_owned()),
    };
    let body = json!({ "model": model, "input": input });
    Ok(post(
        format!("{}/embeddings", base(credential)?),
        headers(credential)?,
        &body,
        Duration::from_secs(60),
    ))
}

/// An `embed` step's result: the vector for one text, or the vectors for a list, in order.
pub(crate) fn embed_answer(fields: &BTreeMap<String, Value>, body: &Json, ms: u64) -> Result<Value, String> {
    let mut data: Vec<&Json> = body
        .get("data")
        .and_then(Json::as_array)
        .map(|d| d.iter().collect())
        .ok_or("the answer has no embeddings")?;
    data.sort_by_key(|d| d.get("index").and_then(Json::as_u64).unwrap_or(0));
    let vectors: Vec<Json> = data.iter().filter_map(|d| d.get("embedding").cloned()).collect();
    let dimensions = vectors.first().and_then(Json::as_array).map_or(0, Vec::len);
    let usage = body.get("usage").cloned().unwrap_or(Json::Null);
    let one = matches!(fields.get("input"), Some(Value::Text(_)));
    let mut result = json!({ "dimensions": dimensions, "usage": usage, "metrics": { "latency_ms": ms } });
    if one {
        put(
            &mut result,
            "embedding",
            vectors.into_iter().next().unwrap_or(Json::Null),
        );
    } else {
        put(&mut result, "embeddings", Json::Array(vectors));
    }
    Ok(Value::from_json(&result))
}

/// The optional sampling settings a model refused (`temperature is deprecated for this model`), so the step can ask
/// again without them: a workflow written for one model keeps working on another that takes fewer settings.
pub(crate) fn refused_settings(answer: &Value) -> Vec<&'static str> {
    let json = as_json(answer);
    if json.get("status").and_then(Json::as_u64) != Some(400) {
        return Vec::new();
    }
    let said = json
        .get("body")
        .map(|b| {
            b.pointer("/error/message")
                .and_then(Json::as_str)
                .map_or_else(|| b.to_string(), str::to_owned)
        })
        .unwrap_or_default()
        .to_lowercase();
    let refusing = [
        "deprecated",
        "not supported",
        "unsupported",
        "does not support",
        "not allowed",
        "cannot be",
    ]
    .iter()
    .any(|w| said.contains(w));
    if !refusing {
        return Vec::new();
    }
    ["temperature", "top_p"]
        .into_iter()
        .filter(|s| said.contains(s))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_step_naming_no_model_asks_its_credential_s() {
        let credential = |fields: &[(&str, &str)], kind: &str| Credential {
            kind: kind.to_owned(),
            fields: fields
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        };
        let step = |model: Option<&str>| {
            let mut f: BTreeMap<String, Value> = BTreeMap::new();
            f.insert("prompt".to_owned(), Value::from_json(&serde_json::json!("Hi")));
            if let Some(m) = model {
                f.insert("model".to_owned(), Value::from_json(&serde_json::json!(m)));
            }
            f
        };
        let model_sent = |r: HttpRequest| -> String {
            let body: Json = serde_json::from_slice(&r.body.unwrap().1).unwrap();
            body["model"].as_str().unwrap().to_owned()
        };
        let kept = credential(
            &[("base_url", "https://api.example.com/v1"), ("model", "small-1")],
            "ai",
        );
        assert_eq!(
            model_sent(chat_request(&kept, &step(Some("large-2"))).unwrap()),
            "large-2"
        );
        assert_eq!(model_sent(chat_request(&kept, &step(None)).unwrap()), "small-1");
        let bare = credential(&[("base_url", "https://api.example.com/v1")], "ai");
        let e = chat_request(&bare, &step(None)).unwrap_err();
        assert!(e.contains("credential's model"), "{e}");
        // A test run without the credential yet: it says what would be asked, rather than failing.
        let stand_in = credential(&[], "test");
        assert_eq!(
            model_sent(chat_request(&stand_in, &step(None)).unwrap()),
            "the credential's model"
        );
    }

    #[test]
    fn a_models_listing_reads_as_ids_and_names_whichever_shape_it_comes_in() {
        let answer = |status: u64, body: Json| {
            Value::from_json(&json!({ "status": status, "headers": {}, "body": body }))
        };
        let standard = answer(
            200,
            json!({ "data": [{ "id": "b-model" }, { "id": "a-model", "display_name": "Model A" }] }),
        );
        assert_eq!(
            models_in(&standard).unwrap(),
            vec![
                ("a-model".to_owned(), Some("Model A".to_owned())),
                ("b-model".to_owned(), None)
            ]
        );
        let other = answer(200, json!({ "models": [{ "name": "c-model" }] }));
        assert_eq!(models_in(&other).unwrap()[0].0, "c-model");
        let refused = answer(401, json!({ "error": { "message": "bad key" } }));
        assert!(models_in(&refused).unwrap_err().contains("401 bad key"));
        let request = models_request(&Credential {
            kind: "ai".to_owned(),
            fields: [
                ("base_url".to_owned(), "https://models.example.com/v1/".to_owned()),
                ("api_key".to_owned(), "k".to_owned()),
            ]
            .into(),
        })
        .unwrap();
        assert_eq!(
            (request.method.as_str(), request.url.as_str()),
            ("GET", "https://models.example.com/v1/models")
        );
        assert!(
            request.read
                && request
                    .headers
                    .contains(&("authorization".to_owned(), "Bearer k".to_owned()))
        );
    }
}
