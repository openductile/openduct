// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Webhooks: `/hooks/<hook>` takes a delivery from any system - JSON, a form or text - for the workflow whose
//! (random) hook it is, checks its token if the workflow has one (`x-openduct-token`, a bearer token, the header
//! messengers send it in, or `?token=`), keeps it in the workflow's topic and only then answers 202, so a delivery
//! answered is a delivery that runs. A workflow that is off answers 503, and a hook no workflow has 404.
//!
//! A workflow with a respond step answers its caller itself: the hook waits (up to 30 seconds) for the run, and
//! answers with the status, headers and body the respond step gave - 500 if the run failed, 202 as before if no
//! respond step ran or the run took longer.

use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Response, StatusCode};
use openduct_flow::runner::{self, Topics};
use openduct_flow::{Kind, Step, Trigger};
use serde_json::{Value, json};

use crate::State;
use crate::history::end_of;
use crate::http::{Answer, Call, json as answer_json, problem};
use crate::workflows::{definition_of, now_ms, queue, queued};

/// How long a hook whose workflow answers its caller waits for the answer.
const RESPOND_WAIT: Duration = Duration::from_secs(30);

/// Headers a respond step may not set: the hook answers from the studio's own address, so no cookies; and those
/// the server sets itself.
const NOT_SET: &[&str] = &[
    "set-cookie",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "upgrade",
];

/// The headers a token may come in.
const TOKEN_HEADERS: &[&str] = &["x-openduct-token", "x-telegram-bot-api-secret-token"];

pub(crate) async fn deliver(state: &State, call: &Call, hook: &str) -> Answer {
    let found = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .find(|w| w.hook == hook && w.deleted.is_none());
    let Some(record) = found else {
        return problem(StatusCode::NOT_FOUND, "no such webhook");
    };
    let definition = definition_of(state, &record).await;
    let Ok(workflow) = crate::custom_apps::workflow(&record.workspace, &definition, Some(&record.id)) else {
        return problem(StatusCode::NOT_FOUND, "no such webhook");
    };
    // G3 app triggers: an app's own delivery, checked against the app's signature.
    if let Trigger::App(t) = &workflow.trigger {
        return crate::app_hooks::deliver(state, call, &record, t).await;
    }
    let Trigger::Webhook { token } = &workflow.trigger else {
        return problem(StatusCode::NOT_FOUND, "no such webhook");
    };
    if !record.active {
        return problem(StatusCode::SERVICE_UNAVAILABLE, "the workflow is off");
    }
    if let Some(token) = token {
        let header = |name: &str| {
            call.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let given = TOKEN_HEADERS
            .iter()
            .find_map(|h| header(h))
            .or_else(|| header("authorization").and_then(|a| a.strip_prefix("Bearer ")))
            .or_else(|| query_value(&call.query, "token"))
            .unwrap_or_default();
        if !same(given.as_bytes(), token.as_bytes()) {
            return problem(
                StatusCode::UNAUTHORIZED,
                "the webhook's token is missing or wrong",
            );
        }
    }
    let value = if call.content_type.contains("json") {
        call.body.clone()
    } else if call.content_type.starts_with("application/x-www-form-urlencoded") {
        Value::Object(form(&String::from_utf8_lossy(&call.raw)))
    } else if call.content_type.starts_with("multipart/form-data") {
        let mut fields = multipart(&call.content_type, &call.raw);
        // G6 files: the form's files kept, each given to the run as its field's value.
        let kept = crate::run_files::uploads(
            state,
            &record.workspace,
            &call.content_type,
            &call.raw,
            &mut fields,
        )
        .await;
        if let Err(e) = kept {
            return problem(StatusCode::UNPROCESSABLE_ENTITY, &e);
        }
        Value::Object(fields)
    } else {
        Value::String(String::from_utf8_lossy(&call.raw).into_owned())
    };
    let shown: serde_json::Map<String, Value> = call
        .headers
        .iter()
        .filter(|(k, _)| {
            !matches!(k.as_str(), "authorization" | "cookie") && !TOKEN_HEADERS.contains(&k.as_str())
        })
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let rest = call
        .path
        .trim_start_matches('/')
        .splitn(3, '/')
        .nth(2)
        .unwrap_or_default();
    let query: serde_json::Map<String, Value> = form(&call.query)
        .into_iter()
        .filter(|(k, _)| k != "token")
        .collect();
    let details = json!({
        "trigger": "webhook", "method": call.method.as_str(), "path": format!("/{rest}"),
        "query": Value::Object(query), "headers": shown, "at": runner::iso(now_ms()),
    });
    if !answers(&workflow.steps) {
        return queue(state, &record.id, &value, &details).await;
    }
    let runs = Topics::of(&record.id).runs;
    let id = record.id.clone();
    let topic = runs.clone();
    // Where the runs end now: this delivery's run comes after.
    let from = match state
        .with_client(|client| {
            Box::pin(async move {
                runner::prepare(client, &id).await?;
                end_of(client, &topic).await
            })
        })
        .await
    {
        Ok(from) => from,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let run = match queued(state, &record.id, &value, &details).await {
        Ok(run) => run,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let deadline = Instant::now() + RESPOND_WAIT;
    let mut next = from;
    loop {
        // Waits with every other reader of the node, in one request (Shared::fetch): answered the moment a run is
        // kept, so the caller hears as soon as its run is done - not at the next look.
        let found =
            match tokio::time::timeout_at(deadline.into(), state.shared.fetch(&runs, &[(0, next)])).await {
                Ok(Ok(fetched)) => fetched
                    .into_iter()
                    .filter(|f| f.partition == 0)
                    .flat_map(|f| crate::history::records_from(f.batches, &mut next))
                    .collect::<Vec<_>>(),
                // The broker did not answer this once: looked at again, until the caller's time is up.
                Ok(Err(_)) => {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    Vec::new()
                }
                Err(_) => Vec::new(),
            };
        for r in found {
            if r.key.as_deref() == Some(run.as_bytes()) {
                let kept: Value = r
                    .value
                    .as_ref()
                    .and_then(|v| serde_json::from_slice(v).ok())
                    .unwrap_or(Value::Null);
                return reply(&kept, &run);
            }
        }
        if Instant::now() >= deadline {
            return answer_json(StatusCode::ACCEPTED, &json!({ "accepted": true, "run": run }));
        }
    }
}

/// Whether any of these steps (a branch's, a loop's or a case's included) answers the caller.
fn answers(steps: &[Step]) -> bool {
    steps.iter().any(|s| match &s.kind {
        Kind::Respond { .. } => true,
        Kind::If { then, otherwise, .. } => answers(then) || answers(otherwise),
        Kind::ForEach { steps, .. } => answers(steps),
        Kind::Switch { cases, otherwise } => cases.iter().any(|c| answers(&c.steps)) || answers(otherwise),
        Kind::Ai(ai) => ai.routes().any(answers), // G7 more AI steps: a category's steps
        _ => false,
    })
}

/// The answer a finished run gives its caller: its last respond step's.
fn reply(kept: &Value, run: &str) -> Answer {
    if kept["status"] == "failed" {
        return problem(StatusCode::INTERNAL_SERVER_ERROR, "the workflow failed");
    }
    let given = kept["steps"]
        .as_array()
        .and_then(|steps| {
            steps
                .iter()
                .rev()
                .find(|s| s["kind"] == "respond" && s["error"].is_null())
        })
        .map(|s| &s["result"]);
    let Some(given) = given else {
        return answer_json(StatusCode::ACCEPTED, &json!({ "accepted": true, "run": run }));
    };
    let status = given["status"]
        .as_u64()
        .and_then(|s| u16::try_from(s).ok())
        .and_then(|s| StatusCode::from_u16(s).ok())
        .unwrap_or(StatusCode::OK);
    let (body, kind) = match &given["body"] {
        Value::String(text) => (text.clone(), "text/plain; charset=utf-8"),
        Value::Null => (String::new(), "text/plain; charset=utf-8"),
        other => (other.to_string(), "application/json"),
    };
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(hyper::header::CONTENT_TYPE, HeaderValue::from_static(kind));
    if let Some(given) = given["headers"].as_object() {
        for (name, value) in given {
            let lower = name.to_ascii_lowercase();
            if NOT_SET.contains(&lower.as_str()) {
                continue;
            }
            if let (Ok(name), Some(Ok(value))) = (
                HeaderName::try_from(lower),
                value.as_str().map(HeaderValue::from_str),
            ) {
                headers.insert(name, value);
            }
        }
    }
    response
}

fn query_value<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
}

/// `a=1&b=x%20y` as a record of text.
/// A multipart form's fields (`multipart/form-data`, as Jotform and forms with a file upload send them): each text
/// field by its name - a name given more than once, a list - and a file by its file name only; its bytes are not kept.
/// (A delivery's files are kept by `crate::run_files::uploads`, which gives each field its file.) // G6 files
pub(crate) fn multipart(content_type: &str, raw: &[u8]) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    let Some(boundary) = content_type
        .split(';')
        .find_map(|p| p.trim().strip_prefix("boundary="))
        .map(|b| b.trim_matches('"'))
        .filter(|b| !b.is_empty())
    else {
        return out;
    };
    let text = String::from_utf8_lossy(raw);
    let delimiter = format!("--{boundary}");
    for part in text.split(delimiter.as_str()).skip(1) {
        if part.starts_with("--") {
            break;
        }
        let part = part.strip_prefix("\r\n").unwrap_or(part);
        let Some((head, body)) = part.split_once("\r\n\r\n") else {
            continue;
        };
        let body = body.strip_suffix("\r\n").unwrap_or(body);
        let Some(disposition) = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-disposition:"))
        else {
            continue;
        };
        let param = |key: &str| {
            disposition
                .split(';')
                .find_map(|p| p.trim().strip_prefix(key).and_then(|v| v.strip_prefix('=')))
                .map(|v| v.trim_matches('"').to_owned())
        };
        let Some(name) = param("name") else { continue };
        let value = Value::String(param("filename").unwrap_or_else(|| body.to_owned()));
        match out.get_mut(&name) {
            Some(Value::Array(list)) => list.push(value),
            Some(first) => *first = Value::Array(vec![first.take(), value]),
            None => {
                out.insert(name, value);
            }
        }
    }
    out
}

pub(crate) fn form(text: &str) -> serde_json::Map<String, Value> {
    text.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(k), Value::String(decode(v)))
        })
        .collect()
}

// forms-and-resume: also a form's answer's.
pub(crate) fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        match b {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = bytes.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok());
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(v) => {
                        out.push(v);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Equal, in a time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_multipart_form_reads_field_by_field_and_a_file_by_its_name() {
        let raw = "--XyZ\r\nContent-Disposition: form-data; name=\"q3_email\"\r\n\r\nada@example.com\r\n\
                   --XyZ\r\nContent-Disposition: form-data; name=\"size\"\r\n\r\nM\r\n\
                   --XyZ\r\nContent-Disposition: form-data; name=\"size\"\r\n\r\nL\r\n\
                   --XyZ\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"tote.jpg\"\r\n\
                   Content-Type: image/jpeg\r\n\r\n\u{1}\u{2}binary\r\n--XyZ--\r\n";
        let fields = multipart("multipart/form-data; boundary=XyZ", raw.as_bytes());
        assert_eq!(
            Value::Object(fields),
            serde_json::json!({ "q3_email": "ada@example.com", "size": ["M", "L"], "photo": "tote.jpg" })
        );
        assert!(multipart("multipart/form-data", raw.as_bytes()).is_empty());
    }

    #[test]
    fn a_form_and_a_query_read_as_text_and_tokens_compare_whole() {
        let fields = form("name=Ada+Lovelace&city=S%C3%A3o%20Paulo&bad=%zz");
        assert_eq!(fields.get("name"), Some(&json!("Ada Lovelace")));
        assert_eq!(fields.get("city"), Some(&json!("São Paulo")));
        assert_eq!(fields.get("bad"), Some(&json!("%zz")));
        assert_eq!(query_value("a=1&token=abc", "token"), Some("abc"));
        assert!(same(b"abc", b"abc") && !same(b"abc", b"abd") && !same(b"abc", b"ab"));
    }
}
