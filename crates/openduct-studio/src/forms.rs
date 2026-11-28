// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Forms the studio serves, and the one-time addresses of runs that wait.
//!
//! - `/forms/<hook>`: a form trigger's page (its workflow's random hook, as a webhook's), and its answers - each
//!   checked as the page checks it, then a run started with it.
//! - `/waits/<token>`: a run that waits. For a form or an approval, its page and its answer; for a wait for a call,
//!   any call (its method, body, query and headers are the step's result). The first answer is the one; later ones
//!   are told the address has had its answer.
//!
//! These are reached by whatever name the person or the system knows, as webhooks are, with no session: a page carries
//! a token of its own (signed, a day long), so an answer comes from the page it was given on - another site cannot
//! post one through someone's browser - and a form trigger may ask for a password and takes a few answers a minute
//! from one address.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use hyper::{Method, StatusCode};
use openduct_flow::Trigger;
use openduct_flow::forms::{Form, Given};
use openduct_flow::runner;
use openduct_flow::waits::{Answered, is_token};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::State;
use crate::api::access;
use crate::form_page::{Filled, form_page, html, message_page, thanks_page};
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, WorkflowRecord};
use crate::workflows::{definition_of, now_ms, queued, workflow_in};

/// The largest answer to a form, files and all, in bytes.
pub(crate) const MAX_BODY: usize = 3 * 1024 * 1024;
/// How long a form's page may be open before its answer is sent.
const PAGE_LIFE_MS: i64 = 24 * 3600 * 1000;
/// How long a call to an address no run waits at yet is held, for the run that hands it out to get there.
const HOLD: Duration = Duration::from_secs(5);
/// The most such calls held at once.
const HELD_AT_ONCE: usize = 64;
/// The most parts a multipart answer has.
const MAX_PARTS: usize = 500;

/// Whether a path is one of these: reached as a webhook is.
pub(crate) fn is_outside(path: &str) -> bool {
    path.starts_with("/forms/") || path.starts_with("/waits/")
}

/// What guards these addresses on this node: how many answers each address sent each form this minute, and the
/// calls held.
#[derive(Debug)]
pub(crate) struct Guard {
    counts: std::sync::Mutex<HashMap<(IpAddr, String), (i64, u32)>>,
    held: Semaphore,
}

impl Default for Guard {
    fn default() -> Self {
        Self {
            counts: std::sync::Mutex::new(HashMap::new()),
            held: Semaphore::new(HELD_AT_ONCE),
        }
    }
}

impl Guard {
    /// Whether `ip` may send `target` one more answer this minute (`per_minute` 0: as many as it likes).
    fn allows(&self, ip: IpAddr, target: &str, per_minute: u32) -> bool {
        if per_minute == 0 {
            return true;
        }
        let minute = now_ms() / 60_000;
        let mut counts = match self.counts.lock() {
            Ok(c) => c,
            Err(poisoned) => poisoned.into_inner(),
        };
        if counts.len() > 50_000 {
            counts.retain(|_, (m, _)| *m == minute);
        }
        let entry = counts.entry((ip, target.to_owned())).or_insert((minute, 0));
        if entry.0 != minute {
            *entry = (minute, 0);
        }
        entry.1 = entry.1.saturating_add(1);
        entry.1 <= per_minute
    }
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["forms", hook]) => form_trigger(state, call, hook, false).await,
        (&Method::POST, ["forms", hook]) => form_trigger(state, call, hook, true).await,
        (_, ["waits", token]) => wait(state, call, token).await,
        (&Method::GET, ["api", "workflows", id, "waiting"]) => waiting(state, call, id).await,
        _ => return None,
    })
}

/// A page's token: when it was given, signed for what it is for.
fn page_token(state: &State, target: &str) -> String {
    let issued = now_ms();
    format!("{issued}.{}", state.keys.sign(&format!("form|{target}|{issued}")))
}

fn page_token_ok(state: &State, target: &str, token: &str) -> bool {
    let Some((issued, signature)) = token.split_once('.') else {
        return false;
    };
    let Ok(at) = issued.parse::<i64>() else {
        return false;
    };
    let age = now_ms() - at;
    (-60_000..=PAGE_LIFE_MS).contains(&age) && state.keys.signed(&format!("form|{target}|{at}"), signature)
}

/// Whether a request names another site as where it comes from.
fn from_elsewhere(call: &Call) -> bool {
    let Some(origin) = call
        .headers
        .iter()
        .find(|(k, _)| k == "origin")
        .map(|(_, v)| v.as_str())
    else {
        return false;
    };
    if origin == "null" {
        return false;
    }
    let host = origin.split_once("://").map_or(origin, |(_, rest)| rest);
    !host.eq_ignore_ascii_case(&call.host)
}

/// The workflow whose form is at `hook`, and its form.
async fn form_of(state: &State, hook: &str) -> Option<(WorkflowRecord, Form)> {
    let record = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .find(|w| w.hook == hook && w.deleted.is_none())?;
    let workflow = crate::custom_apps::workflow(
        &record.workspace,
        &definition_of(state, &record).await,
        Some(&record.id),
    )
    .ok()?;
    match workflow.trigger {
        Trigger::Form(form) => Some((record, *form)),
        _ => None,
    }
}

/// A form trigger's page, or its answer.
async fn form_trigger(state: &State, call: &Call, hook: &str, answering: bool) -> Answer {
    let Some((record, form)) = form_of(state, hook).await else {
        return html(
            StatusCode::NOT_FOUND,
            message_page("No such form", "There is no form at this address."),
        );
    };
    if !record.active {
        return html(
            StatusCode::SERVICE_UNAVAILABLE,
            message_page(&form.title, "This form is closed for now."),
        );
    }
    let target = format!("trigger|{hook}");
    let page = |status: StatusCode, values: &[(String, String)], problems: &[(String, String)]| {
        let token = page_token(state, &target);
        let filled = Filled {
            token: &token,
            values,
            problems,
            password: !form.password.is_empty(),
        };
        html(status, form_page(&form, &filled))
    };
    if !answering {
        return page(StatusCode::OK, &[], &[]);
    }
    if from_elsewhere(call) {
        return html(
            StatusCode::FORBIDDEN,
            message_page(
                &form.title,
                "An answer from another site is not taken: open the form and send it from there.",
            ),
        );
    }
    if !state.forms.allows(call.peer, &target, form.rate_limit) {
        return html(
            StatusCode::TOO_MANY_REQUESTS,
            message_page(
                &form.title,
                "Too many answers from here: wait a minute, then send it again.",
            ),
        );
    }
    let given = match given_of(call) {
        Ok(g) => g,
        Err(why) => return page(StatusCode::BAD_REQUEST, &[], &[(String::new(), why)]),
    };
    let values = texts(&given);
    if !page_token_ok(state, &target, &text_of(&given, "_form")) {
        let again = "This page was open too long, or came from elsewhere: please send it again.".to_owned();
        return page(StatusCode::BAD_REQUEST, &values, &[(String::new(), again)]);
    }
    if !form.password.is_empty() && !same(text_of(&given, "_password").as_bytes(), form.password.as_bytes()) {
        return page(
            StatusCode::UNAUTHORIZED,
            &values,
            &[("_password".to_owned(), "The password is wrong.".to_owned())],
        );
    }
    let answers = match openduct_flow::forms::check(&form, &given) {
        Ok(a) => a,
        Err(problems) => return page(StatusCode::UNPROCESSABLE_ENTITY, &values, &problems),
    };
    let details = json!({ "trigger": "form", "form": form.title, "at": runner::iso(now_ms()) });
    match queued(state, &record.id, &answers, &details).await {
        Ok(_) => html(StatusCode::OK, thanks_page(&form)),
        Err(e) => {
            tracing::warn!(
                workflow = record.id,
                error = e,
                "a form's answer could not be kept"
            );
            html(
                StatusCode::SERVICE_UNAVAILABLE,
                message_page(
                    &form.title,
                    "The answer could not be kept just now: please send it again in a moment.",
                ),
            )
        }
    }
}

/// The run waiting at `token` and its form - held a moment for one not there yet, as when the address is called as
/// soon as it is handed out.
async fn found(
    state: &State,
    token: &str,
) -> Result<Option<(openduct_flow::waits::Summary, Option<Form>)>, String> {
    if let Some(found) = state.waits.open_wait(token).await? {
        return Ok(Some(found));
    }
    let Ok(_held) = state.forms.held.try_acquire() else {
        return Ok(None);
    };
    let until = tokio::time::Instant::now() + HOLD;
    while tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Some(found) = state.waits.open_wait(token).await? {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

/// A run that waits: its form's page, its answer, or a call to it.
async fn wait(state: &State, call: &Call, token: &str) -> Answer {
    let gone = || {
        html(
            StatusCode::NOT_FOUND,
            message_page(
                "Nothing here",
                "Nothing waits at this address: it may have had its answer, or it never was.",
            ),
        )
    };
    if !is_token(token) {
        return gone();
    }
    let (summary, form) = match found(state, token).await {
        Ok(Some(found)) => found,
        Ok(None) => return gone(),
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    // A run of a workflow deleted since: nothing goes on.
    let gone_workflow = state
        .model
        .workflows
        .get(&summary.workflow)
        .await
        .is_none_or(|w| w.deleted.is_some());
    if gone_workflow {
        return gone();
    }
    if summary.kind == "webhook" {
        return called(state, call, token, &summary).await;
    }
    let Some(form) = form else {
        return gone();
    };
    if summary.answered.is_some() {
        return html(
            StatusCode::CONFLICT,
            message_page(&form.title, "This form has had its answer already: thank you."),
        );
    }
    let target = format!("wait|{token}");
    let page = |status: StatusCode, values: &[(String, String)], problems: &[(String, String)]| {
        let shown = page_token(state, &target);
        let filled = Filled {
            token: &shown,
            values,
            problems,
            password: false,
        };
        html(status, form_page(&form, &filled))
    };
    match call.method {
        Method::GET => return page(StatusCode::OK, &[], &[]),
        Method::POST => {}
        _ => {
            return problem(
                StatusCode::METHOD_NOT_ALLOWED,
                "a form's page is read, and its answer posted",
            );
        }
    }
    if from_elsewhere(call) {
        return html(
            StatusCode::FORBIDDEN,
            message_page(
                &form.title,
                "An answer from another site is not taken: open the form and send it from there.",
            ),
        );
    }
    let given = match given_of(call) {
        Ok(g) => g,
        Err(why) => return page(StatusCode::BAD_REQUEST, &[], &[(String::new(), why)]),
    };
    let values = texts(&given);
    if !page_token_ok(state, &target, &text_of(&given, "_form")) {
        let again = "This page was open too long, or came from elsewhere: please send it again.".to_owned();
        return page(StatusCode::BAD_REQUEST, &values, &[(String::new(), again)]);
    }
    let answers = match openduct_flow::forms::check(&form, &given) {
        Ok(a) => a,
        Err(problems) => return page(StatusCode::UNPROCESSABLE_ENTITY, &values, &problems),
    };
    match state.waits.answer(token, "answer", &answers).await {
        Ok(Answered::Taken) => html(StatusCode::OK, thanks_page(&form)),
        Ok(Answered::Already(_)) => html(
            StatusCode::CONFLICT,
            message_page(
                &form.title,
                "This form has had its answer already: yours was not taken.",
            ),
        ),
        Ok(Answered::Unknown) => gone(),
        Err(e) => {
            tracing::warn!(error = e, "an answer to a form could not be kept");
            html(
                StatusCode::SERVICE_UNAVAILABLE,
                message_page(
                    &form.title,
                    "The answer could not be kept just now: please send it again in a moment.",
                ),
            )
        }
    }
}

/// A call to a wait for a call: what it sent is the step's result.
async fn called(state: &State, call: &Call, token: &str, summary: &openduct_flow::waits::Summary) -> Answer {
    if call.method == Method::HEAD || call.method == Method::OPTIONS {
        return problem(
            StatusCode::METHOD_NOT_ALLOWED,
            "call this address with GET, POST, PUT, PATCH or DELETE",
        );
    }
    if summary.answered.is_some() {
        return problem(StatusCode::CONFLICT, "this address has had its call already");
    }
    let body = if call.content_type.contains("json") {
        call.body.clone()
    } else if call.content_type.starts_with("application/x-www-form-urlencoded") {
        Value::Object(crate::hooks::form(&String::from_utf8_lossy(&call.raw)))
    } else if call.content_type.starts_with("multipart/form-data") {
        Value::Object(crate::hooks::multipart(&call.content_type, &call.raw))
    } else if call.raw.is_empty() {
        Value::Null
    } else {
        Value::String(String::from_utf8_lossy(&call.raw).into_owned())
    };
    let headers: serde_json::Map<String, Value> = call
        .headers
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "authorization" | "cookie") && !k.starts_with("x-openduct-"))
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let payload = json!({
        "method": call.method.as_str(),
        "body": body,
        "query": Value::Object(crate::hooks::form(&call.query)),
        "headers": headers,
    });
    match state.waits.answer(token, "answer", &payload).await {
        Ok(Answered::Taken) => json(StatusCode::OK, &json!({ "resumed": true, "run": summary.run })),
        Ok(Answered::Already(_)) => problem(StatusCode::CONFLICT, "this address has had its call already"),
        Ok(Answered::Unknown) => problem(StatusCode::NOT_FOUND, "nothing waits at this address"),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// The runs of a workflow that wait, with their addresses.
async fn waiting(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = workflow_in(state, &access, id).await {
        return answer;
    }
    match state.waits.waiting(id).await {
        Ok(found) => {
            let shown: Vec<Value> = found
                .into_iter()
                .map(|w| {
                    let url = (w.kind != "time").then(|| state.waits.address(&w.token));
                    json!({
                        "run": w.run, "step": w.step, "kind": w.kind, "title": w.title, "since": w.since,
                        "until": w.until, "on_timeout": w.on_timeout, "answered": w.answered, "url": url,
                    })
                })
                .collect();
            json(StatusCode::OK, &json!({ "waiting": shown }))
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// What a form's answer sent, field by field.
fn given_of(call: &Call) -> Result<Vec<(String, Given)>, String> {
    if call.content_type.starts_with("multipart/form-data") {
        return parts(&call.content_type, &call.raw);
    }
    if call.content_type.starts_with("application/x-www-form-urlencoded") {
        let text = String::from_utf8_lossy(&call.raw);
        let pairs: Vec<(String, Given)> = text
            .split('&')
            .filter(|p| !p.is_empty())
            .take(MAX_PARTS + 1)
            .map(|pair| {
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                (crate::hooks::decode(k), Given::Text(crate::hooks::decode(v)))
            })
            .collect();
        if pairs.len() > MAX_PARTS {
            return Err("An answer of too many parts.".to_owned());
        }
        return Ok(pairs);
    }
    Err("Send the form from its page.".to_owned())
}

/// The text values of an answer, to show them again.
fn texts(given: &[(String, Given)]) -> Vec<(String, String)> {
    given
        .iter()
        .filter(|(n, _)| !n.starts_with('_') || n == "_comment")
        .filter_map(|(n, g)| match g {
            Given::Text(t) => Some((n.clone(), t.chars().take(100_000).collect())),
            Given::File { .. } => None,
        })
        .collect()
}

fn text_of(given: &[(String, Given)], name: &str) -> String {
    given
        .iter()
        .find_map(|(n, g)| match g {
            Given::Text(t) if n == name => Some(t.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// Where `needle` first is in `hay`, from `from`.
fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    let first = *needle.first()?;
    let mut at = from;
    while let Some(rest) = hay.get(at..) {
        let i = rest.iter().position(|b| *b == first)?;
        let start = at + i;
        if hay.get(start..start + needle.len()) == Some(needle) {
            return Some(start);
        }
        at = start + 1;
    }
    None
}

/// A multipart answer's parts, files whole.
fn parts(content_type: &str, raw: &[u8]) -> Result<Vec<(String, Given)>, String> {
    let boundary = content_type
        .split(';')
        .find_map(|p| p.trim().strip_prefix("boundary="))
        .map(|b| b.trim_matches('"'))
        .filter(|b| !b.is_empty() && b.len() <= 200)
        .ok_or("Send the form from its page.")?;
    let delimiter = format!("--{boundary}").into_bytes();
    let mut out = Vec::new();
    let Some(mut at) = find(raw, &delimiter, 0).map(|i| i + delimiter.len()) else {
        return Err("Send the form from its page.".to_owned());
    };
    loop {
        let rest = raw.get(at..).unwrap_or_default();
        if rest.starts_with(b"--") || rest.is_empty() {
            break;
        }
        let start = at + if rest.starts_with(b"\r\n") { 2 } else { 0 };
        let Some(end) = find(raw, &delimiter, start) else {
            break;
        };
        let part = raw.get(start..end).unwrap_or_default();
        let part = part.strip_suffix(b"\r\n").unwrap_or(part);
        at = end + delimiter.len();
        let Some(split) = find(part, b"\r\n\r\n", 0) else {
            continue;
        };
        let head = String::from_utf8_lossy(part.get(..split).unwrap_or_default()).into_owned();
        let body = part.get(split + 4..).unwrap_or_default();
        let header = |name: &str| {
            head.split("\r\n")
                .find(|l| l.to_ascii_lowercase().starts_with(name))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().to_owned())
        };
        let Some(disposition) = header("content-disposition:") else {
            continue;
        };
        let param = |key: &str| {
            disposition
                .split(';')
                .find_map(|p| p.trim().strip_prefix(key).and_then(|v| v.strip_prefix('=')))
                .map(|v| v.trim_matches('"').to_owned())
        };
        let Some(name) = param("name") else { continue };
        let given = match param("filename") {
            Some(file) => Given::File {
                name: file,
                content_type: header("content-type:")
                    .unwrap_or_else(|| "application/octet-stream".to_owned()),
                bytes: body.to_vec(),
            },
            None => Given::Text(String::from_utf8_lossy(body).into_owned()),
        };
        out.push((name, given));
        if out.len() > MAX_PARTS {
            return Err("An answer of too many parts.".to_owned());
        }
    }
    Ok(out)
}

/// Equal, in a time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]

    use super::*;

    #[test]
    fn a_multipart_answer_keeps_a_file_byte_for_byte() {
        let mut raw = b"--b0\r\nContent-Disposition: form-data; name=\"_form\"\r\n\r\n1.x\r\n\
                        --b0\r\nContent-Disposition: form-data; name=\"cv\"; filename=\"cv.pdf\"\r\n\
                        Content-Type: application/pdf\r\n\r\n"
            .to_vec();
        raw.extend_from_slice(&[0, 255, 13, 10, 45, 45, 1]);
        raw.extend_from_slice(
            b"\r\n--b0\r\nContent-Disposition: form-data; name=\"note\"\r\n\r\nhi\r\n--b0--\r\n",
        );
        let got = parts("multipart/form-data; boundary=b0", &raw).unwrap();
        assert_eq!(got.len(), 3);
        assert!(matches!(&got[0].1, Given::Text(t) if t == "1.x"));
        let Given::File {
            name,
            content_type,
            bytes,
        } = &got[1].1
        else {
            panic!("a file")
        };
        assert_eq!(
            (name.as_str(), content_type.as_str()),
            ("cv.pdf", "application/pdf")
        );
        assert_eq!(bytes, &[0, 255, 13, 10, 45, 45, 1]);
        assert!(matches!(&got[2].1, Given::Text(t) if t == "hi"));
        assert!(parts("multipart/form-data", &raw).is_err());
    }

    #[test]
    fn an_address_takes_a_few_answers_a_minute() {
        let guard = Guard::default();
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert!((0..3).all(|_| guard.allows(ip, "f", 3)));
        assert!(!guard.allows(ip, "f", 3));
        assert!(guard.allows(ip, "another form", 3));
        assert!(guard.allows("203.0.113.10".parse().unwrap(), "f", 3));
        assert!((0..100).all(|_| guard.allows(ip, "open", 0)));
    }

    #[test]
    fn needles_are_found_where_they_are() {
        assert_eq!(find(b"abcabd", b"abd", 0), Some(3));
        assert_eq!(find(b"abcabd", b"abd", 4), None);
        assert_eq!(find(b"", b"x", 0), None);
        assert!(is_outside("/forms/abc") && is_outside("/waits/x") && !is_outside("/api/forms"));
    }
}
