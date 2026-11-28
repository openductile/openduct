// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow's chat (its trigger is `chat`), at `/chat/<hook>` - the workflow's random address, as a webhook's is:
//!
//! - `GET /chat/<hook>`: the chat's page, made here (its texts and colours the trigger's), with nothing from
//!   anywhere else; framed only by the studio and the sites the trigger names (`origins`);
//! - `GET /chat/<hook>/embed.js`: a script a site puts on its pages for a chat button that opens the page;
//! - `POST /chat/<hook>/messages`: a message, as JSON - from the page, a site the trigger names, or a front end's
//!   server - answered whole, or as events as the answer is made ([`message`]).
//!
//! Nothing here uses the studio's sign-in: no cookie is read or set, so a site cannot act for anyone through a
//! visitor's browser. A message from a browser must come from the chat's own page or a site the trigger names, as
//! JSON (which another site cannot send without asking first, and is told no); the trigger's password, when it has
//! one, comes with every message; messages are limited per conversation and per address.
//!
//! `POST /api/workflows/<id>/chat` is the studio's "try the chat": the editor's workflow run as a preview on a
//! message and the conversation so far (kept by the panel), its answer and its run given back.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::HeaderValue;
use hyper::{Method, Response, StatusCode};
use openduct_flow::chat::{self, Answer, Chat};
use openduct_flow::{Event, Trigger, Workflow};
use serde_json::{Value, json};

use crate::State;
use crate::api::access;
use crate::http::{Answer as Whole, Call, Streamed, json as answer_json, problem};
use crate::model::{Role, WorkflowRecord, new_id};
use crate::workflows::{definition_of, workflow_in};

mod limits;
mod message;
mod page;
mod sessions;

/// What chats keep on this node: their conversations, and their limits' counts.
#[derive(Debug, Default)]
pub(crate) struct Chats {
    rates: limits::Rates,
    /// Wrong passwords, by address.
    passwords: crate::auth::Failures,
    conversations: sessions::Conversations,
}

/// A workflow's chat, found by its address.
struct Found {
    record: WorkflowRecord,
    workflow: Workflow,
    chat: Chat,
}

pub(crate) fn boxed(answer: Whole) -> Streamed {
    answer.map(BodyExt::boxed)
}

/// An answer that a site the chat allows may read: `origin` is that site, when it is one.
fn cors(mut answer: Streamed, origin: Option<&str>) -> Streamed {
    if let Some(Ok(value)) = origin.map(HeaderValue::from_str) {
        let headers = answer.headers_mut();
        headers.insert("access-control-allow-origin", value);
        headers.insert("vary", HeaderValue::from_static("origin"));
    }
    answer
}

pub(crate) async fn route(state: &State, call: Call) -> Streamed {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    let (hook, rest) = match segments.as_slice() {
        ["chat", hook] => (*hook, ""),
        ["chat", hook, rest] => (*hook, *rest),
        _ => return boxed(problem(StatusCode::NOT_FOUND, "no such chat")),
    };
    let Some(found) = find(state, hook).await else {
        return boxed(problem(StatusCode::NOT_FOUND, "no such chat"));
    };
    let read = matches!(call.method, Method::GET | Method::HEAD);
    match (rest, read) {
        ("", true) => page::page(&found, hook, query_has(&call.query, "embed", "1")),
        ("chat.js", true) => page::script(),
        ("chat.css", true) => page::style(&found.chat),
        ("embed.js", true) => page::embed(&found.chat),
        ("messages", false) => {
            let origin = match origin_of(&call, &found.chat) {
                Ok(o) => o,
                Err(refused) => return refused,
            };
            if call.method == Method::OPTIONS {
                return preflight(origin.as_deref());
            }
            if call.method != Method::POST {
                return boxed(problem(StatusCode::METHOD_NOT_ALLOWED, "a message is a POST"));
            }
            message::take(state, &call, found, origin).await
        }
        _ => boxed(problem(StatusCode::NOT_FOUND, "no such chat")),
    }
}

fn query_has(query: &str, name: &str, value: &str) -> bool {
    query
        .split('&')
        .any(|pair| pair.split_once('=') == Some((name, value)))
}

/// The workflow whose chat this is: one not deleted, whose trigger is `chat`.
async fn find(state: &State, hook: &str) -> Option<Found> {
    if hook.is_empty() || hook.len() > 64 {
        return None;
    }
    let record = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .find(|w| w.hook == hook && w.deleted.is_none())?;
    let definition = definition_of(state, &record).await;
    let workflow = crate::custom_apps::workflow(&record.workspace, &definition, Some(&record.id)).ok()?;
    let Trigger::Chat(chat) = &workflow.trigger else {
        return None;
    };
    let chat = (**chat).clone();
    Some(Found {
        record,
        workflow,
        chat,
    })
}

/// Who sent a message: `None` for the chat's own page or a caller that is not a browser (no `Origin`), the site when
/// it is one the chat allows; refused when it is any other.
fn origin_of(call: &Call, chat: &Chat) -> Result<Option<String>, Streamed> {
    let Some(origin) = call
        .headers
        .iter()
        .find(|(k, _)| k == "origin")
        .map(|(_, v)| v.trim().to_owned())
    else {
        return Ok(None);
    };
    let own = origin
        .split_once("://")
        .is_some_and(|(_, host)| host.eq_ignore_ascii_case(&call.host));
    if own {
        return Ok(None);
    }
    if origin != "null" && chat.allows(&origin) {
        return Ok(Some(origin));
    }
    Err(boxed(problem(
        StatusCode::FORBIDDEN,
        "this chat takes no messages from that site: add it to the chat's sites",
    )))
}

/// The answer to a browser asking whether a site may send messages.
fn preflight(origin: Option<&str>) -> Streamed {
    let mut answer = Response::new(Full::new(Bytes::new()));
    *answer.status_mut() = StatusCode::NO_CONTENT;
    let headers = answer.headers_mut();
    headers.insert("access-control-allow-methods", HeaderValue::from_static("POST"));
    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("content-type, accept, authorization, x-openduct-token"),
    );
    headers.insert("access-control-max-age", HeaderValue::from_static("600"));
    cors(boxed(answer), origin)
}

/// Equal, in a time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The chat's address, for a workflow whose trigger is a chat.
pub(crate) fn address(definition: &Value, record: &WorkflowRecord) -> Option<String> {
    crate::custom_apps::workflow(&record.workspace, definition, Some(&record.id))
        .ok()
        .filter(|w| matches!(w.trigger, Trigger::Chat(_)))
        .map(|_| format!("/chat/{}", record.hook))
}

/// "Try the chat": the editor's workflow (or the newest kept) run as a preview - the model asked for real, writes
/// only shown - on `{message, history, session?, person?}`; its answer and its run given back.
pub(crate) async fn try_it(state: &State, call: &Call, id: &str) -> Whole {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let definition = match call.body.get("definition") {
        Some(d) => d.clone(),
        None => definition_of(state, &record).await,
    };
    let workflow = match crate::custom_apps::workflow(&record.workspace, &definition, Some(id)) {
        Ok(w) => w,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let Trigger::Chat(chat) = &workflow.trigger else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "this workflow does not start with a chat",
        );
    };
    let checked = (|| {
        let message = chat::message_of(call.body.get("message"), chat.max_message)?;
        let history = chat::history_from(call.body.get("history").unwrap_or(&Value::Null), chat.history)?;
        let person = chat::person_of(call.body.get("person"))?;
        Ok::<_, String>((message, history, person))
    })();
    let (message, history, person) = match checked {
        Ok(c) => c,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let session = match call.body.get("session").and_then(Value::as_str) {
        Some(s) if chat::session_ok(s) => s.to_owned(),
        _ => format!("try-{}", new_id()),
    };
    let at = openduct_flow::runner::iso(crate::workflows::now_ms());
    let mut details = chat::event_details(&session, &new_id(), None, &at);
    if let Some(fields) = details.as_object_mut() {
        fields.insert("try".to_owned(), json!(true));
    }
    let event = Event {
        value: openduct_expr::Value::from_json(&chat::event_value(&message, &session, &history, &person)),
        details: openduct_expr::Value::from_json(&details),
    };
    let live = match state.effects_for(&access.workspace.id) {
        Ok(e) => e,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let effects = openduct_flow::Preview::new(live);
    let ran = openduct_flow::run(&workflow, &event, &effects).await.to_json();
    let (answer, failed) = match chat::answer_of(&ran) {
        Answer::Said { text, .. } => (Some(text), false),
        Answer::Nothing => (None, false),
        Answer::Failed => (None, true),
    };
    answer_json(
        StatusCode::OK,
        &json!({
            "answer": answer, "failed": failed, "error_text": chat.texts.error,
            "session": session, "run": ran,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_compare_whole_and_a_query_is_read_by_its_pairs() {
        assert!(same(b"abc", b"abc") && !same(b"abc", b"abd") && !same(b"abc", b"ab"));
        assert!(query_has("embed=1&x=2", "embed", "1"));
        assert!(!query_has("embedded=1", "embed", "1") && !query_has("", "embed", "1"));
    }
}
