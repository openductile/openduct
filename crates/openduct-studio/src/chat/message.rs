// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A message to a chat, `POST /chat/<hook>/messages` with `{"message", "session"?, "person"?, "password"?}`: checked,
//! limited, kept on the workflow's topic for a run (keyed by its conversation, so one conversation's messages run in
//! order), and answered with the run's answer - whole, as JSON, or, when the caller takes `text/event-stream`, as
//! events: `start`, then `text` as the answer is made (and `reset` when its step starts again), then `answer` (or
//! `error`).

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame};
use hyper::header::HeaderValue;
use hyper::{Response, StatusCode};
use openduct_broker::client::{Outgoing, records};
use openduct_flow::chat::{self, Answer, Listening};
use openduct_flow::runner::{self, Topics};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;

use super::sessions::Waiting;
use super::{Found, boxed, cors, same};
use crate::State;
use crate::http::{Call, Streamed, json as answer_json, problem};
use crate::model::new_id;

/// The largest message request, in bytes.
const MAX_REQUEST: usize = 64 * 1024;
/// How often a stream of events says it is still there, through proxies that close quiet connections.
const PING: Duration = Duration::from_secs(15);

/// Takes a message for the chat `found`, from a caller whose site `origin` the chat allows (or its own page).
pub(super) async fn take(state: &State, call: &Call, found: Found, origin: Option<String>) -> Streamed {
    let refuse = |status: StatusCode, why: &str| cors(boxed(problem(status, why)), origin.as_deref());
    if !found.record.active {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "this chat is closed for now");
    }
    let chat = &found.chat;
    let id = &found.record.id;
    let limits = &state.chat;
    if !limits
        .rates
        .allow(&format!("{id}|a|{}", call.peer), chat.per_address)
    {
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "too many messages from here: wait a minute",
        );
    }
    if call.raw.len() > MAX_REQUEST {
        return refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("a message request of at most {MAX_REQUEST} bytes"),
        );
    }
    let Ok(body) = serde_json::from_slice::<Json>(&call.raw) else {
        return refuse(
            StatusCode::BAD_REQUEST,
            r#"a message is sent as JSON: {"message": "..."}"#,
        );
    };
    if let Some(password) = &chat.password {
        if !limits.passwords.may_try(call.peer) {
            return refuse(
                StatusCode::TOO_MANY_REQUESTS,
                "too many wrong passwords: wait a minute",
            );
        }
        let header = |name: &str| {
            call.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let given = body
            .get("password")
            .and_then(Json::as_str)
            .or_else(|| header("x-openduct-token"))
            .or_else(|| header("authorization").and_then(|a| a.strip_prefix("Bearer ")))
            .unwrap_or_default();
        if !same(given.as_bytes(), password.as_bytes()) {
            limits.passwords.failed(call.peer);
            return refuse(
                StatusCode::UNAUTHORIZED,
                "the chat's password is missing or wrong",
            );
        }
    }
    let message = match chat::message_of(body.get("message"), chat.max_message) {
        Ok(m) => m,
        Err(e) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let session = match body.get("session").and_then(Json::as_str) {
        Some(s) if chat::session_ok(s) => s.to_owned(),
        Some(_) => {
            return refuse(
                StatusCode::UNPROCESSABLE_ENTITY,
                "session: 16 to 128 letters, digits, '-', '_', '.' or ':'",
            );
        }
        None => new_id(),
    };
    let person = match chat::person_of(body.get("person")) {
        Ok(p) => p,
        Err(e) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    if !limits.rates.allow(&format!("{id}|s|{session}"), chat.per_session) {
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "too many messages in this conversation: wait a minute",
        );
    }
    let follower = match limits.conversations.follower(&state.shared, id).await {
        Ok(f) => f,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let history = follower.history(&session, chat.history);
    let reply = new_id();
    let streams = call
        .headers
        .iter()
        .any(|(k, v)| k == "accept" && v.contains("text/event-stream"));
    let step = if streams {
        chat.streamed_step(&found.workflow.steps)
    } else {
        None
    };
    let listening = step.as_ref().map(|_| chat::listen(&reply));
    let Some(waiting) = follower.wait(&reply) else {
        return refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "the chat is busy: try again in a moment",
        );
    };
    let value = chat::event_value(&message, &session, &history, &person);
    let details = chat::event_details(&session, &reply, step.as_deref(), &runner::iso(now_ms()));
    let run = match queue(state, id, &session, &value, &details).await {
        Ok(run) => run,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let said = Said {
        session,
        run,
        error: chat.texts.error.clone(),
    };
    if streams {
        return cors(events(waiting, listening, chat.wait, said), origin.as_deref());
    }
    cors(whole(waiting, chat.wait, &said).await, origin.as_deref())
}

/// What every answer to a message names.
struct Said {
    session: String,
    run: String,
    /// The trigger's words for a run that failed.
    error: String,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

/// The message kept on the workflow's topic, keyed by its conversation: the id its run will have.
async fn queue(
    state: &State,
    id: &str,
    session: &str,
    value: &Json,
    details: &Json,
) -> Result<String, String> {
    let topics = Topics::of(id);
    let unreachable =
        |e: openduct_broker::client::ClientError| format!("the chat cannot keep the message: {e}");
    let mut client = state.shared.lend().await.map_err(unreachable)?;
    let partitions = client.partition_count(&topics.input).await.map_err(unreachable)?;
    // Not the schedule's partition (0), as every other run: one conversation always in the same one, so in order.
    let partition = if partitions > 1 {
        1 + records::partition_for(session.as_bytes(), partitions - 1)
    } else {
        0
    };
    let message = Outgoing {
        key: Some(Bytes::from(session.as_bytes().to_vec())),
        value: Some(Bytes::from(runner::envelope(value, details))),
        headers: Vec::new(),
    };
    let offset = client
        .produce(&topics.input, partition, &[message], now_ms())
        .await
        .map_err(unreachable)?;
    Ok(format!("{partition}-{offset}"))
}

/// What the chat shows of a finished run: the event's name and its data.
fn shown(run: Option<Json>, said: &Said) -> (&'static str, StatusCode, Json) {
    let Some(run) = run else {
        return (
            "error",
            StatusCode::GATEWAY_TIMEOUT,
            json!({ "error": "no answer in time", "session": said.session, "run": said.run }),
        );
    };
    match chat::answer_of(&run) {
        Answer::Said { text, data } => {
            let mut out = json!({ "answer": text, "session": said.session, "run": said.run });
            if let (Some(data), Some(fields)) = (data, out.as_object_mut()) {
                fields.insert("data".to_owned(), data);
            }
            ("answer", StatusCode::OK, out)
        }
        Answer::Nothing => (
            "answer",
            StatusCode::OK,
            json!({ "answer": "", "session": said.session, "run": said.run }),
        ),
        Answer::Failed => (
            "error",
            StatusCode::BAD_GATEWAY,
            json!({ "error": said.error, "session": said.session, "run": said.run }),
        ),
    }
}

/// The answer, whole, as JSON.
async fn whole(mut waiting: Waiting, wait: Duration, said: &Said) -> Streamed {
    let run = tokio::time::timeout(wait, &mut waiting.answer)
        .await
        .ok()
        .and_then(Result::ok);
    let (_, status, body) = shown(run, said);
    boxed(answer_json(status, &body))
}

/// A body sent as it is made: what the channel is given, in order, until it is closed.
struct Parts(mpsc::Receiver<Bytes>);

impl Body for Parts {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.get_mut()
            .0
            .poll_recv(cx)
            .map(|part| part.map(|b| Ok(Frame::data(b))))
    }
}

fn event(name: &str, data: &Json) -> Bytes {
    Bytes::from(format!("event: {name}\ndata: {data}\n\n"))
}

/// The answer as events, as it is made.
fn events(waiting: Waiting, listening: Option<Listening>, wait: Duration, said: Said) -> Streamed {
    let (sender, receiver) = mpsc::channel(64);
    tokio::spawn(tell(sender, waiting, listening, wait, said));
    let mut response = Response::new(Parts(receiver).boxed());
    let headers = response.headers_mut();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    headers.insert(hyper::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

/// Tells the caller, as events, what the streamed step says and then the run's answer; stops when the caller goes.
async fn tell(
    sender: mpsc::Sender<Bytes>,
    mut waiting: Waiting,
    mut listening: Option<Listening>,
    wait: Duration,
    said: Said,
) {
    if sender
        .send(event(
            "start",
            &json!({ "session": said.session, "run": said.run }),
        ))
        .await
        .is_err()
    {
        return;
    }
    let deadline = tokio::time::sleep(wait);
    tokio::pin!(deadline);
    let mut ping = tokio::time::interval(PING);
    ping.tick().await;
    let (mut attempt, mut told) = (0u32, 0usize);
    loop {
        let changed = async {
            match listening.as_mut() {
                Some(l) => l.receiver.changed().await.is_ok(),
                None => std::future::pending().await,
            }
        };
        let next = tokio::select! {
            run = &mut waiting.answer => Next::Done(run.ok()),
            () = &mut deadline => Next::Done(None),
            () = sender.closed() => return,
            _ = ping.tick() => Next::Ping,
            heard = changed => Next::Heard(heard),
        };
        let part = match next {
            Next::Done(run) => {
                let (name, _, data) = shown(run, &said);
                let _ = sender.send(event(name, &data)).await;
                return;
            }
            Next::Ping => Bytes::from_static(b": still here\n\n"),
            Next::Heard(false) => {
                listening = None;
                continue;
            }
            Next::Heard(true) => {
                let Some(l) = listening.as_mut() else { continue };
                let out = {
                    let partial = l.receiver.borrow_and_update();
                    let mut out = String::new();
                    if partial.attempt != attempt {
                        attempt = partial.attempt;
                        told = 0;
                        out.push_str("event: reset\ndata: {}\n\n");
                    }
                    if let Some(new) = partial.text.get(told..).filter(|t| !t.is_empty()) {
                        out.push_str(&format!("event: text\ndata: {}\n\n", json!({ "text": new })));
                        told = partial.text.len();
                    }
                    out
                };
                if out.is_empty() {
                    continue;
                }
                Bytes::from(out)
            }
        };
        if sender.send(part).await.is_err() {
            return;
        }
    }
}

/// What woke the events up.
enum Next {
    /// The run's record, or none in time.
    Done(Option<Json>),
    Ping,
    /// The streamed step said more (false: it will say no more here).
    Heard(bool),
}
