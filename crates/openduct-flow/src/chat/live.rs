// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An answer shown as it is made. The chat that took a message listens under the message's `reply` id; the run's
//! streamed step asks its model for events ([`ask_streaming`]) and tells what each adds to whoever listens on this
//! node. A run on another node of a cluster has no listener here: its answer is shown whole when it is done, as it
//! always is in the end - what is streamed only shows the answer sooner.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use openduct_expr::Value;
use serde_json::{Value as Json, json};
use tokio::sync::watch;

use super::sse::Reader;
use crate::apps::Credential;
use crate::run::{Effects, HttpRequest};

/// The most chats listening at once on a node; past it, answers are shown whole.
const MAX_LISTENING: usize = 4_096;
/// The most text told to a listener: past it, the rest is shown with the whole answer.
const MAX_TOLD: usize = 256 * 1024;

/// What a step has said so far.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Partial {
    /// Counted up each time the step begins again (a retry): what was shown before is to be cleared.
    pub attempt: u32,
    pub text: String,
}

type Listeners = Mutex<HashMap<String, watch::Sender<Partial>>>;

fn listeners() -> &'static Listeners {
    static LISTENERS: OnceLock<Listeners> = OnceLock::new();
    LISTENERS.get_or_init(Listeners::default)
}

/// A chat listening for an answer as it is made; it stops listening when dropped.
#[derive(Debug)]
pub struct Listening {
    reply: String,
    pub receiver: watch::Receiver<Partial>,
}

impl Drop for Listening {
    fn drop(&mut self) {
        if let Ok(mut held) = listeners().lock() {
            held.remove(&self.reply);
        }
    }
}

/// Listens for what the step answering `reply` says as it says it. Past [`MAX_LISTENING`] at once, nothing is told.
pub fn listen(reply: &str) -> Listening {
    let (sender, receiver) = watch::channel(Partial::default());
    if let Ok(mut held) = listeners().lock()
        && held.len() < MAX_LISTENING
    {
        held.insert(reply.to_owned(), sender);
    }
    Listening {
        reply: reply.to_owned(),
        receiver,
    }
}

fn tell(reply: &str, change: impl FnOnce(&mut Partial)) {
    if let Ok(held) = listeners().lock()
        && let Some(sender) = held.get(reply)
    {
        sender.send_modify(change);
    }
}

/// The `reply` a chat listens under, when this step of this run is the one its chat streams: a run of a chat (its
/// `trigger.stream` names the step), and an `ai` step.
pub fn streams(details: &Value, step: &str, app: &str) -> Option<String> {
    let Value::Record(details) = details else {
        return None;
    };
    let named = |name: &str| match details.get(name) {
        Some(Value::Text(t)) => Some(t.to_string()),
        _ => None,
    };
    (app == "ai" && named("trigger").as_deref() == Some("chat") && named("stream").as_deref() == Some(step))
        .then(|| named("reply"))
        .flatten()
}

/// An `ai` step's request, asked for as events: each piece of the answer told to the chat listening under `reply`,
/// and the whole answer given back as the request would have had it without streaming. A service that refuses to
/// stream is asked again plainly.
pub async fn ask_streaming(
    effects: &dyn Effects,
    request: HttpRequest,
    credential: &Credential,
    reply: &str,
) -> Result<Value, String> {
    let messages = credential
        .fields
        .get("api")
        .is_some_and(|a| a.trim() == "messages");
    let Some(streamed) = streamed(&request, messages) else {
        return effects.http(request).await;
    };
    tell(reply, |p| {
        p.attempt += 1;
        p.text.clear();
    });
    let mut reader = Reader::new(messages);
    let answer = {
        let reader = &mut reader;
        let part = Box::new(move |bytes: &[u8]| {
            let added = reader.feed(bytes);
            if !added.is_empty() {
                tell(reply, |p| {
                    if p.text.len() + added.len() <= MAX_TOLD {
                        p.text.push_str(&added);
                    }
                });
            }
        });
        effects.http_parts(streamed, part).await?
    };
    let json: Json = serde_json::from_str(&answer.to_json()).unwrap_or(Json::Null);
    let status = json.get("status").and_then(Json::as_u64).unwrap_or(0);
    if !(200..300).contains(&status) {
        // Refused (a setting it does not take, streaming itself): asked as the step always asks.
        return effects.http(request).await;
    }
    if reader.fed == 0
        && let Some(Json::String(text)) = json.get("body")
    {
        // These effects gave the answer whole: it is read now.
        let added = reader.feed(text.as_bytes());
        tell(reply, |p| p.text.push_str(&added));
    }
    let rest = reader.finish();
    if !rest.is_empty() {
        tell(reply, |p| p.text.push_str(&rest));
    }
    if let Some(error) = &reader.error {
        return Err(format!("the model stopped: {error}"));
    }
    let Some(body) = reader.body() else {
        // Not events: a service that answered whole although asked to stream.
        return Ok(answer);
    };
    Ok(Value::from_json(&json!({
        "status": status,
        "headers": json.get("headers").cloned().unwrap_or(Json::Null),
        "body": body,
    })))
}

/// The request with streaming asked for; `None` when its body is not the JSON an `ai` step sends.
fn streamed(request: &HttpRequest, messages: bool) -> Option<HttpRequest> {
    let (kind, bytes) = request.body.as_ref()?;
    let mut body: Json = serde_json::from_slice(bytes).ok()?;
    let fields = body.as_object_mut()?;
    fields.insert("stream".to_owned(), json!(true));
    if !messages {
        // The token counts come in a last event of their own.
        fields.insert("stream_options".to_owned(), json!({ "include_usage": true }));
    }
    let mut out = request.clone();
    out.body = Some((kind.clone(), body.to_string().into_bytes()));
    out.headers
        .push(("accept".to_owned(), "text/event-stream".to_owned()));
    Some(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn only_the_step_a_chat_names_streams() {
        let details = Value::from_json(&json!({ "trigger": "chat", "reply": "r-1", "stream": "answer" }));
        assert_eq!(streams(&details, "answer", "ai").as_deref(), Some("r-1"));
        assert_eq!(streams(&details, "classify", "ai"), None);
        assert_eq!(streams(&details, "answer", "embed"), None);
        let webhook = Value::from_json(&json!({ "trigger": "webhook", "reply": "r-1", "stream": "answer" }));
        assert_eq!(streams(&webhook, "answer", "ai"), None);
        assert_eq!(streams(&Value::Null, "answer", "ai"), None);
    }

    #[test]
    fn a_listener_hears_what_is_told_and_stops_hearing_when_dropped() {
        let listening = listen("reply-a");
        tell("reply-a", |p| p.text.push_str("Hel"));
        tell("reply-a", |p| p.text.push_str("lo"));
        tell("reply-b", |p| p.text.push_str("not for a"));
        assert_eq!(listening.receiver.borrow().text, "Hello");
        drop(listening);
        assert!(!listeners().lock().unwrap().contains_key("reply-a"));
    }

    #[test]
    fn streaming_is_asked_for_in_the_request_s_own_shape() {
        let request = HttpRequest {
            read: true,
            method: "POST".to_owned(),
            url: "https://models.example/v1/chat/completions".to_owned(),
            headers: Vec::new(),
            query: Vec::new(),
            body: Some((
                "application/json".to_owned(),
                br#"{"model":"m","messages":[]}"#.to_vec(),
            )),
            timeout: std::time::Duration::from_secs(5),
        };
        let asked: Json =
            serde_json::from_slice(&streamed(&request, false).unwrap().body.unwrap().1).unwrap();
        assert_eq!(asked["stream"], true);
        assert_eq!(asked["stream_options"]["include_usage"], true);
        let asked: Json = serde_json::from_slice(&streamed(&request, true).unwrap().body.unwrap().1).unwrap();
        assert!(asked.get("stream_options").is_none());
        let none = HttpRequest {
            body: None,
            ..request
        };
        assert!(streamed(&none, false).is_none());
    }
}
