// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The chat trigger: a conversation, each message a run. The studio serves the chat - a page of its own, a script tag
//! or an iframe for a site, and JSON for a front end of one's own (see the studio's `chat` module); this module holds
//! what the workflow says about it, what a run of it is given, what its answer is, and how an AI step's answer is
//! shown as it is made.
//!
//! A run of a chat is given, as `value`:
//!
//! - `message`: what the person wrote;
//! - `session`: the conversation's id - the same for every message of one conversation, kept by the chat;
//! - `history`: the conversation's earlier messages, oldest first, as an AI step's `messages` takes them
//!   (`[{role: user|assistant, content}]`), at most the trigger's `history` turns and [`HISTORY_BYTES`];
//! - `person`: what the site or the front end said about the person (`name`, `email`, ...), or `{}`.
//!
//! and, as `trigger`: `{trigger: "chat", session, at, reply}` (`reply` names this message's answer; `stream`, the
//! step whose answer is shown as it is made, when the chat asked for that).
//!
//! **The session id is the conversation's key**: what the chat keeps, what its earlier messages are found by, and what
//! an agent's memory keys a conversation by. It is `value.session` (and `trigger.session`).
//!
//! The answer the chat shows is the run's: its last respond step's body (its `text`, `answer` or `message` field, or
//! the body itself when it is text), or else its last step's `text`, `answer`, `message`, `output` or `reply` (an `ai`
//! step's `text`). A run that failed shows the trigger's error text, never the error itself.

use std::time::Duration;

use serde_json::{Map, Value as Json, json};

use crate::model::{Kind, Step};

mod live;
mod sse;

pub use live::{Listening, Partial, ask_streaming, listen, streams};

/// What is handed each part of an answer as it comes ([`crate::Effects::http_parts`]).
pub(crate) type Parts<'a> = Box<dyn FnMut(&[u8]) + Send + 'a>;

/// The most turns of a conversation a run is given.
pub const MAX_HISTORY: usize = 50;
/// The most bytes (as JSON text) of earlier messages a run is given: older turns are left out first.
pub const HISTORY_BYTES: usize = 24 * 1024;
/// The most bytes a message holds, whatever its number of characters.
pub const MESSAGE_BYTES: usize = 16 * 1024;
/// The longest message a trigger may allow, in characters.
pub const MAX_MESSAGE: usize = 16_000;
/// The longest answer a chat shows, in characters: a longer one is cut short.
pub const MAX_ANSWER: usize = 32_000;
/// The longest a chat may wait for its answer.
pub const MAX_WAIT: Duration = Duration::from_secs(300);
/// The most fields of a person's details, each name and text at most so long.
const PERSON_FIELDS: usize = 20;
const PERSON_NAME: usize = 64;
const PERSON_TEXT: usize = 1_000;
/// The most sites a chat may be put on.
const MAX_ORIGINS: usize = 20;

/// What a chat trigger says: its page's texts and colours, the sites it may be put on, its password, how much of a
/// conversation a run is given, and its limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chat {
    pub texts: Texts,
    pub theme: Theme,
    /// Sites (`https://www.example.com`) that may put the chat on their pages and call it from them; `*` for any.
    pub origins: Vec<String>,
    /// A password the page asks for (and a front end sends as a bearer token) before a message is taken.
    pub password: Option<String>,
    /// How many earlier turns (a message and its answer) a run is given: 0 to [`MAX_HISTORY`].
    pub history: usize,
    /// The longest message, in characters.
    pub max_message: usize,
    /// Messages a minute, per conversation and per address: 0 for no limit.
    pub per_session: u32,
    pub per_address: u32,
    /// How long the chat waits for a run's answer.
    pub wait: Duration,
    pub stream: Stream,
}

/// The page's words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Texts {
    pub title: String,
    pub welcome: String,
    pub placeholder: String,
    pub send: String,
    pub error: String,
}

/// The page's colours, each `#rrggbb` (or `#rgb`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Theme {
    pub accent: String,
    pub background: String,
    pub text: String,
}

/// Which step's answer the chat shows as it is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stream {
    /// The workflow's last step, when it is an `ai` step.
    Last,
    /// None: each answer is shown whole.
    Off,
    /// The `ai` step of this name.
    Step(String),
}

impl Default for Chat {
    fn default() -> Self {
        Self {
            texts: Texts {
                title: "Chat".to_owned(),
                welcome: "Hi! How can I help?".to_owned(),
                placeholder: "Type a message".to_owned(),
                send: "Send".to_owned(),
                error: "Sorry, something went wrong. Please try again.".to_owned(),
            },
            theme: Theme {
                accent: "#0f766e".to_owned(),
                background: "#ffffff".to_owned(),
                text: "#1f2933".to_owned(),
            },
            origins: Vec::new(),
            password: None,
            history: 10,
            max_message: 4_000,
            per_session: 20,
            per_address: 60,
            wait: Duration::from_secs(120),
            stream: Stream::Last,
        }
    }
}

const SETTINGS: &[&str] = &[
    "texts", "theme", "origins", "password", "history", "limits", "wait", "stream",
];

impl Chat {
    /// A chat trigger's settings, checked; a mistake says where it is (`chat.texts.title: ...`).
    pub(crate) fn parse(settings: &Map<String, Json>) -> Result<Self, String> {
        if let Some(k) = settings.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
            return Err(format!(
                "chat.{k}: not a setting of chat ({})",
                SETTINGS.join(", ")
            ));
        }
        let mut chat = Self::default();
        if let Some(texts) = settings.get("texts") {
            let texts = record(
                texts,
                "chat.texts",
                &["title", "welcome", "placeholder", "send", "error"],
            )?;
            let t = &mut chat.texts;
            for (name, into, most, lines) in [
                ("title", &mut t.title, 80, false),
                ("welcome", &mut t.welcome, 1_000, true),
                ("placeholder", &mut t.placeholder, 100, false),
                ("send", &mut t.send, 30, false),
                ("error", &mut t.error, 300, false),
            ] {
                if let Some(v) = texts.get(name) {
                    *into = words(v, &format!("chat.texts.{name}"), most, lines)?;
                }
            }
        }
        if let Some(theme) = settings.get("theme") {
            let theme = record(theme, "chat.theme", &["accent", "background", "text"])?;
            let t = &mut chat.theme;
            for (name, into) in [
                ("accent", &mut t.accent),
                ("background", &mut t.background),
                ("text", &mut t.text),
            ] {
                if let Some(v) = theme.get(name) {
                    *into = colour(v, &format!("chat.theme.{name}"))?;
                }
            }
        }
        if let Some(origins) = settings.get("origins") {
            chat.origins = origins_of(origins)?;
        }
        match settings.get("password") {
            None | Some(Json::Null) => {}
            Some(Json::String(p)) if p.is_empty() => {}
            Some(Json::String(p)) => {
                if p.chars().count() > 256 || p.chars().any(char::is_control) {
                    return Err(
                        "chat.password: at most 256 characters, none of them control characters".to_owned(),
                    );
                }
                chat.password = Some(p.clone());
            }
            Some(_) => return Err("chat.password: text".to_owned()),
        }
        if let Some(n) = settings.get("history") {
            chat.history = whole(n, "chat.history", 0, MAX_HISTORY as u64)? as usize;
        }
        if let Some(limits) = settings.get("limits") {
            let limits = record(limits, "chat.limits", &["message", "per_session", "per_address"])?;
            if let Some(n) = limits.get("message") {
                chat.max_message = whole(n, "chat.limits.message", 1, MAX_MESSAGE as u64)? as usize;
            }
            if let Some(n) = limits.get("per_session") {
                chat.per_session = whole(n, "chat.limits.per_session", 0, 10_000)? as u32;
            }
            if let Some(n) = limits.get("per_address") {
                chat.per_address = whole(n, "chat.limits.per_address", 0, 10_000)? as u32;
            }
        }
        if let Some(wait) = settings.get("wait") {
            let written = wait.as_str().ok_or("chat.wait: how long, as 30s or 2m")?;
            chat.wait = crate::model::duration(written)
                .filter(|d| *d >= Duration::from_secs(1) && *d <= MAX_WAIT)
                .ok_or_else(|| format!("chat.wait: {written:?} is not 1s to 5m"))?;
        }
        chat.stream = match settings.get("stream") {
            None | Some(Json::Null | Json::Bool(true)) => Stream::Last,
            Some(Json::Bool(false)) => Stream::Off,
            Some(Json::String(name)) if !name.trim().is_empty() => Stream::Step(name.trim().to_owned()),
            Some(_) => return Err("chat.stream: the name of an ai step, or false".to_owned()),
        };
        Ok(chat)
    }

    /// The trigger against the steps: the step it streams is an `ai` step of the workflow.
    pub(crate) fn check(&self, steps: &[Step]) -> Result<(), String> {
        if let Stream::Step(name) = &self.stream
            && !find(steps, name).is_some_and(is_ai)
        {
            return Err(format!("trigger: chat.stream: no ai step is called {name:?}"));
        }
        Ok(())
    }

    /// The step whose answer is shown as it is made, if any.
    pub fn streamed_step(&self, steps: &[Step]) -> Option<String> {
        match &self.stream {
            Stream::Off => None,
            Stream::Step(name) => Some(name.clone()),
            Stream::Last => steps.last().filter(|s| is_ai(s)).map(|s| s.name.clone()),
        }
    }

    /// Whether a site may put the chat on its pages and call it: `origin` as a browser sends it.
    pub fn allows(&self, origin: &str) -> bool {
        self.origins
            .iter()
            .any(|o| o == "*" || o.eq_ignore_ascii_case(origin))
    }
}

fn is_ai(step: &Step) -> bool {
    matches!(&step.kind, Kind::App(app) if app.app == "ai")
}

/// The step called `name`, in a branch, a loop or a case too.
fn find<'s>(steps: &'s [Step], name: &str) -> Option<&'s Step> {
    steps.iter().find_map(|s| {
        if s.name == name {
            return Some(s);
        }
        match &s.kind {
            Kind::If { then, otherwise, .. } => find(then, name).or_else(|| find(otherwise, name)),
            Kind::ForEach { steps, .. } => find(steps, name),
            Kind::Switch { cases, otherwise } => cases
                .iter()
                .find_map(|c| find(&c.steps, name))
                .or_else(|| find(otherwise, name)),
            _ => None,
        }
    })
}

fn record<'j>(value: &'j Json, at: &str, allowed: &[&str]) -> Result<&'j Map<String, Json>, String> {
    let fields = value
        .as_object()
        .ok_or_else(|| format!("{at}: a set of settings ({})", allowed.join(", ")))?;
    if let Some(k) = fields.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(format!("{at}.{k}: not a setting ({})", allowed.join(", ")));
    }
    Ok(fields)
}

/// Words for the page: plain text, at most `most` characters, on one line unless `lines`.
fn words(value: &Json, at: &str, most: usize, lines: bool) -> Result<String, String> {
    let text = value.as_str().ok_or_else(|| format!("{at}: text"))?;
    if text.chars().count() > most {
        return Err(format!("{at}: at most {most} characters"));
    }
    if text.chars().any(|c| c.is_control() && !(lines && c == '\n')) {
        return Err(format!(
            "{at}: plain text, {}",
            if lines { "lines and words" } else { "on one line" }
        ));
    }
    Ok(text.to_owned())
}

/// `#rrggbb` or `#rgb`, in small letters: nothing else reaches the page's style.
fn colour(value: &Json, at: &str) -> Result<String, String> {
    let text = value.as_str().unwrap_or_default().trim();
    let hex = text.strip_prefix('#').unwrap_or_default();
    if !matches!(hex.len(), 3 | 6) || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("{at}: a colour as #rrggbb, not {value}"));
    }
    Ok(text.to_ascii_lowercase())
}

/// The sites a chat may be on: `https://name[:port]` (or `http://` for a machine's own name), or `*`.
fn origins_of(value: &Json) -> Result<Vec<String>, String> {
    let list = value
        .as_array()
        .ok_or("chat.origins: a list of sites, as https://www.example.com")?;
    if list.len() > MAX_ORIGINS {
        return Err(format!("chat.origins: at most {MAX_ORIGINS} sites"));
    }
    let mut out = Vec::new();
    for (i, item) in list.iter().enumerate() {
        let at = format!("chat.origins[{i}]");
        let text = item
            .as_str()
            .ok_or_else(|| format!("{at}: a site, as https://www.example.com"))?
            .trim();
        let text = text.strip_suffix('/').unwrap_or(text).to_ascii_lowercase();
        if text == "*" {
            out.push(text);
            continue;
        }
        let Some((scheme, rest)) = text.split_once("://") else {
            return Err(format!(
                "{at}: {text:?} is not a site: https://name, with no path"
            ));
        };
        let (host, port) = rest.rsplit_once(':').unwrap_or((rest, ""));
        let host_ok = !host.is_empty()
            && host.len() <= 253
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
            && !host.starts_with(['.', '-']);
        let port_ok = port.is_empty() || (port.len() <= 5 && port.chars().all(|c| c.is_ascii_digit()));
        if !matches!(scheme, "http" | "https") || !host_ok || !port_ok {
            return Err(format!(
                "{at}: {text:?} is not a site: https://name, with no path"
            ));
        }
        out.push(text);
    }
    Ok(out)
}

fn whole(value: &Json, at: &str, least: u64, most: u64) -> Result<u64, String> {
    value
        .as_u64()
        .filter(|n| (least..=most).contains(n))
        .ok_or_else(|| format!("{at}: a whole number, {least} to {most}"))
}

/// One earlier turn of a conversation: what the person wrote, and the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Turn {
    pub message: String,
    pub answer: String,
}

impl Turn {
    /// Its size as a run is given it.
    pub fn bytes(&self) -> usize {
        self.message.len() + self.answer.len() + 64
    }
}

/// The last `turns` of these (oldest first), within [`HISTORY_BYTES`].
pub fn bounded(all: &[Turn], turns: usize) -> &[Turn] {
    let mut bytes = 0;
    let mut start = all.len();
    for (i, turn) in all.iter().enumerate().rev().take(turns.min(MAX_HISTORY)) {
        bytes += turn.bytes();
        if bytes > HISTORY_BYTES {
            break;
        }
        start = i;
    }
    all.get(start..).unwrap_or_default()
}

/// The earlier messages as a run is given them: `[{role, content}]`.
pub fn history_json(turns: &[Turn]) -> Json {
    Json::Array(
        turns
            .iter()
            .flat_map(|t| {
                [
                    json!({ "role": "user", "content": t.message }),
                    json!({ "role": "assistant", "content": t.answer }),
                ]
            })
            .collect(),
    )
}

/// Earlier messages a caller sent (`[{role, content}]`, as [`history_json`] gives them): the turns they make, the
/// last `turns` within [`HISTORY_BYTES`]. For trying a chat in the studio, where the panel keeps the conversation.
pub fn history_from(value: &Json, turns: usize) -> Result<Vec<Turn>, String> {
    let list = match value {
        Json::Null => return Ok(Vec::new()),
        Json::Array(list) if list.len() <= 2 * MAX_HISTORY + 2 => list,
        _ => return Err(format!("history: a list of at most {} messages", 2 * MAX_HISTORY)),
    };
    let mut out = Vec::new();
    let mut waiting: Option<String> = None;
    for (i, item) in list.iter().enumerate() {
        let role = item.get("role").and_then(Json::as_str).unwrap_or_default();
        let content = item
            .get("content")
            .and_then(Json::as_str)
            .ok_or_else(|| format!("history[{i}].content: text"))?;
        if content.len() > MESSAGE_BYTES * 2 {
            return Err(format!(
                "history[{i}].content: at most {} bytes",
                MESSAGE_BYTES * 2
            ));
        }
        match (role, waiting.take()) {
            ("user", _) => waiting = Some(content.to_owned()),
            ("assistant", Some(message)) => out.push(Turn {
                message,
                answer: content.to_owned(),
            }),
            ("assistant", None) => {}
            _ => return Err(format!("history[{i}].role: user or assistant")),
        }
    }
    Ok(bounded(&out, turns).to_vec())
}

/// A message, checked: text, not empty, at most `chars` characters and [`MESSAGE_BYTES`].
pub fn message_of(value: Option<&Json>, chars: usize) -> Result<String, String> {
    let text = value
        .and_then(Json::as_str)
        .ok_or("message: what the person wrote")?;
    let text = text.trim();
    if text.is_empty() {
        return Err("message: what the person wrote".to_owned());
    }
    if text.chars().count() > chars || text.len() > MESSAGE_BYTES {
        return Err(format!("message: at most {chars} characters"));
    }
    if text.chars().any(|c| c.is_control() && !matches!(c, '\n' | '\t')) {
        return Err("message: text, without control characters".to_owned());
    }
    Ok(text.to_owned())
}

/// A conversation's id as a chat keeps it: 16 to 128 letters, digits, `-`, `_`, `.` or `:`. It is the
/// conversation's key, so it must not be guessable: the page makes a random one.
pub fn session_ok(id: &str) -> bool {
    (16..=128).contains(&id.len())
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
}

/// What a site or a front end said about the person, checked: a record of at most 20 names (letters, digits, `_`),
/// each with text, a number or a yes/no.
pub fn person_of(value: Option<&Json>) -> Result<Json, String> {
    let fields = match value {
        None | Some(Json::Null) => return Ok(json!({})),
        Some(Json::Object(fields)) if fields.len() <= PERSON_FIELDS => fields,
        Some(_) => return Err(format!("person: a record of at most {PERSON_FIELDS} fields")),
    };
    for (name, value) in fields {
        let name_ok = !name.is_empty()
            && name.len() <= PERSON_NAME
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !name_ok {
            return Err(format!("person.{name}: a name of letters, digits and _"));
        }
        let value_ok = match value {
            Json::String(s) => s.chars().count() <= PERSON_TEXT && !s.chars().any(char::is_control),
            Json::Number(_) | Json::Bool(_) | Json::Null => true,
            _ => false,
        };
        if !value_ok {
            return Err(format!(
                "person.{name}: text of at most {PERSON_TEXT} characters, a number or true/false"
            ));
        }
    }
    Ok(Json::Object(fields.clone()))
}

/// What a run of a chat is given, as `value`.
pub fn event_value(message: &str, session: &str, history: &[Turn], person: &Json) -> Json {
    json!({ "message": message, "session": session, "history": history_json(history), "person": person })
}

/// How a run of a chat started, as `trigger`: `reply` names its answer, and `stream` the step shown as it is made.
pub fn event_details(session: &str, reply: &str, stream: Option<&str>, at: &str) -> Json {
    let mut details = json!({ "trigger": "chat", "session": session, "at": at, "reply": reply });
    if let (Some(step), Some(fields)) = (stream, details.as_object_mut()) {
        fields.insert("stream".to_owned(), json!(step));
    }
    details
}

/// What a finished run says to the chat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Its answer, and the respond step's body when one gave it (what a front end of one's own may read more from).
    Said { text: String, data: Option<Json> },
    /// Nothing: it was filtered out, or its last step gave no text.
    Nothing,
    /// It failed: the chat says its error text, never the run's error.
    Failed,
}

/// The answer of a finished run, from its record as a run's history keeps it (`status`, `steps`).
pub fn answer_of(run: &Json) -> Answer {
    match run.get("status").and_then(Json::as_str) {
        Some("failed") => return Answer::Failed,
        Some("skipped") => return Answer::Nothing,
        _ => {}
    }
    let steps = run
        .get("steps")
        .and_then(Json::as_array)
        .map_or(&[][..], Vec::as_slice);
    let responded = steps
        .iter()
        .rev()
        .find(|s| s.get("kind").and_then(Json::as_str) == Some("respond") && s["error"].is_null());
    let (text, data) = match responded {
        Some(step) => {
            let body = step.pointer("/result/body").cloned().unwrap_or(Json::Null);
            let text = match &body {
                Json::String(s) => Some(s.clone()),
                Json::Number(_) | Json::Bool(_) => Some(body.to_string()),
                other => text_field(other, &["text", "answer", "message"]),
            };
            (text, Some(body))
        }
        None => (
            steps.last().and_then(|s| s.get("result")).and_then(|r| match r {
                Json::String(s) => Some(s.clone()),
                other => text_field(other, &["text", "answer", "message", "output", "reply"]),
            }),
            None,
        ),
    };
    match text.map(|t| t.trim().to_owned()).filter(|t| !t.is_empty()) {
        Some(text) => Answer::Said {
            text: cut(&text, MAX_ANSWER),
            data,
        },
        None => Answer::Nothing,
    }
}

fn text_field(value: &Json, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|n| value.get(*n).and_then(Json::as_str))
        .map(str::to_owned)
}

/// At most `chars` characters of `text`.
pub fn cut(text: &str, chars: usize) -> String {
    match text.char_indices().nth(chars) {
        Some((at, _)) => text.get(..at).unwrap_or(text).to_owned(),
        None => text.to_owned(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn parse(yaml: serde_json::Value) -> Result<Chat, String> {
        Chat::parse(yaml.as_object().unwrap())
    }

    #[test]
    fn a_chat_with_no_settings_has_safe_defaults_and_every_setting_is_checked() {
        let chat = parse(json!({})).unwrap();
        assert_eq!(chat, Chat::default());
        assert!(chat.origins.is_empty() && chat.password.is_none());
        let full = parse(json!({
            "texts": { "title": "Help desk", "welcome": "Hi!\nAsk away.", "send": "Go" },
            "theme": { "accent": "#ABC", "background": "#101820" },
            "origins": ["https://www.example.com/", "http://localhost:3000", "*"],
            "password": "letmein",
            "history": 4,
            "limits": { "message": 500, "per_session": 0, "per_address": 100 },
            "wait": "30s",
            "stream": false,
        }))
        .unwrap();
        assert_eq!(full.texts.title, "Help desk");
        assert_eq!(full.texts.placeholder, "Type a message");
        assert_eq!(full.theme.accent, "#abc");
        assert_eq!(
            full.origins,
            ["https://www.example.com", "http://localhost:3000", "*"]
        );
        assert_eq!(
            (full.history, full.max_message, full.per_session, full.per_address),
            (4, 500, 0, 100)
        );
        assert_eq!(full.wait, Duration::from_secs(30));
        assert_eq!(full.stream, Stream::Off);
        assert!(full.allows("https://anything.example"));

        for (bad, says) in [
            (json!({ "colour": "red" }), "chat.colour: not a setting"),
            (
                json!({ "texts": { "title": "x".repeat(81) } }),
                "chat.texts.title: at most 80",
            ),
            (
                json!({ "texts": { "title": "a\nb" } }),
                "chat.texts.title: plain text",
            ),
            (
                json!({ "theme": { "accent": "red; background: url(x)" } }),
                "chat.theme.accent: a colour",
            ),
            (
                json!({ "origins": ["https://example.com/path"] }),
                "chat.origins[0]",
            ),
            (json!({ "origins": ["javascript://x"] }), "chat.origins[0]"),
            (
                json!({ "origins": ["https://a.example 'unsafe-inline'"] }),
                "chat.origins[0]",
            ),
            (json!({ "history": 51 }), "chat.history: a whole number, 0 to 50"),
            (json!({ "limits": { "message": 0 } }), "chat.limits.message"),
            (json!({ "wait": "10m" }), "chat.wait"),
            (json!({ "password": "a\u{0}b" }), "chat.password"),
            (json!({ "stream": 3 }), "chat.stream"),
        ] {
            let e = parse(bad.clone()).unwrap_err();
            assert!(e.contains(says), "{bad}: {e}");
        }
    }

    #[test]
    fn a_site_is_allowed_only_when_named() {
        let chat = parse(json!({ "origins": ["https://shop.example.com"] })).unwrap();
        assert!(chat.allows("https://shop.example.com") && chat.allows("HTTPS://Shop.Example.com"));
        assert!(
            !chat.allows("https://shop.example.com.evil.example") && !chat.allows("http://shop.example.com")
        );
        assert!(!Chat::default().allows("https://shop.example.com"));
    }

    #[test]
    fn a_run_is_given_the_last_turns_within_its_bounds() {
        let turns: Vec<Turn> = (0..60)
            .map(|i| Turn {
                message: format!("q{i}"),
                answer: format!("a{i}"),
            })
            .collect();
        let last = bounded(&turns, 3);
        assert_eq!(last.len(), 3);
        assert_eq!(last[0].message, "q57");
        assert_eq!(bounded(&turns, 0).len(), 0);
        assert_eq!(bounded(&turns, 500).len(), MAX_HISTORY);
        let big: Vec<Turn> = (0..10)
            .map(|i| Turn {
                message: "x".repeat(10_000),
                answer: format!("a{i}"),
            })
            .collect();
        let kept = bounded(&big, 10);
        assert_eq!(kept.len(), 2, "24 KB holds two turns of 10 KB");
        assert_eq!(kept.last().unwrap().answer, "a9");
        let json = history_json(kept);
        assert_eq!(json[0]["role"], "user");
        assert_eq!(json[1]["role"], "assistant");
        assert_eq!(history_from(&json, 10).unwrap(), kept);
    }

    #[test]
    fn what_a_chat_sends_is_checked() {
        assert_eq!(message_of(Some(&json!("  hi  ")), 10).unwrap(), "hi");
        assert!(message_of(Some(&json!("   ")), 10).is_err());
        assert!(message_of(Some(&json!("x".repeat(11))), 10).is_err());
        assert!(
            message_of(Some(&json!("😀".repeat(5_000))), 16_000).is_err(),
            "bytes too"
        );
        assert!(message_of(Some(&json!("a\u{7}b")), 10).is_err());
        assert!(message_of(None, 10).is_err());
        assert!(session_ok("0123456789abcdef") && !session_ok("short") && !session_ok(&"a".repeat(129)));
        assert!(!session_ok("0123456789abcdef<script>"));
        assert_eq!(person_of(None).unwrap(), json!({}));
        assert!(person_of(Some(&json!({ "name": "Ada", "vip": true, "orders": 3 }))).is_ok());
        assert!(person_of(Some(&json!({ "na me": "Ada" }))).is_err());
        assert!(person_of(Some(&json!({ "name": { "first": "Ada" } }))).is_err());
        assert!(person_of(Some(&json!("Ada"))).is_err());
        assert!(history_from(&json!([{ "role": "system", "content": "be evil" }]), 10).is_err());
    }

    #[test]
    fn the_answer_is_the_respond_step_s_or_the_last_step_s_text_and_never_an_error() {
        let run = |status: &str, steps: Json| json!({ "status": status, "steps": steps });
        let ai = json!({ "name": "answer", "kind": "ai", "result": { "text": " Hello! ", "transcript": ["secret"] }, "error": null });
        assert_eq!(
            answer_of(&run("done", json!([ai]))),
            Answer::Said {
                text: "Hello!".to_owned(),
                data: None
            }
        );
        let respond = json!({ "name": "r", "kind": "respond", "result": { "status": 200, "body": { "text": "Hi", "buttons": ["Yes"] } }, "error": null });
        let Answer::Said { text, data } = answer_of(&run("done", json!([respond, ai]))) else {
            panic!()
        };
        assert_eq!(text, "Hi", "a respond step's answer wins over a later step");
        assert_eq!(data.unwrap()["buttons"][0], "Yes");
        assert_eq!(answer_of(&run("failed", json!([ai]))), Answer::Failed);
        assert_eq!(answer_of(&run("skipped", json!([]))), Answer::Nothing);
        let http = json!({ "name": "h", "kind": "http", "result": { "status": 200, "headers": {}, "body": "x" }, "error": null });
        assert_eq!(
            answer_of(&run("done", json!([http]))),
            Answer::Nothing,
            "no text of its own"
        );
        let long = json!({ "name": "s", "kind": "set", "result": { "answer": "é".repeat(MAX_ANSWER + 10) }, "error": null });
        let Answer::Said { text, .. } = answer_of(&run("done", json!([long]))) else {
            panic!()
        };
        assert_eq!(text.chars().count(), MAX_ANSWER);
    }

    #[test]
    fn a_run_is_told_its_message_session_history_and_person() {
        let turns = [Turn {
            message: "Hi".to_owned(),
            answer: "Hello".to_owned(),
        }];
        let value = event_value(
            "Where is my order?",
            "s-0123456789abcdef",
            &turns,
            &json!({ "name": "Ada" }),
        );
        assert_eq!(
            value["history"][1],
            json!({ "role": "assistant", "content": "Hello" })
        );
        assert_eq!(value["session"], "s-0123456789abcdef");
        let details = event_details("s-0123456789abcdef", "r1", Some("answer"), "2026-11-27T10:00:00Z");
        assert_eq!(details["trigger"], "chat");
        assert_eq!(details["stream"], "answer");
        assert!(event_details("s", "r1", None, "t").get("stream").is_none());
    }
}
