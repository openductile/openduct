// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One poll of an app's trigger: the list asked for (page after page, as its action says), each item told from the
//! others by its id, those not seen before taken - oldest first, up to the most a poll takes - and, if the trigger
//! says, each read in full. What was seen is given back to be kept until the next poll: the items' ids (and, for a
//! trigger that tells changes, a hash of each one's version) and where the next poll starts. The first poll takes
//! nothing: it only notes what is there, so a trigger starts from now.
//!
//! [`Polling`] is the poll as a series of requests and answers, so a recorded test drives it with no network;
//! [`poll`] sends them through a run's effects.

use std::collections::{HashMap, HashSet};

use openduct_expr::{Env, Record, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use super::definition::{Action, AppDef};
use super::triggers::{Poll, Request, TriggerDef};
use crate::apps::Credential;
use crate::run::{Effects, HttpRequest};

/// The most items a poll lists, pages together.
const MOST_LISTED: usize = 2_000;
/// The most items kept as seen: the newest, when there are more.
pub const MOST_SEEN: usize = 5_000;
/// The most items a poll takes, by default and at most.
pub const POLL_ITEMS: u32 = 50;
pub const MOST_POLL_ITEMS: u32 = 500;
/// An id longer than this is kept as its hash.
const LONGEST_ID: usize = 128;
/// The largest item a run is given, in bytes as JSON: a larger one is said, not given.
const LARGEST_ITEM: usize = 1024 * 1024;

/// What a trigger's polls have seen, kept from one poll to the next.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Seen {
    /// Where the next poll starts, as the trigger's cursor said (null: from the start of its list).
    #[serde(default)]
    pub cursor: Json,
    /// The items seen, oldest first: each one's id, and a hash of its version ("" for a trigger that tells none).
    #[serde(default)]
    pub items: Vec<(String, String)>,
    /// The scheduled time of the poll that kept it, in milliseconds since 1970.
    #[serde(default)]
    pub slot: i64,
    /// Why the last poll failed, if it did: what it had seen is kept as it was.
    #[serde(default)]
    pub failing: Option<String>,
    /// No poll has noted what is there yet (the first ones failed): the next one is the first.
    #[serde(default)]
    pub pending: bool,
}

/// What a poll found: the items to run (each with its id), and what is seen now.
#[derive(Debug)]
pub struct Polled {
    pub runs: Vec<(String, Value)>,
    pub seen: Seen,
}

enum Phase {
    Listing,
    Reading(usize),
    Done,
}

/// A poll, request by request.
pub struct Polling<'a> {
    app: &'a AppDef,
    poll: &'a Poll,
    /// The credential, a token renewed in it as the driver renews one.
    pub credential: Credential,
    /// What the trigger's requests read as `input`: its settings and the names beside them.
    names: Value,
    seen: Option<&'a Seen>,
    max_items: usize,
    phase: Phase,
    page: Value,
    pages: u32,
    listed: Vec<Value>,
    taken: Vec<(String, String, Value)>,
    finished: Option<Seen>,
}

impl std::fmt::Debug for Polling<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Polling")
            .field("app", &self.app.id)
            .finish_non_exhaustive()
    }
}

/// The record a trigger's requests read as `input`: the workflow's settings for it, and beside them `extra`.
pub(crate) fn names_of(settings: &Json, extra: &[(&str, Value)]) -> Value {
    let mut record = match Value::from_json(settings) {
        Value::Record(r) => (*r).clone(),
        _ => Record::new(),
    };
    for (k, v) in extra {
        record.insert((*k).to_owned(), v.clone());
    }
    Value::record(record)
}

/// The inputs a trigger's request gives its action: its own `input` worked out over `names` (and each of them by
/// itself, as `cursor`, `item` ...), or `names` as they are.
pub(crate) fn inputs_for(request: &Request, names: &Value) -> Result<Value, String> {
    if request.input.is_empty() {
        return Ok(names.clone());
    }
    let mut env_names = vec![("input".to_owned(), names.clone())];
    if let Value::Record(r) = names {
        for k in super::triggers::RESERVED {
            if let Some(v) = r.get(*k) {
                env_names.push(((*k).to_owned(), v.clone()));
            }
        }
    }
    let null = Value::Null;
    let env = Env {
        value: &null,
        message: &null,
        names: &env_names,
    };
    let mut out = Record::new();
    for (k, t) in &request.input {
        out.insert(
            k.clone(),
            crate::run::tree(t, &env).map_err(|e| format!("input.{k}: {e}"))?,
        );
    }
    Ok(Value::record(out))
}

fn hash(text: &str) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// An item's id as kept: its text, or a long one's hash.
fn id_text(v: &Value) -> Option<String> {
    let t = crate::template::as_text(v);
    let t = t.trim();
    if t.is_empty() {
        return None;
    }
    Some(if t.len() > LONGEST_ID {
        format!("#{}", hash(t))
    } else {
        t.to_owned()
    })
}

/// Rows under a header row, each a record of the header's names (and `row_number`, its place counting the header as
/// 1); a row that is not a list is kept as it is.
fn under_header(listed: Vec<Value>) -> Vec<Value> {
    let mut rows = listed.into_iter();
    let Some(Value::List(header)) = rows.next() else {
        return Vec::new();
    };
    let names: Vec<String> = header
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let t = crate::template::as_text(h).trim().to_owned();
            if t.is_empty() {
                format!("column_{}", i + 1)
            } else {
                t
            }
        })
        .collect();
    rows.enumerate()
        .map(|(i, row)| match row {
            Value::List(cells) => {
                let mut r = Record::new();
                r.insert("row_number".to_owned(), Value::number((i + 2).into()));
                for (n, name) in names.iter().enumerate() {
                    r.insert(
                        name.clone(),
                        cells.get(n).cloned().unwrap_or_else(|| Value::text("")),
                    );
                }
                Value::record(r)
            }
            other => other,
        })
        .collect()
}

impl<'a> Polling<'a> {
    /// A poll of `trigger` of `app` with `credential`: `settings` are the workflow's for it, `seen` what the polls
    /// before kept (none: the first poll), `now_ms` its time.
    #[allow(clippy::too_many_arguments)] // each is the poll's own, and a test gives them one by one
    pub fn new(
        app: &'a AppDef,
        trigger: &'a TriggerDef,
        credential: Credential,
        settings: &Json,
        workflow: &str,
        seen: Option<&'a Seen>,
        max_items: u32,
        now_ms: i64,
    ) -> Result<Self, String> {
        let poll = trigger
            .polls()
            .ok_or_else(|| format!("{}'s {} does not poll", app.name, trigger.label))?;
        let settings = &trigger.settings_of(settings);
        let names = names_of(
            settings,
            &[
                (
                    "cursor",
                    seen.map_or(Value::Null, |s| Value::from_json(&s.cursor)),
                ),
                ("now", Value::text(crate::runner::iso(now_ms))),
                ("workflow", Value::text(workflow)),
            ],
        );
        Ok(Self {
            app,
            poll,
            credential,
            names,
            seen,
            max_items: usize::try_from(max_items.clamp(1, MOST_POLL_ITEMS)).unwrap_or(1),
            phase: Phase::Listing,
            page: Value::Null,
            pages: 0,
            listed: Vec::new(),
            taken: Vec::new(),
            finished: None,
        })
    }

    fn list_action(&self) -> Result<&'a Action, String> {
        self.poll
            .list
            .action(&self.app.actions)
            .ok_or_else(|| format!("{} has no such action", self.app.name))
    }

    fn each_action(&self) -> Result<Option<(&'a Request, &'a Action)>, String> {
        match &self.poll.each {
            Some(r) => Ok(Some((
                r,
                r.action(&self.app.actions)
                    .ok_or_else(|| format!("{} has no such action", self.app.name))?,
            ))),
            None => Ok(None),
        }
    }

    /// The request to send now; none once the poll is done.
    pub fn request(&self) -> Result<Option<HttpRequest>, String> {
        self.request_with(&self.credential)
    }

    fn request_with(&self, credential: &Credential) -> Result<Option<HttpRequest>, String> {
        match self.phase {
            Phase::Done => Ok(None),
            Phase::Listing => {
                let inputs = inputs_for(&self.poll.list, &self.names)?;
                super::request(self.app, self.list_action()?, credential, &inputs, &self.page).map(Some)
            }
            Phase::Reading(at) => {
                let (request, action) = self.each_action()?.ok_or("nothing to read")?;
                let item = self
                    .taken
                    .get(at)
                    .map(|(_, _, v)| v.clone())
                    .unwrap_or(Value::Null);
                let names = match &self.names {
                    Value::Record(r) => {
                        let mut r = (**r).clone();
                        r.insert("item".to_owned(), item);
                        Value::record(r)
                    }
                    other => other.clone(),
                };
                let inputs = inputs_for(request, &names)?;
                super::request(self.app, action, credential, &inputs, &Value::Null).map(Some)
            }
        }
    }

    /// Takes the answer to the request just sent.
    pub fn answered(&mut self, answer: &Value) -> Result<(), String> {
        match self.phase {
            Phase::Done => Err("the poll had finished".to_owned()),
            Phase::Listing => {
                let action = self.list_action()?;
                let got = super::answer(self.app, action, &self.credential, answer, &self.page)?;
                let seen_here = self.poll.newest_first
                    && match self.seen {
                        // The first poll only notes what is there now: the newest page is enough.
                        None => true,
                        Some(seen) => {
                            let ids: HashSet<&str> = seen.items.iter().map(|(id, _)| id.as_str()).collect();
                            got.items.iter().enumerate().any(|(i, item)| {
                                self.id_of(item, i)
                                    .is_ok_and(|id| id.is_some_and(|id| ids.contains(id.as_str())))
                            })
                        }
                    };
                self.listed.extend(got.items);
                self.pages += 1;
                if matches!(got.next, Value::Null)
                    || seen_here
                    || self.pages >= action.max_pages
                    || self.listed.len() >= MOST_LISTED
                {
                    self.listed.truncate(MOST_LISTED);
                    self.select()?;
                } else {
                    self.page = got.next;
                }
                Ok(())
            }
            Phase::Reading(at) => {
                let (_, action) = self.each_action()?.ok_or("nothing to read")?;
                let got = super::answer(self.app, action, &self.credential, answer, &Value::Null)?;
                if let Some(t) = self.taken.get_mut(at) {
                    t.2 = got.result;
                }
                self.phase = if at + 1 < self.taken.len() {
                    Phase::Reading(at + 1)
                } else {
                    Phase::Done
                };
                Ok(())
            }
        }
    }

    fn env_names(&self, item: &Value, index: usize) -> Vec<(String, Value)> {
        vec![
            ("item".to_owned(), item.clone()),
            ("index".to_owned(), Value::number(index.into())),
            ("input".to_owned(), self.names.clone()),
        ]
    }

    fn id_of(&self, item: &Value, index: usize) -> Result<Option<String>, String> {
        let names = self.env_names(item, index);
        let env = Env {
            value: item,
            message: &Value::Null,
            names: &names,
        };
        Ok(id_text(
            &self.poll.id.evaluate(&env).map_err(|e| format!("id: {e}"))?,
        ))
    }

    fn version_of(&self, item: &Value, index: usize) -> Result<String, String> {
        let Some(version) = &self.poll.version else {
            return Ok(String::new());
        };
        let names = self.env_names(item, index);
        let env = Env {
            value: item,
            message: &Value::Null,
            names: &names,
        };
        let v = version.evaluate(&env).map_err(|e| format!("version: {e}"))?;
        Ok(hash(&v.to_json()).chars().take(16).collect())
    }

    /// The items listed, oldest first, told apart; those to run taken; what is seen now worked out.
    fn select(&mut self) -> Result<(), String> {
        let mut listed = std::mem::take(&mut self.listed);
        if self.poll.newest_first {
            listed.reverse();
        }
        if self.poll.header_row {
            listed = under_header(listed);
        }
        // Each item with its id and version; one with no id cannot be told apart, and is passed; one listed twice
        // counts once.
        let mut items: Vec<(String, String, Value)> = Vec::new();
        let mut ids = HashSet::new();
        for (i, item) in listed.into_iter().enumerate() {
            let Some(id) = self.id_of(&item, i)? else { continue };
            if ids.insert(id.clone()) {
                let version = self.version_of(&item, i)?;
                items.push((id, version, item));
            }
        }
        let before: HashMap<&str, &str> = self
            .seen
            .map(|s| s.items.iter().map(|(i, v)| (i.as_str(), v.as_str())).collect())
            .unwrap_or_default();
        let first = self.seen.is_none();
        let new = |(id, version, _): &(String, String, Value)| {
            !first && before.get(id.as_str()) != Some(&version.as_str())
        };
        // Oldest first, up to the most a poll takes; what comes after the first one left is for the next poll.
        let mut taken = Vec::new();
        let mut done = items.len();
        for (at, item) in items.iter().enumerate() {
            if new(item) {
                if taken.len() == self.max_items {
                    done = at;
                    break;
                }
                taken.push(item.clone());
            }
        }
        let old_cursor = self.seen.map_or(Json::Null, |s| s.cursor.clone());
        // Worked out after every poll; one that gives null keeps the one before, as does one that cannot be worked out
        // when nothing was dealt with (the last of no items).
        let cursor = match &self.poll.cursor {
            Some(c) => {
                let dealt: Vec<Value> = items.iter().take(done).map(|(_, _, v)| v.clone()).collect();
                let names = vec![
                    ("items".to_owned(), Value::list(dealt)),
                    ("cursor".to_owned(), Value::from_json(&old_cursor)),
                    ("input".to_owned(), self.names.clone()),
                ];
                let env = Env {
                    value: &Value::Null,
                    message: &Value::Null,
                    names: &names,
                };
                match c.evaluate(&env) {
                    Ok(Value::Null) => old_cursor,
                    Ok(v) => serde_json::from_str(&v.to_json()).unwrap_or(Json::Null),
                    Err(e) if done > 0 => return Err(format!("cursor: {e}")),
                    Err(_) => old_cursor,
                }
            }
            _ => old_cursor,
        };
        // Seen now: what was seen and is not listed now, then what is listed and dealt with - a new item taken at its
        // version, one seen before at the version it was seen at (one that changed and was left for the next poll
        // is taken then).
        let listed_ids: HashSet<&str> = items.iter().map(|(id, _, _)| id.as_str()).collect();
        let taken_ids: HashSet<&str> = taken.iter().map(|(id, _, _)| id.as_str()).collect();
        let mut seen: Vec<(String, String)> = self
            .seen
            .map(|s| {
                s.items
                    .iter()
                    .filter(|(id, _)| !listed_ids.contains(id.as_str()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        for (id, version, _) in &items {
            if first || taken_ids.contains(id.as_str()) {
                seen.push((id.clone(), version.clone()));
            } else if let Some(old) = before.get(id.as_str()) {
                seen.push((id.clone(), (*old).to_owned()));
            }
        }
        if seen.len() > MOST_SEEN {
            seen.drain(..seen.len() - MOST_SEEN);
        }
        self.finished = Some(Seen {
            cursor,
            items: seen,
            slot: 0,
            failing: None,
            pending: false,
        });
        self.taken = taken;
        self.phase = if self.poll.each.is_some() && !self.taken.is_empty() {
            Phase::Reading(0)
        } else {
            Phase::Done
        };
        Ok(())
    }

    /// What the poll found, once done.
    pub fn finish(self, slot: i64) -> Result<Polled, String> {
        if !matches!(self.phase, Phase::Done) {
            return Err("the poll has not finished".to_owned());
        }
        let mut seen = self.finished.clone().unwrap_or_default();
        seen.slot = slot;
        let mut runs = Vec::with_capacity(self.taken.len());
        for (i, (id, _, item)) in self.taken.iter().enumerate() {
            let value = match &self.poll.event {
                Some(e) => {
                    let names = self.env_names(item, i);
                    let env = Env {
                        value: item,
                        message: &Value::Null,
                        names: &names,
                    };
                    e.evaluate(&env).map_err(|e| format!("event: {e}"))?
                }
                None => item.clone(),
            };
            let size = value.to_json().len();
            let value = if size > LARGEST_ITEM {
                Value::from_json(
                    &json!({ "id": id, "too_large": format!("{size} bytes: more than a run is given") }),
                )
            } else {
                value
            };
            runs.push((id.clone(), value));
        }
        Ok(Polled { runs, seen })
    }
}

fn status_of(answer: &Value) -> u64 {
    match answer {
        Value::Record(r) => r
            .get("status")
            .and_then(|s| crate::template::as_text(s).parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

/// A fresh token for an OAuth sign-in's credential, kept in it.
pub(crate) async fn fresh(
    effects: &dyn Effects,
    app: &AppDef,
    name: &str,
    credential: &Credential,
) -> Result<Credential, String> {
    let sign_in = app
        .sign_in(&credential.kind)
        .ok_or_else(|| format!("{} has no sign-in {}", app.name, credential.kind))?;
    let registration = effects.registration(&app.id);
    let request = super::oauth::refresh(sign_in, credential, registration.as_ref())
        .map_err(|e| format!("{name}: {e}"))?;
    let answer = effects.http(request).await?;
    let tokens = super::oauth::tokens(&answer, credential).map_err(|e| format!("{name}: {e}"))?;
    effects.keep_credential(name, tokens.clone()).await?;
    let mut updated = credential.clone();
    updated.fields.extend(tokens);
    Ok(updated)
}

/// Sends `request`, with a token renewed first when it is running out, and once more when the app no longer takes it.
pub(crate) async fn send(
    effects: &dyn Effects,
    app: &AppDef,
    name: &str,
    credential: &mut Credential,
    build: &(dyn Fn(&Credential) -> Result<Option<HttpRequest>, String> + Sync),
) -> Result<Option<Value>, String> {
    let oauth = app.sign_in(&credential.kind).filter(|s| s.oauth2.is_some());
    if let Some(sign_in) = oauth
        && super::oauth::wanted(sign_in, credential, false)
    {
        *credential = fresh(effects, app, name, credential).await?;
    }
    let Some(request) = build(credential)? else {
        return Ok(None);
    };
    let answer = effects.http(request).await?;
    if status_of(&answer) == 401
        && let Some(sign_in) = oauth
        && super::oauth::wanted(sign_in, credential, true)
    {
        *credential = fresh(effects, app, name, credential).await?;
        if let Some(again) = build(credential)? {
            return effects.http(again).await.map(Some);
        }
    }
    Ok(Some(answer))
}

/// Runs a poll through `effects`, the credential called `name`: what it found.
pub async fn poll(
    effects: &dyn Effects,
    mut polling: Polling<'_>,
    name: &str,
    slot: i64,
) -> Result<Polled, String> {
    // A poll makes at most its list's pages and one request for each item taken.
    for _ in 0..(super::definition::MOST_PAGES as usize + MOST_POLL_ITEMS as usize + 1) {
        let mut credential = polling.credential.clone();
        let answer = send(effects, polling.app, name, &mut credential, &|c| {
            polling.request_with(c)
        })
        .await?;
        polling.credential = credential;
        match answer {
            Some(answer) => polling.answered(&answer)?,
            None => return polling.finish(slot),
        }
    }
    Err("the poll made more requests than it may".to_owned())
}
