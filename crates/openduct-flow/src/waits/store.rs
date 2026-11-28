// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Where runs that wait are kept: one topic of the broker's own, read by every node in the same order. Each record is
//! keyed by what it is about - `wait/<token>` (a run that waits, with all it has done so far), `claim/<token>` (an
//! answer, or its timeout), `queued/<token>`, `resumed/<token>`, `done/<token>` - and every node keeps, in memory, only
//! a short summary of each wait and where its records are.
//!
//! Who answered first is decided by the log: every claim of a token is written, and the first in the log wins, the
//! same on every node - so two people answering one form at once, or an answer and its timeout, end with one answer.
//! The run goes on through its workflow's own topic, as an event that names the token; the first run to take it up
//! (again by the log) is the one that goes on, so an event given twice runs once.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use openduct_broker::client::{ClientError, Outgoing, Routed, records};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use tokio::sync::{Mutex, watch};

use super::{OnTimeout, Waiting};
use crate::forms::{Form, MAX_TIMEOUT};
use crate::runner::{PARTITIONS, Topics, envelope, iso, now_ms};

/// The topic runs that wait are kept in.
pub const TOPIC: &str = "__openduct.flow.waits";
/// The most runs of one workflow waiting at once.
pub const MAX_WAITING: usize = 10_000;
/// The most runs waiting at once, all workflows together.
const MAX_ALL: usize = 200_000;
/// The most a run that waits keeps of what it has done, and the largest answer, in bytes as JSON.
pub const MAX_STATE: usize = 8 * 1024 * 1024;
/// How long the topic keeps a record: past the longest wait, with a week to spare.
const RETENTION: Duration = Duration::from_secs(MAX_TIMEOUT.as_secs() + 7 * 24 * 3600);
/// How long a finished wait's token is remembered, so a run cut short after it went on does not wait on it again.
const DONE_KEPT_MS: i64 = 24 * 3600 * 1000;
/// How long an answer may wait for its run to be told before the sweeper tells it again.
const TELL_AGAIN_MS: i64 = 15_000;
/// The key that makes tokens is written again this often, so the topic's retention never takes it.
const KEY_AGAIN_MS: i64 = 30 * 24 * 3600 * 1000;
/// The most bytes one read asks for: more than the largest record.
const READ_BYTES: i32 = 16 * 1024 * 1024;
const CONNECT: Duration = Duration::from_secs(10);
/// How long a forming cluster has to become ready.
const FORMING: Duration = Duration::from_secs(90);

/// What a run that waits keeps: where it is, what it has done, and what it waits for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Paused {
    pub workflow: String,
    /// The run's id: the same from its first part to its last.
    pub run: String,
    pub value: Json,
    pub details: Json,
    /// The event as it came, for CatchErr should the run fail later.
    #[serde(default)]
    pub original: Option<String>,
    #[serde(default)]
    pub hops: u32,
    /// Its steps' records so far, in the order they ran.
    pub journal: Vec<Json>,
    /// The tokens handed out to the steps that wait, by step.
    #[serde(default)]
    pub tokens: BTreeMap<String, String>,
    /// How long it has run so far, waits left out.
    pub ms: u64,
    pub waiting: Waiting,
}

/// A run that waits, as a page or a list shows it.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub token: String,
    pub workflow: String,
    pub run: String,
    pub step: String,
    /// `form`, `approval`, `webhook` or `time`.
    pub kind: String,
    /// A form's title.
    pub title: String,
    pub since: i64,
    pub until: i64,
    pub on_timeout: OnTimeout,
    /// Who answered (`answer`, `timeout`, `time`), once one has.
    pub answered: Option<String>,
}

/// What became of an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answered {
    /// It is the answer: the run goes on with it.
    Taken,
    /// Another answer came first (`answer`, `timeout` or `time`).
    Already(String),
    /// No run waits at this address.
    Unknown,
}

/// The answer a run waited for, as its claim holds it.
#[derive(Debug, Clone)]
pub(crate) struct Claim {
    pub by: String,
    pub at: i64,
    pub answer: Json,
}

#[derive(Debug)]
struct Entry {
    summary: Summary,
    /// Where its `wait/` record is.
    at: i64,
    /// The first claim: who, when, where.
    claim: Option<(String, i64, i64)>,
    queued: bool,
    /// The first run to take it up, and where that was said.
    resumed: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    client: Option<Routed>,
    next: i64,
    entries: HashMap<String, Entry>,
    /// Tokens finished, and when.
    done: HashMap<String, i64>,
    key: Option<Vec<u8>>,
    key_written: i64,
}

/// The runs that wait, on the broker at `broker`.
pub struct Waits {
    broker: String,
    /// The address its one-time addresses start with: `https://flows.example.com`.
    base: String,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for Waits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Waits")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct Head {
    workflow: String,
    run: String,
    waiting: Waiting,
}

impl Waits {
    /// The runs that wait, read from the broker at `broker` (the topic made the first time); their addresses start
    /// with `base`.
    pub async fn open(broker: &str, base: &str) -> Result<Self, String> {
        let waits = Self {
            broker: broker.to_owned(),
            base: base.trim_end_matches('/').to_owned(),
            inner: Mutex::new(Inner::default()),
        };
        let deadline = tokio::time::Instant::now() + FORMING;
        loop {
            let tried = async {
                let mut inner = waits.inner.lock().await;
                waits.make_topic(&mut inner).await?;
                waits.read(&mut inner).await?;
                waits.keep_key(&mut inner).await
            };
            match tried.await {
                Ok(()) => return Ok(waits),
                Err(e) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!(error = e, "the runs that wait cannot be read yet");
                    waits.inner.lock().await.client = None;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(format!("cannot read the runs that wait: {e}")),
            }
        }
    }

    /// The address a token's page and calls are at.
    pub fn address(&self, token: &str) -> String {
        format!("{}/waits/{token}", self.base)
    }

    async fn client<'a>(&self, inner: &'a mut Inner) -> Result<&'a mut Routed, String> {
        if inner.client.is_none() {
            inner.client = Some(
                Routed::connect(&self.broker, CONNECT)
                    .await
                    .map_err(|e| format!("cannot reach the broker at {}: {e}", self.broker))?,
            );
        }
        inner
            .client
            .as_mut()
            .ok_or_else(|| "no connection to the broker".to_owned())
    }

    async fn make_topic(&self, inner: &mut Inner) -> Result<(), String> {
        let client = self.client(inner).await?;
        let have = client
            .topics()
            .await
            .map_err(|e| format!("cannot list topics: {e}"))?;
        if have.iter().any(|t| t.name == TOPIC) {
            return Ok(());
        }
        let retention = i64::try_from(RETENTION.as_millis()).unwrap_or(i64::MAX);
        match client.create_topic(TOPIC, 1, Some(retention)).await {
            Ok(()) | Err(ClientError::Refused { code: 36, .. }) => Ok(()),
            Err(e) => Err(format!("cannot make the topic {TOPIC}: {e}")),
        }
    }

    /// The key tokens are made with: the first written, the same on every node; written again now and then.
    async fn keep_key(&self, inner: &mut Inner) -> Result<(), String> {
        let stale = inner.key_written < now_ms() - KEY_AGAIN_MS;
        if inner.key.is_some() && !stale {
            return Ok(());
        }
        let key = match &inner.key {
            Some(k) => k.clone(),
            None => {
                let mut fresh = [0u8; 32];
                ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut fresh)
                    .map_err(|_| "cannot make a random key".to_owned())?;
                fresh.to_vec()
            }
        };
        let value = json!({ "key": base64::engine::general_purpose::STANDARD.encode(&key) });
        self.write(inner, "key", Some(&value)).await.map(drop)
    }

    /// Reads what was written since the last read.
    async fn read(&self, inner: &mut Inner) -> Result<(), String> {
        loop {
            let from = inner.next;
            let client = self.client(inner).await?;
            let fetched = client
                .fetch(TOPIC, &[(0, from)], 0, READ_BYTES)
                .await
                .map_err(|e| format!("cannot read {TOPIC}: {e}"))?;
            let Some(answer) = fetched.into_iter().next() else {
                return Ok(());
            };
            match answer.error_code {
                0 => {}
                1 if from < answer.log_start => {
                    inner.next = answer.log_start;
                    continue;
                }
                code => return Err(format!("cannot read {TOPIC} (error code {code})")),
            }
            let mut batches = answer.batches;
            let mut moved = false;
            while !batches.is_empty() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                for record in set.records {
                    if record.offset < inner.next || record.control {
                        continue;
                    }
                    inner.next = record.offset + 1;
                    moved = true;
                    fold(inner, &record);
                }
            }
            if !moved || inner.next >= answer.high_watermark {
                return Ok(());
            }
        }
    }

    /// Writes a record and reads up to it: where it is.
    async fn write(&self, inner: &mut Inner, key: &str, value: Option<&Json>) -> Result<i64, String> {
        let bytes = value.map(|v| v.to_string().into_bytes());
        let message = Outgoing {
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: bytes.map(Bytes::from),
            headers: Vec::new(),
        };
        let client = self.client(inner).await?;
        let offset = match client.produce(TOPIC, 0, &[message], now_ms()).await {
            Ok(o) => o,
            Err(e) => {
                inner.client = None;
                return Err(format!("cannot keep it: {e}"));
            }
        };
        self.read(inner).await?;
        Ok(offset)
    }

    /// The record at `offset`, read again from the log.
    async fn record_at(&self, inner: &mut Inner, offset: i64) -> Result<Json, String> {
        let client = self.client(inner).await?;
        let fetched = client
            .fetch(TOPIC, &[(0, offset)], 0, READ_BYTES)
            .await
            .map_err(|e| format!("cannot read {TOPIC}: {e}"))?;
        for answer in fetched {
            let mut batches = answer.batches;
            while !batches.is_empty() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                if let Some(r) = set.records.iter().find(|r| r.offset == offset) {
                    return r
                        .value
                        .as_ref()
                        .and_then(|v| serde_json::from_slice(v).ok())
                        .ok_or_else(|| "a record of the runs that wait does not read".to_owned());
                }
            }
        }
        Err("a run that waits is no longer kept".to_owned())
    }

    fn key_of(inner: &Inner) -> Result<hmac::Key, String> {
        inner
            .key
            .as_ref()
            .map(|k| hmac::Key::new(hmac::HMAC_SHA256, k))
            .ok_or_else(|| "the runs that wait have no key yet".to_owned())
    }

    /// The tokens of a part of a run (see [`super::tokens`]).
    pub(crate) async fn tokens(
        &self,
        workflow: &crate::model::Workflow,
        seed: &str,
        kept: &BTreeMap<String, String>,
        fresh: &str,
    ) -> Result<BTreeMap<String, String>, String> {
        let inner = self.inner.lock().await;
        Ok(super::tokens(&Self::key_of(&inner)?, workflow, seed, kept, fresh))
    }

    /// Keeps a run that waits. The outer error is the broker's (the run is tried again); the inner one says why the
    /// run cannot wait (it fails, saying so).
    pub(crate) async fn pause(&self, paused: &Paused) -> Result<Result<(), String>, String> {
        let token = &paused.waiting.token;
        let state = serde_json::to_value(paused).map_err(|e| e.to_string())?;
        if state.to_string().len() > MAX_STATE {
            return Ok(Err(format!(
                "a run that waits keeps what its steps did so far, at most {} MB: this one's is more",
                MAX_STATE / (1024 * 1024)
            )));
        }
        let mut inner = self.inner.lock().await;
        self.read(&mut inner).await?;
        // Kept already (the same run, cut short and run again), or finished: nothing more to keep.
        if inner.entries.contains_key(token) || inner.done.contains_key(token) {
            return Ok(Ok(()));
        }
        let of_workflow = inner
            .entries
            .values()
            .filter(|e| e.summary.workflow == paused.workflow)
            .count();
        if of_workflow >= MAX_WAITING || inner.entries.len() >= MAX_ALL {
            return Ok(Err(format!(
                "too many runs wait already ({of_workflow} of this workflow's; at most {MAX_WAITING})"
            )));
        }
        self.write(&mut inner, &format!("wait/{token}"), Some(&state))
            .await?;
        Ok(Ok(()))
    }

    /// The run waiting at `token`, if one is.
    pub async fn find(&self, token: &str) -> Result<Option<Summary>, String> {
        let mut inner = self.inner.lock().await;
        if let Err(e) = self.read(&mut inner).await {
            inner.client = None;
            return Err(e);
        }
        Ok(inner.entries.get(token).map(|e| e.summary.clone()))
    }

    /// The run waiting at `token` and, for a form or an approval, the form its page shows.
    pub async fn open_wait(&self, token: &str) -> Result<Option<(Summary, Option<Form>)>, String> {
        let mut inner = self.inner.lock().await;
        if let Err(e) = self.read(&mut inner).await {
            inner.client = None;
            return Err(e);
        }
        let Some(entry) = inner.entries.get(token) else {
            return Ok(None);
        };
        let (summary, at) = (entry.summary.clone(), entry.at);
        if !matches!(summary.kind.as_str(), "form" | "approval") {
            return Ok(Some((summary, None)));
        }
        let record = self.record_at(&mut inner, at).await?;
        let form = record.pointer("/waiting/form").and_then(Form::from_json);
        Ok(Some((summary, form)))
    }

    /// The runs of `workflow` that wait, the oldest first.
    pub async fn waiting(&self, workflow: &str) -> Result<Vec<Summary>, String> {
        let mut inner = self.inner.lock().await;
        if let Err(e) = self.read(&mut inner).await {
            inner.client = None;
            return Err(e);
        }
        let mut out: Vec<Summary> = inner
            .entries
            .values()
            .filter(|e| e.summary.workflow == workflow)
            .map(|e| e.summary.clone())
            .collect();
        out.sort_by_key(|s| s.since);
        Ok(out)
    }

    /// An answer for the run waiting at `token` (`by`: `answer`, `timeout` or `time`): the first in the log is the
    /// one; the run is told of it through its workflow's own topic.
    pub async fn answer(&self, token: &str, by: &str, answer: &Json) -> Result<Answered, String> {
        if answer.to_string().len() > MAX_STATE {
            return Err(format!("an answer of at most {} MB", MAX_STATE / (1024 * 1024)));
        }
        let mut inner = self.inner.lock().await;
        let outcome = self.claim(&mut inner, token, by, answer).await;
        if outcome.is_err() {
            inner.client = None;
        }
        outcome
    }

    async fn claim(
        &self,
        inner: &mut Inner,
        token: &str,
        by: &str,
        answer: &Json,
    ) -> Result<Answered, String> {
        self.read(inner).await?;
        let Some(entry) = inner.entries.get(token) else {
            return Ok(Answered::Unknown);
        };
        if let Some((first, _, _)) = &entry.claim {
            return Ok(Answered::Already(first.clone()));
        }
        let mine = self
            .write(
                inner,
                &format!("claim/{token}"),
                Some(&json!({ "by": by, "at": now_ms(), "answer": answer })),
            )
            .await?;
        match inner.entries.get(token).and_then(|e| e.claim.clone()) {
            Some((_, _, at)) if at == mine => {
                self.tell(inner, token).await?;
                Ok(Answered::Taken)
            }
            Some((first, _, _)) => Ok(Answered::Already(first)),
            None => Ok(Answered::Unknown),
        }
    }

    /// Tells the run its answer is in: an event to its workflow's own topic, naming the token - always to the same
    /// partition for one token.
    async fn tell(&self, inner: &mut Inner, token: &str) -> Result<(), String> {
        let Some(workflow) = inner.entries.get(token).map(|e| e.summary.workflow.clone()) else {
            return Ok(());
        };
        let topic = Topics::of(&workflow).input;
        let spread = u32::try_from(PARTITIONS.max(2) - 1).unwrap_or(1);
        let lane = token
            .bytes()
            .fold(0u32, |h, b| h.wrapping_mul(31).wrapping_add(u32::from(b)));
        let partition = 1 + i32::try_from(lane % spread).unwrap_or(0);
        let message = Outgoing {
            key: Some(Bytes::from(token.as_bytes().to_vec())),
            value: Some(Bytes::from(envelope(
                &Json::Null,
                &json!({ "trigger": "resume", "token": token, "at": iso(now_ms()) }),
            ))),
            headers: Vec::new(),
        };
        let client = self.client(inner).await?;
        if let Err(e) = client.produce(&topic, partition, &[message], now_ms()).await {
            inner.client = None;
            return Err(format!("cannot tell the run its answer is in: {e}"));
        }
        self.write(
            inner,
            &format!("queued/{token}"),
            Some(&json!({ "at": now_ms() })),
        )
        .await
        .map(drop)
    }

    /// The run waiting at `token`, taken up by run `run` (the event that told it): what it kept and its answer - or
    /// nothing, when another run took it up first, or nothing waits there any more.
    pub(crate) async fn take(&self, token: &str, run: &str) -> Result<Option<(Paused, Claim)>, String> {
        let mut inner = self.inner.lock().await;
        let taken = self.taking(&mut inner, token, run).await;
        if taken.is_err() {
            inner.client = None;
        }
        taken
    }

    async fn taking(
        &self,
        inner: &mut Inner,
        token: &str,
        run: &str,
    ) -> Result<Option<(Paused, Claim)>, String> {
        self.read(inner).await?;
        let Some(entry) = inner.entries.get(token) else {
            return Ok(None);
        };
        let Some((_, _, claim_at)) = entry.claim.clone() else {
            return Ok(None);
        };
        let at = entry.at;
        match entry.resumed.clone() {
            Some(first) if first != run => return Ok(None),
            Some(_) => {}
            None => {
                self.write(inner, &format!("resumed/{token}"), Some(&json!({ "run": run })))
                    .await?;
                let first = inner.entries.get(token).and_then(|e| e.resumed.clone());
                if first.as_deref() != Some(run) {
                    return Ok(None);
                }
            }
        }
        let paused: Paused = serde_json::from_value(self.record_at(inner, at).await?)
            .map_err(|e| format!("a run that waits does not read: {e}"))?;
        let claim = self.record_at(inner, claim_at).await?;
        Ok(Some((
            paused,
            Claim {
                by: claim
                    .get("by")
                    .and_then(Json::as_str)
                    .unwrap_or("answer")
                    .to_owned(),
                at: claim.get("at").and_then(Json::as_i64).unwrap_or_else(now_ms),
                answer: claim.get("answer").cloned().unwrap_or(Json::Null),
            },
        )))
    }

    /// The run waiting at `token` has gone on and its part is kept: its token is done with.
    pub(crate) async fn done(&self, token: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        let written = self
            .write(&mut inner, &format!("done/{token}"), Some(&json!({})))
            .await;
        if written.is_err() {
            inner.client = None;
        }
        written.map(drop)
    }

    /// Answers the waits whose time has come (a time reached, an answer too late), and tells again a run whose answer
    /// it was never told of (a node lost between the two). Every node does it; the log decides who did it first.
    pub async fn sweep(&self) -> Result<usize, String> {
        let mut inner = self.inner.lock().await;
        let swept = self.sweeping(&mut inner).await;
        if swept.is_err() {
            inner.client = None;
        }
        swept
    }

    async fn sweeping(&self, inner: &mut Inner) -> Result<usize, String> {
        self.read(inner).await?;
        self.keep_key(inner).await?;
        let now = now_ms();
        let cutoff = now - DONE_KEPT_MS;
        inner.done.retain(|_, at| *at >= cutoff);
        let due: Vec<(String, &'static str)> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.claim.is_none() && e.summary.until <= now)
            .map(|(t, e)| {
                (
                    t.clone(),
                    if e.summary.kind == "time" {
                        "time"
                    } else {
                        "timeout"
                    },
                )
            })
            .collect();
        let untold: Vec<String> = inner
            .entries
            .iter()
            .filter(|(_, e)| {
                !e.queued
                    && e.claim
                        .as_ref()
                        .is_some_and(|(_, at, _)| *at + TELL_AGAIN_MS <= now)
            })
            .map(|(t, _)| t.clone())
            .collect();
        let mut count = 0;
        for (token, by) in due {
            if self.claim(inner, &token, by, &json!({})).await? == Answered::Taken {
                count += 1;
            }
        }
        for token in untold {
            self.tell(inner, &token).await?;
            count += 1;
        }
        Ok(count)
    }
}

/// One record read into what the node keeps.
fn fold(inner: &mut Inner, record: &Record) {
    let Some(key) = record
        .key
        .as_ref()
        .map(|k| String::from_utf8_lossy(k).into_owned())
    else {
        return;
    };
    let value: Option<Json> = record.value.as_ref().and_then(|v| serde_json::from_slice(v).ok());
    if key == "key" {
        if let Some(k) = value
            .as_ref()
            .and_then(|v| v.get("key").and_then(Json::as_str))
            .and_then(|k| base64::engine::general_purpose::STANDARD.decode(k).ok())
            .filter(|k| k.len() >= 32)
        {
            inner.key.get_or_insert(k);
            inner.key_written = inner.key_written.max(record.timestamp);
        }
        return;
    }
    let Some((kind, token)) = key.split_once('/') else {
        return;
    };
    let token = token.to_owned();
    match kind {
        "wait" => {
            if inner.entries.contains_key(&token) || inner.done.contains_key(&token) {
                return;
            }
            let Some(head) = record
                .value
                .as_ref()
                .and_then(|v| serde_json::from_slice::<Head>(v).ok())
            else {
                return;
            };
            let title = head
                .waiting
                .form
                .as_ref()
                .and_then(|f| f.get("title").and_then(Json::as_str))
                .unwrap_or_default()
                .to_owned();
            inner.entries.insert(
                token.clone(),
                Entry {
                    summary: Summary {
                        token,
                        workflow: head.workflow,
                        run: head.run,
                        step: head.waiting.step,
                        kind: head.waiting.kind,
                        title,
                        since: head.waiting.since,
                        until: head.waiting.until,
                        on_timeout: head.waiting.on_timeout,
                        answered: None,
                    },
                    at: record.offset,
                    claim: None,
                    queued: false,
                    resumed: None,
                },
            );
        }
        "done" => {
            inner.entries.remove(&token);
            inner.done.insert(token, record.timestamp);
        }
        _ => {
            let Some(entry) = inner.entries.get_mut(&token) else {
                return;
            };
            let value = value.unwrap_or(Json::Null);
            match kind {
                "claim" if entry.claim.is_none() => {
                    let by = value
                        .get("by")
                        .and_then(Json::as_str)
                        .unwrap_or("answer")
                        .to_owned();
                    entry.summary.answered = Some(by.clone());
                    entry.claim = Some((
                        by,
                        value.get("at").and_then(Json::as_i64).unwrap_or(record.timestamp),
                        record.offset,
                    ));
                }
                "queued" => entry.queued = true,
                "resumed" if entry.resumed.is_none() => {
                    entry.resumed = value.get("run").and_then(Json::as_str).map(str::to_owned);
                }
                _ => {}
            }
        }
    }
}

/// Sweeps every second (see [`Waits::sweep`]) until `stop` says so.
pub async fn keep_sweeping(waits: Arc<Waits>, mut stop: watch::Receiver<bool>) {
    loop {
        if let Err(e) = waits.sweep().await {
            tracing::warn!(error = e, "the runs that wait could not be looked over");
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = stop.changed() => return,
        }
        if *stop.borrow() {
            return;
        }
    }
}
