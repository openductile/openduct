// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An agent's memory: the conversation kept under a key between runs, and where it is kept.
//!
//! It is kept on the broker, in one compacted topic ([`LogMemory`]) - what OpenDuct already has, and what its
//! studio keeps everything else in: durable (written to disk before it is acknowledged), the same on every node of a
//! cluster, and nothing more to run. The newest record of a key is its conversation, a record with no value forgets
//! it, and compaction removes what was replaced. A node keeps in memory only where each key's newest record is (an
//! offset and a time), read from the topic as it grows, and reads a conversation when a run asks for it - so a node
//! that has many conversations does not hold them.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{ClientError, Outgoing, Routed, records};
use openduct_expr::{Env, Value};
use serde_json::{Value as Json, json};
use tokio::sync::Mutex;

use super::{Agent, Pending, REMEMBERED, cut};
use crate::run::Effects;

/// The topic agents' memories are kept in.
pub const MEMORY_TOPIC: &str = "__openduct.memory";
/// The largest conversation kept, in bytes as JSON: the oldest turns are let go to keep under it.
const MAX_KEPT: usize = 256 * 1024;
/// The longest key, in characters.
const MAX_KEY: usize = 200;
/// The header a record's forgetting time is in (milliseconds since 1970).
const FORGET_AT: &str = "openduct-forget-at";
/// The most bytes one read asks for.
const READ_BYTES: i32 = 4 * 1024 * 1024;
/// How many conversations past their time one write forgets on the way.
const SWEEP: usize = 16;
/// The most conversations kept, every workspace's together: what each node holds of them is their key and two
/// numbers, so this is about 30 MB of a node's memory at most.
pub const MAX_CONVERSATIONS: usize = 100_000;

/// What a run's memory began with, and how to keep the turn it adds.
pub(super) struct Recalled {
    /// The messages given back: `{role, content}`, oldest first.
    pub history: Vec<Json>,
    /// The key as the step named it, and as it is kept (`<workflow>/<step>/<key>`); none when memory is off.
    key: Option<(String, String)>,
    cleared: bool,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// What the step's memory holds for this run: forgotten first, when its `clear` holds.
pub(super) async fn recall(
    effects: &dyn Effects,
    agent: &Agent,
    env: &Env<'_>,
    scope: &str,
) -> Result<Recalled, String> {
    let Some(m) = &agent.memory else {
        return Ok(Recalled {
            history: Vec::new(),
            key: None,
            cleared: false,
        });
    };
    let key = m.key.render(env).map_err(|e| format!("memory.key: {e}"))?;
    let key = key.trim().to_owned();
    if key.is_empty() {
        return Err("memory.key: it is empty in this run - whose conversation is it?".to_owned());
    }
    if key.chars().count() > MAX_KEY {
        return Err(format!("memory.key: at most {MAX_KEY} characters"));
    }
    let kept_as = format!("{scope}/{key}");
    let cleared = match &m.clear {
        Some(c) => matches!(
            c.evaluate(env).map_err(|e| format!("memory.clear: {e}"))?,
            Value::Bool(true)
        ),
        None => false,
    };
    let mut history = Vec::new();
    // vector stores: kept on the broker, or in the store the step names.
    if cleared {
        super::stores::keep_in(effects, &m.store, &kept_as, None, m.forget_after).await?;
    } else {
        let found = super::stores::recall_in(effects, &m.store, &kept_as).await?;
        if let Json::Array(list) = serde_json::from_str::<Json>(&found.to_json()).unwrap_or(Json::Null) {
            history = list
                .into_iter()
                .filter(|t| {
                    matches!(t.get("role").and_then(Json::as_str), Some("user" | "assistant"))
                        && t.get("content").is_some_and(Json::is_string)
                })
                .map(|t| json!({ "role": t.get("role"), "content": t.get("content") }))
                .collect();
        }
        let most = m.window * 2;
        if history.len() > most {
            history.drain(..history.len() - most);
        }
    }
    Ok(Recalled {
        history,
        key: Some((key, kept_as)),
        cleared,
    })
}

impl Recalled {
    /// Keeps the run's turn - its input and the agent's answer - after what was there, the last turns of it; what the
    /// record says of it. A memory that cannot be written does not fail the step (its tools have run): it is said.
    pub(super) async fn keep(
        &self,
        effects: &dyn Effects,
        agent: &Agent,
        input: &str,
        answer: &str,
    ) -> Option<Json> {
        let ((key, kept_as), m) = (self.key.as_ref()?, agent.memory.as_ref()?);
        let mut turns = self.history.clone();
        turns.push(json!({ "role": "user", "content": cut(input, REMEMBERED) }));
        turns.push(json!({ "role": "assistant", "content": cut(answer, REMEMBERED) }));
        let most = m.window * 2;
        if turns.len() > most {
            turns.drain(..turns.len() - most);
        }
        while turns.len() > 2 && Json::Array(turns.clone()).to_string().len() > MAX_KEPT {
            turns.drain(..2);
        }
        let count = turns.len();
        let mut said = json!({
            "key": key, "recalled": self.history.len(), "kept": count, "cleared": self.cleared,
        });
        // vector stores: kept on the broker, or in the store the step names.
        let turns = Some(Value::from_json(&Json::Array(turns)));
        let written = super::stores::keep_in(effects, &m.store, kept_as, turns, m.forget_after).await;
        match written {
            Ok(v) if matches!(&v, Value::Record(r) if r.get("test").is_some()) => {
                crate::apps::put(&mut said, "test", json!("not kept"));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = e, "an agent's memory could not be kept");
                crate::apps::put(&mut said, "kept", json!(0));
                crate::apps::put(&mut said, "error", json!(e));
            }
        }
        Some(said)
    }
}

/// Agents' memories on the broker at an address: see the module's description.
#[derive(Debug)]
pub struct LogMemory {
    broker: String,
    topic: String,
    /// The most conversations kept, every workspace's together.
    most: usize,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    client: Option<Routed>,
    made: bool,
    /// Each key's newest record: its offset, and when it is to be forgotten.
    index: HashMap<String, (i64, i64)>,
    next: i64,
}

impl LogMemory {
    /// Memories kept on the broker at `broker` (`host:port`), in [`MEMORY_TOPIC`]; nothing is reached until a run
    /// asks.
    pub fn new(broker: &str) -> Self {
        Self::in_topic(broker, MEMORY_TOPIC)
    }

    /// Memories kept in another topic.
    pub fn in_topic(broker: &str, topic: &str) -> Self {
        Self {
            broker: broker.to_owned(),
            topic: topic.to_owned(),
            most: MAX_CONVERSATIONS,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// These memories, keeping at most `most` conversations ([`MAX_CONVERSATIONS`] unless said): past it, a new one
    /// lets go of the one nearest to being forgotten.
    #[must_use]
    pub fn keeping_at_most(mut self, most: usize) -> Self {
        self.most = most.max(1);
        self
    }

    async fn client<'a>(&self, inner: &'a mut Inner) -> Result<&'a mut Routed, String> {
        if inner.client.is_none() {
            let client = Routed::connect(&self.broker, Duration::from_secs(10))
                .await
                .map_err(|e| format!("cannot reach the broker for memory: {e}"))?;
            inner.client = Some(client);
        }
        inner
            .client
            .as_mut()
            .ok_or_else(|| "no connection to the broker".to_owned())
    }

    /// The topic, made the first time (compacted: only each key's newest record stays).
    async fn make(&self, inner: &mut Inner) -> Result<(), String> {
        if inner.made {
            return Ok(());
        }
        let topic = self.topic.clone();
        let client = self.client(inner).await?;
        let have = client
            .topics()
            .await
            .map_err(|e| format!("cannot list the broker's topics: {e}"))?;
        if !have.iter().any(|t| t.name == topic) {
            match client
                .create_topic_with(&topic, 1, None, &[("cleanup.policy", "compact")])
                .await
            {
                Ok(()) | Err(ClientError::Refused { code: 36, .. }) => {}
                Err(e) => return Err(format!("cannot make the topic {topic}: {e}")),
            }
        }
        inner.made = true;
        Ok(())
    }

    /// Reads what was written since the last read, into the index.
    async fn catch_up(&self, inner: &mut Inner) -> Result<(), String> {
        loop {
            let from = inner.next;
            let topic = self.topic.clone();
            let client = self.client(inner).await?;
            let fetched = client
                .fetch(&topic, &[(0, from)], 0, READ_BYTES)
                .await
                .map_err(|e| format!("cannot read {topic}: {e}"))?;
            let Some(answer) = fetched.into_iter().next() else {
                return Ok(());
            };
            match answer.error_code {
                0 => {}
                1 if from < answer.log_start => {
                    inner.next = answer.log_start;
                    continue;
                }
                code => return Err(format!("cannot read {topic} (error code {code})")),
            }
            let moved = index(inner, answer.batches)?;
            if !moved || inner.next >= answer.high_watermark {
                return Ok(());
            }
        }
    }

    /// The conversation under `key`: its turns, or null.
    async fn read(&self, key: &str) -> Result<Value, String> {
        let mut inner = self.inner.lock().await;
        let outcome = self.read_locked(&mut inner, key).await;
        if outcome.is_err() {
            inner.client = None;
        }
        outcome
    }

    async fn read_locked(&self, inner: &mut Inner, key: &str) -> Result<Value, String> {
        self.make(inner).await?;
        self.catch_up(inner).await?;
        let Some(&(offset, forget_at)) = inner.index.get(key) else {
            return Ok(Value::Null);
        };
        if forget_at <= now_ms() {
            return Ok(Value::Null);
        }
        let topic = self.topic.clone();
        let client = self.client(inner).await?;
        let fetched = client
            .fetch(&topic, &[(0, offset)], 0, READ_BYTES)
            .await
            .map_err(|e| format!("cannot read {topic}: {e}"))?;
        let Some(mut batches) = fetched.into_iter().next().map(|f| f.batches) else {
            return Ok(Value::Null);
        };
        while !batches.is_empty() {
            let Ok(set) =
                RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            else {
                break;
            };
            for record in set.records {
                if record.offset == offset && record.key.as_deref() == Some(key.as_bytes()) {
                    let kept: Json = record
                        .value
                        .as_ref()
                        .and_then(|v| serde_json::from_slice(v).ok())
                        .unwrap_or(Json::Null);
                    return Ok(Value::from_json(kept.get("turns").unwrap_or(&Json::Null)));
                }
            }
        }
        Ok(Value::Null)
    }

    /// Keeps `turns` under `key` until `keep` from now; with none, forgets it.
    async fn write(&self, key: &str, turns: Option<Value>, keep: Duration) -> Result<Value, String> {
        let now = now_ms();
        let value = match &turns {
            Some(t) => {
                let turns: Json = serde_json::from_str(&t.to_json()).unwrap_or(Json::Null);
                let bytes = json!({ "schema": 1, "turns": turns, "updated": now })
                    .to_string()
                    .into_bytes();
                if bytes.len() > MAX_KEPT + 1024 {
                    return Err(format!("a memory of more than {} KiB", MAX_KEPT / 1024));
                }
                Some(Bytes::from(bytes))
            }
            None => None,
        };
        let forget_at = now.saturating_add(i64::try_from(keep.as_millis()).unwrap_or(i64::MAX));
        let headers = if value.is_some() {
            vec![(FORGET_AT.to_owned(), Bytes::from(forget_at.to_string()))]
        } else {
            Vec::new()
        };
        let message = Outgoing {
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value,
            headers,
        };
        let mut inner = self.inner.lock().await;
        let outcome = self.write_locked(&mut inner, vec![message]).await;
        if outcome.is_err() {
            inner.client = None;
        }
        outcome?;
        Ok(Value::from_json(&json!({ "kept": turns.is_some() })))
    }

    async fn write_locked(&self, inner: &mut Inner, mut messages: Vec<Outgoing>) -> Result<(), String> {
        self.make(inner).await?;
        self.catch_up(inner).await?;
        // Conversations past their time are forgotten on the way: compaction then removes them from the disk.
        let now = now_ms();
        let expired: Vec<String> = inner
            .index
            .iter()
            .filter(|(_, (_, forget_at))| *forget_at <= now)
            .map(|(k, _)| k.clone())
            .take(SWEEP)
            .collect();
        let mut forgotten = expired;
        // At the most conversations, a new one lets go of the one nearest to being forgotten.
        let new = messages
            .iter()
            .filter(|m| m.value.is_some())
            .filter_map(|m| m.key.as_deref())
            .filter(|k| !inner.index.contains_key(String::from_utf8_lossy(k).as_ref()))
            .count();
        let over = (inner.index.len() + new)
            .saturating_sub(forgotten.len())
            .saturating_sub(self.most);
        if over > 0 {
            let mut oldest: Vec<(&String, i64)> = inner
                .index
                .iter()
                .filter(|(k, _)| !forgotten.contains(k))
                .map(|(k, (_, forget_at))| (k, *forget_at))
                .collect();
            oldest.sort_by_key(|(_, forget_at)| *forget_at);
            forgotten.extend(oldest.into_iter().take(over).map(|(k, _)| k.clone()));
        }
        for key in forgotten {
            if messages.iter().all(|m| m.key.as_deref() != Some(key.as_bytes())) {
                messages.insert(
                    0,
                    Outgoing {
                        key: Some(Bytes::from(key.into_bytes())),
                        value: None,
                        headers: Vec::new(),
                    },
                );
            }
        }
        let topic = self.topic.clone();
        let client = self.client(inner).await?;
        client
            .produce(&topic, 0, &messages, now)
            .await
            .map_err(|e| format!("cannot keep the memory: {e}"))?;
        self.catch_up(inner).await
    }
}

/// Applies the records in `batches` to the index; whether any was read.
fn index(inner: &mut Inner, mut batches: Bytes) -> Result<bool, String> {
    let mut moved = false;
    while !batches.is_empty() {
        let set =
            match RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            {
                Ok(set) => set,
                // A batch cut short at the end of an answer is read whole next time.
                Err(_) if moved => break,
                Err(e) => return Err(format!("cannot read a record batch: {e}")),
            };
        for record in set.records {
            if record.offset < inner.next || record.control {
                continue;
            }
            inner.next = record.offset + 1;
            moved = true;
            let Some(key) = record
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).into_owned())
            else {
                continue;
            };
            if record.value.is_none() {
                inner.index.remove(&key);
                continue;
            }
            let forget_at = record
                .headers
                .iter()
                .find(|(k, _)| k.as_str() == FORGET_AT)
                .and_then(|(_, v)| v.as_ref())
                .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
                .unwrap_or(i64::MAX);
            inner.index.insert(key, (record.offset, forget_at));
        }
    }
    Ok(moved)
}

impl crate::effects::Memory for LogMemory {
    fn recall<'a>(&'a self, key: &'a str) -> Pending<'a> {
        Box::pin(self.read(key))
    }

    fn remember<'a>(&'a self, key: &'a str, turns: Option<Value>, keep: Duration) -> Pending<'a> {
        Box::pin(self.write(key, turns, keep))
    }
}
