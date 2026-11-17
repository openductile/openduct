// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What the studio keeps - accounts now; workflows, credentials and runs later - as messages in a topic of the
//! broker's own: one partition, kept for ever, the last message of a key its value and a message with no value
//! taking the key away. Every node of a cluster reads the same topic, so every node sees the same records, and
//! nothing needs a database of its own.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{Outgoing, Routed, records};
use serde_json::Value;
use tokio::sync::Mutex;

/// How long a connection to the broker may take.
const CONNECT: Duration = Duration::from_secs(10);
/// The most bytes one read asks for.
const READ_BYTES: i32 = 4 * 1024 * 1024;
/// How long a forming cluster has to become ready for the studio's records.
const FORMING: Duration = Duration::from_secs(90);
/// The refusals a forming cluster gives: no leader yet, not the controller, timed out, no broker ready.
const RETRIABLE: &[i16] = &[5, 6, 7, 8, 15, 41];
/// The largest record, in bytes as JSON.
pub(crate) const MAX_RECORD: usize = 1024 * 1024;

/// A topic's records by key.
#[derive(Debug)]
pub(crate) struct Records {
    topic: String,
    broker: String,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    client: Option<Routed>,
    values: BTreeMap<String, Value>,
    /// The first value ever written for each key, when kept ([`Records::open_keeping_first`]): the same on every
    /// node, since every node reads the same log.
    first: Option<BTreeMap<String, Value>>,
    /// The offset of the next message to read.
    next: i64,
}

impl Records {
    /// The records of `topic` on the broker at `broker`, the topic made if it is not there yet.
    pub(crate) async fn open(broker: &str, topic: &str) -> Result<Self, String> {
        Self::opened(broker, topic, false).await
    }

    /// As [`Records::open`], keeping too the first value written for each key ([`Records::first`]).
    pub(crate) async fn open_keeping_first(broker: &str, topic: &str) -> Result<Self, String> {
        Self::opened(broker, topic, true).await
    }

    async fn opened(broker: &str, topic: &str, keep_first: bool) -> Result<Self, String> {
        let records = Self {
            topic: topic.to_owned(),
            broker: broker.to_owned(),
            inner: Mutex::new(Inner {
                first: keep_first.then(BTreeMap::new),
                ..Inner::default()
            }),
        };
        // A cluster that is forming has no leader, or no broker ready, for a moment: asked again until it has.
        let deadline = tokio::time::Instant::now() + FORMING;
        loop {
            match records.make_topic().await {
                Ok(()) => break,
                Err((retry, e)) if retry && tokio::time::Instant::now() < deadline => {
                    tracing::debug!(
                        topic,
                        error = e,
                        "the cluster is not ready for the studio's records yet"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err((_, e)) => return Err(e),
            }
        }
        // The topic may be there from before while its partition has no leader yet (a node just restarted): the
        // first read waits for one as long as making the topic would have.
        loop {
            match records.catch_up().await {
                Ok(()) => break,
                Err(e) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!(topic, error = e, "the studio's records cannot be read yet");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(records)
    }

    /// The topic made if it is not there yet; on failure, whether asking again may help.
    async fn make_topic(&self) -> Result<(), (bool, String)> {
        use openduct_broker::client::ClientError;
        let mut inner = self.inner.lock().await;
        let topic = self.topic.as_str();
        let client = match self.client(&mut inner).await {
            Ok(c) => c,
            Err(e) => return Err((true, e)),
        };
        let listed = client.topics().await;
        let exists = match listed {
            Ok(topics) => topics.iter().any(|t| t.name == topic),
            Err(e) => {
                inner.client = None;
                return Err((true, format!("cannot list the broker's topics: {e}")));
            }
        };
        if exists {
            return Ok(());
        }
        match client.create_topic(topic, 1, None).await {
            // Made by another node a moment ago.
            Ok(()) | Err(ClientError::Refused { code: 36, .. }) => Ok(()),
            Err(ClientError::Refused { code, message }) => Err((
                RETRIABLE.contains(&code),
                format!(
                    "cannot make the topic {topic}: {}",
                    ClientError::Refused { code, message }
                ),
            )),
            Err(e) => {
                inner.client = None;
                Err((true, format!("cannot make the topic {topic}: {e}")))
            }
        }
    }

    async fn client<'a>(&self, inner: &'a mut Inner) -> Result<&'a mut Routed, String> {
        if inner.client.is_none() {
            let client = Routed::connect(&self.broker, CONNECT)
                .await
                .map_err(|e| format!("cannot reach the broker at {}: {e}", self.broker))?;
            inner.client = Some(client);
        }
        inner
            .client
            .as_mut()
            .ok_or_else(|| "no connection to the broker".to_owned())
    }

    /// Reads what was written since the last read.
    pub(crate) async fn catch_up(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        let result = self.read_locked(&mut inner).await;
        if result.is_err() {
            // The next call connects again.
            inner.client = None;
        }
        result
    }

    async fn read_locked(&self, inner: &mut Inner) -> Result<(), String> {
        loop {
            let from = inner.next;
            let client = self.client(inner).await?;
            let fetched = client
                .fetch(&self.topic, &[(0, from)], 0, READ_BYTES)
                .await
                .map_err(|e| format!("cannot read {}: {e}", self.topic))?;
            let Some(answer) = fetched.into_iter().next() else {
                return Ok(());
            };
            match answer.error_code {
                0 => {}
                // Read from before the log's start: carry on from its start.
                1 if from < answer.log_start => {
                    inner.next = answer.log_start;
                    continue;
                }
                code => return Err(format!("cannot read {} (error code {code})", self.topic)),
            }
            let moved = apply(inner, answer.batches)?;
            if !moved || inner.next >= answer.high_watermark {
                return Ok(());
            }
        }
    }

    /// The record of `key`.
    pub(crate) async fn get(&self, key: &str) -> Option<Value> {
        self.inner.lock().await.values.get(key).cloned()
    }

    /// The first value written for `key`, when kept.
    pub(crate) async fn first(&self, key: &str) -> Option<Value> {
        self.inner.lock().await.first.as_ref()?.get(key).cloned()
    }

    /// Every record, by key.
    pub(crate) async fn all(&self) -> Vec<(String, Value)> {
        let inner = self.inner.lock().await;
        inner.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Whether there are none.
    pub(crate) async fn is_empty(&self) -> bool {
        self.inner.lock().await.values.is_empty()
    }

    /// Writes `value` as the record of `key` (`None` takes it away), and reads it back, in the order the broker
    /// holds it.
    pub(crate) async fn put(&self, key: &str, value: Option<&Value>) -> Result<(), String> {
        let bytes = value.map(|v| v.to_string().into_bytes());
        if bytes.as_ref().is_some_and(|b| b.len() > MAX_RECORD) {
            return Err(format!("a record of more than {MAX_RECORD} bytes"));
        }
        let message = Outgoing {
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: bytes.map(Bytes::from),
            headers: Vec::new(),
        };
        let mut inner = self.inner.lock().await;
        let written = {
            let client = self.client(&mut inner).await?;
            client.produce(&self.topic, 0, &[message], now_ms()).await
        };
        if let Err(e) = written {
            inner.client = None;
            return Err(format!("cannot write to {}: {e}", self.topic));
        }
        let read = self.read_locked(&mut inner).await;
        if read.is_err() {
            inner.client = None;
        }
        read
    }
}

/// Applies the records in `batches`; answers whether any was read.
fn apply(inner: &mut Inner, mut batches: Bytes) -> Result<bool, String> {
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
            match record.value.as_ref() {
                None => {
                    inner.values.remove(&key);
                }
                Some(value) => match serde_json::from_slice::<Value>(value) {
                    Ok(v) => {
                        if let Some(first) = inner.first.as_mut() {
                            first.entry(key.clone()).or_insert_with(|| v.clone());
                        }
                        inner.values.insert(key, v);
                    }
                    Err(e) => tracing::warn!(key, error = %e, "skipping a record that is not JSON"),
                },
            }
        }
    }
    Ok(moved)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
