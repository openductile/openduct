// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The registry kept in step with its topic, on any broker of a cluster.
//!
//! Every broker that serves the registry holds its state in memory and reads the topic (`_schemas`, one
//! partition, compacted) for ever, applying each change as it arrives. A request that only reads is answered
//! from memory. A request that changes something is planned from memory, its changes are written to the topic,
//! and it is answered once this broker has read them back and they are what the registry now holds - so a client
//! told "registered, id 7" finds id 7 here at once, and on every other broker as soon as it has read that far.
//!
//! Two brokers may each plan a registration before seeing the other's. The topic puts them in one order, every
//! broker applies that order, and the later of two that cannot both stand is skipped by all (`Registry::apply`).
//! The broker whose change was skipped sees that it did not take, and plans again from what the topic gave it.
//!
//! It reaches the topic as any client of the cluster does, through the node's shared connections - so it works
//! the same on one broker and on many, whichever of them leads the topic's partition.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{ClientError, EARLIEST, LATEST, Outgoing, Shared, records};
use tokio::sync::{Mutex, watch};

use crate::api::{self, Access, Everything, Outcome, Request, Response};
use crate::log::{self, TOPIC};
use crate::registry::{Change, Registry};

/// How long a write may take to come back through the topic before its client is told it timed out.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// How many times a change that lost to another broker's is planned again. Each loss means another broker's
/// change went through, so the registry as a whole always moves; this bounds how long one request waits its turn.
const ATTEMPTS: u32 = 64;
/// Kafka's answer for a topic that exists already.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// One broker's registry.
pub struct Service {
    shared: Arc<Shared>,
    state: RwLock<Registry>,
    /// The offset after the last record applied.
    read: watch::Sender<i64>,
    /// One change at a time from this broker: each is planned from a state that holds the one before.
    writing: Mutex<()>,
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("read", &*self.read.borrow())
            .finish_non_exhaustive()
    }
}

fn failed(status: u16, code: u32, message: &str) -> Response {
    Response {
        status,
        body: serde_json::json!({ "error_code": code, "message": message }).to_string(),
    }
}

/// The record batches of a fetch answer, each with the offset after its last record - which an empty batch (one
/// compaction left to cover its offsets) has too.
fn batches(mut raw: Bytes) -> Vec<(Bytes, i64)> {
    let mut out = Vec::new();
    while raw.len() >= 61 {
        let field = |at: usize, len: usize| raw.get(at..at + len);
        let (Some(base), Some(length), Some(delta)) = (field(0, 8), field(8, 4), field(23, 4)) else {
            break;
        };
        let base = i64::from_be_bytes(base.try_into().unwrap_or_default());
        let length = i32::from_be_bytes(length.try_into().unwrap_or_default());
        let delta = i32::from_be_bytes(delta.try_into().unwrap_or_default());
        let Some(total) = usize::try_from(length).ok().and_then(|l| l.checked_add(12)) else {
            break;
        };
        if total < 61 || total > raw.len() {
            break;
        }
        out.push((
            raw.split_to(total),
            base.saturating_add(i64::from(delta)).saturating_add(1),
        ));
    }
    out
}

impl Service {
    /// Starts the registry on this node: makes its topic if there is none, reads it to the end, and goes on
    /// reading. Answered once the registry holds everything the topic did when it started.
    pub async fn start(shared: Arc<Shared>) -> Result<Arc<Self>, String> {
        let (from, end) = {
            let mut client = shared
                .lend()
                .await
                .map_err(|e| format!("the registry cannot reach the broker: {e}"))?;
            match client
                .create_topic_with(TOPIC, 1, None, &[("cleanup.policy", "compact")])
                .await
            {
                Ok(()) => tracing::info!(topic = TOPIC, "the registry's topic was created"),
                Err(ClientError::Refused {
                    code: TOPIC_ALREADY_EXISTS,
                    ..
                }) => {}
                Err(e) => return Err(format!("the registry's topic {TOPIC} cannot be created: {e}")),
            }
            let from = client
                .list_offset(TOPIC, 0, EARLIEST)
                .await
                .map_err(|e| e.to_string())?;
            let end = client
                .list_offset(TOPIC, 0, LATEST)
                .await
                .map_err(|e| e.to_string())?;
            (from, end)
        };
        let (read, mut caught_up) = watch::channel(from);
        let service = Arc::new(Self {
            shared,
            state: RwLock::new(Registry::default()),
            read,
            writing: Mutex::new(()),
        });
        tokio::spawn(Arc::clone(&service).follow(from));
        tokio::time::timeout(WRITE_TIMEOUT, caught_up.wait_for(|at| *at >= end))
            .await
            .map_err(|_| format!("the registry did not read {TOPIC} to its end in time"))?
            .map_err(|_| "the registry stopped reading its topic".to_owned())?;
        let subjects = service.state.read().map(|s| s.subjects(true).len()).unwrap_or(0);
        tracing::info!(subjects, records = end - from, "the registry has read its topic");
        Ok(service)
    }

    /// Reads the topic for ever, applying what it holds. Ends when nobody holds the service any more.
    async fn follow(self: Arc<Self>, mut from: i64) {
        let service = Arc::downgrade(&self);
        drop(self);
        loop {
            let Some(service) = service.upgrade() else {
                return;
            };
            let answers = match service.shared.fetch(TOPIC, &[(0, from)]).await {
                Ok(answers) => answers,
                Err(e) => {
                    tracing::warn!(error = %e, "the registry cannot read its topic; trying again");
                    drop(service);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            for answer in answers {
                if answer.error_code != 0 {
                    tracing::warn!(
                        code = answer.error_code,
                        "the registry's topic answered an error; trying again"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                // Everything before the topic's start is gone: read on from there.
                from = from.max(answer.log_start);
                for (mut batch, next) in batches(answer.batches) {
                    if next <= from {
                        continue;
                    }
                    match RecordBatchDecoder::decode_with_custom_compression(
                        &mut batch,
                        Some(records::decompress),
                    ) {
                        Ok(set) => {
                            service.apply(set.records.iter().filter(|r| r.offset >= from).map(|r| {
                                (r.offset, r.key.as_deref().unwrap_or_default(), r.value.as_deref())
                            }))
                        }
                        Err(e) => {
                            tracing::error!(error = %e, upto = next, "a batch of the registry's topic does not read; skipped")
                        }
                    }
                    from = next;
                }
                service.read.send_replace(from);
            }
        }
    }

    fn apply<'a>(&self, records: impl Iterator<Item = (i64, &'a [u8], Option<&'a [u8]>)>) {
        let Ok(mut state) = self.state.write() else {
            return;
        };
        for (offset, key, value) in records {
            match log::decode(key, value) {
                Ok(Some(change)) => {
                    if let Err(why) = state.apply(&change) {
                        tracing::warn!(offset, "{why}");
                    }
                }
                Ok(None) => {}
                Err(why) => tracing::warn!(offset, "a record of the registry's topic is skipped: {why}"),
            }
        }
    }

    fn plan(&self, request: &Request<'_>, access: &(dyn Access + Sync)) -> Outcome {
        match self.state.read() {
            Ok(state) => api::handle_as(&state, request, access),
            Err(_) => Outcome::Answer(failed(500, 50001, "The registry's state is not usable")),
        }
    }

    fn took(&self, changes: &[Change]) -> bool {
        self.state
            .read()
            .is_ok_and(|state| changes.iter().all(|c| state.holds(c)))
    }

    /// One request, answered (see the top of this file).
    pub async fn handle(&self, request: &Request<'_>) -> Response {
        self.handle_as(request, &Everything).await
    }

    /// One request from a caller who may do what `access` says.
    pub async fn handle_as(&self, request: &Request<'_>, access: &(dyn Access + Sync)) -> Response {
        if let Outcome::Answer(response) = self.plan(request, access) {
            return response;
        }
        let _one_at_a_time = self.writing.lock().await;
        for attempt in 0..ATTEMPTS {
            let (changes, then) = match self.plan(request, access) {
                Outcome::Answer(response) => return response,
                Outcome::Write { changes, then } => (changes, then),
            };
            let messages: Vec<Outgoing> = changes
                .iter()
                .map(|change| {
                    let record = log::encode(change);
                    Outgoing {
                        key: Some(Bytes::from(record.key)),
                        value: record.value.map(Bytes::from),
                        headers: Vec::new(),
                    }
                })
                .collect();
            let count = i64::try_from(messages.len()).unwrap_or(i64::MAX);
            let first = match self.shared.produce(TOPIC, 0, messages).await {
                Ok(offset) => offset,
                Err(e) => {
                    tracing::error!(error = %e, "the registry cannot write to its topic");
                    return failed(
                        500,
                        50001,
                        &format!("Error while writing to the registry's topic: {e}"),
                    );
                }
            };
            let mut read = self.read.subscribe();
            let through = first.saturating_add(count);
            if tokio::time::timeout(WRITE_TIMEOUT, read.wait_for(|at| *at >= through))
                .await
                .is_err()
            {
                return failed(500, 50002, "The change was written, and not read back in time");
            }
            if self.took(&changes) {
                return then;
            }
            tracing::debug!(attempt, "a change lost to another broker's; planning it again");
            // A pause of a different length each time, so two brokers do not keep meeting.
            let jitter = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos() % 5_000);
            tokio::time::sleep(Duration::from_micros(
                u64::from(jitter) + u64::from(attempt) * 500,
            ))
            .await;
        }
        failed(
            500,
            50002,
            "The registry is changing too fast for this request: try again",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(base: i64, last_delta: i32, body: usize) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&base.to_be_bytes());
        b.extend_from_slice(&i32::try_from(49 + body).unwrap().to_be_bytes());
        b.extend_from_slice(&[0u8; 11]);
        b.extend_from_slice(&last_delta.to_be_bytes());
        b.extend_from_slice(&vec![0u8; 34 + body]);
        b
    }

    #[test]
    fn a_fetch_answer_is_split_into_its_batches_with_where_each_ends() {
        let mut raw = batch(10, 2, 30);
        raw.extend_from_slice(&batch(13, 0, 0));
        raw.extend_from_slice(&batch(14, 6, 5));
        let found = batches(Bytes::from(raw.clone()));
        let ends: Vec<i64> = found.iter().map(|(_, next)| *next).collect();
        assert_eq!(
            ends,
            vec![13, 14, 21],
            "an empty batch still says how far it reaches"
        );
        assert_eq!(found[0].0.len(), 91);
        // Cut short, or claiming more than there is: what is whole is given, the rest left.
        assert_eq!(batches(Bytes::from(raw[..100].to_vec())).len(), 1);
        let mut lying = batch(0, 0, 0);
        lying[8..12].copy_from_slice(&i32::MAX.to_be_bytes());
        assert!(batches(Bytes::from(lying)).is_empty());
        assert!(batches(Bytes::from_static(b"short")).is_empty());
    }
}
