// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One node's connections to its cluster, shared by everything on the node that reads and writes: however many
//! readers there are, the node keeps a few connections.
//!
//! - A call borrows a client for as long as it takes ([`Shared::lend`]).
//! - Joining a group can wait seconds for it to settle, so a join borrows a client of its own kind
//!   ([`Shared::lend_for_join`]): joins never hold up calls.
//! - Waiting for new records goes into one request on behalf of every reader waiting ([`Shared::fetch`]): a thousand
//!   idle readers cost one waiting request, not a thousand. A reader with nothing new stays in the wait; one with
//!   records is answered at once.
//! - Writes that arrive together, from any writer, go in one request to each leader ([`Shared::produce`]): a thousand
//!   workflows each keeping a run cost a few requests, not a thousand.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;

use super::{ClientError, ClientSecurity, Fetched, Outgoing, Routed};

/// Clients for calls: as many calls at once as this.
const CALLS: usize = 16;
/// Clients for joins: as many groups joining at once as this (a new group waits a few seconds to settle).
const JOINS: usize = 64;
/// Clients kept open for joins while none is joining.
const JOINS_KEPT: usize = 4;
/// Requests waiting for records at once. A reader that starts waiting while all are out joins the next one.
const WAITS: usize = 4;
/// How long a request waits for records before it is sent again.
const WAIT_MS: i32 = 1_000;
/// The most one partition gives a reader at a time (one batch always, however large).
const PARTITION_BYTES: i32 = 1024 * 1024;
/// The most one answer holds, from all its partitions.
const ANSWER_BYTES: i32 = 16 * 1024 * 1024;
/// How many times in a row a request may fail before its readers are told.
const FAILURES: u32 = 5;
/// Write requests out at once. Writes that arrive while all are out go together in the next.
const WRITES: usize = 8;
/// The most one write request holds (one write always, however large).
const WRITE_BYTES: usize = 8 * 1024 * 1024;

/// Clients lent out, each back when its borrower is done.
struct Pool {
    seed: String,
    timeout: Duration,
    security: ClientSecurity,
    idle: Mutex<Vec<Routed>>,
    slots: Arc<Semaphore>,
    kept: usize,
}

impl Pool {
    fn new(seed: &str, timeout: Duration, security: &ClientSecurity, size: usize, kept: usize) -> Arc<Self> {
        Arc::new(Self {
            seed: seed.to_owned(),
            timeout,
            security: security.clone(),
            idle: Mutex::new(Vec::new()),
            slots: Arc::new(Semaphore::new(size)),
            kept,
        })
    }

    fn fresh(&self) -> Routed {
        Routed::unconnected(&self.seed, self.timeout, &self.security)
    }

    async fn lend(self: &Arc<Self>) -> Result<Lease, ClientError> {
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|_| closed())?;
        let client = self
            .idle
            .lock()
            .ok()
            .and_then(|mut idle| idle.pop())
            .unwrap_or_else(|| self.fresh());
        Ok(Lease {
            pool: Arc::clone(self),
            client,
            _slot: slot,
        })
    }
}

/// A client borrowed from a node's connections; back to them when dropped.
pub struct Lease {
    pool: Arc<Pool>,
    client: Routed,
    _slot: OwnedSemaphorePermit,
}

impl Deref for Lease {
    type Target = Routed;
    fn deref(&self) -> &Routed {
        &self.client
    }
}

impl DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut Routed {
        &mut self.client
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let client = std::mem::replace(&mut self.client, self.pool.fresh());
        if let Ok(mut idle) = self.pool.idle.lock()
            && idle.len() < self.pool.kept
        {
            idle.push(client);
        }
    }
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

fn closed() -> ClientError {
    ClientError::Protocol("the node's connections are closed".to_owned())
}

/// A reader waiting for records of `topic`'s partitions from these offsets.
struct Want {
    topic: String,
    from: Vec<(i32, i64)>,
    reply: oneshot::Sender<Result<Vec<Fetched>, ClientError>>,
    failures: u32,
}

/// Messages for one partition, and where to say the first one's offset.
struct Write {
    topic: String,
    partition: i32,
    messages: Vec<Outgoing>,
    reply: oneshot::Sender<Result<i64, ClientError>>,
}

/// One node's connections to its cluster, shared.
pub struct Shared {
    calls: Arc<Pool>,
    joins: Arc<Pool>,
    wants: mpsc::UnboundedSender<Want>,
    /// The waiting side, taken when the first reader waits (a runtime is running then).
    hub: Mutex<Option<(mpsc::UnboundedReceiver<Want>, Arc<Pool>)>>,
    writes: mpsc::UnboundedSender<Write>,
    /// The writing side, taken at the first write.
    writer: Mutex<Option<(mpsc::UnboundedReceiver<Write>, Arc<Pool>)>>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("seed", &self.calls.seed)
            .finish_non_exhaustive()
    }
}

impl Shared {
    /// The connections of a node to the cluster `seed` belongs to. None is made until it is needed.
    pub fn new(seed: &str, timeout: Duration, security: &ClientSecurity) -> Self {
        let (wants, waiting) = mpsc::unbounded_channel();
        let (writes, writing) = mpsc::unbounded_channel();
        Self {
            calls: Pool::new(seed, timeout, security, CALLS, CALLS),
            joins: Pool::new(seed, timeout, security, JOINS, JOINS_KEPT),
            wants,
            hub: Mutex::new(Some((waiting, Pool::new(seed, timeout, security, WAITS, WAITS)))),
            writes,
            writer: Mutex::new(Some((
                writing,
                Pool::new(seed, timeout, security, WRITES, WRITES),
            ))),
        }
    }

    /// A client for a call or a few: produce, commit, a heartbeat, metadata. Give it back (drop it) when done.
    pub async fn lend(&self) -> Result<Lease, ClientError> {
        self.calls.lend().await
    }

    /// A client for joining a group, which can wait seconds for the group to settle.
    pub async fn lend_for_join(&self) -> Result<Lease, ClientError> {
        self.joins.lend().await
    }

    /// Records of `topic`'s partitions from these offsets: answered when any of them has some, or with an error;
    /// waiting meanwhile in one request with every other reader of the node.
    pub async fn fetch(&self, topic: &str, from: &[(i32, i64)]) -> Result<Vec<Fetched>, ClientError> {
        if let Ok(mut held) = self.hub.lock()
            && let Some((waiting, pool)) = held.take()
        {
            tokio::spawn(hub(waiting, pool));
        }
        let (reply, answer) = oneshot::channel();
        self.wants
            .send(Want {
                topic: topic.to_owned(),
                from: from.to_vec(),
                reply,
                failures: 0,
            })
            .map_err(|_| closed())?;
        answer.await.map_err(|_| closed())?
    }
}

impl Shared {
    /// Writes `messages` to `partition` of `topic`, together with whatever else the node writes at that moment: the
    /// first one's offset, once every replica has them.
    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        messages: Vec<Outgoing>,
    ) -> Result<i64, ClientError> {
        if let Ok(mut held) = self.writer.lock()
            && let Some((writing, pool)) = held.take()
        {
            tokio::spawn(writer(writing, pool));
        }
        let (reply, answer) = oneshot::channel();
        self.writes
            .send(Write {
                topic: topic.to_owned(),
                partition,
                messages,
                reply,
            })
            .map_err(|_| closed())?;
        answer.await.map_err(|_| closed())?
    }
}

/// What a write's messages weigh.
fn weight(write: &Write) -> usize {
    write
        .messages
        .iter()
        .map(|m| {
            m.key.as_ref().map_or(0, Bytes::len)
                + m.value.as_ref().map_or(0, Bytes::len)
                + m.headers.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
        })
        .sum()
}

fn copy(e: &ClientError) -> ClientError {
    match e {
        ClientError::Refused { code, message } => ClientError::Refused {
            code: *code,
            message: message.clone(),
        },
        other => ClientError::Protocol(other.to_string()),
    }
}

/// Sends the writes waiting, in the order they came, up to [`WRITE_BYTES`] a request and [`WRITES`] requests out at
/// once: each partition's messages in one batch, each write told where its own begin.
async fn writer(mut writes: mpsc::UnboundedReceiver<Write>, pool: Arc<Pool>) {
    let mut pending: VecDeque<Write> = VecDeque::new();
    let mut out: JoinSet<()> = JoinSet::new();
    loop {
        while !pending.is_empty() && out.len() < WRITES {
            let mut taken: Vec<Write> = Vec::new();
            let mut bytes = 0;
            while let Some(next) = pending.front() {
                let size = weight(next);
                if !taken.is_empty() && bytes + size > WRITE_BYTES {
                    break;
                }
                bytes += size;
                if let Some(w) = pending.pop_front() {
                    taken.push(w);
                }
            }
            // Each partition once, its writes' messages one after another; each write's place: (entry, first).
            let mut entries: Vec<(String, i32, Vec<Outgoing>)> = Vec::new();
            let mut places: Vec<(usize, i64)> = Vec::new();
            for w in &taken {
                match entries
                    .iter()
                    .position(|(t, p, _)| *t == w.topic && *p == w.partition)
                {
                    Some(e) => {
                        if let Some((_, _, messages)) = entries.get_mut(e) {
                            places.push((e, i64::try_from(messages.len()).unwrap_or(0)));
                            messages.extend(w.messages.iter().cloned());
                        }
                    }
                    None => {
                        places.push((entries.len(), 0));
                        entries.push((w.topic.clone(), w.partition, w.messages.clone()));
                    }
                }
            }
            let lending = Arc::clone(&pool);
            out.spawn(async move {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0));
                let results = match lending.lend().await {
                    Ok(mut client) => client.produce_many(&entries, now).await,
                    Err(e) => entries.iter().map(|_| Err(copy(&e))).collect(),
                };
                for (w, (entry, first)) in taken.into_iter().zip(places) {
                    let answer = match results.get(entry) {
                        Some(Ok(base)) => Ok(base + first),
                        Some(Err(e)) => Err(copy(e)),
                        None => Err(ClientError::Protocol("no answer for the partition".to_owned())),
                    };
                    let _ = w.reply.send(answer);
                }
            });
        }
        tokio::select! {
            write = writes.recv() => match write {
                Some(w) => {
                    pending.push_back(w);
                    while let Ok(w) = writes.try_recv() {
                        pending.push_back(w);
                    }
                }
                None => return,
            },
            Some(_) = out.join_next(), if !out.is_empty() => {}
        }
    }
}

type Asked = (Vec<Want>, Result<Vec<(String, Fetched)>, ClientError>);

/// Sends every waiting reader's partitions in one request (up to [`WAITS`] out at once) and answers each reader whose
/// partitions came back with records it has not read; the others wait on in the next request.
///
/// Readers of the same partition - workflows reading one topic, say - share its place in the request: it is read from
/// the earliest offset any of them wants, and each takes the records from its own offset on.
async fn hub(mut wants: mpsc::UnboundedReceiver<Want>, pool: Arc<Pool>) {
    let mut pending: Vec<Want> = Vec::new();
    let mut out: JoinSet<Asked> = JoinSet::new();
    let mut turn = 0usize;
    loop {
        pending.retain(|w| !w.reply.is_closed());
        if !pending.is_empty() && out.len() < WAITS {
            let batch = std::mem::take(&mut pending);
            let mut earliest: BTreeMap<(String, i32), i64> = BTreeMap::new();
            for w in &batch {
                for &(p, o) in &w.from {
                    earliest
                        .entry((w.topic.clone(), p))
                        .and_modify(|e| *e = (*e).min(o))
                        .or_insert(o);
                }
            }
            let mut asked: Vec<(String, i32, i64)> =
                earliest.into_iter().map(|((t, p), o)| (t, p, o)).collect();
            // Started at a different partition each time: when an answer is full, the same ones are not always left
            // out.
            turn = turn.wrapping_add(1);
            let n = asked.len().max(1);
            asked.rotate_left(turn % n);
            let lending = Arc::clone(&pool);
            out.spawn(async move {
                let got = match lending.lend().await {
                    Ok(mut client) => {
                        client
                            .fetch_each(&asked, WAIT_MS, PARTITION_BYTES, ANSWER_BYTES)
                            .await
                    }
                    Err(e) => Err(e),
                };
                if got.is_err() {
                    // A moment before it is tried again.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                (batch, got)
            });
        }
        tokio::select! {
            want = wants.recv() => match want {
                Some(w) => {
                    pending.push(w);
                    while let Ok(w) = wants.try_recv() {
                        pending.push(w);
                    }
                }
                // The node's connections dropped: it is stopping.
                None => return,
            },
            Some(done) = out.join_next(), if !out.is_empty() => {
                // A request that panicked drops its readers' replies: each is told the connections closed.
                if let Ok((batch, got)) = done {
                    settle(batch, got, &mut pending);
                }
            }
        }
    }
}

/// The offset after the last record in `batches` (record batches as stored), read from their headers alone.
fn end_of(batches: &[u8]) -> Option<i64> {
    let mut rest = batches;
    let mut end = None;
    while let (Some(base), Some(length)) = (rest.get(0..8), rest.get(8..12)) {
        let base = i64::from_be_bytes(base.try_into().ok()?);
        let length = usize::try_from(i32::from_be_bytes(length.try_into().ok()?)).ok()?;
        if let Some(delta) = rest.get(23..27) {
            let delta = i32::from_be_bytes(delta.try_into().ok()?);
            end = Some(base + i64::from(delta) + 1);
        }
        rest = rest.get(12 + length..).unwrap_or_default();
    }
    end
}

/// Answers the readers of a request that came back: those with records past their own offsets (or an error of a
/// partition's); the rest wait on. A request that failed is tried again, its readers told after [`FAILURES`] in a row.
fn settle(batch: Vec<Want>, got: Result<Vec<(String, Fetched)>, ClientError>, pending: &mut Vec<Want>) {
    match got {
        Ok(answers) => {
            let by: HashMap<(String, i32), Fetched> =
                answers.into_iter().map(|(t, f)| ((t, f.partition), f)).collect();
            for mut w in batch {
                let mine: Vec<(i64, &Fetched)> = w
                    .from
                    .iter()
                    .filter_map(|(p, o)| by.get(&(w.topic.clone(), *p)).map(|f| (*o, f)))
                    .collect();
                let news = mine
                    .iter()
                    .any(|(o, f)| f.error_code != 0 || end_of(&f.batches).is_some_and(|end| end > *o));
                if news {
                    let mine = mine.into_iter().map(|(_, f)| f.clone()).collect();
                    let _ = w.reply.send(Ok(mine));
                } else {
                    w.failures = 0;
                    pending.push(w);
                }
            }
        }
        Err(e) => {
            for mut w in batch {
                w.failures += 1;
                if w.failures >= FAILURES {
                    let _ = w.reply.send(Err(ClientError::Protocol(e.to_string())));
                } else {
                    pending.push(w);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::end_of;

    /// A record batch's header: its first offset, and how many records past the first it holds.
    fn batch(base: i64, delta: i32, body: usize) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend(base.to_be_bytes());
        b.extend(i32::try_from(15 + body).unwrap_or(0).to_be_bytes());
        b.extend([0u8; 11]);
        b.extend(delta.to_be_bytes());
        b.extend(vec![0u8; body]);
        b
    }

    #[test]
    fn the_end_of_batches_comes_from_their_headers() {
        assert_eq!(end_of(&[]), None);
        let mut two = batch(10, 4, 20);
        two.extend(batch(15, 2, 8));
        assert_eq!(end_of(&two), Some(18));
        // A batch cut short at the end of an answer counts as far as its header goes.
        let mut cut = batch(0, 9, 30);
        cut.truncate(27);
        assert_eq!(end_of(&cut), Some(10));
    }
}
