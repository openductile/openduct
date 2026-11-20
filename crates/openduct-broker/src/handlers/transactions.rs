// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The transaction coordinator: what lets a producer write to several partitions (and commit a consumer group's
//! offsets) so that all of it becomes visible together, or none of it does.
//!
//! A producer names itself with a **transactional id**. The coordinator gives it a producer id and an epoch
//! (InitProducerId); each time the id is initialised again the epoch rises, which fences whoever held it before -
//! a producer that died and was restarted takes over cleanly, and anything its former self left open is aborted.
//!
//! A transaction then goes:
//!
//! 1. **AddPartitionsToTxn** - the producer says which partitions it is about to write to; **AddOffsetsToTxn** and
//!    **TxnOffsetCommit** - which group's offsets are part of it, and what they are (kept here, not yet the
//!    group's).
//! 2. The producer writes its records, marked transactional, to those partitions.
//! 3. **EndTxn** - commit or abort. The coordinator first records its decision, then writes a marker to every
//!    partition of the transaction (which is what makes the records committed or aborted to a `read_committed`
//!    consumer), then - on commit - makes the offsets the group's, then records that it is done. If the broker
//!    stops between the decision and the end, it finishes when it starts again: a decision recorded is carried
//!    out.
//!
//! What it knows of each transactional id is kept in the internal topic `__transaction_state` (compacted): one
//! record a change, the id its key. A transaction left open longer than its timeout is aborted, and its producer
//! fenced.
//!
//! **In a cluster** the coordinator is the broker that leads the topic's partition: FindCoordinator names it, and
//! any other broker answers `NOT_COORDINATOR`. A broker that comes to lead the partition reads the topic and
//! carries on - finishing what was decided - and one that finds the topic changed under it (another broker led
//! it meanwhile) reads it again before it answers. Markers reach partitions other brokers lead, and a group's
//! offsets its own coordinator, through the store, which sends them to each leader. TxnOffsetCommit is the one
//! request a client sends elsewhere - to the group's coordinator - so the offsets it carries are written to the
//! coordinator's topic from wherever they arrive, and the coordinator finds them there when the transaction ends.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::describe_producers_response::{
    PartitionResponse, ProducerState, TopicResponse,
};
use kafka_protocol::messages::describe_transactions_response::{
    TopicData as DescribedTopic, TransactionState as DescribedTransaction,
};
use kafka_protocol::messages::list_transactions_response::TransactionState;
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    EndTxnRequest, EndTxnResponse, InitProducerIdRequest, InitProducerIdResponse, ProducerId,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::messages::{
    DescribeProducersRequest, DescribeProducersResponse, DescribeTransactionsRequest,
    DescribeTransactionsResponse, ListTransactionsRequest, ListTransactionsResponse, TopicName,
    TransactionalId,
};
use kafka_protocol::messages::{WriteTxnMarkersRequest, WriteTxnMarkersResponse};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use openduct_storage::batch::Marker;
use openduct_storage::{CommittedOffset, StoreError, TRANSACTIONS_TOPIC, TopicConfig, TopicSettings};
use serde::{Deserialize, Serialize};

use super::{State, blocking, now_ms};
use crate::codes;

const INVALID_PRODUCER_EPOCH: i16 = 47;
const INVALID_TXN_STATE: i16 = 48;
const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
const CONCURRENT_TRANSACTIONS: i16 = 51;
const PRODUCER_FENCED: i16 = 90;
const NOT_COORDINATOR: i16 = 16;
const INVALID_REGULAR_EXPRESSION: i16 = 128;

/// The longest a transaction may be asked to stay open (Kafka's `transaction.max.timeout.ms` default).
const MAX_TIMEOUT_MS: i32 = 900_000;

/// Where a transaction stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Stage {
    /// None open.
    Empty,
    /// Open: partitions or offsets have been added.
    Ongoing,
    /// Decided: to be committed. The markers may not all be written yet.
    Committing,
    /// Decided: to be aborted.
    Aborting,
}

/// An offset a transaction will commit for a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Pending {
    topic: String,
    partition: i32,
    offset: i64,
    metadata: Option<String>,
}

/// What the coordinator knows of one transactional id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Transaction {
    producer_id: i64,
    epoch: i16,
    timeout_ms: i32,
    stage: Stage,
    partitions: BTreeSet<(String, i32)>,
    /// By group.
    offsets: BTreeMap<String, Vec<Pending>>,
    /// When it last changed: an open one older than its timeout is aborted.
    updated_ms: i64,
    /// When the transaction open now began; 0 when none is, or in a record written before this was kept.
    #[serde(default)]
    started_ms: i64,
    /// Decided under the second transaction protocol: who the producer is once this transaction is ended.
    #[serde(default)]
    next: Option<Next>,
    /// The last transaction ended under the second protocol: by whom, and how - so the producer's retry of that
    /// request (it has not heard its new epoch yet) is answered again.
    #[serde(default)]
    last_end: Option<(i64, i16, bool)>,
}

/// The producer id and epoch a transactional id's producer goes on with after a transaction (the second
/// transaction protocol gives it the next epoch at every end, so what is late from the one before is refused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Next {
    producer_id: i64,
    epoch: i16,
    /// The epoch its markers carry: above the transaction's own, so each partition fences what is late from it.
    marker_epoch: i16,
}

/// Offsets a transaction will commit for a group, as the request that carried them was written to the topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Offsets {
    /// The transactional id they are for.
    pending_for: String,
    producer_id: i64,
    epoch: i16,
    group: String,
    offsets: Vec<Pending>,
    /// Partitions the transaction wrote to, as their leaders told it (the second transaction protocol: a
    /// partition is part of a transaction by its first record, with no request of the producer's).
    #[serde(default)]
    added: Vec<(String, i32)>,
}

/// What the topic holds, read: every transactional id, and the offsets waiting for their transactions to end -
/// each with its record's key, in the order they were written.
#[derive(Debug, Default)]
struct Loaded {
    known: HashMap<String, Transaction>,
    pending: Vec<(String, Offsets)>,
}

/// The coordinator's state on this broker.
#[derive(Debug)]
pub(crate) struct Transactions {
    /// One for each partition of the coordinator's topic there may be.
    slots: Vec<Slot>,
    /// Each transaction open on a partition this broker leads, and when this broker first saw it open.
    open_since: parking_lot::Mutex<HashMap<(String, i32, i64, i64), std::time::Instant>>,
}

/// What this broker knows of one partition of the coordinator's topic, while it leads it.
#[derive(Debug)]
struct Slot {
    loaded: tokio::sync::Mutex<Option<Loaded>>,
    /// How far the partition had been read, or written by this broker, when that state was right: if it has
    /// grown otherwise, it is read again.
    read_upto: AtomicI64,
}

/// The partitions a cluster's coordinator topic is made with: its transactional ids are shared out over them, and
/// so over the brokers that lead them. (One broker alone coordinates every id: one partition.)
const COORDINATOR_PARTITIONS: i32 = 16;
/// The most partitions of the topic the coordinator works with, however it was made.
const MAX_COORDINATOR_PARTITIONS: i32 = 64;

impl Default for Transactions {
    fn default() -> Self {
        Self {
            slots: (0..MAX_COORDINATOR_PARTITIONS)
                .map(|_| Slot {
                    loaded: tokio::sync::Mutex::new(None),
                    read_upto: AtomicI64::new(-1),
                })
                .collect(),
            open_since: parking_lot::Mutex::new(HashMap::new()),
        }
    }
}

/// How many partitions of the coordinator's topic there are to work with (1 before it is made).
fn partitions(state: &State) -> i32 {
    state
        .store
        .topic(TRANSACTIONS_TOPIC)
        .map_or(1, |t| t.partitions.clamp(1, MAX_COORDINATOR_PARTITIONS))
}

/// The partition of the coordinator's topic that keeps a transactional id: the same on every broker and in
/// every release (the hash Kafka's coordinators use for a string).
pub(crate) fn partition_of(state: &State, id: &str) -> i32 {
    let hash = id
        .encode_utf16()
        .fold(0_i32, |h, unit| h.wrapping_mul(31).wrapping_add(i32::from(unit)));
    (hash & i32::MAX) % partitions(state)
}

/// The key of a record of offsets: unlike any transactional id a client would choose, and its own each time.
fn offsets_key(id: &str, group: &str) -> String {
    format!("\u{1}offsets\u{1}{id}\u{1}{group}\u{1}{}", uuid::Uuid::new_v4())
}

impl Transaction {
    /// When the open transaction began (for one recorded before that was kept: when it last changed).
    fn began_ms(&self) -> i64 {
        if self.started_ms > 0 {
            self.started_ms
        } else {
            self.updated_ms
        }
    }

    /// The stage by Kafka's name for it.
    fn stage_name(&self) -> &'static str {
        match self.stage {
            Stage::Empty => "Empty",
            Stage::Ongoing => "Ongoing",
            Stage::Committing => "PrepareCommit",
            Stage::Aborting => "PrepareAbort",
        }
    }
}

/// Every stage Kafka names, for a filter: those OpenDuct never shows match nothing.
const STAGE_NAMES: [&str; 8] = [
    "Empty",
    "Ongoing",
    "PrepareCommit",
    "PrepareAbort",
    "CompleteCommit",
    "CompleteAbort",
    "Dead",
    "PrepareEpochFence",
];

fn record(key: &str, value: Option<Vec<u8>>) -> Result<Bytes, StoreError> {
    let records = [Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: now_ms(),
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value: value.map(Bytes::from),
        headers: IndexMap::new(),
    }];
    let mut batch = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut batch,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|e| StoreError::Io(std::io::Error::other(e.to_string())))?;
    Ok(batch.freeze())
}

/// Why this broker cannot answer as the coordinator, if it cannot: in a cluster the coordinator is the broker
/// leading the topic's partition.
fn unavailable(state: &State, partition: i32) -> Option<i16> {
    let view = state.store.cluster()?;
    // Not until every broker keeps transactions: the client asks again.
    if !state.store.transactions_ready() {
        return Some(codes::COORDINATOR_NOT_AVAILABLE);
    }
    match state
        .store
        .placement(TRANSACTIONS_TOPIC, partition)
        .and_then(|p| p.leader)
    {
        Some(leader) if leader == view.me => None,
        Some(_) => Some(NOT_COORDINATOR),
        None => Some(codes::COORDINATOR_NOT_AVAILABLE),
    }
}

/// Makes the coordinator's topic if there is none: compacted, since only the newest record of each key matters.
pub(crate) async fn ensure_topic(state: &State) -> Result<(), StoreError> {
    if state.store.topic(TRANSACTIONS_TOPIC).is_some() {
        return Ok(());
    }
    if !state.store.transactions_ready() {
        return Err(StoreError::Unavailable(
            "the cluster does not keep transactions yet".to_owned(),
        ));
    }
    let partitions = if state.store.cluster().is_some() {
        COORDINATOR_PARTITIONS
    } else {
        1
    };
    blocking(&state.store, move |s| {
        let made = s.create_topic(
            TRANSACTIONS_TOPIC,
            TopicConfig {
                partitions,
                retention_ms: None,
            },
        );
        if !matches!(made, Ok(()) | Err(StoreError::TopicExists(_))) {
            made?;
        }
        // A store that does not compact (yet: a cluster being upgraded) keeps every record.
        let mut settings = TopicSettings::default();
        if settings.set(openduct_storage::CLEANUP_POLICY, "compact").is_ok() {
            let _ = s.set_topic_settings(TRANSACTIONS_TOPIC, settings);
        }
        Ok(())
    })
    .await
}

impl Transactions {
    /// What a partition of the topic holds - read when this broker has not read it, or when it has grown by
    /// another broker's writing since. Whatever was decided and not finished is finished here.
    async fn loaded<'a>(
        &'a self,
        state: &State,
        partition: i32,
    ) -> Result<tokio::sync::MappedMutexGuard<'a, Loaded>, StoreError> {
        let slot = usize::try_from(partition)
            .ok()
            .and_then(|p| self.slots.get(p))
            .ok_or_else(|| {
                StoreError::Unavailable("no such partition of the coordinator's topic".to_owned())
            })?;
        let mut held = slot.loaded.lock().await;
        ensure_topic(state).await?;
        let end = blocking(&state.store, move |s| s.offsets(TRANSACTIONS_TOPIC, partition))
            .await?
            .high_watermark;
        if held.is_none() || slot.read_upto.load(Ordering::Acquire) != end {
            // All of it the first time; after that, what was written since (by another broker: a group's offsets,
            // a partition a transaction wrote to). Reading a record again changes nothing.
            let known_upto = held.is_some().then(|| slot.read_upto.load(Ordering::Acquire));
            let before = held.take().unwrap_or_default();
            let (mut loaded, upto) = blocking(&state.store, move |s| {
                let mut loaded = before;
                let start = s.offsets(TRANSACTIONS_TOPIC, partition)?.log_start;
                let mut from = known_upto.map_or(start, |upto| upto.max(start));
                loop {
                    let read = s.read(TRANSACTIONS_TOPIC, partition, from, 4 << 20)?;
                    if read.batches.is_empty() {
                        break;
                    }
                    for batch in read.batches {
                        let Ok(header) = openduct_storage::batch::parse_stored_header(&batch) else {
                            continue;
                        };
                        from = header.base_offset + i64::from(header.last_offset_delta) + 1;
                        let mut bytes = batch;
                        let Ok(set) = RecordBatchDecoder::decode(&mut bytes) else {
                            continue;
                        };
                        for r in set.records {
                            let Some(key) = r.key.and_then(|k| String::from_utf8(k.to_vec()).ok()) else {
                                continue;
                            };
                            // The newest record of a key stands; one with no value takes the key away.
                            loaded.pending.retain(|(held, _)| *held != key);
                            let Some(value) = r.value else {
                                loaded.known.remove(&key);
                                continue;
                            };
                            if let Ok(offsets) = serde_json::from_slice::<Offsets>(&value) {
                                loaded.pending.push((key, offsets));
                            } else if let Ok(t) = serde_json::from_slice::<Transaction>(&value) {
                                loaded.known.insert(key, t);
                            }
                        }
                    }
                }
                Ok((loaded, from))
            })
            .await?;
            slot.read_upto.store(upto, Ordering::Release);
            let unfinished: Vec<String> = loaded
                .known
                .iter()
                .filter(|(_, t)| matches!(t.stage, Stage::Committing | Stage::Aborting))
                .map(|(id, _)| id.clone())
                .collect();
            for id in unfinished {
                if let Some(mut t) = loaded.known.get(&id).cloned() {
                    tracing::info!(transactional_id = %id, "finishing a transaction decided before this broker coordinated it");
                    finish(state, &id, &mut t, &mut loaded.pending).await?;
                    loaded.known.insert(id, t);
                }
            }
            *held = Some(loaded);
        }
        let mut loaded =
            tokio::sync::MutexGuard::map(held, |loaded| loaded.get_or_insert_with(Loaded::default));
        begun_by_what_waits(&mut loaded);
        Ok(loaded)
    }
}

/// A transaction is open once anything waits for its end - a partition its leader says it wrote to, offsets a
/// group's coordinator took for it - whether or not its producer ever asked the coordinator to add them (under
/// the second transaction protocol it does not).
fn begun_by_what_waits(loaded: &mut Loaded) {
    let Loaded { known, pending } = loaded;
    for (_, waiting) in pending.iter() {
        let Some(t) = known.get_mut(&waiting.pending_for) else {
            continue;
        };
        if t.producer_id != waiting.producer_id
            || t.epoch != waiting.epoch
            || !matches!(t.stage, Stage::Empty | Stage::Ongoing)
        {
            continue;
        }
        if t.stage == Stage::Empty {
            t.stage = Stage::Ongoing;
            t.started_ms = now_ms();
        }
        t.partitions.extend(waiting.added.iter().cloned());
    }
}

/// A partition's leader tells the coordinator that a transaction wrote to the partition: a record in the
/// coordinator's topic, through the topic's leader, kept before the transaction's first record there is. The
/// error to answer the producer if it could not be.
pub(crate) async fn added_by_its_first_record(
    state: &State,
    id: &str,
    producer_id: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
) -> Result<(), i16> {
    ensure_topic(state).await.map_err(|e| store_code(&e))?;
    let told = Offsets {
        pending_for: id.to_owned(),
        producer_id,
        epoch,
        group: String::new(),
        offsets: Vec::new(),
        added: vec![(topic.to_owned(), partition)],
    };
    let value = serde_json::to_vec(&told).map_err(|_| codes::INVALID_REQUEST)?;
    let batch = record(&offsets_key(id, "+"), Some(value)).map_err(|e| codes::of_store_error(&e))?;
    let slot = partition_of(state, id);
    blocking(&state.store, move |s| {
        s.append_through_leader(TRANSACTIONS_TOPIC, slot, batch)
    })
    .await
    .map(|_| ())
    .map_err(|e| store_code(&e))
}

/// Writes a record of the topic as its leader: where it landed. This broker's view of the topic stays right if
/// nothing else was written meanwhile.
async fn write(state: &State, partition: i32, key: &str, value: Option<Vec<u8>>) -> Result<(), StoreError> {
    let batch = record(key, value)?;
    let at = blocking(&state.store, move |s| {
        s.append(TRANSACTIONS_TOPIC, partition, batch)
    })
    .await?
    .base_offset;
    if let Some(slot) = usize::try_from(partition)
        .ok()
        .and_then(|p| state.transactions.slots.get(p))
    {
        let _ = slot
            .read_upto
            .compare_exchange(at, at + 1, Ordering::AcqRel, Ordering::Acquire);
    }
    Ok(())
}

/// Keeps what is known of an id in the topic: answered once it is stored.
async fn keep(state: &State, id: &str, t: &Transaction) -> Result<(), StoreError> {
    let value = serde_json::to_vec(t).map_err(|e| StoreError::Io(std::io::Error::other(e.to_string())))?;
    write(state, partition_of(state, id), id, Some(value)).await
}

/// Carries out a decision already recorded: a marker on every partition, the offsets made the group's on a
/// commit, and then the transaction is over - recorded too. `pending`: the offsets waiting in the topic; those
/// for this id are used (the current producer's, on a commit) and taken away.
async fn finish(
    state: &State,
    id: &str,
    t: &mut Transaction,
    pending: &mut Vec<(String, Offsets)>,
) -> Result<(), StoreError> {
    let marker = match t.stage {
        Stage::Committing => Marker::Commit,
        Stage::Aborting => Marker::Abort,
        Stage::Empty | Stage::Ongoing => return Ok(()),
    };
    let (producer_id, epoch) = (t.producer_id, t.epoch);
    let marker_epoch = t.next.map_or(epoch, |n| n.marker_epoch);
    let partitions: Vec<(String, i32)> = t.partitions.iter().cloned().collect();
    // The offsets the transaction's own producer asked for, by group; a later request's replace an earlier one's.
    let mine: Vec<(String, Offsets)> = pending
        .iter()
        .filter(|(_, o)| o.pending_for == id)
        .cloned()
        .collect();
    let mut offsets: BTreeMap<String, Vec<Pending>> = BTreeMap::new();
    if marker == Marker::Commit {
        for (_, asked) in mine
            .iter()
            .filter(|(_, o)| o.producer_id == producer_id && o.epoch == epoch)
        {
            let held = offsets.entry(asked.group.clone()).or_default();
            for new in &asked.offsets {
                held.retain(|p| !(p.topic == new.topic && p.partition == new.partition));
                held.push(new.clone());
            }
        }
    }
    blocking(&state.store, move |s| {
        for (topic, partition) in &partitions {
            match s.end_transaction(topic, *partition, producer_id, marker_epoch, marker, 0) {
                // A topic deleted since has nothing left to mark.
                Ok(_) | Err(StoreError::UnknownTopic(_) | StoreError::UnknownPartition { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        let at = now_ms();
        for (group, pending) in &offsets {
            let committed: Vec<CommittedOffset> = pending
                .iter()
                .map(|p| CommittedOffset {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    offset: p.offset,
                    metadata: p.metadata.clone(),
                    commit_timestamp_ms: at,
                })
                .collect();
            s.commit_offsets(group, &committed)?;
        }
        Ok(())
    })
    .await?;
    // Consumers waiting for committed data look again.
    state.appended.send_modify(|n| *n = n.wrapping_add(1));
    t.stage = Stage::Empty;
    t.partitions.clear();
    t.offsets.clear();
    t.updated_ms = now_ms();
    if let Some(next) = t.next.take() {
        t.last_end = Some((producer_id, epoch, marker == Marker::Commit));
        (t.producer_id, t.epoch) = (next.producer_id, next.epoch);
    }
    keep(state, id, t).await?;
    // The offsets are used, or were not wanted: their records go.
    for (key, _) in &mine {
        write(state, partition_of(state, id), key, None).await?;
    }
    pending.retain(|(_, o)| o.pending_for != id);
    Ok(())
}

/// Whether the request is from the id's current producer; the error to answer if it is not.
fn current(t: Option<&Transaction>, producer_id: i64, epoch: i16) -> Result<(), i16> {
    let Some(t) = t else {
        return Err(INVALID_PRODUCER_ID_MAPPING);
    };
    if t.producer_id != producer_id {
        return Err(INVALID_PRODUCER_ID_MAPPING);
    }
    match epoch.cmp(&t.epoch) {
        std::cmp::Ordering::Less => Err(PRODUCER_FENCED),
        std::cmp::Ordering::Greater => Err(INVALID_PRODUCER_EPOCH),
        std::cmp::Ordering::Equal => Ok(()),
    }
}

fn store_code(e: &StoreError) -> i16 {
    match e {
        // The topic's leader moved: the client looks for the coordinator again.
        StoreError::NotLeader { .. } => NOT_COORDINATOR,
        // A partition is between leaders, or a broker just went: what was decided stands, and the client's next
        // try carries it through.
        StoreError::Unavailable(_) => codes::COORDINATOR_NOT_AVAILABLE,
        other => codes::of_store_error(other),
    }
}

/// InitProducerId with a transactional id: the id's producer id and its next epoch. Whatever its former holder
/// left open is aborted first, with the new epoch in the markers, so the partitions fence the former holder too.
pub(crate) async fn init(state: &State, req: &InitProducerIdRequest, id: &str) -> InitProducerIdResponse {
    let refused = |code: i16| {
        InitProducerIdResponse::default()
            .with_error_code(code)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1)
    };
    if let Some(code) = unavailable(state, partition_of(state, id)) {
        return refused(code);
    }
    // (A control character is kept for the coordinator's own keys.)
    if id.is_empty() || id.contains('\u{1}') {
        return refused(codes::INVALID_REQUEST);
    }
    if !(1..=MAX_TIMEOUT_MS).contains(&req.transaction_timeout_ms) {
        return refused(INVALID_TRANSACTION_TIMEOUT);
    }
    let mut known = match state.transactions.loaded(state, partition_of(state, id)).await {
        Ok(known) => known,
        Err(e) => return refused(store_code(&e)),
    };
    let mut t = match known.known.get(id).cloned() {
        Some(mut t) => {
            // A producer that names an id and epoch (a client recovering from an error) must be the current one.
            if req.producer_id.0 >= 0
                && let Err(code) = current(Some(&t), req.producer_id.0, req.producer_epoch)
            {
                return refused(code);
            }
            if matches!(t.stage, Stage::Committing | Stage::Aborting)
                && let Err(e) = finish(state, id, &mut t, &mut known.pending).await
            {
                return refused(store_code(&e));
            }
            let open = t.stage == Stage::Ongoing;
            // The next epoch - or, when epochs run out, a new producer id from the start.
            if t.epoch >= i16::MAX - 1 {
                match blocking(&state.store, |s| s.next_producer_id()).await {
                    Ok(producer_id) => {
                        // What the old id left open is aborted under the old id.
                        if open {
                            t.stage = Stage::Aborting;
                            if let Err(e) = finish(state, id, &mut t, &mut known.pending).await {
                                return refused(store_code(&e));
                            }
                        }
                        t.producer_id = producer_id;
                        t.epoch = 0;
                    }
                    Err(e) => return refused(store_code(&e)),
                }
            } else {
                t.epoch += 1;
                if open {
                    t.stage = Stage::Aborting;
                    if let Err(e) = keep(state, id, &t).await {
                        return refused(store_code(&e));
                    }
                    if let Err(e) = finish(state, id, &mut t, &mut known.pending).await {
                        return refused(store_code(&e));
                    }
                }
            }
            t
        }
        None => match blocking(&state.store, |s| s.next_producer_id()).await {
            Ok(producer_id) => Transaction {
                producer_id,
                epoch: 0,
                timeout_ms: req.transaction_timeout_ms,
                stage: Stage::Empty,
                partitions: BTreeSet::new(),
                offsets: BTreeMap::new(),
                updated_ms: now_ms(),
                started_ms: 0,
                next: None,
                last_end: None,
            },
            Err(e) => return refused(store_code(&e)),
        },
    };
    t.timeout_ms = req.transaction_timeout_ms;
    t.updated_ms = now_ms();
    if let Err(e) = keep(state, id, &t).await {
        return refused(store_code(&e));
    }
    let answer = InitProducerIdResponse::default()
        .with_producer_id(ProducerId(t.producer_id))
        .with_producer_epoch(t.epoch);
    known.known.insert(id.to_owned(), t);
    answer
}

/// Changes an id's open transaction, as its current producer: the error to answer, if any.
async fn change(
    state: &State,
    denied: Option<i16>,
    id: &str,
    producer_id: i64,
    epoch: i16,
    with: impl FnOnce(&mut Transaction) -> bool,
) -> i16 {
    if let Some(code) = denied.or_else(|| unavailable(state, partition_of(state, id))) {
        return code;
    }
    let mut known = match state.transactions.loaded(state, partition_of(state, id)).await {
        Ok(known) => known,
        Err(e) => return store_code(&e),
    };
    if let Err(code) = current(known.known.get(id), producer_id, epoch) {
        return code;
    }
    let Some(mut t) = known.known.get(id).cloned() else {
        return INVALID_PRODUCER_ID_MAPPING;
    };
    if matches!(t.stage, Stage::Committing | Stage::Aborting) {
        return CONCURRENT_TRANSACTIONS;
    }
    let begins = t.stage != Stage::Ongoing;
    let changed = with(&mut t) || begins;
    t.stage = Stage::Ongoing;
    if begins {
        t.started_ms = now_ms();
    }
    if changed {
        t.updated_ms = now_ms();
        if let Err(e) = keep(state, id, &t).await {
            return store_code(&e);
        }
        known.known.insert(id.to_owned(), t);
    }
    0
}

/// One transaction's part of AddPartitionsToTxn: these partitions are part of the producer's transaction - or,
/// when a partition's leader only asks (`verify_only`), whether they are.
async fn add_to_one(
    state: &State,
    id: &str,
    producer_id: i64,
    epoch: i16,
    topics: &[AddPartitionsToTxnTopic],
    verify_only: bool,
    denied: Option<i16>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    let wanted: Vec<(String, i32)> = topics
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| (t.name.0.to_string(), *p)))
        .collect();
    // A partition that is not there is the one refused; the rest are then not added either, as on Kafka.
    let store = Arc::clone(&state.store);
    let missing: BTreeSet<(String, i32)> = wanted
        .iter()
        .filter(|(topic, partition)| {
            store
                .topic(topic)
                .is_none_or(|t| *partition < 0 || *partition >= t.partitions)
        })
        .cloned()
        .collect();
    // Asked only: each partition answers for itself.
    let mut outside: BTreeSet<(String, i32)> = BTreeSet::new();
    let code = if !missing.is_empty() {
        0
    } else if verify_only {
        match verify(state, denied, id, producer_id, epoch).await {
            Ok(inside) => {
                outside = wanted.iter().filter(|p| !inside.contains(*p)).cloned().collect();
                0
            }
            Err(code) => code,
        }
    } else {
        let adding = wanted.clone();
        change(state, denied, id, producer_id, epoch, move |t| {
            // Every one is added, whether or not an earlier one was new.
            let before = t.partitions.len();
            t.partitions.extend(adding);
            t.partitions.len() > before
        })
        .await
    };
    topics
        .iter()
        .map(|t| {
            AddPartitionsToTxnTopicResult::default()
                .with_name(t.name.clone())
                .with_results_by_partition(
                    t.partitions
                        .iter()
                        .map(|p| {
                            let this = (t.name.0.to_string(), *p);
                            AddPartitionsToTxnPartitionResult::default()
                                .with_partition_index(*p)
                                .with_partition_error_code(if missing.is_empty() {
                                    if outside.contains(&this) {
                                        INVALID_TXN_STATE
                                    } else {
                                        code
                                    }
                                } else if missing.contains(&this) {
                                    codes::UNKNOWN_TOPIC_OR_PARTITION
                                } else {
                                    // Not attempted, because another of the request's partitions failed.
                                    78
                                })
                        })
                        .collect(),
                )
        })
        .collect()
}

/// The partitions of the transaction an id's current producer has open - none when it has none open.
async fn verify(
    state: &State,
    denied: Option<i16>,
    id: &str,
    producer_id: i64,
    epoch: i16,
) -> Result<BTreeSet<(String, i32)>, i16> {
    let partition = partition_of(state, id);
    if let Some(code) = denied.or_else(|| unavailable(state, partition)) {
        return Err(code);
    }
    let known = state
        .transactions
        .loaded(state, partition)
        .await
        .map_err(|e| store_code(&e))?;
    current(known.known.get(id), producer_id, epoch)?;
    Ok(known
        .known
        .get(id)
        .filter(|t| t.stage == Stage::Ongoing)
        .map(|t| t.partitions.clone())
        .unwrap_or_default())
}

/// AddPartitionsToTxn. Versions 0 to 3, from a producer: these partitions are part of its transaction. Versions
/// 4 and 5, as brokers use it: several transactions at once, each added to or only asked about.
/// `denied(id, verify_only)`: the error to answer when the access rules do not let the principal do that.
pub(crate) async fn add_partitions(
    state: &State,
    req: &AddPartitionsToTxnRequest,
    version: i16,
    denied: &(dyn Fn(&str, bool) -> Option<i16> + Sync),
) -> AddPartitionsToTxnResponse {
    if version <= 3 {
        let id = req.v3_and_below_transactional_id.0.as_str();
        let results = add_to_one(
            state,
            id,
            req.v3_and_below_producer_id.0,
            req.v3_and_below_producer_epoch,
            &req.v3_and_below_topics,
            false,
            denied(id, false),
        )
        .await;
        return AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(results);
    }
    let mut results = Vec::new();
    for one in &req.transactions {
        let id = one.transactional_id.0.as_str();
        let topics = add_to_one(
            state,
            id,
            one.producer_id.0,
            one.producer_epoch,
            &one.topics,
            one.verify_only,
            denied(id, one.verify_only),
        )
        .await;
        results.push(
            AddPartitionsToTxnResult::default()
                .with_transactional_id(one.transactional_id.clone())
                .with_topic_results(topics),
        );
    }
    AddPartitionsToTxnResponse::default().with_results_by_transaction(results)
}

/// AddOffsetsToTxn: this group's offsets will be part of the transaction.
pub(crate) async fn add_offsets(
    state: &State,
    req: &AddOffsetsToTxnRequest,
    denied: Option<i16>,
) -> AddOffsetsToTxnResponse {
    let group = req.group_id.0.to_string();
    let code = change(
        state,
        denied,
        req.transactional_id.0.as_str(),
        req.producer_id.0,
        req.producer_epoch,
        move |t| {
            let new = !t.offsets.contains_key(&group);
            t.offsets.entry(group).or_default();
            new
        },
    )
    .await;
    AddOffsetsToTxnResponse::default().with_error_code(code)
}

/// Writes the offsets a transaction will commit to the coordinator's topic: as the coordinator, if this broker
/// is it; otherwise (a client sends them to the group's coordinator) through the topic's leader, and the
/// coordinator finds them there when the transaction ends - where it uses only its current producer's.
async fn keep_offsets(state: &State, asked: Offsets) -> i16 {
    let key = offsets_key(&asked.pending_for, &asked.group);
    let Ok(value) = serde_json::to_vec(&asked) else {
        return codes::INVALID_REQUEST;
    };
    let partition = partition_of(state, &asked.pending_for);
    if unavailable(state, partition).is_none() {
        let mut known = match state.transactions.loaded(state, partition).await {
            Ok(known) => known,
            Err(e) => return store_code(&e),
        };
        if let Err(code) = current(
            known.known.get(&asked.pending_for),
            asked.producer_id,
            asked.epoch,
        ) {
            return code;
        }
        return match write(state, partition, &key, Some(value)).await {
            Ok(()) => {
                known.pending.push((key, asked));
                0
            }
            Err(e) => store_code(&e),
        };
    }
    if state.store.topic(TRANSACTIONS_TOPIC).is_none() {
        return INVALID_PRODUCER_ID_MAPPING;
    }
    let batch = match record(&key, Some(value)) {
        Ok(batch) => batch,
        Err(e) => return codes::of_store_error(&e),
    };
    match blocking(&state.store, move |s| {
        s.append_through_leader(TRANSACTIONS_TOPIC, partition, batch)
    })
    .await
    {
        Ok(_) => 0,
        Err(e) => codes::of_store_error(&e),
    }
}

/// TxnOffsetCommit: the offsets the transaction will commit for the group - the group's only if it commits.
pub(crate) async fn offset_commit(
    state: &State,
    req: &TxnOffsetCommitRequest,
    denied: Option<i16>,
) -> TxnOffsetCommitResponse {
    let group = req.group_id.0.to_string();
    let offsets: Vec<Pending> = req
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions.iter().map(|p| Pending {
                topic: t.name.0.to_string(),
                partition: p.partition_index,
                offset: p.committed_offset,
                metadata: p.committed_metadata.as_ref().map(ToString::to_string),
            })
        })
        .collect();
    let asked = Offsets {
        pending_for: req.transactional_id.0.to_string(),
        producer_id: req.producer_id.0,
        epoch: req.producer_epoch,
        group,
        offsets,
        added: Vec::new(),
    };
    let code = match denied {
        Some(code) => code,
        None => keep_offsets(state, asked).await,
    };
    TxnOffsetCommitResponse::default().with_topics(
        req.topics
            .iter()
            .map(|t| {
                TxnOffsetCommitResponseTopic::default()
                    .with_name(t.name.clone())
                    .with_partitions(
                        t.partitions
                            .iter()
                            .map(|p| {
                                TxnOffsetCommitResponsePartition::default()
                                    .with_partition_index(p.partition_index)
                                    .with_error_code(code)
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

/// EndTxn: the transaction is committed, or aborted. Answered once every marker is written.
pub(crate) async fn end(
    state: &State,
    req: &EndTxnRequest,
    version: i16,
    denied: Option<i16>,
) -> EndTxnResponse {
    // From version 5 (the second transaction protocol) the producer goes on with the next epoch after every
    // transaction, and is told it here.
    let second = version >= 5;
    let answer = |code: i16| EndTxnResponse::default().with_error_code(code);
    let ended = |t: &Transaction| {
        if second {
            EndTxnResponse::default()
                .with_producer_id(ProducerId(t.producer_id))
                .with_producer_epoch(t.epoch)
        } else {
            EndTxnResponse::default()
        }
    };
    let id = req.transactional_id.0.as_str();
    if let Some(code) = denied.or_else(|| unavailable(state, partition_of(state, id))) {
        return answer(code);
    }
    let mut known = match state.transactions.loaded(state, partition_of(state, id)).await {
        Ok(known) => known,
        Err(e) => return answer(store_code(&e)),
    };
    // The same request again, from a producer that did not hear the answer: answered again.
    if second
        && let Some(t) = known.known.get(id)
        && t.stage == Stage::Empty
        && t.last_end == Some((req.producer_id.0, req.producer_epoch, req.committed))
    {
        return ended(t);
    }
    if let Err(code) = current(known.known.get(id), req.producer_id.0, req.producer_epoch) {
        return answer(code);
    }
    let Some(mut t) = known.known.get(id).cloned() else {
        return answer(INVALID_PRODUCER_ID_MAPPING);
    };
    match (t.stage, req.committed) {
        // Nothing was added: nothing to end. An abort of nothing is harmless; a commit of nothing is a mistake.
        (Stage::Empty, false) => return ended(&t),
        (Stage::Empty, true) => return answer(INVALID_TXN_STATE),
        // A retry of the EndTxn that decided it is carried through; the opposite decision is refused.
        (Stage::Committing, false) | (Stage::Aborting, true) => return answer(INVALID_TXN_STATE),
        (Stage::Committing | Stage::Aborting, _) => {}
        (Stage::Ongoing, commit) => {
            if second {
                // The next epoch - or, when epochs run out, a new producer id from the start. The markers carry
                // an epoch above the transaction's own either way.
                let next = if t.epoch >= i16::MAX - 1 {
                    match blocking(&state.store, |s| s.next_producer_id()).await {
                        Ok(producer_id) => Next {
                            producer_id,
                            epoch: 0,
                            marker_epoch: t.epoch.saturating_add(1),
                        },
                        Err(e) => return answer(store_code(&e)),
                    }
                } else {
                    Next {
                        producer_id: t.producer_id,
                        epoch: t.epoch + 1,
                        marker_epoch: t.epoch + 1,
                    }
                };
                t.next = Some(next);
            }
            t.stage = if commit {
                Stage::Committing
            } else {
                Stage::Aborting
            };
            t.updated_ms = now_ms();
            // The decision first: once it is stored, the transaction ends this way whatever happens next.
            if let Err(e) = keep(state, id, &t).await {
                return answer(store_code(&e));
            }
        }
    }
    let finished = finish(state, id, &mut t, &mut known.pending).await;
    let response = ended(&t);
    known.known.insert(id.to_owned(), t);
    match finished {
        Ok(()) => response,
        Err(e) => answer(store_code(&e)),
    }
}

/// Aborts every transaction left open past its timeout, and fences its producer: called from time to time.
pub(crate) async fn expire(state: &State) {
    // Nothing to look at until a transactional producer has come - now, or before the broker last stopped: what
    // one left open then must still be timed out, whether or not it ever comes back.
    if state.store.topic(TRANSACTIONS_TOPIC).is_none() {
        return;
    }
    for partition in 0..partitions(state) {
        if unavailable(state, partition).is_some() {
            continue;
        }
        let Ok(mut known) = state.transactions.loaded(state, partition).await else {
            continue;
        };
        let now = now_ms();
        let late: Vec<String> = known
            .known
            .iter()
            .filter(|(_, t)| t.stage == Stage::Ongoing && now - t.began_ms() > i64::from(t.timeout_ms))
            .map(|(id, _)| id.clone())
            .collect();
        for id in late {
            let Some(mut t) = known.known.get(&id).cloned() else {
                continue;
            };
            tracing::warn!(transactional_id = %id, timeout_ms = t.timeout_ms, "a transaction was open past its timeout: aborted");
            // The next epoch, so the producer that left it open is told it was fenced when it comes back.
            t.epoch = t.epoch.saturating_add(1);
            t.stage = Stage::Aborting;
            if keep(state, &id, &t).await.is_ok()
                && finish(state, &id, &mut t, &mut known.pending).await.is_ok()
            {
                known.known.insert(id, t);
            }
        }
    }
}

/// Aborts every transaction that has been open on a partition this broker leads for longer than any transaction
/// may run. A coordinator ends its transactions well before (the longest timeout it accepts is shorter); what is
/// left is one no coordinator knows of - its producer wrote here and never told one - which would otherwise hold
/// back the partition's consumers of committed data for good.
pub(crate) async fn abort_hanging(state: &State) {
    let open = blocking(&state.store, |s| {
        let mut open = Vec::new();
        for topic in s.topics() {
            for partition in 0..topic.partitions {
                // Not led here, or gone meanwhile: not this broker's to look at.
                if let Ok(here) = s.open_transactions(&topic.name, partition) {
                    open.extend(here.into_iter().map(|(producer, epoch, first)| {
                        (topic.name.clone(), partition, producer, epoch, first)
                    }));
                }
            }
        }
        Ok(open)
    })
    .await
    .unwrap_or_default();
    let now = std::time::Instant::now();
    let late: Vec<(String, i32, i64, i16, i64)> = {
        let mut since = state.transactions.open_since.lock();
        since.retain(|(topic, partition, producer, first), _| {
            open.iter()
                .any(|o| o.0 == *topic && o.1 == *partition && o.2 == *producer && o.4 == *first)
        });
        open.into_iter()
            .filter(|(topic, partition, producer, _, first)| {
                let seen = *since
                    .entry((topic.clone(), *partition, *producer, *first))
                    .or_insert(now);
                now.duration_since(seen) > state.config.hanging_transaction_after
            })
            .collect()
    };
    for (topic, partition, producer, epoch, first) in late {
        tracing::warn!(
            %topic, partition, producer_id = producer, first_offset = first,
            "a transaction was open longer than any may run, and no coordinator ended it: aborted"
        );
        let name = topic.clone();
        let ended = blocking(&state.store, move |s| {
            s.end_transaction(&name, partition, producer, epoch, Marker::Abort, 0)
        })
        .await;
        if ended.is_ok() {
            state.appended.send_modify(|n| *n = n.wrapping_add(1));
        }
    }
}

/// ListTransactions (versions 0 and 1): the transactional ids this broker coordinates that the principal may
/// see - none, on a broker that coordinates none (a client asks every broker).
pub(crate) async fn list(
    state: &State,
    req: &ListTransactionsRequest,
    may: &(dyn Fn(&str) -> bool + Sync),
) -> ListTransactionsResponse {
    let mut res = ListTransactionsResponse::default();
    res.unknown_state_filters = req
        .state_filters
        .iter()
        .filter(|f| !STAGE_NAMES.contains(&f.as_str()))
        .cloned()
        .collect();
    // By a pattern the whole id must match (version 2). A pattern is a stranger's text: its compiled size is
    // bounded, and matching takes time in proportion to the id only.
    let pattern = match req.transactional_id_pattern.as_ref().filter(|p| !p.is_empty()) {
        Some(p) => match regex::RegexBuilder::new(&format!("^(?:{})$", p.as_str()))
            .size_limit(1 << 20)
            .build()
        {
            Ok(pattern) => Some(pattern),
            Err(_) => return res.with_error_code(INVALID_REGULAR_EXPRESSION),
        },
        None => None,
    };
    if state.store.topic(TRANSACTIONS_TOPIC).is_none() {
        return res;
    }
    let now = now_ms();
    let mut listed: Vec<TransactionState> = Vec::new();
    // Each partition of the coordinator's topic this broker leads.
    for partition in 0..partitions(state) {
        if unavailable(state, partition).is_some() {
            continue;
        }
        let known = match state.transactions.loaded(state, partition).await {
            Ok(known) => known,
            Err(e) => return res.with_error_code(store_code(&e)),
        };
        listed.extend(
            known
                .known
                .iter()
        .filter(|(id, t)| {
            may(id)
                && pattern.as_ref().is_none_or(|p| p.is_match(id))
                && (req.state_filters.is_empty() || req.state_filters.iter().any(|f| f.as_str() == t.stage_name()))
                && (req.producer_id_filters.is_empty()
                    || req.producer_id_filters.iter().any(|p| p.0 == t.producer_id))
                // Open for at least this long (version 1).
                && (req.duration_filter < 0
                    || (t.stage != Stage::Empty && now - t.began_ms() >= req.duration_filter))
        })
        .map(|(id, t)| {
            TransactionState::default()
                .with_transactional_id(TransactionalId(super::s(id.clone())))
                .with_producer_id(ProducerId(t.producer_id))
                .with_transaction_state(super::s(t.stage_name()))
        })
        );
    }
    listed.sort_by(|a, b| a.transactional_id.0.cmp(&b.transactional_id.0));
    res.with_transaction_states(listed)
}

/// DescribeTransactions: what the coordinator knows of each id.
pub(crate) async fn describe(
    state: &State,
    req: &DescribeTransactionsRequest,
    may: &(dyn Fn(&str) -> bool + Sync),
) -> DescribeTransactionsResponse {
    const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
    const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
    let refused = |id: &TransactionalId, code: i16| {
        DescribedTransaction::default()
            .with_transactional_id(id.clone())
            .with_error_code(code)
    };
    let all = |code: i16| {
        DescribeTransactionsResponse::default()
            .with_transaction_states(req.transactional_ids.iter().map(|id| refused(id, code)).collect())
    };
    if state.store.topic(TRANSACTIONS_TOPIC).is_none() {
        return all(TRANSACTIONAL_ID_NOT_FOUND);
    }
    let mut states = Vec::new();
    for id in &req.transactional_ids {
        {
            if !may(id.0.as_str()) {
                states.push(refused(id, TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
                continue;
            }
            // Each id is answered by the broker that coordinates it.
            let partition = partition_of(state, id.0.as_str());
            if let Some(code) = unavailable(state, partition) {
                states.push(refused(id, code));
                continue;
            }
            let known = match state.transactions.loaded(state, partition).await {
                Ok(known) => known,
                Err(e) => {
                    states.push(refused(id, store_code(&e)));
                    continue;
                }
            };
            let Some(t) = known.known.get(id.0.as_str()) else {
                states.push(refused(id, TRANSACTIONAL_ID_NOT_FOUND));
                continue;
            };
            let mut topics: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
            for (topic, partition) in &t.partitions {
                topics.entry(topic.as_str()).or_default().push(*partition);
            }
            let described = DescribedTransaction::default()
                .with_transactional_id(id.clone())
                .with_transaction_state(super::s(t.stage_name()))
                .with_transaction_timeout_ms(t.timeout_ms)
                .with_transaction_start_time_ms(if t.stage == Stage::Empty { -1 } else { t.began_ms() })
                .with_producer_id(ProducerId(t.producer_id))
                .with_producer_epoch(t.epoch)
                .with_topics(
                    topics
                        .into_iter()
                        .map(|(topic, partitions)| {
                            DescribedTopic::default()
                                .with_topic(TopicName(super::s(topic)))
                                .with_partitions(partitions)
                        })
                        .collect(),
                );
            states.push(described);
        }
    }
    DescribeTransactionsResponse::default().with_transaction_states(states)
}

/// DescribeProducers: the producers each partition knows, and where the open transaction of each starts - asked
/// of the partition's leader. `may`: whether the principal may read the topic.
pub(crate) async fn describe_producers(
    state: &State,
    req: &DescribeProducersRequest,
    may: &(dyn Fn(&str) -> bool + Sync),
) -> DescribeProducersResponse {
    const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    let mut topics = Vec::new();
    for topic in &req.topics {
        let name = topic.name.0.to_string();
        let mut partitions = Vec::new();
        for partition in &topic.partition_indexes {
            let mut part = PartitionResponse::default().with_partition_index(*partition);
            if may(&name) {
                let (asked, p) = (name.clone(), *partition);
                match blocking(&state.store, move |s| s.active_producers(&asked, p)).await {
                    Ok(producers) => {
                        part.active_producers = producers
                            .into_iter()
                            .map(|p| {
                                ProducerState::default()
                                    .with_producer_id(ProducerId(p.producer_id))
                                    .with_producer_epoch(i32::from(p.epoch))
                                    .with_last_sequence(p.last_sequence)
                                    .with_last_timestamp(p.last_timestamp)
                                    .with_coordinator_epoch(0)
                                    .with_current_txn_start_offset(p.open_from.unwrap_or(-1))
                            })
                            .collect();
                    }
                    Err(e) => {
                        part.error_code = codes::of_store_error(&e);
                        part.error_message = Some(super::s(e.to_string()));
                    }
                }
            } else {
                part.error_code = TOPIC_AUTHORIZATION_FAILED;
            }
            partitions.push(part);
        }
        topics.push(
            TopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }
    DescribeProducersResponse::default().with_topics(topics)
}

/// WriteTxnMarkers: a marker written to partitions directly, as an operator does to end a transaction no
/// coordinator knows any more (`kafka-transactions abort`). `allowed`: whether the principal may act for the
/// cluster. The store takes the marker to each partition's leader.
pub(crate) async fn write_markers(
    state: &State,
    req: &WriteTxnMarkersRequest,
    allowed: bool,
) -> WriteTxnMarkersResponse {
    const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
    let mut results = Vec::new();
    for marker in &req.markers {
        let (producer_id, epoch, coordinator_epoch) = (
            marker.producer_id.0,
            marker.producer_epoch,
            marker.coordinator_epoch,
        );
        let kind = if marker.transaction_result {
            Marker::Commit
        } else {
            Marker::Abort
        };
        let mut topics = Vec::new();
        for topic in &marker.topics {
            let mut partitions = Vec::new();
            for partition in &topic.partition_indexes {
                let code = if allowed {
                    let (name, p) = (topic.name.0.to_string(), *partition);
                    match blocking(&state.store, move |s| {
                        s.end_transaction(&name, p, producer_id, epoch, kind, coordinator_epoch)
                    })
                    .await
                    {
                        Ok(_) => 0,
                        Err(e) => codes::of_store_error(&e),
                    }
                } else {
                    CLUSTER_AUTHORIZATION_FAILED
                };
                partitions.push(
                    WritableTxnMarkerPartitionResult::default()
                        .with_partition_index(*partition)
                        .with_error_code(code),
                );
            }
            topics.push(
                WritableTxnMarkerTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            );
        }
        results.push(
            WritableTxnMarkerResult::default()
                .with_producer_id(marker.producer_id)
                .with_topics(topics),
        );
    }
    // Consumers waiting for committed data look again.
    state.appended.send_modify(|n| *n = n.wrapping_add(1));
    WriteTxnMarkersResponse::default().with_markers(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction(producer_id: i64, epoch: i16) -> Transaction {
        Transaction {
            producer_id,
            epoch,
            timeout_ms: 60_000,
            stage: Stage::Empty,
            partitions: BTreeSet::new(),
            offsets: BTreeMap::new(),
            updated_ms: 0,
            started_ms: 0,
            next: None,
            last_end: None,
        }
    }

    #[test]
    fn only_an_ids_current_producer_may_change_its_transaction() {
        let t = transaction(7, 3);
        assert_eq!(current(Some(&t), 7, 3), Ok(()));
        assert_eq!(current(Some(&t), 7, 2), Err(PRODUCER_FENCED), "a former holder");
        assert_eq!(current(Some(&t), 7, 4), Err(INVALID_PRODUCER_EPOCH));
        assert_eq!(current(Some(&t), 8, 3), Err(INVALID_PRODUCER_ID_MAPPING));
        assert_eq!(
            current(None, 7, 3),
            Err(INVALID_PRODUCER_ID_MAPPING),
            "an id never initialised"
        );
    }

    #[test]
    fn what_is_known_of_an_id_is_kept_as_a_record_that_reads_back() {
        let mut t = transaction(7, 3);
        t.stage = Stage::Committing;
        t.partitions.insert(("orders".to_owned(), 2));
        t.offsets.insert(
            "billing".to_owned(),
            vec![Pending {
                topic: "orders".to_owned(),
                partition: 2,
                offset: 41,
                metadata: None,
            }],
        );
        let batch = record("payments-1", Some(serde_json::to_vec(&t).unwrap())).unwrap();
        let mut bytes = batch;
        let set = RecordBatchDecoder::decode(&mut bytes).unwrap();
        let r = &set.records[0];
        assert_eq!(r.key.as_deref(), Some(&b"payments-1"[..]));
        assert_eq!(
            serde_json::from_slice::<Transaction>(r.value.as_ref().unwrap()).unwrap(),
            t
        );
    }
}
