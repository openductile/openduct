// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One function per Kafka API: decode what the client asked, do it, and build the answer for the client's
//! version. A response field is set only where the client's version has it - the codec refuses anything else.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{AbortedTransaction, FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse, HeartbeatRequest,
    HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, MetadataRequest, MetadataResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse,
    ProducerId, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use openduct_storage::batch::split_batches;
use openduct_storage::{CommittedOffset, Pending, ReadResult, Store, StoreError, TopicConfig, TopicInfo};
use tokio::sync::watch;

use crate::codes;
use crate::config::BrokerConfig;
use crate::groups::{Coordinator, JoinRequest};

mod admin;
mod cluster;
mod configs;
mod group_admin;
mod offsets;
mod topics;
pub(crate) mod transactions;
pub(crate) use admin::{
    alter_partition_reassignments, init_producer_refused, list_partition_reassignments,
    reassignment_list_refused, reassignments_refused, unregister_broker, unregister_refused,
};
pub(crate) use cluster::{
    find_coordinator, heartbeat_refused, join_refused, leave_refused, not_coordinator, offset_commit_refused,
    offset_fetch_refused, sync_refused,
};
pub(crate) use configs::{alter_configs, describe_cluster, describe_configs, incremental_alter_configs};
pub(crate) use group_admin::{delete_groups, delete_offsets, describe_groups, list_groups};
pub(crate) use offsets::{delete_records, list_offsets, offset_for_leader_epoch};
pub(crate) use topics::{create_partitions, create_topics, delete_topics};

/// The APIs OpenDuct answers and the versions of each. Capped where a newer version would need features OpenDuct does
/// not have (topic ids, transactions, the KIP-848 consumer protocol), so clients negotiate down to what works.
pub(crate) const SUPPORTED: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 12),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::DeleteRecords, 0, 2),
    (ApiKey::Metadata, 0, 12),
    (ApiKey::OffsetCommit, 2, 8),
    (ApiKey::OffsetFetch, 1, 8),
    (ApiKey::FindCoordinator, 0, 4),
    (ApiKey::JoinGroup, 0, 9),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::DescribeGroups, 0, 6),
    (ApiKey::ListGroups, 0, 5),
    (ApiKey::DeleteGroups, 0, 2),
    (ApiKey::OffsetDelete, 0, 0),
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::CreateTopics, 2, 7),
    (ApiKey::DeleteTopics, 1, 6),
    (ApiKey::CreatePartitions, 0, 3),
    (ApiKey::InitProducerId, 0, 4),
    (ApiKey::AddPartitionsToTxn, 0, 5),
    (ApiKey::AddOffsetsToTxn, 0, 3),
    (ApiKey::EndTxn, 0, 5),
    (ApiKey::TxnOffsetCommit, 0, 5),
    (ApiKey::WriteTxnMarkers, 0, 1),
    (ApiKey::DescribeProducers, 0, 0),
    (ApiKey::DescribeTransactions, 0, 0),
    (ApiKey::ListTransactions, 0, 2),
    (ApiKey::DescribeConfigs, 1, 4),
    (ApiKey::IncrementalAlterConfigs, 0, 1),
    (ApiKey::AlterConfigs, 0, 2),
    (ApiKey::OffsetForLeaderEpoch, 2, 4),
    (ApiKey::AlterPartitionReassignments, 0, 1),
    (ApiKey::ListPartitionReassignments, 0, 0),
    (ApiKey::UnregisterBroker, 0, 0),
    (ApiKey::DescribeCluster, 0, 2),
    (ApiKey::SaslHandshake, 1, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
];

pub(crate) fn supports(key: ApiKey, version: i16) -> bool {
    SUPPORTED
        .iter()
        .any(|(k, min, max)| *k == key && (*min..=*max).contains(&version))
}

/// Everything a request handler may use.
#[derive(Debug)]
pub(crate) struct State {
    pub(crate) config: BrokerConfig,
    pub(crate) store: Arc<dyn Store>,
    pub(crate) groups: Coordinator,
    /// Bumped on every append, so a waiting Fetch looks again.
    pub(crate) appended: watch::Sender<u64>,
    /// Becomes true when the broker is stopping, so a waiting Fetch answers at once.
    pub(crate) stopping: watch::Receiver<bool>,
    /// The broker-wide budget of request bytes (`max_request_memory`).
    pub(crate) request_memory: Arc<tokio::sync::Semaphore>,
    /// What the broker measures, for `--metrics-listen`.
    pub(crate) metrics: crate::metrics::Metrics,
    /// The transaction coordinator's state.
    pub(crate) transactions: transactions::Transactions,
}

fn s(text: impl Into<String>) -> StrBytes {
    StrBytes::from_string(text.into())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Run a store call off the network threads: it may block on the disk.
async fn blocking<T: Send + 'static>(
    store: &Arc<dyn Store>,
    f: impl FnOnce(&dyn Store) -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    let store = Arc::clone(store);
    match tokio::task::spawn_blocking(move || f(store.as_ref())).await {
        Ok(result) => result,
        Err(e) => Err(StoreError::Io(std::io::Error::other(format!(
            "storage task failed: {e}"
        )))),
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// `second_protocol`: whether the store keeps transactions now, so a client may use Kafka 4's second transaction
/// protocol (the feature `transaction.version` at 2) - announced from version 3, where features are told.
pub(crate) fn api_versions(
    _req: &ApiVersionsRequest,
    version: i16,
    second_protocol: bool,
) -> ApiVersionsResponse {
    let mut res = ApiVersionsResponse::default().with_api_keys(
        SUPPORTED
            .iter()
            .map(|(k, min, max)| {
                ApiVersion::default()
                    .with_api_key(*k as i16)
                    .with_min_version(*min)
                    .with_max_version(*max)
            })
            .collect(),
    );
    if version >= 3 && second_protocol {
        use kafka_protocol::messages::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey};
        res.supported_features = vec![
            SupportedFeatureKey::default()
                .with_name(s("transaction.version"))
                .with_min_version(0)
                .with_max_version(2),
        ];
        res.finalized_features = vec![
            FinalizedFeatureKey::default()
                .with_name(s("transaction.version"))
                .with_min_version_level(0)
                .with_max_version_level(2),
        ];
        res.finalized_features_epoch = 1;
    }
    res.error_code = codes::NONE;
    res
}

/// The answer to an ApiVersions of a version OpenDuct does not speak: sent as v0, so any client can read it and
/// retry with one it does.
pub(crate) fn api_versions_unsupported() -> ApiVersionsResponse {
    let mut res = api_versions(&ApiVersionsRequest::default(), 0, false);
    res.error_code = codes::UNSUPPORTED_VERSION;
    res
}

/// A topic's settings, read off the network threads like every store call: a store may block on its storage.
pub(super) async fn topic_info(state: &State, name: &str) -> Option<TopicInfo> {
    let name = name.to_owned();
    blocking(&state.store, move |s| Ok(s.topic(&name)))
        .await
        .ok()
        .flatten()
}

/// Every topic, read off the network threads.
pub(crate) async fn topic_list(state: &State) -> Vec<TopicInfo> {
    blocking(&state.store, |s| Ok(s.topics()))
        .await
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------------------------------------------

async fn ensure_topic(state: &State, name: &str) -> Result<(), StoreError> {
    if topic_info(state, name).await.is_some() {
        return Ok(());
    }
    if !state.config.auto_create_topics {
        return Err(StoreError::UnknownTopic(name.to_owned()));
    }
    let partitions = state.config.default_partitions;
    let name = name.to_owned();
    match blocking(&state.store, move |s| {
        s.create_topic(
            &name,
            TopicConfig {
                partitions,
                retention_ms: None,
            },
        )
    })
    .await
    {
        Ok(()) | Err(StoreError::TopicExists(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

pub(crate) async fn metadata(state: &State, req: &MetadataRequest, version: i16) -> MetadataResponse {
    let wanted: Option<Vec<String>> = match &req.topics {
        // v0: an empty list means every topic. v1+: null means every topic, an empty list none.
        Some(topics) if version == 0 && topics.is_empty() => None,
        Some(topics) => Some(
            topics
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.0.to_string()))
                .collect(),
        ),
        None => None,
    };
    let auto_create = state.config.auto_create_topics && (version < 4 || req.allow_auto_topic_creation);
    let names: Vec<String> = match wanted {
        None => topic_list(state).await.into_iter().map(|t| t.name).collect(),
        Some(names) => names,
    };
    let cluster = cluster::brokers(state);
    let mut topics = Vec::new();
    for name in names {
        if auto_create {
            let _ = ensure_topic(state, &name).await;
        }
        let mut topic = MetadataResponseTopic::default()
            .with_name(Some(TopicName(s(name.clone()))))
            .with_is_internal(openduct_storage::is_internal_topic(&name));
        match topic_info(state, &name).await {
            Some(info) => {
                // The topic's own id (v10+): clients tell topics apart by it.
                topic.topic_id = info.id;
                topic.partitions = (0..info.partitions)
                    .map(|p| {
                        let mut place = cluster::place(state, &name, p);
                        // A leader is only named if it is among the brokers answered: just after a broker leaves,
                        // the cluster may still think it leads for a moment.
                        if place
                            .leader
                            .is_some_and(|l| !cluster.brokers.iter().any(|b| b.node_id == l))
                        {
                            place.leader = None;
                        }
                        // During an election no broker leads: the client asks again shortly.
                        let mut part = MetadataResponsePartition::default()
                            .with_partition_index(p)
                            .with_error_code(
                                place.leader.map_or(codes::LEADER_NOT_AVAILABLE, |_| codes::NONE),
                            )
                            .with_leader_id(place.leader.unwrap_or(BrokerId(-1)))
                            .with_isr_nodes(place.replicas.clone())
                            .with_replica_nodes(place.replicas);
                        if version >= 7 {
                            part.leader_epoch = place.leader_epoch;
                        }
                        part
                    })
                    .collect();
            }
            None => {
                topic.error_code = if openduct_storage::validate_topic_name(&name).is_ok() {
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                } else {
                    codes::INVALID_TOPIC_EXCEPTION
                };
            }
        }
        topics.push(topic);
    }
    let mut res = MetadataResponse::default()
        .with_brokers(cluster.brokers)
        .with_topics(topics);
    if version >= 1 {
        res.controller_id = cluster.coordinator.unwrap_or(BrokerId(-1));
    }
    if version >= 2 {
        res.cluster_id = Some(s(state.config.cluster_id.clone()));
    }
    res
}

// ---------------------------------------------------------------------------------------------------------------

/// A produce request whose batches are written, in order, and on their way to being kept (on the device, or on most
/// brokers). [`produce_finish`] waits for that and answers; meanwhile the connection's next request can be written,
/// so their flushes are shared.
pub(crate) struct ProduceStarted {
    version: i16,
    acks: i16,
    /// Per topic its name and partition indexes, as asked: the answer keeps the request's order.
    topics: Vec<(TopicName, Vec<i32>)>,
    /// Per topic, per partition: an answer already known (a refusal), or the index of its work.
    planned: Vec<Vec<Result<usize, (i16, String)>>>,
    /// Per work item: where its first batch landed and the log start, or why it was refused - known now, or once
    /// the waiting below is done.
    appended: Appending,
}

type Placed = Result<(i64, i64), (i16, String)>;

enum Appending {
    Known(Vec<Placed>),
    Waiting(tokio::task::JoinHandle<Vec<Placed>>),
}

/// From Produce 12 a partition is part of a transaction by the transaction's first record there, with no request
/// of the producer's: the partition's leader tells the coordinator before it writes the record - once for each
/// transaction, since a producer with one open here is known to have been told of.
async fn added_to_its_transaction(
    state: &State,
    req: &ProduceRequest,
    version: i16,
    topic: &str,
    partition: i32,
    batches: &[Bytes],
) -> Result<(), (i16, String)> {
    let (Some(id), true) = (&req.transactional_id, version >= 12) else {
        return Ok(());
    };
    let Some(header) = batches
        .first()
        .and_then(|b| openduct_storage::batch::parse_header(b).ok())
        .filter(openduct_storage::batch::BatchHeader::is_transactional)
    else {
        return Ok(());
    };
    let (name, producer) = (topic.to_owned(), header.producer_id);
    let open = blocking(&state.store, move |s| s.open_transactions(&name, partition))
        .await
        .is_ok_and(|open| open.iter().any(|(id, _, _)| *id == producer));
    if open {
        return Ok(());
    }
    transactions::added_by_its_first_record(
        state,
        id.0.as_str(),
        header.producer_id,
        header.producer_epoch,
        topic,
        partition,
    )
    .await
    .map_err(|code| {
        (
            code,
            "the transaction's coordinator could not be told of this partition".to_owned(),
        )
    })
}

/// Write every partition's batches now - each partition on its own thread, a partition's batches in order - and
/// start waiting for them to reach the device.
pub(crate) async fn produce_begin(state: &State, req: &ProduceRequest, version: i16) -> ProduceStarted {
    let acks_ok = (-1..=1).contains(&req.acks);
    let mut planned: Vec<Vec<Result<usize, (i16, String)>>> = Vec::new();
    let mut topics = Vec::new();
    let mut work: Vec<(String, i32, Vec<Bytes>)> = Vec::new();
    for topic in &req.topic_data {
        let name = topic.name.0.to_string();
        let topic_ready = ensure_topic(state, &name).await;
        let mut parts = Vec::new();
        for p in &topic.partition_data {
            let planned_one = if !acks_ok {
                Err((codes::INVALID_REQUIRED_ACKS, "acks must be 0, 1 or -1".to_owned()))
            } else if let Err(e) = &topic_ready {
                Err((codes::of_store_error(e), e.to_string()))
            } else {
                match partition_batches(p.records.clone(), req.transactional_id.is_some()) {
                    Ok(batches) => {
                        match added_to_its_transaction(state, req, version, &name, p.index, &batches).await {
                            Ok(()) => {
                                work.push((name.clone(), p.index, batches));
                                Ok(work.len() - 1)
                            }
                            Err(refused) => Err(refused),
                        }
                    }
                    Err(refused) => Err(refused),
                }
            };
            parts.push(planned_one);
        }
        planned.push(parts);
        topics.push((
            topic.name.clone(),
            topic.partition_data.iter().map(|p| p.index).collect(),
        ));
    }
    let begun = append_all(state, work).await;
    let waiting = begun
        .iter()
        .any(|b| b.as_ref().is_ok_and(|ps| ps.iter().any(|p| !p.is_ready())));
    let appended = if waiting {
        Appending::Waiting(tokio::task::spawn_blocking(move || wait_all(begun)))
    } else {
        Appending::Known(begun.into_iter().map(first_placed).collect())
    };
    ProduceStarted {
        version,
        acks: req.acks,
        topics,
        planned,
        appended,
    }
}

/// Where a work item's first batch landed, once every batch of it is kept; the first refusal otherwise.
fn first_placed(begun: Result<Vec<Pending>, (i16, String)>) -> Placed {
    let mut first = None;
    for pending in begun? {
        match pending.wait() {
            Ok(a) => {
                first.get_or_insert((a.base_offset, a.log_start_offset));
            }
            Err(e) => return Err((codes::of_store_error(&e), e.to_string())),
        }
    }
    first.ok_or_else(|| (codes::KAFKA_STORAGE_ERROR, "no batch appended".to_owned()))
}

/// Wait for every work item at once (each on its own thread: a flush waited for alone would not start until the one
/// before it ended).
fn wait_all(begun: Vec<Result<Vec<Pending>, (i16, String)>>) -> Vec<Placed> {
    let waits =
        |b: &Result<Vec<Pending>, (i16, String)>| b.as_ref().is_ok_and(|ps| ps.iter().any(|p| !p.is_ready()));
    if begun.iter().filter(|b| waits(b)).count() <= 1 {
        return begun.into_iter().map(first_placed).collect();
    }
    std::thread::scope(|scope| {
        let waiting: Vec<_> = begun
            .into_iter()
            .map(|b| scope.spawn(move || first_placed(b)))
            .collect();
        waiting
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| {
                    Err((codes::KAFKA_STORAGE_ERROR, "an append wait panicked".to_owned()))
                })
            })
            .collect()
    })
}

/// The answer to a Produce, once its batches are on the device; `None` for acks=0, which is never answered.
pub(crate) async fn produce_finish(state: &State, started: ProduceStarted) -> Option<ProduceResponse> {
    let ProduceStarted {
        version,
        acks,
        topics,
        planned,
        appended,
    } = started;
    let appended = match appended {
        Appending::Known(placed) => placed,
        Appending::Waiting(waiting) => waiting.await.unwrap_or_default(),
    };
    // Fetches waiting for news are woken only now: before, the batches were not readable.
    if appended.iter().any(Result::is_ok) {
        state.appended.send_modify(|n| *n = n.wrapping_add(1));
    }
    let mut responses = Vec::new();
    for ((name, indexes), parts) in topics.into_iter().zip(planned) {
        let mut partitions = Vec::new();
        for (index, planned_one) in indexes.into_iter().zip(parts) {
            let mut out = PartitionProduceResponse::default().with_index(index);
            let result = planned_one.and_then(|i| {
                appended.get(i).cloned().unwrap_or_else(|| {
                    Err((codes::KAFKA_STORAGE_ERROR, "no answer from the store".to_owned()))
                })
            });
            match result {
                Ok(base) => {
                    out.base_offset = base.0;
                    if version >= 5 {
                        out.log_start_offset = base.1;
                    }
                }
                Err((code, message)) => {
                    out.error_code = code;
                    out.base_offset = -1;
                    if version >= 8 {
                        out.error_message = Some(s(message));
                    }
                }
            }
            partitions.push(out);
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(name)
                .with_partition_responses(partitions),
        );
    }
    if acks == 0 {
        return None;
    }
    Some(ProduceResponse::default().with_responses(responses))
}

/// One partition's records, split into the batches the store appends.
fn partition_batches(records: Option<Bytes>, transactional: bool) -> Result<Vec<Bytes>, (i16, String)> {
    let records = records.ok_or((codes::INVALID_RECORD, "a partition with no records".to_owned()))?;
    let batches = split_batches(&records).map_err(|e| {
        let e = StoreError::Batch(e);
        (codes::of_store_error(&e), e.to_string())
    })?;
    if batches.is_empty() {
        return Err((
            codes::INVALID_RECORD,
            "a partition with no record batches".to_owned(),
        ));
    }
    // A transaction's records come in a request that names its transactional id (which is what the access rules
    // are checked against): without one they would open a transaction nothing would ever end.
    if !transactional && batches.iter().any(|(header, _)| header.is_transactional()) {
        return Err((
            codes::INVALID_REQUEST,
            "a transactional batch in a request that names no transactional id".to_owned(),
        ));
    }
    Ok(batches.into_iter().map(|(_, batch)| batch).collect())
}

/// Write every partition's batches, each partition on its own thread (one alone runs in place): for each, its
/// appends under way, or why it was refused. A partition's batches stay in order.
async fn append_all(
    state: &State,
    work: Vec<(String, i32, Vec<Bytes>)>,
) -> Vec<Result<Vec<Pending>, (i16, String)>> {
    if work.is_empty() {
        return Vec::new();
    }
    let count = work.len();
    type Begun = Result<Vec<Pending>, StoreError>;
    let answered = blocking(&state.store, move |s| {
        let one = |(topic, partition, batches): (String, i32, Vec<Bytes>)| -> Begun {
            let mut pending = Vec::with_capacity(batches.len());
            for batch in batches {
                pending.push(s.append_begin(&topic, partition, batch)?);
            }
            Ok(pending)
        };
        if work.len() == 1 {
            return Ok(work.into_iter().map(one).collect::<Vec<_>>());
        }
        Ok(std::thread::scope(|scope| {
            let running: Vec<_> = work.into_iter().map(|w| scope.spawn(move || one(w))).collect();
            running
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap_or_else(|_| Err(StoreError::Io(std::io::Error::other("an append panicked"))))
                })
                .collect::<Vec<_>>()
        }))
    })
    .await;
    let refusal = |e: &StoreError| (codes::of_store_error(e), e.to_string());
    match answered {
        Ok(results) => results.into_iter().map(|r| r.map_err(|e| refusal(&e))).collect(),
        Err(e) => (0..count).map(|_| Err(refusal(&e))).collect(),
    }
}

// ---------------------------------------------------------------------------------------------------------------

pub(crate) async fn fetch(state: &State, req: &FetchRequest, version: i16) -> FetchResponse {
    let max_wait = Duration::from_millis(u64::try_from(req.max_wait_ms.max(0)).unwrap_or(0))
        .min(state.config.max_fetch_wait);
    let deadline = Instant::now() + max_wait;
    let min_bytes = usize::try_from(req.min_bytes.max(0)).unwrap_or(0);
    let max_bytes = usize::try_from(req.max_bytes.max(1))
        .unwrap_or(usize::MAX)
        .min(state.config.max_fetch_bytes);
    let mut appended = state.appended.subscribe();
    let mut stopping = state.stopping.clone();
    loop {
        appended.mark_unchanged();
        let (response, bytes) = fetch_once(state, req, version, max_bytes).await;
        let now = Instant::now();
        if bytes >= min_bytes.max(1) || now >= deadline {
            return response;
        }
        // Nothing (or not enough) yet: wait for an append anywhere, the client's wait to run out, or the broker to
        // stop.
        let appended_meanwhile = tokio::select! {
            changed = appended.changed() => changed.is_ok(),
            () = tokio::time::sleep(deadline - now) => false,
            _ = stopping.wait_for(|stop| *stop) => false,
        };
        if !appended_meanwhile {
            let (response, _) = fetch_once(state, req, version, max_bytes).await;
            return response;
        }
    }
}

async fn fetch_once(
    state: &State,
    req: &FetchRequest,
    version: i16,
    max_bytes: usize,
) -> (FetchResponse, usize) {
    let mut total = 0usize;
    let mut topics = Vec::new();
    for t in &req.topics {
        let name = t.topic.0.to_string();
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let mut out = PartitionData::default().with_partition_index(p.partition);
            let budget = usize::try_from(p.partition_max_bytes.max(1))
                .unwrap_or(usize::MAX)
                .min(max_bytes.saturating_sub(total).max(1));
            let (topic, partition, offset) = (name.clone(), p.partition, p.fetch_offset);
            // read_committed: only what is before the last stable offset, with the aborted transactions in it
            // named for the consumer to skip.
            let committed_only = req.isolation_level == 1;
            // Once the answer is full, the other partitions report their ends and wait for the next Fetch.
            let full = total > 0 && total >= max_bytes;
            let read = blocking(&state.store, move |s| {
                if full {
                    s.offsets(&topic, partition).map(|offsets| {
                        (
                            ReadResult {
                                batches: Vec::new(),
                                offsets,
                            },
                            Vec::new(),
                        )
                    })
                } else {
                    let mut read = s.read(&topic, partition, offset, budget)?;
                    let mut aborted = Vec::new();
                    if committed_only {
                        let stable = read.offsets.last_stable;
                        read.batches.retain(|b| {
                            openduct_storage::batch::parse_stored_header(b)
                                .is_ok_and(|h| h.base_offset < stable)
                        });
                        aborted = s.aborted_transactions(&topic, partition, offset, stable)?;
                    }
                    Ok((read, aborted))
                }
            })
            .await;
            match read {
                Ok((read, aborted)) => {
                    out.high_watermark = read.offsets.high_watermark;
                    out.last_stable_offset = read.offsets.last_stable;
                    if committed_only && version >= 4 {
                        out.aborted_transactions = Some(
                            aborted
                                .iter()
                                .map(|t| {
                                    AbortedTransaction::default()
                                        .with_producer_id(t.producer_id.into())
                                        .with_first_offset(t.first_offset)
                                })
                                .collect(),
                        );
                    }
                    if version >= 5 {
                        out.log_start_offset = read.offsets.log_start;
                    }
                    let mut size: usize = read.batches.iter().map(Bytes::len).sum();
                    // Only the answer's first batch may go over its limit (so a consumer is never stuck behind a
                    // large batch); a later partition whose next batch does not fit waits for the next Fetch.
                    let fits = total == 0 || size <= max_bytes.saturating_sub(total);
                    if !fits {
                        size = 0;
                    }
                    let mut records = BytesMut::with_capacity(size);
                    for b in read.batches.iter().filter(|_| fits) {
                        records.extend_from_slice(b);
                    }
                    total = total.saturating_add(size);
                    out.records = Some(records.freeze());
                }
                Err(e) => {
                    out.error_code = codes::of_store_error(&e);
                    out.high_watermark = -1;
                    if let StoreError::OffsetOutOfRange {
                        log_start,
                        high_watermark,
                        ..
                    } = e
                    {
                        out.high_watermark = high_watermark;
                        out.last_stable_offset = high_watermark;
                        if version >= 5 {
                            out.log_start_offset = log_start;
                        }
                    }
                    out.records = Some(Bytes::new());
                }
            }
            partitions.push(out);
        }
        topics.push(
            FetchableTopicResponse::default()
                .with_topic(t.topic.clone())
                .with_partitions(partitions),
        );
    }
    (FetchResponse::default().with_responses(topics), total)
}

// ---------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------------------

pub(crate) async fn init_producer_id(state: &State, req: &InitProducerIdRequest) -> InitProducerIdResponse {
    // A transactional id is the transaction coordinator's to answer.
    if let Some(id) = &req.transactional_id {
        return transactions::init(state, req, id.0.as_str()).await;
    }
    let mut res = InitProducerIdResponse::default();
    // ALWAYS A NEW ID, whatever id and epoch the request names, as Kafka does for a producer that is not
    // transactional: clients send their current id and epoch - or 0 and 0 for a new producer (2026-11-15: keeping
    // the named id gave every such producer id 0, and their sequences refused each other's batches for good).
    match blocking(&state.store, |s| s.next_producer_id()).await {
        Ok(id) => {
            res.producer_id = ProducerId(id);
            res.producer_epoch = 0;
        }
        Err(e) => {
            res.error_code = codes::of_store_error(&e);
            res.producer_id = ProducerId(-1);
            res.producer_epoch = -1;
        }
    }
    res
}

// ---------------------------------------------------------------------------------------------------------------

pub(crate) async fn join_group(
    state: &State,
    req: &JoinGroupRequest,
    version: i16,
    client_id: &str,
    host: std::net::IpAddr,
) -> JoinGroupResponse {
    let session = Duration::from_millis(u64::try_from(req.session_timeout_ms.max(0)).unwrap_or(0));
    // v0 has no rebalance timeout: it is the session timeout.
    let rebalance = if version >= 1 {
        Duration::from_millis(u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(0))
    } else {
        session
    };
    let outcome = state
        .groups
        .join(JoinRequest {
            group: req.group_id.0.to_string(),
            member_id: req.member_id.to_string(),
            group_instance_id: req.group_instance_id.as_ref().map(|g| g.to_string()),
            client_id: if client_id.is_empty() {
                "client".to_owned()
            } else {
                client_id.to_owned()
            },
            client_host: format!("/{host}"),
            session_timeout: session,
            rebalance_timeout: rebalance,
            protocol_type: req.protocol_type.to_string(),
            protocols: req
                .protocols
                .iter()
                .map(|p| (p.name.to_string(), p.metadata.clone()))
                .collect(),
            require_known_member_id: version >= 4,
        })
        .await;
    let mut res = JoinGroupResponse::default()
        .with_error_code(outcome.error)
        .with_generation_id(outcome.generation)
        .with_leader(s(outcome.leader))
        .with_member_id(s(outcome.member_id))
        .with_members(
            outcome
                .members
                .into_iter()
                .map(|(id, instance, metadata)| {
                    let mut m = JoinGroupResponseMember::default()
                        .with_member_id(s(id))
                        .with_metadata(metadata);
                    if version >= 5 {
                        m.group_instance_id = instance.map(s);
                    }
                    m
                })
                .collect(),
        );
    if version >= 7 {
        res.protocol_type = outcome.protocol_type.map(s);
    }
    res.protocol_name = outcome.protocol_name.map(s);
    if version < 7 && res.protocol_name.is_none() {
        // Before v7 the protocol name is not nullable.
        res.protocol_name = Some(StrBytes::default());
    }
    res
}

pub(crate) async fn sync_group(state: &State, req: &SyncGroupRequest, version: i16) -> SyncGroupResponse {
    let outcome = state
        .groups
        .sync(
            &req.group_id.0,
            &req.member_id,
            req.generation_id,
            req.assignments
                .iter()
                .map(|a| (a.member_id.to_string(), a.assignment.clone()))
                .collect(),
        )
        .await;
    let mut res = SyncGroupResponse::default()
        .with_error_code(outcome.error)
        .with_assignment(outcome.assignment);
    if version >= 5 {
        res.protocol_type = outcome.protocol_type.map(s);
        res.protocol_name = outcome.protocol_name.map(s);
    }
    res
}

pub(crate) fn heartbeat(state: &State, req: &HeartbeatRequest) -> HeartbeatResponse {
    HeartbeatResponse::default().with_error_code(state.groups.heartbeat(
        &req.group_id.0,
        &req.member_id,
        req.generation_id,
    ))
}

pub(crate) fn leave_group(state: &State, req: &LeaveGroupRequest, version: i16) -> LeaveGroupResponse {
    if version >= 3 {
        let ids: Vec<String> = req.members.iter().map(|m| m.member_id.to_string()).collect();
        let results = state.groups.leave(&req.group_id.0, &ids);
        LeaveGroupResponse::default().with_members(
            results
                .into_iter()
                .map(|(id, code)| {
                    MemberResponse::default()
                        .with_member_id(s(id))
                        .with_error_code(code)
                })
                .collect(),
        )
    } else {
        let results = state.groups.leave(&req.group_id.0, &[req.member_id.to_string()]);
        let code = results
            .first()
            .map(|(_, c)| *c)
            .unwrap_or(codes::UNKNOWN_MEMBER_ID);
        LeaveGroupResponse::default().with_error_code(code)
    }
}

pub(crate) async fn offset_commit(state: &State, req: &OffsetCommitRequest) -> OffsetCommitResponse {
    let group = req.group_id.0.to_string();
    let allowed = state
        .groups
        .check_commit(&group, &req.member_id, req.generation_id_or_member_epoch);
    let mut commits = Vec::new();
    let mut topics = Vec::new();
    let now = now_ms();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let info = topic_info(state, &name).await;
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let code = if allowed != codes::NONE {
                allowed
            } else {
                match &info {
                    Some(info) if (0..info.partitions).contains(&p.partition_index) => {
                        commits.push(CommittedOffset {
                            topic: name.clone(),
                            partition: p.partition_index,
                            offset: p.committed_offset,
                            metadata: p.committed_metadata.as_ref().map(|m| m.to_string()),
                            commit_timestamp_ms: now,
                        });
                        codes::NONE
                    }
                    _ => codes::UNKNOWN_TOPIC_OR_PARTITION,
                }
            };
            partitions.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(p.partition_index)
                    .with_error_code(code),
            );
        }
        topics.push((t.name.clone(), partitions));
    }
    let stored = if commits.is_empty() {
        Ok(())
    } else {
        let g = group.clone();
        blocking(&state.store, move |s| s.commit_offsets(&g, &commits)).await
    };
    let failed = stored.err().map(|e| codes::of_store_error(&e));
    OffsetCommitResponse::default().with_topics(
        topics
            .into_iter()
            .map(|(name, mut partitions)| {
                if let Some(code) = failed {
                    for p in &mut partitions {
                        if p.error_code == codes::NONE {
                            p.error_code = code;
                        }
                    }
                }
                OffsetCommitResponseTopic::default()
                    .with_name(name)
                    .with_partitions(partitions)
            })
            .collect(),
    )
}

/// One group's committed offsets for the asked partitions (all of them when `topics` is `None`).
async fn committed_for(
    state: &State,
    group: &str,
    topics: Option<Vec<(String, Vec<i32>)>>,
) -> Result<Vec<(String, Vec<(i32, i64, Option<String>)>)>, StoreError> {
    let g = group.to_owned();
    let committed = blocking(&state.store, move |s| s.committed_offsets(&g)).await?;
    let asked = match topics {
        Some(t) => t,
        None => {
            let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
            for c in &committed {
                match by_topic.iter_mut().find(|(t, _)| *t == c.topic) {
                    Some((_, ps)) => ps.push(c.partition),
                    None => by_topic.push((c.topic.clone(), vec![c.partition])),
                }
            }
            by_topic
        }
    };
    Ok(asked
        .into_iter()
        .map(|(topic, partitions)| {
            let rows = partitions
                .into_iter()
                .map(|p| {
                    committed
                        .iter()
                        .find(|c| c.topic == topic && c.partition == p)
                        .map(|c| (p, c.offset, c.metadata.clone()))
                        .unwrap_or((p, -1, None))
                })
                .collect();
            (topic, rows)
        })
        .collect())
}

pub(crate) async fn offset_fetch(
    state: &State,
    req: &OffsetFetchRequest,
    version: i16,
) -> OffsetFetchResponse {
    if version >= 8 {
        let mut groups = Vec::new();
        for g in &req.groups {
            let asked = g.topics.as_ref().map(|ts| {
                ts.iter()
                    .map(|t| (t.name.0.to_string(), t.partition_indexes.clone()))
                    .collect()
            });
            let mut out = OffsetFetchResponseGroup::default().with_group_id(g.group_id.clone());
            match committed_for(state, &g.group_id.0, asked).await {
                Ok(rows) => {
                    out.topics = rows
                        .into_iter()
                        .map(|(topic, ps)| {
                            OffsetFetchResponseTopics::default()
                                .with_name(TopicName(s(topic)))
                                .with_partitions(
                                    ps.into_iter()
                                        .map(|(p, offset, metadata)| {
                                            OffsetFetchResponsePartitions::default()
                                                .with_partition_index(p)
                                                .with_committed_offset(offset)
                                                .with_metadata(Some(s(metadata.unwrap_or_default())))
                                        })
                                        .collect(),
                                )
                        })
                        .collect();
                }
                Err(e) => out.error_code = codes::of_store_error(&e),
            }
            groups.push(out);
        }
        return OffsetFetchResponse::default().with_groups(groups);
    }
    let asked = req.topics.as_ref().map(|ts| {
        ts.iter()
            .map(|t| (t.name.0.to_string(), t.partition_indexes.clone()))
            .collect()
    });
    let mut res = OffsetFetchResponse::default();
    match committed_for(state, &req.group_id.0, asked).await {
        Ok(rows) => {
            res.topics = rows
                .into_iter()
                .map(|(topic, ps)| {
                    OffsetFetchResponseTopic::default()
                        .with_name(TopicName(s(topic)))
                        .with_partitions(
                            ps.into_iter()
                                .map(|(p, offset, metadata)| {
                                    OffsetFetchResponsePartition::default()
                                        .with_partition_index(p)
                                        .with_committed_offset(offset)
                                        .with_metadata(Some(s(metadata.unwrap_or_default())))
                                })
                                .collect(),
                        )
                })
                .collect();
        }
        Err(e) => {
            if version >= 2 {
                res.error_code = codes::of_store_error(&e);
            }
        }
    }
    res
}
