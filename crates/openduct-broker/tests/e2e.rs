// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The broker end to end, over real TCP: an independent Kafka client (rskafka, which OpenDuct did not write)
//! creating topics, producing, fetching and listing offsets; durability across a restart; long-poll fetches;
//! consumer groups and idempotent producers over the wire; and hostile input.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics};
use kafka_protocol::messages::offset_for_leader_epoch_request::{
    OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    FindCoordinatorRequest, GroupId, HeartbeatRequest, InitProducerIdRequest, JoinGroupRequest,
    LeaveGroupRequest, ListOffsetsRequest, OffsetCommitRequest, OffsetFetchRequest,
    OffsetForLeaderEpochRequest, ProduceRequest, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
use openduct_broker::client::Client;
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{FileStore, FileStoreConfig, MemoryStore, Store};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression as RsCompression, OffsetAt, UnknownTopicHandling};
use rskafka::record::Record as RsRecord;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn config() -> BrokerConfig {
    let mut c = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    c.advertised_port = 0;
    c.group_initial_rebalance_delay = Duration::ZERO;
    c
}

async fn start(store: Arc<dyn Store>) -> (Broker, String) {
    let broker = Broker::start(config(), store).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

fn record(value: &str) -> RsRecord {
    RsRecord {
        key: Some(b"k".to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: Default::default(),
        timestamp: chrono::Utc::now(),
    }
}

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

#[tokio::test]
async fn an_independent_kafka_client_creates_produces_fetches_and_lists_offsets() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let client = ClientBuilder::new(vec![addr]).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("sensors", 2, 3, 5_000)
        .await
        .unwrap();
    let topics = client.list_topics().await.unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].partitions.len(), 2);

    let partition = client
        .partition_client("sensors", 1, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let offsets = partition
        .produce(
            vec![record("a"), record("b"), record("c")],
            RsCompression::NoCompression,
        )
        .await
        .unwrap();
    assert_eq!(offsets, vec![0, 1, 2]);
    let more = partition
        .produce(vec![record("d")], RsCompression::NoCompression)
        .await
        .unwrap();
    assert_eq!(more, vec![3]);

    let (records, high_watermark) = partition.fetch_records(1, 1..1_000_000, 1_000).await.unwrap();
    assert_eq!(high_watermark, 4);
    let values: Vec<String> = records
        .iter()
        .filter(|r| r.offset >= 1)
        .map(|r| String::from_utf8(r.record.value.clone().unwrap()).unwrap())
        .collect();
    assert_eq!(values, vec!["b", "c", "d"]);
    assert_eq!(partition.get_offset(OffsetAt::Earliest).await.unwrap(), 0);
    assert_eq!(partition.get_offset(OffsetAt::Latest).await.unwrap(), 4);

    let mut admin = Client::connect(broker.local_addr().to_string().as_str(), Duration::from_secs(10))
        .await
        .unwrap();
    admin.delete_topic("sensors").await.unwrap();
    assert!(client.list_topics().await.unwrap().is_empty());
    let again = admin.delete_topic("sensors").await;
    assert!(matches!(
        again,
        Err(openduct_broker::client::ClientError::Refused { code: 3, .. })
    ));
    broker.shutdown().await;
}

#[tokio::test]
async fn data_written_through_the_protocol_survives_a_broker_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Arc::new(FileStore::open(dir.path(), FileStoreConfig::default()).unwrap());
        let (broker, addr) = start(store).await;
        let client = ClientBuilder::new(vec![addr]).build().await.unwrap();
        client
            .controller_client()
            .unwrap()
            .create_topic("durable", 1, 1, 5_000)
            .await
            .unwrap();
        let p = client
            .partition_client("durable", 0, UnknownTopicHandling::Error)
            .await
            .unwrap();
        p.produce(vec![record("kept")], RsCompression::NoCompression)
            .await
            .unwrap();
        broker.shutdown().await;
    }
    let store = Arc::new(FileStore::open(dir.path(), FileStoreConfig::default()).unwrap());
    let (broker, addr) = start(store).await;
    let client = ClientBuilder::new(vec![addr]).build().await.unwrap();
    let p = client
        .partition_client("durable", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let (records, hw) = p.fetch_records(0, 1..1_000_000, 100).await.unwrap();
    assert_eq!(hw, 1);
    assert_eq!(records[0].record.value.as_deref(), Some(&b"kept"[..]));
    broker.shutdown().await;
}

#[tokio::test]
async fn a_stopping_broker_does_not_wait_out_a_fetch_waiting_for_records() {
    // The restart drill in a test lane, 2026-11-24: a stopping broker was still waiting on something sixty seconds
    // later, when its pod was killed. A fetch waiting for records that will not come must not be that: the stop
    // ends it. (It passes with or without the drain's bound in server.rs - a waiting fetch already ends on the stop.
    // The bound is for a request that does not: in the drill, most likely a write to a partition the stopping
    // broker had been handed back, which members.rs `leave` now prevents.)
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let client = Arc::new(ClientBuilder::new(vec![addr]).build().await.unwrap());
    client
        .controller_client()
        .unwrap()
        .create_topic("quiet", 1, 1, 5_000)
        .await
        .unwrap();
    let reader = client
        .partition_client("quiet", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let waiting = tokio::spawn(async move { reader.fetch_records(0, 1..1_000_000, 30_000).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(15), broker.shutdown())
        .await
        .expect("the broker stopped, not after the fetch's thirty seconds");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "stopped in {:?}",
        started.elapsed()
    );
    waiting.abort();
}

#[tokio::test]
async fn a_waiting_fetch_returns_as_soon_as_a_record_arrives() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let client = Arc::new(ClientBuilder::new(vec![addr]).build().await.unwrap());
    client
        .controller_client()
        .unwrap()
        .create_topic("live", 1, 1, 5_000)
        .await
        .unwrap();
    let reader = client
        .partition_client("live", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let started = Instant::now();
    let waiting = tokio::spawn(async move { reader.fetch_records(0, 1..1_000_000, 10_000).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let writer = client
        .partition_client("live", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    writer
        .produce(vec![record("now")], RsCompression::NoCompression)
        .await
        .unwrap();
    let (records, _) = waiting.await.unwrap().unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "woken by the append, not the 10 s wait"
    );
    broker.shutdown().await;
}

fn join(group: &str, member: &str) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(5_000)
        .with_member_id(s(member))
        .with_protocol_type(s("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(s("range"))
                .with_metadata(Bytes::from_static(b"subscription")),
        ])
}

#[tokio::test]
async fn a_consumer_group_joins_syncs_heartbeats_commits_and_leaves_over_the_wire() {
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "orders",
            openduct_storage::TopicConfig {
                partitions: 2,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, addr) = start(store).await;
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();

    let coordinator = c
        .send(&FindCoordinatorRequest::default().with_key(s("billing")), 3)
        .await
        .unwrap();
    assert_eq!(coordinator.error_code, 0);
    assert_eq!(coordinator.port, i32::from(broker.local_addr().port()));

    let first = c.send(&join("billing", ""), 9).await.unwrap();
    assert_eq!(
        first.error_code, 79,
        "MEMBER_ID_REQUIRED, with an id to join with"
    );
    let member = first.member_id.to_string();
    let joined = c.send(&join("billing", &member), 9).await.unwrap();
    assert_eq!(joined.error_code, 0);
    assert_eq!(joined.leader.to_string(), member);
    assert_eq!(joined.protocol_name.as_deref(), Some("range"));
    assert_eq!(joined.members.len(), 1);

    let synced = c
        .send(
            &SyncGroupRequest::default()
                .with_group_id(GroupId(s("billing")))
                .with_generation_id(joined.generation_id)
                .with_member_id(s(&member))
                .with_protocol_type(Some(s("consumer")))
                .with_protocol_name(Some(s("range")))
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(s(&member))
                        .with_assignment(Bytes::from_static(b"orders:0,1")),
                ]),
            5,
        )
        .await
        .unwrap();
    assert_eq!(synced.error_code, 0);
    assert_eq!(synced.assignment, Bytes::from_static(b"orders:0,1"));

    let beat = c
        .send(
            &HeartbeatRequest::default()
                .with_group_id(GroupId(s("billing")))
                .with_generation_id(joined.generation_id)
                .with_member_id(s(&member)),
            4,
        )
        .await
        .unwrap();
    assert_eq!(beat.error_code, 0);

    let committed = c
        .send(
            &OffsetCommitRequest::default()
                .with_group_id(GroupId(s("billing")))
                .with_generation_id_or_member_epoch(joined.generation_id)
                .with_member_id(s(&member))
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(s("orders")))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(1)
                                .with_committed_offset(42)
                                .with_committed_metadata(Some(s("note"))),
                        ]),
                ]),
            8,
        )
        .await
        .unwrap();
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);

    let fetched = c
        .send(
            &OffsetFetchRequest::default().with_groups(vec![
                OffsetFetchRequestGroup::default()
                    .with_group_id(GroupId(s("billing")))
                    .with_topics(Some(vec![
                        OffsetFetchRequestTopics::default()
                            .with_name(TopicName(s("orders")))
                            .with_partition_indexes(vec![0, 1]),
                    ])),
            ]),
            8,
        )
        .await
        .unwrap();
    let parts = &fetched.groups[0].topics[0].partitions;
    assert_eq!(
        (parts[0].partition_index, parts[0].committed_offset),
        (0, -1),
        "never committed"
    );
    assert_eq!((parts[1].partition_index, parts[1].committed_offset), (1, 42));
    assert_eq!(parts[1].metadata.as_deref(), Some("note"));

    let left = c
        .send(
            &LeaveGroupRequest::default()
                .with_group_id(GroupId(s("billing")))
                .with_members(vec![MemberIdentity::default().with_member_id(s(&member))]),
            5,
        )
        .await
        .unwrap();
    assert_eq!(left.members[0].error_code, 0);
    broker.shutdown().await;
}

fn idempotent_batch(producer_id: i64, sequence: i32, values: &[&str]) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(i).unwrap(),
            sequence: sequence + i32::try_from(i).unwrap(),
            timestamp: 1_700_000_000_000,
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    buf.freeze()
}

fn produce(topic: &str, batch: Bytes) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s(topic)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch)),
                ]),
        ])
}

#[tokio::test]
async fn an_idempotent_producers_retry_keeps_its_offset_and_a_gap_is_refused() {
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "payments",
            openduct_storage::TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, addr) = start(store).await;
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    // An idempotent (not transactional) producer: no transactional id, no id of its own yet.
    let request = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_producer_id(kafka_protocol::messages::ProducerId(-1))
        .with_producer_epoch(-1);
    let id = c.send(&request, 4).await.unwrap();
    assert_eq!(id.error_code, 0);
    let pid = id.producer_id.0;

    let first = idempotent_batch(pid, 0, &["a", "b"]);
    let stored = c.send(&produce("payments", first.clone()), 9).await.unwrap();
    assert_eq!(stored.responses[0].partition_responses[0].base_offset, 0);
    // The producer did not see the answer and sends the same batch again: same offset, nothing new stored.
    let retried = c.send(&produce("payments", first), 9).await.unwrap();
    let p = &retried.responses[0].partition_responses[0];
    assert_eq!((p.error_code, p.base_offset), (0, 0));
    let next = c
        .send(&produce("payments", idempotent_batch(pid, 2, &["c"])), 9)
        .await
        .unwrap();
    assert_eq!(next.responses[0].partition_responses[0].base_offset, 2);
    // Sequence 4 when 3 is next: refused, OUT_OF_ORDER_SEQUENCE_NUMBER.
    let gap = c
        .send(&produce("payments", idempotent_batch(pid, 4, &["e"])), 9)
        .await
        .unwrap();
    assert_eq!(gap.responses[0].partition_responses[0].error_code, 45);
    broker.shutdown().await;
}

#[tokio::test]
async fn an_unknown_topic_is_an_error_and_a_corrupt_batch_is_refused() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    let missing = c
        .send(&produce("nope", idempotent_batch(-1, -1, &["x"])), 9)
        .await
        .unwrap();
    assert_eq!(missing.responses[0].partition_responses[0].error_code, 3);
    c.create_topic("t", 1, None).await.unwrap();
    let mut bad = idempotent_batch(-1, -1, &["x"]).to_vec();
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    let corrupt = c.send(&produce("t", Bytes::from(bad)), 9).await.unwrap();
    assert_eq!(
        corrupt.responses[0].partition_responses[0].error_code, 2,
        "CORRUPT_MESSAGE"
    );
    broker.shutdown().await;
}

async fn closes(addr: &str, bytes: &[u8]) -> bool {
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    raw.write_all(bytes).await.unwrap();
    let mut buf = [0u8; 64];
    matches!(
        tokio::time::timeout(Duration::from_secs(5), raw.read(&mut buf)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test]
async fn hostile_input_closes_only_that_connection() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    // A request claiming 2 GiB: refused before a byte of it is read.
    assert!(closes(&addr, &i32::MAX.to_be_bytes()).await);
    // A negative size.
    assert!(closes(&addr, &(-5i32).to_be_bytes()).await);
    // An API that does not exist.
    let mut unknown = 8i32.to_be_bytes().to_vec();
    unknown.extend_from_slice(&[0x7F, 0x00, 0, 0, 0, 0, 0, 1]);
    assert!(closes(&addr, &unknown).await);
    // Garbage where a Metadata request should be.
    let mut garbage = 12i32.to_be_bytes().to_vec();
    garbage.extend_from_slice(&[0, 3, 0, 12, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert!(closes(&addr, &garbage).await);
    // And the broker still serves everyone else.
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    assert!(c.api_versions().await.unwrap().api_keys.len() >= 14);
    broker.shutdown().await;
}

#[tokio::test]
async fn a_request_claiming_a_huge_list_is_refused_and_the_broker_carries_on() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let frame = |body: &[u8]| {
        let mut frame = i32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(body);
        frame
    };
    // Metadata v1 (key 3), no client id, a topic list of 2,147,483,647 entries: four bytes that would ask for
    // over 100 GB if the count sized the list.
    let old = frame(&[0, 3, 0, 1, 0, 0, 0, 1, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF]);
    assert!(closes(&addr, &old).await);
    // Metadata v9, as newer clients send it: a compact list of 4,294,967,294 entries.
    let compact = frame(&[
        0, 3, 0, 9, 0, 0, 0, 2, 0xFF, 0xFF, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F,
    ]);
    assert!(closes(&addr, &compact).await);
    // One topic whose name claims 4 GiB.
    let name = frame(&[
        0, 3, 0, 9, 0, 0, 0, 3, 0xFF, 0xFF, 0, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F,
    ]);
    assert!(closes(&addr, &name).await);
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    assert!(c.api_versions().await.unwrap().api_keys.len() >= 14);
    broker.shutdown().await;
}

#[tokio::test]
async fn an_api_versions_request_of_a_future_version_is_answered_so_the_client_can_retry() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let mut raw = tokio::net::TcpStream::connect(&addr).await.unwrap();
    // ApiVersions (key 18) at version 99, correlation 7, no client id: a client newer than the broker.
    let header = [0u8, 18, 0, 99, 0, 0, 0, 7, 0xFF, 0xFF];
    let mut frame = i32::try_from(header.len()).unwrap().to_be_bytes().to_vec();
    frame.extend_from_slice(&header);
    raw.write_all(&frame).await.unwrap();
    let mut size = [0u8; 4];
    raw.read_exact(&mut size).await.unwrap();
    let mut body = vec![0u8; usize::try_from(i32::from_be_bytes(size)).unwrap()];
    raw.read_exact(&mut body).await.unwrap();
    // Answered as version 0, which every client can read: correlation id, UNSUPPORTED_VERSION, the real list.
    assert_eq!(i32::from_be_bytes(body[0..4].try_into().unwrap()), 7);
    assert_eq!(i16::from_be_bytes(body[4..6].try_into().unwrap()), 35);
    let count = i32::from_be_bytes(body[6..10].try_into().unwrap());
    assert!(
        count >= 14,
        "the versions it does support, so the client can retry"
    );
    let first_key_max = i16::from_be_bytes(body[14..16].try_into().unwrap());
    assert!(first_key_max >= 0);
    broker.shutdown().await;
}

async fn start_with(config: BrokerConfig, store: Arc<dyn Store>) -> (Broker, String) {
    let broker = Broker::start(config, store).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

#[tokio::test]
async fn a_request_that_never_arrives_is_dropped_and_its_memory_comes_back() {
    let mut config = config();
    config.max_request_bytes = 1024 * 1024;
    config.max_request_memory = 1024 * 1024;
    config.request_read_timeout = Duration::from_millis(500);
    let (broker, addr) = start_with(config, Arc::new(MemoryStore::new())).await;
    // Announces 1 MiB - the whole request budget - and sends nothing more.
    let mut stalled = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stalled.write_all(&(1024 * 1024i32).to_be_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Another client waits for room in the budget rather than the broker over-spending it...
    let started = Instant::now();
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    c.api_versions().await.unwrap();
    // ...and gets it once the stalled request is dropped.
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "waited for the budget"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    let mut buf = [0u8; 8];
    assert!(
        matches!(stalled.read(&mut buf).await, Ok(0) | Err(_)),
        "the stalled client was closed"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn a_fetch_answer_is_capped_but_always_carries_the_first_batch() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::{BrokerId, FetchRequest};
    let mut config = config();
    // Smaller than one batch: the first batch goes whole, and no other partition adds to the answer.
    config.max_fetch_bytes = 1;
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "big",
            openduct_storage::TopicConfig {
                partitions: 2,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, addr) = start_with(config, store).await;
    let client = ClientBuilder::new(vec![addr.clone()]).build().await.unwrap();
    for p in 0..2 {
        let pc = client
            .partition_client("big", p, UnknownTopicHandling::Error)
            .await
            .unwrap();
        pc.produce(vec![record("x"), record("y")], RsCompression::NoCompression)
            .await
            .unwrap();
    }
    let partition = |p: i32| {
        FetchPartition::default()
            .with_partition(p)
            .with_fetch_offset(0)
            .with_partition_max_bytes(1024 * 1024)
    };
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(50 * 1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(s("big")))
                .with_partitions(vec![partition(0), partition(1)]),
        ]);
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    let answer = c.send(&request, 12).await.unwrap();
    let parts = &answer.responses[0].partitions;
    assert!(
        parts[0].records.as_ref().is_some_and(|r| !r.is_empty()),
        "the first batch, whole"
    );
    assert!(
        parts[1].records.as_ref().is_some_and(|r| r.is_empty()),
        "the answer is full"
    );
    assert_eq!(
        parts[1].high_watermark, 2,
        "but the partition's end is still reported"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn a_waiting_fetch_does_not_hold_up_a_stop() {
    let (broker, addr) = start(Arc::new(MemoryStore::new())).await;
    let client = ClientBuilder::new(vec![addr]).build().await.unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic("quiet", 1, 1, 5_000)
        .await
        .unwrap();
    let reader = client
        .partition_client("quiet", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let _waiting = tokio::spawn(async move { reader.fetch_records(0, 1..1_000_000, 25_000).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    broker.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "stopped without waiting out the fetch"
    );
}

#[tokio::test]
async fn a_configuration_that_could_never_serve_a_request_is_refused() {
    let mut config = config();
    config.max_request_memory = 1024;
    config.max_request_bytes = 4096;
    let refused = Broker::start(config, Arc::new(MemoryStore::new())).await;
    assert!(matches!(refused, Err(openduct_broker::BrokerError::Config(_))));
}

#[tokio::test]
async fn a_later_partitions_batch_that_would_overfill_the_answer_waits_for_the_next_fetch() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::{BrokerId, FetchRequest};
    let mut config = config();
    config.max_fetch_bytes = 1_000;
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "mixed",
            openduct_storage::TopicConfig {
                partitions: 2,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, addr) = start_with(config, store).await;
    let client = ClientBuilder::new(vec![addr.clone()]).build().await.unwrap();
    let small = client
        .partition_client("mixed", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    small
        .produce(vec![record("small")], RsCompression::NoCompression)
        .await
        .unwrap();
    let large = client
        .partition_client("mixed", 1, UnknownTopicHandling::Error)
        .await
        .unwrap();
    large
        .produce(vec![record(&"x".repeat(10_000))], RsCompression::NoCompression)
        .await
        .unwrap();
    let partition = |p: i32| {
        FetchPartition::default()
            .with_partition(p)
            .with_fetch_offset(0)
            .with_partition_max_bytes(1024 * 1024)
    };
    let fetch = |order: Vec<i32>| {
        FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_max_bytes(50 * 1024 * 1024)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(TopicName(s("mixed")))
                    .with_partitions(order.into_iter().map(partition).collect()),
            ])
    };
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    let answer = c.send(&fetch(vec![0, 1]), 12).await.unwrap();
    let parts = &answer.responses[0].partitions;
    assert!(
        parts[0]
            .records
            .as_ref()
            .is_some_and(|r| !r.is_empty() && r.len() < 1_000)
    );
    assert!(
        parts[1].records.as_ref().is_some_and(|r| r.is_empty()),
        "10 kB would overfill a 1 kB answer"
    );
    assert_eq!(parts[1].high_watermark, 1);
    // Asked first, the large batch goes whole: a consumer is never stuck behind it.
    let answer = c.send(&fetch(vec![1]), 12).await.unwrap();
    assert!(
        answer.responses[0].partitions[0]
            .records
            .as_ref()
            .is_some_and(|r| r.len() > 10_000)
    );
    broker.shutdown().await;
}

/// A batch of `values` as a producer that is not idempotent sends it, claiming `epoch` as its leader epoch.
fn batch_claiming_epoch(values: &[&str], epoch: i32) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: epoch,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(i).unwrap(),
            sequence: -1,
            timestamp: 1_700_000_000_000,
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    buf.freeze()
}

fn epoch_request(topic: &str, current: i32, epoch: i32) -> OffsetForLeaderEpochRequest {
    OffsetForLeaderEpochRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId(-1))
        .with_topics(vec![
            OffsetForLeaderTopic::default()
                .with_topic(TopicName(s(topic)))
                .with_partitions(vec![
                    OffsetForLeaderPartition::default()
                        .with_partition(0)
                        .with_current_leader_epoch(current)
                        .with_leader_epoch(epoch),
                ]),
        ])
}

#[tokio::test]
async fn batches_carry_the_leaders_epoch_and_a_consumer_can_check_its_position_against_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileStore::open(dir.path(), FileStoreConfig::default()).unwrap());
    store
        .create_topic(
            "positions",
            openduct_storage::TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, addr) = start(store).await;
    let mut c = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    let versions = c.api_versions().await.unwrap();
    let api = versions.api_keys.iter().find(|k| k.api_key == 23).unwrap();
    assert_eq!((api.min_version, api.max_version), (2, 4));

    // The producer claims epoch 7; the broker stores the batch in its own epoch, as a Kafka leader does.
    let stored = c
        .send(&produce("positions", batch_claiming_epoch(&["a", "b"], 7)), 9)
        .await
        .unwrap();
    assert_eq!(stored.responses[0].partition_responses[0].error_code, 0);
    let fetched = c.fetch("positions", &[(0, 0)], 0, 1_000_000).await.unwrap();
    let epoch = i32::from_be_bytes(fetched[0].batches[12..16].try_into().unwrap());
    assert_eq!(epoch, 0, "a broker on its own writes in epoch 0");

    // ListOffsets answers the epoch beside the offset.
    let listed = c
        .send(
            &ListOffsetsRequest::default()
                .with_replica_id(kafka_protocol::messages::BrokerId(-1))
                .with_topics(vec![
                    ListOffsetsTopic::default()
                        .with_name(TopicName(s("positions")))
                        .with_partitions(vec![ListOffsetsPartition::default().with_timestamp(-1)]),
                ]),
            7,
        )
        .await
        .unwrap();
    let p = &listed.topics[0].partitions[0];
    assert_eq!((p.error_code, p.offset, p.leader_epoch), (0, 2, 0));

    // Where epoch 0 ended: it is the current one, so at the end.
    for version in 2..=4 {
        let answer = c.send(&epoch_request("positions", 0, 0), version).await.unwrap();
        let e = &answer.topics[0].partitions[0];
        assert_eq!(
            (e.error_code, e.leader_epoch, e.end_offset),
            (0, 0, 2),
            "v{version}"
        );
    }
    // A later epoch than the broker's is unknown; a client that believes the leader is further on asks again.
    let answer = c.send(&epoch_request("positions", -1, 1), 4).await.unwrap();
    let e = &answer.topics[0].partitions[0];
    assert_eq!((e.error_code, e.leader_epoch, e.end_offset), (0, -1, -1));
    let answer = c.send(&epoch_request("positions", 3, 0), 4).await.unwrap();
    assert_eq!(
        answer.topics[0].partitions[0].error_code, 76,
        "UNKNOWN_LEADER_EPOCH"
    );
    let answer = c.send(&epoch_request("nowhere", 0, 0), 4).await.unwrap();
    assert_eq!(
        answer.topics[0].partitions[0].error_code, 3,
        "UNKNOWN_TOPIC_OR_PARTITION"
    );
    broker.shutdown().await;
}
