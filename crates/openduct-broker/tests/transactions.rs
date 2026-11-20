// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Transactions as a client sees them in a partition's log: a consumer that reads only committed data
//! (`isolation.level=read_committed`) is given nothing from a transaction still open, and is told which
//! transactions were aborted; one that reads everything sees every batch. The transactions here are written
//! straight into the store, as the transaction coordinator will write them.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, BrokerId, EndTxnRequest, FetchRequest, FindCoordinatorRequest,
    InitProducerIdRequest, ListOffsetsRequest, ProduceRequest, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use openduct_broker::client::Client;
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::batch::Marker;
use openduct_storage::{FileStore, FileStoreConfig, MemoryStore, Store, TopicConfig};

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

/// A batch of these values from `producer`, part of a transaction or not, as the Kafka protocol library encodes
/// it.
fn batch(producer: i64, sequence: i32, transactional: bool, values: &[&str]) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: producer,
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

/// What a fetch from `offset` gives: (values of the data records, last stable offset, high watermark, the
/// aborted transactions named).
async fn fetch(c: &mut Client, offset: i64, isolation: i8) -> (Vec<String>, i64, i64, Vec<(i64, i64)>) {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(0)
        .with_max_bytes(1 << 20)
        .with_isolation_level(isolation)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(s("t")))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);
    let answer = c.send(&request, 11).await.unwrap();
    let part = &answer.responses[0].partitions[0];
    assert_eq!(part.error_code, 0);
    let aborted: Vec<(i64, i64)> = part
        .aborted_transactions
        .iter()
        .flatten()
        .map(|t| (t.producer_id.0, t.first_offset))
        .collect();
    // As a client applies them: the records of an aborted transaction, from its first offset to its marker, are
    // skipped; markers are never data.
    let mut skipping: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    let mut values = Vec::new();
    let mut batches = part.records.clone().unwrap_or_default();
    while !batches.is_empty() {
        let set = RecordBatchDecoder::decode(&mut batches).unwrap();
        for r in set.records {
            if aborted
                .iter()
                .any(|(pid, first)| *pid == r.producer_id && *first == r.offset)
            {
                skipping.insert(r.producer_id);
            }
            if r.control {
                skipping.remove(&r.producer_id);
                continue;
            }
            if r.transactional && skipping.contains(&r.producer_id) {
                continue;
            }
            values.push(String::from_utf8(r.value.unwrap().to_vec()).unwrap());
        }
    }
    (values, part.last_stable_offset, part.high_watermark, aborted)
}

#[tokio::test]
async fn a_consumer_of_committed_data_waits_for_open_transactions_and_skips_aborted_ones() {
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    let broker = Broker::start(config, store.clone()).await.unwrap();
    let mut c = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    const ALL: i8 = 0;
    const COMMITTED: i8 = 1;

    // Plain data, then producer 7 opens a transaction, then more plain data after it.
    store.append("t", 0, batch(-1, -1, false, &["plain-1"])).unwrap();
    store
        .append("t", 0, batch(7, 0, true, &["seven-a", "seven-b"]))
        .unwrap();
    store.append("t", 0, batch(-1, -1, false, &["plain-2"])).unwrap();
    // Everything is there for a consumer that reads everything; one that reads committed data stops at the
    // open transaction - plain-2, written after it, waits too.
    let (values, stable, end, _) = fetch(&mut c, 0, ALL).await;
    assert_eq!(values, ["plain-1", "seven-a", "seven-b", "plain-2"]);
    assert_eq!((stable, end), (1, 4));
    let (values, stable, end, aborted) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!(values, ["plain-1"]);
    assert_eq!((stable, end, aborted.len()), (1, 4, 0));

    // Producer 9 writes a transaction and commits it: still behind producer 7's.
    store.append("t", 0, batch(9, 0, true, &["nine"])).unwrap();
    store.end_transaction("t", 0, 9, 0, Marker::Commit, 0).unwrap();
    assert_eq!(fetch(&mut c, 0, COMMITTED).await.0, ["plain-1"]);

    // Producer 7 aborts. Now all of it is stable: its records are skipped, the committed ones delivered.
    store.end_transaction("t", 0, 7, 0, Marker::Abort, 0).unwrap();
    let (values, stable, end, aborted) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!(values, ["plain-1", "plain-2", "nine"]);
    assert_eq!((stable, end), (7, 7));
    assert_eq!(
        aborted,
        vec![(7, 1)],
        "the aborted transaction, by its producer and first offset"
    );
    // A consumer that reads everything is told of none, and sees them all.
    let (values, _, _, aborted) = fetch(&mut c, 0, ALL).await;
    assert_eq!(values, ["plain-1", "seven-a", "seven-b", "plain-2", "nine"]);
    assert!(aborted.is_empty());
    // From past the aborted transaction, there is nothing to skip.
    assert!(fetch(&mut c, 7, COMMITTED).await.3.is_empty());

    // The end, to each: with a transaction open again, a committed reader's end is the last stable offset.
    store.append("t", 0, batch(7, 2, true, &["again"])).unwrap();
    let end_for = |isolation: i8| {
        ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_isolation_level(isolation)
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(TopicName(s("t")))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-1),
                    ]),
            ])
    };
    let all = c.send(&end_for(ALL), 5).await.unwrap();
    let committed = c.send(&end_for(COMMITTED), 5).await.unwrap();
    assert_eq!(all.topics[0].partitions[0].offset, 8);
    assert_eq!(committed.topics[0].partitions[0].offset, 7);

    // A client cannot yet write a transaction itself: refused, with nothing stored.
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s("t")))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch(11, 0, true, &["mine"]))),
                ]),
        ]);
    let refused = c.send(&produce, 9).await.unwrap();
    let part = &refused.responses[0].partition_responses[0];
    assert_eq!(part.error_code, 42, "{:?}", part.error_message);
    assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 8);
    broker.shutdown().await;
}

/// A batch of these values, part of the transaction of this producer id and epoch.
fn in_transaction(producer: i64, epoch: i16, sequence: i32, values: &[&str]) -> Bytes {
    let mut b = batch(producer, sequence, true, values).to_vec();
    b[51..53].copy_from_slice(&epoch.to_be_bytes());
    let crc = openduct_storage::batch::checksum(&b);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(b)
}

/// A transactional producer, as a client drives the coordinator.
struct Producer {
    id: &'static str,
    producer_id: i64,
    epoch: i16,
}

impl Producer {
    async fn init(c: &mut Client, id: &'static str, timeout_ms: i32) -> Result<Self, i16> {
        let answer = c
            .send(
                &InitProducerIdRequest::default()
                    .with_transactional_id(Some(TransactionalId(s(id))))
                    .with_transaction_timeout_ms(timeout_ms)
                    .with_producer_id(ProducerId(-1))
                    .with_producer_epoch(-1),
                2,
            )
            .await
            .unwrap();
        match answer.error_code {
            0 => Ok(Self {
                id,
                producer_id: answer.producer_id.0,
                epoch: answer.producer_epoch,
            }),
            code => Err(code),
        }
    }

    async fn add(&self, c: &mut Client, topic: &str) -> i16 {
        let answer = c
            .send(
                &AddPartitionsToTxnRequest::default()
                    .with_v3_and_below_transactional_id(TransactionalId(s(self.id)))
                    .with_v3_and_below_producer_id(ProducerId(self.producer_id))
                    .with_v3_and_below_producer_epoch(self.epoch)
                    .with_v3_and_below_topics(vec![
                        AddPartitionsToTxnTopic::default()
                            .with_name(TopicName(s(topic)))
                            .with_partitions(vec![0]),
                    ]),
                1,
            )
            .await
            .unwrap();
        answer.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code
    }

    async fn write(&self, c: &mut Client, topic: &str, sequence: i32, values: &[&str]) -> i16 {
        let request = ProduceRequest::default()
            .with_transactional_id(Some(TransactionalId(s(self.id))))
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName(s(topic)))
                    .with_partition_data(vec![PartitionProduceData::default().with_index(0).with_records(
                        Some(in_transaction(self.producer_id, self.epoch, sequence, values)),
                    )]),
            ]);
        c.send(&request, 9).await.unwrap().responses[0].partition_responses[0].error_code
    }

    async fn end(&self, c: &mut Client, commit: bool) -> i16 {
        c.send(
            &EndTxnRequest::default()
                .with_transactional_id(TransactionalId(s(self.id)))
                .with_producer_id(ProducerId(self.producer_id))
                .with_producer_epoch(self.epoch)
                .with_committed(commit),
            1,
        )
        .await
        .unwrap()
        .error_code
    }
}

async fn start(store: Arc<dyn Store>) -> (Broker, Client) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    let broker = Broker::start(config, store).await.unwrap();
    let client = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    (broker, client)
}

#[tokio::test]
async fn a_producer_begins_writes_and_ends_a_transaction_through_the_coordinator() {
    const COMMITTED: i8 = 1;
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, mut c) = start(store.clone()).await;
    // The coordinator of a transactional id is this broker.
    let found = c
        .send(
            &FindCoordinatorRequest::default()
                .with_key(s("payments"))
                .with_key_type(1),
            2,
        )
        .await
        .unwrap();
    assert_eq!((found.error_code, found.node_id.0), (0, 0));

    let p = Producer::init(&mut c, "payments", 60_000).await.unwrap();
    assert_eq!(p.epoch, 0);
    // A partition is part of the transaction before it is written to.
    assert_eq!(p.add(&mut c, "t").await, 0);
    assert_eq!(p.add(&mut c, "nowhere").await, 3, "UNKNOWN_TOPIC_OR_PARTITION");
    assert_eq!(p.write(&mut c, "t", 0, &["one", "two"]).await, 0);
    assert_eq!(
        fetch(&mut c, 0, COMMITTED).await.0,
        Vec::<String>::new(),
        "not before it is committed"
    );
    assert_eq!(p.end(&mut c, true).await, 0);
    assert_eq!(fetch(&mut c, 0, COMMITTED).await.0, ["one", "two"]);

    // The next one is aborted: never seen. Ending what is not open: an abort is harmless, a commit a mistake.
    assert_eq!(p.add(&mut c, "t").await, 0);
    assert_eq!(p.write(&mut c, "t", 2, &["three"]).await, 0);
    assert_eq!(p.end(&mut c, false).await, 0);
    assert_eq!(fetch(&mut c, 0, COMMITTED).await.0, ["one", "two"]);
    assert_eq!(p.end(&mut c, false).await, 0);
    assert_eq!(p.end(&mut c, true).await, 48, "INVALID_TXN_STATE");

    // The id is initialised again - a producer restarted. It gets the next epoch; what its former self left
    // open is aborted; and its former self is refused everywhere, by the coordinator and by the partition.
    assert_eq!(p.add(&mut c, "t").await, 0);
    assert_eq!(p.write(&mut c, "t", 3, &["left-open"]).await, 0);
    let next = Producer::init(&mut c, "payments", 60_000).await.unwrap();
    assert_eq!((next.producer_id, next.epoch), (p.producer_id, 1));
    assert_eq!(p.add(&mut c, "t").await, 90, "PRODUCER_FENCED");
    assert_eq!(p.end(&mut c, true).await, 90);
    assert_eq!(
        p.write(&mut c, "t", 4, &["from-a-zombie"]).await,
        90,
        "the partition knows too"
    );
    assert_eq!(next.add(&mut c, "t").await, 0);
    assert_eq!(next.write(&mut c, "t", 0, &["four"]).await, 0);
    assert_eq!(next.end(&mut c, true).await, 0);
    let (values, stable, end, _) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!(values, ["one", "two", "four"]);
    assert_eq!(stable, end, "nothing is left open");

    // What the coordinator refuses: a producer id that is not the id's, a timeout out of range, no id.
    let stranger = Producer {
        id: "payments",
        producer_id: next.producer_id + 1,
        epoch: 1,
    };
    assert_eq!(stranger.add(&mut c, "t").await, 49, "INVALID_PRODUCER_ID_MAPPING");
    assert_eq!(
        Producer::init(&mut c, "other", 0).await.err(),
        Some(50),
        "INVALID_TRANSACTION_TIMEOUT"
    );
    assert_eq!(Producer::init(&mut c, "", 1_000).await.err(), Some(42));
    broker.shutdown().await;
}

#[tokio::test]
async fn a_transaction_left_open_is_aborted_at_its_timeout_even_across_a_restart() {
    const COMMITTED: i8 = 1;
    let dir = tempfile::tempdir().unwrap();
    let open =
        || -> Arc<dyn Store> { Arc::new(FileStore::open(dir.path(), FileStoreConfig::default()).unwrap()) };
    let committed;
    {
        let store = open();
        store
            .create_topic(
                "t",
                TopicConfig {
                    partitions: 1,
                    retention_ms: None,
                },
            )
            .unwrap();
        let (broker, mut c) = start(store).await;
        // One transaction committed, and another - of a producer with a short timeout - left open.
        let steady = Producer::init(&mut c, "steady", 60_000).await.unwrap();
        assert_eq!(steady.add(&mut c, "t").await, 0);
        assert_eq!(steady.write(&mut c, "t", 0, &["kept"]).await, 0);
        assert_eq!(steady.end(&mut c, true).await, 0);
        committed = steady;
        let flaky = Producer::init(&mut c, "flaky", 1_500).await.unwrap();
        assert_eq!(flaky.add(&mut c, "t").await, 0);
        assert_eq!(flaky.write(&mut c, "t", 0, &["lost"]).await, 0);
        let (values, stable, end, _) = fetch(&mut c, 0, COMMITTED).await;
        assert_eq!((values, stable < end), (vec!["kept".to_owned()], true));
        broker.shutdown().await;
    }
    // The broker starts again; the producer that left the transaction open never comes back. Its timeout still
    // runs out, and what it wrote is aborted - nothing is held back for good.
    let (broker, mut c) = start(open()).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (values, stable, end, aborted) = fetch(&mut c, 0, COMMITTED).await;
        if stable == end {
            assert_eq!(values, ["kept"]);
            assert_eq!(aborted.len(), 1);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the open transaction was never aborted"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // What the coordinator knew, it knows still: the committed producer goes on with its id and epoch, and the
    // one that timed out is told it was fenced.
    assert_eq!(committed.add(&mut c, "t").await, 0);
    assert_eq!(committed.write(&mut c, "t", 1, &["more"]).await, 0);
    assert_eq!(committed.end(&mut c, true).await, 0);
    let gone = Producer {
        id: "flaky",
        producer_id: committed.producer_id + 1,
        epoch: 0,
    };
    assert_eq!(gone.end(&mut c, true).await, 90, "PRODUCER_FENCED");
    assert_eq!(fetch(&mut c, 0, COMMITTED).await.0, ["kept", "more"]);
    broker.shutdown().await;
}

#[tokio::test]
async fn an_operator_lists_and_describes_transactions_and_a_partitions_producers() {
    use kafka_protocol::messages::{DescribeTransactionsRequest, ListTransactionsRequest};
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, mut c) = start(store.clone()).await;
    // Before any transactional producer came: nothing to list, and an id asked for is not found.
    let listed = c.send(&ListTransactionsRequest::default(), 0).await.unwrap();
    assert_eq!((listed.error_code, listed.transaction_states.len()), (0, 0));

    let idle = Producer::init(&mut c, "idle", 60_000).await.unwrap();
    let busy = Producer::init(&mut c, "busy", 45_000).await.unwrap();
    assert_eq!(busy.add(&mut c, "t").await, 0);
    assert_eq!(busy.write(&mut c, "t", 0, &["one", "two"]).await, 0);

    let names = |r: &kafka_protocol::messages::ListTransactionsResponse| -> Vec<(String, String)> {
        r.transaction_states
            .iter()
            .map(|t| (t.transactional_id.0.to_string(), t.transaction_state.to_string()))
            .collect()
    };
    let pair = |a: &str, b: &str| (a.to_owned(), b.to_owned());
    let listed = c.send(&ListTransactionsRequest::default(), 0).await.unwrap();
    assert_eq!(names(&listed), [pair("busy", "Ongoing"), pair("idle", "Empty")]);
    // By stage, by producer id, by how long it has been open - and a stage that does not exist is named back.
    let ongoing = ListTransactionsRequest::default().with_state_filters(vec![s("Ongoing"), s("Sleeping")]);
    let listed = c.send(&ongoing, 0).await.unwrap();
    assert_eq!(names(&listed), [pair("busy", "Ongoing")]);
    assert_eq!(listed.unknown_state_filters, [s("Sleeping")]);
    let by_producer =
        ListTransactionsRequest::default().with_producer_id_filters(vec![ProducerId(idle.producer_id)]);
    assert_eq!(
        names(&c.send(&by_producer, 0).await.unwrap()),
        [pair("idle", "Empty")]
    );
    let long_open = ListTransactionsRequest::default().with_duration_filter(3_600_000);
    assert_eq!(names(&c.send(&long_open, 1).await.unwrap()), []);
    // By a pattern the whole id matches (version 2); one that is no pattern is refused.
    let by_pattern =
        |p: &'static str| ListTransactionsRequest::default().with_transactional_id_pattern(Some(s(p)));
    assert_eq!(
        names(&c.send(&by_pattern("bu.*"), 2).await.unwrap()),
        [pair("busy", "Ongoing")]
    );
    assert_eq!(names(&c.send(&by_pattern("us"), 2).await.unwrap()), []);
    assert_eq!(names(&c.send(&by_pattern(".*"), 2).await.unwrap()).len(), 2);
    assert_eq!(c.send(&by_pattern("bu(sy"), 2).await.unwrap().error_code, 128);
    let open_at_all = ListTransactionsRequest::default().with_duration_filter(0);
    assert_eq!(
        names(&c.send(&open_at_all, 1).await.unwrap()),
        [pair("busy", "Ongoing")]
    );

    let described = c
        .send(
            &DescribeTransactionsRequest::default().with_transactional_ids(vec![
                TransactionalId(s("busy")),
                TransactionalId(s("idle")),
                TransactionalId(s("nobody")),
            ]),
            0,
        )
        .await
        .unwrap();
    let [b, i, n] = &described.transaction_states[..] else {
        panic!("three answers");
    };
    assert_eq!(
        (
            b.error_code,
            b.transaction_state.as_str(),
            b.transaction_timeout_ms
        ),
        (0, "Ongoing", 45_000)
    );
    assert_eq!((b.producer_id.0, b.producer_epoch), (busy.producer_id, 0));
    assert!(b.transaction_start_time_ms > 0);
    assert_eq!(b.topics.len(), 1);
    assert_eq!(
        (b.topics[0].topic.0.as_str(), &b.topics[0].partitions[..]),
        ("t", &[0][..])
    );
    assert_eq!(
        (
            i.error_code,
            i.transaction_state.as_str(),
            i.transaction_start_time_ms,
            i.topics.len()
        ),
        (0, "Empty", -1, 0)
    );
    assert_eq!(n.error_code, 105, "TRANSACTIONAL_ID_NOT_FOUND");

    // The partition: its one producer, the last sequence it wrote, and where its open transaction starts.
    let answer = partition_producers(&mut c).await;
    let parts = &answer.topics[0].partitions;
    assert_eq!(parts[0].error_code, 0);
    let [p] = &parts[0].active_producers[..] else {
        panic!("one producer");
    };
    assert_eq!(
        (
            p.producer_id.0,
            p.producer_epoch,
            p.last_sequence,
            p.current_txn_start_offset
        ),
        (busy.producer_id, 0, 1, 0)
    );
    assert!(p.last_timestamp > 0);
    assert_eq!(parts[1].error_code, 3, "UNKNOWN_TOPIC_OR_PARTITION");
    // Committed: the producer is still known, with no transaction open.
    assert_eq!(busy.end(&mut c, true).await, 0);
    let answer = partition_producers(&mut c).await;
    assert_eq!(
        answer.topics[0].partitions[0].active_producers[0].current_txn_start_offset,
        -1
    );
    let listed = c.send(&ListTransactionsRequest::default(), 0).await.unwrap();
    assert_eq!(names(&listed), [pair("busy", "Empty"), pair("idle", "Empty")]);
    broker.shutdown().await;
}

async fn partition_producers(c: &mut Client) -> kafka_protocol::messages::DescribeProducersResponse {
    use kafka_protocol::messages::describe_producers_request::TopicRequest;
    let request = kafka_protocol::messages::DescribeProducersRequest::default().with_topics(vec![
        TopicRequest::default()
            .with_name(TopicName(s("t")))
            .with_partition_indexes(vec![0, 7]),
    ]);
    c.send(&request, 0).await.unwrap()
}

#[tokio::test]
async fn an_operator_aborts_a_transaction_no_coordinator_will_end() {
    use kafka_protocol::messages::WriteTxnMarkersRequest;
    use kafka_protocol::messages::write_txn_markers_request::{WritableTxnMarker, WritableTxnMarkerTopic};
    const COMMITTED: i8 = 1;
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, mut c) = start(store.clone()).await;
    // A producer wrote in a transaction and added the partition to none: no coordinator will ever end it, and
    // everything written after it is held back from consumers of committed data.
    let hanging = Producer {
        id: "hanging",
        producer_id: 4_242,
        epoch: 3,
    };
    assert_eq!(hanging.write(&mut c, "t", 0, &["never-decided"]).await, 0);
    let plain = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(s("t")))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch(-1, 0, false, &["waiting"]))),
                ]),
        ]);
    assert_eq!(
        c.send(&plain, 9).await.unwrap().responses[0].partition_responses[0].error_code,
        0
    );
    let (values, stable, end, _) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!((values.len(), stable, end), (0, 0, 2));
    // What the operator's tool sees, and sends: the producer and epoch of the open transaction, aborted.
    let seen = partition_producers(&mut c).await;
    let p = &seen.topics[0].partitions[0].active_producers[0];
    assert_eq!(
        (p.producer_id.0, p.producer_epoch, p.current_txn_start_offset),
        (4_242, 3, 0)
    );
    let abort = |partitions: Vec<i32>| {
        WriteTxnMarkersRequest::default().with_markers(vec![
            WritableTxnMarker::default()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(3)
                .with_transaction_result(false)
                .with_coordinator_epoch(0)
                .with_topics(vec![
                    WritableTxnMarkerTopic::default()
                        .with_name(TopicName(s("t")))
                        .with_partition_indexes(partitions),
                ]),
        ])
    };
    let answer = c.send(&abort(vec![0, 9]), 1).await.unwrap();
    let parts = &answer.markers[0].topics[0].partitions;
    assert_eq!((parts[0].error_code, parts[1].error_code), (0, 3));
    let (values, stable, end, aborted) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!(values, ["waiting"]);
    assert_eq!((stable, end, aborted.len()), (3, 3, 1));
    let seen = partition_producers(&mut c).await;
    assert_eq!(
        seen.topics[0].partitions[0].active_producers[0].current_txn_start_offset,
        -1
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn a_broker_asks_whether_partitions_are_in_a_transaction_and_adds_to_several_at_once() {
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTransaction;
    let store = Arc::new(MemoryStore::new());
    for topic in ["t", "u"] {
        store
            .create_topic(
                topic,
                TopicConfig {
                    partitions: 1,
                    retention_ms: None,
                },
            )
            .unwrap();
    }
    let (broker, mut c) = start(store.clone()).await;
    let first = Producer::init(&mut c, "first", 60_000).await.unwrap();
    let second = Producer::init(&mut c, "second", 60_000).await.unwrap();
    let one = |p: &Producer, topic: &str, verify_only: bool| {
        AddPartitionsToTxnTransaction::default()
            .with_transactional_id(TransactionalId(s(p.id)))
            .with_producer_id(ProducerId(p.producer_id))
            .with_producer_epoch(p.epoch)
            .with_verify_only(verify_only)
            .with_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(TopicName(s(topic)))
                    .with_partitions(vec![0]),
            ])
    };
    // What each transaction of the answer says of its one partition.
    let codes = |answer: &kafka_protocol::messages::AddPartitionsToTxnResponse| -> Vec<(String, i16)> {
        answer
            .results_by_transaction
            .iter()
            .map(|r| {
                (
                    r.transactional_id.0.to_string(),
                    r.topic_results[0].results_by_partition[0].partition_error_code,
                )
            })
            .collect()
    };
    let pair = |id: &str, code: i16| (id.to_owned(), code);
    // Nothing is open yet: asked about, no partition is in a transaction.
    let ask = AddPartitionsToTxnRequest::default()
        .with_transactions(vec![one(&first, "t", true), one(&second, "u", true)]);
    let answer = c.send(&ask, 4).await.unwrap();
    assert_eq!(
        codes(&answer),
        [pair("first", 48), pair("second", 48)],
        "INVALID_TXN_STATE"
    );
    // Two transactions added to in one request.
    let add = AddPartitionsToTxnRequest::default()
        .with_transactions(vec![one(&first, "t", false), one(&second, "u", false)]);
    assert_eq!(
        codes(&c.send(&add, 4).await.unwrap()),
        [pair("first", 0), pair("second", 0)]
    );
    // Asked again: each one's own partition is in; the other's is not; and an epoch that is not the id's is
    // refused (INVALID_PRODUCER_EPOCH).
    let ask = AddPartitionsToTxnRequest::default().with_transactions(vec![
        one(&first, "t", true),
        one(&second, "t", true),
        one(
            &Producer {
                id: "first",
                producer_id: first.producer_id,
                epoch: 7,
            },
            "t",
            true,
        ),
    ]);
    let answer = c.send(&ask, 5).await.unwrap();
    assert_eq!(
        codes(&answer),
        [pair("first", 0), pair("second", 48), pair("first", 47)]
    );
    // Ended: nothing is in a transaction any more. (EndTxn version 4 is version 3 with one more error to give.)
    let end = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(s("first")))
        .with_producer_id(ProducerId(first.producer_id))
        .with_producer_epoch(first.epoch)
        .with_committed(true);
    assert_eq!(c.send(&end, 4).await.unwrap().error_code, 0);
    let ask = AddPartitionsToTxnRequest::default().with_transactions(vec![one(&first, "t", true)]);
    assert_eq!(codes(&c.send(&ask, 4).await.unwrap()), [pair("first", 48)]);
    broker.shutdown().await;
}

#[tokio::test]
async fn a_transaction_no_coordinator_will_end_is_aborted_by_the_partition_itself() {
    const COMMITTED: i8 = 1;
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    // (A quarter of an hour and more in a broker as it runs.)
    config.hanging_transaction_after = Duration::from_secs(3);
    let store: Arc<dyn Store> = store;
    let broker = Broker::start(config, store).await.unwrap();
    let mut c = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    // A producer writes in a transaction it never told a coordinator of, and is gone.
    let stray = Producer {
        id: "stray",
        producer_id: 9_001,
        epoch: 0,
    };
    assert_eq!(stray.write(&mut c, "t", 0, &["never-decided"]).await, 0);
    // One a coordinator knows is open beside it, and is left alone: it is its coordinator's to end.
    let known = Producer::init(&mut c, "known", 1_000).await.unwrap();
    assert_eq!(known.add(&mut c, "t").await, 0);
    assert_eq!(known.write(&mut c, "t", 0, &["decided"]).await, 0);
    assert_eq!(known.end(&mut c, true).await, 0);
    let (values, stable, end, _) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!(
        (values.len(), stable < end),
        (0, true),
        "held back by the stray transaction"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (values, stable, end, aborted) = fetch(&mut c, 0, COMMITTED).await;
        if stable == end {
            assert_eq!(values, ["decided"]);
            assert_eq!(aborted, [(9_001, 0)]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the stray transaction was never aborted"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    broker.shutdown().await;
}

/// Not a test: leaves a transaction no coordinator knows of on a running broker, for the proof with Kafka's
/// `kafka-transactions find-hanging` (tests/clients/README.md).
///
///     OPENDUCT_STRAY=127.0.0.1:19395 OPENDUCT_STRAY_TOPIC=t cargo test -p openduct-broker --test transactions \
///         leave_a_stray_transaction -- --ignored
#[tokio::test]
#[ignore = "a helper for a proof by hand: it needs a running broker"]
async fn leave_a_stray_transaction() {
    let address = std::env::var("OPENDUCT_STRAY").expect("OPENDUCT_STRAY=<host:port>");
    let topic = std::env::var("OPENDUCT_STRAY_TOPIC").expect("OPENDUCT_STRAY_TOPIC=<topic>");
    let mut c = Client::connect(&address, Duration::from_secs(10)).await.unwrap();
    let stray = Producer {
        id: "stray",
        producer_id: 9_001,
        epoch: 0,
    };
    assert_eq!(stray.write(&mut c, &topic, 0, &["never-decided"]).await, 0);
}

#[tokio::test]
async fn the_second_transaction_protocol_adds_partitions_by_their_first_record_and_raises_the_epoch_at_every_end()
 {
    const COMMITTED: i8 = 1;
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: None,
            },
        )
        .unwrap();
    let (broker, mut c) = start(store.clone()).await;
    let mut p = Producer::init(&mut c, "second", 60_000).await.unwrap();
    // Produce 12, and no AddPartitionsToTxn: the partition's leader tells the coordinator.
    async fn write12(c: &mut Client, p: &Producer, sequence: i32, values: &[&str]) -> i16 {
        let request = ProduceRequest::default()
            .with_transactional_id(Some(TransactionalId(s(p.id))))
            .with_acks(-1)
            .with_timeout_ms(5_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName(s("t")))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(in_transaction(p.producer_id, p.epoch, sequence, values))),
                    ]),
            ]);
        c.send(&request, 12).await.unwrap().responses[0].partition_responses[0].error_code
    }
    // EndTxn 5: (error, the producer id and epoch to go on with).
    async fn end5(c: &mut Client, p: &Producer, commit: bool) -> (i16, i64, i16) {
        let answer = c
            .send(
                &EndTxnRequest::default()
                    .with_transactional_id(TransactionalId(s(p.id)))
                    .with_producer_id(ProducerId(p.producer_id))
                    .with_producer_epoch(p.epoch)
                    .with_committed(commit),
                5,
            )
            .await
            .unwrap();
        (answer.error_code, answer.producer_id.0, answer.producer_epoch)
    }
    assert_eq!(write12(&mut c, &p, 0, &["one"]).await, 0);
    assert_eq!(write12(&mut c, &p, 1, &["two"]).await, 0);
    assert_eq!(fetch(&mut c, 0, COMMITTED).await.0, Vec::<String>::new());
    assert_eq!(
        end5(&mut c, &p, true).await,
        (0, p.producer_id, 1),
        "the next epoch"
    );
    assert_eq!(fetch(&mut c, 0, COMMITTED).await.0, ["one", "two"]);
    // The answer was lost and the producer asks again, with the epoch it still has: answered the same.
    assert_eq!(end5(&mut c, &p, true).await, (0, p.producer_id, 1));
    // The other decision for that transaction, and anything else from the old epoch, is refused.
    assert_eq!(end5(&mut c, &p, false).await.0, 90, "PRODUCER_FENCED");
    assert_eq!(
        write12(&mut c, &p, 2, &["late"]).await,
        90,
        "the partition knows the new epoch too"
    );
    // On with the new epoch: aborted this time.
    p.epoch = 1;
    assert_eq!(write12(&mut c, &p, 0, &["three"]).await, 0);
    assert_eq!(end5(&mut c, &p, false).await, (0, p.producer_id, 2));
    let (values, stable, end, aborted) = fetch(&mut c, 0, COMMITTED).await;
    assert_eq!(values, ["one", "two"]);
    assert_eq!((stable, aborted.len()), (end, 1));
    // Nothing open: an abort is harmless and changes nothing; a commit is a mistake.
    p.epoch = 2;
    assert_eq!(end5(&mut c, &p, false).await, (0, p.producer_id, 2));
    assert_eq!(end5(&mut c, &p, true).await.0, 48, "INVALID_TXN_STATE");
    // A transaction of that protocol left open is timed out like any other: the coordinator knows of it.
    let listed = c
        .send(&kafka_protocol::messages::ListTransactionsRequest::default(), 0)
        .await
        .unwrap();
    assert_eq!(listed.transaction_states[0].transaction_state.as_str(), "Empty");
    assert_eq!(write12(&mut c, &p, 0, &["four"]).await, 0);
    let listed = c
        .send(&kafka_protocol::messages::ListTransactionsRequest::default(), 0)
        .await
        .unwrap();
    assert_eq!(listed.transaction_states[0].transaction_state.as_str(), "Ongoing");
    broker.shutdown().await;
}
