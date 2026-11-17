// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CreatePartitions and DeleteRecords over the wire: a topic grown, the new partitions written and read, records
//! deleted from the front, every refusal with its reason - and both kept by the file store across a restart.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsTopic,
};
use kafka_protocol::messages::{BrokerId, CreatePartitionsRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use openduct_broker::client::{Client, Outgoing};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{FileStore, FileStoreConfig, Store, TopicConfig};

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

/// A topic to grow: its name, the count asked for, and the brokers of each new partition, if named.
type Growth<'a> = (&'a str, i32, Option<Vec<Vec<i32>>>);

fn grow(topics: &[Growth<'_>], validate_only: bool) -> CreatePartitionsRequest {
    CreatePartitionsRequest::default()
        .with_timeout_ms(5_000)
        .with_validate_only(validate_only)
        .with_topics(
            topics
                .iter()
                .map(|(name, count, assignment)| {
                    CreatePartitionsTopic::default()
                        .with_name(TopicName(s(name)))
                        .with_count(*count)
                        .with_assignments(assignment.as_ref().map(|a| {
                            a.iter()
                                .map(|b| {
                                    CreatePartitionsAssignment::default()
                                        .with_broker_ids(b.iter().map(|id| BrokerId(*id)).collect())
                                })
                                .collect()
                        }))
                })
                .collect(),
        )
}

async fn start(dir: &std::path::Path) -> (Broker, Client) {
    let store = Arc::new(FileStore::open(dir, FileStoreConfig::default()).unwrap());
    if store.topic("orders").is_none() {
        store
            .create_topic(
                "orders",
                TopicConfig {
                    partitions: 2,
                    retention_ms: None,
                },
            )
            .unwrap();
    }
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    let broker = Broker::start(config, store).await.unwrap();
    let client = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    (broker, client)
}

async fn partitions_of(c: &mut Client, topic: &str) -> usize {
    c.topics()
        .await
        .unwrap()
        .into_iter()
        .find(|t| t.name == topic)
        .map_or(0, |t| t.partitions)
}

#[tokio::test]
async fn a_topic_grows_its_new_partitions_take_writes_and_the_growth_lasts() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, mut c) = start(dir.path()).await;
    for version in 0..=3 {
        let count = 3 + i32::from(version);
        let res = c
            .send(&grow(&[("orders", count, None)], false), version)
            .await
            .unwrap();
        assert_eq!(
            res.results[0].error_code, 0,
            "v{version}: {:?}",
            res.results[0].error_message
        );
        assert_eq!(
            partitions_of(&mut c, "orders").await,
            usize::try_from(count).unwrap()
        );
    }
    // The newest partition takes a write and gives it back.
    let first = c
        .produce(
            "orders",
            5,
            &[Outgoing {
                key: None,
                value: Some(bytes::Bytes::from_static(b"new")),
                headers: Vec::new(),
            }],
            1_763_280_000_000,
        )
        .await
        .unwrap();
    assert_eq!(first, 0, "a new partition starts empty");
    let read = c.fetch("orders", &[(5, 0)], 0, 1_000_000).await.unwrap();
    assert_eq!(read[0].error_code, 0);
    assert!(!read[0].batches.is_empty());

    // Only checked, and an assignment naming this broker: fine. Each refusal with its reason.
    let checked = c.send(&grow(&[("orders", 9, None)], true), 3).await.unwrap();
    assert_eq!(checked.results[0].error_code, 0);
    assert_eq!(partitions_of(&mut c, "orders").await, 6, "only checked");
    let mine = c
        .send(&grow(&[("orders", 7, Some(vec![vec![0]]))], false), 3)
        .await
        .unwrap();
    assert_eq!(mine.results[0].error_code, 0);
    for (topics, code, says) in [
        (vec![("orders", 7, None)], 37, "never taken away"),
        (vec![("orders", 2, None)], 37, "never taken away"),
        (vec![("orders", 100_000, None)], 37, "between 1 and"),
        (
            vec![("orders", 9, Some(vec![vec![0]]))],
            39,
            "2 partitions are added but 1",
        ),
        (vec![("orders", 8, Some(vec![vec![4]]))], 39, "on its own"),
        (vec![("nowhere", 3, None)], 3, "no topic"),
    ] {
        let res = c.send(&grow(&topics, false), 3).await.unwrap();
        let r = &res.results[0];
        assert_eq!(r.error_code, code, "{topics:?}");
        let message = r.error_message.as_ref().unwrap().to_string();
        assert!(message.contains(says), "{topics:?}: {message}");
    }
    let twice = c
        .send(&grow(&[("orders", 8, None), ("orders", 9, None)], false), 3)
        .await
        .unwrap();
    assert_eq!(twice.results[1].error_code, 42, "a topic named twice");
    assert_eq!(
        partitions_of(&mut c, "orders").await,
        8,
        "the first of the two applied"
    );
    broker.shutdown().await;

    // After a restart: the partitions, and what was written to the new one.
    let (broker, mut c) = start(dir.path()).await;
    assert_eq!(partitions_of(&mut c, "orders").await, 8);
    let read = c.fetch("orders", &[(5, 0)], 0, 1_000_000).await.unwrap();
    assert!(!read[0].batches.is_empty());
    broker.shutdown().await;
}

fn delete_records(partitions: &[(i32, i64)]) -> kafka_protocol::messages::DeleteRecordsRequest {
    use kafka_protocol::messages::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
    kafka_protocol::messages::DeleteRecordsRequest::default()
        .with_timeout_ms(5_000)
        .with_topics(vec![
            DeleteRecordsTopic::default()
                .with_name(TopicName(s("orders")))
                .with_partitions(
                    partitions
                        .iter()
                        .map(|(p, offset)| {
                            DeleteRecordsPartition::default()
                                .with_partition_index(*p)
                                .with_offset(*offset)
                        })
                        .collect(),
                ),
        ])
}

async fn earliest(c: &mut Client, partition: i32) -> i64 {
    c.list_offset("orders", partition, openduct_broker::client::EARLIEST)
        .await
        .unwrap()
}

#[tokio::test]
async fn records_are_deleted_from_the_front_and_stay_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, mut c) = start(dir.path()).await;
    let message = || Outgoing {
        key: None,
        value: Some(bytes::Bytes::from_static(b"x")),
        headers: Vec::new(),
    };
    for _ in 0..5 {
        c.produce("orders", 0, &[message(), message()], 1_763_280_000_000)
            .await
            .unwrap();
    }
    for version in 0..=2 {
        let target = 2 + 2 * i64::from(version);
        let res = c.send(&delete_records(&[(0, target)]), version).await.unwrap();
        let p = &res.topics[0].partitions[0];
        assert_eq!((p.error_code, p.low_watermark), (0, target), "v{version}");
        assert_eq!(earliest(&mut c, 0).await, target);
    }
    let below = c.fetch("orders", &[(0, 3)], 0, 1_000_000).await.unwrap();
    assert_eq!(below[0].error_code, 1, "OFFSET_OUT_OF_RANGE: deleted");
    let from = c.fetch("orders", &[(0, 6)], 0, 1_000_000).await.unwrap();
    assert_eq!(from[0].error_code, 0);

    // Back before the start: nothing moves; past the end, a partition that does not exist: refused; -1: the end.
    let res = c
        .send(&delete_records(&[(0, 1), (1, 99), (7, 0)]), 2)
        .await
        .unwrap();
    let answers: Vec<(i16, i64)> = res.topics[0]
        .partitions
        .iter()
        .map(|p| (p.error_code, p.low_watermark))
        .collect();
    assert_eq!(answers, vec![(0, 6), (1, -1), (3, -1)]);
    let all = c.send(&delete_records(&[(0, -1)]), 2).await.unwrap();
    assert_eq!(all.topics[0].partitions[0].low_watermark, 10);
    broker.shutdown().await;

    let (broker, mut c) = start(dir.path()).await;
    assert_eq!(earliest(&mut c, 0).await, 10, "deleted for good");
    let next = c
        .produce("orders", 0, &[message()], 1_763_280_000_000)
        .await
        .unwrap();
    assert_eq!(next, 10, "offsets go on from where they were");
    broker.shutdown().await;
}
