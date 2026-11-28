// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The routing client against one broker: every request finds that broker as the leader and the coordinator, so the
//! calls work as the plain client's do. (Against a cluster, crates/openduct/tests/studio_cluster.rs.)

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openduct_broker::client::{Client, EARLIEST, Outgoing, Routed};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

#[tokio::test]
async fn one_broker_is_the_leader_and_the_coordinator_of_everything() {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let mut client = Routed::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    client.create_topic("events", 3, None).await.unwrap();
    assert_eq!(client.partition_count("events").await.unwrap(), 3);
    for p in 0..3 {
        let message = Outgoing {
            key: None,
            value: Some(Bytes::from(format!("m-{p}").into_bytes())),
            headers: Vec::new(),
        };
        assert_eq!(client.produce("events", p, &[message], 0).await.unwrap(), 0);
    }
    let fetched = client
        .fetch("events", &[(0, 0), (1, 0), (2, 0)], 0, 1 << 20)
        .await
        .unwrap();
    assert_eq!(fetched.len(), 3);
    assert!(fetched.iter().all(|f| f.error_code == 0 && f.high_watermark == 1));
    assert_eq!(client.list_offset("events", 2, EARLIEST).await.unwrap(), 0);

    let member = client
        .join_group("readers", "events", 3, 10_000, None)
        .await
        .unwrap();
    assert_eq!(member.partitions, vec![0, 1, 2]);
    client.commit_member(&member, &[(0, 1), (1, 1)]).await.unwrap();
    let committed = client.committed("readers", "events", &[0, 1, 2]).await.unwrap();
    assert!(
        committed.contains(&(0, 1)) && committed.contains(&(1, 1)),
        "{committed:?}"
    );
    client.heartbeat(&member).await.unwrap();
    client.leave_group(&member).await.unwrap();
}

/// A caller that gives up in the middle of a request (its own time ran out, or it was dropped - the person a
/// webhook's answer was for went away) leaves that answer on its way. The next request must not read it as its own:
/// the connection is not used again, and a new one is made without the caller seeing anything.
#[tokio::test]
async fn a_request_given_up_midway_does_not_poison_the_next_one() {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let mut client = Routed::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    client.create_topic("events", 1, None).await.unwrap();
    for round in 0..3 {
        // A read of an empty partition that the broker holds for two seconds: given up after a tenth of one.
        let waited = tokio::time::timeout(
            Duration::from_millis(100),
            client.fetch("events", &[(0, i64::from(round))], 2_000, 1 << 20),
        )
        .await;
        assert!(
            waited.is_err(),
            "the read was still waiting when the caller gave up"
        );
        // Its answer arrives later, on the old connection; these are answered on a new one.
        assert_eq!(
            client.partition_count("events").await.unwrap(),
            1,
            "round {round}"
        );
        let message = Outgoing {
            key: None,
            value: Some(Bytes::from_static(b"m")),
            headers: Vec::new(),
        };
        assert_eq!(
            client.produce("events", 0, &[message], 0).await.unwrap(),
            i64::from(round)
        );
    }
    broker.shutdown().await;
}

/// The plain client heals itself: left in the middle of a request, its next request goes on a connection made anew
/// and is answered - it never reads the answer still on its way as its own (seen as "answer for request N arrived
/// for request N+1" on a webhook, after its caller went away).
#[tokio::test]
async fn a_client_left_midway_sends_its_next_request_on_a_new_connection() {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    let mut client = Client::connect(&addr, Duration::from_secs(10)).await.unwrap();
    client.create_topic("events", 1, None).await.unwrap();
    for _ in 0..3 {
        assert!(!client.mid_exchange());
        let waited = tokio::time::timeout(
            Duration::from_millis(100),
            client.fetch("events", &[(0, 0)], 2_000, 1 << 20),
        )
        .await;
        assert!(
            waited.is_err(),
            "the read was still waiting when the caller gave up"
        );
        assert!(client.mid_exchange());
        assert!(client.topics().await.unwrap().iter().any(|t| t.name == "events"));
        assert_eq!(client.list_offset("events", 0, EARLIEST).await.unwrap(), 0);
    }
    broker.shutdown().await;
}

/// A reader told to stop while it waits for records gives up that read, then commits and leaves its group on the
/// same client: both must reach the broker, so the next reader has the partitions at once.
#[tokio::test]
async fn a_reader_that_gave_up_a_read_still_commits_and_leaves() {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    let mut one = Routed::connect(&addr, Duration::from_secs(10)).await.unwrap();
    one.create_topic("events", 1, None).await.unwrap();
    let member = one
        .join_group("readers", "events", 1, 30_000, None)
        .await
        .unwrap();
    let waited = tokio::time::timeout(
        Duration::from_millis(100),
        one.fetch("events", &[(0, 0)], 2_000, 1 << 20),
    )
    .await;
    assert!(waited.is_err());
    one.commit_member(&member, &[(0, 0)]).await.unwrap();
    one.leave_group(&member).await.unwrap();

    let mut two = Routed::connect(&addr, Duration::from_secs(10)).await.unwrap();
    let next = tokio::time::timeout(
        Duration::from_secs(5),
        two.join_group("readers", "events", 1, 30_000, None),
    )
    .await
    .expect("the group is free at once: the first reader left it")
    .unwrap();
    assert_eq!(next.partitions, vec![0]);
    broker.shutdown().await;
}
