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
use openduct_broker::client::{EARLIEST, Outgoing, Routed};
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
