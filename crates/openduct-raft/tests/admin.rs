// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Changes an admin makes to a cluster's topics, through any broker: every broker sees them, and partitions added
//! work like the first ones.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_storage::{Store, StoreError};
use openraft::async_runtime::WatchReceiver;

use common::{Cluster, batch, copy_holds, on, topic};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retention_change_through_any_broker_reaches_every_broker() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("logs", topic(2)))
        .await
        .unwrap();
    // Through a broker that does not lead the controller: forwarded to it.
    let follower = cluster
        .running
        .values()
        .find(|r| r.id() != coordinator.id())
        .cloned()
        .unwrap();
    on(&follower, |r| r.set_retention("logs", Some(3_600_000)))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    for r in cluster.running.values() {
        while r.topic("logs").and_then(|t| t.retention_ms) != Some(3_600_000) {
            assert!(
                Instant::now() < deadline,
                "broker {} never saw the change",
                r.id()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(r.topic("logs").unwrap().partitions, 2);
    }
    on(&follower, |r| r.set_retention("logs", None)).await.unwrap();
    for r in cluster.running.values() {
        while r.topic("logs").unwrap().retention_ms.is_some() {
            assert!(
                Instant::now() < deadline,
                "broker {} kept the old retention",
                r.id()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert!(matches!(
        on(&follower, |r| r.set_retention("nowhere", None)).await,
        Err(StoreError::UnknownTopic(_))
    ));
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitions_added_through_any_broker_are_led_written_and_copied() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("orders", topic(2)))
        .await
        .unwrap();
    let follower = cluster
        .running
        .values()
        .find(|r| r.id() != coordinator.id())
        .cloned()
        .unwrap();
    on(&follower, |r| r.add_partitions("orders", 4, None))
        .await
        .unwrap();
    // Placed where it is asked: the fifth partition led by broker 3, copied on 3 and 1.
    on(&follower, |r| {
        r.add_partitions("orders", 5, Some(vec![vec![3, 1]]))
    })
    .await
    .unwrap();
    for r in cluster.running.values() {
        let deadline = Instant::now() + Duration::from_secs(20);
        while r.topic("orders").map(|t| t.partitions) != Some(5) {
            assert!(
                Instant::now() < deadline,
                "broker {} never saw the partitions",
                r.id()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert_eq!(coordinator.placement("orders", 4).unwrap().replicas, vec![3, 1]);
    for p in 2..5 {
        for i in 0..3 {
            assert_eq!(
                cluster.append("orders", p, batch(2, i)).await.base_offset,
                i * 2,
                "partition {p}"
            );
        }
    }
    for r in cluster.running.values() {
        copy_holds(r, "orders", 2, 3).await;
        copy_holds(r, "orders", 3, 3).await;
    }
    let holder = &cluster.running[&3];
    copy_holds(holder, "orders", 4, 3).await;
    assert!(
        cluster.running[&2].local_partition("orders", 4).is_none(),
        "not placed on broker 2"
    );

    // Refused: fewer, a broker not in the cluster, placements that do not add up, the offsets topic.
    for (total, assignment) in [(3, None), (6, Some(vec![vec![9]])), (7, Some(vec![vec![1]]))] {
        let refused = on(&follower, move |r| r.add_partitions("orders", total, assignment)).await;
        assert!(
            matches!(
                refused,
                Err(StoreError::PartitionsNotAdded { .. } | StoreError::InvalidReplicas(_))
            ),
            "{total}: {refused:?}"
        );
    }
    assert!(matches!(
        on(&follower, |r| r.add_partitions(
            openduct_raft::offsets::OFFSETS_TOPIC,
            32,
            None
        ))
        .await,
        Err(StoreError::InvalidReplicas(_) | StoreError::UnknownTopic(_))
    ));
    assert_eq!(
        coordinator.topic("orders").unwrap().partitions,
        5,
        "nothing refused changed it"
    );
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_records_are_gone_on_every_copy_and_on_one_rebuilt_from_a_snapshot() {
    // Snapshots every 20 entries, so the broker brought back with nothing is given one.
    let mut cluster = Cluster::new(3, 3, 20);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("d", topic(1))).await.unwrap();
    // Not broker 1, which would start a new cluster with an empty disk.
    let lost = 3;
    for i in 0..10 {
        cluster.append("d", 0, batch(2, i)).await;
    }
    let leader = cluster.leader("d", 0).await;
    let follower = cluster
        .running
        .values()
        .find(|r| r.id() != leader.id())
        .cloned()
        .unwrap();
    assert!(matches!(
        on(&follower, |r| r.delete_records("d", 0, 4)).await,
        Err(StoreError::NotLeader { .. })
    ));
    assert_eq!(on(&leader, |r| r.delete_records("d", 0, 7)).await.unwrap(), 7);
    assert!(matches!(
        on(&leader, |r| r.delete_records("d", 0, 99)).await,
        Err(StoreError::OffsetOutOfRange { .. })
    ));
    let start_of = |r: &Arc<openduct_raft::Replica>| r.local_partition("d", 0).map(|l| l.offsets().log_start);
    let deadline = Instant::now() + Duration::from_secs(20);
    for r in cluster.running.values() {
        while start_of(r) != Some(7) {
            assert!(
                Instant::now() < deadline,
                "broker {} starts at {:?}",
                r.id(),
                start_of(r)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    // More writes, so a snapshot comes after the deletion; then broker 3 loses its disk and is rebuilt from it.
    for i in 10..40 {
        cluster.append("d", 0, batch(1, i)).await;
    }
    // The log before the snapshot removed: the rebuilt broker cannot catch up from the log alone.
    let leader = cluster.leader("d", 0).await;
    let raft = leader.partition_raft("d", 0).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while raft.metrics().borrow_watched().purged.is_none() {
        assert!(
            Instant::now() < deadline,
            "the leader never removed old log entries"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.stop(lost).await;
    std::fs::remove_dir_all(cluster.dir(lost)).unwrap();
    cluster.start(lost).await;
    let rebuilt = Arc::clone(&cluster.running[&lost]);
    // Caught up: 20 offsets before, 30 after (read from offset 0, a copy no longer has anything to give).
    let end_of =
        |r: &Arc<openduct_raft::Replica>| r.local_partition("d", 0).map(|l| l.offsets().high_watermark);
    let deadline = Instant::now() + Duration::from_secs(60);
    while end_of(&rebuilt) != Some(50) {
        assert!(
            Instant::now() < deadline,
            "the rebuilt copy ends at {:?}",
            end_of(&rebuilt)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(start_of(&rebuilt), Some(7), "the start came with the snapshot");
    assert!(rebuilt.local_partition("d", 0).unwrap().read(5, 1 << 20).is_err());
    cluster.stop_all().await;
}
