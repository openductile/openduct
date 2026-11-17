// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A partition with one copy rests - its Raft clock stopped - and wakes when it is given more: its leader shows itself
//! alive again, so the new copies hold no elections of their own, and one of them takes over when the leader is lost.
//! A one-copy partition moved to another broker is led there.

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

use openduct_raft::Replica;
use openduct_storage::Store;
use openraft::async_runtime::WatchReceiver;

use common::{Cluster, batch, on, topic};

async fn until(what: &str, r: &Arc<Replica>, check: impl Fn(&Replica) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !check(r) {
        assert!(Instant::now() < deadline, "broker {}: never {what}", r.id());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Who leads a partition here, and in which term.
fn leadership(r: &Replica, topic: &str, partition: i32) -> (Option<u64>, u64) {
    let raft = r.partition_raft(topic, partition).unwrap();
    let metrics = raft.metrics();
    let m = metrics.borrow_watched();
    (m.current_leader, m.current_term)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_copy_partition_rests_and_wakes_when_it_is_given_more_copies() {
    let mut cluster = Cluster::new(3, 1, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("solo", topic(2)))
        .await
        .unwrap();
    for p in 0..2 {
        assert_eq!(cluster.append("solo", p, batch(1, 0)).await.base_offset, 0);
    }
    let home = coordinator.placement("solo", 0).unwrap().replicas;
    assert_eq!(home.len(), 1);
    let home = home[0];
    let others: Vec<i32> = [1, 2, 3].into_iter().filter(|b| *b != home).collect();

    // Resting well past an election's wait, it still takes writes at once.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(cluster.append("solo", 0, batch(1, 1)).await.base_offset, 1);

    // Given two more copies: its leader shows itself alive again, so no copy holds an election of its own.
    let replicas = vec![home, others[0], others[1]];
    on(&coordinator, move |r| r.reassign("solo", 0, replicas))
        .await
        .unwrap();
    until("finishes growing the partition", &coordinator, |r| {
        r.reassignments().is_empty()
    })
    .await;
    let leader = Arc::clone(&cluster.running[&u64::try_from(home).unwrap()]);
    let before = leadership(&leader, "solo", 0);
    assert_eq!(before.0, Some(u64::try_from(home).unwrap()));
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(
        leadership(&leader, "solo", 0),
        before,
        "the copies held an election: the leader was silent"
    );
    assert_eq!(cluster.append("solo", 0, batch(1, 2)).await.base_offset, 2);

    // Partition 1, one copy, moves to another broker: led there, writes go on.
    let from = coordinator.placement("solo", 1).unwrap().replicas[0];
    let to = [1, 2, 3].into_iter().find(|b| *b != from).unwrap();
    on(&coordinator, move |r| r.reassign("solo", 1, vec![to]))
        .await
        .unwrap();
    until("finishes moving the partition", &coordinator, |r| {
        r.reassignments().is_empty()
    })
    .await;
    let there = Arc::clone(&cluster.running[&u64::try_from(to).unwrap()]);
    until("leads the moved partition", &there, |r| {
        r.partition_leader("solo", 1) == Some(u64::try_from(to).unwrap())
    })
    .await;
    assert_eq!(cluster.append("solo", 1, batch(1, 1)).await.base_offset, 1);

    // The grown partition's leader lost: one of its new copies takes over.
    cluster.stop(u64::try_from(home).unwrap()).await;
    let next = Arc::clone(&cluster.running[&u64::try_from(others[0]).unwrap()]);
    until("sees a new leader", &next, |r| {
        r.partition_leader("solo", 0)
            .is_some_and(|l| l != u64::try_from(home).unwrap())
    })
    .await;
    assert_eq!(cluster.append("solo", 0, batch(1, 3)).await.base_offset, 3);
    cluster.stop_all().await;
}
