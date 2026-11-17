// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Leader epochs in a replicated partition: each batch is stored in the term of the leader that wrote it, the same
//! bytes on every copy; after the leader is lost, OffsetForLeaderEpoch finds where the old epoch ended - exactly where
//! the new leader's first batch begins - and ListOffsets answers each offset's epoch.

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
use openduct_storage::batch::parse_header;
use openduct_storage::{EpochEnd, Store, StoreError};

use common::{Cluster, batch, copy_holds, on, topic};

/// The epoch every batch of a broker's own copy of `e/0` was stored in.
fn stored_epochs(r: &Replica) -> Vec<i32> {
    r.local_partition("e", 0)
        .unwrap()
        .read(0, usize::MAX)
        .unwrap()
        .batches
        .iter()
        .map(|b| parse_header(b).unwrap().partition_leader_epoch)
        .collect()
}

/// The leader's answer for `epoch`, asked again while leadership settles (a loaded runner can elect once more).
async fn epoch_end(cluster: &Cluster, epoch: i32) -> (Arc<Replica>, EpochEnd) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = cluster.leader("e", 0).await;
        match on(&leader, move |r| r.epoch_end("e", 0, epoch)).await {
            Ok(end) => return (leader, end),
            Err(e) => {
                assert!(Instant::now() < deadline, "no answer for epoch {epoch}: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_epoch_ends_where_the_next_leader_began_on_every_copy() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("e", topic(1))).await.unwrap();

    // Three batches of two records, offsets 0-5, in the first leader's epoch.
    for i in 0..3 {
        assert_eq!(cluster.append("e", 0, batch(2, i)).await.base_offset, i * 2);
    }
    for r in cluster.running.values() {
        copy_holds(r, "e", 0, 3).await;
    }
    let first = stored_epochs(&*cluster.leader("e", 0).await)[0];
    assert!(first >= 1, "a leader's term, not the producer's value: {first}");
    for r in cluster.running.values() {
        assert_eq!(stored_epochs(r), vec![first; 3], "broker {}", r.id());
    }
    let (leader, end) = epoch_end(&cluster, first).await;
    if end.current == first {
        assert_eq!(end.end_offset, Some(6), "the current epoch runs to the end");
    }
    assert_eq!(on(&leader, |r| r.epoch_of("e", 0, 3)).await.unwrap(), first);
    assert_eq!(
        on(&leader, |r| r.epoch_of("e", 0, 6)).await.unwrap(),
        end.current,
        "past the last batch: the epoch now"
    );
    // Only the leader answers.
    let other = Arc::clone(cluster.running.values().find(|r| r.id() != leader.id()).unwrap());
    assert!(matches!(
        on(&other, move |r| r.epoch_end("e", 0, first)).await,
        Err(StoreError::NotLeader { .. })
    ));

    // The leader is lost: the next one writes offsets 6-9 in a later epoch.
    let lost = leader.id();
    cluster.stop(lost).await;
    for i in 3..5 {
        assert_eq!(cluster.append("e", 0, batch(2, i)).await.base_offset, i * 2);
    }
    let leader = cluster.leader("e", 0).await;
    copy_holds(&leader, "e", 0, 5).await;
    let epochs = stored_epochs(&leader);
    let second = epochs[3];
    assert!(second > first, "{epochs:?}");
    assert_eq!(epochs, vec![first, first, first, second, second]);

    // Where the first epoch ended: where the new leader's first batch begins.
    let (leader, end) = epoch_end(&cluster, first).await;
    assert_eq!(end.end_offset, Some(6));
    assert!(end.current >= second);
    // An epoch in between, in which nothing was written (an election no one won, say), ends there too.
    if second > first + 1 {
        assert_eq!(epoch_end(&cluster, first + 1).await.1.end_offset, Some(6));
    }
    let (_, end) = epoch_end(&cluster, second).await;
    assert_eq!(end.end_offset, Some(10));
    let (_, end) = epoch_end(&cluster, end.current + 1).await;
    assert_eq!(end.end_offset, None, "an epoch after the current one is unknown");
    assert_eq!(on(&leader, |r| r.epoch_of("e", 0, 7)).await.unwrap(), second);
    assert_eq!(on(&leader, |r| r.epoch_of("e", 0, 0)).await.unwrap(), first);

    // The lost broker comes back and holds the same bytes: the epochs were decided by the log, not by each copy.
    cluster.start(lost).await;
    let back = Arc::clone(&cluster.running[&lost]);
    copy_holds(&back, "e", 0, 5).await;
    assert_eq!(stored_epochs(&back), epochs);
    assert_eq!(
        back.local_partition("e", 0).unwrap().read(0, usize::MAX).unwrap(),
        leader
            .local_partition("e", 0)
            .unwrap()
            .read(0, usize::MAX)
            .unwrap()
    );
    cluster.stop_all().await;
}
