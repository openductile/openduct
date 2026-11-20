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
use openduct_raft::Replica;

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

fn settings(pairs: &[(&str, &str)]) -> openduct_storage::TopicSettings {
    let mut s = openduct_storage::TopicSettings::default();
    for (name, value) in pairs {
        s.set(name, value).unwrap();
    }
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_topics_settings_wait_for_every_broker_to_be_upgraded_then_hold_on_every_copy() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    // A segment a batch, so retention by size has closed segments to remove.
    cluster.segment_bytes = Some(1);
    // Broker 3 still runs the build before (feature level 1): it could not read the new commands.
    cluster.levels.insert(3, 1);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("orders", topic(1)))
        .await
        .unwrap();
    let small = settings(&[("max.message.bytes", "100")]);
    let refused = on(&coordinator, {
        let small = small.clone();
        move |r| r.set_topic_settings("orders", small)
    })
    .await;
    match refused {
        Err(StoreError::Unsupported(why)) => assert!(why.contains("upgraded"), "{why}"),
        other => panic!("refused until every broker is upgraded, not {other:?}"),
    }
    assert!(
        cluster
            .running
            .values()
            .all(|r| r.topic("orders").is_none_or(|t| t.settings.is_empty()))
    );

    // Broker 3 is upgraded; the level rises, and the settings are taken through a broker that does not lead.
    cluster.stop(3).await;
    cluster.levels.remove(&3);
    cluster.start(3).await;
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let any = cluster.running.values().next().cloned().unwrap();
        let small = small.clone();
        match on(&any, move |r| r.set_topic_settings("orders", small)).await {
            Ok(()) => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "never taken after the upgrade: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    for r in cluster.running.values() {
        while r
            .topic("orders")
            .is_none_or(|t| t.settings.max_message_bytes() != 100)
        {
            assert!(
                Instant::now() < deadline,
                "broker {} never saw the settings",
                r.id()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // max.message.bytes: the leader refuses a larger batch before any copy holds it.
    let one = batch(1, 1_000);
    assert!(one.len() <= 100);
    for i in 0..6 {
        assert_eq!(
            cluster.append("orders", 0, batch(1, 1_000 + i)).await.base_offset,
            i
        );
    }
    let leader = cluster.leader("orders", 0).await;
    let big = bytes::Bytes::from(vec![0u8; 400]);
    assert!(matches!(
        on(&leader, move |r| r.append("orders", 0, big)).await,
        Err(StoreError::BatchTooLarge {
            size: 400,
            limit: 100
        })
    ));
    for r in cluster.running.values() {
        copy_holds(r, "orders", 0, 6).await;
    }

    // retention.bytes: room for two and a half batches - three stay, on every copy.
    let keep = (one.len() * 5 / 2).to_string();
    let any = cluster.running.values().next().cloned().unwrap();
    on(&any, move |r| {
        r.set_topic_settings(
            "orders",
            settings(&[("max.message.bytes", "100"), ("retention.bytes", &keep)]),
        )
    })
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = cluster.leader("orders", 0).await;
        if leader
            .topic("orders")
            .unwrap()
            .settings
            .retention_bytes()
            .is_some()
            && on(&leader, |r| r.enforce_retention(0)).await.unwrap() == 3
        {
            break;
        }
        assert!(Instant::now() < deadline, "retention by size never ran");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for r in cluster.running.values() {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let log = r.local_partition("orders", 0).unwrap();
            let offsets = log.offsets();
            if (offsets.log_start, offsets.high_watermark) == (3, 6) {
                break;
            }
            assert!(Instant::now() < deadline, "broker {}: {offsets:?}", r.id());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let leader = cluster.leader("orders", 0).await;
    assert_eq!(
        on(&leader, |r| r.enforce_retention(0)).await.unwrap(),
        0,
        "nothing more the next round"
    );
    cluster.stop_all().await;
}

/// What a broker's own copy of a partition holds: its stored batches, and what a consumer would see in them -
/// (offset, key, has a value).
fn copy_of(r: &Replica, topic: &str) -> (Vec<bytes::Bytes>, Vec<(i64, String, bool)>) {
    // A broker just started may not have opened the partition yet: nothing held, so far.
    let batches = r
        .local_partition(topic, 0)
        .and_then(|log| log.read(0, usize::MAX).ok())
        .map(|read| read.batches)
        .unwrap_or_default();
    let mut seen = Vec::new();
    for b in &batches {
        let (header, all) = openduct_storage::records(b).unwrap();
        for record in all {
            let key = String::from_utf8(b[record.key.unwrap()].to_vec()).unwrap();
            seen.push((
                header.base_offset + i64::from(record.offset_delta),
                key,
                !record.tombstone,
            ));
        }
    }
    (batches, seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compacted_topic_is_cleaned_alike_on_every_copy_by_its_leader() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    // A segment a batch: the newest is the one being written to, which is never cleaned.
    cluster.segment_bytes = Some(1);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("prices", topic(1)))
        .await
        .unwrap();
    // Taken once the cluster is at the level that compacts (every broker here runs this build).
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let any = cluster.running.values().next().cloned().unwrap();
        let wanted = settings(&[("cleanup.policy", "compact"), ("delete.retention.ms", "1000")]);
        match on(&any, move |r| r.set_topic_settings("prices", wanted)).await {
            Ok(()) => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "compaction never taken: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    // a and b; a again; c; a tombstone for b; d.
    let written: [&[(&str, Option<&str>)]; 5] = [
        &[("a", Some("1")), ("b", Some("1"))],
        &[("a", Some("2"))],
        &[("c", Some("1"))],
        &[("b", None)],
        &[("d", Some("1"))],
    ];
    for (i, records) in written.iter().enumerate() {
        cluster
            .append("prices", 0, common::keyed(records, 1_000 + i as i64))
            .await;
    }
    for r in cluster.running.values() {
        copy_holds(r, "prices", 0, 5).await;
    }

    // A pass, ordered by the leader once it sees the setting.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = cluster.leader("prices", 0).await;
        if leader.topic("prices").is_some_and(|t| t.settings.compacts())
            && on(&leader, |r| r.compact(10_000)).await.unwrap() == 2
        {
            break;
        }
        assert!(Instant::now() < deadline, "the leader never compacted");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after_first = vec![
        (2, "a".to_owned(), true),
        (3, "c".to_owned(), true),
        (4, "b".to_owned(), false),
        (5, "d".to_owned(), true),
    ];
    let same_everywhere = |cluster: &Cluster, want: &[(i64, String, bool)]| {
        let copies: Vec<_> = cluster.running.values().map(|r| copy_of(r, "prices")).collect();
        copies
            .iter()
            .all(|(bytes, seen)| seen == want && *bytes == copies[0].0)
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while !same_everywhere(&cluster, &after_first) {
        assert!(Instant::now() < deadline, "the copies differ after the pass");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for r in cluster.running.values() {
        let offsets = r.local_partition("prices", 0).unwrap().offsets();
        assert_eq!(
            (offsets.log_start, offsets.high_watermark),
            (0, 6),
            "offsets never move"
        );
    }

    // Nothing to do costs nothing; the tombstone goes after its wait, on every copy; retention leaves the topic be.
    let leader = cluster.leader("prices", 0).await;
    assert_eq!(on(&leader, |r| r.compact(10_500)).await.unwrap(), 0);
    assert_eq!(on(&leader, |r| r.compact(11_000)).await.unwrap(), 1);
    let after_second = vec![
        (2, "a".to_owned(), true),
        (3, "c".to_owned(), true),
        (5, "d".to_owned(), true),
    ];
    let deadline = Instant::now() + Duration::from_secs(20);
    while !same_everywhere(&cluster, &after_second) {
        assert!(
            Instant::now() < deadline,
            "the copies differ after the tombstone went"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    on(&leader, |r| r.set_retention("prices", Some(1))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        on(&leader, |r| r.enforce_retention(i64::MAX / 2)).await.unwrap(),
        0
    );

    // A broker restarted reads its compacted segments back, and writes go on at the next offset.
    let restarted = cluster
        .running
        .keys()
        .copied()
        .find(|id| *id != leader.id())
        .unwrap();
    cluster.stop(restarted).await;
    cluster.start(restarted).await;
    assert_eq!(
        cluster
            .append("prices", 0, common::keyed(&[("e", Some("1"))], 2_000))
            .await
            .base_offset,
        6
    );
    let mut with_e = after_second.clone();
    with_e.push((6, "e".to_owned(), true));
    let deadline = Instant::now() + Duration::from_secs(60);
    while !same_everywhere(&cluster, &with_e) {
        assert!(Instant::now() < deadline, "the restarted broker's copy differs");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_ms_rolls_every_copy_at_the_same_batch_and_the_leader_keeps_the_topics_rules() {
    // Segments of the usual size: only segment.ms ever closes one here.
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("prices", topic(1)))
        .await
        .unwrap();
    let wanted = || {
        settings(&[
            ("cleanup.policy", "compact"),
            ("delete.retention.ms", "1000"),
            ("segment.ms", "100"),
            ("min.insync.replicas", "2"),
        ])
    };
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let any = cluster.running.values().next().cloned().unwrap();
        match on(&any, move |r| r.set_topic_settings("prices", wanted())).await {
            Ok(()) => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "never taken: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    // Two of three copies hold a write before it is acknowledged: two is kept, three would be a promise nothing
    // keeps.
    let any = cluster.running.values().next().cloned().unwrap();
    let mut three = wanted();
    three.set("min.insync.replicas", "3").unwrap();
    match on(&any, move |r| r.set_topic_settings("prices", three)).await {
        Err(StoreError::Unsupported(why)) => assert!(why.contains("majority"), "{why}"),
        other => panic!("refused, not {other:?}"),
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = cluster.leader("prices", 0).await;
        if leader.topic("prices").is_some_and(|t| t.settings.compacts()) {
            break;
        }
        assert!(Instant::now() < deadline, "the leader never saw the settings");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A compacted topic takes only records with a key: refused by the leader, held by no copy.
    let leader = cluster.leader("prices", 0).await;
    assert!(matches!(
        on(&leader, |r| r.append(
            "prices",
            0,
            common::records_batch(&[(None, Some("v"))], 900)
        ))
        .await,
        Err(StoreError::KeylessRecord(0))
    ));

    // a at 1000 and 1050 share a segment; 1100 starts the next, 1200 the one after.
    for (i, at) in [1_000i64, 1_050, 1_100, 1_200].into_iter().enumerate() {
        let value = i.to_string();
        let appended = cluster
            .append("prices", 0, common::keyed(&[("a", Some(&value))], at))
            .await;
        assert_eq!(appended.base_offset, i as i64);
    }
    for r in cluster.running.values() {
        copy_holds(r, "prices", 0, 4).await;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let bases: Vec<i64> = r
                .local_partition("prices", 0)
                .unwrap()
                .local_segments()
                .iter()
                .map(|s| s.base)
                .collect();
            // The closed segments: the one being written to (from offset 3) is not among them.
            if bases == vec![0, 2] {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "broker {}: closed segments {bases:?}",
                r.id()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    // And so compaction reaches them, alike on every copy.
    let leader = cluster.leader("prices", 0).await;
    assert_eq!(on(&leader, |r| r.compact(10_000)).await.unwrap(), 2);
    let want = vec![(2, "a".to_owned(), true), (3, "a".to_owned(), true)];
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let copies: Vec<_> = cluster.running.values().map(|r| copy_of(r, "prices")).collect();
        if copies
            .iter()
            .all(|(bytes, seen)| *seen == want && *bytes == copies[0].0)
        {
            break;
        }
        assert!(Instant::now() < deadline, "the copies differ");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.stop_all().await;
}

/// `batch` as part of `producer`'s transaction.
fn transactional(producer: i64, sequence: i32, timestamp: i64) -> bytes::Bytes {
    let mut b = batch(1, timestamp).to_vec();
    b[21..23].copy_from_slice(&0x10i16.to_be_bytes());
    b[43..51].copy_from_slice(&producer.to_be_bytes());
    b[51..53].copy_from_slice(&0i16.to_be_bytes());
    b[53..57].copy_from_slice(&sequence.to_be_bytes());
    let crc = crc32c::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    bytes::Bytes::from(b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transactions_records_and_its_marker_reach_every_copy_and_mean_the_same_there() {
    use openduct_storage::batch::Marker;
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("payments", topic(1)))
        .await
        .unwrap();
    // Taken once every broker reads transactions (all three here run this build).
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let leader = cluster.leader("payments", 0).await;
        match on(&leader, |r| r.append("payments", 0, transactional(7, 0, 1))).await {
            Ok(appended) => {
                assert_eq!(appended.base_offset, 0);
                break;
            }
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "the transaction's record was never taken: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    cluster.append("payments", 0, batch(1, 2)).await;
    cluster.append("payments", 0, transactional(9, 0, 3)).await;
    // A client may not write a marker; the leader writes them when a transaction ends.
    let leader = cluster.leader("payments", 0).await;
    let forged = openduct_storage::batch::marker_batch(7, 0, Marker::Commit, 0, 0);
    assert!(matches!(
        on(&leader, move |r| r.append("payments", 0, forged)).await,
        Err(StoreError::Batch(_))
    ));
    let offsets = on(&leader, |r| r.offsets("payments", 0)).await.unwrap();
    assert_eq!(
        (offsets.high_watermark, offsets.last_stable),
        (3, 0),
        "nothing is stable past the first open one"
    );
    let committed = on(&leader, |r| {
        r.end_transaction("payments", 0, 9, 0, Marker::Commit, 1)
    })
    .await
    .unwrap();
    assert_eq!(committed.base_offset, 3);
    let aborted = on(&leader, |r| {
        r.end_transaction("payments", 0, 7, 0, Marker::Abort, 1)
    })
    .await
    .unwrap();
    assert_eq!(aborted.base_offset, 4);
    // Every copy holds the same five batches, and reads the same from them.
    for r in cluster.running.values() {
        copy_holds(r, "payments", 0, 5).await;
        let log = r.local_partition("payments", 0).unwrap();
        let offsets = log.offsets();
        assert_eq!(
            (offsets.high_watermark, offsets.last_stable),
            (5, 5),
            "broker {}",
            r.id()
        );
        assert_eq!(
            log.aborted_transactions(0, 100),
            vec![openduct_storage::AbortedTransaction {
                producer_id: 7,
                first_offset: 0,
                last_offset: 4
            }],
            "broker {}",
            r.id()
        );
    }
    // A broker restarted reads it back from its segments.
    let restarted = cluster
        .running
        .keys()
        .copied()
        .find(|id| *id != leader.id())
        .unwrap();
    cluster.stop(restarted).await;
    cluster.start(restarted).await;
    copy_holds(&cluster.running[&restarted], "payments", 0, 5).await;
    let log = cluster.running[&restarted]
        .local_partition("payments", 0)
        .unwrap();
    assert_eq!(log.aborted_transactions(0, 100).len(), 1);
    assert_eq!(log.offsets().last_stable, 5);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_names_itself_before_it_knows_the_cluster() {
    // One broker of three, started alone: no election can be won, and its copy of the membership is empty. A
    // client that reaches it is still told of a broker to ask again - this one - and of no controller yet.
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start(1).await;
    let alone = cluster.running[&1].clone();
    let view = on(&alone, |r| r.cluster()).await.unwrap();
    // (Itself alone in its first instant; all three it was configured with once it has proposed the cluster.)
    assert!(view.brokers.iter().any(|b| b.id == 1), "{:?}", view.brokers);
    assert_eq!((view.me, view.coordinator), (1, None));
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_removes_an_aborted_transactions_records_alike_on_every_copy() {
    use openduct_storage::batch::Marker;
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.segment_bytes = Some(1);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("prices", topic(1)))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let any = cluster.running.values().next().cloned().unwrap();
        let wanted = settings(&[("cleanup.policy", "compact"), ("delete.retention.ms", "0")]);
        match on(&any, move |r| r.set_topic_settings("prices", wanted)).await {
            Ok(()) => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "compaction never taken: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    // `records` as part of `producer`'s transaction.
    let in_transaction = |records: &[(&str, Option<&str>)], producer: i64| {
        let mut b = common::keyed(records, 1_000).to_vec();
        b[21..23].copy_from_slice(&0x10i16.to_be_bytes());
        b[43..51].copy_from_slice(&producer.to_be_bytes());
        b[51..53].copy_from_slice(&0i16.to_be_bytes());
        b[53..57].copy_from_slice(&0i32.to_be_bytes());
        let crc = crc32c::crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        bytes::Bytes::from(b)
    };
    // a; an aborted newer a; a committed b; an open c; d, e.
    cluster
        .append("prices", 0, common::keyed(&[("a", Some("1"))], 1_000))
        .await; // 0
    cluster
        .append("prices", 0, in_transaction(&[("a", Some("lost"))], 7))
        .await; // 1
    let leader = cluster.leader("prices", 0).await;
    on(&leader, |r| {
        r.end_transaction("prices", 0, 7, 0, Marker::Abort, 0)
    })
    .await
    .unwrap(); // 2
    cluster
        .append("prices", 0, in_transaction(&[("b", Some("1"))], 8))
        .await; // 3
    let leader = cluster.leader("prices", 0).await;
    on(&leader, |r| {
        r.end_transaction("prices", 0, 8, 0, Marker::Commit, 0)
    })
    .await
    .unwrap(); // 4
    cluster
        .append("prices", 0, in_transaction(&[("c", Some("maybe"))], 9))
        .await; // 5
    cluster
        .append("prices", 0, common::keyed(&[("d", Some("1"))], 1_000))
        .await; // 6
    cluster
        .append("prices", 0, common::keyed(&[("e", Some("1"))], 1_000))
        .await; // 7
    for r in cluster.running.values() {
        copy_holds(r, "prices", 0, 8).await;
    }
    // The pass runs after the markers were written (they carry the time).
    let now = openduct_storage::now_ms() + 10_000;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = cluster.leader("prices", 0).await;
        if on(&leader, move |r| r.compact(now)).await.unwrap() == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "the leader never compacted");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The aborted a is gone and replaced nothing; the committed b and the open c stay.
    let want = vec![
        (0, "a".to_owned(), true),
        (3, "b".to_owned(), true),
        (5, "c".to_owned(), true),
        (6, "d".to_owned(), true),
        (7, "e".to_owned(), true),
    ];
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let copies: Vec<_> = cluster.running.values().map(|r| copy_of(r, "prices")).collect();
        if copies
            .iter()
            .all(|(bytes, seen)| *seen == want && *bytes == copies[0].0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the copies differ: {:?}",
            copies.iter().map(|c| &c.1).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.stop_all().await;
}
