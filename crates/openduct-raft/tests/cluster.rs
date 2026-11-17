// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Three brokers in one process, over real TCP: partitions' leaders spread over the brokers; what a leader is told,
//! every replica keeps; a partition carries on when its leader is lost; a broker that comes back, or comes back
//! with nothing, catches up; a broker holding no copy of a partition still names its leader.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_raft::{Replica, ReplicationTls, S3, S3Settings, TierConfig};
use openduct_storage::{CommittedOffset, Store, StoreError};
use openraft::async_runtime::WatchReceiver;

use common::{Cluster, batch, copy_holds, on, topic};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leaders_spread_and_a_partition_carries_on_when_its_leader_is_lost() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(3))).await.unwrap();

    // Each partition's first replica leads it: one partition per broker - once the first election's winner, if it
    // was another, has handed back (every second; a loaded runner takes a few).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut leaders = BTreeSet::new();
        for p in 0..3 {
            leaders.insert(cluster.leader("t", p).await.id());
        }
        if leaders.len() == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the three partitions' leaders are three brokers, not {leaders:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    for p in 0..3 {
        for i in 0..10 {
            let a = cluster.append("t", p, batch(2, i)).await;
            assert_eq!(a.base_offset, i * 2);
        }
    }
    // A broker that does not lead a partition refuses its appends and reads, naming the leader.
    let leader0 = cluster.leader("t", 0).await;
    let other = Arc::clone(cluster.running.values().find(|r| r.id() != leader0.id()).unwrap());
    assert!(matches!(
        on(&other, |r| r.append("t", 0, batch(1, 0))).await,
        Err(StoreError::NotLeader { leader: Some(_) })
    ));
    assert!(matches!(
        on(&other, |r| r.read("t", 0, 0, 1)).await,
        Err(StoreError::NotLeader { .. })
    ));
    // Topics and producer ids may be asked of any broker.
    let follower = Arc::clone(
        cluster
            .running
            .values()
            .find(|r| r.id() != coordinator.id())
            .unwrap(),
    );
    on(&follower, |r| r.create_topic("u", topic(1))).await.unwrap();
    assert!(follower.topic("u").is_some());
    assert!(matches!(
        on(&follower, |r| r.create_topic("u", topic(1))).await,
        Err(StoreError::TopicExists(_))
    ));
    let a = on(&coordinator, |r| r.next_producer_id()).await.unwrap();
    let b = on(&follower, |r| r.next_producer_id()).await.unwrap();
    assert_ne!(a, b);
    for r in cluster.running.values() {
        for p in 0..3 {
            copy_holds(r, "t", p, 10).await;
        }
    }
    // The leader as it is now (on a loaded runner one can have been elected since), as the placement names it.
    let deadline = Instant::now() + Duration::from_secs(20);
    let leader0 = loop {
        let leader0 = cluster.leader("t", 0).await;
        let placement = leader0.placement("t", 0).unwrap();
        assert_eq!(placement.replicas.len(), 3);
        if placement.leader == Some(i32::try_from(leader0.id()).unwrap()) {
            break leader0;
        }
        assert!(
            Instant::now() < deadline,
            "the placement names {:?}, not the leader {}",
            placement.leader,
            leader0.id()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Partition 0's leader is lost: its other replicas elect one of themselves; offsets go on.
    let lost = leader0.id();
    let written = leader0
        .local_partition("t", 0)
        .unwrap()
        .read(0, usize::MAX)
        .unwrap();
    cluster.stop(lost).await;
    let leader0 = cluster.leader("t", 0).await;
    assert_ne!(leader0.id(), lost);
    assert_eq!(
        leader0
            .local_partition("t", 0)
            .unwrap()
            .read(0, usize::MAX)
            .unwrap(),
        written
    );
    for i in 10..15 {
        assert_eq!(cluster.append("t", 0, batch(2, i)).await.base_offset, i * 2);
    }
    // The other partitions, led elsewhere, take writes all along.
    for p in 1..3 {
        assert_eq!(cluster.append("t", p, batch(2, 10)).await.base_offset, 20);
    }

    // The lost broker comes back with its disk and catches up on every partition.
    cluster.start(lost).await;
    let back = Arc::clone(&cluster.running[&lost]);
    copy_holds(&back, "t", 0, 15).await;
    copy_holds(&back, "t", 1, 11).await;
    copy_holds(&back, "t", 2, 11).await;
    assert_eq!(
        back.local_partition("t", 0).unwrap().read(0, usize::MAX).unwrap(),
        leader0
            .local_partition("t", 0)
            .unwrap()
            .read(0, usize::MAX)
            .unwrap()
    );
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_that_lost_its_disk_is_rebuilt_from_snapshots() {
    let mut cluster = Cluster::new(3, 3, 20);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(3))).await.unwrap();
    // Not broker 1, which would start a new cluster with an empty disk.
    let lost = 3;
    cluster.stop(lost).await;
    for i in 0..120 {
        let p = i32::try_from(i % 3).unwrap();
        cluster.append("t", p, batch(1, i)).await;
    }
    let committed = vec![CommittedOffset {
        topic: "t".to_owned(),
        partition: 1,
        offset: 7,
        metadata: Some("m".to_owned()),
        commit_timestamp_ms: 5,
    }];
    let coordinator = cluster.coordinator().await;
    let c = committed.clone();
    on(&coordinator, move |r| r.commit_offsets("g", &c))
        .await
        .unwrap();
    // Snapshots were taken and the log before them removed: the lost broker cannot catch up from the log alone.
    let leader = cluster.leader("t", 0).await;
    let raft = leader.partition_raft("t", 0).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while raft.metrics().borrow_watched().purged.is_none() {
        assert!(
            Instant::now() < deadline,
            "partition 0's leader never removed old log entries"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    std::fs::remove_dir_all(cluster.dir(lost)).unwrap();
    cluster.start(lost).await;
    let rebuilt = Arc::clone(&cluster.running[&lost]);
    for p in 0..3 {
        copy_holds(&rebuilt, "t", p, 40).await;
        let leader = cluster.leader("t", p).await;
        assert_eq!(
            rebuilt
                .local_partition("t", p)
                .unwrap()
                .read(0, usize::MAX)
                .unwrap(),
            leader
                .local_partition("t", p)
                .unwrap()
                .read(0, usize::MAX)
                .unwrap()
        );
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while rebuilt.committed_offsets("g").unwrap() != committed {
        assert!(
            Instant::now() < deadline,
            "the rebuilt broker lacks the group's offsets"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_holding_no_copy_of_a_partition_names_its_leader() {
    let mut cluster = Cluster::new(3, 2, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(3))).await.unwrap();
    for p in 0..3 {
        // Two copies of each partition: the third broker learns the leader from the others.
        let leader = cluster.leader("t", p).await;
        let placement = leader.placement("t", p).unwrap();
        assert_eq!(placement.replicas.len(), 2);
        let outsider = cluster
            .running
            .values()
            .find(|r| !placement.replicas.contains(&i32::try_from(r.id()).unwrap()))
            .unwrap();
        assert!(outsider.local_partition("t", p).is_none());
        assert!(matches!(
            on(outsider, move |r| r.append("t", p, batch(1, 0))).await,
            Err(StoreError::NotLeader { leader: Some(_) })
        ));
        cluster.append("t", p, batch(1, 0)).await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_broker_back_with_an_empty_disk_rejoins_rather_than_starting_a_new_cluster() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(3))).await.unwrap();
    for p in 0..3 {
        cluster.append("t", p, batch(1, 0)).await;
    }
    // Broker 1 starts a new cluster when its disk is empty - unless the others say one exists.
    cluster.stop(1).await;
    std::fs::remove_dir_all(cluster.dir(1)).unwrap();
    cluster.start(1).await;
    let back = Arc::clone(&cluster.running[&1]);
    for p in 0..3 {
        copy_holds(&back, "t", p, 1).await;
    }
    assert!(back.topic("t").is_some());
    cluster.coordinator().await;
    for p in 0..3 {
        assert_eq!(cluster.append("t", p, batch(1, 1)).await.base_offset, 1);
    }
    cluster.stop_all().await;
}

/// Waits until `check` holds on `r`.
async fn until(what: &str, r: &Arc<Replica>, check: impl Fn(&Replica) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !check(r) {
        assert!(Instant::now() < deadline, "broker {}: never {what}", r.id());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_joins_partitions_move_onto_it_and_another_broker_leaves() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(3))).await.unwrap();
    for p in 0..3 {
        for i in 0..5 {
            cluster.append("t", p, batch(1, i)).await;
        }
    }

    // Broker 4 joins: every broker sees it among the cluster's brokers.
    cluster.add_broker(4).await;
    for r in cluster.running.values() {
        until("sees four brokers", r, |r| {
            r.cluster().is_some_and(|c| c.brokers.len() == 4)
        })
        .await;
    }

    // Partition 0 moves from brokers 1, 2, 3 onto 4, 2, 3: 4 gets a copy and, being first, the lead.
    let four = Arc::clone(&cluster.running[&4]);
    on(&coordinator, |r| r.reassign("t", 0, vec![4, 2, 3]))
        .await
        .unwrap();
    until("finishes the move", &coordinator, |r| {
        r.reassignments().is_empty()
    })
    .await;
    assert_eq!(coordinator.placement("t", 0).unwrap().replicas, vec![4, 2, 3]);
    copy_holds(&four, "t", 0, 5).await;
    let one = Arc::clone(&cluster.running[&1]);
    until("lets go of partition 0", &one, |r| {
        r.local_partition("t", 0).is_none()
    })
    .await;
    until("leads partition 0", &four, |r| {
        r.partition_leader("t", 0) == Some(4)
    })
    .await;
    assert_eq!(cluster.append("t", 0, batch(1, 5)).await.base_offset, 5);

    // Broker 1 cannot leave while it holds partitions; once they are moved off it, it can.
    assert!(matches!(
        on(&coordinator, |r| r.remove_broker(1)).await,
        Err(StoreError::InvalidReplicas(_))
    ));
    // A broker named twice is refused up front.
    assert!(matches!(
        on(&coordinator, |r| r.reassign("t", 1, vec![4, 4, 2])).await,
        Err(StoreError::InvalidReplicas(_))
    ));
    // As an operator would: every partition still on broker 1 - of "t" and of the consumer groups' offsets topic
    // - moves to a broker that holds none of it yet, the moves finish, and broker 1 leaves; and all again should
    // broker 1 still hold a partition then (one made or moved meanwhile, as the offsets topic can be).
    let any = Arc::clone(&cluster.running[&2]);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        for t in coordinator.topics() {
            for p in 0..t.partitions {
                let Some(placement) = coordinator.placement(&t.name, p) else {
                    continue;
                };
                if !placement.replicas.contains(&1) {
                    continue;
                }
                let Some(spare) = [2, 3, 4].into_iter().find(|b| !placement.replicas.contains(b)) else {
                    continue;
                };
                let target: Vec<i32> = placement
                    .replicas
                    .iter()
                    .map(|r| if *r == 1 { spare } else { *r })
                    .collect();
                let topic = t.name.clone();
                // A partition already on its way elsewhere refuses a second move: it is waited out below.
                let _ = on(&coordinator, move |r| r.reassign(&topic, p, target)).await;
            }
        }
        until("finishes the moves", &coordinator, |r| {
            r.reassignments().is_empty()
        })
        .await;
        match on(&any, |r| r.remove_broker(1)).await {
            Ok(()) => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "broker 1 could not leave: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    for id in [2, 3, 4] {
        until("sees three brokers", &cluster.running[&id], |r| {
            r.cluster()
                .is_some_and(|c| c.brokers.iter().all(|b| b.id != 1) && c.brokers.len() == 3)
        })
        .await;
    }
    cluster.stop(1).await;
    for p in 0..3 {
        cluster.append("t", p, batch(1, 9)).await;
        // Partition 0 took one more batch, after its move.
        copy_holds(&four, "t", p, if p == 0 { 7 } else { 6 }).await;
    }
    cluster.stop_all().await;
}

/// A certificate authority, and certificates it signs for brokers at 127.0.0.1 (each usable both ways).
struct Authority {
    ca: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
}

impl Authority {
    fn new() -> Self {
        let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        Self {
            ca: rcgen::CertifiedIssuer::self_signed(ca, rcgen::KeyPair::generate().unwrap()).unwrap(),
        }
    }

    fn broker(&self) -> ReplicationTls {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &*self.ca).unwrap();
        ReplicationTls::from_pem(
            cert.pem().as_bytes(),
            key.serialize_pem().as_bytes(),
            self.ca.pem().as_bytes(),
        )
        .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brokers_speak_tls_and_one_from_another_authority_never_gets_in() {
    let authority = Authority::new();
    let mut cluster = Cluster::new(3, 3, 10_000);
    for id in 1..=3 {
        cluster.tls.insert(id, authority.broker());
    }
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(3))).await.unwrap();
    for p in 0..3 {
        cluster.append("t", p, batch(1, 0)).await;
    }
    for r in cluster.running.values() {
        for p in 0..3 {
            copy_holds(r, "t", p, 1).await;
        }
    }
    // A broker whose certificate another authority signed is refused at every connection: it never joins.
    cluster.tls.insert(4, Authority::new().broker());
    cluster.add_broker(4).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    for id in 1..=3 {
        let view = cluster.running[&id].cluster().unwrap();
        assert_eq!(view.brokers.len(), 3, "broker {id} lets the stranger in");
    }
    cluster.stop_all().await;
}

/// Every batch of a partition from offset 0, read through `r` (its leader), as base offsets.
async fn every_offset(r: &Arc<Replica>, topic: &'static str, partition: i32) -> Vec<i64> {
    let r = Arc::clone(r);
    tokio::task::spawn_blocking(move || {
        let mut offsets = Vec::new();
        let mut next = 0;
        loop {
            let read = r.read(topic, partition, next, 1).unwrap();
            if read.batches.is_empty() || next >= read.offsets.high_watermark {
                return offsets;
            }
            for b in &read.batches {
                let base = i64::from_be_bytes(b[..8].try_into().unwrap());
                offsets.push(base);
                next = base + 1;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_segments_go_to_object_storage_and_are_read_back_from_there() {
    let (Ok(endpoint), Ok(access), Ok(secret)) = (
        std::env::var("OPENDUCT_TEST_S3"),
        std::env::var("OPENDUCT_TEST_S3_ACCESS"),
        std::env::var("OPENDUCT_TEST_S3_SECRET"),
    ) else {
        eprintln!(
            "skipped: set OPENDUCT_TEST_S3, OPENDUCT_TEST_S3_ACCESS and OPENDUCT_TEST_S3_SECRET (cargo xtask services up)"
        );
        return;
    };
    let s3 = Arc::new(
        S3::new(S3Settings {
            bucket: format!("openduct-tier-{}", std::process::id()),
            region: "us-east-1".to_owned(),
            endpoint: Some(endpoint),
            keys: Some((access, secret)),
        })
        .unwrap(),
    );
    s3.create_bucket().await.unwrap();
    let mut cluster = Cluster::new(3, 3, 40);
    cluster.tier = Some(TierConfig {
        s3,
        prefix: "test".to_owned(),
        local_retention_bytes: 0,
        local_retention_ms: None,
        every: Duration::from_millis(200),
    });
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(1))).await.unwrap();
    for i in 0..100 {
        cluster.append("t", 0, batch(1, i)).await;
    }
    // The leader stores closed segments and every copy drops its local ones: the old offsets are remote only.
    let leader = cluster.leader("t", 0).await;
    until("stores and trims its segments", &leader, |r| {
        let log = r.local_partition("t", 0).unwrap();
        log.tiered().len() >= 5 && log.local_segments().iter().all(|s| !s.tiered)
    })
    .await;
    let expected: Vec<i64> = (0..100).collect();
    assert_eq!(every_offset(&leader, "t", 0).await, expected);

    // A new leader reads the stored segments as well.
    let lost = leader.id();
    cluster.stop(lost).await;
    let leader = cluster.leader("t", 0).await;
    assert_eq!(every_offset(&leader, "t", 0).await, expected);

    // A broker back with an empty disk is rebuilt from a snapshot that lists the stored segments.
    std::fs::remove_dir_all(cluster.dir(lost)).unwrap();
    cluster.start(lost).await;
    let rebuilt = Arc::clone(&cluster.running[&lost]);
    let leader_log = leader.local_partition("t", 0).unwrap();
    until("knows the stored segments", &rebuilt, move |r| {
        r.local_partition("t", 0)
            .is_some_and(|log| log.tiered() == leader_log.tiered())
    })
    .await;
    let log = rebuilt.local_partition("t", 0).unwrap();
    let first = tokio::task::spawn_blocking(move || log.read(0, 1).unwrap().batches)
        .await
        .unwrap();
    assert_eq!(i64::from_be_bytes(first[0][..8].try_into().unwrap()), 0);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_feature_level_rises_once_every_broker_has_it_and_an_older_build_is_then_kept_out() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    // Brokers 1 and 2 run a build of level 2; broker 3 still runs the build before (level 1).
    for id in 1..=3 {
        cluster.levels.insert(id, if id == 3 { 1 } else { 2 });
    }
    cluster.start_all().await;
    cluster.coordinator().await;
    for r in cluster.running.values() {
        until("uses feature level 1", r, |r| r.feature_level() == 1).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        cluster.running.values().all(|r| r.feature_level() == 1),
        "raised before broker 3 had the build"
    );

    // Broker 3 is upgraded: once it registers its new build, the cluster moves to level 2.
    cluster.stop(3).await;
    cluster.levels.insert(3, 2);
    cluster.start(3).await;
    for r in cluster.running.values() {
        until("uses feature level 2", r, |r| r.feature_level() == 2).await;
    }

    // Broker 3 goes back to the old build: the level stays, the old build gets nothing and serves nothing.
    cluster.stop(3).await;
    cluster.levels.insert(3, 1);
    cluster.start(3).await;
    let old = Arc::clone(&cluster.running[&3]);
    let coordinator = cluster
        .running
        .values()
        .find(|r| r.id() != 3 && r.coordinator().is_some_and(|c| c != 3) && r.coordinator() == Some(r.id()))
        .cloned();
    let coordinator = match coordinator {
        Some(c) => c,
        None => {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let c = cluster.running[&1].coordinator().filter(|c| *c != 3);
                if let Some(c) = c {
                    break Arc::clone(&cluster.running[&c]);
                }
                assert!(
                    Instant::now() < deadline,
                    "brokers 1 and 2 elected no coordinator"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    on(&coordinator, |r| r.create_topic("after", topic(1)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        old.topic("after").is_none(),
        "the old build was sent what it cannot read"
    );
    assert!(matches!(
        on(&old, |r| r.create_topic("from-the-old-build", topic(1))).await,
        Err(StoreError::Unavailable(_))
    ));
    for id in [1, 2] {
        assert_eq!(cluster.running[&id].feature_level(), 2);
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_spread_over_coordinators_and_their_offsets_outlive_one() {
    let mut cluster = Cluster::new(3, 3, 10_000);
    cluster.start_all().await;
    let first = cluster.coordinator().await;
    on(&first, |r| r.create_topic("t", topic(1))).await.unwrap();
    // Every broker, not only the first: commits go through each.
    for id in [1, 2, 3] {
        until("makes the offsets topic", &cluster.running[&id], |r| {
            (0..16).all(|p| {
                r.partition_leader(openduct_raft::offsets::OFFSETS_TOPIC, p)
                    .is_some()
            })
        })
        .await;
    }
    let groups: Vec<String> = (0..24).map(|i| format!("g{i}")).collect();
    // Committed through any broker: forwarded to the group's coordinator.
    for (i, g) in groups.iter().enumerate() {
        let via = Arc::clone(&cluster.running[&(1 + (i as u64 % 3))]);
        let (g, offset) = (g.clone(), i as i64 * 10);
        let o = CommittedOffset {
            topic: "t".to_owned(),
            partition: 0,
            offset,
            metadata: None,
            commit_timestamp_ms: 1,
        };
        // As a client does: again while the group's offsets partition is between leaders.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let (g2, o) = (g.clone(), o.clone());
            match on(&via, move |r| r.commit_offsets(&g2, &[o])).await {
                Ok(()) => break,
                Err(e @ (StoreError::NotLeader { .. } | StoreError::Unavailable(_))) => {
                    assert!(Instant::now() < deadline, "{g}'s offsets were never taken: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("{g}'s offsets were refused: {e}"),
            }
        }
    }
    // Spread once each offsets partition's leadership has gone to its first replica (a few seconds after the topic
    // was made: any replica may win the first election).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let coordinators: BTreeSet<i32> = groups.iter().filter_map(|g| first.group_coordinator(g)).collect();
        if coordinators.len() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "every group on one coordinator: {coordinators:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Each broker lists the groups it coordinates, and only those: together, every group once (ListGroups).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut listed = Vec::new();
        let mut misplaced = Vec::new();
        for r in cluster.running.values() {
            let me = i32::try_from(r.id()).unwrap();
            for g in on(r, |r| r.groups()).await.unwrap() {
                if r.group_coordinator(&g) != Some(me) {
                    misplaced.push((r.id(), g.clone()));
                }
                listed.push(g);
            }
        }
        listed.sort();
        let mut want = groups.clone();
        want.sort();
        if listed == want && misplaced.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "listed {listed:?}, by a broker not coordinating them {misplaced:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The coordinator of g0 stops: its groups move to the brokers left, offsets and all.
    let gone = u64::try_from(first.group_coordinator("g0").unwrap()).unwrap();
    cluster.stop(gone).await;
    let survivor = Arc::clone(cluster.running.values().next().unwrap());
    for (i, g) in groups.iter().enumerate() {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let now = survivor.group_coordinator(g).and_then(|c| u64::try_from(c).ok());
            if let Some(c) = now.filter(|c| *c != gone)
                && let Some(r) = cluster.running.get(&c)
            {
                let g2 = g.clone();
                if let Ok(found) = on(r, move |r| r.committed_offsets(&g2)).await
                    && found.first().map(|o| o.offset) == Some(i as i64 * 10)
                {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{g}'s offsets did not survive its coordinator"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // A group's offsets deleted through any broker: gone from every copy of its offsets partition.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match on(&survivor, |r| r.delete_offsets("g5", None)).await {
            Ok(()) => break,
            Err(e @ (StoreError::NotLeader { .. } | StoreError::Unavailable(_))) => {
                assert!(Instant::now() < deadline, "g5 was never deleted: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("g5's deletion was refused: {e}"),
        }
    }
    let partition = openduct_raft::offsets::partition_of("g5", 16);
    for r in cluster.running.values() {
        let r = Arc::clone(r);
        until("drops g5's offsets", &r, move |r| {
            r.local_partition(openduct_raft::offsets::OFFSETS_TOPIC, partition)
                .is_none_or(|log| log.committed_offsets("g5").is_empty())
        })
        .await;
        let listed = on(&r, |r| r.groups()).await.unwrap();
        assert!(
            !listed.contains(&"g5".to_owned()),
            "broker {} still lists g5",
            r.id()
        );
    }
    cluster.stop_all().await;
}
