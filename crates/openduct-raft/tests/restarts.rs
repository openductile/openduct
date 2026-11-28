// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Restart drills, three brokers over real TCP with their own disks: whatever is restarted, however often and in
//! whatever order, every record that was written is read back from every copy, and the log goes on taking writes.
//!
//! Written after 2026-11-24, when a node replacement and its rollback restarted every broker twice and two of three
//! could not start their partitions again: a broker's own snapshot was removed at its second clean stop, with the
//! Raft log behind it already purged. No test restarted a broker more than once.

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

use common::{Cluster, batch, copy_holds, on, topic};

const PARTITIONS: i32 = 3;

/// A cluster with one topic, written to until every copy of every partition has taken a snapshot and removed the
/// log before it: from here a copy can be rebuilt on its broker only from that snapshot.
async fn written_and_purged() -> (Cluster, Vec<usize>) {
    let mut cluster = Cluster::new(3, 3, 20);
    cluster.start_all().await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(PARTITIONS)))
        .await
        .unwrap();
    let mut held = vec![0usize; usize::try_from(PARTITIONS).unwrap()];
    for i in 0..150 {
        write(&cluster, &mut held, i).await;
    }
    for r in cluster.running.values() {
        for p in 0..PARTITIONS {
            purged(r, p).await;
        }
    }
    (cluster, held)
}

async fn write(cluster: &Cluster, held: &mut [usize], i: i64) {
    let p = usize::try_from(i).unwrap() % held.len();
    cluster.append("t", i32::try_from(p).unwrap(), batch(1, i)).await;
    held[p] += 1;
}

async fn purged(r: &Arc<Replica>, partition: i32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done = r
            .partition_raft("t", partition)
            .is_some_and(|raft| raft.metrics().borrow_watched().purged.is_some());
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "broker {} never removed old log entries of t/{partition}",
            r.id()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every running broker's own copy of every partition holds exactly what was written, and they are the same bytes.
async fn every_copy_holds(cluster: &Cluster, held: &[usize], when: &str) {
    for p in 0..PARTITIONS {
        let want = held[usize::try_from(p).unwrap()];
        let mut seen = None;
        for r in cluster.running.values() {
            copy_holds(r, "t", p, want).await;
            let read = r.local_partition("t", p).unwrap().read(0, usize::MAX).unwrap();
            match &seen {
                None => seen = Some(read),
                Some(first) => assert_eq!(&read, first, "{when}: broker {} differs on t/{p}", r.id()),
            }
        }
    }
}

/// ONE DRILL AT A TIME. Each drill is a whole cluster - three brokers and some sixty partitions, every one with
/// its own elections and heartbeats. The test runner starts them all at once; ten clusters on a two-core machine
/// starve each other of the time their leaders need to be heard, and drills failed there with "no broker leads the
/// cluster" and partitions without a leader - on a throttled machine all ten fail together and all ten pass one
/// after another. A drill is about one cluster's restarts, not about ten sharing a machine, so each waits its turn.
static ONE_DRILL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn alone() -> tokio::sync::MutexGuard<'static, ()> {
    ONE_DRILL.lock().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_broker_stopped_cleanly_and_started_again_three_times_keeps_every_record() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    every_copy_holds(&cluster, &held, "before any restart").await;
    let mut i = 1000;
    for round in 1..=3 {
        // All of them at once, cleanly - what a node group's replacement, and then its rollback, does.
        cluster.stop_all().await;
        cluster.start_all().await;
        every_copy_holds(&cluster, &held, &format!("after restart {round} of all")).await;
        // And the log still takes writes, on every partition.
        for _ in 0..PARTITIONS {
            write(&cluster, &mut held, i).await;
            i += 1;
        }
        every_copy_holds(&cluster, &held, &format!("after writing in round {round}")).await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brokers_restarted_one_at_a_time_round_after_round_keep_every_record_and_take_writes_throughout() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    let mut i = 2000;
    for round in 1..=3 {
        for id in 1..=3u64 {
            cluster.stop(id).await;
            // Two of three are up: the log takes writes while the third is away.
            for _ in 0..PARTITIONS {
                write(&cluster, &mut held, i).await;
                i += 1;
            }
            cluster.start(id).await;
            every_copy_holds(&cluster, &held, &format!("round {round}, broker {id} back")).await;
        }
    }
    cluster.stop_all().await;
}

/// Every partition directory of a broker: `<data>/partitions/<topic id>/<partition>`.
fn partition_dirs(cluster: &Cluster, id: u64) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for topic in std::fs::read_dir(cluster.dir(id).join("partitions")).unwrap() {
        for partition in std::fs::read_dir(topic.unwrap().path()).unwrap() {
            out.push(partition.unwrap().path());
        }
    }
    out.sort();
    out
}

/// What 2026-11-24 left on two brokers' disks: the Raft log purged, and no snapshot. Answers how many copies
/// were left so (a partition that never took a snapshot - an internal topic nothing was written to - has none to
/// lose, and its log is whole).
fn lose_snapshots(cluster: &Cluster, id: u64) -> usize {
    let mut lost = 0;
    for dir in partition_dirs(cluster, id) {
        let snapshots = dir.join("store").join("snapshots");
        if std::fs::read_dir(&snapshots).is_ok_and(|mut d| d.next().is_some()) {
            std::fs::remove_dir_all(&snapshots).unwrap();
            lost += 1;
        }
    }
    assert!(
        lost >= usize::try_from(PARTITIONS).unwrap(),
        "broker {id} had only {lost} snapshots to lose"
    );
    lost
}

fn set_aside(cluster: &Cluster, id: u64) -> usize {
    std::fs::read_dir(cluster.dir(id).join("set-aside")).map_or(0, Iterator::count)
}

async fn no_copy_is_marked_recovering(cluster: &Cluster, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let marked: Vec<_> = partition_dirs(cluster, id)
            .into_iter()
            .filter(|d| d.join("recovering").exists())
            .collect();
        if marked.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "broker {id} still marks copies as recovering: {marked:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_that_cannot_be_rebuilt_is_set_aside_and_rebuilt_from_the_leader_by_itself() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    // Not broker 1, which starts the cluster.
    let damaged = 3;
    cluster.stop(damaged).await;
    let lost = lose_snapshots(&cluster, damaged);
    // Written while it is away: what it must also catch up with.
    for i in 3000..3009 {
        write(&cluster, &mut held, i).await;
    }
    cluster.start(damaged).await;
    every_copy_holds(&cluster, &held, "the damaged broker back").await;
    assert_eq!(
        set_aside(&cluster, damaged),
        lost,
        "each copy that could not be rebuilt is kept aside, not removed"
    );
    assert!(cluster.running[&damaged].partitions_not_started().is_empty());
    // Caught up, it is a full member again: nothing on its disk says otherwise...
    no_copy_is_marked_recovering(&cluster, damaged).await;
    // ...so the partition lives on with it and one other, the third away.
    cluster.stop(1).await;
    for i in 3100..3109 {
        write(&cluster, &mut held, i).await;
    }
    cluster.start(1).await;
    every_copy_holds(&cluster, &held, "after the rebuilt broker carried the partition").await;
    // And every broker restarted again, twice, changes nothing.
    for round in 1..=2 {
        cluster.stop_all().await;
        cluster.start_all().await;
        every_copy_holds(&cluster, &held, &format!("restart {round} after the repair")).await;
    }
    assert_eq!(set_aside(&cluster, damaged), lost, "nothing more was set aside");
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_being_rebuilt_is_still_one_after_a_restart_until_it_has_caught_up() {
    let _alone = alone().await;
    let (mut cluster, held) = written_and_purged().await;
    let broker = 3;
    cluster.stop(broker).await;
    // As a broker stopped half way through a rebuild leaves it: the mark, and whatever it had fetched.
    for dir in partition_dirs(&cluster, broker) {
        std::fs::File::create(dir.join("recovering")).unwrap();
    }
    cluster.start(broker).await;
    // It is read back at the start, and let go of once the copy is level with its leader - not before, not never.
    every_copy_holds(&cluster, &held, "the marked broker back").await;
    no_copy_is_marked_recovering(&cluster, broker).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_most_copies_beyond_rebuilding_nothing_is_removed_and_no_leader_is_made_up() {
    let _alone = alone().await;
    let (mut cluster, held) = written_and_purged().await;
    cluster.stop_all().await;
    // Two of three, as on 2026-11-24.
    let lost = [lose_snapshots(&cluster, 1), lose_snapshots(&cluster, 2)];
    cluster.start_all().await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    // The two set their copies aside and wait to be caught up. They vote for no one, so the one copy that has
    // everything is never outvoted by two that have nothing: no partition gets a leader with an empty log.
    for (id, lost) in [1u64, 2].into_iter().zip(lost) {
        assert_eq!(set_aside(&cluster, id), lost, "broker {id}");
    }
    let mut waiting = 0;
    for p in 0..PARTITIONS {
        let want = held[usize::try_from(p).unwrap()];
        // Who LEADS is the broker that says so of itself. (A broker may still name the leader it last voted
        // for; that is what it remembers, not a leader.)
        let leading: Vec<u64> = cluster
            .running
            .values()
            .filter(|r| r.partition_leader("t", p) == Some(r.id()))
            .map(|r| r.id())
            .collect();
        assert!(
            leading.iter().all(|l| *l == 3),
            "t/{p} is led by a broker that lost its copy: {leading:?}"
        );
        if leading == [3] {
            // The surviving broker led this partition before and still holds its term: it needs nobody's vote,
            // and the two emptied copies are caught up from it. The partition is whole again by itself.
            for r in cluster.running.values() {
                copy_holds(r, "t", p, want).await;
            }
        } else {
            // One of the lost brokers led it. The surviving copy cannot be elected by two that may not vote: the
            // partition waits - for a person to say the surviving copy is to be taken (the recovery setting).
            waiting += 1;
            let have = cluster.running[&3]
                .local_partition("t", p)
                .and_then(|log| log.read(0, usize::MAX).ok())
                .map_or(0, |read| read.batches.len());
            assert_eq!(have, want, "the surviving copy of t/{p} is whole");
        }
    }
    assert!(
        waiting > 0,
        "leaders were spread over three brokers: some partition was led by a lost one"
    );
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_a_persons_word_the_surviving_copy_leads_and_the_lost_ones_are_rebuilt_from_it() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    cluster.stop_all().await;
    lose_snapshots(&cluster, 1);
    lose_snapshots(&cluster, 2);
    // Without the word: the partitions the lost brokers led wait (the test above).
    cluster.start_all().await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    cluster.stop_all().await;

    // With it, on every broker: a recovery, named.
    cluster.accept_surviving_copies = Some("2026-11-24".to_owned());
    cluster.start_all().await;
    every_copy_holds(&cluster, &held, "the surviving copy accepted").await;
    for p in 0..PARTITIONS {
        // Nothing was lost: every write had reached the surviving broker before the others lost theirs.
        assert_eq!(
            cluster.leader("t", p).await.id(),
            3,
            "t/{p} is led by the copy that kept its log"
        );
    }
    for i in 5000..5009 {
        write(&cluster, &mut held, i).await;
    }
    no_copy_is_marked_recovering(&cluster, 1).await;
    no_copy_is_marked_recovering(&cluster, 2).await;

    // The recovery is written down as done on every broker, by itself.
    for id in 1..=3 {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !std::fs::read_to_string(cluster.dir(id).join("recoveries-done"))
            .is_ok_and(|done| done.lines().any(|l| l == "2026-11-24"))
        {
            assert!(
                Instant::now() < deadline,
                "broker {id} never wrote the recovery down as done"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // THE NAME IS LEFT IN THE SETTINGS, as it will be. A later loss - two brokers again - is NOT settled by it: the
    // partitions a lost broker led wait, exactly as with no word at all.
    cluster.stop_all().await;
    lose_snapshots(&cluster, 1);
    lose_snapshots(&cluster, 2);
    cluster.start_all().await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let waiting = (0..PARTITIONS)
        .filter(|p| {
            !cluster
                .running
                .values()
                .any(|r| r.partition_leader("t", *p) == Some(r.id()))
        })
        .count();
    assert!(waiting > 0, "a spent name settled a new loss");
    // A person decides again, with a new name.
    cluster.stop_all().await;
    cluster.accept_surviving_copies = Some("2026-11-25".to_owned());
    cluster.start_all().await;
    every_copy_holds(&cluster, &held, "the second recovery").await;

    // The word is taken away, and everything restarted twice: the log is as whole as it ever was, and the rebuilt
    // brokers are full members - the partition lives on without the one that saved it.
    cluster.accept_surviving_copies = None;
    for round in 1..=2 {
        cluster.stop_all().await;
        cluster.start_all().await;
        every_copy_holds(&cluster, &held, &format!("restart {round} without the word")).await;
    }
    cluster.stop(3).await;
    for i in 5100..5109 {
        write(&cluster, &mut held, i).await;
    }
    cluster.start(3).await;
    every_copy_holds(&cluster, &held, "the survivor back after the others carried on").await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_gives_away_what_it_leads_before_it_stops_so_its_partitions_are_never_leaderless() {
    let _alone = alone().await;
    // The restart drill in a test lane, 2026-11-24: brokers replaced one at a time, and each time some writes
    // were refused - the partitions the stopped broker led had no leader until the others elected one.
    let (mut cluster, mut held) = written_and_purged().await;
    let mut n = 20_000;
    for round in 1..=2 {
        for id in 1..=3u64 {
            for other in 1..=3 {
                becomes_ready(&cluster, other, "before a broker is stopped").await;
            }
            let left = cluster.running[&id].hand_over(Duration::from_secs(5)).await;
            assert_eq!(
                left, 0,
                "round {round}: broker {id} still leads {left} groups after handing over"
            );
            // `left == 0` above is the claim: for every group it led, the stopping broker has HEARD another broker
            // lead it, while still up itself - no election's wait in between. Seen from the others as well; a
            // moment is allowed, since on a slow machine a leader may be changing again for its own reasons.
            for p in 0..PARTITIONS {
                let deadline = Instant::now() + Duration::from_secs(2);
                while !cluster
                    .running
                    .values()
                    .any(|r| r.id() != id && r.partition_leader("t", p) == Some(r.id()))
                {
                    assert!(
                        Instant::now() < deadline,
                        "round {round}: nobody else leads t/{p} as broker {id} stops"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            // And each takes a write, sent as a client sends it: to whoever leads. Not "on the first try": this
            // cluster elects after 300 ms of silence, and a runner short of CPU goes silent for longer than that,
            // so a leader may change again for reasons that have nothing to do with the broker stopping (CI,
            // 2026-11-24: one write of eighteen met such a change).
            // Consecutive numbers: one write to each partition.
            for _ in 0..PARTITIONS {
                n += 1;
                write(&cluster, &mut held, n).await;
            }
            cluster.stop(id).await;
            // And while it is away.
            for i in 0..6 {
                write(
                    &cluster,
                    &mut held,
                    30_000 + i64::from(round) * 1000 + i64::try_from(id).unwrap() * 100 + i,
                )
                .await;
            }
            cluster.start(id).await;
        }
    }
    every_copy_holds(&cluster, &held, "two rounds of handed-over restarts").await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_broker_is_never_handed_its_partitions_back_and_gets_them_once_it_is_back() {
    let _alone = alone().await;
    // The restart drill in a test lane, 2026-11-24: a broker gave its partitions away as it began to stop, and the
    // leader hand-back gave every one of them back to it five seconds later - it was caught up, as a stopping broker
    // is. It led them while it was being killed, and a write found no leader. A broker that is leaving takes no
    // leadership, offered or by election, for however long it takes to stop.
    let (mut cluster, mut held) = written_and_purged().await;
    for id in 1..=3u64 {
        becomes_ready(&cluster, id, "before the drill").await;
    }
    let leaving = 1u64;
    assert_eq!(
        cluster.running[&leaving].hand_over(Duration::from_secs(5)).await,
        0
    );
    // Stays up well past the hand-back's wait and checks (test timing: 4 s, then two checks three seconds apart).
    let until = Instant::now() + Duration::from_secs(14);
    let mut n = 50_000;
    while Instant::now() < until {
        for p in 0..PARTITIONS {
            assert_ne!(
                cluster.running[&leaving].partition_leader("t", p),
                Some(leaving),
                "the leaving broker was handed t/{p} back"
            );
        }
        n += 1;
        write(&cluster, &mut held, n).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    cluster.stop(leaving).await;
    cluster.start(leaving).await;
    becomes_ready(&cluster, leaving, "back after stopping").await;
    // Back, and no longer leaving: a partition it is first broker of goes back to it once the wait is over.
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let leads = (0..PARTITIONS)
            .filter(|p| cluster.running[&leaving].partition_leader("t", *p) == Some(leaving))
            .count();
        if leads > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the broker back from a restart was never handed a partition"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    every_copy_holds(&cluster, &held, "after a broker left and came back").await;
    cluster.stop_all().await;
}

/// How many partitions some running broker leads by its own word.
fn led(cluster: &Cluster) -> usize {
    (0..PARTITIONS)
        .filter(|p| {
            cluster
                .running
                .values()
                .any(|r| r.partition_leader("t", *p) == Some(r.id()))
        })
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_no_copy_left_only_the_stronger_word_starts_the_partitions_again_and_they_start_empty() {
    let _alone = alone().await;
    // Production, 2026-11-24: every copy of eighteen partitions had lost what could rebuild it. They waited for
    // ever - under the first word too, which is for a copy that SURVIVED.
    let (mut cluster, _held) = written_and_purged().await;
    cluster.stop_all().await;
    for id in 1..=3 {
        lose_snapshots(&cluster, id);
    }
    cluster.accept_surviving_copies = Some("first-word".to_owned());
    cluster.start_all().await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(
        led(&cluster),
        0,
        "no copy kept its log: under the first word nobody may lead"
    );
    for id in 1..=3 {
        assert!(
            !cluster.running[&id].readiness().is_ready(),
            "broker {id} says it is ready with every partition leaderless"
        );
    }
    cluster.stop_all().await;

    // The stronger word, on every broker. Nothing happens before the wait is over...
    cluster.accept_surviving_copies = None;
    cluster.start_empty_where_none_survived = Some("2026-11-24".to_owned());
    let started = Instant::now();
    cluster.start_all().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        led(&cluster),
        0,
        "started empty before the wait for a surviving copy was over"
    );
    // ...then every partition is started again, empty, and every broker stands well.
    for id in 1..=3 {
        becomes_ready(&cluster, id, "started empty").await;
    }
    assert!(
        started.elapsed() >= Duration::from_secs(5),
        "the wait was cut short"
    );
    let mut held = vec![0usize; usize::try_from(PARTITIONS).unwrap()];
    every_copy_holds(&cluster, &held, "started empty").await;
    for id in 1..=3 {
        no_copy_is_marked_recovering(&cluster, id).await;
        // What was on each disk is kept, not removed.
        assert!(
            set_aside(&cluster, id) >= usize::try_from(PARTITIONS).unwrap(),
            "broker {id}"
        );
    }
    // They take writes, and keep them across a restart; the word, spent, does nothing more.
    for i in 0..30 {
        write(&cluster, &mut held, 9000 + i).await;
    }
    every_copy_holds(&cluster, &held, "written after the empty start").await;
    cluster.stop_all().await;
    cluster.start_all().await;
    every_copy_holds(&cluster, &held, "restarted, the word still given").await;
    for id in 1..=3 {
        becomes_ready(&cluster, id, "restarted").await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stronger_word_never_empties_a_partition_of_which_a_copy_kept_its_log() {
    let _alone = alone().await;
    let (mut cluster, held) = written_and_purged().await;
    cluster.stop_all().await;
    lose_snapshots(&cluster, 1);
    lose_snapshots(&cluster, 2);
    cluster.start_empty_where_none_survived = Some("with-a-survivor".to_owned());
    cluster.start_all().await;
    // The copy that kept its log leads, long before the wait is over, and the others are rebuilt from it:
    // every record is still there, on every broker.
    every_copy_holds(&cluster, &held, "a surviving copy under the stronger word").await;
    for p in 0..PARTITIONS {
        assert_eq!(
            cluster.leader("t", p).await.id(),
            3,
            "t/{p} is led by the copy that kept its log"
        );
    }
    // And it stays so well past the wait: nothing is started empty later.
    tokio::time::sleep(Duration::from_secs(7)).await;
    every_copy_holds(&cluster, &held, "after the wait").await;
    for id in 1..=3 {
        becomes_ready(&cluster, id, "a surviving copy").await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_word_changes_nothing_for_a_cluster_whose_copies_are_whole() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    cluster.stop_all().await;
    cluster.accept_surviving_copies = Some("a-drill".to_owned());
    cluster.start_all().await;
    every_copy_holds(&cluster, &held, "whole copies, with the word").await;
    for id in 1..=3 {
        assert_eq!(set_aside(&cluster, id), 0, "nothing is set aside on broker {id}");
    }
    for i in 6000..6009 {
        write(&cluster, &mut held, i).await;
    }
    every_copy_holds(&cluster, &held, "after writing").await;
    cluster.stop_all().await;
}

async fn becomes_ready(cluster: &Cluster, id: u64, when: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let r = cluster.running[&id].readiness();
        if r.is_ready() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{when}: broker {id} is not ready: {:?}",
            r.reasons()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_means_joined_started_led_and_level_not_merely_answering() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    for id in 1..=3 {
        becomes_ready(&cluster, id, "a whole cluster").await;
        let r = cluster.running[&id].readiness();
        assert!(r.held >= usize::try_from(PARTITIONS).unwrap());
        assert!(r.reasons().is_empty());
    }
    // One broker away: the two left still lead and follow every partition between them - they stay ready, which is
    // what lets exactly one broker be taken down at a time.
    cluster.stop(3).await;
    for i in 7000..7030 {
        write(&cluster, &mut held, i).await;
    }
    becomes_ready(&cluster, 1, "one broker away").await;
    becomes_ready(&cluster, 2, "one broker away").await;

    // It comes back having lost its copies: it answers at once, and is NOT ready until they are rebuilt.
    lose_snapshots(&cluster, 3);
    cluster.start(3).await;
    let just_back = cluster.running[&3].readiness();
    assert!(
        !just_back.is_ready(),
        "a broker with copies still to rebuild said it was ready: {just_back:?}"
    );
    assert!(!just_back.reasons().is_empty());
    becomes_ready(&cluster, 3, "the copies rebuilt").await;
    every_copy_holds(&cluster, &held, "the rebuilt broker ready").await;

    // Two brokers away: the one left leads nothing it can commit and its partitions have no leader soon after -
    // it is not ready, and says why.
    cluster.stop(1).await;
    cluster.stop(2).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = cluster.running[&3].readiness();
        if !r.is_ready() {
            assert!(!r.reasons().is_empty());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a broker alone of three still says it is ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.start(1).await;
    cluster.start(2).await;
    for id in 1..=3 {
        becomes_ready(&cluster, id, "everyone back").await;
    }
    cluster.stop_all().await;
}

/// Enough changes to the cluster's own state that every broker's copy of it took a snapshot and purged its log.
async fn cluster_state_purged(cluster: &Cluster) {
    let coordinator = cluster.coordinator().await;
    for n in 0..40 {
        let name = format!("extra-{n}");
        on(&coordinator, move |r| r.create_topic(&name, topic(1)))
            .await
            .unwrap();
    }
    for id in cluster.running.keys() {
        let purged = cluster.dir(*id).join("controller").join("raft").join("purged");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !purged.exists() {
            assert!(
                Instant::now() < deadline,
                "broker {id} never purged the cluster's own log"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_that_lost_its_copy_of_the_clusters_state_rebuilds_it_and_keeps_its_partitions() {
    let _alone = alone().await;
    let (mut cluster, mut held) = written_and_purged().await;
    cluster_state_purged(&cluster).await;
    // Broker 3, and then broker 1 - the one that starts a cluster when it finds none, and must not start another.
    for (round, lost) in [3u64, 1].into_iter().enumerate() {
        cluster.stop(lost).await;
        let snapshots = cluster
            .dir(lost)
            .join("controller")
            .join("state")
            .join("snapshots");
        assert!(std::fs::read_dir(&snapshots).unwrap().next().is_some());
        std::fs::remove_dir_all(&snapshots).unwrap();
        let partitions_before = partition_dirs(&cluster, lost).len();
        for i in 0..9 {
            write(
                &cluster,
                &mut held,
                8000 + i64::try_from(round).unwrap() * 100 + i,
            )
            .await;
        }
        cluster.start(lost).await;

        // Its copy of the cluster's state was set aside and rebuilt; the copies of its partitions were never
        // touched - not removed while it did not yet know what it holds - and are level again.
        every_copy_holds(&cluster, &held, &format!("broker {lost} back")).await;
        becomes_ready(&cluster, lost, "the cluster's state rebuilt").await;
        assert_eq!(
            set_aside(&cluster, lost),
            1,
            "only the cluster's state was set aside on broker {lost}"
        );
        assert_eq!(
            partition_dirs(&cluster, lost).len(),
            partitions_before,
            "broker {lost} removed partition directories while rebuilding the cluster's state"
        );
        assert!(!cluster.dir(lost).join("controller").join("recovering").exists());
        // One cluster, as before: every broker names the same coordinator, and it takes a new topic.
        // Asked again while the lead changes hands, as any client does: a broker that has just rejoined can
        // move it, and "no broker leads the cluster" for that moment is an answer to retry, not a fault. A
        // cluster that stays without one for twenty seconds is a fault, and fails here.
        let name = format!("after-{lost}");
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let coordinator = cluster.coordinator().await;
            let n = name.clone();
            match on(&coordinator, move |r| r.create_topic(&n, topic(1))).await {
                Ok(_) => break,
                Err(e) => assert!(
                    Instant::now() < deadline,
                    "broker {lost} back: the cluster never took a new topic: {e}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    // And all of them restarted twice.
    for round in 1..=2 {
        cluster.stop_all().await;
        cluster.start_all().await;
        every_copy_holds(&cluster, &held, &format!("restart {round}")).await;
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_that_does_not_know_what_it_holds_removes_nothing() {
    let _alone = alone().await;
    let (mut cluster, held) = written_and_purged().await;
    cluster_state_purged(&cluster).await;
    cluster.stop_all().await;
    // Broker 3 lost its copy of the cluster's state - and comes up ALONE: the others are not there to rebuild it
    // from, so for as long as this lasts it knows of no topic and of no partition it holds.
    let snapshots = cluster.dir(3).join("controller").join("state").join("snapshots");
    std::fs::remove_dir_all(&snapshots).unwrap();
    let before = partition_dirs(&cluster, 3);
    assert!(before.len() >= usize::try_from(PARTITIONS).unwrap());
    cluster.start(3).await;
    // Several of its passes over what it should hold (one every five seconds).
    tokio::time::sleep(Duration::from_secs(13)).await;
    assert!(!cluster.running[&3].readiness().is_ready());
    assert_eq!(
        partition_dirs(&cluster, 3),
        before,
        "a broker that did not know what it holds removed partition copies that were whole"
    );
    // The others come back: it learns what it holds again, and every copy on it is as it was.
    cluster.start(1).await;
    cluster.start(2).await;
    every_copy_holds(&cluster, &held, "the cluster back around the broker").await;
    becomes_ready(&cluster, 3, "the cluster's state rebuilt").await;
    assert_eq!(partition_dirs(&cluster, 3), before);
    cluster.stop_all().await;
}

/// Until the broker names `want` as how it started as to its disk (`Replica::empty_disk`).
async fn started_empty(cluster: &Cluster, id: u64, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let now = cluster.running[&id].empty_disk();
        if now == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "broker {id} started {now:?}, not {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A BROKER NEW TO ITS CLUSTER HAS NOTHING TO LOSE (2026-11-25). It starts with an empty disk while its cluster is
/// up, as a founder a few seconds late does - and its copies vote at once. It was taken for a broker that lost its
/// disk: it refused its vote to every partition made before it came, and with another broker away those partitions
/// had no leader (CI: the schema registry's topic, never led, with two late founders of three). A broker that did
/// run in the cluster and comes back with an empty disk is still one that lost it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_new_to_its_cluster_votes_at_once_and_one_back_with_an_empty_disk_still_lost_it() {
    let _alone = alone().await;
    let mut cluster = Cluster::new(3, 3, 1000);
    cluster.start(1).await;
    cluster.start(2).await;
    let coordinator = cluster.coordinator().await;
    on(&coordinator, |r| r.create_topic("t", topic(PARTITIONS)))
        .await
        .unwrap();
    let mut held = vec![0usize; usize::try_from(PARTITIONS).unwrap()];
    for i in 0..30 {
        write(&cluster, &mut held, i).await;
    }
    // Away, as if killed: the broker not leading the cluster's state, once it leads a partition of `t` - which
    // then has no leader, and only the new broker's vote makes one.
    let away = if coordinator.id() == 1 { 2 } else { 1 };
    let deadline = Instant::now() + Duration::from_secs(30);
    while (0..PARTITIONS).all(|p| cluster.running[&away].partition_leader("t", p) != Some(away)) {
        assert!(
            Instant::now() < deadline,
            "broker {away} never led a partition of t"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.stop(away).await;
    // Broker 3's first start: an empty disk, and its cluster up.
    cluster.start(3).await;
    started_empty(&cluster, 3, "new").await;
    for i in 30..60 {
        write(&cluster, &mut held, i).await;
    }
    no_copy_is_marked_recovering(&cluster, 3).await;
    every_copy_holds(&cluster, &held, "a new broker in place of one away").await;

    cluster.start(away).await;
    every_copy_holds(&cluster, &held, "every broker back").await;
    // Broker 3 has run here now: back with an empty disk, it lost it, and is rebuilt before its copies vote.
    cluster.stop(3).await;
    std::fs::remove_dir_all(cluster.dir(3)).unwrap();
    cluster.start(3).await;
    started_empty(&cluster, 3, "lost").await;
    for i in 60..90 {
        write(&cluster, &mut held, i).await;
    }
    every_copy_holds(&cluster, &held, "rebuilt after losing its disk").await;
    assert_eq!(cluster.running[&away].empty_disk(), "kept");
    cluster.stop_all().await;
}
