// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A broker that starts with an empty disk while its cluster is up: new to the cluster, or back after losing what
//! it held. Only the second has anything to rebuild before its copies may vote.
//!
//! The cluster remembers every broker that has started in it: a broker registers its build with the controller
//! (`Command::Register`), and no broker starts a copy of any partition before the cluster has it on record
//! (`Inner::reconcile`). So a broker the controller's state names ran here before and may have held copies: it lost
//! its disk, and its copies of the partitions that existed catch up before they vote. A broker the state does not
//! name never held a copy of anything: its empty copies have nothing to lose and vote at once. It looks itself up
//! before it registers in this start, once its copy of the cluster's state has everything the cluster had committed
//! when it asked.
//!
//! Taking every broker that found its cluster up for one that lost its disk was wrong for a founder a few seconds
//! late: the cluster formed while it asked, and it then refused its vote to every partition made in those seconds.
//! With two such brokers of three, those partitions never had a leader (seen on CI, 2026-11-25).

use std::sync::Arc;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use tokio::time::{Instant, sleep};

use super::Inner;
use crate::net::Links;
use crate::replica::ReplicaConfig;

/// Where a broker that started with an empty disk stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmptyDisk {
    /// It kept its disk, or no cluster was up when it started: nothing to decide.
    No,
    /// The cluster was up, at this controller index; not yet known whether this broker ran in it before.
    Undecided { at: u64 },
    /// It ran in this cluster before: its copies of partitions made by `at` lost their data.
    Lost { at: u64 },
    /// New to the cluster: its copies hold nothing to lose. A partition moving to it still catches up first.
    New { at: u64 },
}

impl EmptyDisk {
    /// Settled from the cluster's state as of `at`: whether that state has this broker on record.
    pub(crate) fn decide(at: u64, on_record: bool) -> Self {
        if on_record {
            Self::Lost { at }
        } else {
            Self::New { at }
        }
    }

    /// Whether this broker's copy of a partition made at controller index `created_at` must catch up before it
    /// votes. `placed_here`: the broker is one of the partition's brokers (not one it is moving to).
    pub(crate) fn must_catch_up(self, created_at: u64, placed_here: bool) -> bool {
        match self {
            Self::No | Self::Undecided { .. } => false,
            Self::Lost { at } => created_at <= at,
            Self::New { at } => !placed_here && created_at <= at,
        }
    }

    /// Whether this broker may register in this start: not while it still has to look itself up.
    pub(crate) fn may_register(self) -> bool {
        !matches!(self, Self::Undecided { .. })
    }
}

/// For a broker starting with an empty disk: whether the cluster exists already - the highest index any other
/// broker's controller has committed - asking the others for a little while.
pub(super) async fn existing_cluster(links: &Links, config: &ReplicaConfig) -> Option<u64> {
    let deadline = std::time::Instant::now() + (config.timing.election_max * 2).max(Duration::from_secs(3));
    loop {
        let mut found = None;
        for (id, b) in &config.brokers {
            if *id != config.id
                && let Some(info) = links.probe(&b.raft).await
                && let Some(committed) = info.controller_committed
            {
                found = Some(found.map_or(committed, |f: u64| f.max(committed)));
            }
        }
        if found.is_some() || std::time::Instant::now() >= deadline {
            return found;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Settles an undecided broker once its copy of the cluster's state has applied everything up to `at`, and wakes
/// the partitions' reconcile, which waits for it.
pub(super) async fn settle(inner: Arc<Inner>, at: u64) {
    let mut said = Instant::now();
    loop {
        let applied = inner
            .controller
            .metrics()
            .borrow_watched()
            .last_applied
            .map(|l| l.index);
        if applied >= Some(at) {
            let on_record = inner.control.state().brokers.contains_key(&inner.id);
            *inner.empty_disk.lock() = EmptyDisk::decide(at, on_record);
            if on_record {
                tracing::warn!(
                    controller_index = at,
                    "this broker ran in this cluster before and lost its disk: its copies of the partitions that \
                     existed are rebuilt from the others before they vote"
                );
            } else {
                tracing::info!(
                    controller_index = at,
                    "this broker is new to its cluster: its copies hold nothing to rebuild and take part at once"
                );
            }
            inner.control.bump();
            return;
        }
        if said.elapsed() >= Duration::from_secs(30) {
            tracing::info!(
                controller_index = at,
                applied = ?applied,
                "this broker started with an empty disk and is still reading the cluster's state; it starts its \
                 partitions once it knows whether it ran here before"
            );
            said = Instant::now();
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_broker_on_record_rebuilds_the_copies_it_was_given_before_it_went() {
        let lost = EmptyDisk::decide(10, true);
        assert_eq!(lost, EmptyDisk::Lost { at: 10 });
        assert!(lost.must_catch_up(10, true), "made by then: it held it");
        assert!(lost.must_catch_up(3, false));
        assert!(
            !lost.must_catch_up(11, true),
            "made after it came back: nothing lost"
        );

        let new = EmptyDisk::decide(10, false);
        assert_eq!(new, EmptyDisk::New { at: 10 });
        assert!(
            !new.must_catch_up(10, true),
            "a founder's copy holds nothing to lose"
        );
        assert!(!new.must_catch_up(3, true));
        assert!(
            new.must_catch_up(3, false),
            "a partition moving to it catches up first"
        );
        assert!(!new.must_catch_up(11, false));

        for settled in [EmptyDisk::No, lost, new] {
            assert!(settled.may_register());
        }
        let undecided = EmptyDisk::Undecided { at: 10 };
        assert!(
            !undecided.may_register(),
            "it looks itself up before it registers"
        );
        assert!(!undecided.must_catch_up(3, true));
        assert!(!EmptyDisk::No.must_catch_up(3, true));
    }
}
