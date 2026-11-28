// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The cluster changing shape: brokers joining and leaving, partitions moving onto other brokers, and leaders going
//! back to their partition's first broker.
//!
//! A move is started by a controller command (`Reassign`): the brokers newly given the partition start its group
//! empty, and the partition's leader takes them in - as learners until they have caught up, then as voters, the
//! brokers given up leaving its group in the same change (Raft's joint consensus) - and then tells the controller the
//! move is done (`Reassigned`). The brokers given up then stop the group and remove its files. A leader that stops
//! part way leaves the move to the next one, which picks it up where it stands.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;

use super::Inner;
use crate::members::{Forwarded, Member, dismiss};
use crate::types::{Command, GroupId, PartitionRaft};

/// How many checks in a row a partition's first broker must be caught up before leadership goes back to it.
const STEADY_CHECKS: u32 = 2;

/// Moves partitions along and hands leaders back, on every broker, until it stops.
pub(super) async fn tend(inner: Arc<Inner>) {
    // At once: a broker starts no partition before the cluster has it on record (reconcile).
    inner.register_and_raise().await;
    let mut tick = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        inner.register_and_raise().await;
        inner.ensure_offsets_topic().await;
        inner.move_partitions();
        tick += 1;
        if tick.is_multiple_of(3) {
            inner.return_leaders().await;
        }
    }
}

impl Inner {
    /// For a broker new to the cluster (or back with an empty disk): asks the controller's leader to take it in,
    /// until it is a voter.
    pub(super) async fn join(self: Arc<Self>) {
        let Some(me) = self.config.brokers.get(&self.id).cloned() else {
            return;
        };
        loop {
            if self.voters().contains(&self.id) {
                return;
            }
            let mut asked: Vec<(u64, String)> = self
                .config
                .brokers
                .iter()
                .filter(|(id, _)| **id != self.id)
                .map(|(id, b)| (*id, b.raft.clone()))
                .collect();
            while let Some((target, addr)) = asked.pop() {
                match self
                    .links
                    .membership(target, &addr, self.id, Some(me.clone()))
                    .await
                {
                    Ok(()) => {
                        tracing::info!("this broker joined the cluster");
                        return;
                    }
                    Err(Forwarded::NotLeader { leader: Some(l) }) if l != target => {
                        if let Some(b) = self.config.brokers.get(&l) {
                            asked.push((l, b.raft.clone()));
                        }
                    }
                    Err(e) => tracing::debug!(error = ?e, "not taken into the cluster yet"),
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// On the controller's leader: makes the offsets topic once the cluster has the brokers for its copies (as many
    /// as the replication factor asks, or every broker it started with if fewer).
    async fn ensure_offsets_topic(&self) {
        use crate::offsets::OFFSETS_TOPIC;
        if self.controller.metrics().borrow_watched().current_leader != Some(self.id)
            || self.control.state().topics.contains_key(OFFSETS_TOPIC)
        {
            return;
        }
        let wanted = self
            .config
            .replication_factor
            .max(1)
            .min(self.config.brokers.len().max(1));
        if self.voters().len() < wanted {
            return;
        }
        let partitions = self
            .config
            .offsets_partitions
            .clamp(1, openduct_storage::MAX_PARTITIONS);
        let command = Command::CreateTopic {
            id: uuid::Uuid::new_v4(),
            name: OFFSETS_TOPIC.to_owned(),
            config: openduct_storage::TopicConfig {
                partitions,
                retention_ms: None,
            },
            replicas: self.place(partitions),
        };
        match self.write_async(command).await {
            Ok(_) => tracing::info!(
                partitions,
                "consumer groups' offsets now spread over {OFFSETS_TOPIC}"
            ),
            Err(e) => tracing::debug!(error = %e, "making the offsets topic goes on later"),
        }
    }

    /// Keeps this broker's build registered with the controller and, on the controller's leader, raises the
    /// cluster's feature level once every broker supports a higher one.
    async fn register_and_raise(&self) {
        // A broker that started with an empty disk first looks itself up in the cluster's record (empty_disk.rs).
        if !self.empty_disk.lock().may_register() {
            return;
        }
        let supported = self.members.levels().supported();
        let wanted = crate::controller::BrokerRecord {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            supported_level: supported,
        };
        let registered = self.control.state().brokers.get(&self.id) == Some(&wanted);
        if !registered {
            let command = Command::Register {
                id: self.id,
                version: wanted.version,
                supported_level: supported,
            };
            if let Err(e) = self.write_async(command).await {
                tracing::debug!(error = %e, "registration with the controller goes on later");
            }
            return;
        }
        if self.controller.metrics().borrow_watched().current_leader != Some(self.id) {
            return;
        }
        let (level, floor) = {
            let state = self.control.state();
            let floor = self
                .voters()
                .iter()
                .map(|v| state.brokers.get(v).map(|b| b.supported_level))
                .collect::<Option<Vec<u32>>>()
                .and_then(|levels| levels.into_iter().min());
            (state.level, floor)
        };
        if let Some(floor) = floor.filter(|f| *f > level) {
            tracing::info!(
                from = level,
                to = floor,
                "every broker supports a higher feature level: raising it"
            );
            if let Err(e) = self.write_async(Command::RaiseLevel { level: floor }).await {
                tracing::debug!(error = %e, "raising the feature level goes on later");
            }
        }
    }

    /// The controller's voters: the brokers of the cluster.
    pub(super) fn voters(&self) -> BTreeSet<u64> {
        self.controller
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    /// Takes broker `id` out of the controller's group, through its leader.
    pub(super) async fn leave(&self, id: u64) -> Result<(), Forwarded> {
        for _ in 0..50 {
            let leader = self.controller.metrics().borrow_watched().current_leader;
            let answer = match leader {
                Some(l) if l == self.id => dismiss(&self.controller, id).await,
                Some(l) => match self.broker(l) {
                    Some(b) => self.links.membership(l, &b.raft, id, None).await,
                    None => Err(Forwarded::NotLeader { leader: None }),
                },
                None => Err(Forwarded::NotLeader { leader: None }),
            };
            match answer {
                Err(Forwarded::NotLeader { .. }) => tokio::time::sleep(Duration::from_millis(100)).await,
                done => return done,
            }
        }
        Err(Forwarded::Unavailable("no broker leads the cluster".to_owned()))
    }

    /// The partitions moving, as the controller has them: their group, topic, partition and destination.
    fn moving(&self) -> Vec<(GroupId, String, i32, Vec<u64>)> {
        let state = self.control.state();
        state
            .topics
            .values()
            .flat_map(|t| {
                t.moving.iter().map(|(p, target)| {
                    let group = GroupId::Partition {
                        topic: t.info.id,
                        partition: *p,
                    };
                    (group, t.info.name.clone(), *p, target.clone())
                })
            })
            .collect()
    }

    /// Starts moving every partition this broker leads that the controller says is to move (one task each).
    fn move_partitions(self: &Arc<Self>) {
        for (group, topic, partition, target) in self.moving() {
            let Some(Member::Partition(raft, _)) = self.members.get(&group) else {
                continue;
            };
            if self.members.leader_of(&group).map(|(l, _)| l) != Some(self.id) {
                continue;
            }
            if !self.moves.lock().insert(group) {
                continue;
            }
            let inner = Arc::clone(self);
            tokio::spawn(async move {
                if let Err(e) = inner.move_one(&raft, &topic, partition, &target).await {
                    tracing::info!(%group, error = %e, "the move goes on later");
                }
                inner.moves.lock().remove(&group);
            });
        }
    }

    async fn move_one(
        &self,
        raft: &PartitionRaft,
        topic: &str,
        partition: i32,
        target: &[u64],
    ) -> Result<(), String> {
        let (voters, nodes): (BTreeSet<u64>, BTreeSet<u64>) = {
            let metrics = raft.metrics();
            let m = metrics.borrow_watched();
            let membership = m.membership_config.membership();
            (
                membership.voter_ids().collect(),
                membership.nodes().map(|(id, _)| *id).collect(),
            )
        };
        let wanted: BTreeSet<u64> = target.iter().copied().collect();
        if voters != wanted {
            // The new brokers first catch up as learners, so the group never waits on a copy far behind.
            for id in target.iter().filter(|id| !voters.contains(id)) {
                let broker = self
                    .broker(*id)
                    .ok_or_else(|| format!("broker {id} is not in the cluster"))?;
                if !nodes.contains(id) {
                    tracing::info!(%topic, partition, broker = id, "copying a partition to a broker");
                }
                raft.add_learner(*id, broker, true)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            raft.change_membership(wanted, false)
                .await
                .map_err(|e| e.to_string())?;
        }
        self.write_async(Command::Reassigned {
            topic: topic.to_owned(),
            partition,
            replicas: target.to_vec(),
        })
        .await
        .map_err(|e| e.to_string())?;
        tracing::info!(%topic, partition, replicas = ?target, "partition moved");
        Ok(())
    }

    /// Hands each partition this broker leads to its first broker, once that one has caught up for a while: after a
    /// broker comes back, the leaders it lost go back to it, and the work spreads again.
    async fn return_leaders(&self) {
        let preferred: BTreeMap<GroupId, u64> = {
            let state = self.control.state();
            state
                .topics
                .values()
                .flat_map(|t| {
                    t.replicas.iter().enumerate().filter_map(|(p, r)| {
                        let partition = i32::try_from(p).ok()?;
                        if t.moving.contains_key(&partition) {
                            return None;
                        }
                        let first = *r.first()?;
                        Some((
                            GroupId::Partition {
                                topic: t.info.id,
                                partition,
                            },
                            first,
                        ))
                    })
                })
                .collect()
        };
        let mut steady = self.steady.lock().clone();
        let mut kept = BTreeMap::new();
        // How long each partition has been led here: one taken over only now is not handed back yet - its first
        // broker most likely gave it away because it is stopping (Timing::hand_back_after).
        let now = tokio::time::Instant::now();
        let mut since = std::mem::take(&mut *self.leading_since.lock());
        let mut led = BTreeMap::new();
        for (group, member) in self.members.all() {
            if member.leader().0 == Some(self.id) {
                led.insert(group, since.remove(&group).unwrap_or(now));
            }
        }
        *self.leading_since.lock() = led.clone();
        for (group, first) in preferred {
            if first == self.id {
                continue;
            }
            let Some(Member::Partition(raft, _)) = self.members.get(&group) else {
                continue;
            };
            let caught_up = {
                let metrics = raft.metrics();
                let m = metrics.borrow_watched();
                m.current_leader == Some(self.id)
                    && m.membership_config.membership().voter_ids().any(|v| v == first)
                    && m.replication
                        .as_ref()
                        .and_then(|r| r.get(&first))
                        .is_some_and(|matched| matched.map(|l| l.index) >= m.last_log_index)
            };
            if !caught_up
                || led
                    .get(&group)
                    .is_none_or(|t| now.duration_since(*t) < self.config.timing.hand_back_after)
            {
                continue;
            }
            let checks = steady.remove(&group).unwrap_or(0) + 1;
            if checks >= STEADY_CHECKS {
                tracing::info!(%group, to = first, "handing leadership back to the partition's first broker");
                let _ = raft.trigger().transfer_leader(first).await;
            } else {
                kept.insert(group, checks);
            }
        }
        steady.clear();
        *self.steady.lock() = kept;
    }
}
