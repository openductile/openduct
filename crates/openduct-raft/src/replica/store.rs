// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The [`Store`] a broker of a cluster serves: controller writes through its leader, a partition's appends and reads
//! on the partition's leader, everything else from this broker's own copy.

use std::time::{Duration, Instant};

use bytes::Bytes;
use openduct_storage::{
    Appended, ClusterView, CommittedOffset, EpochEnd, MAX_PARTITIONS, PartitionOffsets, Pending, Placement,
    ReadResult, Reassignment, Store, StoreError, TopicConfig, TopicInfo, validate_topic_name,
};
use openraft::error::ClientWriteError;
use openraft::impls::ProgressResponder;

use super::{Replica, TOPIC_WAIT, group_of, kafka_address};
use crate::controller::TopicRecord;
use crate::members::Forwarded;
use crate::offsets::OFFSETS_TOPIC;
use crate::types::Command;

impl Replica {
    /// Whether the cluster keeps transactions yet: a broker of the release before refuses their records and
    /// markers, so none is written until every broker reads them.
    fn transactions_in_use(&self, topic: &str) -> Result<(), StoreError> {
        let (active, needed) = (
            self.inner.members.levels().active(),
            crate::features::TRANSACTIONS_LEVEL,
        );
        if active < needed {
            return Err(StoreError::Unsupported(format!(
                "transactions need every broker of the cluster upgraded (feature level {needed}; the cluster is at \
                 {active}); {topic:?} is unchanged"
            )));
        }
        Ok(())
    }
}

impl Store for Replica {
    fn create_topic(&self, name: &str, config: TopicConfig) -> Result<(), StoreError> {
        // Checked here too, so nothing the controller would refuse is replicated.
        validate_topic_name(name)?;
        if !(1..=MAX_PARTITIONS).contains(&config.partitions) {
            return Err(StoreError::InvalidPartitions {
                requested: config.partitions,
                max: MAX_PARTITIONS,
            });
        }
        let replicas = self.inner.place(config.partitions);
        if replicas.iter().any(Vec::is_empty) {
            // A cluster just formed, its brokers not known yet: worth asking again in a moment.
            return Err(StoreError::Unavailable(
                "no broker of the cluster is ready yet to hold the topic's partitions".to_owned(),
            ));
        }
        self.inner
            .write(Command::CreateTopic {
                id: uuid::Uuid::new_v4(),
                name: name.to_owned(),
                config,
                replicas,
            })?
            .done()?;
        // Answered once every partition has a leader (or after a while): a client writes to it right away.
        let deadline = Instant::now() + TOPIC_WAIT;
        while Instant::now() < deadline {
            if (0..config.partitions).all(|p| self.partition_leader(name, p).is_some()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }

    fn delete_topic(&self, name: &str) -> Result<(), StoreError> {
        self.inner
            .write(Command::DeleteTopic {
                name: name.to_owned(),
            })?
            .done()
    }

    fn topics(&self) -> Vec<TopicInfo> {
        self.inner
            .control
            .state()
            .topics
            .values()
            .map(|t| t.info.clone())
            .collect()
    }

    fn topic(&self, name: &str) -> Option<TopicInfo> {
        self.inner
            .control
            .state()
            .topics
            .get(name)
            .map(|t| t.info.clone())
    }

    fn append(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Appended, StoreError> {
        self.append_begin(topic, partition, batch)?.wait()
    }

    fn append_begin(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Pending, StoreError> {
        let (raft, shared) = self.inner.leading(topic, partition)?;
        // What the topic's settings ask of a batch (openduct_storage admit.rs), decided here by the leader before
        // the batch is proposed: no copy ever holds one that was refused, and all hold the same bytes.
        let settings = self
            .inner
            .control
            .state()
            .topics
            .get(topic)
            .map(|t| t.info.settings.clone())
            .unwrap_or_default();
        let batch = openduct_storage::admitted(&settings, batch, openduct_storage::now_ms())?;
        // What a client may not send (a transaction's marker) is refused here, where clients' batches come in.
        let sent = openduct_storage::batch::parse_header(&batch)?;
        openduct_storage::batch::check_supported(&sent)?;
        if sent.is_transactional() {
            self.transactions_in_use(topic)?;
        }
        // segment.ms: the segment is closed first when its records already span that long - told to every copy,
        // in order before the batch, so their segments start at the same one.
        if self.inner.members.levels().active() >= crate::features::COMPACTION_LEVEL
            && let Ok(header) = openduct_storage::batch::parse_header(&batch)
            && shared.log().roll_due(settings.segment_ms(), header.max_timestamp)
        {
            self.inner
                .runtime
                .block_on(raft.client_write_ff(Command::Roll, None))
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        }
        let (responder, answer) = ProgressResponder::complete_only();
        // Proposed now, in order; answered once most replicas hold it and it is applied here.
        self.inner
            .runtime
            .block_on(raft.client_write_ff(Command::Append { batch }, Some(responder)))
            .map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let runtime = self.inner.runtime.clone();
        Ok(Pending::later(move || match runtime.block_on(answer) {
            Ok(Ok(written)) => written.data.appended(),
            Ok(Err(ClientWriteError::ForwardToLeader(f))) => Err(StoreError::NotLeader {
                leader: f.leader_node.map(|b| b.kafka),
            }),
            Ok(Err(e)) => Err(StoreError::Unavailable(e.to_string())),
            Err(_) => Err(StoreError::Unavailable(
                "replication stopped before the append was kept".to_owned(),
            )),
        }))
    }

    fn read(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<ReadResult, StoreError> {
        self.inner
            .leading(topic, partition)?
            .1
            .log()
            .read(offset, max_bytes)
    }

    fn end_transaction(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        marker: openduct_storage::batch::Marker,
        coordinator_epoch: i32,
    ) -> Result<Appended, StoreError> {
        self.transactions_in_use(topic)?;
        // The marker is a batch like any other to the partition's copies: its leader writes it - this broker, or
        // the one this one sends it to - and each copy stores the same bytes.
        let batch = openduct_storage::batch::marker_batch(
            producer_id,
            producer_epoch,
            marker,
            openduct_storage::now_ms(),
            coordinator_epoch,
        );
        let group = self.inner.group(topic, partition)?;
        self.inner
            .write_through_leader(group, Command::Append { batch })?
            .appended()
    }

    fn transactions_ready(&self) -> bool {
        self.inner.members.levels().active() >= crate::features::TRANSACTIONS_LEVEL
    }

    fn append_through_leader(
        &self,
        topic: &str,
        partition: i32,
        batch: Bytes,
    ) -> Result<Appended, StoreError> {
        let group = self.inner.group(topic, partition)?;
        self.inner
            .write_through_leader(group, Command::Append { batch })?
            .appended()
    }

    fn open_transactions(&self, topic: &str, partition: i32) -> Result<Vec<(i64, i16, i64)>, StoreError> {
        Ok(self.inner.leading(topic, partition)?.1.log().open_transactions())
    }

    fn active_producers(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<openduct_storage::ActiveProducer>, StoreError> {
        Ok(self.inner.leading(topic, partition)?.1.log().active_producers())
    }

    fn aborted_transactions(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        upto: i64,
    ) -> Result<Vec<openduct_storage::AbortedTransaction>, StoreError> {
        Ok(self
            .inner
            .leading(topic, partition)?
            .1
            .log()
            .aborted_transactions(from, upto))
    }

    fn offsets(&self, topic: &str, partition: i32) -> Result<PartitionOffsets, StoreError> {
        Ok(self.inner.leading(topic, partition)?.1.log().offsets())
    }

    fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<Option<(i64, i64)>, StoreError> {
        self.inner
            .leading(topic, partition)?
            .1
            .log()
            .offset_for_timestamp(timestamp)
    }

    fn max_timestamp(&self, topic: &str, partition: i32) -> Result<Option<(i64, i64)>, StoreError> {
        Ok(self.inner.leading(topic, partition)?.1.log().max_timestamp())
    }

    fn delete_records(&self, topic: &str, partition: i32, offset: i64) -> Result<i64, StoreError> {
        let (raft, shared) = self.inner.leading(topic, partition)?;
        // Resolved and checked here, on the leader, against what is committed: every copy then moves to the same
        // offset, and none past what it holds.
        let offsets = shared.log().offsets();
        let before = if offset == -1 {
            offsets.high_watermark
        } else {
            offset
        };
        if !(0..=offsets.high_watermark).contains(&before) {
            return Err(StoreError::OffsetOutOfRange {
                requested: offset,
                log_start: offsets.log_start,
                high_watermark: offsets.high_watermark,
            });
        }
        if before <= offsets.log_start {
            return Ok(offsets.log_start);
        }
        match self
            .inner
            .runtime
            .block_on(raft.client_write(Command::DeleteRecords { before }))
        {
            Ok(written) => written.data.done()?,
            Err(e) => return Err(StoreError::Unavailable(e.to_string())),
        }
        Ok(shared.log().offsets().log_start)
    }

    fn epoch_of(&self, topic: &str, partition: i32, offset: i64) -> Result<i32, StoreError> {
        let (epoch, shared) = self.inner.leading_in_epoch(topic, partition)?;
        Ok(shared.log().epoch_of(offset, epoch))
    }

    fn epoch_end(&self, topic: &str, partition: i32, epoch: i32) -> Result<EpochEnd, StoreError> {
        let (current, shared) = self.inner.leading_in_epoch(topic, partition)?;
        Ok(EpochEnd {
            current,
            end_offset: shared.log().epoch_end(epoch, current),
        })
    }

    fn next_producer_id(&self) -> Result<i64, StoreError> {
        self.inner.write(Command::NextProducerId)?.producer_id()
    }

    fn add_partitions(
        &self,
        name: &str,
        total: i32,
        assignment: Option<Vec<Vec<i32>>>,
    ) -> Result<(), StoreError> {
        let record = self.inner.record(name)?;
        let has = i32::try_from(record.replicas.len()).unwrap_or(i32::MAX);
        openduct_storage::validate_growth(name, has, total)?;
        let adding = usize::try_from(total - has).unwrap_or(0);
        let replicas = match assignment {
            None => self.inner.place(total - has),
            Some(given) => {
                let known: std::collections::BTreeSet<u64> =
                    self.inner.brokers().into_iter().map(|(id, _)| id).collect();
                if given.len() != adding {
                    return Err(StoreError::InvalidReplicas(format!(
                        "{adding} partitions are added but {} placements were given",
                        given.len()
                    )));
                }
                let mut out = Vec::with_capacity(adding);
                for brokers in given {
                    let ids: Vec<u64> = brokers.iter().filter_map(|b| u64::try_from(*b).ok()).collect();
                    let distinct: std::collections::BTreeSet<u64> = ids.iter().copied().collect();
                    if ids.is_empty() || ids.len() != brokers.len() || distinct.len() != ids.len() {
                        return Err(StoreError::InvalidReplicas(format!(
                            "{brokers:?} is not a list of distinct broker ids"
                        )));
                    }
                    if let Some(unknown) = ids.iter().find(|id| !known.contains(id)) {
                        return Err(StoreError::InvalidReplicas(format!(
                            "broker {unknown} is not in the cluster"
                        )));
                    }
                    out.push(ids);
                }
                out
            }
        };
        self.inner
            .write(Command::AddPartitions {
                name: name.to_owned(),
                total,
                replicas,
            })?
            .done()?;
        // Answered once every new partition has a leader (or after a while), as a new topic is.
        let deadline = Instant::now() + TOPIC_WAIT;
        while Instant::now() < deadline {
            if (has..total).all(|p| self.partition_leader(name, p).is_some()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }

    fn set_retention(&self, name: &str, retention_ms: Option<i64>) -> Result<(), StoreError> {
        self.inner
            .write(Command::SetRetention {
                name: name.to_owned(),
                retention_ms,
            })?
            .done()
    }

    fn set_topic_settings(
        &self,
        name: &str,
        settings: openduct_storage::TopicSettings,
    ) -> Result<(), StoreError> {
        // A broker of the version before could not read the command: only once every broker is upgraded.
        let (active, needed) = (
            self.inner.members.levels().active(),
            crate::features::TOPIC_SETTINGS_LEVEL,
        );
        if active < needed {
            return Err(StoreError::Unsupported(format!(
                "a topic's settings beside retention.ms need every broker of the cluster upgraded (feature level \
                 {needed}; the cluster is at {active}); {name:?} is unchanged"
            )));
        }
        // min.insync.replicas: a write is acknowledged once a majority of the partition's copies hold it, so any
        // number up to that majority is kept; more would be a promise nothing here keeps.
        let wanted = settings.min_insync_replicas();
        let majority = self
            .inner
            .control
            .state()
            .topics
            .get(name)
            .and_then(|t| t.replicas.iter().map(Vec::len).min())
            .map_or(1, |copies| copies / 2 + 1);
        if usize::try_from(wanted).unwrap_or(usize::MAX) > majority {
            return Err(StoreError::Unsupported(format!(
                "min.insync.replicas={wanted} is more than a majority of {name:?}'s copies ({majority}), which is \
                 what holds a write before it is acknowledged; {name:?} is unchanged"
            )));
        }
        // Compaction's settings need the release that compacts on every broker.
        let compaction = [
            openduct_storage::DELETE_RETENTION_MS,
            openduct_storage::MIN_COMPACTION_LAG_MS,
        ];
        let needed = crate::features::COMPACTION_LEVEL;
        if active < needed
            && (settings.compacts() || compaction.iter().any(|name| settings.own(name).is_some()))
        {
            return Err(StoreError::Unsupported(format!(
                "log compaction (cleanup.policy=compact and its settings) needs every broker of the cluster \
                 upgraded (feature level {needed}; the cluster is at {active}); {name:?} is unchanged"
            )));
        }
        self.inner
            .write(Command::SetTopicSettings {
                name: name.to_owned(),
                settings,
            })?
            .done()
    }

    fn delete_offsets(&self, group: &str, partitions: Option<&[(String, i32)]>) -> Result<(), StoreError> {
        let command = Command::DeleteOffsets {
            group: group.to_owned(),
            partitions: partitions.map(<[(String, i32)]>::to_vec),
        };
        // Where the group's offsets are: in the controller (committed before the offsets topic existed), in its
        // offsets partition, or both.
        if self.inner.control.state().offsets.contains_key(group) {
            self.inner.write(command.clone())?.done()?;
        }
        match self.inner.offsets_partition(group) {
            Some(partition) => self.inner.commit_through_leader(partition, command),
            None => Ok(()),
        }
    }

    fn commit_offsets(&self, group: &str, offsets: &[CommittedOffset]) -> Result<(), StoreError> {
        let command = Command::CommitOffsets {
            group: group.to_owned(),
            offsets: offsets.to_vec(),
        };
        let Some(partition) = self.inner.offsets_partition(group) else {
            // Before the offsets topic exists: through the controller.
            return self.inner.write(command)?.done();
        };
        self.inner.commit_through_leader(partition, command)
    }

    fn groups(&self) -> Result<Vec<String>, StoreError> {
        let state = self.inner.control.state();
        // Committed through the controller (before the offsets topic existed), and in the offsets partitions this
        // broker leads - the groups it answers for.
        let mut out: std::collections::BTreeSet<String> = state.offsets.keys().cloned().collect();
        let partitions = state.topics.get(OFFSETS_TOPIC).map_or(0, |t| t.info.partitions);
        drop(state);
        for p in 0..partitions {
            if self.partition_leader(OFFSETS_TOPIC, p) == Some(self.id())
                && let Some(log) = self.local_partition(OFFSETS_TOPIC, p)
            {
                out.extend(log.groups());
            }
        }
        Ok(out.into_iter().collect())
    }

    fn committed_offsets(&self, group: &str) -> Result<Vec<CommittedOffset>, StoreError> {
        let state = self.inner.control.state();
        // Committed before the offsets topic existed, or since: the newer of the two wins.
        let mut found: Vec<CommittedOffset> = state.offsets.get(group).cloned().unwrap_or_default();
        if let Some(partition) = self.inner.offsets_partition(group) {
            // This broker's copy: a client asks the group's coordinator, which leads the partition.
            let log = self.local_partition(OFFSETS_TOPIC, partition).ok_or_else(|| {
                StoreError::Unavailable(format!("this broker holds no copy of {group}'s offsets"))
            })?;
            for o in log.committed_offsets(group) {
                match found
                    .iter_mut()
                    .find(|f| f.topic == o.topic && f.partition == o.partition)
                {
                    Some(f) => *f = o,
                    None => found.push(o),
                }
            }
        }
        // Offsets on topics deleted since are not answered.
        found.retain(|o| state.topics.contains_key(&o.topic));
        Ok(found)
    }

    fn group_coordinator(&self, group: &str) -> Option<i32> {
        match self.inner.offsets_partition(group) {
            Some(partition) => self.placement(OFFSETS_TOPIC, partition).and_then(|p| p.leader),
            None => self.coordinator().and_then(|l| i32::try_from(l).ok()),
        }
    }

    fn enforce_retention(&self, now_ms: i64) -> Result<usize, StoreError> {
        // Each partition's leader decides by its clock, for every copy alike.
        let topics: Vec<TopicRecord> = self.inner.control.state().topics.values().cloned().collect();
        let mut removed = 0;
        let sized = self.inner.members.levels().active() >= crate::features::TOPIC_SETTINGS_LEVEL;
        for t in topics {
            // A topic that only compacts keeps its newest records for ever.
            let deletes = t.info.settings.deletes();
            let by_time = t
                .info
                .retention_ms
                .filter(|_| deletes)
                .map(|retention| Command::Retain {
                    before_ms: now_ms.saturating_sub(retention),
                });
            let keep = t.info.settings.retention_bytes().filter(|_| sized && deletes);
            if by_time.is_none() && keep.is_none() {
                continue;
            }
            for p in 0..t.info.partitions {
                let Ok((raft, shared)) = self.inner.leading(&t.info.name, p) else {
                    continue;
                };
                // By size only when the local disk alone could be over the limit... or older data is remote:
                // a partition well under its limit costs no entry in its log each round.
                let by_size = keep
                    .filter(|keep| {
                        let log = shared.log();
                        log.local_bytes() >= *keep || !log.tiered().is_empty()
                    })
                    .map(|keep| Command::RetainBytes { keep });
                for command in by_time.iter().cloned().chain(by_size) {
                    match self.inner.runtime.block_on(raft.client_write(command)) {
                        Ok(written) => removed += written.data.removed()?,
                        Err(e) => {
                            tracing::debug!(topic = %t.info.name, partition = p, error = %e, "retention left for later")
                        }
                    }
                }
            }
        }
        Ok(removed)
    }

    fn compact(&self, now_ms: i64) -> Result<usize, StoreError> {
        if self.inner.members.levels().active() < crate::features::COMPACTION_LEVEL {
            return Ok(0);
        }
        // Each partition's leader decides, by its clock and the settings it sees, for every copy alike.
        let topics: Vec<TopicRecord> = self.inner.control.state().topics.values().cloned().collect();
        let mut removed = 0;
        for t in topics.iter().filter(|t| t.info.settings.compacts()) {
            let (lag_ms, delete_retention_ms) = (
                t.info.settings.min_compaction_lag_ms(),
                t.info.settings.delete_retention_ms(),
            );
            for p in 0..t.info.partitions {
                let Ok((raft, shared)) = self.inner.leading(&t.info.name, p) else {
                    continue;
                };
                // A pass with nothing to do costs no entry in the partition's log.
                if !shared.log().compaction_due(now_ms, lag_ms, delete_retention_ms) {
                    continue;
                }
                let command = Command::Compact {
                    now_ms,
                    lag_ms,
                    delete_retention_ms,
                };
                match self.inner.runtime.block_on(raft.client_write(command)) {
                    Ok(written) => removed += written.data.removed()?,
                    Err(e) => {
                        tracing::debug!(topic = %t.info.name, partition = p, error = %e, "compaction left for later")
                    }
                }
            }
        }
        Ok(removed)
    }

    fn cluster(&self) -> Option<ClusterView> {
        // A broker still catching up (a learner) is listed once it is a voter: nothing can be placed on it before.
        let voters = self.inner.voters();
        let mut brokers: Vec<_> = self
            .inner
            .brokers()
            .into_iter()
            .filter(|(id, _)| voters.contains(id))
            .collect();
        let coordinator = self.coordinator();
        // Just after start, this broker may know who leads before its copy of the membership names every broker:
        // the leader is then one it was configured with. A view never names a coordinator it does not list.
        if let Some(c) = coordinator
            && !brokers.iter().any(|(id, _)| *id == c)
            && let Some(b) = self.inner.config.brokers.get(&c)
        {
            brokers.push((c, b.clone()));
        }
        // In its first moments a broker's copy of the membership names no one yet. It answers for itself then: a
        // client told of no broker at all has no one to ask again (librdkafka's admin client waits for good).
        if brokers.is_empty()
            && let Some(b) = self.inner.config.brokers.get(&self.inner.id)
        {
            brokers.push((self.inner.id, b.clone()));
        }
        let listed = |c: &u64| brokers.iter().any(|(id, _)| id == c);
        Some(ClusterView {
            me: i32::try_from(self.inner.id).unwrap_or(-1),
            coordinator: coordinator.filter(listed).and_then(|l| i32::try_from(l).ok()),
            brokers: brokers
                .iter()
                .filter_map(|(id, b)| kafka_address(*id, b))
                .collect(),
        })
    }

    fn reassign(&self, topic: &str, partition: i32, replicas: Vec<i32>) -> Result<(), StoreError> {
        let brokers = self.inner.voters();
        let replicas: Vec<u64> = replicas
            .into_iter()
            .map(|r| u64::try_from(r).ok().filter(|r| brokers.contains(r)).ok_or(r))
            .collect::<Result<_, i32>>()
            .map_err(|r| StoreError::InvalidReplicas(format!("broker {r} is not in the cluster")))?;
        let mut distinct = replicas.clone();
        distinct.sort_unstable();
        distinct.dedup();
        if distinct.len() != replicas.len() {
            return Err(StoreError::InvalidReplicas(format!(
                "{replicas:?} names a broker more than once"
            )));
        }
        self.inner.group(topic, partition)?;
        self.inner
            .write(Command::Reassign {
                topic: topic.to_owned(),
                partition,
                replicas,
            })?
            .done()
    }

    fn reassignments(&self) -> Vec<Reassignment> {
        let ids = |v: &[u64]| {
            v.iter()
                .filter_map(|r| i32::try_from(*r).ok())
                .collect::<Vec<i32>>()
        };
        let state = self.inner.control.state();
        let mut out = Vec::new();
        for t in state.topics.values() {
            for (p, target) in &t.moving {
                let current = usize::try_from(*p)
                    .ok()
                    .and_then(|i| t.replicas.get(i))
                    .cloned()
                    .unwrap_or_default();
                let adding: Vec<u64> = target.iter().filter(|r| !current.contains(r)).copied().collect();
                let removing: Vec<u64> = current.iter().filter(|r| !target.contains(r)).copied().collect();
                out.push(Reassignment {
                    topic: t.info.name.clone(),
                    partition: *p,
                    replicas: ids(target),
                    adding: ids(&adding),
                    removing: ids(&removing),
                });
            }
        }
        out
    }

    fn remove_broker(&self, id: i32) -> Result<(), StoreError> {
        let id =
            u64::try_from(id).map_err(|_| StoreError::InvalidReplicas(format!("{id} is not a broker id")))?;
        if !self.inner.voters().contains(&id) {
            return Err(StoreError::InvalidReplicas(format!(
                "broker {id} is not in the cluster"
            )));
        }
        let held = {
            let state = self.inner.control.state();
            state
                .topics
                .values()
                .map(|t| {
                    t.replicas.iter().filter(|r| r.contains(&id)).count()
                        + t.moving.values().filter(|r| r.contains(&id)).count()
                })
                .sum::<usize>()
        };
        if held > 0 {
            return Err(StoreError::InvalidReplicas(format!(
                "broker {id} still holds {held} partitions: move them to other brokers first"
            )));
        }
        match self.inner.runtime.block_on(self.inner.leave(id)) {
            Ok(()) => Ok(()),
            Err(Forwarded::NotLeader { .. }) => {
                Err(StoreError::Unavailable("no broker leads the cluster".to_owned()))
            }
            Err(Forwarded::Unavailable(why)) => Err(StoreError::Unavailable(why)),
        }
    }

    fn placement(&self, topic: &str, partition: i32) -> Option<Placement> {
        let record = self.inner.record(topic).ok()?;
        let group = group_of(&record, partition).ok()?;
        let replicas = record.replicas.get(usize::try_from(partition).ok()?)?;
        let leader = self.inner.members.leader_of(&group);
        Some(Placement {
            leader: leader.and_then(|(l, _)| i32::try_from(l).ok()),
            leader_epoch: leader.map_or(-1, |(_, term)| crate::partition::epoch_of_term(term)),
            replicas: replicas.iter().filter_map(|r| i32::try_from(*r).ok()).collect(),
        })
    }
}
