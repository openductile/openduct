// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a store is: topics, partitions of record batches, committed group offsets and producer ids. The broker
//! speaks the Kafka protocol on top of this; a store only keeps data safe and finds it again.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// A topic as it is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfig {
    pub partitions: i32,
    /// How long batches are kept, in milliseconds; `None` keeps them until deleted.
    pub retention_ms: Option<i64>,
}

/// A topic as it is listed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicInfo {
    /// The topic's own id, given when it is created and kept for its life: Kafka clients tell topics apart by it
    /// (a client that sees one id for every topic mixes their partitions up). A topic recreated under the same name
    /// gets a new one.
    #[serde(default)]
    pub id: uuid::Uuid,
    pub name: String,
    pub partitions: i32,
    pub retention_ms: Option<i64>,
}

/// Where an appended batch landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// The offset of the batch's first record - or, for a producer's retry, the offset it was stored at first.
    pub base_offset: i64,
    pub log_start_offset: i64,
    /// True when this was a retry of a batch already stored; nothing new was written.
    pub duplicate: bool,
}

/// The two ends of a partition: the first offset still kept, and the offset the next record will get.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionOffsets {
    pub log_start: i64,
    pub high_watermark: i64,
}

/// Batches read from a partition, exactly as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    pub batches: Vec<Bytes>,
    pub offsets: PartitionOffsets,
}

/// A consumer group's committed position on one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: i64,
}

/// An append under way: written in order, and answered once it may be acknowledged - for the file store once its
/// batch is on the device, for a replicated store once most brokers hold it. A store that answers an append at once
/// returns it [`Pending::ready`].
pub struct Pending(PendingState);

enum PendingState {
    Ready(Appended),
    Later(Box<dyn FnOnce() -> Result<Appended, StoreError> + Send>),
}

impl Pending {
    pub fn ready(appended: Appended) -> Self {
        Self(PendingState::Ready(appended))
    }

    pub fn later(wait: impl FnOnce() -> Result<Appended, StoreError> + Send + 'static) -> Self {
        Self(PendingState::Later(Box::new(wait)))
    }

    /// True when waiting returns at once.
    pub fn is_ready(&self) -> bool {
        matches!(self.0, PendingState::Ready(_))
    }

    /// Blocks until the append may be acknowledged; where it landed (or why the store could not keep it).
    pub fn wait(self) -> Result<Appended, StoreError> {
        match self.0 {
            PendingState::Ready(appended) => Ok(appended),
            PendingState::Later(wait) => wait(),
        }
    }
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            PendingState::Ready(a) => write!(f, "Pending(ready, {a:?})"),
            PendingState::Later(_) => f.write_str("Pending(waiting)"),
        }
    }
}

/// The internal topic holding consumer groups' committed offsets in a replicated store. Clients may not write,
/// create or delete it.
pub const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// Whether a topic is one of OpenDuct's own.
pub fn is_internal_topic(topic: &str) -> bool {
    topic == OFFSETS_TOPIC
}

/// One broker of a cluster, as clients reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerAddress {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

/// The brokers that share a replicated store, and the one coordinating it: consumer groups, and the topic list, are
/// that broker's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterView {
    /// This broker's id.
    pub me: i32,
    /// The coordinating broker, if one is known now (none during an election).
    pub coordinator: Option<i32>,
    pub brokers: Vec<BrokerAddress>,
}

impl ClusterView {
    pub fn coordinates(&self) -> bool {
        self.coordinator == Some(self.me)
    }
}

/// Where one partition of a replicated store lives: the brokers holding a copy, and the one leading it - which
/// takes its writes and answers its reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The leading broker, if one is known now (none during an election).
    pub leader: Option<i32>,
    /// The leader's term: it grows with every new leader (Kafka's leader epoch).
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
}

/// A partition on its way to other brokers: those it will be on (the first one leading), and those it is being added
/// to and removed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reassignment {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    pub adding: Vec<i32>,
    pub removing: Vec<i32>,
}

/// The epoch a partition's leader writes in when the partition is kept by one broker alone. A replicated partition's
/// epoch is its Raft group's term: it grows with every new leader.
pub const SINGLE_BROKER_EPOCH: i32 = 0;

/// Where a leader epoch ended in a partition (Kafka's OffsetForLeaderEpoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEnd {
    /// The partition's epoch now.
    pub current: i32,
    /// The first offset written in a later epoch, or the high watermark when none is; `None` for an epoch the
    /// partition cannot have (negative, or after `current`).
    pub end_offset: Option<i64>,
}

/// A place to keep OpenDuct's data. Every method is synchronous and may block on storage; the broker calls them off
/// its network threads.
///
/// Guarantees every store must give:
/// - `append` returns only once the batch is as durable as the store promises (for the file store: on disk, after
///   `fsync`, unless configured otherwise); a batch is either stored whole or not at all.
/// - offsets in a partition are dense, increase by one per record, and are never reused.
/// - `read` returns batches byte for byte as they were appended (with their base offset set).
pub trait Store: Send + Sync + std::fmt::Debug + 'static {
    fn create_topic(&self, name: &str, config: TopicConfig) -> Result<(), StoreError>;
    /// Delete a topic, its data and every group's committed offsets on it. Once this returns the topic is gone
    /// for good, across restarts; a crash part way leaves it either whole or gone, never half there.
    fn delete_topic(&self, name: &str) -> Result<(), StoreError>;
    fn topics(&self) -> Vec<TopicInfo>;
    fn topic(&self, name: &str) -> Option<TopicInfo>;
    /// Add partitions to a topic, `total` in all after it (CreatePartitions); each new partition starts empty.
    /// `assignment`, if given, names the brokers of each new partition (the first to lead it): a shared store places
    /// them there, a store kept by one broker has only itself.
    fn add_partitions(
        &self,
        name: &str,
        total: i32,
        assignment: Option<Vec<Vec<i32>>>,
    ) -> Result<(), StoreError>;
    /// Change how long a topic keeps its batches (`None`: for ever); retention acts on it from its next round.
    fn set_retention(&self, name: &str, retention_ms: Option<i64>) -> Result<(), StoreError>;

    /// Append one record batch (already split and checked by the broker). Applies the idempotent-producer rules.
    fn append(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Appended, StoreError>;
    /// Append in two steps: written now, in order - so the next append, from this request or the next one, follows
    /// it - and acknowledged once the returned [`Pending`] has been waited for. The broker writes a connection's next
    /// request while the last one waits, so their flushes (or their replication) are shared. The default is an
    /// append answered at once.
    fn append_begin(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Pending, StoreError> {
        self.append(topic, partition, batch).map(Pending::ready)
    }
    /// Batches holding `offset` and after, up to `max_bytes` (at least one batch when there is one).
    fn read(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<ReadResult, StoreError>;
    fn offsets(&self, topic: &str, partition: i32) -> Result<PartitionOffsets, StoreError>;
    /// The first offset at or after `timestamp` and its batch's timestamp; `None` when no batch is that new.
    fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<Option<(i64, i64)>, StoreError>;
    /// The offset and timestamp of the record with the largest timestamp, if any.
    fn max_timestamp(&self, topic: &str, partition: i32) -> Result<Option<(i64, i64)>, StoreError>;
    /// DeleteRecords: the partition starts at `offset` from now on (`-1`: its high watermark) - never past the high
    /// watermark, never moved back - and whole segments before it go. The new start.
    fn delete_records(&self, topic: &str, partition: i32, offset: i64) -> Result<i64, StoreError>;
    /// The leader epoch to answer beside `offset` in ListOffsets: the epoch of the batch holding it (past the last
    /// batch, the current epoch), never lower than the epoch it was written in.
    fn epoch_of(&self, topic: &str, partition: i32, offset: i64) -> Result<i32, StoreError>;
    /// OffsetForLeaderEpoch: the partition's current epoch, and where `epoch` ended - so a client can check that its
    /// position was not taken back.
    fn epoch_end(&self, topic: &str, partition: i32, epoch: i32) -> Result<EpochEnd, StoreError>;

    /// A producer id never handed out before, by this store, ever.
    fn next_producer_id(&self) -> Result<i64, StoreError>;

    fn commit_offsets(&self, group: &str, offsets: &[CommittedOffset]) -> Result<(), StoreError>;
    fn committed_offsets(&self, group: &str) -> Result<Vec<CommittedOffset>, StoreError>;
    /// Removes a group's committed offsets: of these topics and partitions, or (`None`) every one, so the group is
    /// gone once it has no members. Offsets not held are ignored.
    fn delete_offsets(&self, group: &str, partitions: Option<&[(String, i32)]>) -> Result<(), StoreError>;
    /// The groups with committed offsets kept here (a shared store: those this broker answers for, as the leader of
    /// their offsets), sorted - for ListGroups, which shows a group that only commits offsets too.
    fn groups(&self) -> Result<Vec<String>, StoreError>;

    /// Remove batches older than their topic's retention. Returns how many batches were removed.
    fn enforce_retention(&self, now_ms: i64) -> Result<usize, StoreError>;

    /// The brokers sharing this store and its coordinator; `None` for a store kept by this broker alone. Cheap: it
    /// never blocks.
    fn cluster(&self) -> Option<ClusterView> {
        None
    }

    /// The broker coordinating consumer group `group` (its joins, heartbeats and commits), if one is known now. A
    /// store kept by one broker coordinates every group itself.
    fn group_coordinator(&self, _group: &str) -> Option<i32> {
        self.cluster().and_then(|c| c.coordinator)
    }

    /// Where a partition of a shared store lives; `None` for a store kept by this broker alone (it leads them all).
    /// Cheap: it never blocks.
    fn placement(&self, _topic: &str, _partition: i32) -> Option<Placement> {
        None
    }

    /// Moves a partition of a shared store onto `replicas` (the first one to lead it). Answered once the move is
    /// under way; [`Store::reassignments`] tells when it is done.
    fn reassign(&self, _topic: &str, _partition: i32, _replicas: Vec<i32>) -> Result<(), StoreError> {
        Err(StoreError::InvalidReplicas(
            "a broker on its own holds every partition: there is nowhere to move one".to_owned(),
        ))
    }

    /// The partitions being moved now.
    fn reassignments(&self) -> Vec<Reassignment> {
        Vec::new()
    }

    /// Takes a broker out of a shared store's cluster - once no partition is on it any more.
    fn remove_broker(&self, _id: i32) -> Result<(), StoreError> {
        Err(StoreError::InvalidReplicas(
            "a broker on its own is the whole cluster".to_owned(),
        ))
    }
}

/// Removes `group`'s offsets of `partitions` (all of them when `None`) from a map of every group's, dropping the group
/// when none is left: one rule for every store.
pub fn forget_offsets(
    all: &mut std::collections::BTreeMap<String, Vec<CommittedOffset>>,
    group: &str,
    partitions: Option<&[(String, i32)]>,
) {
    match partitions {
        None => {
            all.remove(group);
        }
        Some(gone) => {
            if let Some(held) = all.get_mut(group) {
                held.retain(|o| !gone.iter().any(|(t, p)| *t == o.topic && *p == o.partition));
                if held.is_empty() {
                    all.remove(group);
                }
            }
        }
    }
}
