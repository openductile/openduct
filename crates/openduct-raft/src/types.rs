// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What brokers replicate: the commands that change the store, what each one answers, and who the brokers are.

use std::fmt;

use bytes::Bytes;
use openduct_storage::batch::BatchError;
use openduct_storage::{Appended, CommittedOffset, StoreError, TopicConfig};
use serde::{Deserialize, Serialize};

/// A broker as the others reach it: its replication address, and the address Kafka clients use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Broker {
    pub raft: String,
    pub kafka: String,
}

/// Which Raft group a message or a log belongs to: the controller (topics, producer ids, committed offsets), or one
/// partition (its batches).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum GroupId {
    Controller,
    Partition { topic: uuid::Uuid, partition: i32 },
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupId::Controller => f.write_str("controller"),
            GroupId::Partition { topic, partition } => write!(f, "partition {topic}/{partition}"),
        }
    }
}

/// A change to the cluster's state, in the log of one group. Every replica of the group applies the same commands in
/// the same order, and each command depends only on the ones before it - never on a clock or a random number of the
/// broker applying it - so every copy is the same.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// The controller's: a topic and, per partition, the brokers holding it (the first one is its preferred
    /// leader). Its id and placement are chosen once, by the broker that proposes it.
    CreateTopic {
        id: uuid::Uuid,
        name: String,
        config: TopicConfig,
        replicas: Vec<Vec<u64>>,
    },
    DeleteTopic {
        name: String,
    },
    NextProducerId,
    CommitOffsets {
        group: String,
        offsets: Vec<CommittedOffset>,
    },
    /// The controller's: a partition starts moving onto `replicas` (the first one to lead it).
    Reassign {
        topic: String,
        partition: i32,
        replicas: Vec<u64>,
    },
    /// The controller's: the partition's group is now `replicas` - the move is done.
    Reassigned {
        topic: String,
        partition: i32,
        replicas: Vec<u64>,
    },
    /// A partition's: a batch appended.
    Append {
        batch: Bytes,
    },
    /// A partition's: a closed segment is now in object storage.
    Tiered {
        segment: openduct_storage::TieredSegment,
    },
    /// A partition's: local copies of segments in object storage holding only offsets before this go.
    Offload {
        before: i64,
    },
    /// The controller's: a broker's build - its version and the feature level it supports - as it started.
    Register {
        id: u64,
        version: String,
        supported_level: u32,
    },
    /// The controller's: the cluster's feature level rises to this (never down); proposed once every broker of
    /// the cluster supports it.
    RaiseLevel {
        level: u32,
    },
    /// A partition's: segments whose newest batch is older than this removed. The time is the proposer's.
    Retain {
        before_ms: i64,
    },
    /// A group's committed offsets removed - of these topics and partitions, or every one of them - in the
    /// controller (offsets committed before the offsets topic existed) or in a partition of the offsets topic.
    DeleteOffsets {
        group: String,
        partitions: Option<Vec<(String, i32)>>,
    },
    /// The controller's: how long a topic keeps its batches (`None`: for ever).
    SetRetention {
        name: String,
        retention_ms: Option<i64>,
    },
    /// The controller's: partitions added to a topic, `total` in all after it - placed on these brokers (the first
    /// to lead each). Refused if the topic changed since it was proposed.
    AddPartitions {
        name: String,
        total: i32,
        replicas: Vec<Vec<u64>>,
    },
    /// A partition's: it starts at this offset from now on (DeleteRecords) - resolved by its leader, so every copy
    /// moves to the same one.
    DeleteRecords {
        before: i64,
    },
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::CreateTopic { name, config, .. } => {
                write!(f, "create topic {name} ({} partitions)", config.partitions)
            }
            Command::DeleteTopic { name } => write!(f, "delete topic {name}"),
            Command::NextProducerId => f.write_str("next producer id"),
            Command::CommitOffsets { group, offsets } => {
                write!(f, "commit {} offsets of group {group}", offsets.len())
            }
            Command::Reassign {
                topic,
                partition,
                replicas,
            } => write!(f, "move {topic}/{partition} onto {replicas:?}"),
            Command::Reassigned {
                topic,
                partition,
                replicas,
            } => write!(f, "{topic}/{partition} is on {replicas:?}"),
            Command::Append { batch } => write!(f, "append {} bytes", batch.len()),
            Command::Retain { before_ms } => write!(f, "remove what is older than {before_ms}"),
            Command::Tiered { segment } => {
                write!(
                    f,
                    "offsets {} to {} stored as {}",
                    segment.base, segment.last, segment.key
                )
            }
            Command::Offload { before } => write!(f, "drop local copies before offset {before}"),
            Command::Register {
                id,
                version,
                supported_level,
            } => write!(f, "broker {id} runs {version} (feature level {supported_level})"),
            Command::RaiseLevel { level } => write!(f, "the cluster's feature level rises to {level}"),
            Command::DeleteRecords { before } => write!(f, "records before offset {before} deleted"),
            Command::AddPartitions {
                name,
                total,
                replicas,
            } => {
                write!(
                    f,
                    "topic {name} grows to {total} partitions ({} new)",
                    replicas.len()
                )
            }
            Command::SetRetention { name, retention_ms } => match retention_ms {
                Some(ms) => write!(f, "topic {name} keeps batches {ms} ms"),
                None => write!(f, "topic {name} keeps batches for ever"),
            },
            Command::DeleteOffsets { group, partitions } => match partitions {
                Some(p) => write!(f, "delete {} offsets of group {group}", p.len()),
                None => write!(f, "delete group {group}'s offsets"),
            },
        }
    }
}

/// What a command answered on the broker that applied it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Done,
    Appended {
        base_offset: i64,
        log_start_offset: i64,
        duplicate: bool,
    },
    ProducerId(i64),
    Removed(u64),
    Refused(Refusal),
}

impl Reply {
    pub(crate) fn appended(self) -> Result<Appended, StoreError> {
        match self {
            Reply::Appended {
                base_offset,
                log_start_offset,
                duplicate,
            } => Ok(Appended {
                base_offset,
                log_start_offset,
                duplicate,
            }),
            other => Err(other.unexpected()),
        }
    }

    pub(crate) fn done(self) -> Result<(), StoreError> {
        match self {
            Reply::Done => Ok(()),
            other => Err(other.unexpected()),
        }
    }

    fn unexpected(self) -> StoreError {
        match self {
            Reply::Refused(r) => r.into_error(),
            other => StoreError::Corrupt(format!("the cluster answered {other:?} where it should not")),
        }
    }

    pub(crate) fn producer_id(self) -> Result<i64, StoreError> {
        match self {
            Reply::ProducerId(id) => Ok(id),
            other => Err(other.unexpected()),
        }
    }

    pub(crate) fn removed(self) -> Result<usize, StoreError> {
        match self {
            Reply::Removed(n) => Ok(usize::try_from(n).unwrap_or(usize::MAX)),
            other => Err(other.unexpected()),
        }
    }
}

/// A store's refusal of a command. It depends only on the commands before it, so every broker refuses alike, and it
/// travels back to the broker the client asked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refusal {
    UnknownTopic(String),
    UnknownPartition {
        topic: String,
        partition: i32,
    },
    TopicExists(String),
    InvalidTopicName(String),
    InvalidPartitions {
        requested: i32,
        max: i32,
    },
    OffsetOutOfRange {
        requested: i64,
        log_start: i64,
        high_watermark: i64,
    },
    OutOfOrderSequence,
    ProducerFenced,
    Batch(BatchError),
    /// A command sent to a group that does not take it (a partition's to the controller, or the reverse).
    Misrouted(String),
}

impl Refusal {
    /// The refusal a store error is - or `None` for a failure of this broker (its disk, its files), which is not the
    /// command's doing and stops the broker's copy instead.
    pub(crate) fn of(error: &StoreError) -> Option<Self> {
        Some(match error {
            StoreError::UnknownTopic(t) => Refusal::UnknownTopic(t.clone()),
            StoreError::UnknownPartition { topic, partition } => Refusal::UnknownPartition {
                topic: topic.clone(),
                partition: *partition,
            },
            StoreError::TopicExists(t) => Refusal::TopicExists(t.clone()),
            StoreError::InvalidTopicName(t) => Refusal::InvalidTopicName(t.clone()),
            StoreError::InvalidPartitions { requested, max } => Refusal::InvalidPartitions {
                requested: *requested,
                max: *max,
            },
            StoreError::OffsetOutOfRange {
                requested,
                log_start,
                high_watermark,
            } => Refusal::OffsetOutOfRange {
                requested: *requested,
                log_start: *log_start,
                high_watermark: *high_watermark,
            },
            StoreError::OutOfOrderSequence => Refusal::OutOfOrderSequence,
            StoreError::ProducerFenced => Refusal::ProducerFenced,
            StoreError::Batch(b) => Refusal::Batch(b.clone()),
            StoreError::NotLeader { .. }
            | StoreError::PartitionsNotAdded { .. }
            | StoreError::InvalidReplicas(_)
            | StoreError::Unavailable(_)
            | StoreError::Locked(_)
            | StoreError::Corrupt(_)
            | StoreError::Io(_) => return None,
        })
    }

    pub(crate) fn into_error(self) -> StoreError {
        match self {
            Refusal::UnknownTopic(t) => StoreError::UnknownTopic(t),
            Refusal::UnknownPartition { topic, partition } => {
                StoreError::UnknownPartition { topic, partition }
            }
            Refusal::TopicExists(t) => StoreError::TopicExists(t),
            Refusal::InvalidTopicName(t) => StoreError::InvalidTopicName(t),
            Refusal::InvalidPartitions { requested, max } => StoreError::InvalidPartitions { requested, max },
            Refusal::OffsetOutOfRange {
                requested,
                log_start,
                high_watermark,
            } => StoreError::OffsetOutOfRange {
                requested,
                log_start,
                high_watermark,
            },
            Refusal::OutOfOrderSequence => StoreError::OutOfOrderSequence,
            Refusal::ProducerFenced => StoreError::ProducerFenced,
            Refusal::Batch(b) => StoreError::Batch(b),
            Refusal::Misrouted(what) => {
                StoreError::Corrupt(format!("{what} reached a group that does not take it"))
            }
        }
    }
}

openraft::declare_raft_types!(
    /// The types OpenDuct's replication is made of.
    pub TypeConfig:
        D = Command,
        R = Reply,
        Node = Broker,
);

pub type LogId = openraft::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::alias::VoteOf<TypeConfig>;
pub type Entry = openraft::alias::EntryOf<TypeConfig>;
pub type StoredMembership = openraft::alias::StoredMembershipOf<TypeConfig>;
pub type SnapshotMeta = openraft::alias::SnapshotMetaOf<TypeConfig>;
/// The controller's Raft group, and a partition's.
pub type ControllerRaft = openraft::Raft<TypeConfig, crate::controller::ControllerMachine>;
pub type PartitionRaft = openraft::Raft<TypeConfig, crate::partition::PartitionMachine>;
