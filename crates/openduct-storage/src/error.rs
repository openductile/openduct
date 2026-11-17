// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use crate::batch::BatchError;

/// Everything a store can refuse or fail at. The broker maps each to the Kafka error code a client understands.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("topic {0:?} does not exist")]
    UnknownTopic(String),
    #[error("topic {topic:?} has no partition {partition}")]
    UnknownPartition { topic: String, partition: i32 },
    #[error("topic {0:?} already exists")]
    TopicExists(String),
    #[error(
        "{0:?} is not a valid topic name: 1 to 249 of letters, digits, '.', '_' and '-', and not '.' or '..'"
    )]
    InvalidTopicName(String),
    #[error("a topic needs between 1 and {max} partitions, not {requested}")]
    InvalidPartitions { requested: i32, max: i32 },
    #[error(
        "the topic has {has} partitions: ask for more to add some, not {requested} (partitions are never taken away)"
    )]
    PartitionsNotAdded { has: i32, requested: i32 },
    #[error("offset {requested} is outside the log ({log_start} to {high_watermark})")]
    OffsetOutOfRange {
        requested: i64,
        log_start: i64,
        high_watermark: i64,
    },
    #[error("the batch's sequence number does not follow the producer's last one")]
    OutOfOrderSequence,
    #[error("the producer's epoch is older than one already seen: another producer has taken its place")]
    ProducerFenced,
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error("the data directory {0} is in use by another OpenDuct process")]
    Locked(PathBuf),
    /// A replicated store's write or partition read sent to a broker that does not lead it.
    #[error("this broker does not lead the cluster{}", .leader.as_deref().map(|l| format!("; broker {l} does")).unwrap_or_default())]
    NotLeader { leader: Option<String> },
    /// Brokers a partition cannot be placed on (unknown, repeated, none), or a broker that cannot leave yet.
    #[error("{0}")]
    InvalidReplicas(String),
    /// A replicated store that cannot reach most of its brokers, or has no leader yet.
    #[error("the cluster cannot take this now: {0}")]
    Unavailable(String),
    #[error("data is corrupt: {0}")]
    Corrupt(String),
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

/// The most partitions one topic may have: bounds memory and open files on small devices.
pub const MAX_PARTITIONS: i32 = 10_000;

/// Kafka's topic-name rules - which also keep a name safe to use as a directory name.
pub fn validate_topic_name(name: &str) -> Result<(), StoreError> {
    let ok = !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(StoreError::InvalidTopicName(name.to_owned()))
    }
}

/// Whether a topic of `has` partitions may grow to `total`: more than now, within the limit, and not the internal
/// offsets topic, whose partition count places every group.
pub fn validate_growth(name: &str, has: i32, total: i32) -> Result<(), StoreError> {
    if crate::is_internal_topic(name) {
        return Err(StoreError::InvalidReplicas(format!(
            "{name}'s partitions are fixed: every group's place depends on their number"
        )));
    }
    if total <= has {
        return Err(StoreError::PartitionsNotAdded {
            has,
            requested: total,
        });
    }
    validate_partitions(total)
}

pub(crate) fn validate_partitions(requested: i32) -> Result<(), StoreError> {
    if (1..=MAX_PARTITIONS).contains(&requested) {
        Ok(())
    } else {
        Err(StoreError::InvalidPartitions {
            requested,
            max: MAX_PARTITIONS,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_names_follow_kafkas_rules_and_cannot_escape_a_directory() {
        for good in ["events", "sensor-events", "a.b_c-1", &"x".repeat(249)] {
            assert!(validate_topic_name(good).is_ok(), "{good}");
        }
        for bad in ["", ".", "..", "../etc", "a/b", "a b", "é", &"x".repeat(250)] {
            assert!(validate_topic_name(bad).is_err(), "{bad}");
        }
    }
}
