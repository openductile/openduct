// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Kafka error codes OpenDuct answers with, and how a store's refusals map onto them.

use openduct_storage::StoreError;
use openduct_storage::batch::BatchError;

pub(crate) const NONE: i16 = 0;
pub(crate) const OFFSET_OUT_OF_RANGE: i16 = 1;
pub(crate) const CORRUPT_MESSAGE: i16 = 2;
pub(crate) const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub(crate) const LEADER_NOT_AVAILABLE: i16 = 5;
pub(crate) const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub(crate) const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub(crate) const NOT_COORDINATOR: i16 = 16;
pub(crate) const INVALID_TOPIC_EXCEPTION: i16 = 17;
pub(crate) const INVALID_REQUIRED_ACKS: i16 = 21;
pub(crate) const ILLEGAL_GENERATION: i16 = 22;
pub(crate) const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub(crate) const INVALID_GROUP_ID: i16 = 24;
pub(crate) const UNKNOWN_MEMBER_ID: i16 = 25;
pub(crate) const INVALID_SESSION_TIMEOUT: i16 = 26;
pub(crate) const REBALANCE_IN_PROGRESS: i16 = 27;
pub(crate) const UNSUPPORTED_VERSION: i16 = 35;
pub(crate) const TOPIC_ALREADY_EXISTS: i16 = 36;
pub(crate) const INVALID_PARTITIONS: i16 = 37;
pub(crate) const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub(crate) const INVALID_CONFIG: i16 = 40;
pub(crate) const MESSAGE_TOO_LARGE: i16 = 10;
pub(crate) const INVALID_REQUEST: i16 = 42;
pub(crate) const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;
pub(crate) const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub(crate) const KAFKA_STORAGE_ERROR: i16 = 56;
pub(crate) const NON_EMPTY_GROUP: i16 = 68;
pub(crate) const GROUP_ID_NOT_FOUND: i16 = 69;
pub(crate) const FENCED_LEADER_EPOCH: i16 = 74;
pub(crate) const UNKNOWN_LEADER_EPOCH: i16 = 76;
pub(crate) const MEMBER_ID_REQUIRED: i16 = 79;
pub(crate) const GROUP_SUBSCRIBED_TO_TOPIC: i16 = 86;
pub(crate) const INVALID_RECORD: i16 = 87;
pub(crate) const PRODUCER_FENCED: i16 = 90;
pub(crate) const UNKNOWN_TOPIC_ID: i16 = 100;
pub(crate) const MISMATCHED_ENDPOINT_TYPE: i16 = 114;
pub(crate) const UNSUPPORTED_ENDPOINT_TYPE: i16 = 115;

/// The Kafka error code for a store's refusal or failure.
pub(crate) fn of_store_error(error: &StoreError) -> i16 {
    match error {
        StoreError::UnknownTopic(_) | StoreError::UnknownPartition { .. } => UNKNOWN_TOPIC_OR_PARTITION,
        StoreError::TopicExists(_) => TOPIC_ALREADY_EXISTS,
        StoreError::InvalidTopicName(_) => INVALID_TOPIC_EXCEPTION,
        StoreError::InvalidPartitions { .. } | StoreError::PartitionsNotAdded { .. } => INVALID_PARTITIONS,
        StoreError::OffsetOutOfRange { .. } => OFFSET_OUT_OF_RANGE,
        StoreError::OutOfOrderSequence => OUT_OF_ORDER_SEQUENCE_NUMBER,
        StoreError::ProducerFenced => PRODUCER_FENCED,
        StoreError::Batch(b) => match b {
            BatchError::CrcMismatch => CORRUPT_MESSAGE,
            BatchError::UnsupportedMagic(_) => UNSUPPORTED_FOR_MESSAGE_FORMAT,
            BatchError::Truncated
            | BatchError::InvalidLength(_)
            | BatchError::InvalidRecordCount { .. }
            | BatchError::Transactional => INVALID_RECORD,
        },
        StoreError::NotLeader { .. } => NOT_LEADER_OR_FOLLOWER,
        StoreError::Unavailable(_) => LEADER_NOT_AVAILABLE,
        StoreError::InvalidReplicas(_) => INVALID_REPLICA_ASSIGNMENT,
        StoreError::BatchTooLarge { .. } => MESSAGE_TOO_LARGE,
        StoreError::KeylessRecord(_) => INVALID_RECORD,
        StoreError::Unsupported(_) => INVALID_CONFIG,
        StoreError::Locked(_) | StoreError::Corrupt(_) | StoreError::Io(_) => KAFKA_STORAGE_ERROR,
    }
}
