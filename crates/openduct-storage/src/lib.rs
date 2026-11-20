// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The OpenDuct log: topics of partitions of Kafka record batches, kept by a [`Store`].
//!
//! Two stores ship today: [`MemoryStore`] for tests, development and short-lived buffers, and [`FileStore`], a
//! durable log on local disk for a single node. Every store gives the same guarantees ([`Store`]): a write is
//! acknowledged only once it is as durable as the store promises, offsets are dense and never reused, and batches
//! come back byte for byte as they were written.

#![forbid(unsafe_code)]

mod admit;
pub mod batch;
pub mod codec;
mod compact;
mod error;
mod file;
mod memory;
mod partition;
mod records;
mod settings;
mod store;

pub use admit::{admitted, now_ms};
pub use compact::Passes;
pub use error::{MAX_PARTITIONS, StoreError, validate_growth, validate_topic_name};
pub use file::checkpoint::{Manifest, ManifestFile, checked_path, restore_checkpoint, sync_checkpoint};
pub use file::partition_log::PartitionLog;
pub use file::tier::{LocalSegment, Remote, TieredSegment};
pub use file::{FileStore, FileStoreConfig, FsyncPolicy, MAX_BATCH_BYTES};
pub use memory::MemoryStore;
pub use partition::{AbortedTransaction, ActiveProducer};
pub use records::{Opened, RecordView, RecordsError, Retained, open, records, retain};
pub use settings::{
    CLEANUP_POLICY, CREATE_TIME, DELETE_RETENTION_MS, LOG_APPEND_TIME, MAX_MESSAGE_BYTES,
    MESSAGE_TIMESTAMP_TYPE, MIN_COMPACTION_LAG_MS, MIN_INSYNC_REPLICAS, POLICY_BOTH, POLICY_COMPACT,
    POLICY_DELETE, RETENTION_BYTES, SEGMENT_MS, SettingKind, SettingSpec, TOPIC_SETTINGS, TopicSettings,
    check_setting, setting_spec, setting_text,
};
pub use store::{
    Appended, BrokerAddress, ClusterView, CommittedOffset, EpochEnd, PartitionOffsets, Pending, Placement,
    ReadResult, Reassignment, SINGLE_BROKER_EPOCH, Store, TopicConfig, TopicInfo,
};
pub use store::{OFFSETS_TOPIC, TRANSACTIONS_TOPIC, forget_offsets, is_internal_topic};
