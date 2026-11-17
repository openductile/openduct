// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A store in memory: fast, and gone when the process ends. For tests, development and short-lived edge buffers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::batch::{BatchHeader, check_supported, parse_header, stored_form};
use crate::error::{StoreError, validate_partitions, validate_topic_name};
use crate::partition::{Admission, Location, PartitionIndex};
use crate::store::{
    Appended, CommittedOffset, EpochEnd, PartitionOffsets, ReadResult, SINGLE_BROKER_EPOCH, Store,
    TopicConfig, TopicInfo,
};

#[derive(Debug)]
struct Topic {
    info: TopicInfo,
    partitions: Vec<Mutex<PartitionIndex>>,
}

/// The in-memory store.
#[derive(Debug, Default)]
pub struct MemoryStore {
    topics: RwLock<HashMap<String, Topic>>,
    groups: Mutex<HashMap<String, Vec<CommittedOffset>>>,
    next_producer_id: AtomicI64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_partition<T>(
        &self,
        topic: &str,
        partition: i32,
        f: impl FnOnce(&mut PartitionIndex) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| StoreError::UnknownTopic(topic.to_owned()))?;
        let p = usize::try_from(partition)
            .ok()
            .and_then(|i| t.partitions.get(i))
            .ok_or_else(|| StoreError::UnknownPartition {
                topic: topic.to_owned(),
                partition,
            })?;
        let mut guard = p.lock();
        f(&mut guard)
    }
}

impl Store for MemoryStore {
    fn create_topic(&self, name: &str, config: TopicConfig) -> Result<(), StoreError> {
        validate_topic_name(name)?;
        validate_partitions(config.partitions)?;
        let mut topics = self.topics.write();
        if topics.contains_key(name) {
            return Err(StoreError::TopicExists(name.to_owned()));
        }
        let partitions = (0..config.partitions)
            .map(|_| Mutex::new(PartitionIndex::default()))
            .collect();
        topics.insert(
            name.to_owned(),
            Topic {
                info: TopicInfo {
                    id: uuid::Uuid::new_v4(),
                    name: name.to_owned(),
                    partitions: config.partitions,
                    retention_ms: config.retention_ms,
                },
                partitions,
            },
        );
        Ok(())
    }

    fn delete_topic(&self, name: &str) -> Result<(), StoreError> {
        if self.topics.write().remove(name).is_none() {
            return Err(StoreError::UnknownTopic(name.to_owned()));
        }
        for offsets in self.groups.lock().values_mut() {
            offsets.retain(|o| o.topic != name);
        }
        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        let mut out: Vec<TopicInfo> = self.topics.read().values().map(|t| t.info.clone()).collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn topic(&self, name: &str) -> Option<TopicInfo> {
        self.topics.read().get(name).map(|t| t.info.clone())
    }

    fn add_partitions(
        &self,
        name: &str,
        total: i32,
        _assignment: Option<Vec<Vec<i32>>>,
    ) -> Result<(), StoreError> {
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(name)
            .ok_or_else(|| StoreError::UnknownTopic(name.to_owned()))?;
        crate::validate_growth(name, topic.info.partitions, total)?;
        for _ in topic.info.partitions..total {
            topic.partitions.push(Mutex::new(PartitionIndex::default()));
        }
        topic.info.partitions = total;
        Ok(())
    }

    fn set_retention(&self, name: &str, retention_ms: Option<i64>) -> Result<(), StoreError> {
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(name)
            .ok_or_else(|| StoreError::UnknownTopic(name.to_owned()))?;
        topic.info.retention_ms = retention_ms;
        Ok(())
    }

    fn append(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Appended, StoreError> {
        let header = parse_header(&batch)?;
        check_supported(&header)?;
        self.with_partition(topic, partition, |index| match index.admit(&header)? {
            Admission::Duplicate(base_offset) => Ok(Appended {
                base_offset,
                log_start_offset: index.log_start,
                duplicate: true,
            }),
            Admission::New => {
                let base_offset = index.next_offset;
                let stored = stored_form(&batch, base_offset, SINGLE_BROKER_EPOCH);
                let size = stored.len();
                let header = BatchHeader {
                    partition_leader_epoch: SINGLE_BROKER_EPOCH,
                    ..header
                };
                index.record(&header, base_offset, size, Location::Memory(stored));
                Ok(Appended {
                    base_offset,
                    log_start_offset: index.log_start,
                    duplicate: false,
                })
            }
        })
    }

    fn read(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<ReadResult, StoreError> {
        self.with_partition(topic, partition, |index| {
            let batches = index
                .select(offset, max_bytes)?
                .into_iter()
                .filter_map(|e| match e.location {
                    Location::Memory(bytes) => Some(bytes),
                    Location::File { .. } => None,
                })
                .collect();
            Ok(ReadResult {
                batches,
                offsets: PartitionOffsets {
                    log_start: index.log_start,
                    high_watermark: index.next_offset,
                },
            })
        })
    }

    fn offsets(&self, topic: &str, partition: i32) -> Result<PartitionOffsets, StoreError> {
        self.with_partition(topic, partition, |index| {
            Ok(PartitionOffsets {
                log_start: index.log_start,
                high_watermark: index.next_offset,
            })
        })
    }

    fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<Option<(i64, i64)>, StoreError> {
        self.with_partition(
            topic,
            partition,
            |index| Ok(index.offset_for_timestamp(timestamp)),
        )
    }

    fn max_timestamp(&self, topic: &str, partition: i32) -> Result<Option<(i64, i64)>, StoreError> {
        self.with_partition(topic, partition, |index| Ok(index.max_timestamp()))
    }

    fn delete_records(&self, topic: &str, partition: i32, offset: i64) -> Result<i64, StoreError> {
        self.with_partition(topic, partition, |index| {
            let end = index.next_offset;
            let target = if offset == -1 { end } else { offset };
            if !(0..=end).contains(&target) {
                return Err(StoreError::OffsetOutOfRange {
                    requested: offset,
                    log_start: index.log_start,
                    high_watermark: end,
                });
            }
            index.truncate_before(target);
            Ok(index.log_start)
        })
    }

    fn epoch_of(&self, topic: &str, partition: i32, offset: i64) -> Result<i32, StoreError> {
        self.with_partition(topic, partition, |index| {
            Ok(index.epoch_of(offset, SINGLE_BROKER_EPOCH, index.next_offset))
        })
    }

    fn epoch_end(&self, topic: &str, partition: i32, epoch: i32) -> Result<EpochEnd, StoreError> {
        self.with_partition(topic, partition, |index| {
            Ok(EpochEnd {
                current: SINGLE_BROKER_EPOCH,
                end_offset: index.epoch_end(epoch, SINGLE_BROKER_EPOCH, index.next_offset),
            })
        })
    }

    fn next_producer_id(&self) -> Result<i64, StoreError> {
        Ok(self.next_producer_id.fetch_add(1, Ordering::SeqCst))
    }

    fn commit_offsets(&self, group: &str, offsets: &[CommittedOffset]) -> Result<(), StoreError> {
        let mut groups = self.groups.lock();
        let committed = groups.entry(group.to_owned()).or_default();
        for o in offsets {
            committed.retain(|c| !(c.topic == o.topic && c.partition == o.partition));
            committed.push(o.clone());
        }
        Ok(())
    }

    fn committed_offsets(&self, group: &str) -> Result<Vec<CommittedOffset>, StoreError> {
        Ok(self.groups.lock().get(group).cloned().unwrap_or_default())
    }

    fn delete_offsets(&self, group: &str, partitions: Option<&[(String, i32)]>) -> Result<(), StoreError> {
        let mut groups = self.groups.lock();
        let mut held: std::collections::BTreeMap<String, Vec<CommittedOffset>> = groups
            .remove(group)
            .map(|o| (group.to_owned(), o))
            .into_iter()
            .collect();
        crate::forget_offsets(&mut held, group, partitions);
        groups.extend(held);
        Ok(())
    }

    fn groups(&self) -> Result<Vec<String>, StoreError> {
        let mut out: Vec<String> = self.groups.lock().keys().cloned().collect();
        out.sort();
        Ok(out)
    }

    fn enforce_retention(&self, now_ms: i64) -> Result<usize, StoreError> {
        let topics = self.topics.read();
        let mut removed = 0usize;
        for t in topics.values() {
            let Some(retention) = t.info.retention_ms else {
                continue;
            };
            let cutoff = now_ms.saturating_sub(retention);
            for p in &t.partitions {
                let mut index = p.lock();
                // Keep the newest batch's offsets meaningful: remove whole batches older than the cutoff.
                let keep_from = index
                    .entries
                    .iter()
                    .find(|e| e.max_timestamp >= cutoff)
                    .map(|e| e.base_offset)
                    .unwrap_or(index.next_offset);
                let before = index.entries.len();
                index.truncate_before(keep_from);
                removed += before - index.entries.len();
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::tests::batch;

    fn config(partitions: i32) -> TopicConfig {
        TopicConfig {
            partitions,
            retention_ms: None,
        }
    }

    #[test]
    fn offsets_are_dense_and_reads_return_the_batches_as_stored() {
        let store = MemoryStore::new();
        store.create_topic("t", config(2)).unwrap();
        assert_eq!(
            store.append("t", 1, batch(3, -1, -1, -1, 0)).unwrap().base_offset,
            0
        );
        assert_eq!(
            store.append("t", 1, batch(2, -1, -1, -1, 0)).unwrap().base_offset,
            3
        );
        let read = store.read("t", 1, 0, usize::MAX).unwrap();
        assert_eq!(read.batches.len(), 2);
        assert_eq!(read.offsets.high_watermark, 5);
        assert_eq!(parse_header(&read.batches[1]).unwrap().base_offset, 3);
        assert!(matches!(
            store.append("t", 2, batch(1, -1, -1, -1, 0)),
            Err(StoreError::UnknownPartition { .. })
        ));
        assert!(matches!(
            store.create_topic("t", config(1)),
            Err(StoreError::TopicExists(_))
        ));
    }

    #[test]
    fn each_topic_has_its_own_id() {
        let store = MemoryStore::new();
        store.create_topic("a", config(1)).unwrap();
        store.create_topic("b", config(1)).unwrap();
        let (a, b) = (store.topic("a").unwrap().id, store.topic("b").unwrap().id);
        assert!(!a.is_nil() && !b.is_nil() && a != b);
    }

    #[test]
    fn retention_removes_whole_old_batches() {
        let store = MemoryStore::new();
        store
            .create_topic(
                "r",
                TopicConfig {
                    partitions: 1,
                    retention_ms: Some(1_000),
                },
            )
            .unwrap();
        store.append("r", 0, batch(1, -1, -1, -1, 1_000)).unwrap();
        store.append("r", 0, batch(1, -1, -1, -1, 5_000)).unwrap();
        assert_eq!(store.enforce_retention(5_500).unwrap(), 1);
        let offsets = store.offsets("r", 0).unwrap();
        assert_eq!((offsets.log_start, offsets.high_watermark), (1, 2));
    }

    #[test]
    fn a_deleted_topic_and_its_committed_offsets_are_gone() {
        let store = MemoryStore::new();
        store.create_topic("t", config(1)).unwrap();
        store.append("t", 0, batch(2, -1, -1, -1, 5)).unwrap();
        let committed = CommittedOffset {
            topic: "t".into(),
            partition: 0,
            offset: 2,
            metadata: None,
            commit_timestamp_ms: 0,
        };
        store.commit_offsets("g", &[committed]).unwrap();
        store.delete_topic("t").unwrap();
        assert!(store.topics().is_empty());
        assert!(store.committed_offsets("g").unwrap().is_empty());
        assert!(matches!(
            store.delete_topic("t"),
            Err(StoreError::UnknownTopic(_))
        ));
        store.create_topic("t", config(1)).unwrap();
        assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 0);
    }
}
