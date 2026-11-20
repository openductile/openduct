// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One partition's log in a directory of its own: what a broker of a cluster keeps for each partition it holds a copy
//! of. The same segments, frames, recovery and producer rules as the file store's partitions; topics, groups and
//! producer ids are kept elsewhere (by the cluster).
//!
//! A checkpoint links its segments with their lengths, as the file store's does, and adds `producers.json`: the
//! producer table, which outlives the batches retention removes. A log opened from a restored checkpoint takes its
//! producers from that file (then removes it), so every copy of the partition decides a producer's retry alike.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use parking_lot::Mutex;

use super::checkpoint::{Manifest, ManifestFile};
use super::tier::{Remote, TIERED, TieredSegment};
use super::{FilePartition, FileStoreConfig, sync_dir};
use crate::error::StoreError;
use crate::partition::Producer;
use crate::store::{Appended, PartitionOffsets, Pending, ReadResult};

const PRODUCERS: &str = "producers.json";
/// Where the log starts, once DeleteRecords has moved it past what its segments show.
pub(super) const START: &str = "start.json";
/// Consumer groups' committed offsets, in a partition of the offsets topic (none elsewhere).
const OFFSETS: &str = "offsets.json";

/// The formats of a checkpoint's JSON files this build reads; it writes the newest.
const FORMATS: std::ops::RangeInclusive<u32> = 1..=1;

/// A checkpoint file's content with its format.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Versioned<T> {
    format: u32,
    value: T,
}

pub(super) fn write_versioned<T: serde::Serialize>(value: T) -> Result<Vec<u8>, StoreError> {
    serde_json::to_vec(&Versioned {
        format: *FORMATS.end(),
        value,
    })
    .map_err(|e| StoreError::Corrupt(format!("cannot encode a checkpoint file: {e}")))
}

pub(super) fn read_versioned<T: serde::de::DeserializeOwned>(
    path: &Path,
    raw: &[u8],
) -> Result<T, StoreError> {
    let v: Versioned<T> =
        serde_json::from_slice(raw).map_err(|e| StoreError::Corrupt(format!("{}: {e}", path.display())))?;
    if !FORMATS.contains(&v.format) {
        return Err(StoreError::Corrupt(format!(
            "{} is format {}; this build reads formats {} to {}: it was written by a newer OpenDuct",
            path.display(),
            v.format,
            FORMATS.start(),
            FORMATS.end()
        )));
    }
    Ok(v.value)
}

/// The segments in object storage, and the keys of those retention removed (still to delete).
#[derive(Debug, Default)]
pub(crate) struct Tier {
    pub(crate) segments: Vec<TieredSegment>,
    pub(crate) garbage: Vec<String>,
}

/// Removes from `segments` (in object storage) those holding only offsets before `start`; their keys, to delete.
fn drop_tiered_before(segments: &mut Vec<TieredSegment>, start: i64) -> Vec<String> {
    let (gone, kept): (Vec<_>, Vec<_>) = std::mem::take(segments).into_iter().partition(|s| s.last < start);
    *segments = kept;
    gone.into_iter().map(|s| s.key).collect()
}

/// One partition's log on disk.
#[derive(Debug)]
pub struct PartitionLog {
    dir: PathBuf,
    config: FileStoreConfig,
    pub(super) partition: Mutex<FilePartition>,
    pub(super) tier: Mutex<Tier>,
    /// Committed offsets by group: kept only in the partitions of the offsets topic.
    offsets: Mutex<std::collections::BTreeMap<String, Vec<crate::CommittedOffset>>>,
    pub(super) remote: Option<std::sync::Arc<dyn Remote>>,
    // Held for the log's life; dropping it releases the directory.
    _lock: File,
}

impl PartitionLog {
    /// Opens (or creates) the partition in `dir`, recovering its segments. Fails if another process holds it.
    pub fn open(dir: impl Into<PathBuf>, config: FileStoreConfig) -> Result<Self, StoreError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("LOCK"))?;
        if lock.try_lock().is_err() {
            return Err(StoreError::Locked(dir));
        }
        let mut partition = FilePartition::open(dir.clone())?;
        let producers = dir.join(PRODUCERS);
        match fs::read(&producers) {
            Ok(raw) => {
                let table: Vec<(i64, Producer)> = read_versioned(&producers, &raw)?;
                partition.index.set_producers(table);
                // Taken: from here on the log's own batches keep the table.
                fs::remove_file(&producers)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let tiered = dir.join(TIERED);
        let segments: Vec<TieredSegment> = match fs::read(&tiered) {
            Ok(raw) => {
                let list = read_versioned(&tiered, &raw)?;
                fs::remove_file(&tiered)?;
                list
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let offsets_file = dir.join(OFFSETS);
        let offsets = match fs::read(&offsets_file) {
            Ok(raw) => {
                let map = read_versioned(&offsets_file, &raw)?;
                fs::remove_file(&offsets_file)?;
                map
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => std::collections::BTreeMap::new(),
            Err(e) => return Err(e.into()),
        };
        if let Some(first) = segments.first() {
            // Offsets in object storage are still the log's.
            partition.index.log_start = partition.index.log_start.min(first.base);
        }
        let start_file = dir.join(START);
        let mut garbage = Vec::new();
        let mut segments = segments;
        match fs::read(&start_file) {
            Ok(raw) => {
                let start: i64 = read_versioned(&start_file, &raw)?;
                fs::remove_file(&start_file)?;
                garbage.extend(drop_tiered_before(&mut segments, start));
                partition.move_start(start)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(Self {
            dir,
            config,
            partition: Mutex::new(partition),
            tier: Mutex::new(Tier { segments, garbage }),
            offsets: Mutex::new(offsets),
            remote: None,
            _lock: lock,
        })
    }

    /// Reads offsets stored remotely through `remote`.
    pub fn with_remote(mut self, remote: std::sync::Arc<dyn Remote>) -> Self {
        self.remote = Some(remote);
        self
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Records a group's committed offsets (each replacing the one before for its topic and partition).
    pub fn commit_offsets(&self, group: &str, offsets: &[crate::CommittedOffset]) {
        let mut all = self.offsets.lock();
        let held = all.entry(group.to_owned()).or_default();
        for o in offsets {
            match held
                .iter_mut()
                .find(|h| h.topic == o.topic && h.partition == o.partition)
            {
                Some(h) => *h = o.clone(),
                None => held.push(o.clone()),
            }
        }
    }

    /// A group's committed offsets, as this copy holds them.
    pub fn committed_offsets(&self, group: &str) -> Vec<crate::CommittedOffset> {
        self.offsets.lock().get(group).cloned().unwrap_or_default()
    }

    /// Removes a group's offsets of these topics and partitions, or every one (`None`).
    pub fn delete_offsets(&self, group: &str, partitions: Option<&[(String, i32)]>) {
        crate::forget_offsets(&mut self.offsets.lock(), group, partitions);
    }

    /// The groups with offsets committed in this partition, sorted.
    pub fn groups(&self) -> Vec<String> {
        self.offsets.lock().keys().cloned().collect()
    }

    /// Appends a batch (already split and checked), applying the idempotent-producer rules, in the epoch of a
    /// partition kept by one broker.
    pub fn append(&self, batch: Bytes) -> Result<Appended, StoreError> {
        self.append_in(batch, crate::SINGLE_BROKER_EPOCH)
    }

    /// As [`PartitionLog::append`], stored in `leader_epoch`: a replicated partition's term, the same on every copy.
    pub fn append_in(&self, batch: Bytes, leader_epoch: i32) -> Result<Appended, StoreError> {
        self.partition
            .lock()
            .begin(&batch, &self.config, leader_epoch)?
            .wait()
    }

    /// Writes a batch now, in order; it may be acknowledged once the returned [`Pending`] is waited for.
    pub fn append_begin(&self, batch: Bytes) -> Result<Pending, StoreError> {
        self.partition
            .lock()
            .begin(&batch, &self.config, crate::SINGLE_BROKER_EPOCH)
    }

    /// DeleteRecords: the log starts at `offset` from now on (`-1`: its high watermark) - checked, never moved back -
    /// and what only held offsets before it goes, here and in object storage. The new start.
    pub fn delete_records(&self, offset: i64) -> Result<i64, StoreError> {
        let mut p = self.partition.lock();
        let start = p.start_after_delete(offset)?;
        let gone = drop_tiered_before(&mut self.tier.lock().segments, start);
        self.tier.lock().garbage.extend(gone);
        p.move_start(start)?;
        Ok(p.index.log_start)
    }

    /// The leader epoch to answer beside `offset` (see [`crate::Store::epoch_of`]); `current` is the partition's
    /// epoch now. An offset only in object storage gets the epoch of the first batch on the local disk, which is
    /// never lower than its own.
    pub fn epoch_of(&self, offset: i64, current: i32) -> i32 {
        self.partition.lock().epoch_of(offset, current)
    }

    /// Where `epoch` ended (see [`crate::Store::epoch_end`]); `current` is the partition's epoch now. Batches only in
    /// object storage are before every local one, so an epoch that ended among them is answered with a later offset
    /// - never an earlier one, which is the direction that matters.
    pub fn epoch_end(&self, epoch: i32, current: i32) -> Option<i64> {
        self.partition.lock().epoch_end(epoch, current)
    }

    /// Batches holding `offset` and after, up to `max_bytes` (at least one batch when there is one).
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<ReadResult, StoreError> {
        // Offsets no longer on the local disk come from object storage.
        if let Some(read) = self.read_tiered(offset, max_bytes)? {
            return Ok(read);
        }
        self.partition.lock().read(offset, max_bytes)
    }

    /// The transactions open on the partition now.
    pub fn open_transactions(&self) -> Vec<(i64, i16, i64)> {
        self.partition.lock().index.open_transactions()
    }

    /// The producers the partition knows now.
    pub fn active_producers(&self) -> Vec<crate::ActiveProducer> {
        self.partition.lock().index.active_producers()
    }

    /// The aborted transactions with records at or after `from` and before `upto`.
    pub fn aborted_transactions(&self, from: i64, upto: i64) -> Vec<crate::AbortedTransaction> {
        self.partition.lock().index.aborted_between(from, upto)
    }

    pub fn offsets(&self) -> PartitionOffsets {
        self.partition.lock().offsets()
    }

    /// The first offset at or after `timestamp` and its batch's timestamp.
    pub fn offset_for_timestamp(&self, timestamp: i64) -> Result<Option<(i64, i64)>, StoreError> {
        if let Some(found) = self.tiered_offset_for_timestamp(timestamp)? {
            return Ok(Some(found));
        }
        Ok(self.partition.lock().offset_for_timestamp(timestamp))
    }

    /// The offset and timestamp of the record with the largest timestamp.
    ///
    /// For records only in object storage, the answer is a segment's: its newest timestamp and last offset.
    pub fn max_timestamp(&self) -> Option<(i64, i64)> {
        let local = self.partition.lock().max_timestamp();
        let remote = self
            .tier
            .lock()
            .segments
            .iter()
            .max_by_key(|s| s.max_timestamp)
            .map(|s| (s.last, s.max_timestamp));
        match (local, remote) {
            (Some(l), Some(r)) if r.1 > l.1 => Some(r),
            (None, r) => r,
            (l, _) => l,
        }
    }

    /// Removes whole segments whose newest batch is older than `cutoff_ms`; how many batches went.
    pub fn enforce_retention(&self, cutoff_ms: i64) -> Result<usize, StoreError> {
        let mut p = self.partition.lock();
        self.retain_tiers(&mut p, cutoff_ms)
    }

    /// `retention.bytes`: the oldest segments go while what is left would still hold at least `keep` bytes; how
    /// many batches went.
    pub fn enforce_size(&self, keep: u64) -> Result<usize, StoreError> {
        let mut p = self.partition.lock();
        self.retain_size_tiers(&mut p, keep)
    }

    /// `segment.ms`: whether the segment being written to already spans that long for a batch of this timestamp.
    pub fn roll_due(&self, segment_ms: i64, timestamp: i64) -> bool {
        self.partition.lock().roll_due(segment_ms, timestamp)
    }

    /// Closes the segment being written to and starts the next (nothing, when it is empty).
    pub fn roll(&self) -> Result<(), StoreError> {
        self.partition.lock().roll()
    }

    /// Log compaction, one pass (file/cleaner.rs): the closed segments on the local disk that object storage
    /// does not hold, cleaned as far as `lag_ms` allows. How many records went.
    pub fn compact(&self, now_ms: i64, lag_ms: i64, delete_retention_ms: i64) -> Result<usize, StoreError> {
        let floor = self.compaction_floor();
        self.partition
            .lock()
            .compact(now_ms, lag_ms, delete_retention_ms, floor)
    }

    /// Whether a pass now would have anything to do.
    pub fn compaction_due(&self, now_ms: i64, lag_ms: i64, delete_retention_ms: i64) -> bool {
        let floor = self.compaction_floor();
        self.partition
            .lock()
            .compaction_due(now_ms, lag_ms, delete_retention_ms, floor)
    }

    /// Segments object storage holds are never written again: compaction starts after the last of them.
    fn compaction_floor(&self) -> i64 {
        self.tier.lock().segments.last().map_or(i64::MIN, |s| s.last + 1)
    }

    /// The bytes of its segments on the local disk.
    pub fn local_bytes(&self) -> u64 {
        self.partition.lock().segments.values().map(|s| s.size).sum()
    }

    /// Takes a checkpoint into `dest` (a new directory): every segment linked with its length now, and the producer
    /// table. Appends wait meanwhile. On the device once `sync_checkpoint` has run on it.
    pub fn checkpoint(&self, dest: &Path) -> Result<Manifest, StoreError> {
        if dest.exists() {
            return Err(StoreError::Corrupt(format!(
                "{} already exists: a checkpoint is taken into a new directory",
                dest.display()
            )));
        }
        let p = self.partition.lock();
        fs::create_dir_all(dest)?;
        let mut files = Vec::new();
        for seg in p.segments.values() {
            let Some(name) = seg.path.file_name().and_then(|f| f.to_str()) else {
                continue;
            };
            if fs::hard_link(&seg.path, dest.join(name)).is_err() {
                fs::copy(&seg.path, dest.join(name))?;
            }
            files.push(ManifestFile {
                path: name.to_owned(),
                len: seg.size,
            });
        }
        let table = write_versioned(p.index.producers())?;
        let p_start = p.index.log_start;
        // What compaction did here, when it did anything: the copy made from this waits for tombstones alike.
        let cleaning = fs::read(p.dir.join(super::cleaner::STATE)).ok();
        let tiered = write_versioned(self.tier.lock().segments.clone())?;
        let offsets = {
            let held = self.offsets.lock();
            (!held.is_empty())
                .then(|| write_versioned(held.clone()))
                .transpose()?
        };
        drop(p);
        if let Some(offsets) = offsets {
            fs::write(dest.join(OFFSETS), &offsets)?;
            files.push(ManifestFile {
                path: OFFSETS.to_owned(),
                len: offsets.len() as u64,
            });
        }
        if let Some(cleaning) = cleaning {
            fs::write(dest.join(super::cleaner::STATE), &cleaning)?;
            files.push(ManifestFile {
                path: super::cleaner::STATE.to_owned(),
                len: cleaning.len() as u64,
            });
        }
        let start = write_versioned(p_start)?;
        fs::write(dest.join(START), &start)?;
        files.push(ManifestFile {
            path: START.to_owned(),
            len: start.len() as u64,
        });
        fs::write(dest.join(TIERED), &tiered)?;
        files.push(ManifestFile {
            path: TIERED.to_owned(),
            len: tiered.len() as u64,
        });
        let mut f = File::create(dest.join(PRODUCERS))?;
        f.write_all(&table)?;
        files.push(ManifestFile {
            path: PRODUCERS.to_owned(),
            len: table.len() as u64,
        });
        sync_dir(dest)?;
        Ok(Manifest { files })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::tests::batch;
    use crate::file::FsyncPolicy;
    use crate::file::checkpoint::{restore_checkpoint, sync_checkpoint};

    #[test]
    fn a_restored_partition_has_its_batches_and_remembers_producers_retention_forgot() {
        let dir = tempfile::tempdir().unwrap();
        let config = FileStoreConfig {
            segment_bytes: 200,
            fsync: FsyncPolicy::Never,
        };
        let log = PartitionLog::open(dir.path().join("live"), config).unwrap();
        // Producer 7's batch lands in the first segment (three batches fill one), which retention then removes.
        log.append(batch(1, 7, 0, 0, 100)).unwrap();
        log.append(batch(1, -1, -1, -1, 100)).unwrap();
        log.append(batch(1, -1, -1, -1, 100)).unwrap();
        for i in 0..3 {
            log.append(batch(1, -1, -1, -1, 10_000 + i)).unwrap();
        }
        assert!(log.enforce_retention(1_000).unwrap() > 0);
        let then = log.read(3, usize::MAX).unwrap();

        let snap = dir.path().join("snap");
        let manifest = log.checkpoint(&snap).unwrap();
        sync_checkpoint(&snap, &manifest).unwrap();
        log.append(batch(1, -1, -1, -1, 20_000)).unwrap();
        drop(log);

        restore_checkpoint(&snap, &manifest, &dir.path().join("restored")).unwrap();
        let restored = PartitionLog::open(dir.path().join("restored"), config).unwrap();
        assert_eq!(restored.read(3, usize::MAX).unwrap(), then);
        assert_eq!(restored.offsets().log_start, 3);
        // The producer is still known: its retry of sequence 0 is a duplicate, and a gap is refused, as on the
        // copy that never restarted.
        assert!(restored.append(batch(1, 7, 0, 0, 100)).unwrap().duplicate);
        assert!(matches!(
            restored.append(batch(1, 7, 0, 5, 100)),
            Err(StoreError::OutOfOrderSequence)
        ));
        assert!(!dir.path().join("restored").join(PRODUCERS).exists());
    }

    /// The checkpoint's own files, as format 1 writes them: every later build must read them (see
    /// `openduct-raft`'s golden files for the rest). `OPENDUCT_WRITE_GOLDEN=1` writes them anew.
    #[test]
    fn checkpoint_files_are_written_and_read_as_format_1() {
        let dir = tempfile::tempdir().unwrap();
        let config = FileStoreConfig {
            segment_bytes: 200,
            fsync: FsyncPolicy::Never,
        };
        let log = PartitionLog::open(dir.path().join("live"), config).unwrap();
        for seq in 0..3 {
            log.append(batch(1, 7, 0, seq, 100)).unwrap();
        }
        log.mark_tiered(TieredSegment {
            base: 0,
            last: 2,
            max_timestamp: 100,
            size: 255,
            key: "prefix/topic/0/00000000000000000000-00000000000000000002.log".to_owned(),
        })
        .unwrap();
        log.commit_offsets(
            "billing",
            &[crate::CommittedOffset {
                topic: "orders".to_owned(),
                partition: 2,
                offset: 41,
                metadata: Some("m".to_owned()),
                commit_timestamp_ms: 1_700_000_000_000,
            }],
        );
        let snap = dir.path().join("snap");
        log.checkpoint(&snap).unwrap();
        let golden = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/v1");
        for name in [PRODUCERS, TIERED, OFFSETS] {
            let written = fs::read(snap.join(name)).unwrap();
            if std::env::var("OPENDUCT_WRITE_GOLDEN").is_ok() {
                fs::create_dir_all(&golden).unwrap();
                fs::write(golden.join(name), &written).unwrap();
            }
            assert_eq!(
                fs::read(golden.join(name)).unwrap(),
                written,
                "{name} is not written as format 1 was"
            );
        }
        // And read: a log opened over the golden files takes them.
        let restored = dir.path().join("restored");
        fs::create_dir_all(&restored).unwrap();
        for name in [PRODUCERS, TIERED, OFFSETS] {
            fs::copy(golden.join(name), restored.join(name)).unwrap();
        }
        let reopened = PartitionLog::open(&restored, config).unwrap();
        assert_eq!(reopened.tiered().len(), 1);
        assert_eq!(reopened.committed_offsets("billing")[0].offset, 41);
    }

    #[test]
    fn a_checkpoint_carries_where_deleted_records_left_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = PartitionLog::open(dir.path().join("live"), FileStoreConfig::default()).unwrap();
        for i in 0..5 {
            log.append_in(batch(2, -1, -1, -1, 100 + i), 3).unwrap();
        }
        assert_eq!(log.delete_records(6).unwrap(), 6);
        assert!(log.delete_records(11).is_err(), "past the end");
        let manifest = log.checkpoint(&dir.path().join("snap")).unwrap();
        sync_checkpoint(&dir.path().join("snap"), &manifest).unwrap();
        drop(log);
        restore_checkpoint(&dir.path().join("snap"), &manifest, &dir.path().join("restored")).unwrap();
        let restored = PartitionLog::open(dir.path().join("restored"), FileStoreConfig::default()).unwrap();
        assert_eq!(restored.offsets().log_start, 6);
        assert!(restored.read(4, 1 << 20).is_err());
        assert_eq!(restored.offsets().high_watermark, 10);
    }
}
