// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Tiered storage for a partition's log: closed segments copied to object storage (S3), so the local disk keeps
//! only the recent ones and a broker that is rebuilt need not fetch the old ones from another broker.
//!
//! The store keeps the list of segments in object storage (`TieredSegment`) and changes it only when told:
//! [`PartitionLog::mark_tiered`] once a segment is stored remotely, [`PartitionLog::offload`] to drop local copies
//! of segments stored remotely, retention to forget remote segments as it removes local ones. A replicated
//! broker applies those as commands of the partition's log, so every copy keeps the same list - and a snapshot
//! carries it (`tiered.json`). Uploading and deleting objects is the caller's work (the partition's leader); a
//! read of an offset no longer on the local disk fetches its segment through [`Remote`] into a small local cache.
//!
//! The list's bookkeeping follows segment boundaries, so every broker of a cluster must use the same segment size.

use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::partition_log::PartitionLog;
use super::{FilePartition, Frame, read_frame, sync_dir};
use crate::batch::parse_stored_header;
use crate::error::StoreError;
use crate::store::ReadResult;

pub(crate) const TIERED: &str = "tiered.json";
const CACHE: &str = "remote-cache";
/// Segments kept in the local cache, per partition.
const CACHED: usize = 4;

/// Object storage, as a partition's log reads segments back from it. Blocking: called off the async threads.
pub trait Remote: Send + Sync + std::fmt::Debug {
    /// Copies the object `key` into the file `dest`.
    fn get(&self, key: &str, dest: &Path) -> Result<(), StoreError>;
}

/// A segment in object storage: the offsets it holds, its newest batch's timestamp, its size, and its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredSegment {
    pub base: i64,
    pub last: i64,
    pub max_timestamp: i64,
    pub size: u64,
    pub key: String,
}

/// A closed local segment: the offsets it holds, its newest batch's timestamp, its size and its file, and whether
/// object storage holds it already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSegment {
    pub base: i64,
    pub last: i64,
    pub max_timestamp: i64,
    pub size: u64,
    pub path: PathBuf,
    pub tiered: bool,
}

impl FilePartition {
    /// The closed segments, oldest first (the active one is never closed).
    fn closed(&self) -> Vec<(i64, i64, i64, u64, PathBuf)> {
        let active = self.active.as_ref().map(|a| a.base);
        let bases: Vec<i64> = self.segments.keys().copied().collect();
        bases
            .windows(2)
            .filter(|w| w.first().copied() != active)
            .filter_map(|w| {
                let (&base, &next) = (w.first()?, w.get(1)?);
                let seg = self.segments.get(&base)?;
                Some((base, next - 1, seg.max_timestamp, seg.size, seg.path.clone()))
            })
            .collect()
    }

    /// Drops whole local segments holding only offsets before `offset`; the log's start stays where it is (the
    /// offsets are still in object storage). How many batches' local copies went.
    fn drop_local_before(&mut self, offset: i64) -> Result<usize, StoreError> {
        let mut dropped = 0usize;
        for (base, last, _, _, _) in self.closed() {
            if last >= offset {
                break;
            }
            if let Some(seg) = self.segments.remove(&base) {
                self.readers.remove(&base);
                fs::remove_file(&seg.path)?;
                let next = last + 1;
                while self.index.entries.front().is_some_and(|e| e.last_offset < next) {
                    self.index.entries.pop_front();
                    dropped += 1;
                }
            }
        }
        if dropped > 0 {
            sync_dir(&self.dir)?;
        }
        Ok(dropped)
    }
}

/// The batches of a segment file holding `offset` and after, up to `max_bytes` (at least one).
fn batches_from(file: &Path, offset: i64, max_bytes: usize) -> Result<Vec<Bytes>, StoreError> {
    let f = File::open(file)?;
    let len = f.metadata()?.len();
    let mut reader = BufReader::new(f);
    let (mut position, mut total, mut out) = (0u64, 0usize, Vec::new());
    loop {
        match read_frame(&mut reader, len.saturating_sub(position)) {
            Frame::End => return Ok(out),
            Frame::Damaged { why, .. } => {
                return Err(StoreError::Corrupt(format!(
                    "{} at byte {position}: {why}",
                    file.display()
                )));
            }
            Frame::Ok { batch, len: size } => {
                position += size;
                let header = parse_stored_header(&batch)?;
                if header.base_offset + i64::from(header.last_offset_delta) < offset {
                    continue;
                }
                if !out.is_empty() && total.saturating_add(batch.len()) > max_bytes {
                    return Ok(out);
                }
                total = total.saturating_add(batch.len());
                out.push(batch);
                if total >= max_bytes {
                    return Ok(out);
                }
            }
        }
    }
}

/// The first batch of a segment file whose newest record reaches `timestamp`: its offset and that timestamp.
fn first_at(file: &Path, timestamp: i64) -> Result<Option<(i64, i64)>, StoreError> {
    let f = File::open(file)?;
    let len = f.metadata()?.len();
    let mut reader = BufReader::new(f);
    let mut position = 0u64;
    loop {
        match read_frame(&mut reader, len.saturating_sub(position)) {
            Frame::End => return Ok(None),
            Frame::Damaged { why, .. } => {
                return Err(StoreError::Corrupt(format!(
                    "{} at byte {position}: {why}",
                    file.display()
                )));
            }
            Frame::Ok { batch, len: size } => {
                position += size;
                let header = parse_stored_header(&batch)?;
                if header.max_timestamp >= timestamp {
                    return Ok(Some((header.base_offset, header.max_timestamp)));
                }
            }
        }
    }
}

impl PartitionLog {
    /// Where the next segment to store remotely starts: right after the last one stored, or the log's start.
    fn frontier(&self, p: &FilePartition) -> i64 {
        let tier = self.tier.lock();
        match tier.segments.last() {
            Some(last) => (last.last + 1).max(p.index.log_start),
            None => p.index.log_start,
        }
    }

    /// The closed local segments, oldest first, each with whether object storage holds it.
    pub fn local_segments(&self) -> Vec<LocalSegment> {
        let p = self.partition.lock();
        let frontier = self.frontier(&p);
        p.closed()
            .into_iter()
            .map(|(base, last, max_timestamp, size, path)| LocalSegment {
                base,
                last,
                max_timestamp,
                size,
                path,
                tiered: last < frontier,
            })
            .collect()
    }

    /// The segments in object storage, oldest first.
    pub fn tiered(&self) -> Vec<TieredSegment> {
        self.tier.lock().segments.clone()
    }

    /// Records a segment now in object storage. It must reach the frontier: one already covered is a no-op, one
    /// leaving a gap is refused (another broker stored it first, or the log moved on).
    pub fn mark_tiered(&self, segment: TieredSegment) -> Result<(), StoreError> {
        let p = self.partition.lock();
        let frontier = self.frontier(&p);
        if segment.last < frontier {
            return Ok(());
        }
        if segment.base > frontier || segment.last >= p.index.next_offset {
            return Err(StoreError::OffsetOutOfRange {
                requested: segment.base,
                log_start: frontier,
                high_watermark: p.index.next_offset,
            });
        }
        self.tier.lock().segments.push(segment);
        Ok(())
    }

    /// Drops the local copies of segments, stored remotely, holding only offsets before `before`. Reads of them go
    /// to object storage from now on. How many batches' local copies went.
    pub fn offload(&self, before: i64) -> Result<usize, StoreError> {
        let mut p = self.partition.lock();
        let covered = self.frontier(&p);
        p.drop_local_before(before.min(covered))
    }

    /// Keys of remote segments retention removed, for the caller to delete.
    pub fn take_garbage(&self) -> Vec<String> {
        std::mem::take(&mut self.tier.lock().garbage)
    }

    /// Retention over both tiers: remote segments whose newest batch is older than `cutoff` are forgotten (their
    /// keys left for [`Self::take_garbage`]), then local ones are removed. How many batches went.
    pub(super) fn retain_tiers(&self, p: &mut FilePartition, cutoff: i64) -> Result<usize, StoreError> {
        let mut removed = 0usize;
        let start_before = p.index.log_start;
        {
            let mut tier = self.tier.lock();
            while tier.segments.first().is_some_and(|s| s.max_timestamp < cutoff) {
                let gone = tier.segments.remove(0);
                tier.garbage.push(gone.key);
                removed += 1;
            }
        }
        removed += p.enforce_retention(cutoff)?;
        // The log starts at its first offset still kept: in object storage when a segment is left there (local
        // retention moved the start to its first local segment).
        if let Some(first) = self.tier.lock().segments.first() {
            p.index.log_start = first.base.max(start_before);
        }
        Ok(removed)
    }

    /// `retention.bytes` over both tiers: the oldest segments go - those only in object storage first (their keys
    /// left for [`Self::take_garbage`]), then local ones - while what is left would still hold at least `keep`
    /// bytes. A segment both on disk and in object storage counts once. How many batches went.
    pub(super) fn retain_size_tiers(&self, p: &mut FilePartition, keep: u64) -> Result<usize, StoreError> {
        let mut removed = 0usize;
        let local_first = p
            .index
            .entries
            .front()
            .map_or(p.index.next_offset, |e| e.base_offset);
        let local: u64 = p.segments.values().map(|s| s.size).sum();
        let remote_left = {
            let mut tier = self.tier.lock();
            let only_remote = |s: &TieredSegment| s.last < local_first;
            let mut total = local
                + tier
                    .segments
                    .iter()
                    .filter(|s| only_remote(s))
                    .map(|s| s.size)
                    .sum::<u64>();
            while let Some(first) = tier.segments.first()
                && only_remote(first)
                && total.saturating_sub(first.size) >= keep
            {
                total -= first.size;
                let gone = tier.segments.remove(0);
                tier.garbage.push(gone.key);
                removed += 1;
            }
            tier.segments.first().is_some_and(only_remote)
        };
        if remote_left {
            // Older data is still kept in object storage: the log starts at the first of it.
            if let Some(first) = self.tier.lock().segments.first() {
                p.index.log_start = p.index.log_start.max(first.base);
            }
            return Ok(removed);
        }
        // Nothing older is left remotely: the log starts on the local disk, and local segments go next.
        p.index.log_start = p.index.log_start.max(local_first);
        removed += p.enforce_size(keep)?;
        let start = p.index.log_start;
        let mut tier = self.tier.lock();
        while tier.segments.first().is_some_and(|s| s.last < start) {
            // The remote copy of a local segment that just went.
            let gone = tier.segments.remove(0);
            tier.garbage.push(gone.key);
        }
        Ok(removed)
    }

    /// A read of offsets no longer on the local disk: from the remote segment holding `offset`. `None` when the
    /// offset is local.
    pub(super) fn read_tiered(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Option<ReadResult>, StoreError> {
        let (segment, offsets) = {
            let p = self.partition.lock();
            let local_first = p
                .index
                .entries
                .front()
                .map_or(p.index.next_offset, |e| e.base_offset);
            if offset >= local_first || offset < p.index.log_start {
                return Ok(None);
            }
            let tier = self.tier.lock();
            let Some(segment) = tier.segments.iter().find(|s| s.last >= offset).cloned() else {
                return Ok(None);
            };
            (segment, p.offsets())
        };
        let file = self.cached(&segment)?;
        let batches = batches_from(&file, offset, max_bytes)?;
        Ok(Some(ReadResult { batches, offsets }))
    }

    /// The first remote batch reaching `timestamp`, if it is before every local one.
    pub(super) fn tiered_offset_for_timestamp(
        &self,
        timestamp: i64,
    ) -> Result<Option<(i64, i64)>, StoreError> {
        let segment = self
            .tier
            .lock()
            .segments
            .iter()
            .find(|s| s.max_timestamp >= timestamp)
            .cloned();
        match segment {
            Some(s) => first_at(&self.cached(&s)?, timestamp),
            None => Ok(None),
        }
    }

    /// A remote segment's local copy in the cache, fetched first if it is not there.
    fn cached(&self, segment: &TieredSegment) -> Result<PathBuf, StoreError> {
        let remote = self.remote.as_ref().ok_or_else(|| {
            StoreError::Unavailable(format!(
                "offsets {} to {} are in object storage, which this broker is not set up to read",
                segment.base, segment.last
            ))
        })?;
        let dir = self.dir().join(CACHE);
        let path = dir.join(format!("{:020}-{:020}.log", segment.base, segment.last));
        if path.exists() {
            return Ok(path);
        }
        fs::create_dir_all(&dir)?;
        let tmp = path.with_extension("part");
        Arc::clone(remote).get(&segment.key, &tmp)?;
        if fs::metadata(&tmp)?.len() != segment.size {
            let _ = fs::remove_file(&tmp);
            return Err(StoreError::Corrupt(format!(
                "{} in object storage is not the size it was stored at",
                segment.key
            )));
        }
        fs::rename(&tmp, &path)?;
        // A few segments are kept; the one used longest ago goes.
        let mut kept: Vec<(std::time::SystemTime, PathBuf)> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "log"))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        kept.sort();
        while kept.len() > CACHED {
            let (_, oldest) = kept.remove(0);
            if oldest != path {
                let _ = fs::remove_file(oldest);
            }
        }
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::tests::batch;
    use crate::file::checkpoint::{restore_checkpoint, sync_checkpoint};
    use crate::file::{FileStoreConfig, FsyncPolicy};

    /// Object storage in a directory.
    #[derive(Debug)]
    struct Bucket(PathBuf);

    impl Remote for Bucket {
        fn get(&self, key: &str, dest: &Path) -> Result<(), StoreError> {
            fs::copy(self.0.join(key), dest)?;
            Ok(())
        }
    }

    fn upload(log: &PartitionLog, bucket: &Path) -> usize {
        let mut stored = 0;
        for s in log.local_segments().into_iter().filter(|s| !s.tiered) {
            let key = format!("t-{:020}.log", s.base);
            fs::copy(&s.path, bucket.join(&key)).unwrap();
            log.mark_tiered(TieredSegment {
                base: s.base,
                last: s.last,
                max_timestamp: s.max_timestamp,
                size: s.size,
                key,
            })
            .unwrap();
            stored += 1;
        }
        stored
    }

    #[test]
    fn segments_in_object_storage_are_read_back_after_their_local_copies_go() {
        let dir = tempfile::tempdir().unwrap();
        let bucket = dir.path().join("bucket");
        fs::create_dir_all(&bucket).unwrap();
        let config = FileStoreConfig {
            segment_bytes: 200,
            fsync: FsyncPolicy::Never,
        };
        let log = PartitionLog::open(dir.path().join("live"), config)
            .unwrap()
            .with_remote(Arc::new(Bucket(bucket.clone())));
        for i in 0..12 {
            log.append(batch(1, -1, -1, -1, 1_000 * (i + 1))).unwrap();
        }
        let everything = log.read(0, usize::MAX).unwrap().batches;
        assert!(upload(&log, &bucket) >= 3);
        assert_eq!(upload(&log, &bucket), 0, "nothing is stored twice");
        // A segment already covered is a no-op; one leaving a gap is refused.
        assert!(
            log.mark_tiered(TieredSegment {
                base: 0,
                last: 2,
                max_timestamp: 0,
                size: 0,
                key: "x".into()
            })
            .is_ok()
        );
        assert!(
            log.mark_tiered(TieredSegment {
                base: 50,
                last: 60,
                max_timestamp: 0,
                size: 0,
                key: "y".into()
            })
            .is_err()
        );

        assert!(log.offload(i64::MAX).unwrap() > 0);
        assert!(
            log.local_segments().iter().all(|s| !s.tiered),
            "local copies of stored segments are gone"
        );
        assert_eq!(log.offsets().log_start, 0, "the log still starts at 0");
        // Reading from 0 walks through object storage, segment by segment, then the local disk.
        let mut read = Vec::new();
        let mut offset = 0;
        while offset < 12 {
            let got = log.read(offset, 1).unwrap().batches;
            offset += i64::try_from(got.len()).unwrap();
            read.extend(got);
        }
        assert_eq!(read, everything);
        assert_eq!(log.offset_for_timestamp(2_500).unwrap().map(|(o, _)| o), Some(2));

        // A snapshot carries the remote list: a copy restored from it reads object storage too.
        let snap = dir.path().join("snap");
        let manifest = log.checkpoint(&snap).unwrap();
        sync_checkpoint(&snap, &manifest).unwrap();
        restore_checkpoint(&snap, &manifest, &dir.path().join("restored")).unwrap();
        let restored = PartitionLog::open(dir.path().join("restored"), config)
            .unwrap()
            .with_remote(Arc::new(Bucket(bucket.clone())));
        assert_eq!(restored.tiered(), log.tiered());
        assert_eq!(restored.read(0, 1).unwrap().batches.first(), everything.first());

        // Retention forgets remote segments as it removes local ones, and leaves their keys to delete.
        let gone = log.tiered().len();
        log.enforce_retention(6_500).unwrap();
        assert!(log.tiered().len() < gone);
        assert!(!log.take_garbage().is_empty());
        assert!(log.offsets().log_start > 0);
    }

    #[test]
    fn retention_by_size_counts_both_tiers_once_and_removes_the_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let bucket = dir.path().join("bucket");
        fs::create_dir_all(&bucket).unwrap();
        // A segment a batch.
        let config = FileStoreConfig {
            segment_bytes: 1,
            fsync: FsyncPolicy::Never,
        };
        let log = PartitionLog::open(dir.path().join("live"), config)
            .unwrap()
            .with_remote(Arc::new(Bucket(bucket.clone())));
        for i in 0..8 {
            log.append(batch(1, -1, -1, -1, 1_000 * (i + 1))).unwrap();
        }
        let one = log.local_segments()[0].size;
        // The first four go to object storage and leave the disk; the next three are in both places.
        assert_eq!(upload(&log, &bucket), 7);
        log.offload(4).unwrap();
        assert_eq!(log.local_bytes(), one * 4);
        assert_eq!(log.tiered().len(), 7);

        // Eight batches kept in all (not eleven): a limit of eight removes nothing.
        assert_eq!(log.enforce_size(one * 8).unwrap(), 0);
        assert_eq!(log.offsets().log_start, 0);

        // Room for six: the two oldest go, and they are remote ones.
        assert_eq!(log.enforce_size(one * 6).unwrap(), 2);
        assert_eq!(log.offsets().log_start, 2);
        assert_eq!(log.take_garbage().len(), 2);
        assert_eq!(log.local_bytes(), one * 4, "the local disk was not touched");
        assert_eq!(
            log.read(2, 1).unwrap().batches.len(),
            1,
            "what is left remotely still reads"
        );

        // Room for two and a half: the last two remote ones go, then one local one - and its remote copy with it.
        assert_eq!(log.enforce_size(one * 5 / 2).unwrap(), 3);
        let offsets = log.offsets();
        assert_eq!((offsets.log_start, offsets.high_watermark), (5, 8));
        assert_eq!(log.take_garbage().len(), 3);
        assert!(log.tiered().iter().all(|s| s.base >= 5), "{:?}", log.tiered());
        assert_eq!(log.read(5, usize::MAX).unwrap().batches.len(), 3);
        assert_eq!(
            log.enforce_size(one * 5 / 2).unwrap(),
            0,
            "nothing more the next round"
        );
    }

    #[test]
    fn compaction_leaves_segments_in_object_storage_alone_and_a_checkpoint_carries_what_it_did() {
        use crate::records::tests::keyed;
        let dir = tempfile::tempdir().unwrap();
        let bucket = dir.path().join("bucket");
        fs::create_dir_all(&bucket).unwrap();
        let config = FileStoreConfig {
            segment_bytes: 1,
            fsync: FsyncPolicy::Never,
        };
        let log = PartitionLog::open(dir.path().join("live"), config)
            .unwrap()
            .with_remote(Arc::new(Bucket(bucket.clone())));
        // a five times, a segment each; the first two segments go to object storage.
        for i in 0..2 {
            log.append(keyed(&[(Some("a"), Some("v"))], 1_000 + i, 0))
                .unwrap();
        }
        log.append(keyed(&[(Some("b"), Some("v"))], 1_002, 0)).unwrap();
        assert_eq!(upload(&log, &bucket), 2);
        for i in 3..6 {
            log.append(keyed(&[(Some("a"), Some("v"))], 1_000 + i, 0))
                .unwrap();
        }
        assert!(log.compaction_due(5_000, 0, 1_000));
        // Of the local segments past those stored (offsets 2, 3, 4; 5 is being written to), the a at 3 is
        // replaced by the a at 4. The two a's in object storage stay as they were stored.
        assert_eq!(log.compact(5_000, 0, 1_000).unwrap(), 1);
        assert!(!log.compaction_due(5_500, 0, 1_000));
        let kept: Vec<i32> = log
            .read(0, usize::MAX)
            .unwrap()
            .batches
            .iter()
            .map(|b| crate::batch::parse_stored_header(b).unwrap().record_count)
            .collect();
        assert_eq!(kept, vec![1, 1, 1, 0, 1, 1]);

        // A copy made from a checkpoint knows the pass ran: nothing is due there either.
        let snap = dir.path().join("snap");
        let manifest = log.checkpoint(&snap).unwrap();
        assert!(manifest.files.iter().any(|f| f.path == "cleaner.json"));
        sync_checkpoint(&snap, &manifest).unwrap();
        restore_checkpoint(&snap, &manifest, &dir.path().join("restored")).unwrap();
        let restored = PartitionLog::open(dir.path().join("restored"), config).unwrap();
        assert!(!restored.compaction_due(5_500, 0, 1_000));
        assert_eq!(restored.read(2, usize::MAX).unwrap().batches.len(), 4);
    }
}
