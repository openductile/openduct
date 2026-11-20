// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A store on local disk, durable across restarts and crashes: for one node - an edge gateway, a robot, a single
//! server. One directory holds everything:
//!
//! ```text
//! <dir>/LOCK                                   held exclusively while a broker has the store open
//! <dir>/meta.json                              format version and the next producer id
//! <dir>/topics/<topic>/topic.json              the topic's settings
//! <dir>/topics/<topic>/<partition>/<base>.log  segments, named by their first offset (20 digits)
//! <dir>/topics/<topic>/<partition>/start.json  where the partition starts, once DeleteRecords has moved it
//! <dir>/groups/<group, hex>.json               a consumer group's committed offsets
//! <dir>/trash/                                 deleted topics on their way out; emptied on open
//! ```
//!
//! A segment is a sequence of frames, one per batch: `[length: u32][crc32c: u32][batch]`, big-endian, where the
//! checksum covers the batch as stored (base offset included). An append is written whole and, by default, flushed
//! to the device (`fsync`) before it is acknowledged.
//!
//! Recovery on open reads every frame back. A damaged frame at the very end of the newest segment is a write the
//! machine did not finish (power loss, crash): it was never acknowledged, so it is cut off. Damage anywhere else is
//! data that was acknowledged and has since been lost or corrupted on the device: the store refuses to open, rather
//! than serve a log with a hole in it.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::error::{StoreError, validate_partitions, validate_topic_name};
use crate::store::{
    Appended, CommittedOffset, EpochEnd, PartitionOffsets, Pending, ReadResult, SINGLE_BROKER_EPOCH, Store,
    TopicConfig, TopicInfo,
};

pub(crate) mod checkpoint;
mod cleaner;
mod commit;
pub(crate) mod partition_log;
mod segments;
pub(crate) mod tier;

#[cfg(test)]
use segments::segment_name;
use segments::{FilePartition, Frame, read_frame};

const FORMAT_VERSION: u32 = 1;
const FRAME_HEADER: usize = 8;
/// The largest batch a segment may hold - the same bound as the largest request the broker accepts.
pub const MAX_BATCH_BYTES: usize = 100 * 1024 * 1024;

/// When appended data is flushed to the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Before every acknowledgement. Nothing acknowledged is lost on power failure.
    Always,
    /// Left to the operating system. Faster; a power failure can lose recently acknowledged batches. Only for data
    /// that can be lost.
    Never,
}

#[derive(Debug, Clone, Copy)]
pub struct FileStoreConfig {
    /// A segment is closed and a new one started once it reaches this size.
    pub segment_bytes: u64,
    pub fsync: FsyncPolicy,
}

impl Default for FileStoreConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 * 1024 * 1024,
            fsync: FsyncPolicy::Always,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Meta {
    format: u32,
    next_producer_id: i64,
}

#[derive(Debug)]
struct FileTopic {
    info: TopicInfo,
    partitions: Vec<Mutex<FilePartition>>,
}

/// The on-disk store.
#[derive(Debug)]
pub struct FileStore {
    root: PathBuf,
    config: FileStoreConfig,
    // Held for the store's life; dropping it releases the directory for another process.
    _lock: File,
    topics: RwLock<HashMap<String, FileTopic>>,
    meta: Mutex<Meta>,
    groups: GroupLocks,
}

/// Locks for groups' committed offsets: one group's file at a time (writers of different groups go on together, each
/// flushing its own file), or every group's at once for what reads or rewrites them all.
#[derive(Debug)]
struct GroupLocks {
    all: RwLock<()>,
    one: Vec<Mutex<()>>,
}

impl GroupLocks {
    fn new() -> Self {
        Self {
            all: RwLock::new(()),
            one: (0..64).map(|_| Mutex::new(())).collect(),
        }
    }

    /// `group`'s file, held.
    fn one(
        &self,
        group: &str,
    ) -> (
        parking_lot::RwLockReadGuard<'_, ()>,
        Option<parking_lot::MutexGuard<'_, ()>>,
    ) {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        group.hash(&mut h);
        let stripe = usize::try_from(h.finish() % 64).unwrap_or(0);
        let all = self.all.read();
        (all, self.one.get(stripe).map(Mutex::lock))
    }

    /// Every group's file, held.
    fn all(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.all.write()
    }
}

fn sync_dir(dir: &Path) -> Result<(), StoreError> {
    // A directory's entries (a new or renamed file) are durable only once the directory itself is synced.
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Replace `path` with `contents` so that a crash leaves either the old file or the new one, never a torn one.
fn write_atomically(path: &Path, contents: &[u8]) -> Result<(), StoreError> {
    let dir = path
        .parent()
        .ok_or_else(|| StoreError::Corrupt(format!("{} has no parent directory", path.display())))?;
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(dir)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, StoreError> {
    let raw = fs::read(path)?;
    serde_json::from_slice(&raw).map_err(|e| StoreError::Corrupt(format!("{}: {e}", path.display())))
}

fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    serde_json::to_vec_pretty(value).map_err(|e| StoreError::Corrupt(format!("cannot serialise: {e}")))
}

/// Where a partition's log starts as DeleteRecords left it (0 when it never ran): `start.json` beside its segments.
fn recorded_start(dir: &Path) -> Result<i64, StoreError> {
    let path = dir.join(partition_log::START);
    match fs::read(&path) {
        Ok(raw) => partition_log::read_versioned(&path, &raw),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The name [`hex`] spelled, if `s` is one it could have spelled.
fn unhex(s: &str) -> Option<String> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..s.len())
        .step_by(2)
        .map(|i| s.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
        .collect();
    String::from_utf8(bytes?).ok()
}

impl FileStore {
    /// Open (or create) the store in `root`, recovering every partition. Fails if another process holds it.
    pub fn open(root: impl Into<PathBuf>, config: FileStoreConfig) -> Result<Self, StoreError> {
        let root = root.into();
        fs::create_dir_all(root.join("topics"))?;
        fs::create_dir_all(root.join("groups"))?;
        fs::create_dir_all(root.join("trash"))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("LOCK"))?;
        if lock.try_lock().is_err() {
            return Err(StoreError::Locked(root));
        }
        let meta_path = root.join("meta.json");
        let meta: Meta = if meta_path.exists() {
            read_json(&meta_path)?
        } else {
            let fresh = Meta {
                format: FORMAT_VERSION,
                next_producer_id: 0,
            };
            write_atomically(&meta_path, &to_json(&fresh)?)?;
            fresh
        };
        if meta.format != FORMAT_VERSION {
            return Err(StoreError::Corrupt(format!(
                "the data format is version {}; this OpenDuct reads version {FORMAT_VERSION}",
                meta.format
            )));
        }
        // Topics deleted before a stop: their removal may not have finished; it finishes now.
        for entry in fs::read_dir(root.join("trash"))? {
            let path = entry?.path();
            if let Err(e) = fs::remove_dir_all(&path) {
                tracing::warn!(dir = %path.display(), error = %e, "cannot remove a deleted topic's files yet");
            }
        }
        let mut topics = HashMap::new();
        for entry in fs::read_dir(root.join("topics"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let settings = entry.path().join("topic.json");
            if !settings.exists() {
                // A topic whose creation never finished (its settings are written last): never served.
                tracing::warn!(dir = %entry.path().display(), "skipping a topic directory with no settings");
                continue;
            }
            let mut info: TopicInfo = read_json(&settings)?;
            if info.id.is_nil() {
                // Written before topics had ids: given one now, and kept from here on.
                info.id = uuid::Uuid::new_v4();
                write_atomically(&settings, &to_json(&info)?)?;
            }
            if entry.file_name().to_str() != Some(info.name.as_str()) {
                return Err(StoreError::Corrupt(format!(
                    "{} holds the settings of topic {:?}",
                    entry.path().display(),
                    info.name
                )));
            }
            let mut partitions = Vec::new();
            for p in 0..info.partitions {
                let dir = entry.path().join(p.to_string());
                let mut partition = FilePartition::open(dir.clone())?;
                partition.move_start(recorded_start(&dir)?)?;
                partitions.push(Mutex::new(partition));
            }
            topics.insert(info.name.clone(), FileTopic { info, partitions });
        }
        Ok(Self {
            root,
            config,
            _lock: lock,
            topics: RwLock::new(topics),
            meta: Mutex::new(meta),
            groups: GroupLocks::new(),
        })
    }

    fn with_partition<T>(
        &self,
        topic: &str,
        partition: i32,
        f: impl FnOnce(&mut FilePartition) -> Result<T, StoreError>,
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

    /// Drop every group's committed offsets on `topic`, so a topic of the same name created later starts clean.
    /// Best effort: the topic is already gone, so a group file that cannot be rewritten is reported, not fatal.
    fn forget_topic_offsets(&self, topic: &str) {
        let _guard = self.groups.all();
        let forget = |path: &Path| -> Result<(), StoreError> {
            let mut committed: Vec<CommittedOffset> = read_json(path)?;
            let before = committed.len();
            committed.retain(|c| c.topic != topic);
            if committed.len() != before {
                write_atomically(path, &to_json(&committed)?)?;
            }
            Ok(())
        };
        let entries = match fs::read_dir(self.root.join("groups")) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(topic, error = %e, "cannot list groups to drop a deleted topic's offsets");
                return;
            }
        };
        for path in entries.filter_map(|e| e.ok().map(|e| e.path())) {
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Err(e) = forget(&path) {
                tracing::warn!(topic, file = %path.display(), error = %e, "cannot drop a deleted topic's offsets");
            }
        }
    }

    /// The directory the store keeps its files in.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn group_path(&self, group: &str) -> PathBuf {
        self.root.join("groups").join(format!("{}.json", hex(group)))
    }

    /// Create a topic with the id it is given - a replicated store's brokers all give a topic the same one.
    pub fn create_topic_with_id(
        &self,
        name: &str,
        config: TopicConfig,
        id: uuid::Uuid,
    ) -> Result<(), StoreError> {
        validate_topic_name(name)?;
        validate_partitions(config.partitions)?;
        let mut topics = self.topics.write();
        if topics.contains_key(name) {
            return Err(StoreError::TopicExists(name.to_owned()));
        }
        let dir = self.root.join("topics").join(name);
        fs::create_dir_all(&dir)?;
        let info = TopicInfo {
            id,
            name: name.to_owned(),
            partitions: config.partitions,
            retention_ms: config.retention_ms,
            settings: Default::default(),
        };
        let mut partitions = Vec::new();
        for p in 0..config.partitions {
            partitions.push(Mutex::new(FilePartition::open(dir.join(p.to_string()))?));
        }
        // The settings file is written last: a topic directory without it is skipped on open, so a topic whose
        // creation a crash interrupted is never served.
        write_atomically(&dir.join("topic.json"), &to_json(&info)?)?;
        sync_dir(&self.root.join("topics"))?;
        topics.insert(name.to_owned(), FileTopic { info, partitions });
        Ok(())
    }

    fn delete_topic_files(&self, name: &str) -> Result<(), StoreError> {
        let mut topics = self.topics.write();
        if !topics.contains_key(name) {
            return Err(StoreError::UnknownTopic(name.to_owned()));
        }
        // One rename takes the topic out of `topics/`: after a crash it is either still there whole (the delete
        // was never acknowledged) or in `trash/`, which the next open empties.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let trashed = self.root.join("trash").join(format!("{name}-{nanos}"));
        fs::rename(self.root.join("topics").join(name), &trashed)?;
        sync_dir(&self.root.join("topics"))?;
        sync_dir(&self.root.join("trash"))?;
        topics.remove(name);
        drop(topics);
        if let Err(e) = fs::remove_dir_all(&trashed) {
            tracing::warn!(dir = %trashed.display(), error = %e, "a deleted topic's files are removed on the next open");
        }
        self.forget_topic_offsets(name);
        Ok(())
    }
}

impl Store for FileStore {
    fn create_topic(&self, name: &str, config: TopicConfig) -> Result<(), StoreError> {
        self.create_topic_with_id(name, config, uuid::Uuid::new_v4())
    }

    fn delete_topic(&self, name: &str) -> Result<(), StoreError> {
        self.delete_topic_files(name)
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
        let dir = self.root.join("topics").join(name);
        let mut added = Vec::new();
        for p in topic.info.partitions..total {
            let partition = dir.join(p.to_string());
            // Left by an add a crash interrupted before the settings were written: never served, so it goes.
            if partition.exists() {
                fs::remove_dir_all(&partition)?;
            }
            added.push(Mutex::new(FilePartition::open(partition)?));
        }
        let mut info = topic.info.clone();
        info.partitions = total;
        // The settings last, as when the topic was made: after a crash before this, the topic has its old partitions.
        write_atomically(&dir.join("topic.json"), &to_json(&info)?)?;
        topic.partitions.extend(added);
        topic.info = info;
        Ok(())
    }

    fn set_retention(&self, name: &str, retention_ms: Option<i64>) -> Result<(), StoreError> {
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(name)
            .ok_or_else(|| StoreError::UnknownTopic(name.to_owned()))?;
        let mut info = topic.info.clone();
        info.retention_ms = retention_ms;
        // On disk before it is served: a crash leaves the old setting or the new one, whole.
        write_atomically(
            &self.root.join("topics").join(name).join("topic.json"),
            &to_json(&info)?,
        )?;
        topic.info = info;
        Ok(())
    }

    fn set_topic_settings(&self, name: &str, settings: crate::TopicSettings) -> Result<(), StoreError> {
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(name)
            .ok_or_else(|| StoreError::UnknownTopic(name.to_owned()))?;
        crate::store::one_copy(name, &settings)?;
        let mut info = topic.info.clone();
        info.settings = settings;
        // On disk before it is served, as retention is: a crash leaves the old settings or the new ones, whole.
        write_atomically(
            &self.root.join("topics").join(name).join("topic.json"),
            &to_json(&info)?,
        )?;
        topic.info = info;
        Ok(())
    }

    fn append(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Appended, StoreError> {
        self.append_begin(topic, partition, batch)?.wait()
    }

    fn compact(&self, now_ms: i64) -> Result<usize, StoreError> {
        let topics = self.topics.read();
        let mut removed = 0usize;
        for t in topics.values().filter(|t| t.info.settings.compacts()) {
            let (lag, wait) = (
                t.info.settings.min_compaction_lag_ms(),
                t.info.settings.delete_retention_ms(),
            );
            for p in &t.partitions {
                removed += p.lock().compact(now_ms, lag, wait, i64::MIN)?;
            }
        }
        Ok(removed)
    }

    fn end_transaction(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        marker: crate::batch::Marker,
        coordinator_epoch: i32,
    ) -> Result<Appended, StoreError> {
        let batch = crate::batch::marker_batch(
            producer_id,
            producer_epoch,
            marker,
            crate::now_ms(),
            coordinator_epoch,
        );
        let config = self.config;
        self.with_partition(topic, partition, |p| {
            p.begin(&batch, &config, SINGLE_BROKER_EPOCH)
        })?
        .wait()
    }

    fn open_transactions(&self, topic: &str, partition: i32) -> Result<Vec<(i64, i16, i64)>, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.index.open_transactions()))
    }

    fn active_producers(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<crate::ActiveProducer>, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.index.active_producers()))
    }

    fn aborted_transactions(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        upto: i64,
    ) -> Result<Vec<crate::AbortedTransaction>, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.index.aborted_between(from, upto)))
    }

    fn append_begin(&self, topic: &str, partition: i32, batch: Bytes) -> Result<Pending, StoreError> {
        // What the topic's settings ask of a batch (admit.rs); an unknown topic is refused just below.
        let (batch, segment_ms) = match self.topics.read().get(topic) {
            Some(t) => (
                crate::admitted(&t.info.settings, batch, crate::now_ms())?,
                Some(t.info.settings.segment_ms()),
            ),
            None => (batch, None),
        };
        // What a client may not send (a marker) is refused here, where clients' batches come in.
        crate::batch::check_supported(&crate::batch::parse_header(&batch)?)?;
        let config = self.config;
        self.with_partition(topic, partition, |p| {
            // segment.ms: the segment is closed first when its records already span that long.
            p.roll_ms = segment_ms;
            p.begin(&batch, &config, SINGLE_BROKER_EPOCH)
        })
    }

    fn read(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<ReadResult, StoreError> {
        self.with_partition(topic, partition, |p| p.read(offset, max_bytes))
    }

    fn offsets(&self, topic: &str, partition: i32) -> Result<PartitionOffsets, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.offsets()))
    }

    fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<Option<(i64, i64)>, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.offset_for_timestamp(timestamp)))
    }

    fn max_timestamp(&self, topic: &str, partition: i32) -> Result<Option<(i64, i64)>, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.max_timestamp()))
    }

    fn delete_records(&self, topic: &str, partition: i32, offset: i64) -> Result<i64, StoreError> {
        self.with_partition(topic, partition, |p| {
            let start = p.start_after_delete(offset)?;
            if start > p.index.log_start {
                // On the device before the log moves: after a crash the start is the new one or the old one, whole.
                write_atomically(
                    &p.dir.join(partition_log::START),
                    &partition_log::write_versioned(start)?,
                )?;
                p.move_start(start)?;
            }
            Ok(p.index.log_start)
        })
    }

    fn epoch_of(&self, topic: &str, partition: i32, offset: i64) -> Result<i32, StoreError> {
        self.with_partition(topic, partition, |p| Ok(p.epoch_of(offset, SINGLE_BROKER_EPOCH)))
    }

    fn epoch_end(&self, topic: &str, partition: i32, epoch: i32) -> Result<EpochEnd, StoreError> {
        self.with_partition(topic, partition, |p| {
            Ok(EpochEnd {
                current: SINGLE_BROKER_EPOCH,
                end_offset: p.epoch_end(epoch, SINGLE_BROKER_EPOCH),
            })
        })
    }

    fn next_producer_id(&self) -> Result<i64, StoreError> {
        let mut meta = self.meta.lock();
        let id = meta.next_producer_id;
        let next = Meta {
            format: meta.format,
            next_producer_id: id + 1,
        };
        // Durable before the id is handed out: a crash never hands the same id out twice.
        write_atomically(&self.root.join("meta.json"), &to_json(&next)?)?;
        *meta = next;
        Ok(id)
    }

    fn commit_offsets(&self, group: &str, offsets: &[CommittedOffset]) -> Result<(), StoreError> {
        let _guard = self.groups.one(group);
        let path = self.group_path(group);
        let mut committed: Vec<CommittedOffset> = if path.exists() {
            read_json(&path)?
        } else {
            Vec::new()
        };
        for o in offsets {
            committed.retain(|c| !(c.topic == o.topic && c.partition == o.partition));
            committed.push(o.clone());
        }
        write_atomically(&path, &to_json(&committed)?)
    }

    fn committed_offsets(&self, group: &str) -> Result<Vec<CommittedOffset>, StoreError> {
        let _guard = self.groups.one(group);
        let path = self.group_path(group);
        if path.exists() {
            read_json(&path)
        } else {
            Ok(Vec::new())
        }
    }

    fn delete_offsets(&self, group: &str, partitions: Option<&[(String, i32)]>) -> Result<(), StoreError> {
        let _guard = self.groups.one(group);
        let path = self.group_path(group);
        if !path.exists() {
            return Ok(());
        }
        let committed: Vec<CommittedOffset> = read_json(&path)?;
        let mut held = std::collections::BTreeMap::from([(group.to_owned(), committed)]);
        crate::forget_offsets(&mut held, group, partitions);
        match held.remove(group) {
            Some(left) => write_atomically(&path, &to_json(&left)?),
            None => {
                fs::remove_file(&path)?;
                sync_dir(&self.root.join("groups"))?;
                Ok(())
            }
        }
    }

    fn groups(&self) -> Result<Vec<String>, StoreError> {
        let _guard = self.groups.all();
        let dir = self.root.join("groups");
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for entry in entries {
            let name = entry?.file_name();
            // Only the files this store wrote: <hex of the name>.json (a write in progress has another name).
            if let Some(group) = name
                .to_str()
                .and_then(|n| n.strip_suffix(".json"))
                .and_then(unhex)
            {
                out.push(group);
            }
        }
        out.sort();
        Ok(out)
    }

    fn enforce_retention(&self, now_ms: i64) -> Result<usize, StoreError> {
        let topics = self.topics.read();
        let mut removed = 0usize;
        for t in topics.values() {
            // A topic that only compacts keeps its newest records for ever.
            let deletes = t.info.settings.deletes();
            let cutoff = t
                .info
                .retention_ms
                .filter(|_| deletes)
                .map(|retention| now_ms.saturating_sub(retention));
            let keep_bytes = t.info.settings.retention_bytes().filter(|_| deletes);
            for p in &t.partitions {
                if let Some(cutoff) = cutoff {
                    removed += p.lock().enforce_retention(cutoff)?;
                }
                if let Some(keep) = keep_bytes {
                    removed += p.lock().enforce_size(keep)?;
                }
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
pub(crate) mod tests;
