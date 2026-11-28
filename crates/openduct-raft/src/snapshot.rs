// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Snapshots on disk, the same for every group: a directory of files and `snapshot.json` (what the snapshot covers
//! and its files with their lengths), written last, so a directory without it is an unfinished one.
//!
//! A machine keeps them under its own directory: `snapshots/snap-<n>/` for its snapshots (only the newest is kept,
//! and older ones until no transfer reads them) and `incoming/<n>/` for one on its way from the leader.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use openduct_storage::{Manifest, StoreError, checked_path, sync_checkpoint};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::stable::{self, Versioned};
use crate::types::SnapshotMeta;

const RECORD: &str = "snapshot.json";

/// A snapshot's files on disk. Removed once a newer snapshot replaces it and nothing (a transfer to another broker)
/// reads it any more.
#[derive(Debug)]
pub struct SnapshotFiles {
    pub(crate) dir: PathBuf,
    pub(crate) manifest: Manifest,
    obsolete: AtomicBool,
    /// The snapshot that replaced this one: these files go only while that one is on disk and reads back.
    replaced_by: Mutex<Option<PathBuf>>,
    /// Files on their way in that nothing has taken in.
    unadopted: AtomicBool,
}

impl SnapshotFiles {
    pub(crate) fn new(dir: PathBuf, manifest: Manifest) -> Self {
        Self {
            dir,
            manifest,
            obsolete: AtomicBool::new(false),
            replaced_by: Mutex::new(None),
            unadopted: AtomicBool::new(false),
        }
    }

    /// For files still on their way in (under `incoming/`): removed when let go of, unless they were moved in
    /// among the machine's snapshots meanwhile. Never a snapshot anything relies on, so nothing stands for it.
    pub(crate) fn discard_unless_taken_in(&self) {
        if self.dir.components().any(|c| c.as_os_str() == "incoming") {
            self.unadopted.store(true, Ordering::Release);
        }
    }

    /// Marks the files for removal once the last reader lets go of them - replaced by the snapshot in `by`.
    pub(crate) fn retire(&self, by: &Path) {
        *self.replaced_by.lock() = Some(by.to_path_buf());
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for SnapshotFiles {
    fn drop(&mut self) {
        if self.unadopted.load(Ordering::Acquire) {
            // Moved in, it is no longer here: nothing to remove.
            let _ = fs::remove_dir_all(&self.dir);
            return;
        }
        if !self.obsolete.load(Ordering::Acquire) {
            return;
        }
        // A SNAPSHOT IS REMOVED ONLY WHILE ANOTHER ONE IS ON DISK AND READS BACK. Whatever marked this one - and a
        // wrong mark is how a broker once lost its only snapshot - the removal itself looks first. Kept when in
        // doubt: an older snapshot left behind is cleared at the next start, a missing one is a lost copy.
        let replaced = self.replaced_by.lock().clone();
        let stands = replaced
            .as_deref()
            .is_some_and(|by| by != self.dir && read_record(by).is_ok());
        if !stands {
            tracing::warn!(
                dir = %self.dir.display(),
                "a snapshot marked as replaced is kept: the one said to replace it is not readable on disk"
            );
            return;
        }
        if let Err(e) = fs::remove_dir_all(&self.dir)
            && e.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %self.dir.display(), error = %e, "cannot remove a replaced snapshot");
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    meta: SnapshotMeta,
    manifest: Manifest,
}

/// `snapshot.json` as written: OpenDuct's own shape of what the snapshot covers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredRecord {
    pub(crate) meta: stable::SnapshotMeta,
    pub(crate) manifest: Manifest,
}

pub(crate) fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

pub(crate) fn io_of(e: StoreError) -> io::Error {
    match e {
        StoreError::Io(e) => e,
        other => io::Error::other(other.to_string()),
    }
}

pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Runs blocking file work off the async threads.
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| other(format!("a storage task failed: {e}")))?
}

fn write_record(dir: &Path, meta: &SnapshotMeta, manifest: &Manifest) -> io::Result<()> {
    let raw = serde_json::to_vec(&Versioned::current(StoredRecord {
        meta: meta.into(),
        manifest: manifest.clone(),
    }))
    .map_err(|e| other(format!("cannot encode a snapshot record: {e}")))?;
    let path = dir.join(RECORD);
    {
        let mut f = fs::File::create(&path)?;
        io::Write::write_all(&mut f, &raw)?;
        f.sync_all()?;
    }
    sync_dir(dir)
}

fn read_record(dir: &Path) -> io::Result<Record> {
    let path = dir.join(RECORD);
    let raw = fs::read(&path)?;
    let versioned: Versioned<StoredRecord> =
        serde_json::from_slice(&raw).map_err(|e| other(format!("{}: {e}", path.display())))?;
    let stored = versioned.open(&path.display().to_string()).map_err(other)?;
    Ok(Record {
        meta: stored.meta.try_into().map_err(other)?,
        manifest: stored.manifest,
    })
}

/// A snapshot just written or taken in, read back from the disk: its record is there, readable, and says what was
/// meant. Only then does anything rely on it.
fn read_back(dir: &Path, meta: &SnapshotMeta) -> io::Result<()> {
    let record = read_record(dir).map_err(|e| {
        other(format!(
            "the snapshot just written to {} does not read back: {e}",
            dir.display()
        ))
    })?;
    if record.meta.last_log_id != meta.last_log_id {
        return Err(other(format!(
            "the snapshot just written to {} reads back as a different one",
            dir.display()
        )));
    }
    Ok(())
}

/// Copies a snapshot's files, each up to its recorded length.
fn copy_snapshot(from: &Path, manifest: &Manifest, to: &Path) -> io::Result<()> {
    for f in &manifest.files {
        let rel = checked_path(&f.path).map_err(io_of)?;
        let target = to.join(&rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut source = io::Read::take(fs::File::open(from.join(&rel))?, f.len);
        let mut out = fs::File::create(&target)?;
        io::copy(&mut source, &mut out)?;
    }
    sync_checkpoint(to, manifest).map_err(io_of)
}

/// Removes what was left of a snapshot that was never finished: a directory, or a stray file.
fn remove_unfinished(path: &Path) -> io::Result<()> {
    let gone = if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    match gone {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

#[derive(Debug)]
struct Current {
    meta: SnapshotMeta,
    files: Arc<SnapshotFiles>,
}

/// One machine's snapshots.
#[derive(Debug)]
pub(crate) struct Snapshots {
    dir: PathBuf,
    counter: AtomicU64,
    current: Mutex<Option<Current>>,
    floor: Arc<Floor>,
}

/// HOW FAR A MACHINE'S RAFT LOG MAY BE PURGED: up to the last entry covered by a snapshot that is on this disk and
/// was read back from it - and no further. The log store asks here before every purge (log.rs), so that whatever
/// else goes wrong with snapshots, the entries a copy would need to rebuild itself are never the ones removed.
/// Raised only: when a snapshot is opened at a start, finished, or taken in, after its record was read back.
#[derive(Debug, Default)]
pub struct Floor {
    /// The index covered, plus one; 0: no snapshot, nothing may be purged.
    covered: AtomicU64,
}

impl Floor {
    /// A floor as a snapshot covering `index` would leave it (`None`: no snapshot) - for a log store used without
    /// a machine.
    pub fn covering(index: Option<u64>) -> Arc<Self> {
        let floor = Self::default();
        floor.raise(index);
        Arc::new(floor)
    }

    fn raise(&self, index: Option<u64>) {
        if let Some(index) = index {
            self.covered.fetch_max(index.saturating_add(1), Ordering::AcqRel);
        }
    }

    /// The last index a snapshot on disk covers; `None`: there is no snapshot.
    pub(crate) fn covered(&self) -> Option<u64> {
        self.covered.load(Ordering::Acquire).checked_sub(1)
    }
}

impl Snapshots {
    /// Opens the snapshots under `dir`: the newest complete one is kept, every older one, every unfinished one (a
    /// directory with no record: the record is written last), and whatever was on its way in, removed.
    ///
    /// A snapshot whose record is there and CANNOT BE READ is never removed, and the machine does not open: it may
    /// be the only copy of the partition on this broker (the Raft log behind it is purged), and what cannot be read
    /// now - a disk that failed one read, a record written by a newer build - may be readable at the next try or
    /// by the right build. Removing it would turn a passing fault into a lost copy.
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
        let snapshots = dir.join("snapshots");
        fs::create_dir_all(&snapshots)?;
        if dir.join("incoming").exists() {
            fs::remove_dir_all(dir.join("incoming"))?;
        }
        let mut newest: Option<(Record, PathBuf)> = None;
        let mut counter = 0u64;
        for entry in fs::read_dir(&snapshots)? {
            let path = entry?.path();
            if let Some(n) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("snap-"))
                .and_then(|n| n.parse::<u64>().ok())
            {
                counter = counter.max(n + 1);
            }
            match read_record(&path) {
                Ok(record)
                    if newest
                        .as_ref()
                        .is_none_or(|(n, _)| record.meta.last_log_id > n.meta.last_log_id) =>
                {
                    if let Some((_, older)) = newest.replace((record, path)) {
                        fs::remove_dir_all(older)?;
                    }
                }
                // An older one than the newest already found.
                Ok(_) => fs::remove_dir_all(&path)?,
                // No record: it was never finished.
                Err(e) if e.kind() == io::ErrorKind::NotFound => remove_unfinished(&path)?,
                Err(e) => {
                    return Err(other(format!(
                        "the snapshot in {} cannot be read ({e}); it is kept as it is, and this copy does not start \
                         until it can be",
                        path.display()
                    )));
                }
            }
        }
        let current = newest.map(|(record, path)| Current {
            meta: record.meta,
            files: Arc::new(SnapshotFiles::new(path, record.manifest)),
        });
        let floor = Arc::new(Floor::default());
        floor.raise(
            current
                .as_ref()
                .and_then(|c| c.meta.last_log_id.as_ref().map(|l| l.index)),
        );
        Ok(Self {
            dir: dir.to_path_buf(),
            counter: AtomicU64::new(counter),
            current: Mutex::new(current),
            floor,
        })
    }

    fn snapshots(&self) -> PathBuf {
        self.dir.join("snapshots")
    }

    /// A new directory name for a snapshot being made.
    pub(crate) fn next_dir(&self) -> PathBuf {
        self.snapshots().join(format!(
            "snap-{:020}",
            self.counter.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// A new, empty directory for a snapshot on its way from the leader.
    pub(crate) fn incoming_dir(&self) -> io::Result<PathBuf> {
        let dir = self
            .dir
            .join("incoming")
            .join(self.counter.fetch_add(1, Ordering::Relaxed).to_string());
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    pub(crate) fn current(&self) -> Option<(SnapshotMeta, Arc<SnapshotFiles>)> {
        self.current
            .lock()
            .as_ref()
            .map(|c| (c.meta.clone(), Arc::clone(&c.files)))
    }

    fn set_current(&self, meta: SnapshotMeta, files: Arc<SnapshotFiles>, newer_only: bool) {
        let mut current = self.current.lock();
        if newer_only
            && current
                .as_ref()
                .is_some_and(|c| c.meta.last_log_id >= meta.last_log_id)
        {
            // Older than the current one: it goes, the current one being what stands in its place.
            if let Some(c) = current.as_ref() {
                files.retire(&c.files.dir);
            }
            return;
        }
        let same = current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(&c.files, &files) || c.files.dir == files.dir);
        let by = files.dir.clone();
        self.floor.raise(meta.last_log_id.as_ref().map(|l| l.index));
        if let Some(old) = current.replace(Current { meta, files })
            && !same
        {
            // Only a snapshot that was REPLACED is retired. On a start Raft installs the machine's own current
            // snapshot - the same files, handed back - and retiring "the old one" then marked the only snapshot
            // for removal at the next clean stop, with the Raft log behind it already purged: the partition could
            // not be rebuilt on this broker again (seen 2026-11-24, after every broker was restarted twice).
            old.files.retire(&by);
        }
    }

    /// How far this machine's Raft log may be purged: see [`Floor`].
    pub(crate) fn floor(&self) -> Arc<Floor> {
        Arc::clone(&self.floor)
    }

    /// Completes a snapshot made in `dir` (from [`Self::next_dir`]): its files flushed, its record written. It becomes
    /// the current one unless a newer one already is. Blocking.
    pub(crate) fn finish(
        &self,
        dir: PathBuf,
        meta: SnapshotMeta,
        manifest: Manifest,
    ) -> io::Result<Arc<SnapshotFiles>> {
        sync_checkpoint(&dir, &manifest).map_err(io_of)?;
        write_record(&dir, &meta, &manifest)?;
        sync_dir(&self.snapshots())?;
        // Read back before anything is purged on the strength of it.
        read_back(&dir, &meta)?;
        let files = Arc::new(SnapshotFiles::new(dir, manifest));
        self.set_current(meta, Arc::clone(&files), true);
        Ok(files)
    }

    /// Takes in a snapshot Raft installs: one of this machine's own (on a restart), one received from the leader
    /// (moved in), or another machine's in this process (copied). It becomes the current one. Blocking.
    pub(crate) fn adopt(
        &self,
        meta: &SnapshotMeta,
        snapshot: Arc<SnapshotFiles>,
    ) -> io::Result<Arc<SnapshotFiles>> {
        let files = if snapshot.dir.starts_with(self.snapshots()) {
            snapshot
        } else if snapshot.dir.starts_with(self.dir.join("incoming")) {
            let dir = self.next_dir();
            write_record(&snapshot.dir, meta, &snapshot.manifest)?;
            fs::rename(&snapshot.dir, &dir)?;
            sync_dir(&self.snapshots())?;
            Arc::new(SnapshotFiles::new(dir, snapshot.manifest.clone()))
        } else {
            let dir = self.next_dir();
            copy_snapshot(&snapshot.dir, &snapshot.manifest, &dir)?;
            write_record(&dir, meta, &snapshot.manifest)?;
            sync_dir(&self.snapshots())?;
            Arc::new(SnapshotFiles::new(dir, snapshot.manifest.clone()))
        };
        read_back(&files.dir, meta)?;
        self.set_current(meta.clone(), Arc::clone(&files), false);
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What Raft does with a machine that keeps nothing in memory across a start: it asks for the current snapshot
    /// and installs it - the machine's own, handed straight back.
    fn restart(dir: &Path) -> Snapshots {
        let snapshots = Snapshots::open(dir).unwrap();
        if let Some((meta, files)) = snapshots.current() {
            snapshots.adopt(&meta, files).unwrap();
        }
        snapshots
    }

    #[test]
    fn a_machines_own_snapshot_is_still_there_after_any_number_of_restarts() {
        let tmp = tempfile::tempdir().unwrap();
        let first = Snapshots::open(tmp.path()).unwrap();
        let dir = first.next_dir();
        fs::create_dir_all(&dir).unwrap();
        first
            .finish(dir, SnapshotMeta::default(), Manifest::default())
            .unwrap();
        assert!(first.current().is_some());
        // Stopped cleanly: everything is dropped.
        drop(first);

        // Started, and stopped cleanly again before a newer snapshot was made - twice over. The Raft log behind this
        // snapshot is purged: without it, the partition cannot be rebuilt on this broker.
        for start in 1..=3 {
            let snapshots = restart(tmp.path());
            assert!(
                snapshots.current().is_some(),
                "the snapshot is gone at start {start}"
            );
            drop(snapshots);
        }
    }
    fn at(index: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_log_id: Some(
                stable::LogId {
                    term: 1,
                    node: 1,
                    index,
                }
                .into(),
            ),
            ..SnapshotMeta::default()
        }
    }

    fn finished(snapshots: &Snapshots, index: u64) -> PathBuf {
        let dir = snapshots.next_dir();
        fs::create_dir_all(&dir).unwrap();
        snapshots
            .finish(dir.clone(), at(index), Manifest::default())
            .unwrap();
        dir
    }

    fn index_of(snapshots: &Snapshots) -> u64 {
        snapshots.current().unwrap().0.last_log_id.unwrap().index
    }

    #[test]
    fn a_newer_snapshot_replaces_the_older_one_and_only_then_is_the_older_one_removed() {
        let tmp = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(tmp.path()).unwrap();
        let older = finished(&snapshots, 10);
        let newer = finished(&snapshots, 20);
        assert_eq!(index_of(&snapshots), 20);
        drop(snapshots);
        assert!(
            !older.exists(),
            "the replaced snapshot goes once nothing holds it"
        );
        assert!(newer.exists());
        // An older one finished late does not take the newer one's place, and goes.
        let snapshots = restart(tmp.path());
        let late = finished(&snapshots, 15);
        assert_eq!(index_of(&snapshots), 20);
        drop(snapshots);
        assert!(!late.exists());
        assert!(newer.exists());
        assert_eq!(index_of(&restart(tmp.path())), 20);
    }

    #[test]
    fn an_unfinished_snapshot_is_removed_at_a_start_and_the_finished_one_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(tmp.path()).unwrap();
        let kept = finished(&snapshots, 10);
        // A crash while the next one was being made: its files are there, its record is not.
        let unfinished = snapshots.next_dir();
        fs::create_dir_all(unfinished.join("segments")).unwrap();
        fs::write(unfinished.join("segments").join("0.log"), b"half").unwrap();
        // And one that was on its way in from the leader.
        let incoming = snapshots.incoming_dir().unwrap();
        fs::write(incoming.join("part"), b"half").unwrap();
        drop(snapshots);

        let snapshots = restart(tmp.path());
        assert!(!unfinished.exists());
        assert!(!incoming.exists());
        assert!(kept.exists());
        assert_eq!(index_of(&snapshots), 10);
    }

    #[test]
    fn a_snapshot_whose_record_cannot_be_read_is_never_removed_and_the_machine_does_not_open() {
        let tmp = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(tmp.path()).unwrap();
        let only = finished(&snapshots, 10);
        drop(snapshots);
        let record = only.join(RECORD);
        let good = fs::read(&record).unwrap();

        // Damaged on disk, and written by a build newer than this one: neither is a reason to remove it.
        let newer_build =
            String::from_utf8(good.clone())
                .unwrap()
                .replacen("\"format\":1", "\"format\":99", 1);
        assert_ne!(newer_build.as_bytes(), good.as_slice());
        for bad in [b"{ not json".to_vec(), newer_build.into_bytes()] {
            fs::write(&record, &bad).unwrap();
            let refused = Snapshots::open(tmp.path()).expect_err("an unreadable record opens nothing");
            assert!(refused.to_string().contains("cannot be read"), "{refused}");
            assert!(only.exists(), "the snapshot is still there");
            assert_eq!(fs::read(&record).unwrap(), bad, "and untouched");
        }
        // Readable again (the fault passed, or the right build is running): it opens with the snapshot it had.
        fs::write(&record, &good).unwrap();
        assert_eq!(index_of(&restart(tmp.path())), 10);
    }

    #[test]
    fn the_log_may_be_purged_only_as_far_as_a_snapshot_read_back_from_disk_covers() {
        let tmp = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(tmp.path()).unwrap();
        let floor = snapshots.floor();
        assert_eq!(floor.covered(), None, "no snapshot: nothing may be purged");
        finished(&snapshots, 10);
        assert_eq!(floor.covered(), Some(10));
        finished(&snapshots, 25);
        assert_eq!(floor.covered(), Some(25));
        // An older one finished late lowers nothing.
        finished(&snapshots, 15);
        assert_eq!(floor.covered(), Some(25));
        drop(snapshots);
        // After a start it is what the snapshot found on disk covers.
        assert_eq!(restart(tmp.path()).floor().covered(), Some(25));
        // And with the snapshot gone from the disk, nothing.
        fs::remove_dir_all(tmp.path().join("snapshots")).unwrap();
        assert_eq!(Snapshots::open(tmp.path()).unwrap().floor().covered(), None);
    }

    #[test]
    fn a_snapshot_marked_as_replaced_is_kept_when_what_replaced_it_is_not_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(tmp.path()).unwrap();
        let older = finished(&snapshots, 10);
        let held = snapshots.current().unwrap().1;
        let newer = finished(&snapshots, 20);
        // The newer one is lost before the older one is let go of (a fault, a mistake, anything).
        fs::remove_file(newer.join(RECORD)).unwrap();
        drop(held);
        drop(snapshots);
        assert!(older.exists(), "the only readable snapshot was removed");
        // At the next start it is the one found, and the copy can still be rebuilt from it.
        assert_eq!(index_of(&restart(tmp.path())), 10);
    }
}
