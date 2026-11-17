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
}

impl SnapshotFiles {
    pub(crate) fn new(dir: PathBuf, manifest: Manifest) -> Self {
        Self {
            dir,
            manifest,
            obsolete: AtomicBool::new(false),
        }
    }

    /// Marks the files for removal once the last reader lets go of them.
    pub(crate) fn retire(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for SnapshotFiles {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire)
            && let Err(e) = fs::remove_dir_all(&self.dir)
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
}

impl Snapshots {
    /// Opens the snapshots under `dir`: the newest complete one is kept, every other one, and whatever was on its way
    /// in, removed.
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
                _ => fs::remove_dir_all(&path)?,
            }
        }
        let current = newest.map(|(record, path)| Current {
            meta: record.meta,
            files: Arc::new(SnapshotFiles::new(path, record.manifest)),
        });
        Ok(Self {
            dir: dir.to_path_buf(),
            counter: AtomicU64::new(counter),
            current: Mutex::new(current),
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
            files.retire();
            return;
        }
        if let Some(old) = current.replace(Current { meta, files }) {
            old.files.retire();
        }
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
        self.set_current(meta.clone(), Arc::clone(&files), false);
        Ok(files)
    }
}
