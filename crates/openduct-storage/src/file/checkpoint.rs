// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Checkpoints: a store's files as they stand at one moment, kept as hard links in a directory of their own.
//!
//! A log segment only ever grows (and is removed whole by retention), and every other file is replaced by a rename,
//! never rewritten in place. So a hard link taken while the store is still, together with each file's length at that
//! moment, keeps that moment for good: later appends land past the recorded length, a later rename leaves the linked
//! file as it was, and a removed segment lives on through its link. Taking one costs a link per file, not a copy.
//!
//! [`restore_checkpoint`] builds a store directory back from one: every file linked (or copied, across devices) and
//! cut to its recorded length. A cut shortens the file for every link to it, so a store is only ever restored from
//! its newest checkpoint: an older one may share a file the cut would shorten below its own length.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{FileStore, sync_dir};
use crate::error::StoreError;

/// One file of a checkpoint: its path inside the store (parts joined with `/`) and its length when taken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub len: u64,
}

/// Every file of a checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub files: Vec<ManifestFile>,
}

impl Manifest {
    /// The bytes the checkpoint holds.
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.len).sum()
    }
}

/// A manifest path as a relative path that stays inside the directory it is joined to: plain parts only, never `..`,
/// a root or a drive. A manifest can come from another broker, so this is checked before any file is touched.
pub fn checked_path(path: &str) -> Result<PathBuf, StoreError> {
    let refused = || {
        StoreError::Corrupt(format!(
            "a checkpoint names the path {path:?}, outside its directory"
        ))
    };
    if path.is_empty() || path.contains('\\') {
        return Err(refused());
    }
    let mut out = PathBuf::new();
    for part in path.split('/') {
        let as_path = Path::new(part);
        match as_path.components().next() {
            Some(Component::Normal(_)) if as_path.components().count() == 1 => out.push(part),
            _ => return Err(refused()),
        }
    }
    Ok(out)
}

fn link_or_copy(from: &Path, to: &Path) -> Result<(), StoreError> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    if fs::hard_link(from, to).is_err() {
        // Another device (or a file system without links): a copy keeps the same bytes.
        fs::copy(from, to)?;
    }
    Ok(())
}

/// Every directory the files sit in, deepest first, and `root` itself.
fn directories(root: &Path, manifest: &Manifest) -> Result<Vec<PathBuf>, StoreError> {
    let mut dirs = BTreeSet::new();
    for f in &manifest.files {
        let mut at = checked_path(&f.path)?;
        while at.pop() && !at.as_os_str().is_empty() {
            dirs.insert(root.join(&at));
        }
    }
    let mut dirs: Vec<PathBuf> = dirs.into_iter().collect();
    dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
    dirs.push(root.to_path_buf());
    Ok(dirs)
}

/// Flushes a checkpoint to the device: its files (up to their lengths, and past them, which is harmless) and the
/// directory entries that name them. Until this returns, a crash may lose the checkpoint.
pub fn sync_checkpoint(dir: &Path, manifest: &Manifest) -> Result<(), StoreError> {
    for f in &manifest.files {
        File::open(dir.join(checked_path(&f.path)?))?.sync_all()?;
    }
    for d in directories(dir, manifest)? {
        sync_dir(&d)?;
    }
    Ok(())
}

/// Builds the store directory `dest` (which must not exist yet) from the checkpoint in `from`: each file linked, or
/// copied, and cut to its recorded length; then flushed. Open it with [`FileStore::open`].
pub fn restore_checkpoint(from: &Path, manifest: &Manifest, dest: &Path) -> Result<(), StoreError> {
    if dest.exists() {
        return Err(StoreError::Corrupt(format!(
            "{} already exists: a store is restored into a new directory",
            dest.display()
        )));
    }
    fs::create_dir_all(dest)?;
    for f in &manifest.files {
        let rel = checked_path(&f.path)?;
        let target = dest.join(&rel);
        link_or_copy(&from.join(&rel), &target)?;
        let file = OpenOptions::new().write(true).open(&target)?;
        let len = file.metadata()?.len();
        if len < f.len {
            return Err(StoreError::Corrupt(format!(
                "{} holds {len} bytes; its checkpoint recorded {}",
                target.display(),
                f.len
            )));
        }
        if len > f.len {
            file.set_len(f.len)?;
        }
        file.sync_all()?;
    }
    for d in directories(dest, manifest)? {
        sync_dir(&d)?;
    }
    Ok(())
}

impl FileStore {
    /// Takes a checkpoint of the store into `dest` (a new directory): every file linked as it stands now, with its
    /// length. Appends, topic changes and offset commits wait while the links are made. The checkpoint is on the
    /// device only once [`sync_checkpoint`] has run on it.
    pub fn checkpoint(&self, dest: &Path) -> Result<Manifest, StoreError> {
        if dest.exists() {
            return Err(StoreError::Corrupt(format!(
                "{} already exists: a checkpoint is taken into a new directory",
                dest.display()
            )));
        }
        // The whole store held still: no append, no topic created or deleted, no offset or producer id written.
        let topics = self.topics.write();
        let _groups = self.groups.all();
        let _meta = self.meta.lock();
        fs::create_dir_all(dest)?;
        let mut files = Vec::new();
        let mut link = |rel: String, len: Option<u64>| -> Result<(), StoreError> {
            let path = checked_path(&rel)?;
            let from = self.root.join(&path);
            let len = match len {
                Some(len) => len,
                None => fs::metadata(&from)?.len(),
            };
            link_or_copy(&from, &dest.join(&path))?;
            files.push(ManifestFile { path: rel, len });
            Ok(())
        };
        link("meta.json".to_owned(), None)?;
        let mut groups: Vec<String> = Vec::new();
        for entry in fs::read_dir(self.root.join("groups"))? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str()
                && name.ends_with(".json")
            {
                groups.push(name.to_owned());
            }
        }
        groups.sort();
        for g in groups {
            link(format!("groups/{g}"), None)?;
        }
        let mut names: Vec<&String> = topics.keys().collect();
        names.sort();
        for name in names {
            let Some(t) = topics.get(name) else { continue };
            link(format!("topics/{name}/topic.json"), None)?;
            for (i, p) in t.partitions.iter().enumerate() {
                let p = p.lock();
                for seg in p.segments.values() {
                    let Some(file) = seg.path.file_name().and_then(|f| f.to_str()) else {
                        continue;
                    };
                    // The length the store has written, which may run ahead of what a reader sees yet: whatever
                    // the store held at this moment is in the checkpoint.
                    link(format!("topics/{name}/{i}/{file}"), Some(seg.size))?;
                }
                if p.dir.join(super::partition_log::START).exists() {
                    link(format!("topics/{name}/{i}/{}", super::partition_log::START), None)?;
                }
            }
        }
        Ok(Manifest { files })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::tests::batch;
    use crate::file::FileStoreConfig;
    use crate::store::{CommittedOffset, Store, TopicConfig};

    fn small_segments() -> FileStoreConfig {
        FileStoreConfig {
            segment_bytes: 200,
            ..FileStoreConfig::default()
        }
    }

    #[test]
    fn a_restored_store_is_the_store_as_it_was_when_the_checkpoint_was_taken() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("live");
        let store = FileStore::open(&live, small_segments()).unwrap();
        let config = TopicConfig {
            partitions: 2,
            retention_ms: Some(1_000),
        };
        store.create_topic("t", config).unwrap();
        for i in 0..6 {
            store
                .append("t", i % 2, batch(2, -1, -1, -1, i64::from(i) * 100))
                .unwrap();
        }
        let offset = |o: i64| CommittedOffset {
            topic: "t".to_owned(),
            partition: 0,
            offset: o,
            metadata: None,
            commit_timestamp_ms: 1,
        };
        store.commit_offsets("g", &[offset(4)]).unwrap();
        let producer = store.next_producer_id().unwrap();
        let then = store.read("t", 0, 0, usize::MAX).unwrap();

        let snap = dir.path().join("snap");
        let manifest = store.checkpoint(&snap).unwrap();
        sync_checkpoint(&snap, &manifest).unwrap();
        assert!(manifest.files.iter().filter(|f| f.path.ends_with(".log")).count() >= 2);

        // The store moves on: more appends, a new commit, retention removes the oldest segments, a new topic.
        for i in 0..4 {
            store.append("t", 0, batch(1, -1, -1, -1, 10_000 + i)).unwrap();
        }
        store.commit_offsets("g", &[offset(9)]).unwrap();
        assert!(store.enforce_retention(10_500).unwrap() > 0);
        store
            .create_topic(
                "later",
                TopicConfig {
                    partitions: 1,
                    retention_ms: None,
                },
            )
            .unwrap();
        drop(store);

        let restored_dir = dir.path().join("restored");
        restore_checkpoint(&snap, &manifest, &restored_dir).unwrap();
        let restored = FileStore::open(&restored_dir, small_segments()).unwrap();
        assert_eq!(restored.read("t", 0, 0, usize::MAX).unwrap(), then);
        assert_eq!(restored.committed_offsets("g").unwrap(), vec![offset(4)]);
        assert_eq!(restored.next_producer_id().unwrap(), producer + 1);
        assert!(restored.topic("later").is_none());
        assert_eq!(restored.offsets("t", 1).unwrap().high_watermark, 6);
    }

    #[test]
    fn manifest_paths_stay_inside_their_directory() {
        for good in [
            "meta.json",
            "topics/a/0/00000000000000000000.log",
            "groups/6162.json",
        ] {
            assert!(checked_path(good).is_ok(), "{good}");
        }
        for bad in [
            "",
            "/etc/passwd",
            "../x",
            "a/../../x",
            "a//b",
            "a/./b",
            "a\\b",
            "a/",
        ] {
            assert!(checked_path(bad).is_err(), "{bad}");
        }
    }
}
