// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A partition's state machine: its log on this broker, changed only by applying the group's committed commands -
//! the same ones, in the same order, on every replica.
//!
//! The log itself is never flushed: the group's Raft log and its snapshots are what survive a crash. On every start
//! it is rebuilt from the newest snapshot and brought up to date by re-applying the Raft log after it. A snapshot is
//! a checkpoint of the partition - hard links to its segments with their lengths, and its producer table - so taking
//! one costs a link per segment, and it is what a replica that fell too far behind receives from the leader.
//!
//! Under the machine's directory: `live-<n>/`, the log being served (`n` grows each time it is rebuilt), and the
//! snapshots (`snapshot`).

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::{Stream, StreamExt};
use openduct_storage::{FileStoreConfig, FsyncPolicy, Manifest, PartitionLog, Remote, restore_checkpoint};
use openraft::alias::SnapshotOf;
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use parking_lot::{Mutex, RwLock};

use crate::snapshot::{SnapshotFiles, Snapshots, blocking, io_of, other};
use crate::types::{Command, Entry, LogId, Refusal, Reply, SnapshotMeta, StoredMembership, TypeConfig};

#[derive(Debug)]
pub(crate) struct PartitionShared {
    dir: PathBuf,
    config: FileStoreConfig,
    /// Object storage, to read segments no longer on the local disk.
    remote: Option<Arc<dyn Remote>>,
    log: RwLock<Arc<PartitionLog>>,
    generation: AtomicU64,
    /// The last entry applied and the last membership applied.
    applied: Mutex<(Option<LogId>, StoredMembership)>,
    pub(crate) snapshots: Snapshots,
}

/// The state machine handed to a partition's Raft group.
#[derive(Debug, Clone)]
pub struct PartitionMachine {
    shared: Arc<PartitionShared>,
}

impl PartitionShared {
    fn with_remote(&self, log: PartitionLog) -> PartitionLog {
        match &self.remote {
            Some(r) => log.with_remote(Arc::clone(r)),
            None => log,
        }
    }

    /// The log as it is now. Reads go to it; it is replaced when a snapshot is installed.
    pub(crate) fn log(&self) -> Arc<PartitionLog> {
        Arc::clone(&self.log.read())
    }

    /// Replaces the served log with one rebuilt from the snapshot in `files`.
    fn restore(&self, files: &SnapshotFiles) -> io::Result<()> {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let dir = self.dir.join(format!("live-{generation}"));
        restore_checkpoint(&files.dir, &files.manifest, &dir).map_err(io_of)?;
        let log = Arc::new(self.with_remote(PartitionLog::open(&dir, self.config).map_err(io_of)?));
        let old = std::mem::replace(&mut *self.log.write(), log);
        let old_dir = old.dir().to_path_buf();
        drop(old);
        if let Err(e) = fs::remove_dir_all(&old_dir) {
            tracing::warn!(dir = %old_dir.display(), error = %e, "cannot remove the replaced log yet");
        }
        Ok(())
    }

    /// Applies one committed entry. A refusal is the command's answer; a failure of this broker's disk is an error
    /// that stops the group here.
    fn apply_one(&self, entry: Entry) -> io::Result<Reply> {
        let log_id = entry.log_id;
        let reply = match entry.payload {
            EntryPayload::Blank => Reply::Done,
            EntryPayload::Membership(m) => {
                self.applied.lock().1 = StoredMembership::new(Some(log_id), m);
                Reply::Done
            }
            EntryPayload::Normal(command) => self.run(command, epoch_of_term(log_id.leader_id.term))?,
        };
        self.applied.lock().0 = Some(log_id);
        Ok(reply)
    }

    /// Runs one command of an entry written in `epoch` (its term): an appended batch is stored in that epoch, so every
    /// copy stores the same bytes.
    fn run(&self, command: Command, epoch: i32) -> io::Result<Reply> {
        let log = self.log();
        let what = command.to_string();
        let result = match command {
            Command::Append { batch } => log.append_in(batch, epoch).map(|a| Reply::Appended {
                base_offset: a.base_offset,
                log_start_offset: a.log_start_offset,
                duplicate: a.duplicate,
            }),
            Command::Retain { before_ms } => log
                .enforce_retention(before_ms)
                .map(|n| Reply::Removed(u64::try_from(n).unwrap_or(u64::MAX))),
            Command::Tiered { segment } => log.mark_tiered(segment).map(|()| Reply::Done),
            Command::Offload { before } => log
                .offload(before)
                .map(|n| Reply::Removed(u64::try_from(n).unwrap_or(u64::MAX))),
            // In a partition of the offsets topic: a group's commit.
            Command::CommitOffsets { group, offsets } => {
                log.commit_offsets(&group, &offsets);
                Ok(Reply::Done)
            }
            Command::DeleteOffsets { group, partitions } => {
                log.delete_offsets(&group, partitions.as_deref());
                Ok(Reply::Done)
            }
            Command::DeleteRecords { before } => log.delete_records(before).map(|_| Reply::Done),
            _ => return Ok(Reply::Refused(Refusal::Misrouted(what))),
        };
        match result {
            Ok(reply) => Ok(reply),
            Err(e) => match Refusal::of(&e) {
                Some(refusal) => Ok(Reply::Refused(refusal)),
                None => Err(other(format!("applying \"{what}\" failed on this broker: {e}"))),
            },
        }
    }
}

/// A partition's leader epoch for a Raft term (Kafka's epochs are 32 bits; a term that large is never reached).
pub(crate) fn epoch_of_term(term: u64) -> i32 {
    i32::try_from(term).unwrap_or(i32::MAX)
}

impl PartitionMachine {
    /// Opens the machine in `dir`: an empty log to start from (Raft then installs the newest snapshot and re-applies
    /// the log after it). `config` is the log's; nothing in it is flushed.
    pub fn open(
        dir: impl Into<PathBuf>,
        config: FileStoreConfig,
        remote: Option<Arc<dyn Remote>>,
    ) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("live-") {
                fs::remove_dir_all(entry.path())?;
            }
        }
        let snapshots = Snapshots::open(&dir)?;
        let config = FileStoreConfig {
            fsync: FsyncPolicy::Never,
            ..config
        };
        let log = PartitionLog::open(dir.join("live-0"), config).map_err(io_of)?;
        let log = match &remote {
            Some(r) => log.with_remote(Arc::clone(r)),
            None => log,
        };
        Ok(Self {
            shared: Arc::new(PartitionShared {
                dir,
                config,
                remote,
                log: RwLock::new(Arc::new(log)),
                generation: AtomicU64::new(0),
                applied: Mutex::new((None, StoredMembership::default())),
                snapshots,
            }),
        })
    }

    pub(crate) fn shared(&self) -> Arc<PartitionShared> {
        Arc::clone(&self.shared)
    }
}

/// A snapshot being made: the log's files already linked (consistent with the entries applied so far), still to be
/// flushed and recorded.
#[derive(Debug)]
pub struct PartitionSnapshot {
    shared: Arc<PartitionShared>,
    dir: PathBuf,
    taken: Result<(SnapshotMeta, Manifest), String>,
}

impl RaftSnapshotBuilder<TypeConfig> for PartitionSnapshot {
    type SnapshotData = Arc<SnapshotFiles>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, Arc<SnapshotFiles>>, io::Error> {
        let (meta, manifest) = self.taken.clone().map_err(other)?;
        let (shared, dir, m) = (Arc::clone(&self.shared), self.dir.clone(), meta.clone());
        let files = blocking(move || shared.snapshots.finish(dir, m, manifest)).await?;
        Ok(SnapshotOf::<TypeConfig, _> {
            meta,
            snapshot: files,
        })
    }
}

impl RaftStateMachine<TypeConfig> for PartitionMachine {
    type SnapshotData = Arc<SnapshotFiles>;
    type SnapshotBuilder = PartitionSnapshot;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok(self.shared.applied.lock().clone())
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        // Whatever is ready is applied together, off the async threads (the log writes files), in order.
        let mut chunks = entries.ready_chunks(256);
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.into_iter().collect::<io::Result<Vec<_>>>()?;
            let shared = Arc::clone(&self.shared);
            blocking(move || {
                for (entry, responder) in chunk {
                    let reply = shared.apply_one(entry)?;
                    if let Some(responder) = responder {
                        responder.send(reply);
                    }
                }
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<PartitionSnapshot> {
        Some(self.get_snapshot_builder().await)
    }

    async fn get_snapshot_builder(&mut self) -> PartitionSnapshot {
        // Linked here, between two applies, so the snapshot is exactly the entries applied so far.
        let shared = Arc::clone(&self.shared);
        let dir = shared.snapshots.next_dir();
        let (last_log_id, last_membership) = shared.applied.lock().clone();
        let taken = shared
            .log()
            .checkpoint(&dir)
            .map(|manifest| {
                (
                    SnapshotMeta {
                        last_log_id,
                        last_membership,
                    },
                    manifest,
                )
            })
            .map_err(|e| format!("cannot take a snapshot: {e}"));
        PartitionSnapshot { shared, dir, taken }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Arc<SnapshotFiles>,
    ) -> Result<(), io::Error> {
        let shared = Arc::clone(&self.shared);
        let meta = meta.clone();
        blocking(move || {
            let files = shared.snapshots.adopt(&meta, snapshot)?;
            shared.restore(&files)?;
            *shared.applied.lock() = (meta.last_log_id, meta.last_membership.clone());
            Ok(())
        })
        .await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Arc<SnapshotFiles>>>, io::Error> {
        Ok(self
            .shared
            .snapshots
            .current()
            .map(|(meta, files)| SnapshotOf::<TypeConfig, _> {
                meta,
                snapshot: files,
            }))
    }
}
