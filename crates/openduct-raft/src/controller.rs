// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The controller: the cluster's own Raft group, over every broker. Its state is small - the topics with, per
//! partition, the brokers holding it; the next producer id; consumer groups' committed offsets - so it is kept in
//! memory, rebuilt on start from its newest snapshot (`state.json`) and the log after it.
//!
//! Every broker applies the same controller commands; each then starts the partition groups it is given and stops
//! those of deleted topics (see `replica`).

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use openduct_storage::{
    CommittedOffset, MAX_PARTITIONS, Manifest, ManifestFile, StoreError, TopicInfo, validate_topic_name,
};
use openraft::alias::SnapshotOf;
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::snapshot::{SnapshotFiles, Snapshots, blocking, other};
use crate::stable::Versioned;
use crate::types::{Command, Entry, LogId, Refusal, Reply, SnapshotMeta, StoredMembership, TypeConfig};

const STATE: &str = "state.json";

/// A topic as the controller keeps it: its settings, and per partition the brokers holding a copy (the first one
/// is the partition's preferred leader).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub info: TopicInfo,
    pub replicas: Vec<Vec<u64>>,
    /// The index of the controller entry that created it: a broker back with an empty disk knows which partitions
    /// held data before it went.
    #[serde(default)]
    pub created_at: u64,
    /// Partitions on their way to other brokers: where each is going (the first broker to lead it).
    #[serde(default)]
    pub moving: BTreeMap<i32, Vec<u64>>,
    /// The index of the controller entry that added each partition after the topic's first ones, in order: the
    /// last `added_at.len()` partitions. Not written while there are none, so a state without any reads and writes
    /// as before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_at: Vec<u64>,
}

impl TopicRecord {
    /// The index of the controller entry that created `partition`: its topic's, or the one that added it.
    pub fn created_at_of(&self, partition: usize) -> u64 {
        let first_added = self.replicas.len().saturating_sub(self.added_at.len());
        partition
            .checked_sub(first_added)
            .and_then(|i| self.added_at.get(i).copied())
            .unwrap_or(self.created_at)
    }
}

/// A broker's build, as it registered when it last started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRecord {
    pub version: String,
    pub supported_level: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerState {
    pub topics: BTreeMap<String, TopicRecord>,
    pub next_producer_id: i64,
    pub offsets: BTreeMap<String, Vec<CommittedOffset>>,
    /// Every broker's build, by id.
    #[serde(default)]
    pub brokers: BTreeMap<u64, BrokerRecord>,
    /// The feature level the cluster uses (see `features`); 0 until the first brokers have registered.
    #[serde(default)]
    pub level: u32,
}

impl ControllerState {
    /// Applies the command of entry `index`; a refusal is its answer.
    fn run(&mut self, command: Command, index: u64) -> Reply {
        let refused =
            |e: StoreError| Reply::Refused(Refusal::of(&e).unwrap_or(Refusal::Misrouted(e.to_string())));
        match command {
            Command::CreateTopic {
                id,
                name,
                config,
                replicas,
            } => {
                if let Err(e) = validate_topic_name(&name) {
                    return refused(e);
                }
                let fits = usize::try_from(config.partitions).is_ok_and(|n| n == replicas.len());
                if !(1..=MAX_PARTITIONS).contains(&config.partitions)
                    || !fits
                    || replicas.iter().any(Vec::is_empty)
                {
                    return refused(StoreError::InvalidPartitions {
                        requested: config.partitions,
                        max: MAX_PARTITIONS,
                    });
                }
                if self.topics.contains_key(&name) {
                    return refused(StoreError::TopicExists(name));
                }
                let info = TopicInfo {
                    id,
                    name: name.clone(),
                    partitions: config.partitions,
                    retention_ms: config.retention_ms,
                    settings: Default::default(),
                };
                self.topics.insert(
                    name,
                    TopicRecord {
                        info,
                        replicas,
                        created_at: index,
                        moving: BTreeMap::new(),
                        added_at: Vec::new(),
                    },
                );
                Reply::Done
            }
            Command::DeleteTopic { name } => {
                if self.topics.remove(&name).is_none() {
                    return refused(StoreError::UnknownTopic(name));
                }
                // A topic of the same name created later starts clean.
                for committed in self.offsets.values_mut() {
                    committed.retain(|c| c.topic != name);
                }
                self.offsets.retain(|_, committed| !committed.is_empty());
                Reply::Done
            }
            Command::NextProducerId => {
                let id = self.next_producer_id;
                self.next_producer_id += 1;
                Reply::ProducerId(id)
            }
            Command::CommitOffsets { group, offsets } => {
                let committed = self.offsets.entry(group).or_default();
                for o in offsets {
                    committed.retain(|c| !(c.topic == o.topic && c.partition == o.partition));
                    committed.push(o);
                }
                Reply::Done
            }
            Command::AddPartitions {
                name,
                total,
                replicas,
            } => {
                let Some(t) = self.topics.get_mut(&name) else {
                    return refused(StoreError::UnknownTopic(name));
                };
                let fits = usize::try_from(total).is_ok_and(|n| n == t.replicas.len() + replicas.len());
                if openduct_storage::is_internal_topic(&name)
                    || !fits
                    || replicas.is_empty()
                    || total > MAX_PARTITIONS
                {
                    // Another change came first (the total no longer adds up), or nothing to add: proposed again.
                    return refused(StoreError::InvalidPartitions {
                        requested: total,
                        max: MAX_PARTITIONS,
                    });
                }
                if replicas.iter().any(Vec::is_empty) {
                    return refused(StoreError::InvalidReplicas(
                        "a new partition with no broker".to_owned(),
                    ));
                }
                t.added_at.extend(std::iter::repeat_n(index, replicas.len()));
                t.replicas.extend(replicas);
                t.info.partitions = total;
                Reply::Done
            }
            Command::SetRetention { name, retention_ms } => {
                let Some(t) = self.topics.get_mut(&name) else {
                    return refused(StoreError::UnknownTopic(name));
                };
                t.info.retention_ms = retention_ms;
                Reply::Done
            }
            Command::SetTopicSettings { name, settings } => {
                let Some(t) = self.topics.get_mut(&name) else {
                    return refused(StoreError::UnknownTopic(name));
                };
                t.info.settings = settings;
                Reply::Done
            }
            Command::DeleteOffsets { group, partitions } => {
                openduct_storage::forget_offsets(&mut self.offsets, &group, partitions.as_deref());
                Reply::Done
            }
            Command::Reassign {
                topic,
                partition,
                replicas,
            } => {
                let Some(t) = self.topics.get_mut(&topic) else {
                    return refused(StoreError::UnknownTopic(topic));
                };
                let Some(current) = usize::try_from(partition).ok().and_then(|p| t.replicas.get(p)) else {
                    return refused(StoreError::UnknownPartition { topic, partition });
                };
                let distinct: std::collections::BTreeSet<&u64> = replicas.iter().collect();
                if replicas.is_empty() || distinct.len() != replicas.len() {
                    return refused(StoreError::InvalidReplicas(format!(
                        "{topic}/{partition}: {replicas:?} is not a list of distinct brokers"
                    )));
                }
                if *current == replicas {
                    t.moving.remove(&partition);
                } else {
                    t.moving.insert(partition, replicas);
                }
                Reply::Done
            }
            Command::Reassigned {
                topic,
                partition,
                replicas,
            } => {
                // Done only for the move it ends: a later one, or none, leaves the record as it is.
                if let Some(t) = self.topics.get_mut(&topic)
                    && t.moving.get(&partition) == Some(&replicas)
                    && let Some(slot) = usize::try_from(partition)
                        .ok()
                        .and_then(|p| t.replicas.get_mut(p))
                {
                    *slot = replicas;
                    t.moving.remove(&partition);
                }
                Reply::Done
            }
            Command::Register {
                id,
                version,
                supported_level,
            } => {
                self.brokers.insert(
                    id,
                    BrokerRecord {
                        version,
                        supported_level,
                    },
                );
                Reply::Done
            }
            Command::RaiseLevel { level } => {
                // Never down: a level in use stays in use.
                self.level = self.level.max(level);
                Reply::Done
            }
            other @ (Command::Append { .. }
            | Command::Retain { .. }
            | Command::RetainBytes { .. }
            | Command::Compact { .. }
            | Command::Roll
            | Command::Tiered { .. }
            | Command::Offload { .. }
            | Command::DeleteRecords { .. }) => Reply::Refused(Refusal::Misrouted(other.to_string())),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ControllerShared {
    state: RwLock<ControllerState>,
    applied: Mutex<(Option<LogId>, StoredMembership)>,
    pub(crate) snapshots: Snapshots,
    /// Bumped whenever the state changes, so the broker starts and stops partition groups.
    changed: watch::Sender<u64>,
}

impl ControllerShared {
    pub(crate) fn state(&self) -> parking_lot::RwLockReadGuard<'_, ControllerState> {
        self.state.read()
    }

    pub(crate) fn watch(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn bump(&self) {
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
}

/// The state machine handed to the controller's Raft group.
#[derive(Debug, Clone)]
pub struct ControllerMachine {
    shared: Arc<ControllerShared>,
}

impl ControllerMachine {
    /// Opens the machine in `dir`: an empty state to start from, and the newest snapshot, which Raft installs.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let snapshots = Snapshots::open(&dir)?;
        Ok(Self {
            shared: Arc::new(ControllerShared {
                state: RwLock::new(ControllerState::default()),
                applied: Mutex::new((None, StoredMembership::default())),
                snapshots,
                changed: watch::channel(0).0,
            }),
        })
    }

    pub(crate) fn shared(&self) -> Arc<ControllerShared> {
        Arc::clone(&self.shared)
    }
}

/// A snapshot being made: the state as encoded between two applies, still to be written.
#[derive(Debug)]
pub struct ControllerSnapshot {
    shared: Arc<ControllerShared>,
    meta: SnapshotMeta,
    state: Result<Vec<u8>, String>,
}

impl RaftSnapshotBuilder<TypeConfig> for ControllerSnapshot {
    type SnapshotData = Arc<SnapshotFiles>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, Arc<SnapshotFiles>>, io::Error> {
        let state = self.state.clone().map_err(other)?;
        let (shared, meta) = (Arc::clone(&self.shared), self.meta.clone());
        let files = blocking(move || {
            let dir = shared.snapshots.next_dir();
            fs::create_dir_all(&dir)?;
            fs::write(dir.join(STATE), &state)?;
            let manifest = Manifest {
                files: vec![ManifestFile {
                    path: STATE.to_owned(),
                    len: state.len() as u64,
                }],
            };
            shared.snapshots.finish(dir, meta, manifest)
        })
        .await?;
        Ok(SnapshotOf::<TypeConfig, _> {
            meta: self.meta.clone(),
            snapshot: files,
        })
    }
}

impl RaftStateMachine<TypeConfig> for ControllerMachine {
    type SnapshotData = Arc<SnapshotFiles>;
    type SnapshotBuilder = ControllerSnapshot;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok(self.shared.applied.lock().clone())
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut chunks = entries.ready_chunks(256);
        while let Some(chunk) = chunks.next().await {
            let mut changed = false;
            for item in chunk {
                let (entry, responder): (Entry, _) = item?;
                let log_id = entry.log_id;
                let reply = match entry.payload {
                    EntryPayload::Blank => Reply::Done,
                    EntryPayload::Membership(m) => {
                        self.shared.applied.lock().1 = StoredMembership::new(Some(log_id), m);
                        changed = true;
                        Reply::Done
                    }
                    EntryPayload::Normal(command) => {
                        changed = true;
                        self.shared.state.write().run(command, log_id.index)
                    }
                };
                self.shared.applied.lock().0 = Some(log_id);
                if let Some(responder) = responder {
                    responder.send(reply);
                }
            }
            if changed {
                self.shared.bump();
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<ControllerSnapshot> {
        Some(self.get_snapshot_builder().await)
    }

    async fn get_snapshot_builder(&mut self) -> ControllerSnapshot {
        let (last_log_id, last_membership) = self.shared.applied.lock().clone();
        let state = serde_json::to_vec(&Versioned::current(self.shared.state.read().clone()))
            .map_err(|e| format!("cannot encode the state: {e}"));
        ControllerSnapshot {
            shared: Arc::clone(&self.shared),
            meta: SnapshotMeta {
                last_log_id,
                last_membership,
            },
            state,
        }
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
            let raw = fs::read(files.dir.join(STATE))?;
            let versioned: Versioned<ControllerState> =
                serde_json::from_slice(&raw).map_err(|e| other(format!("a controller snapshot: {e}")))?;
            let state = versioned.open("a controller snapshot").map_err(other)?;
            *shared.state.write() = state;
            *shared.applied.lock() = (meta.last_log_id, meta.last_membership.clone());
            shared.bump();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(partitions: i32) -> Command {
        Command::CreateTopic {
            id: uuid::Uuid::nil(),
            name: "t".to_owned(),
            config: openduct_storage::TopicConfig {
                partitions,
                retention_ms: None,
            },
            replicas: (0..partitions).map(|_| vec![1, 2]).collect(),
        }
    }

    fn grow(total: i32, added: usize) -> Command {
        Command::AddPartitions {
            name: "t".to_owned(),
            total,
            replicas: vec![vec![2, 1]; added],
        }
    }

    #[test]
    fn partitions_are_added_once_and_each_knows_the_entry_that_made_it() {
        let mut state = ControllerState::default();
        assert!(matches!(state.run(topic(2), 5), Reply::Done));
        assert!(matches!(state.run(grow(3, 1), 9), Reply::Done));
        // Two admins grew it to 4 at once: the second proposal no longer adds up, and is refused.
        assert!(matches!(state.run(grow(4, 1), 12), Reply::Done));
        assert!(matches!(state.run(grow(4, 1), 13), Reply::Refused(_)));
        let t = &state.topics["t"];
        assert_eq!((t.info.partitions, t.replicas.len()), (4, 4));
        assert_eq!(
            (0..5).map(|p| t.created_at_of(p)).collect::<Vec<_>>(),
            vec![5, 5, 9, 12, 5],
            "the topic's, each added one's, and the topic's past the end"
        );
        // Nothing to add, a partition with no broker, the offsets topic, an unknown topic: refused.
        assert!(matches!(state.run(grow(4, 0), 14), Reply::Refused(_)));
        let empty = Command::AddPartitions {
            name: "t".to_owned(),
            total: 5,
            replicas: vec![vec![]],
        };
        assert!(matches!(state.run(empty, 15), Reply::Refused(_)));
        let unknown = Command::AddPartitions {
            name: "nope".to_owned(),
            total: 2,
            replicas: vec![vec![1]],
        };
        assert!(matches!(state.run(unknown, 16), Reply::Refused(_)));
        assert_eq!(state.topics["t"].info.partitions, 4, "nothing refused changed it");
    }
}
