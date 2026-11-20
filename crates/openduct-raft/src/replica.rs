// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A broker of a cluster: the [`Store`] it serves, backed by Raft groups.
//!
//! - The **controller** group runs on every broker and keeps the cluster's state: topics and, per partition, the
//!   brokers holding it; producer ids; consumer groups' committed offsets. Its leader coordinates the consumer
//!   groups. A write for it may reach any broker: one that does not lead forwards it, then waits until its own copy
//!   has it, so what it answers next agrees.
//! - Each **partition** is a group of its own over the brokers holding it (three by default), led by one of them:
//!   leaders spread over the brokers, and so does the work. Its appends are answered once most of its replicas hold
//!   them; a broker that does not lead it refuses its appends and reads (`NOT_LEADER_OR_FOLLOWER`), and the client
//!   finds the leader in the metadata.
//!
//! Every broker starts the groups of the partitions the controller gives it and stops those of deleted topics. A
//! partition's first replica starts its group, and its other replicas hold the group's opening entry from the start,
//! so any two of them elect a leader should the first be lost; if the first is not there, they elect one after a
//! while. The broker with the lowest id starts the controller of a new cluster.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_storage::{BrokerAddress, FileStoreConfig, PartitionLog, StoreError};
use openraft::async_runtime::WatchReceiver;
use openraft::storage::RaftLogStorage;
use openraft::{Config, RaftLogReader, SnapshotPolicy};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::controller::{ControllerMachine, ControllerShared, TopicRecord};
use crate::flush::Flusher;
use crate::log::LogStore;
use crate::members::{Forwarded, Member, Members, lead_write};
use crate::net::{self, ClusterAuth, Links};
use crate::partition::{PartitionMachine, PartitionShared};
use crate::types::{Broker, Command, ControllerRaft, GroupId, PartitionRaft, Reply};

mod moves;
mod quiet;
mod store;
mod tiering;

/// How long a write waits for a leader (an election, a leader being reached) before the client is told to retry.
const WRITE_WAIT: Duration = Duration::from_secs(5);
/// How long creating a topic waits for its partitions to have leaders.
const TOPIC_WAIT: Duration = Duration::from_secs(10);

/// Raft's clock: how often a leader shows it is alive, and how long the others wait before electing another.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub heartbeat: Duration,
    pub election_min: Duration,
    pub election_max: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_millis(200),
            election_min: Duration::from_millis(1_500),
            election_max: Duration::from_millis(3_000),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// This broker's id: unique in the cluster, never given to another broker, 1 to `i32::MAX` (Kafka's ids).
    pub id: u64,
    /// The broker's data directory: the controller's log and state, and each partition's log and copy.
    pub dir: PathBuf,
    /// Where this broker listens for the others.
    pub listen: SocketAddr,
    /// Every broker of the cluster, this one included, with how the others and clients reach it.
    pub brokers: BTreeMap<u64, Broker>,
    pub auth: ClusterAuth,
    pub store: FileStoreConfig,
    pub timing: Timing,
    /// How many brokers hold each partition of a new topic (at most every broker).
    pub replication_factor: usize,
    /// A group takes a snapshot after this many entries; its log before it is then removed.
    pub snapshot_every: u64,
    /// When a group's log starts a new segment file.
    pub log_segment_bytes: u64,
    /// Closed segments copied to object storage, local copies trimmed; `None` keeps everything on the local disk.
    pub tier: Option<crate::tier::TierConfig>,
    /// The feature level this broker's build supports (see `features`): `features::SUPPORTED_LEVEL`, lower only
    /// to test an older build.
    pub supported_level: u32,
    /// Partitions of the offsets topic (see `offsets`): how far consumer groups' commits spread.
    pub offsets_partitions: i32,
}

impl ReplicaConfig {
    pub fn new(
        id: u64,
        dir: impl Into<PathBuf>,
        listen: SocketAddr,
        brokers: BTreeMap<u64, Broker>,
        auth: ClusterAuth,
    ) -> Self {
        Self {
            id,
            dir: dir.into(),
            listen,
            brokers,
            auth,
            store: FileStoreConfig::default(),
            timing: Timing::default(),
            replication_factor: 3,
            snapshot_every: 20_000,
            log_segment_bytes: 16 * 1024 * 1024,
            tier: None,
            supported_level: crate::features::SUPPORTED_LEVEL,
            offsets_partitions: 16,
        }
    }
}

fn kafka_address(id: u64, broker: &Broker) -> Option<BrokerAddress> {
    let (host, port) = broker.kafka.rsplit_once(':')?;
    Some(BrokerAddress {
        id: i32::try_from(id).ok()?,
        host: host.trim_start_matches('[').trim_end_matches(']').to_owned(),
        port: port.parse().ok()?,
    })
}

fn group_of(record: &TopicRecord, partition: i32) -> Result<GroupId, StoreError> {
    if partition < 0 || partition >= record.info.partitions {
        return Err(StoreError::UnknownPartition {
            topic: record.info.name.clone(),
            partition,
        });
    }
    Ok(GroupId::Partition {
        topic: record.info.id,
        partition,
    })
}

/// `broker.json` in the data directory: whose it is, and which copy of that broker - a new one whenever the broker
/// starts with an empty disk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Identity {
    pub(crate) id: u64,
    pub(crate) cluster: String,
    pub(crate) incarnation: uuid::Uuid,
}

const IDENTITY: &str = "broker.json";

/// This broker's identity: from its data directory, or new (and `true`) when the directory is empty.
fn identity(config: &ReplicaConfig) -> io::Result<(Identity, bool)> {
    let path = config.dir.join(IDENTITY);
    match std::fs::read(&path) {
        Ok(raw) => {
            let versioned: crate::stable::Versioned<Identity> = serde_json::from_slice(&raw)
                .map_err(|e| io::Error::other(format!("{}: {e}", path.display())))?;
            let found = versioned
                .open(&path.display().to_string())
                .map_err(io::Error::other)?;
            if found.id != config.id || found.cluster != config.auth.cluster {
                return Err(io::Error::other(format!(
                    "{} holds broker {} of cluster {:?}, not broker {} of {:?}",
                    config.dir.display(),
                    found.id,
                    found.cluster,
                    config.id,
                    config.auth.cluster
                )));
            }
            Ok((found, false))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok((
            Identity {
                id: config.id,
                cluster: config.auth.cluster.clone(),
                incarnation: uuid::Uuid::new_v4(),
            },
            true,
        )),
        Err(e) => Err(e),
    }
}

fn write_identity(config: &ReplicaConfig, identity: &Identity) -> io::Result<()> {
    let raw = serde_json::to_vec_pretty(&crate::stable::Versioned::current(identity.clone()))
        .map_err(|e| io::Error::other(e.to_string()))?;
    let tmp = config.dir.join(format!("{IDENTITY}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        io::Write::write_all(&mut f, &raw)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, config.dir.join(IDENTITY))?;
    crate::snapshot::sync_dir(&config.dir)
}

/// For a broker starting with an empty disk: whether the cluster exists already - the highest index any other
/// broker's controller has committed - asking the others for a little while.
async fn existing_cluster(links: &Links, config: &ReplicaConfig) -> Option<u64> {
    let deadline = Instant::now() + (config.timing.election_max * 2).max(Duration::from_secs(3));
    loop {
        let mut found = None;
        for (id, b) in &config.brokers {
            if *id != config.id
                && let Some(info) = links.probe(&b.raft).await
                && let Some(committed) = info.controller_committed
            {
                found = Some(found.map_or(committed, |f: u64| f.max(committed)));
            }
        }
        if found.is_some() || Instant::now() >= deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[derive(Debug)]
struct Inner {
    id: u64,
    /// Set when this broker came back with an empty disk to a cluster that existed: the controller's index then.
    /// Groups that existed by then recover before they vote.
    rejoined_at: Option<u64>,
    config: ReplicaConfig,
    raft_config: Arc<Config>,
    flusher: Arc<Flusher>,
    links: Arc<Links>,
    members: Arc<Members>,
    controller: ControllerRaft,
    control: Arc<ControllerShared>,
    runtime: Handle,
    /// Partitions this broker is moving now.
    moves: parking_lot::Mutex<BTreeSet<GroupId>>,
    /// Set once the cluster uses a feature level this broker's build does not support: it serves nothing more.
    outdated: std::sync::atomic::AtomicBool,
    /// Partitions whose first broker has been caught up for that many checks in a row.
    steady: parking_lot::Mutex<BTreeMap<GroupId, u32>>,
}

/// This broker's part of the cluster.
pub struct Replica {
    inner: Arc<Inner>,
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

impl Replica {
    /// Opens this broker's groups, starts answering the other brokers and - on the broker with the lowest id, the
    /// first time - starts the cluster's controller.
    pub async fn start(config: ReplicaConfig) -> io::Result<Self> {
        if i32::try_from(config.id).is_err() || config.id == 0 {
            return Err(io::Error::other("a broker id is between 1 and 2147483647"));
        }
        if !config.brokers.contains_key(&config.id) {
            return Err(io::Error::other(format!(
                "broker {} is not among the cluster's brokers",
                config.id
            )));
        }
        for (id, b) in &config.brokers {
            if kafka_address(*id, b).is_none() {
                return Err(io::Error::other(format!(
                    "broker {id}: {:?} is not a host:port",
                    b.kafka
                )));
            }
        }
        let millis = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let raft_config = Arc::new(
            Config {
                cluster_name: config.auth.cluster.clone(),
                heartbeat_interval: millis(config.timing.heartbeat),
                election_timeout_min: millis(config.timing.election_min),
                election_timeout_max: millis(config.timing.election_max),
                snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_every),
                // Kept after a snapshot, so a replica a little behind catches up from the log, not a whole snapshot.
                max_in_snapshot_log_to_keep: config.snapshot_every / 10,
                max_payload_entries: 64,
                enable_pre_vote: Some(true),
                // New entries go to the followers as soon as they are written here, not once they are flushed: the
                // leader's flush and the followers' overlap instead of following one another.
                broadcast_submitted_on_append: Some(true),
                // A follower found holding less than its leader thought (a broker back with an empty disk) is
                // caught up again from the start, rather than its replication stalling. openraft advises against
                // this in general, as a log that goes back is usually a storage fault; here it is the expected
                // case, and safety stays with the follower: a broker that lost its disk refuses votes until it has
                // caught up (members.rs), so it never helps elect a leader missing what it had acknowledged. The
                // one-time allowance per new incarnation (allow_reset) alone was missed on a CI runner: a leader
                // met the emptied log unprepared, and a debug build stopped on openraft's assertion.
                allow_log_reversion: Some(true),
                ..Config::default()
            }
            .validate()
            .map_err(|e| io::Error::other(format!("replication settings: {e}")))?,
        );
        std::fs::create_dir_all(&config.dir)?;
        let (identity, fresh) = identity(&config)?;
        let flusher = Flusher::start()?;
        let members = Arc::new(Members::new(config.supported_level));
        let links = Links::new(config.id, config.auth.clone(), Arc::clone(&members));
        let dir = config.dir.join("controller");
        let log = LogStore::open(dir.join("raft"), config.log_segment_bytes, Arc::clone(&flusher))?;
        let machine = ControllerMachine::open(dir.join("state"))?;
        let control = machine.shared();
        let controller = ControllerRaft::new(
            config.id,
            Arc::clone(&raft_config),
            links.network(GroupId::Controller),
            log,
            machine,
        )
        .await
        .map_err(|e| io::Error::other(format!("the controller did not start: {e}")))?;
        if !members.insert(
            GroupId::Controller,
            Member::Controller(controller.clone(), Arc::clone(&control)),
        ) {
            let _ = controller.shutdown().await;
            return Err(io::Error::other("the broker stopped while it started"));
        }
        let listener = TcpListener::bind(config.listen).await?;
        let server = tokio::spawn(net::serve(
            listener,
            Arc::clone(&members),
            Arc::new(config.auth.clone()),
            identity.incarnation,
        ));
        let initialized = controller
            .is_initialized()
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        // An empty disk: a new cluster, or a broker of an existing one that lost its data.
        let rejoined_at = if fresh {
            existing_cluster(&links, &config).await
        } else {
            None
        };
        if let Some(index) = rejoined_at {
            tracing::warn!(
                controller_index = index,
                "this broker's disk is empty but its cluster exists: rejoining, its copies rebuilt from the others"
            );
            members.mark_recovering(GroupId::Controller);
        } else if config.brokers.keys().next() == Some(&config.id) && !initialized {
            tracing::info!(brokers = config.brokers.len(), "starting a new cluster");
            if let Err(e) = controller.initialize(config.brokers.clone()).await {
                tracing::info!(error = %e, "the cluster was already started");
            }
        }
        if fresh {
            write_identity(&config, &identity)?;
        }
        let inner = Arc::new(Inner {
            id: config.id,
            rejoined_at,
            config,
            raft_config,
            flusher,
            links,
            members,
            controller,
            control,
            runtime: Handle::current(),
            moves: parking_lot::Mutex::new(BTreeSet::new()),
            outdated: std::sync::atomic::AtomicBool::new(false),
            steady: parking_lot::Mutex::new(BTreeMap::new()),
        });
        let mut tasks = vec![
            server,
            tokio::spawn(follow_controller(Arc::clone(&inner))),
            tokio::spawn(learn_leaders(Arc::clone(&inner))),
            tokio::spawn(moves::tend(Arc::clone(&inner))),
        ];
        if let Some(tier) = inner.config.tier.clone() {
            tasks.push(tokio::spawn(tiering::tend(Arc::clone(&inner), tier)));
        }
        if inner.rejoined_at.is_some() {
            // New to the cluster, or back with an empty disk: taken in by the controller's leader.
            tasks.push(tokio::spawn(Arc::clone(&inner).join()));
        }
        Ok(Self { inner, tasks })
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// The cluster's feature level, as this broker's copy of the controller has it.
    pub fn feature_level(&self) -> u32 {
        self.inner.control.state().level
    }

    /// The broker coordinating the cluster (leading its controller), as this broker knows it now.
    pub fn coordinator(&self) -> Option<u64> {
        self.inner.controller.metrics().borrow_watched().current_leader
    }

    /// The controller's Raft group on this broker.
    pub fn controller(&self) -> &ControllerRaft {
        &self.inner.controller
    }

    /// The leader of a partition, as this broker knows it now.
    pub fn partition_leader(&self, topic: &str, partition: i32) -> Option<u64> {
        let group = self.inner.group(topic, partition).ok()?;
        self.inner.members.leader_of(&group).map(|(l, _)| l)
    }

    /// This broker's own copy of a partition, if it holds one.
    pub fn local_partition(&self, topic: &str, partition: i32) -> Option<Arc<PartitionLog>> {
        let group = self.inner.group(topic, partition).ok()?;
        match self.inner.members.get(&group)? {
            Member::Partition(_, shared) => Some(shared.log()),
            Member::Controller(..) => None,
        }
    }

    /// A partition's Raft group on this broker, if it holds a copy.
    pub fn partition_raft(&self, topic: &str, partition: i32) -> Option<PartitionRaft> {
        let group = self.inner.group(topic, partition).ok()?;
        match self.inner.members.get(&group)? {
            Member::Partition(raft, _) => Some(raft),
            Member::Controller(..) => None,
        }
    }

    /// Stops taking part: no more answers to the other brokers, and every group stopped.
    pub async fn shutdown(&self) {
        // First, so a group started meanwhile is stopped by whoever started it rather than missed here.
        self.inner.members.close();
        for t in &self.tasks {
            t.abort();
        }
        for (_, member) in self.inner.members.all() {
            member.shutdown().await;
        }
    }
}

/// Starts and stops partition groups as the controller's state says, whenever it changes (and every few seconds).
async fn follow_controller(inner: Arc<Inner>) {
    let mut changed = inner.control.watch();
    loop {
        let level = inner.control.state().level;
        inner.members.levels().set_active(level);
        let supported = inner.members.levels().supported();
        if level > supported && !inner.outdated.swap(true, std::sync::atomic::Ordering::AcqRel) {
            tracing::error!(
                cluster_level = level,
                supported,
                "the cluster uses features this broker's build does not have: it serves nothing until it is upgraded"
            );
        }
        if let Err(e) = inner.reconcile().await {
            tracing::warn!(error = %e, "cannot start or stop a partition's group yet");
        }
        tokio::select! {
            _ = changed.changed() => {}
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    }
}

/// Asks the other brokers which leaders they know, for the partitions this broker holds no copy of.
async fn learn_leaders(inner: Arc<Inner>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for (id, broker) in inner.brokers() {
            if id == inner.id {
                continue;
            }
            if let Some(reported) = inner.links.leaders(id, &broker.raft).await {
                inner.members.learn(reported);
            }
        }
    }
}

impl Inner {
    /// Every broker of the cluster, as the controller's membership has them.
    fn brokers(&self) -> Vec<(u64, Broker)> {
        let metrics = self.controller.metrics();
        let m = metrics.borrow_watched();
        m.membership_config
            .membership()
            .nodes()
            .map(|(id, b)| (*id, b.clone()))
            .collect()
    }

    fn broker(&self, id: u64) -> Option<Broker> {
        self.controller
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .get_node(&id)
            .cloned()
    }

    fn record(&self, topic: &str) -> Result<TopicRecord, StoreError> {
        self.control
            .state()
            .topics
            .get(topic)
            .cloned()
            .ok_or_else(|| StoreError::UnknownTopic(topic.to_owned()))
    }

    fn group(&self, topic: &str, partition: i32) -> Result<GroupId, StoreError> {
        group_of(&self.record(topic)?, partition)
    }

    /// A consumer group's commit, through the leader of its partition of the offsets topic - this broker, or the
    /// one it forwards to.
    fn commit_through_leader(&self, partition: i32, command: Command) -> Result<(), StoreError> {
        let group = self.group(crate::offsets::OFFSETS_TOPIC, partition)?;
        self.write_through_leader(group, command)?.done()
    }

    /// A write to a partition by the broker itself - a group's commit, a transaction's marker, a record of an
    /// internal topic - through the partition's leader: this broker, or the one it forwards to. Waits out an
    /// election.
    pub(crate) fn write_through_leader(&self, group: GroupId, command: Command) -> Result<Reply, StoreError> {
        let deadline = Instant::now() + WRITE_WAIT;
        loop {
            let leader = self.members.leader_of(&group).map(|(l, _)| l);
            let answer = match (leader, self.members.get(&group)) {
                (Some(l), Some(Member::Partition(raft, _))) if l == self.id => {
                    Some(self.runtime.block_on(lead_write(&raft, command.clone())))
                }
                (Some(l), _) => match self.broker(l) {
                    Some(b) => Some(self.runtime.block_on(self.links.forward(
                        l,
                        &b.raft,
                        group,
                        command.clone(),
                        WRITE_WAIT,
                    ))),
                    None => None,
                },
                (None, _) => None,
            };
            match answer {
                Some(Ok((reply, _))) => return Ok(reply),
                Some(Err(Forwarded::Unavailable(why))) => return Err(StoreError::Unavailable(why)),
                Some(Err(Forwarded::NotLeader { .. })) | None => {}
            }
            if Instant::now() >= deadline {
                return Err(StoreError::Unavailable(format!("{group} has no leader now")));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The partition of the offsets topic holding `group`'s offsets, once the topic exists.
    fn offsets_partition(&self, group: &str) -> Option<i32> {
        let partitions = self
            .control
            .state()
            .topics
            .get(crate::offsets::OFFSETS_TOPIC)?
            .info
            .partitions;
        Some(crate::offsets::partition_of(group, partitions))
    }

    fn partition_dir(&self, group: &GroupId) -> Option<PathBuf> {
        match group {
            GroupId::Partition { topic, partition } => Some(
                self.config
                    .dir
                    .join("partitions")
                    .join(topic.to_string())
                    .join(partition.to_string()),
            ),
            GroupId::Controller => None,
        }
    }

    /// The partitions this broker holds a copy of, or is being given, by the controller's state: with their
    /// replicas, the index that created their topic, and whether this broker may start the group (a broker a
    /// partition is moving to never does: the leader takes it in).
    fn wanted(&self) -> BTreeMap<GroupId, (Vec<u64>, u64, bool)> {
        let state = self.control.state();
        let mut out = BTreeMap::new();
        for t in state.topics.values() {
            for (p, replicas) in t.replicas.iter().enumerate() {
                let Ok(partition) = i32::try_from(p) else {
                    continue;
                };
                let group = GroupId::Partition {
                    topic: t.info.id,
                    partition,
                };
                let incoming = t
                    .moving
                    .get(&partition)
                    .is_some_and(|target| target.contains(&self.id));
                if replicas.contains(&self.id) {
                    out.insert(group, (replicas.clone(), t.created_at_of(p), true));
                } else if incoming {
                    out.insert(group, (replicas.clone(), t.created_at_of(p), false));
                }
            }
        }
        out
    }

    async fn reconcile(self: &Arc<Self>) -> io::Result<()> {
        let wanted = self.wanted();
        for (group, (replicas, created_at, may_start)) in &wanted {
            if self.members.get(group).is_none() {
                if self.rejoined_at.is_some_and(|at| *created_at <= at) {
                    self.members.mark_recovering(*group);
                }
                // In its own task, so a stop does not cut it off half way: it finishes, and the group it started is
                // then stopped (Members::insert refuses it once the broker is stopping).
                let (me, group, replicas, may_start) =
                    (Arc::clone(self), *group, replicas.clone(), *may_start);
                tokio::spawn(async move { me.start_partition(group, &replicas, may_start).await })
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))??;
            }
        }
        for (group, member) in self.members.all() {
            if matches!(group, GroupId::Partition { .. }) && !wanted.contains_key(&group) {
                member.shutdown().await;
                self.members.remove(&group);
                if let Some(dir) = self.partition_dir(&group) {
                    let _ = std::fs::remove_dir_all(dir);
                }
            }
        }
        let (ids, live): (BTreeSet<String>, BTreeSet<GroupId>) = {
            let state = self.control.state();
            let ids = state.topics.values().map(|t| t.info.id.to_string()).collect();
            let live = state
                .topics
                .values()
                .flat_map(|t| {
                    (0..t.info.partitions).map(|partition| GroupId::Partition {
                        topic: t.info.id,
                        partition,
                    })
                })
                .collect();
            (ids, live)
        };
        // Topics deleted while this broker was away: their directories go too.
        if let Ok(entries) = std::fs::read_dir(self.config.dir.join("partitions")) {
            for entry in entries.flatten() {
                if !ids.contains(entry.file_name().to_string_lossy().as_ref()) {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
        self.members.forget_remote(|g| live.contains(g));
        Ok(())
    }

    async fn start_partition(
        self: &Arc<Self>,
        group: GroupId,
        replicas: &[u64],
        may_start: bool,
    ) -> io::Result<()> {
        let Some(dir) = self.partition_dir(&group) else {
            return Ok(());
        };
        let mut log = LogStore::open(
            dir.join("raft"),
            self.config.log_segment_bytes,
            Arc::clone(&self.flusher),
        )?;
        let nodes: BTreeMap<u64, Broker> = replicas
            .iter()
            .filter_map(|id| self.broker(*id).map(|b| (*id, b)))
            .collect();
        // A new partition's other replicas are given, before their group starts, the opening entry its first replica
        // writes as it starts the group - the same entry, as though it had reached them already. Started by the first
        // replica alone, a group whose first replica was lost after being elected but before that entry reached the
        // others could elect no one until it came back: they had voted, so Raft counted them started, yet they held
        // no members (seen in the cluster tests). Holding the entry, they elect a leader after the usual wait; and
        // with no election of their own at once, the first replica still leads first. Only with every replica known,
        // so the members are the first replica's own, and not on a broker rebuilt from an empty disk (it catches up
        // from the leader instead).
        let first = replicas.first().copied();
        let state = log.get_log_state().await?;
        let empty = state.last_log_id.is_none()
            && state.last_purged_log_id.is_none()
            && log.read_vote().await?.is_none();
        let seeded = may_start
            && empty
            && nodes.len() == replicas.len()
            && !self.members.is_recovering(&group)
            && first.is_some_and(|f| f != self.id);
        if let (true, Some(first)) = (seeded, first) {
            log.seed(opening_entry(first, nodes.clone())).await?;
        }
        let remote = self.config.tier.as_ref().map(|t| {
            Arc::new(crate::tier::Fetch {
                s3: Arc::clone(&t.s3),
                runtime: self.runtime.clone(),
            }) as Arc<dyn openduct_storage::Remote>
        });
        let machine = PartitionMachine::open(dir.join("store"), self.config.store, remote)?;
        let shared = machine.shared();
        let raft = PartitionRaft::new(
            self.id,
            Arc::clone(&self.raft_config),
            self.links.network(group),
            log,
            machine,
        )
        .await
        .map_err(|e| io::Error::other(format!("{group} did not start: {e}")))?;
        if !self
            .members
            .insert(group, Member::Partition(raft.clone(), shared))
        {
            // The broker is stopping: this group stops with it rather than lead on alone.
            let _ = raft.shutdown().await;
            return Ok(());
        }
        tokio::spawn(quiet::quiet_when_alone(raft.clone(), self.id));
        if !may_start
            || seeded
            || raft
                .is_initialized()
                .await
                .map_err(|e| io::Error::other(e.to_string()))?
        {
            return Ok(());
        }
        if replicas.first() == Some(&self.id) {
            // The preferred leader starts the group, and so leads it first: leaders spread as topics are placed.
            if let Err(e) = raft.initialize(nodes).await {
                tracing::debug!(%group, error = %e, "the group was already started");
            }
        } else {
            // Should the preferred leader be away, another replica starts the group after a while.
            let wait = self.config.timing.election_max * 3;
            let members = Arc::clone(&self.members);
            tokio::spawn(async move {
                tokio::time::sleep(wait).await;
                let started = raft.is_initialized().await.unwrap_or(true);
                if !started && members.leader_of(&group).is_none() && members.get(&group).is_some() {
                    tracing::info!(%group, "its first replica is away: starting the group here");
                    let _ = raft.initialize(nodes).await;
                }
            });
        }
        Ok(())
    }

    /// Refused once the cluster uses features this broker's build does not have.
    fn current(&self) -> Result<(), StoreError> {
        if self.outdated.load(std::sync::atomic::Ordering::Acquire) {
            return Err(StoreError::Unavailable(
                "this broker's build is older than the features its cluster uses: upgrade it".to_owned(),
            ));
        }
        Ok(())
    }

    /// The partition's group here when this broker leads it; refused with where its leader is otherwise.
    fn leading(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(PartitionRaft, Arc<PartitionShared>), StoreError> {
        self.current()?;
        let group = self.group(topic, partition)?;
        let leader = self.members.leader_of(&group).map(|(l, _)| l);
        match self.members.get(&group) {
            Some(Member::Partition(raft, shared)) if leader == Some(self.id) => Ok((raft, shared)),
            _ => Err(StoreError::NotLeader {
                leader: leader.and_then(|l| self.broker(l)).map(|b| b.kafka),
            }),
        }
    }

    /// As [`Self::leading`], with the epoch (term) this broker leads the partition in.
    pub(crate) fn leading_in_epoch(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(i32, Arc<PartitionShared>), StoreError> {
        let group = self.group(topic, partition)?;
        let (_, shared) = self.leading(topic, partition)?;
        let term = self.members.leader_of(&group).map_or(0, |(_, term)| term);
        Ok((crate::partition::epoch_of_term(term), shared))
    }

    fn write(&self, command: Command) -> Result<Reply, StoreError> {
        self.current()?;
        self.runtime.block_on(self.write_async(command))
    }

    /// A controller write through its leader - this broker, or the one it forwards to - waiting out an election.
    async fn write_async(&self, command: Command) -> Result<Reply, StoreError> {
        let deadline = Instant::now() + WRITE_WAIT;
        loop {
            let leader = self.controller.metrics().borrow_watched().current_leader;
            let attempt = match leader {
                Some(l) if l == self.id => Some(lead_write(&self.controller, command.clone()).await),
                Some(l) => match self.broker(l) {
                    Some(b) => Some(
                        self.links
                            .forward(l, &b.raft, GroupId::Controller, command.clone(), WRITE_WAIT)
                            .await,
                    ),
                    None => None,
                },
                None => None,
            };
            match attempt {
                Some(Ok((reply, index))) => {
                    // This broker's own copy catches up before it answers, so its next read agrees.
                    let _ = self
                        .controller
                        .wait(Some(WRITE_WAIT))
                        .applied_index_at_least(Some(index), "a forwarded write")
                        .await;
                    return Ok(reply);
                }
                Some(Err(Forwarded::Unavailable(why))) => return Err(StoreError::Unavailable(why)),
                Some(Err(Forwarded::NotLeader { .. })) | None => {}
            }
            if Instant::now() >= deadline {
                return Err(StoreError::Unavailable("no broker leads the cluster".to_owned()));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Where a new topic's partitions go: `rf` brokers each, the first one leading, each topic starting where the
    /// last one stopped so leaders spread over the brokers.
    fn place(&self, partitions: i32) -> Vec<Vec<u64>> {
        let mut brokers: Vec<u64> = self.brokers().into_iter().map(|(id, _)| id).collect();
        brokers.sort_unstable();
        let n = brokers.len().max(1);
        let rf = self.config.replication_factor.clamp(1, n);
        let start: usize = self
            .control
            .state()
            .topics
            .values()
            .map(|t| usize::try_from(t.info.partitions).unwrap_or(0))
            .sum();
        (0..usize::try_from(partitions).unwrap_or(0))
            .map(|p| {
                (0..rf)
                    .filter_map(|i| brokers.get((start + p + i) % n).copied())
                    .collect()
            })
            .collect()
    }
}

/// A new partition's opening entry, as its first replica's Raft writes it when it starts the group (`initialize`): the
/// group's members, at index 0, under term 0 and that replica's id.
fn opening_entry(first: u64, nodes: BTreeMap<u64, Broker>) -> crate::types::Entry {
    crate::types::Entry {
        log_id: crate::types::LogId::new(
            openraft::vote::leader_id_adv::LeaderId {
                term: 0,
                node_id: first,
            },
            0,
        ),
        payload: openraft::EntryPayload::Membership(nodes.into()),
    }
}
