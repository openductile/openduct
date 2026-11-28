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
use empty_disk::EmptyDisk;

mod empty_disk;
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
    /// How long a partition of which every copy here is being rebuilt is given to find a leader - a copy that
    /// kept its log - before it is started empty (`ReplicaConfig::start_empty_where_none_survived`). Many
    /// elections long: a copy that kept its log and can be reached leads well inside it.
    pub empty_start_wait: Duration,
    /// How long a broker leads a partition before it may hand it back to the partition's first broker. Longer than
    /// any broker takes to stop (a pod's grace period is sixty seconds): a broker that has just given its
    /// partitions away is stopping, and handing them straight back made it lead while it was being killed (UAT
    /// drill, 2026-11-24). A broker that restarts gets its partitions back this long after it went.
    pub hand_back_after: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_millis(200),
            election_min: Duration::from_millis(1_500),
            election_max: Duration::from_millis(3_000),
            empty_start_wait: Duration::from_secs(30),
            hand_back_after: Duration::from_secs(120),
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
    /// A person's word that, where most copies of a partition were lost, the copies which kept their log are to be
    /// taken as the truth (members.rs `accept_surviving_copies`): the name of this recovery, any text a person
    /// chooses. It holds for ONE recovery: once every partition has a leader again the name is written into the
    /// data directory as spent, and from then on it does nothing - at a later start, after a later loss - until a
    /// person gives a new name. So a setting left in place is harmless, where a switch left on would let a later
    /// loss be settled without anyone deciding it. `None`: such a partition waits.
    pub accept_surviving_copies: Option<String>,
    /// A person's STRONGER word, for a partition of which NO copy kept its log (2026-11-24: Production's event log
    /// had 18 such partitions - every copy set aside - and they waited for ever, as the word above is built to
    /// leave them). Named, and spent, as that one is, and it includes it. Where a copy that kept its log is in
    /// reach it leads within moments and the others are rebuilt from it, exactly as under the word above. Only a
    /// partition that still has no leader after `Timing::empty_start_wait` is started again EMPTY by the copies
    /// being rebuilt: what it held is gone (the set-aside directories keep what was on each disk). THE RISK a
    /// person takes with this word: a copy that kept its log but could not be reached during that wait is passed
    /// over, and what only it held is lost with the rest. `None`: such a partition waits.
    pub start_empty_where_none_survived: Option<String>,
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
            accept_surviving_copies: None,
            start_empty_where_none_survived: None,
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

#[derive(Debug)]
struct Inner {
    id: u64,
    /// Set when this broker started with an empty disk while its cluster was up: the controller's index then. It is
    /// taken in by the controller's leader (`join`); whether its copies catch up before they vote is `empty_disk`'s.
    rejoined_at: Option<u64>,
    /// New to the cluster, or back after losing its disk (`empty_disk.rs`).
    empty_disk: parking_lot::Mutex<EmptyDisk>,
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
    /// The recovery a person named at this start, while it is still in force (`accept_surviving_copies`).
    recovery: parking_lot::Mutex<Option<String>>,
    /// Whether the stronger word is in force: a partition with no copy that kept its log is started empty.
    empty_start: std::sync::atomic::AtomicBool,
    /// Partitions whose first broker has been caught up for that many checks in a row.
    steady: parking_lot::Mutex<BTreeMap<GroupId, u32>>,
    /// Since when this broker has led each partition it leads, as the hand-back first saw it.
    leading_since: parking_lot::Mutex<BTreeMap<GroupId, tokio::time::Instant>>,
}

/// This broker's part of the cluster.
pub struct Replica {
    inner: Arc<Inner>,
    /// Its own tasks: the server for the other brokers first. Taken by `shutdown`, which waits for each to end.
    tasks: parking_lot::Mutex<Vec<JoinHandle<()>>>,
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
        let recovery = match config.accept_surviving_copies.as_deref().map(str::trim) {
            Some(name) if !name.is_empty() => {
                if spent_recoveries(&config.dir).iter().any(|spent| spent == name) {
                    tracing::info!(
                        recovery = name,
                        "--accept-surviving-copies names a recovery that is done: it has no effect. A new loss \
                         needs a new name, given by a person"
                    );
                    None
                } else {
                    tracing::warn!(
                        recovery = name,
                        "started with --accept-surviving-copies: where most copies of a partition were lost, the \
                         copies that kept their log are taken as they are - until every partition has a leader"
                    );
                    members.accept_surviving_copies(true);
                    Some(name.to_owned())
                }
            }
            _ => None,
        };
        // The stronger word (config: start_empty_where_none_survived). It includes the one above - the copies being
        // rebuilt vote - and is spent the same way, under a name of its own kind.
        let empty_word = config
            .start_empty_where_none_survived
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(|name| format!("{EMPTY_WORD}{name}"));
        let (recovery, empty_start) = match empty_word {
            Some(key) if spent_recoveries(&config.dir).contains(&key) => {
                tracing::info!(
                    recovery = %key,
                    "--start-empty-where-no-copy-survived names a recovery that is done: it has no effect. A new \
                     loss needs a new name, given by a person"
                );
                (recovery, false)
            }
            Some(key) => {
                tracing::warn!(
                    recovery = %key,
                    wait_secs = config.timing.empty_start_wait.as_secs(),
                    "started with --start-empty-where-no-copy-survived: a partition of which no copy kept its log \
                     is started again EMPTY once it has had no leader for the wait - what it held is gone, and so \
                     is what a copy that could not be reached in that time alone held"
                );
                members.accept_surviving_copies(true);
                (Some(key), true)
            }
            None => (recovery, false),
        };
        let links = Links::new(config.id, config.auth.clone(), Arc::clone(&members));
        let dir = config.dir.join("controller");
        let open_log = || LogStore::open(dir.join("raft"), config.log_segment_bytes, Arc::clone(&flusher));
        let mut log = open_log()?;
        let mut machine = ControllerMachine::open(dir.join("state"))?;
        // THE CLUSTER'S OWN STATE IS CHECKED AS A PARTITION'S COPY IS (start_partition): purged with no snapshot on
        // this disk to cover it, this broker's copy of it is lost. It is set aside and rebuilt from the others. The
        // partitions' copies on this disk are untouched by that - and are not to be touched until this broker knows
        // again what it holds (reconcile removes nothing while the cluster's state here is being rebuilt).
        let mark = dir.join(RECOVERING_MARK);
        let mut controller_lost = mark.exists();
        if let Some(purged) = log.get_log_state().await?.last_purged_log_id
            && !machine.covers(&purged)
        {
            drop(machine);
            drop(log);
            let kept = set_aside(&config.dir, &GroupId::Controller, &dir)?;
            write_mark(&dir, &mark)?;
            controller_lost = true;
            tracing::error!(
                purged = %purged,
                kept = %kept.display(),
                "this broker's copy of the cluster's state cannot be rebuilt: its log is purged and no snapshot on \
                 this disk covers it. The copy is set aside and rebuilt from the other brokers"
            );
            log = open_log()?;
            machine = ControllerMachine::open(dir.join("state"))?;
        }
        if controller_lost {
            members.mark_recovering_at(GroupId::Controller, mark.clone());
        }
        let control = machine.shared();
        log.purge_no_further_than(control.snapshots.floor());
        let controller = ControllerRaft::new(
            config.id,
            Arc::clone(&raft_config),
            links.network(GroupId::Controller),
            log,
            machine,
        )
        .await
        .map_err(|e| io::Error::other(format!("the controller did not start: {e}")))?;
        if controller_lost {
            // It follows and is caught up; it neither stands for election nor starts a cluster of its own.
            controller.runtime_config().elect(false);
        }
        if !members.insert(
            GroupId::Controller,
            Member::Controller(controller.clone(), Arc::clone(&control)),
        ) {
            let _ = controller.shutdown().await;
            return Err(io::Error::other("the broker stopped while it started"));
        }
        if controller_lost {
            tokio::spawn(settle_controller_when_level(Arc::clone(&members)));
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
            empty_disk::existing_cluster(&links, &config).await
        } else {
            None
        };
        if let Some(index) = rejoined_at {
            tracing::warn!(
                controller_index = index,
                "this broker's disk is empty but its cluster exists: rejoining, its copies rebuilt from the others"
            );
            members.mark_recovering(GroupId::Controller);
        } else if config.brokers.keys().next() == Some(&config.id) && !initialized && !controller_lost {
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
            empty_disk: parking_lot::Mutex::new(
                rejoined_at.map_or(EmptyDisk::No, |at| EmptyDisk::Undecided { at }),
            ),
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
            recovery: parking_lot::Mutex::new(recovery),
            empty_start: std::sync::atomic::AtomicBool::new(empty_start),
            steady: parking_lot::Mutex::new(BTreeMap::new()),
            leading_since: parking_lot::Mutex::new(BTreeMap::new()),
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
        if let Some(at) = inner.rejoined_at {
            // New to the cluster, or back with an empty disk: taken in by the controller's leader, and told which.
            tasks.push(tokio::spawn(Arc::clone(&inner).join()));
            tasks.push(tokio::spawn(empty_disk::settle(Arc::clone(&inner), at)));
        }
        Ok(Self {
            inner,
            tasks: parking_lot::Mutex::new(tasks),
        })
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// How it started as to its disk: "kept", "undecided", "new" (copies vote at once) or "lost" (empty_disk.rs).
    pub fn empty_disk(&self) -> &'static str {
        match *self.inner.empty_disk.lock() {
            EmptyDisk::No => "kept",
            EmptyDisk::Undecided { .. } => "undecided",
            EmptyDisk::New { .. } => "new",
            EmptyDisk::Lost { .. } => "lost",
        }
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

    /// How this broker stands: joined, and every partition it holds started, not being rebuilt, led, and level with
    /// its leader (members.rs [`crate::Readiness`]).
    pub fn readiness(&self) -> crate::Readiness {
        // A leader that has heard from most of its group within two elections' time is leading.
        let mut r = self
            .inner
            .members
            .readiness(self.inner.id, self.inner.config.timing.election_max * 2);
        // Against what it SHOULD hold, not only what it has started: a broker that has just come up has started
        // nothing yet, and nothing wrong with nothing is not ready.
        r.pending = self
            .inner
            .wanted()
            .keys()
            .filter(|group| self.inner.members.get(group).is_none())
            .count()
            .saturating_sub(r.not_started.len());
        r
    }

    /// The partitions this broker should hold a copy of and could not start at its last pass, each with why -
    /// empty when all of them are up. What "ready" and `cluster status` are told from: a broker that answers and
    /// holds partitions it cannot start is not a healthy one.
    pub fn partitions_not_started(&self) -> Vec<(String, String)> {
        self.inner
            .members
            .not_started()
            .into_iter()
            .map(|(group, why)| (group.to_string(), why))
            .collect()
    }

    /// BEFORE A BROKER STOPS, IT GIVES AWAY WHAT IT LEADS (2026-11-24). Stopped as it was, every partition it led
    /// had no leader until the others noticed and elected one - seconds in which writes to them were refused; the
    /// restart drill in a test lane counted the refusals. Asked first, the follower furthest along takes each
    /// group over at once, and this broker goes on answering (and passing writes on) until it has. Returns how
    /// many groups this broker still leads when `within` is over: they are stopped as before, and elect.
    pub async fn hand_over(&self, within: Duration) -> usize {
        let me = self.inner.id;
        // FIRST: from now on this broker takes no leadership back, offered or by election (members.rs `leave`).
        self.inner.members.leave();
        let until = tokio::time::Instant::now() + within;
        let mut handed = 0usize;
        // The partitions first, the cluster's own group last: a broker taking a partition over writes that down
        // with the cluster's leader, which must not be on its way out at that moment.
        for cluster_group in [false, true] {
            let mut asked = BTreeMap::<GroupId, tokio::time::Instant>::new();
            // A hand-over the other broker did not take up ends with this one leading again after an election's
            // time: it is asked again then, not left.
            let again = self.inner.config.timing.election_max + Duration::from_millis(200);
            loop {
                let mut waiting = 0usize;
                for (group, member) in self.inner.members.all() {
                    if matches!(group, GroupId::Controller) != cluster_group {
                        continue;
                    }
                    let leader = member.leader().0;
                    if leader == Some(me) {
                        // Not asked again while the first asking is under way: that would only start it over.
                        if asked.get(&group).is_none_or(|at| at.elapsed() >= again)
                            && member.hand_over(me).await
                        {
                            asked.insert(group, tokio::time::Instant::now());
                        }
                        waiting += usize::from(asked.contains_key(&group));
                    } else if asked.contains_key(&group) && leader.is_none() {
                        // Given away, and nobody has yet been HEARD to lead it: not over until one is.
                        waiting += 1;
                    }
                }
                if waiting == 0 || tokio::time::Instant::now() >= until {
                    handed += asked.len();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        let left = self
            .inner
            .members
            .all()
            .into_iter()
            .filter(|(_, member)| member.leader().0 == Some(me))
            .count();
        if handed > 0 {
            tracing::info!(
                handed_over = handed,
                still_led = left,
                "leadership given to the other brokers before stopping"
            );
        }
        left
    }

    /// Stops taking part: no more answers to the other brokers, and every group stopped. When it returns, its
    /// tasks have ended - the listener among them - so its address is free for a broker started again on it.
    pub async fn shutdown(&self) {
        // First, so a group started meanwhile is stopped by whoever started it rather than missed here.
        self.inner.members.close();
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for t in &tasks {
            t.abort();
        }
        for t in tasks {
            // An aborted task answers "cancelled" once it has stopped and dropped what it held.
            let _ = t.await;
        }
        for (_, member) in self.inner.members.all() {
            member.shutdown().await;
        }
    }
}

/// Watches a copy being rebuilt and lets go of it as soon as it is level with its leader; ends then, or when the
/// group stops.
async fn settle_when_level(members: Arc<Members>, group: GroupId, raft: PartitionRaft) {
    let mut metrics = raft.metrics();
    loop {
        match members.get(&group) {
            Some(member) => {
                if members.may_vote(&group, &member) && !members.is_recovering(&group) {
                    return;
                }
            }
            // Not taken in yet (this runs from its start), or stopped.
            None if members.is_closed() => return,
            None => {}
        }
        tokio::select! {
            changed = metrics.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
}

/// As [`settle_when_level`], for the cluster's own group.
async fn settle_controller_when_level(members: Arc<Members>) {
    let group = GroupId::Controller;
    loop {
        match members.get(&group) {
            Some(member) => {
                if members.may_vote(&group, &member) && !members.is_recovering(&group) {
                    return;
                }
            }
            None if members.is_closed() => return,
            None => {}
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The recoveries a person named that are done, a name a line: `<data>/recoveries-done`.
const RECOVERIES_DONE: &str = "recoveries-done";
/// How the stronger word's names are written there, so the same text given for the two words is two recoveries.
const EMPTY_WORD: &str = "start-empty:";

/// NO COPY KEPT ITS LOG. Waits `wait` for a leader - a copy that kept its log leads within moments, and this copy
/// is then rebuilt from it and this ends. With none in all that time, and still being rebuilt, the copy takes
/// part in starting the partition again from nothing: it may stand for election, and the group is started here if
/// it was never started on this (emptied) disk. Every copy does the same; Raft elects one of them.
async fn start_empty_after_wait(inner: Arc<Inner>, group: GroupId, raft: PartitionRaft, replicas: Vec<u64>) {
    let members = Arc::clone(&inner.members);
    let wait = inner.config.timing.empty_start_wait;
    let until = tokio::time::Instant::now() + wait;
    loop {
        if members.is_closed() || !members.is_recovering(&group) {
            return;
        }
        // A leader seen at any moment ends it: this copy follows that one, and is rebuilt from it.
        if raft.metrics().borrow_watched().current_leader.is_some() {
            return;
        }
        if tokio::time::Instant::now() >= until {
            // Started only with every replica's address known, so the members are the partition's own.
            let nodes: BTreeMap<u64, Broker> = replicas
                .iter()
                .filter_map(|id| inner.broker(*id).map(|b| (*id, b)))
                .collect();
            if nodes.len() == replicas.len() {
                tracing::error!(
                    %group,
                    waited_secs = wait.as_secs(),
                    "no copy of this partition that kept its log has led it in all that time: by a person's \
                     word (--start-empty-where-no-copy-survived) it is started again EMPTY. What it held is \
                     gone; what was on this disk is in the set-aside directory"
                );
                raft.runtime_config().elect(true);
                if !raft.is_initialized().await.unwrap_or(true)
                    && let Err(e) = raft.initialize(nodes).await
                {
                    tracing::debug!(%group, error = %e, "the group was already started");
                }
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn spent_recoveries(data: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(data.join(RECOVERIES_DONE))
        .map(|text| {
            text.lines()
                .map(|l| l.trim().to_owned())
                .filter(|l| !l.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

impl Inner {
    /// A recovery a person named is over once this broker stands well again: every partition it holds started,
    /// led and level, none being rebuilt. Its name is written down as spent - on the device, before the word is
    /// withdrawn - and the copies here vote by the ordinary rule again.
    fn settle_recovery(&self) {
        let Some(name) = self.recovery.lock().clone() else {
            return;
        };
        if !self
            .members
            .readiness(self.id, self.config.timing.election_max * 2)
            .is_ready()
            || self.wanted().keys().any(|g| self.members.get(g).is_none())
        {
            return;
        }
        let mut spent = spent_recoveries(&self.config.dir);
        spent.push(name.clone());
        let path = self.config.dir.join(RECOVERIES_DONE);
        let written = (|| {
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, spent.join("\n") + "\n")?;
            std::fs::File::open(&tmp)?.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            crate::snapshot::sync_dir(&self.config.dir)
        })();
        match written {
            Ok(()) => {
                self.members.accept_surviving_copies(false);
                self.empty_start
                    .store(false, std::sync::atomic::Ordering::Release);
                *self.recovery.lock() = None;
                tracing::warn!(
                    recovery = %name,
                    "the recovery is done on this broker: every partition it holds has a leader and is level. \
                     --accept-surviving-copies has no effect any more and may be removed"
                );
            }
            // Still in force, and tried again at the next pass: a recovery is never ended by a write that failed.
            Err(e) => {
                tracing::warn!(recovery = %name, error = %e, "the recovery is done but cannot be written down as done yet")
            }
        }
    }
}

/// The file beside a partition's copy that says it lost what it had and is being rebuilt from the leader.
const RECOVERING_MARK: &str = "recovering";

/// Writes the mark, on the device before anything relies on it.
fn write_mark(dir: &std::path::Path, mark: &std::path::Path) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::File::create(mark)?.sync_all()?;
    crate::snapshot::sync_dir(dir)
}

/// Moves a copy that cannot be rebuilt out of the way, whole: `<data>/set-aside/<group>-<when>`. Kept for a person
/// to look at; nothing in the broker reads or removes it.
fn set_aside(data: &std::path::Path, group: &GroupId, dir: &std::path::Path) -> io::Result<PathBuf> {
    let aside = data.join("set-aside");
    std::fs::create_dir_all(&aside)?;
    let when = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let name = group.to_string().replace(['/', ' '], "-");
    let kept = aside.join(format!("{name}-{when}"));
    std::fs::rename(dir, &kept)?;
    crate::snapshot::sync_dir(&aside)?;
    if let Some(parent) = dir.parent() {
        crate::snapshot::sync_dir(parent)?;
    }
    Ok(kept)
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
        inner.members.settle_recovered();
        inner.settle_recovery();
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
        // NO COPY BEFORE THE CLUSTER HAS THIS BROKER ON RECORD: what tells a lost disk from a new one (empty_disk.rs).
        if !self.control.state().brokers.contains_key(&self.id) {
            return Ok(());
        }
        let wanted = self.wanted();
        let mut not_started: Vec<(GroupId, String)> = Vec::new();
        for (group, (replicas, created_at, may_start)) in &wanted {
            if self.members.get(group).is_none() {
                if self.empty_disk.lock().must_catch_up(*created_at, *may_start) {
                    self.members.mark_recovering(*group);
                }
                // In its own task, so a stop does not cut it off half way: it finishes, and the group it started is
                // then stopped (Members::insert refuses it once the broker is stopping).
                let (me, group, replicas, may_start) =
                    (Arc::clone(self), *group, replicas.clone(), *may_start);
                let started =
                    tokio::spawn(async move { me.start_partition(group, &replicas, may_start).await })
                        .await
                        .map_err(|e| io::Error::other(e.to_string()))
                        .and_then(|r| r);
                // EACH PARTITION FAILS ALONE. One that cannot start is said, counted and tried again at the next
                // pass; the ones after it are still started. Returning here stopped every later partition of the
                // broker for as long as one of them could not start.
                if let Err(e) = started {
                    tracing::warn!(%group, error = %e, "a partition's group did not start on this broker; the others go on");
                    not_started.push((group, e.to_string()));
                }
            }
        }
        self.members.set_not_started(not_started);
        // NOTHING IS REMOVED ON AN OLD OR EMPTY LIST. What follows stops groups and removes the directories of
        // partitions this broker no longer holds and of topics that were deleted - by the cluster's state as this
        // broker has it. That is only to be believed while this broker's copy of it is level with a leader it
        // hears from: on a broker whose copy is being rebuilt, or cut off from the others, the list is short or
        // empty, and acting on it would remove partition copies that are whole.
        let authoritative = {
            let r = self
                .members
                .readiness(self.id, self.config.timing.election_max * 2);
            r.controller_level && !self.members.is_recovering(&GroupId::Controller)
        };
        if !authoritative {
            return Ok(());
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
        // A COPY BEING REBUILT STAYS ONE ACROSS A RESTART. That it lost what it had is written beside it and read
        // back here: held in memory alone, a broker restarted half way through catching up came back with a short
        // log and nothing to say it must not vote yet.
        let mark = dir.join(RECOVERING_MARK);
        if mark.exists() {
            self.members.mark_recovering_at(group, mark.clone());
        } else if self.members.is_recovering(&group) {
            write_mark(&dir, &mark)?;
            self.members.mark_recovering_at(group, mark.clone());
        }
        let open_log = || {
            LogStore::open(
                dir.join("raft"),
                self.config.log_segment_bytes,
                Arc::clone(&self.flusher),
            )
        };
        let remote = self.config.tier.as_ref().map(|t| {
            Arc::new(crate::tier::Fetch {
                s3: Arc::clone(&t.s3),
                runtime: self.runtime.clone(),
            }) as Arc<dyn openduct_storage::Remote>
        });
        let open_machine = || PartitionMachine::open(dir.join("store"), self.config.store, remote.clone());
        let mut log = open_log()?;
        let mut machine = open_machine()?;
        // THE COPY IS CHECKED BEFORE IT SERVES. Its Raft log holds nothing up to `purged`; only a snapshot on this
        // disk reaching that far can rebuild it. With none, this copy is lost - whatever the reason - and trying to
        // start it for ever helps nobody: it is set aside (kept, never removed here), and the broker takes the
        // partition up again as a broker with an empty disk does - caught up from the leader, voting for no one
        // until it is. The other copies are what the data lives on; with a majority of them gone the partition
        // waits, and nothing here pretends otherwise.
        if let Some(purged) = log.get_log_state().await?.last_purged_log_id
            && !machine.covers(&purged)
        {
            drop(machine);
            drop(log);
            let kept = set_aside(&self.config.dir, &group, &dir)?;
            std::fs::create_dir_all(&dir)?;
            write_mark(&dir, &mark)?;
            self.members.mark_recovering_at(group, mark.clone());
            tracing::error!(
                %group,
                purged = %purged,
                kept = %kept.display(),
                "this broker's copy cannot be rebuilt: its log is purged and no snapshot on this disk covers it. \
                 The copy is set aside and rebuilt from the partition's leader"
            );
            log = open_log()?;
            machine = open_machine()?;
        }
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
        log.purge_no_further_than(machine.purge_floor());
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
        // A copy being rebuilt never stands for election: it has nothing to lead with. It follows, and is caught
        // up; standing again is given back with its vote, once it is level with its leader (members.rs).
        if self.members.is_recovering(&group) {
            raft.runtime_config().elect(false);
            // Let go of THE MOMENT it is level with its leader, not at the next look a few seconds on: a mark still
            // on a copy that has caught up holds it back at the next start, and with most copies so marked no
            // leader could be elected for them to be level with.
            tokio::spawn(settle_when_level(Arc::clone(&self.members), group, raft.clone()));
            if self.empty_start.load(std::sync::atomic::Ordering::Acquire) {
                tokio::spawn(start_empty_after_wait(
                    Arc::clone(self),
                    group,
                    raft.clone(),
                    replicas.to_vec(),
                ));
            }
        }
        if !self
            .members
            .insert(group, Member::Partition(raft.clone(), shared))
        {
            // The broker is stopping: this group stops with it rather than lead on alone.
            let _ = raft.shutdown().await;
            return Ok(());
        }
        tokio::spawn(quiet::quiet_when_alone(raft.clone(), self.id));
        // A COPY BEING REBUILT NEVER STARTS THE GROUP (found by the drill of 2026-11-24): starting it makes this copy
        // its first leader, with nothing in it - elect(false) above does not hold that back - and with the copies
        // being rebuilt voting (a person's first word) a partition of which no copy kept its log came back empty
        // at once, unasked. Only the stronger word does that, after its wait (start_empty_after_wait).
        if !may_start
            || seeded
            || self.members.is_recovering(&group)
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
