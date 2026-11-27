// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The Raft groups this broker takes part in - the controller and the partitions it holds a copy of - and what it
//! knows of the leaders of the others' partitions.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use openraft::ChangeMembers;
use openraft::async_runtime::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, SnapshotResponse, StreamAppendError, TransferLeaderRequest, TransferLeaderResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;

use crate::controller::ControllerShared;
use crate::partition::PartitionShared;
use crate::snapshot::SnapshotFiles;
use crate::types::{
    Broker, Command, ControllerRaft, GroupId, LogId, PartitionRaft, Reply, SnapshotMeta, TypeConfig, Vote,
};

/// Why the controller's leader did not take a forwarded write.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum Forwarded {
    NotLeader { leader: Option<u64> },
    Unavailable(String),
}

/// A group's leader as one broker knows it: who, and in which term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LeaderOf {
    pub(crate) group: GroupId,
    pub(crate) leader: u64,
    pub(crate) term: u64,
}

/// One group this broker takes part in.
#[derive(Debug, Clone)]
pub(crate) enum Member {
    Controller(ControllerRaft, Arc<ControllerShared>),
    Partition(PartitionRaft, Arc<PartitionShared>),
}

/// Runs `$body` with `$r` bound to the member's Raft handle, whichever kind of group it is.
macro_rules! with_raft {
    ($member:expr, $r:ident => $body:expr) => {
        match $member {
            Member::Controller($r, _) => $body,
            Member::Partition($r, _) => $body,
        }
    };
}

impl Member {
    pub(crate) async fn append(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<Option<LogId>, StreamAppendError<TypeConfig>>, String> {
        with_raft!(self, r => r.append_entries(rpc).await.map(|a| a.map_err(Into::into)).map_err(|e| e.to_string()))
    }

    pub(crate) async fn vote(
        &self,
        rpc: VoteRequest<TypeConfig>,
        pre: bool,
    ) -> Result<VoteResponse<TypeConfig>, String> {
        if pre {
            with_raft!(self, r => r.pre_vote(rpc).await.map_err(|e| e.to_string()))
        } else {
            with_raft!(self, r => r.vote(rpc).await.map_err(|e| e.to_string()))
        }
    }

    pub(crate) async fn install(
        &self,
        vote: Vote,
        meta: SnapshotMeta,
        files: Arc<SnapshotFiles>,
    ) -> Result<SnapshotResponse<TypeConfig>, String> {
        let snapshot = openraft::alias::SnapshotOf::<TypeConfig, _> {
            meta,
            snapshot: files,
        };
        with_raft!(self, r => r.install_full_snapshot(vote, snapshot).await.map_err(|e| e.to_string()))
    }

    /// The leader hands this broker its leadership.
    pub(crate) async fn take_leadership(
        &self,
        req: TransferLeaderRequest<TypeConfig>,
    ) -> Result<TransferLeaderResponse<TypeConfig>, String> {
        with_raft!(self, r => r.handle_transfer_leader(req).await.map_err(|e| e.to_string()))
    }

    pub(crate) fn incoming_dir(&self) -> std::io::Result<PathBuf> {
        match self {
            Member::Controller(_, s) => s.snapshots.incoming_dir(),
            Member::Partition(_, s) => s.snapshots.incoming_dir(),
        }
    }

    /// The leader this member knows of, and the term.
    pub(crate) fn leader(&self) -> (Option<u64>, u64) {
        with_raft!(self, r => {
            let metrics = r.metrics();
            let m = metrics.borrow_watched();
            (m.current_leader, m.current_term)
        })
    }

    /// Asks the follower that is furthest along to take this group over, when this broker leads it. True when it
    /// was asked; false when this broker does not lead the group, or no other copy has anything yet.
    pub(crate) async fn hand_over(&self, me: u64) -> bool {
        let to = with_raft!(self, r => {
            let metrics = r.metrics();
            let m = metrics.borrow_watched();
            if m.current_leader != Some(me) {
                None
            } else {
                m.replication.as_ref().and_then(|copies| {
                    copies
                        .iter()
                        .filter(|(id, at)| **id != me && at.is_some())
                        .max_by_key(|(_, at)| at.as_ref().map(|l| l.index))
                        .map(|(id, _)| *id)
                })
            }
        });
        let Some(to) = to else { return false };
        with_raft!(self, r => r.trigger().transfer_leader(to).await).is_ok()
    }

    /// Never stands for election from now on: its election timer no longer fires.
    pub(crate) fn stand_down(&self) {
        with_raft!(self, r => r.runtime_config().elect(false));
    }

    pub(crate) async fn shutdown(&self) {
        if let Err(e) = with_raft!(self, r => r.shutdown().await) {
            tracing::warn!(error = %e, "a replication group did not stop cleanly");
        }
    }
}

fn forwarded(e: RaftError<TypeConfig, ClientWriteError<TypeConfig>>) -> Forwarded {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            Forwarded::NotLeader { leader: f.leader_id }
        }
        e => Forwarded::Unavailable(e.to_string()),
    }
}

/// On the controller's leader: takes broker `id` into the cluster - first as a learner until it has caught up,
/// then as a voter. A broker already in is answered at once.
pub(crate) async fn admit(raft: &ControllerRaft, id: u64, broker: Broker) -> Result<(), Forwarded> {
    let (leader, voter) = {
        let metrics = raft.metrics();
        let m = metrics.borrow_watched();
        let voter = m.membership_config.membership().voter_ids().any(|v| v == id);
        (m.current_leader, voter)
    };
    if leader != Some(*raft.node_id()) {
        return Err(Forwarded::NotLeader { leader });
    }
    if voter {
        return Ok(());
    }
    tracing::info!(broker = id, "a broker joins the cluster");
    raft.add_learner(id, broker, true).await.map_err(forwarded)?;
    raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([id])), true)
        .await
        .map_err(forwarded)?;
    Ok(())
}

/// On the controller's leader: takes broker `id` out of the cluster.
pub(crate) async fn dismiss(raft: &ControllerRaft, id: u64) -> Result<(), Forwarded> {
    let leader = raft.metrics().borrow_watched().current_leader;
    if leader != Some(*raft.node_id()) {
        return Err(Forwarded::NotLeader { leader });
    }
    tracing::info!(broker = id, "a broker leaves the cluster");
    raft.change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([id])), false)
        .await
        .map_err(forwarded)?;
    Ok(())
}

/// A write on the controller's leader: its answer, and the index of its entry. Refused when this broker does not
/// lead the controller (any more).
pub(crate) async fn lead_write<SM>(
    raft: &openraft::Raft<TypeConfig, SM>,
    command: Command,
) -> Result<(Reply, u64), Forwarded>
where
    SM: openraft::storage::RaftStateMachine<TypeConfig>,
{
    match raft.client_write(command).await {
        Ok(written) => Ok((written.data, written.log_id.index)),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            Err(Forwarded::NotLeader { leader: f.leader_id })
        }
        Err(e) => Err(Forwarded::Unavailable(e.to_string())),
    }
}

#[derive(Debug)]
pub(crate) struct Members {
    /// The feature levels of this broker and its cluster.
    levels: crate::features::Levels,
    groups: RwLock<HashMap<GroupId, Member>>,
    /// Leaders of groups this broker holds no copy of, as the brokers that do report them.
    remote: RwLock<HashMap<GroupId, (u64, u64)>>,
    /// Groups whose copy here was lost - with this broker's disk, or found beyond rebuilding at a start - and is
    /// not yet caught up: they vote for no one, lest they elect a leader missing writes this broker acknowledged
    /// before. With each, the mark on disk that says so across a restart (a partition's; removed once caught up).
    recovering: RwLock<HashMap<GroupId, Option<std::path::PathBuf>>>,
    /// The partitions this broker should hold and could not start at the last pass, each with why.
    not_started: RwLock<Vec<(GroupId, String)>>,
    /// Set as the broker stops: no group is taken in after it.
    closed: std::sync::atomic::AtomicBool,
    /// Set when the broker begins to stop, before it gives away what it leads: from then on it takes no leadership,
    /// offered or by election (UAT drill, 2026-11-24 - see `leave`).
    leaving: std::sync::atomic::AtomicBool,
    /// A person's word that the surviving copies are to be taken (`--accept-surviving-copies`): a copy being
    /// rebuilt then votes for a copy that is not.
    accept_surviving: std::sync::atomic::AtomicBool,
    /// When each group's copy here last took something from its leader - an entry or a heartbeat - in milliseconds
    /// since `started`. What a follower knows of its leader being there: Raft's own state goes on naming the leader
    /// it last followed for as long as no other is elected.
    heard: RwLock<HashMap<GroupId, Arc<std::sync::atomic::AtomicU64>>>,
    started: std::time::Instant,
}

/// A copy this many entries behind what its leader has committed still counts as level with it: a follower of a
/// busy partition is always a moment behind.
const READY_WITHIN: u64 = 1_000;

/// HOW A BROKER STANDS - what "ready" means. A broker that answers is not thereby one that can be relied on: it is
/// ready when it has joined its cluster and every partition it holds a copy of is started, is not being rebuilt,
/// has a leader, and is level with that leader. Only then may another broker be taken down ("one at a time" means
/// nothing if the one that just came back counts as back the moment it answers).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Readiness {
    /// Whether the cluster's own group has a leader this broker knows.
    pub controller_led: bool,
    /// Whether this broker's copy of the cluster's own state (topics, where their partitions live) is level with
    /// that leader: only then does it know what it should hold.
    pub controller_level: bool,
    /// The partitions this broker holds a copy of.
    pub held: usize,
    /// Partitions the cluster says it should hold whose group is not started here yet (a broker just started).
    pub pending: usize,
    /// Partitions it should hold and could not start, each with why.
    pub not_started: Vec<(GroupId, String)>,
    /// Copies being rebuilt from their leader.
    pub recovering: usize,
    /// Copies whose partition has no leader.
    pub without_leader: usize,
    /// Copies further behind their leader than a busy follower is.
    pub behind: usize,
}

impl Readiness {
    pub fn is_ready(&self) -> bool {
        self.controller_led
            && self.controller_level
            && self.pending == 0
            && self.not_started.is_empty()
            && self.recovering == 0
            && self.without_leader == 0
            && self.behind == 0
    }

    /// Why it is not ready, a line a reason; empty when it is.
    pub fn reasons(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.controller_led {
            out.push("this broker has not joined its cluster: no leader of the cluster is known".to_owned());
        }
        if self.controller_led && !self.controller_level {
            out.push("this broker is still catching up with what the cluster holds".to_owned());
        }
        if self.pending > 0 {
            out.push(format!(
                "{} partitions it should hold are not started here yet",
                self.pending
            ));
        }
        for (group, why) in &self.not_started {
            out.push(format!("{group} did not start: {why}"));
        }
        let of = self.held;
        if self.recovering > 0 {
            out.push(format!(
                "{} of {of} copies are being rebuilt from their leader",
                self.recovering
            ));
        }
        if self.without_leader > 0 {
            out.push(format!(
                "{} of {of} partitions held here have no leader",
                self.without_leader
            ));
        }
        if self.behind > 0 {
            out.push(format!("{} of {of} copies are behind their leader", self.behind));
        }
        out
    }
}

impl Members {
    /// The groups of a broker whose build supports feature level `supported`.
    pub(crate) fn new(supported: u32) -> Self {
        Self {
            levels: crate::features::Levels::new(supported),
            groups: RwLock::default(),
            remote: RwLock::default(),
            recovering: RwLock::default(),
            not_started: RwLock::default(),
            closed: std::sync::atomic::AtomicBool::new(false),
            leaving: std::sync::atomic::AtomicBool::new(false),
            accept_surviving: std::sync::atomic::AtomicBool::new(false),
            heard: RwLock::default(),
            started: std::time::Instant::now(),
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// This broker's copy of `group` took an entry or a heartbeat from its leader just now (net.rs).
    pub(crate) fn heard_from_leader(&self, group: &GroupId) {
        let now = self.now_ms().max(1);
        if let Some(at) = self.heard.read().get(group) {
            at.store(now, std::sync::atomic::Ordering::Release);
            return;
        }
        self.heard
            .write()
            .entry(*group)
            .or_default()
            .store(now, std::sync::atomic::Ordering::Release);
    }

    /// How long ago this broker's copy of `group` last heard from a leader; `None`: never, since this start.
    fn since_heard(&self, group: &GroupId) -> Option<u64> {
        let at = self
            .heard
            .read()
            .get(group)?
            .load(std::sync::atomic::Ordering::Acquire);
        (at > 0).then(|| self.now_ms().saturating_sub(at))
    }

    /// WHEN MOST COPIES OF A PARTITION ARE LOST, the ones that are left cannot elect a leader: the copies being
    /// rebuilt vote for no one, and that is what keeps a leader from being elected without writes they had
    /// acknowledged. It also means the partition waits for ever. A person may say the surviving copies are to be
    /// taken as they are - knowing that a write only the lost copies held is gone. Then a copy being rebuilt votes.
    /// It still never stands for election itself (replica.rs), so only a copy that kept its log can lead, and
    /// among those Raft elects one that is not behind the others taking part.
    pub(crate) fn accept_surviving_copies(&self, accept: bool) {
        self.accept_surviving
            .store(accept, std::sync::atomic::Ordering::Release);
    }

    /// Takes no group in from now on (the broker is stopping); what is held now is what gets stopped.
    /// Whether the broker is stopping.
    /// THE BROKER IS LEAVING (UAT restart drill, 2026-11-24). It gave its partitions away as it began to stop, and
    /// five seconds later another broker's leader hand-back (replica/moves.rs) gave every one of them BACK to it: to
    /// the others it was a caught-up first replica like any other. It led nineteen partitions while stopping, was
    /// killed at its grace period's end, and a write found no leader. From now on a leaving broker refuses
    /// leadership when it is offered and never stands for election, until the process ends.
    pub(crate) fn leave(&self) {
        self.leaving.store(true, std::sync::atomic::Ordering::Release);
        for (_, member) in self.all() {
            member.stand_down();
        }
    }

    pub(crate) fn is_leaving(&self) -> bool {
        self.leaving.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        let _held = self.groups.write();
        self.closed.store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn levels(&self) -> &crate::features::Levels {
        &self.levels
    }

    pub(crate) fn get(&self, group: &GroupId) -> Option<Member> {
        self.groups.read().get(group).cloned()
    }

    /// Takes a group in; refused once the broker is stopping - the caller stops the group instead, so none started
    /// while the stop was under way is left running behind it.
    #[must_use]
    pub(crate) fn insert(&self, group: GroupId, member: Member) -> bool {
        let mut groups = self.groups.write();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        groups.insert(group, member);
        true
    }

    pub(crate) fn remove(&self, group: &GroupId) -> Option<Member> {
        self.heard.write().remove(group);
        self.groups.write().remove(group)
    }

    pub(crate) fn all(&self) -> Vec<(GroupId, Member)> {
        self.groups.read().iter().map(|(g, m)| (*g, m.clone())).collect()
    }

    /// The leaders of the groups held here, as far as they are known: what this broker tells the others.
    pub(crate) fn hosted_leaders(&self) -> Vec<LeaderOf> {
        self.groups
            .read()
            .iter()
            .filter_map(|(group, m)| {
                let (leader, term) = m.leader();
                leader.map(|leader| LeaderOf {
                    group: *group,
                    leader,
                    term,
                })
            })
            .collect()
    }

    /// A group's leader and term: from the group itself when it is held here, else as last reported.
    pub(crate) fn leader_of(&self, group: &GroupId) -> Option<(u64, u64)> {
        if let Some(m) = self.get(group) {
            let (leader, term) = m.leader();
            return leader.map(|l| (l, term));
        }
        self.remote.read().get(group).copied()
    }

    /// Takes in what another broker reports, keeping the newer term for each group not held here.
    pub(crate) fn learn(&self, reported: Vec<LeaderOf>) {
        let held = self.groups.read();
        let mut remote = self.remote.write();
        for r in reported {
            if held.contains_key(&r.group) {
                continue;
            }
            let newer = remote.get(&r.group).is_none_or(|(_, term)| r.term >= *term);
            if newer {
                remote.insert(r.group, (r.leader, r.term));
            }
        }
    }

    /// What the last pass could not start (replica.rs reconcile): empty when every partition this broker holds is up.
    pub(crate) fn set_not_started(&self, groups: Vec<(GroupId, String)>) {
        *self.not_started.write() = groups;
    }

    /// The partitions this broker should hold and could not start, each with why.
    pub(crate) fn not_started(&self) -> Vec<(GroupId, String)> {
        self.not_started.read().clone()
    }

    pub(crate) fn mark_recovering(&self, group: GroupId) {
        self.recovering.write().entry(group).or_insert(None);
    }

    /// As [`Self::mark_recovering`], with the mark on disk that is removed once the copy has caught up.
    pub(crate) fn mark_recovering_at(&self, group: GroupId, mark: std::path::PathBuf) {
        self.recovering.write().insert(group, Some(mark));
    }

    /// Whether this broker's copy of `group` was lost and is not yet caught up.
    pub(crate) fn is_recovering(&self, group: &GroupId) -> bool {
        self.recovering.read().contains_key(group)
    }

    /// Whether this broker's copy of `group` may vote: always, unless it is recovering - then once it has applied
    /// everything its leader has committed (and from then on).
    pub(crate) fn may_vote(&self, group: &GroupId, member: &Member) -> bool {
        if !self.recovering.read().contains_key(group) {
            return true;
        }
        let caught_up = with_raft!(member, r => {
            let metrics = r.metrics();
            let m = metrics.borrow_watched();
            m.current_leader.is_some()
                && m.cluster_committed.is_some()
                && m.last_applied.map(|l| l.index) >= m.cluster_committed.map(|c| c.index)
        });
        if caught_up {
            // The mark goes first: were it to stay, the copy would only be held back again at the next start.
            let mark = self.recovering.read().get(group).cloned().flatten();
            if let Some(mark) = mark
                && let Err(e) = std::fs::remove_file(&mark)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(%group, error = %e, "a caught-up copy is still marked as recovering on disk; it votes again once the mark can be removed");
                return false;
            }
            self.recovering.write().remove(group);
            // Level with its leader: it may stand for election again.
            with_raft!(member, r => r.runtime_config().elect(true));
            return true;
        }
        self.accept_surviving.load(std::sync::atomic::Ordering::Acquire)
    }

    /// How this broker stands, for "ready": see [`Readiness`].
    pub(crate) fn readiness(&self, me: u64, majority_within: std::time::Duration) -> Readiness {
        let majority_within = u64::try_from(majority_within.as_millis()).unwrap_or(u64::MAX);
        let mut r = Readiness {
            not_started: self.not_started(),
            ..Readiness::default()
        };
        let recovering = self.recovering.read();
        for (group, member) in self.all() {
            let (leader, applied, committed, since_majority, standing) = with_raft!(&member, raft => {
                let metrics = raft.metrics();
                let m = metrics.borrow_watched();
                (
                    m.current_leader,
                    m.last_applied.map(|l| l.index),
                    m.cluster_committed.map(|c| c.index),
                    m.last_quorum_acked
                        .as_ref()
                        .map(|at| u64::try_from(openraft::Instant::elapsed(&**at).as_millis()).unwrap_or(u64::MAX)),
                    m.state.is_candidate(),
                )
            });
            // A leader named is not a leader there: a broker goes on naming the one it last followed, and one that
            // leads goes on leading in its own eyes with nobody left to hear it. So: a copy standing for election
            // has no leader; and a copy leading is taken at its word only while most of the others have answered
            // it lately.
            // And a copy that follows has a leader only while that leader goes on being heard from.
            let silent = leader.is_some_and(|l| l != me)
                && self.since_heard(&group).is_none_or(|ms| ms > majority_within);
            let leader = if standing || silent { None } else { leader };
            let cut_off = leader == Some(me) && since_majority.is_some_and(|ms| ms > majority_within);
            let level =
                leader == Some(me) || applied.map_or(0, |a| a + READY_WITHIN) >= committed.unwrap_or(0);
            if matches!(group, GroupId::Controller) {
                r.controller_led = leader.is_some() && !cut_off;
                // What this broker should hold is read from the cluster's own state: until that is level with
                // its leader, the list may be an old one - or empty, on a broker that has only just started.
                r.controller_level = r.controller_led && level;
                continue;
            }
            r.held += 1;
            if recovering.contains_key(&group) {
                r.recovering += 1;
            } else if leader.is_none() || cut_off {
                r.without_leader += 1;
            } else if !level {
                r.behind += 1;
            }
        }
        r
    }

    /// Looks at every copy still held back and lets go of the ones that have caught up - their mark on disk removed.
    /// Asked every few seconds, not only when a vote is asked for: a mark left on a copy that caught up long ago
    /// would hold it back at the next start, when no leader is there yet to be caught up with.
    pub(crate) fn settle_recovered(&self) {
        let held: Vec<GroupId> = self.recovering.read().keys().copied().collect();
        for group in held {
            if let Some(member) = self.get(&group) {
                let _ = self.may_vote(&group, &member);
            }
        }
    }

    /// `target` came back with an empty disk: every group this broker leads with `target` in it takes its emptied
    /// log once (Raft refuses a log that goes back, unless told the broker lost its disk).
    pub(crate) async fn allow_reset(&self, me: u64, target: u64) {
        for (group, member) in self.all() {
            if member.leader().0 != Some(me) {
                continue;
            }
            let allowed = with_raft!(&member, r => r.trigger().allow_next_revert(&target, true).await);
            match allowed {
                Ok(Ok(())) => {
                    tracing::info!(%group, broker = target, "a broker back with an empty disk: catching it up")
                }
                Ok(Err(e)) => tracing::debug!(%group, broker = target, error = %e, "no log reset to allow"),
                Err(e) => tracing::debug!(%group, error = %e, "the group is stopping"),
            }
        }
    }

    /// The index the controller here has committed, if it is part of a cluster yet.
    pub(crate) fn controller_committed(&self) -> Option<u64> {
        match self.get(&GroupId::Controller)? {
            Member::Controller(r, _) => r.metrics().borrow_watched().local_committed.map(|c| c.index),
            Member::Partition(..) => None,
        }
    }

    /// Forgets reported leaders of groups that no longer exist.
    pub(crate) fn forget_remote(&self, keep: impl Fn(&GroupId) -> bool) {
        self.remote.write().retain(|g, _| keep(g));
    }
}
