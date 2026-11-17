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
    /// Groups whose copy here was lost with this broker's disk and is not yet caught up: they vote for no one, lest
    /// they elect a leader missing writes this broker acknowledged before.
    recovering: RwLock<std::collections::HashSet<GroupId>>,
    /// Set as the broker stops: no group is taken in after it.
    closed: std::sync::atomic::AtomicBool,
}

impl Members {
    /// The groups of a broker whose build supports feature level `supported`.
    pub(crate) fn new(supported: u32) -> Self {
        Self {
            levels: crate::features::Levels::new(supported),
            groups: RwLock::default(),
            remote: RwLock::default(),
            recovering: RwLock::default(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Takes no group in from now on (the broker is stopping); what is held now is what gets stopped.
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

    pub(crate) fn mark_recovering(&self, group: GroupId) {
        self.recovering.write().insert(group);
    }

    /// Whether this broker's copy of `group` was lost with its disk and is not yet caught up.
    pub(crate) fn is_recovering(&self, group: &GroupId) -> bool {
        self.recovering.read().contains(group)
    }

    /// Whether this broker's copy of `group` may vote: always, unless it is recovering - then once it has applied
    /// everything its leader has committed (and from then on).
    pub(crate) fn may_vote(&self, group: &GroupId, member: &Member) -> bool {
        if !self.recovering.read().contains(group) {
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
            self.recovering.write().remove(group);
        }
        caught_up
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
