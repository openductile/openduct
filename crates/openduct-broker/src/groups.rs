// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The consumer-group coordinator: Kafka's classic group protocol (JoinGroup, SyncGroup, Heartbeat, LeaveGroup),
//! as one broker runs it.
//!
//! A group moves through four phases, as in Kafka:
//!
//! - **Empty** - no members.
//! - **PreparingRebalance** - members are (re)joining. The round ends when every known member has joined, or when
//!   the rebalance timeout passes (members that did not rejoin are removed). A brand-new group waits
//!   `group_initial_rebalance_delay` first, so members starting together land in one assignment.
//! - **CompletingRebalance** - the round has ended: every member has its JoinGroup answer, and the leader is
//!   computing the assignment. Members wait in SyncGroup for it.
//! - **Stable** - the leader's assignment is handed out; members heartbeat.
//!
//! A member that stops heartbeating for its session timeout is removed, and the group rebalances. Waiting requests
//! are bounded: JoinGroup by the rebalance timeout, SyncGroup by the same.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::codes;

/// What a JoinGroup is answered with.
#[derive(Debug, Clone)]
pub(crate) struct JoinOutcome {
    pub(crate) error: i16,
    pub(crate) generation: i32,
    pub(crate) protocol_type: Option<String>,
    pub(crate) protocol_name: Option<String>,
    pub(crate) leader: String,
    pub(crate) member_id: String,
    /// The members and their metadata - for the leader only, empty for everyone else.
    pub(crate) members: Vec<(String, Option<String>, Bytes)>,
}

impl JoinOutcome {
    fn error(error: i16, member_id: String) -> Self {
        Self {
            error,
            generation: -1,
            protocol_type: None,
            protocol_name: None,
            leader: String::new(),
            member_id,
            members: Vec::new(),
        }
    }
}

/// What a SyncGroup is answered with.
#[derive(Debug, Clone)]
pub(crate) struct SyncOutcome {
    pub(crate) error: i16,
    pub(crate) protocol_type: Option<String>,
    pub(crate) protocol_name: Option<String>,
    pub(crate) assignment: Bytes,
}

impl SyncOutcome {
    fn error(error: i16) -> Self {
        Self {
            error,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
        }
    }
}

/// A JoinGroup as the coordinator needs it.
#[derive(Debug, Clone)]
pub(crate) struct JoinRequest {
    pub(crate) group: String,
    pub(crate) member_id: String,
    pub(crate) group_instance_id: Option<String>,
    pub(crate) client_id: String,
    /// Where the member connected from, as Kafka shows it (`/127.0.0.1`).
    pub(crate) client_host: String,
    pub(crate) session_timeout: Duration,
    pub(crate) rebalance_timeout: Duration,
    pub(crate) protocol_type: String,
    pub(crate) protocols: Vec<(String, Bytes)>,
    /// From JoinGroup v4, a member without an id is first given one and asked to join again with it.
    pub(crate) require_known_member_id: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl Phase {
    /// The state's name, as ListGroups and DescribeGroups answer it.
    fn name(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
        }
    }
}

/// What OffsetDelete must leave alone in a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Protected {
    /// The coordinator does not hold the group (it may still have committed offsets).
    Unknown,
    Nothing,
    Topics(std::collections::BTreeSet<String>),
    Everything,
}

/// The topics a consumer subscribes to, from its member metadata (Kafka's ConsumerProtocolSubscription: a version,
/// then the subscription). Metadata that does not read is no subscription.
fn subscribed(metadata: &Bytes) -> Vec<String> {
    use kafka_protocol::messages::ConsumerProtocolSubscription;
    use kafka_protocol::protocol::Decodable;
    let mut body = metadata.clone();
    let Some(version) = body.get(0..2).and_then(|v| <[u8; 2]>::try_from(v).ok()) else {
        return Vec::new();
    };
    let version = i16::from_be_bytes(version).clamp(0, 3);
    bytes::Buf::advance(&mut body, 2);
    ConsumerProtocolSubscription::decode(&mut body, version)
        .map(|s| s.topics.iter().map(ToString::to_string).collect())
        .unwrap_or_default()
}

/// A group as ListGroups and DescribeGroups show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GroupView {
    pub(crate) group_id: String,
    pub(crate) state: &'static str,
    /// `consumer` for a consumer group; empty for a group that only committed offsets.
    pub(crate) protocol_type: String,
    /// The protocol the group settled on (for consumers, the assignor), once it has.
    pub(crate) protocol: String,
    pub(crate) members: Vec<MemberView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberView {
    pub(crate) member_id: String,
    pub(crate) group_instance_id: Option<String>,
    pub(crate) client_id: String,
    pub(crate) client_host: String,
    /// The member's metadata for the protocol chosen; empty before one is.
    pub(crate) metadata: Bytes,
    pub(crate) assignment: Bytes,
}

impl GroupView {
    /// A group known only by its committed offsets: no members, nothing chosen.
    pub(crate) fn empty(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_owned(),
            state: Phase::Empty.name(),
            protocol_type: String::new(),
            protocol: String::new(),
            members: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct Member {
    group_instance_id: Option<String>,
    client_id: String,
    client_host: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    protocols: Vec<(String, Bytes)>,
    assignment: Bytes,
    last_seen: Instant,
    joined: Option<oneshot::Sender<JoinOutcome>>,
    synced: Option<oneshot::Sender<SyncOutcome>>,
}

#[derive(Debug)]
struct Group {
    phase: Phase,
    generation: i32,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: Option<String>,
    members: BTreeMap<String, Member>,
    /// Ids handed out by MEMBER_ID_REQUIRED, not yet used to join.
    pending: HashSet<String>,
    /// When the current phase must end on its own.
    deadline: Option<Instant>,
    /// When a new group's first round may end even if everyone has joined (the initial rebalance delay).
    not_before: Option<Instant>,
}

impl Group {
    fn new() -> Self {
        Self {
            phase: Phase::Empty,
            generation: 0,
            protocol_type: None,
            protocol_name: None,
            leader: None,
            members: BTreeMap::new(),
            pending: HashSet::new(),
            deadline: None,
            not_before: None,
        }
    }

    fn view(&self, group_id: &str) -> GroupView {
        let protocol = self.protocol_name.clone().unwrap_or_default();
        GroupView {
            group_id: group_id.to_owned(),
            state: self.phase.name(),
            protocol_type: self.protocol_type.clone().unwrap_or_default(),
            protocol: protocol.clone(),
            members: self
                .members
                .iter()
                .map(|(id, m)| MemberView {
                    member_id: id.clone(),
                    group_instance_id: m.group_instance_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    metadata: m
                        .protocols
                        .iter()
                        .find(|(name, _)| *name == protocol)
                        .map(|(_, meta)| meta.clone())
                        .unwrap_or_default(),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        }
    }

    fn max_rebalance_timeout(&self) -> Duration {
        self.members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or(Duration::from_secs(60))
    }

    /// Start a rebalance round: members must rejoin within the rebalance timeout.
    fn prepare_rebalance(&mut self, now: Instant) {
        if self.phase == Phase::PreparingRebalance {
            return;
        }
        // Anyone still waiting in SyncGroup for the round that is being abandoned is told to rejoin.
        for m in self.members.values_mut() {
            if let Some(tx) = m.synced.take() {
                let _ = tx.send(SyncOutcome::error(codes::REBALANCE_IN_PROGRESS));
            }
        }
        self.phase = Phase::PreparingRebalance;
        self.deadline = Some(now + self.max_rebalance_timeout());
    }

    fn all_joined(&self) -> bool {
        !self.members.is_empty() && self.members.values().all(|m| m.joined.is_some())
    }

    /// End the join round: drop members that did not rejoin, pick the protocol and leader, answer every joiner.
    fn complete_join(&mut self, now: Instant) {
        self.members.retain(|_, m| m.joined.is_some());
        self.not_before = None;
        if self.members.is_empty() {
            self.phase = Phase::Empty;
            self.generation += 1;
            self.leader = None;
            self.protocol_name = None;
            self.deadline = None;
            return;
        }
        let Some(protocol) = self.select_protocol() else {
            for (id, m) in &mut self.members {
                if let Some(tx) = m.joined.take() {
                    let _ = tx.send(JoinOutcome::error(codes::INCONSISTENT_GROUP_PROTOCOL, id.clone()));
                }
            }
            self.members.clear();
            self.phase = Phase::Empty;
            self.deadline = None;
            return;
        };
        self.generation += 1;
        self.protocol_name = Some(protocol.clone());
        let leader = match &self.leader {
            Some(l) if self.members.contains_key(l) => l.clone(),
            _ => self.members.keys().next().cloned().unwrap_or_default(),
        };
        self.leader = Some(leader.clone());
        self.phase = Phase::CompletingRebalance;
        self.deadline = Some(now + self.max_rebalance_timeout());
        let roster: Vec<(String, Option<String>, Bytes)> = self
            .members
            .iter()
            .map(|(id, m)| {
                let metadata = m
                    .protocols
                    .iter()
                    .find(|(name, _)| *name == protocol)
                    .map(|(_, meta)| meta.clone())
                    .unwrap_or_default();
                (id.clone(), m.group_instance_id.clone(), metadata)
            })
            .collect();
        for (id, m) in &mut self.members {
            m.assignment = Bytes::new();
            m.last_seen = now;
            if let Some(tx) = m.joined.take() {
                let _ = tx.send(JoinOutcome {
                    error: codes::NONE,
                    generation: self.generation,
                    protocol_type: self.protocol_type.clone(),
                    protocol_name: Some(protocol.clone()),
                    leader: leader.clone(),
                    member_id: id.clone(),
                    members: if *id == leader { roster.clone() } else { Vec::new() },
                });
            }
        }
    }

    /// The protocol every member supports, chosen by vote: each member votes for its most preferred candidate.
    fn select_protocol(&self) -> Option<String> {
        let mut candidates: Option<HashSet<&str>> = None;
        for m in self.members.values() {
            let names: HashSet<&str> = m.protocols.iter().map(|(n, _)| n.as_str()).collect();
            candidates = Some(match candidates {
                None => names,
                Some(c) => c.intersection(&names).copied().collect(),
            });
        }
        let candidates = candidates?;
        let mut votes: HashMap<&str, usize> = HashMap::new();
        for m in self.members.values() {
            if let Some((name, _)) = m.protocols.iter().find(|(n, _)| candidates.contains(n.as_str())) {
                *votes.entry(name.as_str()).or_default() += 1;
            }
        }
        // Most votes wins; ties go to the name that sorts first, so the choice is deterministic.
        let mut ranked: Vec<(&str, usize)> = votes.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        ranked.first().map(|(n, _)| (*n).to_owned())
    }

    fn remove_member(&mut self, id: &str, now: Instant) {
        if let Some(mut m) = self.members.remove(id) {
            if let Some(tx) = m.joined.take() {
                let _ = tx.send(JoinOutcome::error(codes::UNKNOWN_MEMBER_ID, id.to_owned()));
            }
            if let Some(tx) = m.synced.take() {
                let _ = tx.send(SyncOutcome::error(codes::UNKNOWN_MEMBER_ID));
            }
        }
        if self.members.is_empty() {
            self.phase = Phase::Empty;
            self.leader = None;
            self.deadline = None;
        } else if self.phase == Phase::PreparingRebalance {
            if self.all_joined() && self.not_before.is_none_or(|t| now >= t) {
                self.complete_join(now);
            }
        } else {
            self.prepare_rebalance(now);
        }
    }
}

/// All the groups this broker coordinates.
#[derive(Debug, Clone)]
pub(crate) struct Coordinator {
    groups: Arc<Mutex<HashMap<String, Group>>>,
    initial_delay: Duration,
    min_session: Duration,
    max_session: Duration,
}

impl Coordinator {
    pub(crate) fn new(initial_delay: Duration, min_session: Duration, max_session: Duration) -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            initial_delay,
            min_session,
            max_session,
        }
    }

    pub(crate) async fn join(&self, req: JoinRequest) -> JoinOutcome {
        if req.group.is_empty() {
            return JoinOutcome::error(codes::INVALID_GROUP_ID, req.member_id);
        }
        if req.session_timeout < self.min_session || req.session_timeout > self.max_session {
            return JoinOutcome::error(codes::INVALID_SESSION_TIMEOUT, req.member_id);
        }
        if req.protocols.is_empty() || req.protocol_type.is_empty() {
            return JoinOutcome::error(codes::INCONSISTENT_GROUP_PROTOCOL, req.member_id);
        }
        let now = Instant::now();
        let wait = req.rebalance_timeout.max(self.initial_delay) + Duration::from_secs(5);
        let (rx, member_id) = {
            let mut groups = self.groups.lock();
            let group = groups.entry(req.group.clone()).or_insert_with(Group::new);
            if group.phase != Phase::Empty
                && group.protocol_type.as_deref() != Some(req.protocol_type.as_str())
            {
                return JoinOutcome::error(codes::INCONSISTENT_GROUP_PROTOCOL, req.member_id);
            }
            let mut member_id = req.member_id.clone();
            if member_id.is_empty() {
                member_id = format!("{}-{}", req.client_id, uuid::Uuid::new_v4());
                if req.require_known_member_id {
                    group.pending.insert(member_id.clone());
                    return JoinOutcome::error(codes::MEMBER_ID_REQUIRED, member_id);
                }
            } else if !group.members.contains_key(&member_id) && !group.pending.remove(&member_id) {
                return JoinOutcome::error(codes::UNKNOWN_MEMBER_ID, member_id);
            }
            let was_empty = group.phase == Phase::Empty;
            if was_empty {
                group.protocol_type = Some(req.protocol_type.clone());
            }
            let (tx, rx) = oneshot::channel();
            let member = group.members.entry(member_id.clone()).or_insert_with(|| Member {
                group_instance_id: req.group_instance_id.clone(),
                client_id: String::new(),
                client_host: String::new(),
                session_timeout: req.session_timeout,
                rebalance_timeout: req.rebalance_timeout,
                protocols: Vec::new(),
                assignment: Bytes::new(),
                last_seen: now,
                joined: None,
                synced: None,
            });
            member.session_timeout = req.session_timeout;
            member.rebalance_timeout = req.rebalance_timeout;
            member.protocols = req.protocols.clone();
            member.client_id.clone_from(&req.client_id);
            member.client_host.clone_from(&req.client_host);
            member.last_seen = now;
            if let Some(old) = member.joined.replace(tx) {
                // A member that joins again while its first join is waiting: the first is answered as superseded.
                let _ = old.send(JoinOutcome::error(codes::UNKNOWN_MEMBER_ID, member_id.clone()));
            }
            if group.phase != Phase::PreparingRebalance {
                group.prepare_rebalance(now);
                if was_empty {
                    let delay = self.initial_delay.min(req.rebalance_timeout);
                    group.not_before = Some(now + delay);
                    group.deadline = Some(now + delay);
                }
            }
            if group.all_joined() && group.not_before.is_none_or(|t| now >= t) {
                group.complete_join(now);
            }
            (rx, member_id)
        };
        match tokio::time::timeout(wait, rx).await {
            Ok(Ok(outcome)) => outcome,
            _ => JoinOutcome::error(codes::REBALANCE_IN_PROGRESS, member_id),
        }
    }

    pub(crate) async fn sync(
        &self,
        group_id: &str,
        member_id: &str,
        generation: i32,
        assignments: Vec<(String, Bytes)>,
    ) -> SyncOutcome {
        let now = Instant::now();
        let (rx, wait) = {
            let mut groups = self.groups.lock();
            let Some(group) = groups.get_mut(group_id) else {
                return SyncOutcome::error(codes::UNKNOWN_MEMBER_ID);
            };
            if !group.members.contains_key(member_id) {
                return SyncOutcome::error(codes::UNKNOWN_MEMBER_ID);
            }
            if generation != group.generation {
                return SyncOutcome::error(codes::ILLEGAL_GENERATION);
            }
            let wait = group.max_rebalance_timeout() + Duration::from_secs(5);
            match group.phase {
                Phase::Empty | Phase::PreparingRebalance => {
                    return SyncOutcome::error(codes::REBALANCE_IN_PROGRESS);
                }
                Phase::Stable => {
                    let m = group.members.get_mut(member_id);
                    return match m {
                        Some(m) => {
                            m.last_seen = now;
                            SyncOutcome {
                                error: codes::NONE,
                                protocol_type: group.protocol_type.clone(),
                                protocol_name: group.protocol_name.clone(),
                                assignment: m.assignment.clone(),
                            }
                        }
                        None => SyncOutcome::error(codes::UNKNOWN_MEMBER_ID),
                    };
                }
                Phase::CompletingRebalance => {}
            }
            let is_leader = group.leader.as_deref() == Some(member_id);
            if is_leader {
                let given: HashMap<String, Bytes> = assignments.into_iter().collect();
                let protocol_type = group.protocol_type.clone();
                let protocol_name = group.protocol_name.clone();
                for (id, m) in &mut group.members {
                    m.assignment = given.get(id).cloned().unwrap_or_default();
                    if let Some(tx) = m.synced.take() {
                        let _ = tx.send(SyncOutcome {
                            error: codes::NONE,
                            protocol_type: protocol_type.clone(),
                            protocol_name: protocol_name.clone(),
                            assignment: m.assignment.clone(),
                        });
                    }
                }
                group.phase = Phase::Stable;
                group.deadline = None;
                let mine = group
                    .members
                    .get_mut(member_id)
                    .map(|m| {
                        m.last_seen = now;
                        m.assignment.clone()
                    })
                    .unwrap_or_default();
                return SyncOutcome {
                    error: codes::NONE,
                    protocol_type,
                    protocol_name,
                    assignment: mine,
                };
            }
            let (tx, rx) = oneshot::channel();
            if let Some(m) = group.members.get_mut(member_id) {
                m.last_seen = now;
                m.synced = Some(tx);
            }
            (rx, wait)
        };
        match tokio::time::timeout(wait, rx).await {
            Ok(Ok(outcome)) => outcome,
            _ => SyncOutcome::error(codes::REBALANCE_IN_PROGRESS),
        }
    }

    pub(crate) fn heartbeat(&self, group_id: &str, member_id: &str, generation: i32) -> i16 {
        let mut groups = self.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        let phase = group.phase;
        let current = group.generation;
        let Some(m) = group.members.get_mut(member_id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        m.last_seen = Instant::now();
        match phase {
            Phase::Empty => codes::UNKNOWN_MEMBER_ID,
            Phase::PreparingRebalance => codes::REBALANCE_IN_PROGRESS,
            Phase::CompletingRebalance | Phase::Stable if generation != current => codes::ILLEGAL_GENERATION,
            Phase::CompletingRebalance | Phase::Stable => codes::NONE,
        }
    }

    /// Remove members; answers each member's own error code.
    pub(crate) fn leave(&self, group_id: &str, member_ids: &[String]) -> Vec<(String, i16)> {
        let mut groups = self.groups.lock();
        let now = Instant::now();
        let Some(group) = groups.get_mut(group_id) else {
            return member_ids
                .iter()
                .map(|m| (m.clone(), codes::UNKNOWN_MEMBER_ID))
                .collect();
        };
        member_ids
            .iter()
            .map(|id| {
                if group.members.contains_key(id) {
                    group.remove_member(id, now);
                    (id.clone(), codes::NONE)
                } else {
                    (id.clone(), codes::UNKNOWN_MEMBER_ID)
                }
            })
            .collect()
    }

    /// Whether a commit from this member at this generation may be stored. A commit with generation -1 and no
    /// member is a standalone consumer's, allowed while the group has no members.
    pub(crate) fn check_commit(&self, group_id: &str, member_id: &str, generation: i32) -> i16 {
        let mut groups = self.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return if generation < 0 {
                codes::NONE
            } else {
                codes::ILLEGAL_GENERATION
            };
        };
        if generation < 0 && member_id.is_empty() {
            return if group.members.is_empty() {
                codes::NONE
            } else {
                codes::ILLEGAL_GENERATION
            };
        }
        let phase = group.phase;
        let current = group.generation;
        let Some(m) = group.members.get_mut(member_id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        m.last_seen = Instant::now();
        if generation != current {
            return codes::ILLEGAL_GENERATION;
        }
        if phase == Phase::PreparingRebalance {
            return codes::REBALANCE_IN_PROGRESS;
        }
        codes::NONE
    }

    /// Run the clock: end join rounds whose time is up, give up on assignments the leader never sent, and remove
    /// members whose session has expired. Called periodically.
    /// Every group this coordinator holds, as it is now.
    pub(crate) fn views(&self) -> Vec<GroupView> {
        let groups = self.groups.lock();
        let mut out: Vec<GroupView> = groups.iter().map(|(id, g)| g.view(id)).collect();
        out.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        out
    }

    /// Forgets a group with no members (DeleteGroups): `Ok(true)` when it was held, `Ok(false)` when it was not, and
    /// `NON_EMPTY_GROUP` while it has members - checked and removed at once, so no member joins in between.
    pub(crate) fn remove_empty(&self, group_id: &str) -> Result<bool, i16> {
        let mut groups = self.groups.lock();
        match groups.get(group_id) {
            None => Ok(false),
            Some(g) if !g.members.is_empty() => Err(codes::NON_EMPTY_GROUP),
            Some(_) => Ok(groups.remove(group_id).is_some()),
        }
    }

    /// What may not lose its offsets (OffsetDelete): nothing in a group with no members; in a consumer group with
    /// members, the topics they subscribe to; in another kind of group with members, everything.
    pub(crate) fn protected(&self, group_id: &str) -> Protected {
        let groups = self.groups.lock();
        let Some(g) = groups.get(group_id) else {
            return Protected::Unknown;
        };
        if g.members.is_empty() {
            return Protected::Nothing;
        }
        if g.protocol_type.as_deref() != Some("consumer") {
            return Protected::Everything;
        }
        Protected::Topics(
            g.members
                .values()
                .flat_map(|m| m.protocols.iter())
                .flat_map(|(_, metadata)| subscribed(metadata))
                .collect(),
        )
    }

    /// One group as it is now, if this coordinator holds it.
    pub(crate) fn view(&self, group_id: &str) -> Option<GroupView> {
        self.groups.lock().get(group_id).map(|g| g.view(group_id))
    }

    pub(crate) fn tick(&self) {
        let now = Instant::now();
        let mut groups = self.groups.lock();
        for group in groups.values_mut() {
            match group.phase {
                Phase::PreparingRebalance => {
                    let initial_over = group.not_before.is_some_and(|t| now >= t);
                    if group.deadline.is_some_and(|d| now >= d) || (initial_over && group.all_joined()) {
                        group.complete_join(now);
                    }
                }
                Phase::CompletingRebalance => {
                    if group.deadline.is_some_and(|d| now >= d) {
                        group.prepare_rebalance(now);
                    }
                }
                Phase::Stable | Phase::Empty => {}
            }
            let expired: Vec<String> = group
                .members
                .iter()
                // A member waiting in JoinGroup or SyncGroup is alive: its wait is bounded on its own.
                .filter(|(_, m)| {
                    m.joined.is_none()
                        && m.synced.is_none()
                        && now.duration_since(m.last_seen) > m.session_timeout
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in expired {
                tracing::info!(member = %id, "consumer group member's session expired");
                group.remove_member(&id, now);
            }
        }
        groups.retain(|_, g| g.phase != Phase::Empty || !g.pending.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coordinator() -> Coordinator {
        Coordinator::new(Duration::ZERO, Duration::from_millis(10), Duration::from_secs(60))
    }

    fn join(group: &str, member: &str, protocols: &[&str]) -> JoinRequest {
        JoinRequest {
            group: group.to_owned(),
            member_id: member.to_owned(),
            group_instance_id: None,
            client_id: "c".to_owned(),
            client_host: "/127.0.0.1".to_owned(),
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(2),
            protocol_type: "consumer".to_owned(),
            protocols: protocols
                .iter()
                .map(|p| ((*p).to_owned(), Bytes::from_static(b"meta")))
                .collect(),
            require_known_member_id: true,
        }
    }

    #[tokio::test]
    async fn a_member_gets_an_id_then_leads_and_receives_its_own_assignment() {
        let c = coordinator();
        let first = c.join(join("g", "", &["range"])).await;
        assert_eq!(first.error, codes::MEMBER_ID_REQUIRED);
        let joined = c.join(join("g", &first.member_id, &["range"])).await;
        assert_eq!(joined.error, codes::NONE);
        assert_eq!(joined.generation, 1);
        assert_eq!(joined.leader, first.member_id);
        assert_eq!(joined.members.len(), 1, "the leader sees the roster");
        let synced = c
            .sync(
                "g",
                &first.member_id,
                1,
                vec![(first.member_id.clone(), Bytes::from_static(b"p0"))],
            )
            .await;
        assert_eq!(synced.error, codes::NONE);
        assert_eq!(synced.assignment, Bytes::from_static(b"p0"));
        assert_eq!(c.heartbeat("g", &first.member_id, 1), codes::NONE);
        assert_eq!(c.heartbeat("g", &first.member_id, 0), codes::ILLEGAL_GENERATION);
    }

    #[tokio::test]
    async fn a_second_member_triggers_a_rebalance_both_rejoin_and_each_gets_its_part() {
        let c = coordinator();
        let a = c.join(join("g", "", &["range"])).await.member_id;
        c.join(join("g", &a, &["range"])).await;
        c.sync("g", &a, 1, vec![(a.clone(), Bytes::from_static(b"all"))])
            .await;
        let b = c.join(join("g", "", &["range"])).await.member_id;
        // B joins: the group starts rebalancing and A learns it from its heartbeat.
        let c2 = c.clone();
        let b2 = b.clone();
        let b_join = tokio::spawn(async move { c2.join(join("g", &b2, &["range"])).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(c.heartbeat("g", &a, 1), codes::REBALANCE_IN_PROGRESS);
        let a_join = c.join(join("g", &a, &["range"])).await;
        let b_join = b_join.await.unwrap();
        assert_eq!((a_join.generation, b_join.generation), (2, 2));
        assert_eq!(a_join.leader, a, "the leader keeps leading");
        assert_eq!(a_join.members.len(), 2);
        assert!(b_join.members.is_empty());
        let c3 = c.clone();
        let b3 = b.clone();
        let b_sync = tokio::spawn(async move { c3.sync("g", &b3, 2, Vec::new()).await });
        let a_sync = c
            .sync(
                "g",
                &a,
                2,
                vec![
                    (a.clone(), Bytes::from_static(b"p0")),
                    (b.clone(), Bytes::from_static(b"p1")),
                ],
            )
            .await;
        assert_eq!(a_sync.assignment, Bytes::from_static(b"p0"));
        assert_eq!(b_sync.await.unwrap().assignment, Bytes::from_static(b"p1"));
    }

    #[tokio::test]
    async fn a_member_that_stops_heartbeating_is_removed_and_one_that_leaves_is_gone() {
        let c = coordinator();
        let mut req = join("g", "", &["range"]);
        req.session_timeout = Duration::from_millis(20);
        let a = c.join(req.clone()).await.member_id;
        req.member_id = a.clone();
        c.join(req).await;
        c.sync("g", &a, 1, Vec::new()).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        c.tick();
        assert_eq!(c.heartbeat("g", &a, 1), codes::UNKNOWN_MEMBER_ID);
        assert_eq!(
            c.leave("g", std::slice::from_ref(&a)),
            vec![(a, codes::UNKNOWN_MEMBER_ID)]
        );
    }

    #[tokio::test]
    async fn members_with_no_protocol_in_common_are_refused() {
        let c = coordinator();
        let a = c.join(join("g", "", &["range"])).await.member_id;
        let joined = c.join(join("g", &a, &["range"])).await;
        assert_eq!(joined.error, codes::NONE);
        let mut other = join("g", "", &["range"]);
        other.protocol_type = "connect".to_owned();
        assert_eq!(c.join(other).await.error, codes::INCONSISTENT_GROUP_PROTOCOL);
    }

    #[tokio::test]
    async fn a_standalone_commit_is_allowed_only_while_the_group_has_no_members() {
        let c = coordinator();
        assert_eq!(c.check_commit("g", "", -1), codes::NONE);
        let a = c.join(join("g", "", &["range"])).await.member_id;
        c.join(join("g", &a, &["range"])).await;
        assert_eq!(c.check_commit("g", "", -1), codes::ILLEGAL_GENERATION);
        assert_eq!(c.check_commit("g", &a, 1), codes::NONE);
        assert_eq!(c.check_commit("g", &a, 7), codes::ILLEGAL_GENERATION);
    }
}
