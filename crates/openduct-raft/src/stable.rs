// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct's own shapes for everything Raft keeps on disk or sends to another broker - log ids, votes,
//! memberships, entries, snapshot metadata and Raft's messages - converted to and from openraft's types at the edge
//! of this crate. An openraft upgrade then changes code, never data or the wire.
//!
//! The rules that keep a format readable by the next version and the one before it:
//! - Every frame and file says its format (`FORMAT` here, `format` in each JSON file).
//! - Enums only grow: a new variant goes at the end; an existing variant's shape never changes - a new shape is a
//!   new variant, and the old one is still read. The same holds for the storage types a [`Command`] carries.
//! - A writer uses a newer format only once the cluster's feature level allows it (`features`), so a broker of the
//!   previous version can still read everything written.
//! - Golden files (`tests/golden/`) of each format must be read by every later build.

use std::collections::{BTreeMap, BTreeSet};

use openraft::raft::{
    AppendEntriesRequest, SnapshotResponse, StreamAppendError, TransferLeaderError, TransferLeaderRequest,
    VoteRequest, VoteResponse,
};
use openraft::{EntryPayload, Membership as RaftMembership};
use serde::{Deserialize, Serialize};

use crate::types::{self, Broker, Command, TypeConfig};

/// The format of a Raft log frame's body, its first byte.
pub(crate) const FORMAT: u8 = 1;

/// A JSON file's content with its format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Versioned<T> {
    pub(crate) format: u32,
    pub(crate) value: T,
}

/// The formats this build reads for a JSON file; it writes the newest the cluster allows.
pub(crate) const JSON_FORMATS: std::ops::RangeInclusive<u32> = 1..=1;

impl<T> Versioned<T> {
    pub(crate) fn current(value: T) -> Self {
        Self { format: 1, value }
    }

    /// The value, when this build reads its format.
    pub(crate) fn open(self, what: &str) -> Result<T, String> {
        if JSON_FORMATS.contains(&self.format) {
            Ok(self.value)
        } else {
            Err(format!(
                "{what} is format {}; this build reads formats {} to {}: it was written by a newer OpenDuct",
                self.format,
                JSON_FORMATS.start(),
                JSON_FORMATS.end()
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LogId {
    pub(crate) term: u64,
    pub(crate) node: u64,
    pub(crate) index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Vote {
    pub(crate) term: u64,
    pub(crate) node: u64,
    pub(crate) committed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Membership {
    /// One set of voters, or two while the membership changes (joint consensus).
    pub(crate) configs: Vec<BTreeSet<u64>>,
    pub(crate) nodes: BTreeMap<u64, Broker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AppliedMembership {
    pub(crate) log_id: Option<LogId>,
    pub(crate) membership: Membership,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Payload {
    Blank,
    Command(Command),
    Membership(Membership),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub(crate) log_id: LogId,
    pub(crate) payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotMeta {
    pub(crate) last_log_id: Option<LogId>,
    pub(crate) last_membership: AppliedMembership,
}

// Raft's messages between brokers.

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Append {
    pub(crate) vote: Vote,
    pub(crate) prev_log_id: Option<LogId>,
    pub(crate) entries: Vec<Entry>,
    pub(crate) leader_commit: Option<LogId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AppendRefusal {
    Conflict(LogId),
    HigherVote(Vote),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VoteAsk {
    pub(crate) vote: Vote,
    pub(crate) last_log_id: Option<LogId>,
    /// The leader that hands its leadership over by this election, if it is one.
    pub(crate) handed_over_by: Option<Vote>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VoteAnswer {
    pub(crate) vote: Vote,
    pub(crate) granted: bool,
    pub(crate) last_log_id: Option<LogId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Handover {
    pub(crate) from_leader: Vote,
    pub(crate) to: u64,
    pub(crate) last_log_id: Option<LogId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HandoverRefusal {
    VoteChanged {
        expected: Vote,
        actual: Vote,
    },
    LogNotFlushed {
        expected: Option<LogId>,
        actual: Option<LogId>,
    },
}

// Conversions. openraft's types appear only here (and in the trait implementations that hand them to Raft).

impl From<types::LogId> for LogId {
    fn from(l: types::LogId) -> Self {
        Self {
            term: l.leader_id.term,
            node: l.leader_id.node_id,
            index: l.index,
        }
    }
}

impl From<LogId> for types::LogId {
    fn from(l: LogId) -> Self {
        types::LogId::new(
            openraft::vote::leader_id_adv::LeaderId {
                term: l.term,
                node_id: l.node,
            },
            l.index,
        )
    }
}

fn ours(l: Option<types::LogId>) -> Option<LogId> {
    l.map(Into::into)
}

fn theirs(l: Option<LogId>) -> Option<types::LogId> {
    l.map(Into::into)
}

impl From<types::Vote> for Vote {
    fn from(v: types::Vote) -> Self {
        Self {
            term: v.leader_id.term,
            node: v.leader_id.node_id,
            committed: v.committed,
        }
    }
}

impl From<Vote> for types::Vote {
    fn from(v: Vote) -> Self {
        if v.committed {
            types::Vote::new_committed(v.term, v.node)
        } else {
            types::Vote::new(v.term, v.node)
        }
    }
}

impl From<&RaftMembership<u64, Broker>> for Membership {
    fn from(m: &RaftMembership<u64, Broker>) -> Self {
        Self {
            configs: m.get_joint_config().clone(),
            nodes: m.nodes().map(|(id, b)| (*id, b.clone())).collect(),
        }
    }
}

impl TryFrom<Membership> for RaftMembership<u64, Broker> {
    type Error = String;

    fn try_from(m: Membership) -> Result<Self, String> {
        RaftMembership::new(m.configs, m.nodes)
            .map_err(|e| format!("a membership names a broker it lists no address for: {e}"))
    }
}

impl From<&types::StoredMembership> for AppliedMembership {
    fn from(m: &types::StoredMembership) -> Self {
        Self {
            log_id: ours(*m.log_id()),
            membership: m.membership().into(),
        }
    }
}

impl TryFrom<AppliedMembership> for types::StoredMembership {
    type Error = String;

    fn try_from(m: AppliedMembership) -> Result<Self, String> {
        Ok(types::StoredMembership::new(
            theirs(m.log_id),
            m.membership.try_into()?,
        ))
    }
}

impl From<&types::Entry> for Entry {
    fn from(e: &types::Entry) -> Self {
        Self {
            log_id: e.log_id.into(),
            payload: match &e.payload {
                EntryPayload::Blank => Payload::Blank,
                EntryPayload::Normal(c) => Payload::Command(c.clone()),
                EntryPayload::Membership(m) => Payload::Membership(m.into()),
            },
        }
    }
}

impl Entry {
    /// The entry as Raft takes it.
    pub(crate) fn into_raft(self) -> Result<types::Entry, String> {
        let e = self;
        Ok(types::Entry {
            log_id: e.log_id.into(),
            payload: match e.payload {
                Payload::Blank => EntryPayload::Blank,
                Payload::Command(c) => EntryPayload::Normal(c),
                Payload::Membership(m) => EntryPayload::Membership(m.try_into()?),
            },
        })
    }
}

impl From<&types::SnapshotMeta> for SnapshotMeta {
    fn from(m: &types::SnapshotMeta) -> Self {
        Self {
            last_log_id: ours(m.last_log_id),
            last_membership: (&m.last_membership).into(),
        }
    }
}

impl TryFrom<SnapshotMeta> for types::SnapshotMeta {
    type Error = String;

    fn try_from(m: SnapshotMeta) -> Result<Self, String> {
        Ok(types::SnapshotMeta {
            last_log_id: theirs(m.last_log_id),
            last_membership: m.last_membership.try_into()?,
        })
    }
}

impl From<&AppendEntriesRequest<TypeConfig>> for Append {
    fn from(r: &AppendEntriesRequest<TypeConfig>) -> Self {
        Self {
            vote: r.vote.into(),
            prev_log_id: ours(r.prev_log_id),
            entries: r.entries.iter().map(Into::into).collect(),
            leader_commit: ours(r.leader_commit),
        }
    }
}

impl TryFrom<Append> for AppendEntriesRequest<TypeConfig> {
    type Error = String;

    fn try_from(a: Append) -> Result<Self, String> {
        Ok(AppendEntriesRequest {
            vote: a.vote.into(),
            prev_log_id: theirs(a.prev_log_id),
            entries: a
                .entries
                .into_iter()
                .map(Entry::into_raft)
                .collect::<Result<_, _>>()?,
            leader_commit: theirs(a.leader_commit),
        })
    }
}

impl From<StreamAppendError<TypeConfig>> for AppendRefusal {
    fn from(e: StreamAppendError<TypeConfig>) -> Self {
        match e {
            StreamAppendError::Conflict(l) => AppendRefusal::Conflict(l.into()),
            StreamAppendError::HigherVote(v) => AppendRefusal::HigherVote(v.into()),
        }
    }
}

impl From<AppendRefusal> for StreamAppendError<TypeConfig> {
    fn from(e: AppendRefusal) -> Self {
        match e {
            AppendRefusal::Conflict(l) => StreamAppendError::Conflict(l.into()),
            AppendRefusal::HigherVote(v) => StreamAppendError::HigherVote(v.into()),
        }
    }
}

impl From<&VoteRequest<TypeConfig>> for VoteAsk {
    fn from(r: &VoteRequest<TypeConfig>) -> Self {
        Self {
            vote: r.vote.into(),
            last_log_id: ours(r.last_log_id),
            handed_over_by: r.leadership_transfer_from.map(Into::into),
        }
    }
}

impl From<VoteAsk> for VoteRequest<TypeConfig> {
    fn from(a: VoteAsk) -> Self {
        let mut r = VoteRequest::new(a.vote.into(), theirs(a.last_log_id));
        r.leadership_transfer_from = a.handed_over_by.map(Into::into);
        r
    }
}

impl From<&VoteResponse<TypeConfig>> for VoteAnswer {
    fn from(r: &VoteResponse<TypeConfig>) -> Self {
        Self {
            vote: r.vote.into(),
            granted: r.vote_granted,
            last_log_id: ours(r.last_log_id),
        }
    }
}

impl From<VoteAnswer> for VoteResponse<TypeConfig> {
    fn from(a: VoteAnswer) -> Self {
        VoteResponse::new(types::Vote::from(a.vote), theirs(a.last_log_id), a.granted)
    }
}

impl From<&TransferLeaderRequest<TypeConfig>> for Handover {
    fn from(r: &TransferLeaderRequest<TypeConfig>) -> Self {
        Self {
            from_leader: (*r.from_leader()).into(),
            to: *r.to_node_id(),
            last_log_id: r.last_log_id().copied().map(Into::into),
        }
    }
}

impl From<Handover> for TransferLeaderRequest<TypeConfig> {
    fn from(h: Handover) -> Self {
        TransferLeaderRequest::new(h.from_leader.into(), h.to, theirs(h.last_log_id))
    }
}

impl From<TransferLeaderError<TypeConfig>> for HandoverRefusal {
    fn from(e: TransferLeaderError<TypeConfig>) -> Self {
        match e {
            TransferLeaderError::VoteChanged { expected, actual } => HandoverRefusal::VoteChanged {
                expected: expected.into(),
                actual: actual.into(),
            },
            TransferLeaderError::LogNotFlushed { expected, actual } => HandoverRefusal::LogNotFlushed {
                expected: ours(expected),
                actual: ours(actual),
            },
        }
    }
}

impl From<HandoverRefusal> for TransferLeaderError<TypeConfig> {
    fn from(e: HandoverRefusal) -> Self {
        match e {
            HandoverRefusal::VoteChanged { expected, actual } => TransferLeaderError::VoteChanged {
                expected: expected.into(),
                actual: actual.into(),
            },
            HandoverRefusal::LogNotFlushed { expected, actual } => TransferLeaderError::LogNotFlushed {
                expected: theirs(expected),
                actual: theirs(actual),
            },
        }
    }
}

impl From<&SnapshotResponse<TypeConfig>> for Vote {
    fn from(r: &SnapshotResponse<TypeConfig>) -> Self {
        r.vote.into()
    }
}

/// A snapshot answer from the vote it carries.
pub(crate) fn snapshot_answer(vote: Vote) -> SnapshotResponse<TypeConfig> {
    SnapshotResponse::new(vote.into())
}

#[cfg(test)]
mod tests;
