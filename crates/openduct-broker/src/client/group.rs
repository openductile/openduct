// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Membership of a consumer group, so several copies of one program share a topic's partitions - the standard
//! consumer protocol, so they can share a group with any other Kafka consumer.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, GroupId, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, OffsetCommitRequest, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use super::{Client, ClientError};

const MEMBER_ID_REQUIRED: i16 = 79;
const REBALANCE_IN_PROGRESS: i16 = 27;
const ILLEGAL_GENERATION: i16 = 22;
const UNKNOWN_MEMBER_ID: i16 = 25;
const PROTOCOL: &str = "range";

/// This program's place in a group: who it is, which generation, and the partitions it was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    pub group: String,
    pub topic: String,
    pub member_id: String,
    pub generation: i32,
    pub partitions: Vec<i32>,
}

/// What the group says on a heartbeat or a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// Carry on.
    Stable,
    /// Members came or went: commit what is done and join again.
    Rebalancing,
}

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

fn subscription(topic: &str) -> Result<Bytes, ClientError> {
    let mut out = BytesMut::new();
    out.put_i16(0);
    ConsumerProtocolSubscription::default()
        .with_topics(vec![s(topic)])
        .encode(&mut out, 0)
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
    Ok(out.freeze())
}

fn assignment(topic: &str, partitions: Vec<i32>) -> Result<Bytes, ClientError> {
    let mut out = BytesMut::new();
    out.put_i16(0);
    ConsumerProtocolAssignment::default()
        .with_assigned_partitions(vec![
            TopicPartition::default()
                .with_topic(TopicName(s(topic)))
                .with_partitions(partitions),
        ])
        .encode(&mut out, 0)
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
    Ok(out.freeze())
}

fn partitions_in(topic: &str, mut bytes: Bytes) -> Result<Vec<i32>, ClientError> {
    if bytes.remaining() < 2 {
        return Ok(Vec::new());
    }
    let version = bytes.get_i16().clamp(0, 3);
    let decoded = ConsumerProtocolAssignment::decode(&mut bytes, version)
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
    let mut partitions: Vec<i32> = decoded
        .assigned_partitions
        .into_iter()
        .filter(|tp| tp.topic.0.as_str() == topic)
        .flat_map(|tp| tp.partitions)
        .collect();
    partitions.sort_unstable();
    Ok(partitions)
}

/// The range assignment: members in name order, each a run of partitions, the first ones one more when they do not
/// divide evenly.
pub fn range_assignment(members: &[String], partitions: i32) -> Vec<(String, Vec<i32>)> {
    let mut sorted = members.to_vec();
    sorted.sort();
    let count = i32::try_from(sorted.len()).unwrap_or(i32::MAX).max(1);
    let (each, extra) = (partitions / count, partitions % count);
    let mut next = 0;
    sorted
        .into_iter()
        .enumerate()
        .map(|(at, member)| {
            let at = i32::try_from(at).unwrap_or(i32::MAX);
            let take = each + i32::from(at < extra);
            let assigned = (next..next + take).collect();
            next += take;
            (member, assigned)
        })
        .collect()
}

impl Client {
    /// Join `group` as a reader of `topic` (with `partitions` partitions), and learn this member's partitions. A
    /// member joining again after a rebalance passes its `previous` membership, so the group knows it is the same one.
    pub async fn join_group(
        &mut self,
        group: &str,
        topic: &str,
        partitions: i32,
        session_timeout_ms: i32,
        previous: Option<&Membership>,
    ) -> Result<Membership, ClientError> {
        let metadata = subscription(topic)?;
        let mut member_id = previous.map(|m| m.member_id.clone()).unwrap_or_default();
        // A join is held until every member has joined again, for up to the rebalance timeout.
        let wait =
            std::time::Duration::from_millis(u64::try_from(session_timeout_ms).unwrap_or(30_000) + 5_000);
        for _ in 0..10 {
            let join = JoinGroupRequest::default()
                .with_group_id(GroupId(s(group)))
                .with_session_timeout_ms(session_timeout_ms)
                .with_rebalance_timeout_ms(session_timeout_ms)
                .with_member_id(s(&member_id))
                .with_protocol_type(s("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(s(PROTOCOL))
                        .with_metadata(metadata.clone()),
                ]);
            let joined = self.send_waiting(&join, 9, wait).await?;
            if joined.error_code == UNKNOWN_MEMBER_ID && !member_id.is_empty() {
                // The group has forgotten this member (its session ended): join as a new one.
                member_id.clear();
                continue;
            }
            if joined.error_code == MEMBER_ID_REQUIRED {
                member_id = joined.member_id.to_string();
                continue;
            }
            if joined.error_code == REBALANCE_IN_PROGRESS {
                continue;
            }
            if joined.error_code != 0 {
                return Err(ClientError::Refused {
                    code: joined.error_code,
                    message: Some(format!("cannot join group {group:?}")),
                });
            }
            member_id = joined.member_id.to_string();
            // The leader decides who reads what; everyone else hears it in the sync.
            let assignments = if joined.leader.as_str() == member_id {
                let members: Vec<String> = joined.members.iter().map(|m| m.member_id.to_string()).collect();
                range_assignment(&members, partitions)
                    .into_iter()
                    .map(|(member, assigned)| {
                        assignment(topic, assigned).map(|bytes| {
                            SyncGroupRequestAssignment::default()
                                .with_member_id(s(&member))
                                .with_assignment(bytes)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
            let sync = SyncGroupRequest::default()
                .with_group_id(GroupId(s(group)))
                .with_generation_id(joined.generation_id)
                .with_member_id(s(&member_id))
                .with_protocol_type(Some(s("consumer")))
                .with_protocol_name(Some(s(PROTOCOL)))
                .with_assignments(assignments);
            let synced = self.send_waiting(&sync, 5, wait).await?;
            if synced.error_code == REBALANCE_IN_PROGRESS {
                continue;
            }
            if synced.error_code != 0 {
                return Err(ClientError::Refused {
                    code: synced.error_code,
                    message: Some(format!("cannot join group {group:?}")),
                });
            }
            return Ok(Membership {
                group: group.to_owned(),
                topic: topic.to_owned(),
                member_id,
                generation: joined.generation_id,
                partitions: partitions_in(topic, synced.assignment)?,
            });
        }
        Err(ClientError::Protocol(format!(
            "group {group:?} kept rebalancing; try again"
        )))
    }

    /// Tell the group this member is alive.
    pub async fn heartbeat(&mut self, member: &Membership) -> Result<GroupState, ClientError> {
        let request = HeartbeatRequest::default()
            .with_group_id(GroupId(s(&member.group)))
            .with_generation_id(member.generation)
            .with_member_id(s(&member.member_id));
        state(self.send(&request, 4).await?.error_code)
    }

    /// Commit this member's progress: for each partition, the offset of the next message it will read.
    pub async fn commit_member(
        &mut self,
        member: &Membership,
        offsets: &[(i32, i64)],
    ) -> Result<GroupState, ClientError> {
        let request = OffsetCommitRequest::default()
            .with_group_id(GroupId(s(&member.group)))
            .with_generation_id_or_member_epoch(member.generation)
            .with_member_id(s(&member.member_id))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(s(&member.topic)))
                    .with_partitions(
                        offsets
                            .iter()
                            .map(|(p, o)| {
                                OffsetCommitRequestPartition::default()
                                    .with_partition_index(*p)
                                    .with_committed_offset(*o)
                            })
                            .collect(),
                    ),
            ]);
        let response = self.send(&request, 8).await?;
        let code = response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| p.error_code)
            .find(|c| *c != 0);
        state(code.unwrap_or(0))
    }

    /// Leave the group, so the others take this member's partitions at once rather than after its session ends.
    pub async fn leave_group(&mut self, member: &Membership) -> Result<(), ClientError> {
        let request = LeaveGroupRequest::default()
            .with_group_id(GroupId(s(&member.group)))
            .with_members(vec![
                MemberIdentity::default().with_member_id(s(&member.member_id)),
            ]);
        self.send(&request, 5).await.map(|_| ())
    }
}

fn state(code: i16) -> Result<GroupState, ClientError> {
    match code {
        0 => Ok(GroupState::Stable),
        REBALANCE_IN_PROGRESS | ILLEGAL_GENERATION | UNKNOWN_MEMBER_ID => Ok(GroupState::Rebalancing),
        code => Err(ClientError::Refused { code, message: None }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_are_shared_in_runs() {
        let members = vec!["b".to_owned(), "a".to_owned(), "c".to_owned()];
        assert_eq!(
            range_assignment(&members, 7),
            vec![
                ("a".to_owned(), vec![0, 1, 2]),
                ("b".to_owned(), vec![3, 4]),
                ("c".to_owned(), vec![5, 6])
            ]
        );
        assert_eq!(
            range_assignment(&["x".to_owned(), "y".to_owned()], 1),
            vec![("x".to_owned(), vec![0]), ("y".to_owned(), vec![])]
        );
    }

    #[test]
    fn an_assignment_reads_back() {
        let bytes = assignment("orders", vec![2, 0]).unwrap();
        assert_eq!(partitions_in("orders", bytes).unwrap(), vec![0, 2]);
        assert_eq!(partitions_in("orders", Bytes::new()).unwrap(), Vec::<i32>::new());
    }
}
