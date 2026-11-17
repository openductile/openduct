// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Membership of a consumer group on the other cluster, run as a task of its own so it keeps the membership alive
//! however long a write takes. It joins with Kafka's standard consumer protocol and range assignment, so copies of
//! one pipeline share the partitions, and so does any other Kafka consumer in the same group. The reading side
//! asks it for committed offsets and to commit; when the group shares its partitions out again, the reading side
//! commits what is stored and lets them go before the task joins again.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, FindCoordinatorRequest, GroupId,
    HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest,
    SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use openduct_broker::client::range_assignment;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until};

use super::cluster::{Cluster, text};
use super::wire::{Connection, Failure};
use crate::web::ATTEMPTS;

/// How long the group waits without a heartbeat before it counts the member gone.
const SESSION_MS: i32 = 45_000;
/// How long the group waits for its members to join again when it shares its partitions out.
const REBALANCE_MS: i32 = 60_000;
const HEARTBEAT: Duration = Duration::from_secs(3);
/// How often the member that assigns partitions looks for topics that gained some.
const CHECK_PARTITIONS: Duration = Duration::from_secs(300);
const PROTOCOL: &str = "range";

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const ILLEGAL_GENERATION: i16 = 22;
const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const UNKNOWN_MEMBER_ID: i16 = 25;
const REBALANCE_IN_PROGRESS: i16 = 27;
const GROUP_AUTHORIZATION_FAILED: i16 = 30;
const MEMBER_ID_REQUIRED: i16 = 79;

/// A partition of a topic.
pub(super) type Part = (String, i32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum State {
    /// Joining, or joining again.
    Joining,
    /// A member, reading these partitions.
    Member { generation: i32, partitions: Vec<Part> },
    /// The group is sharing its partitions out again: commit what is stored, then let them go.
    Rebalancing { generation: i32 },
    /// Stopped for good, and why.
    Failed(String),
}

#[derive(Debug)]
pub(super) enum Ask {
    /// The offsets the group has committed; -1 where it has none.
    Committed {
        partitions: Vec<Part>,
        reply: oneshot::Sender<Result<BTreeMap<Part, i64>, Failure>>,
    },
    Commit {
        generation: i32,
        offsets: BTreeMap<Part, i64>,
        reply: oneshot::Sender<Result<(), Failure>>,
    },
    /// Everything read is committed and the partitions are let go: join again.
    Released,
}

#[derive(Debug)]
pub(super) struct Group {
    pub(super) asks: mpsc::Sender<Ask>,
    pub(super) state: watch::Receiver<State>,
    task: JoinHandle<()>,
}

impl Group {
    pub(super) fn start(cluster: Cluster, group: String, topics: Vec<String>) -> Self {
        let (asks, asked) = mpsc::channel(16);
        let (state_tx, state) = watch::channel(State::Joining);
        let member = Member {
            cluster,
            group,
            topics,
            member_id: String::new(),
            generation: -1,
            leader: false,
            assigned_counts: BTreeMap::new(),
            coordinator: None,
            state: state_tx,
        };
        let task = tokio::spawn(run(member, asked));
        Self { asks, state, task }
    }

    /// Leave the group and wait (briefly) for the task to end.
    pub(super) async fn leave(self) {
        drop(self.asks);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task).await;
    }
}

#[derive(Debug)]
struct Member {
    cluster: Cluster,
    group: String,
    topics: Vec<String>,
    member_id: String,
    generation: i32,
    leader: bool,
    /// The partition counts the last assignment was made with, when this member made it.
    assigned_counts: BTreeMap<String, usize>,
    coordinator: Option<Connection>,
    state: watch::Sender<State>,
}

async fn run(mut m: Member, mut asks: mpsc::Receiver<Ask>) {
    let mut failures = 0;
    loop {
        m.state.send_replace(State::Joining);
        match m.join().await {
            Ok(partitions) => {
                failures = 0;
                m.state.send_replace(State::Member {
                    generation: m.generation,
                    partitions,
                });
            }
            Err(Failure::Lasting(reason)) => {
                m.state.send_replace(State::Failed(reason));
                return;
            }
            Err(Failure::Passing(reason)) => {
                failures += 1;
                if failures >= ATTEMPTS {
                    m.state.send_replace(State::Failed(reason));
                    return;
                }
                tracing::warn!(group = %m.group, %reason, "cannot join the Kafka group yet; trying again");
                if !answer_until(&mut m, &mut asks, Instant::now() + backoff(failures)).await {
                    return;
                }
                continue;
            }
        }
        match m.serve(&mut asks).await {
            Serve::Rejoin => {}
            Serve::Stop => {
                m.leave().await;
                return;
            }
            Serve::Fail(reason) => {
                m.state.send_replace(State::Failed(reason));
                return;
            }
        }
    }
}

fn backoff(failures: u32) -> Duration {
    (Duration::from_millis(500) * 2u32.saturating_pow(failures.saturating_sub(1)))
        .min(Duration::from_secs(10))
}

enum Serve {
    Rejoin,
    Stop,
    Fail(String),
}

/// Answer asks until `deadline`; false when the reading side has gone.
async fn answer_until(m: &mut Member, asks: &mut mpsc::Receiver<Ask>, deadline: Instant) -> bool {
    loop {
        tokio::select! {
            () = sleep_until(deadline) => return true,
            ask = asks.recv() => match ask {
                None => return false,
                Some(ask) => {
                    m.answer(ask).await;
                }
            },
        }
    }
}

impl Member {
    /// Heartbeats and asks, until the group shares its partitions out again or the reading side stops.
    async fn serve(&mut self, asks: &mut mpsc::Receiver<Ask>) -> Serve {
        let mut beat = interval(HEARTBEAT);
        beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        beat.tick().await;
        let mut check = interval(CHECK_PARTITIONS);
        check.tick().await;
        let mut failures = 0;
        loop {
            let rejoin = tokio::select! {
                _ = beat.tick() => match self.heartbeat().await {
                    Ok(member) => {
                        failures = 0;
                        !member
                    }
                    Err(Failure::Lasting(reason)) => return Serve::Fail(reason),
                    Err(Failure::Passing(reason)) => {
                        failures += 1;
                        if failures >= ATTEMPTS {
                            return Serve::Fail(reason);
                        }
                        tracing::warn!(group = %self.group, %reason, "a heartbeat to the Kafka group failed");
                        false
                    }
                },
                _ = check.tick() => self.leader && self.partitions_grew().await,
                ask = asks.recv() => match ask {
                    None => return Serve::Stop,
                    Some(ask) => {
                        self.answer(ask).await;
                        false
                    }
                },
            };
            if rejoin {
                return self.hand_back(asks).await;
            }
        }
    }

    /// Tell the reading side to commit and let its partitions go; answer its last asks until it has.
    async fn hand_back(&mut self, asks: &mut mpsc::Receiver<Ask>) -> Serve {
        self.state.send_replace(State::Rebalancing {
            generation: self.generation,
        });
        let deadline =
            Instant::now() + Duration::from_millis(u64::try_from(REBALANCE_MS / 2).unwrap_or(30_000));
        loop {
            tokio::select! {
                () = sleep_until(deadline) => return Serve::Rejoin,
                ask = asks.recv() => match ask {
                    None => return Serve::Stop,
                    Some(Ask::Released) => return Serve::Rejoin,
                    Some(ask) => self.answer(ask).await,
                },
            }
        }
    }

    async fn answer(&mut self, ask: Ask) {
        match ask {
            Ask::Committed { partitions, reply } => {
                let _ = reply.send(self.committed(&partitions).await);
            }
            Ask::Commit {
                generation,
                offsets,
                reply,
            } => {
                let _ = reply.send(self.commit(generation, &offsets).await);
            }
            Ask::Released => {}
        }
    }

    fn coordinator_error(&mut self, code: i16, doing: &str) -> Failure {
        if matches!(
            code,
            COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
        ) {
            self.coordinator = None;
        }
        match code {
            GROUP_AUTHORIZATION_FAILED => Failure::Lasting(format!(
                "the Kafka cluster does not let {} use group {}: its ACLs need to allow it",
                self.cluster.access().client_id,
                self.group
            )),
            INCONSISTENT_GROUP_PROTOCOL => Failure::Lasting(format!(
                "group {} is in use by consumers that share partitions another way: give this pipeline a group of \
                 its own",
                self.group
            )),
            code => Failure::of(code, doing),
        }
    }

    /// The connection to the group's coordinator, found and opened when there is none.
    async fn coordinator(&mut self) -> Result<&mut Connection, Failure> {
        if self.coordinator.is_none() {
            let request = FindCoordinatorRequest::default()
                .with_key(text(&self.group))
                .with_key_type(0);
            let answer = self.cluster.ask_any(&request).await?;
            if answer.error_code != 0 {
                return Err(self.coordinator_error(answer.error_code, "finding the group's coordinator"));
            }
            let port = u16::try_from(answer.port)
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| {
                    Failure::Lasting("the Kafka cluster named a coordinator without a port".to_owned())
                })?;
            let access = self.cluster.access().clone();
            let conn = Connection::open(&answer.host, port, &access).await?;
            self.coordinator = Some(conn);
        }
        self.coordinator
            .as_mut()
            .ok_or_else(|| Failure::Passing("no coordinator".to_owned()))
    }

    /// Send to the coordinator; a connection that fails is dropped.
    async fn send<R: kafka_protocol::protocol::Request>(
        &mut self,
        request: &R,
        wait: Option<Duration>,
    ) -> Result<R::Response, Failure> {
        let conn = self.coordinator().await?;
        let answer = match wait {
            Some(wait) => conn.call_waiting(request, wait).await,
            None => conn.call(request).await,
        };
        if answer.is_err() {
            self.coordinator = None;
        }
        answer
    }

    /// Join, and the partitions this member is given.
    async fn join(&mut self) -> Result<Vec<Part>, Failure> {
        let wait = Duration::from_millis(u64::try_from(REBALANCE_MS).unwrap_or(60_000) + 10_000);
        for _ in 0..10 {
            let request = JoinGroupRequest::default()
                .with_group_id(GroupId(text(&self.group)))
                .with_session_timeout_ms(SESSION_MS)
                .with_rebalance_timeout_ms(REBALANCE_MS)
                .with_member_id(text(&self.member_id))
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str(PROTOCOL))
                        .with_metadata(subscription(&self.topics)?),
                ]);
            let joined = self.send(&request, Some(wait)).await?;
            match joined.error_code {
                0 => {}
                MEMBER_ID_REQUIRED => {
                    self.member_id = joined.member_id.to_string();
                    continue;
                }
                UNKNOWN_MEMBER_ID => {
                    self.member_id.clear();
                    continue;
                }
                REBALANCE_IN_PROGRESS => continue,
                code => return Err(self.coordinator_error(code, &format!("joining group {}", self.group))),
            }
            self.member_id = joined.member_id.to_string();
            self.leader = joined.leader == joined.member_id;
            let assignments = if self.leader {
                self.assign(&joined.members).await?
            } else {
                Vec::new()
            };
            let request = SyncGroupRequest::default()
                .with_group_id(GroupId(text(&self.group)))
                .with_generation_id(joined.generation_id)
                .with_member_id(text(&self.member_id))
                .with_assignments(assignments);
            let synced = self.send(&request, Some(wait)).await?;
            match synced.error_code {
                0 => {}
                REBALANCE_IN_PROGRESS | ILLEGAL_GENERATION => continue,
                UNKNOWN_MEMBER_ID => {
                    self.member_id.clear();
                    continue;
                }
                code => return Err(self.coordinator_error(code, &format!("joining group {}", self.group))),
            }
            self.generation = joined.generation_id;
            let partitions = assigned(synced.assignment, &self.topics);
            tracing::info!(group = %self.group, generation = self.generation, ?partitions, "joined the Kafka group");
            return Ok(partitions);
        }
        Err(Failure::Passing(format!(
            "group {} kept sharing its partitions out again",
            self.group
        )))
    }

    /// The leader's work: give each member a share of the partitions of the topics it reads.
    async fn assign(
        &mut self,
        members: &[kafka_protocol::messages::join_group_response::JoinGroupResponseMember],
    ) -> Result<Vec<SyncGroupRequestAssignment>, Failure> {
        let mut wanted: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for member in members {
            for topic in subscribed(member.metadata.clone()) {
                wanted
                    .entry(topic)
                    .or_default()
                    .push(member.member_id.to_string());
            }
        }
        let topics: Vec<String> = wanted.keys().cloned().collect();
        let counts = self.counts(&topics).await?;
        let mut shares: BTreeMap<String, Vec<(String, Vec<i32>)>> = BTreeMap::new();
        for (topic, readers) in &wanted {
            let Some(count) = counts.get(topic) else {
                continue;
            };
            let count = i32::try_from(*count).unwrap_or(i32::MAX);
            for (member, partitions) in range_assignment(readers, count) {
                shares
                    .entry(member)
                    .or_default()
                    .push((topic.clone(), partitions));
            }
        }
        self.assigned_counts = counts;
        members
            .iter()
            .map(|member| {
                let share = shares.remove(member.member_id.as_str()).unwrap_or_default();
                assignment(share).map(|bytes| {
                    SyncGroupRequestAssignment::default()
                        .with_member_id(member.member_id.clone())
                        .with_assignment(bytes)
                })
            })
            .collect()
    }

    /// How many partitions each topic has; a topic the cluster does not have is left out.
    async fn counts(&mut self, topics: &[String]) -> Result<BTreeMap<String, usize>, Failure> {
        let mut counts = BTreeMap::new();
        for topic in topics {
            match self.cluster.refresh(std::slice::from_ref(topic)).await {
                Ok(()) => {
                    counts.insert(topic.clone(), self.cluster.partitions(topic).len());
                }
                Err(Failure::Lasting(reason)) if self.topics.contains(topic) => {
                    return Err(Failure::Lasting(reason));
                }
                Err(Failure::Lasting(_)) => {}
                Err(passing) => return Err(passing),
            }
        }
        Ok(counts)
    }

    /// Whether a topic this member assigned has gained partitions since.
    async fn partitions_grew(&mut self) -> bool {
        let topics: Vec<String> = self.assigned_counts.keys().cloned().collect();
        match self.counts(&topics).await {
            Ok(now) => now
                .iter()
                .any(|(t, n)| self.assigned_counts.get(t).is_some_and(|before| n > before)),
            Err(_) => false,
        }
    }

    /// True while still a member of this generation.
    async fn heartbeat(&mut self) -> Result<bool, Failure> {
        let request = HeartbeatRequest::default()
            .with_group_id(GroupId(text(&self.group)))
            .with_generation_id(self.generation)
            .with_member_id(text(&self.member_id));
        let answer = self.send(&request, None).await?;
        match answer.error_code {
            0 => Ok(true),
            REBALANCE_IN_PROGRESS | ILLEGAL_GENERATION => Ok(false),
            UNKNOWN_MEMBER_ID => {
                self.member_id.clear();
                Ok(false)
            }
            code => Err(self.coordinator_error(code, "a heartbeat")),
        }
    }

    async fn committed(&mut self, partitions: &[Part]) -> Result<BTreeMap<Part, i64>, Failure> {
        let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
        for (topic, partition) in partitions {
            by_topic.entry(topic).or_default().push(*partition);
        }
        let request = OffsetFetchRequest::default()
            .with_group_id(GroupId(text(&self.group)))
            .with_topics(Some(
                by_topic
                    .into_iter()
                    .map(|(topic, partitions)| {
                        OffsetFetchRequestTopic::default()
                            .with_name(TopicName(text(topic)))
                            .with_partition_indexes(partitions)
                    })
                    .collect(),
            ));
        let answer = self.send(&request, None).await?;
        if answer.error_code != 0 {
            return Err(self.coordinator_error(answer.error_code, "reading the group's offsets"));
        }
        let mut out = BTreeMap::new();
        for topic in answer.topics {
            for p in topic.partitions {
                if p.error_code != 0 {
                    return Err(self.coordinator_error(p.error_code, "reading the group's offsets"));
                }
                out.insert((topic.name.0.to_string(), p.partition_index), p.committed_offset);
            }
        }
        Ok(out)
    }

    async fn commit(&mut self, generation: i32, offsets: &BTreeMap<Part, i64>) -> Result<(), Failure> {
        let mut by_topic: BTreeMap<&str, Vec<OffsetCommitRequestPartition>> = BTreeMap::new();
        for ((topic, partition), offset) in offsets {
            by_topic.entry(topic).or_default().push(
                OffsetCommitRequestPartition::default()
                    .with_partition_index(*partition)
                    .with_committed_offset(*offset),
            );
        }
        let request = OffsetCommitRequest::default()
            .with_group_id(GroupId(text(&self.group)))
            .with_generation_id_or_member_epoch(generation)
            .with_member_id(text(&self.member_id))
            .with_topics(
                by_topic
                    .into_iter()
                    .map(|(topic, partitions)| {
                        OffsetCommitRequestTopic::default()
                            .with_name(TopicName(text(topic)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );
        let answer = self.send(&request, None).await?;
        let code = answer
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| p.error_code)
            .find(|c| *c != 0);
        match code {
            None => Ok(()),
            Some(REBALANCE_IN_PROGRESS | ILLEGAL_GENERATION | UNKNOWN_MEMBER_ID) => Err(Failure::Passing(
                "the group shared its partitions out before the commit: what was not committed is read again"
                    .to_owned(),
            )),
            Some(code) => Err(self.coordinator_error(code, "committing the group's offsets")),
        }
    }

    async fn leave(&mut self) {
        if self.member_id.is_empty() || self.coordinator.is_none() {
            return;
        }
        let request = LeaveGroupRequest::default()
            .with_group_id(GroupId(text(&self.group)))
            .with_members(vec![
                MemberIdentity::default().with_member_id(text(&self.member_id)),
            ]);
        let _ = self.send(&request, Some(Duration::from_secs(5))).await;
    }
}

/// What a member says it reads, in the consumer protocol: a version, then the subscription.
fn subscription(topics: &[String]) -> Result<Bytes, Failure> {
    let mut out = BytesMut::new();
    out.put_i16(0);
    ConsumerProtocolSubscription::default()
        .with_topics(topics.iter().map(|t| text(t)).collect())
        .encode(&mut out, 0)
        .map_err(|e| Failure::Lasting(format!("cannot write the group subscription: {e}")))?;
    Ok(out.freeze())
}

/// The topics another member says it reads; nothing, for what cannot be read.
fn subscribed(mut bytes: Bytes) -> BTreeSet<String> {
    if bytes.remaining() < 2 {
        return BTreeSet::new();
    }
    let version = bytes.get_i16().clamp(0, 3);
    ConsumerProtocolSubscription::decode(&mut bytes, version)
        .map(|s| s.topics.into_iter().map(|t| t.to_string()).collect())
        .unwrap_or_default()
}

fn assignment(share: Vec<(String, Vec<i32>)>) -> Result<Bytes, Failure> {
    let mut out = BytesMut::new();
    out.put_i16(0);
    ConsumerProtocolAssignment::default()
        .with_assigned_partitions(
            share
                .into_iter()
                .map(|(topic, partitions)| {
                    TopicPartition::default()
                        .with_topic(TopicName(text(&topic)))
                        .with_partitions(partitions)
                })
                .collect(),
        )
        .encode(&mut out, 0)
        .map_err(|e| Failure::Lasting(format!("cannot write the group assignment: {e}")))?;
    Ok(out.freeze())
}

/// The partitions given to this member, of the topics it reads.
fn assigned(mut bytes: Bytes, topics: &[String]) -> Vec<Part> {
    if bytes.remaining() < 2 {
        return Vec::new();
    }
    let version = bytes.get_i16().clamp(0, 3);
    let Ok(decoded) = ConsumerProtocolAssignment::decode(&mut bytes, version) else {
        return Vec::new();
    };
    let mut out: Vec<Part> = decoded
        .assigned_partitions
        .into_iter()
        .filter(|tp| topics.iter().any(|t| t == tp.topic.0.as_str()))
        .flat_map(|tp| {
            let topic = tp.topic.0.to_string();
            tp.partitions.into_iter().map(move |p| (topic.clone(), p))
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn subscriptions_and_assignments_read_back() {
        let topics = vec!["orders".to_owned(), "payments".to_owned()];
        assert_eq!(
            subscribed(subscription(&topics).unwrap()),
            BTreeSet::from(["orders".to_owned(), "payments".to_owned()])
        );
        let bytes = assignment(vec![
            ("payments".to_owned(), vec![1]),
            ("orders".to_owned(), vec![2, 0]),
            ("other".to_owned(), vec![0]),
        ])
        .unwrap();
        assert_eq!(
            assigned(bytes, &topics),
            vec![
                ("orders".to_owned(), 0),
                ("orders".to_owned(), 2),
                ("payments".to_owned(), 1)
            ]
        );
        assert!(assigned(Bytes::from_static(b"\x00"), &topics).is_empty());
        assert!(subscribed(Bytes::from_static(b"\x00\x00\xff\xff\xff\xfe")).is_empty());
    }
}
