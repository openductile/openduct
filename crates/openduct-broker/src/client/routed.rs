// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A client of a cluster: each request goes to the broker that answers for it - a partition's leader, a group's
//! coordinator - found in the cluster's metadata and found again when it has moved (a leader killed, a partition
//! moved, a cluster still forming). Against a single broker it is that broker for everything. The same calls as
//! [`Client`], so what OpenDuct runs inside itself (the studio, workflows) works on one broker or many alike.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{FindCoordinatorRequest, MetadataRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::task::JoinSet;

use super::{Client, ClientError, ClientSecurity, Fetched, GroupState, Membership, Outgoing, TopicSummary};

/// How many times a request is sent again after the broker that answers for it moved.
const TRIES: u32 = 8;

/// The refusals that mean "ask the broker that answers for it now": not the leader, no leader yet, a topic just
/// made, not the coordinator, no coordinator yet, the coordinator still loading, not the controller.
const MOVED: &[i16] = &[3, 5, 6, 14, 15, 16, 41];

/// A client of every broker of a cluster.
pub struct Routed {
    seed: String,
    timeout: Duration,
    security: ClientSecurity,
    first: Option<Client>,
    connections: HashMap<i32, Client>,
    brokers: HashMap<i32, String>,
    leaders: HashMap<(String, i32), i32>,
    coordinators: HashMap<String, i32>,
    controller: Option<i32>,
}

impl std::fmt::Debug for Routed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Routed")
            .field("seed", &self.seed)
            .field("brokers", &self.brokers)
            .finish_non_exhaustive()
    }
}

/// Whether `error` means the request belongs to another broker now (or the connection to it broke).
fn moved(error: &ClientError) -> bool {
    match error {
        ClientError::Refused { code, .. } => MOVED.contains(code),
        ClientError::Connect { .. } | ClientError::Io(_) | ClientError::Timeout(_) => true,
        ClientError::Protocol(_) => false,
    }
}

async fn pause(tries: u32) {
    tokio::time::sleep(Duration::from_millis(100 * u64::from(tries.min(10)))).await;
}

fn refused(code: i16, message: String) -> ClientError {
    ClientError::Refused {
        code,
        message: Some(message),
    }
}

/// Sends a request about a group to its coordinator, asking again where it moved.
macro_rules! at_coordinator {
    ($routed:ident, $group:expr, |$c:ident| $call:expr) => {{
        let group: &str = $group;
        let mut tries = 0;
        loop {
            let done = match $routed.coordinator(group).await {
                Ok(id) => match $routed.to(id).await {
                    Ok($c) => $call.await.map_err(|e| (Some(id), e)),
                    Err(e) => Err((Some(id), e)),
                },
                Err(e) => Err((None, e)),
            };
            match done {
                Err((at, e)) if tries < TRIES && moved(&e) => {
                    tries += 1;
                    $routed.coordinators.remove(group);
                    if let Some(id) = at {
                        $routed.forget(id, &e);
                    }
                    pause(tries).await;
                }
                Err((_, e)) => break Err(e),
                Ok(v) => break Ok(v),
            }
        }
    }};
}

impl Routed {
    /// A client of the cluster `addr` belongs to.
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, ClientError> {
        Self::connect_with(addr, timeout, &ClientSecurity::default()).await
    }

    /// A client of the cluster `addr` belongs to, with TLS and sign-in as `security` says.
    pub async fn connect_with(
        addr: &str,
        timeout: Duration,
        security: &ClientSecurity,
    ) -> Result<Self, ClientError> {
        let first = Client::connect_with(addr, timeout, security).await?;
        Ok(Self {
            seed: addr.to_owned(),
            timeout,
            security: security.clone(),
            first: Some(first),
            connections: HashMap::new(),
            brokers: HashMap::new(),
            leaders: HashMap::new(),
            coordinators: HashMap::new(),
            controller: None,
        })
    }

    /// A client of the cluster `addr` belongs to, connecting when first used.
    pub(super) fn unconnected(addr: &str, timeout: Duration, security: &ClientSecurity) -> Self {
        Self {
            seed: addr.to_owned(),
            timeout,
            security: security.clone(),
            first: None,
            connections: HashMap::new(),
            brokers: HashMap::new(),
            leaders: HashMap::new(),
            coordinators: HashMap::new(),
            controller: None,
        }
    }

    /// The connection to the broker first named, made again when it broke.
    async fn first(&mut self) -> Result<&mut Client, ClientError> {
        if self.first.is_none() {
            self.first = Some(Client::connect_with(&self.seed, self.timeout, &self.security).await?);
        }
        self.first
            .as_mut()
            .ok_or_else(|| ClientError::Protocol("no connection".to_owned()))
    }

    /// The cluster's brokers, controller and (for `topic`, or every topic) partition leaders, asked again.
    async fn learn(&mut self, topic: Option<&str>) -> Result<(), ClientError> {
        let topics = topic.map(|t| {
            vec![
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_string(t.to_owned())))),
            ]
        });
        let asked = self
            .first()
            .await?
            .send(&MetadataRequest::default().with_topics(topics), 12)
            .await;
        let answer = match asked {
            Ok(a) => a,
            Err(e) => {
                self.first = None;
                return Err(e);
            }
        };
        self.brokers = answer
            .brokers
            .iter()
            .map(|b| (b.node_id.0, format!("{}:{}", b.host, b.port)))
            .collect();
        self.controller = (answer.controller_id.0 >= 0).then_some(answer.controller_id.0);
        for t in &answer.topics {
            let Some(name) = &t.name else { continue };
            for p in &t.partitions {
                let key = (name.0.to_string(), p.partition_index);
                if p.error_code == 0 && p.leader_id.0 >= 0 {
                    self.leaders.insert(key, p.leader_id.0);
                } else {
                    self.leaders.remove(&key);
                }
            }
        }
        Ok(())
    }

    /// The connection to broker `id`.
    async fn to(&mut self, id: i32) -> Result<&mut Client, ClientError> {
        if !self.connections.contains_key(&id) {
            if !self.brokers.contains_key(&id) {
                self.learn(None).await?;
            }
            let addr = self
                .brokers
                .get(&id)
                .cloned()
                .ok_or_else(|| refused(5, format!("broker {id} is not in the cluster's metadata")))?;
            let client = Client::connect_with(&addr, self.timeout, &self.security).await?;
            self.connections.insert(id, client);
        }
        self.connections
            .get_mut(&id)
            .ok_or_else(|| ClientError::Protocol("no connection".to_owned()))
    }

    /// The broker leading `partition` of `topic`.
    async fn leader(&mut self, topic: &str, partition: i32) -> Result<i32, ClientError> {
        let key = (topic.to_owned(), partition);
        if let Some(id) = self.leaders.get(&key) {
            return Ok(*id);
        }
        self.learn(Some(topic)).await?;
        self.leaders
            .get(&key)
            .copied()
            .ok_or_else(|| refused(5, format!("partition {partition} of {topic} has no leader yet")))
    }

    /// The broker coordinating `group`.
    async fn coordinator(&mut self, group: &str) -> Result<i32, ClientError> {
        if let Some(id) = self.coordinators.get(group) {
            return Ok(*id);
        }
        let request = FindCoordinatorRequest::default()
            .with_key(StrBytes::from_string(group.to_owned()))
            .with_key_type(0);
        let asked = self.first().await?.send(&request, 3).await;
        let answer = match asked {
            Ok(a) => a,
            Err(e) => {
                self.first = None;
                return Err(e);
            }
        };
        if answer.error_code != 0 {
            return Err(refused(
                answer.error_code,
                format!("group {group} has no coordinator yet"),
            ));
        }
        let id = answer.node_id.0;
        self.brokers
            .insert(id, format!("{}:{}", answer.host, answer.port));
        self.coordinators.insert(group.to_owned(), id);
        Ok(id)
    }

    /// What is known of broker `id` let go, after it refused or its connection broke.
    fn forget(&mut self, id: i32, error: &ClientError) {
        if !matches!(error, ClientError::Refused { .. }) {
            self.connections.remove(&id);
        }
        self.leaders.retain(|_, leader| *leader != id);
        self.coordinators.retain(|_, c| *c != id);
    }

    /// Every topic and its partition count.
    pub async fn topics(&mut self) -> Result<Vec<TopicSummary>, ClientError> {
        let listed = self.first().await?.topics().await;
        if listed.is_err() {
            self.first = None;
        }
        listed
    }

    /// Creates a topic (asking the controller, when the broker first named is not it).
    pub async fn create_topic(
        &mut self,
        name: &str,
        partitions: i32,
        retention_ms: Option<i64>,
    ) -> Result<(), ClientError> {
        self.create_topic_with(name, partitions, retention_ms, &[]).await
    }

    /// As [`Routed::create_topic`], with the topic's other settings by Kafka's names (`cleanup.policy`, ...).
    pub async fn create_topic_with(
        &mut self,
        name: &str,
        partitions: i32,
        retention_ms: Option<i64>,
        settings: &[(&str, &str)],
    ) -> Result<(), ClientError> {
        let made = self
            .first()
            .await?
            .create_topic_with(name, partitions, retention_ms, settings)
            .await;
        match made {
            Err(ClientError::Refused { code: 41, .. }) => {
                self.learn(None).await?;
                let controller = self
                    .controller
                    .ok_or_else(|| refused(41, "the cluster has no controller yet".to_owned()))?;
                self.to(controller)
                    .await?
                    .create_topic_with(name, partitions, retention_ms, settings)
                    .await
            }
            other => other,
        }
    }

    /// Deletes a topic.
    pub async fn delete_topic(&mut self, name: &str) -> Result<(), ClientError> {
        self.first().await?.delete_topic(name).await
    }

    /// How many partitions `topic` has.
    pub async fn partition_count(&mut self, topic: &str) -> Result<i32, ClientError> {
        let counted = self.first().await?.partition_count(topic).await;
        if matches!(counted, Err(ref e) if !matches!(e, ClientError::Refused { .. })) {
            self.first = None;
        }
        counted
    }

    /// Writes `messages` to `partition` of `topic`, at its leader; the offset of the first.
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        messages: &[Outgoing],
        timestamp_ms: i64,
    ) -> Result<i64, ClientError> {
        let mut tries = 0;
        loop {
            let id = self.leader(topic, partition).await;
            let done = match id {
                Ok(id) => match self.to(id).await {
                    Ok(c) => c
                        .produce(topic, partition, messages, timestamp_ms)
                        .await
                        .map_err(|e| (Some(id), e)),
                    Err(e) => Err((Some(id), e)),
                },
                Err(e) => Err((None, e)),
            };
            match done {
                Err((at, e)) if tries < TRIES && moved(&e) => {
                    tries += 1;
                    if let Some(id) = at {
                        self.forget(id, &e);
                    }
                    pause(tries).await;
                }
                Err((_, e)) => return Err(e),
                Ok(offset) => return Ok(offset),
            }
        }
    }

    /// Writes to several partitions of any topics: one request to each leader (the leaders asked at once), each
    /// partition's messages as one batch. For each, in the order given, the first message's offset or why it was
    /// refused; one whose leader moved is written at its new one.
    pub async fn produce_many(
        &mut self,
        wanted: &[(String, i32, Vec<Outgoing>)],
        timestamp_ms: i64,
    ) -> Vec<Result<i64, ClientError>> {
        let mut out: Vec<Option<Result<i64, ClientError>>> = wanted.iter().map(|_| None).collect();
        let mut left: Vec<usize> = (0..wanted.len()).collect();
        let mut tries = 0;
        loop {
            let mut by_leader: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
            let mut again = Vec::new();
            for i in left {
                let Some((topic, partition, _)) = wanted.get(i) else {
                    continue;
                };
                match self.leader(topic, *partition).await {
                    Ok(id) => by_leader.entry(id).or_default().push(i),
                    Err(e) if tries >= TRIES => set(&mut out, i, Err(e)),
                    Err(_) => again.push(i),
                }
            }
            let mut asked = JoinSet::new();
            for (id, entries) in by_leader {
                let client = match self.to(id).await {
                    Ok(_) => self.connections.remove(&id),
                    Err(_) => None,
                };
                let Some(mut client) = client else {
                    self.forget(id, &ClientError::Protocol(String::new()));
                    again.extend(entries);
                    continue;
                };
                let batch: Vec<(String, i32, Vec<Outgoing>)> =
                    entries.iter().filter_map(|i| wanted.get(*i).cloned()).collect();
                asked.spawn(async move {
                    let written = client.produce_topics(&batch, timestamp_ms).await;
                    (id, client, entries, written)
                });
            }
            while let Some(joined) = asked.join_next().await {
                let Ok((id, client, entries, written)) = joined else {
                    continue;
                };
                match written {
                    Ok(results) => {
                        self.connections.insert(id, client);
                        for (i, result) in entries.into_iter().zip(results) {
                            match result {
                                Err(e) if tries < TRIES && moved(&e) => {
                                    if let Some((topic, partition, _)) = wanted.get(i) {
                                        self.leaders.remove(&(topic.clone(), *partition));
                                    }
                                    again.push(i);
                                }
                                other => set(&mut out, i, other),
                            }
                        }
                    }
                    Err(e) => {
                        if matches!(e, ClientError::Refused { .. }) {
                            self.connections.insert(id, client);
                        }
                        if !moved(&e) || tries >= TRIES {
                            for i in entries {
                                set(&mut out, i, Err(ClientError::Protocol(e.to_string())));
                            }
                        } else {
                            self.forget(id, &e);
                            again.extend(entries);
                        }
                    }
                }
            }
            if again.is_empty() {
                break;
            }
            tries += 1;
            pause(tries).await;
            left = again;
        }
        out.into_iter()
            .map(|r| r.unwrap_or_else(|| Err(ClientError::Protocol("no answer for the partition".into()))))
            .collect()
    }

    /// The offset at `at` (a time, or [`super::EARLIEST`] / [`super::LATEST`]) of a partition, at its leader.
    pub async fn list_offset(&mut self, topic: &str, partition: i32, at: i64) -> Result<i64, ClientError> {
        let mut tries = 0;
        loop {
            let done = match self.leader(topic, partition).await {
                Ok(id) => match self.to(id).await {
                    Ok(c) => c
                        .list_offset(topic, partition, at)
                        .await
                        .map_err(|e| (Some(id), e)),
                    Err(e) => Err((Some(id), e)),
                },
                Err(e) => Err((None, e)),
            };
            match done {
                Err((at_broker, e)) if tries < TRIES && moved(&e) => {
                    tries += 1;
                    if let Some(id) = at_broker {
                        self.forget(id, &e);
                    }
                    pause(tries).await;
                }
                Err((_, e)) => return Err(e),
                Ok(offset) => return Ok(offset),
            }
        }
    }

    /// Reads partitions of `topic` from the given offsets, each at its leader (the leaders asked at once). A
    /// partition whose leader moved is asked again at its new one.
    pub async fn fetch(
        &mut self,
        topic: &str,
        from: &[(i32, i64)],
        max_wait_ms: i32,
        max_bytes: i32,
    ) -> Result<Vec<Fetched>, ClientError> {
        let wanted: Vec<(String, i32, i64)> = from.iter().map(|&(p, o)| (topic.to_owned(), p, o)).collect();
        Ok(self
            .fetch_many(&wanted, max_wait_ms, max_bytes, max_bytes)
            .await?
            .into_iter()
            .map(|(_, f)| f)
            .collect())
    }

    /// Reads partitions of any topics from the given offsets: one request to each leader (the leaders asked at once),
    /// answered as soon as any of its partitions has records or `max_wait_ms` passes. Each partition gives at most
    /// `partition_max_bytes`, each leader's answer at most `max_bytes`. A partition whose leader moved is asked again
    /// at its new one.
    pub async fn fetch_many(
        &mut self,
        from: &[(String, i32, i64)],
        max_wait_ms: i32,
        partition_max_bytes: i32,
        max_bytes: i32,
    ) -> Result<Vec<(String, Fetched)>, ClientError> {
        self.fetch_routed(from, max_wait_ms, partition_max_bytes, max_bytes, false)
            .await
    }

    /// As [`Routed::fetch_many`], but a partition that has no leader for long (its topic deleted, say) comes back
    /// alone with its error code, the others answered: one reader's missing topic does not fail every reader's.
    pub async fn fetch_each(
        &mut self,
        from: &[(String, i32, i64)],
        max_wait_ms: i32,
        partition_max_bytes: i32,
        max_bytes: i32,
    ) -> Result<Vec<(String, Fetched)>, ClientError> {
        self.fetch_routed(from, max_wait_ms, partition_max_bytes, max_bytes, true)
            .await
    }

    async fn fetch_routed(
        &mut self,
        from: &[(String, i32, i64)],
        max_wait_ms: i32,
        partition_max_bytes: i32,
        max_bytes: i32,
        each: bool,
    ) -> Result<Vec<(String, Fetched)>, ClientError> {
        let mut answers = Vec::new();
        let mut wanted: Vec<(String, i32, i64)> = from.to_vec();
        let mut tries = 0;
        loop {
            let mut by_leader: BTreeMap<i32, Vec<(String, i32, i64)>> = BTreeMap::new();
            let mut unled = Vec::new();
            for (t, p, o) in wanted {
                match self.leader(&t, p).await {
                    Ok(id) => by_leader.entry(id).or_default().push((t, p, o)),
                    Err(_) => unled.push((t, p, o)),
                }
            }
            let mut asked = JoinSet::new();
            for (id, parts) in by_leader {
                let client = match self.to(id).await {
                    Ok(_) => self.connections.remove(&id),
                    Err(_) => None,
                };
                let Some(mut client) = client else {
                    self.forget(id, &ClientError::Protocol(String::new()));
                    unled.extend(parts);
                    continue;
                };
                // One entry per topic, in the order first asked.
                let mut topics: Vec<(String, Vec<(i32, i64)>)> = Vec::new();
                for (t, p, o) in &parts {
                    match topics.iter_mut().find(|(name, _)| name == t) {
                        Some((_, list)) => list.push((*p, *o)),
                        None => topics.push((t.clone(), vec![(*p, *o)])),
                    }
                }
                asked.spawn(async move {
                    let fetched = client
                        .fetch_topics(&topics, max_wait_ms, partition_max_bytes, max_bytes)
                        .await;
                    (id, client, parts, fetched)
                });
            }
            let mut again = unled;
            while let Some(joined) = asked.join_next().await {
                let Ok((id, client, parts, fetched)) = joined else {
                    continue;
                };
                match fetched {
                    Ok(list) => {
                        self.connections.insert(id, client);
                        for (topic, answer) in list {
                            if MOVED.contains(&answer.error_code) && tries < TRIES {
                                self.leaders.remove(&(topic.clone(), answer.partition));
                                if let Some(asked) = parts
                                    .iter()
                                    .find(|(t, p, _)| *t == topic && *p == answer.partition)
                                {
                                    again.push(asked.clone());
                                }
                            } else {
                                answers.push((topic, answer));
                            }
                        }
                    }
                    Err(e) => {
                        if matches!(e, ClientError::Refused { .. }) {
                            self.connections.insert(id, client);
                        }
                        if !moved(&e) || tries >= TRIES {
                            return Err(e);
                        }
                        self.forget(id, &e);
                        again.extend(parts);
                    }
                }
            }
            if again.is_empty() {
                return Ok(answers);
            }
            tries += 1;
            if tries > TRIES && each {
                answers.extend(again.into_iter().map(|(t, p, _)| {
                    (
                        t,
                        Fetched {
                            partition: p,
                            error_code: 5,
                            high_watermark: -1,
                            log_start: -1,
                            batches: bytes::Bytes::new(),
                        },
                    )
                }));
                return Ok(answers);
            }
            if tries > TRIES {
                let topic = again.first().map(|(t, _, _)| t.clone()).unwrap_or_default();
                return Err(refused(5, format!("{topic} has partitions with no leader")));
            }
            pause(tries).await;
            wanted = again;
        }
    }

    /// The offsets `group` committed for `partitions` of `topic`.
    pub async fn committed(
        &mut self,
        group: &str,
        topic: &str,
        partitions: &[i32],
    ) -> Result<Vec<(i32, i64)>, ClientError> {
        at_coordinator!(self, group, |c| c.committed(group, topic, partitions))
    }

    /// Commits `offsets` of `topic` for `group`.
    pub async fn commit(
        &mut self,
        group: &str,
        topic: &str,
        offsets: &[(i32, i64)],
    ) -> Result<(), ClientError> {
        at_coordinator!(self, group, |c| c.commit(group, topic, offsets))
    }

    /// The position a group saved for a partition.
    pub async fn saved_position(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<String>, ClientError> {
        at_coordinator!(self, group, |c| c.saved_position(group, topic, partition))
    }

    /// Saves a group's position for a partition.
    pub async fn save_position(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        position: &str,
    ) -> Result<(), ClientError> {
        at_coordinator!(self, group, |c| c
            .save_position(group, topic, partition, offset, position))
    }

    /// Joins `group` to read `topic` (or joins again, as `previous`).
    pub async fn join_group(
        &mut self,
        group: &str,
        topic: &str,
        partitions: i32,
        session_timeout_ms: i32,
        previous: Option<&Membership>,
    ) -> Result<Membership, ClientError> {
        at_coordinator!(self, group, |c| c.join_group(
            group,
            topic,
            partitions,
            session_timeout_ms,
            previous
        ))
    }

    /// Tells the group this member is alive.
    pub async fn heartbeat(&mut self, member: &Membership) -> Result<GroupState, ClientError> {
        at_coordinator!(self, &member.group, |c| c.heartbeat(member))
    }

    /// Commits this member's progress.
    pub async fn commit_member(
        &mut self,
        member: &Membership,
        offsets: &[(i32, i64)],
    ) -> Result<GroupState, ClientError> {
        at_coordinator!(self, &member.group, |c| c.commit_member(member, offsets))
    }

    /// Leaves the group.
    pub async fn leave_group(&mut self, member: &Membership) -> Result<(), ClientError> {
        at_coordinator!(self, &member.group, |c| c.leave_group(member))
    }
}

fn set(out: &mut [Option<Result<i64, ClientError>>], at: usize, result: Result<i64, ClientError>) {
    if let Some(slot) = out.get_mut(at) {
        *slot = Some(result);
    }
}
