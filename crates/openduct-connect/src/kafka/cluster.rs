// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Kafka-protocol cluster: where its brokers are, which of them leads each partition of the topics in use, and a
//! connection to each broker, opened when it is first needed and dropped when it fails.

use std::collections::BTreeMap;

use futures_util::future::join_all;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{MetadataRequest, TopicName};
use kafka_protocol::protocol::{Request, StrBytes};

use super::wire::{Access, Connection, Failure};

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// The most partitions one topic is taken to have: more is not an answer from a real cluster.
const MAX_PARTITIONS: i32 = 100_000;

pub(super) fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_owned())
}

#[derive(Debug)]
pub(super) struct Cluster {
    bootstrap: Vec<(String, u16)>,
    access: Access,
    brokers: BTreeMap<i32, (String, u16)>,
    connections: BTreeMap<i32, Connection>,
    /// A connection to a bootstrap address, for questions any broker answers.
    seed: Option<Connection>,
    /// Each topic's partitions and the broker leading each (-1 while none does).
    leaders: BTreeMap<String, BTreeMap<i32, i32>>,
}

impl Cluster {
    pub(super) fn new(bootstrap: Vec<(String, u16)>, access: Access) -> Self {
        Self {
            bootstrap,
            access,
            brokers: BTreeMap::new(),
            connections: BTreeMap::new(),
            seed: None,
            leaders: BTreeMap::new(),
        }
    }

    pub(super) fn access(&self) -> &Access {
        &self.access
    }

    pub(super) fn bootstrap(&self) -> &[(String, u16)] {
        &self.bootstrap
    }

    /// Ask any broker: one already connected, or each bootstrap address in turn.
    pub(super) async fn ask_any<R: Request>(&mut self, request: &R) -> Result<R::Response, Failure> {
        let open = self.connections.keys().next().copied();
        if let Some(id) = open {
            return self.ask(id, request).await;
        }
        if self.seed.is_none() {
            let mut last = Failure::Passing("no brokers are given".to_owned());
            let (bootstrap, access) = (self.bootstrap.clone(), self.access.clone());
            for (host, port) in &bootstrap {
                match Connection::open(host, *port, &access).await {
                    Ok(conn) => {
                        self.seed = Some(conn);
                        break;
                    }
                    Err(lasting @ Failure::Lasting(_)) => return Err(lasting),
                    Err(passing) => last = passing,
                }
            }
            if self.seed.is_none() {
                return Err(last);
            }
        }
        let Some(seed) = self.seed.as_mut() else {
            return Err(Failure::Passing("no broker could be reached".to_owned()));
        };
        let answer = seed.call(request).await;
        if answer.is_err() {
            self.seed = None;
        }
        answer
    }

    /// Ask one broker, connecting to it first if need be; a connection that fails is dropped.
    pub(super) async fn ask<R: Request>(&mut self, broker: i32, request: &R) -> Result<R::Response, Failure> {
        let conn = self.connection(broker).await?;
        let answer = conn.call(request).await;
        if answer.is_err() {
            self.connections.remove(&broker);
        }
        answer
    }

    /// Ask several brokers at once, one request each.
    pub(super) async fn ask_each<R: Request>(
        &mut self,
        requests: BTreeMap<i32, R>,
    ) -> Vec<(i32, Result<R::Response, Failure>)> {
        let mut out = Vec::new();
        for broker in requests.keys() {
            if let Err(failure) = self.connection(*broker).await {
                out.push((*broker, Err(failure)));
            }
        }
        let calls = self.connections.iter_mut().filter_map(|(id, conn)| {
            requests
                .get(id)
                .map(|request| async move { (*id, conn.call(request).await) })
        });
        out.extend(join_all(calls).await);
        for (broker, answer) in &out {
            if answer.is_err() {
                self.connections.remove(broker);
            }
        }
        out
    }

    async fn connection(&mut self, broker: i32) -> Result<&mut Connection, Failure> {
        if !self.connections.contains_key(&broker) {
            let (host, port) = self.brokers.get(&broker).cloned().ok_or_else(|| {
                Failure::Passing(format!("broker {broker} is not one the cluster has named"))
            })?;
            let access = self.access.clone();
            let conn = Connection::open(&host, port, &access).await?;
            self.connections.insert(broker, conn);
        }
        self.connections
            .get_mut(&broker)
            .ok_or_else(|| Failure::Passing(format!("no connection to broker {broker}")))
    }

    /// Learn where the brokers are and which leads each partition of `topics`. A topic the cluster does not have,
    /// or does not let this client use, is a failure that lasts.
    pub(super) async fn refresh(&mut self, topics: &[String]) -> Result<(), Failure> {
        let request = MetadataRequest::default()
            .with_topics(Some(
                topics
                    .iter()
                    .map(|t| MetadataRequestTopic::default().with_name(Some(TopicName(text(t)))))
                    .collect(),
            ))
            .with_allow_auto_topic_creation(false);
        let answer = self.ask_any(&request).await?;
        let brokers: BTreeMap<i32, (String, u16)> = answer
            .brokers
            .iter()
            .filter_map(|b| {
                let port = u16::try_from(b.port).ok().filter(|p| *p > 0)?;
                Some((b.node_id.0, (b.host.to_string(), port)))
            })
            .collect();
        // A broker that moved or left is reached afresh.
        self.connections
            .retain(|id, _| brokers.get(id) == self.brokers.get(id) && brokers.contains_key(id));
        self.brokers = brokers;
        for topic in answer.topics {
            let name = topic.name.map(|n| n.0.to_string()).unwrap_or_default();
            match topic.error_code {
                0 => {}
                UNKNOWN_TOPIC_OR_PARTITION => {
                    return Err(Failure::Lasting(format!(
                        "the Kafka cluster has no topic {name} (create it there first)"
                    )));
                }
                TOPIC_AUTHORIZATION_FAILED => {
                    return Err(Failure::Lasting(format!(
                        "the Kafka cluster does not let {} use topic {name}: its ACLs need to allow it",
                        self.access.client_id
                    )));
                }
                code => return Err(Failure::of(code, &format!("finding topic {name}"))),
            }
            if topic.partitions.len() > usize::try_from(MAX_PARTITIONS).unwrap_or(usize::MAX)
                || topic
                    .partitions
                    .iter()
                    .any(|p| !(0..MAX_PARTITIONS).contains(&p.partition_index))
            {
                return Err(Failure::Lasting(format!(
                    "the Kafka cluster described topic {name} with partitions that cannot be"
                )));
            }
            let leaders = topic
                .partitions
                .iter()
                .map(|p| {
                    let leader = if p.error_code == 0 { p.leader_id.0 } else { -1 };
                    (p.partition_index, leader)
                })
                .collect();
            self.leaders.insert(name, leaders);
        }
        Ok(())
    }

    /// The partitions of `topic`, as last learnt.
    pub(super) fn partitions(&self, topic: &str) -> Vec<i32> {
        self.leaders
            .get(topic)
            .map(|p| p.keys().copied().collect())
            .unwrap_or_default()
    }

    /// The broker leading a partition, when one does.
    pub(super) fn leader(&self, topic: &str, partition: i32) -> Option<i32> {
        self.leaders
            .get(topic)
            .and_then(|p| p.get(&partition))
            .copied()
            .filter(|l| *l >= 0)
    }

    /// Where a broker is, for messages.
    pub(super) fn address(&self, broker: i32) -> String {
        self.brokers
            .get(&broker)
            .map_or_else(|| format!("broker {broker}"), |(h, p)| format!("{h}:{p}"))
    }
}
