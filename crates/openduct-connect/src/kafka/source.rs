// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: kafka:` - reading topics of another Kafka-protocol cluster as a member of a consumer group there, and
//! committing each partition's offset in that group only once what was read is stored in the pipeline's topic.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{BrokerId, FetchRequest, ListOffsetsRequest, TopicName};
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::records;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::cluster::{Cluster, text};
use super::group::{Ask, Group, Part, State};
use super::wire::Failure;
use crate::web::ATTEMPTS;
use crate::{SourceBatch, SourceMessage};

/// How long a fetch waits for something to arrive.
const FETCH_WAIT_MS: i32 = 500;
const FETCH_BYTES: i32 = 8 * 1024 * 1024;
const PARTITION_BYTES: i32 = 1024 * 1024;
/// How often what is stored is committed while reading.
const COMMIT_EVERY: Duration = Duration::from_secs(1);
const ASK_WAIT: Duration = Duration::from_secs(40);

const OFFSET_OUT_OF_RANGE: i16 = 1;
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;

/// The last message of a partition handed on in a batch: whether it was stored, and the offset to commit then.
type Pending = (oneshot::Receiver<Result<(), String>>, Part, i64);

#[derive(Debug)]
pub struct KafkaSource {
    pub(super) shown: String,
    pub(super) topics: Vec<String>,
    pub(super) group_name: String,
    /// Where a partition the group has never read starts: -2 for the earliest kept, -1 for what comes next.
    pub(super) start: i64,
    pub(super) cluster: Cluster,
    group: Option<Group>,
    /// The generation whose partitions are being read; -1 for none.
    generation: i32,
    /// The next offset to fetch, of each partition being read.
    positions: BTreeMap<Part, i64>,
    /// The offset to commit next, of each partition: everything before it is stored.
    stored: BTreeMap<Part, i64>,
    committed: BTreeMap<Part, i64>,
    pending: Vec<Pending>,
    last_commit: Instant,
    failures: u32,
}

impl KafkaSource {
    pub(super) fn new(
        shown: String,
        topics: Vec<String>,
        group_name: String,
        start: i64,
        cluster: Cluster,
    ) -> Self {
        Self {
            shown,
            topics,
            group_name,
            start,
            cluster,
            group: None,
            generation: -1,
            positions: BTreeMap::new(),
            stored: BTreeMap::new(),
            committed: BTreeMap::new(),
            pending: Vec::new(),
            last_commit: Instant::now(),
            failures: 0,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Reach the cluster, check the topics are there, and join the group.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let mut failures = 0;
        loop {
            match self.cluster.refresh(&self.topics).await {
                Ok(()) => break,
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(Failure::Passing(reason)) => {
                    failures += 1;
                    if failures >= ATTEMPTS {
                        return Err(reason);
                    }
                    tokio::time::sleep(backoff(failures)).await;
                }
            }
        }
        let own = Cluster::new(self.cluster.bootstrap().to_vec(), self.cluster.access().clone());
        self.group = Some(Group::start(own, self.group_name.clone(), self.topics.clone()));
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Result<SourceBatch, String> {
        self.settle().await;
        let state = match &self.group {
            Some(group) => group.state.borrow().clone(),
            None => return Err("the kafka source was not started".to_owned()),
        };
        match state {
            State::Failed(reason) => return Err(reason),
            State::Joining => return self.wait_for_group().await,
            State::Rebalancing { generation } => {
                if self.generation == generation {
                    self.commit().await?;
                    self.positions.clear();
                    self.generation = -1;
                    self.ask(Ask::Released).await;
                }
                return self.wait_for_group().await;
            }
            State::Member {
                generation,
                partitions,
            } => {
                if generation != self.generation {
                    self.begin(generation, partitions).await?;
                }
            }
        }
        if self.last_commit.elapsed() >= COMMIT_EVERY {
            self.commit().await?;
        }
        if self.positions.is_empty() {
            // A member with no partitions: more members than partitions.
            return self.wait_for_group().await;
        }
        self.fetch().await
    }

    /// Stop: commit what is stored and leave the group.
    pub(crate) async fn stop(&mut self) {
        self.settle().await;
        if let Err(reason) = self.commit().await {
            tracing::warn!(%reason, "the last commit to the Kafka group failed: what was not committed is read again");
        }
        if let Some(group) = self.group.take() {
            group.leave().await;
        }
    }

    async fn wait_for_group(&mut self) -> Result<SourceBatch, String> {
        if let Some(group) = self.group.as_mut() {
            let _ = tokio::time::timeout(Duration::from_millis(500), group.state.changed()).await;
        }
        Ok(SourceBatch::default())
    }

    /// What the runner has stored since the last call.
    async fn settle(&mut self) {
        for (ack, part, next) in std::mem::take(&mut self.pending) {
            if let Ok(Ok(())) = ack.await {
                let at = self.stored.entry(part).or_insert(next);
                *at = (*at).max(next);
            }
        }
    }

    async fn ask(&mut self, ask: Ask) {
        if let Some(group) = &self.group {
            let _ = group.asks.send(ask).await;
        }
    }

    /// Commit what is stored and not yet committed, on the partitions being read.
    async fn commit(&mut self) -> Result<(), String> {
        self.last_commit = Instant::now();
        let offsets: BTreeMap<Part, i64> = self
            .stored
            .iter()
            .filter(|(part, offset)| {
                self.positions.contains_key(*part) && self.committed.get(*part) != Some(offset)
            })
            .map(|(part, offset)| (part.clone(), *offset))
            .collect();
        if offsets.is_empty() || self.generation < 0 {
            return Ok(());
        }
        let (reply, answer) = oneshot::channel();
        self.ask(Ask::Commit {
            generation: self.generation,
            offsets: offsets.clone(),
            reply,
        })
        .await;
        match tokio::time::timeout(ASK_WAIT, answer).await {
            Ok(Ok(Ok(()))) => {
                self.committed.extend(offsets);
                Ok(())
            }
            Ok(Ok(Err(Failure::Lasting(reason)))) => Err(reason),
            Ok(Ok(Err(Failure::Passing(reason)))) => {
                tracing::warn!(%reason, "a commit to the Kafka group failed; it is tried again");
                Ok(())
            }
            Ok(Err(_)) | Err(_) => Ok(()),
        }
    }

    /// Start reading a generation's partitions: from the group's commits, or from `start` where there are none.
    async fn begin(&mut self, generation: i32, partitions: Vec<Part>) -> Result<(), String> {
        self.positions.clear();
        self.stored.clear();
        self.committed.clear();
        self.generation = generation;
        if partitions.is_empty() {
            return Ok(());
        }
        let (reply, answer) = oneshot::channel();
        self.ask(Ask::Committed {
            partitions: partitions.clone(),
            reply,
        })
        .await;
        let committed = match tokio::time::timeout(ASK_WAIT, answer).await {
            Ok(Ok(Ok(committed))) => committed,
            Ok(Ok(Err(Failure::Lasting(reason)))) => return Err(reason),
            Ok(Ok(Err(Failure::Passing(reason)))) => {
                tracing::warn!(%reason, "cannot read the Kafka group's offsets yet");
                self.generation = -1;
                return Ok(());
            }
            Ok(Err(_)) | Err(_) => {
                self.generation = -1;
                return Ok(());
            }
        };
        let mut fresh = Vec::new();
        for part in partitions {
            match committed.get(&part) {
                Some(offset) if *offset >= 0 => {
                    self.positions.insert(part.clone(), *offset);
                    self.committed.insert(part.clone(), *offset);
                    self.stored.insert(part, *offset);
                }
                _ => fresh.push(part),
            }
        }
        if !fresh.is_empty() {
            let start = self.start;
            match self.offsets(&fresh, start).await {
                Ok(found) => self.positions.extend(found),
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(Failure::Passing(reason)) => {
                    tracing::warn!(%reason, "cannot find where to start reading yet");
                    self.generation = -1;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// The earliest offset kept (`at` -2) or the next to come (`at` -1), of each partition.
    async fn offsets(&mut self, parts: &[Part], at: i64) -> Result<BTreeMap<Part, i64>, Failure> {
        let mut by_leader: BTreeMap<i32, BTreeMap<&str, Vec<i32>>> = BTreeMap::new();
        for (topic, partition) in parts {
            let leader = self.cluster.leader(topic, *partition).ok_or_else(|| {
                Failure::Passing(format!("partition {partition} of {topic} has no leader now"))
            })?;
            by_leader
                .entry(leader)
                .or_default()
                .entry(topic)
                .or_default()
                .push(*partition);
        }
        let mut out = BTreeMap::new();
        for (leader, topics) in by_leader {
            let request = ListOffsetsRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_topics(
                    topics
                        .into_iter()
                        .map(|(topic, partitions)| {
                            ListOffsetsTopic::default()
                                .with_name(TopicName(text(topic)))
                                .with_partitions(
                                    partitions
                                        .into_iter()
                                        .map(|p| {
                                            ListOffsetsPartition::default()
                                                .with_partition_index(p)
                                                .with_timestamp(at)
                                        })
                                        .collect(),
                                )
                        })
                        .collect(),
                );
            let answer = self.cluster.ask(leader, &request).await?;
            for topic in answer.topics {
                for p in topic.partitions {
                    if p.error_code != 0 {
                        let _ = self.cluster.refresh(&self.topics).await;
                        return Err(Failure::of(p.error_code, "finding where to read"));
                    }
                    out.insert((topic.name.0.to_string(), p.partition_index), p.offset);
                }
            }
        }
        Ok(out)
    }

    async fn fetch(&mut self) -> Result<SourceBatch, String> {
        let mut by_leader: BTreeMap<i32, BTreeMap<String, Vec<(i32, i64)>>> = BTreeMap::new();
        let mut leaderless = false;
        for ((topic, partition), offset) in &self.positions {
            match self.cluster.leader(topic, *partition) {
                Some(leader) => by_leader
                    .entry(leader)
                    .or_default()
                    .entry(topic.clone())
                    .or_default()
                    .push((*partition, *offset)),
                None => leaderless = true,
            }
        }
        let requests: BTreeMap<i32, FetchRequest> = by_leader
            .into_iter()
            .map(|(leader, topics)| (leader, fetch_request(topics)))
            .collect();
        let mut messages = Vec::new();
        let mut trouble: Option<Failure> = None;
        for (broker, answer) in self.cluster.ask_each(requests).await {
            let answer = match answer {
                Ok(answer) if answer.error_code == 0 => answer,
                Ok(answer) => {
                    trouble = Some(Failure::of(answer.error_code, "reading"));
                    continue;
                }
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(passing) => {
                    tracing::warn!(broker = %self.cluster.address(broker), reason = %passing.clone().reason(), "a fetch failed");
                    trouble = Some(passing);
                    continue;
                }
            };
            for topic in answer.responses {
                let name = topic.topic.0.to_string();
                for p in topic.partitions {
                    let part = (name.clone(), p.partition_index);
                    let Some(position) = self.positions.get(&part).copied() else {
                        continue;
                    };
                    match p.error_code {
                        0 => {
                            let (read, next) = decode(&part, position, p.records.unwrap_or_default())?;
                            if let Some(last) = read.last() {
                                let (ack, stored) = oneshot::channel();
                                self.pending.push((stored, part.clone(), last.offset + 1));
                                messages.extend(read.into_iter().map(|r| r.message));
                                if let Some(message) = messages.last_mut() {
                                    message.ack = Some(ack);
                                }
                            }
                            self.positions.insert(part, next);
                        }
                        OFFSET_OUT_OF_RANGE => self.reset(part, position).await?,
                        TOPIC_AUTHORIZATION_FAILED => {
                            return Err(format!(
                                "the Kafka cluster does not let {} read topic {name}: its ACLs need to allow it",
                                self.cluster.access().client_id
                            ));
                        }
                        code => match Failure::of(
                            code,
                            &format!("reading partition {} of {name}", p.partition_index),
                        ) {
                            Failure::Lasting(reason) => return Err(reason),
                            passing => trouble = Some(passing),
                        },
                    }
                }
            }
        }
        if trouble.is_some() || leaderless {
            self.failures += 1;
            if self.failures >= ATTEMPTS * 2 {
                return Err(trouble.map_or_else(|| "partitions have no leader".to_owned(), Failure::reason));
            }
            if let Err(Failure::Lasting(reason)) = self.cluster.refresh(&self.topics).await {
                return Err(reason);
            }
            if messages.is_empty() {
                tokio::time::sleep(backoff(self.failures)).await;
            }
        } else {
            self.failures = 0;
        }
        Ok(SourceBatch {
            messages,
            position: None,
        })
    }

    /// A position the partition no longer has: messages removed by the cluster's retention before they were read,
    /// or a position past its end.
    async fn reset(&mut self, part: Part, position: i64) -> Result<(), String> {
        let earliest = match self.offsets(std::slice::from_ref(&part), -2).await {
            Ok(found) => found.get(&part).copied(),
            Err(Failure::Lasting(reason)) => return Err(reason),
            Err(Failure::Passing(_)) => return Ok(()),
        };
        let Some(earliest) = earliest else {
            return Ok(());
        };
        let next = if position < earliest {
            tracing::warn!(
                topic = %part.0,
                partition = part.1,
                lost = earliest - position,
                "the Kafka cluster removed messages before they were read (its retention)"
            );
            earliest
        } else {
            match self.offsets(std::slice::from_ref(&part), -1).await {
                Ok(found) => found.get(&part).copied().unwrap_or(earliest),
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(Failure::Passing(_)) => return Ok(()),
            }
        };
        self.positions.insert(part, next);
        Ok(())
    }
}

fn backoff(failures: u32) -> Duration {
    (Duration::from_millis(500) * 2u32.saturating_pow(failures.saturating_sub(1)))
        .min(Duration::from_secs(10))
}

fn fetch_request(topics: BTreeMap<String, Vec<(i32, i64)>>) -> FetchRequest {
    FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(FETCH_WAIT_MS)
        .with_min_bytes(1)
        .with_max_bytes(FETCH_BYTES)
        .with_isolation_level(0)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(
            topics
                .into_iter()
                .map(|(topic, partitions)| {
                    FetchTopic::default()
                        .with_topic(TopicName(text(&topic)))
                        .with_partitions(
                            partitions
                                .into_iter()
                                .map(|(partition, offset)| {
                                    FetchPartition::default()
                                        .with_partition(partition)
                                        .with_fetch_offset(offset)
                                        .with_partition_max_bytes(PARTITION_BYTES)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
}

/// A message read, with its offset.
#[derive(Debug)]
struct Read {
    offset: i64,
    message: SourceMessage,
}

/// The messages in a partition's record batches from `position` on, and the position after the last whole batch.
/// A batch cut short at the end is fetched whole next time; transaction markers are passed over.
fn decode(part: &Part, mut position: i64, mut rest: Bytes) -> Result<(Vec<Read>, i64), String> {
    let (topic, partition) = part;
    let unreadable = |why: String| {
        format!("partition {partition} of {topic} holds a record batch that cannot be read: {why}")
    };
    let mut out = Vec::new();
    while let (Some(base), Some(length)) = (be_i64(&rest, 0), be_i32(&rest, 8)) {
        let length = usize::try_from(length).map_err(|_| unreadable("a negative length".to_owned()))?;
        let Some(total) = length.checked_add(12).filter(|t| *t <= rest.len()) else {
            break;
        };
        let mut batch = rest.split_to(total);
        let magic = batch.get(16).copied().unwrap_or_default();
        if magic != 2 {
            return Err(format!(
                "partition {partition} of {topic} holds messages in the format of Kafka before 0.11 (version \
                 {magic}), which OpenDuct does not read"
            ));
        }
        let last = be_i32(&batch, 23).ok_or_else(|| unreadable("a header cut short".to_owned()))?;
        let next = base
            .checked_add(i64::from(last))
            .and_then(|n| n.checked_add(1))
            .filter(|n| *n > base && last >= 0)
            .ok_or_else(|| unreadable("offsets that cannot be".to_owned()))?;
        if next <= position {
            continue;
        }
        let set = RecordBatchDecoder::decode_with_custom_compression(&mut batch, Some(records::decompress))
            .map_err(|e| unreadable(e.to_string()))?;
        for record in set.records {
            if record.offset < position || record.control {
                continue;
            }
            let mut headers: Vec<(String, Vec<u8>)> = record
                .headers
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.map(|v| v.to_vec()).unwrap_or_default()))
                .collect();
            headers.push(("kafka-topic".to_owned(), topic.clone().into_bytes()));
            headers.push(("kafka-partition".to_owned(), partition.to_string().into_bytes()));
            headers.push(("kafka-offset".to_owned(), record.offset.to_string().into_bytes()));
            out.push(Read {
                offset: record.offset,
                message: SourceMessage {
                    key: record.key.map(|k| k.to_vec()),
                    value: record.value.map(|v| v.to_vec()),
                    headers,
                    ack: None,
                },
            });
        }
        position = next;
    }
    Ok((out, position))
}

fn be_i64(bytes: &[u8], at: usize) -> Option<i64> {
    bytes
        .get(at..at + 8)
        .and_then(|b| b.try_into().ok())
        .map(i64::from_be_bytes)
}

fn be_i32(bytes: &[u8], at: usize) -> Option<i32> {
    bytes
        .get(at..at + 4)
        .and_then(|b| b.try_into().ok())
        .map(i32::from_be_bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    use super::*;

    fn batch(base: i64, values: &[Option<&str>], compression: Compression) -> BytesMut {
        let records: Vec<Record> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: base + i64::try_from(i).unwrap(),
                sequence: i32::try_from(i).unwrap() - 1,
                timestamp: 1_700_000_000_000,
                key: Some(Bytes::from(format!("k{i}"))),
                value: v.map(|v| Bytes::from(v.to_owned())),
                headers: IndexMap::from([("trace".into(), Some(Bytes::from_static(b"t-1")))]),
                delete_horizon: false,
            })
            .collect();
        let mut out = BytesMut::new();
        RecordBatchEncoder::encode_with_custom_compression(
            &mut out,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
            Some(records::compress),
        )
        .unwrap();
        out
    }

    #[test]
    fn batches_are_read_from_the_position_on() {
        let part = ("orders".to_owned(), 2);
        let mut bytes = batch(10, &[Some("a"), Some("b"), None], Compression::Lz4);
        bytes.extend_from_slice(&batch(13, &[Some("d")], Compression::Zstd));
        let (read, next) = decode(&part, 11, bytes.clone().freeze()).unwrap();
        assert_eq!(next, 14);
        let offsets: Vec<i64> = read.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![11, 12, 13]);
        let first = &read[0].message;
        assert_eq!(first.value.as_deref(), Some(&b"b"[..]));
        assert_eq!(first.key.as_deref(), Some(&b"k1"[..]));
        let header = |name: &str| {
            first
                .headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(header("trace"), Some(b"t-1".to_vec()));
        assert_eq!(header("kafka-topic"), Some(b"orders".to_vec()));
        assert_eq!(header("kafka-partition"), Some(b"2".to_vec()));
        assert_eq!(header("kafka-offset"), Some(b"11".to_vec()));
        // A deletion has no value.
        assert_eq!(read[1].message.value, None);

        // A batch cut short is left for the next fetch.
        let cut = bytes.len() - 5;
        let (read, next) = decode(&part, 10, bytes.clone().freeze().slice(..cut)).unwrap();
        assert_eq!((read.len(), next), (3, 13));
        // Nothing new: the position stays.
        assert_eq!(decode(&part, 14, bytes.freeze()).unwrap().1, 14);
    }

    #[test]
    fn a_batch_whose_last_records_were_compacted_away_moves_the_position_past_it() {
        let part = ("t".to_owned(), 0);
        let mut bytes = batch(100, &[Some("a"), Some("b")], Compression::None);
        // Say the batch ran to offset 109: records 102 to 109 were compacted away.
        bytes[23..27].copy_from_slice(&9i32.to_be_bytes());
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        let (read, next) = decode(&part, 100, bytes.freeze()).unwrap();
        assert_eq!((read.len(), next), (2, 110));
    }

    #[test]
    fn a_batch_claiming_more_records_than_it_holds_is_refused_at_once() {
        let part = ("t".to_owned(), 0);
        let mut bytes = batch(0, &[Some("a")], Compression::None);
        bytes[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        let started = std::time::Instant::now();
        assert!(decode(&part, 0, bytes.freeze()).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn batches_that_cannot_be_are_refused() {
        let part = ("t".to_owned(), 0);
        let mut old = batch(0, &[Some("a")], Compression::None);
        old[16] = 1;
        assert!(
            decode(&part, 0, old.freeze())
                .unwrap_err()
                .contains("before 0.11")
        );
        let mut corrupt = batch(0, &[Some("a")], Compression::None);
        let end = corrupt.len() - 1;
        corrupt[end] ^= 0xff;
        assert!(
            decode(&part, 0, corrupt.freeze())
                .unwrap_err()
                .contains("cannot be read")
        );
        let mut negative = batch(0, &[Some("a")], Compression::None);
        negative[8..12].copy_from_slice(&(-5i32).to_be_bytes());
        assert!(
            decode(&part, 0, negative.freeze())
                .unwrap_err()
                .contains("negative")
        );
        let mut backwards = batch(0, &[Some("a")], Compression::None);
        backwards[23..27].copy_from_slice(&(-3i32).to_be_bytes());
        assert!(
            decode(&part, 0, backwards.freeze())
                .unwrap_err()
                .contains("cannot be")
        );
    }
}
