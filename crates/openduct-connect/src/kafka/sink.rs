// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `to: kafka:` - writing to a topic of another Kafka-protocol cluster. A message with a key goes to the partition
//! Kafka's own clients would choose for it, so it lands with what they write under that key; a batch counts as
//! stored once every replica the topic asks for has it (acks all). A message the topic will not take - larger than
//! it allows, or without the key a compacted topic needs - is set aside with the cluster's reason.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_response::PartitionProduceResponse;
use kafka_protocol::messages::{ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
use openduct_broker::client::records;

use super::cluster::{Cluster, text};
use super::wire::Failure;
use crate::web::ATTEMPTS;
use crate::{SinkOutcome, SinkRecord};

/// The most messages in one batch.
const BATCH_RECORDS: usize = 1_000;
/// About the most bytes in one batch, before compression; a cluster takes 1 MiB a batch unless told otherwise.
const BATCH_BYTES: usize = 512 * 1024;
const PRODUCE_TIMEOUT_MS: i32 = 30_000;

const MESSAGE_TOO_LARGE: i16 = 10;
const RECORD_LIST_TOO_LARGE: i16 = 18;
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const INVALID_RECORD: i16 = 87;

#[derive(Debug)]
pub struct KafkaSink {
    pub(super) shown: String,
    pub(super) topic: String,
    pub(super) compression: Compression,
    pub(super) cluster: Cluster,
    /// Where messages without a key go next, in turns.
    turn: usize,
}

/// Messages bound for one partition, in order: their places in the records being sent.
#[derive(Debug, Clone)]
struct Chunk {
    partition: i32,
    records: Vec<usize>,
}

impl KafkaSink {
    pub(super) fn new(shown: String, topic: String, compression: Compression, cluster: Cluster) -> Self {
        Self {
            shown,
            topic,
            compression,
            cluster,
            turn: 0,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        if records.is_empty() {
            return Ok(SinkOutcome {
                durable: through,
                rejected: Vec::new(),
            });
        }
        let mut failures = 0;
        let partitions = loop {
            let partitions = self.cluster.partitions(&self.topic);
            if !partitions.is_empty() {
                break partitions;
            }
            match self.cluster.refresh(std::slice::from_ref(&self.topic)).await {
                Ok(()) => {}
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(Failure::Passing(reason)) => {
                    failures += 1;
                    if failures >= ATTEMPTS {
                        return Err(reason);
                    }
                    tokio::time::sleep(backoff(failures)).await;
                }
            }
        };
        let count = i32::try_from(partitions.len()).unwrap_or(i32::MAX);
        let turn = i32::try_from(self.turn % partitions.len()).unwrap_or(0);
        self.turn = self.turn.wrapping_add(1);
        let mut by_partition: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for (at, record) in records.iter().enumerate() {
            let partition = record
                .key
                .as_ref()
                .map_or(turn, |k| records::partition_for(k.as_bytes(), count));
            by_partition.entry(partition).or_default().push(at);
        }
        let mut queue: BTreeMap<i32, Vec<Chunk>> = BTreeMap::new();
        for (partition, places) in by_partition {
            queue.insert(partition, chunks(partition, &places, &records));
        }
        let mut rejected: Vec<(usize, String)> = Vec::new();
        let mut failures = 0;
        while queue.values().any(|c| !c.is_empty()) {
            match self.round(&mut queue, &records, &mut rejected).await {
                Ok(()) => failures = 0,
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(Failure::Passing(reason)) => {
                    failures += 1;
                    if failures >= ATTEMPTS {
                        return Err(reason);
                    }
                    tracing::warn!(%reason, topic = %self.topic, "writing to the Kafka cluster failed; trying again");
                    if let Err(Failure::Lasting(reason)) =
                        self.cluster.refresh(std::slice::from_ref(&self.topic)).await
                    {
                        return Err(reason);
                    }
                    tokio::time::sleep(backoff(failures)).await;
                }
            }
        }
        let mut records: Vec<Option<SinkRecord>> = records.into_iter().map(Some).collect();
        let rejected = rejected
            .into_iter()
            .filter_map(|(at, why)| records.get_mut(at).and_then(Option::take).map(|r| (r, why)))
            .collect();
        Ok(SinkOutcome {
            durable: through,
            rejected,
        })
    }

    /// Send the first chunk waiting for each partition, one request to each leader. What could not go stays queued;
    /// the failure that may pass is returned once the rest is done.
    async fn round(
        &mut self,
        queue: &mut BTreeMap<i32, Vec<Chunk>>,
        records: &[SinkRecord],
        rejected: &mut Vec<(usize, String)>,
    ) -> Result<(), Failure> {
        let mut requests: BTreeMap<i32, Vec<(i32, Bytes)>> = BTreeMap::new();
        let mut trouble = None;
        for (partition, chunks) in queue.iter() {
            let Some(chunk) = chunks.first() else {
                continue;
            };
            let Some(leader) = self.cluster.leader(&self.topic, *partition) else {
                trouble = Some(Failure::Passing(format!(
                    "partition {partition} of {} has no leader now",
                    self.topic
                )));
                continue;
            };
            let batch = encode(chunk, records, self.compression)?;
            requests.entry(leader).or_default().push((*partition, batch));
        }
        let requests: BTreeMap<i32, ProduceRequest> = requests
            .into_iter()
            .map(|(leader, batches)| (leader, produce_request(&self.topic, batches)))
            .collect();
        for (_, answer) in self.cluster.ask_each(requests).await {
            let answer = match answer {
                Ok(answer) => answer,
                Err(lasting @ Failure::Lasting(_)) => return Err(lasting),
                Err(passing) => {
                    trouble = Some(passing);
                    continue;
                }
            };
            for p in answer.responses.into_iter().flat_map(|t| t.partition_responses) {
                if let Some(failure) = self.settle(queue, rejected, &p)? {
                    trouble = Some(failure);
                }
            }
        }
        trouble.map_or(Ok(()), Err)
    }

    /// What one partition answered: its chunk done, split, partly set aside, or kept for another try.
    fn settle(
        &self,
        queue: &mut BTreeMap<i32, Vec<Chunk>>,
        rejected: &mut Vec<(usize, String)>,
        p: &PartitionProduceResponse,
    ) -> Result<Option<Failure>, Failure> {
        let Some(chunks) = queue.get_mut(&p.index) else {
            return Ok(None);
        };
        if chunks.is_empty() {
            return Ok(None);
        }
        let reason = || {
            p.error_message
                .as_ref()
                .map(ToString::to_string)
                .filter(|m| !m.is_empty())
        };
        match p.error_code {
            0 => {
                chunks.remove(0);
            }
            MESSAGE_TOO_LARGE | RECORD_LIST_TOO_LARGE => {
                let chunk = chunks.remove(0);
                if chunk.records.len() == 1 {
                    rejected.extend(chunk.records.iter().map(|at| {
                        (
                            *at,
                            format!(
                                "the message is larger than Kafka topic {} takes (its max.message.bytes)",
                                self.topic
                            ),
                        )
                    }));
                } else {
                    let (first, second) = chunk.records.split_at(chunk.records.len() / 2);
                    for half in [second, first] {
                        chunks.insert(
                            0,
                            Chunk {
                                partition: chunk.partition,
                                records: half.to_vec(),
                            },
                        );
                    }
                }
            }
            INVALID_RECORD => {
                let chunk = chunks.remove(0);
                let mut bad: BTreeMap<usize, String> = p
                    .record_errors
                    .iter()
                    .filter_map(|e| {
                        let at = usize::try_from(e.batch_index).ok()?;
                        let why = e
                            .batch_index_error_message
                            .as_ref()
                            .map(ToString::to_string)
                            .or_else(reason)
                            .unwrap_or_else(|| "the Kafka cluster refused it".to_owned());
                        Some((at, why))
                    })
                    .collect();
                if bad.is_empty() {
                    let why = reason().unwrap_or_else(|| "the Kafka cluster refused the batch".to_owned());
                    bad = (0..chunk.records.len()).map(|i| (i, why.clone())).collect();
                }
                let mut keep = Vec::new();
                for (i, at) in chunk.records.iter().enumerate() {
                    match bad.remove(&i) {
                        Some(why) => rejected.push((
                            *at,
                            format!("Kafka topic {} refused the message: {why}", self.topic),
                        )),
                        None => keep.push(*at),
                    }
                }
                if !keep.is_empty() {
                    chunks.insert(
                        0,
                        Chunk {
                            partition: chunk.partition,
                            records: keep,
                        },
                    );
                }
            }
            TOPIC_AUTHORIZATION_FAILED => {
                return Err(Failure::Lasting(format!(
                    "the Kafka cluster does not let {} write to topic {}: its ACLs need to allow it",
                    self.cluster.access().client_id,
                    self.topic
                )));
            }
            code => {
                let doing = format!("writing to partition {} of {}", p.index, self.topic);
                let failure = Failure::of(code, &doing);
                if let Failure::Lasting(r) = failure {
                    return Err(Failure::Lasting(
                        reason().map_or(r.clone(), |m| format!("{r}: {m}")),
                    ));
                }
                return Ok(Some(failure));
            }
        }
        Ok(None)
    }
}

fn backoff(failures: u32) -> Duration {
    (Duration::from_millis(500) * 2u32.saturating_pow(failures.saturating_sub(1)))
        .min(Duration::from_secs(10))
}

/// A partition's messages cut into batches the cluster takes.
fn chunks(partition: i32, places: &[usize], records: &[SinkRecord]) -> Vec<Chunk> {
    let mut out = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut bytes = 0;
    for &at in places {
        let size = records.get(at).map_or(0, |r| {
            r.value.len()
                + r.key.as_ref().map_or(0, String::len)
                + r.headers.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>()
                + 32
        });
        if !current.is_empty() && (current.len() >= BATCH_RECORDS || bytes + size > BATCH_BYTES) {
            out.push(Chunk {
                partition,
                records: std::mem::take(&mut current),
            });
            bytes = 0;
        }
        current.push(at);
        bytes += size;
    }
    if !current.is_empty() {
        out.push(Chunk {
            partition,
            records: current,
        });
    }
    out
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// One chunk as a record batch.
fn encode(chunk: &Chunk, records: &[SinkRecord], compression: Compression) -> Result<Bytes, Failure> {
    let now = now_ms();
    let batch: Vec<Record> = chunk
        .records
        .iter()
        .filter_map(|at| records.get(*at))
        .enumerate()
        .map(|(i, r)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(i).unwrap_or(i64::MAX),
            // No producer id, so the batch's sequence is -1; stepping with the offset keeps the records in one batch.
            sequence: i32::try_from(i).unwrap_or(i32::MAX).wrapping_sub(1),
            timestamp: now,
            key: r.key.clone().map(|k| Bytes::from(k.into_bytes())),
            value: Some(Bytes::from(r.value.clone())),
            headers: r
                .headers
                .iter()
                .map(|(n, v)| (StrBytes::from_string(n.clone()), Some(Bytes::from(v.clone()))))
                .collect::<IndexMap<_, _>>(),
            delete_horizon: false,
        })
        .collect();
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut out,
        &batch,
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
        Some(records::compress),
    )
    .map_err(|e| Failure::Lasting(format!("cannot write a record batch: {e}")))?;
    Ok(out.freeze())
}

fn produce_request(topic: &str, batches: Vec<(i32, Bytes)>) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(PRODUCE_TIMEOUT_MS)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(text(topic)))
                .with_partition_data(
                    batches
                        .into_iter()
                        .map(|(partition, batch)| {
                            PartitionProduceData::default()
                                .with_index(partition)
                                .with_records(Some(batch))
                        })
                        .collect(),
                ),
        ])
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn record(value: usize) -> SinkRecord {
        SinkRecord {
            key: None,
            value: vec![b'x'; value],
            headers: Vec::new(),
            partition: 0,
            offset: 0,
        }
    }

    #[test]
    fn a_chunk_is_one_batch_without_a_producer_id() {
        let records: Vec<SinkRecord> = (0..3).map(|_| record(10)).collect();
        let chunk = Chunk {
            partition: 0,
            records: vec![0, 1, 2],
        };
        for compression in [Compression::None, Compression::Lz4] {
            let mut bytes = encode(&chunk, &records, compression).unwrap();
            let batches = kafka_protocol::records::RecordBatchDecoder::decode_batch_info(&mut bytes).unwrap();
            assert_eq!(batches.len(), 1, "{compression:?}");
            let batch = batches.first().unwrap();
            assert_eq!(
                (batch.record_count, batch.base_sequence, batch.producer_id),
                (3, -1, -1)
            );
        }
    }

    #[test]
    fn batches_stay_within_what_a_cluster_takes() {
        let records: Vec<SinkRecord> = (0..2_500).map(|_| record(10)).collect();
        let places: Vec<usize> = (0..records.len()).collect();
        let sizes: Vec<usize> = chunks(0, &places, &records)
            .iter()
            .map(|c| c.records.len())
            .collect();
        assert_eq!(sizes, vec![1_000, 1_000, 500]);

        let big: Vec<SinkRecord> = vec![record(300_000), record(300_000), record(2_000_000), record(10)];
        let sizes: Vec<Vec<usize>> = chunks(0, &[0, 1, 2, 3], &big)
            .into_iter()
            .map(|c| c.records)
            .collect();
        // A message larger than a batch goes alone; the cluster says whether it takes it.
        assert_eq!(sizes, vec![vec![0], vec![1], vec![2], vec![3]]);
    }
}
