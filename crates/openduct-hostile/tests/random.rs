// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Every hostile-input target, fed valid inputs mutated at random - bits flipped, bytes changed to the values that
//! break length checks, ranges cut, repeated or spliced from another input - with a fixed seed, so a failure comes
//! back on every run. A failure prints the input, in hex, to replay it.
//!
//! `OPENDUCT_HOSTILE_ROUNDS=<n>` multiplies the number of inputs (default 1) for a longer run by hand.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::Path;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{Encodable, StrBytes};
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

/// SplitMix64: small, fast, and the same everywhere.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n.max(1)).unwrap()).unwrap()
    }
}

/// Values that break length and count checks: zero, one, the top and bottom of each width, and a sign bit.
const INTERESTING: [i64; 10] = [
    0,
    1,
    -1,
    0x7F,
    0x80,
    0xFF,
    0x7FFF,
    0xFFFF,
    0x7FFF_FFFF,
    -0x8000_0000,
];

fn mutate(r: &mut Random, seeds: &[Vec<u8>]) -> Vec<u8> {
    let mut data = seeds[r.below(seeds.len())].clone();
    for _ in 0..=r.below(4) {
        let at = r.below(data.len());
        match r.below(8) {
            0 if !data.is_empty() => data[at] ^= 1 << r.below(8),
            1 if !data.is_empty() => data[at] = INTERESTING[r.below(INTERESTING.len())].to_be_bytes()[7],
            2 => {
                // A two- or four-byte field set to an interesting value, as lengths and counts are.
                let width = if r.below(2) == 0 { 2 } else { 4 };
                let value = INTERESTING[r.below(INTERESTING.len())].to_be_bytes();
                for (i, b) in value[8 - width..].iter().enumerate() {
                    if let Some(slot) = data.get_mut(at + i) {
                        *slot = *b;
                    }
                }
            }
            3 => {
                let n = r.below(16) + 1;
                let noise: Vec<u8> = (0..n).map(|_| r.next().to_le_bytes()[0]).collect();
                data.splice(at.min(data.len())..at.min(data.len()), noise);
            }
            4 if !data.is_empty() => {
                let end = (at + r.below(16) + 1).min(data.len());
                data.drain(at..end);
            }
            5 => data.truncate(at),
            6 if !data.is_empty() => {
                let end = (at + r.below(32) + 1).min(data.len());
                let copy = data[at..end].to_vec();
                data.splice(at..at, copy);
            }
            _ => {
                let other = &seeds[r.below(seeds.len())];
                let from = r.below(other.len());
                data.truncate(at);
                data.extend_from_slice(&other[from..]);
            }
        }
    }
    data
}

fn rounds() -> usize {
    std::env::var("OPENDUCT_HOSTILE_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Runs `target` over `n` mutated inputs (and the seeds themselves first), naming the input that failed.
fn drive(name: &str, seed: u64, n: usize, seeds: &[Vec<u8>], target: impl Fn(&[u8]) -> Result<(), String>) {
    let mut r = Random(seed);
    let inputs = seeds
        .iter()
        .cloned()
        .chain((0..n * rounds()).map(|_| mutate(&mut r, seeds)));
    for (i, input) in inputs.enumerate() {
        let hex: String = input.iter().map(|b| format!("{b:02x}")).collect();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| target(&input)));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(why)) => panic!("{name}: input {i} ({} bytes): {why}\n{hex}", input.len()),
            Err(_) => panic!("{name}: input {i} ({} bytes) panicked\n{hex}", input.len()),
        }
    }
}

fn quiet(f: fn(&[u8])) -> impl Fn(&[u8]) -> Result<(), String> {
    move |data| {
        f(data);
        Ok(())
    }
}

// --- seeds ----------------------------------------------------------------------------------------------------

fn header(key: i16, version: i16) -> Vec<u8> {
    let api = ApiKey::try_from(key).unwrap();
    let header = RequestHeader::default()
        .with_request_api_key(key)
        .with_request_api_version(version)
        .with_correlation_id(7)
        .with_client_id(Some(StrBytes::from_static_str("hostile")));
    let mut buf = BytesMut::new();
    header
        .encode(&mut buf, api.request_header_version(version))
        .unwrap();
    buf.to_vec()
}

/// Every API and version the broker serves, as a header and an empty body of each.
fn request_seeds() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for (name, min, max) in openduct_broker::supported_apis() {
        let Some(key) = (0..=100i16).find(|k| ApiKey::try_from(*k).is_ok_and(|a| format!("{a:?}") == name))
        else {
            continue;
        };
        for version in min..=max {
            let mut frame = header(key, version);
            frame.extend_from_slice(&[0u8; 24]);
            out.push(frame);
        }
    }
    assert!(out.len() > 50, "every served API and version: {}", out.len());
    out
}

fn batch(values: &[&str], compression: Compression) -> Vec<u8> {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: 7,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(i).unwrap(),
            sequence: i32::try_from(i).unwrap(),
            timestamp: 1_763_280_000_000,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .unwrap();
    buf.to_vec()
}

fn batch_seeds() -> Vec<Vec<u8>> {
    let one = batch(&["a", "bb"], Compression::None);
    let mut two = one.clone();
    two.extend(batch(&["ccc"], Compression::None));
    vec![one, two, batch(&["many"; 40], Compression::None)]
}

/// Segment files the store wrote itself, with one, two and many batches.
fn segment_seeds() -> Vec<Vec<u8>> {
    [1, 2, 6]
        .iter()
        .map(|n| {
            let dir = tempfile::tempdir().unwrap();
            let log = openduct_storage::PartitionLog::open(dir.path(), Default::default()).unwrap();
            for i in 0..*n {
                log.append(Bytes::from(batch(&[&format!("v{i}")], Compression::None)))
                    .unwrap();
            }
            drop(log);
            std::fs::read(dir.path().join(format!("{:020}.log", 0))).unwrap()
        })
        .collect()
}

fn expression_seeds() -> Vec<Vec<u8>> {
    [
        "name.upperAscii()",
        "round(price * qty * 1.2, 2)",
        "status >= 503 ? 'outage' : status >= 500 ? 'error' : 'ok'",
        "note == null ? 'none' : note",
        "order.lines.map(l, l.qty).sum()",
        "tags.join(', ') + ' for ' + name",
        "timestamp(at) - timestamp('2026-01-01T00:00:00Z')",
        "{'a': 1, 'b': [1, 2, 3]}",
        r"regex.replace(name, r'(\w)(\w*)', r'\2\1')",
        "'%s x %.2f'.format([qty, price])",
        "cel.bind(t, price * qty, t > 10.0 ? t : 0)",
        "order.lines.exists(l, l.qty > 1) && 'a' in tags",
    ]
    .iter()
    .map(|f| f.as_bytes().to_vec())
    .collect()
}

/// Runs `f` on a thread with room for a debug build's recursion (see `openduct_hostile::expression`).
fn big_stack(f: impl FnOnce() + Send + 'static) {
    let worker = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(f)
        .unwrap();
    if let Err(panic) = worker.join() {
        std::panic::resume_unwind(panic);
    }
}

fn pipeline_seeds() -> Vec<Vec<u8>> {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/pipelines");
    let mut out: Vec<Vec<u8>> = std::fs::read_dir(examples)
        .unwrap()
        .filter_map(|e| std::fs::read(e.ok()?.path().join("pipeline.yaml")).ok())
        .collect();
    out.sort();
    assert!(out.len() >= 10, "the example pipelines: {}", out.len());
    out
}

fn mqtt_seeds() -> Vec<Vec<u8>> {
    let mut publish = vec![0x32, 0x0c, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x01];
    publish.extend_from_slice(b"{\"t\":1}"[..5].as_ref());
    vec![
        vec![0x20, 0x02, 0x00, 0x00],
        publish.clone(),
        vec![0x90, 0x03, 0x00, 0x01, 0x00],
        vec![0x40, 0x02, 0x00, 0x01],
        vec![0xd0, 0x00],
        [vec![0x20, 0x02, 0x00, 0x00], publish, vec![0xd0, 0x00]].concat(),
    ]
}

fn chunked(messages: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for m in messages {
        out.extend_from_slice(&u16::try_from(m.len()).unwrap().to_be_bytes());
        out.extend_from_slice(m);
    }
    out
}

/// pgoutput messages, each in an XLogData frame: a table described, a transaction with an insert, an update and a
/// delete.
fn postgres_seeds() -> Vec<Vec<u8>> {
    let frame = |message: Vec<u8>| {
        [
            vec![b'w'],
            vec![0; 8],
            100u64.to_be_bytes().to_vec(),
            vec![0; 8],
            message,
        ]
        .concat()
    };
    let relation = [
        vec![b'R'],
        16_384u32.to_be_bytes().to_vec(),
        b"public\0orders\0".to_vec(),
        vec![b'd'],
        2u16.to_be_bytes().to_vec(),
        vec![1],
        b"id\0".to_vec(),
        23u32.to_be_bytes().to_vec(),
        (-1i32).to_be_bytes().to_vec(),
        vec![0],
        b"total\0".to_vec(),
        1700u32.to_be_bytes().to_vec(),
        (-1i32).to_be_bytes().to_vec(),
    ]
    .concat();
    let tuple = [
        2u16.to_be_bytes().to_vec(),
        vec![b't'],
        1u32.to_be_bytes().to_vec(),
        b"7".to_vec(),
        vec![b't'],
        5u32.to_be_bytes().to_vec(),
        b"12.50".to_vec(),
    ]
    .concat();
    let begin = [
        vec![b'B'],
        100u64.to_be_bytes().to_vec(),
        vec![0; 8],
        9u32.to_be_bytes().to_vec(),
    ]
    .concat();
    let insert = [
        vec![b'I'],
        16_384u32.to_be_bytes().to_vec(),
        vec![b'N'],
        tuple.clone(),
    ]
    .concat();
    let update = [
        vec![b'U'],
        16_384u32.to_be_bytes().to_vec(),
        vec![b'N'],
        tuple.clone(),
    ]
    .concat();
    let delete = [vec![b'D'], 16_384u32.to_be_bytes().to_vec(), vec![b'K'], tuple].concat();
    let commit = [
        vec![b'C', 0],
        100u64.to_be_bytes().to_vec(),
        120u64.to_be_bytes().to_vec(),
        vec![0; 8],
    ]
    .concat();
    let keepalive = [vec![b'k'], 120u64.to_be_bytes().to_vec(), vec![0; 8], vec![1]].concat();
    vec![chunked(&[
        frame(relation),
        frame(begin),
        frame(insert),
        frame(update),
        frame(delete),
        frame(commit),
        keepalive,
    ])]
}

/// Binary log events: a rotate, a query and a commit (XID), each with its 19-byte header, with and without CRC32.
fn mysql_seeds() -> Vec<Vec<u8>> {
    let event = |kind: u8, body: &[u8], checksum: bool| {
        let size = u32::try_from(19 + body.len() + if checksum { 4 } else { 0 }).unwrap();
        let mut e = [
            1_763_280_000u32.to_le_bytes().to_vec(),
            vec![kind],
            1u32.to_le_bytes().to_vec(),
        ]
        .concat();
        e.extend_from_slice(&size.to_le_bytes());
        e.extend_from_slice(&(size + 4).to_le_bytes());
        e.extend_from_slice(&[0, 0]);
        e.extend_from_slice(body);
        if checksum {
            e.extend_from_slice(&[0; 4]);
        }
        e
    };
    let rotate = [4u64.to_le_bytes().to_vec(), b"binlog.000002".to_vec()].concat();
    let query = [
        vec![0; 4],
        vec![0; 4],
        vec![4],
        vec![0, 0],
        vec![0, 0],
        b"shop\0BEGIN".to_vec(),
    ]
    .concat();
    let xid = 9u64.to_le_bytes().to_vec();
    [false, true]
        .iter()
        .map(|&checksum| {
            let mut out = vec![u8::from(checksum)];
            out.extend(chunked(&[
                event(4, &rotate, checksum),
                event(2, &query, checksum),
                event(16, &xid, checksum),
            ]));
            out
        })
        .collect()
}

/// Requests a client sends first: versions, metadata, a topic created, a produce, a fetch, and a sign-in begun.
fn broker_seeds() -> Vec<Vec<u8>> {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::*;
    fn frame<R: Encodable>(key: ApiKey, version: i16, request: &R) -> Vec<u8> {
        let mut out = header(key as i16, version);
        let mut body = BytesMut::new();
        request.encode(&mut body, version).unwrap();
        out.extend_from_slice(&body);
        out
    }
    let topic = || TopicName(StrBytes::from_static_str("t"));
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic())
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(Bytes::from(batch(&["a"], Compression::None)))),
                ]),
        ]);
    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(10)
        .with_max_bytes(1 << 16)
        .with_topics(vec![FetchTopic::default().with_topic(topic()).with_partitions(
            vec![FetchPartition::default().with_partition_max_bytes(1 << 16)],
        )]);
    let messages = [
        frame(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default()),
        frame(ApiKey::Metadata, 12, &MetadataRequest::default()),
        frame(
            ApiKey::CreateTopics,
            7,
            &CreateTopicsRequest::default().with_topics(vec![
                CreatableTopic::default()
                    .with_name(topic())
                    .with_num_partitions(1)
                    .with_replication_factor(1),
            ]),
        ),
        frame(ApiKey::Produce, 9, &produce),
        frame(ApiKey::Fetch, 12, &fetch),
        frame(
            ApiKey::SaslHandshake,
            1,
            &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("SCRAM-SHA-256")),
        ),
        frame(
            ApiKey::SaslAuthenticate,
            2,
            &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from_static(b"n,,n=fuzz,r=abcdefgh")),
        ),
    ];
    vec![
        chunked(&messages[..5]),
        chunked(&[messages[0].clone(), messages[5].clone(), messages[6].clone()]),
        chunked(&messages[3..5]),
    ]
}

// --- the targets ----------------------------------------------------------------------------------------------

#[test]
fn kafka_requests() {
    let seeds = request_seeds();
    for s in &seeds {
        openduct_hostile::kafka_request(s);
    }
    drive(
        "kafka_request",
        1,
        20_000,
        &seeds,
        quiet(openduct_hostile::kafka_request),
    );
}

#[test]
fn record_batches() {
    drive(
        "record_batches",
        2,
        10_000,
        &batch_seeds(),
        quiet(openduct_hostile::record_batches),
    );
}

#[test]
fn segments() {
    drive(
        "segment",
        3,
        1_000,
        &segment_seeds(),
        quiet(openduct_hostile::segment),
    );
}

#[test]
fn expressions() {
    big_stack(|| {
        drive(
            "expression",
            4,
            10_000,
            &expression_seeds(),
            quiet(openduct_hostile::expression),
        );
    });
}

#[test]
fn pipelines() {
    big_stack(|| {
        drive(
            "pipeline",
            5,
            5_000,
            &pipeline_seeds(),
            quiet(openduct_hostile::pipeline),
        );
    });
}

#[test]
fn mqtt_packets() {
    drive("mqtt", 6, 10_000, &mqtt_seeds(), quiet(openduct_hostile::mqtt));
}

#[test]
fn postgres_replication() {
    drive(
        "postgres",
        7,
        10_000,
        &postgres_seeds(),
        quiet(openduct_hostile::postgres),
    );
}

#[test]
fn mysql_binlog() {
    drive("mysql", 8, 10_000, &mysql_seeds(), quiet(openduct_hostile::mysql));
}

#[test]
fn a_broker_under_hostile_requests() {
    drive("broker", 9, 400, &broker_seeds(), openduct_hostile::broker);
}

#[test]
fn chunks_split_and_keep_every_byte() {
    let data = chunked(&[b"ab".to_vec(), vec![], b"cde".to_vec()]);
    let parts = openduct_hostile::chunks(&data);
    assert_eq!(
        parts,
        vec![
            Bytes::from_static(b"ab"),
            Bytes::new(),
            Bytes::from_static(b"cde")
        ]
    );
    // A length longer than what is left takes the rest; a lone byte at the end is a chunk of its own.
    assert_eq!(
        openduct_hostile::chunks(&[0, 9, b'x', b'y']),
        vec![Bytes::from_static(b"xy")]
    );
    assert_eq!(openduct_hostile::chunks(&[7]), vec![Bytes::from_static(&[7])]);
}
