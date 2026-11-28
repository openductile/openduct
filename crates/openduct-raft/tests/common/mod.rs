// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Brokers in one process, over real TCP, for the replication tests: a cluster to start, stop and ask, and a record
//! batch to write. Each test file uses part of it.

#![allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use openduct_raft::{Broker, ClusterAuth, Replica, ReplicaConfig, ReplicationTls, TierConfig, Timing};
use openduct_storage::{Appended, Store, TopicConfig};
use openraft::async_runtime::WatchReceiver;

/// A minimal valid record batch (format 2) of `count` records.
pub fn batch(count: i32, timestamp: i64) -> Bytes {
    const HEADER_LEN: usize = 61;
    let body = vec![0xABu8; 16];
    let mut b = Vec::with_capacity(HEADER_LEN + body.len());
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&i32::try_from(HEADER_LEN - 12 + body.len()).unwrap().to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.push(2u8);
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&(count - 1).to_be_bytes());
    b.extend_from_slice(&timestamp.to_be_bytes());
    b.extend_from_slice(&timestamp.to_be_bytes());
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b.extend_from_slice(&count.to_be_bytes());
    b.extend_from_slice(&body);
    let crc = crc32c::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(b)
}

fn varint(out: &mut Vec<u8>, n: i64) {
    let mut raw = ((n << 1) ^ (n >> 63)) as u64;
    loop {
        let b = (raw & 0x7f) as u8;
        raw >>= 7;
        if raw == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// A record batch (format 2) of real records, each a key and a value - or no value, a tombstone.
#[allow(dead_code)]
pub fn keyed(records: &[(&str, Option<&str>)], timestamp: i64) -> Bytes {
    let with_keys: Vec<(Option<&str>, Option<&str>)> = records.iter().map(|(k, v)| (Some(*k), *v)).collect();
    records_batch(&with_keys, timestamp)
}

/// As [`keyed`], where a record may have no key.
#[allow(dead_code)]
pub fn records_batch(records: &[(Option<&str>, Option<&str>)], timestamp: i64) -> Bytes {
    let mut body = Vec::new();
    for (i, (key, value)) in records.iter().enumerate() {
        let mut r = vec![0u8];
        varint(&mut r, 0);
        varint(&mut r, i as i64);
        match key {
            Some(k) => {
                varint(&mut r, k.len() as i64);
                r.extend_from_slice(k.as_bytes());
            }
            None => varint(&mut r, -1),
        }
        match value {
            Some(v) => {
                varint(&mut r, v.len() as i64);
                r.extend_from_slice(v.as_bytes());
            }
            None => varint(&mut r, -1),
        }
        varint(&mut r, 0);
        varint(&mut body, r.len() as i64);
        body.extend_from_slice(&r);
    }
    let count = i32::try_from(records.len()).unwrap();
    let mut b = Vec::new();
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&i32::try_from(49 + body.len()).unwrap().to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.push(2u8);
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&(count - 1).to_be_bytes());
    b.extend_from_slice(&timestamp.to_be_bytes());
    b.extend_from_slice(&timestamp.to_be_bytes());
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b.extend_from_slice(&count.to_be_bytes());
    b.extend_from_slice(&body);
    let crc = crc32c::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(b)
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub fn topic(partitions: i32) -> TopicConfig {
    TopicConfig {
        partitions,
        retention_ms: None,
    }
}

pub struct Cluster {
    pub root: tempfile::TempDir,
    pub brokers: BTreeMap<u64, Broker>,
    pub listen: BTreeMap<u64, SocketAddr>,
    pub replication_factor: usize,
    pub snapshot_every: u64,
    pub running: BTreeMap<u64, Arc<Replica>>,
    /// Each broker's TLS settings, when the cluster speaks TLS.
    pub tls: BTreeMap<u64, ReplicationTls>,
    /// Tiered storage, with small segments so they close often.
    pub tier: Option<TierConfig>,
    /// The feature level a broker's build supports, when it is not the current one (an older build).
    pub levels: BTreeMap<u64, u32>,
    /// The size a partition's segment closes at, when a test needs many small ones.
    pub segment_bytes: Option<u64>,
    /// Brokers are started with a person's word that the surviving copies are to be taken.
    pub accept_surviving_copies: Option<String>,
    pub start_empty_where_none_survived: Option<String>,
}

impl Cluster {
    pub fn new(n: u64, replication_factor: usize, snapshot_every: u64) -> Self {
        let mut brokers = BTreeMap::new();
        let mut listen = BTreeMap::new();
        for id in 1..=n {
            let raft = free_port();
            listen.insert(id, SocketAddr::from(([127, 0, 0, 1], raft)));
            brokers.insert(
                id,
                Broker {
                    raft: format!("127.0.0.1:{raft}"),
                    kafka: format!("127.0.0.1:{}", 9000 + id),
                },
            );
        }
        Self {
            root: tempfile::tempdir().unwrap(),
            brokers,
            listen,
            replication_factor,
            snapshot_every,
            running: BTreeMap::new(),
            tls: BTreeMap::new(),
            tier: None,
            levels: BTreeMap::new(),
            segment_bytes: None,
            accept_surviving_copies: None,
            start_empty_where_none_survived: None,
        }
    }

    pub fn dir(&self, id: u64) -> PathBuf {
        self.root.path().join(format!("broker-{id}"))
    }

    pub async fn start(&mut self, id: u64) {
        let mut config = ReplicaConfig::new(
            id,
            self.dir(id),
            self.listen[&id],
            self.brokers.clone(),
            ClusterAuth {
                cluster: "test".to_owned(),
                secret: "a-secret-for-tests".to_owned(),
                tls: self.tls.get(&id).cloned(),
            },
        );
        config.timing = Timing {
            heartbeat: Duration::from_millis(50),
            election_min: Duration::from_millis(300),
            election_max: Duration::from_millis(600),
            // Several elections long, as in a real broker (thirty seconds against three), and short enough to drill.
            empty_start_wait: Duration::from_secs(5),
            // Short enough for a drill to see a restarted broker get its partitions back.
            hand_back_after: Duration::from_secs(4),
        };
        config.replication_factor = self.replication_factor;
        config.snapshot_every = self.snapshot_every;
        config.accept_surviving_copies = self.accept_surviving_copies.clone();
        config.start_empty_where_none_survived = self.start_empty_where_none_survived.clone();
        if let Some(level) = self.levels.get(&id) {
            config.supported_level = *level;
        }
        config.log_segment_bytes = 4096;
        if let Some(tier) = &self.tier {
            config.tier = Some(tier.clone());
            config.store.segment_bytes = 1024;
        }
        if let Some(bytes) = self.segment_bytes {
            config.store.segment_bytes = bytes;
        }
        let replica = Replica::start(config).await.unwrap();
        self.running.insert(id, Arc::new(replica));
    }

    /// A broker new to the cluster: started with every broker listed, it joins.
    pub async fn add_broker(&mut self, id: u64) {
        let raft = free_port();
        self.listen.insert(id, SocketAddr::from(([127, 0, 0, 1], raft)));
        self.brokers.insert(
            id,
            Broker {
                raft: format!("127.0.0.1:{raft}"),
                kafka: format!("127.0.0.1:{}", 9000 + id),
            },
        );
        self.start(id).await;
    }

    pub async fn start_all(&mut self) {
        let ids: Vec<u64> = self.brokers.keys().copied().collect();
        for id in ids {
            self.start(id).await;
        }
    }

    pub async fn stop(&mut self, id: u64) {
        if let Some(r) = self.running.remove(&id) {
            // Returns once its listener has gone: the port is free for a start again.
            r.shutdown().await;
        }
    }

    pub async fn stop_all(&mut self) {
        let ids: Vec<u64> = self.running.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }

    /// Waits until every running broker names the same running broker, as `ask` reports it.
    pub async fn agree(&self, what: &str, ask: impl Fn(&Replica) -> Option<u64>) -> Arc<Replica> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let seen: Vec<Option<u64>> = self.running.values().map(|r| ask(r)).collect();
            if let Some(Some(l)) = seen.first()
                && seen.iter().all(|s| *s == Some(*l))
                && let Some(r) = self.running.get(l)
            {
                return Arc::clone(r);
            }
            assert!(Instant::now() < deadline, "no agreed {what}: {seen:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn coordinator(&self) -> Arc<Replica> {
        self.agree("coordinator", |r| r.coordinator()).await
    }

    pub async fn leader(&self, topic: &str, partition: i32) -> Arc<Replica> {
        let (t, p) = (topic.to_owned(), partition);
        self.agree(&format!("leader of {topic}/{partition}"), move |r| {
            r.partition_leader(&t, p)
        })
        .await
    }

    /// Appends through the partition's leader, finding it again while it changes.
    pub async fn append(&self, topic: &str, partition: i32, b: Bytes) -> Appended {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let leader = self.leader(topic, partition).await;
            let (t, b2) = (topic.to_owned(), b.clone());
            match on(&leader, move |r| r.append(&t, partition, b2)).await {
                Ok(a) => return a,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "{topic}/{partition}: never appended: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

/// Runs a store call off the async threads, as the broker does.
pub async fn on<T: Send + 'static>(r: &Arc<Replica>, f: impl FnOnce(&Replica) -> T + Send + 'static) -> T {
    let r = Arc::clone(r);
    tokio::task::spawn_blocking(move || f(&r)).await.unwrap()
}

/// Waits until a broker's own copy of a partition holds `want` batches. A minute: a broker rebuilt from nothing
/// catches up every group it holds, and a CI runner carrying the whole suite at once is slow.
pub async fn copy_holds(r: &Arc<Replica>, topic: &str, partition: i32, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let have = r
            .local_partition(topic, partition)
            .and_then(|log| log.read(0, usize::MAX).ok())
            .map_or(0, |read| read.batches.len());
        if have == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "broker {} holds {have} batches of {topic}/{partition}, not {want}; {}",
            r.id(),
            describe(r, topic, partition)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// What a broker knows of a partition, for a failure's message: whether it knows the topic, and its group's state.
pub fn describe(r: &Replica, topic: &str, partition: i32) -> String {
    let group = r.partition_raft(topic, partition).map_or_else(
        || "no group here".to_owned(),
        |raft| {
            let m = raft.metrics().borrow_watched().clone();
            format!(
                "{:?}, leader {:?}, term {}, last entry {:?}, applied {:?}, members {:?}",
                m.state,
                m.current_leader,
                m.current_term,
                m.last_log_index,
                m.last_applied,
                m.membership_config.membership()
            )
        },
    );
    format!(
        "the topic is {}known here, its group: {group}; the controller here is led by {:?}",
        if r.topic(topic).is_some() { "" } else { "not " },
        r.coordinator()
    )
}
