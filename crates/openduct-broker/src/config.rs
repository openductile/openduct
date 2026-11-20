// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::time::Duration;

/// How a broker runs. Every limit here bounds a resource an untrusted client could otherwise exhaust.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Where to listen for Kafka clients.
    pub listen: SocketAddr,
    /// The host and port clients are told to connect to. Kafka clients ask the broker for its address and then
    /// connect there, so this must be reachable by them (not `0.0.0.0`).
    pub advertised_host: String,
    pub advertised_port: i32,
    /// This broker's id in Metadata answers.
    pub node_id: i32,
    /// The cluster id in Metadata answers.
    pub cluster_id: String,
    /// The largest request accepted, in bytes; a larger one closes the connection.
    pub max_request_bytes: usize,
    /// The most bytes of requests held at once, across every connection (Kafka's `queued.max.request.bytes`). A
    /// request's size is reserved before its body is read; when the budget is spent, reading waits. Must be at
    /// least `max_request_bytes`.
    pub max_request_memory: usize,
    /// A request's body must arrive within this long once its size has been read.
    pub request_read_timeout: Duration,
    /// The most bytes one Fetch answer carries (Kafka's `fetch.max.bytes`), whatever the client asks; the first
    /// batch is always sent whole, so a consumer is never stuck behind one large batch.
    pub max_fetch_bytes: usize,
    /// The most client connections at once; more wait until one closes.
    pub max_connections: usize,
    /// A connection that sends nothing for this long is closed.
    pub idle_timeout: Duration,
    /// The longest a Fetch may wait for data, whatever the client asks.
    pub max_fetch_wait: Duration,
    /// Create a topic when a client writes to or asks for one that does not exist.
    pub auto_create_topics: bool,
    /// Partitions of a topic created automatically or without a partition count.
    pub default_partitions: i32,
    /// How long a new consumer group waits for more members before its first assignment (Kafka's
    /// `group.initial.rebalance.delay.ms`).
    pub group_initial_rebalance_delay: Duration,
    /// The shortest and longest session a consumer group member may ask for.
    pub group_min_session_timeout: Duration,
    pub group_max_session_timeout: Duration,
    /// How often retention is enforced.
    pub retention_check_interval: Duration,
    /// How long a transaction may stay open on a partition before the partition's leader aborts it itself: longer
    /// than any transaction's timeout, so only one no coordinator will end (its producer never told one of this
    /// partition) is ever met.
    pub hanging_transaction_after: Duration,
    /// The client port's TLS, sign-in and access rules (see [`crate::security`]).
    pub security: crate::security::Security,
}

impl BrokerConfig {
    /// Defaults for a broker listening on `listen` and advertising the same address.
    pub fn new(listen: SocketAddr) -> Self {
        Self {
            listen,
            advertised_host: listen.ip().to_string(),
            advertised_port: i32::from(listen.port()),
            node_id: 0,
            cluster_id: "openduct".to_owned(),
            max_request_bytes: 100 * 1024 * 1024,
            max_request_memory: 256 * 1024 * 1024,
            request_read_timeout: Duration::from_secs(60),
            max_fetch_bytes: 55 * 1024 * 1024,
            max_connections: 10_000,
            idle_timeout: Duration::from_secs(600),
            max_fetch_wait: Duration::from_secs(30),
            auto_create_topics: false,
            default_partitions: 1,
            group_initial_rebalance_delay: Duration::from_secs(3),
            group_min_session_timeout: Duration::from_secs(1),
            group_max_session_timeout: Duration::from_secs(1800),
            retention_check_interval: Duration::from_secs(60),
            hanging_transaction_after: Duration::from_secs(16 * 60),
            security: crate::security::Security::open(),
        }
    }
}
