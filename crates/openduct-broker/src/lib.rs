// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The OpenDuct broker: the Kafka wire protocol over an OpenDuct [`Store`](openduct_storage::Store).
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use openduct_broker::{Broker, BrokerConfig};
//! use openduct_storage::MemoryStore;
//!
//! let broker = Broker::start(BrokerConfig::new("127.0.0.1:9092".parse()?), Arc::new(MemoryStore::new())).await?;
//! // ... any Kafka client can now produce to and consume from 127.0.0.1:9092 ...
//! broker.shutdown().await;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

pub mod client;
mod codes;
mod config;
mod groups;
mod handlers;
mod metrics;
pub mod security;

/// Doors for the hostile-input tests (`crates/openduct-hostile` and `fuzz/`), not an API.
#[doc(hidden)]
pub mod hostile {
    /// A request frame (what follows its four-byte length): its header read as the broker reads it, and the
    /// request's API, version and body when the broker serves them - for the request to be decoded next.
    pub fn request_header(frame: &[u8]) -> Option<(kafka_protocol::messages::ApiKey, i16, bytes::Bytes)> {
        crate::server::read_header(frame)
    }
}
mod server;

pub use config::BrokerConfig;
pub use metrics::BrokerMetrics;
pub use server::{Broker, BrokerError};

/// The Kafka APIs OpenDuct answers, with the oldest and newest version of each.
pub fn supported_apis() -> Vec<(&'static str, i16, i16)> {
    handlers::SUPPORTED
        .iter()
        .map(|(key, min, max)| (api_name(*key), *min, *max))
        .collect()
}

fn api_name(key: kafka_protocol::messages::ApiKey) -> &'static str {
    use kafka_protocol::messages::ApiKey;
    match key {
        ApiKey::Produce => "Produce",
        ApiKey::Fetch => "Fetch",
        ApiKey::ListOffsets => "ListOffsets",
        ApiKey::DeleteRecords => "DeleteRecords",
        ApiKey::Metadata => "Metadata",
        ApiKey::OffsetCommit => "OffsetCommit",
        ApiKey::OffsetFetch => "OffsetFetch",
        ApiKey::FindCoordinator => "FindCoordinator",
        ApiKey::JoinGroup => "JoinGroup",
        ApiKey::Heartbeat => "Heartbeat",
        ApiKey::LeaveGroup => "LeaveGroup",
        ApiKey::SyncGroup => "SyncGroup",
        ApiKey::DescribeGroups => "DescribeGroups",
        ApiKey::ListGroups => "ListGroups",
        ApiKey::DeleteGroups => "DeleteGroups",
        ApiKey::OffsetDelete => "OffsetDelete",
        ApiKey::ApiVersions => "ApiVersions",
        ApiKey::CreateTopics => "CreateTopics",
        ApiKey::DeleteTopics => "DeleteTopics",
        ApiKey::CreatePartitions => "CreatePartitions",
        ApiKey::InitProducerId => "InitProducerId",
        ApiKey::OffsetForLeaderEpoch => "OffsetForLeaderEpoch",
        ApiKey::DescribeConfigs => "DescribeConfigs",
        ApiKey::IncrementalAlterConfigs => "IncrementalAlterConfigs",
        ApiKey::DescribeCluster => "DescribeCluster",
        ApiKey::SaslHandshake => "SaslHandshake",
        ApiKey::SaslAuthenticate => "SaslAuthenticate",
        _ => "other",
    }
}
