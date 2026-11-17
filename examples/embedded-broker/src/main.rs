// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct inside your own program: an edge gateway, robot or vehicle computer that keeps its own durable event log
//! with no separate broker process to install or look after.
//!
//! ```text
//! cargo run -p embedded-broker -- ./edge-log      # run it twice: the log survives restarts
//! ```
//!
//! The broker listens on a free local port; devices and programs on the machine use any Kafka client against it.
//! Here the program is its own client.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{FileStore, FileStoreConfig};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, OffsetAt, UnknownTopicHandling};
use rskafka::record::Record;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "edge-log".to_owned());

    // A durable log: each write is on disk (fsync) before it is acknowledged.
    let store = FileStore::open(&dir, FileStoreConfig::default())?;
    let broker = Broker::start(BrokerConfig::new("127.0.0.1:0".parse()?), Arc::new(store)).await?;
    let addr = broker.local_addr().to_string();
    println!("broker running inside this program at {addr}, log in {dir}");

    let client = ClientBuilder::new(vec![addr]).build().await?;
    if !client.list_topics().await?.iter().any(|t| t.name == "events") {
        client
            .controller_client()?
            .create_topic("events", 1, 1, 5_000)
            .await?;
    }
    let events = client
        .partition_client("events", 0, UnknownTopicHandling::Error)
        .await?;

    let now = chrono::Utc::now();
    let event = Record {
        key: Some(b"gateway".to_vec()),
        value: Some(format!(r#"{{"event":"started","at":"{}"}}"#, now.to_rfc3339()).into_bytes()),
        headers: BTreeMap::new(),
        timestamp: now,
    };
    let stored = events.produce(vec![event], Compression::NoCompression).await?;
    println!("stored this start at offset {stored:?}");

    let first = events.get_offset(OffsetAt::Earliest).await?;
    let end = events.get_offset(OffsetAt::Latest).await?;
    println!("the log holds offsets {first}..{end}; every start so far:");
    let mut offset = first;
    while offset < end {
        let (records, _) = events.fetch_records(offset, 1..1_000_000, 100).await?;
        let Some(last) = records.last().map(|r| r.offset) else {
            break;
        };
        for r in records.iter().filter(|r| r.offset >= offset) {
            let value = String::from_utf8_lossy(r.record.value.as_deref().unwrap_or_default()).into_owned();
            println!("  {} {value}", r.offset);
        }
        offset = last + 1;
    }

    broker.shutdown().await;
    Ok(())
}
