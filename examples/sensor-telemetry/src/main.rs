// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Sensor telemetry over OpenDuct, with an ordinary Kafka client (rskafka): nothing here is specific to OpenDuct.
//!
//! ```text
//! openduct serve --memory                                 # in one terminal
//! cargo run -p sensor-telemetry -- produce               # simulated sensors send readings
//! cargo run -p sensor-telemetry -- consume --from-beginning
//! ```
//!
//! Each sensor's readings go to the same partition (chosen from the sensor id), so a consumer sees one sensor's
//! readings in the order they were taken.

use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rskafka::client::partition::{Compression, OffsetAt, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use tokio::sync::mpsc;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Parser)]
#[command(about = "Sensor telemetry over OpenDuct")]
struct Cli {
    /// The broker.
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(long, default_value = "telemetry")]
    topic: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Simulate sensors sending readings.
    Produce {
        #[arg(long, default_value_t = 4)]
        sensors: u32,
        /// Partitions of the topic, when this creates it.
        #[arg(long, default_value_t = 3)]
        partitions: i32,
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
        /// Readings per sensor before stopping; 0 runs until Ctrl-C.
        #[arg(long, default_value_t = 0)]
        count: u64,
    },
    /// Print readings as they arrive, from every partition.
    Consume {
        /// Start with the oldest reading kept instead of new ones only.
        #[arg(long)]
        from_beginning: bool,
        /// Readings to print before stopping; 0 runs until Ctrl-C.
        #[arg(long, default_value_t = 0)]
        max: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = ClientBuilder::new(vec![cli.bootstrap.clone()]).build().await?;
    match cli.command {
        Command::Produce {
            sensors,
            partitions,
            interval_ms,
            count,
        } => produce(&client, &cli.topic, sensors, partitions, interval_ms, count).await,
        Command::Consume { from_beginning, max } => consume(&client, &cli.topic, from_beginning, max).await,
    }
}

/// The topic's partitions, creating the topic first if it does not exist.
async fn ensure_topic(client: &Client, topic: &str, partitions: i32) -> Result<Vec<i32>> {
    if let Some(t) = client.list_topics().await?.into_iter().find(|t| t.name == topic) {
        return Ok(t.partitions.into_iter().collect());
    }
    client
        .controller_client()?
        .create_topic(topic, partitions, 1, 5_000)
        .await?;
    println!("created topic {topic} with {partitions} partitions");
    Ok((0..partitions).collect())
}

async fn produce(
    client: &Client,
    topic: &str,
    sensors: u32,
    partitions: i32,
    interval_ms: u64,
    count: u64,
) -> Result<()> {
    let partition_ids = ensure_topic(client, topic, partitions).await?;
    let mut writers = BTreeMap::new();
    for p in &partition_ids {
        writers.insert(
            *p,
            client
                .partition_client(topic, *p, UnknownTopicHandling::Retry)
                .await?,
        );
    }
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
    let mut sequence = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tick.tick() => {}
        }
        // One batch per partition per tick: every sensor's reading for this moment.
        let mut batches: BTreeMap<i32, Vec<Record>> = BTreeMap::new();
        for sensor in 0..sensors {
            let partition = partition_ids
                .get(usize::try_from(sensor)? % partition_ids.len().max(1))
                .copied()
                .unwrap_or(0);
            batches
                .entry(partition)
                .or_default()
                .push(reading(sensor, sequence));
        }
        for (partition, records) in batches {
            let n = records.len();
            if let Some(writer) = writers.get(&partition) {
                let offsets = writer.produce(records, Compression::NoCompression).await?;
                println!(
                    "reading {sequence}: {n} sensor(s) -> partition {partition}, offsets {:?}",
                    offsets
                );
            }
        }
        sequence += 1;
        if count > 0 && sequence >= count {
            break;
        }
    }
    Ok(())
}

fn reading(sensor: u32, sequence: u64) -> Record {
    // A slow, smooth temperature curve per sensor, so the numbers look like a real room.
    let phase = f64::from(sensor) + f64::from(u32::try_from(sequence % 10_000).unwrap_or(0)) / 10.0;
    let temp_c = 21.0 + 3.0 * phase.sin();
    let now = chrono::Utc::now();
    let value = format!(
        r#"{{"sensor":"sensor-{sensor}","seq":{sequence},"temp_c":{temp_c:.2},"at":"{}"}}"#,
        now.to_rfc3339()
    );
    Record {
        key: Some(format!("sensor-{sensor}").into_bytes()),
        value: Some(value.into_bytes()),
        headers: BTreeMap::from([("unit".to_owned(), b"celsius".to_vec())]),
        timestamp: now,
    }
}

async fn consume(client: &Client, topic: &str, from_beginning: bool, max: u64) -> Result<()> {
    let partitions = client
        .list_topics()
        .await?
        .into_iter()
        .find(|t| t.name == topic)
        .map(|t| t.partitions)
        .ok_or_else(|| {
            format!("no topic {topic}: run `produce` first, or create it with `openduct topic create`")
        })?;
    let (lines, mut received) = mpsc::channel::<String>(1024);
    for partition in partitions {
        let reader = client
            .partition_client(topic, partition, UnknownTopicHandling::Retry)
            .await?;
        let lines = lines.clone();
        tokio::spawn(async move {
            let start = if from_beginning {
                OffsetAt::Earliest
            } else {
                OffsetAt::Latest
            };
            let Ok(mut offset) = reader.get_offset(start).await else {
                return;
            };
            loop {
                // Waits up to a second on the broker for new readings, then asks again.
                let Ok((records, _)) = reader.fetch_records(offset, 1..1_000_000, 1_000).await else {
                    return;
                };
                let from = offset;
                for r in records.into_iter().filter(|r| r.offset >= from) {
                    offset = r.offset + 1;
                    let key =
                        String::from_utf8_lossy(r.record.key.as_deref().unwrap_or_default()).into_owned();
                    let value =
                        String::from_utf8_lossy(r.record.value.as_deref().unwrap_or_default()).into_owned();
                    if lines
                        .send(format!("p{partition}@{} {key} {value}", r.offset))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
    }
    drop(lines);
    let mut printed = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            line = received.recv() => match line {
                Some(line) => {
                    println!("{line}");
                    printed += 1;
                    if max > 0 && printed >= max {
                        break;
                    }
                }
                None => break,
            },
        }
    }
    Ok(())
}
