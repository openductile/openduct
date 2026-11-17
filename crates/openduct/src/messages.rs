// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct send` and `openduct read`: messages in and out from the command line, for trying OpenDuct, scripts, and
//! looking at what a topic holds.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::{Args, ValueEnum};
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{Client, EARLIEST, LATEST, Outgoing, records};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::admin::{Target, connect};
use crate::codec;

/// Messages per batch sent, and the bytes a batch may reach before it is sent.
const BATCH_MESSAGES: usize = 1000;
const BATCH_BYTES: usize = 1024 * 1024;
/// The most a read asks for at once.
const FETCH_BYTES: i32 = 8 * 1024 * 1024;

#[derive(Debug, Args)]
pub(crate) struct SendArgs {
    /// The topic to send to.
    topic: String,
    /// The messages. Without any, each line of standard input is one message (blank lines are skipped).
    messages: Vec<String>,
    /// The key every message carries. Messages with the same key go to the same partition, in order.
    #[arg(long)]
    key: Option<String>,
    /// A header for every message, as name=value. Repeat it for more than one.
    #[arg(long = "header", value_parser = parse_header)]
    headers: Vec<(String, String)>,
    /// Send to this partition rather than letting the key (or turns, without a key) choose.
    #[arg(long)]
    partition: Option<i32>,
    #[command(flatten)]
    target: Target,
}

fn parse_header(text: &str) -> Result<(String, String), String> {
    match text.split_once('=') {
        Some((name, value)) if !name.is_empty() => Ok((name.to_owned(), value.to_owned())),
        _ => Err(format!("a header is written name=value, not {text:?}")),
    }
}

#[derive(Debug, Args)]
pub(crate) struct ReadArgs {
    /// The topic to read.
    topic: String,
    /// Where to start: earliest (every message kept), latest (only messages that arrive from now on), or an offset.
    #[arg(long, default_value = "earliest", value_parser = parse_start)]
    from: Start,
    /// Keep waiting for new messages instead of stopping at the end of the topic. Stop with Ctrl-C.
    #[arg(long)]
    follow: bool,
    /// Stop after this many messages.
    #[arg(long)]
    max: Option<u64>,
    /// Read only this partition.
    #[arg(long)]
    partition: Option<i32>,
    /// Remember how far this reading got under a group name, and carry on from there next time.
    #[arg(long)]
    group: Option<String>,
    /// How each message is printed: `value` (the value alone) or `json` (one line with everything about it).
    #[arg(long, value_enum, default_value_t = Format::Value)]
    format: Format,
    #[command(flatten)]
    target: Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    Earliest,
    Latest,
    Offset(i64),
}

fn parse_start(text: &str) -> Result<Start, String> {
    match text {
        "earliest" => Ok(Start::Earliest),
        "latest" => Ok(Start::Latest),
        _ => match text.parse::<i64>() {
            Ok(n) if n >= 0 => Ok(Start::Offset(n)),
            _ => Err(format!(
                "write earliest, latest or an offset (0 or more), not {text:?}"
            )),
        },
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Format {
    Value,
    Json,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

// ------------------------------------------------------------------------------------------------------------ send

pub(crate) async fn send(args: SendArgs) -> Result<(), String> {
    let mut client = connect(&args.target).await?;
    let partitions = client
        .partition_count(&args.topic)
        .await
        .map_err(|e| format!("cannot send to {}: {e}", args.topic))?;
    if let Some(p) = args.partition
        && !(0..partitions).contains(&p)
    {
        return Err(format!(
            "{} has partitions 0 to {}, not {p}",
            args.topic,
            partitions - 1
        ));
    }
    let key = args.key.as_ref().map(|k| Bytes::from(k.clone().into_bytes()));
    let headers: Vec<(String, Bytes)> = args
        .headers
        .iter()
        .map(|(name, value)| (name.clone(), Bytes::from(value.clone().into_bytes())))
        .collect();
    // With an explicit partition or a key, every message goes to one partition; otherwise each batch takes the next.
    let fixed = args
        .partition
        .or_else(|| key.as_ref().map(|k| records::partition_for(k, partitions)));
    let mut sender = Sender {
        client: &mut client,
        topic: &args.topic,
        partitions,
        fixed,
        turn: 0,
        batch: Vec::new(),
        batch_bytes: 0,
        sent: 0,
    };
    let message = |text: String| Outgoing {
        key: key.clone(),
        value: Some(Bytes::from(text.into_bytes())),
        headers: headers.clone(),
    };
    if args.messages.is_empty() {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| format!("cannot read standard input: {e}"))?
        {
            let line = line.strip_suffix('\r').unwrap_or(&line).to_owned();
            if !line.trim().is_empty() {
                sender.add(message(line)).await?;
            }
        }
    } else {
        for text in args.messages {
            sender.add(message(text)).await?;
        }
    }
    sender.flush().await?;
    let sent = sender.sent;
    println!(
        "sent {sent} message{} to {}",
        if sent == 1 { "" } else { "s" },
        args.topic
    );
    Ok(())
}

struct Sender<'a> {
    client: &'a mut Client,
    topic: &'a str,
    partitions: i32,
    fixed: Option<i32>,
    turn: i32,
    batch: Vec<Outgoing>,
    batch_bytes: usize,
    sent: u64,
}

impl Sender<'_> {
    async fn add(&mut self, message: Outgoing) -> Result<(), String> {
        self.batch_bytes += message.value.as_ref().map_or(0, Bytes::len);
        self.batch.push(message);
        if self.batch.len() >= BATCH_MESSAGES || self.batch_bytes >= BATCH_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), String> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let partition = self.fixed.unwrap_or(self.turn);
        self.turn = (self.turn + 1) % self.partitions.max(1);
        self.client
            .produce(self.topic, partition, &self.batch, now_ms())
            .await
            .map_err(|e| format!("cannot send to {}: {e}", self.topic))?;
        self.sent += u64::try_from(self.batch.len()).unwrap_or(0);
        self.batch.clear();
        self.batch_bytes = 0;
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------------------ read

pub(crate) async fn read(args: ReadArgs) -> Result<(), String> {
    let mut client = connect(&args.target).await?;
    let topic = args.topic.clone();
    let count = client
        .partition_count(&topic)
        .await
        .map_err(|e| format!("cannot read {topic}: {e}"))?;
    let partitions: Vec<i32> = match args.partition {
        Some(p) if (0..count).contains(&p) => vec![p],
        Some(p) => return Err(format!("{topic} has partitions 0 to {}, not {p}", count - 1)),
        None => (0..count).collect(),
    };
    let committed: BTreeMap<i32, i64> = match &args.group {
        Some(group) => client
            .committed(group, &topic, &partitions)
            .await
            .map_err(|e| format!("cannot read the progress of group {group}: {e}"))?
            .into_iter()
            .filter(|(_, offset)| *offset >= 0)
            .collect(),
        None => BTreeMap::new(),
    };
    let mut next = BTreeMap::new();
    let mut end = BTreeMap::new();
    for &p in &partitions {
        let start = match (committed.get(&p), args.from) {
            (Some(offset), _) => *offset,
            (None, Start::Earliest) => list(&mut client, &topic, p, EARLIEST).await?,
            (None, Start::Latest) => list(&mut client, &topic, p, LATEST).await?,
            (None, Start::Offset(offset)) => {
                let (first, last) = (
                    list(&mut client, &topic, p, EARLIEST).await?,
                    list(&mut client, &topic, p, LATEST).await?,
                );
                if offset < first || offset > last {
                    return Err(if first == last {
                        format!("offset {offset} is past the end of partition {p} of {topic}, which is empty")
                    } else {
                        format!(
                            "offset {offset} is outside partition {p} of {topic}, which holds offsets {first} to {}",
                            last - 1
                        )
                    });
                }
                offset
            }
        };
        next.insert(p, start);
        end.insert(p, list(&mut client, &topic, p, LATEST).await?);
    }
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    let mut printed = 0u64;
    loop {
        if args.max.is_some_and(|max| printed >= max) {
            break;
        }
        // Without --follow, a partition is done once it is read up to where it ended when reading began.
        let wanted: Vec<(i32, i64)> = next
            .iter()
            .filter(|(p, offset)| args.follow || end.get(p).is_some_and(|e| *offset < e))
            .map(|(p, offset)| (*p, *offset))
            .collect();
        if wanted.is_empty() {
            break;
        }
        let wait = if args.follow { 1_000 } else { 0 };
        let fetched = tokio::select! {
            fetched = client.fetch(&topic, &wanted, wait, FETCH_BYTES) => fetched.map_err(|e| format!("cannot read {topic}: {e}"))?,
            _ = tokio::signal::ctrl_c() => break,
        };
        let mut moved = false;
        for answer in fetched {
            match answer.error_code {
                0 => {}
                1 => {
                    return Err(format!(
                        "offset {} is outside partition {} of {topic}, which holds offsets {} to {}",
                        next.get(&answer.partition).copied().unwrap_or(0),
                        answer.partition,
                        answer.log_start,
                        answer.high_watermark - 1
                    ));
                }
                code => {
                    return Err(format!(
                        "cannot read partition {} of {topic} (error code {code})",
                        answer.partition
                    ));
                }
            }
            let from = next.get(&answer.partition).copied().unwrap_or(0);
            let mut batches = answer.batches;
            while !batches.is_empty() {
                let decoded = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                );
                let set = match decoded {
                    Ok(set) => set,
                    // A batch cut short at the end of an answer is fetched whole next time.
                    Err(_) if moved => break,
                    Err(e) => {
                        return Err(format!(
                            "cannot read partition {} of {topic}: {e}",
                            answer.partition
                        ));
                    }
                };
                for record in set.records {
                    if record.offset < from || record.control {
                        continue;
                    }
                    if args.max.is_some_and(|max| printed >= max) {
                        break;
                    }
                    let written = match args.format {
                        Format::Value => writeln!(out, "{}", text(record.value.as_ref())),
                        Format::Json => writeln!(out, "{}", json_line(&topic, answer.partition, &record)),
                    };
                    if written.is_err() {
                        // The reader went away (`| head`): stop quietly, as other tools do.
                        return Ok(());
                    }
                    printed += 1;
                    next.insert(answer.partition, record.offset + 1);
                    moved = true;
                }
            }
        }
        if out.flush().is_err() {
            return Ok(());
        }
        if moved && let Some(group) = &args.group {
            let offsets: Vec<(i32, i64)> = next.iter().map(|(p, o)| (*p, *o)).collect();
            client
                .commit(group, &topic, &offsets)
                .await
                .map_err(|e| format!("cannot record the progress of group {group}: {e}"))?;
        }
    }
    let _ = out.flush();
    Ok(())
}

async fn list(client: &mut Client, topic: &str, partition: i32, at: i64) -> Result<i64, String> {
    client
        .list_offset(topic, partition, at)
        .await
        .map_err(|e| format!("cannot read {topic}: {e}"))
}

fn text(bytes: Option<&Bytes>) -> String {
    bytes
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

fn json_line(topic: &str, partition: i32, record: &kafka_protocol::records::Record) -> String {
    let headers: serde_json::Map<String, serde_json::Value> = record
        .headers
        .iter()
        .map(|(name, value)| {
            let value = value.as_ref().map_or(serde_json::Value::Null, |v| {
                serde_json::Value::String(text(Some(v)))
            });
            (name.to_string(), value)
        })
        .collect();
    serde_json::json!({
        "topic": topic,
        "partition": partition,
        "offset": record.offset,
        "timestamp": codec::iso_time(record.timestamp),
        "key": record.key.as_ref().map(|k| text(Some(k))),
        "headers": headers,
        "value": record.value.as_ref().map(|v| text(Some(v))),
    })
    .to_string()
}
