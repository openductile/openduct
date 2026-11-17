// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct pipeline check | test | run`: a pipeline file checked, tried over sample messages, or run against a
//! broker. Whatever the shape - topic to topic, a connector to a topic, a topic to a connector - results are made
//! safe before progress is saved, so no message is lost.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Subcommand;
use kafka_protocol::records::RecordBatchDecoder;
use std::future::Future;
use std::sync::Arc;

use openduct_broker::client::{
    Client, ClientError, EARLIEST, Fetched, GroupState, LATEST, Membership, Outgoing, Routed, records,
};
use openduct_connect::{Limits, Secrets, Sink, SinkRecord, Source};
use openduct_pipeline::{Message, Output, Pipeline, Start, test_lines};

use crate::admin::{Target, connect};

const FETCH_BYTES: i32 = 8 * 1024 * 1024;
const SESSION_MS: i32 = 30_000;
const HEARTBEAT: Duration = Duration::from_secs(3);
const REPORT: Duration = Duration::from_secs(10);

#[derive(Debug, Subcommand)]
pub(crate) enum PipelineCommand {
    /// Check a pipeline file, its connector and every expression in it, without running it.
    Check {
        /// The pipeline file.
        file: PathBuf,
    },
    /// Run a pipeline over messages in a file, with no broker, and print what it would write.
    Test {
        /// The pipeline file.
        file: PathBuf,
        /// Sample messages: one value per line (JSON, or text for a pipeline with format: text).
        #[arg(long)]
        input: PathBuf,
    },
    /// Run a pipeline: read, change and write messages as they arrive, until Ctrl-C or SIGTERM.
    Run {
        /// The pipeline file.
        file: PathBuf,
        /// The secrets it names (`secret:NAME`): one `NAME=value` a line, in a file only you may read (chmod 600).
        #[arg(long)]
        secrets_file: Option<PathBuf>,
        #[command(flatten)]
        target: Box<Target>,
    },
}

fn load(file: &Path) -> Result<Pipeline, String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    Pipeline::parse(&text)
}

pub(crate) async fn command(command: PipelineCommand) -> Result<(), String> {
    match command {
        PipelineCommand::Check { file } => {
            let p = load(&file)?;
            let from = match &p.source {
                Some(c) => Source::build(c, &p.name, Limits::check(true))?.describe(),
                None => p.from_topic.clone(),
            };
            let to = match &p.sink {
                Some(c) => Sink::build(c, &p.name, Limits::check(true))?.describe(),
                None => p.to_topic.clone(),
            };
            println!("pipeline {} is ready: it reads {from} and writes {to}", p.name);
            Ok(())
        }
        PipelineCommand::Test { file, input } => {
            let p = load(&file)?;
            let text = std::fs::read_to_string(&input)
                .map_err(|e| format!("cannot read {}: {e}", input.display()))?;
            for line in test_lines(&p, &text) {
                println!("{line}");
            }
            Ok(())
        }
        PipelineCommand::Run {
            file,
            secrets_file,
            target,
        } => {
            // What connectors report as they go (a connection lost and made again, messages a cluster removed
            // before they were read) reaches the log; RUST_LOG sets the detail.
            crate::serve::init_logging(crate::serve::LogFormat::Text, None);
            // Listening for a stop from the first moment, before connecting.
            let mut stop = std::pin::pin!(stopped());
            let pipeline = load(&file)?;
            let named = match &secrets_file {
                Some(path) => crate::secrets_file::read(path)?,
                None => std::collections::BTreeMap::new(),
            };
            // Run by whoever runs this machine: its own network is theirs to reach.
            let secrets = Limits::new(Secrets::resolve(named, true), true);
            match &pipeline.source {
                Some(c) => {
                    let source = Source::build(c, &pipeline.name, secrets)?;
                    let Some(client) = connect_unless_stopped(&target, stop.as_mut()).await? else {
                        return Ok(());
                    };
                    run_source(&pipeline, source, client, stop, None).await
                }
                None => {
                    let sink = pipeline
                        .sink
                        .as_ref()
                        .map(|c| Sink::build(c, &pipeline.name, secrets))
                        .transpose()?;
                    let Some(client) = connect_unless_stopped(&target, stop.as_mut()).await? else {
                        return Ok(());
                    };
                    run_reader(&pipeline, sink, client, stop, None).await
                }
            }
        }
    }
}

/// The broker calls a pipeline makes - the same for a client of one broker (the command line's) and for one that
/// follows each partition's leader round a cluster (a pipeline run inside OpenDuct).
pub(crate) trait Broker: Send {
    fn fetch<'a>(
        &'a mut self,
        topic: &'a str,
        from: &'a [(i32, i64)],
        max_wait_ms: i32,
        max_bytes: i32,
    ) -> impl Future<Output = Result<Vec<Fetched>, ClientError>> + Send + 'a;
    fn produce<'a>(
        &'a mut self,
        topic: &'a str,
        partition: i32,
        messages: &'a [Outgoing],
        timestamp_ms: i64,
    ) -> impl Future<Output = Result<i64, ClientError>> + Send + 'a;
    fn partition_count<'a>(
        &'a mut self,
        topic: &'a str,
    ) -> impl Future<Output = Result<i32, ClientError>> + Send + 'a;
    fn join_group<'a>(
        &'a mut self,
        group: &'a str,
        topic: &'a str,
        partitions: i32,
        session_timeout_ms: i32,
        previous: Option<&'a Membership>,
    ) -> impl Future<Output = Result<Membership, ClientError>> + Send + 'a;
    fn heartbeat<'a>(
        &'a mut self,
        member: &'a Membership,
    ) -> impl Future<Output = Result<GroupState, ClientError>> + Send + 'a;
    fn commit_member<'a>(
        &'a mut self,
        member: &'a Membership,
        offsets: &'a [(i32, i64)],
    ) -> impl Future<Output = Result<GroupState, ClientError>> + Send + 'a;
    fn leave_group<'a>(
        &'a mut self,
        member: &'a Membership,
    ) -> impl Future<Output = Result<(), ClientError>> + Send + 'a;
    fn committed<'a>(
        &'a mut self,
        group: &'a str,
        topic: &'a str,
        partitions: &'a [i32],
    ) -> impl Future<Output = Result<Vec<(i32, i64)>, ClientError>> + Send + 'a;
    fn list_offset<'a>(
        &'a mut self,
        topic: &'a str,
        partition: i32,
        at: i64,
    ) -> impl Future<Output = Result<i64, ClientError>> + Send + 'a;
    fn saved_position<'a>(
        &'a mut self,
        group: &'a str,
        topic: &'a str,
        partition: i32,
    ) -> impl Future<Output = Result<Option<String>, ClientError>> + Send + 'a;
    fn save_position<'a>(
        &'a mut self,
        group: &'a str,
        topic: &'a str,
        partition: i32,
        offset: i64,
        position: &'a str,
    ) -> impl Future<Output = Result<(), ClientError>> + Send + 'a;
}

macro_rules! broker_for {
    ($t:ty) => {
        impl Broker for $t {
            fn fetch<'a>(
                &'a mut self,
                topic: &'a str,
                from: &'a [(i32, i64)],
                max_wait_ms: i32,
                max_bytes: i32,
            ) -> impl Future<Output = Result<Vec<Fetched>, ClientError>> + Send + 'a {
                <$t>::fetch(self, topic, from, max_wait_ms, max_bytes)
            }
            fn produce<'a>(
                &'a mut self,
                topic: &'a str,
                partition: i32,
                messages: &'a [Outgoing],
                timestamp_ms: i64,
            ) -> impl Future<Output = Result<i64, ClientError>> + Send + 'a {
                <$t>::produce(self, topic, partition, messages, timestamp_ms)
            }
            fn partition_count<'a>(
                &'a mut self,
                topic: &'a str,
            ) -> impl Future<Output = Result<i32, ClientError>> + Send + 'a {
                <$t>::partition_count(self, topic)
            }
            fn join_group<'a>(
                &'a mut self,
                group: &'a str,
                topic: &'a str,
                partitions: i32,
                session_timeout_ms: i32,
                previous: Option<&'a Membership>,
            ) -> impl Future<Output = Result<Membership, ClientError>> + Send + 'a {
                <$t>::join_group(self, group, topic, partitions, session_timeout_ms, previous)
            }
            fn heartbeat<'a>(
                &'a mut self,
                member: &'a Membership,
            ) -> impl Future<Output = Result<GroupState, ClientError>> + Send + 'a {
                <$t>::heartbeat(self, member)
            }
            fn commit_member<'a>(
                &'a mut self,
                member: &'a Membership,
                offsets: &'a [(i32, i64)],
            ) -> impl Future<Output = Result<GroupState, ClientError>> + Send + 'a {
                <$t>::commit_member(self, member, offsets)
            }
            fn leave_group<'a>(
                &'a mut self,
                member: &'a Membership,
            ) -> impl Future<Output = Result<(), ClientError>> + Send + 'a {
                <$t>::leave_group(self, member)
            }
            fn committed<'a>(
                &'a mut self,
                group: &'a str,
                topic: &'a str,
                partitions: &'a [i32],
            ) -> impl Future<Output = Result<Vec<(i32, i64)>, ClientError>> + Send + 'a {
                <$t>::committed(self, group, topic, partitions)
            }
            fn list_offset<'a>(
                &'a mut self,
                topic: &'a str,
                partition: i32,
                at: i64,
            ) -> impl Future<Output = Result<i64, ClientError>> + Send + 'a {
                <$t>::list_offset(self, topic, partition, at)
            }
            fn saved_position<'a>(
                &'a mut self,
                group: &'a str,
                topic: &'a str,
                partition: i32,
            ) -> impl Future<Output = Result<Option<String>, ClientError>> + Send + 'a {
                <$t>::saved_position(self, group, topic, partition)
            }
            fn save_position<'a>(
                &'a mut self,
                group: &'a str,
                topic: &'a str,
                partition: i32,
                offset: i64,
                position: &'a str,
            ) -> impl Future<Output = Result<(), ClientError>> + Send + 'a {
                <$t>::save_position(self, group, topic, partition, offset, position)
            }
        }
    };
}
broker_for!(Client);
broker_for!(Routed);

/// What a pipeline inside OpenDuct is run with: its file, what its workspace's topics are named on the broker
/// beginning with, its own group (which a studio names so no two workspaces' pipelines share one), the secrets it
/// names (from its workspace's credentials), and whether it may reach this machine's own network.
pub(crate) struct Inside {
    pub(crate) source: String,
    pub(crate) topic_prefix: String,
    pub(crate) group: String,
    pub(crate) named: std::collections::BTreeMap<String, String>,
    pub(crate) private_network: bool,
}

/// Runs a pipeline inside OpenDuct against `broker` - following each partition's leader round a cluster - until
/// `stop` is told so; its counts shown in `live` as it goes. It reads no file and nothing of the environment.
pub(crate) async fn run_inside(
    inside: Inside,
    broker: &str,
    mut stop: tokio::sync::watch::Receiver<bool>,
    live: Progress,
) -> Result<(), String> {
    let Inside {
        source,
        topic_prefix,
        group,
        named,
        private_network,
    } = inside;
    let (source, topic_prefix, group) = (source.as_str(), topic_prefix.as_str(), group.as_str());
    let mut pipeline = Pipeline::parse(source)?;
    group.clone_into(&mut pipeline.group);
    // Its workspace's own topics: what it reads and writes, and where it sets messages aside - not a connector's.
    if pipeline.source.is_none() {
        pipeline.from_topic = format!("{topic_prefix}{}", pipeline.from_topic);
    }
    if pipeline.sink.is_none() {
        pipeline.to_topic = format!("{topic_prefix}{}", pipeline.to_topic);
    }
    if let Some(topic) = &mut pipeline.error_topic {
        *topic = format!("{topic_prefix}{topic}");
    }
    let client = Routed::connect(broker, Duration::from_secs(10))
        .await
        .map_err(|e| format!("cannot reach the broker at {broker}: {e}"))?;
    let stopping = async move {
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
    };
    match &pipeline.source {
        Some(c) => {
            let source = Source::build(
                c,
                &pipeline.name,
                Limits::new(Secrets::resolve(named, false), private_network),
            )?;
            run_source(&pipeline, source, client, stopping, Some(live)).await
        }
        None => {
            let sink = pipeline
                .sink
                .as_ref()
                .map(|c| {
                    Sink::build(
                        c,
                        &pipeline.name,
                        Limits::new(Secrets::resolve(named, false), private_network),
                    )
                })
                .transpose()?;
            run_reader(&pipeline, sink, client, stopping, Some(live)).await
        }
    }
}

/// Where a pipeline run inside OpenDuct shows how far it has got: read, written and set aside, since it started.
pub(crate) type Progress = Arc<dyn Fn(u64, u64, u64) + Send + Sync>;

#[derive(Default)]
struct Counts {
    read: u64,
    written: u64,
    set_aside: u64,
    /// Inside OpenDuct: where the counts are shown; what the command line prints goes to the log instead.
    live: Option<Progress>,
}

impl Counts {
    fn inside(live: Option<Progress>) -> Self {
        Self {
            live,
            ..Self::default()
        }
    }

    /// What the command line prints; inside OpenDuct, a line of the log.
    fn say(&self, text: &str) {
        if self.live.is_some() {
            tracing::info!("{text}");
        } else {
            eprintln!("{text}");
        }
    }

    fn publish(&self) {
        if let Some(live) = &self.live {
            live(self.read, self.written, self.set_aside);
        }
    }
}

fn report(name: &str, counts: &Counts) {
    counts.publish();
    counts.say(&format!(
        "pipeline {name}: read {}, wrote {}, set aside {}",
        counts.read, counts.written, counts.set_aside
    ));
}

async fn partitions_of<C: Broker>(client: &mut C, topic: &str, doing: &str) -> Result<i32, String> {
    client
        .partition_count(topic)
        .await
        .map_err(|e| format!("cannot {doing} {topic}: {e} (create it with openduct topic create {topic})"))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Where a message goes: by its key, or in turns.
fn partition_for(key: Option<&[u8]>, partitions: i32, turn: i32) -> i32 {
    key.map_or(turn % partitions.max(1), |k| {
        records::partition_for(k, partitions)
    })
}

fn outgoing(output: Output) -> Outgoing {
    Outgoing {
        key: output.key.map(|k| Bytes::from(k.into_bytes())),
        value: Some(Bytes::from(output.value)),
        headers: output
            .headers
            .into_iter()
            .map(|(k, v)| (k, Bytes::from(v)))
            .collect(),
    }
}

/// A message set aside: unchanged, with why, by whom, and from where.
fn set_aside(
    pipeline: &Pipeline,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: &[(String, Vec<u8>)],
    reason: String,
    source: String,
) -> Outgoing {
    let mut headers: Vec<(String, Bytes)> = headers
        .iter()
        .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
        .collect();
    headers.push(("openduct-error".into(), Bytes::from(reason.into_bytes())));
    headers.push((
        "openduct-pipeline".into(),
        Bytes::from(pipeline.name.clone().into_bytes()),
    ));
    headers.push(("openduct-source".into(), Bytes::from(source.into_bytes())));
    Outgoing { key, value, headers }
}

async fn commit<C: Broker>(
    client: &mut C,
    member: &Membership,
    durable: BTreeMap<i32, i64>,
    name: &str,
) -> Result<GroupState, String> {
    if durable.is_empty() {
        return Ok(GroupState::Stable);
    }
    let offsets: Vec<(i32, i64)> = durable.into_iter().collect();
    client
        .commit_member(member, &offsets)
        .await
        .map_err(|e| format!("pipeline {name}: cannot record its progress: {e}"))
}

// ------------------------------------------------------------------------------------------------------ from a topic

/// Read a topic as one member of the pipeline's group; write each result to a topic or to a sink.
async fn run_reader<C: Broker>(
    pipeline: &Pipeline,
    mut sink: Option<Sink>,
    mut client: C,
    stop: impl Future<Output = ()> + Send,
    live: Option<Progress>,
) -> Result<(), String> {
    let mut counts = Counts::inside(live);
    let name = pipeline.name.clone();
    tokio::pin!(stop);
    // Setting up waits on the broker (its topics, joining the group): a stop asked for meanwhile ends it there.
    let setup = async {
        let from_partitions = partitions_of(&mut client, &pipeline.from_topic, "read").await?;
        let to_partitions = match &sink {
            None => partitions_of(&mut client, &pipeline.to_topic, "write to").await?,
            Some(_) => 0,
        };
        let error_partitions = match &pipeline.error_topic {
            Some(topic) => Some(partitions_of(&mut client, topic, "set messages aside in").await?),
            None => None,
        };
        if let Some(s) = &sink {
            counts.say(&format!("pipeline {name}: writing to {}", s.describe()));
        }
        let member = join(&mut client, pipeline, from_partitions, None, &counts).await?;
        let next = starting_offsets(&mut client, pipeline, &member).await?;
        Ok::<_, String>((from_partitions, to_partitions, error_partitions, member, next))
    };
    let (from_partitions, to_partitions, error_partitions, mut member, mut next) = tokio::select! {
        ready = setup => ready?,
        () = &mut stop => return Ok(()),
    };
    let (mut last_beat, mut last_report) = (Instant::now(), Instant::now());
    let mut turn = 0i32;
    loop {
        let wanted: Vec<(i32, i64)> = next.iter().map(|(p, o)| (*p, *o)).collect();
        let fetched = if wanted.is_empty() {
            // More copies than partitions: this one waits, ready to take over.
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(1)) => Vec::new(),
                () = &mut stop => break,
            }
        } else {
            tokio::select! {
                fetched = client.fetch(&pipeline.from_topic, &wanted, 1_000, FETCH_BYTES) => {
                    fetched.map_err(|e| format!("pipeline {name}: cannot read {}: {e}", pipeline.from_topic))?
                }
                () = &mut stop => break,
            }
        };
        let mut writes: Vec<(String, i32, Outgoing)> = Vec::new();
        let mut for_sink: Vec<SinkRecord> = Vec::new();
        let mut moved: BTreeMap<i32, i64> = BTreeMap::new();
        let mut stopped_at: Option<(i32, i64, String)> = None;
        'answers: for answer in fetched {
            if answer.error_code != 0 {
                return Err(format!(
                    "pipeline {name}: partition {} of {} answered with error code {}",
                    answer.partition, pipeline.from_topic, answer.error_code
                ));
            }
            let from = next.get(&answer.partition).copied().unwrap_or(0);
            let mut batches = answer.batches;
            while !batches.is_empty() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                for record in set.records {
                    if record.offset < from || record.control {
                        continue;
                    }
                    let headers: Vec<(String, Vec<u8>)> = record
                        .headers
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.as_ref().map(|b| b.to_vec()).unwrap_or_default()))
                        .collect();
                    let message = Message {
                        key: record.key.as_deref(),
                        value: record.value.as_deref(),
                        headers: &headers,
                        topic: &pipeline.from_topic,
                        partition: answer.partition,
                        offset: record.offset,
                        timestamp_ms: record.timestamp,
                    };
                    counts.read += 1;
                    match pipeline.process(&message) {
                        Ok(outputs) => {
                            for output in outputs {
                                counts.written += 1;
                                if sink.is_some() {
                                    for_sink.push(SinkRecord {
                                        key: output.key,
                                        value: output.value,
                                        headers: output.headers,
                                        partition: answer.partition,
                                        offset: record.offset,
                                    });
                                } else {
                                    let p = partition_for(
                                        output.key.as_ref().map(|k| k.as_bytes()),
                                        to_partitions,
                                        turn,
                                    );
                                    writes.push((pipeline.to_topic.clone(), p, outgoing(output)));
                                }
                            }
                        }
                        Err(failure) => match (&pipeline.error_topic, error_partitions) {
                            (Some(topic), Some(partitions)) => {
                                let source =
                                    format!("{}/{}/{}", pipeline.from_topic, answer.partition, record.offset);
                                let p = partition_for(record.key.as_deref(), partitions, turn);
                                let aside = set_aside(
                                    pipeline,
                                    record.key.clone(),
                                    record.value.clone(),
                                    &headers,
                                    failure.reason,
                                    source,
                                );
                                writes.push((topic.clone(), p, aside));
                                counts.set_aside += 1;
                            }
                            _ => {
                                counts.read -= 1;
                                stopped_at = Some((answer.partition, record.offset, failure.reason));
                                break 'answers;
                            }
                        },
                    }
                    moved.insert(answer.partition, record.offset + 1);
                }
            }
        }
        turn = turn.wrapping_add(1);
        // Read on from here next time, whether or not a gathering sink has stored it yet: its progress is
        // committed only when it writes what it holds.
        next.extend(moved.iter().map(|(p, o)| (*p, *o)));
        // Stored first, then committed: a crash in between repeats a batch rather than losing it.
        let durable = match &mut sink {
            None => {
                write_all(&mut client, &writes)
                    .await
                    .map_err(|e| format!("pipeline {name}: {e}"))?;
                moved
            }
            Some(s) => {
                let outcome = s
                    .send(for_sink, moved)
                    .await
                    .map_err(|e| format!("pipeline {name}: {e}"))?;
                for (record, reason) in outcome.rejected {
                    let (Some(topic), Some(partitions)) = (&pipeline.error_topic, error_partitions) else {
                        let _ = client.leave_group(&member).await;
                        report(&name, &counts);
                        return Err(format!(
                            "pipeline {name} stopped: {} refused the message from {}/{}/{}: {reason}",
                            pipeline.to_topic, pipeline.from_topic, record.partition, record.offset
                        ));
                    };
                    let source = format!("{}/{}/{}", pipeline.from_topic, record.partition, record.offset);
                    let key = record.key.map(|k| Bytes::from(k.into_bytes()));
                    let p = partition_for(key.as_deref(), partitions, turn);
                    counts.written = counts.written.saturating_sub(1);
                    counts.set_aside += 1;
                    let value = Some(Bytes::from(record.value));
                    writes.push((
                        topic.clone(),
                        p,
                        set_aside(pipeline, key, value, &record.headers, reason, source),
                    ));
                }
                write_all(&mut client, &writes)
                    .await
                    .map_err(|e| format!("pipeline {name}: {e}"))?;
                let mut durable = outcome.durable;
                durable.extend(
                    s.tick()
                        .await
                        .map_err(|e| format!("pipeline {name}: {e}"))?
                        .durable,
                );
                durable
            }
        };
        if commit(&mut client, &member, durable, &name).await? == GroupState::Rebalancing {
            member = join(&mut client, pipeline, from_partitions, Some(&member), &counts).await?;
            next = starting_offsets(&mut client, pipeline, &member).await?;
        }
        if let Some((partition, offset, reason)) = stopped_at {
            let _ = client.leave_group(&member).await;
            report(&name, &counts);
            return Err(format!(
                "pipeline {name} stopped at {}/{partition}/{offset}: {reason}\n(give it an on_error topic to set such \
                 messages aside and carry on; it starts again from this message)",
                pipeline.from_topic
            ));
        }
        if last_beat.elapsed() >= HEARTBEAT {
            last_beat = Instant::now();
            if client
                .heartbeat(&member)
                .await
                .map_err(|e| format!("pipeline {name}: {e}"))?
                == GroupState::Rebalancing
            {
                // Partitions are about to move: write out what a gathering sink holds, and commit it, first.
                if let Some(s) = &mut sink {
                    let held = s.close().await.map_err(|e| format!("pipeline {name}: {e}"))?;
                    commit(&mut client, &member, held.durable, &name).await?;
                }
                member = join(&mut client, pipeline, from_partitions, Some(&member), &counts).await?;
                next = starting_offsets(&mut client, pipeline, &member).await?;
            }
        }
        if last_report.elapsed() >= REPORT {
            last_report = Instant::now();
            report(&name, &counts);
        }
    }
    if let Some(s) = &mut sink {
        let held = s.close().await.map_err(|e| format!("pipeline {name}: {e}"))?;
        commit(&mut client, &member, held.durable, &name).await?;
    }
    let _ = client.leave_group(&member).await;
    report(&name, &counts);
    Ok(())
}

// ------------------------------------------------------------------------------------------------------ from a source

/// Read from a connector; write each result to the pipeline's topic, then save the source's position, then tell the
/// source its messages are safe.
async fn run_source<C: Broker>(
    pipeline: &Pipeline,
    mut source: Source,
    mut client: C,
    stop: impl Future<Output = ()> + Send,
    live: Option<Progress>,
) -> Result<(), String> {
    let mut counts = Counts::inside(live);
    let name = pipeline.name.clone();
    tokio::pin!(stop);
    // Setting up waits on the broker and the source: a stop asked for meanwhile ends it there.
    let setup = async {
        let to_partitions = partitions_of(&mut client, &pipeline.to_topic, "write to").await?;
        let error_partitions = match &pipeline.error_topic {
            Some(topic) => Some(partitions_of(&mut client, topic, "set messages aside in").await?),
            None => None,
        };
        let position = client
            .saved_position(&pipeline.group, &pipeline.to_topic, 0)
            .await
            .map_err(|e| format!("pipeline {name}: cannot read its saved position: {e}"))?;
        source.start().await?;
        Ok::<_, String>((to_partitions, error_partitions, position))
    };
    let (to_partitions, error_partitions, mut position) = tokio::select! {
        ready = setup => ready?,
        () = &mut stop => return Ok(()),
    };
    counts.say(&format!(
        "pipeline {name}: reading {}, writing {}",
        source.describe(),
        pipeline.to_topic
    ));
    let mut last_report = Instant::now();
    let mut turn = 0i32;
    let mut sequence = 0i64;
    loop {
        let batch = tokio::select! {
            batch = source.next(position.as_deref()) => batch.map_err(|e| format!("pipeline {name}: {e}"))?,
            () = &mut stop => break,
        };
        let mut writes: Vec<(String, i32, Outgoing)> = Vec::new();
        let mut acks = Vec::new();
        let mut failure: Option<String> = None;
        for message in batch.messages {
            if let Some(ack) = message.ack {
                acks.push(ack);
            }
            if failure.is_some() {
                continue;
            }
            let input = Message {
                key: message.key.as_deref(),
                value: message.value.as_deref(),
                headers: &message.headers,
                topic: &pipeline.from_topic,
                partition: 0,
                offset: sequence,
                timestamp_ms: now_ms(),
            };
            sequence += 1;
            counts.read += 1;
            match pipeline.process(&input) {
                Ok(outputs) => {
                    for output in outputs {
                        let p = partition_for(output.key.as_ref().map(|k| k.as_bytes()), to_partitions, turn);
                        writes.push((pipeline.to_topic.clone(), p, outgoing(output)));
                        counts.written += 1;
                    }
                }
                Err(f) => match (&pipeline.error_topic, error_partitions) {
                    (Some(topic), Some(partitions)) => {
                        let key = message.key.clone().map(Bytes::from);
                        let p = partition_for(key.as_deref(), partitions, turn);
                        let value = message.value.map(Bytes::from);
                        writes.push((
                            topic.clone(),
                            p,
                            set_aside(
                                pipeline,
                                key,
                                value,
                                &message.headers,
                                f.reason,
                                pipeline.from_topic.clone(),
                            ),
                        ));
                        counts.set_aside += 1;
                    }
                    _ => failure = Some(f.reason),
                },
            }
        }
        turn = turn.wrapping_add(1);
        if let Err(e) = write_all(&mut client, &writes).await {
            for ack in acks {
                let _ = ack.send(Err(e.clone()));
            }
            return Err(format!("pipeline {name}: {e}"));
        }
        if failure.is_none() && batch.position.is_some() && batch.position != position {
            position = batch.position;
            if let Some(p) = &position {
                client
                    .save_position(&pipeline.group, &pipeline.to_topic, 0, sequence, p)
                    .await
                    .map_err(|e| format!("pipeline {name}: cannot save its position: {e}"))?;
            }
        }
        for ack in acks {
            let _ = ack.send(if failure.is_some() {
                Err("not stored".to_owned())
            } else {
                Ok(())
            });
        }
        if let Some(reason) = failure {
            report(&name, &counts);
            return Err(format!(
                "pipeline {name} stopped: {reason}\n(give it an on_error topic to set such messages aside and carry on)"
            ));
        }
        if last_report.elapsed() >= REPORT {
            last_report = Instant::now();
            report(&name, &counts);
        }
    }
    source.stop().await;
    report(&name, &counts);
    Ok(())
}

// ------------------------------------------------------------------------------------------------------ shared

async fn join<C: Broker>(
    client: &mut C,
    pipeline: &Pipeline,
    partitions: i32,
    previous: Option<&Membership>,
    counts: &Counts,
) -> Result<Membership, String> {
    let member = client
        .join_group(
            &pipeline.group,
            &pipeline.from_topic,
            partitions,
            SESSION_MS,
            previous,
        )
        .await
        .map_err(|e| format!("pipeline {}: {e}", pipeline.name))?;
    counts.say(&format!(
        "pipeline {}: reading {} partitions {:?} as group {}, writing {}",
        pipeline.name, pipeline.from_topic, member.partitions, pipeline.group, pipeline.to_topic
    ));
    Ok(member)
}

/// Where to read each partition from: the group's last commit, or the pipeline's `start` for a partition it has never
/// read.
async fn starting_offsets<C: Broker>(
    client: &mut C,
    pipeline: &Pipeline,
    member: &Membership,
) -> Result<BTreeMap<i32, i64>, String> {
    let fail = |e: openduct_broker::client::ClientError| format!("pipeline {}: {e}", pipeline.name);
    let committed: BTreeMap<i32, i64> = client
        .committed(&pipeline.group, &pipeline.from_topic, &member.partitions)
        .await
        .map_err(fail)?
        .into_iter()
        .collect();
    let mut out = BTreeMap::new();
    for &p in &member.partitions {
        let offset = match committed.get(&p) {
            Some(o) if *o >= 0 => *o,
            _ => {
                let at = if pipeline.start == Start::Earliest {
                    EARLIEST
                } else {
                    LATEST
                };
                client
                    .list_offset(&pipeline.from_topic, p, at)
                    .await
                    .map_err(fail)?
            }
        };
        out.insert(p, offset);
    }
    Ok(out)
}

/// Each topic-partition's messages as one batch, in the order they were made.
async fn write_all<C: Broker>(client: &mut C, writes: &[(String, i32, Outgoing)]) -> Result<(), String> {
    let mut batches: BTreeMap<(String, i32), Vec<Outgoing>> = BTreeMap::new();
    for (topic, partition, message) in writes {
        batches
            .entry((topic.clone(), *partition))
            .or_default()
            .push(message.clone());
    }
    let now = now_ms();
    for ((topic, partition), messages) in batches {
        for chunk in messages.chunks(1000) {
            client
                .produce(&topic, partition, chunk, now)
                .await
                .map_err(|e| format!("cannot write to {topic}: {e}"))?;
        }
    }
    Ok(())
}

/// The broker's client, or nothing when the pipeline is asked to stop while it is still connecting.
async fn connect_unless_stopped(
    target: &Target,
    stop: std::pin::Pin<&mut impl Future<Output = ()>>,
) -> Result<Option<Client>, String> {
    tokio::select! {
        client = connect(target) => client.map(Some),
        () = stop => Ok(None),
    }
}

/// Ctrl-C, or SIGTERM where there is one. The handlers are in place when this is called, not when its answer is
/// first awaited: a pipeline asked to stop while it is still starting stops cleanly instead of being killed.
fn stopped() -> impl Future<Output = ()> + Send + 'static {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let terminate = signal(SignalKind::terminate()).ok();
        let interrupt = signal(SignalKind::interrupt()).ok();
        async move {
            match (terminate, interrupt) {
                (Some(mut terminate), Some(mut interrupt)) => {
                    tokio::select! {
                        _ = terminate.recv() => {}
                        _ = interrupt.recv() => {}
                    }
                }
                _ => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
    }
    #[cfg(not(unix))]
    async {
        let _ = tokio::signal::ctrl_c().await;
    }
}
