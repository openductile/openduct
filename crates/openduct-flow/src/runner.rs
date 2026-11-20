// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow kept running: its events read from its topic as a consumer group (so the nodes of a cluster share
//! them), each run recorded in the workflow's history, an event whose run failed set aside with the reason - and the
//! group's position committed only once all that is stored, so a run is never lost: a crash repeats it instead.
//!
//! Each workflow has three topics of its own: `<id>.in` (what its webhook, schedule and Run button write), `<id>.runs`
//! (its history, kept 14 days) and `<id>.failed` (events set aside, kept until run again). A workflow started by a
//! topic reads that topic instead of `<id>.in`.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{
    ClientError, EARLIEST, Fetched, GroupState, LATEST, Membership, Outgoing, Routed, Shared, records,
};
use openduct_expr::Value;
use serde_json::{Value as Json, json};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use crate::model::{Trigger, Workflow};
use crate::run::{Effects, Event, Outcome, run};

/// The prefix of every topic and group a workflow has.
pub const PREFIX: &str = "__openduct.flow.";
/// How long a workflow's history is kept.
pub const HISTORY: Duration = Duration::from_secs(14 * 24 * 3600);
/// The partitions of a workflow's own topic: as many nodes as this can share its runs.
pub const PARTITIONS: i32 = 4;
/// The largest event kept in a run's history, in bytes as JSON; a larger one is shown cut short.
const SHOWN_EVENT: usize = 64 * 1024;
const SESSION_MS: i32 = 30_000;
const HEARTBEAT: Duration = Duration::from_secs(3);

/// The names of a workflow's topics and group.
#[derive(Debug, Clone)]
pub struct Topics {
    pub input: String,
    pub runs: String,
    pub failed: String,
    pub group: String,
}

impl Topics {
    pub fn of(id: &str) -> Self {
        Self {
            input: format!("{PREFIX}{id}.in"),
            runs: format!("{PREFIX}{id}.runs"),
            failed: format!("{PREFIX}{id}.failed"),
            group: format!("{PREFIX}{id}"),
        }
    }
}

/// An event as a workflow's own topic holds it.
pub fn envelope(value: &Json, details: &Json) -> Vec<u8> {
    json!({ "value": value, "details": details })
        .to_string()
        .into_bytes()
}

/// Makes the workflow's topics that are missing.
pub async fn prepare(client: &mut Routed, id: &str) -> Result<(), String> {
    let topics = Topics::of(id);
    let have: Vec<String> = client
        .topics()
        .await
        .map_err(|e| format!("cannot list topics: {e}"))?
        .into_iter()
        .map(|t| t.name)
        .collect();
    let history = i64::try_from(HISTORY.as_millis()).unwrap_or(i64::MAX);
    for (name, partitions, retention) in [
        (&topics.input, PARTITIONS, None),
        (&topics.runs, 1, Some(history)),
        (&topics.failed, 1, None),
    ] {
        if have.contains(name) {
            continue;
        }
        match client.create_topic(name, partitions, retention).await {
            Ok(()) | Err(openduct_broker::client::ClientError::Refused { code: 36, .. }) => {}
            Err(e) => return Err(format!("cannot make the topic {name}: {e}")),
        }
    }
    Ok(())
}

/// Writes an event to the workflow's own topic, for a run (a webhook's delivery, a schedule's tick, the Run button):
/// the id its run will have.
pub async fn queue(client: &mut Routed, id: &str, value: &Json, details: &Json) -> Result<String, String> {
    let topics = Topics::of(id);
    let partitions = client
        .partition_count(&topics.input)
        .await
        .map_err(|e| format!("the workflow is not ready to run: {e}"))?;
    let turn = turn_for(now_ms(), partitions);
    let message = Outgoing {
        key: None,
        value: Some(Bytes::from(envelope(value, details))),
        headers: Vec::new(),
    };
    client
        .produce(&topics.input, turn, &[message], now_ms())
        .await
        .map(|offset| format!("{turn}-{offset}"))
        .map_err(|e| format!("cannot keep the event: {e}"))
}

/// The header a CatchErr record counts its hops in: how many "when a workflow fails" runs, each failing, led to it.
const HOPS: &str = "openduct-hops";

/// The most hops a failure takes: past it, a chain of workflows started by each other's failures stops.
const MAX_HOPS: u32 = 3;

fn hops_of(record: &kafka_protocol::records::Record) -> u32 {
    record
        .headers
        .iter()
        .find(|(k, _)| k.as_str() == HOPS)
        .and_then(|(_, v)| v.as_ref())
        .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
        .unwrap_or(0)
}

/// The partition every scheduled run is queued to: each node of a cluster queues each one, and the one member of the
/// group that reads this partition runs each time once. It holds nothing else (`turn_for`), so the claim of a time -
/// its place committed before it runs (`Runner::claim`) - is never held back by a run of another kind before it.
const SCHEDULE_PARTITION: i32 = 0;

/// The partition a run of any other kind (a webhook's delivery, the Run button) is queued to: turn by turn over all but
/// the schedule's partition - over all of them only in a topic that has no other.
fn turn_for(now: i64, partitions: i32) -> i32 {
    let spread = i64::from(partitions.max(1));
    let turn = if spread > 1 {
        1 + now.rem_euclid(spread - 1)
    } else {
        0
    };
    i32::try_from(turn).unwrap_or(0)
}

/// How many records before a member's starting place it looks back through for the latest scheduled time claimed:
/// every node queues each time, and one that queued late puts an earlier time after a later one.
const CLAIM_WINDOW: i64 = 64;

/// Queues the scheduled run due at `due` (milliseconds since 1970).
async fn queue_scheduled(client: &mut Routed, id: &str, due: i64) -> Result<(), String> {
    let topics = Topics::of(id);
    let at = iso(due);
    let message = Outgoing {
        key: Some(Bytes::from(format!("schedule/{due}").into_bytes())),
        value: Some(Bytes::from(envelope(
            &json!({ "scheduled_at": at }),
            &json!({ "trigger": "schedule", "at": at, "slot": due }),
        ))),
        headers: Vec::new(),
    };
    client
        .produce(&topics.input, SCHEDULE_PARTITION, &[message], now_ms())
        .await
        .map(|_| ())
        .map_err(|e| format!("cannot keep the event: {e}"))
}

/// The latest scheduled time claimed in the schedule's partition before `next` - what the member that read it before
/// ran, or claimed to run - looking back over the last `CLAIM_WINDOW` records.
async fn slot_before(client: &mut Routed, topic: &str, next: &BTreeMap<i32, i64>) -> Option<i64> {
    let next = next.get(&SCHEDULE_PARTITION).copied().filter(|o| *o > 0)?;
    let earliest = client
        .list_offset(topic, SCHEDULE_PARTITION, EARLIEST)
        .await
        .unwrap_or(0);
    let from = (next - CLAIM_WINDOW).max(earliest).max(0);
    let fetched = client
        .fetch(topic, &[(SCHEDULE_PARTITION, from)], 0, 256 * 1024)
        .await
        .ok()?;
    let answer = fetched
        .into_iter()
        .find(|f| f.partition == SCHEDULE_PARTITION && f.error_code == 0)?;
    let mut batches = answer.batches;
    let mut latest = None;
    while !batches.is_empty() {
        let Ok(set) =
            RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
        else {
            break;
        };
        for record in set.records.iter().filter(|r| r.offset >= from && r.offset < next) {
            latest = latest.max(slot_of(&event_of(
                record,
                (topic, SCHEDULE_PARTITION, record.offset),
                true,
            )));
        }
    }
    latest
}

/// A record another workflow sent to CatchErr, as the run it starts sees it: its value as it came, and in its details
/// which workflow it failed in, at which run, and why.
fn failed_event(record: &kafka_protocol::records::Record, workflow: &str, event: Event) -> Event {
    let header = |name: &str| {
        record
            .headers
            .iter()
            .find(|(k, _)| k.as_str() == name)
            .and_then(|(_, v)| v.as_ref())
            .map(|v| String::from_utf8_lossy(v).into_owned())
    };
    let original: Json = serde_json::from_str(&event.details.to_json()).unwrap_or(Json::Null);
    Event {
        value: event.value,
        details: Value::from_json(&json!({
            "trigger": "failed",
            "workflow": workflow,
            "run": header("openduct-run"),
            "error": header("openduct-error"),
            "original": original,
        })),
    }
}

/// The time slot of a scheduled run, from its event's details.
fn slot_of(event: &Event) -> Option<i64> {
    let Value::Record(details) = &event.details else {
        return None;
    };
    details
        .get("slot")
        .and_then(|s| crate::template::as_text(s).parse::<i64>().ok())
}

/// Runs `workflow` until `stop` says so: up to its runs at once, each record's run kept in its history (and set aside
/// to CatchErr when it failed) before its place is committed - so a run is never lost, a crash repeating it instead.
/// Records with the same key in a partition run one after another, in order; others at once.
pub async fn serve(
    workflow: Arc<Workflow>,
    shared: Arc<Shared>,
    effects: Arc<dyn Effects>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut runner = Runner::start(workflow, shared, effects).await?;
    let mut beat = tokio::time::interval(HEARTBEAT);
    beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut tick = tokio::time::interval(COMMIT_EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut fetching: Fetching = Box::pin(std::future::pending());
    let mut is_fetching = false;
    loop {
        if *stop.borrow() {
            break;
        }
        runner.start_held();
        if !is_fetching && runner.wants_more() {
            let shared = Arc::clone(&runner.shared);
            let source = runner.source.clone();
            let from: Vec<(i32, i64)> = runner.progress.fetch_at.iter().map(|(p, o)| (*p, *o)).collect();
            fetching = Box::pin(async move { shared.fetch(&source, &from).await });
            is_fetching = true;
        }
        let rejoin = tokio::select! {
            fetched = &mut fetching, if is_fetching => {
                is_fetching = false;
                runner.take_in(fetched.map_err(|e| runner.fail(&e))?)?;
                runner.claim().await? == GroupState::Rebalancing
            }
            Some(done) = runner.running.join_next(), if !runner.running.is_empty() => {
                runner.finished(done)?;
                false
            }
            _ = tick.tick() => runner.commit().await? == GroupState::Rebalancing,
            _ = beat.tick() => runner.heartbeat().await? == GroupState::Rebalancing,
            _ = stop.changed() => break,
        };
        if rejoin {
            // What is being read is for the partitions held before.
            is_fetching = false;
            runner.rejoin().await?;
        }
    }
    drop(fetching);
    runner.launch_claimed();
    runner.drain(GRACE).await?;
    // How far it got, and leaving the group - briefly: a broker already gone does not hold up a stop.
    let _ = tokio::time::timeout(LAST_WORDS, async {
        let _ = runner.commit().await;
        if let Ok(mut client) = runner.shared.lend().await {
            let _ = client.leave_group(&runner.member).await;
        }
    })
    .await;
    Ok(())
}

/// How many runs a workflow has going at once on each node, when it does not say.
pub const RUNS_AT_ONCE: u32 = 10;
/// The most a workflow may have going at once on each node.
pub const MAX_RUNS_AT_ONCE: u32 = 50;
/// How often finished runs' places are committed: a crash repeats at most this long's runs. Each commit is a write
/// the store flushes to its device, so not more often.
const COMMIT_EVERY: Duration = Duration::from_secs(1);
/// How long a stopping runner, or one whose group is shared out again, lets its runs finish.
const GRACE: Duration = Duration::from_secs(10);
/// How long a stopping runner takes to commit and leave its group.
const LAST_WORDS: Duration = Duration::from_secs(3);

type Fetching = Pin<Box<dyn Future<Output = Result<Vec<Fetched>, ClientError>> + Send>>;

/// A record taken in, its run not started yet.
struct Item {
    partition: i32,
    offset: i64,
    key: Option<Bytes>,
    event: Event,
    original: Option<Bytes>,
    hops: u32,
    /// A scheduled time: committed (claimed) before it runs, so it runs at most once - a run cut short by a node's
    /// loss is not run again by the member that takes the partition over.
    claimed: bool,
}

/// A run finished: where its record was, its key, and whether its run was kept.
type Finished = (i32, i64, Option<Bytes>, Result<(), String>);

/// Where a runner is in each partition.
struct Progress {
    /// Where to read next.
    fetch_at: BTreeMap<i32, i64>,
    /// Records taken in whose runs have not finished.
    open: BTreeMap<i32, BTreeSet<i64>>,
    /// What was committed last.
    committed: BTreeMap<i32, i64>,
}

impl Progress {
    fn new(next: BTreeMap<i32, i64>) -> Self {
        Self {
            committed: next.clone(),
            fetch_at: next,
            open: BTreeMap::new(),
        }
    }

    /// What to commit: in each partition, the first record whose run has not finished (or the next to read).
    fn due(&self) -> Vec<(i32, i64)> {
        self.fetch_at
            .iter()
            .filter_map(|(p, at)| {
                let upto = self.open.get(p).and_then(|o| o.first().copied()).unwrap_or(*at);
                (self.committed.get(p) != Some(&upto)).then_some((*p, upto))
            })
            .collect()
    }
}

/// A workflow's runner on this node: its group membership, what it has read, and its runs going.
struct Runner {
    workflow: Arc<Workflow>,
    shared: Arc<Shared>,
    effects: Arc<dyn Effects>,
    topics: Arc<Topics>,
    source: String,
    earliest: bool,
    enveloped: bool,
    failed_from: Option<String>,
    partitions: i32,
    member: Membership,
    at_once: usize,
    progress: Progress,
    /// The latest scheduled time run: the same time queued by another node is run once - also by a member that has
    /// just taken the partition over, which starts from the time its last reader ran.
    last_slot: Option<i64>,
    /// A scheduled time was taken in and its claim (the commit past it) is still to be made.
    to_claim: bool,
    held: VecDeque<Item>,
    running: JoinSet<Finished>,
    /// Keys (in a partition) with a run going: the next record with one waits for it.
    busy: HashSet<(i32, Bytes)>,
}

impl Runner {
    async fn start(
        workflow: Arc<Workflow>,
        shared: Arc<Shared>,
        effects: Arc<dyn Effects>,
    ) -> Result<Self, String> {
        let fail = |e: ClientError| format!("workflow {}: {e}", workflow.id);
        let topics = Topics::of(&workflow.id);
        let mut client = shared.lend().await.map_err(fail)?;
        prepare(&mut client, &workflow.id).await?;
        let (source, earliest, enveloped) = match &workflow.trigger {
            Trigger::Topic { name, earliest } => (name.clone(), *earliest, false),
            // The other workflow's CatchErr, from what fails from now on (its topics made, should it never have run).
            Trigger::Failed { workflow: other } => {
                prepare(&mut client, other).await?;
                (Topics::of(other).failed, false, true)
            }
            _ => (topics.input.clone(), true, true),
        };
        let failed_from = match &workflow.trigger {
            Trigger::Failed { workflow: other } => Some(other.clone()),
            _ => None,
        };
        let partitions = client.partition_count(&source).await.map_err(fail)?;
        drop(client);
        let member = shared
            .lend_for_join()
            .await
            .map_err(fail)?
            .join_group(&topics.group, &source, partitions, SESSION_MS, None)
            .await
            .map_err(fail)?;
        let at_once = usize::try_from(workflow.runs_at_once.clamp(1, MAX_RUNS_AT_ONCE)).unwrap_or(1);
        let mut runner = Self {
            workflow,
            shared,
            effects,
            topics: Arc::new(topics),
            source,
            earliest,
            enveloped,
            failed_from,
            partitions,
            member,
            at_once,
            progress: Progress::new(BTreeMap::new()),
            last_slot: None,
            to_claim: false,
            held: VecDeque::new(),
            running: JoinSet::new(),
            busy: HashSet::new(),
        };
        runner.read_positions().await?;
        Ok(runner)
    }

    fn fail(&self, e: &ClientError) -> String {
        format!("workflow {}: {e}", self.workflow.id)
    }

    /// Where to read each of its partitions from, as the group last committed.
    async fn read_positions(&mut self) -> Result<(), String> {
        let mut client = self.shared.lend().await.map_err(|e| self.fail(&e))?;
        let next = positions(
            &mut client,
            &self.topics.group,
            &self.source,
            &self.member,
            self.earliest,
        )
        .await?;
        if self.enveloped {
            self.last_slot = self
                .last_slot
                .max(slot_before(&mut client, &self.source, &next).await);
        }
        self.progress = Progress::new(next);
        Ok(())
    }

    /// Whether to read more: it holds partitions, and few records wait to run.
    fn wants_more(&self) -> bool {
        !self.member.partitions.is_empty() && self.held.len() < self.at_once
    }

    /// The records read, each to run - those that are not run (a failure past its hops, a time already run) passed.
    fn take_in(&mut self, fetched: Vec<Fetched>) -> Result<(), String> {
        for answer in fetched {
            if answer.error_code != 0 {
                return Err(format!(
                    "workflow {}: partition {} of {} answered with error code {}",
                    self.workflow.id, answer.partition, self.source, answer.error_code
                ));
            }
            let from = self
                .progress
                .fetch_at
                .get(&answer.partition)
                .copied()
                .unwrap_or(0);
            let mut batches = answer.batches;
            while !batches.is_empty() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                for record in set.records {
                    if record.offset < from {
                        continue;
                    }
                    self.progress.fetch_at.insert(answer.partition, record.offset + 1);
                    if !record.control {
                        self.take(answer.partition, &record);
                    }
                }
            }
        }
        Ok(())
    }

    fn take(&mut self, partition: i32, record: &kafka_protocol::records::Record) {
        let mut event = event_of(
            record,
            (self.source.as_str(), partition, record.offset),
            self.enveloped,
        );
        let mut hops = 0;
        if let Some(other) = &self.failed_from {
            hops = hops_of(record) + 1;
            if hops > MAX_HOPS {
                return;
            }
            event = failed_event(record, other, event);
        }
        let mut claimed = false;
        if let Some(slot) = slot_of(&event).filter(|_| self.enveloped) {
            if self.last_slot.is_some_and(|last| slot <= last) {
                return;
            }
            self.last_slot = Some(slot);
            claimed = true;
            self.to_claim = true;
        }
        // A claimed time is not waited on before committing: its commit IS its claim, made before it runs.
        if !claimed {
            self.progress
                .open
                .entry(partition)
                .or_default()
                .insert(record.offset);
        }
        self.held.push_back(Item {
            partition,
            offset: record.offset,
            key: record.key.clone(),
            event,
            original: record.value.clone(),
            hops,
            claimed,
        });
    }

    /// Starts the records waiting, in order, while there is room: one whose key has a run going (or waiting before
    /// it) waits on.
    fn start_held(&mut self) {
        let mut blocked: HashSet<(i32, Bytes)> = HashSet::new();
        let mut kept = VecDeque::new();
        while let Some(item) = self.held.pop_front() {
            if self.running.len() >= self.at_once {
                kept.push_back(item);
                kept.extend(self.held.drain(..));
                break;
            }
            let lane = item.key.clone().map(|k| (item.partition, k));
            match lane {
                Some(lane) if self.busy.contains(&lane) || blocked.contains(&lane) => {
                    blocked.insert(lane);
                    kept.push_back(item);
                }
                lane => {
                    if let Some(lane) = lane {
                        self.busy.insert(lane);
                    }
                    self.launch(item);
                }
            }
        }
        self.held = kept;
    }

    fn launch(&mut self, item: Item) {
        let workflow = Arc::clone(&self.workflow);
        let effects = Arc::clone(&self.effects);
        let shared = Arc::clone(&self.shared);
        let topics = Arc::clone(&self.topics);
        self.running.spawn(async move {
            let outcome = run(&workflow, &item.event, effects.as_ref()).await;
            crate::meters::run(
                match outcome.outcome {
                    Outcome::Done => crate::meters::Ended::Done,
                    Outcome::Skipped { .. } => crate::meters::Ended::Skipped,
                    Outcome::Failed { .. } => crate::meters::Ended::Failed,
                },
                std::time::Duration::from_millis(outcome.ms),
            );
            let id = format!("{}-{}", item.partition, item.offset);
            let kept = keep(
                &shared,
                &topics,
                &id,
                &item.event,
                &outcome,
                item.original.as_ref(),
                item.hops,
            )
            .await;
            (item.partition, item.offset, item.key, kept)
        });
    }

    fn finished(&mut self, done: Result<Finished, tokio::task::JoinError>) -> Result<(), String> {
        let (partition, offset, key, kept) =
            done.map_err(|e| format!("workflow {}: a run stopped unexpectedly: {e}", self.workflow.id))?;
        if let Some(open) = self.progress.open.get_mut(&partition) {
            open.remove(&offset);
        }
        if let Some(key) = key {
            self.busy.remove(&(partition, key));
        }
        kept
    }

    /// Lets the runs going finish, for at most `within`; those still going then are stopped, to run again.
    async fn drain(&mut self, within: Duration) -> Result<(), String> {
        let until = tokio::time::Instant::now() + within;
        while !self.running.is_empty() {
            match tokio::time::timeout_at(until, self.running.join_next()).await {
                Ok(Some(done)) => self.finished(done)?,
                Ok(None) => break,
                Err(_) => {
                    self.running.abort_all();
                    while self.running.join_next().await.is_some() {}
                    break;
                }
            }
        }
        Ok(())
    }

    /// Claims the scheduled times just taken in, before any of them runs: their places are committed first, so a member
    /// that takes the partition over later starts past them and never runs one of those times again. A commit the
    /// group refuses (this member no longer holds the partition) is an error: nothing it took in runs here, and the
    /// member that holds the partition runs those times.
    async fn claim(&mut self) -> Result<GroupState, String> {
        if !std::mem::take(&mut self.to_claim) {
            return Ok(GroupState::Stable);
        }
        match self.commit().await {
            Ok(state) => Ok(state),
            Err(e) => {
                self.held.retain(|item| !item.claimed);
                Err(e)
            }
        }
    }

    /// Starts every claimed time still waiting, room or not: a time claimed is this member's to run, whether members
    /// come or go or it stops - no one else will.
    fn launch_claimed(&mut self) {
        let (claimed, rest): (VecDeque<Item>, VecDeque<Item>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|item| item.claimed);
        self.held = rest;
        for item in claimed {
            if let Some(key) = item.key.clone() {
                self.busy.insert((item.partition, key));
            }
            self.launch(item);
        }
    }

    /// Commits how far its runs have got; what the group says.
    async fn commit(&mut self) -> Result<GroupState, String> {
        let due = self.progress.due();
        if due.is_empty() {
            return Ok(GroupState::Stable);
        }
        let mut client = self.shared.lend().await.map_err(|e| self.fail(&e))?;
        let state = client
            .commit_member(&self.member, &due)
            .await
            .map_err(|e| self.fail(&e))?;
        self.progress.committed.extend(due);
        Ok(state)
    }

    async fn heartbeat(&mut self) -> Result<GroupState, String> {
        let mut client = self.shared.lend().await.map_err(|e| self.fail(&e))?;
        client.heartbeat(&self.member).await.map_err(|e| self.fail(&e))
    }

    /// Members came or went: its runs finish and are committed, then it joins again and reads on from where the
    /// group committed.
    async fn rejoin(&mut self) -> Result<(), String> {
        self.launch_claimed();
        self.drain(GRACE).await?;
        let _ = self.commit().await;
        self.held.clear();
        self.busy.clear();
        self.member = self
            .shared
            .lend_for_join()
            .await
            .map_err(|e| self.fail(&e))?
            .join_group(
                &self.topics.group,
                &self.source,
                self.partitions,
                SESSION_MS,
                Some(&self.member),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        self.read_positions().await
    }
}

/// Where to read each partition from: the group's last commit, or the start (or the end) for one never read.
async fn positions(
    client: &mut Routed,
    group: &str,
    source: &str,
    member: &Membership,
    earliest: bool,
) -> Result<BTreeMap<i32, i64>, String> {
    let committed: BTreeMap<i32, i64> = client
        .committed(group, source, &member.partitions)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect();
    let mut out = BTreeMap::new();
    for &p in &member.partitions {
        let offset = match committed.get(&p) {
            Some(o) if *o >= 0 => *o,
            _ => client
                .list_offset(source, p, if earliest { EARLIEST } else { LATEST })
                .await
                .map_err(|e| e.to_string())?,
        };
        out.insert(p, offset);
    }
    Ok(out)
}

fn event_of(
    record: &kafka_protocol::records::Record,
    (topic, partition, offset): (&str, i32, i64),
    enveloped: bool,
) -> Event {
    let bytes = record.value.as_deref().unwrap_or_default();
    let parsed = Value::parse_json(&String::from_utf8_lossy(bytes))
        .unwrap_or_else(|_| Value::text(String::from_utf8_lossy(bytes).as_ref()));
    if enveloped && let Value::Record(fields) = &parsed {
        return Event {
            value: fields.get("value").cloned().unwrap_or(Value::Null),
            details: fields.get("details").cloned().unwrap_or(Value::Null),
        };
    }
    let key = record
        .key
        .as_deref()
        .map(|k| String::from_utf8_lossy(k).into_owned());
    Event {
        value: parsed,
        details: Value::from_json(&json!({
            "trigger": "topic", "topic": topic, "partition": partition, "offset": offset, "key": key,
            "timestamp": record.timestamp,
        })),
    }
}

/// The run in the history; the event set aside when the run failed.
async fn keep(
    shared: &Shared,
    topics: &Topics,
    id: &str,
    event: &Event,
    outcome: &crate::run::RunRecord,
    original: Option<&Bytes>,
    hops: u32,
) -> Result<(), String> {
    let mut shown: Json = serde_json::from_str(&event.value.to_json()).unwrap_or(Json::Null);
    if shown.to_string().len() > SHOWN_EVENT {
        shown = json!({ "cut short": format!("an event of more than {SHOWN_EVENT} bytes") });
    }
    let mut history = outcome.to_json();
    if let Some(fields) = history.as_object_mut() {
        fields.insert("run".to_owned(), json!(id));
        fields.insert("at".to_owned(), json!(now_ms()));
        fields.insert("event".to_owned(), shown);
        fields.insert(
            "trigger".to_owned(),
            serde_json::from_str(&event.details.to_json()).unwrap_or(Json::Null),
        );
    }
    let message = Outgoing {
        key: Some(Bytes::from(id.as_bytes().to_vec())),
        value: Some(Bytes::from(history.to_string().into_bytes())),
        headers: Vec::new(),
    };
    // Written with whatever else the node writes at that moment: one request for many runs.
    shared
        .produce(&topics.runs, 0, vec![message])
        .await
        .map_err(|e| format!("cannot keep the run: {e}"))?;
    if let Outcome::Failed { at, error } = &outcome.outcome {
        let aside = Outgoing {
            key: Some(Bytes::from(id.as_bytes().to_vec())),
            value: original.cloned(),
            headers: vec![
                (
                    "openduct-error".to_owned(),
                    Bytes::from(format!("{at}: {error}").into_bytes()),
                ),
                ("openduct-run".to_owned(), Bytes::from(id.as_bytes().to_vec())),
                (HOPS.to_owned(), Bytes::from(hops.to_string().into_bytes())),
            ],
        };
        shared
            .produce(&topics.failed, 0, vec![aside])
            .await
            .map_err(|e| format!("cannot send the record to CatchErr: {e}"))?;
    }
    Ok(())
}

/// Writes a tick of `every` to the workflow's own topic, on the interval, counted from midnight UTC. In a cluster every
/// node writes each tick, to the same partition and naming its time: the member that reads that partition claims each
/// time (commits past it) before it runs it, so a schedule goes on while any node is up and never runs twice for one
/// time - across a node's loss too. A time whose run that loss cut short is not run again.
pub async fn schedule(
    id: String,
    every: Duration,
    shared: Arc<Shared>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let every_ms = i64::try_from(every.as_millis()).unwrap_or(i64::MAX).max(1);
    loop {
        let now = now_ms();
        let due = (now / every_ms + 1) * every_ms;
        let wait = Duration::from_millis(u64::try_from(due - now).unwrap_or(0));
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            _ = stop.changed() => return Ok(()),
        }
        if *stop.borrow() {
            return Ok(());
        }
        let queued = async {
            let mut client = shared.lend().await.map_err(|e| e.to_string())?;
            queue_scheduled(&mut client, &id, due).await
        };
        if let Err(e) = queued.await {
            tracing::warn!(workflow = id, error = e, "a scheduled run could not be queued");
        }
    }
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

/// Milliseconds since 1970 as RFC 3339 text in UTC.
pub fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).map_or_else(
        || ms.to_string(),
        |t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_of_another_kind_never_goes_to_the_schedules_partition() {
        let turns: std::collections::BTreeSet<i32> = (0..1000).map(|now| turn_for(now, PARTITIONS)).collect();
        assert!(!turns.contains(&SCHEDULE_PARTITION), "{turns:?}");
        assert_eq!(
            turns,
            (1..PARTITIONS).collect(),
            "spread over every other partition"
        );
        assert_eq!(turn_for(7, 1), 0, "a topic of one partition has no other");
    }
}
