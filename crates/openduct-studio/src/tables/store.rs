// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Where data tables and workflows' state are kept: one topic of the broker's own (one partition, compacted, so the
//! newest record of each key is what stays), read by every node in the same order, so every node holds the same
//! tables. Each table's definition, each row, each table's next id and each workflow's state is a record of its own,
//! by key; a row taken away is a record with no value, which compaction then takes away too.
//!
//! A change is made in a **turn**: a writer asks for the table's turn with a record of its own (`take`, with how
//! long it may hold it), reads the topic on to that record, and holds the turn when, at that place in the topic, no
//! other writer held it (or the one who did had run past its time). Every node reads the same records in the same
//! order, so all agree on who holds a turn. The holder plans its change on the rows as they are then, writes it, and
//! gives the turn back (`free`). Two runs changing the same row, on one node or two, so change it one after the
//! other: neither change is lost.
//!
//! A writer that stops while holding a turn holds it at most [`TURN`]; it gives up its change itself when the work
//! takes longer than [`WORK`], so it writes nothing once another may hold the turn (nodes' clocks agreeing to within
//! a second).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{ClientError, Outgoing, Routed, records};
use openduct_expr::{Record, Value};
use openduct_flow::tables::{
    Changes, Column, ColumnType, MAX_WORKSPACE_BYTES, Plan, Row, StatePlan, TableData, check_changes,
    check_state, coerce_kept,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use tokio::sync::Mutex;

/// The topic tables are kept in.
pub(crate) const TOPIC: &str = "__openduct.model.tables";
/// The longest a turn is held unless given back.
pub(crate) const TURN: Duration = Duration::from_secs(10);
/// The longest a holder works on a change before giving it up, well within its turn.
const WORK: Duration = Duration::from_secs(7);
/// The longest a writer waits for a turn.
const WAIT: Duration = Duration::from_secs(20);
/// The most bytes written at once: a change this large or smaller is written in one go.
const PART_BYTES: usize = 512 * 1024;
const READ_BYTES: i32 = 4 * 1024 * 1024;
const CONNECT: Duration = Duration::from_secs(10);
/// How long a forming cluster has to become ready.
const FORMING: Duration = Duration::from_secs(90);
/// How long records stay as written before compaction may take the replaced ones out.
const COMPACTION_LAG_MS: &str = "3600000";

/// A table's definition: its name in its workspace, and its columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableDef {
    pub schema: u32,
    pub id: String,
    pub workspace: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub created: u64,
    pub updated: u64,
    pub by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
}

impl TableDef {
    pub(crate) fn columns(&self) -> Vec<Column> {
        self.columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                kind: ColumnType::parse(&c.kind).unwrap_or(ColumnType::Json),
            })
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RowRecord {
    id: u64,
    values: Map<String, Json>,
    created: i64,
    updated: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Counter {
    next: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateRecord {
    values: Map<String, Json>,
    updated: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TurnRecord {
    /// `take` or `free`.
    op: String,
    by: String,
    at: i64,
    until: i64,
}

fn def_key(id: &str) -> String {
    format!("t/{id}")
}
fn row_key(table: &str, row: u64) -> String {
    format!("r/{table}/{row:020}")
}
fn counter_key(table: &str) -> String {
    format!("n/{table}")
}
fn state_key(workspace: &str, workflow: &str) -> String {
    format!("s/{}", state_id(workspace, workflow))
}
/// What a workflow's state is held by: its key without the kind.
fn state_id(workspace: &str, workflow: &str) -> String {
    format!("{workspace}/{workflow}")
}
fn turn_key(name: &str) -> String {
    format!("l/{name}")
}

/// A record to write: its key, and its value (`None` takes the key away).
pub(crate) type Write = (String, Option<Json>);

#[derive(Debug, Default)]
struct Inner {
    client: Option<Routed>,
    next: i64,
    defs: HashMap<String, TableDef>,
    tables: HashMap<String, Arc<TableData>>,
    bytes: HashMap<String, usize>,
    states: HashMap<String, (Arc<Record>, i64)>,
    turns: HashMap<String, (String, i64)>,
}

/// The tables and states of every workspace, as the topic holds them.
#[derive(Debug)]
pub(crate) struct Store {
    broker: String,
    /// This process's own name in the turns it takes.
    node: String,
    inner: Mutex<Inner>,
    /// The turns asked for on this node, one writer at a time each, before the topic is asked.
    here: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    asked: std::sync::atomic::AtomicU64,
}

fn now_ms() -> i64 {
    openduct_flow::tables::now_ms()
}

impl Store {
    /// The store on the broker at `broker`: its topic made if it is not there, and read.
    pub(crate) async fn open(broker: &str) -> Result<Self, String> {
        let store = Self {
            broker: broker.to_owned(),
            node: crate::model::new_id(),
            inner: Mutex::new(Inner::default()),
            here: std::sync::Mutex::new(HashMap::new()),
            asked: std::sync::atomic::AtomicU64::new(0),
        };
        let deadline = tokio::time::Instant::now() + FORMING;
        loop {
            let ready = match store.make_topic().await {
                Ok(()) => store.catch_up().await,
                Err(e) => Err(e),
            };
            match ready {
                Ok(()) => return Ok(store),
                Err(e) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!(error = e, "the cluster is not ready for the tables yet");
                    store.inner.lock().await.client = None;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn make_topic(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        let client = self.client(&mut inner).await?;
        let topics = client
            .topics()
            .await
            .map_err(|e| format!("cannot list topics: {e}"))?;
        if topics.iter().any(|t| t.name == TOPIC) {
            return Ok(());
        }
        let settings = [
            ("cleanup.policy", "compact"),
            ("min.compaction.lag.ms", COMPACTION_LAG_MS),
        ];
        match client.create_topic_with(TOPIC, 1, None, &settings).await {
            Ok(()) | Err(ClientError::Refused { code: 36, .. }) => Ok(()),
            Err(e) => Err(format!("cannot make the topic of tables: {e}")),
        }
    }

    async fn client<'a>(&self, inner: &'a mut Inner) -> Result<&'a mut Routed, String> {
        if inner.client.is_none() {
            let client = Routed::connect(&self.broker, CONNECT)
                .await
                .map_err(|e| format!("cannot reach the broker: {e}"))?;
            inner.client = Some(client);
        }
        inner
            .client
            .as_mut()
            .ok_or_else(|| "no connection to the broker".to_owned())
    }

    /// Reads what was written since the last read.
    pub(crate) async fn catch_up(&self) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        let read = self.read_locked(&mut inner).await;
        if read.is_err() {
            inner.client = None;
        }
        read
    }

    async fn read_locked(&self, inner: &mut Inner) -> Result<(), String> {
        loop {
            let from = inner.next;
            let client = self.client(inner).await?;
            let fetched = client
                .fetch(TOPIC, &[(0, from)], 0, READ_BYTES)
                .await
                .map_err(|e| format!("cannot read the tables: {e}"))?;
            let Some(answer) = fetched.into_iter().next() else {
                return Ok(());
            };
            match answer.error_code {
                0 => {}
                1 if from < answer.log_start => {
                    inner.next = answer.log_start;
                    continue;
                }
                code => return Err(format!("cannot read the tables (error code {code})")),
            }
            let moved = apply_batches(inner, answer.batches)?;
            if !moved || inner.next >= answer.high_watermark {
                return Ok(());
            }
        }
    }

    /// Writes `writes`, in order, in as few batches as their size allows; answers the offset of the last.
    async fn write(&self, writes: Vec<Write>) -> Result<i64, String> {
        let mut parts: Vec<Vec<Outgoing>> = vec![Vec::new()];
        let mut size = 0;
        for (key, value) in writes {
            let value = value.map(|v| Bytes::from(v.to_string().into_bytes()));
            let len = key.len() + value.as_ref().map_or(0, Bytes::len) + 32;
            if size + len > PART_BYTES && parts.last().is_some_and(|p| !p.is_empty()) {
                parts.push(Vec::new());
                size = 0;
            }
            size += len;
            if let Some(part) = parts.last_mut() {
                part.push(Outgoing {
                    key: Some(Bytes::from(key.into_bytes())),
                    value,
                    headers: Vec::new(),
                });
            }
        }
        let mut inner = self.inner.lock().await;
        let mut last = -1;
        for part in parts.iter().filter(|p| !p.is_empty()) {
            let client = self.client(&mut inner).await?;
            match client.produce(TOPIC, 0, part, now_ms()).await {
                Ok(first) => last = first + i64::try_from(part.len()).unwrap_or(1) - 1,
                Err(e) => {
                    inner.client = None;
                    return Err(format!("cannot keep the change: {e}"));
                }
            }
        }
        Ok(last)
    }

    /// The table of `workspace` called `name`, its id beside it.
    async fn find(&self, workspace: &str, name: &str) -> Result<(String, Arc<TableData>), String> {
        let inner = self.inner.lock().await;
        let id = inner
            .defs
            .values()
            .find(|d| d.workspace == workspace && d.name == name)
            .map(|d| d.id.clone())
            .ok_or_else(|| format!("there is no table {name}: make it under Tables"))?;
        let data = inner.tables.get(&id).cloned().unwrap_or_default();
        Ok((id, data))
    }

    /// The table, as every change written before this read left it.
    pub(crate) async fn table(&self, workspace: &str, name: &str) -> Result<Arc<TableData>, String> {
        self.catch_up().await?;
        Ok(self.find(workspace, name).await?.1)
    }

    /// The definitions of a workspace's tables, with each one's rows and bytes, by name.
    pub(crate) async fn list(&self, workspace: &str) -> Result<Vec<(TableDef, usize, usize)>, String> {
        self.catch_up().await?;
        let inner = self.inner.lock().await;
        let mut out: Vec<(TableDef, usize, usize)> = inner
            .defs
            .values()
            .filter(|d| d.workspace == workspace)
            .map(|d| {
                let rows = inner.tables.get(&d.id).map_or(0, |t| t.rows.len());
                (d.clone(), rows, inner.bytes.get(&d.id).copied().unwrap_or(0))
            })
            .collect();
        out.sort_by(|a, b| a.0.name.cmp(&b.0.name));
        Ok(out)
    }

    /// The definition of the table of `workspace` called `name`.
    pub(crate) async fn def(&self, workspace: &str, name: &str) -> Result<TableDef, String> {
        self.catch_up().await?;
        let inner = self.inner.lock().await;
        inner
            .defs
            .values()
            .find(|d| d.workspace == workspace && d.name == name)
            .cloned()
            .ok_or_else(|| format!("there is no table {name}"))
    }

    /// How many bytes a workspace's tables hold.
    async fn workspace_bytes(&self, workspace: &str) -> usize {
        let inner = self.inner.lock().await;
        inner
            .defs
            .values()
            .filter(|d| d.workspace == workspace)
            .map(|d| inner.bytes.get(&d.id).copied().unwrap_or(0))
            .sum()
    }

    /// Changes the table of `workspace` called `name` as `plan` says, in the table's turn.
    pub(crate) async fn change_table(
        &self,
        workspace: &str,
        name: &str,
        plan: Plan<'_>,
    ) -> Result<Value, String> {
        self.catch_up().await?;
        let (id, _) = self.find(workspace, name).await?;
        let turn = format!("table/{id}");
        let held = self.take(&turn).await?;
        let outcome = async {
            let data = {
                let inner = self.inner.lock().await;
                inner.tables.get(&id).cloned()
            }
            .ok_or_else(|| format!("the table {name} was deleted meanwhile"))?;
            let (changes, result) = plan(&data)?;
            check_changes(&data, &changes)?;
            let growth: usize = changes.put.iter().map(Row::size).sum();
            let used = self.workspace_bytes(workspace).await;
            if growth > 0 && used + growth > MAX_WORKSPACE_BYTES {
                return Err(format!(
                    "the workspace's tables would hold more than {} MiB: delete rows, or tables, first",
                    MAX_WORKSPACE_BYTES / 1024 / 1024
                ));
            }
            let writes = row_writes(&id, &data, &changes);
            held.in_time()?;
            self.write(writes).await?;
            Ok(result)
        }
        .await;
        self.give_back(&turn, &held).await;
        outcome
    }

    /// Writes what `work` says, worked out from the tables as they are while the turns named are held: the
    /// studio's own changes - a table made, its columns changed, rows edited by hand.
    pub(crate) async fn in_turns<T>(
        &self,
        turns: &[String],
        work: impl FnOnce(&View<'_>) -> Result<(Vec<Write>, T), String>,
    ) -> Result<T, String> {
        let mut taken: Vec<(String, Held)> = Vec::new();
        let mut outcome = Err("no turn".to_owned());
        for name in turns {
            match self.take(name).await {
                Ok(held) => taken.push((name.clone(), held)),
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        if taken.len() == turns.len() {
            let worked = {
                let inner = self.inner.lock().await;
                work(&View { inner: &inner })
            };
            outcome = match worked {
                Ok((writes, value)) => {
                    let late = taken.iter().find_map(|(_, h)| h.in_time().err());
                    match late {
                        Some(e) => Err(e),
                        None => self.write(writes).await.map(|_| value),
                    }
                }
                Err(e) => Err(e),
            };
        }
        for (name, held) in taken.iter().rev() {
            self.give_back(name, held).await;
        }
        outcome
    }

    /// A workflow's state.
    pub(crate) async fn state(&self, workspace: &str, workflow: &str) -> Result<Arc<Record>, String> {
        self.catch_up().await?;
        let inner = self.inner.lock().await;
        Ok(inner
            .states
            .get(&state_id(workspace, workflow))
            .map(|(s, _)| Arc::clone(s))
            .unwrap_or_default())
    }

    /// The workflows of a workspace that keep state: each one's id, state and when it last changed.
    pub(crate) async fn states(&self, workspace: &str) -> Result<Vec<(String, Arc<Record>, i64)>, String> {
        self.catch_up().await?;
        let inner = self.inner.lock().await;
        let prefix = state_id(workspace, "");
        let mut out: Vec<_> = inner
            .states
            .iter()
            .filter_map(|(k, (s, at))| Some((k.strip_prefix(&prefix)?.to_owned(), Arc::clone(s), *at)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Changes a workflow's state as `plan` says, in the state's turn.
    pub(crate) async fn change_state(
        &self,
        workspace: &str,
        workflow: &str,
        plan: StatePlan<'_>,
    ) -> Result<Value, String> {
        let key = state_key(workspace, workflow);
        let turn = format!("state/{workspace}/{workflow}");
        let held = self.take(&turn).await?;
        let outcome = async {
            let current = {
                let inner = self.inner.lock().await;
                inner
                    .states
                    .get(&state_id(workspace, workflow))
                    .map(|(s, _)| Arc::clone(s))
                    .unwrap_or_default()
            };
            let (next, result) = plan(&current)?;
            if let Some(next) = next {
                check_state(&next)?;
                let write = if next.is_empty() {
                    (key.clone(), None)
                } else {
                    let values = record_json(&next);
                    let record = StateRecord {
                        values,
                        updated: now_ms(),
                    };
                    (
                        key.clone(),
                        Some(serde_json::to_value(&record).map_err(|e| e.to_string())?),
                    )
                };
                held.in_time()?;
                self.write(vec![write]).await?;
            }
            Ok(result)
        }
        .await;
        self.give_back(&turn, &held).await;
        outcome
    }

    /// Takes the turn called `name`: on this node first, then in the topic, waiting for it at most [`WAIT`].
    async fn take(&self, name: &str) -> Result<Held, String> {
        let local = {
            let mut here = self.here.lock().map_err(|_| "the tables are unavailable")?;
            here.retain(|_, m| Arc::strong_count(m) > 1);
            Arc::clone(here.entry(name.to_owned()).or_default())
        };
        let guard = tokio::time::timeout(WAIT, Arc::clone(&local).lock_owned())
            .await
            .map_err(|_| busy(name))?;
        let deadline = Instant::now() + WAIT;
        loop {
            self.catch_up().await?;
            let free = {
                let inner = self.inner.lock().await;
                inner.turns.get(name).is_none_or(|(_, until)| *until < now_ms())
            };
            if free {
                let n = self.asked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let by = format!("{}/{n}", self.node);
                let asked = Instant::now();
                let at = now_ms();
                let until = at + i64::try_from(TURN.as_millis()).unwrap_or(10_000);
                let record = TurnRecord {
                    op: "take".to_owned(),
                    by: by.clone(),
                    at,
                    until,
                };
                let value = serde_json::to_value(&record).map_err(|e| e.to_string())?;
                self.write(vec![(turn_key(name), Some(value))]).await?;
                self.catch_up().await?;
                let mine = {
                    let inner = self.inner.lock().await;
                    inner.turns.get(name).is_some_and(|(holder, _)| *holder == by)
                };
                if mine {
                    return Ok(Held {
                        by,
                        asked,
                        _here: guard,
                    });
                }
            }
            if Instant::now() > deadline {
                return Err(busy(name));
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    /// Gives a turn back, so the next writer need not wait for it to run out.
    async fn give_back(&self, name: &str, held: &Held) {
        let record = TurnRecord {
            op: "free".to_owned(),
            by: held.by.clone(),
            at: now_ms(),
            until: 0,
        };
        let written = match serde_json::to_value(&record) {
            Ok(value) => self.write(vec![(turn_key(name), Some(value))]).await,
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = written.and(self.catch_up().await) {
            tracing::warn!(
                turn = name,
                error = e,
                "a table's turn was not given back: it runs out by itself"
            );
        }
    }
}

fn busy(name: &str) -> String {
    let what = if name.starts_with("state/") {
        "the workflow's state"
    } else {
        "the table"
    };
    format!(
        "{what} is busy: other runs kept changing it for {}s; try again",
        WAIT.as_secs()
    )
}

/// The tables as a change in its turns sees them.
#[derive(Debug)]
pub(crate) struct View<'a> {
    inner: &'a Inner,
}

impl View<'_> {
    /// The definition of the table of `workspace` called `name`, and its rows.
    pub(crate) fn table(&self, workspace: &str, name: &str) -> Option<(TableDef, Arc<TableData>)> {
        let def = self
            .inner
            .defs
            .values()
            .find(|d| d.workspace == workspace && d.name == name)?;
        Some((
            def.clone(),
            self.inner.tables.get(&def.id).cloned().unwrap_or_default(),
        ))
    }

    /// How many tables a workspace has.
    pub(crate) fn count(&self, workspace: &str) -> usize {
        self.inner
            .defs
            .values()
            .filter(|d| d.workspace == workspace)
            .count()
    }
}

/// A turn held: by whom, since when (on this node's clock), and this node's own hold on it.
#[derive(Debug)]
struct Held {
    by: String,
    asked: Instant,
    _here: tokio::sync::OwnedMutexGuard<()>,
}

impl Held {
    /// Whether its holder may still write: a change that took too long is given up, not written late.
    fn in_time(&self) -> Result<(), String> {
        if self.asked.elapsed() > WORK {
            return Err(format!(
                "the change took longer than {}s and was given up: nothing was written",
                WORK.as_secs()
            ));
        }
        Ok(())
    }
}

/// The records a table's changes are written as.
pub(crate) fn row_writes(id: &str, data: &TableData, changes: &Changes) -> Vec<Write> {
    let mut writes: Vec<Write> = Vec::new();
    for row in &changes.put {
        let record = RowRecord {
            id: row.id,
            values: record_json(&row.values),
            created: row.created,
            updated: row.updated,
        };
        writes.push((row_key(id, row.id), serde_json::to_value(&record).ok()));
    }
    for row in &changes.delete {
        writes.push((row_key(id, *row), None));
    }
    if changes.next_id > data.next_id {
        writes.push((
            counter_key(id),
            serde_json::to_value(Counter {
                next: changes.next_id,
            })
            .ok(),
        ));
    }
    writes
}

/// The record that defines a table (`None`: deletes it).
pub(crate) fn def_write(def: &TableDef, keep: bool) -> Write {
    (
        def_key(&def.id),
        if keep {
            serde_json::to_value(def).ok()
        } else {
            None
        },
    )
}

/// The record that takes a workflow's state away.
pub(crate) fn state_cleared(workspace: &str, workflow: &str) -> Write {
    (state_key(workspace, workflow), None)
}

fn record_json(record: &Record) -> Map<String, Json> {
    record
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                serde_json::from_str(&v.to_json()).unwrap_or(Json::Null),
            )
        })
        .collect()
}

fn record_of(values: &Map<String, Json>) -> Record {
    values
        .iter()
        .map(|(k, v)| (k.clone(), Value::from_json(v)))
        .collect()
}

fn apply_batches(inner: &mut Inner, mut batches: Bytes) -> Result<bool, String> {
    let mut moved = false;
    while !batches.is_empty() {
        let set =
            match RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            {
                Ok(set) => set,
                Err(_) if moved => break,
                Err(e) => return Err(format!("cannot read a record batch of the tables: {e}")),
            };
        for record in set.records {
            if record.offset < inner.next || record.control {
                continue;
            }
            inner.next = record.offset + 1;
            moved = true;
            let Some(key) = record
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).into_owned())
            else {
                continue;
            };
            let value = match record.value.as_ref().map(|v| serde_json::from_slice::<Json>(v)) {
                None => None,
                Some(Ok(v)) => Some(v),
                Some(Err(e)) => {
                    tracing::warn!(key, error = %e, "skipping a table record that is not JSON");
                    continue;
                }
            };
            apply(inner, &key, value);
        }
    }
    Ok(moved)
}

/// One record, as every node applies it.
fn apply(inner: &mut Inner, key: &str, value: Option<Json>) {
    let (kind, rest) = key.split_once('/').unwrap_or((key, ""));
    match (kind, value) {
        ("t", Some(v)) => {
            let Ok(def) = serde_json::from_value::<TableDef>(v) else {
                return;
            };
            let columns = def.columns();
            let table = Arc::make_mut(inner.tables.entry(def.id.clone()).or_default());
            let changed = table.columns != columns;
            table.name = def.name.clone();
            table.columns = columns;
            table.next_id = table.next_id.max(1);
            if changed {
                // Rows read before their table, or kept before a column changed type, read as its columns say.
                let kinds: Vec<(String, ColumnType)> =
                    table.columns.iter().map(|c| (c.name.clone(), c.kind)).collect();
                for row in table.rows.values_mut() {
                    row.values = Arc::new(typed(&row.values, &kinds));
                }
            }
            inner.defs.insert(def.id.clone(), def);
        }
        ("t", None) => {
            inner.defs.remove(rest);
            inner.tables.remove(rest);
            inner.bytes.remove(rest);
        }
        ("r", value) => {
            let Some((table_id, _)) = rest.split_once('/') else {
                return;
            };
            let table = Arc::make_mut(inner.tables.entry(table_id.to_owned()).or_default());
            let bytes = inner.bytes.entry(table_id.to_owned()).or_default();
            let row_id = rest.rsplit('/').next().and_then(|n| n.parse::<u64>().ok());
            let Some(row_id) = row_id else { return };
            if let Some(old) = table.rows.remove(&row_id) {
                *bytes = bytes.saturating_sub(old.size());
            }
            if let Some(Ok(record)) = value.map(serde_json::from_value::<RowRecord>) {
                let kinds: Vec<(String, ColumnType)> =
                    table.columns.iter().map(|c| (c.name.clone(), c.kind)).collect();
                let row = Row {
                    id: record.id,
                    values: Arc::new(typed(&record_of(&record.values), &kinds)),
                    created: record.created,
                    updated: record.updated,
                };
                *bytes += row.size();
                table.next_id = table.next_id.max(row_id + 1);
                table.rows.insert(row_id, row);
            }
        }
        ("n", Some(v)) => {
            if let Ok(counter) = serde_json::from_value::<Counter>(v) {
                let table = Arc::make_mut(inner.tables.entry(rest.to_owned()).or_default());
                table.next_id = table.next_id.max(counter.next);
            }
        }
        ("s", Some(v)) => {
            if let Ok(state) = serde_json::from_value::<StateRecord>(v) {
                inner.states.insert(
                    rest.to_owned(),
                    (Arc::new(record_of(&state.values)), state.updated),
                );
            }
        }
        ("s", None) => {
            inner.states.remove(rest);
        }
        ("l", Some(v)) => {
            let Ok(turn) = serde_json::from_value::<TurnRecord>(v) else {
                return;
            };
            let current = inner.turns.get(rest);
            match turn.op.as_str() {
                // Taken when no one holds it, or its holder ran past its time.
                "take" if current.is_none_or(|(_, until)| *until < turn.at) => {
                    inner.turns.insert(rest.to_owned(), (turn.by, turn.until));
                }
                "free" if current.is_some_and(|(by, _)| *by == turn.by) => {
                    inner.turns.remove(rest);
                }
                _ => {}
            }
        }
        _ => {}
    }
}

/// Values read as their columns' types (a value that does not fit is kept as it is).
fn typed(values: &Record, kinds: &[(String, ColumnType)]) -> Record {
    values
        .iter()
        .map(|(k, v)| {
            let value = match kinds.iter().find(|(n, _)| n == k) {
                Some((_, kind)) => coerce_kept(*kind, v),
                None => v.clone(),
            };
            (k.clone(), value)
        })
        .collect()
}

#[cfg(test)]
mod tests;
