// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: postgres_cdc:` - every row inserted, updated and deleted in chosen tables, from PostgreSQL's logical
//! replication. A publication names the tables; a replication slot keeps the server's place, and the pipeline
//! confirms a transaction to it only once its rows are stored, so nothing is lost while the pipeline is stopped.
//! The first time, the rows already in the tables are read too, from the moment the slot was made, so the stream
//! then carries on from exactly there.
//!
//! The pipeline's saved position says which of the two it is doing: `snapshot:<slot>` while it reads the tables
//! (a stop then starts the reading again, with a new slot), `streaming:<slot>` after.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::pgoutput::{self, Column, Relation, Value};
use super::stream::{self, Event, Slot};
use super::wire::{Connection, Failure, Options, Rows, quote_name, quote_text};
use crate::web::ATTEMPTS;
use crate::{SourceBatch, SourceMessage};

/// A batch handed on: whether it was stored, and how far that makes safe.
type Pending = (oneshot::Receiver<Result<(), String>>, Option<u64>);

/// The most messages handed on at once, and the rows read from a table at a time.
const BATCH: usize = 1_000;
const WAIT: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub struct PostgresCdcSource {
    pub(super) shown: String,
    pub(super) options: Options,
    /// The tables to read, as `(schema, table)`; empty when the publication already names them.
    pub(super) tables: Vec<(String, String)>,
    pub(super) publication: String,
    pub(super) slot: String,
    pub(super) snapshot: bool,
    phase: Phase,
    pending: Vec<Pending>,
    /// What the stream may tell the server is safe.
    confirmed: Option<watch::Sender<u64>>,
}

#[derive(Debug)]
enum Phase {
    New,
    Snapshot(Box<Snapshot>),
    Streaming {
        events: mpsc::Receiver<Event>,
        task: JoinHandle<()>,
    },
}

#[derive(Debug)]
struct Snapshot {
    /// The replication connection that made the slot; the stream starts on it once the tables are read.
    replication: Connection,
    /// A connection inside a transaction that sees the tables as they were when the slot was made.
    reader: Connection,
    tables: VecDeque<Table>,
    reading: bool,
    lsn: u64,
}

#[derive(Debug, Clone)]
struct Table {
    schema: String,
    name: String,
    key: Vec<String>,
}

impl PostgresCdcSource {
    pub(super) fn new(
        shown: String,
        options: Options,
        tables: Vec<(String, String)>,
        publication: String,
        slot: String,
        snapshot: bool,
    ) -> Self {
        Self {
            shown,
            options,
            tables,
            publication,
            slot,
            snapshot,
            phase: Phase::New,
            pending: Vec::new(),
            confirmed: None,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Check the server can do this - logical replication on, the publication there or made - before reading.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let mut conn = open(&self.options, false).await?;
        let level = conn.query("SHOW wal_level").await.map_err(Failure::reason)?;
        let level = level.first("wal_level").unwrap_or_default();
        if level != "logical" {
            return Err(format!(
                "{} has wal_level = {level}: change capture needs wal_level = logical (set it and restart the \
                 server; hosted databases call it logical replication)",
                conn.what
            ));
        }
        let exists = conn
            .query(&format!(
                "SELECT pubname FROM pg_publication WHERE pubname = {}",
                quote_text(&self.publication)
            ))
            .await
            .map_err(Failure::reason)?;
        if exists.rows.is_empty() {
            if self.tables.is_empty() {
                return Err(format!(
                    "{} has no publication {}: give tables, or create it",
                    conn.what, self.publication
                ));
            }
            let names: Vec<String> = self
                .tables
                .iter()
                .map(|(s, t)| format!("{}.{}", quote_name(s), quote_name(t)))
                .collect();
            conn.query(&format!(
                "CREATE PUBLICATION {} FOR TABLE {}",
                quote_name(&self.publication),
                names.join(", ")
            ))
            .await
            .map_err(|f| {
                format!(
                    "cannot create publication {}: {} (the tables' owner can: CREATE PUBLICATION {} FOR TABLE {})",
                    self.publication,
                    f.reason(),
                    self.publication,
                    names.join(", ")
                )
            })?;
        } else if !self.tables.is_empty() {
            let have = publication_tables(&mut conn, &self.publication).await?;
            for (schema, table) in &self.tables {
                if !have.iter().any(|t| &t.0 == schema && &t.1 == table) {
                    return Err(format!(
                        "publication {} does not have {schema}.{table} (ALTER PUBLICATION {} ADD TABLE {}.{})",
                        self.publication,
                        self.publication,
                        quote_name(schema),
                        quote_name(table)
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        self.settle().await;
        match &mut self.phase {
            Phase::New => self.begin(position).await,
            Phase::Snapshot(_) => self.read_snapshot().await,
            Phase::Streaming { .. } => self.read_stream().await,
        }
    }

    /// Stop: tell the server what is stored, and end the stream.
    pub(crate) async fn stop(&mut self) {
        self.settle().await;
        self.confirmed = None;
        if let Phase::Streaming { task, .. } = std::mem::replace(&mut self.phase, Phase::New) {
            let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        }
    }

    /// What the runner has stored since the last call; tell the stream how far that makes safe. A batch that was
    /// not stored stops the confirming: the runner stops, and the slot keeps those changes for the next start.
    async fn settle(&mut self) {
        for (stored, safe) in std::mem::take(&mut self.pending) {
            let Ok(Ok(())) = stored.await else {
                break;
            };
            if let Some(lsn) = safe {
                self.confirm(lsn);
            }
        }
    }

    fn confirm(&self, lsn: u64) {
        if let Some(confirmed) = &self.confirmed {
            confirmed.send_if_modified(|now| {
                if lsn > *now {
                    *now = lsn;
                    true
                } else {
                    false
                }
            });
        }
    }

    fn position(&self, kind: &str) -> Option<String> {
        Some(format!("{kind}:{}", self.slot))
    }

    /// The first call: carry on from the slot, or make it (reading the tables first, when asked).
    async fn begin(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        let mut replication = open(&self.options, true).await?;
        let found = replication
            .query(&format!(
                "SELECT plugin, database, active FROM pg_replication_slots WHERE slot_name = {}",
                quote_text(&self.slot)
            ))
            .await
            .map_err(Failure::reason)?;
        let streaming = self.position("streaming");
        let snapshotting = self.position("snapshot");
        let ours = |p: &Option<String>| position == p.as_deref();
        if !found.rows.is_empty() {
            let plugin = found.first("plugin").unwrap_or_default();
            if plugin != "pgoutput" {
                return Err(format!(
                    "replication slot {} uses {plugin}, not pgoutput: give this pipeline a slot of its own",
                    self.slot
                ));
            }
            if found.first("database").as_deref() != Some(self.options.database.as_str()) {
                return Err(format!("replication slot {} is for another database", self.slot));
            }
            if ours(&snapshotting) && self.snapshot {
                // The tables were being read when the pipeline stopped: read them again, from a new slot.
                tracing::warn!(slot = %self.slot, "reading the tables again: the last reading did not finish");
                replication
                    .query(&format!("DROP_REPLICATION_SLOT {}", quote_name(&self.slot)))
                    .await
                    .map_err(|f| format!("cannot start reading the tables again: {}", f.reason()))?;
                return self.make_slot(replication).await;
            }
            if !ours(&streaming) && self.snapshot {
                tracing::warn!(
                    slot = %self.slot,
                    "the replication slot was there before this pipeline: reading changes from it, without the rows \
                     already in the tables"
                );
            }
            self.stream(replication);
            return Ok(SourceBatch {
                messages: Vec::new(),
                position: streaming,
            });
        }
        if ours(&streaming) {
            return Err(format!(
                "replication slot {} is gone, so the changes made since are not known: give a new slot to start \
                 again from the tables as they are now",
                self.slot
            ));
        }
        self.make_slot(replication).await
    }

    async fn make_slot(&mut self, mut replication: Connection) -> Result<SourceBatch, String> {
        let export = if self.snapshot {
            "EXPORT_SNAPSHOT"
        } else {
            "NOEXPORT_SNAPSHOT"
        };
        let made = replication
            .query(&format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput {export}",
                quote_name(&self.slot)
            ))
            .await
            .map_err(|f| {
                format!(
                    "cannot make replication slot {}: {} (the user needs the REPLICATION attribute: ALTER ROLE {} \
                     REPLICATION)",
                    self.slot,
                    f.reason(),
                    quote_name(&self.options.username)
                )
            })?;
        let lsn = made
            .first("consistent_point")
            .and_then(|p| pgoutput::parse_lsn(&p))
            .unwrap_or(0);
        if !self.snapshot {
            self.stream(replication);
            return Ok(SourceBatch {
                messages: Vec::new(),
                position: self.position("streaming"),
            });
        }
        let name = made
            .first("snapshot_name")
            .filter(|n| n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
            .ok_or_else(|| "the server did not give the snapshot of the new slot".to_owned())?;
        let mut reader = open(&self.options, false).await?;
        reader
            .query(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {}",
                quote_text(&name)
            ))
            .await
            .map_err(Failure::reason)?;
        let mut tables = VecDeque::new();
        for (schema, name) in publication_tables(&mut reader, &self.publication).await? {
            let key = primary_key(&mut reader, &schema, &name).await?;
            tables.push_back(Table { schema, name, key });
        }
        self.phase = Phase::Snapshot(Box::new(Snapshot {
            replication,
            reader,
            tables,
            reading: false,
            lsn,
        }));
        Ok(SourceBatch {
            messages: Vec::new(),
            position: self.position("snapshot"),
        })
    }

    /// The next rows of the tables as they were, a part at a time; then the stream.
    async fn read_snapshot(&mut self) -> Result<SourceBatch, String> {
        let Phase::Snapshot(s) = &mut self.phase else {
            return Ok(SourceBatch::default());
        };
        loop {
            let Some(table) = s.tables.front().cloned() else {
                s.reader.query("COMMIT").await.map_err(Failure::reason)?;
                let Phase::Snapshot(s) = std::mem::replace(&mut self.phase, Phase::New) else {
                    return Ok(SourceBatch::default());
                };
                self.stream(s.replication);
                return Ok(SourceBatch {
                    messages: Vec::new(),
                    position: self.position("streaming"),
                });
            };
            if !s.reading {
                s.reader
                    .query(&format!(
                        "DECLARE openduct_rows NO SCROLL CURSOR FOR SELECT * FROM {}.{}",
                        quote_name(&table.schema),
                        quote_name(&table.name)
                    ))
                    .await
                    .map_err(Failure::reason)?;
                s.reading = true;
            }
            let rows = s
                .reader
                .query(&format!("FETCH {BATCH} FROM openduct_rows"))
                .await
                .map_err(Failure::reason)?;
            if rows.rows.is_empty() {
                s.reader
                    .query("CLOSE openduct_rows")
                    .await
                    .map_err(Failure::reason)?;
                s.reading = false;
                s.tables.pop_front();
                continue;
            }
            return Ok(SourceBatch {
                messages: snapshot_messages(&table, &rows, s.lsn),
                position: None,
            });
        }
    }

    fn stream(&mut self, replication: Connection) {
        let (events_tx, events) = mpsc::channel(BATCH * 4);
        let (confirmed, confirmed_rx) = watch::channel(0u64);
        let slot = Slot {
            options: self.options.clone(),
            slot: self.slot.clone(),
            publication: self.publication.clone(),
        };
        let task = tokio::spawn(stream::run(Some(replication), slot, events_tx, confirmed_rx));
        self.confirmed = Some(confirmed);
        self.phase = Phase::Streaming { events, task };
    }

    async fn read_stream(&mut self) -> Result<SourceBatch, String> {
        let Phase::Streaming { events, .. } = &mut self.phase else {
            return Ok(SourceBatch::default());
        };
        let mut messages: Vec<SourceMessage> = Vec::new();
        let mut safe_now: Option<u64> = None;
        let mut safe_after: Option<u64> = None;
        let mut first = true;
        while messages.len() < BATCH {
            let event = if first {
                first = false;
                match tokio::time::timeout(WAIT, events.recv()).await {
                    Ok(event) => event,
                    Err(_) => break,
                }
            } else {
                match events.try_recv() {
                    Ok(event) => Some(event),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => None,
                }
            };
            match event {
                Some(Event::Message(m)) => messages.push(m),
                Some(Event::Done(lsn)) if messages.is_empty() => safe_now = Some(lsn),
                Some(Event::Done(lsn)) => safe_after = Some(lsn),
                Some(Event::Failed(reason)) => return Err(reason),
                None => return Err("the PostgreSQL replication stream stopped".to_owned()),
            }
        }
        if let Some(lsn) = safe_now {
            self.confirm(lsn);
        }
        if let Some(last) = messages.last_mut() {
            let (stored, done) = oneshot::channel();
            last.ack = Some(stored);
            self.pending.push((done, safe_after));
        }
        Ok(SourceBatch {
            messages,
            position: None,
        })
    }
}

/// Connect, again after a failure that may pass.
async fn open(o: &Options, replication: bool) -> Result<Connection, String> {
    let mut failures = 0;
    loop {
        match Connection::open(o, replication).await {
            Ok(c) => return Ok(c),
            Err(Failure::Lasting(reason)) => return Err(reason),
            Err(Failure::Passing(reason)) => {
                failures += 1;
                if failures >= ATTEMPTS {
                    return Err(reason);
                }
                let wait = Duration::from_millis(500) * 2u32.saturating_pow(failures - 1);
                tokio::time::sleep(wait.min(Duration::from_secs(10))).await;
            }
        }
    }
}

async fn publication_tables(
    conn: &mut Connection,
    publication: &str,
) -> Result<Vec<(String, String)>, String> {
    let rows = conn
        .query(&format!(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = {} ORDER BY 1, 2",
            quote_text(publication)
        ))
        .await
        .map_err(Failure::reason)?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|r| match (r.first(), r.get(1)) {
            (Some(Some(s)), Some(Some(t))) => Some((
                String::from_utf8_lossy(s).into_owned(),
                String::from_utf8_lossy(t).into_owned(),
            )),
            _ => None,
        })
        .collect())
}

async fn primary_key(conn: &mut Connection, schema: &str, table: &str) -> Result<Vec<String>, String> {
    let name = format!("{}.{}", quote_name(schema), quote_name(table));
    let rows = conn
        .query(&format!(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = \
             ANY(i.indkey) WHERE i.indisprimary AND i.indrelid = {}::regclass ORDER BY array_position(i.indkey::int2[], \
             a.attnum)",
            quote_text(&name)
        ))
        .await
        .map_err(Failure::reason)?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|r| r.first().cloned().flatten())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect())
}

/// Rows read from a table as it was, as messages like the stream's, with op `snapshot`.
fn snapshot_messages(table: &Table, rows: &Rows, lsn: u64) -> Vec<SourceMessage> {
    let relation = Relation {
        schema: table.schema.clone(),
        table: table.name.clone(),
        columns: rows
            .columns
            .iter()
            .map(|(name, type_oid)| Column {
                name: name.clone(),
                type_oid: *type_oid,
                key: table.key.contains(name),
            })
            .collect(),
    };
    rows.rows
        .iter()
        .map(|row| {
            let values: Vec<Value> = row
                .iter()
                .map(|v| v.clone().map_or(Value::Null, Value::Text))
                .collect();
            stream::row("snapshot", &relation, None, Some(&values), lsn, None)
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn rows_of_the_tables_as_they_were_read_like_changes() {
        let table = Table {
            schema: "public".to_owned(),
            name: "orders".to_owned(),
            key: vec!["id".to_owned()],
        };
        let rows = Rows {
            columns: vec![("id".to_owned(), 23), ("total".to_owned(), 1700)],
            rows: vec![vec![Some(Bytes::from_static(b"1")), None]],
            tag: None,
        };
        let m = snapshot_messages(&table, &rows, 0x10);
        assert_eq!(m[0].key.as_deref(), Some(&b"public.orders/1"[..]));
        assert_eq!(
            String::from_utf8(m[0].value.clone().unwrap()).unwrap(),
            r#"{"op":"snapshot","table":"public.orders","before":null,"after":{"id":1,"total":null},"lsn":"0/10"}"#
        );
    }
}
