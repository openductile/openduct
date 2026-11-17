// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: mysql_cdc:` - every row inserted, updated and deleted in chosen tables, from MySQL's binary log, read as
//! a replica reads it. MySQL keeps no reader's place, so the pipeline saves its own: the binary log position after
//! the last transaction whose rows are stored, `binlog:<file>:<offset>`.
//!
//! The first time, the rows already in the tables are read too. The position is taken first, then the tables are
//! read in one consistent snapshot, then the log is read from that position: a change made in between comes after
//! the row it changed, so the topic ends as the tables are.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::binlog::Table;
use super::stream::{self, Item, Position, Replica};
use super::values::{self, Column};
use super::wire::{Connection, Failure, Options, ResultColumn, quote_name};
use crate::web::ATTEMPTS;
use crate::{SourceBatch, SourceMessage};

const BATCH: usize = 1_000;
const WAIT: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub struct MysqlCdcSource {
    pub(super) shown: String,
    pub(super) options: Options,
    pub(super) tables: Vec<(String, String)>,
    pub(super) server_id: u32,
    pub(super) snapshot: bool,
    phase: Phase,
}

#[derive(Debug)]
enum Phase {
    New,
    Snapshot(Box<Snapshot>),
    Streaming {
        items: mpsc::Receiver<Item>,
        task: JoinHandle<()>,
    },
}

#[derive(Debug)]
struct Snapshot {
    conn: Connection,
    tables: VecDeque<(String, String)>,
    /// The table being read: its columns as the query gives them, and as messages name them.
    reading: Option<(Vec<ResultColumn>, Table)>,
    from: Position,
}

impl MysqlCdcSource {
    pub(super) fn new(
        shown: String,
        options: Options,
        tables: Vec<(String, String)>,
        server_id: u32,
        snapshot: bool,
    ) -> Self {
        Self {
            shown,
            options,
            tables,
            server_id,
            snapshot,
            phase: Phase::New,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Check the server writes what is needed: a binary log, of whole rows, with column names; and the tables.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let mut conn = open(&self.options).await?;
        let (_, rows) = conn
            .query("SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@server_id")
            .await
            .map_err(Failure::reason)?;
        let row: Vec<String> = rows
            .first()
            .map(|r| {
                r.iter()
                    .map(|v| {
                        v.as_ref()
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let get = |i: usize| row.get(i).cloned().unwrap_or_default();
        let what = conn.what.clone();
        if get(0) != "1" {
            return Err(format!(
                "{what} writes no binary log: change capture needs log_bin on"
            ));
        }
        if !get(1).eq_ignore_ascii_case("ROW") {
            return Err(format!(
                "{what} has binlog_format = {}: change capture needs ROW",
                get(1)
            ));
        }
        if !get(2).eq_ignore_ascii_case("FULL") {
            return Err(format!(
                "{what} has binlog_row_image = {}: change capture needs FULL",
                get(2)
            ));
        }
        if get(3) == self.server_id.to_string() {
            return Err(format!(
                "{what} has server_id {}, the one this pipeline reads as: give server_id another",
                self.server_id
            ));
        }
        let metadata = conn.query("SELECT @@binlog_row_metadata").await.ok();
        let metadata = metadata
            .and_then(|(_, rows)| rows.first().and_then(|r| r.first().cloned().flatten()))
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        if !metadata.eq_ignore_ascii_case("FULL") {
            return Err(format!(
                "{what} does not write column names into its binary log: change capture needs binlog_row_metadata = \
                 FULL (SET PERSIST binlog_row_metadata = FULL)"
            ));
        }
        for (schema, table) in &self.tables {
            let (_, found) = conn
                .query(&format!(
                    "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
                ))
                .await
                .map_err(Failure::reason)?;
            if found.is_empty() {
                return Err(format!(
                    "{what} has no table {schema}.{table} (or this user may not see it)"
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        match &mut self.phase {
            Phase::New => self.begin(position).await,
            Phase::Snapshot(_) => self.read_snapshot().await,
            Phase::Streaming { .. } => self.read_stream().await,
        }
    }

    pub(crate) async fn stop(&mut self) {
        if let Phase::Streaming { items, task } = std::mem::replace(&mut self.phase, Phase::New) {
            drop(items);
            let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        }
    }

    async fn begin(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        if let Some(from) = position.and_then(Position::parse) {
            self.stream(from.clone());
            return Ok(SourceBatch::default());
        }
        let mut conn = open(&self.options).await?;
        let from = current_position(&mut conn).await?;
        if !self.snapshot {
            self.stream(from.clone());
            return Ok(SourceBatch {
                messages: Vec::new(),
                position: Some(from.text()),
            });
        }
        // TIMESTAMP as UTC, as the stream writes it; time for the pipeline to write between parts of a large table.
        conn.query("SET time_zone = '+00:00', net_write_timeout = 3600")
            .await
            .map_err(Failure::reason)?;
        conn.query("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            .await
            .map_err(Failure::reason)?;
        self.phase = Phase::Snapshot(Box::new(Snapshot {
            conn,
            tables: self.tables.iter().cloned().collect(),
            reading: None,
            from,
        }));
        Ok(SourceBatch::default())
    }

    async fn read_snapshot(&mut self) -> Result<SourceBatch, String> {
        let Phase::Snapshot(s) = &mut self.phase else {
            return Ok(SourceBatch::default());
        };
        loop {
            if s.reading.is_none() {
                let Some((schema, name)) = s.tables.front().cloned() else {
                    s.conn.query("COMMIT").await.map_err(Failure::reason)?;
                    let from = s.from.clone();
                    self.stream(from.clone());
                    return Ok(SourceBatch {
                        messages: Vec::new(),
                        position: Some(from.text()),
                    });
                };
                let key = primary_key(&mut s.conn, &schema, &name).await?;
                let columns = s
                    .conn
                    .start_query(&format!(
                        "SELECT * FROM {}.{}",
                        quote_name(&schema),
                        quote_name(&name)
                    ))
                    .await
                    .map_err(Failure::reason)?;
                let table = Table {
                    schema,
                    name,
                    columns: columns
                        .iter()
                        .map(|c| Column {
                            name: c.name.clone(),
                            kind: c.kind,
                            key: key.contains(&c.name),
                            ..Column::default()
                        })
                        .collect(),
                };
                s.reading = Some((columns, table));
            }
            let mut messages: Vec<SourceMessage> = Vec::new();
            while messages.len() < BATCH {
                let Some(row) = s.conn.next_row().await.map_err(Failure::reason)? else {
                    s.reading = None;
                    s.tables.pop_front();
                    break;
                };
                let Some((columns, table)) = &s.reading else { break };
                let values: Vec<Option<String>> =
                    columns
                        .iter()
                        .zip(&row)
                        .map(|(c, v)| {
                            Some(v.as_ref().map_or_else(
                                || "null".to_owned(),
                                |v| values::text_value(c.kind, c.charset, v),
                            ))
                        })
                        .collect();
                messages.push(stream::message(
                    "snapshot",
                    table,
                    None,
                    Some(&values),
                    &s.from,
                    None,
                ));
            }
            if !messages.is_empty() {
                return Ok(SourceBatch {
                    messages,
                    position: None,
                });
            }
        }
    }

    fn stream(&mut self, from: Position) {
        let (tx, items) = mpsc::channel(BATCH * 4);
        let replica = Replica {
            options: self.options.clone(),
            server_id: self.server_id,
            tables: self.tables.iter().cloned().collect::<HashSet<_>>(),
        };
        let task = tokio::spawn(stream::run(replica, from, tx));
        self.phase = Phase::Streaming { items, task };
    }

    async fn read_stream(&mut self) -> Result<SourceBatch, String> {
        let Phase::Streaming { items, .. } = &mut self.phase else {
            return Ok(SourceBatch::default());
        };
        let mut messages = Vec::new();
        let mut position = None;
        let mut first = true;
        while messages.len() < BATCH {
            let item = if first {
                first = false;
                match tokio::time::timeout(WAIT, items.recv()).await {
                    Ok(item) => item,
                    Err(_) => break,
                }
            } else {
                match items.try_recv() {
                    Ok(item) => Some(item),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => None,
                }
            };
            match item {
                Some(Item::Message(m)) => messages.push(m),
                Some(Item::Committed(p)) => position = Some(p.text()),
                Some(Item::Failed(reason)) => return Err(reason),
                None => return Err("the MySQL binary log stream stopped".to_owned()),
            }
        }
        Ok(SourceBatch { messages, position })
    }
}

async fn open(o: &Options) -> Result<Connection, String> {
    let mut failures = 0;
    loop {
        match Connection::open(o).await {
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

/// Where the binary log is now (`SHOW BINARY LOG STATUS`, or before MySQL 8.2 `SHOW MASTER STATUS`).
async fn current_position(conn: &mut Connection) -> Result<Position, String> {
    let (columns, rows) = match conn.query("SHOW BINARY LOG STATUS").await {
        Ok(answer) => answer,
        Err(_) => conn.query("SHOW MASTER STATUS").await.map_err(Failure::reason)?,
    };
    let at = |name: &str| columns.iter().position(|c| c.name.eq_ignore_ascii_case(name));
    let row = rows
        .first()
        .ok_or_else(|| format!("{} reports no binary log position (is log_bin on?)", conn.what))?;
    let value = |i: Option<usize>| {
        i.and_then(|i| row.get(i))
            .cloned()
            .flatten()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    };
    let file = value(at("File")).ok_or_else(|| format!("{} reports no binary log file", conn.what))?;
    let offset = value(at("Position"))
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| format!("{} reports no binary log position", conn.what))?;
    Ok(Position { file, offset })
}

async fn primary_key(conn: &mut Connection, schema: &str, table: &str) -> Result<Vec<String>, String> {
    let (_, rows) = conn
        .query(&format!(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{schema}' AND \
             TABLE_NAME = '{table}' AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION"
        ))
        .await
        .map_err(Failure::reason)?;
    Ok(rows
        .iter()
        .filter_map(|r| r.first().cloned().flatten())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect())
}
