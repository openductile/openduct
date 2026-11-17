// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The binary log, read by a task of its own as a replica would read it: each row changed in the chosen tables
//! becomes a message, and each transaction's end is passed on with the position after it, which the pipeline saves
//! once the transaction's messages are stored. After a failure that may pass, it reads again from the last
//! position passed on.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::sync::mpsc;

use super::binlog::{self, Event, Op, Reader, Table};
use super::wire::{Connection, Failure, Options};
use crate::SourceMessage;
use crate::json::json_string;
use crate::web::ATTEMPTS;

/// The server is asked for a heartbeat this often when nothing happens, so a dead connection is found.
const HEARTBEAT_NANOS: u64 = 15_000_000_000;
const QUIET: Duration = Duration::from_secs(60);

/// A place in the binary log: a file and an offset in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Position {
    pub(super) file: String,
    pub(super) offset: u32,
}

impl Position {
    pub(super) fn text(&self) -> String {
        format!("binlog:{}:{}", self.file, self.offset)
    }

    pub(super) fn parse(text: &str) -> Option<Self> {
        let rest = text.strip_prefix("binlog:")?;
        let (file, offset) = rest.rsplit_once(':')?;
        Some(Self {
            file: file.to_owned(),
            offset: offset.parse().ok()?,
        })
    }
}

#[derive(Debug)]
pub(super) enum Item {
    Message(SourceMessage),
    /// A transaction ended: everything before this position is in the messages already sent.
    Committed(Position),
    Failed(String),
}

#[derive(Debug, Clone)]
pub(super) struct Replica {
    pub(super) options: Options,
    pub(super) server_id: u32,
    pub(super) tables: HashSet<(String, String)>,
}

pub(super) async fn run(replica: Replica, mut from: Position, events: mpsc::Sender<Item>) {
    let mut failures = 0;
    loop {
        match read(&replica, &mut from, &events, &mut failures).await {
            Ok(()) => return,
            Err(Failure::Lasting(reason)) => {
                let _ = events.send(Item::Failed(reason)).await;
                return;
            }
            Err(Failure::Passing(reason)) => {
                failures += 1;
                if failures >= ATTEMPTS {
                    let _ = events.send(Item::Failed(reason)).await;
                    return;
                }
                tracing::warn!(%reason, "the MySQL binary log stream broke; reading again");
                let wait = Duration::from_millis(500) * 2u32.saturating_pow(failures - 1);
                tokio::time::sleep(wait.min(Duration::from_secs(10))).await;
            }
        }
    }
}

/// Read from `from` until the reading side goes (`Ok`) or the connection fails; `from` follows each commit.
async fn read(
    replica: &Replica,
    from: &mut Position,
    events: &mpsc::Sender<Item>,
    failures: &mut u32,
) -> Result<(), Failure> {
    let mut c = Connection::open(&replica.options).await?;
    let (_, rows) = c.query("SELECT @@global.binlog_checksum").await?;
    let checksum = rows
        .first()
        .and_then(|r| r.first().cloned().flatten())
        .is_some_and(|v| v.eq_ignore_ascii_case(b"CRC32"));
    c.query(&format!(
        "SET @source_binlog_checksum = @@global.binlog_checksum, @master_binlog_checksum = @@global.binlog_checksum, \
         @source_heartbeat_period = {HEARTBEAT_NANOS}, @master_heartbeat_period = {HEARTBEAT_NANOS}"
    ))
    .await?;
    c.dump_binlog(&from.file, from.offset, replica.server_id).await?;
    let mut reader = Reader::new(checksum);
    let mut maps: HashMap<u64, Table> = HashMap::new();
    let mut file = from.file.clone();
    let mut in_transaction = false;
    loop {
        let raw = match tokio::time::timeout(QUIET, c.next_event()).await {
            Err(_) => return Err(Failure::Passing(format!("{} sent nothing for a minute", c.what))),
            Ok(Ok(Some(raw))) => raw,
            Ok(Ok(None)) => {
                return Err(Failure::Passing(format!(
                    "{} ended the binary log stream",
                    c.what
                )));
            }
            Ok(Err(f)) => return Err(f),
        };
        *failures = 0;
        let (header, event) = reader
            .event(raw)
            .map_err(|why| Failure::Lasting(format!("{} sent {why}", c.what)))?;
        let committed = match event {
            Event::Rotate { file: next, .. } => {
                file = next;
                None
            }
            Event::TableMap { id, table } => {
                maps.insert(id, table);
                None
            }
            Event::Query { statement } => {
                let s = statement.trim();
                if s.eq_ignore_ascii_case("BEGIN") {
                    in_transaction = true;
                    None
                } else if !in_transaction || s.eq_ignore_ascii_case("COMMIT") {
                    // A statement of its own (DDL), or the end of a transaction without transactional tables.
                    in_transaction = false;
                    Some(Position {
                        file: file.clone(),
                        offset: header.next,
                    })
                } else {
                    None
                }
            }
            Event::Commit => {
                in_transaction = false;
                Some(Position {
                    file: file.clone(),
                    offset: header.next,
                })
            }
            Event::Rows {
                op,
                table_id,
                mut data,
                after_present,
                present,
            } => {
                let Some(table) = maps.get(&table_id) else {
                    return Err(Failure::Lasting(format!(
                        "{} sent rows of a table it had not described",
                        c.what
                    )));
                };
                if replica
                    .tables
                    .contains(&(table.schema.clone(), table.name.clone()))
                {
                    let position = Position {
                        file: file.clone(),
                        offset: header.next,
                    };
                    while !data.is_empty() {
                        let first = binlog::row(table, &present, &mut data)
                            .map_err(|why| Failure::Lasting(format!("{} sent {why}", c.what)))?;
                        let (before, after) = match (op, &after_present) {
                            (Op::Update, Some(second)) => {
                                let after = binlog::row(table, second, &mut data)
                                    .map_err(|why| Failure::Lasting(format!("{} sent {why}", c.what)))?;
                                (Some(first), Some(after))
                            }
                            (Op::Delete, _) => (Some(first), None),
                            _ => (None, Some(first)),
                        };
                        let m = message(
                            op_name(op),
                            table,
                            before.as_deref(),
                            after.as_deref(),
                            &position,
                            Some(header.timestamp),
                        );
                        if events.send(Item::Message(m)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                None
            }
            Event::Other => None,
        };
        if let Some(position) = committed {
            *from = position.clone();
            if events.send(Item::Committed(position)).await.is_err() {
                return Ok(());
            }
        }
    }
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
    }
}

fn object(table: &Table, values: &[Option<String>]) -> String {
    let fields: Vec<String> = table
        .columns
        .iter()
        .zip(values)
        .filter_map(|(c, v)| v.as_ref().map(|v| format!("{}:{v}", json_string(&c.name))))
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// One row's change as a message, keyed by table and primary key so one row's changes stay in order.
pub(super) fn message(
    op: &str,
    table: &Table,
    before: Option<&[Option<String>]>,
    after: Option<&[Option<String>]>,
    position: &Position,
    timestamp: Option<u32>,
) -> SourceMessage {
    let name = format!("{}.{}", table.schema, table.name);
    let mut value = format!(
        "{{\"op\":{},\"table\":{},\"before\":{},\"after\":{},\"binlog\":{}",
        json_string(op),
        json_string(&name),
        before.map_or_else(|| "null".to_owned(), |v| object(table, v)),
        after.map_or_else(|| "null".to_owned(), |v| object(table, v)),
        json_string(&format!("{}:{}", position.file, position.offset)),
    );
    if let Some(at) = timestamp.and_then(|t| chrono::DateTime::from_timestamp(i64::from(t), 0)) {
        value.push_str(&format!(
            ",\"committed\":{}",
            json_string(&at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        ));
    }
    value.push('}');
    let mut key = name.clone();
    for (c, v) in table.columns.iter().zip(after.or(before).unwrap_or_default()) {
        if c.key {
            key.push('/');
            if let Some(v) = v {
                key.push_str(v.trim_matches('"'));
            }
        }
    }
    SourceMessage {
        key: Some(key.into_bytes()),
        value: Some(value.into_bytes()),
        headers: vec![
            ("mysql-table".to_owned(), name.into_bytes()),
            ("mysql-op".to_owned(), op.as_bytes().to_vec()),
            (
                "mysql-binlog".to_owned(),
                format!("{}:{}", position.file, position.offset).into_bytes(),
            ),
        ],
        ack: None,
    }
}

/// The hostile-input tests' door (see `crate::hostile`): `chunks` read as binary log events, one after another, the
/// way [`stream`] reads them from the server - each event, its table, and every row of it.
pub(crate) fn replay(checksum: bool, chunks: Vec<bytes::Bytes>) {
    let mut reader = Reader::new(checksum);
    let mut maps: HashMap<u64, Table> = HashMap::new();
    let position = Position {
        file: "binlog.000001".to_owned(),
        offset: 4,
    };
    for raw in chunks {
        match reader.event(raw) {
            Ok((_, Event::TableMap { id, table })) => {
                maps.insert(id, table);
            }
            Ok((
                header,
                Event::Rows {
                    op,
                    table_id,
                    mut data,
                    after_present,
                    present,
                },
            )) => {
                let Some(table) = maps.get(&table_id) else {
                    continue;
                };
                while !data.is_empty() {
                    let Ok(first) = binlog::row(table, &present, &mut data) else {
                        break;
                    };
                    let (before, after) = match (op, &after_present) {
                        (Op::Update, Some(second)) => match binlog::row(table, second, &mut data) {
                            Ok(after) => (Some(first), Some(after)),
                            Err(_) => break,
                        },
                        (Op::Delete, _) => (Some(first), None),
                        _ => (None, Some(first)),
                    };
                    let _ = message(
                        op_name(op),
                        table,
                        before.as_deref(),
                        after.as_deref(),
                        &position,
                        Some(header.timestamp),
                    );
                }
            }
            Ok(_) | Err(_) => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::values::{self, Column};
    use super::*;

    #[test]
    fn positions_and_messages() {
        let p = Position {
            file: "binlog.000007".to_owned(),
            offset: 1234,
        };
        assert_eq!(Position::parse(&p.text()), Some(p.clone()));
        assert_eq!(Position::parse("snapshot"), None);
        let table = Table {
            schema: "shop".to_owned(),
            name: "orders".to_owned(),
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    kind: values::LONG,
                    key: true,
                    ..Column::default()
                },
                Column {
                    name: "total".to_owned(),
                    kind: values::NEWDECIMAL,
                    ..Column::default()
                },
            ],
        };
        let m = message(
            "update",
            &table,
            Some(&[Some("42".to_owned()), Some("5.00".to_owned())]),
            Some(&[Some("42".to_owned()), Some("19.90".to_owned())]),
            &p,
            Some(1_763_200_000),
        );
        assert_eq!(m.key.as_deref(), Some(&b"shop.orders/42"[..]));
        assert_eq!(
            String::from_utf8(m.value.unwrap()).unwrap(),
            r#"{"op":"update","table":"shop.orders","before":{"id":42,"total":5.00},"after":{"id":42,"total":19.90},"binlog":"binlog.000007:1234","committed":"2025-11-15T09:46:40.000Z"}"#
        );
    }
}
