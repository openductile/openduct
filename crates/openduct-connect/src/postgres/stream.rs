// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The replication stream, read by a task of its own so it keeps answering the server (which ends a quiet
//! connection) however long a write takes. Each change becomes a message; each commit is passed on so the reading
//! side can say when it is stored. The task tells the server how far is safe every ten seconds, and at once when
//! that moves, so the WAL before it can be removed.

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};

use super::pgoutput::{self, Change, Relation, Relations, Stream, Value};
use super::wire::{Connection, Failure, Options, quote_name};
use crate::SourceMessage;
use crate::web::ATTEMPTS;

const STATUS_EVERY: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(super) enum Event {
    Message(SourceMessage),
    /// Everything before this position is in the messages already sent (a commit, or the server's position
    /// between transactions).
    Done(u64),
    Failed(String),
}

/// Where the stream comes from.
#[derive(Debug, Clone)]
pub(super) struct Slot {
    pub(super) options: Options,
    pub(super) slot: String,
    pub(super) publication: String,
}

/// Read the stream until the reading side goes; reconnect after a failure that may pass.
pub(super) async fn run(
    mut conn: Option<Connection>,
    slot: Slot,
    events: mpsc::Sender<Event>,
    mut confirmed: watch::Receiver<u64>,
) {
    let mut failures = 0;
    loop {
        let result = match conn.take() {
            Some(c) => Ok(c),
            None => Connection::open(&slot.options, true).await,
        };
        let outcome = match result {
            Ok(mut c) => match start(&mut c, &slot).await {
                Ok(()) => read(&mut c, &events, &mut confirmed, &mut failures).await,
                Err(f) => Err(f),
            },
            Err(f) => Err(f),
        };
        match outcome {
            Ok(()) => return,
            Err(Failure::Lasting(reason)) => {
                let _ = events.send(Event::Failed(reason)).await;
                return;
            }
            Err(Failure::Passing(reason)) => {
                failures += 1;
                if failures >= ATTEMPTS {
                    let _ = events.send(Event::Failed(reason)).await;
                    return;
                }
                tracing::warn!(%reason, slot = %slot.slot, "the PostgreSQL replication stream broke; reading again");
                let wait = Duration::from_millis(500) * 2u32.saturating_pow(failures - 1);
                tokio::time::sleep(wait.min(Duration::from_secs(10))).await;
            }
        }
    }
}

async fn start(c: &mut Connection, slot: &Slot) -> Result<(), Failure> {
    // From where the slot was last confirmed (0/0 asks for that); changes of the publication's tables only.
    let command = format!(
        "START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '1', publication_names '{}')",
        quote_name(&slot.slot),
        slot.publication
    );
    c.start_streaming(&command).await
}

enum Step {
    Data(Result<Option<bytes::Bytes>, Failure>),
    Status,
    Confirmed(bool),
}

/// The stream, until the reading side goes (`Ok`) or the connection fails.
async fn read(
    c: &mut Connection,
    events: &mpsc::Sender<Event>,
    confirmed: &mut watch::Receiver<u64>,
    failures: &mut u32,
) -> Result<(), Failure> {
    let mut relations = Relations::new();
    let mut committed_micros: Option<i64> = None;
    let mut in_transaction = false;
    let mut next_status = Instant::now() + STATUS_EVERY;
    let mut told = 0u64;
    loop {
        let step = tokio::select! {
            data = c.copy_data() => Step::Data(data),
            () = sleep_until(next_status) => Step::Status,
            changed = confirmed.changed() => Step::Confirmed(changed.is_ok()),
        };
        let event = match step {
            Step::Status => {
                let now = *confirmed.borrow();
                told = tell(c, now, &mut next_status).await?;
                continue;
            }
            Step::Confirmed(alive) => {
                let now = *confirmed.borrow();
                if now > told {
                    told = tell(c, now, &mut next_status).await?;
                }
                if alive {
                    continue;
                }
                return Ok(());
            }
            Step::Data(data) => {
                let Some(body) = data? else {
                    return Err(Failure::Passing(
                        "the server ended the replication stream".to_owned(),
                    ));
                };
                *failures = 0;
                let not_understood = |why: String| Failure::Lasting(format!("{} sent {why}", c.what));
                match pgoutput::stream(body).map_err(not_understood)? {
                    Stream::Keepalive { end, reply } => {
                        if reply {
                            let now = *confirmed.borrow();
                            told = tell(c, now, &mut next_status).await?;
                        }
                        if in_transaction {
                            continue;
                        }
                        Event::Done(end)
                    }
                    Stream::Data { end, message } => {
                        match pgoutput::change(message)
                            .map_err(|why| Failure::Lasting(format!("{} sent {why}", c.what)))?
                        {
                            Change::Begin { commit_micros } => {
                                in_transaction = true;
                                committed_micros = Some(commit_micros);
                                continue;
                            }
                            Change::Commit { end } => {
                                in_transaction = false;
                                Event::Done(end)
                            }
                            Change::Relation { id, relation } => {
                                relations.insert(id, relation);
                                continue;
                            }
                            Change::Other => continue,
                            change => {
                                for message in messages(&relations, change, end, committed_micros, &c.what)? {
                                    if !deliver(
                                        c,
                                        events,
                                        Event::Message(message),
                                        confirmed,
                                        &mut next_status,
                                    )
                                    .await?
                                    {
                                        return Ok(());
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }
            }
        };
        if !deliver(c, events, event, confirmed, &mut next_status).await? {
            return Ok(());
        }
    }
}

/// Tell the server how far is safe; what was told.
async fn tell(c: &mut Connection, flushed: u64, next_status: &mut Instant) -> Result<u64, Failure> {
    c.send_copy_data(&pgoutput::status(flushed, now_micros())).await?;
    *next_status = Instant::now() + STATUS_EVERY;
    Ok(flushed)
}

/// Hand an event on, still telling the server how far is safe while the reading side is busy; false when it has
/// gone.
async fn deliver(
    c: &mut Connection,
    events: &mpsc::Sender<Event>,
    event: Event,
    confirmed: &watch::Receiver<u64>,
    next_status: &mut Instant,
) -> Result<bool, Failure> {
    loop {
        let permit = tokio::select! {
            permit = events.reserve() => Some(permit),
            () = sleep_until(*next_status) => None,
        };
        match permit {
            Some(Ok(permit)) => {
                permit.send(event);
                return Ok(true);
            }
            Some(Err(_)) => return Ok(false),
            None => {
                let now = *confirmed.borrow();
                tell(c, now, next_status).await?;
            }
        }
    }
}

/// Microseconds since 2000-01-01, PostgreSQL's epoch.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX) - 946_684_800_000_000)
        .unwrap_or(0)
}

/// The messages one change becomes: one per row, or one per table truncated.
fn messages(
    relations: &Relations,
    change: Change,
    lsn: u64,
    committed_micros: Option<i64>,
    what: &str,
) -> Result<Vec<SourceMessage>, Failure> {
    let relation = |id: u32| {
        relations
            .get(&id)
            .ok_or_else(|| Failure::Lasting(format!("{what} sent a change to a table it had not described")))
    };
    let committed = committed_micros.map(pgoutput::time);
    Ok(match change {
        Change::Insert { id, new } => vec![row(
            "insert",
            relation(id)?,
            None,
            Some(&new),
            lsn,
            committed.as_deref(),
        )],
        Change::Update { id, old, full, new } => {
            let r = relation(id)?;
            let before = old.map(|o| if full { o } else { keys_only(r, o) });
            vec![row(
                "update",
                r,
                before.as_deref(),
                Some(&new),
                lsn,
                committed.as_deref(),
            )]
        }
        Change::Delete { id, old, full } => {
            let r = relation(id)?;
            let before = if full { old } else { keys_only(r, old) };
            vec![row("delete", r, Some(&before), None, lsn, committed.as_deref())]
        }
        Change::Truncate { ids } => ids
            .into_iter()
            .map(|id| relation(id).map(|r| row("truncate", r, None, None, lsn, committed.as_deref())))
            .collect::<Result<_, _>>()?,
        _ => Vec::new(),
    })
}

/// An old row sent as its key only: the other columns are left out rather than shown as null.
fn keys_only(relation: &Relation, values: Vec<Value>) -> Vec<Value> {
    relation
        .columns
        .iter()
        .zip(values)
        .map(|(c, v)| if c.key { v } else { Value::Unchanged })
        .collect()
}

/// One row's change as a message: `{"op", "table", "before", "after", "lsn", "committed"}`, keyed by table and
/// primary key so one row's changes stay in order.
pub(super) fn row(
    op: &str,
    relation: &Relation,
    before: Option<&[Value]>,
    after: Option<&[Value]>,
    lsn: u64,
    committed: Option<&str>,
) -> SourceMessage {
    let columns: Vec<(&str, u32)> = relation
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.type_oid))
        .collect();
    let table = format!("{}.{}", relation.schema, relation.table);
    let render = |values: Option<&[Value]>| values.map(|v| pgoutput::object(&columns, v));
    let (before_json, after_json) = (render(before), render(after));
    let mut value = format!(
        "{{\"op\":{},\"table\":{},\"before\":{},\"after\":{},\"lsn\":{}",
        crate::json::json_string(op),
        crate::json::json_string(&table),
        before_json.as_ref().map_or("null", |(j, _)| j.as_str()),
        after_json.as_ref().map_or("null", |(j, _)| j.as_str()),
        crate::json::json_string(&pgoutput::lsn(lsn)),
    );
    if let Some(at) = committed {
        value.push_str(&format!(",\"committed\":{}", crate::json::json_string(at)));
    }
    let unchanged = after_json.as_ref().map(|(_, u)| u.clone()).unwrap_or_default();
    if !unchanged.is_empty() {
        let names: Vec<String> = unchanged.iter().map(|n| crate::json::json_string(n)).collect();
        value.push_str(&format!(",\"unchanged\":[{}]", names.join(",")));
    }
    value.push('}');
    let key_values = after.or(before).unwrap_or_default();
    let mut key = table.clone();
    for (column, v) in relation.columns.iter().zip(key_values) {
        if column.key {
            key.push('/');
            if let Value::Text(t) = v {
                key.push_str(&String::from_utf8_lossy(t));
            }
        }
    }
    SourceMessage {
        key: Some(key.into_bytes()),
        value: Some(value.into_bytes()),
        headers: vec![
            ("pg-table".to_owned(), table.into_bytes()),
            ("pg-op".to_owned(), op.as_bytes().to_vec()),
            ("pg-lsn".to_owned(), pgoutput::lsn(lsn).into_bytes()),
        ],
        ack: None,
    }
}

/// The hostile-input tests' door (see `crate::hostile`): `chunks` read as replication messages, one after another,
/// the way [`stream`] reads them from the server - the frame, the change, and the messages it becomes.
pub(crate) fn replay(chunks: Vec<bytes::Bytes>) {
    let mut relations = Relations::new();
    for body in chunks {
        let Ok(Stream::Data { end, message }) = pgoutput::stream(body) else {
            continue;
        };
        match pgoutput::change(message) {
            Ok(Change::Relation { id, relation }) => {
                relations.insert(id, relation);
            }
            Ok(change) => {
                let _ = messages(&relations, change, end, Some(0), "the server");
            }
            Err(_) => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;

    use super::super::pgoutput::Column;
    use super::*;

    fn orders() -> Relation {
        Relation {
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    type_oid: 23,
                    key: true,
                },
                Column {
                    name: "total".to_owned(),
                    type_oid: 1700,
                    key: false,
                },
                Column {
                    name: "notes".to_owned(),
                    type_oid: 25,
                    key: false,
                },
            ],
        }
    }

    fn text(s: &'static str) -> Value {
        Value::Text(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn a_change_is_one_message_keyed_by_table_and_primary_key() {
        let m = row(
            "update",
            &orders(),
            Some(&keys_only(&orders(), vec![text("42"), Value::Null, Value::Null])),
            Some(&[text("42"), text("19.90"), Value::Unchanged]),
            0x16_B374_D848,
            Some("2026-11-15T09:52:31.977Z"),
        );
        assert_eq!(m.key.as_deref(), Some(&b"public.orders/42"[..]));
        assert_eq!(
            String::from_utf8(m.value.unwrap()).unwrap(),
            r#"{"op":"update","table":"public.orders","before":{"id":42},"after":{"id":42,"total":19.90},"lsn":"16/B374D848","committed":"2026-11-15T09:52:31.977Z","unchanged":["notes"]}"#
        );
        assert_eq!(m.headers[1], ("pg-op".to_owned(), b"update".to_vec()));
        let gone = row(
            "delete",
            &orders(),
            Some(&[text("7"), Value::Null, Value::Null]),
            None,
            1,
            None,
        );
        assert!(
            String::from_utf8(gone.value.unwrap())
                .unwrap()
                .contains(r#""after":null"#)
        );
    }
}
