// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The replication stream and what PostgreSQL's built-in `pgoutput` plugin writes into it (protocol version 1):
//! transactions, the tables they touch, and each row inserted, updated or deleted, read with every length checked.
//! A row becomes one JSON message, its values written with their types: numbers exact as stored, JSON as JSON,
//! the rest as text.

use std::collections::HashMap;

use bytes::{Buf, Bytes};

use super::wire::{take_cstring, take_i32};
use crate::json::{is_json_number, json_string};

/// What the server sends on the replication stream.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Stream {
    /// WAL data: where it ends, and a `pgoutput` message.
    Data { end: u64, message: Bytes },
    /// The server's position, and whether it wants to hear from the client now.
    Keepalive { end: u64, reply: bool },
}

pub(super) fn stream(mut body: Bytes) -> Result<Stream, String> {
    let short = || "a replication message cut short".to_owned();
    match body.first().copied() {
        Some(b'w') if body.remaining() >= 25 => {
            body.advance(1);
            let _start = body.get_u64();
            let end = body.get_u64();
            let _sent = body.get_i64();
            Ok(Stream::Data { end, message: body })
        }
        Some(b'k') if body.remaining() >= 18 => {
            body.advance(1);
            let end = body.get_u64();
            let _sent = body.get_i64();
            Ok(Stream::Keepalive {
                end,
                reply: body.get_u8() == 1,
            })
        }
        Some(b'w' | b'k') => Err(short()),
        Some(other) => Err(format!("a replication message of a kind not known ({other})")),
        None => Err(short()),
    }
}

/// The client's position, for the server: everything before `flushed` is safe, so the WAL before it can go.
pub(super) fn status(flushed: u64, now_micros: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(34);
    out.push(b'r');
    for _ in 0..3 {
        out.extend_from_slice(&flushed.to_be_bytes());
    }
    out.extend_from_slice(&now_micros.to_be_bytes());
    out.push(0);
    out
}

/// A position in the WAL as PostgreSQL writes it: `16/B374D848`.
pub(super) fn lsn(position: u64) -> String {
    format!("{:X}/{:X}", position >> 32, position & 0xffff_ffff)
}

pub(super) fn parse_lsn(text: &str) -> Option<u64> {
    let (high, low) = text.split_once('/')?;
    Some((u64::from_str_radix(high, 16).ok()? << 32) | u64::from_str_radix(low, 16).ok()?)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) type_oid: u32,
    pub(super) key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Relation {
    pub(super) schema: String,
    pub(super) table: String,
    pub(super) columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Value {
    Null,
    /// A large value the update did not change, which the server does not send again.
    Unchanged,
    Text(Bytes),
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Change {
    Begin {
        commit_micros: i64,
    },
    Commit {
        end: u64,
    },
    Relation {
        id: u32,
        relation: Relation,
    },
    Insert {
        id: u32,
        new: Vec<Value>,
    },
    /// `old` is the row before, when the table sends it: whole (`full`), or only its key.
    Update {
        id: u32,
        old: Option<Vec<Value>>,
        full: bool,
        new: Vec<Value>,
    },
    Delete {
        id: u32,
        old: Vec<Value>,
        full: bool,
    },
    Truncate {
        ids: Vec<u32>,
    },
    /// Something this reader has no use for: an origin, a type, a logical message.
    Other,
}

const MAX_COLUMNS: usize = 1_664;

pub(super) fn change(mut m: Bytes) -> Result<Change, String> {
    let short = || "a change message cut short".to_owned();
    let kind = if m.has_remaining() {
        m.get_u8()
    } else {
        return Err(short());
    };
    let need = |m: &Bytes, n: usize| if m.remaining() >= n { Ok(()) } else { Err(short()) };
    Ok(match kind {
        b'B' => {
            need(&m, 20)?;
            let _final = m.get_u64();
            let commit_micros = m.get_i64();
            Change::Begin { commit_micros }
        }
        b'C' => {
            need(&m, 25)?;
            let _flags = m.get_u8();
            let _commit = m.get_u64();
            let end = m.get_u64();
            Change::Commit { end }
        }
        b'R' => {
            need(&m, 4)?;
            let id = m.get_u32();
            let schema = take_cstring(&mut m).ok_or_else(short)?;
            let table = take_cstring(&mut m).ok_or_else(short)?;
            need(&m, 3)?;
            let _identity = m.get_u8();
            let count = usize::try_from(m.get_i16()).map_err(|_| short())?;
            if count > MAX_COLUMNS {
                return Err(format!("a table of {count} columns, more than PostgreSQL allows"));
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                need(&m, 1)?;
                let flags = m.get_u8();
                let name = take_cstring(&mut m).ok_or_else(short)?;
                need(&m, 8)?;
                let type_oid = m.get_u32();
                let _modifier = m.get_i32();
                columns.push(Column {
                    name,
                    type_oid,
                    key: flags & 1 == 1,
                });
            }
            Change::Relation {
                id,
                relation: Relation {
                    schema,
                    table,
                    columns,
                },
            }
        }
        b'I' => {
            need(&m, 5)?;
            let id = m.get_u32();
            if m.get_u8() != b'N' {
                return Err("an insert without its new row".to_owned());
            }
            Change::Insert {
                id,
                new: tuple(&mut m)?,
            }
        }
        b'U' => {
            need(&m, 5)?;
            let id = m.get_u32();
            let mut old = None;
            let mut marker = m.get_u8();
            let full = marker == b'O';
            if matches!(marker, b'K' | b'O') {
                old = Some(tuple(&mut m)?);
                need(&m, 1)?;
                marker = m.get_u8();
            }
            if marker != b'N' {
                return Err("an update without its new row".to_owned());
            }
            Change::Update {
                id,
                old,
                full,
                new: tuple(&mut m)?,
            }
        }
        b'D' => {
            need(&m, 5)?;
            let id = m.get_u32();
            let marker = m.get_u8();
            if !matches!(marker, b'K' | b'O') {
                return Err("a delete without the row it deletes".to_owned());
            }
            Change::Delete {
                id,
                old: tuple(&mut m)?,
                full: marker == b'O',
            }
        }
        b'T' => {
            need(&m, 5)?;
            let count = usize::try_from(m.get_u32()).map_err(|_| short())?;
            let _options = m.get_u8();
            need(&m, count.saturating_mul(4))?;
            Change::Truncate {
                ids: (0..count).map(|_| m.get_u32()).collect(),
            }
        }
        _ => Change::Other,
    })
}

fn tuple(m: &mut Bytes) -> Result<Vec<Value>, String> {
    let short = || "a row cut short".to_owned();
    if m.remaining() < 2 {
        return Err(short());
    }
    let count = usize::try_from(m.get_i16()).map_err(|_| short())?;
    if count > MAX_COLUMNS {
        return Err(short());
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if !m.has_remaining() {
            return Err(short());
        }
        values.push(match m.get_u8() {
            b'n' => Value::Null,
            b'u' => Value::Unchanged,
            b't' => {
                let length = take_i32(m)
                    .and_then(|l| usize::try_from(l).ok())
                    .ok_or_else(short)?;
                if m.remaining() < length {
                    return Err(short());
                }
                Value::Text(m.split_to(length))
            }
            other => return Err(format!("a value of a kind not known ({other})")),
        });
    }
    Ok(values)
}

/// The tables the stream has described, by the server's id for them.
pub(super) type Relations = HashMap<u32, Relation>;

/// A row as a JSON object, the values in their types; the names of large values an update left unchanged.
pub(super) fn object(columns: &[(&str, u32)], values: &[Value]) -> (String, Vec<String>) {
    let mut out = String::from("{");
    let mut unchanged = Vec::new();
    let mut first = true;
    for ((name, type_oid), value) in columns.iter().zip(values) {
        let rendered = match value {
            Value::Unchanged => {
                unchanged.push((*name).to_owned());
                continue;
            }
            Value::Null => "null".to_owned(),
            Value::Text(text) => typed(*type_oid, text),
        };
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&json_string(name));
        out.push(':');
        out.push_str(&rendered);
    }
    out.push('}');
    (out, unchanged)
}

/// A value from the server's text for it: numbers and booleans as JSON numbers and booleans, exact; JSON as
/// itself; everything else (dates, times, ids, arrays, bytes) as the text PostgreSQL writes for it.
pub(super) fn typed(type_oid: u32, text: &[u8]) -> String {
    let as_text = || json_string(&String::from_utf8_lossy(text));
    match type_oid {
        // bool
        16 => match text {
            b"t" => "true".to_owned(),
            b"f" => "false".to_owned(),
            _ => as_text(),
        },
        // int2, int4, int8, oid, float4, float8, numeric: the server's text is a JSON number unless not finite.
        20 | 21 | 23 | 26 | 700 | 701 | 1700 => {
            let s = String::from_utf8_lossy(text);
            if is_json_number(&s) {
                s.into_owned()
            } else {
                as_text()
            }
        }
        // json, jsonb
        114 | 3802 => {
            let s = String::from_utf8_lossy(text);
            if openduct_expr::Value::parse_json(&s).is_ok() {
                s.trim().to_owned()
            } else {
                as_text()
            }
        }
        _ => as_text(),
    }
}

/// Microseconds since 2000-01-01 (PostgreSQL's epoch) as `2026-11-15T09:52:31.977Z`.
pub(super) fn time(micros: i64) -> String {
    const EPOCH_2000: i64 = 946_684_800;
    let seconds = micros.div_euclid(1_000_000) + EPOCH_2000;
    let nanos = u32::try_from(micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0);
    chrono::DateTime::from_timestamp(seconds, nanos)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::BufMut;

    use super::*;

    fn relation_message() -> Bytes {
        let mut m = vec![b'R'];
        m.put_u32(16_384);
        m.extend_from_slice(b"public\0orders\0");
        m.put_u8(b'd');
        m.put_i16(3);
        for (flags, name, oid) in [(1u8, "id", 23u32), (0, "total", 1700), (0, "notes", 25)] {
            m.put_u8(flags);
            m.extend_from_slice(name.as_bytes());
            m.put_u8(0);
            m.put_u32(oid);
            m.put_i32(-1);
        }
        Bytes::from(m)
    }

    #[test]
    fn changes_are_read() {
        let Change::Relation { id, relation } = change(relation_message()).unwrap() else {
            panic!("a relation");
        };
        assert_eq!(
            (id, relation.table.as_str(), relation.columns.len()),
            (16_384, "orders", 3)
        );
        assert!(relation.columns[0].key && !relation.columns[1].key);

        let mut update = vec![b'U'];
        update.put_u32(16_384);
        update.extend_from_slice(b"K\x00\x03t\x00\x00\x00\x0242nn");
        update.extend_from_slice(b"N\x00\x03t\x00\x00\x00\x0242t\x00\x00\x00\x0519.90u");
        let Change::Update { old, new, .. } = change(Bytes::from(update)).unwrap() else {
            panic!("an update");
        };
        assert_eq!(old.unwrap()[0], Value::Text(Bytes::from_static(b"42")));
        assert_eq!(new[2], Value::Unchanged);

        let mut commit = vec![b'C', 0];
        commit.put_u64(0x16_0000_0100);
        commit.put_u64(0x16_0000_0180);
        commit.put_i64(0);
        assert_eq!(
            change(Bytes::from(commit)).unwrap(),
            Change::Commit { end: 0x16_0000_0180 }
        );
    }

    #[test]
    fn what_cannot_be_is_refused() {
        // A value claiming more bytes than the message has, a row claiming columns it does not have, a table of
        // more columns than PostgreSQL allows, and a message cut short.
        for bad in [
            &b"I\x00\x00\x40\x00N\x00\x01t\x7f\xff\xff\xff"[..],
            b"I\x00\x00\x40\x00N\x00\x05n",
            b"R\x00\x00\x40\x00public\0t\0d\x7f\xff",
            b"C\x00\x01",
            b"",
        ] {
            assert!(change(Bytes::copy_from_slice(bad)).is_err(), "{bad:?}");
        }
        assert!(stream(Bytes::from_static(b"w\x00\x00")).is_err());
        assert!(stream(Bytes::from_static(b"x")).is_err());
    }

    #[test]
    fn rows_are_json_in_their_types() {
        let columns = [
            ("id", 23),
            ("total", 1700),
            ("paid", 16),
            ("meta", 3802),
            ("name", 25),
            ("ratio", 701),
            ("big", 25),
        ];
        let values = [
            Value::Text(Bytes::from_static(b"42")),
            Value::Text(Bytes::from_static(b"19.900")),
            Value::Text(Bytes::from_static(b"t")),
            Value::Text(Bytes::from_static(br#"{"vip": true}"#)),
            Value::Text(Bytes::from_static(b"O'Brien \"Jr\"\n")),
            Value::Text(Bytes::from_static(b"NaN")),
            Value::Unchanged,
        ];
        let (json, unchanged) = object(&columns, &values);
        assert_eq!(
            json,
            r#"{"id":42,"total":19.900,"paid":true,"meta":{"vip": true},"name":"O'Brien \"Jr\"\n","ratio":"NaN"}"#
        );
        assert_eq!(unchanged, vec!["big".to_owned()]);
        for (text, number) in [
            ("0", true),
            ("-12.5e3", true),
            ("1.", false),
            ("01", false),
            ("Infinity", false),
            ("", false),
        ] {
            assert_eq!(is_json_number(text), number, "{text}");
        }
    }

    #[test]
    fn positions_and_times_are_written_as_postgresql_writes_them() {
        assert_eq!(lsn(0x16_B374_D848), "16/B374D848");
        assert_eq!(parse_lsn("16/B374D848"), Some(0x16_B374_D848));
        assert_eq!(parse_lsn("nonsense"), None);
        assert_eq!(time(0), "2000-01-01T00:00:00.000Z");
        assert_eq!(time(848_051_551_977_000), "2026-11-15T09:52:31.977Z");
        let s = status(0x10, 5);
        assert_eq!((s.len(), s[0], s[8]), (34, b'r', 0x10));
    }
}
