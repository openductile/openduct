// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Events of MySQL's binary log: each header, the CRC32 each event ends with (checked), rotations to the next
//! file, transactions, table maps with their full column metadata (names, signedness, character sets, enum and set
//! values, primary key), and row events (versions 1 and 2), every length checked against the event.

use bytes::{Buf, Bytes};

use super::values::{self, Column};
use super::wire::take_lenenc;

const QUERY: u8 = 2;
const ROTATE: u8 = 4;
const FORMAT_DESCRIPTION: u8 = 15;
const XID: u8 = 16;
const TABLE_MAP: u8 = 19;
const WRITE_ROWS_V1: u8 = 23;
const UPDATE_ROWS_V1: u8 = 24;
const DELETE_ROWS_V1: u8 = 25;
const WRITE_ROWS: u8 = 30;
const UPDATE_ROWS: u8 = 31;
const DELETE_ROWS: u8 = 32;
const TRANSACTION_PAYLOAD: u8 = 40;
const HEADER: usize = 19;
const MAX_COLUMNS: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Table {
    pub(super) schema: String,
    pub(super) name: String,
    pub(super) columns: Vec<Column>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Op {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Event {
    /// The log goes on in `file`, from `position`.
    Rotate {
        file: String,
        position: u64,
    },
    /// A statement; `BEGIN` starts a transaction, `COMMIT` ends one that touched no transactional table.
    Query {
        statement: String,
    },
    /// A transaction committed.
    Commit,
    TableMap {
        id: u64,
        table: Table,
    },
    /// Rows changed in table `table_id`: whole rows, or (updates) before and after, as JSON values by column.
    Rows {
        op: Op,
        table_id: u64,
        data: Bytes,
        after_present: Option<Vec<bool>>,
        present: Vec<bool>,
    },
    Other,
}

/// An event's header: when it was written, its kind, and the position after it in its file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Header {
    pub(super) timestamp: u32,
    pub(super) kind: u8,
    pub(super) next: u32,
}

#[derive(Debug, Default)]
pub(super) struct Reader {
    checksum: bool,
}

impl Reader {
    /// A reader for a server that ends its events with a CRC32 (`binlog_checksum = CRC32`) or not. It must be known
    /// from the start: the first event, before the one saying how events are checked, already has one.
    pub(super) fn new(checksum: bool) -> Self {
        Self { checksum }
    }
}

fn short() -> String {
    "an event cut short".to_owned()
}

fn need(b: &Bytes, n: usize) -> Result<(), String> {
    if b.remaining() < n { Err(short()) } else { Ok(()) }
}

fn take_u48(b: &mut Bytes) -> Result<u64, String> {
    need(b, 6)?;
    let low = u64::from(b.get_u32_le());
    Ok(low | u64::from(b.get_u16_le()) << 32)
}

fn take_name(b: &mut Bytes) -> Result<String, String> {
    need(b, 1)?;
    let length = usize::from(b.get_u8());
    need(b, length + 1)?;
    let name = String::from_utf8_lossy(&b.split_to(length)).into_owned();
    b.advance(1);
    Ok(name)
}

fn take_bitmap(b: &mut Bytes, bits: usize) -> Result<Vec<bool>, String> {
    let bytes = bits.div_ceil(8);
    need(b, bytes)?;
    let raw = b.split_to(bytes);
    Ok((0..bits)
        .map(|i| raw.get(i / 8).is_some_and(|byte| byte >> (i % 8) & 1 == 1))
        .collect())
}

fn lenenc_usize(b: &mut Bytes) -> Result<usize, String> {
    take_lenenc(b)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(short)
}

impl Reader {
    /// One event, as the stream gives it.
    pub(super) fn event(&mut self, mut raw: Bytes) -> Result<(Header, Event), String> {
        need(&raw, HEADER)?;
        let whole = raw.clone();
        let timestamp = raw.get_u32_le();
        let kind = raw.get_u8();
        let _server = raw.get_u32_le();
        let size = usize::try_from(raw.get_u32_le()).map_err(|_| short())?;
        let next = raw.get_u32_le();
        let _flags = raw.get_u16_le();
        if size != whole.len() {
            return Err(format!(
                "an event of {} bytes that says it has {size}",
                whole.len()
            ));
        }
        let header = Header {
            timestamp,
            kind,
            next,
        };
        if kind == FORMAT_DESCRIPTION {
            // It ends with how events are checked: 1 for CRC32, then its own CRC32.
            let algorithm = whole
                .len()
                .checked_sub(5)
                .and_then(|at| whole.get(at))
                .copied()
                .unwrap_or(0);
            self.checksum = algorithm == 1;
        }
        if self.checksum {
            let body_end = whole.len().checked_sub(4).ok_or_else(short)?;
            let (body, sum) = (
                whole.get(..body_end).ok_or_else(short)?,
                whole.get(body_end..).ok_or_else(short)?,
            );
            let given = u32::from_le_bytes(sum.try_into().map_err(|_| short())?);
            if crc32fast::hash(body) != given {
                return Err(
                    "an event whose CRC32 does not match: the binary log or the connection is damaged"
                        .to_owned(),
                );
            }
            raw.truncate(raw.remaining().saturating_sub(4));
        }
        Ok((header, self.body(kind, raw)?))
    }

    fn body(&self, kind: u8, mut b: Bytes) -> Result<Event, String> {
        Ok(match kind {
            ROTATE => {
                need(&b, 8)?;
                let position = b.get_u64_le();
                Event::Rotate {
                    file: String::from_utf8_lossy(&b).into_owned(),
                    position,
                }
            }
            QUERY => {
                need(&b, 13)?;
                let _thread = b.get_u32_le();
                let _seconds = b.get_u32_le();
                let schema_length = usize::from(b.get_u8());
                let _error = b.get_u16_le();
                let status_length = usize::from(b.get_u16_le());
                need(&b, status_length + schema_length + 1)?;
                b.advance(status_length + schema_length + 1);
                Event::Query {
                    statement: String::from_utf8_lossy(&b).into_owned(),
                }
            }
            XID => Event::Commit,
            TABLE_MAP => table_map(b)?,
            WRITE_ROWS | UPDATE_ROWS | DELETE_ROWS | WRITE_ROWS_V1 | UPDATE_ROWS_V1 | DELETE_ROWS_V1 => {
                let table_id = take_u48(&mut b)?;
                need(&b, 2)?;
                let _flags = b.get_u16_le();
                if matches!(kind, WRITE_ROWS | UPDATE_ROWS | DELETE_ROWS) {
                    need(&b, 2)?;
                    let extra = usize::from(b.get_u16_le()).saturating_sub(2);
                    need(&b, extra)?;
                    b.advance(extra);
                }
                let count = lenenc_usize(&mut b)?;
                if count > MAX_COLUMNS {
                    return Err(short());
                }
                let present = take_bitmap(&mut b, count)?;
                let update = matches!(kind, UPDATE_ROWS | UPDATE_ROWS_V1);
                let after_present = if update {
                    Some(take_bitmap(&mut b, count)?)
                } else {
                    None
                };
                let op = match kind {
                    WRITE_ROWS | WRITE_ROWS_V1 => Op::Insert,
                    UPDATE_ROWS | UPDATE_ROWS_V1 => Op::Update,
                    _ => Op::Delete,
                };
                Event::Rows {
                    op,
                    table_id,
                    data: b,
                    after_present,
                    present,
                }
            }
            TRANSACTION_PAYLOAD => {
                return Err(
                    "the server compresses its binary log (binlog_transaction_compression = ON), which OpenDuct does \
                     not read yet: turn it off"
                        .to_owned(),
                );
            }
            _ => Event::Other,
        })
    }
}

fn table_map(mut b: Bytes) -> Result<Event, String> {
    let id = take_u48(&mut b)?;
    need(&b, 2)?;
    let _flags = b.get_u16_le();
    let schema = take_name(&mut b)?;
    let name = take_name(&mut b)?;
    let count = lenenc_usize(&mut b)?;
    if count > MAX_COLUMNS {
        return Err(short());
    }
    need(&b, count)?;
    let kinds = b.split_to(count);
    let meta_length = lenenc_usize(&mut b)?;
    need(&b, meta_length)?;
    let mut meta = b.split_to(meta_length);
    let mut columns: Vec<Column> = Vec::with_capacity(count);
    for &kind in kinds.iter() {
        let size = values::meta_size(kind);
        need(&meta, size)?;
        let m = meta.split_to(size);
        let m = [m.first().copied().unwrap_or(0), m.get(1).copied().unwrap_or(0)];
        // VARCHAR and BIT keep their two metadata bytes little-endian; the rest as written.
        columns.push(Column {
            kind,
            meta: m,
            ..Column::default()
        });
    }
    let _nullable = take_bitmap(&mut b, count)?;
    optional_metadata(&mut b, &mut columns)?;
    Ok(Event::TableMap {
        id,
        table: Table {
            schema,
            name,
            columns,
        },
    })
}

/// The real type of a column: CHAR, ENUM and SET are all written STRING, with the real type in their metadata.
fn real_kind(c: &Column) -> u8 {
    if c.kind == values::STRING && c.meta[0] & 0x30 == 0x30 {
        c.meta[0]
    } else {
        c.kind
    }
}

fn numeric(c: &Column) -> bool {
    matches!(
        c.kind,
        values::TINY
            | values::SHORT
            | values::INT24
            | values::LONG
            | values::LONGLONG
            | values::FLOAT
            | values::DOUBLE
            | values::NEWDECIMAL
    )
}

fn character(c: &Column) -> bool {
    matches!(
        real_kind(c),
        values::STRING | values::VAR_STRING | values::VARCHAR | values::BLOB
    )
}

/// The optional metadata MySQL writes with `binlog_row_metadata = FULL`: names, signedness, character sets, enum
/// and set values, the primary key.
fn optional_metadata(b: &mut Bytes, columns: &mut [Column]) -> Result<(), String> {
    while b.has_remaining() {
        let field = b.get_u8();
        let length = lenenc_usize(b)?;
        need(b, length)?;
        let mut v = b.split_to(length);
        match field {
            // Signedness: one bit per numeric column, the highest bit first.
            1 => {
                for (i, c) in columns.iter_mut().filter(|c| numeric(c)).enumerate() {
                    c.unsigned = v.get(i / 8).is_some_and(|byte| byte >> (7 - i % 8) & 1 == 1);
                }
            }
            // The default character set, then the character columns that differ from it.
            2 => {
                let default = take_lenenc(&mut v).ok_or_else(short)?;
                let indexes: Vec<usize> = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| character(c))
                    .map(|(i, _)| i)
                    .collect();
                for &i in &indexes {
                    if let Some(c) = columns.get_mut(i) {
                        c.binary = default == u64::from(values::BINARY);
                    }
                }
                while v.has_remaining() {
                    let nth = lenenc_usize(&mut v)?;
                    let charset = take_lenenc(&mut v).ok_or_else(short)?;
                    if let Some(c) = indexes.get(nth).and_then(|i| columns.get_mut(*i)) {
                        c.binary = charset == u64::from(values::BINARY);
                    }
                }
            }
            // A character set for each character column.
            3 => {
                for c in columns.iter_mut().filter(|c| character(c)) {
                    let charset = take_lenenc(&mut v).ok_or_else(short)?;
                    c.binary = charset == u64::from(values::BINARY);
                }
            }
            4 => {
                for c in columns.iter_mut() {
                    let name = super::wire::take_lenenc_bytes(&mut v).ok_or_else(short)?;
                    c.name = String::from_utf8_lossy(&name).into_owned();
                }
            }
            5 | 6 => {
                let wanted = if field == 5 { values::SET } else { values::ENUM };
                for c in columns.iter_mut().filter(|c| real_kind(c) == wanted) {
                    let count = lenenc_usize(&mut v)?;
                    let mut names = Vec::with_capacity(count.min(v.remaining()));
                    for _ in 0..count {
                        let name = super::wire::take_lenenc_bytes(&mut v).ok_or_else(short)?;
                        names.push(String::from_utf8_lossy(&name).into_owned());
                    }
                    if field == 5 {
                        c.set_values = names
                    } else {
                        c.enum_values = names
                    }
                }
            }
            // The primary key: its columns, or its columns with prefix lengths.
            8 | 9 => {
                while v.has_remaining() {
                    let i = lenenc_usize(&mut v)?;
                    if field == 9 {
                        lenenc_usize(&mut v)?;
                    }
                    if let Some(c) = columns.get_mut(i) {
                        c.key = true;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// One row image: a JSON value (or `None`, for a column the image leaves out) for each column.
pub(super) fn row(table: &Table, present: &[bool], data: &mut Bytes) -> Result<Vec<Option<String>>, String> {
    let shown = present.iter().filter(|p| **p).count();
    let nulls = take_bitmap(data, shown)?;
    let mut out = Vec::with_capacity(table.columns.len());
    let mut nth = 0;
    for (i, column) in table.columns.iter().enumerate() {
        if !present.get(i).copied().unwrap_or(false) {
            out.push(None);
            continue;
        }
        let null = nulls.get(nth).copied().unwrap_or(false);
        nth += 1;
        out.push(Some(if null {
            "null".to_owned()
        } else {
            values::binlog(column, data)?
        }));
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::BufMut;

    use super::*;

    fn event(kind: u8, body: &[u8], next: u32, checksum: bool) -> Bytes {
        let size = HEADER + body.len() + if checksum { 4 } else { 0 };
        let mut e = Vec::with_capacity(size);
        e.put_u32_le(1_763_200_000);
        e.put_u8(kind);
        e.put_u32_le(1);
        e.put_u32_le(u32::try_from(size).unwrap());
        e.put_u32_le(next);
        e.put_u16_le(0);
        e.extend_from_slice(body);
        if checksum {
            let sum = crc32fast::hash(&e);
            e.put_u32_le(sum);
        }
        Bytes::from(e)
    }

    /// shop.orders (id INT PRIMARY KEY, total DECIMAL(10,2), note VARCHAR(100)), with FULL metadata.
    fn orders_map() -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&[7, 0, 0, 0, 0, 0, 1, 0]);
        m.extend_from_slice(b"\x04shop\0\x06orders\0");
        m.push(3);
        m.extend_from_slice(&[values::LONG, values::NEWDECIMAL, values::VARCHAR]);
        m.push(4);
        m.extend_from_slice(&[10, 2, 100, 0]);
        m.push(0b110);
        // Signedness (both numeric columns signed), names, primary key column 0.
        m.extend_from_slice(&[1, 1, 0]);
        m.extend_from_slice(&[
            4, 14, 2, b'i', b'd', 5, b't', b'o', b't', b'a', b'l', 4, b'n', b'o', b't', b'e',
        ]);
        m.extend_from_slice(&[8, 1, 0]);
        m
    }

    #[test]
    fn a_table_map_and_its_rows_are_read() {
        let mut reader = Reader { checksum: true };
        let (header, map) = reader.event(event(TABLE_MAP, &orders_map(), 300, true)).unwrap();
        assert_eq!(header.next, 300);
        let Event::TableMap { id, table } = map else {
            panic!("a table map")
        };
        assert_eq!(
            (id, table.name.as_str(), table.columns[1].name.as_str()),
            (7, "orders", "total")
        );
        assert!(table.columns[0].key && !table.columns[1].key);
        let mut rows = vec![7, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 0b111];
        rows.push(0b100);
        rows.extend_from_slice(&42i32.to_le_bytes());
        rows.extend_from_slice(&[0x80, 0x00, 0x00, 0x13, 0x5a]);
        let (
            _,
            Event::Rows {
                op, data, present, ..
            },
        ) = reader.event(event(WRITE_ROWS, &rows, 400, true)).unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(op, Op::Insert);
        let values = row(&table, &present, &mut data.clone()).unwrap();
        assert_eq!(
            values,
            vec![
                Some("42".to_owned()),
                Some("19.90".to_owned()),
                Some("null".to_owned())
            ]
        );
    }

    #[test]
    fn damaged_and_impossible_events_are_refused() {
        let mut reader = Reader { checksum: true };
        let mut damaged = event(XID, &[1, 0, 0, 0, 0, 0, 0, 0], 10, true).to_vec();
        damaged[20] ^= 1;
        assert!(reader.event(Bytes::from(damaged)).unwrap_err().contains("CRC32"));
        let mut lying = event(XID, &[0; 8], 10, false).to_vec();
        lying[9] = 200;
        assert!(Reader::default().event(Bytes::from(lying)).is_err());
        let mut map = orders_map();
        // The column count, made a length of eight bytes that is not there.
        map[22] = 0xfe;
        assert!(Reader::default().event(event(TABLE_MAP, &map, 1, false)).is_err());
        assert!(
            Reader::default()
                .event(event(TRANSACTION_PAYLOAD, &[], 1, false))
                .unwrap_err()
                .contains("compresses")
        );
        assert_eq!(
            Reader::default()
                .event(event(
                    ROTATE,
                    b"\x04\x00\x00\x00\x00\x00\x00\x00binlog.000002",
                    0,
                    false
                ))
                .unwrap()
                .1,
            Event::Rotate {
                file: "binlog.000002".to_owned(),
                position: 4
            }
        );
    }
}
