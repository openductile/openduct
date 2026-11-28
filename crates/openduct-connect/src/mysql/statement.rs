// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Prepared statements in MySQL's binary protocol, for a workflow's `mysql` step: the SQL prepared as written, with
//! `?` where its values go; each value sent beside it with a type of its own (COM_STMT_EXECUTE); the rows read in
//! binary, each column by its type. Every length is checked before it is read.

use bytes::{Buf, BufMut, Bytes};
use serde_json::{Map, Value};

use super::wire::{
    Connection, DEPRECATE_EOF, Failure, put_lenenc_bytes, server_error, take_lenenc, take_lenenc_bytes,
};
use crate::sql::{Answer, Budget, Param, fraction, hex, number};

// Column types, as MySQL numbers them (enum_field_types).
const DECIMAL: u8 = 0;
const TINY: u8 = 1;
const SHORT: u8 = 2;
const LONG: u8 = 3;
const FLOAT: u8 = 4;
const DOUBLE: u8 = 5;
const NULL: u8 = 6;
const TIMESTAMP: u8 = 7;
const LONGLONG: u8 = 8;
const INT24: u8 = 9;
const DATE: u8 = 10;
const TIME: u8 = 11;
const DATETIME: u8 = 12;
const YEAR: u8 = 13;
const BIT: u8 = 16;
const VECTOR: u8 = 242;
const JSON: u8 = 245;
const NEWDECIMAL: u8 = 246;
const VAR_STRING: u8 = 253;
const GEOMETRY: u8 = 255;
/// The character set of binary data.
const BINARY: u16 = 63;
const UNSIGNED: u16 = 32;
/// More results follow this one (a procedure's).
const MORE_RESULTS: u16 = 0x0008;
/// The most results one statement gives (a procedure's), and columns in one.
const MOST_RESULTS: usize = 64;
const MOST_COLUMNS: u64 = 4096;

/// A column of a binary result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) kind: u8,
    pub(super) flags: u16,
    pub(super) charset: u16,
    pub(super) decimals: u8,
}

fn column(b: &mut Bytes) -> Option<Column> {
    for _ in 0..4 {
        take_lenenc_bytes(b)?;
    }
    let name = String::from_utf8_lossy(&take_lenenc_bytes(b)?).into_owned();
    take_lenenc_bytes(b)?;
    take_lenenc(b)?;
    if b.remaining() < 2 + 4 + 1 + 2 + 1 {
        return None;
    }
    let charset = b.get_u16_le();
    let _length = b.get_u32_le();
    let kind = b.get_u8();
    let flags = b.get_u16_le();
    let decimals = b.get_u8();
    Some(Column {
        name,
        kind,
        flags,
        charset,
        decimals,
    })
}

/// What an OK packet says: rows changed, the id given, the server's status.
fn ok_packet(mut b: Bytes) -> Option<(u64, u64, u16)> {
    b.advance(1);
    let affected = take_lenenc(&mut b)?;
    let id = take_lenenc(&mut b)?;
    (b.remaining() >= 2).then(|| (affected, id, b.get_u16_le()))
}

impl Connection {
    /// Reads `count` column definitions, and the EOF after them on a server that sends one.
    async fn columns(&mut self, count: u64) -> Result<Vec<Column>, Failure> {
        if count > MOST_COLUMNS {
            return Err(self.not_understood());
        }
        let mut out = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let mut definition = self.read_packet().await?;
            out.push(column(&mut definition).ok_or_else(|| self.not_understood())?);
        }
        if count > 0 && self.capabilities & DEPRECATE_EOF == 0 {
            let _eof = self.read_packet().await?;
        }
        Ok(out)
    }

    /// Prepares `sql`: its id, and how many `?` it has.
    async fn prepare(&mut self, sql: &str) -> Result<(u32, usize), Failure> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(0x16);
        payload.extend_from_slice(sql.as_bytes());
        self.command(&payload).await?;
        let mut first = self.read_packet().await?;
        if first.first() == Some(&0xff) {
            return Err(server_error(&self.what, first));
        }
        if first.remaining() < 9 || first.get_u8() != 0 {
            return Err(self.not_understood());
        }
        let id = first.get_u32_le();
        let columns = first.get_u16_le();
        let params = first.get_u16_le();
        self.columns(u64::from(params)).await?;
        self.columns(u64::from(columns)).await?;
        Ok((id, usize::from(params)))
    }

    /// Runs a prepared SQL with `params`, reading its results within `budget`.
    pub(super) async fn execute(
        &mut self,
        sql: &str,
        params: &[Param],
        budget: &mut Budget,
    ) -> Result<Vec<Answer>, Failure> {
        let (id, wanted) = self.prepare(sql).await?;
        if wanted != params.len() {
            self.close(id).await;
            return Err(Failure::Lasting(format!(
                "the SQL has {wanted} places for values (?) and is given {}",
                params.len()
            )));
        }
        self.command(&execute_payload(id, params)).await?;
        let answers = self.results(budget).await;
        self.close(id).await;
        answers
    }

    async fn close(&mut self, id: u32) {
        let mut payload = vec![0x19];
        payload.put_u32_le(id);
        // No answer comes to a close.
        let _ = self.command(&payload).await;
    }

    /// Every result of an executed statement.
    async fn results(&mut self, budget: &mut Budget) -> Result<Vec<Answer>, Failure> {
        let mut answers = Vec::new();
        loop {
            if answers.len() >= MOST_RESULTS {
                return Err(Failure::Lasting(format!(
                    "{} gave more than {MOST_RESULTS} results",
                    self.what
                )));
            }
            let mut first = self.read_packet().await?;
            match first.first().copied() {
                Some(0x00) => {
                    let (affected, id, status) = ok_packet(first).ok_or_else(|| self.not_understood())?;
                    answers.push(Answer {
                        affected: Some(affected),
                        insert_id: (id != 0).then_some(id),
                        ..Answer::default()
                    });
                    if status & MORE_RESULTS == 0 {
                        return Ok(answers);
                    }
                    continue;
                }
                Some(0xff) => return Err(server_error(&self.what, first)),
                _ => {}
            }
            let count = take_lenenc(&mut first).ok_or_else(|| self.not_understood())?;
            let columns = self.columns(count).await?;
            let mut answer = Answer {
                columns: columns.iter().map(|c| c.name.clone()).collect(),
                ..Answer::default()
            };
            let mut too_much = None;
            let status = loop {
                let packet = self.read_packet().await?;
                match packet.first().copied() {
                    Some(0xfe) if packet.len() < 0xff_ffff => break end_status(packet, self.capabilities),
                    Some(0xff) => return Err(server_error(&self.what, packet)),
                    _ => {}
                }
                // Past a limit, the rest is read and let go, so the connection stays in step.
                if too_much.is_some() {
                    continue;
                }
                if let Err(e) = budget.row(answer.rows.len() + 1, packet.len()) {
                    too_much = Some(e);
                    answer.rows.clear();
                    continue;
                }
                answer
                    .rows
                    .push(row(&columns, packet).ok_or_else(|| self.not_understood())?);
            };
            if let Some(e) = too_much {
                return Err(Failure::Lasting(e));
            }
            answers.push(answer);
            if status & MORE_RESULTS == 0 {
                return Ok(answers);
            }
        }
    }
}

/// The server's status at the end of a result: in an EOF packet, or (with DEPRECATE_EOF) an OK packet headed 0xfe.
fn end_status(mut packet: Bytes, capabilities: u32) -> u16 {
    if capabilities & DEPRECATE_EOF != 0 {
        return ok_packet(packet).map_or(0, |(_, _, s)| s);
    }
    packet.advance(1);
    if packet.remaining() >= 4 {
        let _warnings = packet.get_u16_le();
        packet.get_u16_le()
    } else {
        0
    }
}

/// COM_STMT_EXECUTE for statement `id` with `params`, each with its own type.
fn execute_payload(id: u32, params: &[Param]) -> Vec<u8> {
    let mut out = vec![0x17];
    out.put_u32_le(id);
    out.push(0); // no cursor
    out.put_u32_le(1);
    if params.is_empty() {
        return out;
    }
    let mut nulls = vec![0u8; params.len().div_ceil(8)];
    for (i, p) in params.iter().enumerate() {
        if matches!(p, Param::Null)
            && let Some(byte) = nulls.get_mut(i / 8)
        {
            *byte |= 1 << (i % 8);
        }
    }
    out.extend_from_slice(&nulls);
    out.push(1); // the types follow
    for p in params {
        let kind = match p {
            Param::Null => NULL,
            Param::Bool(_) => TINY,
            Param::Int(_) => LONGLONG,
            Param::Decimal(_) => NEWDECIMAL,
            Param::Text(_) => VAR_STRING,
            Param::Time(_) => DATETIME,
        };
        out.extend_from_slice(&[kind, 0]);
    }
    for p in params {
        match p {
            Param::Null => {}
            Param::Bool(b) => out.push(u8::from(*b)),
            Param::Int(n) => out.put_i64_le(*n),
            Param::Decimal(t) | Param::Text(t) => put_lenenc_bytes(&mut out, t.as_bytes()),
            Param::Time(t) => {
                out.push(11);
                out.put_u16_le(u16::try_from(t.year).unwrap_or(0));
                out.extend_from_slice(&[t.month, t.day, t.hour, t.minute, t.second]);
                out.put_u32_le(t.micros);
            }
        }
    }
    out
}

/// A binary row: `0x00`, the null bitmap (two bits in), then each value that is not null.
fn row(columns: &[Column], mut b: Bytes) -> Option<Map<String, Value>> {
    if b.first() != Some(&0) {
        return None;
    }
    b.advance(1);
    let size = (columns.len() + 9) / 8;
    if b.remaining() < size {
        return None;
    }
    let nulls = b.split_to(size);
    let mut out = Map::new();
    for (i, c) in columns.iter().enumerate() {
        let null = nulls
            .get((i + 2) / 8)
            .is_some_and(|byte| byte & (1 << ((i + 2) % 8)) != 0);
        let value = if null || c.kind == NULL {
            Value::Null
        } else {
            value(c, &mut b)?
        };
        out.insert(c.name.clone(), value);
    }
    Some(out)
}

fn fixed(b: &mut Bytes, n: usize) -> Option<Bytes> {
    (b.remaining() >= n).then(|| b.split_to(n))
}

fn integer(bytes: &[u8], unsigned: bool) -> Value {
    let mut raw = [0u8; 8];
    for (to, from) in raw.iter_mut().zip(bytes) {
        *to = *from;
    }
    let n = u64::from_le_bytes(raw);
    if unsigned {
        return Value::from(n);
    }
    let bits = 8 * bytes.len() as u32;
    let shift = 64 - bits;
    // Sign-extend from the value's own width.
    Value::from((n << shift) as i64 >> shift)
}

/// A value as its column's type reads, in the binary protocol.
fn value(c: &Column, b: &mut Bytes) -> Option<Value> {
    let unsigned = c.flags & UNSIGNED != 0;
    Some(match c.kind {
        TINY => integer(&fixed(b, 1)?, unsigned),
        SHORT | YEAR => integer(&fixed(b, 2)?, unsigned),
        LONG | INT24 => integer(&fixed(b, 4)?, unsigned),
        LONGLONG => integer(&fixed(b, 8)?, unsigned),
        FLOAT => {
            let f = f32::from_le_bytes(fixed(b, 4)?.as_ref().try_into().ok()?);
            if f.is_finite() {
                number(&f.to_string())
            } else {
                Value::Null
            }
        }
        DOUBLE => {
            let f = f64::from_le_bytes(fixed(b, 8)?.as_ref().try_into().ok()?);
            serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number)
        }
        DATE | DATETIME | TIMESTAMP => {
            let length = usize::from(*fixed(b, 1)?.first()?);
            let raw = fixed(b, length)?;
            Value::String(datetime(&raw, c.kind == DATE, u32::from(c.decimals.min(6)))?)
        }
        TIME => {
            let length = usize::from(*fixed(b, 1)?.first()?);
            Value::String(time(&fixed(b, length)?, u32::from(c.decimals.min(6)))?)
        }
        _ => {
            let raw = take_lenenc_bytes(b)?;
            match c.kind {
                DECIMAL | NEWDECIMAL => number(&String::from_utf8_lossy(&raw)),
                JSON => serde_json::from_slice(&raw)
                    .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&raw).into_owned())),
                BIT if raw.len() <= 8 => Value::from(raw.iter().fold(0u64, |n, x| n << 8 | u64::from(*x))),
                VECTOR if raw.len() % 4 == 0 => Value::Array(
                    raw.as_chunks::<4>()
                        .0
                        .iter()
                        .map(|f| number(&f32::from_le_bytes(*f).to_string()))
                        .collect(),
                ),
                GEOMETRY | BIT | VECTOR => hex(&raw),
                _ if c.charset == BINARY => hex(&raw),
                _ => Value::String(String::from_utf8_lossy(&raw).into_owned()),
            }
        }
    })
}

/// `YYYY-MM-DD[ HH:MM:SS[.ffffff]]`, from a binary DATE, DATETIME or TIMESTAMP (0, 4, 7 or 11 bytes).
fn datetime(raw: &[u8], date_only: bool, digits: u32) -> Option<String> {
    if !matches!(raw.len(), 0 | 4 | 7 | 11) {
        return None;
    }
    let mut b = raw;
    let (mut year, mut month, mut day, mut hour, mut minute, mut second, mut micros) =
        (0u16, 0, 0, 0, 0, 0, 0u32);
    if raw.len() >= 4 {
        year = b.get_u16_le();
        month = b.get_u8();
        day = b.get_u8();
    }
    if raw.len() >= 7 {
        hour = b.get_u8();
        minute = b.get_u8();
        second = b.get_u8();
    }
    if raw.len() >= 11 {
        micros = b.get_u32_le();
    }
    let date = format!("{year:04}-{month:02}-{day:02}");
    if date_only {
        return Some(date);
    }
    let units = u64::from(micros) / 10u64.pow(6 - digits);
    Some(format!(
        "{date} {hour:02}:{minute:02}:{second:02}{}",
        fraction(units, digits)
    ))
}

/// `[-]HH:MM:SS[.ffffff]`, hours counting whole days, from a binary TIME (0, 8 or 12 bytes).
fn time(raw: &[u8], digits: u32) -> Option<String> {
    if !matches!(raw.len(), 0 | 8 | 12) {
        return None;
    }
    if raw.is_empty() {
        return Some(format!("00:00:00{}", fraction(0, digits)));
    }
    let mut b = raw;
    let negative = b.get_u8() == 1;
    let days = u64::from(b.get_u32_le());
    let hours = days * 24 + u64::from(b.get_u8());
    let (minute, second) = (b.get_u8(), b.get_u8());
    let micros = if raw.len() == 12 { b.get_u32_le() } else { 0 };
    let units = u64::from(micros) / 10u64.pow(6 - digits);
    Some(format!(
        "{}{hours:02}:{minute:02}:{second:02}{}",
        if negative { "-" } else { "" },
        fraction(units, digits)
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::sql::Civil;

    fn col(name: &str, kind: u8, flags: u16, charset: u16, decimals: u8) -> Column {
        Column {
            name: name.to_owned(),
            kind,
            flags,
            charset,
            decimals,
        }
    }

    #[test]
    fn values_go_as_parameters_each_with_its_type() {
        let params = [
            Param::Null,
            Param::Bool(true),
            Param::Int(-2),
            Param::Decimal("19.90".into()),
            Param::Text("x'); DROP TABLE t; --".into()),
            Param::Time(Civil {
                year: 2026,
                month: 11,
                day: 27,
                hour: 9,
                minute: 30,
                second: 5,
                micros: 250_000,
            }),
        ];
        let p = execute_payload(7, &params);
        // Command, id, no cursor, one iteration.
        assert_eq!(&p[..10], &[0x17, 7, 0, 0, 0, 0, 1, 0, 0, 0]);
        // Only the first is null; the types follow.
        assert_eq!(p[10], 0b0000_0001);
        assert_eq!(p[11], 1);
        assert_eq!(
            &p[12..24],
            &[
                NULL, 0, TINY, 0, LONGLONG, 0, NEWDECIMAL, 0, VAR_STRING, 0, DATETIME, 0
            ]
        );
        let mut values = Bytes::copy_from_slice(&p[24..]);
        assert_eq!(values.get_u8(), 1);
        assert_eq!(values.get_i64_le(), -2);
        assert_eq!(&take_lenenc_bytes(&mut values).unwrap()[..], b"19.90");
        assert_eq!(
            &take_lenenc_bytes(&mut values).unwrap()[..],
            b"x'); DROP TABLE t; --"
        );
        assert_eq!(
            &values[..],
            &[11, 0xea, 0x07, 11, 27, 9, 30, 5, 0x90, 0xd0, 0x03, 0x00]
        );
        // The vendor's own example: one VARCHAR "foo" ... sent here as VAR_STRING.
        let q = execute_payload(1, &[Param::Text("foo".into())]);
        assert_eq!(
            q,
            [
                0x17, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, VAR_STRING, 0, 3, b'f', b'o', b'o'
            ]
        );
        assert_eq!(execute_payload(3, &[]).len(), 10);
    }

    #[test]
    fn binary_rows_read_as_their_types() {
        let columns = [
            col("id", LONGLONG, UNSIGNED, 63, 0),
            col("small", TINY, 0, 63, 0),
            col("price", NEWDECIMAL, 0, 63, 2),
            col("name", VAR_STRING, 0, 45, 0),
            col("raw", VAR_STRING, 0, BINARY, 0),
            col("at", DATETIME, 0, 63, 3),
            col("day", DATE, 0, 63, 0),
            col("took", TIME, 0, 63, 0),
            col("extra", JSON, 0, 63, 0),
            col("gone", VAR_STRING, 0, 45, 0),
            col("ratio", DOUBLE, 0, 63, 31),
            col("flags", BIT, 0, 63, 0),
        ];
        let mut b = vec![0u8];
        // Null bitmap, two bits in: column 9 ("gone") is null.
        let mut nulls = [0u8; 2];
        nulls[(9 + 2) / 8] |= 1 << ((9 + 2) % 8);
        b.extend_from_slice(&nulls);
        b.put_u64_le(u64::MAX);
        b.push(0xff);
        put_lenenc_bytes(&mut b, b"19.90");
        put_lenenc_bytes(&mut b, "Zoë".as_bytes());
        put_lenenc_bytes(&mut b, &[0xde, 0xad]);
        b.extend_from_slice(&[11, 0xea, 0x07, 11, 27, 9, 30, 5]);
        b.put_u32_le(123_000);
        b.extend_from_slice(&[4, 0xea, 0x07, 11, 27]);
        b.extend_from_slice(&[8, 1, 1, 0, 0, 0, 2, 3, 4]);
        put_lenenc_bytes(&mut b, br#"{"a": [1, 2]}"#);
        b.put_f64_le(0.5);
        put_lenenc_bytes(&mut b, &[0x01, 0x02]);
        let r = row(&columns, Bytes::from(b)).unwrap();
        assert_eq!(r["id"].to_string(), "18446744073709551615");
        assert_eq!(r["small"], -1);
        assert_eq!(r["price"].to_string(), "19.90");
        assert_eq!(r["name"], "Zoë");
        assert_eq!(r["raw"], "\\xdead");
        assert_eq!(r["at"], "2026-11-27 09:30:05.123");
        assert_eq!(r["day"], "2026-11-27");
        assert_eq!(r["took"], "-26:03:04");
        assert_eq!(r["extra"], serde_json::json!({ "a": [1, 2] }));
        assert_eq!(r["gone"], Value::Null);
        assert_eq!(r["ratio"], 0.5);
        assert_eq!(r["flags"], 258);
        // Cut short anywhere, a row is refused rather than misread.
        let mut whole = vec![0u8, 0];
        whole.put_u64_le(5);
        let one = [col("n", LONGLONG, 0, 63, 0)];
        for cut in 0..whole.len() {
            assert!(
                row(&one, Bytes::copy_from_slice(&whole[..cut])).is_none(),
                "cut at {cut}"
            );
        }
        assert_eq!(row(&one, Bytes::from(whole)).unwrap()["n"], 5);
    }

    #[test]
    fn times_and_dates_keep_their_digits() {
        assert_eq!(datetime(&[], false, 0).unwrap(), "0000-00-00 00:00:00");
        assert_eq!(
            datetime(&[0xea, 0x07, 1, 2, 3, 4, 5], false, 2).unwrap(),
            "2026-01-02 03:04:05.00"
        );
        assert!(datetime(&[1, 2, 3], false, 0).is_none());
        assert_eq!(time(&[], 0).unwrap(), "00:00:00");
        assert!(time(&[0; 5], 0).is_none());
    }
}
