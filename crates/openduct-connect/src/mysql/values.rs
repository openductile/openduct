// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Column values as the binary log stores them, each read with its length checked and written as JSON in its type:
//! numbers exact (decimals digit for digit), dates and times as MySQL writes them (TIMESTAMP in UTC), JSON as
//! JSON, text as text, and binary data as `\x` and hex. The same rendering is given to rows read with a query, so
//! a row reads the same in the first copy and in the stream.

use bytes::{Buf, Bytes};

use super::json;
use crate::json::{is_json_number, json_string};

pub(super) const TINY: u8 = 1;
pub(super) const SHORT: u8 = 2;
pub(super) const LONG: u8 = 3;
pub(super) const FLOAT: u8 = 4;
pub(super) const DOUBLE: u8 = 5;
pub(super) const TIMESTAMP: u8 = 7;
pub(super) const LONGLONG: u8 = 8;
pub(super) const INT24: u8 = 9;
pub(super) const DATE: u8 = 10;
pub(super) const TIME: u8 = 11;
pub(super) const DATETIME: u8 = 12;
pub(super) const YEAR: u8 = 13;
pub(super) const VARCHAR: u8 = 15;
pub(super) const BIT: u8 = 16;
pub(super) const TIMESTAMP2: u8 = 17;
pub(super) const DATETIME2: u8 = 18;
pub(super) const TIME2: u8 = 19;
pub(super) const JSON: u8 = 245;
pub(super) const NEWDECIMAL: u8 = 246;
pub(super) const ENUM: u8 = 247;
pub(super) const SET: u8 = 248;
pub(super) const BLOB: u8 = 252;
pub(super) const VAR_STRING: u8 = 253;
pub(super) const STRING: u8 = 254;
pub(super) const GEOMETRY: u8 = 255;
/// The character set of binary data.
pub(super) const BINARY: u16 = 63;

/// A column as a table map describes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) kind: u8,
    pub(super) meta: [u8; 2],
    pub(super) unsigned: bool,
    /// Binary data (its character set is `binary`), written as hex.
    pub(super) binary: bool,
    pub(super) key: bool,
    pub(super) enum_values: Vec<String>,
    pub(super) set_values: Vec<String>,
}

/// How many metadata bytes a column type has in a table map.
pub(super) fn meta_size(kind: u8) -> usize {
    match kind {
        FLOAT | DOUBLE | BLOB | GEOMETRY | JSON | TIMESTAMP2 | DATETIME2 | TIME2 => 1,
        VARCHAR | VAR_STRING | BIT | NEWDECIMAL | STRING | ENUM | SET => 2,
        _ => 0,
    }
}

fn short() -> String {
    "a value cut short".to_owned()
}

fn take(b: &mut Bytes, n: usize) -> Result<Bytes, String> {
    if b.remaining() < n {
        return Err(short());
    }
    Ok(b.split_to(n))
}

fn le(bytes: &[u8]) -> u64 {
    bytes.iter().rev().fold(0u64, |n, b| (n << 8) | u64::from(*b))
}

fn be(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0u64, |n, b| (n << 8) | u64::from(*b))
}

fn integer(b: &mut Bytes, size: usize, unsigned: bool) -> Result<String, String> {
    let raw = le(&take(b, size)?);
    if unsigned {
        return Ok(raw.to_string());
    }
    let shift = 64 - 8 * u32::try_from(size).unwrap_or(8);
    Ok((((raw << shift) as i64) >> shift).to_string())
}

fn float(value: f64) -> String {
    if value.is_finite() {
        let s = format!("{value}");
        if is_json_number(&s) { s } else { json_string(&s) }
    } else {
        json_string(&value.to_string())
    }
}

/// One value of `column` off the front of a row image, as JSON.
pub(super) fn binlog(column: &Column, b: &mut Bytes) -> Result<String, String> {
    let [m0, m1] = column.meta;
    match column.kind {
        TINY => integer(b, 1, column.unsigned),
        SHORT => integer(b, 2, column.unsigned),
        INT24 => integer(b, 3, column.unsigned),
        LONG => integer(b, 4, column.unsigned),
        LONGLONG => integer(b, 8, column.unsigned),
        FLOAT => {
            let raw: [u8; 4] = take(b, 4)?.as_ref().try_into().map_err(|_| short())?;
            Ok(float(f64::from(f32::from_le_bytes(raw))))
        }
        DOUBLE => Ok(float(f64::from_bits(le(&take(b, 8)?)))),
        YEAR => {
            let y = le(&take(b, 1)?);
            Ok(if y == 0 {
                "0".to_owned()
            } else {
                (1900 + y).to_string()
            })
        }
        NEWDECIMAL => {
            decimal(b, usize::from(m0), usize::from(m1)).map(|d| d.unwrap_or_else(|| "null".to_owned()))
        }
        DATE => {
            let v = le(&take(b, 3)?);
            Ok(json_string(&format!(
                "{:04}-{:02}-{:02}",
                v >> 9,
                (v >> 5) & 15,
                v & 31
            )))
        }
        TIME2 => time2(b, m0).map(|t| json_string(&t)),
        DATETIME2 => datetime2(b, m0).map(|t| json_string(&t)),
        TIMESTAMP2 => {
            let seconds = be(&take(b, 4)?);
            let micros = fraction(b, m0)?;
            Ok(json_string(&timestamp(seconds, micros, m0)))
        }
        VARCHAR | VAR_STRING => {
            let max = usize::from(u16::from_le_bytes([m0, m1]));
            let size = if max > 255 { 2 } else { 1 };
            let length = usize::try_from(le(&take(b, size)?)).map_err(|_| short())?;
            Ok(text(column, &take(b, length)?))
        }
        STRING | ENUM | SET => {
            let (kind, length) = if column.kind == STRING {
                if m0 & 0x30 != 0x30 {
                    (
                        m0 | 0x30,
                        usize::from(m1) | (usize::from((m0 & 0x30) ^ 0x30) << 4),
                    )
                } else {
                    (m0, usize::from(m1))
                }
            } else {
                (column.kind, usize::from(m1))
            };
            match kind {
                ENUM => {
                    let index = le(&take(b, length.clamp(1, 2))?);
                    let name = usize::try_from(index)
                        .ok()
                        .and_then(|i| i.checked_sub(1))
                        .and_then(|i| column.enum_values.get(i))
                        .cloned()
                        .unwrap_or_default();
                    Ok(json_string(&name))
                }
                SET => {
                    let bits = le(&take(b, length.clamp(1, 8))?);
                    let names: Vec<&str> = column
                        .set_values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i < 64 && bits >> i & 1 == 1)
                        .map(|(_, n)| n.as_str())
                        .collect();
                    Ok(json_string(&names.join(",")))
                }
                _ => {
                    let size = if length > 255 { 2 } else { 1 };
                    let n = usize::try_from(le(&take(b, size)?)).map_err(|_| short())?;
                    Ok(text(column, &take(b, n)?))
                }
            }
        }
        BLOB | GEOMETRY | JSON => {
            let size = usize::from(m0).clamp(1, 4);
            let n = usize::try_from(le(&take(b, size)?)).map_err(|_| short())?;
            let data = take(b, n)?;
            match column.kind {
                JSON => json::decode(&data),
                GEOMETRY => Ok(hex(&data)),
                _ => Ok(text(column, &data)),
            }
        }
        BIT => {
            let bytes = usize::from(m1) + usize::from(m0).div_ceil(8);
            if bytes > 8 {
                return Err("a BIT column wider than 64 bits".to_owned());
            }
            Ok(be(&take(b, bytes)?).to_string())
        }
        TIMESTAMP | DATETIME | TIME => Err(format!(
            "column {} is in the date and time format of MySQL before 5.6.4, which OpenDuct does not read (ALTER \
             TABLE ... FORCE writes it anew)",
            column.name
        )),
        other => Err(format!(
            "column {} has a type OpenDuct does not read ({other})",
            column.name
        )),
    }
}

fn text(column: &Column, bytes: &[u8]) -> String {
    if column.binary {
        hex(bytes)
    } else {
        json_string(&String::from_utf8_lossy(bytes))
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len() + 4);
    out.push_str("\"\\\\x");
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out.push('"');
    out
}

/// The fraction of a second after a TIME2, DATETIME2 or TIMESTAMP2, in microseconds.
fn fraction(b: &mut Bytes, fsp: u8) -> Result<u64, String> {
    let size = usize::from(fsp).div_ceil(2);
    if size == 0 {
        return Ok(0);
    }
    let raw = be(&take(b, size)?);
    Ok(match size {
        1 => raw * 10_000,
        2 => raw * 100,
        _ => raw,
    })
}

fn with_fraction(base: String, micros: u64, fsp: u8) -> String {
    if fsp == 0 {
        return base;
    }
    let digits = format!("{micros:06}");
    format!(
        "{base}.{}",
        digits.get(..usize::from(fsp.min(6))).unwrap_or(&digits)
    )
}

fn datetime2(b: &mut Bytes, fsp: u8) -> Result<String, String> {
    let packed = be(&take(b, 5)?).wrapping_sub(0x80_0000_0000);
    let micros = fraction(b, fsp)?;
    Ok(with_fraction(datetime_parts(packed), micros, fsp))
}

/// `YYYY-MM-DD HH:MM:SS` from DATETIME2's packed integer part.
fn datetime_parts(packed: u64) -> String {
    let ymd = packed >> 17;
    let ym = ymd >> 5;
    let hms = packed & 0x1_ffff;
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        ym / 13,
        ym % 13,
        ymd & 31,
        hms >> 12,
        (hms >> 6) & 63,
        hms & 63
    )
}

/// TIME2: a signed hours-minutes-seconds and a fraction, as MySQL packs them; a negative time with a fraction
/// counts its fraction back from the whole second above.
fn time2(b: &mut Bytes, fsp: u8) -> Result<String, String> {
    let mut whole = i64::try_from(be(&take(b, 3)?)).unwrap_or(0) - 0x80_0000;
    let size = usize::from(fsp).div_ceil(2);
    let (mut frac, unit, base) = match size {
        0 => (0, 0, 0),
        n => {
            let raw = i64::try_from(be(&take(b, n)?)).unwrap_or(0);
            match n {
                1 => (raw, 10_000, 0x100),
                2 => (raw, 100, 0x1_0000),
                _ => (raw, 1, 0x100_0000),
            }
        }
    };
    if whole < 0 && frac != 0 {
        whole += 1;
        frac -= base;
    }
    let mut packed = (whole << 24) + frac * unit;
    let negative = packed < 0;
    packed = packed.abs();
    let (hms, micros) = (packed >> 24, packed & 0xff_ffff);
    let base = format!(
        "{}{:02}:{:02}:{:02}",
        if negative { "-" } else { "" },
        (hms >> 12) & 0x3ff,
        (hms >> 6) & 63,
        hms & 63
    );
    Ok(with_fraction(base, u64::try_from(micros).unwrap_or(0), fsp))
}

/// Seconds since 1970, in UTC, as MySQL writes a TIMESTAMP in a session at `+00:00`.
fn timestamp(seconds: u64, micros: u64, fsp: u8) -> String {
    let base = chrono::DateTime::from_timestamp(i64::try_from(seconds).unwrap_or(0), 0)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default();
    with_fraction(base, micros, fsp)
}

/// A DECIMAL(precision, scale) in MySQL's binary form, as its exact digits; `None` for a form that cannot be.
pub(super) fn decimal(b: &mut Bytes, precision: usize, scale: usize) -> Result<Option<String>, String> {
    const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let dig2 = |digits: usize| DIG2BYTES.get(digits).copied().unwrap_or(4);
    if precision == 0 || precision > 65 || scale > precision {
        return Err("a DECIMAL of a size MySQL does not have".to_owned());
    }
    let integral = precision - scale;
    let (int_words, int_rest) = (integral / 9, integral % 9);
    let (frac_words, frac_rest) = (scale / 9, scale % 9);
    let size = int_words * 4 + dig2(int_rest) + frac_words * 4 + dig2(frac_rest);
    let mut raw = take(b, size)?.to_vec();
    let Some(first) = raw.first_mut() else {
        return Ok(None);
    };
    let negative = *first & 0x80 == 0;
    *first ^= 0x80;
    if negative {
        for byte in &mut raw {
            *byte = !*byte;
        }
    }
    let mut at = 0;
    let mut read = |n: usize| {
        let v = be(raw.get(at..at + n).unwrap_or_default());
        at += n;
        v
    };
    let mut digits = String::new();
    if int_rest > 0 {
        digits.push_str(&read(dig2(int_rest)).to_string());
    }
    for _ in 0..int_words {
        let word = read(4);
        if digits.is_empty() {
            digits.push_str(&word.to_string());
        } else {
            digits.push_str(&format!("{word:09}"));
        }
    }
    let digits = digits.trim_start_matches('0');
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(if digits.is_empty() { "0" } else { digits });
    if scale > 0 {
        out.push('.');
        for _ in 0..frac_words {
            out.push_str(&format!("{:09}", read(4)));
        }
        if frac_rest > 0 {
            out.push_str(&format!("{:0width$}", read(dig2(frac_rest)), width = frac_rest));
        }
    }
    if negative && out.bytes().all(|c| matches!(c, b'-' | b'0' | b'.')) {
        out.remove(0);
    }
    Ok(Some(out))
}

/// A value read with a query (the server's text for it), written as the stream writes the same column.
pub(super) fn text_value(kind: u8, charset: u16, value: &[u8]) -> String {
    let s = String::from_utf8_lossy(value);
    match kind {
        TINY | SHORT | INT24 | LONG | LONGLONG | YEAR | FLOAT | DOUBLE | NEWDECIMAL if is_json_number(&s) => {
            s.into_owned()
        }
        JSON => {
            if openduct_expr::Value::parse_json(&s).is_ok() {
                s.trim().to_owned()
            } else {
                json_string(&s)
            }
        }
        BIT if value.len() <= 8 => be(value).to_string(),
        GEOMETRY => hex(value),
        VARCHAR | VAR_STRING | STRING | BLOB if charset == BINARY => hex(value),
        _ => json_string(&s),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn column(kind: u8, meta: [u8; 2]) -> Column {
        Column {
            name: "c".to_owned(),
            kind,
            meta,
            ..Column::default()
        }
    }

    fn read(c: &Column, bytes: &[u8]) -> String {
        binlog(c, &mut Bytes::copy_from_slice(bytes)).unwrap()
    }

    #[test]
    fn numbers_are_exact_and_signed_as_declared() {
        assert_eq!(read(&column(TINY, [0, 0]), b"\xff"), "-1");
        let unsigned = Column {
            unsigned: true,
            ..column(TINY, [0, 0])
        };
        assert_eq!(read(&unsigned, b"\xff"), "255");
        assert_eq!(read(&column(INT24, [0, 0]), b"\xff\xff\x7f"), "8388607");
        assert_eq!(read(&column(INT24, [0, 0]), b"\x00\x00\x80"), "-8388608");
        assert_eq!(
            read(&column(LONGLONG, [0, 0]), b"\x2a\x00\x00\x00\x00\x00\x00\x00"),
            "42"
        );
        assert_eq!(read(&column(DOUBLE, [8, 0]), &1.5f64.to_le_bytes()), "1.5");
    }

    #[test]
    fn decimals_are_read_digit_for_digit() {
        // DECIMAL(10,2): 1234.56, -1234.56, 0.00; DECIMAL(20,10) 1234567890.1234567890, from MySQL's binary form.
        let mut b = Bytes::from_static(&[0x80, 0x00, 0x04, 0xd2, 0x38]);
        assert_eq!(decimal(&mut b, 10, 2).unwrap().unwrap(), "1234.56");
        let mut b = Bytes::from_static(&[0x7f, 0xff, 0xfb, 0x2d, 0xc7]);
        assert_eq!(decimal(&mut b, 10, 2).unwrap().unwrap(), "-1234.56");
        let mut b = Bytes::from_static(&[0x80, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(decimal(&mut b, 10, 2).unwrap().unwrap(), "0.00");
        let mut b = Bytes::from_static(&[0x81, 0x0d, 0xfb, 0x38, 0xd2, 0x07, 0x5b, 0xcd, 0x15, 0x00]);
        assert_eq!(decimal(&mut b, 20, 10).unwrap().unwrap(), "1234567890.1234567890");
        assert!(decimal(&mut Bytes::from_static(&[0x80]), 10, 2).is_err());
        assert!(decimal(&mut Bytes::new(), 70, 2).is_err());
    }

    #[test]
    fn dates_and_times_read_as_mysql_writes_them() {
        // 2026-11-15 = (2026 << 9) | (11 << 5) | 15.
        let date = (2026u32 << 9) | (11 << 5) | 15;
        let d = date.to_le_bytes();
        assert_eq!(
            binlog(&column(DATE, [0, 0]), &mut Bytes::copy_from_slice(&d[..3])).unwrap(),
            "\"2026-11-15\""
        );
        // DATETIME2(3) 2026-11-15 09:52:31.977.
        let ymd = ((2026u64 * 13 + 11) << 5) | 15;
        let packed = ((ymd << 17) | (9 << 12) | (52 << 6) | 31) + 0x80_0000_0000;
        let mut bytes = packed.to_be_bytes()[3..].to_vec();
        bytes.extend_from_slice(&9770u16.to_be_bytes());
        assert_eq!(
            binlog(&column(DATETIME2, [3, 0]), &mut Bytes::from(bytes)).unwrap(),
            "\"2026-11-15 09:52:31.977\""
        );
        // TIMESTAMP2(0) at 1_763_200_000 seconds.
        assert_eq!(
            binlog(
                &column(TIMESTAMP2, [0, 0]),
                &mut Bytes::copy_from_slice(&1_763_200_000u32.to_be_bytes())
            )
            .unwrap(),
            "\"2025-11-15 09:46:40\""
        );
        // TIME2(0) 12:34:56 and -00:00:01.
        let t = ((12u64 << 12) | (34 << 6) | 56) + 0x80_0000;
        assert_eq!(
            binlog(
                &column(TIME2, [0, 0]),
                &mut Bytes::copy_from_slice(&t.to_be_bytes()[5..])
            )
            .unwrap(),
            "\"12:34:56\""
        );
        let t = 0x80_0000u64 - 1;
        assert_eq!(
            binlog(
                &column(TIME2, [0, 0]),
                &mut Bytes::copy_from_slice(&t.to_be_bytes()[5..])
            )
            .unwrap(),
            "\"-00:00:01\""
        );
    }

    #[test]
    fn text_enums_sets_and_binary() {
        let varchar = column(VARCHAR, [0x2c, 0x01]);
        assert_eq!(read(&varchar, b"\x03\x00O'B"), "\"O'B\"");
        let binary = Column {
            binary: true,
            ..column(BLOB, [2, 0])
        };
        assert_eq!(read(&binary, b"\x02\x00\x01\xff"), "\"\\\\x01ff\"");
        let e = Column {
            enum_values: vec!["new".into(), "paid".into()],
            ..column(STRING, [ENUM, 1])
        };
        assert_eq!(read(&e, b"\x02"), "\"paid\"");
        let s = Column {
            set_values: vec!["a".into(), "b".into(), "c".into()],
            ..column(STRING, [SET, 1])
        };
        assert_eq!(read(&s, b"\x05"), "\"a,c\"");
        // A value longer than the row: refused.
        assert!(binlog(&varchar, &mut Bytes::from_static(b"\x09\x00ab")).is_err());
        assert_eq!(text_value(NEWDECIMAL, 33, b"12.50"), "12.50");
        assert_eq!(text_value(BLOB, BINARY, b"\x01\x02"), "\"\\\\x0102\"");
        assert_eq!(text_value(BIT, BINARY, b"\x01\x00"), "256");
    }
}
