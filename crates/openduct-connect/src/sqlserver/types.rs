// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SQL Server's types as TDS describes them (TYPE_INFO) and its values, each read as JSON in its type: numbers
//! exact (decimal and money digit for digit), dates and times in ISO 8601 as SQL Server reads them back, text from
//! UTF-16 (nvarchar) or its collation's code page (varchar: Windows-1252 or UTF-8), binary data as `\x` and hex; and
//! the values a statement is given, each sent as a parameter of its own type.

use bytes::{Buf, BufMut, Bytes};
use serde_json::Value;

use super::wire::{take, ucs2};
use crate::sql::{Param, date_of, days_of, fraction, hex, number};

const NULLTYPE: u8 = 0x1f;
const INT1: u8 = 0x30;
const BIT: u8 = 0x32;
const INT2: u8 = 0x34;
const INT4: u8 = 0x38;
const DATETIM4: u8 = 0x3a;
const FLT4: u8 = 0x3b;
const MONEY: u8 = 0x3c;
const DATETIME: u8 = 0x3d;
const FLT8: u8 = 0x3e;
const MONEY4: u8 = 0x7a;
const INT8: u8 = 0x7f;
const GUID: u8 = 0x24;
const INTN: u8 = 0x26;
const DECIMAL: u8 = 0x37;
const NUMERIC: u8 = 0x3f;
const BITN: u8 = 0x68;
const DECIMALN: u8 = 0x6a;
const NUMERICN: u8 = 0x6c;
const FLTN: u8 = 0x6d;
const MONEYN: u8 = 0x6e;
const DATETIMN: u8 = 0x6f;
const DATEN: u8 = 0x28;
const TIMEN: u8 = 0x29;
const DATETIME2N: u8 = 0x2a;
const DATETIMEOFFSETN: u8 = 0x2b;
const BIGVARBIN: u8 = 0xa5;
const BIGVARCHR: u8 = 0xa7;
const BIGBINARY: u8 = 0xad;
const BIGCHAR: u8 = 0xaf;
const NVARCHAR: u8 = 0xe7;
const NCHAR: u8 = 0xef;
const TEXT: u8 = 0x23;
const NTEXT: u8 = 0x63;
const IMAGE: u8 = 0x22;
const XML: u8 = 0xf1;
const UDT: u8 = 0xf0;
const VARIANT: u8 = 0x62;
/// A ushort length that means "max": the value comes in chunks (PLP).
const MAX: u16 = 0xffff;
/// Days from 0001-01-01 and from 1900-01-01 to 1970-01-01.
const FROM_YEAR_1: i64 = 719_162;
const FROM_1900: i64 = 25_567;

/// How text in a column is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Text {
    Utf16,
    Utf8,
    Windows1252,
    /// Another code page: bytes past ASCII are not read.
    Other,
}

/// A column's type, as COLMETADATA describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Kind {
    /// A type of a fixed size: its code and size.
    Fixed(u8, usize),
    /// A type whose value comes with a one-byte length: int, bit, float, money, datetime, uniqueidentifier.
    Sized(u8),
    Decimal {
        scale: u8,
    },
    Date,
    Time {
        scale: u8,
    },
    DateTime2 {
        scale: u8,
    },
    DateTimeOffset {
        scale: u8,
    },
    /// Text or bytes with a two-byte length, or in chunks (`max`).
    Short {
        text: Option<Text>,
        max: bool,
    },
    /// text, ntext, image.
    Long {
        text: Option<Text>,
    },
    Xml,
    Udt,
    Variant,
}

fn u8_of(b: &mut Bytes) -> Option<u8> {
    b.has_remaining().then(|| b.get_u8())
}

fn u16_of(b: &mut Bytes) -> Option<u16> {
    (b.remaining() >= 2).then(|| b.get_u16_le())
}

fn u32_of(b: &mut Bytes) -> Option<u32> {
    (b.remaining() >= 4).then(|| b.get_u32_le())
}

/// A B_VARCHAR (a one-byte count of UTF-16 units) or US_VARCHAR (two bytes).
pub(super) fn varchar(b: &mut Bytes, wide: bool) -> Option<String> {
    let count = if wide {
        usize::from(u16_of(b)?)
    } else {
        usize::from(u8_of(b)?)
    };
    Some(utf16(&take(b, count * 2)?))
}

pub(super) fn utf16(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u16::from_le_bytes(*p))
        .collect();
    String::from_utf16_lossy(&units)
}

/// How a collation (five bytes) writes varchar.
pub(super) fn text_of(collation: &[u8]) -> Text {
    let mut raw = [0u8; 4];
    for (to, from) in raw.iter_mut().zip(collation) {
        *to = *from;
    }
    let info = u32::from_le_bytes(raw);
    if (info >> 26) & 1 == 1 {
        return Text::Utf8;
    }
    let sort = collation.get(4).copied().unwrap_or(0);
    if sort != 0 {
        // The SQL collations of code page 1252 (SQL_Latin1_General_CP1_...): sort orders 51 to 61, 71 and 72.
        return if (51..=61).contains(&sort) || (71..=72).contains(&sort) {
            Text::Windows1252
        } else {
            Text::Other
        };
    }
    // Windows collations: the languages written in code page 1252.
    const LATIN: &[u32] = &[
        0x03, 0x06, 0x07, 0x09, 0x0a, 0x0b, 0x0c, 0x0f, 0x10, 0x13, 0x14, 0x16, 0x1d, 0x21, 0x2d, 0x36, 0x38,
        0x3e, 0x41, 0x56,
    ];
    if LATIN.contains(&(info & 0x3ff)) || info & 0xf_ffff == 0 {
        Text::Windows1252
    } else {
        Text::Other
    }
}

/// Windows-1252's characters 0x80 to 0x9f (the rest is Latin-1).
const CP1252: [u16; 32] = [
    0x20ac, 0x81, 0x201a, 0x192, 0x201e, 0x2026, 0x2020, 0x2021, 0x2c6, 0x2030, 0x160, 0x2039, 0x152, 0x8d,
    0x17d, 0x8f, 0x90, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014, 0x2dc, 0x2122, 0x161, 0x203a,
    0x153, 0x9d, 0x17e, 0x178,
];

fn decode(bytes: &[u8], text: Text) -> String {
    match text {
        Text::Utf16 => utf16(bytes),
        Text::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
        Text::Windows1252 => bytes
            .iter()
            .map(|&b| match b {
                0x80..=0x9f => char::from_u32(u32::from(
                    CP1252.get(usize::from(b - 0x80)).copied().unwrap_or(0xfffd),
                ))
                .unwrap_or('\u{fffd}'),
                _ => char::from(b),
            })
            .collect(),
        Text::Other => bytes
            .iter()
            .map(|&b| if b < 0x80 { char::from(b) } else { '\u{fffd}' })
            .collect(),
    }
}

/// Reads a TYPE_INFO.
pub(super) fn type_info(b: &mut Bytes) -> Option<Kind> {
    let code = u8_of(b)?;
    Some(match code {
        NULLTYPE => Kind::Fixed(code, 0),
        INT1 | BIT => Kind::Fixed(code, 1),
        INT2 => Kind::Fixed(code, 2),
        INT4 | DATETIM4 | FLT4 | MONEY4 => Kind::Fixed(code, 4),
        MONEY | DATETIME | FLT8 | INT8 => Kind::Fixed(code, 8),
        GUID | INTN | BITN | FLTN | MONEYN | DATETIMN => {
            u8_of(b)?;
            Kind::Sized(code)
        }
        DECIMAL | NUMERIC | DECIMALN | NUMERICN => {
            take(b, 2)?;
            Kind::Decimal { scale: u8_of(b)? }
        }
        DATEN => Kind::Date,
        TIMEN => Kind::Time { scale: u8_of(b)? },
        DATETIME2N => Kind::DateTime2 { scale: u8_of(b)? },
        DATETIMEOFFSETN => Kind::DateTimeOffset { scale: u8_of(b)? },
        BIGVARBIN | BIGBINARY => Kind::Short {
            text: None,
            max: u16_of(b)? == MAX,
        },
        BIGVARCHR | BIGCHAR | NVARCHAR | NCHAR => {
            let max = u16_of(b)? == MAX;
            let collation = take(b, 5)?;
            let text = if matches!(code, NVARCHAR | NCHAR) {
                Text::Utf16
            } else {
                text_of(&collation)
            };
            Kind::Short {
                text: Some(text),
                max,
            }
        }
        TEXT | NTEXT | IMAGE => {
            u32_of(b)?;
            let text = match code {
                IMAGE => None,
                NTEXT => {
                    take(b, 5)?;
                    Some(Text::Utf16)
                }
                _ => Some(text_of(&take(b, 5)?)),
            };
            // The table's name, in parts.
            for _ in 0..u8_of(b)? {
                varchar(b, true)?;
            }
            Kind::Long { text }
        }
        XML => {
            if u8_of(b)? == 1 {
                varchar(b, false)?;
                varchar(b, false)?;
                varchar(b, true)?;
            }
            Kind::Xml
        }
        UDT => {
            u16_of(b)?;
            varchar(b, false)?;
            varchar(b, false)?;
            varchar(b, false)?;
            varchar(b, true)?;
            Kind::Udt
        }
        VARIANT => {
            u32_of(b)?;
            Kind::Variant
        }
        _ => return None,
    })
}

/// A value in chunks (PLP): its bytes, or none for null.
fn chunks(b: &mut Bytes) -> Option<Option<Vec<u8>>> {
    if b.remaining() < 8 {
        return None;
    }
    let total = b.get_u64_le();
    if total == u64::MAX {
        return Some(None);
    }
    let mut out = Vec::with_capacity(usize::try_from(total).unwrap_or(0).min(b.remaining()));
    loop {
        let n = usize::try_from(u32_of(b)?).ok()?;
        if n == 0 {
            return Some(Some(out));
        }
        out.extend_from_slice(&take(b, n)?);
    }
}

/// Reads a value of `kind`; none when the bytes are cut short or not understood.
pub(super) fn value(kind: &Kind, b: &mut Bytes) -> Option<Value> {
    Some(match kind {
        Kind::Fixed(code, size) => fixed(*code, &take(b, *size)?)?,
        Kind::Sized(code) => {
            let n = usize::from(u8_of(b)?);
            if n == 0 {
                return Some(Value::Null);
            }
            let raw = take(b, n)?;
            let as_fixed = match (*code, n) {
                (GUID, 16) => return Some(Value::String(guid(&raw))),
                (INTN, 1) => INT1,
                (INTN, 2) => INT2,
                (INTN, 4) => INT4,
                (INTN, 8) => INT8,
                (BITN, 1) => BIT,
                (FLTN, 4) => FLT4,
                (FLTN, 8) => FLT8,
                (MONEYN, 4) => MONEY4,
                (MONEYN, 8) => MONEY,
                (DATETIMN, 4) => DATETIM4,
                (DATETIMN, 8) => DATETIME,
                _ => return None,
            };
            fixed(as_fixed, &raw)?
        }
        Kind::Decimal { scale } => {
            let n = usize::from(u8_of(b)?);
            if n == 0 {
                return Some(Value::Null);
            }
            decimal(&take(b, n)?, *scale)?
        }
        Kind::Date => match u8_of(b)? {
            0 => Value::Null,
            3 => Value::String(date(&take(b, 3)?)),
            _ => return None,
        },
        Kind::Time { scale } => match u8_of(b)? {
            0 => Value::Null,
            n => Value::String(time(&take(b, usize::from(n))?, *scale)?),
        },
        Kind::DateTime2 { scale } | Kind::DateTimeOffset { scale } => {
            let n = usize::from(u8_of(b)?);
            if n == 0 {
                return Some(Value::Null);
            }
            let raw = take(b, n)?;
            let offset = matches!(kind, Kind::DateTimeOffset { .. });
            Value::String(datetime2(&raw, *scale, offset)?)
        }
        Kind::Short { text, max: true } => match chunks(b)? {
            None => Value::Null,
            Some(raw) => bytes_or_text(&raw, *text),
        },
        Kind::Short { text, max: false } => {
            let n = u16_of(b)?;
            if n == MAX {
                return Some(Value::Null);
            }
            bytes_or_text(&take(b, usize::from(n))?, *text)
        }
        Kind::Long { text } => {
            let pointer = usize::from(u8_of(b)?);
            if pointer == 0 {
                return Some(Value::Null);
            }
            take(b, pointer + 8)?;
            let n = usize::try_from(u32_of(b)?).ok()?;
            bytes_or_text(&take(b, n)?, *text)
        }
        Kind::Xml => match chunks(b)? {
            None => Value::Null,
            Some(raw) => Value::String(utf16(&raw)),
        },
        Kind::Udt => match chunks(b)? {
            None => Value::Null,
            Some(raw) => hex(&raw),
        },
        Kind::Variant => variant(b)?,
    })
}

fn bytes_or_text(raw: &[u8], text: Option<Text>) -> Value {
    match text {
        Some(t) => Value::String(decode(raw, t)),
        None => hex(raw),
    }
}

fn le(raw: &[u8]) -> u128 {
    raw.iter().rev().fold(0u128, |n, b| n << 8 | u128::from(*b))
}

fn fixed(code: u8, raw: &[u8]) -> Option<Value> {
    let size = match code {
        NULLTYPE => 0,
        INT1 | BIT => 1,
        INT2 => 2,
        INT4 | FLT4 | MONEY4 | DATETIM4 => 4,
        _ => 8,
    };
    if raw.len() != size {
        return None;
    }
    let mut b = raw;
    Some(match code {
        NULLTYPE => Value::Null,
        INT1 => Value::from(b.get_u8()),
        BIT => Value::Bool(b.get_u8() != 0),
        INT2 => Value::from(b.get_i16_le()),
        INT4 => Value::from(b.get_i32_le()),
        INT8 => Value::from(b.get_i64_le()),
        FLT4 => {
            let f = b.get_f32_le();
            if f.is_finite() {
                number(&f.to_string())
            } else {
                Value::Null
            }
        }
        FLT8 => serde_json::Number::from_f64(b.get_f64_le()).map_or(Value::Null, Value::Number),
        MONEY => {
            let high = i64::from(b.get_i32_le());
            let low = i64::from(b.get_u32_le());
            number(&scaled(i128::from(high << 32 | low), 4))
        }
        MONEY4 => number(&scaled(i128::from(b.get_i32_le()), 4)),
        DATETIME => {
            let days = i64::from(b.get_i32_le());
            let ticks = u64::from(b.get_u32_le());
            // 1/300 of a second, as SQL Server rounds it to milliseconds.
            let ms = (ticks * 10 + 1) / 3;
            let (y, m, d) = date_of(days - FROM_1900);
            Value::String(format!(
                "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}",
                ms / 3_600_000,
                ms / 60_000 % 60,
                ms / 1000 % 60,
                ms % 1000
            ))
        }
        DATETIM4 => {
            let days = i64::from(b.get_u16_le());
            let minutes = b.get_u16_le();
            let (y, m, d) = date_of(days - FROM_1900);
            Value::String(format!(
                "{y:04}-{m:02}-{d:02}T{:02}:{:02}:00",
                minutes / 60,
                minutes % 60
            ))
        }
        _ => return None,
    })
}

/// An integer's digits with `scale` of them after the point.
fn scaled(n: i128, scale: u32) -> String {
    let digits = n.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, part) = padded.split_at(padded.len() - scale);
    let sign = if n < 0 { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{part}")
    }
}

fn decimal(raw: &[u8], scale: u8) -> Option<Value> {
    let (sign, magnitude) = raw.split_first()?;
    if magnitude.len() > 16 {
        return None;
    }
    let n = i128::try_from(le(magnitude)).ok()?;
    Some(number(&scaled(if *sign == 0 { -n } else { n }, u32::from(scale))))
}

fn guid(raw: &[u8]) -> String {
    let mut out = String::with_capacity(36);
    for (i, at) in [3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15]
        .iter()
        .enumerate()
    {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{:02X}", raw.get(*at).copied().unwrap_or(0)));
    }
    out
}

fn date(raw: &[u8]) -> String {
    let (y, m, d) = date_of(i64::try_from(le(raw)).unwrap_or(0) - FROM_YEAR_1);
    format!("{y:04}-{m:02}-{d:02}")
}

/// A time of day: units of 10^-scale seconds since midnight.
fn time(raw: &[u8], scale: u8) -> Option<String> {
    if raw.len() > 5 || scale > 7 {
        return None;
    }
    let units = u64::try_from(le(raw)).ok()?;
    let per_second = 10u64.pow(u32::from(scale));
    let seconds = units / per_second;
    Some(format!(
        "{:02}:{:02}:{:02}{}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        fraction(units % per_second, u32::from(scale))
    ))
}

/// The size of a time of `scale`.
fn time_size(scale: u8) -> usize {
    match scale {
        0..=2 => 3,
        3..=4 => 4,
        _ => 5,
    }
}

/// A datetime2 (time, then date), or a datetimeoffset (in UTC, then the offset in minutes): written in its own time.
fn datetime2(raw: &[u8], scale: u8, offset: bool) -> Option<String> {
    let t = time_size(scale);
    let units = u64::try_from(le(raw.get(..t)?)).ok()?;
    let days = i64::try_from(le(raw.get(t..t + 3)?)).ok()? - FROM_YEAR_1;
    let minutes = if offset {
        i16::from_le_bytes(raw.get(t + 3..t + 5)?.try_into().ok()?)
    } else {
        0
    };
    if raw.len() != t + 3 + if offset { 2 } else { 0 } {
        return None;
    }
    let per_second = 10i64.pow(u32::from(scale));
    let mut seconds = i64::try_from(units).ok()? / per_second + i64::from(minutes) * 60;
    let part = i64::try_from(units).ok()? % per_second;
    let day = days + seconds.div_euclid(86_400);
    seconds = seconds.rem_euclid(86_400);
    let (y, m, d) = date_of(day);
    let mut out = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}{}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        fraction(u64::try_from(part).unwrap_or(0), u32::from(scale))
    );
    if offset {
        let sign = if minutes < 0 { '-' } else { '+' };
        let m = minutes.unsigned_abs();
        out.push_str(&format!("{sign}{:02}:{:02}", m / 60, m % 60));
    }
    Some(out)
}

/// A sql_variant: its own type, then its value.
fn variant(b: &mut Bytes) -> Option<Value> {
    let n = usize::try_from(u32_of(b)?).ok()?;
    if n == 0 {
        return Some(Value::Null);
    }
    let mut v = take(b, n)?;
    let code = u8_of(&mut v)?;
    let props = usize::from(u8_of(&mut v)?);
    let mut p = take(&mut v, props)?;
    let raw = v;
    Some(match code {
        INT1 | BIT | INT2 | INT4 | INT8 | FLT4 | FLT8 | MONEY | MONEY4 | DATETIME | DATETIM4 => {
            fixed(code, &raw)?
        }
        GUID => Value::String(guid(&raw)),
        DECIMALN | NUMERICN => {
            u8_of(&mut p)?;
            decimal(&raw, u8_of(&mut p)?)?
        }
        DATEN => Value::String(date(&raw)),
        TIMEN => Value::String(time(&raw, u8_of(&mut p)?)?),
        DATETIME2N => Value::String(datetime2(&raw, u8_of(&mut p)?, false)?),
        DATETIMEOFFSETN => Value::String(datetime2(&raw, u8_of(&mut p)?, true)?),
        NVARCHAR | NCHAR => Value::String(utf16(&raw)),
        BIGVARCHR | BIGCHAR => Value::String(decode(&raw, text_of(&take(&mut p, 5)?))),
        _ => hex(&raw),
    })
}

/// A parameter's declaration (for sp_executesql) and its TYPE_INFO and value, as RPC sends them.
pub(super) fn param(p: &Param, collation: [u8; 5]) -> (String, Vec<u8>) {
    let mut out = Vec::new();
    let nvarchar = |out: &mut Vec<u8>, text: Option<&str>| -> String {
        let bytes = text.map(ucs2);
        match bytes {
            Some(b) if b.len() > 8000 => {
                out.push(NVARCHAR);
                out.put_u16_le(MAX);
                out.extend_from_slice(&collation);
                out.put_u64_le(b.len() as u64);
                out.put_u32_le(u32::try_from(b.len()).unwrap_or(u32::MAX));
                out.extend_from_slice(&b);
                out.put_u32_le(0);
                "nvarchar(max)".to_owned()
            }
            _ => {
                out.push(NVARCHAR);
                out.put_u16_le(8000);
                out.extend_from_slice(&collation);
                match bytes {
                    Some(b) => {
                        out.put_u16_le(u16::try_from(b.len()).unwrap_or(0));
                        out.extend_from_slice(&b);
                    }
                    None => out.put_u16_le(MAX),
                }
                "nvarchar(4000)".to_owned()
            }
        }
    };
    let declared = match p {
        Param::Null => nvarchar(&mut out, None),
        Param::Bool(v) => {
            out.extend_from_slice(&[BITN, 1, 1, u8::from(*v)]);
            "bit".to_owned()
        }
        Param::Int(n) => {
            out.extend_from_slice(&[INTN, 8, 8]);
            out.put_i64_le(*n);
            "bigint".to_owned()
        }
        Param::Decimal(text) => match decimal_parts(text) {
            Some((negative, magnitude, scale)) => {
                out.extend_from_slice(&[DECIMALN, 17, 38, scale, 17, u8::from(!negative)]);
                out.extend_from_slice(&magnitude.to_le_bytes());
                format!("decimal(38,{scale})")
            }
            None => nvarchar(&mut out, Some(text)),
        },
        Param::Text(text) => nvarchar(&mut out, Some(text)),
        Param::Time(t) => {
            // datetime2(7): 100 ns units since midnight (5 bytes), then days since 0001-01-01 (3 bytes).
            out.extend_from_slice(&[DATETIME2N, 7, 8]);
            let units = ((u64::from(t.hour) * 60 + u64::from(t.minute)) * 60 + u64::from(t.second))
                * 10_000_000
                + u64::from(t.micros) * 10;
            out.extend_from_slice(units.to_le_bytes().get(..5).unwrap_or_default());
            let days = days_of(i64::from(t.year), t.month, t.day) + FROM_YEAR_1;
            out.extend_from_slice(
                u32::try_from(days)
                    .unwrap_or(0)
                    .to_le_bytes()
                    .get(..3)
                    .unwrap_or_default(),
            );
            "datetime2(7)".to_owned()
        }
    };
    (declared, out)
}

/// A decimal's sign, digits and scale, when it fits decimal(38, scale).
fn decimal_parts(text: &str) -> Option<(bool, u128, u8)> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, part) = digits.split_once('.').unwrap_or((digits, ""));
    if (whole.is_empty() && part.is_empty())
        || !whole.bytes().chain(part.bytes()).all(|b| b.is_ascii_digit())
        || whole.trim_start_matches('0').len() + part.len() > 38
    {
        return None;
    }
    let all = format!("{whole}{part}");
    let magnitude = all.trim_start_matches('0').parse::<u128>().unwrap_or(0);
    Some((negative, magnitude, u8::try_from(part.len()).ok()?))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::sql::Civil;

    fn read(info: &[u8], value_bytes: &[u8]) -> Value {
        let mut b = Bytes::copy_from_slice(info);
        let kind = type_info(&mut b).unwrap();
        assert!(b.is_empty(), "the whole TYPE_INFO is read");
        let mut v = Bytes::copy_from_slice(value_bytes);
        let out = value(&kind, &mut v).unwrap();
        assert!(v.is_empty(), "the whole value is read");
        out
    }

    #[test]
    fn numbers_read_exactly() {
        assert_eq!(read(&[INTN, 4], &[4, 0xff, 0xff, 0xff, 0xff]), -1);
        assert_eq!(read(&[INTN, 1], &[1, 0xff]), 255, "tinyint is unsigned");
        assert_eq!(read(&[INTN, 8], &[0]), Value::Null);
        assert_eq!(read(&[INT8], &u64::MAX.to_le_bytes()[..]), -1);
        assert_eq!(read(&[BITN, 1], &[1, 1]), true);
        // decimal(10,2) 19.90 = 1990, positive.
        assert_eq!(
            read(&[DECIMALN, 17, 10, 2], &[5, 1, 0xc6, 0x07, 0, 0]).to_string(),
            "19.90"
        );
        assert_eq!(
            read(&[DECIMALN, 17, 10, 2], &[5, 0, 0x05, 0, 0, 0]).to_string(),
            "-0.05"
        );
        // money 12.3456 = 123456 in ten-thousandths: high 0, low 123456.
        let mut money = vec![8, 0, 0, 0, 0];
        money.extend_from_slice(&123_456u32.to_le_bytes());
        assert_eq!(read(&[MONEYN, 8], &money).to_string(), "12.3456");
        assert_eq!(
            read(&[FLTN, 8], &[&[8][..], &0.5f64.to_le_bytes()[..]].concat()),
            0.5
        );
        assert_eq!(read(&[FLT4], &1.1f32.to_le_bytes()).to_string(), "1.1");
    }

    #[test]
    fn dates_and_times_read_as_sql_server_writes_them() {
        // datetime: 2026-11-27 = 46_351 days from 1900-01-01; 09:30:05.123 is 10_261_537 ticks of 1/300 s.
        let days = days_of(2026, 11, 27) + FROM_1900;
        assert_eq!(days, 46_351);
        let mut dt = vec![8];
        dt.extend_from_slice(&i32::try_from(days).unwrap().to_le_bytes());
        dt.extend_from_slice(&10_261_537u32.to_le_bytes());
        assert_eq!(read(&[DATETIMN, 8], &dt), "2026-11-27T09:30:05.123");
        // date: days since 0001-01-01.
        let d = u32::try_from(days_of(2026, 11, 27) + FROM_YEAR_1)
            .unwrap()
            .to_le_bytes();
        assert_eq!(read(&[DATEN], &[3, d[0], d[1], d[2]]), "2026-11-27");
        // datetime2(3): 09:30:05.250 = 34_205_250 ms, in 4 bytes; then the date.
        let mut dt2 = vec![7];
        dt2.extend_from_slice(&34_205_250u32.to_le_bytes());
        dt2.extend_from_slice(&d[..3]);
        assert_eq!(read(&[DATETIME2N, 3], &dt2), "2026-11-27T09:30:05.250");
        // datetimeoffset(0): 23:30 UTC on the 27th at +02:00 is 01:30 on the 28th.
        let mut off = vec![8];
        off.extend_from_slice(&(23 * 3600 + 30 * 60u32).to_le_bytes()[..3]);
        off.extend_from_slice(&d[..3]);
        off.extend_from_slice(&120i16.to_le_bytes());
        assert_eq!(read(&[DATETIMEOFFSETN, 0], &off), "2026-11-28T01:30:00+02:00");
        assert_eq!(
            read(&[TIMEN, 7], &[5, 0x80, 0x96, 0x98, 0, 0]),
            "00:00:01.0000000"
        );
    }

    #[test]
    fn text_comes_from_utf16_or_the_collations_code_page() {
        let latin1 = [0x09, 0x04, 0xd0, 0x00, 0x34];
        let mut info = vec![BIGVARCHR, 0x32, 0x00];
        info.extend_from_slice(&latin1);
        assert_eq!(read(&info, &[3, 0, b'Z', 0x80, 0xe9]), "Z€é");
        let mut wide = vec![NVARCHAR, 0x64, 0x00];
        wide.extend_from_slice(&latin1);
        let zoe = ucs2("Zoë");
        assert_eq!(read(&wide, &[&[6, 0][..], &zoe[..]].concat()), "Zoë");
        assert_eq!(read(&wide, &[0xff, 0xff]), Value::Null);
        // nvarchar(max), in two chunks.
        let mut max = vec![NVARCHAR, 0xff, 0xff];
        max.extend_from_slice(&latin1);
        let mut plp = (zoe.len() as u64).to_le_bytes().to_vec();
        plp.extend_from_slice(&2u32.to_le_bytes());
        plp.extend_from_slice(&zoe[..2]);
        plp.extend_from_slice(&4u32.to_le_bytes());
        plp.extend_from_slice(&zoe[2..]);
        plp.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(read(&max, &plp), "Zoë");
        assert_eq!(read(&[BIGVARBIN, 0x10, 0], &[2, 0, 0xde, 0xad]), "\\xdead");
        // A UTF-8 collation (its flag set).
        assert_eq!(text_of(&[0x09, 0x04, 0x00, 0x04, 0x00]), Text::Utf8);
        assert_eq!(
            text_of(&[0x11, 0x04, 0, 0, 0]),
            Text::Other,
            "Japanese is not code page 1252"
        );
        let g = [
            0x78, 0x56, 0x34, 0x12, 0x34, 0x12, 0x78, 0x56, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        assert_eq!(
            read(&[GUID, 16], &[&[16][..], &g[..]].concat()),
            "12345678-1234-5678-0102-030405060708"
        );
    }

    #[test]
    fn values_cut_short_are_refused() {
        let mut b = Bytes::from_static(&[DECIMALN, 17]);
        assert!(type_info(&mut b).is_none());
        let kind = Kind::Sized(INTN);
        assert!(value(&kind, &mut Bytes::from_static(&[4, 1, 2])).is_none());
        assert!(
            value(&kind, &mut Bytes::from_static(&[3, 1, 2, 3])).is_none(),
            "no int of 3 bytes"
        );
        let plp = Kind::Short {
            text: Some(Text::Utf16),
            max: true,
        };
        assert!(
            value(
                &plp,
                &mut Bytes::from_static(&[4, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 1])
            )
            .is_none()
        );
    }

    #[test]
    fn parameters_are_sent_with_their_types() {
        let c = [0x09, 0x04, 0xd0, 0x00, 0x34];
        assert_eq!(
            param(&Param::Int(-2), c),
            (
                "bigint".to_owned(),
                [&[INTN, 8, 8][..], &(-2i64).to_le_bytes()[..]].concat()
            )
        );
        assert_eq!(param(&Param::Bool(true), c).1, vec![BITN, 1, 1, 1]);
        let (d, bytes) = param(&Param::Decimal("-19.90".into()), c);
        assert_eq!(d, "decimal(38,2)");
        assert_eq!(&bytes[..6], &[DECIMALN, 17, 38, 2, 17, 0]);
        assert_eq!(u128::from_le_bytes(bytes[6..22].try_into().unwrap()), 1990);
        let (d, bytes) = param(&Param::Text("x'; DROP TABLE t; --".into()), c);
        assert_eq!(d, "nvarchar(4000)");
        assert_eq!(&bytes[10..], &ucs2("x'; DROP TABLE t; --")[..]);
        assert_eq!(param(&Param::Null, c).1[8..], [0xff, 0xff]);
        let (d, long) = param(&Param::Text("y".repeat(5000)), c);
        assert_eq!(d, "nvarchar(max)");
        assert_eq!(long.len(), 1 + 2 + 5 + 8 + 4 + 10_000 + 4);
        // Too many digits for decimal(38): sent as text.
        assert_eq!(param(&Param::Decimal("1".repeat(40)), c).0, "nvarchar(4000)");
        let (d, t) = param(
            &Param::Time(Civil {
                year: 2026,
                month: 11,
                day: 27,
                hour: 0,
                minute: 0,
                second: 1,
                micros: 0,
            }),
            c,
        );
        assert_eq!(d, "datetime2(7)");
        let mut kind = Bytes::copy_from_slice(&t[..2]);
        let k = type_info(&mut kind).unwrap();
        let mut v = Bytes::copy_from_slice(&t[2..]);
        assert_eq!(value(&k, &mut v).unwrap(), "2026-11-27T00:00:01.0000000");
    }
}
