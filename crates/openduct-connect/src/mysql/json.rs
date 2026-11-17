// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MySQL's binary JSON, as a JSON column's value is stored in the binary log, written back as JSON text. Every
//! offset and length is checked against the value before it is followed, and nesting is limited, so a value that
//! is not one is refused rather than misread.

use bytes::Bytes;

use crate::json::json_string;

const SMALL_OBJECT: u8 = 0x00;
const LARGE_OBJECT: u8 = 0x01;
const SMALL_ARRAY: u8 = 0x02;
const LARGE_ARRAY: u8 = 0x03;
const LITERAL: u8 = 0x04;
const INT16: u8 = 0x05;
const UINT16: u8 = 0x06;
const INT32: u8 = 0x07;
const UINT32: u8 = 0x08;
const INT64: u8 = 0x09;
const UINT64: u8 = 0x0a;
const DOUBLE: u8 = 0x0b;
const STRING: u8 = 0x0c;
const OPAQUE: u8 = 0x0f;
const MAX_DEPTH: usize = 100;

fn bad() -> String {
    "a JSON value that cannot be read".to_owned()
}

/// The JSON text of a binary JSON value; an empty value is JSON `null`.
pub(super) fn decode(data: &[u8]) -> Result<String, String> {
    let Some((&kind, rest)) = data.split_first() else {
        return Ok("null".to_owned());
    };
    let mut out = String::new();
    value(kind, rest, 0, &mut out)?;
    Ok(out)
}

fn le(bytes: &[u8]) -> u64 {
    bytes.iter().rev().fold(0u64, |n, b| (n << 8) | u64::from(*b))
}

fn slice(data: &[u8], at: usize, n: usize) -> Result<&[u8], String> {
    data.get(at..at.checked_add(n).ok_or_else(bad)?).ok_or_else(bad)
}

fn value(kind: u8, data: &[u8], depth: usize, out: &mut String) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("a JSON value nested too deep".to_owned());
    }
    match kind {
        SMALL_OBJECT | LARGE_OBJECT => container(data, kind == LARGE_OBJECT, true, depth, out),
        SMALL_ARRAY | LARGE_ARRAY => container(data, kind == LARGE_ARRAY, false, depth, out),
        LITERAL => {
            out.push_str(match data.first() {
                Some(0) => "null",
                Some(1) => "true",
                Some(2) => "false",
                _ => return Err(bad()),
            });
            Ok(())
        }
        INT16 => number(slice(data, 0, 2)?, true, out),
        UINT16 => number(slice(data, 0, 2)?, false, out),
        INT32 => number(slice(data, 0, 4)?, true, out),
        UINT32 => number(slice(data, 0, 4)?, false, out),
        INT64 => number(slice(data, 0, 8)?, true, out),
        UINT64 => number(slice(data, 0, 8)?, false, out),
        DOUBLE => {
            let v = f64::from_bits(le(slice(data, 0, 8)?));
            out.push_str(&if v.is_finite() {
                format!("{v}")
            } else {
                "null".to_owned()
            });
            Ok(())
        }
        STRING => {
            let (length, used) = variable_length(data)?;
            out.push_str(&json_string(&String::from_utf8_lossy(slice(data, used, length)?)));
            Ok(())
        }
        OPAQUE => opaque(data, out),
        _ => Err(bad()),
    }
}

fn number(bytes: &[u8], signed: bool, out: &mut String) -> Result<(), String> {
    let raw = le(bytes);
    if signed {
        let shift = 64 - 8 * u32::try_from(bytes.len()).unwrap_or(8);
        out.push_str(&(((raw << shift) as i64) >> shift).to_string());
    } else {
        out.push_str(&raw.to_string());
    }
    Ok(())
}

/// A string's length: seven bits a byte, the high bit saying more follow; and the bytes it took.
fn variable_length(data: &[u8]) -> Result<(usize, usize), String> {
    let mut length = 0usize;
    for (i, b) in data.iter().take(5).enumerate() {
        length |= usize::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Ok((length, i + 1));
        }
    }
    Err(bad())
}

fn container(data: &[u8], large: bool, object: bool, depth: usize, out: &mut String) -> Result<(), String> {
    let width = if large { 4 } else { 2 };
    let count = usize::try_from(le(slice(data, 0, width)?)).map_err(|_| bad())?;
    let size = usize::try_from(le(slice(data, width, width)?)).map_err(|_| bad())?;
    if size > data.len() || count > size {
        return Err(bad());
    }
    let data = slice(data, 0, size)?;
    let keys_at = 2 * width;
    let key_entry = width + 2;
    let values_at = keys_at + if object { count * key_entry } else { 0 };
    let value_entry = 1 + width;
    out.push(if object { '{' } else { '[' });
    for i in 0..count {
        if i > 0 {
            out.push(',');
        }
        if object {
            let entry = slice(data, keys_at + i * key_entry, key_entry)?;
            let offset = usize::try_from(le(slice(entry, 0, width)?)).map_err(|_| bad())?;
            let length = usize::try_from(le(slice(entry, width, 2)?)).map_err(|_| bad())?;
            out.push_str(&json_string(&String::from_utf8_lossy(slice(
                data, offset, length,
            )?)));
            out.push(':');
        }
        let entry = slice(data, values_at + i * value_entry, value_entry)?;
        let (&kind, field) = entry.split_first().ok_or_else(bad)?;
        // Small values are kept in the entry itself; the rest are at an offset into the container.
        let inline = matches!(kind, LITERAL | INT16 | UINT16) || large && matches!(kind, INT32 | UINT32);
        if inline {
            value(kind, field, depth + 1, out)?;
        } else {
            let offset = usize::try_from(le(field)).map_err(|_| bad())?;
            value(kind, data.get(offset..).ok_or_else(bad)?, depth + 1, out)?;
        }
    }
    out.push(if object { '}' } else { ']' });
    Ok(())
}

/// A value of a MySQL type kept as it is in JSON: decimals, dates and times written as text; anything else as
/// hex.
fn opaque(data: &[u8], out: &mut String) -> Result<(), String> {
    let (&field, rest) = data.split_first().ok_or_else(bad)?;
    let (length, used) = variable_length(rest)?;
    let body = slice(rest, used, length)?;
    let text = match field {
        super::values::NEWDECIMAL => {
            let [precision, scale, ..] = body else {
                return Err(bad());
            };
            let mut digits = Bytes::copy_from_slice(body.get(2..).unwrap_or_default());
            super::values::decimal(&mut digits, usize::from(*precision), usize::from(*scale))?
                .ok_or_else(bad)?
        }
        super::values::DATE | super::values::DATETIME | super::values::TIMESTAMP | super::values::TIME => {
            let packed = i64::from_le_bytes(slice(body, 0, 8)?.try_into().map_err(|_| bad())?);
            packed_time(field, packed)
        }
        _ => {
            let hex: String = body.iter().map(|b| format!("{b:02x}")).collect();
            format!("\\x{hex}")
        }
    };
    out.push_str(&json_string(&text));
    Ok(())
}

/// MySQL's packed form of a date or time (integer part above 24 bits of microseconds) as it writes it.
fn packed_time(field: u8, packed: i64) -> String {
    let negative = packed < 0;
    let packed = packed.unsigned_abs();
    let (whole, micros) = (packed >> 24, packed & 0xff_ffff);
    let fraction = if micros == 0 {
        String::new()
    } else {
        format!(".{micros:06}")
    };
    if field == super::values::TIME {
        return format!(
            "{}{:02}:{:02}:{:02}{fraction}",
            if negative { "-" } else { "" },
            (whole >> 12) & 0x3ff,
            (whole >> 6) & 63,
            whole & 63
        );
    }
    let ymd = whole >> 17;
    let ym = ymd >> 5;
    let hms = whole & 0x1_ffff;
    let date = format!("{:04}-{:02}-{:02}", ym / 13, ym % 13, ymd & 31);
    if field == super::values::DATE {
        return date;
    }
    format!(
        "{date} {:02}:{:02}:{:02}{fraction}",
        hms >> 12,
        (hms >> 6) & 63,
        hms & 63
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn objects_arrays_and_scalars_read_back_as_json() {
        // {"a": 1, "b": [true, "x"]}, as MySQL stores it (small object, small array).
        let array: &[u8] = &[2, 0, 12, 0, LITERAL, 1, 0, STRING, 10, 0, 1, b'x'];
        let mut object = vec![2, 0];
        let header = 4 + 2 * 4 + 2 * 3;
        let keys = header + 2;
        let size = keys + array.len();
        object.extend_from_slice(&u16::try_from(size).unwrap().to_le_bytes());
        object.extend_from_slice(&u16::try_from(header).unwrap().to_le_bytes());
        object.extend_from_slice(&1u16.to_le_bytes());
        object.extend_from_slice(&u16::try_from(header + 1).unwrap().to_le_bytes());
        object.extend_from_slice(&1u16.to_le_bytes());
        object.extend_from_slice(&[INT16, 1, 0]);
        object.push(SMALL_ARRAY);
        object.extend_from_slice(&u16::try_from(keys).unwrap().to_le_bytes());
        object.extend_from_slice(b"ab");
        object.extend_from_slice(array);
        let mut value_bytes = vec![SMALL_OBJECT];
        value_bytes.extend_from_slice(&object);
        assert_eq!(decode(&value_bytes).unwrap(), r#"{"a":1,"b":[true,"x"]}"#);
        assert_eq!(decode(&[]).unwrap(), "null");
        assert_eq!(
            decode(&[INT64, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]).unwrap(),
            "-1"
        );
        assert_eq!(decode(&[STRING, 3, b'a', b'"', b'b']).unwrap(), r#""a\"b""#);
    }

    #[test]
    fn values_that_cannot_be_are_refused() {
        // Counts and offsets past the value's end, a string longer than its value, nesting too deep.
        for bad in [
            &[SMALL_OBJECT, 0xff, 0xff, 4, 0][..],
            &[SMALL_ARRAY, 1, 0, 20, 0, STRING, 50, 0],
            &[STRING, 10, b'a'],
            &[STRING, 0x80, 0x80, 0x80, 0x80, 0x80],
            &[0x7e],
        ] {
            assert!(decode(bad).is_err(), "{bad:?}");
        }
        let mut out = String::new();
        assert!(
            value(SMALL_ARRAY, &[0, 0, 4, 0], MAX_DEPTH + 1, &mut out)
                .unwrap_err()
                .contains("too deep")
        );
    }
}
