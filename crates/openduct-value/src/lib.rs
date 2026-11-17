// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The values OpenDuct's pipelines and connectors carry: a message as named fields, exact decimals, text, lists and
//! times - how they come from JSON and go back to it, and how they read as text.

use std::fmt::Write as _;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use indexmap::IndexMap;
use rust_decimal::Decimal;

/// A record: named fields, in the order they came.
pub type Record = IndexMap<String, Value>;

/// One value. Lists, records and text are shared, so reading a field of a large message never copies it.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    /// An exact decimal, always without trailing zeros.
    Number(Decimal),
    /// A number too large or too precise to calculate with. It passes through unchanged; arithmetic refuses it.
    BigNumber(Arc<str>),
    Text(Arc<str>),
    List(Arc<Vec<Value>>),
    Record(Arc<Record>),
    Time(DateTime<Utc>),
}

impl Value {
    pub fn number(n: Decimal) -> Self {
        Self::Number(n.normalize())
    }

    pub fn text(text: impl Into<Arc<str>>) -> Self {
        Self::Text(text.into())
    }

    pub fn list(items: Vec<Value>) -> Self {
        Self::List(Arc::new(items))
    }

    pub fn record(record: Record) -> Self {
        Self::Record(Arc::new(record))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The kind, as `type_of` names it.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Number(_) | Self::BigNumber(_) => "number",
            Self::Text(_) => "text",
            Self::List(_) => "list",
            Self::Record(_) => "record",
            Self::Time(_) => "time",
        }
    }

    /// The value as an error message shows it: `"two"`, `5`, `null`, `a list`.
    pub fn describe(&self) -> String {
        match self {
            Self::Text(t) => {
                let shown: String = t.chars().take(40).collect();
                let more = if t.chars().count() > 40 { "..." } else { "" };
                format!("{:?}", format!("{shown}{more}"))
            }
            Self::List(_) => "a list".to_owned(),
            Self::Record(_) => "a record".to_owned(),
            other => other.to_text(),
        }
    }

    /// The value as text, as `to_text` and `concat` write it. Null is empty text here; callers that skip null do so
    /// before asking.
    pub fn to_text(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Bool(b) => b.to_string(),
            Self::Number(n) => n.normalize().to_string(),
            Self::BigNumber(raw) => raw.to_string(),
            Self::Text(t) => t.to_string(),
            Self::List(_) | Self::Record(_) => self.to_json(),
            Self::Time(t) => iso(t),
        }
    }

    // ------------------------------------------------------------------------------------------------- JSON

    /// From JSON. Numbers keep every digit they were written with; one too large to calculate with is kept as written.
    pub fn from_json(json: &serde_json::Value) -> Self {
        match json {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Bool(*b),
            serde_json::Value::Number(n) => {
                let text = n.to_string();
                parse_number(&text).map_or_else(|| Self::BigNumber(text.into()), Self::number)
            }
            serde_json::Value::String(s) => Self::text(s.as_str()),
            serde_json::Value::Array(items) => Self::list(items.iter().map(Self::from_json).collect()),
            serde_json::Value::Object(fields) => Self::record(
                fields
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::from_json(v)))
                    .collect(),
            ),
        }
    }

    /// Parse JSON text into a value.
    pub fn parse_json(text: &str) -> Result<Self, String> {
        let json: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        Ok(Self::from_json(&json))
    }

    /// The value as compact JSON text. A time is written as ISO 8601 text in UTC.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    fn write_json(&self, out: &mut String) {
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Self::Number(n) => out.push_str(&n.normalize().to_string()),
            Self::BigNumber(raw) => out.push_str(raw),
            Self::Text(t) => write_json_text(t, out),
            Self::Time(t) => write_json_text(&iso(t), out),
            Self::List(items) => {
                out.push('[');
                for (at, item) in items.iter().enumerate() {
                    if at > 0 {
                        out.push(',');
                    }
                    item.write_json(out);
                }
                out.push(']');
            }
            Self::Record(fields) => {
                out.push('{');
                for (at, (name, value)) in fields.iter().enumerate() {
                    if at > 0 {
                        out.push(',');
                    }
                    write_json_text(name, out);
                    out.push(':');
                    value.write_json(out);
                }
                out.push('}');
            }
        }
    }

    /// Roughly how many bytes the value takes as JSON: what the answer-size limit measures.
    pub fn size(&self) -> usize {
        match self {
            Self::Null | Self::Bool(_) => 5,
            Self::Number(_) => 30,
            Self::BigNumber(raw) => raw.len(),
            Self::Text(t) => t.len() + 2,
            Self::Time(_) => 26,
            Self::List(items) => items.iter().map(|v| v.size() + 1).sum::<usize>() + 2,
            Self::Record(fields) => fields.iter().map(|(k, v)| k.len() + 4 + v.size()).sum::<usize>() + 2,
        }
    }
}

fn write_json_text(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// A time as ISO 8601 text in UTC: `2026-11-14T22:30:00Z`, with a fraction of a second only when there is one.
pub fn iso(time: &DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// Text as an exact decimal: `42`, `-3.5`, `1.5e3`. `None` when it is not a number or is too large.
pub fn parse_number(text: &str) -> Option<Decimal> {
    let text = text.trim();
    if text.is_empty()
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
    {
        return None;
    }
    if text.contains(['e', 'E']) {
        Decimal::from_scientific(text).ok()
    } else {
        // Exact or not at all: a number with more digits than can be kept is never quietly rounded.
        Decimal::from_str_exact(text).ok()
    }
}

/// A time from ISO 8601 text: `2026-11-15T09:52:31Z`, with an offset, or a date alone (midnight UTC).
pub fn parse_iso_time(text: &str) -> Option<DateTime<Utc>> {
    let text = text.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(text) {
        return Some(t.with_timezone(&Utc));
    }
    if let Ok(t) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(t.and_utc());
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_numbers_keep_their_digits_and_lose_trailing_zeros() {
        let v = Value::parse_json(
            r#"{"a": 0.1, "b": 24.50, "c": 1.5e3, "d": 123456789012345678901234567890123}"#,
        )
        .unwrap();
        assert_eq!(
            v.to_json(),
            r#"{"a":0.1,"b":24.5,"c":1500,"d":123456789012345678901234567890123}"#
        );
        let Value::Record(r) = &v else { panic!() };
        assert!(
            matches!(r["d"], Value::BigNumber(_)),
            "too large to calculate with, kept as written"
        );
    }

    #[test]
    fn times_are_written_in_utc_with_a_fraction_only_when_there_is_one() {
        let t = parse_iso_time("2026-11-15T10:01:13+01:00").unwrap();
        assert_eq!(iso(&t), "2026-11-15T09:01:13Z");
        let t = parse_iso_time("2026-11-15T09:52:31.977Z").unwrap();
        assert_eq!(iso(&t), "2026-11-15T09:52:31.977Z");
        assert_eq!(
            iso(&parse_iso_time("2026-11-15").unwrap()),
            "2026-11-15T00:00:00Z"
        );
    }
}
