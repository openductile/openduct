// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Snowflake's values both ways. An answer's rows come as text (the SQL API's `jsonv2`), typed by the answer's
//! `rowType`, and become plain JSON: numbers exact, dates and times in ISO 8601, semi-structured values as JSON. A
//! message's field is bound as text, with the cast that makes it the column's type.

use chrono::{DateTime, Days, FixedOffset, NaiveDate, NaiveTime};

use crate::json::{is_json_number, json_string};

/// A column of a table or an answer: its name as Snowflake keeps it, and its type as the SQL API names it
/// (`fixed`, `text`, `timestamp_ntz`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) kind: String,
    pub(super) precision: Option<u64>,
    pub(super) scale: Option<u64>,
}

/// The columns of an answer, from its `resultSetMetaData.rowType`.
pub(super) fn columns(meta: &serde_json::Value) -> Vec<Column> {
    meta.get("rowType")
        .and_then(|r| r.as_array())
        .map(|types| {
            types
                .iter()
                .filter_map(|t| {
                    Some(Column {
                        name: t.get("name")?.as_str()?.to_owned(),
                        kind: t.get("type")?.as_str()?.to_ascii_lowercase(),
                        precision: t.get("precision").and_then(serde_json::Value::as_u64),
                        scale: t.get("scale").and_then(serde_json::Value::as_u64),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// An unquoted name (`ORDERS`, `order_id`): Snowflake keeps it in upper case.
pub(super) fn plain_name(text: &str) -> bool {
    (1..=255).contains(&text.len())
        && text.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// A name in double quotes, exactly as Snowflake keeps it.
pub(super) fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Seconds since 1970 as the SQL API writes them (`1616173619.000000000`, `-1.500000000`): whole seconds and
/// nanoseconds, the nanoseconds never negative.
fn epoch(text: &str) -> Option<(i64, u32)> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || fraction.len() > 9
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let whole: i64 = whole.parse().ok()?;
    let nanos: u32 = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<9}").parse().ok()?
    };
    match (negative, nanos) {
        (false, n) => Some((whole, n)),
        (true, 0) => Some((-whole, 0)),
        (true, n) => Some((-whole - 1, 1_000_000_000 - n)),
    }
}

fn text_or_null(text: Option<String>) -> String {
    text.map_or_else(|| "null".to_owned(), |t| json_string(&t))
}

/// One value of an answer as JSON text.
pub(super) fn to_json(column: &Column, value: Option<&str>) -> String {
    let Some(v) = value else {
        return "null".to_owned();
    };
    match column.kind.as_str() {
        "fixed" | "real" | "decfloat" if is_json_number(v) => v.to_owned(),
        "boolean" if v == "true" || v == "false" => v.to_owned(),
        "date" => text_or_null(
            v.parse::<i64>()
                .ok()
                .and_then(|days| {
                    let start = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                    if days >= 0 {
                        start.checked_add_days(Days::new(days.unsigned_abs()))
                    } else {
                        start.checked_sub_days(Days::new(days.unsigned_abs()))
                    }
                })
                .map(|d| d.format("%Y-%m-%d").to_string())
                .or_else(|| Some(v.to_owned())),
        ),
        "time" => text_or_null(
            epoch(v)
                .and_then(|(s, n)| NaiveTime::from_num_seconds_from_midnight_opt(u32::try_from(s).ok()?, n))
                .map(|t| t.format("%H:%M:%S%.f").to_string())
                .or_else(|| Some(v.to_owned())),
        ),
        "timestamp_ntz" => text_or_null(
            epoch(v)
                .and_then(|(s, n)| DateTime::from_timestamp(s, n))
                .map(|t| t.naive_utc().format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                .or_else(|| Some(v.to_owned())),
        ),
        "timestamp_ltz" => text_or_null(
            epoch(v)
                .and_then(|(s, n)| DateTime::from_timestamp(s, n))
                .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string())
                .or_else(|| Some(v.to_owned())),
        ),
        // Seconds, a space, and the offset in minutes plus 1440.
        "timestamp_tz" => text_or_null(
            v.split_once(' ')
                .and_then(|(at, offset)| {
                    let (s, n) = epoch(at)?;
                    let minutes = offset.parse::<i32>().ok()? - 1440;
                    let zone = FixedOffset::east_opt(minutes.checked_mul(60)?)?;
                    Some(DateTime::from_timestamp(s, n)?.with_timezone(&zone))
                })
                .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.f%:z").to_string())
                .or_else(|| Some(v.to_owned())),
        ),
        "variant" | "object" | "array" => match serde_json::from_str::<serde_json::Value>(v) {
            Ok(parsed) => parsed.to_string(),
            Err(_) => json_string(v),
        },
        _ => json_string(v),
    }
}

/// The SQL that makes the text bound at `at` (`?`, `$1`) the column's type.
pub(super) fn cast(column: &Column, at: &str) -> String {
    match column.kind.as_str() {
        "fixed" => format!(
            "{at}::NUMBER({},{})",
            column.precision.unwrap_or(38),
            column.scale.unwrap_or(0)
        ),
        "real" => format!("{at}::FLOAT"),
        "decfloat" => format!("{at}::DECFLOAT"),
        "text" => format!("{at}::VARCHAR"),
        "boolean" => format!("{at}::BOOLEAN"),
        "date" => format!("{at}::DATE"),
        "time" => format!("{at}::TIME"),
        "timestamp_ntz" => format!("{at}::TIMESTAMP_NTZ"),
        "timestamp_ltz" => format!("{at}::TIMESTAMP_LTZ"),
        "timestamp_tz" => format!("{at}::TIMESTAMP_TZ"),
        "binary" => format!("{at}::BINARY"),
        "variant" => format!("PARSE_JSON({at})"),
        "object" => format!("PARSE_JSON({at})::OBJECT"),
        "array" => format!("PARSE_JSON({at})::ARRAY"),
        _ => at.to_owned(),
    }
}

/// A message's field as the text bound for the column: `None` for null. A semi-structured column takes the value's
/// JSON; any other takes text as it is, and a number or a yes/no written out.
pub(super) fn bound(column: &Column, value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        v if matches!(column.kind.as_str(), "variant" | "object" | "array") => Some(v.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn column(kind: &str) -> Column {
        Column {
            name: "C".to_owned(),
            kind: kind.to_owned(),
            precision: Some(10),
            scale: Some(2),
        }
    }

    #[test]
    fn answers_become_plain_json() {
        let cases = [
            ("fixed", Some("12.50"), "12.50"),
            (
                "fixed",
                Some("123456789012345678901234567890"),
                "123456789012345678901234567890",
            ),
            ("real", Some("inf"), "\"inf\""),
            ("text", Some("a \"b\""), r#""a \"b\"""#),
            ("boolean", Some("true"), "true"),
            ("date", Some("18262"), "\"2020-01-01\""),
            ("date", Some("-1"), "\"1969-12-31\""),
            ("time", Some("82919.000000000"), "\"23:01:59\""),
            ("time", Some("3600.500000000"), "\"01:00:00.500\""),
            (
                "timestamp_ntz",
                Some("1616173619.000000000"),
                "\"2021-03-19T17:06:59\"",
            ),
            (
                "timestamp_ntz",
                Some("-1.500000000"),
                "\"1969-12-31T23:59:58.500\"",
            ),
            (
                "timestamp_ltz",
                Some("1616173619.123456789"),
                "\"2021-03-19T17:06:59.123456789Z\"",
            ),
            // The offset is minutes plus 1440: 1500 is an hour east of UTC, 960 eight hours west.
            (
                "timestamp_tz",
                Some("1616173619.000000000 1500"),
                "\"2021-03-19T18:06:59+01:00\"",
            ),
            (
                "timestamp_tz",
                Some("1616173619.000000000 960"),
                "\"2021-03-19T09:06:59-08:00\"",
            ),
            ("variant", Some("{\n  \"a\": [1, 2]\n}"), r#"{"a":[1,2]}"#),
            ("variant", Some("\"text\""), "\"text\""),
            ("binary", Some("48656C6C6F"), "\"48656C6C6F\""),
            ("text", None, "null"),
        ];
        for (kind, value, want) in cases {
            assert_eq!(to_json(&column(kind), value), want, "{kind} {value:?}");
        }
    }

    #[test]
    fn epochs_are_read_exactly() {
        assert_eq!(epoch("1616173619.000000000"), Some((1_616_173_619, 0)));
        assert_eq!(epoch("82919.5"), Some((82_919, 500_000_000)));
        assert_eq!(epoch("-1.250000000"), Some((-2, 750_000_000)));
        assert_eq!(epoch("-3"), Some((-3, 0)));
        for bad in ["", ".5", "1.0000000001", "1e5", "12a"] {
            assert_eq!(epoch(bad), None, "{bad}");
        }
    }

    #[test]
    fn fields_are_bound_as_text_with_the_column_cast() {
        assert_eq!(cast(&column("fixed"), "$1"), "$1::NUMBER(10,2)");
        assert_eq!(cast(&column("object"), "?"), "PARSE_JSON(?)::OBJECT");
        assert_eq!(
            bound(&column("text"), &serde_json::json!("Ada")),
            Some("Ada".to_owned())
        );
        assert_eq!(
            bound(&column("fixed"), &serde_json::json!(12.5)),
            Some("12.5".to_owned())
        );
        assert_eq!(
            bound(&column("variant"), &serde_json::json!("Ada")),
            Some("\"Ada\"".to_owned())
        );
        assert_eq!(
            bound(&column("variant"), &serde_json::json!({"a": 1})),
            Some(r#"{"a":1}"#.to_owned())
        );
        assert_eq!(bound(&column("text"), &serde_json::Value::Null), None);
        assert!(plain_name("ORDERS") && plain_name("order_id") && plain_name("_x$1"));
        assert!(!plain_name("1x") && !plain_name("a b") && !plain_name("a\"b") && !plain_name(""));
        assert_eq!(quoted("we\"ird"), "\"we\"\"ird\"");
    }
}
