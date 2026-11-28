// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A table as CSV, and CSV as a table's rows: the first row names the columns; a new table takes its columns' types
//! from what they hold. A cell a spreadsheet would run as a formula (`=`, `+`, `-`, `@` first) is written with a `'`
//! before it, and read back without it.

use openduct_expr::{Record, Value};
use openduct_flow::tables::{Column, ColumnType, SYSTEM_COLUMNS, TableData, coerce, parse_time};
use serde_json::{Map, Value as Json};

/// A column's name made from a CSV header's: small letters, digits and `_`, starting with a letter.
pub(crate) fn column_name(header: &str) -> String {
    let mut out = String::new();
    for c in header.trim().chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let mut out = out.trim_matches('_').chars().take(60).collect::<String>();
    if !out.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        out = format!("c_{out}");
    }
    out
}

/// CSV read: its headers as written, and its rows, each by header.
pub(crate) type Read = (Vec<String>, Vec<Map<String, Json>>);

/// The rows of CSV text, each by its header's column name, with the headers as written beside.
pub(crate) fn rows(text: &str) -> Result<Read, String> {
    let read = crate::files::rows_of(text.as_bytes(), "text/csv")?;
    // Every row holds every header, in the header row's order; with no rows, the header row says them.
    let headers: Vec<String> = match read.first() {
        Some(first) => first.keys().cloned().collect(),
        None => text
            .lines()
            .next()
            .map(|first| {
                first
                    .split(',')
                    .map(|h| h.trim().trim_matches('"').to_owned())
                    .collect()
            })
            .unwrap_or_default(),
    };
    Ok((headers, read))
}

/// The columns a new table takes from CSV: each named from its header, typed by what every cell holds - numbers,
/// true and false, dates and times - or text.
pub(crate) fn columns_of(headers: &[String], rows: &[Map<String, Json>]) -> Vec<Column> {
    let mut out: Vec<Column> = Vec::new();
    for header in headers {
        let name = column_name(header);
        if SYSTEM_COLUMNS.contains(&name.as_str()) || out.iter().any(|c| c.name == name) {
            continue;
        }
        let cells: Vec<&str> = rows
            .iter()
            .filter_map(|r| r.get(header).and_then(Json::as_str))
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .collect();
        let all = |kind: ColumnType| {
            !cells.is_empty() && cells.iter().all(|c| coerce(kind, &Value::text(*c)).is_ok())
        };
        let kind = if all(ColumnType::Number) {
            ColumnType::Number
        } else if !cells.is_empty()
            && cells
                .iter()
                .all(|c| matches!(c.to_ascii_lowercase().as_str(), "true" | "false"))
        {
            ColumnType::Boolean
        } else if !cells.is_empty() && cells.iter().all(|c| parse_time(c).is_some()) {
            ColumnType::DateTime
        } else {
            ColumnType::Text
        };
        out.push(Column { name, kind });
    }
    out
}

/// CSV rows as a table's values: each header a column of the table (as written, or as [`column_name`] makes it),
/// each cell read as its column's type.
pub(crate) fn values_of(
    table: &TableData,
    headers: &[String],
    rows: &[Map<String, Json>],
) -> Result<Vec<Record>, String> {
    let mut columns: Vec<(String, String, ColumnType)> = Vec::new();
    for header in headers {
        let name = if table.columns.iter().any(|c| c.name == *header) {
            header.clone()
        } else {
            column_name(header)
        };
        if SYSTEM_COLUMNS.contains(&name.as_str()) {
            continue;
        }
        let kind = table.column(&name).map_err(|_| {
            format!(
                "the CSV's column {header:?} is not one of {}'s: add it first",
                table.name
            )
        })?;
        columns.push((header.clone(), name, kind));
    }
    rows.iter()
        .enumerate()
        .map(|(i, row)| {
            let mut values = Record::new();
            for (header, name, kind) in &columns {
                let cell = unguarded(row.get(header).and_then(Json::as_str).unwrap_or_default());
                let value = match kind {
                    ColumnType::Json => serde_json::from_str::<Json>(cell)
                        .map_or_else(|_| Value::text(cell), |j| Value::from_json(&j)),
                    ColumnType::Text => Value::text(cell),
                    _ => coerce(*kind, &Value::text(cell))
                        .map_err(|e| format!("line {}: {name}: {e}", i + 2))?,
                };
                values.insert(name.clone(), value);
            }
            Ok(values)
        })
        .collect()
}

/// A cell that would read as a formula, kept as text: a `'` before it.
fn guarded(text: String) -> String {
    if text.starts_with(['=', '+', '-', '@', '\t', '\r']) {
        format!("'{text}")
    } else {
        text
    }
}

fn unguarded(text: &str) -> &str {
    match text.strip_prefix('\'') {
        Some(rest) if rest.starts_with(['=', '+', '-', '@', '\t', '\r']) => rest,
        _ => text,
    }
}

fn field(text: &str) -> String {
    if text.contains([',', '"', '\n', '\r']) || text.trim() != text {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text.to_owned()
    }
}

/// A table as CSV: `id`, its columns, `created_at` and `updated_at`.
pub(crate) fn export(table: &TableData) -> String {
    let mut header = vec!["id".to_owned()];
    header.extend(table.columns.iter().map(|c| c.name.clone()));
    header.extend(["created_at".to_owned(), "updated_at".to_owned()]);
    let mut out = header.join(",");
    out.push_str("\r\n");
    for row in table.shown() {
        let Value::Record(fields) = row else { continue };
        let cells: Vec<String> = header
            .iter()
            .map(|h| {
                let value = fields.get(h).cloned().unwrap_or(Value::Null);
                let text = match &value {
                    Value::Text(t) => guarded(t.to_string()),
                    Value::Null => String::new(),
                    other => other.to_text(),
                };
                field(&text)
            })
            .collect();
        out.push_str(&cells.join(","));
        out.push_str("\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_table_takes_its_columns_and_types_from_the_csv() {
        let (headers, rows) =
            rows("Email,Score,Active,Signed up,Notes\r\nada@example.com,42,true,2026-11-01,\"likes, commas\"\nsam@example.com,7.5,false,2026-11-02,\n").unwrap();
        let columns = columns_of(&headers, &rows);
        let shown: Vec<(String, &str)> = columns.iter().map(|c| (c.name.clone(), c.kind.name())).collect();
        assert_eq!(
            shown,
            vec![
                ("email".to_owned(), "text"),
                ("score".to_owned(), "number"),
                ("active".to_owned(), "boolean"),
                ("signed_up".to_owned(), "datetime"),
                ("notes".to_owned(), "text"),
            ]
        );
        let table = TableData {
            name: "leads".to_owned(),
            columns,
            next_id: 1,
            ..TableData::default()
        };
        let values = values_of(&table, &headers, &rows).unwrap();
        assert_eq!(values[0]["notes"].to_json(), r#""likes, commas""#);
        assert_eq!(values[1]["score"].to_json(), "7.5");
        assert!(matches!(values[0]["signed_up"], Value::Time(_)));
        assert_eq!(column_name(" 2nd  Phone # "), "c_2nd_phone");
    }

    #[test]
    fn csv_out_quotes_what_it_must_and_guards_formulas() {
        let table = TableData {
            name: "t".to_owned(),
            columns: vec![Column {
                name: "note".to_owned(),
                kind: ColumnType::Text,
            }],
            next_id: 2,
            rows: [(
                1,
                openduct_flow::tables::Row {
                    id: 1,
                    values: std::sync::Arc::new(
                        [("note".to_owned(), Value::text("=HYPERLINK(\"x\"), said \"hi\""))]
                            .into_iter()
                            .collect(),
                    ),
                    created: 0,
                    updated: 0,
                },
            )]
            .into_iter()
            .collect(),
            stand_in: false,
        };
        let csv = export(&table);
        assert!(
            csv.starts_with(
                "id,note,created_at,updated_at\r\n1,\"'=HYPERLINK(\"\"x\"\"), said \"\"hi\"\"\","
            ),
            "{csv}"
        );
        let (headers, rows) = rows(&csv).unwrap();
        let back = values_of(&table, &headers, &rows).unwrap();
        assert_eq!(
            back[0]["note"].to_json(),
            r#""=HYPERLINK(\"x\"), said \"hi\"""#,
            "read back as written"
        );
        let wrong = values_of(&table, &["nope".to_owned()], &rows).unwrap_err();
        assert!(wrong.contains("not one of t's"), "{wrong}");
    }
}
