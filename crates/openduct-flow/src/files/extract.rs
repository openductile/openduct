// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What an `extract` step reads from a file: rows from CSV and spreadsheets (the first row naming the columns), a
//! value from JSON, text from a text file, a web page or a PDF, Base64 from anything.

use base64::Engine as _;
use openduct_expr::{Record, Value};
use serde_json::json;

use super::{FileData, Format};

/// A spreadsheet's cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Cell {
    Empty,
    Text(String),
    /// A number, as written.
    Number(String),
    Bool(bool),
    /// A date or time, as ISO 8601 text.
    Date(String),
}

/// A sheet's cells, row by row, and which sheet of which it is.
#[derive(Debug, Clone, Default)]
pub(super) struct Grid {
    pub(super) sheet: Option<String>,
    pub(super) sheets: Vec<String>,
    pub(super) rows: Vec<Vec<Cell>>,
}

/// Whether a built-in number format (by its id) shows a date or a time.
pub(super) fn builtin_date(id: u32) -> bool {
    matches!(id, 14..=22 | 27..=36 | 45..=47 | 50..=58)
}

/// Whether a number format's code shows a date or a time: it has d, m, y, h or s outside quotes and brackets.
pub(super) fn date_format(code: &str) -> bool {
    let (mut quoted, mut bracket, mut escaped) = (false, false, false);
    for c in code.chars() {
        match c {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            '"' => quoted = !quoted,
            '[' if !quoted => bracket = true,
            ']' if !quoted => bracket = false,
            'd' | 'D' | 'm' | 'M' | 'y' | 'Y' | 'h' | 'H' | 's' | 'S' if !quoted && !bracket => return true,
            _ => {}
        }
    }
    false
}

/// A spreadsheet's date serial as ISO 8601 text: a date, a time, or both. Day 60 of the 1900 system is the 29th of
/// February 1900, which never was: days after it count from a day earlier.
pub(super) fn serial_date(serial: f64, from_1904: bool) -> Option<String> {
    if !serial.is_finite() || !(0.0..2_958_466.0).contains(&serial) {
        return None;
    }
    let base = if from_1904 {
        chrono::NaiveDate::from_ymd_opt(1904, 1, 1)?
    } else if serial < 60.0 {
        chrono::NaiveDate::from_ymd_opt(1899, 12, 31)?
    } else {
        chrono::NaiveDate::from_ymd_opt(1899, 12, 30)?
    };
    #[allow(clippy::cast_possible_truncation)] // within the range checked above
    let days = serial.floor() as i64;
    #[allow(clippy::cast_possible_truncation)] // a fraction of a day, in seconds
    let seconds = ((serial - serial.floor()) * 86_400.0).round() as i64;
    let at = base.and_hms_opt(0, 0, 0)? + chrono::Duration::days(days) + chrono::Duration::seconds(seconds);
    Some(if serial < 1.0 && !from_1904 {
        at.format("%H:%M:%S").to_string()
    } else if seconds == 0 {
        at.format("%Y-%m-%d").to_string()
    } else {
        at.format("%Y-%m-%dT%H:%M:%S").to_string()
    })
}

/// A number written in a file as a value: exact, or as text when it is not one.
fn number(text: &str) -> Value {
    match serde_json::from_str::<serde_json::Value>(text.trim()) {
        Ok(n @ serde_json::Value::Number(_)) => Value::from_json(&n),
        _ => Value::text(text),
    }
}

fn cell_value(cell: &Cell) -> Value {
    match cell {
        Cell::Empty => Value::Null,
        Cell::Text(t) | Cell::Date(t) => Value::text(t.as_str()),
        Cell::Number(n) => number(n),
        Cell::Bool(b) => Value::Bool(*b),
    }
}

/// Rows of cells as records, the first row naming the columns (an empty name is `column_<n>`, a name given twice
/// gets `_2`); or, without a header, as lists. Rows with nothing in them are left out.
fn records(rows: Vec<Vec<Cell>>, header: bool) -> (Vec<Value>, Vec<String>) {
    let mut rows = rows.into_iter().filter(|r| {
        r.iter()
            .any(|c| !matches!(c, Cell::Empty) && *c != Cell::Text(String::new()))
    });
    if !header {
        let out: Vec<Value> = rows
            .map(|r| Value::list(r.iter().map(cell_value).collect()))
            .collect();
        return (out, Vec::new());
    }
    let Some(first) = rows.next() else {
        return (Vec::new(), Vec::new());
    };
    let mut columns: Vec<String> = Vec::new();
    for (i, cell) in first.iter().enumerate() {
        let mut name = match cell {
            Cell::Empty => String::new(),
            other => crate::template::as_text(&cell_value(other)).trim().to_owned(),
        };
        if name.is_empty() {
            name = format!("column_{}", i + 1);
        }
        let base = name.clone();
        let mut n = 2;
        while columns.contains(&name) {
            name = format!("{base}_{n}");
            n += 1;
        }
        columns.push(name);
    }
    let out = rows
        .map(|r| {
            let mut record = Record::new();
            for (i, name) in columns.iter().enumerate() {
                record.insert(name.clone(), r.get(i).map_or(Value::Null, cell_value));
            }
            Value::record(record)
        })
        .collect();
    (out, columns)
}

fn text_of(file: &FileData) -> Result<&str, String> {
    let bytes = file.bytes.as_slice();
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    std::str::from_utf8(bytes).map_err(|_| format!("{} is not UTF-8 text", file.name))
}

/// The settings an `extract` step reads a file with.
#[derive(Debug)]
pub(super) struct Asked<'a> {
    pub(super) format: Format,
    pub(super) sheet: Option<&'a str>,
    pub(super) header: bool,
    pub(super) delimiter: u8,
}

/// What an `extract` step gives: `{rows, count, columns}` for a table (with `sheet` and `sheets` for a spreadsheet),
/// `{data}` for JSON, `{text}` for text (with `title` for a web page, `pages` for a PDF), `{base64}` for Base64.
pub(super) fn extract(file: &FileData, asked: &Asked<'_>) -> Result<Value, String> {
    let table = |grid: Grid| {
        let (rows, columns) = records(grid.rows, asked.header);
        let mut out = json!({ "count": rows.len(), "columns": columns });
        if let (Some(sheet), Some(fields)) = (&grid.sheet, out.as_object_mut()) {
            fields.insert("sheet".to_owned(), json!(sheet));
            fields.insert("sheets".to_owned(), json!(grid.sheets));
        }
        let mut record = match Value::from_json(&out) {
            Value::Record(r) => (*r).clone(),
            _ => Record::new(),
        };
        record.shift_insert(0, "rows".to_owned(), Value::list(rows));
        Value::record(record)
    };
    Ok(match asked.format {
        Format::Csv => {
            let rows = super::csv::read(text_of(file)?, asked.delimiter)?;
            let cells = rows
                .into_iter()
                .map(|r| r.into_iter().map(Cell::Text).collect())
                .collect();
            table(Grid {
                rows: cells,
                ..Grid::default()
            })
        }
        Format::Xlsx => table(super::xlsx::read(&file.bytes, asked.sheet)?),
        Format::Xls => table(super::xls::read(&file.bytes, asked.sheet)?),
        Format::Json => {
            let text = text_of(file)?;
            let data = match serde_json::from_str::<serde_json::Value>(text) {
                Ok(v) => v,
                // JSON lines: a value on each line.
                Err(first) => serde_json::Value::Array(
                    text.lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(serde_json::from_str)
                        .collect::<Result<_, _>>()
                        .map_err(|_| format!("{} is not JSON: {first}", file.name))?,
                ),
            };
            let count = data.as_array().map(Vec::len);
            let mut out = json!({ "data": data });
            if let (Some(n), Some(fields)) = (count, out.as_object_mut()) {
                fields.insert("count".to_owned(), json!(n));
            }
            Value::from_json(&out)
        }
        Format::Text => Value::from_json(&json!({ "text": text_of(file)? })),
        Format::Html => {
            let (text, title) = super::html::to_text(text_of(file)?);
            Value::from_json(&json!({ "text": text, "title": title }))
        }
        Format::Pdf => {
            let (text, pages) = super::pdf::text(&file.bytes)?;
            Value::from_json(&json!({ "text": text, "pages": pages }))
        }
        Format::Base64 => Value::from_json(&json!({
            "base64": base64::engine::general_purpose::STANDARD.encode(file.bytes.as_slice()),
            "name": file.name, "type": file.media_type, "size": file.bytes.len(),
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spreadsheet_dates_and_formats_read_as_their_cells_show_them() {
        assert_eq!(serial_date(45_988.0, false).as_deref(), Some("2025-11-27"));
        assert_eq!(
            serial_date(45_988.5, false).as_deref(),
            Some("2025-11-27T12:00:00")
        );
        assert_eq!(serial_date(1.0, false).as_deref(), Some("1900-01-01"));
        assert_eq!(serial_date(61.0, false).as_deref(), Some("1900-03-01"));
        assert_eq!(serial_date(0.75, false).as_deref(), Some("18:00:00"));
        assert_eq!(serial_date(0.0, true).as_deref(), Some("1904-01-01"));
        assert_eq!(serial_date(f64::NAN, false), None);
        assert_eq!(serial_date(-1.0, false), None);
        assert!(date_format("yyyy-mm-dd") && date_format("[h]:mm:ss") && date_format("d/m/yy"));
        assert!(
            !date_format("0.00")
                && !date_format("\"days\" 0")
                && !date_format("[Red]0.00")
                && !date_format("General")
        );
        assert!(builtin_date(14) && !builtin_date(2));
    }

    #[test]
    fn the_first_row_names_the_columns_and_empty_rows_are_left_out() {
        let t = |s: &str| Cell::Text(s.to_owned());
        let rows = vec![
            vec![t("id"), Cell::Empty, t("id")],
            vec![t("A-1"), Cell::Number("2.50".into()), Cell::Bool(true)],
            vec![Cell::Empty, t("")],
            vec![t("A-2")],
        ];
        let (records, columns) = records(rows.clone(), true);
        assert_eq!(columns, vec!["id", "column_2", "id_2"]);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].to_json(), r#"{"id":"A-1","column_2":2.5,"id_2":true}"#);
        assert_eq!(
            records[1].to_json(),
            r#"{"id":"A-2","column_2":null,"id_2":null}"#
        );
        let (lists, none) = super::records(rows, false);
        assert!(none.is_empty());
        assert_eq!(lists.len(), 3);
    }
}
