// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CSV as RFC 4180 has it - fields in double quotes may hold the delimiter, quotes (doubled) and new lines - read
//! into rows and written from them.

/// The most rows a file is read as.
pub(super) const MAX_ROWS: usize = 100_000;
/// The most columns a row has.
pub(super) const MAX_COLUMNS: usize = 1_000;
/// The most cells a spreadsheet's sheet is read as.
pub(super) const MAX_CELLS: usize = 1_000_000;

/// The records of a CSV text: each a list of fields.
pub(super) fn read(text: &str, delimiter: u8) -> Result<Vec<Vec<String>>, String> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let delimiter = char::from(delimiter);
    let mut records: Vec<Vec<String>> = Vec::new();
    let (mut record, mut field, mut quoted, mut chars) =
        (Vec::new(), String::new(), false, text.chars().peekable());
    let mut was_quoted = false;
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            (true, '"') => quoted = false,
            (true, other) => field.push(other),
            (false, '"') if field.is_empty() && !was_quoted => {
                quoted = true;
                was_quoted = true;
            }
            (false, c) if c == delimiter => {
                record.push(std::mem::take(&mut field));
                was_quoted = false;
                if record.len() > MAX_COLUMNS {
                    return Err(format!("a row of more than {MAX_COLUMNS} columns"));
                }
            }
            (false, '\r') => {}
            (false, '\n') => {
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
                was_quoted = false;
                if records.len() > MAX_ROWS + 1 {
                    return Err(format!("more than {MAX_ROWS} rows"));
                }
            }
            (false, other) => field.push(other),
        }
    }
    if quoted {
        return Err("a quoted field never ends".to_owned());
    }
    if !field.is_empty() || !record.is_empty() {
        record.push(field);
        records.push(record);
    }
    records.retain(|r| !(r.len() == 1 && r.first().is_some_and(String::is_empty)));
    Ok(records)
}

/// One field as CSV writes it: in quotes when it holds the delimiter, a quote, a new line, or space at an end.
fn field(text: &str, delimiter: char, out: &mut String) {
    let needs = text.contains([delimiter, '"', '\n', '\r'])
        || text.starts_with(char::is_whitespace)
        || text.ends_with(char::is_whitespace);
    if needs {
        out.push('"');
        out.push_str(&text.replace('"', "\"\""));
        out.push('"');
    } else {
        out.push_str(text);
    }
}

/// Rows of fields as CSV text, each row ended by CRLF.
pub(super) fn write(rows: &[Vec<String>], delimiter: u8) -> String {
    let delimiter = char::from(delimiter);
    let mut out = String::new();
    for row in rows {
        for (i, text) in row.iter().enumerate() {
            if i > 0 {
                out.push(delimiter);
            }
            field(text, delimiter, &mut out);
        }
        out.push_str("\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_reads_quotes_delimiters_and_new_lines_and_writes_them_back() {
        let text = "\u{feff}id;note\r\n1;\"with; a semicolon\"\n2;\"said \"\"hi\"\"\non two lines\"\n\n";
        let rows = read(text, b';').unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1], vec!["1", "with; a semicolon"]);
        assert_eq!(rows[2][1], "said \"hi\"\non two lines");
        let again = read(&write(&rows, b';'), b';').unwrap();
        assert_eq!(again, rows);
        assert_eq!(
            write(&[vec![" x".into(), "a,b".into()]], b','),
            "\" x\",\"a,b\"\r\n"
        );
        assert!(read("a,\"never ends", b',').is_err());
        assert!(read(&"x,".repeat(MAX_COLUMNS + 2), b',').is_err());
        assert!(read(&"x\n".repeat(MAX_ROWS + 5), b',').is_err());
    }
}
