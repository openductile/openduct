// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! XLSX spreadsheets (Office Open XML): a sheet read into cells - text, numbers, true or false, dates as the cell's
//! format shows them - and rows written as a sheet of one workbook, the header row in bold.

use super::csv::{MAX_CELLS, MAX_COLUMNS, MAX_ROWS};
use super::extract::{Cell, Grid, date_format, serial_date};
use super::xml::{Event, Reader, attribute, escape};
use super::zip::Archive;

/// The longest text a cell holds in a spreadsheet application.
const MAX_CELL: usize = 32_767;

fn text_of(bytes: Option<Vec<u8>>) -> String {
    bytes
        .map(|b| String::from_utf8(b).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
        .unwrap_or_default()
}

/// The sheets of a workbook - each its name and its part - and whether its dates count from 1904.
fn sheets(archive: &Archive<'_>) -> Result<(Vec<(String, String)>, bool), String> {
    let workbook = text_of(archive.read("xl/workbook.xml")?);
    if workbook.is_empty() {
        return Err("not an XLSX spreadsheet: it has no workbook".to_owned());
    }
    let rels = text_of(archive.read("xl/_rels/workbook.xml.rels")?);
    let mut targets = Vec::new();
    let mut reader = Reader::new(&rels);
    while let Some(event) = reader.next_event() {
        if let Event::Open {
            name: "Relationship",
            attributes,
            ..
        } = event
            && let (Some(id), Some(target)) = (attribute(&attributes, "Id"), attribute(&attributes, "Target"))
        {
            let part = match target.strip_prefix('/') {
                Some(absolute) => absolute.to_owned(),
                None => format!("xl/{target}"),
            };
            targets.push((id.to_owned(), part));
        }
    }
    let (mut out, mut from_1904) = (Vec::new(), false);
    let mut reader = Reader::new(&workbook);
    while let Some(event) = reader.next_event() {
        match event {
            Event::Open {
                name: "workbookPr",
                attributes,
                ..
            } => {
                from_1904 = matches!(attribute(&attributes, "date1904"), Some("1" | "true"));
            }
            Event::Open {
                name: "sheet",
                attributes,
                ..
            } => {
                let name = attribute(&attributes, "name").unwrap_or_default().to_owned();
                let id = attribute(&attributes, "id").unwrap_or_default();
                if let Some((_, part)) = targets.iter().find(|(i, _)| i == id) {
                    out.push((name, part.clone()));
                }
            }
            _ => {}
        }
    }
    Ok((out, from_1904))
}

fn shared_strings(archive: &Archive<'_>) -> Result<Vec<String>, String> {
    let text = text_of(archive.read("xl/sharedStrings.xml")?);
    let (mut out, mut current, mut in_text, mut phonetic) = (Vec::new(), String::new(), false, false);
    let mut reader = Reader::new(&text);
    while let Some(event) = reader.next_event() {
        match event {
            Event::Open { name: "si", .. } => current.clear(),
            Event::Close("si") => {
                if out.len() >= MAX_ROWS * 10 {
                    return Err("a spreadsheet of too many texts".to_owned());
                }
                out.push(std::mem::take(&mut current));
            }
            Event::Open {
                name: "rPh",
                empty: false,
                ..
            } => phonetic = true,
            Event::Close("rPh") => phonetic = false,
            Event::Open {
                name: "t",
                empty: false,
                ..
            } => in_text = true,
            Event::Close("t") => in_text = false,
            Event::Text(t) if in_text && !phonetic => current.push_str(&t),
            _ => {}
        }
    }
    Ok(out)
}

/// Which of a workbook's cell formats show a date or a time.
fn date_styles(archive: &Archive<'_>) -> Result<Vec<bool>, String> {
    let text = text_of(archive.read("xl/styles.xml")?);
    let (mut custom, mut out, mut in_xfs) = (Vec::new(), Vec::new(), false);
    let mut reader = Reader::new(&text);
    while let Some(event) = reader.next_event() {
        match event {
            Event::Open {
                name: "numFmt",
                attributes,
                ..
            } => {
                if let (Some(id), Some(code)) = (
                    attribute(&attributes, "numFmtId"),
                    attribute(&attributes, "formatCode"),
                ) {
                    custom.push((id.parse::<u32>().unwrap_or(0), date_format(code)));
                }
            }
            Event::Open {
                name: "cellXfs",
                empty: false,
                ..
            } => in_xfs = true,
            Event::Close("cellXfs") => in_xfs = false,
            Event::Open {
                name: "xf",
                attributes,
                ..
            } if in_xfs => {
                let id: u32 = attribute(&attributes, "numFmtId")
                    .and_then(|i| i.parse().ok())
                    .unwrap_or(0);
                let date = custom
                    .iter()
                    .find(|(c, _)| *c == id)
                    .map_or_else(|| super::extract::builtin_date(id), |(_, d)| *d);
                out.push(date);
            }
            _ => {}
        }
    }
    Ok(out)
}

/// A column's place from a cell's reference: `C7` is 2.
fn column_of(reference: &str) -> Option<usize> {
    let mut n = 0usize;
    let mut any = false;
    for b in reference.bytes().take_while(u8::is_ascii_alphabetic) {
        n = n
            .checked_mul(26)?
            .checked_add(usize::from(b.to_ascii_uppercase() - b'A') + 1)?;
        any = true;
    }
    any.then(|| n - 1)
}

/// A column's letters: 0 is `A`, 26 is `AA`.
fn column_name(mut n: usize) -> String {
    let mut out = Vec::new();
    loop {
        out.push(b'A' + u8::try_from(n % 26).unwrap_or(0));
        if n < 26 {
            break;
        }
        n = n / 26 - 1;
    }
    out.reverse();
    String::from_utf8_lossy(&out).into_owned()
}

/// The sheet called `wanted` (the first, when not given) of an XLSX workbook, as rows of cells.
pub(super) fn read(bytes: &[u8], wanted: Option<&str>) -> Result<Grid, String> {
    let archive = Archive::open(bytes)?;
    let (sheets, from_1904) = sheets(&archive)?;
    let names: Vec<String> = sheets.iter().map(|(n, _)| n.clone()).collect();
    let (name, part) = match wanted {
        Some(w) => sheets
            .iter()
            .find(|(n, _)| n == w)
            .ok_or_else(|| format!("there is no sheet {w:?}: it has {}", names.join(", ")))?,
        None => sheets.first().ok_or("the workbook has no sheets")?,
    };
    let strings = shared_strings(&archive)?;
    let dates = date_styles(&archive)?;
    let text = text_of(archive.read(part)?);
    let mut rows: Vec<Vec<Cell>> = Vec::new();
    let mut cells = 0usize;
    let mut row: Vec<Cell> = Vec::new();
    let (mut kind, mut style, mut column) = (String::new(), 0usize, 0usize);
    let (mut value, mut in_value, mut inline) = (String::new(), false, false);
    let mut reader = Reader::new(&text);
    while let Some(event) = reader.next_event() {
        match event {
            Event::Open {
                name: "row", empty, ..
            } => {
                row.clear();
                column = 0;
                if empty {
                    rows.push(Vec::new());
                }
            }
            Event::Close("row") => {
                if rows.len() > MAX_ROWS {
                    return Err(format!("more than {MAX_ROWS} rows"));
                }
                cells += row.len();
                if cells > MAX_CELLS {
                    return Err(format!("a sheet of more than {MAX_CELLS} cells"));
                }
                rows.push(std::mem::take(&mut row));
            }
            Event::Open {
                name: "c",
                attributes,
                empty,
            } => {
                kind = attribute(&attributes, "t").unwrap_or("n").to_owned();
                style = attribute(&attributes, "s")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                if let Some(c) = attribute(&attributes, "r").and_then(column_of) {
                    column = c;
                }
                if column >= MAX_COLUMNS {
                    return Err(format!("a row of more than {MAX_COLUMNS} columns"));
                }
                value.clear();
                if empty {
                    column += 1;
                }
            }
            Event::Open {
                name: "v",
                empty: false,
                ..
            } => in_value = true,
            Event::Close("v") => in_value = false,
            Event::Open {
                name: "is",
                empty: false,
                ..
            } => inline = true,
            Event::Close("is") => inline = false,
            Event::Text(t) if in_value || inline => value.push_str(&t),
            Event::Close("c") => {
                let cell = match kind.as_str() {
                    "s" => value
                        .trim()
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| strings.get(i))
                        .map_or(Cell::Empty, |s| Cell::Text(s.clone())),
                    "b" => Cell::Bool(value.trim() == "1"),
                    "str" | "inlineStr" | "e" | "d" => Cell::Text(value.clone()),
                    _ if value.trim().is_empty() => Cell::Empty,
                    _ if dates.get(style).copied().unwrap_or(false) => value
                        .trim()
                        .parse::<f64>()
                        .ok()
                        .and_then(|n| serial_date(n, from_1904))
                        .map_or_else(|| Cell::Number(value.trim().to_owned()), Cell::Date),
                    _ => Cell::Number(value.trim().to_owned()),
                };
                if row.len() <= column {
                    row.resize(column + 1, Cell::Empty);
                }
                if let Some(place) = row.get_mut(column) {
                    *place = cell;
                }
                column += 1;
            }
            _ => {}
        }
    }
    Ok(Grid {
        sheet: Some(name.clone()),
        sheets: names,
        rows,
    })
}

const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

/// A workbook of one sheet: `rows`, the first in bold when `header`.
pub(super) fn write(sheet: &str, rows: &[Vec<Cell>], header: bool) -> Result<Vec<u8>, String> {
    let sheet: String = sheet
        .chars()
        .map(|c| {
            if "[]:*?/\\".contains(c) || c.is_control() {
                '_'
            } else {
                c
            }
        })
        .take(31)
        .collect();
    let sheet = if sheet.trim().is_empty() {
        "Sheet1".to_owned()
    } else {
        sheet
    };
    let mut data = String::new();
    for (r, row) in rows.iter().enumerate() {
        data.push_str(&format!("<row r=\"{}\">", r + 1));
        let bold = if header && r == 0 { " s=\"1\"" } else { "" };
        for (c, cell) in row.iter().enumerate() {
            let at = format!("{}{}", column_name(c), r + 1);
            match cell {
                Cell::Empty => {}
                Cell::Bool(b) => data.push_str(&format!(
                    "<c r=\"{at}\" t=\"b\"{bold}><v>{}</v></c>",
                    u8::from(*b)
                )),
                Cell::Number(n) => data.push_str(&format!("<c r=\"{at}\"{bold}><v>{}</v></c>", escape(n))),
                Cell::Text(t) | Cell::Date(t) => {
                    let cut: String = t.chars().take(MAX_CELL).collect();
                    data.push_str(&format!(
                        "<c r=\"{at}\" t=\"inlineStr\"{bold}><is><t xml:space=\"preserve\">{}</t></is></c>",
                        escape(&cut)
                    ));
                }
            }
        }
        data.push_str("</row>");
    }
    let head = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    let types = format!(
        "{head}<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">\
         <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>\
         <Default Extension=\"xml\" ContentType=\"application/xml\"/>\
         <Override PartName=\"/xl/workbook.xml\" \
         ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>\
         <Override PartName=\"/xl/worksheets/sheet1.xml\" \
         ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>\
         <Override PartName=\"/xl/styles.xml\" \
         ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/></Types>"
    );
    let root = format!(
        "{head}<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" \
         Target=\"xl/workbook.xml\"/></Relationships>"
    );
    let workbook = format!(
        "{head}<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"{}\" sheetId=\"1\" \
         r:id=\"rId1\"/></sheets></workbook>",
        escape(&sheet)
    );
    let workbook_rels = format!(
        "{head}<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" \
         Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId2\" Type=\"{OFFICE}/styles\" \
         Target=\"styles.xml\"/></Relationships>"
    );
    let styles = format!(
        "{head}<styleSheet xmlns=\"{MAIN}\"><fonts count=\"2\"><font><sz val=\"11\"/><name val=\"Calibri\"/>\
         </font><font><b/><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts><fills count=\"2\"><fill>\
         <patternFill patternType=\"none\"/></fill><fill><patternFill patternType=\"gray125\"/></fill></fills>\
         <borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>\
         <cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>\
         <cellXfs count=\"2\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>\
         <xf numFmtId=\"0\" fontId=\"1\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyFont=\"1\"/></cellXfs>\
         <cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles></styleSheet>"
    );
    let worksheet = format!("{head}<worksheet xmlns=\"{MAIN}\"><sheetData>{data}</sheetData></worksheet>");
    super::zip::write(&[
        ("[Content_Types].xml", types.into_bytes()),
        ("_rels/.rels", root.into_bytes()),
        ("xl/workbook.xml", workbook.into_bytes()),
        ("xl/_rels/workbook.xml.rels", workbook_rels.into_bytes()),
        ("xl/styles.xml", styles.into_bytes()),
        ("xl/worksheets/sheet1.xml", worksheet.into_bytes()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_are_named_and_read_as_spreadsheets_name_them() {
        for (n, name) in [(0, "A"), (25, "Z"), (26, "AA"), (701, "ZZ"), (702, "AAA")] {
            assert_eq!(column_name(n), name);
            assert_eq!(column_of(&format!("{name}12")), Some(n));
        }
        assert_eq!(column_of("12"), None);
    }

    #[test]
    fn a_sheet_written_reads_back_cell_for_cell() {
        let rows = vec![
            vec![
                Cell::Text("id".into()),
                Cell::Text("total".into()),
                Cell::Text("paid".into()),
            ],
            vec![
                Cell::Text("A-1".into()),
                Cell::Number("12.5".into()),
                Cell::Bool(true),
            ],
            vec![Cell::Text(" <b>&\"x\" ".into()), Cell::Empty, Cell::Bool(false)],
        ];
        let bytes = write("Orders: Q3", &rows, true).unwrap();
        let grid = read(&bytes, None).unwrap();
        assert_eq!(grid.sheet.as_deref(), Some("Orders_ Q3"));
        assert_eq!(grid.rows, rows);
        assert!(read(&bytes, Some("Nope")).unwrap_err().contains("Orders_ Q3"));
        assert!(read(b"PK nonsense", None).is_err());
    }
}
