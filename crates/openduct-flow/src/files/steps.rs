// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The work of the steps that read and make files - `extract`, `convert`, `file_details` - and of the steps that
//! send or take one: an HTTP request's file or multipart body and its answer as a file, an e-mail's attachments.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use openduct_expr::{Compiled, Env, Record, Value};
use serde_json::Value as Json;

use super::extract::{Asked, Cell};
use super::multipart::Out;
use super::{Convert, Details, Extract, FileData, Format, Part, Reference, Response, RunFiles, is_file};
use crate::model::{Body, Http};
use crate::run::{Effects, HttpRequest};

/// How long reading or writing one file may take.
const WORK_TIME: Duration = Duration::from_secs(60);
/// The most files one e-mail carries.
const MAX_ATTACHMENTS: usize = 10;

/// Work on a file's bytes, done off the run's own thread and bounded in time.
async fn work<T: Send + 'static>(
    job: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    match tokio::time::timeout(WORK_TIME, tokio::task::spawn_blocking(job)).await {
        Ok(Ok(done)) => done,
        Ok(Err(_)) => Err("the file could not be read".to_owned()),
        Err(_) => Err(format!(
            "the file took more than {}s to read",
            WORK_TIME.as_secs()
        )),
    }
}

/// The file an expression names - or, when it gives text, that text as a file.
// storage: also what a storage step writes.
pub(crate) async fn file_of(
    effects: &dyn Effects,
    files: &RunFiles,
    expression: &Compiled,
    env: &Env<'_>,
) -> Result<FileData, String> {
    let given = expression.evaluate(env).map_err(|e| format!("file: {e}"))?;
    match &given {
        Value::Text(t) if !is_file(&given) => Ok(FileData {
            name: "text".to_owned(),
            media_type: "text/plain".to_owned(),
            bytes: Arc::new(t.as_bytes().to_vec()),
        }),
        _ => files.read(effects, &given).await,
    }
}

pub(crate) async fn extract(
    effects: &dyn Effects,
    files: &RunFiles,
    x: &Extract,
    env: &Env<'_>,
) -> Result<Value, String> {
    let file = file_of(effects, files, &x.file, env).await?;
    let format = match x.format {
        Some(f) => f,
        None => Format::of(&file).ok_or_else(|| {
            format!(
                "which format {} is in is not known from its name or type: set format (csv, json, xlsx, xls, text, \
                 html, pdf or base64)",
                file.name
            )
        })?,
    };
    let sheet = x
        .sheet
        .as_ref()
        .map(|t| t.render(env))
        .transpose()
        .map_err(|e| format!("sheet: {e}"))?;
    let (header, delimiter) = (x.header, x.delimiter);
    work(move || {
        let asked = Asked {
            format,
            sheet: sheet.as_deref(),
            header,
            delimiter,
        };
        super::extract::extract(&file, &asked).map_err(|e| format!("{} ({}): {e}", file.name, format.name()))
    })
    .await
}

/// A value as a cell of a table: text, a number, true or false; a list or a record as JSON.
fn cell(value: &Value) -> Cell {
    match value {
        Value::Null => Cell::Empty,
        Value::Bool(b) => Cell::Bool(*b),
        Value::Number(_) | Value::BigNumber(_) => Cell::Number(value.to_text()),
        Value::Time(_) => Cell::Date(value.to_text()),
        other => Cell::Text(other.to_text()),
    }
}

/// Rows (records, or lists) as a table: a header row of the columns asked for (or every column, in the order they
/// come), then a row for each.
fn table(rows: &Value, columns: &[String], header: bool) -> Result<Vec<Vec<Cell>>, String> {
    let rows = match rows {
        Value::List(items) => items,
        Value::Null => return Ok(Vec::new()),
        other => return Err(format!("rows: a list of records, not {}", other.describe())),
    };
    if rows.len() > super::csv::MAX_ROWS {
        return Err(format!("rows: at most {}", super::csv::MAX_ROWS));
    }
    let mut names: Vec<String> = columns.to_vec();
    if names.is_empty() {
        for row in rows.iter() {
            if let Value::Record(r) = row {
                for k in r.keys() {
                    if !names.contains(k) {
                        names.push(k.clone());
                    }
                }
            }
            if names.len() > super::csv::MAX_COLUMNS {
                return Err(format!("rows: at most {} columns", super::csv::MAX_COLUMNS));
            }
        }
    }
    let mut out = Vec::with_capacity(rows.len() + 1);
    if header && !names.is_empty() {
        out.push(names.iter().map(|n| Cell::Text(n.clone())).collect());
    }
    for row in rows.iter() {
        out.push(match row {
            Value::Record(r) => names.iter().map(|n| r.get(n).map_or(Cell::Empty, cell)).collect(),
            Value::List(cells) => cells.iter().take(super::csv::MAX_COLUMNS).map(cell).collect(),
            other => vec![cell(other)],
        });
    }
    Ok(out)
}

fn as_text(cells: Vec<Vec<Cell>>) -> Vec<Vec<String>> {
    cells
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|c| match c {
                    Cell::Empty => String::new(),
                    Cell::Bool(b) => b.to_string(),
                    Cell::Text(t) | Cell::Number(t) | Cell::Date(t) => t,
                })
                .collect()
        })
        .collect()
}

pub(crate) async fn convert(
    effects: &dyn Effects,
    files: &RunFiles,
    c: &Convert,
    step: &str,
    env: &Env<'_>,
) -> Result<Value, String> {
    let from = c
        .from
        .as_ref()
        .map(|e| e.evaluate(env))
        .transpose()
        .map_err(|e| {
            format!(
                "{}: {e}",
                if c.format == Format::Base64 {
                    "base64"
                } else {
                    "rows"
                }
            )
        })?;
    let (media_type, extension) = c.format.media_type();
    let bytes = match c.format {
        Format::Csv | Format::Xlsx => {
            let cells = table(from.as_ref().unwrap_or(&Value::Null), &c.columns, c.header)?;
            let (format, sheet, delimiter, header) = (c.format, c.sheet.clone(), c.delimiter, c.header);
            work(move || match format {
                Format::Csv => Ok(super::csv::write(&as_text(cells), delimiter).into_bytes()),
                _ => super::xlsx::write(&sheet, &cells, header),
            })
            .await?
        }
        Format::Json => {
            let value = from.unwrap_or(Value::Null);
            let json: Json = serde_json::from_str(&value.to_json()).unwrap_or(Json::Null);
            serde_json::to_vec_pretty(&json).map_err(|e| e.to_string())?
        }
        Format::Text | Format::Html => match (&c.text, from) {
            (Some(t), _) => t.render(env).map_err(|e| format!("text: {e}"))?.into_bytes(),
            (None, Some(v)) => v.to_text().into_bytes(),
            (None, None) => Vec::new(),
        },
        Format::Base64 => {
            let text = from.map(|v| v.to_text()).unwrap_or_default();
            let clean: String = text.chars().filter(|c| !c.is_whitespace()).collect();
            let clean = clean
                .split_once("base64,")
                .map_or(clean.as_str(), |(_, b)| b)
                .to_owned();
            let bare = clean.trim_end_matches('=');
            base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(bare)
                .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(bare))
                .map_err(|_| "base64: not Base64 text".to_owned())?
        }
        Format::Xls | Format::Pdf => return Err(format!("files are not written as {}", c.format.name())),
    };
    let name = match &c.name {
        Some(t) => t.render(env).map_err(|e| format!("name: {e}"))?,
        None => format!("{step}.{extension}"),
    };
    let media_type = c.media_type.clone().unwrap_or_else(|| {
        if c.format == Format::Base64 {
            guess_type(&name).to_owned()
        } else {
            media_type.to_owned()
        }
    });
    files.make(effects, &name, &media_type, bytes).await
}

/// A media type from a file's name.
pub(crate) fn guess_type(name: &str) -> &'static str {
    let extension = name
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "csv" => "text/csv",
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "json" => "application/json",
        "xml" => "application/xml",
        "zip" => "application/zip",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    }
}

pub(crate) async fn details(
    effects: &dyn Effects,
    files: &RunFiles,
    d: &Details,
    env: &Env<'_>,
) -> Result<Value, String> {
    let given = d.file.evaluate(env).map_err(|e| format!("file: {e}"))?;
    let file = files.read(effects, &given).await?;
    let mut reference = Reference::of(&given).ok_or("file: not a file")?;
    if let Some(name) = &d.name {
        reference.name = super::clean_name(&name.render(env).map_err(|e| format!("name: {e}"))?);
    }
    if let Some(media_type) = &d.media_type {
        reference.media_type = super::clean_type(media_type);
    }
    let renamed = FileData {
        name: reference.name.clone(),
        media_type: reference.media_type.clone(),
        bytes: file.bytes,
    };
    let Value::Record(fields) = reference.value() else {
        return Err("file: not a file".to_owned());
    };
    let mut out: Record = (*fields).clone();
    let extension = reference
        .name
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase());
    out.insert("extension".to_owned(), extension.map_or(Value::Null, Value::text));
    out.insert(
        "format".to_owned(),
        Format::of(&renamed).map_or(Value::Null, |f| Value::text(f.name())),
    );
    out.insert("sha256".to_owned(), Value::text(reference.sha256.as_str()));
    Ok(Value::record(out))
}

/// A header's value in a request's headers, taken out of them.
fn take_header(headers: &mut Vec<(String, String)>, name: &str) -> Option<String> {
    let at = headers.iter().position(|(k, _)| k.eq_ignore_ascii_case(name))?;
    Some(headers.remove(at).1)
}

/// An HTTP step's request sent: with its file or multipart body; its answer read as a file when it is one.
pub(crate) async fn http(
    effects: &dyn Effects,
    files: &RunFiles,
    http: &Http,
    mut built: HttpRequest,
    env: &Env<'_>,
) -> Result<Value, String> {
    match &http.body {
        Body::File(expression) => {
            let file = file_of(effects, files, expression, env).await?;
            let kind =
                take_header(&mut built.headers, "content-type").unwrap_or_else(|| file.media_type.clone());
            built.body = Some((kind, file.bytes.to_vec()));
        }
        Body::Multipart(fields) => {
            let mut texts: Vec<(&str, String)> = Vec::new();
            let mut held: Vec<(&str, FileData)> = Vec::new();
            for (name, part) in fields {
                match part {
                    Part::Text(t) => texts.push((name, t.render(env).map_err(|e| format!("{name}: {e}"))?)),
                    Part::File {
                        file,
                        name: rename,
                        media_type,
                    } => {
                        let mut found = file_of(effects, files, file, env)
                            .await
                            .map_err(|e| format!("{name}: {e}"))?;
                        if let Some(t) = rename {
                            found.name =
                                super::clean_name(&t.render(env).map_err(|e| format!("{name}: {e}"))?);
                        }
                        if let Some(m) = media_type {
                            found.media_type = super::clean_type(m);
                        }
                        held.push((name, found));
                    }
                }
            }
            let mut parts: Vec<Out<'_>> = texts.into_iter().map(|(n, t)| Out::Text(n, t)).collect();
            parts.extend(held.iter().map(|(n, f)| Out::File(n, f)));
            let (kind, bytes) = super::multipart::write(&parts);
            take_header(&mut built.headers, "content-type");
            built.body = Some((kind, bytes));
        }
        _ => {}
    }
    if http.response == Response::Text {
        return effects.http(built).await;
    }
    let url = built.url.clone();
    let raw = effects.http_bytes(built).await?;
    let Some(body) = raw.body else {
        return Ok(raw.answer);
    };
    let Value::Record(answer) = &raw.answer else {
        return Ok(raw.answer);
    };
    let mut answer: Record = (**answer).clone();
    let headers: serde_json::Map<String, Json> = match answer.get("headers") {
        Some(h) => serde_json::from_str(&h.to_json()).unwrap_or_default(),
        None => serde_json::Map::new(),
    };
    let status = answer
        .get("status")
        .map(Value::to_text)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let wanted = status < 400
        && !body.is_empty()
        && match http.response {
            Response::File => true,
            Response::Auto => binary(&header("content-type"), &header("content-disposition"), &body),
            Response::Text => false,
        };
    let value = if wanted {
        let name = file_name(&header("content-disposition"), &url);
        let mut media_type = header("content-type");
        if media_type.is_empty() {
            media_type = guess_type(&name).to_owned();
        }
        files.make(effects, &name, &media_type, body).await?
    } else {
        Value::from_json(&super::body_value(&headers, &body))
    };
    answer.insert("body".to_owned(), value);
    Ok(Value::record(answer))
}

/// Whether an answer is a file: sent as an attachment, or of a type that is not text, JSON, XML or a form - or, with
/// no type, not text.
fn binary(content_type: &str, disposition: &str, body: &[u8]) -> bool {
    if disposition
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("attachment")
    {
        return true;
    }
    let kind = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if kind.is_empty() {
        return std::str::from_utf8(body).is_err();
    }
    let text = kind.starts_with("text/")
        || [
            "json",
            "xml",
            "javascript",
            "x-www-form-urlencoded",
            "yaml",
            "graphql",
        ]
        .iter()
        .any(|t| kind.contains(t));
    !text
}

/// A downloaded file's name: the one its answer gives, or the last part of its address, or `download`.
fn file_name(disposition: &str, url: &str) -> String {
    let lower = disposition.to_ascii_lowercase();
    if let Some(at) = lower.find("filename*=") {
        let value = disposition
            .get(at + 10..)
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim();
        if let Some((_, encoded)) = value.split_once("''") {
            return percent_decoded(encoded.trim_matches('"'));
        }
    }
    if let Some(at) = lower.find("filename=") {
        let value = disposition.get(at + 9..).unwrap_or_default();
        let value = match value.strip_prefix('"') {
            Some(q) => q.split('"').next().unwrap_or_default(),
            None => value.split(';').next().unwrap_or_default().trim(),
        };
        if !value.is_empty() {
            return value.to_owned();
        }
    }
    let path = url.split(['?', '#']).next().unwrap_or_default();
    let last = path.rsplit('/').next().unwrap_or_default();
    if last.is_empty() || path.matches('/').count() < 3 {
        "download".to_owned()
    } else {
        percent_decoded(last)
    }
}

fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        if b == b'%'
            && let Some(v) = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The files an e-mail carries: each expression a file, or a list of them.
pub(crate) async fn attachments(
    effects: &dyn Effects,
    files: &RunFiles,
    list: &[Compiled],
    env: &Env<'_>,
) -> Result<Vec<FileData>, String> {
    let mut out = Vec::new();
    for (i, expression) in list.iter().enumerate() {
        let given = expression
            .evaluate(env)
            .map_err(|e| format!("attachments[{i}]: {e}"))?;
        let items = match given {
            Value::Null => continue,
            Value::List(items) => items.to_vec(),
            one => vec![one],
        };
        for item in items {
            if out.len() >= MAX_ATTACHMENTS {
                return Err(format!("attachments: at most {MAX_ATTACHMENTS} files"));
            }
            out.push(
                files
                    .read(effects, &item)
                    .await
                    .map_err(|e| format!("attachments[{i}]: {e}"))?,
            );
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_is_a_file_by_its_type_and_named_by_its_header() {
        assert!(binary("application/pdf", "", b"%PDF"));
        assert!(binary("text/csv", "attachment; filename=x.csv", b"a,b"));
        assert!(!binary("application/json; charset=utf-8", "", b"{}"));
        assert!(!binary("text/html", "inline", b"<p>"));
        assert!(!binary("", "", b"plain text"));
        assert!(binary("", "", &[0xff, 0xfe, 0x00]));
        assert_eq!(
            file_name("attachment; filename=\"Q3 report.pdf\"", "https://x.example/a"),
            "Q3 report.pdf"
        );
        assert_eq!(
            file_name("attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.pdf", ""),
            "résumé.pdf"
        );
        assert_eq!(
            file_name("", "https://x.example/files/photo%201.png?size=2"),
            "photo 1.png"
        );
        assert_eq!(file_name("", "https://x.example/"), "download");
        assert_eq!(file_name("", "https://x.example"), "download");
    }

    #[test]
    fn rows_become_a_table_with_the_columns_asked_for() {
        let rows = Value::parse_json(
            r#"[{"id": "A-1", "total": 12.5}, {"id": "A-2", "note": "late", "paid": true}]"#,
        )
        .unwrap();
        let all = table(&rows, &[], true).unwrap();
        assert_eq!(
            all[0],
            ["id", "total", "note", "paid"]
                .map(|n| Cell::Text(n.into()))
                .to_vec()
        );
        assert_eq!(all[1][1], Cell::Number("12.5".into()));
        assert_eq!(all[2][3], Cell::Bool(true));
        let some = table(&rows, &["note".to_owned(), "id".to_owned()], false).unwrap();
        assert_eq!(
            some,
            vec![
                vec![Cell::Empty, Cell::Text("A-1".into())],
                vec![Cell::Text("late".into()), Cell::Text("A-2".into())]
            ]
        );
        assert!(table(&Value::text("x"), &[], true).is_err());
        assert_eq!(guess_type("photo.JPG"), "image/jpeg");
    }
}
