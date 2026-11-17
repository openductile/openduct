// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Lookup files: a workspace's reference data - rates, price lists, mappings - by path (`/rates.csv`), up to 5 MiB
//! each. Their bytes are kept in parts in a topic of their own (the file's record says where), so no file is held in
//! memory whole until a run reads it. A `lookup` step finds rows in one: CSV (a header row, then rows), JSON (a list
//! of records) or JSON lines. The studio shows a file's first rows before it is used.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Response, StatusCode};
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{Outgoing, Routed, records};
use openduct_expr::Value as Data;
use ring::digest;
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;

use crate::State;
use crate::api::{access, text};
use crate::http::{Answer, Call, json, problem};
use crate::model::{FILE_BODIES, FileRecord, Model, Role, SCHEMA, file_key, new_id, now};

/// The largest file.
pub(crate) const MAX_FILE: usize = 5 * 1024 * 1024;
/// The size of a part of a file's bytes.
const PART: usize = 512 * 1024;
/// The most rows a file is read as.
const MAX_ROWS: usize = 100_000;
/// The rows a preview shows.
const PREVIEW_ROWS: usize = 50;

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "files"]) => list(state, call).await,
        (&Method::POST, ["api", "files"]) => upload(state, call).await,
        (&Method::GET, ["api", "files", "content"]) => download(state, call).await,
        (&Method::GET, ["api", "files", "preview"]) => preview(state, call).await,
        (&Method::DELETE, ["api", "files"]) => remove(state, call).await,
        _ => return None,
    })
}

/// `/rates.csv`: a path a step can name - from the root, no `..`, a few characters.
fn path_problem(path: &str) -> Option<&'static str> {
    let fine = path.starts_with('/')
        && path.len() <= 200
        && !path.split('/').any(|p| p == ".." || p == ".")
        && !path.contains("//")
        && path
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '/' | '-' | '_' | '.' | ' '));
    (!fine).then_some(
        "path: from the root, as /rates.csv or /lists/prices.json (letters, digits, - _ . and spaces)",
    )
}

fn query_path(call: &Call) -> String {
    let encoded = call
        .query
        .split('&')
        .find_map(|p| p.strip_prefix("path="))
        .unwrap_or_default();
    crate::hooks::form(&format!("p={encoded}"))
        .get("p")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn shown(f: &FileRecord) -> Value {
    json!({ "path": f.path, "content_type": f.content_type, "size": f.size, "sha256": f.sha256, "updated": f.updated })
}

async fn list(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let _ = state.model.files.refresh().await;
    let mut out: Vec<Value> = state
        .model
        .files
        .all()
        .await
        .iter()
        .filter(|(_, f)| f.workspace == access.workspace.id && f.deleted.is_none())
        .map(|(_, f)| shown(f))
        .collect();
    out.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    json(StatusCode::OK, &json!({ "files": out }))
}

fn kind_of(path: &str, given: &str) -> String {
    if !given.is_empty() {
        return given.to_owned();
    }
    match path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "csv" => "text/csv",
        "json" => "application/json",
        "jsonl" | "ndjson" => "application/jsonl",
        "txt" => "text/plain",
        _ => "application/octet-stream",
    }
    .to_owned()
}

async fn upload(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let path = text(&call.body, "path").trim().to_owned();
    if let Some(why) = path_problem(&path) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, why);
    }
    let bytes = match (
        call.body.get("content").and_then(Value::as_str),
        call.body.get("content_base64").and_then(Value::as_str),
    ) {
        (Some(t), None) => t.as_bytes().to_vec(),
        (None, Some(b)) => match B64.decode(b) {
            Ok(bytes) => bytes,
            Err(_) => return problem(StatusCode::UNPROCESSABLE_ENTITY, "content_base64: not Base64"),
        },
        _ => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "content (text) or content_base64",
            );
        }
    };
    if bytes.len() > MAX_FILE {
        return problem(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("a file of at most {MAX_FILE} bytes"),
        );
    }
    let content_type = kind_of(&path, text(&call.body, "content_type"));
    if (content_type.contains("csv") || content_type.contains("json"))
        && let Err(e) = rows_of(&bytes, &content_type)
    {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("the file cannot be read as rows: {e}"),
        );
    }
    let parts = match keep_bytes(state, &bytes).await {
        Ok(p) => p,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let key = file_key(&access.workspace.id, &path);
    let id = state.model.files.get(&key).await.map_or_else(new_id, |f| f.id);
    let sha = digest::digest(&digest::SHA256, &bytes);
    let record = FileRecord {
        schema: SCHEMA,
        id,
        workspace: access.workspace.id,
        path,
        content_type,
        size: bytes.len() as u64,
        sha256: sha.as_ref().iter().map(|b| format!("{b:02x}")).collect(),
        parts,
        updated: now(),
        by: access.user.id,
        deleted: None,
    };
    match state.model.files.put(&key, &record).await {
        Ok(()) => json(StatusCode::CREATED, &shown(&record)),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// Writes the bytes in parts; answers each part's offset.
async fn keep_bytes(state: &State, bytes: &[u8]) -> Result<Vec<i64>, String> {
    let chunks: Vec<Vec<u8>> = bytes.chunks(PART).map(<[u8]>::to_vec).collect();
    state
        .with_client(|client| {
            Box::pin(async move {
                ensure_topic(client).await?;
                let mut offsets = Vec::new();
                for chunk in chunks {
                    let message = Outgoing {
                        key: None,
                        value: Some(Bytes::from(chunk)),
                        headers: Vec::new(),
                    };
                    let at = client
                        .produce(
                            FILE_BODIES,
                            0,
                            &[message],
                            i64::try_from(now()).unwrap_or(0) * 1000,
                        )
                        .await
                        .map_err(|e| format!("cannot keep the file: {e}"))?;
                    offsets.push(at);
                }
                Ok(offsets)
            })
        })
        .await
}

async fn ensure_topic(client: &mut Routed) -> Result<(), String> {
    let exists = client
        .topics()
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .any(|t| t.name == FILE_BODIES);
    if !exists {
        match client.create_topic(FILE_BODIES, 1, None).await {
            Ok(()) | Err(openduct_broker::client::ClientError::Refused { code: 36, .. }) => {}
            Err(e) => return Err(format!("cannot make the topic of files: {e}")),
        }
    }
    Ok(())
}

/// The bytes of a file kept in parts at `offsets`.
async fn read_bytes(client: &mut Routed, offsets: &[i64]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for &offset in offsets {
        let fetched = client
            .fetch(
                FILE_BODIES,
                &[(0, offset)],
                0,
                i32::try_from(PART * 2).unwrap_or(i32::MAX),
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut found = None;
        for answer in fetched {
            let mut batches = answer.batches;
            while !batches.is_empty() && found.is_none() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                found = set
                    .records
                    .into_iter()
                    .find(|r| r.offset == offset)
                    .and_then(|r| r.value);
            }
        }
        let part = found.ok_or_else(|| "a part of the file is missing".to_owned())?;
        out.extend_from_slice(&part);
    }
    Ok(out)
}

/// A file of the asker's workspace by the path the request names; a 404 for any other.
async fn found(state: &State, call: &Call, need: Role) -> Result<FileRecord, Answer> {
    let access = access(state, call, need).await?;
    let path = query_path(call);
    state
        .model
        .files
        .get(&file_key(&access.workspace.id, &path))
        .await
        .filter(|f| f.deleted.is_none())
        .ok_or_else(|| problem(StatusCode::NOT_FOUND, "no such file"))
}

async fn bytes_of(state: &State, file: &FileRecord) -> Result<Vec<u8>, String> {
    let parts = file.parts.clone();
    state
        .with_client(|client| Box::pin(async move { read_bytes(client, &parts).await }))
        .await
}

async fn download(state: &State, call: &Call) -> Answer {
    let file = match found(state, call, Role::Viewer).await {
        Ok(f) => f,
        Err(answer) => return answer,
    };
    match bytes_of(state, &file).await {
        Ok(bytes) => {
            let mut response = Response::new(Full::new(Bytes::from(bytes)));
            if let Ok(v) = HeaderValue::from_str(&file.content_type) {
                response.headers_mut().insert(header::CONTENT_TYPE, v);
            }
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn preview(state: &State, call: &Call) -> Answer {
    let file = match found(state, call, Role::Viewer).await {
        Ok(f) => f,
        Err(answer) => return answer,
    };
    let bytes = match bytes_of(state, &file).await {
        Ok(b) => b,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    match rows_of(&bytes, &file.content_type) {
        Ok(rows) => {
            let mut columns: Vec<String> = Vec::new();
            for row in rows.iter().take(PREVIEW_ROWS) {
                for k in row.keys() {
                    if !columns.contains(k) {
                        columns.push(k.clone());
                    }
                }
            }
            let first: Vec<Value> = rows
                .iter()
                .take(PREVIEW_ROWS)
                .cloned()
                .map(Value::Object)
                .collect();
            json(
                StatusCode::OK,
                &json!({ "rows": rows.len(), "columns": columns, "first": first }),
            )
        }
        Err(e) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("the file cannot be read as rows: {e}"),
        ),
    }
}

async fn remove(state: &State, call: &Call) -> Answer {
    let mut file = match found(state, call, Role::Editor).await {
        Ok(f) => f,
        Err(answer) => return answer,
    };
    file.deleted = Some(now());
    file.updated = now();
    match state
        .model
        .files
        .put(&file_key(&file.workspace, &file.path), &file)
        .await
    {
        Ok(()) => json(StatusCode::OK, &json!({ "deleted": file.path })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// A file's rows: CSV (the first row names the columns), JSON (a list of records) or JSON lines.
pub(crate) fn rows_of(bytes: &[u8], content_type: &str) -> Result<Vec<Map<String, Value>>, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "not UTF-8 text")?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let rows: Vec<Map<String, Value>> = if content_type.contains("csv") {
        csv(text)?
    } else if content_type.contains("jsonl") {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
            .map(|(i, l)| match serde_json::from_str::<Value>(l) {
                Ok(Value::Object(o)) => Ok(o),
                _ => Err(format!("line {} is not a JSON record", i + 1)),
            })
            .collect::<Result<_, _>>()?
    } else {
        match serde_json::from_str::<Value>(text).map_err(|e| format!("not JSON: {e}"))? {
            Value::Array(items) => items
                .into_iter()
                .enumerate()
                .map(|(i, v)| match v {
                    Value::Object(o) => Ok(o),
                    _ => Err(format!("item {i} is not a record")),
                })
                .collect::<Result<_, _>>()?,
            _ => return Err("a list of records".to_owned()),
        }
    };
    if rows.len() > MAX_ROWS {
        return Err(format!("more than {MAX_ROWS} rows"));
    }
    Ok(rows)
}

/// CSV as RFC 4180 has it: fields in double quotes may hold commas, quotes (doubled) and new lines.
fn csv(text: &str) -> Result<Vec<Map<String, Value>>, String> {
    let mut records: Vec<Vec<String>> = Vec::new();
    let (mut record, mut field, mut quoted, mut chars) =
        (Vec::new(), String::new(), false, text.chars().peekable());
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            (true, '"') => quoted = false,
            (true, other) => field.push(other),
            (false, '"') if field.is_empty() => quoted = true,
            (false, ',') => record.push(std::mem::take(&mut field)),
            (false, '\r') => {}
            (false, '\n') => {
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
            }
            (false, other) => field.push(other),
        }
        if records.len() > MAX_ROWS + 1 {
            return Err(format!("more than {MAX_ROWS} rows"));
        }
    }
    if quoted {
        return Err("a quoted field never ends".to_owned());
    }
    if !field.is_empty() || !record.is_empty() {
        record.push(field);
        records.push(record);
    }
    let mut lines = records
        .into_iter()
        .filter(|r| !(r.len() == 1 && r.first().is_some_and(String::is_empty)));
    let header = lines
        .next()
        .ok_or("an empty file: the first row names the columns")?;
    Ok(lines
        .map(|r| {
            header
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    (
                        name.trim().to_owned(),
                        Value::String(r.get(i).cloned().unwrap_or_default()),
                    )
                })
                .collect()
        })
        .collect())
}

/// A workspace's files, as its runs read them: rows kept in memory once read, until the file changes.
#[derive(Debug)]
pub(crate) struct Files {
    pub(crate) model: Arc<Model>,
    pub(crate) broker: String,
    pub(crate) workspace: String,
    client: Mutex<Option<Routed>>,
    read: Mutex<HashMap<String, (String, Data)>>,
}

impl Files {
    pub(crate) fn new(model: Arc<Model>, broker: &str, workspace: &str) -> Self {
        Self {
            model,
            broker: broker.to_owned(),
            workspace: workspace.to_owned(),
            client: Mutex::new(None),
            read: Mutex::new(HashMap::new()),
        }
    }

    async fn rows(&self, path: &str) -> Result<Data, String> {
        let file = self
            .model
            .files
            .get(&file_key(&self.workspace, path))
            .await
            .filter(|f| f.deleted.is_none())
            .ok_or_else(|| format!("there is no file {path}: add it under Files"))?;
        if let Some((sha, rows)) = self.read.lock().await.get(path)
            && *sha == file.sha256
        {
            return Ok(rows.clone());
        }
        let mut held = self.client.lock().await;
        if held.is_none() {
            let client = Routed::connect(&self.broker, Duration::from_secs(10))
                .await
                .map_err(|e| format!("cannot reach the broker: {e}"))?;
            *held = Some(client);
        }
        let client = held.as_mut().ok_or("no connection to the broker")?;
        let bytes = match read_bytes(client, &file.parts).await {
            Ok(b) => b,
            Err(e) => {
                *held = None;
                return Err(e);
            }
        };
        drop(held);
        let rows = rows_of(&bytes, &file.content_type)?;
        let value = Data::from_json(&Value::Array(rows.into_iter().map(Value::Object).collect()));
        let mut read = self.read.lock().await;
        if read.len() > 32 {
            read.clear();
        }
        read.insert(path.to_owned(), (file.sha256, value.clone()));
        Ok(value)
    }
}

/// Lookup files in a folder of this machine, read as the studio reads its own (`/rates.csv` is `<dir>/rates.csv`): for
/// `openduct workflow test --files-dir`.
#[derive(Debug)]
pub(crate) struct LocalFiles {
    pub(crate) dir: std::path::PathBuf,
}

impl openduct_flow::Files for LocalFiles {
    fn rows<'a>(
        &'a self,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Data, String>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(why) = path_problem(path) {
                return Err(format!("{path}: {why}"));
            }
            let file = self.dir.join(path.trim_start_matches('/'));
            let bytes = std::fs::read(&file)
                .map_err(|e| format!("there is no file {path} ({}): {e}", file.display()))?;
            let kind = if path.ends_with(".csv") {
                "text/csv"
            } else {
                "application/json"
            };
            let rows = rows_of(&bytes, kind)?;
            Ok(Data::from_json(&Value::Array(
                rows.into_iter().map(Value::Object).collect(),
            )))
        })
    }
}

impl openduct_flow::Files for Files {
    fn rows<'a>(
        &'a self,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Data, String>> + Send + 'a>> {
        Box::pin(Files::rows(self, path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_reads_as_rfc_4180_has_it() {
        let rows = csv("currency,rate,note\r\nEUR,1.08,\"plain\"\nGBP,1.27,\"with, a comma\"\nJPY,0.0067,\"said \"\"yen\"\"\"\n\n").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1]["note"], "with, a comma");
        assert_eq!(rows[2]["note"], r#"said "yen""#);
        assert_eq!(rows[0]["rate"], "1.08");
        assert!(csv("a,b\n\"never ends").is_err());
        assert!(csv("").is_err());
    }

    #[test]
    fn json_and_json_lines_read_as_rows_and_paths_are_checked() {
        assert_eq!(
            rows_of(br#"[{"a": 1}, {"a": 2}]"#, "application/json")
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            rows_of(b"{\"a\": 1}\n{\"a\": 2}\n", "application/jsonl")
                .unwrap()
                .len(),
            2
        );
        assert!(rows_of(br#"{"a": 1}"#, "application/json").is_err());
        assert!(path_problem("/rates.csv").is_none() && path_problem("/lists/price list.json").is_none());
        for bad in ["rates.csv", "/../x", "/a//b", "/a/./b", "/a;b"] {
            assert!(path_problem(bad).is_some(), "{bad}");
        }
        assert_eq!(kind_of("/x.csv", ""), "text/csv");
    }
}
