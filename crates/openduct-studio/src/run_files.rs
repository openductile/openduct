// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The files a workspace's runs make and are sent - a download, a sheet made of rows, a webhook form's upload - kept
//! in the log: a topic of the workspace's own, with the same retention as the runs' history, so a file goes when the
//! runs that made it go and any node of a cluster reads it. A file is written in parts of 512 KiB, in one batch: its
//! place is its first part's offset and how many parts it has. A run's record names its files; members of the
//! workspace download them from it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Response, StatusCode};
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{Outgoing, Shared, records};
use openduct_flow::files::{FileStore, MAX_FILE, multipart};
use serde_json::{Map, Value};

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, problem};
use crate::model::Role;

/// The size of a part of a file's bytes.
const PART: usize = 512 * 1024;
/// The most files one webhook delivery brings.
pub(crate) const MAX_UPLOADS: usize = 10;
/// The largest webhook delivery that brings files: one file of the largest size, and its form.
pub(crate) const MAX_UPLOAD: usize = MAX_FILE + 1024 * 1024;

/// The topic a workspace's run files are kept in.
fn topic_of(workspace: &str) -> String {
    format!("__openduct.run-files.{workspace}")
}

/// A workspace's run files, in its topic.
#[derive(Debug)]
pub(crate) struct Store {
    shared: Arc<Shared>,
    topic: String,
    ready: AtomicBool,
}

impl Store {
    pub(crate) fn new(shared: Arc<Shared>, workspace: &str) -> Self {
        Self {
            shared,
            topic: topic_of(workspace),
            ready: AtomicBool::new(false),
        }
    }

    async fn keep_bytes(&self, bytes: &[u8]) -> Result<String, String> {
        if bytes.len() > MAX_FILE {
            return Err(format!("a file of more than {MAX_FILE} bytes"));
        }
        let mut client = self
            .shared
            .lend()
            .await
            .map_err(|e| format!("cannot reach the broker: {e}"))?;
        if !self.ready.load(Ordering::Relaxed) {
            let history = i64::try_from(openduct_flow::runner::HISTORY.as_millis()).unwrap_or(i64::MAX);
            match client.create_topic(&self.topic, 1, Some(history)).await {
                Ok(()) | Err(openduct_broker::client::ClientError::Refused { code: 36, .. }) => {}
                Err(e) => return Err(format!("cannot make the topic of run files: {e}")),
            }
            self.ready.store(true, Ordering::Relaxed);
        }
        let parts: Vec<Outgoing> = if bytes.is_empty() {
            vec![Outgoing {
                key: None,
                value: Some(Bytes::new()),
                headers: Vec::new(),
            }]
        } else {
            bytes
                .chunks(PART)
                .map(|chunk| Outgoing {
                    key: None,
                    value: Some(Bytes::copy_from_slice(chunk)),
                    headers: Vec::new(),
                })
                .collect()
        };
        let count = parts.len();
        let first = client
            .produce(&self.topic, 0, &parts, crate::workflows::now_ms())
            .await
            .map_err(|e| format!("cannot keep the file: {e}"))?;
        Ok(format!("{first}.{count}"))
    }

    async fn load_bytes(&self, place: &str) -> Result<Vec<u8>, String> {
        let (first, count) = place
            .split_once('.')
            .and_then(|(f, c)| Some((f.parse::<i64>().ok()?, c.parse::<usize>().ok()?)))
            .filter(|(f, c)| *f >= 0 && (1..=MAX_FILE / PART + 1).contains(c))
            .ok_or("not a place a file is kept")?;
        let end = first + i64::try_from(count).unwrap_or(0);
        let mut parts: Vec<Option<Bytes>> = vec![None; count];
        let mut client = self
            .shared
            .lend()
            .await
            .map_err(|e| format!("cannot reach the broker: {e}"))?;
        let mut next = first;
        while next < end {
            let most = i32::try_from(PART * count + 64 * 1024).unwrap_or(i32::MAX);
            let fetched = client
                .fetch(&self.topic, &[(0, next)], 0, most)
                .await
                .map_err(|e| format!("cannot read the file: {e}"))?;
            let Some(answer) = fetched.into_iter().next() else {
                break;
            };
            if answer.error_code != 0 {
                return Err(
                    "it is no longer kept: files are kept as long as the runs' history, 14 days".to_owned(),
                );
            }
            let before = next;
            let mut batches = answer.batches;
            while !batches.is_empty() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                for record in set.records {
                    if record.offset >= next && record.offset < end {
                        let at = usize::try_from(record.offset - first).unwrap_or(usize::MAX);
                        if let Some(slot) = parts.get_mut(at) {
                            *slot = Some(record.value.unwrap_or_default());
                        }
                        next = record.offset + 1;
                    }
                }
            }
            if next == before {
                break;
            }
        }
        let mut out = Vec::new();
        for part in parts {
            out.extend_from_slice(&part.ok_or("a part of it is missing")?);
            if out.len() > MAX_FILE {
                return Err("it is larger than a file may be".to_owned());
            }
        }
        Ok(out)
    }
}

type Pending<'a, T> = std::pin::Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

impl FileStore for Store {
    fn keep<'a>(&'a self, bytes: Arc<Vec<u8>>) -> Pending<'a, String> {
        Box::pin(async move { self.keep_bytes(&bytes).await })
    }

    fn load<'a>(&'a self, place: &'a str) -> Pending<'a, Vec<u8>> {
        Box::pin(self.load_bytes(place))
    }
}

/// A webhook form's files kept, each its field's value in `fields` (a field with several, a list of them): what the
/// run is given is the files, by reference, not their bytes.
pub(crate) async fn uploads(
    state: &State,
    workspace: &str,
    content_type: &str,
    raw: &[u8],
    fields: &mut Map<String, Value>,
) -> Result<(), String> {
    let parts = multipart::parse(content_type, raw)?;
    // A file field left empty (a browser sends it with no name and no bytes) brings no file.
    let files: Vec<&multipart::Part> = parts
        .iter()
        .filter(|p| {
            p.file_name
                .as_ref()
                .is_some_and(|n| !n.is_empty() || !p.bytes.is_empty())
        })
        .collect();
    if files.len() > MAX_UPLOADS {
        return Err(format!("a delivery of at most {MAX_UPLOADS} files"));
    }
    let store = Store::new(Arc::clone(&state.shared), workspace);
    let mut kept: Vec<(String, Value)> = Vec::new();
    for part in files {
        let name = part.file_name.clone().unwrap_or_default();
        let media_type = part
            .media_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let place = store.keep_bytes(&part.bytes).await?;
        let value = openduct_flow::files::value_of(&place, &name, &media_type, &part.bytes);
        kept.push((
            part.name.clone(),
            serde_json::from_str(&value.to_json()).unwrap_or(Value::Null),
        ));
    }
    let mut grouped: Vec<(String, Vec<Value>)> = Vec::new();
    for (name, value) in kept {
        match grouped.iter_mut().find(|(n, _)| *n == name) {
            Some((_, list)) => list.push(value),
            None => grouped.push((name, vec![value])),
        }
    }
    for (name, mut list) in grouped {
        let value = if list.len() == 1 {
            list.pop().unwrap_or(Value::Null)
        } else {
            Value::Array(list)
        };
        fields.insert(name, value);
    }
    Ok(())
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    match (&call.method, call.path.as_str()) {
        (&Method::GET, "/api/runs/file") => Some(download(state, call).await),
        _ => None,
    }
}

/// A run's file, for a member of its workspace: `?file=<its reference>&name=<its name>`. It is always a download,
/// never shown in the studio's page, whatever it holds.
async fn download(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let asked = crate::hooks::form(&call.query);
    let reference = asked.get("file").and_then(Value::as_str).unwrap_or_default();
    let Some((Some(place), sha256)) = openduct_flow::files::parse_reference(reference) else {
        return problem(
            StatusCode::NOT_FOUND,
            "no such file: a test run's files are not kept, so they cannot be downloaded",
        );
    };
    let store = Store::new(Arc::clone(&state.shared), &access.workspace.id);
    let bytes = match store.load_bytes(&place).await {
        Ok(b) if openduct_flow::files::matches(&b, &sha256) => b,
        Ok(_) => return problem(StatusCode::NOT_FOUND, "no such file"),
        Err(e) => return problem(StatusCode::NOT_FOUND, &format!("no such file: {e}")),
    };
    let name = asked.get("name").and_then(Value::as_str).unwrap_or("file");
    let plain: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || " .-_()".contains(c) {
                c
            } else {
                '_'
            }
        })
        .take(200)
        .collect();
    let encoded: String = name
        .bytes()
        .take(600)
        .map(|b| {
            if b.is_ascii_alphanumeric() || b".-_".contains(&b) {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    let mut response = Response::new(Full::new(Bytes::from(bytes)));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(v) = HeaderValue::from_str(&format!(
        "attachment; filename=\"{plain}\"; filename*=UTF-8''{encoded}"
    )) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    response
}
