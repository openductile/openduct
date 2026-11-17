// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct's own log, kept on disk as OpenTelemetry log records: OTLP JSON Lines, each line one
//! `ExportLogsServiceRequest` holding one record - the form OpenTelemetry's file exporter writes and the Collector's
//! `otlpjsonfile` receiver reads. A file a day (UTC), `openduct-YYYY-MM-DD.jsonl`, in the log directory; the oldest
//! go once more than the days kept are there. A thread of its own writes them, so logging never waits on the disk:
//! when it falls more than [`QUEUE`] records behind, records are dropped (and the next one written says how many).

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value as Json, json};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// The most records waiting for the disk.
const QUEUE: usize = 10_000;

/// Where and how long OpenDuct's log is kept.
#[derive(Debug, Clone)]
pub(crate) struct LogFiles {
    pub dir: PathBuf,
    pub keep_days: u32,
    /// This node, as `service.instance.id`.
    pub instance: String,
}

/// The layer that hands each record to the writer.
pub(crate) struct OtlpFile {
    queue: SyncSender<String>,
    dropped: Arc<AtomicU64>,
    resource: Arc<Json>,
}

/// The directory being written, once logging is set up: the studio reads it back.
static DIR: OnceLock<PathBuf> = OnceLock::new();

/// The log directory in use, if any.
pub(crate) fn dir() -> Option<PathBuf> {
    DIR.get().cloned()
}

impl OtlpFile {
    /// Starts the writer; `None` (and a word on stderr) when the directory cannot be made.
    pub(crate) fn start(files: &LogFiles) -> Option<Self> {
        if let Err(e) = std::fs::create_dir_all(&files.dir) {
            eprintln!("openduct: cannot keep the log in {}: {e}", files.dir.display());
            return None;
        }
        let _ = DIR.set(files.dir.clone());
        let (queue, waiting) = sync_channel(QUEUE);
        let dropped = Arc::new(AtomicU64::new(0));
        let (dir, keep) = (files.dir.clone(), files.keep_days);
        let lost = Arc::clone(&dropped);
        let spawned = std::thread::Builder::new()
            .name("openduct-log".to_owned())
            .spawn(move || write_all(&dir, keep, &waiting, &lost));
        if let Err(e) = spawned {
            eprintln!("openduct: cannot start the log writer: {e}");
            return None;
        }
        let resource = json!({ "attributes": [
            attribute("service.name", &json!({ "stringValue": "openduct" })),
            attribute("service.version", &json!({ "stringValue": env!("CARGO_PKG_VERSION") })),
            attribute("service.instance.id", &json!({ "stringValue": files.instance })),
        ] });
        Some(Self {
            queue,
            dropped,
            resource: Arc::new(resource),
        })
    }
}

fn attribute(key: &str, value: &Json) -> Json {
    json!({ "key": key, "value": value })
}

/// An event's message and its other fields, each as an OTLP value.
#[derive(Default)]
struct Fields {
    message: String,
    attributes: Vec<Json>,
}

impl Fields {
    fn put(&mut self, field: &Field, value: Json) {
        self.attributes.push(attribute(field.name(), &value));
    }
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        } else {
            self.put(field, json!({ "stringValue": value }));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}");
        if field.name() == "message" {
            self.message = text;
        } else {
            self.put(field, json!({ "stringValue": text }));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        // OTLP's JSON writes 64-bit integers as text.
        self.put(field, json!({ "intValue": value.to_string() }));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, json!({ "intValue": value.to_string() }));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, json!({ "boolValue": value }));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, json!({ "doubleValue": value }));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, json!({ "stringValue": value.to_string() }));
    }
}

/// OpenTelemetry's severity number and text for a level.
fn severity(level: &Level) -> (u8, &'static str) {
    match *level {
        Level::TRACE => (1, "TRACE"),
        Level::DEBUG => (5, "DEBUG"),
        Level::INFO => (9, "INFO"),
        Level::WARN => (13, "WARN"),
        Level::ERROR => (17, "ERROR"),
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

impl<S: Subscriber> Layer<S> for OtlpFile {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let lost = self.dropped.swap(0, Ordering::Relaxed);
        if lost > 0 {
            fields.attributes.push(attribute(
                "openduct.log.dropped",
                &json!({ "intValue": lost.to_string() }),
            ));
        }
        let meta = event.metadata();
        let (number, text) = severity(meta.level());
        let at = now_nanos().to_string();
        let line = json!({ "resourceLogs": [{
            "resource": *self.resource,
            "scopeLogs": [{
                "scope": { "name": meta.target() },
                "logRecords": [{
                    "timeUnixNano": at,
                    "observedTimeUnixNano": at,
                    "severityNumber": number,
                    "severityText": text,
                    "body": { "stringValue": fields.message },
                    "attributes": fields.attributes,
                }],
            }],
        }] });
        match self.queue.try_send(line.to_string()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

/// The civil date (UTC) of a day counted from 1970-01-01.
fn civil(days: i64) -> (i64, u32, u32) {
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let month = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Today's file name (UTC).
fn name_for(nanos: u128) -> String {
    let days = i64::try_from(nanos / 86_400_000_000_000).unwrap_or(0);
    let (y, m, d) = civil(days);
    format!("openduct-{y:04}-{m:02}-{d:02}.jsonl")
}

/// The log's files, oldest first.
pub(crate) fn files_in(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("openduct-") && n.ends_with(".jsonl"))
                })
                .collect()
        })
        .unwrap_or_default();
    found.sort();
    found
}

fn prune(dir: &Path, keep: u32) {
    let files = files_in(dir);
    let keep = usize::try_from(keep).unwrap_or(usize::MAX);
    if files.len() > keep {
        for old in files.iter().take(files.len() - keep) {
            let _ = std::fs::remove_file(old);
        }
    }
}

fn open(path: &Path) -> Option<BufWriter<File>> {
    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(f) => Some(BufWriter::new(f)),
        Err(e) => {
            // Not through tracing: that would come back here.
            eprintln!("openduct: cannot write the log to {}: {e}", path.display());
            None
        }
    }
}

fn write_all(dir: &Path, keep: u32, waiting: &Receiver<String>, dropped: &AtomicU64) {
    let mut name = name_for(now_nanos());
    prune(dir, keep);
    let mut out = open(&dir.join(&name));
    while let Ok(first) = waiting.recv() {
        let today = name_for(now_nanos());
        if today != name {
            if let Some(w) = out.as_mut() {
                let _ = w.flush();
            }
            name = today;
            out = open(&dir.join(&name));
            prune(dir, keep);
        }
        let Some(w) = out.as_mut() else {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let mut line = Some(first);
        while let Some(text) = line {
            if w.write_all(text.as_bytes())
                .and_then(|()| w.write_all(b"\n"))
                .is_err()
            {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
            line = waiting.try_recv().ok();
        }
        // Nothing more waiting: what was written is on disk for a reader now.
        let _ = w.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_are_civil_dates() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(20_773), (2026, 11, 16));
        assert_eq!(civil(11_016), (2000, 2, 29));
        assert_eq!(
            name_for(20_773 * 86_400_000_000_000 + 5),
            "openduct-2026-11-16.jsonl"
        );
    }
}
