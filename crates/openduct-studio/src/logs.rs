// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The Logs page: OpenDuct's own log, read back from its files (OpenTelemetry records, OTLP JSON Lines, a file a
//! day) - the newest records first found, filtered by level, by text and by time, given oldest first. It covers
//! every workspace, so only the installation's admin reads it. In a cluster, each node keeps its own.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use hyper::StatusCode;
use serde_json::{Value, json};

use crate::State;
use crate::api::signed_in;
use crate::hooks::form;
use crate::http::{Answer, Call, json, problem};

/// The most of a file read back, from its end.
const TAIL_BYTES: u64 = 8 * 1024 * 1024;
/// The most records given at once.
const MAX_LINES: usize = 2000;
/// The files read back at most: today's and the days before it.
const MAX_FILES: usize = 3;

pub(crate) async fn list(state: &State, call: &Call) -> Answer {
    let user = match signed_in(state, call).await {
        Ok((user, _)) => user,
        Err(answer) => return answer,
    };
    if !user.admin {
        return problem(
            StatusCode::FORBIDDEN,
            "only the installation's admin reads its log",
        );
    }
    let Some(dir) = state.log_dir.clone() else {
        return json(
            StatusCode::OK,
            &json!({ "lines": [], "kept": false, "why": "OpenDuct keeps no log file here: it runs in memory, or with --log-dir none" }),
        );
    };
    let asked = form(&call.query);
    let level = asked.get("level").and_then(Value::as_str).map_or(0, severity_of);
    // What was typed, as one phrase: found as it is, whatever its case.
    let needle = asked
        .get("q")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    let time = |name: &str| -> u128 {
        asked
            .get(name)
            .and_then(Value::as_str)
            .and_then(|a| a.parse().ok())
            .unwrap_or(0)
    };
    // A page: the records after one time (following the log) or before one (an older page).
    let (after, before) = (time("after"), time("before"));
    let limit = asked
        .get("limit")
        .and_then(Value::as_str)
        .and_then(|l| l.parse().ok())
        .unwrap_or(300)
        .clamp(1, MAX_LINES);
    let shown_dir = dir.display().to_string();
    // One more than asked for, to know whether there is an older page.
    let mut found = tokio::task::spawn_blocking(move || read(&dir, level, &needle, after, before, limit + 1))
        .await
        .unwrap_or_default();
    let older = found.len() > limit;
    if older {
        found.remove(0);
    }
    json(
        StatusCode::OK,
        &json!({ "lines": found, "kept": true, "dir": shown_dir, "older": older }),
    )
}

/// How many error records the log holds from `since` (nanoseconds since 1970) on - in the newest files read back.
pub(crate) fn count_errors(dir: &Path, since: u128) -> usize {
    read(dir, 17, "", since, 0, MAX_LINES).len()
}

/// The severity number a level name stands for (OpenTelemetry's: 1 trace, 5 debug, 9 info, 13 warn, 17 error).
fn severity_of(level: &str) -> u64 {
    match level {
        "debug" => 5,
        "info" => 9,
        "warn" => 13,
        "error" => 17,
        _ => 0,
    }
}

fn files(dir: &Path) -> Vec<PathBuf> {
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

/// The last [`TAIL_BYTES`] of a file, from the start of a whole line.
fn tail(path: &Path) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return String::new();
    };
    let size = file.metadata().map(|m| m.len()).unwrap_or(0);
    let from = size.saturating_sub(TAIL_BYTES);
    if file.seek(SeekFrom::Start(from)).is_err() {
        return String::new();
    }
    let mut bytes = Vec::new();
    let _ = file.read_to_end(&mut bytes);
    let text = String::from_utf8_lossy(&bytes).into_owned();
    if from > 0 {
        // Cut in the middle of a line: start at the next one.
        text.split_once('\n')
            .map(|(_, rest)| rest.to_owned())
            .unwrap_or_default()
    } else {
        text
    }
}

/// A value of an OTLP attribute, as plain JSON.
fn plain(value: &Value) -> Value {
    if let Some(s) = value.get("stringValue") {
        return s.clone();
    }
    if let Some(i) = value.get("intValue") {
        return i
            .as_str()
            .and_then(|t| t.parse::<i64>().ok())
            .map_or_else(|| i.clone(), Value::from);
    }
    value
        .get("boolValue")
        .or_else(|| value.get("doubleValue"))
        .cloned()
        .unwrap_or(Value::Null)
}

/// The value at a JSON pointer, or null.
pub(crate) fn at<'a>(v: &'a Value, pointer: &str) -> &'a Value {
    static NULL: Value = Value::Null;
    v.pointer(pointer).unwrap_or(&NULL)
}

/// One record of a line, as the page shows it.
fn record(line: &str) -> Option<(u128, u64, Value)> {
    let parsed: Value = serde_json::from_str(line).ok()?;
    let resource = at(&parsed, "/resourceLogs/0");
    let scope = at(resource, "/scopeLogs/0");
    let r = at(scope, "/logRecords/0");
    let time: u128 = at(r, "/timeUnixNano").as_str()?.parse().ok()?;
    let severity = at(r, "/severityNumber").as_u64().unwrap_or(0);
    let mut fields = serde_json::Map::new();
    for a in at(r, "/attributes").as_array().into_iter().flatten() {
        if let Some(key) = at(a, "/key").as_str() {
            fields.insert(key.to_owned(), plain(at(a, "/value")));
        }
    }
    let node = at(resource, "/resource/attributes")
        .as_array()
        .into_iter()
        .flatten()
        .find(|a| at(a, "/key") == "service.instance.id")
        .map(|a| plain(at(a, "/value")))
        .unwrap_or(Value::Null);
    let shown = json!({
        "at": time.to_string(),
        "ms": u64::try_from(time / 1_000_000).unwrap_or(0),
        "level": at(r, "/severityText").as_str().unwrap_or("INFO"),
        "target": at(scope, "/scope/name").as_str().unwrap_or_default(),
        "text": at(r, "/body/stringValue").as_str().unwrap_or_default(),
        "fields": fields,
        "node": node,
    });
    Some((time, severity, shown))
}

/// What a search looks through: the message, where it came from, and its fields as `name=value` - as the page shows
/// them, not as the file keeps them.
fn searched(shown: &Value) -> String {
    let mut text = format!(
        "{} {}",
        at(shown, "/text").as_str().unwrap_or_default(),
        at(shown, "/target").as_str().unwrap_or_default()
    );
    if let Some(fields) = shown.get("fields").and_then(Value::as_object) {
        for (k, v) in fields {
            let v = v.as_str().map_or_else(|| v.to_string(), str::to_owned);
            text.push_str(&format!(" {k}={v}"));
        }
    }
    text.to_lowercase()
}

fn read(dir: &Path, level: u64, needle: &str, after: u128, before: u128, limit: usize) -> Vec<Value> {
    let needle = needle.to_lowercase();
    let mut found: Vec<Value> = Vec::new();
    for path in files(dir).iter().rev().take(MAX_FILES) {
        let text = tail(path);
        for line in text.lines().rev() {
            let Some((at, severity, shown)) = record(line) else {
                continue;
            };
            if before > 0 && at >= before {
                continue;
            }
            if at <= after {
                found.reverse();
                return found;
            }
            if severity < level {
                continue;
            }
            if !needle.is_empty() && !searched(&shown).contains(&needle) {
                continue;
            }
            found.push(shown);
            if found.len() >= limit {
                found.reverse();
                return found;
            }
        }
    }
    found.reverse();
    found
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    fn line(at: u128, level: &str, number: u64, text: &str) -> String {
        json!({ "resourceLogs": [{
            "resource": { "attributes": [{ "key": "service.instance.id", "value": { "stringValue": "node-1" } }] },
            "scopeLogs": [{ "scope": { "name": "openduct_studio::manager" }, "logRecords": [{
                "timeUnixNano": at.to_string(), "severityNumber": number, "severityText": level,
                "body": { "stringValue": text },
                "attributes": [{ "key": "workflow", "value": { "stringValue": "w1" } }, { "key": "n", "value": { "intValue": "3" } }],
            }] }],
        }] })
        .to_string()
    }

    #[test]
    fn the_log_reads_back_newest_kept_oldest_first_by_level_text_and_time() {
        let dir = tempfile::tempdir().unwrap();
        let lines = [
            line(1_000_000_000, "INFO", 9, "workflow running"),
            line(2_000_000_000, "WARN", 13, "cannot reach the broker"),
            line(3_000_000_000, "INFO", 9, "workflow stopped"),
        ];
        std::fs::write(
            dir.path().join("openduct-2026-11-16.jsonl"),
            lines.join("\n") + "\n",
        )
        .unwrap();
        let all = read(dir.path(), 0, "", 0, 0, 10);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0]["text"], "workflow running");
        assert_eq!(all[0]["fields"]["n"], 3);
        assert_eq!(all[0]["node"], "node-1");
        assert_eq!(all[0]["ms"], 1000);
        let warnings = read(dir.path(), 13, "", 0, 0, 10);
        assert_eq!(warnings.len(), 1);
        let found = read(dir.path(), 0, "stopped", 0, 0, 10);
        assert_eq!(found.len(), 1);
        // A phrase is found as written, case aside - not its words anywhere in a line.
        assert_eq!(read(dir.path(), 0, "workflow stopped", 0, 0, 10).len(), 1);
        assert_eq!(read(dir.path(), 0, "stopped workflow", 0, 0, 10).len(), 0);
        assert_eq!(read(dir.path(), 0, "WORKFLOW=W1", 0, 0, 10).len(), 3);
        assert_eq!(found.len(), 1);
        let newer = read(dir.path(), 0, "", 1_000_000_000, 0, 10);
        // An older page: before the second record, only the first.
        let older = read(dir.path(), 0, "", 0, 2_000_000_000, 10);
        assert_eq!(older.len(), 1);
        assert_eq!(older[0]["text"], "workflow running");
        assert_eq!(newer.len(), 2);
        let last = read(dir.path(), 0, "", 0, 0, 1);
        assert_eq!(last[0]["text"], "workflow stopped");
    }
}
