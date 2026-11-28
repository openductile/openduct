// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The storage triggers: `storage` - a run for each new (or changed) file in a folder of this machine, on an SFTP or
//! FTP server, or in S3, the folder polled - and `redis`, a run for each message of a channel.
//!
//! A folder's polls are a schedule's times: each queued by every node and claimed by the one that reads the schedule's
//! partition, which lists the folder and writes each new file's run together with what it saw, in one batch - both
//! kept or neither (see [`crate::runner`]). So a cluster lists a folder once each time, and a restart neither runs a
//! file twice nor passes one by. The first poll notes what is there and runs nothing (unless `start: existing`), so a
//! trigger starts from now. What it saw is each file's path and its version (its size and time, or its ETag); with
//! `when: new_or_changed`, a file whose version changes runs again.

use std::collections::HashMap;
use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Map, Value as Json};

use super::paths::{Pattern, Rel};
use super::{Action, Asked, Entry, Place, remote};
use crate::app_files::poll::{MOST_SEEN, Seen};
use crate::run::Effects;

/// How often a folder is looked at, by default and at the least.
const EVERY: Duration = Duration::from_secs(300);
const LEAST: Duration = Duration::from_secs(10);
/// The most files one poll runs, by default and at most.
const POLL_ITEMS: i64 = 50;
const MOST_POLL_ITEMS: i64 = 500;
/// A path longer than this is kept as its hash in what was seen.
const LONGEST_ID: usize = 128;

/// What starts a workflow: a folder's new files, or a channel's messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Watch {
    Files(FilesWatch),
    Channel(ChannelWatch),
}

/// New files in a folder, polled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesWatch {
    pub place: Place,
    pub folder: String,
    pub pattern: Option<Pattern>,
    pub recursive: bool,
    pub every: Duration,
    /// A file whose version changes runs again.
    pub changed: bool,
    /// The first poll runs the files already there.
    pub existing: bool,
    pub max_items: usize,
    pub bucket: Option<String>,
    /// The folder paths are inside, by its id (Dropbox, Box, OneDrive, Google Drive). // storage places
    pub folder_id: Option<String>,
    /// Which drive (OneDrive and SharePoint). // storage places
    pub drive: Option<String>,
}

/// A Redis channel's messages (a pattern, with `*`, `?` or `[`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelWatch {
    pub credential: String,
    pub channel: String,
}

impl Watch {
    /// The trigger's kind, as the studio names it.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Files(_) => "storage",
            Self::Channel(_) => "redis",
        }
    }

    /// How often it polls; none for a channel.
    pub fn every(&self) -> Option<Duration> {
        match self {
            Self::Files(f) => Some(f.every),
            Self::Channel(_) => None,
        }
    }

    /// The channel it listens to, when it does.
    pub fn channel(&self) -> Option<&ChannelWatch> {
        match self {
            Self::Channel(c) => Some(c),
            Self::Files(_) => None,
        }
    }
}

fn only(s: &Map<String, Json>, kind: &str, allowed: &[&str]) -> Result<(), String> {
    match s.keys().find(|k| !allowed.contains(&k.as_str())) {
        Some(k) => Err(format!(
            "{kind}.{k}: not a setting of {kind} ({})",
            allowed.join(", ")
        )),
        None => Ok(()),
    }
}

/// A workflow's trigger, from a storage trigger's settings.
pub(crate) fn trigger(kind: &str, s: &Map<String, Json>) -> Result<crate::Trigger, String> {
    Ok(crate::Trigger::Storage(Box::new(parse(kind, s)?)))
}

/// A storage trigger's settings, read: `storage: {...}` or `redis: {...}`.
pub(crate) fn parse(kind: &str, s: &Map<String, Json>) -> Result<Watch, String> {
    let text = |name: &str| s.get(name).and_then(Json::as_str);
    if kind == "redis" {
        only(s, kind, &["credential", "channel"])?;
        let credential = text("credential")
            .filter(|c| !c.trim().is_empty())
            .ok_or("redis.credential: the redis credential")?;
        let channel = text("channel")
            .filter(|c| !c.is_empty() && c.len() <= 256)
            .ok_or("redis.channel: the channel to listen to (or a pattern, as orders.*)")?;
        return Ok(Watch::Channel(ChannelWatch {
            credential: credential.to_owned(),
            channel: channel.to_owned(),
        }));
    }
    only(
        s,
        kind,
        &[
            "root",
            "credential",
            "folder",
            "pattern",
            "recursive",
            "every",
            "when",
            "start",
            "max_items",
            "bucket",
            "folder_id", // storage places
            "drive",     // storage places
        ],
    )?;
    let place = super::place(s, kind)?;
    let folder = text("folder").unwrap_or_default().to_owned();
    Rel::parse(&folder, true).map_err(|e| format!("{kind}.folder: {e}"))?;
    let every = match text("every") {
        None => EVERY,
        Some(e) => crate::duration(e)
            .filter(|d| *d >= LEAST && *d <= Duration::from_secs(86_400))
            .ok_or_else(|| format!("{kind}.every: how often, as 1m or 1h - from 10s to 1d"))?,
    };
    let changed = match text("when") {
        None | Some("new") => false,
        Some("new_or_changed") => true,
        Some(other) => return Err(format!("{kind}.when: new or new_or_changed, not {other:?}")),
    };
    let existing = match text("start") {
        None | Some("now") => false,
        Some("existing") => true,
        Some(other) => return Err(format!("{kind}.start: now or existing, not {other:?}")),
    };
    let max_items = match s.get("max_items") {
        None | Some(Json::Null) => POLL_ITEMS,
        Some(n) => n
            .as_i64()
            .filter(|n| (1..=MOST_POLL_ITEMS).contains(n))
            .ok_or_else(|| format!("{kind}.max_items: 1 to {MOST_POLL_ITEMS}"))?,
    };
    let recursive = match s.get("recursive") {
        None | Some(Json::Null) => false,
        Some(Json::Bool(b)) => *b,
        Some(_) => return Err(format!("{kind}.recursive: true or false")),
    };
    Ok(Watch::Files(FilesWatch {
        place,
        folder,
        pattern: text("pattern")
            .map(|p| Pattern::parse(p).map_err(|e| format!("{kind}.pattern: {e}")))
            .transpose()?,
        recursive,
        every,
        changed,
        existing,
        max_items: usize::try_from(max_items).unwrap_or(50),
        bucket: text("bucket").map(str::to_owned),
        folder_id: text("folder_id").map(str::to_owned), // storage places
        drive: text("drive").map(str::to_owned),         // storage places
    }))
}

/// What a poll found: the files to run (each with its id), and what is seen now.
#[derive(Debug)]
pub(crate) struct Polled {
    pub runs: Vec<(String, Value)>,
    pub seen: Seen,
}

fn id_of(path: &str) -> String {
    if path.len() <= LONGEST_ID {
        path.to_owned()
    } else {
        let hash = ring::digest::digest(&ring::digest::SHA256, path.as_bytes());
        hash.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// The files to run from a folder's entries, given what the polls before saw.
pub(crate) fn compare(w: &FilesWatch, entries: Vec<Entry>, before: Option<&Seen>, slot: i64) -> Polled {
    let mut files: Vec<Entry> = entries
        .into_iter()
        .filter(|e| !e.folder && w.pattern.as_ref().is_none_or(|p| p.matches(&e.name)))
        .collect();
    files.sort_by(|a, b| a.modified.cmp(&b.modified).then_with(|| a.path.cmp(&b.path)));
    let first = before.is_none_or(|b| b.pending);
    let known: HashMap<&str, &str> = before
        .map(|b| b.items.iter().map(|(i, v)| (i.as_str(), v.as_str())).collect())
        .unwrap_or_default();
    let mut runs = Vec::new();
    let mut items = Vec::with_capacity(files.len());
    for e in &files {
        let id = id_of(&e.path);
        let was = known.get(id.as_str()).copied();
        let fresh = if first {
            w.existing
        } else {
            match was {
                None => true,
                Some(v) => w.changed && v != e.version,
            }
        };
        if fresh && runs.len() >= w.max_items {
            // Left for the next poll: kept as it was seen before, or not at all.
            if let Some(v) = was {
                items.push((id, v.to_owned()));
            }
            continue;
        }
        if fresh {
            let mut value = e.value();
            if let Value::Record(r) = &mut value {
                std::sync::Arc::make_mut(r).insert(
                    "change".to_owned(),
                    Value::text(if was.is_some() { "changed" } else { "new" }),
                );
            }
            runs.push((id.clone(), value));
        }
        items.push((id, e.version.clone()));
    }
    Polled {
        runs,
        seen: Seen {
            cursor: Json::Null,
            items,
            slot,
            failing: None,
            pending: false,
        },
    }
}

/// One poll of a folder, through a real run's effects: its entries listed, and compared with what was seen.
pub(crate) async fn poll(
    effects: &dyn Effects,
    w: &FilesWatch,
    before: Option<&Seen>,
    slot: i64,
) -> Result<Polled, String> {
    let places = effects
        .storage()
        .allows(false)
        .map_err(|_| "this run reaches no storage".to_owned())?;
    let within = super::cloud::Within {
        bucket: w.bucket.clone(),
        folder_id: w.folder_id.clone(),
        drive: w.drive.clone(),
        export: super::Export::default(),
    }; // storage places
    let mut session = match &w.place {
        Place::Root(root) => remote::Session::Local(places.roots.session(root)?),
        Place::Credential(name) => {
            let c = effects.credential(name)?;
            // storage places: Dropbox, OneDrive and Google Drive by their lists of changes; Azure, Google Cloud
            // Storage and Box by their folders' listings, below.
            if super::cloud::feeds::takes(&c.kind) {
                return super::cloud::feeds::poll(effects, w, name, &c, before, slot).await;
            }
            match c.kind.as_str() {
                "sftp" => remote::Session::Sftp(Box::new(
                    super::sftp::Session::open(effects, name, &c, places).await?,
                )),
                "ftp" => remote::Session::Ftp(Box::new(super::ftp::Session::open(&c, places).await?)),
                "s3" => remote::Session::S3(Box::new(super::s3::Session::new(
                    effects,
                    &c,
                    w.bucket.as_deref(),
                )?)),
                kind if super::cloud::takes(kind) => remote::Session::Cloud(Box::new(
                    super::cloud::Session::open(effects, name, &c, &within).await?,
                )), // storage places
                other => {
                    return Err(format!(
                        "{name} is a {other} credential: a storage trigger takes an sftp, ftp, s3, azure_blob, gcs, \
                         dropbox, box, microsoft_365 or google_drive one"
                    ));
                }
            }
        }
    };
    let asked = Asked {
        action: Action::List,
        path: w.folder.clone(),
        to: None,
        pattern: None,
        recursive: w.recursive,
        limit: MOST_SEEN,
        overwrite: false,
        expires: Duration::ZERO,
        method: "GET",
        bucket: w.bucket.clone(),
        folder_id: w.folder_id.clone(),   // storage places
        drive: w.drive.clone(),           // storage places
        export: super::Export::default(), // storage places
    };
    let listed = async {
        let dir = session.path(&asked.path)?;
        let (listed, more) = session.list(&dir, asked.recursive).await?;
        let files: Vec<&remote::Listed> = listed.iter().filter(|l| !l.folder).collect();
        if more || files.len() > MOST_SEEN {
            return Err(format!(
                "{} holds more than {MOST_SEEN} files: a trigger watches a folder of fewer - move the files it has run \
                 into another folder, or watch one below it",
                if w.folder.is_empty() { "the folder" } else { w.folder.as_str() }
            ));
        }
        Ok(listed.iter().map(|l| session.entry(l)).collect::<Vec<_>>())
    }
    .await;
    session.close().await;
    Ok(compare(w, listed?, before, slot))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, version: &str, modified: i64) -> Entry {
        Entry {
            name: path.rsplit('/').next().unwrap_or(path).to_owned(),
            path: path.to_owned(),
            folder: false,
            size: 1,
            modified: Some(modified),
            version: version.to_owned(),
        }
    }

    fn watch(changed: bool, existing: bool, max_items: usize) -> FilesWatch {
        FilesWatch {
            place: Place::Root("inbox".into()),
            folder: String::new(),
            pattern: Some(Pattern::parse("*.csv").unwrap()),
            recursive: false,
            every: EVERY,
            changed,
            existing,
            max_items,
            bucket: None,
            folder_id: None,
            drive: None,
        }
    }

    #[test]
    fn the_first_poll_notes_what_is_there_and_later_ones_run_new_files() {
        let w = watch(false, false, 50);
        let first = compare(&w, vec![entry("a.csv", "1", 1), entry("b.txt", "1", 2)], None, 10);
        assert!(first.runs.is_empty(), "a trigger starts from now");
        assert_eq!(
            first.seen.items,
            [("a.csv".to_owned(), "1".to_owned())],
            "the pattern holds"
        );
        let next = compare(
            &w,
            vec![entry("a.csv", "2", 1), entry("c.csv", "1", 3)],
            Some(&first.seen),
            20,
        );
        let ran: Vec<&str> = next.runs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ran, ["c.csv"], "a changed file runs only when asked");
        assert_eq!(next.seen.slot, 20);
        // A file gone and come back is new again.
        let gone = compare(&w, vec![entry("c.csv", "1", 3)], Some(&next.seen), 30);
        let back = compare(
            &w,
            vec![entry("c.csv", "1", 3), entry("a.csv", "2", 1)],
            Some(&gone.seen),
            40,
        );
        assert_eq!(back.runs.len(), 1);
    }

    #[test]
    fn changed_files_run_again_and_the_most_wait_for_the_next_poll() {
        let w = watch(true, true, 2);
        let first = compare(
            &w,
            vec![
                entry("c.csv", "1", 3),
                entry("a.csv", "1", 1),
                entry("b.csv", "1", 2),
            ],
            None,
            1,
        );
        let ran: Vec<&str> = first.runs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(
            ran,
            ["a.csv", "b.csv"],
            "existing files, oldest first, two at most"
        );
        assert_eq!(first.seen.items.len(), 2, "the third is left for the next poll");
        let next = compare(
            &w,
            vec![
                entry("a.csv", "2", 4),
                entry("b.csv", "1", 2),
                entry("c.csv", "1", 3),
            ],
            Some(&first.seen),
            2,
        );
        let ran: Vec<&str> = next.runs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ran, ["c.csv", "a.csv"]);
        let changed = next
            .runs
            .iter()
            .find(|(id, _)| id == "a.csv")
            .map(|(_, v)| v.to_json())
            .unwrap();
        assert!(changed.contains("\"change\":\"changed\""), "{changed}");
        assert!(id_of(&"x".repeat(200)).len() == 64);
    }

    #[test]
    fn settings_are_checked() {
        let read = |j: serde_json::Value| parse("storage", j.as_object().unwrap());
        assert!(
            read(serde_json::json!({ "root": "inbox", "folder": "in", "pattern": "*.csv", "every": "1m" }))
                .is_ok()
        );
        for (bad, says) in [
            (serde_json::json!({ "folder": "in" }), "root"),
            (serde_json::json!({ "root": "inbox", "every": "1s" }), "from 10s"),
            (
                serde_json::json!({ "root": "inbox", "folder": "../x" }),
                ".. would lead out",
            ),
            (
                serde_json::json!({ "root": "inbox", "when": "always" }),
                "new_or_changed",
            ),
            (serde_json::json!({ "root": "inbox", "max_items": 0 }), "1 to 500"),
            (serde_json::json!({ "root": "inbox", "nope": 1 }), "not a setting"),
        ] {
            let err = read(bad.clone()).unwrap_err();
            assert!(err.contains(says), "{bad}: {err}");
        }
        let redis = parse(
            "redis",
            serde_json::json!({ "credential": "cache", "channel": "orders.*" })
                .as_object()
                .unwrap(),
        );
        assert_eq!(
            redis.unwrap().channel().map(|c| c.channel.as_str()),
            Some("orders.*")
        );
        assert!(
            parse(
                "redis",
                serde_json::json!({ "credential": "cache" }).as_object().unwrap()
            )
            .is_err()
        );
    }
}
