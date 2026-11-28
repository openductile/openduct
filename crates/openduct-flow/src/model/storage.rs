// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the storage steps (see [`crate::storage`]): `storage` (files in a folder of this machine, on an SFTP or FTP
//! server, in S3), `compress` (archives packed and unpacked) and `redis` (keys and values) - each setting checked, and
//! only the settings its action takes allowed.

use std::time::Duration;

use serde_json::{Map, Value as Json};

use super::{Kind, Reader, duration, text};
use crate::storage::archive::{ArchiveAction, ArchiveStep, Format};
use crate::storage::paths::Pattern;
use crate::storage::redis::{RedisAction, RedisStep};
use crate::storage::{Action, FilesStep, LIST_ITEMS, MOST_LIST_ITEMS, Place, StorageStep, place};

fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

fn only(s: &Map<String, Json>, at: &str, what: &str, allowed: &[&str]) -> Result<(), String> {
    match s.keys().find(|k| !allowed.contains(&k.as_str())) {
        Some(k) => Err(format!(
            "{at}.{k}: not a setting of {what} ({})",
            allowed.join(", ")
        )),
        None => Ok(()),
    }
}

fn flag(s: &Map<String, Json>, name: &str, at: &str) -> Result<bool, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(false),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

fn number(
    s: &Map<String, Json>,
    name: &str,
    at: &str,
    default: i64,
    range: std::ops::RangeInclusive<i64>,
) -> Result<i64, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(n) => n.as_i64().filter(|n| range.contains(n)).ok_or_else(|| {
            format!(
                "{at}.{name}: a whole number, {} to {}",
                range.start(),
                range.end()
            )
        }),
    }
}

fn wait(s: &Map<String, Json>, name: &str, at: &str, most: Duration) -> Result<Option<Duration>, String> {
    let Some(given) = text(s.get(name)) else {
        return Ok(None);
    };
    duration(given)
        .filter(|d| *d >= Duration::from_secs(1) && *d <= most)
        .map(Some)
        .ok_or_else(|| {
            format!(
                "{at}.{name}: how long, as 30s, 15m, 1h or 1d - at most {}d",
                most.as_secs() / 86_400
            )
        })
}

fn pattern(s: &Map<String, Json>, at: &str) -> Result<Option<Pattern>, String> {
    text(s.get("pattern"))
        .map(|p| Pattern::parse(p).map_err(|e| format!("{at}.pattern: {e}")))
        .transpose()
}

impl Reader {
    pub(super) fn storage(&self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        Ok(Kind::Storage(match kind {
            "storage" => StorageStep::Files(Box::new(self.files_step(body, at)?)),
            "compress" => StorageStep::Archive(Box::new(self.archive_step(body, at)?)),
            _ => StorageStep::Redis(Box::new(self.redis_step(body, at)?)),
        }))
    }

    fn files_step(&self, body: &Json, at: &str) -> Result<FilesStep, String> {
        let s = settings(body, at)?;
        let named =
            text(s.get("action")).ok_or_else(|| format!("{at}.action: {}", Action::ALL.join(", ")))?;
        let action = Action::parse(named)
            .ok_or_else(|| format!("{at}.action: {} - not {named:?}", Action::ALL.join(", ")))?;
        let own: &[&str] = match action {
            Action::List => &["pattern", "recursive", "limit"],
            Action::Write => &["file", "overwrite"],
            Action::Move | Action::Copy => &["to", "overwrite"],
            Action::Presign => &["expires", "method"],
            Action::Read => &["export"], // storage places
            Action::Delete | Action::MakeFolder => &[],
        };
        let mut allowed = vec!["action", "root", "credential", "path", "bucket"];
        allowed.extend_from_slice(&["folder_id", "drive"]); // storage places
        allowed.extend_from_slice(own);
        only(s, at, &format!("storage's {named}"), &allowed)?;
        let place = place(s, at)?;
        if action == Action::Presign && matches!(place, Place::Root(_)) {
            return Err(format!(
                "{at}.action: presign is for S3, Azure and Google Cloud Storage, not a folder of this machine"
            ));
        }
        let path = match s.get("path") {
            Some(p) => self.template(p, &format!("{at}.path"))?,
            None if action == Action::List => crate::template::Template::text(""),
            None => return Err(format!("{at}.path: the path, as in/orders.csv")),
        };
        let to = match s.get("to") {
            Some(t) => Some(self.template(t, &format!("{at}.to"))?),
            None if matches!(action, Action::Move | Action::Copy) => {
                return Err(format!("{at}.to: where it goes, as done/orders.csv"));
            }
            None => None,
        };
        let file = match s.get("file") {
            Some(f) => Some(self.expression(f, &format!("{at}.file"))?),
            None if action == Action::Write => {
                return Err(format!(
                    "{at}.file: what to write, as steps.report (a file) or text"
                ));
            }
            None => None,
        };
        let most = i64::try_from(MOST_LIST_ITEMS).unwrap_or(10_000);
        let default = i64::try_from(LIST_ITEMS).unwrap_or(1_000);
        let method = match text(s.get("method")).map(str::to_ascii_lowercase).as_deref() {
            None | Some("get") => "GET",
            Some("put") => "PUT",
            Some(other) => return Err(format!("{at}.method: get or put, not {other:?}")),
        };
        Ok(FilesStep {
            action,
            place,
            path,
            to,
            file,
            pattern: pattern(s, at)?,
            recursive: flag(s, "recursive", at)?,
            limit: usize::try_from(number(s, "limit", at, default, 1..=most)?).unwrap_or(LIST_ITEMS),
            overwrite: flag(s, "overwrite", at)?,
            expires: wait(s, "expires", at, Duration::from_secs(7 * 86_400))?
                .unwrap_or(Duration::from_secs(3600)),
            method,
            bucket: s
                .get("bucket")
                .map(|b| self.template(b, &format!("{at}.bucket")))
                .transpose()?,
            // storage places
            folder_id: s
                .get("folder_id")
                .map(|b| self.template(b, &format!("{at}.folder_id")))
                .transpose()?,
            drive: s
                .get("drive")
                .map(|b| self.template(b, &format!("{at}.drive")))
                .transpose()?,
            export: match text(s.get("export")) {
                None => crate::storage::Export::default(),
                Some(e) => crate::storage::Export::parse(e).ok_or_else(|| {
                    format!(
                        "{at}.export: {} - how a Google Docs file is read, not {e:?}",
                        crate::storage::cloud::Export::ALL.join(", ")
                    )
                })?,
            },
        })
    }

    fn archive_step(&self, body: &Json, at: &str) -> Result<ArchiveStep, String> {
        let s = settings(body, at)?;
        let named = text(s.get("action")).ok_or_else(|| format!("{at}.action: pack, unpack or list"))?;
        let action = match named {
            "pack" => ArchiveAction::Pack,
            "unpack" => ArchiveAction::Unpack,
            "list" => ArchiveAction::List,
            other => return Err(format!("{at}.action: pack, unpack or list - not {other:?}")),
        };
        let allowed: &[&str] = match action {
            ArchiveAction::Pack => &["action", "format", "files", "name"],
            ArchiveAction::Unpack => &["action", "format", "file", "pattern"],
            ArchiveAction::List => &["action", "format", "file"],
        };
        only(s, at, &format!("compress's {named}"), allowed)?;
        let format = text(s.get("format"))
            .filter(|f| *f != "auto")
            .map(|f| {
                Format::parse(f).ok_or_else(|| format!("{at}.format: zip, gzip, tar or tar.gz - not {f:?}"))
            })
            .transpose()?;
        let expression = |name: &str, hint: &str| -> Result<Option<openduct_expr::Compiled>, String> {
            match s.get(name) {
                Some(v) => Ok(Some(self.expression(v, &format!("{at}.{name}"))?)),
                None => Err(format!("{at}.{name}: {hint}")),
            }
        };
        let (files, file) = match action {
            ArchiveAction::Pack => (
                expression("files", "the files to pack, as [steps.report, steps.photo]")?,
                None,
            ),
            _ => (None, expression("file", "the archive, as steps.download.body")?),
        };
        Ok(ArchiveStep {
            action,
            format,
            files,
            file,
            name: s
                .get("name")
                .map(|n| self.template(n, &format!("{at}.name")))
                .transpose()?,
            pattern: pattern(s, at)?,
        })
    }

    fn redis_step(&self, body: &Json, at: &str) -> Result<RedisStep, String> {
        let s = settings(body, at)?;
        let named =
            text(s.get("action")).ok_or_else(|| format!("{at}.action: {}", RedisAction::ALL.join(", ")))?;
        let action = RedisAction::parse(named)
            .ok_or_else(|| format!("{at}.action: {} - not {named:?}", RedisAction::ALL.join(", ")))?;
        let own: &[&str] = match action {
            RedisAction::Get | RedisAction::Delete | RedisAction::Exists => &["key"],
            RedisAction::Set => &["key", "value", "ttl", "if_missing"],
            RedisAction::Increment => &["key", "by"],
            RedisAction::Push => &["key", "value", "end"],
            RedisAction::Pop => &["key", "end"],
            RedisAction::Range => &["key", "start", "stop"],
            RedisAction::Publish => &["channel", "value"],
            RedisAction::Expire => &["key", "ttl"],
        };
        let mut allowed = vec!["action", "credential"];
        allowed.extend_from_slice(own);
        only(s, at, &format!("redis's {named}"), &allowed)?;
        for needed in own.iter().filter(|n| matches!(**n, "key" | "value" | "channel")) {
            if !s.contains_key(*needed) {
                return Err(format!("{at}.{needed}: {named} needs it"));
            }
        }
        if action == RedisAction::Expire && !s.contains_key("ttl") {
            return Err(format!("{at}.ttl: how long the key is kept, as 1h"));
        }
        let credential = text(s.get("credential"))
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| format!("{at}.credential: the redis credential"))?
            .to_owned();
        let template = |name: &str| {
            s.get(name)
                .map(|v| self.template(v, &format!("{at}.{name}")))
                .transpose()
        };
        let left = match text(s.get("end")) {
            None => None,
            Some("left") => Some(true),
            Some("right") => Some(false),
            Some(other) => return Err(format!("{at}.end: left or right, not {other:?}")),
        };
        Ok(RedisStep {
            credential,
            action,
            key: template("key")?,
            value: template("value")?,
            ttl: wait(s, "ttl", at, Duration::from_secs(10 * 365 * 86_400))?,
            if_missing: flag(s, "if_missing", at)?,
            by: template("by")?,
            left,
            start: number(s, "start", at, 0, -1_000_000..=1_000_000)?,
            stop: number(s, "stop", at, -1, -1_000_000..=1_000_000)?,
            channel: template("channel")?,
        })
    }
}
