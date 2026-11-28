// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One place's session, whatever it is - a folder of this machine, an SFTP or FTP server, an S3 bucket - and a
//! step's action done on it, its result made the same way for every place: a listing's entries, a file read into the
//! run, what was written, moved, copied, deleted or made.

use openduct_expr::{Record, Value};
use serde_json::json;

use super::paths::Rel;
use super::{Action, Asked, Entry, MOST_LIST_ITEMS, cloud, ftp, local, s3, sftp};
use crate::files::{FileData, MAX_FILE, RunFiles};
use crate::run::Effects;

/// How deep a list goes into the folders below, at most.
pub(crate) const MAX_LIST_DEPTH: usize = 8;

/// An entry as a place lists it: its path from where the place's paths start.
#[derive(Debug, Clone)]
pub(crate) struct Listed {
    pub rel: Rel,
    pub folder: bool,
    pub size: u64,
    pub modified: Option<i64>,
    pub version: String,
}

/// A place, reached.
pub(crate) enum Session<'a> {
    Local(local::Session),
    Sftp(Box<sftp::Session>),
    Ftp(Box<ftp::Session>),
    S3(Box<s3::Session<'a>>),
    /// Azure, Google Cloud Storage, Dropbox, Box, OneDrive, Google Drive. // storage places
    Cloud(Box<cloud::Session<'a>>),
}

impl Session<'_> {
    /// The credential's own folder (or bucket prefix): paths are inside it.
    fn base(&self) -> Rel {
        match self {
            Self::Local(_) => Rel::default(),
            Self::Sftp(s) => s.base.clone(),
            Self::Ftp(s) => s.base.clone(),
            Self::S3(s) => s.base.clone(),
            Self::Cloud(s) => s.base(), // storage places
        }
    }

    /// A path the step names, checked, inside the credential's own folder when it has one. A path from the top is
    /// taken only on a server whose credential names no folder.
    pub(crate) fn path(&self, text: &str) -> Result<Rel, String> {
        let base = self.base();
        // storage places: a place reached over its vendor's API takes `/Invoices/a.pdf` as `Invoices/a.pdf`, inside
        // its own folder still.
        if matches!(self, Self::Cloud(_)) {
            let mut rel = Rel::parse(text, true)?;
            rel.absolute = false;
            return Ok(rel.under(&base));
        }
        let from_top = matches!(self, Self::Sftp(_) | Self::Ftp(_)) && base.is_top() && !base.absolute;
        let rel = Rel::parse(text, from_top)?;
        Ok(if rel.absolute { rel } else { rel.under(&base) })
    }

    /// A path as the step's results name it: inside the credential's own folder, as it was given.
    pub(crate) fn shown(&self, rel: &Rel) -> String {
        let base = self.base();
        if base.is_top() && !base.absolute {
            return rel.text();
        }
        rel.inside(&base).map_or_else(|| rel.text(), |r| r.text())
    }

    pub(crate) async fn list(&mut self, dir: &Rel, recursive: bool) -> Result<(Vec<Listed>, bool), String> {
        let depth = if recursive { MAX_LIST_DEPTH } else { 1 };
        match self {
            Self::Local(s) => s.list(dir, depth, MOST_LIST_ITEMS).await,
            Self::Sftp(s) => s.list(dir, depth, MOST_LIST_ITEMS).await,
            Self::Ftp(s) => s.list(dir, depth, MOST_LIST_ITEMS).await,
            Self::S3(s) => s.list(dir, recursive, MOST_LIST_ITEMS).await,
            Self::Cloud(s) => s.list(dir, recursive, MOST_LIST_ITEMS).await, // storage places
        }
    }

    pub(crate) async fn read(&mut self, path: &Rel) -> Result<(Vec<u8>, Option<i64>), String> {
        match self {
            Self::Local(s) => s.read(path, MAX_FILE).await,
            Self::Sftp(s) => s.read(path, MAX_FILE).await,
            Self::Ftp(s) => s.read(path, MAX_FILE).await,
            Self::S3(s) => s.read(path, MAX_FILE).await,
            Self::Cloud(s) => s.read(path, MAX_FILE).await.map(|(b, m, _)| (b, m)), // storage places
        }
    }

    /// A file read, and the name it is read as: its own, or another a place gives it (a Google Doc exported as
    /// `Report.docx`). // storage places
    async fn read_named(&mut self, path: &Rel) -> Result<(Vec<u8>, Option<i64>, String), String> {
        let own = path.name().unwrap_or("file").to_owned();
        match self {
            Self::Cloud(s) => s
                .read(path, MAX_FILE)
                .await
                .map(|(b, m, n)| (b, m, n.unwrap_or(own))),
            _ => self.read(path).await.map(|(b, m)| (b, m, own)),
        }
    }

    pub(crate) async fn write(
        &mut self,
        path: &Rel,
        bytes: &FileData,
        overwrite: bool,
    ) -> Result<(), String> {
        match self {
            Self::Local(s) => s.write(path, bytes, overwrite).await,
            Self::Sftp(s) => s.write(path, &bytes.bytes, overwrite).await,
            Self::Ftp(s) => s.write(path, &bytes.bytes, overwrite).await,
            Self::S3(s) => s.write(path, bytes, overwrite).await,
            Self::Cloud(s) => s.write(path, bytes, overwrite).await, // storage places
        }
    }

    async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        match self {
            Self::Local(s) => s.rename(from, to, overwrite).await,
            Self::Sftp(s) => s.rename(from, to, overwrite).await,
            Self::Ftp(s) => s.rename(from, to, overwrite).await,
            Self::S3(s) => {
                s.copy(from, to, overwrite).await?;
                s.delete(from).await.map(|_| ())
            }
            Self::Cloud(s) => s.rename(from, to, overwrite).await, // storage places
        }
    }

    /// A copy made where the place can make one itself; on a server that cannot, the file read and written again.
    async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        match self {
            Self::Local(s) => s.copy(from, to, overwrite).await,
            Self::S3(s) => s.copy(from, to, overwrite).await,
            Self::Cloud(s) => s.copy(from, to, overwrite).await, // storage places
            Self::Sftp(_) | Self::Ftp(_) => {
                let (bytes, _) = self.read(from).await?;
                let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let file = FileData {
                    name: from.name().unwrap_or("file").to_owned(),
                    media_type: crate::files::steps::guess_type(from.name().unwrap_or_default()).to_owned(),
                    bytes: std::sync::Arc::new(bytes),
                };
                self.write(to, &file, overwrite).await?;
                Ok(size)
            }
        }
    }

    async fn delete(&mut self, path: &Rel) -> Result<bool, String> {
        match self {
            Self::Local(s) => s.delete(path).await,
            Self::Sftp(s) => s.delete(path).await,
            Self::Ftp(s) => s.delete(path).await,
            Self::S3(s) => s.delete(path).await,
            Self::Cloud(s) => s.delete(path).await, // storage places
        }
    }

    async fn make_folder(&mut self, path: &Rel) -> Result<bool, String> {
        match self {
            Self::Local(s) => s.make_folder(path).await,
            Self::Sftp(s) => s.make_folder(path).await,
            Self::Ftp(s) => s.make_folder(path).await,
            Self::S3(s) => s.make_folder(path).await,
            Self::Cloud(s) => s.make_folder(path).await, // storage places
        }
    }

    /// Ends the session: a server is told goodbye.
    pub(crate) async fn close(self) {
        match self {
            Self::Sftp(s) => s.close().await,
            Self::Ftp(s) => s.close().await,
            Self::Local(_) | Self::S3(_) | Self::Cloud(_) => {}
        }
    }

    /// The listing's entries as a step gives them.
    pub(crate) fn entry(&self, listed: &Listed) -> Entry {
        Entry {
            name: listed.rel.name().unwrap_or_default().to_owned(),
            path: self.shown(&listed.rel),
            folder: listed.folder,
            size: listed.size,
            modified: listed.modified,
            version: listed.version.clone(),
        }
    }
}

/// Where a move or a copy goes, checked.
fn target(session: &Session<'_>, asked: &Asked) -> Result<Rel, String> {
    let to = asked.to.as_deref().ok_or("to: where it goes")?;
    let to = session.path(to)?;
    if to.is_top() || to.name().is_none() {
        return Err("to: a path with a name at its end".to_owned());
    }
    Ok(to)
}

fn record(fields: serde_json::Value) -> Value {
    Value::from_json(&fields)
}

/// The step's action, done on the place; its result.
pub(crate) async fn act(
    session: &mut Session<'_>,
    effects: &dyn Effects,
    files: &RunFiles,
    asked: &Asked,
    upload: Option<&FileData>,
) -> Result<Value, String> {
    let path = session.path(&asked.path)?;
    let shown = session.shown(&path);
    let named = |what: &str| -> Result<(), String> {
        if path.name().is_none() {
            Err(format!("path: the {what}, as in/orders.csv"))
        } else {
            Ok(())
        }
    };
    match asked.action {
        Action::List => {
            let (listed, mut more) = session.list(&path, asked.recursive).await?;
            let mut out = Vec::new();
            for l in &listed {
                let entry = session.entry(l);
                let kept = match &asked.pattern {
                    Some(p) => !entry.folder && p.matches(&entry.name),
                    None => true,
                };
                if !kept {
                    continue;
                }
                if out.len() >= asked.limit {
                    more = true;
                    break;
                }
                out.push(entry.value());
            }
            let mut result = Record::new();
            result.insert("count".to_owned(), record(json!(out.len())));
            result.insert("files".to_owned(), Value::list(out));
            result.insert("more".to_owned(), Value::Bool(more));
            result.insert("path".to_owned(), Value::text(shown.as_str()));
            Ok(Value::record(result))
        }
        Action::Read => {
            named("file to read")?;
            let (bytes, modified, name) = session.read_named(&path).await?;
            let made = files
                .make(effects, &name, crate::files::steps::guess_type(&name), bytes)
                .await?;
            let Value::Record(r) = made else {
                return Ok(made);
            };
            let mut out: Record = (*r).clone();
            out.insert("path".to_owned(), Value::text(shown.as_str()));
            out.insert(
                "modified".to_owned(),
                modified.map_or(Value::Null, |m| Value::text(crate::runner::iso(m))),
            );
            Ok(Value::record(out))
        }
        Action::Write => {
            named("file to write")?;
            let upload = upload.ok_or("file: what to write, as steps.report or text")?;
            session.write(&path, upload, asked.overwrite).await?;
            Ok(record(
                json!({ "written": true, "path": shown, "size": upload.bytes.len() }),
            ))
        }
        Action::Move => {
            named("file or folder to move")?;
            let to = target(session, asked)?;
            session.rename(&path, &to, asked.overwrite).await?;
            Ok(record(
                json!({ "moved": true, "from": shown, "to": session.shown(&to) }),
            ))
        }
        Action::Copy => {
            named("file to copy")?;
            let to = target(session, asked)?;
            let size = session.copy(&path, &to, asked.overwrite).await?;
            Ok(record(
                json!({ "copied": true, "from": shown, "to": session.shown(&to), "size": size }),
            ))
        }
        Action::Delete => {
            named("file or empty folder to delete")?;
            let deleted = session.delete(&path).await?;
            Ok(record(json!({ "deleted": deleted, "path": shown })))
        }
        Action::MakeFolder => {
            named("folder to make")?;
            let made = session.make_folder(&path).await?;
            Ok(record(json!({ "made": made, "path": shown })))
        }
        Action::Presign => {
            named("object")?;
            match session {
                Session::S3(s) => s.presign(&path, asked.method, asked.expires),
                Session::Cloud(s) => s.presign(&path, asked.method, asked.expires), // storage places
                _ => Err("presign: S3, Azure and Google Cloud Storage only".to_owned()),
            }
        }
    }
}
