// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Google Drive stand-in (v3): `files.list` answering the queries the steps make (`'<id>' in parents and trashed =
//! false`, with `and name = '<name>'`; `\'` and `\\` read as Drive reads them), two files a page; downloads, and
//! Google Docs that hold no bytes (403 `fileNotDownloadable`) exported instead; resumable uploads (a session address
//! on Google's host, then the bytes put there); copies, moves (`addParents` / `removeParents`) and the trash; and
//! `changes`, from a start page token, two a page.

use serde_json::{Value as Json, json};

use super::http::{Reply, Request};
use super::tree::Drive;

pub(crate) const TOKEN: &str = "test-token-drive";
pub(crate) const FOLDER: &str = "application/vnd.google-apps.folder";
pub(crate) const DOC: &str = "application/vnd.google-apps.document";

pub(crate) struct GDrive {
    pub drive: Drive,
    /// Upload sessions: id -> (an existing file's id, or the new file's metadata).
    uploads: Vec<(String, Result<String, Json>)>,
}

fn error(status: u16, reason: &str, message: &str) -> Reply {
    Reply::json(
        status,
        &json!({ "error": { "code": status, "message": message, "errors": [{ "reason": reason, "message": message }] } }),
    )
}

/// The texts between single quotes in a query, `\'` and `\\` read.
fn quoted(q: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = q.chars();
    while let Some(c) = chars.next() {
        if c == '\'' {
            let mut text = String::new();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => text.push(chars.next().unwrap()),
                    '\'' => break,
                    other => text.push(other),
                }
            }
            out.push(text);
        }
    }
    out
}

impl GDrive {
    pub(crate) fn new() -> Self {
        Self {
            drive: Drive::new("root"),
            uploads: Vec::new(),
        }
    }

    pub(crate) fn file(&self, id: &str) -> Json {
        let n = &self.drive.nodes[id];
        let kind = if n.folder {
            FOLDER.to_owned()
        } else if n.kind.is_empty() {
            "application/octet-stream".to_owned()
        } else {
            n.kind.clone()
        };
        let mut f = json!({
            "id": id, "name": n.name, "mimeType": kind, "parents": [n.parent], "trashed": n.trashed,
            "modifiedTime": super::iso(n.modified), "version": n.version.to_string(),
        });
        if !n.folder && kind != DOC {
            f["size"] = json!(n.bytes.len().to_string());
            f["md5Checksum"] = json!(format!("{:032x}", n.version * 131 + n.bytes.len() as u64));
        }
        f
    }

    fn page(
        &self,
        all: Vec<Json>,
        token: Option<&str>,
        key: &str,
        next_key: &str,
        last: Option<(&str, String)>,
    ) -> Reply {
        let start = token
            .and_then(|t| t.rsplit('-').next())
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = json!({ key: all.iter().skip(start).take(2).cloned().collect::<Vec<_>>() });
        if start + 2 < all.len() {
            let prefix = token
                .and_then(|t| t.split_once('-').map(|(p, _)| p.to_owned()))
                .unwrap_or_default();
            body[next_key] = json!(if prefix.is_empty() {
                format!("p-{}", start + 2)
            } else {
                format!("{prefix}-{}", start + 2)
            });
        } else if let Some((k, v)) = last {
            body[k] = json!(v);
        }
        Reply::json(200, &body)
    }

    pub(crate) fn handle(&mut self, r: &Request) -> Reply {
        if r.bearer() != Some(TOKEN) {
            return error(401, "authError", "Invalid Credentials");
        }
        let segments: Vec<&str> = r.path.trim_start_matches('/').split('/').collect();
        let body = r.json();
        match (r.method.as_str(), segments.as_slice()) {
            ("GET", ["drive", "v3", "files"]) => {
                let q = r.q("q").unwrap_or_default();
                let texts = quoted(q);
                let parent = texts.first().cloned().unwrap_or_default();
                let mut found: Vec<&super::tree::Node> = self.drive.children(&parent);
                if q.contains(" and name = ") {
                    let name = texts.get(1).cloned().unwrap_or_default();
                    found.retain(|n| n.name == name);
                }
                let all: Vec<Json> = found.iter().map(|n| self.file(&n.id)).collect();
                self.page(all, r.q("pageToken"), "files", "nextPageToken", None)
            }
            ("GET", ["drive", "v3", "files", id]) => match self.drive.nodes.get(*id) {
                None => error(404, "notFound", &format!("File not found: {id}.")),
                Some(n) if r.q("alt") == Some("media") => {
                    if n.kind == DOC {
                        return error(
                            403,
                            "fileNotDownloadable",
                            "Only files with binary content can be downloaded.",
                        );
                    }
                    Reply::bytes(200, "application/octet-stream", n.bytes.clone())
                }
                Some(_) => Reply::json(200, &self.file(id)),
            },
            ("GET", ["drive", "v3", "files", id, "export"]) => {
                let to = r.q("mimeType").unwrap_or_default();
                let text = String::from_utf8_lossy(&self.drive.nodes[*id].bytes).into_owned();
                Reply::bytes(200, to, format!("{text} (as {to})").into_bytes())
            }
            ("POST", ["drive", "v3", "files"]) => {
                let parent = body["parents"][0].as_str().unwrap();
                let id = self.drive.add(
                    parent,
                    body["name"].as_str().unwrap(),
                    body["mimeType"] == FOLDER,
                    Vec::new(),
                    "",
                );
                Reply::json(200, &self.file(&id))
            }
            ("POST", ["drive", "v3", "files", id, "copy"]) => {
                let made = self.drive.copy(
                    id,
                    body["parents"][0].as_str().unwrap(),
                    body["name"].as_str().unwrap(),
                );
                Reply::json(
                    200,
                    &json!({ "id": made, "size": self.drive.nodes[&made].bytes.len().to_string() }),
                )
            }
            ("PATCH", ["drive", "v3", "files", id]) => {
                let id = (*id).to_owned();
                if body["trashed"] == true {
                    self.drive.remove(&id);
                    return Reply::json(200, &self.file(&id));
                }
                let n = self.drive.nodes[&id].clone();
                let parent = match (r.q("addParents"), r.q("removeParents")) {
                    (Some(add), Some(remove)) if remove == n.parent => add.to_owned(),
                    (Some(_), _) => {
                        return error(400, "badRequest", "removeParents names no parent of the file");
                    }
                    _ => n.parent.clone(),
                };
                self.drive
                    .relocate(&id, &parent, body["name"].as_str().unwrap_or(&n.name));
                Reply::json(200, &self.file(&id))
            }
            ("POST" | "PATCH", ["upload", "drive", "v3", "files", rest @ ..])
                if r.q("uploadType") == Some("resumable") =>
            {
                let session = format!("u{}", self.uploads.len() + 1);
                let target = match rest {
                    [] => Err(body),
                    [id] => Ok((*id).to_owned()),
                    _ => return error(400, "badRequest", "no such upload"),
                };
                self.uploads.push((session.clone(), target));
                Reply::empty(200).with(
                    "location",
                    &format!("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id={session}"),
                )
            }
            ("PUT", ["upload", "drive", "v3", "files"]) => {
                let session = r.q("upload_id").unwrap_or_default();
                let Some((_, target)) = self.uploads.iter().find(|(s, _)| s == session).cloned() else {
                    return error(404, "notFound", "no such upload session");
                };
                let id = match target {
                    Ok(id) => {
                        self.drive.put(&id, r.body.clone());
                        id
                    }
                    Err(meta) => self.drive.add(
                        meta["parents"][0].as_str().unwrap(),
                        meta["name"].as_str().unwrap(),
                        false,
                        r.body.clone(),
                        "",
                    ),
                };
                Reply::json(200, &self.file(&id))
            }
            ("GET", ["drive", "v3", "changes", "startPageToken"]) => Reply::json(
                200,
                &json!({ "startPageToken": self.drive.log.len().to_string() }),
            ),
            ("GET", ["drive", "v3", "changes"]) => {
                let token = r.q("pageToken").unwrap_or_default();
                let since: usize = token.split('-').next().unwrap().parse().unwrap();
                let mut changed: Vec<String> = Vec::new();
                for (id, _) in self.drive.log.iter().skip(since) {
                    changed.retain(|c| c != id);
                    changed.push(id.clone());
                }
                let all: Vec<Json> = changed
                    .iter()
                    .map(|id| json!({ "kind": "drive#change", "changeType": "file", "fileId": id, "removed": false, "file": self.file(id) }))
                    .collect();
                let paged = if token.contains('-') {
                    token.to_owned()
                } else {
                    format!("{since}-0")
                };
                self.page(
                    all,
                    Some(&paged),
                    "changes",
                    "nextPageToken",
                    Some(("newStartPageToken", self.drive.log.len().to_string())),
                )
            }
            _ => error(404, "notFound", "Not found"),
        }
    }
}
