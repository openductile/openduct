// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Dropbox stand-in (HTTP API v2): RPC endpoints with JSON bodies, content endpoints with their arguments in the
//! `Dropbox-API-Arg` header (refused unless it is HTTP-header-safe: ASCII only), endpoint errors as 409 with an
//! `error_summary`, an access token that has run out (401) until it is refreshed at `/oauth2/token`, and a folder's
//! cursor that tells what changed since it was given - a page of two entries at a time.

use serde_json::{Value as Json, json};

use super::http::{Reply, Request, decode};
use super::tree::Drive;

/// The token the stand-in takes; `OLD` has run out.
pub(crate) const TOKEN: &str = "test-token-dropbox-2";
pub(crate) const OLD: &str = "test-token-dropbox-1";
pub(crate) const REFRESH: &str = "standin-refresh";

pub(crate) struct Dropbox {
    pub drive: Drive,
    pub refreshed: usize,
    /// The next cursor read is said to be reset.
    pub reset_next: bool,
}

fn endpoint_error(summary: &str) -> Reply {
    let tags: Vec<&str> = summary
        .split('/')
        .filter(|t| !t.is_empty() && *t != "..")
        .collect();
    let mut error = json!({});
    for tag in tags.iter().rev() {
        error = if error == json!({}) {
            json!({ ".tag": tag })
        } else {
            json!({ ".tag": tag, *tag: error })
        };
    }
    Reply::json(
        409,
        &json!({ "error_summary": format!("{summary}/.."), "error": error }),
    )
}

impl Dropbox {
    pub(crate) fn new() -> Self {
        Self {
            drive: Drive::new("root"),
            refreshed: 0,
            reset_next: false,
        }
    }

    fn node_at(&self, path: &str) -> Option<String> {
        if let Some(id) = path.strip_prefix("id:") {
            return self
                .drive
                .nodes
                .get(id)
                .filter(|n| !n.trashed)
                .map(|n| n.id.clone());
        }
        self.drive.at("root", path, true).map(|n| n.id.clone())
    }

    fn display(&self, id: &str) -> String {
        format!("/{}", self.drive.path_of("root", id).unwrap_or_default())
    }

    pub(crate) fn metadata(&self, id: &str) -> Json {
        let n = &self.drive.nodes[id];
        let shown = self.display(id);
        if n.trashed {
            return json!({ ".tag": "deleted", "name": n.name, "path_display": shown, "path_lower": shown.to_lowercase() });
        }
        if n.folder {
            return json!({ ".tag": "folder", "name": n.name, "id": format!("id:{id}"), "path_display": shown, "path_lower": shown.to_lowercase() });
        }
        json!({
            ".tag": "file", "name": n.name, "id": format!("id:{id}"), "path_display": shown,
            "path_lower": shown.to_lowercase(), "size": n.bytes.len(), "server_modified": super::iso(n.modified),
            "client_modified": super::iso(n.modified), "rev": format!("{:x}{:06x}", n.version, n.modified % 1_000_000),
        })
    }

    /// A folder made at `path`, with the folders above it.
    pub(crate) fn folders(&mut self, path: &str) -> String {
        let mut at = "root".to_owned();
        for name in path.split('/').filter(|n| !n.is_empty()) {
            at = match self.drive.child(&at, name, true) {
                Some(n) => n.id.clone(),
                None => self.drive.add(&at, name, true, Vec::new(), ""),
            };
        }
        at
    }

    /// A file put at `path` (as a person would, in Dropbox itself).
    pub(crate) fn put(&mut self, path: &str, bytes: &[u8]) -> String {
        let (parent, name) = path.rsplit_once('/').unwrap();
        let parent = self.folders(parent);
        match self.drive.child(&parent, name, true).map(|n| n.id.clone()) {
            Some(id) => {
                self.drive.put(&id, bytes.to_vec());
                id
            }
            None => self.drive.add(&parent, name, false, bytes.to_vec(), ""),
        }
    }

    /// The entries of a folder (all of them below it when `recursive`).
    fn entries(&self, folder: &str, recursive: bool) -> Vec<Json> {
        let mut out = Vec::new();
        let mut queue = vec![folder.to_owned()];
        while let Some(f) = queue.pop() {
            for n in self.drive.children(&f) {
                out.push(self.metadata(&n.id));
                if n.folder && recursive {
                    queue.push(n.id.clone());
                }
            }
        }
        out
    }

    fn inside(&self, folder: &str, id: &str, recursive: bool) -> bool {
        let mut at = self.drive.nodes[id].parent.clone();
        loop {
            if at == folder {
                return true;
            }
            if !recursive || at.is_empty() {
                return false;
            }
            at = self
                .drive
                .nodes
                .get(&at)
                .map(|n| n.parent.clone())
                .unwrap_or_default();
        }
    }

    /// A page of a listing (or of what changed): two entries, and the cursor to go on with.
    fn page(&self, cursor: &Json) -> Reply {
        let folder = cursor["folder"].as_str().unwrap().to_owned();
        let recursive = cursor["recursive"].as_bool().unwrap();
        let entries: Vec<Json> = match cursor["since"].as_u64() {
            None => self.entries(&folder, recursive),
            Some(since) => {
                let mut seen = Vec::new();
                for (id, _) in self.drive.log.iter().skip(usize::try_from(since).unwrap()) {
                    if !seen.contains(id) && self.inside(&folder, id, recursive) {
                        seen.push(id.clone());
                    }
                }
                seen.iter().map(|id| self.metadata(id)).collect()
            }
        };
        let offset = usize::try_from(cursor["offset"].as_u64().unwrap_or(0)).unwrap();
        let page: Vec<Json> = entries.iter().skip(offset).take(2).cloned().collect();
        let more = offset + 2 < entries.len();
        let next = if more {
            json!({ "folder": folder, "recursive": recursive, "since": cursor["since"], "offset": offset + 2, "at": cursor["at"] })
        } else {
            json!({ "folder": folder, "recursive": recursive, "since": cursor["at"], "offset": 0, "at": cursor["at"] })
        };
        Reply::json(
            200,
            &json!({ "entries": page, "cursor": next.to_string(), "has_more": more }),
        )
    }

    pub(crate) fn handle(&mut self, r: &Request) -> Reply {
        if r.path == "/oauth2/token" {
            let body = String::from_utf8_lossy(&r.body).into_owned();
            if body.contains("grant_type=refresh_token")
                && body.contains(&format!("refresh_token={REFRESH}"))
                && body.contains("client_id=cid")
            {
                self.refreshed += 1;
                return Reply::json(
                    200,
                    &json!({ "access_token": TOKEN, "token_type": "bearer", "expires_in": 14400 }),
                );
            }
            return Reply::json(400, &json!({ "error": "invalid_grant" }));
        }
        match r.bearer() {
            Some(TOKEN) => {}
            Some(OLD) => {
                return Reply::json(
                    401,
                    &json!({ "error_summary": "expired_access_token/..", "error": { ".tag": "expired_access_token" } }),
                );
            }
            _ => {
                return Reply::json(
                    401,
                    &json!({ "error_summary": "invalid_access_token/..", "error": { ".tag": "invalid_access_token" } }),
                );
            }
        }
        let arg: Json = match r.header("dropbox-api-arg") {
            Some(a) if !a.is_ascii() => {
                return Reply::bytes(400, "text/plain", b"Error in call: header not HTTP-safe".to_vec());
            }
            Some(a) => serde_json::from_str(a).unwrap(),
            None => r.json(),
        };
        let path = |k: &str| arg[k].as_str().unwrap_or_default().to_owned();
        match r.path.as_str() {
            "/2/files/get_metadata" => match self.node_at(&path("path")) {
                Some(id) => Reply::json(200, &self.metadata(&id)),
                None => endpoint_error("path/not_found"),
            },
            "/2/files/list_folder" => {
                let folder = match self.node_at(&path("path")) {
                    Some(id) => id,
                    None => return endpoint_error("path/not_found"),
                };
                let recursive = arg["recursive"].as_bool().unwrap_or(false);
                let mut reply = self
                    .page(&json!({ "folder": folder, "recursive": recursive, "at": self.drive.log.len() }));
                if recursive && folder != "root" {
                    // A recursive listing names the folder itself first, as Dropbox's does.
                    let mut body: Json = serde_json::from_slice(&reply.body).unwrap();
                    body["entries"]
                        .as_array_mut()
                        .unwrap()
                        .insert(0, self.metadata(&folder));
                    reply = Reply::json(200, &body);
                }
                reply
            }
            "/2/files/list_folder/continue" => {
                if std::mem::take(&mut self.reset_next) {
                    return endpoint_error("reset");
                }
                let cursor: Json = serde_json::from_str(&path("cursor")).unwrap();
                let mut cursor = cursor;
                if cursor["since"].is_number() && cursor["offset"] == 0 {
                    cursor["at"] = json!(self.drive.log.len());
                }
                self.page(&cursor)
            }
            "/2/files/download" => match self.node_at(&path("path")) {
                Some(id) if !self.drive.nodes[&id].folder => Reply::bytes(
                    200,
                    "application/octet-stream",
                    self.drive.nodes[&id].bytes.clone(),
                )
                .with("dropbox-api-result", &super::ascii(&self.metadata(&id))),
                _ => endpoint_error("path/not_found"),
            },
            "/2/files/upload" => {
                let target = path("path");
                if self.node_at(&target).is_some() && arg["mode"] == "add" {
                    return endpoint_error("path/conflict/file");
                }
                let id = self.put(&target, &r.body);
                Reply::json(200, &self.metadata(&id))
            }
            "/2/files/move_v2" | "/2/files/copy_v2" => {
                let Some(from) = self.node_at(&path("from_path")) else {
                    return endpoint_error("from_lookup/not_found");
                };
                let to = path("to_path");
                if self.node_at(&to).is_some() {
                    return endpoint_error("to/conflict/file");
                }
                let (parent, name) = to.rsplit_once('/').unwrap();
                let parent = self.folders(parent);
                let id = if r.path.ends_with("move_v2") {
                    // What was at the old path is told as deleted there, as Dropbox tells a move.
                    let old = self.drive.nodes[&from].clone();
                    let tomb = self.drive.add(&old.parent, &old.name, old.folder, Vec::new(), "");
                    self.drive.remove(&tomb);
                    self.drive.relocate(&from, &parent, name);
                    from
                } else {
                    self.drive.copy(&from, &parent, name)
                };
                Reply::json(200, &json!({ "metadata": self.metadata(&id) }))
            }
            "/2/files/delete_v2" => match self.node_at(&path("path")) {
                Some(id) => {
                    let meta = self.metadata(&id);
                    self.drive.remove(&id);
                    Reply::json(200, &json!({ "metadata": meta }))
                }
                None => endpoint_error("path_lookup/not_found"),
            },
            "/2/files/create_folder_v2" => {
                let target = path("path");
                if let Some(id) = self.node_at(&target) {
                    return endpoint_error(if self.drive.nodes[&id].folder {
                        "path/conflict/folder"
                    } else {
                        "path/conflict/file"
                    });
                }
                let id = self.folders(&target);
                Reply::json(200, &json!({ "metadata": self.metadata(&id) }))
            }
            _ => Reply::json(
                400,
                &json!({ "error_summary": format!("unknown route {}", decode(&r.path)) }),
            ),
        }
    }
}
