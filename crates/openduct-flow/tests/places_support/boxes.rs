// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Box stand-in (API 2.0): folders' items two at a time (marker paging), a download answered with a redirect to
//! `dl.boxcloud.com`, uploads to `upload.box.com` as multipart form data (the attributes part first, or 400), names
//! told apart regardless of their case (409 `item_name_in_use`), and a folder deleted only when empty.

use serde_json::{Value as Json, json};

use super::http::{Reply, Request};
use super::tree::Drive;

/// The tokens it takes: an OAuth one and a developer token.
pub(crate) const TOKEN: &str = "box-standin";
pub(crate) const DEVELOPER: &str = "box-developer-standin";

pub(crate) struct Boxes {
    pub drive: Drive,
    port: u16,
}

fn error(status: u16, code: &str) -> Reply {
    Reply::json(
        status,
        &json!({ "type": "error", "status": status, "code": code, "message": format!("{code} here"), "request_id": "r1" }),
    )
}

/// The parts of a multipart body: (name, bytes), in order.
pub(crate) fn parts(r: &Request) -> Vec<(String, Vec<u8>)> {
    let kind = r.header("content-type").unwrap_or_default();
    let Some(boundary) = kind.split("boundary=").nth(1) else {
        return Vec::new();
    };
    let marker = format!("--{boundary}");
    let body = &r.body;
    let mut out = Vec::new();
    let mut starts = Vec::new();
    let mut i = 0;
    while i + marker.len() <= body.len() {
        if &body[i..i + marker.len()] == marker.as_bytes() {
            starts.push(i);
            i += marker.len();
        } else {
            i += 1;
        }
    }
    for pair in starts.windows(2) {
        let part = &body[pair[0] + marker.len() + 2..pair[1] - 2];
        let split = part.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&part[..split]).into_owned();
        let name = head
            .split("name=\"")
            .nth(1)
            .and_then(|n| n.split('"').next())
            .unwrap_or_default()
            .to_owned();
        out.push((name, part[split + 4..].to_vec()));
    }
    out
}

impl Boxes {
    pub(crate) fn new(port: u16) -> Self {
        Self {
            drive: Drive::new("0"),
            port,
        }
    }

    fn entry(&self, id: &str) -> Json {
        let n = &self.drive.nodes[id];
        if n.folder {
            return json!({ "type": "folder", "id": id, "name": n.name, "etag": n.version.to_string() });
        }
        json!({
            "type": "file", "id": id, "name": n.name, "size": n.bytes.len(), "etag": n.version.to_string(),
            "sha1": format!("{:040x}", n.version * 7919 + n.bytes.len() as u64), "modified_at": super::iso(n.modified),
        })
    }

    /// The id of a node, Box's way: its digits (the tree's own ids are `n<digits>`).
    fn id(raw: &str) -> String {
        if raw == "0" {
            raw.to_owned()
        } else {
            format!("n{raw}")
        }
    }

    fn shown(id: &str) -> String {
        id.trim_start_matches('n').to_owned()
    }

    fn out(&self, id: &str) -> Json {
        let mut e = self.entry(id);
        e["id"] = json!(Self::shown(id));
        e
    }

    pub(crate) fn handle(&mut self, r: &Request) -> Reply {
        if r.host == "dl.boxcloud.com" {
            let id = r.path.trim_start_matches("/d/");
            return Reply::bytes(
                200,
                "application/octet-stream",
                self.drive.nodes[id].bytes.clone(),
            );
        }
        if !matches!(r.bearer(), Some(TOKEN | DEVELOPER)) {
            return Reply::empty(401);
        }
        let segments: Vec<&str> = r.path.trim_start_matches('/').split('/').collect();
        let body = r.json();
        let name_in_use =
            |me: &Self, parent: &str, name: &str| me.drive.child(parent, name, true).map(|n| n.id.clone());
        match (r.method.as_str(), r.host.as_str(), segments.as_slice()) {
            ("GET", "api.box.com", ["2.0", "folders", id, "items"]) => {
                let id = Self::id(id);
                if !self.drive.nodes.contains_key(&id) {
                    return error(404, "not_found");
                }
                let all: Vec<Json> = self.drive.children(&id).iter().map(|n| self.out(&n.id)).collect();
                let start = r.q("marker").and_then(|m| m.parse::<usize>().ok()).unwrap_or(0);
                let page: Vec<Json> = all.iter().skip(start).take(2).cloned().collect();
                let next = if start + 2 < all.len() {
                    json!((start + 2).to_string())
                } else {
                    json!(null)
                };
                Reply::json(200, &json!({ "entries": page, "limit": 2, "next_marker": next }))
            }
            ("GET", "api.box.com", ["2.0", "files", id, "content"]) => {
                let id = Self::id(id);
                Reply::empty(302).with(
                    "location",
                    &format!("http://127.0.0.1:{}/dl.boxcloud.com/d/{id}", self.port),
                )
            }
            ("POST", "upload.box.com", ["api", "2.0", "files", rest @ ..]) => {
                let parts = parts(r);
                if parts.first().map(|p| p.0.as_str()) != Some("attributes") {
                    return error(400, "metadata_after_file_contents");
                }
                let attributes: Json = serde_json::from_slice(&parts[0].1).unwrap();
                let bytes = parts.get(1).map(|p| p.1.clone()).unwrap_or_default();
                let id = match rest {
                    ["content"] => {
                        let parent = Self::id(attributes["parent"]["id"].as_str().unwrap());
                        let name = attributes["name"].as_str().unwrap();
                        if name_in_use(self, &parent, name).is_some() {
                            return error(409, "item_name_in_use");
                        }
                        self.drive.add(&parent, name, false, bytes, "")
                    }
                    [id, "content"] => {
                        let id = Self::id(id);
                        self.drive.put(&id, bytes);
                        id
                    }
                    _ => return error(404, "not_found"),
                };
                Reply::json(201, &json!({ "total_count": 1, "entries": [self.out(&id)] }))
            }
            ("POST", "api.box.com", ["2.0", "files", id, "copy"]) => {
                let parent = Self::id(body["parent"]["id"].as_str().unwrap());
                let name = body["name"].as_str().unwrap();
                if name_in_use(self, &parent, name).is_some() {
                    return error(409, "item_name_in_use");
                }
                let made = self.drive.copy(&Self::id(id), &parent, name);
                Reply::json(201, &self.out(&made))
            }
            ("PUT", "api.box.com", ["2.0", "files" | "folders", id]) => {
                let id = Self::id(id);
                let parent = Self::id(body["parent"]["id"].as_str().unwrap());
                let name = body["name"].as_str().unwrap();
                if name_in_use(self, &parent, name).is_some_and(|n| n != id) {
                    return error(409, "item_name_in_use");
                }
                self.drive.relocate(&id, &parent, name);
                Reply::json(200, &self.out(&id))
            }
            ("DELETE", "api.box.com", ["2.0", kind, id]) => {
                let id = Self::id(id);
                if !self.drive.nodes.get(&id).is_some_and(|n| !n.trashed) {
                    return error(404, "not_found");
                }
                if *kind == "folders" && !self.drive.children(&id).is_empty() {
                    return error(400, "folder_not_empty");
                }
                self.drive.remove(&id);
                Reply::empty(204)
            }
            ("POST", "api.box.com", ["2.0", "folders"]) => {
                let parent = Self::id(body["parent"]["id"].as_str().unwrap());
                let name = body["name"].as_str().unwrap();
                if name_in_use(self, &parent, name).is_some() {
                    return error(409, "item_name_in_use");
                }
                let id = self.drive.add(&parent, name, true, Vec::new(), "");
                Reply::json(201, &self.out(&id))
            }
            _ => error(404, "not_found"),
        }
    }
}
