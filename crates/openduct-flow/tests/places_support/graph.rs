// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Microsoft Graph drive stand-in (v1.0): items by path (`/root:/a/b:`) or by id (`/items/{id}`, `/items/{id}:/a:`)
//! in the person's drive, another drive or a site's; children two at a time (`@odata.nextLink` on Graph's host); a
//! download answered with a redirect to a pre-authenticated address; uploads with `@microsoft.graph.conflictBehavior`;
//! and `delta` - `token=latest` to start from now, then each item changed since (its latest state, a deleted one
//! with the `deleted` facet, no path in its `parentReference`, as Graph's delta gives).

use serde_json::{Value as Json, json};

use super::http::{Reply, Request, decode};
use super::tree::Drive;

pub(crate) const TOKEN: &str = "ms-standin";

pub(crate) struct Graph {
    pub drive: Drive,
    port: u16,
    /// The drives asked for, as their addresses began.
    pub drives: Vec<String>,
    pub resync_next: bool,
}

fn error(status: u16, code: &str) -> Reply {
    Reply::json(
        status,
        &json!({ "error": { "code": code, "message": format!("{code} here") } }),
    )
}

impl Graph {
    pub(crate) fn new(port: u16) -> Self {
        Self {
            drive: Drive::new("ROOT"),
            port,
            drives: Vec::new(),
            resync_next: false,
        }
    }

    pub(crate) fn item(&self, id: &str, delta: bool) -> Json {
        let n = &self.drive.nodes[id];
        if n.trashed {
            return json!({ "id": id, "deleted": { "state": "deleted" }, "parentReference": { "id": n.parent } });
        }
        let mut item = json!({
            "id": id, "name": if id == "ROOT" { "root" } else { n.name.as_str() }, "size": n.bytes.len(),
            "eTag": format!("\"{{{id}}},{}\"", n.version), "lastModifiedDateTime": super::iso(n.modified),
        });
        if n.folder {
            item["folder"] = json!({ "childCount": self.drive.children(id).len() });
        } else {
            item["file"] = json!({ "mimeType": "application/octet-stream" });
        }
        if !n.parent.is_empty() {
            item["parentReference"] = if delta {
                json!({ "id": n.parent, "driveId": "D1" })
            } else {
                json!({ "id": n.parent, "driveId": "D1", "path": format!("/drive/root:/{}", self.drive.path_of("ROOT", &n.parent).unwrap_or_default()) })
            };
        }
        item
    }

    /// The node an address names (`/root`, `/items/{id}`, either followed by `:/path:`), and what is after it.
    fn resolve(&self, rest: &str) -> Result<(Option<String>, String, String), Reply> {
        let (base, after) = if let Some(r) = rest.strip_prefix("/root") {
            ("ROOT".to_owned(), r.to_owned())
        } else if let Some(r) = rest.strip_prefix("/items/") {
            let end = r.find([':', '/']).unwrap_or(r.len());
            (r[..end].to_owned(), r[end..].to_owned())
        } else {
            return Err(error(400, "invalidRequest"));
        };
        if !self.drive.nodes.contains_key(&base) {
            return Err(error(404, "itemNotFound"));
        }
        if let Some(path) = after.strip_prefix(":/") {
            let (path, tail) = path.split_once(':').unwrap_or((path, ""));
            let path = decode(path);
            let found = self.drive.at(&base, &path, true).map(|n| n.id.clone());
            let (parent, name) = match path.rsplit_once('/') {
                Some((p, n)) => (self.drive.at(&base, p, true).map(|n| n.id.clone()), n.to_owned()),
                None => (Some(base.clone()), path.clone()),
            };
            return match found {
                Some(id) => Ok((Some(id), tail.to_owned(), name)),
                None => match parent {
                    Some(p) => Ok((None, tail.to_owned(), format!("{p}\u{0}{name}"))),
                    None => Err(error(404, "itemNotFound")),
                },
            };
        }
        Ok((Some(base), after, String::new()))
    }

    pub(crate) fn handle(&mut self, r: &Request) -> Reply {
        if r.host == "files.1drv.com" {
            return Reply::bytes(
                200,
                "application/octet-stream",
                self.drive.nodes[r.path.trim_start_matches('/')].bytes.clone(),
            );
        }
        if r.bearer() != Some(TOKEN) {
            return error(401, "InvalidAuthenticationToken");
        }
        let Some(rest) = r.path.strip_prefix("/v1.0") else {
            return error(400, "invalidRequest");
        };
        let (drive, rest) = if let Some(x) = rest.strip_prefix("/me/drive") {
            ("/me/drive".to_owned(), x)
        } else if let Some(x) = rest.strip_prefix("/drives/") {
            let end = x.find('/').unwrap_or(x.len());
            (format!("/drives/{}", &x[..end]), &x[end..])
        } else if let Some(x) = rest.strip_prefix("/sites/") {
            let end = x.find("/drive").unwrap_or(x.len());
            (format!("/sites/{}/drive", &x[..end]), &x[end + 6..])
        } else {
            return error(400, "invalidRequest");
        };
        if !self.drives.contains(&drive) {
            self.drives.push(drive.clone());
        }
        if rest == "/root/delta" {
            return self.delta(r, &drive);
        }
        let (found, tail, missing) = match self.resolve(rest) {
            Ok(x) => x,
            Err(e) => return e,
        };
        let body = r.json();
        match (r.method.as_str(), tail.as_str(), found) {
            ("GET", "", Some(id)) => Reply::json(200, &self.item(&id, false)),
            ("GET", "/children", Some(id)) => {
                let all: Vec<Json> = self
                    .drive
                    .children(&id)
                    .iter()
                    .map(|n| self.item(&n.id, false))
                    .collect();
                let start = r
                    .q("$skiptoken")
                    .and_then(|t| t.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut page =
                    json!({ "value": all.iter().skip(start).take(2).cloned().collect::<Vec<_>>() });
                if start + 2 < all.len() {
                    page["@odata.nextLink"] = json!(format!(
                        "https://graph.microsoft.com/v1.0{drive}/items/{id}/children?$skiptoken={}",
                        start + 2
                    ));
                }
                Reply::json(200, &page)
            }
            ("GET", "/content", Some(id)) => Reply::empty(302).with(
                "location",
                &format!("http://127.0.0.1:{}/files.1drv.com/{id}", self.port),
            ),
            ("PUT", "/content", found) => {
                let id = match found {
                    Some(id) if r.q("@microsoft.graph.conflictBehavior") == Some("fail") => {
                        let _ = id;
                        return error(409, "nameAlreadyExists");
                    }
                    Some(id) => {
                        self.drive.put(&id, r.body.clone());
                        id
                    }
                    None => {
                        let (parent, name) = missing.split_once('\u{0}').unwrap();
                        self.drive.add(parent, name, false, r.body.clone(), "")
                    }
                };
                Reply::json(201, &self.item(&id, false))
            }
            ("POST", "/children", Some(parent)) => {
                let name = body["name"].as_str().unwrap();
                if self.drive.child(&parent, name, true).is_some() {
                    return error(409, "nameAlreadyExists");
                }
                let id = self.drive.add(&parent, name, true, Vec::new(), "");
                Reply::json(201, &self.item(&id, false))
            }
            ("PATCH", "", Some(id)) => {
                let parent = body["parentReference"]["id"].as_str().unwrap().to_owned();
                let name = body["name"].as_str().unwrap();
                if self.drive.child(&parent, name, true).is_some_and(|n| n.id != id) {
                    return error(409, "nameAlreadyExists");
                }
                self.drive.relocate(&id, &parent, name);
                Reply::json(200, &self.item(&id, false))
            }
            ("DELETE", "", Some(id)) => {
                self.drive.remove(&id);
                Reply::empty(204)
            }
            (_, _, None) => error(404, "itemNotFound"),
            _ => error(400, "invalidRequest"),
        }
    }

    fn delta(&mut self, r: &Request, drive: &str) -> Reply {
        let link =
            |token: String| format!("https://graph.microsoft.com/v1.0{drive}/root/delta?token={token}");
        let token = r.q("token").unwrap_or_default().to_owned();
        if token == "latest" {
            return Reply::json(
                200,
                &json!({ "value": [], "@odata.deltaLink": link(self.drive.log.len().to_string()) }),
            );
        }
        if std::mem::take(&mut self.resync_next) {
            return error(410, "resyncChangesApplyDifferences");
        }
        let (since, offset) = token.split_once('-').unwrap_or((&token, "0"));
        let (since, offset): (usize, usize) = (since.parse().unwrap(), offset.parse().unwrap());
        let mut changed: Vec<String> = Vec::new();
        for (id, _) in self.drive.log.iter().skip(since) {
            changed.retain(|c| c != id);
            changed.push(id.clone());
        }
        let page: Vec<Json> = changed
            .iter()
            .skip(offset)
            .take(2)
            .map(|id| self.item(id, true))
            .collect();
        let mut body = json!({ "value": page });
        if offset + 2 < changed.len() {
            body["@odata.nextLink"] = json!(link(format!("{since}-{}", offset + 2)));
        } else {
            body["@odata.deltaLink"] = json!(link(self.drive.log.len().to_string()));
        }
        Reply::json(200, &body)
    }
}
