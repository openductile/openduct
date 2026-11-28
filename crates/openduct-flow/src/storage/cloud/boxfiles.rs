// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Box over its API (2.0): a path walked name by name through each folder's items (`GET /folders/{id}/items`, marker
//! paging) to the ids Box works by; files downloaded (`GET /files/{id}/content`, the redirect followed), uploaded
//! (`upload.box.com`'s `POST /files/content`, or a new version), copied (`POST /files/{id}/copy`), moved and renamed
//! (`PUT /files/{id}`, `PUT /folders/{id}`), deleted (as the account keeps deleted items) and folders made (`POST /folders`) - with a
//! `box` credential's token (the studio's Connect, OAuth 2.0) or a `box_token` (a developer token, for an hour).
//! Paths start from the step's `folder_id`, or from All Files (`0`). Box matches names whatever their case.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value as Json, json};

use super::super::paths::Rel;
use super::super::remote::Listed;
use super::connected::Bearer;
use super::tree::{self, Item, Tree};
use super::web::{self, Answer};
use crate::files::FileData;

const API: &str = "https://api.box.com/2.0";
const UPLOAD: &str = "https://upload.box.com/api/2.0";
const FIELDS: &str = "id,type,name,size,modified_at,etag,sha1";
/// The most pages of one folder read.
const MAX_PAGES: usize = 20;

pub(crate) struct Session<'a> {
    bearer: Bearer<'a>,
    top: String,
    /// Each folder's items, as read in this step.
    seen: HashMap<String, Vec<Item>>,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("boxfiles::Session")
            .field("top", &self.top)
            .finish_non_exhaustive()
    }
}

fn refusal(answer: &Answer, what: &str) -> String {
    let json = answer.json();
    let said = match (web::text(&json, "code"), web::text(&json, "message")) {
        (Some(c), Some(m)) => format!("{c} ({m})"),
        (Some(c), None) => c.to_owned(),
        _ => answer.said(200),
    };
    let shown: String = said.chars().take(300).collect();
    format!("Box refused to {what}: {} {shown}", answer.status)
}

fn code(answer: &Answer) -> String {
    web::text(&answer.json(), "code").unwrap_or_default().to_owned()
}

fn item(entry: &Json) -> Option<Item> {
    let kind = web::text(entry, "type")?;
    if kind != "file" && kind != "folder" {
        return None;
    }
    Some(Item {
        id: web::text(entry, "id")?.to_owned(),
        name: web::text(entry, "name")?.to_owned(),
        folder: kind == "folder",
        size: web::number(entry, "size").unwrap_or(0),
        modified: web::text(entry, "modified_at").and_then(web::rfc3339_ms),
        version: web::text(entry, "sha1")
            .or_else(|| web::text(entry, "etag"))
            .unwrap_or_default()
            .to_owned(),
        parent: None,
    })
}

/// An id Box gives: digits.
fn id_ok(id: &str) -> bool {
    !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_digit())
}

impl Tree for Session<'_> {
    fn top(&self) -> String {
        self.top.clone()
    }

    async fn children(&mut self, id: &str) -> Result<Vec<Item>, String> {
        if let Some(known) = self.seen.get(id) {
            return Ok(known.clone());
        }
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut q = vec![
                ("fields", FIELDS.to_owned()),
                ("limit", "1000".to_owned()),
                ("usemarker", "true".to_owned()),
            ];
            if let Some(m) = &marker {
                q.push(("marker", m.clone()));
            }
            let answer = self
                .bearer
                .json(
                    "GET",
                    format!("{API}/folders/{id}/items?{}", web::query(&q)),
                    None,
                )
                .await?;
            if answer.status == 404 {
                return Err(format!("Box has no folder {id} this credential may see"));
            }
            if !answer.ok() {
                return Err(refusal(&answer, "list a folder"));
            }
            let page = answer.json();
            out.extend(
                page.get("entries")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(item),
            );
            match web::text(&page, "next_marker") {
                Some(next) if !next.is_empty() => marker = Some(next.to_owned()),
                _ => break,
            }
        }
        self.seen.insert(id.to_owned(), out.clone());
        Ok(out)
    }

    async fn make(&mut self, parent: &str, name: &str) -> Result<Item, String> {
        let answer = self
            .bearer
            .json(
                "POST",
                format!("{API}/folders?fields={FIELDS}"),
                Some(&json!({ "name": name, "parent": { "id": parent } })),
            )
            .await?;
        if answer.status == 409 && code(&answer) == "item_name_in_use" {
            self.seen.remove(parent);
            return self
                .child(parent, name)
                .await?
                .filter(|i| i.folder)
                .ok_or_else(|| format!("there is a file called {name} where a folder is to be made"));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("make the folder {name}")));
        }
        self.seen.remove(parent);
        item(&answer.json()).ok_or_else(|| "Box made the folder and did not say its id".to_owned())
    }
}

impl<'a> Session<'a> {
    pub(crate) fn new(bearer: Bearer<'a>, folder_id: Option<&str>) -> Result<Self, String> {
        let top = folder_id
            .map(str::trim)
            .filter(|i| !i.is_empty())
            .unwrap_or("0")
            .to_owned();
        if !id_ok(&top) {
            return Err("folder_id: a Box folder's id, the digits at the end of its address".to_owned());
        }
        Ok(Self {
            bearer,
            top,
            seen: HashMap::new(),
        })
    }

    async fn found(&mut self, rel: &Rel) -> Result<Item, String> {
        tree::find(self, rel)
            .await?
            .ok_or_else(|| format!("there is nothing at {}", rel.text()))
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let at = self.found(dir).await?;
        if !at.folder {
            return Err(format!("{} is not a folder", dir.text()));
        }
        let (all, more) = tree::walk(self, &at.id, dir, recursive, most).await?;
        Ok((all.into_iter().map(|(l, _)| l).collect(), more))
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let file = self.found(rel).await?;
        if file.folder {
            return Err(format!("{} is a folder, not a file", rel.text()));
        }
        let url = format!("{API}/files/{}/content", file.id);
        let mut answer = self
            .bearer
            .send(web::request("GET", url.clone(), Vec::new(), None))
            .await?;
        // A file uploaded a moment ago may not be ready: Box says when to ask again.
        if answer.status == 202 {
            let wait = answer
                .header("retry-after")
                .and_then(|r| r.parse().ok())
                .unwrap_or(1u64)
                .min(5);
            tokio::time::sleep(Duration::from_secs(wait)).await;
            answer = self
                .bearer
                .send(web::request("GET", url, Vec::new(), None))
                .await?;
        }
        if !answer.ok() || answer.status == 202 {
            return Err(refusal(&answer, &format!("read {}", rel.text())));
        }
        if answer.body.len() > most {
            return Err(format!(
                "{} is more than {most} bytes: a run reads files of at most that",
                rel.text()
            ));
        }
        Ok((answer.body, file.modified))
    }

    async fn upload(
        &mut self,
        url: String,
        attributes: Json,
        name: &str,
        file: &FileData,
    ) -> Result<Answer, String> {
        let attributes = attributes.to_string();
        let (kind, body) = web::form_data(&[
            ("attributes", None, "application/json", attributes.as_bytes()),
            ("file", Some(name), "application/octet-stream", &file.bytes),
        ])?;
        self.bearer
            .send(web::request("POST", url, Vec::new(), Some((kind, body))))
            .await
    }

    pub(crate) async fn write(&mut self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        let name = tree::named(rel, "file to write")?;
        let parent = tree::ensure(self, &rel.parent()).await?;
        let answer = match self.child(&parent, &name).await? {
            Some(there) if there.folder => return Err(format!("{} is a folder", rel.text())),
            Some(_) if !overwrite => {
                return Err(format!(
                    "there is a file at {} already: set overwrite to replace it",
                    rel.text()
                ));
            }
            Some(there) => {
                let url = format!("{UPLOAD}/files/{}/content", there.id);
                self.upload(url, json!({ "name": name }), &name, file).await?
            }
            None => {
                let url = format!("{UPLOAD}/files/content");
                self.upload(
                    url,
                    json!({ "name": name, "parent": { "id": parent } }),
                    &name,
                    file,
                )
                .await?
            }
        };
        self.seen.remove(&parent);
        if answer.status == 409 && code(&answer) == "item_name_in_use" {
            return Err(format!(
                "there is a file at {} already: set overwrite to replace it",
                rel.text()
            ));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("store {}", rel.text())));
        }
        Ok(())
    }

    /// The folder a move or a copy goes into, and its name there; something there already is refused, unless
    /// `overwrite` and it is a file (then it goes first).
    async fn target(&mut self, to: &Rel, overwrite: bool) -> Result<(String, String), String> {
        let name = tree::named(to, "place it goes")?;
        let parent = tree::ensure(self, &to.parent()).await?;
        match self.child(&parent, &name).await? {
            None => {}
            Some(there) if overwrite && !there.folder => {
                self.remove(&there, to).await?;
                self.seen.remove(&parent);
            }
            Some(_) => {
                return Err(format!(
                    "there is a file or folder at {} already: set overwrite to replace a file",
                    to.text()
                ));
            }
        }
        Ok((parent, name))
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        let moving = self.found(from).await?;
        let (parent, name) = self.target(to, overwrite).await?;
        let kind = if moving.folder { "folders" } else { "files" };
        let answer = self
            .bearer
            .json(
                "PUT",
                format!("{API}/{kind}/{}", moving.id),
                Some(&json!({ "name": name, "parent": { "id": parent } })),
            )
            .await?;
        self.seen.clear();
        if answer.status == 409 {
            return Err(format!("there is a file or folder at {} already", to.text()));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("move {}", from.text())));
        }
        Ok(())
    }

    pub(crate) async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let source = self.found(from).await?;
        if source.folder {
            return Err(format!("{} is a folder: a copy is of a file", from.text()));
        }
        let (parent, name) = self.target(to, overwrite).await?;
        let answer = self
            .bearer
            .json(
                "POST",
                format!("{API}/files/{}/copy?fields=id,name,size", source.id),
                Some(&json!({ "name": name, "parent": { "id": parent } })),
            )
            .await?;
        self.seen.remove(&parent);
        if answer.status == 409 {
            return Err(format!("there is a file at {} already", to.text()));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("copy {}", from.text())));
        }
        Ok(web::number(&answer.json(), "size").unwrap_or(source.size))
    }

    async fn remove(&mut self, it: &Item, rel: &Rel) -> Result<(), String> {
        let url = if it.folder {
            format!("{API}/folders/{}?recursive=false", it.id)
        } else {
            format!("{API}/files/{}", it.id)
        };
        let answer = self
            .bearer
            .send(web::request("DELETE", url, Vec::new(), None))
            .await?;
        if answer.status == 400 && code(&answer) == "folder_not_empty" {
            return Err(format!(
                "{} is a folder with files in it: only an empty folder is deleted",
                rel.text()
            ));
        }
        if !answer.ok() && answer.status != 404 {
            return Err(refusal(&answer, &format!("delete {}", rel.text())));
        }
        Ok(())
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        if rel.parts.is_empty() {
            return Err("path: the file or empty folder to delete, not the top".to_owned());
        }
        let Some(it) = tree::find(self, rel).await? else {
            return Ok(false);
        };
        self.remove(&it, rel).await?;
        self.seen.clear();
        Ok(true)
    }

    pub(crate) async fn make_folder(&mut self, rel: &Rel) -> Result<bool, String> {
        let name = tree::named(rel, "folder to make")?;
        let parent = tree::ensure(self, &rel.parent()).await?;
        match self.child(&parent, &name).await? {
            Some(there) if there.folder => Ok(false),
            Some(_) => Err(format!("there is a file at {} already", rel.text())),
            None => self.make(&parent, &name).await.map(|_| true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_are_read_as_box_writes_them() {
        let f = item(&json!({ "type": "file", "id": "12", "name": "a.csv", "size": 3, "modified_at": "2026-11-27T10:00:00-08:00", "sha1": "abc" })).unwrap();
        assert_eq!(
            (f.id.as_str(), f.folder, f.size, f.version.as_str()),
            ("12", false, 3, "abc")
        );
        assert!(f.modified.is_some());
        assert!(item(&json!({ "type": "web_link", "id": "1", "name": "x" })).is_none());
        assert!(id_ok("0") && id_ok("123") && !id_ok("12a") && !id_ok("../1"));
    }
}
