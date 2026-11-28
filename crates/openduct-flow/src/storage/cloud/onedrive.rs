// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OneDrive and SharePoint document libraries over Microsoft Graph (v1.0): items addressed by path from the drive's
//! root or from a folder (`/root:/in/a.csv:`, `/items/{id}:/in/a.csv:`), folders listed (`/children`, following
//! `@odata.nextLink` on Graph's own host only), files downloaded (`/content`, the redirect to a pre-authenticated
//! address followed), uploaded (`PUT .../{parent-id}:/{name}:/content`, at most 250 MB, with
//! `@microsoft.graph.conflictBehavior` `fail` or `replace`), moved and renamed (`PATCH` of the item's
//! `parentReference` and `name`), deleted (to the recycle bin), and folders made (`POST .../children`) - with a
//! `microsoft_365` credential's token, as the Microsoft 365 app signs in. A copy is the file read and written again
//! (Graph's own copy finishes later, on its own).
//!
//! Which drive: the step's `drive` - `me` (the person's own OneDrive, by default), a drive's id, or `site:<site id>`
//! for a SharePoint site's document library; and `folder_id` for a folder the paths are inside.

use serde_json::{Value as Json, json};

use super::super::paths::Rel;
use super::super::remote::Listed;
use super::connected::Bearer;
use super::tree::{self, Item, Tree};
use super::web::{self, Answer};
use crate::files::FileData;

pub(crate) const GRAPH: &str = "https://graph.microsoft.com/v1.0";
pub(crate) const HOST: &str = "graph.microsoft.com";
/// The most pages of one folder read.
const MAX_PAGES: usize = 50;
const SELECT: &str = "id,name,size,lastModifiedDateTime,eTag,file,folder,parentReference,deleted";

pub(crate) struct Session<'a> {
    pub(crate) bearer: Bearer<'a>,
    /// The drive's address: `/me/drive`, `/drives/{id}`, `/sites/{id}/drive`.
    pub(crate) drive: String,
    /// The folder paths start from: `root`, or `items/{id}`.
    top: String,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("onedrive::Session")
            .field("drive", &self.drive)
            .finish_non_exhaustive()
    }
}

/// An id Graph gives a drive, a site or an item: letters, digits and `!,._-`.
fn id_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 300
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!,._-".contains(&b))
}

/// The drive a step names.
pub(crate) fn drive_of(drive: Option<&str>) -> Result<String, String> {
    match drive.map(str::trim).filter(|d| !d.is_empty()) {
        None | Some("me") => Ok("/me/drive".to_owned()),
        Some(d) => match d.strip_prefix("site:") {
            Some(site) if id_ok(site) => Ok(format!("/sites/{site}/drive")),
            Some(_) => {
                Err("drive: site:<the site's id>, as site:contoso.sharepoint.com,1a2b...,3c4d...".to_owned())
            }
            None if id_ok(d) => Ok(format!("/drives/{d}")),
            None => Err("drive: me, a drive's id, or site:<a SharePoint site's id>".to_owned()),
        },
    }
}

pub(crate) fn refusal(answer: &Answer, what: &str) -> String {
    let json = answer.json();
    let said = match (
        json.pointer("/error/code").and_then(Json::as_str),
        json.pointer("/error/message").and_then(Json::as_str),
    ) {
        (Some(c), Some(m)) => format!("{c} ({m})"),
        (Some(c), None) => c.to_owned(),
        _ => answer.said(200),
    };
    let shown: String = said.chars().take(300).collect();
    format!("Microsoft Graph refused to {what}: {} {shown}", answer.status)
}

fn code(answer: &Answer) -> String {
    answer
        .json()
        .pointer("/error/code")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// A driveItem as Graph gives it.
pub(crate) fn item(entry: &Json) -> Option<Item> {
    Some(Item {
        id: web::text(entry, "id")?.to_owned(),
        name: web::text(entry, "name").unwrap_or_default().to_owned(),
        folder: entry.get("folder").is_some(),
        size: web::number(entry, "size").unwrap_or(0),
        modified: web::text(entry, "lastModifiedDateTime").and_then(web::rfc3339_ms),
        version: web::text(entry, "eTag").unwrap_or_default().to_owned(),
        parent: entry
            .pointer("/parentReference/id")
            .and_then(Json::as_str)
            .map(str::to_owned),
    })
}

/// A path's names, each encoded for the address.
fn encoded(rel: &Rel) -> String {
    rel.parts
        .iter()
        .map(|p| web::encode(p, &[]))
        .collect::<Vec<_>>()
        .join("/")
}

impl Tree for Session<'_> {
    fn top(&self) -> String {
        self.top.clone()
    }

    async fn children(&mut self, id: &str) -> Result<Vec<Item>, String> {
        let mut url = format!(
            "{GRAPH}{}/{}/children?$top=200&$select={SELECT}",
            self.drive,
            self.at(id)
        );
        let mut out = Vec::new();
        for _ in 0..MAX_PAGES {
            let answer = self.bearer.json("GET", url, None).await?;
            if !answer.ok() {
                return Err(refusal(&answer, "list a folder"));
            }
            let page = answer.json();
            out.extend(
                page.get("value")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(item),
            );
            match web::text(&page, "@odata.nextLink") {
                Some(next) => url = web::on_host(next, HOST)?,
                None => break,
            }
        }
        Ok(out)
    }

    async fn child(&mut self, parent: &str, name: &str) -> Result<Option<Item>, String> {
        let url = format!(
            "{GRAPH}{}/{}:/{}:?$select={SELECT}",
            self.drive,
            self.at(parent),
            web::encode(name, &[])
        );
        self.get(url).await
    }

    async fn make(&mut self, parent: &str, name: &str) -> Result<Item, String> {
        let url = format!(
            "{GRAPH}{}/{}/children?@microsoft.graph.conflictBehavior=fail",
            self.drive,
            self.at(parent)
        );
        let answer = self
            .bearer
            .json("POST", url, Some(&json!({ "name": name, "folder": {} })))
            .await?;
        if answer.status == 409 {
            return self
                .child(parent, name)
                .await?
                .filter(|i| i.folder)
                .ok_or_else(|| format!("there is a file called {name} where a folder is to be made"));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("make the folder {name}")));
        }
        item(&answer.json()).ok_or_else(|| "Graph made the folder and did not say its id".to_owned())
    }
}

impl<'a> Session<'a> {
    pub(crate) fn new(
        bearer: Bearer<'a>,
        drive: Option<&str>,
        folder_id: Option<&str>,
    ) -> Result<Self, String> {
        let top = match folder_id.map(str::trim).filter(|i| !i.is_empty()) {
            None => "root".to_owned(),
            Some(id) if id_ok(id) => format!("items/{id}"),
            Some(_) => return Err("folder_id: a OneDrive folder's id".to_owned()),
        };
        Ok(Self {
            bearer,
            drive: drive_of(drive)?,
            top,
        })
    }

    /// An item's address within the drive: the top folder (`root`, `items/{id}`) or an item by its id.
    fn at(&self, id: &str) -> String {
        if id == self.top || id == "root" || id.starts_with("items/") {
            id.to_owned()
        } else {
            format!("items/{id}")
        }
    }

    /// An item by path from the top, in one request: the top itself for an empty path.
    fn by_path(&self, rel: &Rel) -> String {
        if rel.parts.is_empty() {
            format!("{GRAPH}{}/{}", self.drive, self.top)
        } else {
            format!("{GRAPH}{}/{}:/{}:", self.drive, self.top, encoded(rel))
        }
    }

    async fn get(&mut self, url: String) -> Result<Option<Item>, String> {
        let answer = self.bearer.json("GET", url, None).await?;
        if answer.status == 404 {
            return Ok(None);
        }
        if !answer.ok() {
            return Err(refusal(&answer, "look at an item"));
        }
        Ok(item(&answer.json()))
    }

    pub(crate) async fn find(&mut self, rel: &Rel) -> Result<Option<Item>, String> {
        let url = format!("{}?$select={SELECT}", self.by_path(rel));
        self.get(url).await
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let at = self
            .find(dir)
            .await?
            .ok_or_else(|| format!("there is no folder at {}", dir.text()))?;
        if !at.folder {
            return Err(format!("{} is not a folder", dir.text()));
        }
        let (all, more) = tree::walk(self, &at.id, dir, recursive, most).await?;
        Ok((all.into_iter().map(|(l, _)| l).collect(), more))
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let file = self
            .find(rel)
            .await?
            .ok_or_else(|| format!("there is nothing at {}", rel.text()))?;
        if file.folder {
            return Err(format!("{} is a folder, not a file", rel.text()));
        }
        let url = format!("{GRAPH}{}/items/{}/content", self.drive, file.id);
        let answer = self
            .bearer
            .send(web::request("GET", url, Vec::new(), None))
            .await?;
        if !answer.ok() {
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

    pub(crate) async fn write(&mut self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        let name = tree::named(rel, "file to write")?;
        let parent = tree::ensure(self, &rel.parent()).await?;
        let url = format!(
            "{GRAPH}{}/{}:/{}:/content?@microsoft.graph.conflictBehavior={}",
            self.drive,
            self.at(&parent),
            web::encode(&name, &[]),
            if overwrite { "replace" } else { "fail" }
        );
        let body = Some((crate::files::clean_type(&file.media_type), file.bytes.to_vec()));
        let answer = self
            .bearer
            .send(web::request("PUT", url, Vec::new(), body))
            .await?;
        if answer.status == 409 {
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

    /// Where a move goes: its folder's id and its name, a file there taken away first when `overwrite`.
    async fn target(&mut self, to: &Rel, overwrite: bool) -> Result<(String, String), String> {
        let name = tree::named(to, "place it goes")?;
        let parent = tree::ensure(self, &to.parent()).await?;
        match self.child(&parent, &name).await? {
            None => {}
            Some(there) if overwrite && !there.folder => self.remove(&there.id, to).await?,
            Some(_) => {
                return Err(format!(
                    "there is a file or folder at {} already: set overwrite to replace a file",
                    to.text()
                ));
            }
        }
        Ok((parent, name))
    }

    /// The id a folder is known by to Graph: the top's own id when it is the top.
    async fn real_id(&mut self, id: &str) -> Result<String, String> {
        if id == self.top {
            let top = self
                .find(&Rel::default())
                .await?
                .ok_or("the drive's top folder is not there")?;
            return Ok(top.id);
        }
        Ok(id.to_owned())
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        let moving = self
            .find(from)
            .await?
            .ok_or_else(|| format!("there is nothing at {}", from.text()))?;
        let (parent, name) = self.target(to, overwrite).await?;
        let parent = self.real_id(&parent).await?;
        let url = format!("{GRAPH}{}/items/{}", self.drive, moving.id);
        let answer = self
            .bearer
            .json(
                "PATCH",
                url,
                Some(&json!({ "name": name, "parentReference": { "id": parent } })),
            )
            .await?;
        if answer.status == 409 {
            return Err(format!("there is a file or folder at {} already", to.text()));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("move {}", from.text())));
        }
        Ok(())
    }

    pub(crate) async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let (bytes, _) = self.read(from, crate::files::MAX_FILE).await?;
        let name = from.name().unwrap_or("file");
        let file = FileData {
            name: name.to_owned(),
            media_type: crate::files::steps::guess_type(name).to_owned(),
            bytes: std::sync::Arc::new(bytes),
        };
        self.write(to, &file, overwrite).await?;
        Ok(u64::try_from(file.bytes.len()).unwrap_or(u64::MAX))
    }

    async fn remove(&mut self, id: &str, rel: &Rel) -> Result<(), String> {
        let url = format!("{GRAPH}{}/items/{id}", self.drive);
        let answer = self
            .bearer
            .send(web::request("DELETE", url, Vec::new(), None))
            .await?;
        if !answer.ok() && answer.status != 404 {
            return Err(refusal(&answer, &format!("delete {}", rel.text())));
        }
        Ok(())
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        if rel.parts.is_empty() {
            return Err("path: the file or empty folder to delete, not the top".to_owned());
        }
        let url = format!("{}?$select={SELECT}", self.by_path(rel));
        let answer = self.bearer.json("GET", url, None).await?;
        if answer.status == 404 {
            return Ok(false);
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("look at {}", rel.text())));
        }
        let found = answer.json();
        let children = found
            .pointer("/folder/childCount")
            .and_then(Json::as_u64)
            .unwrap_or(0);
        if children > 0 {
            return Err(format!(
                "{} is a folder with files in it: only an empty folder is deleted",
                rel.text()
            ));
        }
        let id = web::text(&found, "id")
            .ok_or("Graph did not say the item's id")?
            .to_owned();
        self.remove(&id, rel).await?;
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

/// Whether an answer says the delta's token is too old: start again from a listing.
pub(crate) fn resync(answer: &Answer) -> bool {
    answer.status == 410 || code(answer).starts_with("resync")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drives_and_items_are_named_as_graph_addresses_them() {
        assert_eq!(drive_of(None).unwrap(), "/me/drive");
        assert_eq!(drive_of(Some("b!AbC-1_x")).unwrap(), "/drives/b!AbC-1_x");
        assert_eq!(
            drive_of(Some("site:contoso.sharepoint.com,2C712604,9658")).unwrap(),
            "/sites/contoso.sharepoint.com,2C712604,9658/drive"
        );
        for bad in ["../x", "a/b", "site:", "site:a b", "x?y"] {
            assert!(drive_of(Some(bad)).is_err(), "{bad}");
        }
        let i = item(&json!({ "id": "1", "name": "a.csv", "size": 3, "eTag": "\"{X},2\"", "file": {}, "parentReference": { "id": "p" } })).unwrap();
        assert_eq!(
            (i.folder, i.parent.as_deref(), i.version.as_str()),
            (false, Some("p"), "\"{X},2\"")
        );
        assert_eq!(
            encoded(&Rel::parse("My Files/#9.docx", false).unwrap()),
            "My%20Files/%239.docx"
        );
    }
}
