// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Google Drive over its API (v3): a path walked name by name (`files.list` with `'<folder>' in parents and name =
//! '<name>' and trashed = false`, the name's `'` and `\` escaped as Drive asks), files downloaded (`alt=media`) or -
//! Google Docs, Sheets, Slides and Drawings, which hold no bytes of their own - exported (`files.export`, at most
//! 10 MB) as the step's `export` says; uploaded (a resumable upload, its session address followed only on Google's
//! own host), copied (`files.copy`), moved and renamed (`addParents` / `removeParents`), deleted (moved to the
//! trash, so it can be had back) and folders made - with a `google_drive` credential's
//! token, as the Google Drive app signs in. Paths start from the step's `folder_id` (a folder's id, one of a shared
//! drive's too), or from My Drive (`root`). Drive lets two files have one name: a path names the one changed last.

use serde_json::{Value as Json, json};

use super::super::paths::Rel;
use super::super::remote::Listed;
use super::connected::Bearer;
use super::tree::{self, Item, Tree};
use super::web::{self, Answer};
use crate::files::FileData;

pub(crate) const API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD: &str = "https://www.googleapis.com/upload/drive/v3";
pub(crate) const HOST: &str = "www.googleapis.com";
pub(crate) const FOLDER: &str = "application/vnd.google-apps.folder";
pub(crate) const FIELDS: &str = "id,name,mimeType,size,modifiedTime,md5Checksum,version,parents,trashed";
/// The most pages of one folder read.
const MAX_PAGES: usize = 20;

/// What a Google Docs editors file is read as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Export {
    /// Word, Excel, PowerPoint (a drawing: PDF).
    #[default]
    Office,
    Pdf,
    /// Plain text; a sheet as CSV (its first sheet).
    Text,
    /// OpenDocument.
    Open,
}

impl Export {
    pub(crate) const ALL: &'static [&'static str] = &["office", "pdf", "text", "open"];

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "office" => Self::Office,
            "pdf" => Self::Pdf,
            "text" => Self::Text,
            "open" => Self::Open,
            _ => return None,
        })
    }

    /// The type a file of Google type `kind` is exported as, and the end its name gets.
    fn of(self, kind: &str) -> Option<(&'static str, &'static str)> {
        const PDF: (&str, &str) = ("application/pdf", "pdf");
        let doc = match kind.strip_prefix("application/vnd.google-apps.")? {
            "document" => match self {
                Self::Office => (
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "docx",
                ),
                Self::Pdf => PDF,
                Self::Text => ("text/plain", "txt"),
                Self::Open => ("application/vnd.oasis.opendocument.text", "odt"),
            },
            "spreadsheet" => match self {
                Self::Office => (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "xlsx",
                ),
                Self::Pdf => PDF,
                Self::Text => ("text/csv", "csv"),
                Self::Open => ("application/vnd.oasis.opendocument.spreadsheet", "ods"),
            },
            "presentation" => match self {
                Self::Office => (
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                    "pptx",
                ),
                Self::Pdf => PDF,
                Self::Text => ("text/plain", "txt"),
                Self::Open => ("application/vnd.oasis.opendocument.presentation", "odp"),
            },
            "drawing" => PDF,
            _ => return None,
        };
        Some(doc)
    }
}

pub(crate) struct Session<'a> {
    pub(crate) bearer: Bearer<'a>,
    top: String,
    pub(crate) export: Export,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("gdrive::Session")
            .field("top", &self.top)
            .finish_non_exhaustive()
    }
}

/// An id Drive gives: letters, digits, `-` and `_` (or `root`).
pub(crate) fn id_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// A text in a Drive query, between single quotes: `\` and `'` escaped.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

pub(crate) fn refusal(answer: &Answer, what: &str) -> String {
    let json = answer.json();
    let said = json
        .pointer("/error/message")
        .and_then(Json::as_str)
        .map_or_else(|| answer.said(200), str::to_owned);
    let shown: String = said.chars().take(300).collect();
    format!("Google Drive refused to {what}: {} {shown}", answer.status)
}

/// A file as Drive gives it.
pub(crate) fn item(f: &Json) -> Option<Item> {
    let kind = web::text(f, "mimeType").unwrap_or_default();
    Some(Item {
        id: web::text(f, "id")?.to_owned(),
        name: web::text(f, "name").unwrap_or_default().to_owned(),
        folder: kind == FOLDER,
        size: web::number(f, "size").unwrap_or(0),
        modified: web::text(f, "modifiedTime").and_then(web::rfc3339_ms),
        version: web::text(f, "md5Checksum")
            .or_else(|| web::text(f, "version"))
            .unwrap_or_default()
            .to_owned(),
        parent: f
            .get("parents")
            .and_then(Json::as_array)
            .and_then(|p| p.first())
            .and_then(Json::as_str)
            .map(str::to_owned),
    })
}

impl Tree for Session<'_> {
    fn top(&self) -> String {
        self.top.clone()
    }

    async fn children(&mut self, id: &str) -> Result<Vec<Item>, String> {
        self.search(
            &format!("{} in parents and trashed = false", quoted(id)),
            1000,
            MAX_PAGES,
        )
        .await
    }

    async fn child(&mut self, parent: &str, name: &str) -> Result<Option<Item>, String> {
        let q = format!(
            "{} in parents and name = {} and trashed = false",
            quoted(parent),
            quoted(name)
        );
        Ok(self.search(&q, 10, 1).await?.into_iter().next())
    }

    async fn make(&mut self, parent: &str, name: &str) -> Result<Item, String> {
        let url = format!(
            "{API}/files?supportsAllDrives=true&fields={}",
            web::encode(FIELDS, &[])
        );
        let answer = self
            .bearer
            .json(
                "POST",
                url,
                Some(&json!({ "name": name, "mimeType": FOLDER, "parents": [parent] })),
            )
            .await?;
        if !answer.ok() {
            return Err(refusal(&answer, &format!("make the folder {name}")));
        }
        item(&answer.json()).ok_or_else(|| "Drive made the folder and did not say its id".to_owned())
    }
}

impl<'a> Session<'a> {
    pub(crate) fn new(bearer: Bearer<'a>, folder_id: Option<&str>, export: Export) -> Result<Self, String> {
        let top = folder_id
            .map(str::trim)
            .filter(|i| !i.is_empty())
            .unwrap_or("root")
            .to_owned();
        if !id_ok(&top) {
            return Err("folder_id: a Drive folder's id, the end of its address".to_owned());
        }
        Ok(Self { bearer, top, export })
    }

    /// The files a query finds, the one changed last first.
    async fn search(&mut self, q: &str, size: usize, pages: usize) -> Result<Vec<Item>, String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..pages {
            let mut params = vec![
                ("q", q.to_owned()),
                ("pageSize", size.to_string()),
                ("orderBy", "modifiedTime desc".to_owned()),
                ("fields", format!("nextPageToken,files({FIELDS})")),
                ("supportsAllDrives", "true".to_owned()),
                ("includeItemsFromAllDrives", "true".to_owned()),
            ];
            if let Some(t) = &token {
                params.push(("pageToken", t.clone()));
            }
            let answer = self
                .bearer
                .json("GET", format!("{API}/files?{}", web::query(&params)), None)
                .await?;
            if !answer.ok() {
                return Err(refusal(&answer, "find files"));
            }
            let page = answer.json();
            out.extend(
                page.get("files")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(item),
            );
            match web::text(&page, "nextPageToken") {
                Some(next) if !next.is_empty() => token = Some(next.to_owned()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn file(&mut self, id: &str) -> Result<Option<Json>, String> {
        let url = format!(
            "{API}/files/{id}?supportsAllDrives=true&fields={}",
            web::encode(FIELDS, &[])
        );
        let answer = self.bearer.json("GET", url, None).await?;
        if answer.status == 404 {
            return Ok(None);
        }
        if !answer.ok() {
            return Err(refusal(&answer, "look at a file"));
        }
        Ok(Some(answer.json()))
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

    /// A file's bytes, its time, and its name when an export gives it another (`Report` read as `Report.docx`).
    pub(crate) async fn read(
        &mut self,
        rel: &Rel,
        most: usize,
    ) -> Result<(Vec<u8>, Option<i64>, Option<String>), String> {
        let found = self.found(rel).await?;
        if found.folder {
            return Err(format!("{} is a folder, not a file", rel.text()));
        }
        let meta = self.file(&found.id).await?.unwrap_or_default();
        let kind = web::text(&meta, "mimeType").unwrap_or_default().to_owned();
        let (url, name) = match self.export.of(&kind) {
            Some((to, end)) => (
                format!(
                    "{API}/files/{}/export?mimeType={}",
                    found.id,
                    web::encode(to, &[])
                ),
                Some(format!("{}.{end}", found.name)),
            ),
            None if kind.starts_with("application/vnd.google-apps.") => {
                return Err(format!(
                    "{} is a Google {kind} file, which holds nothing to read",
                    rel.text()
                ));
            }
            None => (
                format!("{API}/files/{}?alt=media&supportsAllDrives=true", found.id),
                None,
            ),
        };
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
        Ok((answer.body, found.modified, name))
    }

    /// Bytes sent to a resumable upload: the session opened (`init`, with its metadata), then the bytes put there.
    async fn upload(
        &mut self,
        method: &str,
        init: String,
        metadata: &Json,
        file: &FileData,
    ) -> Result<Answer, String> {
        let kind = crate::files::clean_type(&file.media_type);
        let opened = self
            .bearer
            .send(web::request(
                method,
                init,
                vec![
                    ("x-upload-content-type".to_owned(), kind.clone()),
                    ("x-upload-content-length".to_owned(), file.bytes.len().to_string()),
                ],
                Some((
                    "application/json; charset=UTF-8".to_owned(),
                    metadata.to_string().into_bytes(),
                )),
            ))
            .await?;
        if !opened.ok() {
            return Ok(opened);
        }
        let session = opened
            .header("location")
            .ok_or("Drive opened no upload")
            .and_then(|l| web::on_host(l, HOST).map_err(|_| "Drive's upload address is not on its host"))?;
        self.bearer
            .send(web::request(
                "PUT",
                session,
                Vec::new(),
                Some((kind, file.bytes.to_vec())),
            ))
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
                let init = format!(
                    "{UPLOAD}/files/{}?uploadType=resumable&supportsAllDrives=true",
                    there.id
                );
                self.upload("PATCH", init, &json!({}), file).await?
            }
            None => {
                let init = format!("{UPLOAD}/files?uploadType=resumable&supportsAllDrives=true");
                self.upload("POST", init, &json!({ "name": name, "parents": [parent] }), file)
                    .await?
            }
        };
        if !answer.ok() {
            return Err(refusal(&answer, &format!("store {}", rel.text())));
        }
        Ok(())
    }

    /// Where a move or a copy goes, a file there trashed first when `overwrite`.
    async fn target(&mut self, to: &Rel, overwrite: bool) -> Result<(String, String), String> {
        let name = tree::named(to, "place it goes")?;
        let parent = tree::ensure(self, &to.parent()).await?;
        match self.child(&parent, &name).await? {
            None => {}
            Some(there) if overwrite && !there.folder => self.trash(&there.id, to).await?,
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
        let mut params = vec![("supportsAllDrives", "true".to_owned())];
        let old = moving.parent.clone().unwrap_or_default();
        if old != parent {
            params.push(("addParents", parent.clone()));
            if !old.is_empty() {
                params.push(("removeParents", old));
            }
        }
        let url = format!("{API}/files/{}?{}", moving.id, web::query(&params));
        let answer = self
            .bearer
            .json("PATCH", url, Some(&json!({ "name": name })))
            .await?;
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
        let url = format!(
            "{API}/files/{}/copy?supportsAllDrives=true&fields=id,size",
            source.id
        );
        let answer = self
            .bearer
            .json("POST", url, Some(&json!({ "name": name, "parents": [parent] })))
            .await?;
        if !answer.ok() {
            return Err(refusal(&answer, &format!("copy {}", from.text())));
        }
        Ok(web::number(&answer.json(), "size").unwrap_or(source.size))
    }

    async fn trash(&mut self, id: &str, rel: &Rel) -> Result<(), String> {
        let url = format!("{API}/files/{id}?supportsAllDrives=true");
        let answer = self
            .bearer
            .json("PATCH", url, Some(&json!({ "trashed": true })))
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
        let Some(it) = tree::find(self, rel).await? else {
            return Ok(false);
        };
        if it.folder {
            let q = format!("{} in parents and trashed = false", quoted(&it.id));
            if !self.search(&q, 1, 1).await?.is_empty() {
                return Err(format!(
                    "{} is a folder with files in it: only an empty folder is deleted",
                    rel.text()
                ));
            }
        }
        self.trash(&it.id, rel).await?;
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
    fn queries_escape_names_and_docs_export_as_asked() {
        assert_eq!(quoted("quinn's paper\\essay"), r"'quinn\'s paper\\essay'");
        let doc = "application/vnd.google-apps.document";
        assert_eq!(Export::Office.of(doc).unwrap().1, "docx");
        assert_eq!(
            Export::Text
                .of("application/vnd.google-apps.spreadsheet")
                .unwrap(),
            ("text/csv", "csv")
        );
        assert_eq!(
            Export::Office
                .of("application/vnd.google-apps.drawing")
                .unwrap()
                .0,
            "application/pdf"
        );
        assert!(Export::Pdf.of("application/vnd.google-apps.form").is_none());
        assert!(Export::Pdf.of("text/csv").is_none());
        assert!(id_ok("root") && id_ok("1AbC-d_9") && !id_ok("a'b") && !id_ok("a b"));
        let f = item(&json!({ "id": "f1", "name": "a.csv", "mimeType": "text/csv", "size": "12", "md5Checksum": "m", "parents": ["p1"] })).unwrap();
        assert_eq!(
            (f.size, f.version.as_str(), f.parent.as_deref(), f.folder),
            (12, "m", Some("p1"), false)
        );
    }
}
