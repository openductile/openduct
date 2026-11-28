// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Dropbox over its HTTP API (v2): files listed (`files/list_folder`, then `/continue` while it has more), got
//! (`files/download`, its arguments in the `Dropbox-API-Arg` header, written as HTTP-header-safe JSON), put
//! (`files/upload`: `add`, strictly, or `overwrite`), moved and copied (`files/move_v2`, `files/copy_v2`), deleted
//! (`files/delete_v2`) and folders made (`files/create_folder_v2`) - with the token of a `dropbox` credential, which
//! the studio's Connect makes with the install's own Dropbox app. Paths are Dropbox's own (`/Invoices/a.pdf`), under
//! the folder whose id the step's `folder_id` gives (`id:...`), or from the top of the Dropbox (or the app's own
//! folder, for an app with that access).

use serde_json::{Value as Json, json};

use super::super::paths::Rel;
use super::super::remote::Listed;
use super::connected::Bearer;
use super::web::{self, Answer};
use crate::files::FileData;

pub(crate) const API: &str = "https://api.dropboxapi.com/2";
const CONTENT: &str = "https://content.dropboxapi.com/2";
/// The most pages a listing reads.
pub(crate) const MAX_PAGES: usize = 20;

pub(crate) struct Session<'a> {
    pub(crate) bearer: Bearer<'a>,
    /// The folder the paths are inside.
    pub base: Rel,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("dropbox::Session")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

/// A path as Dropbox names it: `/a/b`, or `` for the top.
pub(crate) fn path_of(rel: &Rel) -> String {
    if rel.parts.is_empty() {
        String::new()
    } else {
        format!("/{}", rel.parts.join("/"))
    }
}

/// The error an endpoint gave (409), by its summary: `path/not_found/...`.
pub(crate) fn summary(answer: &Answer) -> String {
    web::text(&answer.json(), "error_summary")
        .unwrap_or_default()
        .to_owned()
}

pub(crate) fn refusal(answer: &Answer, what: &str) -> String {
    let json = answer.json();
    let said = json
        .pointer("/user_message/text")
        .and_then(Json::as_str)
        .or_else(|| web::text(&json, "error_summary"))
        .map_or_else(|| answer.said(200), str::to_owned);
    let shown: String = said.chars().take(300).collect();
    format!("Dropbox refused to {what}: {} {shown}", answer.status)
}

/// An entry of a listing, as Dropbox gives it: a file's or a folder's metadata (a deleted one is none).
pub(crate) fn listed(entry: &Json) -> Option<(Listed, String)> {
    let tag = web::text(entry, ".tag")?;
    let shown = web::text(entry, "path_display")?;
    let lower = web::text(entry, "path_lower").unwrap_or(shown).to_owned();
    let rel = Rel::parse(shown.trim_start_matches('/'), false).ok()?;
    match tag {
        "file" => Some((
            Listed {
                rel,
                folder: false,
                size: web::number(entry, "size").unwrap_or(0),
                modified: web::text(entry, "server_modified").and_then(web::rfc3339_ms),
                version: web::text(entry, "rev").unwrap_or_default().to_owned(),
            },
            lower,
        )),
        "folder" => Some((
            Listed {
                rel,
                folder: true,
                size: 0,
                modified: None,
                version: String::new(),
            },
            lower,
        )),
        _ => None,
    }
}

impl<'a> Session<'a> {
    /// A session under the folder `folder_id` names (`id:...`), or from the top.
    pub(crate) async fn open(mut bearer: Bearer<'a>, folder_id: Option<&str>) -> Result<Self, String> {
        let mut base = Rel::default();
        if let Some(id) = folder_id.map(str::trim).filter(|i| !i.is_empty()) {
            let id = if id.starts_with("id:") {
                id.to_owned()
            } else {
                format!("id:{id}")
            };
            if id.len() > 200
                || id
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || c == '/')
            {
                return Err("folder_id: a Dropbox folder's id, as id:a4ayc_80_OEAAAAAAAAAXw".to_owned());
            }
            let answer = bearer
                .json(
                    "POST",
                    format!("{API}/files/get_metadata"),
                    Some(&json!({ "path": id })),
                )
                .await?;
            if answer.status == 409 {
                return Err(format!("folder_id: there is no folder {id} in this Dropbox"));
            }
            if !answer.ok() {
                return Err(refusal(&answer, "find the folder_id"));
            }
            let meta = answer.json();
            if web::text(&meta, ".tag") != Some("folder") {
                return Err(format!("folder_id: {id} is not a folder"));
            }
            base = Rel::parse(
                web::text(&meta, "path_display")
                    .unwrap_or_default()
                    .trim_start_matches('/'),
                false,
            )?;
        }
        Ok(Self { bearer, base })
    }

    async fn call(&mut self, route: &str, args: &Json) -> Result<Answer, String> {
        self.bearer
            .json("POST", format!("{API}/{route}"), Some(args))
            .await
    }

    /// What is at `rel`: its metadata, or none.
    pub(crate) async fn metadata(&mut self, rel: &Rel) -> Result<Option<Json>, String> {
        if rel.parts.is_empty() {
            return Ok(Some(json!({ ".tag": "folder", "path_display": "/" })));
        }
        let answer = self
            .call("files/get_metadata", &json!({ "path": path_of(rel) }))
            .await?;
        if answer.status == 409 && summary(&answer).starts_with("path/not_found") {
            return Ok(None);
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("look at {}", rel.text())));
        }
        Ok(Some(answer.json()))
    }

    /// Every entry of a folder (into those below when `recursive`), page after page; and its cursor.
    pub(crate) async fn list_all(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<(Listed, String)>, bool, String), String> {
        let folder = path_of(dir);
        let mut answer = self
            .call(
                "files/list_folder",
                &json!({ "path": folder, "recursive": recursive, "limit": 2000 }),
            )
            .await?;
        let mut out = Vec::new();
        for _ in 0..MAX_PAGES {
            if answer.status == 409 && summary(&answer).starts_with("path/not_found") {
                return Err(format!("there is no folder at {}", dir.text()));
            }
            if !answer.ok() {
                return Err(refusal(&answer, &format!("list {}", dir.text())));
            }
            let page = answer.json();
            let lower_dir = folder.to_lowercase();
            for entry in page.get("entries").and_then(Json::as_array).into_iter().flatten() {
                if let Some((l, lower)) = listed(entry)
                    && lower != lower_dir
                {
                    out.push((l, lower));
                }
            }
            let cursor = web::text(&page, "cursor").unwrap_or_default().to_owned();
            if out.len() >= most {
                out.truncate(most);
                return Ok((out, true, cursor));
            }
            if page.get("has_more").and_then(Json::as_bool) != Some(true) {
                out.sort_by(|a, b| a.0.rel.parts.cmp(&b.0.rel.parts));
                return Ok((out, false, cursor));
            }
            answer = self
                .call("files/list_folder/continue", &json!({ "cursor": cursor }))
                .await?;
        }
        Ok((out, true, String::new()))
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let (all, more, _) = self.list_all(dir, recursive, most).await?;
        Ok((all.into_iter().map(|(l, _)| l).collect(), more))
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let arg = web::ascii_json(&json!({ "path": path_of(rel) }));
        let request = web::request(
            "POST",
            format!("{CONTENT}/files/download"),
            vec![("dropbox-api-arg".to_owned(), arg)],
            None,
        );
        let answer = self.bearer.send(request).await?;
        if answer.status == 409 && summary(&answer).starts_with("path/not_found") {
            return Err(format!("there is nothing at {}", rel.text()));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("read {}", rel.text())));
        }
        if answer.body.len() > most {
            return Err(format!(
                "{} is more than {most} bytes: a run reads files of at most that",
                rel.text()
            ));
        }
        let modified = answer
            .header("dropbox-api-result")
            .and_then(|r| serde_json::from_str::<Json>(r).ok())
            .and_then(|m| web::text(&m, "server_modified").and_then(web::rfc3339_ms));
        Ok((answer.body, modified))
    }

    pub(crate) async fn write(&mut self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        // Strictly: a file of the same bytes there is a conflict too, as a write that may not replace one should be.
        let arg = web::ascii_json(&json!({
            "path": path_of(rel), "mode": if overwrite { "overwrite" } else { "add" },
            "autorename": false, "mute": true, "strict_conflict": !overwrite,
        }));
        let request = web::request(
            "POST",
            format!("{CONTENT}/files/upload"),
            vec![("dropbox-api-arg".to_owned(), arg)],
            Some(("application/octet-stream".to_owned(), file.bytes.to_vec())),
        );
        let answer = self.bearer.send(request).await?;
        if answer.status == 409 && summary(&answer).contains("conflict") {
            return Err(format!(
                "there is a file or folder at {} already: set overwrite to replace it",
                rel.text()
            ));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("store {}", rel.text())));
        }
        Ok(())
    }

    /// Where a move or a copy goes, made free: refused when something is there, unless `overwrite` and it is a file.
    async fn clear(&mut self, to: &Rel, overwrite: bool) -> Result<(), String> {
        match self.metadata(to).await? {
            None => Ok(()),
            Some(m) if overwrite && web::text(&m, ".tag") == Some("file") => {
                self.delete(to).await.map(|_| ())
            }
            Some(_) => Err(format!(
                "there is a file or folder at {} already: set overwrite to replace a file",
                to.text()
            )),
        }
    }

    async fn relocate(&mut self, route: &str, from: &Rel, to: &Rel, overwrite: bool) -> Result<Json, String> {
        self.clear(to, overwrite).await?;
        let answer = self
            .call(
                route,
                &json!({ "from_path": path_of(from), "to_path": path_of(to), "autorename": false }),
            )
            .await?;
        if answer.status == 409 && summary(&answer).starts_with("from_lookup/not_found") {
            return Err(format!("there is nothing at {}", from.text()));
        }
        if answer.status == 409 && summary(&answer).contains("conflict") {
            return Err(format!("there is a file or folder at {} already", to.text()));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("move or copy {}", from.text())));
        }
        Ok(answer.json())
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        self.relocate("files/move_v2", from, to, overwrite)
            .await
            .map(|_| ())
    }

    pub(crate) async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let done = self.relocate("files/copy_v2", from, to, overwrite).await?;
        Ok(done
            .get("metadata")
            .and_then(|m| web::number(m, "size"))
            .unwrap_or(0))
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        let Some(meta) = self.metadata(rel).await? else {
            return Ok(false);
        };
        if web::text(&meta, ".tag") == Some("folder") {
            // Dropbox deletes a folder with all in it: only an empty one is, here.
            let answer = self
                .call("files/list_folder", &json!({ "path": path_of(rel), "limit": 1 }))
                .await?;
            let empty = answer
                .json()
                .get("entries")
                .and_then(Json::as_array)
                .is_none_or(Vec::is_empty);
            if !answer.ok() || !empty {
                return Err(format!(
                    "{} is a folder with files in it: only an empty folder is deleted",
                    rel.text()
                ));
            }
        }
        let answer = self
            .call("files/delete_v2", &json!({ "path": path_of(rel) }))
            .await?;
        if answer.status == 409 && summary(&answer).starts_with("path_lookup/not_found") {
            return Ok(false);
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("delete {}", rel.text())));
        }
        Ok(true)
    }

    pub(crate) async fn make_folder(&mut self, rel: &Rel) -> Result<bool, String> {
        let answer = self
            .call(
                "files/create_folder_v2",
                &json!({ "path": path_of(rel), "autorename": false }),
            )
            .await?;
        if answer.status == 409 && summary(&answer).contains("conflict/folder") {
            return Ok(false);
        }
        if answer.status == 409 && summary(&answer).contains("conflict") {
            return Err(format!("there is a file at {} already", rel.text()));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("make the folder {}", rel.text())));
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_are_read_as_dropbox_writes_them() {
        let file = json!({
            ".tag": "file", "name": "a.csv", "path_display": "/In/a.csv", "path_lower": "/in/a.csv",
            "size": 12, "server_modified": "2026-11-27T10:00:00Z", "rev": "a1c10ce0dd78", "id": "id:x",
        });
        let (l, lower) = listed(&file).unwrap();
        assert_eq!(
            (l.rel.text(), lower.as_str(), l.size, l.version.as_str()),
            ("In/a.csv".to_owned(), "/in/a.csv", 12, "a1c10ce0dd78")
        );
        assert_eq!(l.modified, Some(1_795_773_600_000));
        assert!(listed(&json!({ ".tag": "deleted", "name": "x", "path_display": "/x" })).is_none());
        assert_eq!(path_of(&Rel::parse("a/b", false).unwrap()), "/a/b");
        assert_eq!(path_of(&Rel::default()), "");
    }
}
