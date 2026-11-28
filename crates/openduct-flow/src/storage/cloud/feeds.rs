// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The storage trigger where the vendor keeps a list of changes: Dropbox (`files/list_folder` and its cursor), OneDrive
//! and SharePoint (Graph's `delta`, from `token=latest`), Google Drive (`changes`, from its start page token). Each
//! look reads the changes since the last one, not the whole folder; what it saw is kept as the polled listing's is -
//! with the runs it starts, in one batch on the schedule's partition (see [`crate::runner`]) - so a cluster looks once
//! each time, and a restart neither runs a file twice nor passes one by.
//!
//! What is kept: each file of the folder watched and its version (Dropbox by its path; Graph and Drive by id, as they
//! advise, with the folders below it, so a file is placed by its folder's id - their changes name no path), where the
//! changes go on from, and the files found and not run yet (beyond `max_items`), which run first the next time. The
//! first look lists the folder and runs nothing (unless `start: existing`); a cursor the vendor no longer takes (a
//! Dropbox `reset`, Graph's 410) is met by listing the folder again and comparing it with what was kept.

use std::collections::BTreeMap;

use openduct_expr::Value;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use super::super::paths::Rel;
use super::super::watch::{FilesWatch, Polled};
use super::super::{Entry, MOST_LIST_ITEMS};
use super::connected::Bearer;
use super::tree::{self, Item};
use super::web;
use super::{dropbox, gdrive, onedrive};
use crate::app_files::poll::{MOST_SEEN, Seen};
use crate::apps::Credential;
use crate::run::Effects;

/// The most files found and kept for a later look.
const MOST_WAITING: usize = 1_000;
/// The most folders below the one watched kept.
const MOST_FOLDERS: usize = 2_000;
/// The most pages of changes one look reads; the rest is read the next time.
const MAX_PAGES: usize = 20;
/// The most folders that came into the one watched listed in one look.
const MOST_ENTERED: usize = 20;
/// How deep a folder is placed, at most.
const DEEPEST: usize = 32;

/// A file found and not run yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Waiting {
    id: String,
    change: String,
    name: String,
    path: String,
    size: u64,
    modified: Option<i64>,
    version: String,
}

/// What a look keeps beside the files' versions, in what was seen's cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct State {
    /// What it is of: a trigger whose place or folder changed starts again.
    place: String,
    /// Where the changes go on from: a cursor, a delta link, a page token.
    feed: String,
    /// The folder watched, by id (Graph, Drive).
    #[serde(default)]
    root: String,
    /// The folders below it: id -> (its folder's id, its name).
    #[serde(default)]
    folders: BTreeMap<String, (String, String)>,
    #[serde(default)]
    waiting: Vec<Waiting>,
}

/// A change, as Graph and Drive give one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Change {
    item: Item,
    gone: bool,
}

/// Whether credentials of `kind` are watched by their vendor's changes.
pub(crate) fn takes(kind: &str) -> bool {
    matches!(kind, "dropbox" | "microsoft_365" | "google_drive")
}

/// One look at the folder `w` watches with credential `name`.
pub(crate) async fn poll(
    effects: &dyn Effects,
    w: &FilesWatch,
    name: &str,
    c: &Credential,
    before: Option<&Seen>,
    slot: i64,
) -> Result<Polled, String> {
    let place = format!(
        "{}|{}|{}|{}|{}",
        c.kind,
        w.folder,
        w.folder_id.as_deref().unwrap_or_default(),
        w.drive.as_deref().unwrap_or_default(),
        w.recursive
    );
    let state = before
        .and_then(|b| serde_json::from_value::<State>(b.cursor.clone()).ok())
        .filter(|s| s.place == place);
    let known: BTreeMap<String, String> = match (&state, before) {
        (Some(_), Some(b)) => b.items.iter().cloned().collect(),
        _ => BTreeMap::new(),
    };
    let mut look = Look {
        w,
        known,
        fresh: Vec::new(),
        state: state.clone().unwrap_or_else(|| State {
            place: place.clone(),
            ..State::default()
        }),
    };
    let first = state.is_none();
    match c.kind.as_str() {
        "dropbox" => {
            let bearer = Bearer::open(effects, name, c.clone(), "Dropbox").await?;
            let mut s = dropbox::Session::open(bearer, w.folder_id.as_deref()).await?;
            look.dropbox(&mut s, first).await?;
        }
        "microsoft_365" => {
            let bearer = Bearer::open(effects, name, c.clone(), "Microsoft Graph").await?;
            let mut s = onedrive::Session::new(bearer, w.drive.as_deref(), w.folder_id.as_deref())?;
            look.graph(&mut s, first).await?;
        }
        "google_drive" => {
            let bearer = Bearer::open(effects, name, c.clone(), "Google Drive").await?;
            let mut s = gdrive::Session::new(bearer, w.folder_id.as_deref(), gdrive::Export::default())?;
            look.drive(&mut s, first).await?;
        }
        other => return Err(format!("{other} keeps no list of changes")),
    }
    look.finish(slot)
}

/// A look in progress: what was kept, what it found.
struct Look<'w> {
    w: &'w FilesWatch,
    /// Each file kept and its version.
    known: BTreeMap<String, String>,
    fresh: Vec<Waiting>,
    state: State,
}

impl Look<'_> {
    /// The folder watched, as the step's paths name it.
    fn folder(&self) -> Result<Rel, String> {
        let mut rel = Rel::parse(&self.w.folder, true)?;
        rel.absolute = false;
        Ok(rel)
    }

    fn matches(&self, name: &str) -> bool {
        self.w.pattern.as_ref().is_none_or(|p| p.matches(name))
    }

    /// A file seen in a listing or a change: run when new (or changed, when asked), and kept.
    fn saw(&mut self, id: &str, w: Waiting) {
        let was = self.known.insert(id.to_owned(), w.version.clone());
        let change = match was {
            None => "new",
            Some(v) if self.w.changed && v != w.version => "changed",
            Some(_) => return,
        };
        self.fresh.push(Waiting {
            change: change.to_owned(),
            ..w
        });
    }

    fn forget(&mut self, id: &str) {
        self.known.remove(id);
    }

    /// The files of a full listing: on the first look noted (and run only with `start: existing`); after one the
    /// vendor no longer takes the cursor of, compared with what was kept.
    fn listed(&mut self, files: Vec<(String, Waiting)>, first: bool) {
        let ids: std::collections::BTreeSet<&String> = files.iter().map(|(id, _)| id).collect();
        self.known.retain(|id, _| ids.contains(id));
        for (id, w) in files {
            if first && !self.w.existing {
                self.known.insert(id, w.version);
            } else {
                self.saw(&id, w);
            }
        }
    }

    fn finish(mut self, slot: i64) -> Result<Polled, String> {
        if self.known.len() > MOST_SEEN {
            return Err(format!(
                "{} holds more than {MOST_SEEN} files: a trigger watches a folder of fewer - move the files it has \
                 run into another folder, or watch one below it",
                if self.w.folder.is_empty() {
                    "the folder"
                } else {
                    self.w.folder.as_str()
                }
            ));
        }
        // What waited from before goes first, then what is new; each file once, as last seen, while it is there.
        let mut queue: Vec<Waiting> = Vec::new();
        for w in std::mem::take(&mut self.state.waiting)
            .into_iter()
            .chain(std::mem::take(&mut self.fresh))
        {
            queue.retain(|q| q.id != w.id);
            queue.push(w);
        }
        queue.retain(|q| self.known.contains_key(&q.id));
        queue.sort_by(|a, b| a.modified.cmp(&b.modified).then_with(|| a.path.cmp(&b.path)));
        let later = queue.split_off(queue.len().min(self.w.max_items));
        if later.len() > MOST_WAITING {
            tracing::warn!(
                folder = self.w.folder,
                left = later.len() - MOST_WAITING,
                "more new files than a trigger keeps waiting: the newest are passed by"
            );
        }
        self.state.waiting = later.into_iter().take(MOST_WAITING).collect();
        let runs = queue
            .into_iter()
            .map(|q| {
                let entry = Entry {
                    name: q.name,
                    path: q.path,
                    folder: false,
                    size: q.size,
                    modified: q.modified,
                    version: q.version,
                };
                let mut value = entry.value();
                if let Value::Record(r) = &mut value {
                    std::sync::Arc::make_mut(r).insert("change".to_owned(), Value::text(q.change));
                }
                (q.id, value)
            })
            .collect();
        Ok(Polled {
            runs,
            seen: Seen {
                cursor: serde_json::to_value(&self.state).unwrap_or(Json::Null),
                items: self.known.into_iter().collect(),
                slot,
                failing: None,
                pending: false,
            },
        })
    }

    // ------------------------------------------------------------------------------------------------- Dropbox

    async fn dropbox(&mut self, s: &mut dropbox::Session<'_>, first: bool) -> Result<(), String> {
        let dir = self.folder()?.under(&s.base);
        let shown = |rel: &Rel, base: &Rel| rel.inside(base).map_or_else(|| rel.text(), |r| r.text());
        if !first {
            let mut cursor = self.state.feed.clone();
            for _ in 0..MAX_PAGES {
                let answer = s
                    .bearer
                    .json(
                        "POST",
                        format!("{}/files/list_folder/continue", dropbox::API),
                        Some(&json!({ "cursor": cursor })),
                    )
                    .await?;
                if answer.status == 409 && dropbox::summary(&answer).starts_with("reset") {
                    return self.dropbox_listing(s, &dir, false).await;
                }
                if !answer.ok() {
                    return Err(dropbox::refusal(&answer, "read the folder's changes"));
                }
                let page = answer.json();
                for entry in page.get("entries").and_then(Json::as_array).into_iter().flatten() {
                    let lower = web::text(entry, "path_lower").unwrap_or_default().to_owned();
                    match (web::text(entry, ".tag"), dropbox::listed(entry)) {
                        (Some("deleted"), _) => {
                            let inside = format!("{lower}/");
                            self.known.retain(|k, _| *k != lower && !k.starts_with(&inside));
                        }
                        (Some("file"), Some((l, lower)))
                            if self.matches(l.rel.name().unwrap_or_default()) =>
                        {
                            let w = waiting(
                                &lower,
                                &l.rel,
                                shown(&l.rel, &s.base),
                                l.size,
                                l.modified,
                                &l.version,
                            );
                            self.saw(&lower, w);
                        }
                        (Some("file"), Some((_, lower))) => self.forget(&lower),
                        _ => {}
                    }
                }
                cursor = web::text(&page, "cursor").unwrap_or(&cursor).to_owned();
                if page.get("has_more").and_then(Json::as_bool) != Some(true) {
                    break;
                }
            }
            self.state.feed = cursor;
            return Ok(());
        }
        self.dropbox_listing(s, &dir, true).await
    }

    async fn dropbox_listing(
        &mut self,
        s: &mut dropbox::Session<'_>,
        dir: &Rel,
        first: bool,
    ) -> Result<(), String> {
        let (all, more, cursor) = s.list_all(dir, self.w.recursive, MOST_SEEN + 1).await?;
        if more || cursor.is_empty() {
            return Err(format!(
                "{} holds more than {MOST_SEEN} files: watch a folder of fewer",
                dir.text()
            ));
        }
        let files: Vec<(String, Waiting)> = all
            .into_iter()
            .filter(|(l, _)| !l.folder && self.matches(l.rel.name().unwrap_or_default()))
            .map(|(l, lower)| {
                let path = l.rel.inside(&s.base).map_or_else(|| l.rel.text(), |r| r.text());
                let w = waiting(&lower, &l.rel, path, l.size, l.modified, &l.version);
                (lower, w)
            })
            .collect();
        self.listed(files, first);
        self.state.feed = cursor;
        Ok(())
    }

    // ------------------------------------------------------------------------------------- Graph and Drive, by id

    /// The path of an item in folder `parent` called `name`, as the step names paths; none when it is not in the
    /// folder watched (or below it, when the trigger looks there too).
    fn path_in(&self, parent: &str, name: &str) -> Option<String> {
        let mut names = vec![name.to_owned()];
        let mut at = parent.to_owned();
        for _ in 0..DEEPEST {
            if at == self.state.root {
                names.reverse();
                let mut rel = self.folder().ok()?;
                for n in names {
                    rel = rel.child(&n);
                }
                return Some(rel.text());
            }
            if !self.w.recursive {
                return None;
            }
            let (up, folder) = self.state.folders.get(&at)?;
            names.push(folder.clone());
            at = up.clone();
        }
        None
    }

    /// The folder watched, listed whole: its folders kept, its files noted (or compared with what was kept).
    async fn by_id_listing<T: tree::Tree>(
        &mut self,
        t: &mut T,
        root: &Item,
        first: bool,
    ) -> Result<(), String> {
        let at = self.folder()?;
        let (all, more) = tree::walk(t, &root.id, &at, self.w.recursive, MOST_SEEN + MOST_FOLDERS).await?;
        if more {
            return Err(format!(
                "{} holds more than {MOST_SEEN} files: watch a folder of fewer",
                at.text()
            ));
        }
        self.state.root = root.id.clone();
        self.state.folders.clear();
        let mut files = Vec::new();
        for (l, item) in all {
            if item.folder {
                if let Some(parent) = &item.parent {
                    self.state
                        .folders
                        .insert(item.id.clone(), (parent.clone(), item.name.clone()));
                }
            } else if self.matches(&item.name) {
                files.push((
                    item.id.clone(),
                    waiting(&item.id, &l.rel, l.rel.text(), l.size, l.modified, &l.version),
                ));
            }
        }
        self.listed(files, first);
        Ok(())
    }

    /// Changes applied: folders first (a file is placed by its folder), then files; folders that came in listed.
    async fn apply<T: tree::Tree>(&mut self, t: &mut T, changes: Vec<Change>) -> Result<(), String> {
        let mut last: BTreeMap<String, Change> = BTreeMap::new();
        let mut order = Vec::new();
        for c in changes {
            if !last.contains_key(&c.item.id) {
                order.push(c.item.id.clone());
            }
            last.insert(c.item.id.clone(), c);
        }
        let changes: Vec<Change> = order.into_iter().filter_map(|id| last.remove(&id)).collect();
        let mut entered = Vec::new();
        for c in changes
            .iter()
            .filter(|c| c.item.folder && c.item.id != self.state.root)
        {
            if c.gone {
                self.state.folders.remove(&c.item.id);
                continue;
            }
            let parent = c.item.parent.clone().unwrap_or_default();
            let inside =
                self.w.recursive && (parent == self.state.root || self.state.folders.contains_key(&parent));
            if inside && self.state.folders.len() < MOST_FOLDERS {
                let new = self
                    .state
                    .folders
                    .insert(c.item.id.clone(), (parent, c.item.name.clone()))
                    .is_none();
                if new {
                    entered.push(c.item.id.clone());
                }
            } else {
                self.state.folders.remove(&c.item.id);
            }
        }
        for c in changes.iter().filter(|c| !c.item.folder) {
            if c.gone {
                // A deleted item may come without its folder facet: whatever it was, it is gone.
                self.forget(&c.item.id);
                self.state.folders.remove(&c.item.id);
                continue;
            }
            let parent = c.item.parent.clone().unwrap_or_default();
            match self.path_in(&parent, &c.item.name) {
                Some(path) if self.matches(&c.item.name) => {
                    let rel = Rel::parse(&path, false).unwrap_or_default();
                    let w = waiting(
                        &c.item.id,
                        &rel,
                        path,
                        c.item.size,
                        c.item.modified,
                        &c.item.version,
                    );
                    self.saw(&c.item.id, w);
                }
                _ => self.forget(&c.item.id),
            }
        }
        // A folder that came in brings the files it holds, which no change names.
        for id in entered.into_iter().take(MOST_ENTERED) {
            let Some((parent, name)) = self.state.folders.get(&id).cloned() else {
                continue;
            };
            let Some(path) = self.path_in(&parent, &name) else {
                continue;
            };
            let at = Rel::parse(&path, false)?;
            let (all, _) = tree::walk(t, &id, &at, true, MOST_LIST_ITEMS).await?;
            for (l, item) in all {
                if item.folder {
                    if let Some(p) = &item.parent
                        && self.state.folders.len() < MOST_FOLDERS
                    {
                        self.state
                            .folders
                            .insert(item.id.clone(), (p.clone(), item.name.clone()));
                    }
                } else if self.matches(&item.name) && !self.known.contains_key(&item.id) {
                    let w = waiting(&item.id, &l.rel, l.rel.text(), l.size, l.modified, &l.version);
                    self.saw(&item.id, w);
                }
            }
        }
        Ok(())
    }

    async fn graph(&mut self, s: &mut onedrive::Session<'_>, first: bool) -> Result<(), String> {
        let root_rel = self.folder()?;
        if !first {
            let mut url = self.state.feed.clone();
            let mut changes = Vec::new();
            let mut resync = false;
            for _ in 0..MAX_PAGES {
                let answer = s
                    .bearer
                    .json("GET", web::on_host(&url, onedrive::HOST)?, None)
                    .await?;
                if onedrive::resync(&answer) {
                    resync = true;
                    break;
                }
                if !answer.ok() {
                    return Err(onedrive::refusal(&answer, "read the drive's changes"));
                }
                let page = answer.json();
                for v in page.get("value").and_then(Json::as_array).into_iter().flatten() {
                    if let Some(item) = onedrive::item(v) {
                        changes.push(Change {
                            gone: v.get("deleted").is_some(),
                            item,
                        });
                    }
                }
                if let Some(next) = web::text(&page, "@odata.nextLink") {
                    url = next.to_owned();
                    continue;
                }
                if let Some(delta) = web::text(&page, "@odata.deltaLink") {
                    url = delta.to_owned();
                }
                break;
            }
            if !resync {
                self.state.feed = url;
                return self.apply(s, changes).await;
            }
        }
        // From now: the delta's latest link first, so what changes while the folder is listed is read next time.
        let latest = format!("{}{}/root/delta?token=latest", onedrive::GRAPH, s.drive);
        let answer = s.bearer.json("GET", latest, None).await?;
        if !answer.ok() {
            return Err(onedrive::refusal(&answer, "start reading the drive's changes"));
        }
        self.state.feed = web::text(&answer.json(), "@odata.deltaLink")
            .ok_or("Graph gave no delta link to read changes from")?
            .to_owned();
        let root = s
            .find(&root_rel)
            .await?
            .filter(|r| r.folder)
            .ok_or_else(|| format!("there is no folder at {}", root_rel.text()))?;
        self.by_id_listing(s, &root, first).await
    }

    async fn drive(&mut self, s: &mut gdrive::Session<'_>, first: bool) -> Result<(), String> {
        let root_rel = self.folder()?;
        if !first {
            let mut token = self.state.feed.clone();
            let mut changes = Vec::new();
            for _ in 0..MAX_PAGES {
                let q = web::query(&[
                    ("pageToken", token.clone()),
                    ("pageSize", "1000".to_owned()),
                    ("includeRemoved", "true".to_owned()),
                    ("spaces", "drive".to_owned()),
                    ("supportsAllDrives", "true".to_owned()),
                    ("includeItemsFromAllDrives", "true".to_owned()),
                    (
                        "fields",
                        format!(
                            "nextPageToken,newStartPageToken,changes(fileId,removed,file({}))",
                            gdrive::FIELDS
                        ),
                    ),
                ]);
                let answer = s
                    .bearer
                    .json("GET", format!("{}/changes?{q}", gdrive::API), None)
                    .await?;
                if !answer.ok() {
                    return Err(gdrive::refusal(&answer, "read the drive's changes"));
                }
                let page = answer.json();
                for c in page.get("changes").and_then(Json::as_array).into_iter().flatten() {
                    let removed = c.get("removed").and_then(Json::as_bool).unwrap_or(false);
                    let file = c
                        .get("file")
                        .cloned()
                        .unwrap_or_else(|| json!({ "id": c.get("fileId") }));
                    let trashed = file.get("trashed").and_then(Json::as_bool).unwrap_or(false);
                    if let Some(item) = gdrive::item(&file) {
                        changes.push(Change {
                            gone: removed || trashed,
                            item,
                        });
                    }
                }
                if let Some(next) = web::text(&page, "nextPageToken") {
                    token = next.to_owned();
                    continue;
                }
                if let Some(start) = web::text(&page, "newStartPageToken") {
                    token = start.to_owned();
                }
                break;
            }
            self.state.feed = token;
            return self.apply(s, changes).await;
        }
        let url = format!("{}/changes/startPageToken?supportsAllDrives=true", gdrive::API);
        let answer = s.bearer.json("GET", url, None).await?;
        if !answer.ok() {
            return Err(gdrive::refusal(&answer, "start reading the drive's changes"));
        }
        self.state.feed = web::text(&answer.json(), "startPageToken")
            .ok_or("Google Drive gave no page token to read changes from")?
            .to_owned();
        let root = tree::find(s, &root_rel)
            .await?
            .filter(|r| r.folder)
            .ok_or_else(|| format!("there is no folder at {}", root_rel.text()))?;
        self.by_id_listing(s, &root, first).await
    }
}

fn waiting(id: &str, rel: &Rel, path: String, size: u64, modified: Option<i64>, version: &str) -> Waiting {
    Waiting {
        id: id.to_owned(),
        change: "new".to_owned(),
        name: rel.name().unwrap_or_default().to_owned(),
        path,
        size,
        modified,
        version: version.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Place;
    use crate::storage::paths::Pattern;

    fn watch(recursive: bool, changed: bool, max_items: usize) -> FilesWatch {
        FilesWatch {
            place: Place::Credential("drive".into()),
            folder: "in".into(),
            pattern: Some(Pattern::parse("*.csv").unwrap()),
            recursive,
            every: std::time::Duration::from_secs(60),
            changed,
            existing: false,
            max_items,
            bucket: None,
            folder_id: None,
            drive: None,
        }
    }

    fn file(id: &str, parent: &str, name: &str, version: &str, modified: i64) -> Change {
        Change {
            item: Item {
                id: id.into(),
                name: name.into(),
                folder: false,
                size: 1,
                modified: Some(modified),
                version: version.into(),
                parent: Some(parent.into()),
            },
            gone: false,
        }
    }

    fn folder(id: &str, parent: &str, name: &str) -> Change {
        Change {
            item: Item {
                folder: true,
                ..file(id, parent, name, "", 0).item
            },
            gone: false,
        }
    }

    struct Nothing;

    impl tree::Tree for Nothing {
        fn top(&self) -> String {
            "root".into()
        }
        async fn children(&mut self, _: &str) -> Result<Vec<Item>, String> {
            Ok(Vec::new())
        }
        async fn make(&mut self, _: &str, _: &str) -> Result<Item, String> {
            Err("no".into())
        }
    }

    fn block<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn changes_are_placed_by_their_folder_and_run_once() {
        let w = watch(true, false, 50);
        let mut look = Look {
            w: &w,
            known: [("old".to_owned(), "1".to_owned())].into_iter().collect(),
            fresh: Vec::new(),
            state: State {
                root: "R".into(),
                ..State::default()
            },
        };
        block(look.apply(
            &mut Nothing,
            vec![
                file("a", "R", "a.csv", "1", 3),
                file("b", "S", "b.csv", "1", 2),
                folder("S", "R", "sub"),
                file("c", "X", "c.csv", "1", 1),
                file("d", "R", "d.txt", "1", 1),
                file("old", "R", "old.csv", "2", 1),
                file("a", "R", "a.csv", "2", 4),
            ],
        ))
        .unwrap();
        let ran: Vec<(String, String)> = look
            .fresh
            .iter()
            .map(|f| (f.id.clone(), f.path.clone()))
            .collect();
        assert_eq!(
            ran,
            [
                ("a".to_owned(), "in/a.csv".to_owned()),
                ("b".to_owned(), "in/sub/b.csv".to_owned())
            ],
            "a file outside, another kind and a changed one (not asked for) do not run; a file twice is once"
        );
        assert_eq!(look.known.get("a").map(String::as_str), Some("2"));
        // Gone, moved out, and a folder that left takes its files with it.
        block(look.apply(
            &mut Nothing,
            vec![
                Change {
                    gone: true,
                    ..file("a", "R", "a.csv", "2", 4)
                },
                folder("S", "X", "sub"),
                file("b", "S", "b.csv", "1", 2),
            ],
        ))
        .unwrap();
        assert!(!look.known.contains_key("a") && !look.known.contains_key("b"));
        let polled = look.finish(7).unwrap();
        assert!(polled.runs.is_empty(), "a file gone before its run is not run");
        assert_eq!(polled.seen.slot, 7);
    }

    #[test]
    fn more_than_a_look_runs_waits_for_the_next_one() {
        let w = watch(false, true, 2);
        let mut look = Look {
            w: &w,
            known: BTreeMap::new(),
            fresh: Vec::new(),
            state: State {
                root: "R".into(),
                ..State::default()
            },
        };
        block(look.apply(
            &mut Nothing,
            vec![
                file("1", "R", "1.csv", "1", 1),
                file("2", "R", "2.csv", "1", 2),
                file("3", "R", "3.csv", "1", 3),
                folder("S", "R", "sub"),
                file("4", "S", "4.csv", "1", 4),
            ],
        ))
        .unwrap();
        let polled = look.finish(1).unwrap();
        let ran: Vec<&str> = polled.runs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ran, ["1", "2"], "oldest first; a folder below is not watched");
        let state: State = serde_json::from_value(polled.seen.cursor.clone()).unwrap();
        assert_eq!(state.waiting.len(), 1);
        // The next look: what waited runs, then a change to a file run before (asked for: new_or_changed).
        let mut next = Look {
            w: &w,
            known: polled.seen.items.iter().cloned().collect(),
            fresh: Vec::new(),
            state,
        };
        block(next.apply(&mut Nothing, vec![file("1", "R", "1.csv", "2", 5)])).unwrap();
        let polled = next.finish(2).unwrap();
        let ran: Vec<(String, String)> = polled
            .runs
            .iter()
            .map(|(id, v)| {
                (
                    id.clone(),
                    serde_json::from_str::<Json>(&v.to_json()).unwrap()["change"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                )
            })
            .collect();
        assert_eq!(
            ran,
            [
                ("3".to_owned(), "new".to_owned()),
                ("1".to_owned(), "changed".to_owned())
            ]
        );
    }

    #[test]
    fn a_first_listing_notes_and_a_later_one_compares() {
        let w = watch(true, false, 50);
        let mut look = Look {
            w: &w,
            known: BTreeMap::new(),
            fresh: Vec::new(),
            state: State::default(),
        };
        let rel = Rel::parse("in/a.csv", false).unwrap();
        look.listed(
            vec![("a".into(), waiting("a", &rel, rel.text(), 1, None, "1"))],
            true,
        );
        assert!(look.fresh.is_empty() && look.known.len() == 1);
        let b = Rel::parse("in/b.csv", false).unwrap();
        look.listed(
            vec![("b".into(), waiting("b", &b, b.text(), 1, None, "1"))],
            false,
        );
        assert_eq!(look.fresh.len(), 1);
        assert!(
            !look.known.contains_key("a"),
            "a file gone since is no longer kept"
        );
    }
}
