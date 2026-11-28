// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A drive in memory for the stand-ins of places found by id (Box, OneDrive, Google Drive) and of Dropbox: files and
//! folders by id, each in a folder, and a log of what changed, so a vendor's list of changes can be told.

use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub id: String,
    pub parent: String,
    pub name: String,
    pub folder: bool,
    pub bytes: Vec<u8>,
    pub kind: String,
    pub version: u64,
    pub modified: i64,
    pub trashed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Drive {
    pub nodes: BTreeMap<String, Node>,
    /// What changed, in order: an id, and whether it is gone.
    pub log: Vec<(String, bool)>,
    next: u64,
    clock: i64,
}

impl Drive {
    /// A drive whose top folder is `root`.
    pub(crate) fn new(root: &str) -> Self {
        let mut d = Self {
            clock: 1_795_773_600_000,
            ..Self::default()
        };
        d.nodes.insert(
            root.to_owned(),
            Node {
                id: root.to_owned(),
                parent: String::new(),
                name: String::new(),
                folder: true,
                bytes: Vec::new(),
                kind: String::new(),
                version: 1,
                modified: d.clock,
                trashed: false,
            },
        );
        d
    }

    fn tick(&mut self) -> i64 {
        self.clock += 1_000;
        self.clock
    }

    /// The children of folder `id` (not trashed).
    pub(crate) fn children(&self, id: &str) -> Vec<&Node> {
        let mut out: Vec<&Node> = self
            .nodes
            .values()
            .filter(|n| n.parent == id && !n.trashed && n.id != id)
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The child of `parent` called `name`, as `same` compares names.
    pub(crate) fn child(&self, parent: &str, name: &str, any_case: bool) -> Option<&Node> {
        self.children(parent).into_iter().find(|n| {
            if any_case {
                n.name.to_lowercase() == name.to_lowercase()
            } else {
                n.name == name
            }
        })
    }

    /// What is at `path` from `top` (names separated by `/`).
    pub(crate) fn at(&self, top: &str, path: &str, any_case: bool) -> Option<&Node> {
        let mut at = self.nodes.get(top)?;
        for name in path.split('/').filter(|n| !n.is_empty()) {
            at = self.child(&at.id.clone(), name, any_case)?;
        }
        Some(at)
    }

    /// The path of `id` from `top`, or none when it is not below it.
    pub(crate) fn path_of(&self, top: &str, id: &str) -> Option<String> {
        let mut names = Vec::new();
        let mut at = self.nodes.get(id)?;
        while at.id != top {
            names.push(at.name.clone());
            at = self.nodes.get(&at.parent)?;
        }
        names.reverse();
        Some(names.join("/"))
    }

    pub(crate) fn add(
        &mut self,
        parent: &str,
        name: &str,
        folder: bool,
        bytes: Vec<u8>,
        kind: &str,
    ) -> String {
        self.next += 1;
        let id = format!("n{}", self.next);
        let modified = self.tick();
        self.nodes.insert(
            id.clone(),
            Node {
                id: id.clone(),
                parent: parent.to_owned(),
                name: name.to_owned(),
                folder,
                bytes,
                kind: kind.to_owned(),
                version: 1,
                modified,
                trashed: false,
            },
        );
        self.log.push((id.clone(), false));
        id
    }

    /// A file's bytes replaced.
    pub(crate) fn put(&mut self, id: &str, bytes: Vec<u8>) {
        let modified = self.tick();
        if let Some(n) = self.nodes.get_mut(id) {
            n.bytes = bytes;
            n.version += 1;
            n.modified = modified;
        }
        self.log.push((id.to_owned(), false));
    }

    /// Moved into `parent`, named `name`.
    pub(crate) fn relocate(&mut self, id: &str, parent: &str, name: &str) {
        let modified = self.tick();
        if let Some(n) = self.nodes.get_mut(id) {
            n.parent = parent.to_owned();
            n.name = name.to_owned();
            n.version += 1;
            n.modified = modified;
        }
        self.log.push((id.to_owned(), false));
    }

    pub(crate) fn copy(&mut self, id: &str, parent: &str, name: &str) -> String {
        let n = self.nodes.get(id).cloned().unwrap();
        self.add(parent, name, false, n.bytes, &n.kind)
    }

    pub(crate) fn remove(&mut self, id: &str) {
        if let Some(n) = self.nodes.get_mut(id) {
            n.trashed = true;
        }
        self.log.push((id.to_owned(), true));
    }
}
