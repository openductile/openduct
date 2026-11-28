// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Places whose files are found by id (Box, OneDrive and SharePoint, Google Drive): a path the step names is walked
//! name by name from the place's top folder - its `folder_id`, or the top of the drive - a folder made where a
//! write needs one, and a folder listed (into the folders below, at most [`MAX_LIST_DEPTH`] deep) by its id.

use super::super::paths::Rel;
use super::super::remote::{Listed, MAX_LIST_DEPTH};

/// A file or a folder, as a place gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item {
    pub id: String,
    pub name: String,
    pub folder: bool,
    pub size: u64,
    pub modified: Option<i64>,
    /// What tells one version of it from another (an ETag, a revision, a checksum).
    pub version: String,
    /// The folder it is in, when the place says.
    pub parent: Option<String>,
}

/// What a place found by id does: a folder's children, one child by its name, a folder made.
pub(crate) trait Tree {
    /// The id the place's paths start from.
    fn top(&self) -> String;

    /// Every child of the folder `id` (page after page, within the step's budget).
    async fn children(&mut self, id: &str) -> Result<Vec<Item>, String>;

    /// The child of folder `parent` called `name`, if there is one.
    async fn child(&mut self, parent: &str, name: &str) -> Result<Option<Item>, String> {
        let wanted = name.to_lowercase();
        Ok(self
            .children(parent)
            .await?
            .into_iter()
            .find(|i| i.name.to_lowercase() == wanted))
    }

    /// A folder called `name` made in folder `parent`.
    async fn make(&mut self, parent: &str, name: &str) -> Result<Item, String>;
}

/// What is at `rel`: the top folder itself, a folder or a file, or nothing.
pub(crate) async fn find<T: Tree>(tree: &mut T, rel: &Rel) -> Result<Option<Item>, String> {
    let mut at = Item {
        id: tree.top(),
        name: String::new(),
        folder: true,
        size: 0,
        modified: None,
        version: String::new(),
        parent: None,
    };
    for name in &rel.parts {
        if !at.folder {
            return Ok(None);
        }
        match tree.child(&at.id, name).await? {
            Some(next) => at = next,
            None => return Ok(None),
        }
    }
    Ok(Some(at))
}

/// The id of the folder at `rel`, each folder on the way made when it is not there. A file in the way is said.
pub(crate) async fn ensure<T: Tree>(tree: &mut T, rel: &Rel) -> Result<String, String> {
    let mut id = tree.top();
    let mut walked = Rel::default();
    for name in &rel.parts {
        walked = walked.child(name);
        id = match tree.child(&id, name).await? {
            Some(i) if i.folder => i.id,
            Some(_) => return Err(format!("{} is a file, not a folder", walked.text())),
            None => tree.make(&id, name).await?.id,
        };
    }
    Ok(id)
}

/// What is in the folder `id` (whose path is `at`): each entry with its path, into the folders below when
/// `recursive`; at most `most`, and whether there was more.
pub(crate) async fn walk<T: Tree>(
    tree: &mut T,
    id: &str,
    at: &Rel,
    recursive: bool,
    most: usize,
) -> Result<(Vec<(Listed, Item)>, bool), String> {
    let depth = if recursive { MAX_LIST_DEPTH } else { 1 };
    let mut out = Vec::new();
    let mut more = false;
    let mut queue = vec![(id.to_owned(), at.clone(), 1usize)];
    'walking: while let Some((folder, path, level)) = queue.pop() {
        for item in tree.children(&folder).await? {
            if out.len() >= most {
                more = true;
                break 'walking;
            }
            let rel = path.child(&item.name);
            if item.folder && level < depth {
                queue.push((item.id.clone(), rel.clone(), level + 1));
            }
            out.push((
                Listed {
                    rel,
                    folder: item.folder,
                    size: item.size,
                    modified: item.modified,
                    version: item.version.clone(),
                },
                item,
            ));
        }
    }
    out.sort_by(|a, b| a.0.rel.parts.cmp(&b.0.rel.parts));
    Ok((out, more))
}

/// A name a place can hold: what a path's last name is, checked again where a place is stricter.
pub(crate) fn named(rel: &Rel, what: &str) -> Result<String, String> {
    rel.name()
        .map(str::to_owned)
        .ok_or_else(|| format!("path: the {what}, as in/orders.csv"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// A tree in memory: folder id -> children.
    struct Memory(BTreeMap<String, Vec<Item>>, usize);

    fn item(id: &str, name: &str, folder: bool) -> Item {
        Item {
            id: id.to_owned(),
            name: name.to_owned(),
            folder,
            size: 1,
            modified: None,
            version: "v".to_owned(),
            parent: None,
        }
    }

    impl Tree for Memory {
        fn top(&self) -> String {
            "0".to_owned()
        }

        async fn children(&mut self, id: &str) -> Result<Vec<Item>, String> {
            Ok(self.0.get(id).cloned().unwrap_or_default())
        }

        async fn make(&mut self, parent: &str, name: &str) -> Result<Item, String> {
            self.1 += 1;
            let made = item(&format!("m{}", self.1), name, true);
            self.0.entry(parent.to_owned()).or_default().push(made.clone());
            Ok(made)
        }
    }

    fn block<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn paths_are_walked_by_name_and_folders_made_on_the_way() {
        let mut t = Memory(BTreeMap::new(), 0);
        t.0.insert("0".into(), vec![item("1", "In", true), item("2", "a.csv", false)]);
        t.0.insert(
            "1".into(),
            vec![item("3", "b.csv", false), item("4", "deep", true)],
        );
        t.0.insert("4".into(), vec![item("5", "c.csv", false)]);
        let rel = |p: &str| Rel::parse(p, false).unwrap();
        block(async {
            assert_eq!(
                find(&mut t, &rel("in/B.CSV")).await.unwrap().unwrap().id,
                "3",
                "names as the place matches them"
            );
            assert!(find(&mut t, &rel("a.csv/x")).await.unwrap().is_none());
            assert!(find(&mut t, &rel("nope")).await.unwrap().is_none());
            assert_eq!(find(&mut t, &rel("")).await.unwrap().unwrap().id, "0");
            assert_eq!(ensure(&mut t, &rel("in/deep")).await.unwrap(), "4");
            let made = ensure(&mut t, &rel("out/reports")).await.unwrap();
            assert_eq!(made, "m2");
            assert!(
                ensure(&mut t, &rel("a.csv/x"))
                    .await
                    .unwrap_err()
                    .contains("is a file")
            );
            let (all, more) = walk(&mut t, "1", &rel("in"), true, 100).await.unwrap();
            let paths: Vec<String> = all.iter().map(|(l, _)| l.rel.text()).collect();
            assert_eq!(paths, ["in/b.csv", "in/deep", "in/deep/c.csv"]);
            assert!(!more);
            let (some, more) = walk(&mut t, "1", &rel("in"), true, 2).await.unwrap();
            assert!(more && some.len() == 2);
            let (top, _) = walk(&mut t, "1", &rel("in"), false, 100).await.unwrap();
            assert_eq!(top.len(), 2);
        });
    }
}
