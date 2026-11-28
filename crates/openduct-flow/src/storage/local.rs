// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Folders of this machine: only those the installation's admin allows when OpenDuct starts
//! (`--files-root <name>=<path>`, several allowed), each named, and nothing outside them. A path is checked
//! ([`super::paths`]), then followed on the disk: where it leads - through any link on the way - must still be inside
//! the folder, before a file is opened and again once it is open (the same file, by its device and number), so a link
//! swapped in meanwhile does not lead out. Writes go to a file of their own beside the one written and are renamed
//! over it, so a reader never sees half a file; a write that may not replace one makes it new or fails.
//!
//! Every folder is open to every workspace of the installation: give a root only what each of them may see.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use super::paths::Rel;
use super::remote::Listed;
use crate::files::FileData;

/// How long one action on a folder may take.
const WORK_TIME: Duration = Duration::from_secs(60);
/// The largest file a copy within a folder makes.
const MAX_COPY: u64 = 256 * 1024 * 1024;
/// What the files a write makes before it is done end in: a listing leaves them out.
const PART: &str = ".openduct-part";

/// A folder the admin allows: its name, and where it is (as the disk names it, links followed).
#[derive(Debug, Clone)]
struct Root {
    name: String,
    path: PathBuf,
}

/// The folders the installation's admin allows.
#[derive(Debug, Clone, Default)]
pub struct Roots {
    roots: Vec<Root>,
}

impl Roots {
    /// The folders given as `name=path`: each name letters, digits, `-` and `_`; each path an existing folder,
    /// written from the top of the disk.
    pub fn parse(given: &[String]) -> Result<Self, String> {
        let mut roots: Vec<Root> = Vec::new();
        for one in given.iter().map(|g| g.trim()).filter(|g| !g.is_empty()) {
            let (name, path) = one
                .split_once('=')
                .ok_or_else(|| format!("--files-root {one:?}: name=path, as invoices=/srv/invoices"))?;
            let name = name.trim();
            if name.is_empty()
                || name.len() > 64
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                return Err(format!(
                    "--files-root {one:?}: a name of letters, digits, - and _, at most 64"
                ));
            }
            if roots.iter().any(|r| r.name == name) {
                return Err(format!("--files-root: {name} is given twice"));
            }
            let path = Path::new(path.trim());
            if !path.is_absolute() {
                return Err(format!(
                    "--files-root {one:?}: the folder from the top of the disk, as /srv/invoices"
                ));
            }
            let path = std::fs::canonicalize(path)
                .map_err(|e| format!("--files-root {one:?}: cannot be found ({e})"))?;
            if !path.is_dir() {
                return Err(format!("--files-root {one:?}: not a folder"));
            }
            roots.push(Root {
                name: name.to_owned(),
                path,
            });
        }
        Ok(Self { roots })
    }

    /// The folders' names.
    pub fn names(&self) -> Vec<&str> {
        self.roots.iter().map(|r| r.name.as_str()).collect()
    }

    pub(crate) fn session(&self, name: &str) -> Result<Session, String> {
        let root = self.roots.iter().find(|r| r.name == name).ok_or_else(|| {
            if self.roots.is_empty() {
                format!(
                    "there is no folder {name:?}: this installation allows none (its admin starts OpenDuct with \
                     --files-root {name}=/a/folder)"
                )
            } else {
                format!(
                    "there is no folder {name:?}: this installation allows {}",
                    self.names().join(", ")
                )
            }
        })?;
        Ok(Session {
            root: Arc::new(root.path.clone()),
        })
    }
}

/// A folder's session: where it is.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    root: Arc<PathBuf>,
}

/// Work on the disk, done off the run's own thread and bounded in time.
async fn work<T: Send + 'static>(
    job: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    match tokio::time::timeout(WORK_TIME, tokio::task::spawn_blocking(job)).await {
        Ok(Ok(done)) => done,
        Ok(Err(_)) => Err("the folder could not be reached".to_owned()),
        Err(_) => Err(format!("the folder took more than {}s", WORK_TIME.as_secs())),
    }
}

fn shown(rel: &Rel) -> String {
    if rel.is_top() {
        "the folder's top".to_owned()
    } else {
        rel.text()
    }
}

fn outside(rel: &Rel) -> String {
    format!(
        "{}: it leads out of the folder (through a link), and is refused",
        shown(rel)
    )
}

fn io(rel: &Rel, e: &std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::NotFound => format!("there is nothing at {}", shown(rel)),
        std::io::ErrorKind::PermissionDenied => format!("{}: OpenDuct may not open it", shown(rel)),
        _ => format!("{}: {e}", shown(rel)),
    }
}

/// Where `rel` is on the disk, links followed, checked to be inside `root`. Names past the deepest that exists are
/// added as they are (they are made by the action).
fn resolve(root: &Path, rel: &Rel) -> Result<PathBuf, String> {
    for have in (0..=rel.parts.len()).rev() {
        let (there, rest) = rel.parts.split_at(have);
        let mut candidate = root.to_path_buf();
        candidate.extend(there);
        if let Ok(found) = std::fs::canonicalize(&candidate) {
            if !found.starts_with(root) {
                return Err(outside(rel));
            }
            let mut out = found;
            out.extend(rest);
            return Ok(out);
        }
    }
    Err(outside(rel))
}

/// Where the folder `rel` is in, links followed and checked, with its own name added unfollowed: what a delete or a
/// move acts on is the entry itself (a link, not what it leads to).
fn resolve_entry(root: &Path, rel: &Rel) -> Result<PathBuf, String> {
    let name = rel.name().ok_or("the folder's top is not deleted or moved")?;
    Ok(resolve(root, &rel.parent())?.join(name))
}

/// An open file is still the one at `path`, inside `root`: a link swapped in after the path was checked is caught.
fn still_inside(root: &Path, path: &Path, open: &std::fs::File, rel: &Rel) -> Result<(), String> {
    let found = std::fs::canonicalize(path).map_err(|e| io(rel, &e))?;
    if !found.starts_with(root) {
        return Err(outside(rel));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = open.metadata().map_err(|e| io(rel, &e))?;
        let there = std::fs::metadata(&found).map_err(|e| io(rel, &e))?;
        if opened.dev() != there.dev() || opened.ino() != there.ino() {
            return Err(outside(rel));
        }
    }
    #[cfg(not(unix))]
    let _ = open;
    Ok(())
}

fn millis(meta: &std::fs::Metadata) -> Option<i64> {
    let since = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(since.as_millis()).ok()
}

fn version(meta: &std::fs::Metadata) -> String {
    let nanos = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    format!("{}-{nanos}", meta.len())
}

/// A name for the file a write makes first: beside the one written, its own.
fn part_name(target: &Path) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let short: String = name.chars().take(100).collect();
    target.with_file_name(format!(".{short}.{stamp:x}{PART}"))
}

fn list_into(
    root: &Path,
    dir: &Path,
    at: &Rel,
    depth: usize,
    most: usize,
    out: &mut Vec<Listed>,
) -> Result<bool, String> {
    let mut names: Vec<(String, std::fs::DirEntry)> = std::fs::read_dir(dir)
        .map_err(|e| io(at, &e))?
        .flatten()
        .map(|e| (e.file_name().to_string_lossy().into_owned(), e))
        .filter(|(n, _)| !n.ends_with(PART))
        .collect();
    names.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, entry) in names {
        if out.len() >= most {
            return Ok(true);
        }
        let rel = at.child(&name);
        let Ok(kind) = entry.file_type() else { continue };
        let path = entry.path();
        // A link is listed as what it leads to - only when that is inside the folder - and never gone into.
        let (meta, linked) = if kind.is_symlink() {
            match std::fs::canonicalize(&path) {
                Ok(to) if to.starts_with(root) => match std::fs::metadata(&to) {
                    Ok(m) => (m, true),
                    Err(_) => continue,
                },
                _ => continue,
            }
        } else {
            match entry.metadata() {
                Ok(m) => (m, false),
                Err(_) => continue,
            }
        };
        out.push(Listed {
            rel: rel.clone(),
            folder: meta.is_dir(),
            size: if meta.is_dir() { 0 } else { meta.len() },
            modified: millis(&meta),
            version: version(&meta),
        });
        if meta.is_dir() && !linked && depth > 1 && list_into(root, &path, &rel, depth - 1, most, out)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Bytes written to `target`: replacing what is there (made beside it and renamed over it), or as a new file.
fn write_file(root: &Path, target: &Path, rel: &Rel, bytes: &[u8], overwrite: bool) -> Result<(), String> {
    let parent = target.parent().ok_or_else(|| outside(rel))?;
    std::fs::create_dir_all(parent).map_err(|e| io(&rel.parent(), &e))?;
    if !std::fs::canonicalize(parent)
        .map_err(|e| io(&rel.parent(), &e))?
        .starts_with(root)
    {
        return Err(outside(rel));
    }
    if target.is_dir() {
        return Err(format!("there is a folder at {}", shown(rel)));
    }
    if !overwrite {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::AlreadyExists => format!(
                    "there is a file at {} already: set overwrite to replace it",
                    shown(rel)
                ),
                _ => io(rel, &e),
            })?;
        let written = file.write_all(bytes).and_then(|()| file.sync_all());
        if let Err(e) = written {
            let _ = std::fs::remove_file(target);
            return Err(io(rel, &e));
        }
        return Ok(());
    }
    let part = part_name(target);
    let made = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&part)
        .and_then(|mut f| f.write_all(bytes).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&part, target));
    if let Err(e) = made {
        let _ = std::fs::remove_file(&part);
        return Err(io(rel, &e));
    }
    Ok(())
}

/// The file at `rel`, open, checked to be a file inside the folder: with its details.
fn open_file(root: &Path, rel: &Rel) -> Result<(std::fs::File, std::fs::Metadata), String> {
    let path = resolve(root, rel)?;
    let file = std::fs::File::open(&path).map_err(|e| io(rel, &e))?;
    still_inside(root, &path, &file, rel)?;
    let meta = file.metadata().map_err(|e| io(rel, &e))?;
    if meta.is_dir() {
        return Err(format!("{} is a folder, not a file", shown(rel)));
    }
    Ok((file, meta))
}

impl Session {
    pub(crate) async fn list(
        &self,
        dir: &Rel,
        depth: usize,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let (root, dir) = (Arc::clone(&self.root), dir.clone());
        work(move || {
            let path = resolve(&root, &dir)?;
            if !path.is_dir() {
                return Err(format!("{} is not a folder", shown(&dir)));
            }
            let mut out = Vec::new();
            let more = list_into(&root, &path, &dir, depth, most, &mut out)?;
            Ok((out, more))
        })
        .await
    }

    pub(crate) async fn read(&self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let (root, rel) = (Arc::clone(&self.root), rel.clone());
        work(move || {
            let (file, meta) = open_file(&root, &rel)?;
            let limit = u64::try_from(most).unwrap_or(u64::MAX);
            if meta.len() > limit {
                return Err(format!(
                    "{} is {} bytes: a run reads files of at most {most}",
                    shown(&rel),
                    meta.len()
                ));
            }
            let mut bytes = Vec::new();
            file.take(limit + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| io(&rel, &e))?;
            if bytes.len() > most {
                return Err(format!("{} grew past {most} bytes as it was read", shown(&rel)));
            }
            Ok((bytes, millis(&meta)))
        })
        .await
    }

    pub(crate) async fn write(&self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        let (root, rel, bytes) = (Arc::clone(&self.root), rel.clone(), Arc::clone(&file.bytes));
        work(move || {
            let target = resolve(&root, &rel)?;
            write_file(&root, &target, &rel, &bytes, overwrite)
        })
        .await
    }

    pub(crate) async fn rename(&self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        let (root, from, to) = (Arc::clone(&self.root), from.clone(), to.clone());
        work(move || {
            let source = resolve_entry(&root, &from)?;
            std::fs::symlink_metadata(&source).map_err(|e| io(&from, &e))?;
            let target = resolve(&root, &to)?;
            if target.starts_with(&source) {
                return Err(format!("{} cannot go inside itself", shown(&from)));
            }
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| io(&to.parent(), &e))?;
            }
            if std::fs::symlink_metadata(&target).is_ok() {
                if !overwrite {
                    return Err(format!(
                        "there is a file at {} already: set overwrite to replace it",
                        shown(&to)
                    ));
                }
                if target.is_dir() {
                    return Err(format!("there is a folder at {}", shown(&to)));
                }
            }
            std::fs::rename(&source, &target).map_err(|e| io(&from, &e))
        })
        .await
    }

    pub(crate) async fn copy(&self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let (root, from, to) = (Arc::clone(&self.root), from.clone(), to.clone());
        work(move || {
            let (file, meta) = open_file(&root, &from)?;
            if meta.len() > MAX_COPY {
                return Err(format!(
                    "{} is {} bytes: a copy within a folder is of at most {MAX_COPY}",
                    shown(&from),
                    meta.len()
                ));
            }
            let mut bytes = Vec::new();
            file.take(MAX_COPY + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| io(&from, &e))?;
            let target = resolve(&root, &to)?;
            write_file(&root, &target, &to, &bytes, overwrite)?;
            Ok(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        })
        .await
    }

    pub(crate) async fn delete(&self, rel: &Rel) -> Result<bool, String> {
        let (root, rel) = (Arc::clone(&self.root), rel.clone());
        work(move || {
            let path = resolve_entry(&root, &rel)?;
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                return Ok(false);
            };
            let done = if meta.is_dir() {
                std::fs::remove_dir(&path).map_err(|e| match e.kind() {
                    std::io::ErrorKind::DirectoryNotEmpty => format!(
                        "{} is a folder with files in it: only an empty folder is deleted",
                        shown(&rel)
                    ),
                    _ => io(&rel, &e),
                })
            } else {
                std::fs::remove_file(&path).map_err(|e| io(&rel, &e))
            };
            done.map(|()| true)
        })
        .await
    }

    pub(crate) async fn make_folder(&self, rel: &Rel) -> Result<bool, String> {
        let (root, rel) = (Arc::clone(&self.root), rel.clone());
        work(move || {
            let path = resolve(&root, &rel)?;
            if path.is_dir() {
                return Ok(false);
            }
            if path.exists() {
                return Err(format!("there is a file at {}", shown(&rel)));
            }
            std::fs::create_dir_all(&path).map_err(|e| io(&rel, &e))?;
            if !std::fs::canonicalize(&path)
                .map_err(|e| io(&rel, &e))?
                .starts_with(&*root)
            {
                return Err(outside(&rel));
            }
            Ok(true)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(text: &str) -> Rel {
        Rel::parse(text, false).unwrap_or_default()
    }

    fn folder() -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir().join(format!(
            "openduct-roots-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        let root = base.join("root");
        let _ = std::fs::create_dir_all(root.join("in"));
        let _ = std::fs::write(base.join("secret.txt"), "outside");
        let _ = std::fs::write(root.join("in/a.csv"), "id\n1\n");
        (
            std::fs::canonicalize(&base).unwrap_or(base),
            std::fs::canonicalize(&root).unwrap_or(root),
        )
    }

    #[test]
    fn roots_are_named_folders_given_from_the_top() {
        let (base, root) = folder();
        let given = |g: &str| Roots::parse(&[g.to_owned()]).err().unwrap_or_default();
        let ok = Roots::parse(&[format!("inbox={}", root.display())]).unwrap_or_default();
        assert_eq!(ok.names(), ["inbox"]);
        assert!(given("inbox").contains("name=path"));
        assert!(given("in box=/tmp").contains("letters, digits"));
        assert!(given("inbox=relative/path").contains("from the top"));
        assert!(given("inbox=/no/such/folder/here").contains("cannot be found"));
        assert!(given(&format!("f={}", base.join("secret.txt").display())).contains("not a folder"));
        let twice = Roots::parse(&[format!("a={}", root.display()), format!("a={}", base.display())]);
        assert!(twice.err().unwrap_or_default().contains("twice"));
        assert!(
            ok.session("other")
                .err()
                .unwrap_or_default()
                .contains("allows inbox")
        );
        assert!(
            Roots::default()
                .session("x")
                .err()
                .unwrap_or_default()
                .contains("--files-root x=")
        );
        let _ = std::fs::remove_dir_all(base);
    }

    #[cfg(unix)]
    #[test]
    fn a_link_that_leads_out_is_refused_and_one_inside_is_followed() {
        let (base, root) = folder();
        let _ = std::os::unix::fs::symlink(base.join("secret.txt"), root.join("in/leak.txt"));
        let _ = std::os::unix::fs::symlink(&base, root.join("up"));
        let _ = std::os::unix::fs::symlink(root.join("in/a.csv"), root.join("same.csv"));
        assert!(resolve(&root, &rel("in/leak.txt")).is_err());
        assert!(resolve(&root, &rel("up/secret.txt")).is_err());
        assert!(
            resolve(&root, &rel("up/new.txt")).is_err(),
            "a write through a link out"
        );
        assert!(resolve(&root, &rel("up")).is_err());
        assert_eq!(
            resolve(&root, &rel("same.csv")).ok(),
            Some(root.join("in/a.csv")),
            "a link inside the folder is followed"
        );
        assert_eq!(
            resolve(&root, &rel("in/new/x.txt")).ok(),
            Some(root.join("in/new/x.txt"))
        );
        assert!(open_file(&root, &rel("in/leak.txt")).is_err());
        assert!(
            open_file(&root, &rel("in"))
                .err()
                .unwrap_or_default()
                .contains("is a folder")
        );
        let mut listed = Vec::new();
        assert!(!list_into(&root, &root, &Rel::default(), 3, 100, &mut listed).unwrap_or(true));
        let names: Vec<String> = listed.iter().map(|l| l.rel.text()).collect();
        assert_eq!(
            names,
            ["in", "in/a.csv", "same.csv"],
            "links out are left out, links are not gone into"
        );
        // A delete or a move acts on the link itself, never on what it leads to.
        assert_eq!(
            resolve_entry(&root, &rel("in/leak.txt")).ok(),
            Some(root.join("in/leak.txt"))
        );
        let _ = std::fs::remove_dir_all(base);
    }
}
