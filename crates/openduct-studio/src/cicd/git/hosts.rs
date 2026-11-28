// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A repository on GitHub (or GitHub Enterprise Server), GitLab or Gitea, reached through its REST API - no git
//! program: a folder's files listed and read, files committed in one commit to a branch or to a new one, and a pull
//! (merge) request opened. Every request goes out as a workflow's HTTP step does - never to this machine's own
//! network unless OpenDuct was started to allow it - with the workspace's token credential, a time limit and a
//! bounded answer.
//!
//! - GitHub: the Git database API (`git/ref`, `git/trees`, `git/blobs`, `git/commits`, `git/refs`) and `pulls`.
//! - GitLab: `repository/tree`, `repository/files/:path/raw`, `repository/commits` (several files, one commit) and
//!   `merge_requests`.
//! - Gitea: `contents` (a folder listed; several files changed in one commit), `raw`, and `pulls`.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use openduct_flow::{Effects, HttpRequest};
use serde_json::{Value, json};

use crate::workflow_files::{MAX_FILE, MAX_FILES, is_workflow_file};

/// How long one request to the host may take.
const TIME: Duration = Duration::from_secs(20);
/// The most pages of a GitLab folder listing read (100 entries each).
const MAX_PAGES: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Host {
    GitHub,
    GitLab,
    Gitea,
}

impl Host {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        match name {
            "github" => Some(Self::GitHub),
            "gitlab" => Some(Self::GitLab),
            "gitea" => Some(Self::Gitea),
            _ => None,
        }
    }
}

/// A repository, and where in it the workflows are.
#[derive(Clone)]
pub(crate) struct Repo {
    pub host: Host,
    /// The API's address: `https://api.github.com`, `https://git.example.com/api/v4` ...
    pub api: String,
    /// `owner/repo` (GitHub, Gitea); the project's path, encoded (GitLab).
    pub path: String,
    pub branch: String,
    /// The folder, without slashes at either end; empty for the repository's top.
    pub folder: String,
    pub token: String,
    pub effects: Arc<dyn Effects>,
}

impl std::fmt::Debug for Repo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Repo")
            .field("api", &self.api)
            .field("path", &self.path)
            .field("branch", &self.branch)
            .field("folder", &self.folder)
            .finish_non_exhaustive()
    }
}

/// The repository's address as `(scheme://host[:port], [path segments])`, `.git` taken off the last.
pub(crate) fn split_url(url: &str) -> Option<(String, Vec<String>)> {
    let (scheme, rest) = url.split_once("://")?;
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    if host.is_empty() || host.contains('@') || !matches!(scheme, "http" | "https") {
        return None;
    }
    let mut segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if let Some(last) = segments.last_mut()
        && let Some(stem) = last.strip_suffix(".git")
    {
        *last = stem.to_owned();
    }
    let fine = |s: &String| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    segments
        .iter()
        .all(fine)
        .then(|| (format!("{scheme}://{host}"), segments))
}

/// The API's address and the repository's path in it, from the repository's address.
pub(crate) fn api_of(host: Host, url: &str) -> Result<(String, String), String> {
    let (origin, segments) =
        split_url(url).ok_or("url: the repository's address, as https://github.com/owner/repo")?;
    match host {
        Host::GitHub | Host::Gitea if segments.len() != 2 => {
            Err("url: the repository's address ends in owner/repo".to_owned())
        }
        Host::GitLab if segments.len() < 2 => {
            Err("url: the project's address ends in group/project".to_owned())
        }
        Host::GitHub => {
            let api = if origin == "https://github.com" {
                "https://api.github.com".to_owned()
            } else {
                format!("{origin}/api/v3")
            };
            Ok((api, segments.join("/")))
        }
        Host::Gitea => Ok((format!("{origin}/api/v1"), segments.join("/"))),
        Host::GitLab => Ok((format!("{origin}/api/v4"), segments.join("%2F"))),
    }
}

/// A path's segments percent-encoded for a URL; with `slashes`, `/` too.
fn encoded(path: &str, slashes: bool) -> String {
    let mut out = String::new();
    for b in path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(char::from(b)),
            b'/' if !slashes => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A file of the folder: its path in the repository and its blob's id.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub path: String,
    pub sha: String,
}

/// A file to commit: its path, its text, and whether it is there already (an update).
#[derive(Debug, Clone)]
pub(crate) struct Change {
    pub path: String,
    pub text: String,
    pub exists: Option<String>,
}

/// The id Git gives a file's bytes (a blob's SHA-1), to tell an unchanged file without reading it.
pub(crate) fn blob_id(text: &str) -> String {
    let mut bytes = format!("blob {}\0", text.len()).into_bytes();
    bytes.extend_from_slice(text.as_bytes());
    ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl Repo {
    fn headers(&self) -> Vec<(String, String)> {
        let pair = |k: &str, v: String| (k.to_owned(), v);
        match self.host {
            Host::GitHub => vec![
                pair("accept", "application/vnd.github+json".to_owned()),
                pair("authorization", format!("Bearer {}", self.token)),
                pair("x-github-api-version", "2022-11-28".to_owned()),
            ],
            Host::GitLab => vec![
                pair("accept", "application/json".to_owned()),
                pair("private-token", self.token.clone()),
            ],
            Host::Gitea => vec![
                pair("accept", "application/json".to_owned()),
                pair("authorization", format!("token {}", self.token)),
            ],
        }
    }

    /// One request: its status and body; a status other than `want` is an error saying what the host said.
    async fn ask(
        &self,
        method: &str,
        url: String,
        body: Option<Value>,
        want: &[u16],
    ) -> Result<Vec<u8>, String> {
        let request = HttpRequest {
            read: method == "GET",
            method: method.to_owned(),
            url,
            headers: self.headers(),
            query: Vec::new(),
            body: body.map(|b| ("application/json".to_owned(), b.to_string().into_bytes())),
            timeout: TIME,
        };
        let shown = format!("{method} {}", request.url);
        let raw = self
            .effects
            .http_bytes(request)
            .await
            .map_err(|e| format!("{shown}: {e}"))?;
        let meta: Value = serde_json::from_str(&raw.answer.to_json()).unwrap_or(Value::Null);
        let status = meta.get("status").and_then(Value::as_u64).unwrap_or(0);
        let body = raw.body.unwrap_or_default();
        if want.iter().any(|w| u64::from(*w) == status) {
            return Ok(body);
        }
        let said = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|v| v.get("message").or_else(|| v.get("error")).cloned())
            .map(|m| m.as_str().map_or_else(|| m.to_string(), str::to_owned))
            .unwrap_or_default();
        let said: String = said.chars().take(300).collect();
        Err(match status {
            401 | 403 => format!(
                "{shown}: the host refused the token ({status}{}): give it access to the repository",
                colon(&said)
            ),
            404 => format!(
                "{shown}: not found (404{}): check the repository, branch and folder",
                colon(&said)
            ),
            _ => format!("{shown}: the host answered {status}{}", colon(&said)),
        })
    }

    async fn ask_json(
        &self,
        method: &str,
        url: String,
        body: Option<Value>,
        want: &[u16],
    ) -> Result<Value, String> {
        let bytes = self.ask(method, url, body, want).await?;
        serde_json::from_slice(&bytes).map_err(|_| "the host's answer is not JSON".to_owned())
    }

    fn in_folder(&self, name: &str) -> String {
        if self.folder.is_empty() {
            name.to_owned()
        } else {
            format!("{}/{name}", self.folder)
        }
    }

    /// The workflow files of the folder at `at` (a branch or a commit); none when the folder is not there.
    pub(crate) async fn list(&self, at: &str) -> Result<Vec<Entry>, String> {
        let mut out = match self.host {
            Host::GitHub => self.list_github(at).await?,
            Host::GitLab => self.list_gitlab(at).await?,
            Host::Gitea => self.list_gitea(at).await?,
        };
        out.retain(|e| is_workflow_file(e.path.rsplit('/').next().unwrap_or_default()));
        out.sort_by(|a, b| a.path.cmp(&b.path));
        if out.len() > MAX_FILES {
            return Err(format!("the folder holds more than {MAX_FILES} workflow files"));
        }
        Ok(out)
    }

    async fn list_github(&self, at: &str) -> Result<Vec<Entry>, String> {
        let base = format!("{}/repos/{}/git/trees", self.api, self.path);
        let mut tree = self
            .ask_json("GET", format!("{base}/{}", encoded(at, true)), None, &[200])
            .await?;
        for segment in self.folder.split('/').filter(|s| !s.is_empty()) {
            let sha = entries(&tree)
                .find(|e| {
                    e.get("path").and_then(Value::as_str) == Some(segment)
                        && e.get("type").and_then(Value::as_str) == Some("tree")
                })
                .and_then(|e| e.get("sha").and_then(Value::as_str));
            // No such folder yet: none of its files, then (Save to Git makes it).
            let Some(sha) = sha.map(str::to_owned) else {
                return Ok(Vec::new());
            };
            tree = self
                .ask_json("GET", format!("{base}/{sha}"), None, &[200])
                .await?;
        }
        Ok(entries(&tree)
            .filter(|e| e.get("type").and_then(Value::as_str) == Some("blob"))
            .filter_map(|e| {
                Some(Entry {
                    path: self.in_folder(e.get("path")?.as_str()?),
                    sha: e.get("sha")?.as_str()?.to_owned(),
                })
            })
            .collect())
    }

    async fn list_gitlab(&self, at: &str) -> Result<Vec<Entry>, String> {
        let mut out = Vec::new();
        for page in 1..=MAX_PAGES {
            let url = format!(
                "{}/projects/{}/repository/tree?path={}&ref={}&per_page=100&page={page}",
                self.api,
                self.path,
                encoded(&self.folder, true),
                encoded(at, true)
            );
            // A folder not there yet is answered 404: none of its files, then.
            let listed = self.ask_json("GET", url, None, &[200, 404]).await?;
            let items: Vec<&Value> = listed.as_array().map(|a| a.iter().collect()).unwrap_or_default();
            let full = items.len() >= 100;
            out.extend(
                items
                    .into_iter()
                    .filter(|e| e.get("type").and_then(Value::as_str) == Some("blob"))
                    .filter_map(|e| {
                        Some(Entry {
                            path: e.get("path")?.as_str()?.to_owned(),
                            sha: e.get("id")?.as_str()?.to_owned(),
                        })
                    }),
            );
            if !full {
                break;
            }
        }
        Ok(out)
    }

    async fn list_gitea(&self, at: &str) -> Result<Vec<Entry>, String> {
        let url = if self.folder.is_empty() {
            format!(
                "{}/repos/{}/contents?ref={}",
                self.api,
                self.path,
                encoded(at, true)
            )
        } else {
            format!(
                "{}/repos/{}/contents/{}?ref={}",
                self.api,
                self.path,
                encoded(&self.folder, false),
                encoded(at, true)
            )
        };
        // A folder not there yet is answered 404: none of its files, then.
        let listed = self.ask_json("GET", url, None, &[200, 404]).await?;
        Ok(listed
            .as_array()
            .map(|a| a.iter().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .filter(|e| e.get("type").and_then(Value::as_str) == Some("file"))
            .filter_map(|e| {
                Some(Entry {
                    path: e.get("path")?.as_str()?.to_owned(),
                    sha: e.get("sha")?.as_str()?.to_owned(),
                })
            })
            .collect())
    }

    /// A file's text at `at`.
    pub(crate) async fn read(&self, entry: &Entry, at: &str) -> Result<String, String> {
        let bytes = match self.host {
            Host::GitHub => {
                let url = format!("{}/repos/{}/git/blobs/{}", self.api, self.path, entry.sha);
                let blob = self.ask_json("GET", url, None, &[200]).await?;
                let content: String = blob
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                B64.decode(content)
                    .map_err(|_| format!("{}: not Base64", entry.path))?
            }
            Host::GitLab => {
                let url = format!(
                    "{}/projects/{}/repository/files/{}/raw?ref={}",
                    self.api,
                    self.path,
                    encoded(&entry.path, true),
                    encoded(at, true)
                );
                self.ask("GET", url, None, &[200]).await?
            }
            Host::Gitea => {
                let url = format!(
                    "{}/repos/{}/raw/{}?ref={}",
                    self.api,
                    self.path,
                    encoded(&entry.path, false),
                    encoded(at, true)
                );
                self.ask("GET", url, None, &[200]).await?
            }
        };
        if bytes.len() > MAX_FILE {
            return Err(format!("{}: larger than {MAX_FILE} bytes", entry.path));
        }
        String::from_utf8(bytes).map_err(|_| format!("{}: not UTF-8 text", entry.path))
    }

    /// The files committed in one commit: to the branch, or to `new_branch` made from it. The new commit's id.
    pub(crate) async fn commit(
        &self,
        changes: &[Change],
        message: &str,
        new_branch: Option<&str>,
    ) -> Result<String, String> {
        match self.host {
            Host::GitHub => self.commit_github(changes, message, new_branch).await,
            Host::GitLab => {
                let actions: Vec<Value> = changes
                    .iter()
                    .map(|c| json!({ "action": if c.exists.is_some() { "update" } else { "create" }, "file_path": c.path, "content": c.text }))
                    .collect();
                let mut body = json!({ "branch": new_branch.unwrap_or(&self.branch), "commit_message": message, "actions": actions });
                if new_branch.is_some()
                    && let Some(fields) = body.as_object_mut()
                {
                    fields.insert("start_branch".to_owned(), json!(self.branch));
                }
                let url = format!("{}/projects/{}/repository/commits", self.api, self.path);
                let made = self.ask_json("POST", url, Some(body), &[200, 201]).await?;
                made.get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| "the host gave no commit".to_owned())
            }
            Host::Gitea => {
                let files: Vec<Value> = changes
                    .iter()
                    .map(|c| {
                        let mut f = json!({ "operation": if c.exists.is_some() { "update" } else { "create" }, "path": c.path, "content": B64.encode(&c.text) });
                        if let (Some(sha), Some(fields)) = (&c.exists, f.as_object_mut()) {
                            fields.insert("sha".to_owned(), json!(sha));
                        }
                        f
                    })
                    .collect();
                let mut body = json!({ "files": files, "branch": self.branch, "message": message });
                if let (Some(new), Some(fields)) = (new_branch, body.as_object_mut()) {
                    fields.insert("new_branch".to_owned(), json!(new));
                }
                let url = format!("{}/repos/{}/contents", self.api, self.path);
                let made = self.ask_json("POST", url, Some(body), &[200, 201]).await?;
                made.pointer("/commit/sha")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| "the host gave no commit".to_owned())
            }
        }
    }

    async fn commit_github(
        &self,
        changes: &[Change],
        message: &str,
        new_branch: Option<&str>,
    ) -> Result<String, String> {
        let base = format!("{}/repos/{}/git", self.api, self.path);
        let head = self
            .ask_json(
                "GET",
                format!("{base}/ref/heads/{}", encoded(&self.branch, false)),
                None,
                &[200],
            )
            .await?;
        let parent = head
            .pointer("/object/sha")
            .and_then(Value::as_str)
            .ok_or("the branch has no commit")?
            .to_owned();
        let commit = self
            .ask_json("GET", format!("{base}/commits/{parent}"), None, &[200])
            .await?;
        let base_tree = commit
            .pointer("/tree/sha")
            .and_then(Value::as_str)
            .ok_or("the commit has no tree")?
            .to_owned();
        let entries: Vec<Value> = changes
            .iter()
            .map(|c| json!({ "path": c.path, "mode": "100644", "type": "blob", "content": c.text }))
            .collect();
        let tree = self
            .ask_json(
                "POST",
                format!("{base}/trees"),
                Some(json!({ "base_tree": base_tree, "tree": entries })),
                &[201],
            )
            .await?;
        let tree = tree
            .get("sha")
            .and_then(Value::as_str)
            .ok_or("the host made no tree")?
            .to_owned();
        let made = self
            .ask_json(
                "POST",
                format!("{base}/commits"),
                Some(json!({ "message": message, "tree": tree, "parents": [parent] })),
                &[201],
            )
            .await?;
        let sha = made
            .get("sha")
            .and_then(Value::as_str)
            .ok_or("the host made no commit")?
            .to_owned();
        match new_branch {
            Some(new) => {
                self.ask(
                    "POST",
                    format!("{base}/refs"),
                    Some(json!({ "ref": format!("refs/heads/{new}"), "sha": sha })),
                    &[201],
                )
                .await?;
            }
            None => {
                self.ask(
                    "PATCH",
                    format!("{base}/refs/heads/{}", encoded(&self.branch, false)),
                    Some(json!({ "sha": sha, "force": false })),
                    &[200],
                )
                .await?;
            }
        }
        Ok(sha)
    }

    /// A pull (merge) request from `head` into the branch: its address.
    pub(crate) async fn pull_request(&self, head: &str, title: &str, body: &str) -> Result<String, String> {
        let (url, request, field) = match self.host {
            Host::GitHub | Host::Gitea => (
                format!("{}/repos/{}/pulls", self.api, self.path),
                json!({ "title": title, "head": head, "base": self.branch, "body": body }),
                "html_url",
            ),
            Host::GitLab => (
                format!("{}/projects/{}/merge_requests", self.api, self.path),
                json!({ "title": title, "source_branch": head, "target_branch": self.branch, "description": body, "remove_source_branch": true }),
                "web_url",
            ),
        };
        let made = self.ask_json("POST", url, Some(request), &[200, 201]).await?;
        Ok(made
            .get(field)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned())
    }
}

fn colon(said: &str) -> String {
    if said.is_empty() {
        String::new()
    } else {
        format!(": {said}")
    }
}

fn entries(tree: &Value) -> impl Iterator<Item = &Value> {
    tree.get("tree").and_then(Value::as_array).into_iter().flatten()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_repository_address_gives_its_api_and_path() {
        assert_eq!(
            api_of(Host::GitHub, "https://github.com/acme/flows.git").unwrap(),
            ("https://api.github.com".to_owned(), "acme/flows".to_owned())
        );
        assert_eq!(
            api_of(Host::GitHub, "https://git.example.com/acme/flows")
                .unwrap()
                .0,
            "https://git.example.com/api/v3"
        );
        assert_eq!(
            api_of(Host::GitLab, "https://gitlab.com/acme/team/flows").unwrap(),
            (
                "https://gitlab.com/api/v4".to_owned(),
                "acme%2Fteam%2Fflows".to_owned()
            )
        );
        assert_eq!(
            api_of(Host::Gitea, "http://127.0.0.1:3000/a/b").unwrap().0,
            "http://127.0.0.1:3000/api/v1"
        );
        assert!(api_of(Host::GitHub, "https://github.com/acme").is_err());
        assert!(api_of(Host::GitHub, "https://github.com/acme/flows/tree/main").is_err());
        assert!(api_of(Host::GitHub, "https://user:pw@github.com/a/b").is_err());
        assert!(api_of(Host::GitHub, "ftp://github.com/a/b").is_err());
        assert!(api_of(Host::GitLab, "https://gitlab.com/a/../b").is_err());
    }

    #[test]
    fn a_files_id_is_gits_blob_id() {
        // `printf 'hello\n' | git hash-object --stdin`
        assert_eq!(blob_id("hello\n"), "ce013625030ba8dba906f756967f9e9ca394464a");
        assert_eq!(encoded("a b/c?.yaml", false), "a%20b/c%3F.yaml");
        assert_eq!(encoded("a/b", true), "a%2Fb");
    }
}
