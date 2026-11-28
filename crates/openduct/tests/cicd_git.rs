// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Git sync, end to end against a stand-in of GitHub's REST API on this machine (the Git database API and pulls, as
//! GitHub documents them): a workspace's workflows saved to a folder of a repository in one commit, nothing
//! committed when nothing changed, a push to the branch deployed through the signed webhook (a forged delivery and
//! another branch refused or left alone), a folder with a wrong file refused whole, and a save that opens a pull
//! request. This is a stand-in, not GitHub itself.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde_json::{Value, json};
use studio::{call, owner, start_with};

const TOKEN: &str = "gh-token-TEST";

/// A repository as the stand-in keeps it: blobs by id, trees as their files, commits, branches, pull requests.
#[derive(Default)]
struct Repo {
    blobs: HashMap<String, String>,
    trees: HashMap<String, BTreeMap<String, String>>,
    commits: HashMap<String, String>,
    refs: BTreeMap<String, String>,
    pulls: Vec<Value>,
    n: u64,
}

fn blob_id(text: &str) -> String {
    let mut bytes = format!("blob {}\0", text.len()).into_bytes();
    bytes.extend_from_slice(text.as_bytes());
    ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl Repo {
    fn next(&mut self) -> String {
        self.n += 1;
        format!("{:040x}", 0x00c0_ffee_0000_u64 + self.n)
    }

    /// A commit on `branch` holding `files` (path to text) over what it held.
    fn commit(&mut self, branch: &str, files: &[(&str, &str)]) -> String {
        let mut tree = self
            .refs
            .get(branch)
            .and_then(|c| self.commits.get(c))
            .and_then(|t| self.trees.get(t))
            .cloned()
            .unwrap_or_default();
        for (path, text) in files {
            let id = blob_id(text);
            self.blobs.insert(id.clone(), (*text).to_owned());
            tree.insert((*path).to_owned(), id);
        }
        let tree_id = format!("tree{}", self.next());
        self.trees.insert(tree_id.clone(), tree);
        let sha = self.next();
        self.commits.insert(sha.clone(), tree_id);
        self.refs.insert(branch.to_owned(), sha.clone());
        sha
    }

    fn file(&self, branch: &str, path: &str) -> Option<String> {
        let tree = self.trees.get(self.commits.get(self.refs.get(branch)?)?)?;
        self.blobs.get(tree.get(path)?).cloned()
    }

    /// A tree's entries: `id` a branch, a commit, a tree, or `<tree>:<folder>`.
    fn listing(&self, id: &str) -> Option<Value> {
        let (root, prefix) = id.split_once(':').unwrap_or((id, ""));
        let root = match (self.refs.get(root), self.commits.get(root)) {
            (Some(c), _) => self.commits.get(c)?.clone(),
            (None, Some(t)) => t.clone(),
            _ => root.to_owned(),
        };
        let files = self.trees.get(&root)?;
        let mut entries: Vec<Value> = Vec::new();
        for (path, blob) in files {
            let rest = if prefix.is_empty() {
                path.as_str()
            } else {
                match path.strip_prefix(&format!("{prefix}/")) {
                    Some(r) => r,
                    None => continue,
                }
            };
            match rest.split_once('/') {
                Some((folder, _)) => {
                    let sub = if prefix.is_empty() {
                        folder.to_owned()
                    } else {
                        format!("{prefix}/{folder}")
                    };
                    let entry = json!({ "path": folder, "mode": "040000", "type": "tree", "sha": format!("{root}:{sub}") });
                    if !entries.contains(&entry) {
                        entries.push(entry);
                    }
                }
                None => entries.push(json!({ "path": rest, "mode": "100644", "type": "blob", "sha": blob })),
            }
        }
        Some(json!({ "sha": id, "tree": entries, "truncated": false }))
    }

    /// One request of the REST API: its status and answer.
    fn answer(&mut self, method: &str, path: &str, body: &Value) -> (u16, Value) {
        let Some(rest) = path.strip_prefix("/api/v3/repos/acme/flows/") else {
            return (404, json!({ "message": "Not Found" }));
        };
        let parts: Vec<&str> = rest.split('/').collect();
        match (method, parts.as_slice()) {
            ("GET", ["git", "trees", id]) => self
                .listing(id)
                .map_or((404, json!({ "message": "Not Found" })), |t| (200, t)),
            ("GET", ["git", "blobs", sha]) => match self.blobs.get(*sha) {
                Some(text) => (
                    200,
                    json!({ "sha": sha, "encoding": "base64", "size": text.len(), "content": base64::engine::general_purpose::STANDARD.encode(text) }),
                ),
                None => (404, json!({ "message": "Not Found" })),
            },
            ("GET", ["git", "ref", "heads", branch]) => match self.refs.get(*branch) {
                Some(sha) => (
                    200,
                    json!({ "ref": format!("refs/heads/{branch}"), "object": { "sha": sha, "type": "commit" } }),
                ),
                None => (404, json!({ "message": "Not Found" })),
            },
            ("GET", ["git", "commits", sha]) => match self.commits.get(*sha) {
                Some(tree) => (200, json!({ "sha": sha, "tree": { "sha": tree } })),
                None => (404, json!({ "message": "Not Found" })),
            },
            ("POST", ["git", "trees"]) => {
                let Some(mut files) = self
                    .trees
                    .get(body["base_tree"].as_str().unwrap_or_default())
                    .cloned()
                else {
                    return (422, json!({ "message": "base_tree is not a tree" }));
                };
                for e in body["tree"].as_array().unwrap() {
                    assert_eq!(
                        (e["mode"].as_str(), e["type"].as_str()),
                        (Some("100644"), Some("blob"))
                    );
                    let text = e["content"].as_str().unwrap().to_owned();
                    let id = blob_id(&text);
                    self.blobs.insert(id.clone(), text);
                    files.insert(e["path"].as_str().unwrap().to_owned(), id);
                }
                let id = format!("tree{}", self.next());
                self.trees.insert(id.clone(), files);
                (201, json!({ "sha": id, "tree": [], "truncated": false }))
            }
            ("POST", ["git", "commits"]) => {
                let tree = body["tree"].as_str().unwrap().to_owned();
                assert!(
                    self.trees.contains_key(&tree)
                        && body["message"].is_string()
                        && body["parents"].is_array()
                );
                let sha = self.next();
                self.commits.insert(sha.clone(), tree.clone());
                (201, json!({ "sha": sha, "tree": { "sha": tree } }))
            }
            ("PATCH", ["git", "refs", "heads", branch]) => {
                let sha = body["sha"].as_str().unwrap().to_owned();
                self.refs.insert((*branch).to_owned(), sha.clone());
                (
                    200,
                    json!({ "ref": format!("refs/heads/{branch}"), "object": { "sha": sha } }),
                )
            }
            ("POST", ["git", "refs"]) => {
                let name = body["ref"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("refs/heads/")
                    .unwrap()
                    .to_owned();
                self.refs.insert(name, body["sha"].as_str().unwrap().to_owned());
                (201, json!({ "ref": body["ref"] }))
            }
            ("POST", ["pulls"]) => {
                self.pulls.push(body.clone());
                let n = self.pulls.len();
                (
                    201,
                    json!({ "number": n, "html_url": format!("https://github.example/acme/flows/pull/{n}") }),
                )
            }
            _ => (404, json!({ "message": "Not Found" })),
        }
    }
}

/// The stand-in on this machine: every request must carry the repository's token.
fn stand_in(repo: Arc<Mutex<Repo>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut seen = Vec::new();
            let mut buf = [0u8; 16384];
            let (head, body) = loop {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break (String::new(), Vec::new()),
                    Ok(n) => seen.extend_from_slice(&buf[..n]),
                }
                let Some(at) = seen.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let head = String::from_utf8_lossy(&seen[..at]).into_owned();
                let length: usize = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                if seen.len() >= at + 4 + length {
                    break (head, seen[at + 4..at + 4 + length].to_vec());
                }
            };
            if head.is_empty() {
                continue;
            }
            let mut first = head.lines().next().unwrap().split(' ');
            let (method, path) = (first.next().unwrap().to_owned(), first.next().unwrap().to_owned());
            let signed = head
                .lines()
                .any(|l| l.eq_ignore_ascii_case(&format!("authorization: Bearer {TOKEN}")));
            let (status, answer) = if signed {
                let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                repo.lock()
                    .unwrap()
                    .answer(&method, path.split('?').next().unwrap(), &body)
            } else {
                (401, json!({ "message": "Bad credentials" }))
            };
            let text = answer.to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
        }
    });
    addr
}

fn signature(secret: &str, body: &str) -> String {
    let tag = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes()),
        body.as_bytes(),
    );
    format!(
        "sha256={}",
        tag.as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

fn deliver(studio: &str, workspace: &str, event: &str, body: &str, signed_with: &str) -> studio::Answer {
    studio::http(
        studio,
        "POST",
        &format!("/hooks/git/{workspace}"),
        &[
            "content-type: application/json",
            &format!("x-github-event: {event}"),
            &format!("x-hub-signature-256: {}", signature(signed_with, body)),
        ],
        Some(body),
    )
}

fn workflow_named(studio: &str, signed: &[String], name: &str) -> Value {
    let all = call(studio, signed, "GET", "/api/workflows", None);
    all.body["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == name)
        .cloned()
        .unwrap_or(Value::Null)
}

#[test]
fn workflows_are_saved_to_a_repository_and_deployed_from_it_on_a_signed_push() {
    let repo = Arc::new(Mutex::new(Repo::default()));
    repo.lock().unwrap().commit("main", &[("README.md", "# Flows\n")]);
    let host = stand_in(Arc::clone(&repo));
    let data = tempfile::tempdir().unwrap();
    let running = start_with(
        data.path(),
        &[
            "--listen",
            "127.0.0.1:0",
            "--studio-listen",
            "127.0.0.1:0",
            "--allow-private-network",
        ],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let workspace = call(&studio, &signed, "GET", "/api/session", None).body["workspaces"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "github", "kind": "bearer", "fields": { "token": TOKEN } }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let sub = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(r#"{"definition": {"name": "Sub", "steps": [{"set": {"a": 1}}]}}"#),
    );
    let sub_id = sub.body["id"].as_str().unwrap().to_owned();
    let caller =
        json!({ "definition": { "name": "Caller", "steps": [{ "run_workflow": { "workflow": sub_id } }] } });
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            "/api/workflows",
            Some(&caller.to_string())
        )
        .status,
        201
    );

    // Git sync set: a credential that is not a bearer one, or an address that is not the repository's, refused.
    let wrong = call(
        &studio,
        &signed,
        "PUT",
        "/api/cicd/settings",
        Some(
            r#"{"git": {"provider": "github", "url": "https://github.com/acme", "branch": "main", "credential": "github"}}"#,
        ),
    );
    assert_eq!(wrong.status, 422, "{}", wrong.body);
    let settings = json!({ "git": {
        "provider": "github", "url": format!("http://{host}/acme/flows"), "branch": "main", "folder": "flows",
        "credential": "github",
    } });
    let set = call(
        &studio,
        &signed,
        "PUT",
        "/api/cicd/settings",
        Some(&settings.to_string()),
    );
    assert_eq!(set.status, 200, "{}", set.body);
    assert_eq!(set.body["git"]["hook"], format!("/hooks/git/{workspace}"));
    let secret = call(&studio, &signed, "POST", "/api/cicd/git/secret", Some("{}"));
    assert_eq!(secret.status, 201, "{}", secret.body);
    let secret = secret.body["secret"].as_str().unwrap().to_owned();
    assert!(
        !call(&studio, &signed, "GET", "/api/cicd/settings", None)
            .body
            .to_string()
            .contains(&secret),
        "shown once"
    );

    // Save to Git: one commit with a file a workflow; another workflow named by its name, not its id.
    let saved = call(
        &studio,
        &signed,
        "POST",
        "/api/cicd/git/save",
        Some(r#"{"message": "First save"}"#),
    );
    assert_eq!(saved.status, 200, "{}", saved.body);
    assert_eq!(saved.body["saved"], 2);
    let file = repo.lock().unwrap().file("main", "flows/caller.yaml").unwrap();
    assert!(
        file.contains("workflow: Sub") && !file.contains(&sub_id),
        "{file}"
    );
    assert!(
        repo.lock().unwrap().file("main", "README.md").is_some(),
        "the rest of the repository kept"
    );
    let again = call(&studio, &signed, "POST", "/api/cicd/git/save", Some("{}"));
    assert_eq!(
        (again.body["saved"].as_u64(), again.body["unchanged"].as_u64()),
        (Some(0), Some(2)),
        "{}",
        again.body
    );

    // A developer's push to the branch: one workflow changed, one added that turns itself on.
    let sub_file = repo.lock().unwrap().file("main", "flows/sub.yaml").unwrap();
    let pushed = repo.lock().unwrap().commit(
        "main",
        &[
            ("flows/sub.yaml", &sub_file.replace("a: 1", "a: 2")),
            (
                "flows/extra.yaml",
                "name: Extra\non: true\ntrigger: manual\nsteps:\n  - set: { b: true }\n",
            ),
        ],
    );
    let push = json!({ "ref": "refs/heads/main", "after": pushed, "deleted": false }).to_string();
    let forged = deliver(&studio, &workspace, "push", &push, "not the secret");
    assert_eq!(forged.status, 401, "{}", forged.body);
    assert_eq!(workflow_named(&studio, &signed, "Extra"), Value::Null);
    assert_eq!(deliver(&studio, &workspace, "ping", "{}", &secret).status, 200);
    let elsewhere = json!({ "ref": "refs/heads/feature", "after": pushed }).to_string();
    assert_eq!(
        deliver(&studio, &workspace, "push", &elsewhere, &secret).status,
        202,
        "another branch is left alone"
    );
    let deployed = deliver(&studio, &workspace, "push", &push, &secret);
    assert_eq!(deployed.status, 200, "{}", deployed.body);
    assert_eq!(deployed.body["summary"]["create"], 1);
    assert_eq!(deployed.body["summary"]["update"], 1);
    assert_eq!(workflow_named(&studio, &signed, "Extra")["active"], true);
    let v = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{sub_id}/versions"),
        None,
    );
    assert_eq!(v.body["versions"][0]["definition"]["steps"][0]["set"]["a"], 2);
    assert_eq!(v.body["versions"][0]["source"]["source"], "git");
    assert_eq!(
        v.body["versions"][0]["source"]["detail"],
        format!("main at {}", &pushed[..12])
    );
    let shown = call(&studio, &signed, "GET", "/api/cicd/settings", None);
    assert_eq!(shown.body["last_deploy"]["ok"], true, "{}", shown.body);

    // A wrong file in the folder: the deploy is refused whole, and says so.
    repo.lock().unwrap().commit(
        "main",
        &[
            ("flows/sub.yaml", &sub_file.replace("a: 1", "a: 3")),
            ("flows/broken.yaml", "name: Broken\ntrigger: { nonsense: {} }\n"),
        ],
    );
    let refused = call(&studio, &signed, "POST", "/api/cicd/git/deploy", Some("{}"));
    assert_eq!(refused.status, 422, "{}", refused.body);
    assert!(
        refused.body.to_string().contains("flows/broken.yaml"),
        "{}",
        refused.body
    );
    let v = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{sub_id}/versions"),
        None,
    );
    assert_eq!(
        v.body["versions"][0]["definition"]["steps"][0]["set"]["a"], 2,
        "nothing changed"
    );
    assert_eq!(
        call(&studio, &signed, "GET", "/api/cicd/settings", None).body["last_deploy"]["ok"],
        false
    );

    // Saved as a pull request: a branch of its own, and a request into main.
    let mut proposing = settings.clone();
    proposing["git"]["changes"] = json!("pull_request");
    assert_eq!(
        call(
            &studio,
            &signed,
            "PUT",
            "/api/cicd/settings",
            Some(&proposing.to_string())
        )
        .status,
        200
    );
    let renamed = json!({ "definition": { "name": "Sub", "description": "now described", "steps": [{ "set": { "a": 2 } }] } });
    assert_eq!(
        call(
            &studio,
            &signed,
            "PUT",
            &format!("/api/workflows/{sub_id}"),
            Some(&renamed.to_string())
        )
        .status,
        200
    );
    let proposed = call(&studio, &signed, "POST", "/api/cicd/git/save", Some("{}"));
    assert_eq!(proposed.status, 200, "{}", proposed.body);
    assert_eq!(
        proposed.body["pull_request"],
        "https://github.example/acme/flows/pull/1"
    );
    let r = repo.lock().unwrap();
    assert_eq!(r.pulls[0]["base"], "main");
    let head = r.pulls[0]["head"].as_str().unwrap().to_owned();
    assert!(head.starts_with("openduct/my-workspace-"), "{head}");
    assert!(r.file(&head, "flows/sub.yaml").unwrap().contains("now described"));
    assert!(
        !r.file("main", "flows/sub.yaml")
            .unwrap()
            .contains("now described"),
        "main untouched until merged"
    );
}
