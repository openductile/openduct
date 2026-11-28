// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Git sync: a workspace's workflows kept in a folder of a repository on GitHub, GitLab or Gitea.
//!
//! - **Settings**: the repository's address (https), a branch, a folder, and a token credential of the workspace
//!   (a bearer credential, sealed as every credential is) that may read and write the repository.
//! - **Save to Git** writes each workflow as `workflow pull` does and commits the files that changed, in one commit:
//!   to the branch, or to a new branch with a pull (merge) request into it.
//! - **Deploy from Git** reads the folder and pushes it as `workflow push` does (every file checked first, nothing
//!   changed if one is wrong): by hand, or on a push to the branch, delivered to `/hooks/git/<workspace>` and signed
//!   with the webhook secret the studio makes (GitHub and Gitea: `X-Hub-Signature-256` / `X-Gitea-Signature`;
//!   GitLab: its signing token's `webhook-signature`, or the secret token).

mod hook;
mod hosts;

use std::collections::{BTreeMap, HashMap, HashSet};

use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub(crate) use hook::webhook;
use hosts::{Change, Host, Repo, api_of, blob_id};

use super::apply::{Source, current, plan_and_apply, portable, scopes_of};
use super::plan::{Incoming, Options};
use crate::State;
use crate::api::{Access, access};
use crate::api_tokens::by_token;
use crate::http::{Answer, Call, json, problem};
use crate::model::{PRODUCTION, Role, SCHEMA, now};
use crate::workflow_files::{self, in_scope};

/// A workspace's repository, as its admins set it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct GitSettings {
    /// `github`, `gitlab` or `gitea`.
    pub provider: String,
    pub url: String,
    pub branch: String,
    /// The folder of the workflow files; empty for the repository's top.
    #[serde(default)]
    pub folder: String,
    /// The name of the workspace's bearer credential the host takes.
    pub credential: String,
    /// `commit` (to the branch) or `pull_request`.
    #[serde(default)]
    pub changes: String,
    /// The webhook's secret, sealed; empty until one is made.
    #[serde(default)]
    pub hook_secret: String,
    /// GitLab's signing token for its webhook, sealed, when given.
    #[serde(default)]
    pub signing_token: String,
}

impl GitSettings {
    pub(crate) fn shown(&self, workspace: &str) -> Value {
        json!({
            "provider": self.provider, "url": self.url, "branch": self.branch, "folder": self.folder,
            "credential": self.credential, "changes": self.changes,
            "hook": format!("/hooks/git/{workspace}"),
            "secret": !self.hook_secret.is_empty(), "signing_token": !self.signing_token.is_empty(),
        })
    }
}

fn refused(why: &str) -> Answer {
    problem(StatusCode::UNPROCESSABLE_ENTITY, why)
}

/// A branch's name as Git takes it, kept simple.
fn branch_ok(b: &str) -> bool {
    !b.is_empty()
        && b.len() <= 200
        && !b.starts_with(['-', '/', '.'])
        && !b.ends_with(['/', '.'])
        && !b.contains("..")
        && !b.contains("//")
        && b.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
}

fn folder_ok(f: &str) -> bool {
    f.len() <= 200
        && f.split('/').all(|s| !s.is_empty() && s != "." && s != "..")
        && f.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
}

fn seal_bound(workspace: &str, what: &str) -> String {
    format!("{workspace}/git/{what}")
}

/// A sealed secret's value.
pub(crate) fn opened(state: &State, workspace: &str, what: &str, sealed: &str) -> Option<String> {
    if sealed.is_empty() {
        return None;
    }
    state
        .vault
        .open(&seal_bound(workspace, what), sealed)
        .and_then(|f| f.get("value").cloned())
}

fn sealed(state: &State, workspace: &str, what: &str, value: &str) -> Result<String, Answer> {
    let fields: BTreeMap<String, String> = [("value".to_owned(), value.to_owned())].into();
    state
        .vault
        .seal(&seal_bound(workspace, what), &fields)
        .map_err(|e| problem(StatusCode::INTERNAL_SERVER_ERROR, &e))
}

/// The Git settings a request sends, checked; `None` when it takes Git sync away.
pub(crate) async fn settings_from(
    state: &State,
    access: &Access,
    given: &Value,
    before: Option<GitSettings>,
) -> Result<Option<GitSettings>, Answer> {
    if given.is_null() {
        return Ok(None);
    }
    let text = |name: &str| {
        given
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned()
    };
    let provider = text("provider");
    let Some(host) = Host::parse(&provider) else {
        return Err(refused("git.provider: github, gitlab or gitea"));
    };
    let url = text("url").trim_end_matches('/').to_owned();
    let plain_ok = state.allow_private_network && url.starts_with("http://");
    if url.len() > 500 || !(url.starts_with("https://") || plain_ok) {
        return Err(refused(
            "git.url: the repository's https address (http only with --allow-private-network)",
        ));
    }
    if let Err(why) = api_of(host, &url) {
        return Err(refused(&format!("git.{why}")));
    }
    let branch = text("branch");
    if !branch_ok(&branch) {
        return Err(refused("git.branch: a branch's name, as main"));
    }
    let folder = text("folder").trim_matches('/').to_owned();
    if !folder.is_empty() && !folder_ok(&folder) {
        return Err(refused(
            "git.folder: a folder of the repository, as workflows or ops/flows",
        ));
    }
    let credential = text("credential");
    if state
        .vault
        .get(&access.workspace.id, PRODUCTION, &credential)
        .is_none()
    {
        // Made a moment ago, perhaps.
        state.vault.refresh(&state.model).await;
    }
    match state.vault.get(&access.workspace.id, PRODUCTION, &credential) {
        Some(c) if c.kind == "bearer" => {}
        Some(c) => {
            return Err(refused(&format!(
                "git.credential: {credential} is a {} credential; the token is kept as a bearer one",
                c.kind
            )));
        }
        None => {
            return Err(refused(
                "git.credential: the name of a bearer credential of this workspace holding the host's token",
            ));
        }
    }
    let changes = match text("changes").as_str() {
        "" | "commit" => "commit".to_owned(),
        "pull_request" => "pull_request".to_owned(),
        _ => return Err(refused("git.changes: commit or pull_request")),
    };
    let before = before.unwrap_or_default();
    let signing_token = match given.get("signing_token") {
        None | Some(Value::Null) => before.signing_token,
        Some(Value::String(t)) if t.trim().is_empty() => String::new(),
        Some(Value::String(t)) if t.len() <= 200 => sealed(state, &access.workspace.id, "signing", t.trim())?,
        Some(_) => return Err(refused("git.signing_token: GitLab's signing token, as whsec_...")),
    };
    Ok(Some(GitSettings {
        provider,
        url,
        branch,
        folder,
        credential,
        changes,
        hook_secret: before.hook_secret,
        signing_token,
    }))
}

/// The workspace's repository, reached with its token credential.
pub(crate) fn repo(state: &State, workspace: &str, git: &GitSettings) -> Result<Repo, String> {
    let host = Host::parse(&git.provider).ok_or("Git sync: no host chosen")?;
    let (api, path) = api_of(host, &git.url)?;
    let token = state
        .vault
        .get(workspace, PRODUCTION, &git.credential)
        .and_then(|c| c.fields.get("token").cloned())
        .filter(|t| !t.is_empty())
        .ok_or_else(|| format!("the credential {} holds no token", git.credential))?;
    Ok(Repo {
        host,
        api,
        path,
        branch: git.branch.clone(),
        folder: git.folder.clone(),
        token,
        effects: state.effects_for(workspace)?,
    })
}

fn git_of(delivery: &super::Delivery) -> Result<GitSettings, Answer> {
    delivery.git.clone().ok_or_else(|| {
        problem(
            StatusCode::CONFLICT,
            "this workspace has no Git sync: set it in Settings",
        )
    })
}

/// A new webhook secret: shown this once, kept sealed.
pub(crate) async fn new_secret(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if by_token(&access) {
        return problem(
            StatusCode::FORBIDDEN,
            "a webhook secret is made in the studio, by an admin",
        );
    }
    let mut delivery = state.cicd.delivery(&access.workspace.id).await;
    let mut git = match git_of(&delivery) {
        Ok(g) => g,
        Err(answer) => return answer,
    };
    let secret = match crate::api_tokens::new_token() {
        Ok(t) => t.trim_start_matches(crate::api_tokens::PREFIX).to_owned(),
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    git.hook_secret = match sealed(state, &access.workspace.id, "hook", &secret) {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    delivery.git = Some(git);
    delivery.updated = now();
    if let Err(e) = state.cicd.settings.put(&access.workspace.id, &delivery).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    json(
        StatusCode::CREATED,
        &json!({ "secret": secret, "hook": format!("/hooks/git/{}", access.workspace.id) }),
    )
}

/// The workflow files of the folder at `at`, read: by name, and those that are not workflows' files.
async fn read_folder(
    repo: &Repo,
    at: &str,
) -> Result<Vec<(hosts::Entry, Result<workflow_files::WorkflowFile, String>)>, String> {
    let mut out = Vec::new();
    for entry in repo.list(at).await? {
        let name = entry.path.rsplit('/').next().unwrap_or_default().to_owned();
        let read = match repo.read(&entry, at).await {
            Ok(text) => workflow_files::read(&name, &text),
            Err(e) => Err(e),
        };
        out.push((entry, read));
    }
    Ok(out)
}

/// `POST /api/cicd/git/save` `{message}`: each workflow (those the token reaches) written to the folder, those that
/// changed committed.
pub(crate) async fn save(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let git = match git_of(&state.cicd.delivery(&access.workspace.id).await) {
        Ok(g) => g,
        Err(answer) => return answer,
    };
    let repo = match repo(state, &access.workspace.id, &git) {
        Ok(r) => r,
        Err(e) => return problem(StatusCode::CONFLICT, &e),
    };
    let existing = match read_folder(&repo, &git.branch).await {
        Ok(e) => e,
        Err(e) => return problem(StatusCode::BAD_GATEWAY, &e),
    };
    // A workflow keeps the file that names it; a new one is given a name no file has.
    let mut by_name: HashMap<String, hosts::Entry> = HashMap::new();
    let mut taken: HashSet<String> = HashSet::new();
    for (entry, read) in &existing {
        taken.insert(entry.path.rsplit('/').next().unwrap_or_default().to_owned());
        if let Ok(f) = read
            && let Some(name) = workflow_files::name_of(&f.definition)
        {
            by_name.entry(name.to_owned()).or_insert_with(|| entry.clone());
        }
    }
    let scopes = scopes_of(state, &access).await;
    let all = current(state, &access.workspace.id).await;
    let mut changes = Vec::new();
    let mut unchanged = 0;
    for (record, c) in &all {
        if !in_scope(scopes.as_deref(), &record.name) {
            continue;
        }
        let text = workflow_files::write(&portable(&c.definition, &all), record.active);
        let (path, exists) = match by_name.get(&record.name) {
            Some(e) => (e.path.clone(), Some(e.sha.clone())),
            None => {
                let file = workflow_files::file_name(&record.name, &mut taken);
                let path = if git.folder.is_empty() {
                    file
                } else {
                    format!("{}/{file}", git.folder)
                };
                (path, None)
            }
        };
        if exists.as_deref() == Some(blob_id(&text).as_str()) {
            unchanged += 1;
            continue;
        }
        changes.push(hosts::Change { path, text, exists });
    }
    if changes.is_empty() {
        return json(
            StatusCode::OK,
            &json!({ "saved": 0, "unchanged": unchanged, "commit": null }),
        );
    }
    let message = call
        .body
        .get("message")
        .and_then(Value::as_str)
        .map(|m| m.trim().chars().take(500).collect::<String>())
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| {
            format!(
                "{} workflow(s) saved from OpenDuct ({})",
                changes.len(),
                access.workspace.name
            )
        });
    let outcome = commit_or_propose(&repo, &git, &changes, &message, &access).await;
    match outcome {
        Ok((commit, request)) => {
            tracing::info!(workspace = %access.workspace.id, by = %access.user.email, files = changes.len(),
                commit = %commit, "workflows saved to Git");
            let files: Vec<&String> = changes.iter().map(|c: &Change| &c.path).collect();
            json(
                StatusCode::OK,
                &json!({ "saved": changes.len(), "unchanged": unchanged, "files": files, "commit": commit, "pull_request": request }),
            )
        }
        Err(e) => problem(StatusCode::BAD_GATEWAY, &e),
    }
}

async fn commit_or_propose(
    repo: &Repo,
    git: &GitSettings,
    changes: &[Change],
    message: &str,
    access: &Access,
) -> Result<(String, Option<String>), String> {
    if git.changes != "pull_request" {
        return Ok((repo.commit(changes, message, None).await?, None));
    }
    let branch = format!(
        "openduct/{}-{}",
        openduct_flow::slug(&access.workspace.name),
        now()
    );
    let commit = repo.commit(changes, message, Some(&branch)).await?;
    let body = format!(
        "Saved from the OpenDuct workspace {} by {}.",
        access.workspace.name, access.user.email
    );
    let url = repo.pull_request(&branch, message, &body).await?;
    Ok((commit, Some(url)))
}

/// `POST /api/cicd/git/deploy` `{dry_run}`: the branch's folder pushed now.
pub(crate) async fn deploy_by_hand(state: &State, call: &Call) -> Answer {
    let dry_run = call.body.get("dry_run").and_then(Value::as_bool).unwrap_or(false);
    let access = match access(state, call, if dry_run { Role::Viewer } else { Role::Editor }).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let git = match git_of(&state.cicd.delivery(&access.workspace.id).await) {
        Ok(g) => g,
        Err(answer) => return answer,
    };
    let (status, body) = deploy(state, call, &access, &git, &git.branch, dry_run).await;
    json(status, &body)
}

/// The folder at `at` (a branch, or a pushed commit) pushed into the workspace; the outcome kept as its last deploy.
pub(crate) async fn deploy(
    state: &State,
    call: &Call,
    access: &Access,
    git: &GitSettings,
    at: &str,
    dry_run: bool,
) -> (StatusCode, Value) {
    let outcome = async {
        let repo = repo(state, &access.workspace.id, git)
            .map_err(|e| (StatusCode::CONFLICT, json!({ "error": e })))?;
        let files = read_folder(&repo, at)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, json!({ "error": e })))?;
        let mut incoming = Vec::new();
        let mut problems = Vec::new();
        for (entry, read) in files {
            match read {
                Ok(f) => incoming.push(Incoming {
                    file: entry.path,
                    definition: f.definition,
                    on: f.on,
                }),
                Err(e) => problems.push(json!({ "file": entry.path, "name": null, "problem": e })),
            }
        }
        if !problems.is_empty() {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                super::plan::Refusal::Problems(problems).body(),
            ));
        }
        if incoming.is_empty() {
            let place = if git.folder.is_empty() {
                "the repository's top".to_owned()
            } else {
                format!("the folder {}", git.folder)
            };
            let why = format!("{place} holds no workflow files at {at}: nothing was changed");
            return Err((StatusCode::UNPROCESSABLE_ENTITY, json!({ "error": why })));
        }
        let short: String = at.chars().take(12).collect();
        let source = Source {
            kind: "git",
            detail: Some(format!("{} at {short}", git.branch)),
            from_file: false,
        };
        let options = Options {
            activate: false,
            prune: false,
            scopes: scopes_of(state, access).await,
        };
        Ok(plan_and_apply(state, call, access, &incoming, &options, &source, dry_run).await)
    }
    .await;
    let (status, mut body) = outcome.unwrap_or_else(|e| e);
    if let Some(fields) = body.as_object_mut() {
        fields.insert("at".to_owned(), json!(at));
    }
    if !dry_run {
        let mut delivery = state.cicd.delivery(&access.workspace.id).await;
        delivery.schema = SCHEMA;
        delivery.last_deploy = Some(json!({
            "at": now(), "commit": at, "ok": status == StatusCode::OK, "error": body.get("error"),
            "summary": body.get("summary"), "by": access.user.email,
        }));
        if let Err(e) = state.cicd.settings.put(&access.workspace.id, &delivery).await {
            tracing::warn!(error = e, "cannot keep the last deploy from Git");
        }
    }
    (status, body)
}
