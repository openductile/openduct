// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workflows through CI/CD: a workspace's workflows pulled as files, compared with a folder, pushed from it (all
//! checked first), rolled back, and promoted from one workspace to the next - a workspace marked as development,
//! test or production, in an order; a production workspace may take changes only by push or promotion. Each version
//! saved this way records where it came from (a push, a promotion, a rollback, Git) and with which token. Git sync
//! ([`git`]) commits a workspace's workflows to a repository and deploys them from it.
//!
//! The API: `GET /api/cicd/workflows` (pull), `POST /api/cicd/push` (diff when `dry_run`), `POST /api/cicd/rollback`,
//! `POST /api/cicd/promote`, `GET|PUT /api/cicd/settings`, `GET /api/cicd/environments`, and Git's
//! `POST /api/cicd/git/{save,deploy,secret}` and `POST /hooks/git/<workspace>`.

mod apply;
mod git;
mod plan;
mod promote;

use hyper::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::State;
use crate::api::{Access, access};
use crate::api_tokens::{Tokens, by_token, person_of, token_of};
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, SCHEMA, Table, now, version_key};

pub(crate) const SETTINGS: &str = "__openduct.model.delivery";
pub(crate) const SOURCES: &str = "__openduct.model.version-sources";
/// The kinds of environment a workspace may be marked as.
pub(crate) const ENVIRONMENTS: &[&str] = &["development", "test", "production"];

/// A workspace's place among the environments, and its Git sync.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Delivery {
    pub schema: u32,
    pub workspace: String,
    /// `development`, `test` or `production`; `None`: not an environment.
    #[serde(default)]
    pub environment: Option<String>,
    /// Its place: promotion goes from a lower one to a higher one.
    #[serde(default)]
    pub order: u32,
    /// Changes come only by push or promotion, not edits in the studio.
    #[serde(default)]
    pub locked: bool,
    #[serde(default)]
    pub git: Option<git::GitSettings>,
    /// What the last deploy from Git did.
    #[serde(default)]
    pub last_deploy: Option<Value>,
    #[serde(default)]
    pub updated: u64,
    #[serde(default)]
    pub by: String,
}

/// Where a version came from, when not from the studio's editor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VersionSource {
    pub schema: u32,
    pub workflow: String,
    pub number: u64,
    /// `push`, `promotion`, `rollback` or `git`.
    pub source: String,
    /// The token it was made with, by name and id.
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub token_id: Option<String>,
    /// Who: a person's e-mail address, or the token's maker's.
    pub by: String,
    /// More: `from Test v4`, `to v2`, `commit 1a2b3c4`.
    #[serde(default)]
    pub detail: Option<String>,
    pub at: u64,
}

/// What the node keeps of CI/CD: API tokens, workspaces' settings, versions' sources, and one deploy at a time.
#[derive(Debug)]
pub(crate) struct Store {
    pub(crate) tokens: Tokens,
    pub(crate) settings: Table<Delivery>,
    pub(crate) sources: Table<VersionSource>,
    /// Git deliveries not signed, by address.
    pub(crate) hook_failures: crate::auth::Failures,
    deploying: tokio::sync::Mutex<()>,
}

impl Store {
    pub(crate) async fn open(broker: &str) -> Result<Self, String> {
        Ok(Self {
            tokens: Tokens::open(broker).await?,
            settings: Table::open(broker, SETTINGS).await?,
            sources: Table::open(broker, SOURCES).await?,
            hook_failures: crate::auth::Failures::default(),
            deploying: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) async fn delivery(&self, workspace: &str) -> Delivery {
        let _ = self.settings.refresh().await;
        self.settings.get(workspace).await.unwrap_or_else(|| Delivery {
            schema: SCHEMA,
            workspace: workspace.to_owned(),
            ..Delivery::default()
        })
    }
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::POST, ["hooks", "git", workspace]) => git::webhook(state, call, workspace).await,
        (&Method::GET, ["api", "cicd", "workflows"]) => apply::pull(state, call).await,
        (&Method::POST, ["api", "cicd", "push"]) => apply::push(state, call).await,
        (&Method::POST, ["api", "cicd", "rollback"]) => apply::rollback(state, call).await,
        (&Method::POST, ["api", "cicd", "promote"]) => promote::promote(state, call).await,
        (&Method::GET, ["api", "cicd", "settings"]) => settings(state, call).await,
        (&Method::PUT, ["api", "cicd", "settings"]) => set_settings(state, call).await,
        (&Method::GET, ["api", "cicd", "environments"]) => environments(state, call).await,
        (&Method::GET, ["api", "cicd", "whoami"]) => whoami(state, call).await,
        (&Method::POST, ["api", "cicd", "git", "save"]) => git::save(state, call).await,
        (&Method::POST, ["api", "cicd", "git", "deploy"]) => git::deploy_by_hand(state, call).await,
        (&Method::POST, ["api", "cicd", "git", "secret"]) => git::new_secret(state, call).await,
        _ => return None,
    })
}

/// Whether a person may change the workflows of the workspace in the studio: not when it takes changes only by
/// push or promotion. A token (a push) always may.
pub(crate) async fn studio_edit(state: &State, access: &Access) -> Result<(), Answer> {
    if by_token(access) || !state.cicd.delivery(&access.workspace.id).await.locked {
        return Ok(());
    }
    Err(problem(
        StatusCode::CONFLICT,
        "this workspace takes changes only by push or promotion: change the workflow where it is developed, then \
         promote or push it",
    ))
}

/// Where version `number` of workflow `workflow` came from, as the history shows it.
pub(crate) async fn source_of(state: &State, workflow: &str, number: u64) -> Value {
    match state.cicd.sources.get(&version_key(workflow, number)).await {
        Some(s) => json!({ "source": s.source, "token": s.token, "by": s.by, "detail": s.detail }),
        None => json!({ "source": "studio" }),
    }
}

/// Who a version is recorded as made by, and with which token.
pub(crate) async fn made_by(state: &State, access: &Access) -> (String, Option<(String, String)>) {
    match token_of(state, access).await {
        Some(t) => (t.by_email.clone(), Some((t.name, t.id))),
        None => (access.user.email.clone(), None),
    }
}

fn delivery_shown(d: &Delivery, admin: bool) -> Value {
    json!({
        "environment": d.environment, "order": d.order, "locked": d.locked,
        "git": d.git.as_ref().map(|g| g.shown(&d.workspace)),
        "last_deploy": d.last_deploy,
        "manage": admin,
    })
}

async fn settings(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let delivery = state.cicd.delivery(&access.workspace.id).await;
    json(
        StatusCode::OK,
        &delivery_shown(&delivery, access.role == Role::Admin && !by_token(&access)),
    )
}

/// The workspace's environment, order, lock and Git sync, as its admins set them in Settings.
async fn set_settings(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if by_token(&access) {
        return problem(
            StatusCode::FORBIDDEN,
            "a workspace's settings are changed in the studio, by an admin",
        );
    }
    let mut delivery = state.cicd.delivery(&access.workspace.id).await;
    let body = &call.body;
    if let Some(e) = body.get("environment") {
        delivery.environment = match e {
            Value::Null => None,
            Value::String(s) if ENVIRONMENTS.contains(&s.as_str()) => Some(s.clone()),
            _ => {
                return problem(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "environment: development, test, production, or null for none",
                );
            }
        };
    }
    if let Some(o) = body.get("order") {
        match o
            .as_u64()
            .and_then(|o| u32::try_from(o).ok())
            .filter(|o| *o <= 1000)
        {
            Some(o) => delivery.order = o,
            None => return problem(StatusCode::UNPROCESSABLE_ENTITY, "order: 0 to 1000, lower first"),
        }
    }
    if let Some(l) = body.get("locked") {
        match l.as_bool() {
            Some(l) => delivery.locked = l,
            None => return problem(StatusCode::UNPROCESSABLE_ENTITY, "locked: true or false"),
        }
    }
    if let Some(g) = body.get("git") {
        match git::settings_from(state, &access, g, delivery.git.take()).await {
            Ok(git) => delivery.git = git,
            Err(answer) => return answer,
        }
    }
    delivery.schema = SCHEMA;
    delivery.updated = now();
    delivery.by = access.user.email.clone();
    if let Err(e) = state.cicd.settings.put(&access.workspace.id, &delivery).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    tracing::info!(workspace = %access.workspace.id, by = %access.user.email, environment = ?delivery.environment,
        locked = delivery.locked, "a workspace's delivery settings changed");
    json(StatusCode::OK, &delivery_shown(&delivery, true))
}

/// The workspaces the asker sees, each with its environment and order, and whether the asker may promote into it:
/// what the studio's Promote button offers.
async fn environments(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(person) = person_of(state, &access).await else {
        return problem(StatusCode::UNAUTHORIZED, "sign in first");
    };
    let mut out = Vec::new();
    for (w, role) in state.model.workspaces_of(&person).await {
        let d = state.cicd.delivery(&w.id).await;
        if d.environment.is_none() {
            continue;
        }
        out.push(json!({
            "id": w.id, "name": w.name, "environment": d.environment, "order": d.order, "role": role,
            "here": w.id == access.workspace.id,
        }));
    }
    out.sort_by_key(|w| w["order"].as_u64());
    json(StatusCode::OK, &json!({ "environments": out }))
}

/// Who a request acts as: the workspace, its role, and the token's name, for the command line.
async fn whoami(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let token = token_of(state, &access).await;
    let d = state.cicd.delivery(&access.workspace.id).await;
    json(
        StatusCode::OK,
        &json!({
            "workspace": { "id": access.workspace.id, "name": access.workspace.name, "environment": d.environment },
            "role": access.role,
            "token": token.map(|t| json!({ "name": t.name, "expires": t.expires, "workflows": t.workflows })),
        }),
    )
}
