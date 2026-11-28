// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workspace's custom apps (`/api/apps/custom`): made on the "Add an app" page from an API description (its
//! address, or a file) or an MCP server's address - with the generator `openduct app from-openapi` uses - or written
//! by hand, previewed, then saved. Each is kept on the log beside the workspace's other records, so every node has
//! it, and belongs to its workspace alone: its steps are read only in that workspace's workflows, and its sign-ins
//! are credential kinds no other workspace may use (see `openduct_flow::app_files::custom`).
//!
//! Editors and admins add, change and remove them; viewers see them. A description or a server is reached through
//! the workspace's effects, so as an HTTP step reaches one: this machine's own network refused unless the studio
//! allows it.

use std::sync::LazyLock;

use hyper::header::{self, HeaderValue};
use hyper::{Method, StatusCode};
use openduct_flow::Workflow;
use openduct_flow::app_files::{self, custom, generate, quality};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, SCHEMA, now};

/// The topic custom apps are kept in.
pub(crate) const CUSTOM_APPS: &str = "__openduct.model.custom-apps";
/// The largest request the page sends: a description of a few megabytes, as text in JSON.
pub(crate) const MAX_BODY: usize = 12 * 1024 * 1024;
/// The longest README kept, in bytes.
const MAX_README: usize = 64 * 1024;
/// The most recorded tests kept with an app, and the largest of them.
const MAX_TESTS: usize = custom::MAX_ACTIONS + 20;
const MAX_TEST: usize = 32 * 1024;
/// How long reaching a description or a server may take.
const FETCH: std::time::Duration = std::time::Duration::from_secs(60);

/// Descriptions read at once on a node: each may be large.
static MAKING: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(2));

/// A custom app, as kept.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CustomApp {
    pub schema: u32,
    pub workspace: String,
    pub id: String,
    /// Its `app.yaml`, as written.
    pub definition: String,
    #[serde(default)]
    pub readme: String,
    /// Its recorded tests: (file, text).
    #[serde(default)]
    pub tests: Vec<(String, String)>,
    /// Where it was made from.
    #[serde(default)]
    pub source: String,
    pub by: String,
    pub created: u64,
    pub updated: u64,
    #[serde(default)]
    pub deleted: Option<u64>,
}

fn key(workspace: &str, id: &str) -> String {
    format!("{workspace}/{id}")
}

/// A workflow of `workspace`, read with its custom apps in reach (and no other workspace's).
pub(crate) fn workflow(workspace: &str, definition: &Value, id: Option<&str>) -> Result<Workflow, String> {
    custom::within(workspace, || Workflow::from_json(definition, id))
}

/// Brings this node's custom apps up to date with the log: every workspace's, as kept.
pub(crate) async fn sync(state: &State) {
    if let Err(e) = state.model.custom_apps.refresh().await {
        tracing::warn!(error = e, "cannot read the custom apps");
        return;
    }
    let mut by_workspace: std::collections::BTreeMap<String, Vec<(String, String)>> = Default::default();
    for (_, app) in state.model.custom_apps.all().await {
        let apps = by_workspace.entry(app.workspace.clone()).or_default();
        if app.deleted.is_none() {
            apps.push((app.id, app.definition));
        }
    }
    for (workspace, apps) in by_workspace {
        for problem in custom::set(&workspace, &apps) {
            tracing::warn!(workspace, problem, "a custom app cannot be read");
        }
    }
}

/// Brings one workspace's custom apps up to date, after a change on this node.
async fn sync_workspace(state: &State, workspace: &str) {
    let apps: Vec<(String, String)> = state
        .model
        .custom_apps
        .all()
        .await
        .into_iter()
        .filter(|(_, a)| a.workspace == workspace && a.deleted.is_none())
        .map(|(_, a)| (a.id, a.definition))
        .collect();
    for problem in custom::set(workspace, &apps) {
        tracing::warn!(workspace, problem, "a custom app cannot be read");
    }
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::POST, ["api", "apps", "custom"]) => make(state, call).await,
        (&Method::GET, ["api", "apps", "custom"]) => list(state, call).await,
        (&Method::GET, ["api", "apps", "custom", id]) => one(state, call, id).await,
        (&Method::GET, ["api", "apps", "custom", id, "app.yaml"]) => export(state, call, id).await,
        (&Method::PUT, ["api", "apps", "custom", id]) => change(state, call, id).await,
        (&Method::DELETE, ["api", "apps", "custom", id]) => remove(state, call, id).await,
        _ => return None,
    })
}

fn text<'a>(body: &'a Value, name: &str) -> &'a str {
    body.get(name).and_then(Value::as_str).unwrap_or_default().trim()
}

fn names(body: &Value, name: &str) -> Option<Vec<String>> {
    body.get(name)?.as_array().map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .take(generate::MAX_ACTIONS * 2)
            .map(str::to_owned)
            .collect()
    })
}

/// The text at `url`, reached through the workspace's effects.
async fn fetch(state: &State, workspace: &str, url: &str) -> Result<String, String> {
    if !url.starts_with("https://") && !url.starts_with("http://") {
        return Err("url: the description's https:// address".to_owned());
    }
    let effects = state.effects_for(workspace)?;
    let request = openduct_flow::HttpRequest {
        read: true,
        method: "GET".to_owned(),
        url: url.to_owned(),
        headers: vec![(
            "accept".to_owned(),
            "application/json, application/yaml, text/yaml, */*".to_owned(),
        )],
        query: Vec::new(),
        body: None,
        timeout: FETCH,
    };
    let raw = effects.http_bytes(request).await?;
    let status = match &raw.answer {
        openduct_expr::Value::Record(r) => r.get("status").map(openduct_flow::as_text).unwrap_or_default(),
        _ => String::new(),
    };
    if !status.starts_with('2') {
        return Err(format!("{url} answered {status}"));
    }
    String::from_utf8(raw.body.unwrap_or_default()).map_err(|_| format!("{url}: the description is not text"))
}

/// The sign-ins a custom app may have: not a sign-in at the vendor, which needs the installation's registration.
fn allowed(s: &generate::ProposedSignIn) -> bool {
    !s.kind.contains("at the vendor")
}

/// Makes an app as the request asks: from a description, a server, or a definition written by hand.
async fn made(state: &State, workspace: &str, body: &Value) -> Result<generate::Generated, String> {
    let id = text(body, "name").to_owned();
    let title = Some(text(body, "title").to_owned()).filter(|t| !t.is_empty());
    let category = Some(text(body, "category").to_owned()).filter(|t| !t.is_empty());
    let actions = body
        .get("actions")
        .and_then(Value::as_u64)
        .map_or(generate::DEFAULT_ACTIONS, |n| {
            usize::try_from(n).unwrap_or(0).min(custom::MAX_ACTIONS)
        });
    let today = generate::today();
    match text(body, "from") {
        "openapi" => {
            let url = text(body, "url");
            let (spec, source) = if url.is_empty() {
                let given = body.get("text").and_then(Value::as_str).unwrap_or_default();
                if given.trim().is_empty() {
                    return Err("text: the description, or url: its address".to_owned());
                }
                let file = text(body, "file");
                (
                    given.to_owned(),
                    if file.is_empty() {
                        "a description uploaded".to_owned()
                    } else {
                        file.to_owned()
                    },
                )
            } else {
                (fetch(state, workspace, url).await?, url.to_owned())
            };
            let sign_in = match text(body, "sign_in") {
                "" => None,
                s => Some(s.parse::<generate::SignInChoice>()?),
            };
            let mut options = generate::Options {
                id,
                name: title,
                category,
                actions,
                only: names(body, "only").unwrap_or_default(),
                chosen: names(body, "chosen"),
                sign_ins: names(body, "sign_ins"),
                sign_in,
                base_url: Some(text(body, "base_url").to_owned()).filter(|b| !b.is_empty()),
                docs: Some(text(body, "docs").to_owned()).filter(|d| !d.is_empty()),
                origin: url.split_once("://").and_then(|(scheme, rest)| {
                    rest.split('/').next().map(|host| format!("{scheme}://{host}"))
                }),
                source,
                made_on: today,
            };
            let _turn = MAKING
                .acquire()
                .await
                .map_err(|_| "the studio is stopping".to_owned())?;
            let first = {
                let (spec, options) = (spec.clone(), options.clone());
                tokio::task::spawn_blocking(move || generate::from_openapi(&spec, &options))
                    .await
                    .map_err(|_| "the description could not be read".to_owned())??
            };
            // Unless the person chose, the sign-ins a custom app may have are the ones made.
            let fit: Vec<String> = first
                .sign_ins
                .iter()
                .filter(|s| allowed(s))
                .map(|s| s.id.clone())
                .collect();
            if options.sign_ins.is_some() || fit.is_empty() || fit.len() == first.sign_ins.len() {
                return Ok(first);
            }
            options.sign_ins = Some(fit);
            tokio::task::spawn_blocking(move || generate::from_openapi(&spec, &options))
                .await
                .map_err(|_| "the description could not be read".to_owned())?
        }
        "mcp" => {
            let url = text(body, "url").to_owned();
            let effects = state.effects_for(workspace)?;
            let token = text(body, "token");
            let credential = (!token.is_empty()).then(|| openduct_flow::Credential {
                kind: "bearer".to_owned(),
                fields: [("token".to_owned(), token.to_owned())].into_iter().collect(),
            });
            let server = openduct_flow::mcp::Server {
                url: url.clone(),
                credential,
                timeout: FETCH,
            };
            let mut session = openduct_flow::mcp::Session::new(effects.as_ref(), server);
            let listed = session.tools().await;
            session.close().await;
            let tools = match listed? {
                openduct_flow::mcp::Reached::Answered(t) => t,
                openduct_flow::mcp::Reached::NotSent(_) => return Err("the server was not asked".to_owned()),
            };
            generate::from_mcp(
                &tools,
                &generate::McpOptions {
                    id,
                    name: title,
                    category,
                    url,
                    actions,
                    chosen: names(body, "chosen"),
                    docs: Some(text(body, "docs").to_owned()).filter(|d| !d.is_empty()),
                    made_on: today,
                },
            )
        }
        "definition" => {
            let definition = body
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let app = custom::read(&definition, "the definition")?;
            Ok(generate::Generated {
                id: app.id.clone(),
                name: app.name.clone(),
                app_yaml: definition,
                readme: body
                    .get("readme")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                tests: Vec::new(),
                proposed: Vec::new(),
                sign_ins: Vec::new(),
                not_done: Vec::new(),
            })
        }
        _ => Err("from: openapi (a description), mcp (a server) or definition (an app.yaml)".to_owned()),
    }
}

/// What a custom app's checks and quality come to: what stops it being kept (none when it may be), and its
/// report, with the recordings that do not hold.
fn looked_at(definition: &str, readme: &str, tests: &[(String, String)]) -> (Vec<String>, Value) {
    let app = match custom::read(definition, "the app") {
        Ok(a) => a,
        Err(e) => return (vec![e], Value::Null),
    };
    let mut problems = Vec::new();
    if readme.len() > MAX_README {
        problems.push(format!("readme: at most {} KB", MAX_README / 1024));
    }
    if tests.len() > MAX_TESTS || tests.iter().any(|(_, t)| t.len() > MAX_TEST) {
        problems.push(format!(
            "tests: at most {MAX_TESTS}, each of at most {} KB",
            MAX_TEST / 1024
        ));
    }
    let failing: Vec<String> = tests
        .iter()
        .filter_map(|(file, t)| app_files::recorded::check(&app, t, file).err())
        .collect();
    let mut report = quality::report(&app, tests, !readme.trim().is_empty()).to_json();
    if let Some(r) = report.as_object_mut() {
        r.insert("failing".to_owned(), json!(failing));
    }
    (problems, report)
}

fn shown(app: &CustomApp) -> Value {
    let (_, report) = looked_at(&app.definition, &app.readme, &app.tests);
    let name = custom::read(&app.definition, &app.id)
        .map(|a| a.name)
        .unwrap_or_else(|_| app.id.clone());
    json!({
        "id": app.id, "name": name, "definition": app.definition, "readme": app.readme,
        "tests": app.tests.iter().map(|(f, t)| json!({ "file": f, "text": t })).collect::<Vec<_>>(),
        "source": app.source, "created": app.created, "updated": app.updated, "quality": report,
    })
}

/// `POST /api/apps/custom`: an app made and shown (`save: false`), or made and kept.
async fn make(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let workspace = access.workspace.id.clone();
    let made = match made(state, &workspace, &call.body).await {
        Ok(m) => m,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let (mut problems, report) = looked_at(&made.app_yaml, &made.readme, &made.tests);
    let _ = state.model.custom_apps.refresh().await;
    let mine: Vec<CustomApp> = state
        .model
        .custom_apps
        .all()
        .await
        .into_iter()
        .map(|(_, a)| a)
        .filter(|a| a.workspace == workspace && a.deleted.is_none())
        .collect();
    if mine.iter().any(|a| a.id == made.id) {
        problems.push(format!(
            "{}: the workspace has an app of this name already",
            made.id
        ));
    }
    if mine.len() >= custom::MAX_APPS {
        problems.push(format!(
            "a workspace has at most {} custom apps",
            custom::MAX_APPS
        ));
    }
    let sign_ins: Vec<Value> = made
        .sign_ins
        .iter()
        .map(|s| json!({ "id": s.id, "label": s.label, "kind": s.kind, "chosen": s.chosen, "allowed": allowed(s) }))
        .collect();
    let mut shown_made = made.to_json();
    if let Some(m) = shown_made.as_object_mut() {
        m.insert("sign_ins".to_owned(), json!(sign_ins));
    }
    let save = call.body.get("save").and_then(Value::as_bool).unwrap_or(false);
    if !save {
        return json(
            StatusCode::OK,
            &json!({ "made": shown_made, "quality": report, "problems": problems, "saved": false }),
        );
    }
    if let Some(first) = problems.first() {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, first);
    }
    let record = CustomApp {
        schema: SCHEMA,
        workspace: workspace.clone(),
        id: made.id.clone(),
        definition: made.app_yaml,
        readme: made.readme,
        tests: made.tests,
        source: text(&call.body, "url").to_owned(),
        by: access.user.id.clone(),
        created: now(),
        updated: now(),
        deleted: None,
    };
    if let Err(e) = state
        .model
        .custom_apps
        .put(&key(&workspace, &record.id), &record)
        .await
    {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    sync_workspace(state, &workspace).await;
    tracing::info!(workspace, app = record.id, "a custom app added");
    json(
        StatusCode::CREATED,
        &json!({ "app": shown(&record), "saved": true }),
    )
}

async fn mine(state: &State, workspace: &str, id: &str) -> Option<CustomApp> {
    let _ = state.model.custom_apps.refresh().await;
    state
        .model
        .custom_apps
        .get(&key(workspace, id))
        .await
        .filter(|a| a.deleted.is_none() && a.workspace == workspace)
}

/// `GET /api/apps/custom`: the workspace's custom apps.
async fn list(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let _ = state.model.custom_apps.refresh().await;
    let apps: Vec<Value> = state
        .model
        .custom_apps
        .all()
        .await
        .into_iter()
        .map(|(_, a)| a)
        .filter(|a| a.workspace == access.workspace.id && a.deleted.is_none())
        .map(|a| {
            let (_, report) = looked_at(&a.definition, &a.readme, &a.tests);
            let app = custom::read(&a.definition, &a.id).ok();
            json!({
                "id": a.id, "name": app.as_ref().map_or_else(|| a.id.clone(), |x| x.name.clone()),
                "actions": app.as_ref().map_or(0, |x| x.actions.len()), "score": report.get("score").cloned().unwrap_or(Value::Null),
                "source": a.source, "updated": a.updated,
            })
        })
        .collect();
    json(
        StatusCode::OK,
        &json!({ "apps": apps, "role": access.role, "most": custom::MAX_APPS }),
    )
}

async fn one(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    match mine(state, &access.workspace.id, id).await {
        Some(app) => json(StatusCode::OK, &shown(&app)),
        None => problem(StatusCode::NOT_FOUND, "no such app in this workspace"),
    }
}

/// `GET /api/apps/custom/<id>/app.yaml`: its definition, to save as a file.
async fn export(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(app) = mine(state, &access.workspace.id, id).await else {
        return problem(StatusCode::NOT_FOUND, "no such app in this workspace");
    };
    let mut answer = hyper::Response::new(http_body_util::Full::new(bytes::Bytes::from(app.definition)));
    let headers = answer.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/yaml; charset=utf-8"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"app.yaml\""),
    );
    answer
}

/// `PUT /api/apps/custom/<id>`: its definition (and README) changed; its recordings kept and run again.
async fn change(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(mut app) = mine(state, &access.workspace.id, id).await else {
        return problem(StatusCode::NOT_FOUND, "no such app in this workspace");
    };
    if let Some(d) = call.body.get("definition").and_then(Value::as_str) {
        app.definition = d.to_owned();
    }
    if let Some(r) = call.body.get("readme").and_then(Value::as_str) {
        app.readme = r.to_owned();
    }
    let (problems, _) = looked_at(&app.definition, &app.readme, &app.tests);
    if let Some(first) = problems.first() {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, first);
    }
    match custom::read(&app.definition, id) {
        Ok(read) if read.id != id => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!(
                    "app: the definition is of {}; an app's name stays as it was made",
                    read.id
                ),
            );
        }
        Ok(_) => {}
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    }
    app.updated = now();
    if let Err(e) = state.model.custom_apps.put(&key(&app.workspace, id), &app).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    sync_workspace(state, &app.workspace).await;
    json(StatusCode::OK, &json!({ "app": shown(&app) }))
}

async fn remove(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(mut app) = mine(state, &access.workspace.id, id).await else {
        return problem(StatusCode::NOT_FOUND, "no such app in this workspace");
    };
    app.deleted = Some(now());
    app.updated = now();
    if let Err(e) = state.model.custom_apps.put(&key(&app.workspace, id), &app).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    sync_workspace(state, &app.workspace).await;
    json(StatusCode::OK, &json!({ "removed": id }))
}

/// The workspace's custom apps as the editor's catalog shows apps (`GET /api/apps`), marked as custom.
pub(crate) async fn catalog(state: &State, call: &Call) -> Vec<Value> {
    let Ok(access) = access(state, call, Role::Viewer).await else {
        return Vec::new();
    };
    custom::of(&access.workspace.id)
        .into_iter()
        .map(|a| {
            let mut shown = crate::app_catalog::shown(state, a);
            if let Some(o) = shown.as_object_mut() {
                o.insert("custom".to_owned(), json!(true));
            }
            shown
        })
        .collect()
}

/// The workspace's custom apps' sign-ins as kinds of credential (`GET /api/credential-kinds`).
pub(crate) async fn credential_kinds(state: &State, call: &Call) -> Vec<Value> {
    let Ok(access) = access(state, call, Role::Viewer).await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for app in custom::of(&access.workspace.id) {
        for s in &app.sign_ins {
            let Some(k) = openduct_flow::apps::credential_kind(&s.id) else {
                continue;
            };
            let fields: Vec<Value> = k
                .fields
                .iter()
                .map(|f| {
                    let name = f.trim_end_matches('?');
                    json!({ "name": name, "optional": f.ends_with('?'), "secret": k.secrets.contains(&name) })
                })
                .collect();
            out.push(json!({
                "kind": k.kind, "label": k.label, "fields": fields, "app": app.id, "connect": false,
                "registered": false, "custom": true,
            }));
        }
    }
    out
}

/// Whether `kind` is another workspace's custom kind of credential, which this one may not use.
pub(crate) fn kind_of_another(kind: &str, workspace: &str) -> bool {
    custom::owner_of_kind(kind).is_some_and(|w| w != workspace)
}
