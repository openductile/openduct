// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! API tokens: how CI and other programs call the studio's API without a person's session. Each belongs to one
//! workspace and is made there by an admin, in Settings: a name, a role (viewer, editor or admin - never above its
//! maker's), an expiry (90 days unless said, a year at most), optional workflow scopes, and so many requests a minute.
//! It is shown once and kept only as its SHA-256; a revoked one stops working on every node within seconds.
//!
//! A request sends it as `Authorization: Bearer odapi_...` and is then taken as its maker's would be, limited to the
//! token's role: a maker who has since lost that role, or left the workspace, takes the token's power with them.
//! Every request made with one is written to the log with the token's name, and what it changes is recorded as the
//! token's (`token:<id>`). A token never makes or revokes tokens, nor signs in at an app's vendor.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use hyper::header::HeaderValue;
use hyper::{Method, StatusCode};
use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::State;
use crate::api::Access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, SCHEMA, Table, User, new_id, now};

/// Where tokens are kept.
pub(crate) const API_TOKENS: &str = "__openduct.model.api-tokens";
/// What every token begins with, so one found in a file or a log is known for what it is.
pub(crate) const PREFIX: &str = "odapi_";
/// How long a token lasts unless its maker says, and at most.
pub(crate) const DEFAULT_DAYS: u64 = 90;
pub(crate) const MAX_DAYS: u64 = 365;
/// Requests a minute: by default, and at most.
pub(crate) const PER_MINUTE: u32 = 120;
pub(crate) const MAX_PER_MINUTE: u32 = 1000;
/// The most live tokens a workspace has, and workflow scopes a token names.
pub(crate) const MAX_TOKENS: usize = 100;
const MAX_SCOPES: usize = 100;
/// How long tokens read once are trusted before they are read again: a revocation on another node.
const FRESH: Duration = Duration::from_secs(2);
/// What a user made of a token looks like: its id is this and the token's.
const TOKEN_USER: &str = "token:";

/// A token, as kept: never the token itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ApiToken {
    pub schema: u32,
    pub id: String,
    pub workspace: String,
    /// What it is for, as its maker named it ("GitHub Actions deploy").
    pub name: String,
    /// The SHA-256 of the token, in hex: what it is found by.
    pub hash: String,
    /// Its last four characters, to tell it from others.
    pub hint: String,
    pub role: Role,
    /// The workflows it reaches, by name or a beginning ending in `*`; none: every workflow.
    #[serde(default)]
    pub workflows: Option<Vec<String>>,
    pub per_minute: u32,
    pub created: u64,
    /// When it stops working, in seconds since 1970.
    pub expires: u64,
    /// Its maker: their id and e-mail address.
    pub by: String,
    pub by_email: String,
    #[serde(default)]
    pub revoked: Option<u64>,
}

/// The SHA-256 of a token, in hex.
pub(crate) fn hash(token: &str) -> String {
    digest::digest(&digest::SHA256, token.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A new token: 32 random bytes, URL-safe.
pub(crate) fn new_token() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "no randomness to make a token with".to_owned())?;
    Ok(format!(
        "{PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    ))
}

/// Whether a text could be a token at all: only one is ever looked up.
pub(crate) fn well_formed(token: &str) -> bool {
    token.len() == PREFIX.len() + 43
        && token.starts_with(PREFIX)
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// What each node keeps of the tokens: the record, each token's recent requests, wrong tokens by address.
#[derive(Debug)]
pub(crate) struct Tokens {
    pub(crate) table: Table<ApiToken>,
    requests: Mutex<HashMap<String, VecDeque<Instant>>>,
    failures: crate::auth::Failures,
    read: Mutex<Option<Instant>>,
}

impl Tokens {
    pub(crate) async fn open(broker: &str) -> Result<Self, String> {
        Ok(Self {
            table: Table::open(broker, API_TOKENS).await?,
            requests: Mutex::default(),
            failures: crate::auth::Failures::default(),
            read: Mutex::default(),
        })
    }

    /// Whether the token may make one more request now; if not, in how many seconds it may.
    pub(crate) fn allow(&self, token: &ApiToken) -> Result<(), u64> {
        allow(&self.requests, &token.id, token.per_minute)
    }

    /// The tokens read again when the last read is older than [`FRESH`].
    async fn fresh(&self) {
        let stale = self
            .read
            .lock()
            .map(|mut read| {
                let stale = read.is_none_or(|at| at.elapsed() >= FRESH);
                if stale {
                    *read = Some(Instant::now());
                }
                stale
            })
            .unwrap_or(true);
        if stale {
            let _ = self.table.refresh().await;
        }
    }

    /// A workspace's tokens, newest last; revoked ones too when asked.
    pub(crate) async fn of(&self, workspace: &str, revoked: bool) -> Vec<ApiToken> {
        let _ = self.table.refresh().await;
        let mut out: Vec<ApiToken> = self
            .table
            .all()
            .await
            .into_iter()
            .map(|(_, t)| t)
            .filter(|t| t.workspace == workspace && (revoked || t.revoked.is_none()))
            .collect();
        out.sort_by_key(|t| t.created);
        out
    }
}

/// One more request of `id` in its minute, if under `per_minute`; if not, in how many seconds there is room.
fn allow(requests: &Mutex<HashMap<String, VecDeque<Instant>>>, id: &str, per_minute: u32) -> Result<(), u64> {
    let Ok(mut requests) = requests.lock() else {
        return Err(60);
    };
    if requests.len() > 100_000 {
        requests.retain(|_, seen| seen.back().is_some_and(|t| t.elapsed() < Duration::from_secs(60)));
    }
    let seen = requests.entry(id.to_owned()).or_default();
    while seen
        .front()
        .is_some_and(|t| t.elapsed() >= Duration::from_secs(60))
    {
        seen.pop_front();
    }
    let limit = usize::try_from(per_minute.clamp(1, MAX_PER_MINUTE)).unwrap_or(1);
    if seen.len() >= limit {
        let oldest = seen.front().map_or(Duration::ZERO, Instant::elapsed);
        return Err(60u64.saturating_sub(oldest.as_secs()).max(1));
    }
    seen.push_back(Instant::now());
    Ok(())
}

/// Whether a token found works at `at` (seconds since 1970): not revoked, not expired.
pub(crate) fn standing(token: &ApiToken, at: u64) -> Result<(), &'static str> {
    if token.revoked.is_some() {
        return Err("this token was revoked");
    }
    if token.expires <= at {
        return Err("this token has expired: an admin of the workspace makes a new one in Settings");
    }
    Ok(())
}

/// Whether a request is made with a token, not a person's session.
pub(crate) fn by_token(access: &Access) -> bool {
    access.user.id.starts_with(TOKEN_USER)
}

/// The token a request was made with, if it was.
pub(crate) async fn token_of(state: &State, access: &Access) -> Option<ApiToken> {
    let id = access.user.id.strip_prefix(TOKEN_USER)?;
    state
        .cicd
        .tokens
        .table
        .all()
        .await
        .into_iter()
        .map(|(_, t)| t)
        .find(|t| t.id == id)
}

/// The person a request is made for: the one signed in, or the maker of the token it carries.
pub(crate) async fn person_of(state: &State, access: &Access) -> Option<User> {
    if !by_token(access) {
        return Some(access.user.clone());
    }
    let token = token_of(state, access).await?;
    state
        .model
        .users
        .get(&token.by_email)
        .await
        .filter(|u| u.id == token.by && u.deleted.is_none())
}

/// The bearer token a request carries, if it is one of ours.
fn bearer(call: &Call) -> Option<&str> {
    call.headers
        .iter()
        .find(|(k, _)| k == "authorization")
        .map(|(_, v)| v.trim())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim)
        .filter(|t| t.starts_with(PREFIX))
}

fn refused(status: StatusCode, why: &str) -> Answer {
    let mut answer = problem(status, why);
    if status == StatusCode::UNAUTHORIZED {
        answer.headers_mut().insert(
            hyper::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"OpenDuct\""),
        );
    }
    answer
}

/// Requests no token makes, whatever its role: tokens themselves, MCP's tokens, signing in at an app's vendor.
fn people_only(path: &str) -> bool {
    path == "/api/tokens"
        || path.starts_with("/api/tokens/")
        || path.starts_with("/api/mcp/tokens")
        || path.starts_with("/api/oauth/")
}

/// A request made with an API token, checked: `None` when it carries none (a session's, then), else who it acts as
/// in the token's workspace, or why it is refused.
pub(crate) async fn access(state: &State, call: &Call, need: Role) -> Option<Result<Access, Answer>> {
    let given = bearer(call)?;
    Some(checked(state, call, given, need).await)
}

async fn checked(state: &State, call: &Call, given: &str, need: Role) -> Result<Access, Answer> {
    let tokens = &state.cicd.tokens;
    if !tokens.failures.may_try(call.peer) {
        return Err(problem(
            StatusCode::TOO_MANY_REQUESTS,
            "too many wrong tokens: wait a minute",
        ));
    }
    let unknown = || refused(StatusCode::UNAUTHORIZED, "this is not a token of this studio");
    if !well_formed(given) {
        tokens.failures.failed(call.peer);
        return Err(unknown());
    }
    let key = hash(given);
    tokens.fresh().await;
    let mut found = tokens.table.get(&key).await;
    if found.is_none() {
        // Made on another node a moment ago, perhaps.
        let _ = tokens.table.refresh().await;
        found = tokens.table.get(&key).await;
    }
    let Some(token) = found else {
        tokens.failures.failed(call.peer);
        return Err(unknown());
    };
    tokens.failures.succeeded(call.peer);
    if let Err(why) = standing(&token, now()) {
        return Err(refused(StatusCode::UNAUTHORIZED, why));
    }
    if call.workspace.as_deref().is_some_and(|w| w != token.workspace) {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "this token is for another workspace",
        ));
    }
    let Some(workspace) = state
        .model
        .workspaces
        .get(&token.workspace)
        .await
        .filter(|w| w.deleted.is_none())
    else {
        return Err(refused(
            StatusCode::UNAUTHORIZED,
            "this token's workspace is gone",
        ));
    };
    let maker = state
        .model
        .users
        .get(&token.by_email)
        .await
        .filter(|u| u.id == token.by && u.deleted.is_none());
    let maker_role = match &maker {
        Some(m) => state
            .model
            .workspaces_of(m)
            .await
            .into_iter()
            .find(|(w, _)| w.id == token.workspace)
            .map(|(_, role)| role),
        None => None,
    };
    let Some(maker_role) = maker_role else {
        return Err(refused(
            StatusCode::UNAUTHORIZED,
            "the person who made this token is no longer in its workspace",
        ));
    };
    if people_only(&call.path) {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "a token cannot do this: it is done in the studio by a person signed in",
        ));
    }
    if token.workflows.is_some() && !call.path.starts_with("/api/cicd/") {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "this token reaches only some workflows: it makes only the workflow push, pull, diff, rollback and \
             promote requests",
        ));
    }
    let role = token.role.min(maker_role);
    if role < need {
        let what = match need {
            Role::Admin => "an admin's",
            Role::Editor => "an editor's",
            Role::Viewer => "a viewer's",
        };
        return Err(problem(
            StatusCode::FORBIDDEN,
            &format!("this needs {what} role in the workspace: the token has less"),
        ));
    }
    if let Err(wait) = tokens.allow(&token) {
        let mut answer = problem(
            StatusCode::TOO_MANY_REQUESTS,
            &format!(
                "this token makes at most {} requests a minute: wait {wait}s",
                token.per_minute
            ),
        );
        if let Ok(v) = HeaderValue::from_str(&wait.to_string()) {
            answer.headers_mut().insert(hyper::header::RETRY_AFTER, v);
        }
        return Err(answer);
    }
    tracing::info!(
        token = %token.name,
        token_id = %token.id,
        workspace = %token.workspace,
        method = %call.method,
        path = %call.path,
        "a request with an API token"
    );
    Ok(Access {
        user: User {
            schema: SCHEMA,
            id: format!("{TOKEN_USER}{}", token.id),
            email: format!("{TOKEN_USER}{}", token.name),
            name: token.name.clone(),
            password: String::new(),
            generation: 0,
            admin: false,
            created: token.created,
            updated: token.created,
            deleted: None,
        },
        workspace,
        role,
    })
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "tokens"]) => list(state, call).await,
        (&Method::POST, ["api", "tokens"]) => issue(state, call).await,
        (&Method::DELETE, ["api", "tokens", id]) => revoke(state, call, id).await,
        _ => return None,
    })
}

fn shown(t: &ApiToken) -> Value {
    json!({
        "id": t.id, "name": t.name, "hint": t.hint, "role": t.role, "workflows": t.workflows,
        "per_minute": t.per_minute, "created": t.created, "expires": t.expires, "by": t.by_email,
        "expired": t.expires <= now(),
    })
}

async fn list(state: &State, call: &Call) -> Answer {
    let access = match crate::api::access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let tokens: Vec<Value> = state
        .cicd
        .tokens
        .of(&access.workspace.id, false)
        .await
        .iter()
        .map(shown)
        .collect();
    json(
        StatusCode::OK,
        &json!({ "tokens": tokens, "default_days": DEFAULT_DAYS, "max_days": MAX_DAYS }),
    )
}

/// What a request to make a token asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Asked {
    pub name: String,
    pub role: Role,
    pub days: u64,
    pub workflows: Option<Vec<String>>,
    pub per_minute: u32,
}

/// What a request to make a token asks for, checked: its name, role, days, scopes and requests a minute.
pub(crate) fn asked(body: &Value, maker: Role) -> Result<Asked, String> {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if name.is_empty() || name.chars().count() > 100 {
        return Err(
            "name: what the token is for, as the CI job that uses it (1 to 100 characters)".to_owned(),
        );
    }
    let role: Role = match body.get("role") {
        None | Some(Value::Null) => Role::Editor,
        Some(r) => {
            serde_json::from_value(r.clone()).map_err(|_| "role: viewer, editor or admin".to_owned())?
        }
    };
    if role > maker {
        return Err("role: at most your own role in the workspace".to_owned());
    }
    let days = match body.get("days") {
        None | Some(Value::Null) => DEFAULT_DAYS,
        Some(d) => d
            .as_u64()
            .filter(|d| (1..=MAX_DAYS).contains(d))
            .ok_or_else(|| format!("days: how long it lasts, 1 to {MAX_DAYS}"))?,
    };
    let workflows = match body.get("workflows") {
        None | Some(Value::Null) => None,
        Some(Value::Array(names)) => {
            let mut chosen: Vec<String> = Vec::new();
            for n in names {
                let n = n.as_str().map(str::trim).unwrap_or_default();
                if n.is_empty() || n.chars().count() > 101 || n.trim_end_matches('*').contains('*') {
                    return Err(
                        "workflows: workflow names, or beginnings ending in * (as billing-*)".to_owned(),
                    );
                }
                if !chosen.iter().any(|c| c == n) {
                    chosen.push(n.to_owned());
                }
            }
            if chosen.is_empty() || chosen.len() > MAX_SCOPES {
                return Err(format!(
                    "workflows: 1 to {MAX_SCOPES} names, or none given for every workflow"
                ));
            }
            Some(chosen)
        }
        Some(_) => return Err("workflows: a list of workflow names".to_owned()),
    };
    let per_minute = match body.get("per_minute") {
        None | Some(Value::Null) => PER_MINUTE,
        Some(n) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (1..=MAX_PER_MINUTE).contains(n))
            .ok_or_else(|| format!("per_minute: requests a minute, 1 to {MAX_PER_MINUTE}"))?,
    };
    Ok(Asked {
        name,
        role,
        days,
        workflows,
        per_minute,
    })
}

/// A new token: shown this once, kept only as its hash.
async fn issue(state: &State, call: &Call) -> Answer {
    let access = match crate::api::access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Asked {
        name,
        role,
        days,
        workflows,
        per_minute,
    } = match asked(&call.body, access.role) {
        Ok(a) => a,
        Err(why) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &why),
    };
    let tokens = &state.cicd.tokens;
    if tokens.of(&access.workspace.id, false).await.len() >= MAX_TOKENS {
        return problem(
            StatusCode::CONFLICT,
            &format!("a workspace has at most {MAX_TOKENS} tokens: revoke one first"),
        );
    }
    let token = match new_token() {
        Ok(t) => t,
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    let record = ApiToken {
        schema: SCHEMA,
        id: new_id(),
        workspace: access.workspace.id.clone(),
        name,
        hash: hash(&token),
        hint: token.get(token.len() - 4..).unwrap_or_default().to_owned(),
        role,
        workflows,
        per_minute,
        created: now(),
        expires: now().saturating_add(days * 24 * 3600),
        by: access.user.id.clone(),
        by_email: access.user.email.clone(),
        revoked: None,
    };
    if let Err(e) = tokens.table.put(&record.hash, &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    tracing::info!(token = %record.name, workspace = %record.workspace, by = %record.by_email, "an API token made");
    let mut body = shown(&record);
    if let Some(fields) = body.as_object_mut() {
        fields.insert("token".to_owned(), json!(token));
    }
    json(StatusCode::CREATED, &body)
}

async fn revoke(state: &State, call: &Call, id: &str) -> Answer {
    let access = match crate::api::access(state, call, Role::Admin).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(mut record) = state
        .cicd
        .tokens
        .of(&access.workspace.id, false)
        .await
        .into_iter()
        .find(|t| t.id == id)
    else {
        return problem(StatusCode::NOT_FOUND, "no such token");
    };
    record.revoked = Some(now());
    if let Err(e) = state.cicd.tokens.table.put(&record.hash.clone(), &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    tracing::info!(token = %record.name, workspace = %record.workspace, by = %access.user.email, "an API token revoked");
    json(StatusCode::OK, &json!({ "revoked": id }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_token_is_random_well_formed_and_kept_by_its_hash() {
        let (a, b) = (new_token().unwrap(), new_token().unwrap());
        assert_ne!(a, b);
        assert!(a.starts_with("odapi_") && well_formed(&a) && well_formed(&b));
        assert!(!well_formed("odapi_short") && !well_formed(&format!("{a}x")));
        assert!(
            !well_formed(&a.replacen("odapi_", "odmcp_", 1)),
            "an MCP token is not one"
        );
        assert_eq!(hash(&a).len(), 64);
        assert_ne!(hash(&a), hash(&b));
    }

    #[test]
    fn what_a_token_is_made_with_is_checked() {
        assert_eq!(
            asked(&json!({ "name": " CI " }), Role::Admin).unwrap(),
            Asked {
                name: "CI".to_owned(),
                role: Role::Editor,
                days: DEFAULT_DAYS,
                workflows: None,
                per_minute: PER_MINUTE
            }
        );
        let scoped = asked(
            &json!({ "name": "x", "role": "viewer", "days": 365, "workflows": ["Billing", "orders-*", "Billing"] }),
            Role::Editor,
        )
        .unwrap();
        assert_eq!(scoped.role, Role::Viewer);
        assert_eq!(
            scoped.workflows,
            Some(vec!["Billing".to_owned(), "orders-*".to_owned()])
        );
        let refusal = |body: Value| asked(&body, Role::Editor).unwrap_err();
        assert!(refusal(json!({ "name": "x", "role": "admin" })).contains("at most your own"));
        assert!(refusal(json!({ "name": "x", "days": 366 })).starts_with("days"));
        assert!(refusal(json!({ "name": "x", "days": 0 })).starts_with("days"));
        assert!(refusal(json!({ "name": "" })).starts_with("name"));
        assert!(refusal(json!({ "name": "x", "workflows": [] })).starts_with("workflows"));
        assert!(refusal(json!({ "name": "x", "workflows": ["a*b"] })).starts_with("workflows"));
        assert!(refusal(json!({ "name": "x", "per_minute": 0 })).starts_with("per_minute"));
        assert!(refusal(json!({ "name": "x", "role": "owner" })).starts_with("role"));
    }

    #[test]
    fn a_token_makes_so_many_requests_a_minute_each_its_own() {
        let requests = Mutex::default();
        for _ in 0..3 {
            assert!(allow(&requests, "t1", 3).is_ok());
        }
        let wait = allow(&requests, "t1", 3).unwrap_err();
        assert!((1..=60).contains(&wait));
        assert!(
            allow(&requests, "t2", 3).is_ok(),
            "another token has its own minute"
        );
    }

    #[test]
    fn a_token_works_until_it_expires_or_is_revoked() {
        let mut t = ApiToken {
            schema: 1,
            id: "t".into(),
            workspace: "w".into(),
            name: "CI".into(),
            hash: String::new(),
            hint: String::new(),
            role: Role::Editor,
            workflows: None,
            per_minute: 10,
            created: 1000,
            expires: 1000 + DEFAULT_DAYS * 24 * 3600,
            by: "u".into(),
            by_email: "ada@example.com".into(),
            revoked: None,
        };
        assert!(standing(&t, 1001).is_ok());
        assert!(standing(&t, t.expires).unwrap_err().contains("expired"));
        t.revoked = Some(1002);
        assert!(standing(&t, 1003).unwrap_err().contains("revoked"));
    }

    #[test]
    fn some_requests_are_for_people_only() {
        assert!(people_only("/api/tokens") && people_only("/api/tokens/x") && people_only("/api/mcp/tokens"));
        assert!(people_only("/api/oauth/start"));
        assert!(
            !people_only("/api/workflows") && !people_only("/api/cicd/push") && !people_only("/api/tokensx")
        );
    }
}
