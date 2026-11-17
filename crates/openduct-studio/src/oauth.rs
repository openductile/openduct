// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Connecting an app whose sign-in is OAuth 2.0 (a person signs in at the vendor):
//!
//! - **Registrations** (an install admin, under Settings): the install's own client id and secret with each vendor -
//!   each install registers itself; OpenDuct takes no part. The address to give the vendor is shown:
//!   `<this studio>/oauth/callback`.
//! - **Start** (an editor of a workspace): the vendor's sign-in address, with a state sealed so only this studio can
//!   read it - who asked, for which workspace and credential, the PKCE verifier, and when (ten minutes).
//! - **Callback**: the same person back from the vendor; the code traded for tokens (through the same guarded web
//!   client as a workflow's), kept as the workspace's credential; then back to Credentials.

use std::collections::BTreeMap;
use std::sync::Arc;

use hyper::{Method, StatusCode, header};
use openduct_flow::Effects as _;
use openduct_flow::app_files::{self, Grant, oauth};
use serde_json::{Value, json};

use crate::State;
use crate::api::{access, changing, signed_in};
use crate::credentials::{INSTALL, keep, registration};
use crate::http::{Answer, Call, json, problem};
use crate::model::{CredentialRecord, PRODUCTION, Role, SCHEMA, credential_key, now};

/// How long a sign-in at the vendor may take.
const STATE_LASTS: u64 = 600;
/// What a state is sealed for: it opens as nothing else.
const BOUND: &str = "oauth-state";

/// Apps a person signs in to at the vendor: their id, name, and that sign-in's id.
fn connectable() -> Vec<(&'static app_files::AppDef, &'static app_files::SignIn)> {
    app_files::all()
        .into_iter()
        .flat_map(|a| {
            a.sign_ins
                .iter()
                .filter(|s| {
                    s.oauth2
                        .as_ref()
                        .is_some_and(|o| o.grant == Grant::AuthorizationCode)
                })
                .map(move |s| (a, s))
        })
        .collect()
}

/// Where the vendor sends a person back: this studio, as the person reached it.
fn callback(state: &State, call: &Call) -> String {
    let scheme = if state.secure_cookies { "https" } else { "http" };
    format!("{scheme}://{}/oauth/callback", call.host)
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "oauth", "apps"]) => list(state, call).await,
        (&Method::PUT, ["api", "oauth", "apps", app]) => register(state, call, app).await,
        (&Method::DELETE, ["api", "oauth", "apps", app]) => unregister(state, call, app).await,
        (&Method::POST, ["api", "oauth", "start"]) => start(state, call).await,
        (&Method::GET, ["oauth", "callback"]) => finish(state, call).await,
        _ => return None,
    })
}

async fn admin(state: &State, call: &Call) -> Result<(), Answer> {
    let user = if call.method == Method::GET {
        signed_in(state, call).await?.0
    } else {
        changing(state, call).await?.0
    };
    if !user.admin {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "only the install's admin manages its registrations with vendors",
        ));
    }
    Ok(())
}

async fn list(state: &State, call: &Call) -> Answer {
    if let Err(answer) = admin(state, call).await {
        return answer;
    }
    // One row for each app: its registration is the app's, whichever of its sign-ins (QuickBooks' live and sandbox
    // companies) a workspace connects with.
    let mut apps: Vec<Value> = Vec::new();
    for (a, s) in connectable() {
        if let Some(row) = apps.iter_mut().find(|r| r["app"] == a.id.as_str()) {
            if let Some(ids) = row["sign_ins"].as_array_mut() {
                ids.push(json!(s.id));
            }
            continue;
        }
        let reg = registration(&state.vault, &a.id);
        apps.push(json!({
            "app": a.id, "name": a.name, "sign_in": s.id, "sign_ins": [s.id], "docs": a.docs,
            "registered": reg.is_some(),
            "client_id": reg.map(|r| r.field("client_id")),
        }));
    }
    json(
        StatusCode::OK,
        &json!({ "apps": apps, "callback": callback(state, call) }),
    )
}

async fn register(state: &State, call: &Call, app: &str) -> Answer {
    if let Err(answer) = admin(state, call).await {
        return answer;
    }
    if !connectable().iter().any(|(a, _)| a.id == app) {
        return problem(StatusCode::NOT_FOUND, "no app of that id signs in at its vendor");
    }
    let given = |k: &str| {
        call.body
            .get(k)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    let before = registration(&state.vault, app);
    let Some(client_id) = given("client_id") else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "client_id: the id the vendor gave this install",
        );
    };
    let Some(client_secret) = given("client_secret").or_else(|| before.map(|b| b.field("client_secret")))
    else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "client_secret: the secret the vendor gave this install",
        );
    };
    let fields = BTreeMap::from([
        ("client_id".to_owned(), client_id),
        ("client_secret".to_owned(), client_secret),
    ]);
    let mut record = CredentialRecord {
        schema: SCHEMA,
        workspace: INSTALL.to_owned(),
        environment: PRODUCTION.to_owned(),
        name: app.to_owned(),
        kind: "oauth_registration".to_owned(),
        fields: Vec::new(),
        sealed: String::new(),
        updated: now(),
        by: String::new(),
        deleted: None,
    };
    match keep(state, &mut record, &fields, StatusCode::OK).await.status() {
        StatusCode::OK => json(StatusCode::OK, &json!({ "app": app, "registered": true })),
        status => problem(status, "the registration could not be kept"),
    }
}

async fn unregister(state: &State, call: &Call, app: &str) -> Answer {
    if let Err(answer) = admin(state, call).await {
        return answer;
    }
    let key = credential_key(INSTALL, PRODUCTION, app);
    if let Some(mut record) = state.model.credentials.get(&key).await {
        record.deleted = Some(now());
        record.updated = now();
        if let Err(e) = state.model.credentials.put(&key, &record).await {
            return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
        }
        state.vault.refresh(&state.model).await;
    }
    json(StatusCode::OK, &json!({ "app": app, "registered": false }))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

async fn start(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let text = |k: &str| {
        call.body
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned()
    };
    let (kind, name) = (text("sign_in"), text("name"));
    if !valid_name(&name) {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "name: letters, digits, '_' and '-', at most 64",
        );
    }
    let Some((app, sign_in)) = connectable().into_iter().find(|(_, s)| s.id == kind) else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "sign_in: not one where a person signs in at the vendor",
        );
    };
    let Some(reg) = registration(&state.vault, &app.id) else {
        return problem(
            StatusCode::CONFLICT,
            &format!(
                "this install is not registered with {} yet: the admin adds its client id and secret under Settings",
                app.name
            ),
        );
    };
    let (verifier, challenge) = match oauth::pkce() {
        Ok(p) => p,
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    let back = callback(state, call);
    let held = BTreeMap::from([
        ("user".to_owned(), access.user.id.clone()),
        ("workspace".to_owned(), access.workspace.id.clone()),
        ("name".to_owned(), name),
        ("sign_in".to_owned(), sign_in.id.clone()),
        ("verifier".to_owned(), verifier),
        ("callback".to_owned(), back.clone()),
        ("at".to_owned(), now().to_string()),
    ]);
    let sealed = match state.vault.seal(BOUND, &held) {
        Ok(s) => s,
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    let Some(o) = &sign_in.oauth2 else {
        return problem(StatusCode::INTERNAL_SERVER_ERROR, "not an OAuth sign-in");
    };
    match oauth::authorize_url(o, &reg.field("client_id"), &back, &sealed, &challenge) {
        Ok(url) => json(StatusCode::OK, &json!({ "url": url })),
        Err(e) => problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

/// Back to the Credentials page, saying how it went.
fn back(outcome: &str, value: &str) -> Answer {
    let mut to = reqwest_free_url(outcome, value);
    if to.len() > 1024 {
        to.truncate(1024);
    }
    let mut response = json(StatusCode::FOUND, &json!({ "location": to }));
    if let Ok(v) = header::HeaderValue::from_str(&to) {
        response.headers_mut().insert(header::LOCATION, v);
    }
    response
}

/// `/credentials?<outcome>=<value>`, the value percent-encoded.
fn reqwest_free_url(outcome: &str, value: &str) -> String {
    let mut enc = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            enc.push(char::from(b));
        } else {
            enc.push_str(&format!("%{b:02X}"));
        }
    }
    format!("/credentials?{outcome}={enc}")
}

fn query(call: &Call) -> BTreeMap<String, String> {
    call.query
        .split('&')
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (decode(k), decode(v)))
        .collect()
}

fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        match b {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = bytes.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok());
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(v) => {
                        out.push(v);
                        i += 2;
                    }
                    None => out.push(b),
                }
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn finish(state: &State, call: &Call) -> Answer {
    let (user, _) = match signed_in(state, call).await {
        Ok(u) => u,
        Err(_) => return back("oauth_error", "sign in to the studio first, then connect again"),
    };
    let q = query(call);
    if let Some(e) = q.get("error") {
        let why = q.get("error_description").unwrap_or(e);
        return back("oauth_error", &format!("the vendor said: {why}"));
    }
    let (Some(code), Some(sealed)) = (q.get("code"), q.get("state")) else {
        return back("oauth_error", "the vendor sent no code back");
    };
    let Some(held) = state.vault.open(BOUND, sealed) else {
        return back("oauth_error", "that sign-in was not started here: connect again");
    };
    let field = |k: &str| held.get(k).cloned().unwrap_or_default();
    let started: u64 = field("at").parse().unwrap_or(0);
    if field("user") != user.id {
        return back("oauth_error", "that sign-in was started by someone else");
    }
    if now() > started + STATE_LASTS {
        return back("oauth_error", "that sign-in took too long: connect again");
    }
    let Some((app, sign_in)) = connectable().into_iter().find(|(_, s)| s.id == field("sign_in")) else {
        return back("oauth_error", "that app is no longer here");
    };
    let (Some(o), Some(reg)) = (&sign_in.oauth2, registration(&state.vault, &app.id)) else {
        return back(
            "oauth_error",
            "this install is no longer registered with the vendor",
        );
    };
    let request = oauth::exchange(
        o,
        &reg.field("client_id"),
        &reg.field("client_secret"),
        code,
        &field("callback"),
        &field("verifier"),
    );
    let live = match openduct_flow::Live::new(&state.broker, state.allow_private_network) {
        Ok(l) => Arc::new(l),
        Err(e) => return back("oauth_error", &e),
    };
    let answer = match live.http(request).await {
        Ok(a) => a,
        Err(e) => return back("oauth_error", &format!("{} could not be reached: {e}", app.name)),
    };
    let empty = openduct_flow::Credential::default();
    let mut tokens = match oauth::tokens(&answer, &empty) {
        Ok(t) => t,
        Err(e) => return back("oauth_error", &e),
    };
    // What the vendor's callback gave beside the code (QuickBooks' company), and what a request once connected says
    // (Xero's organisation): kept with the token.
    for (field, given) in &o.callback_fields {
        if let Some(v) = q.get(given) {
            tokens.insert(field.clone(), v.clone());
        }
    }
    let token = tokens.get("access_token").cloned().unwrap_or_default();
    if let Some(request) = oauth::after_connect_request(o, &token) {
        let answer = match live.http(request).await {
            Ok(a) => a,
            Err(e) => {
                return back(
                    "oauth_error",
                    &format!("{} could not be reached once connected: {e}", app.name),
                );
            }
        };
        match oauth::after_connect_fields(o, &answer) {
            Ok(more) => tokens.extend(more),
            Err(e) => return back("oauth_error", &e),
        }
    }
    let (workspace, name) = (field("workspace"), field("name"));
    let key = credential_key(&workspace, PRODUCTION, &name);
    let _ = state.model.credentials.refresh().await;
    let mut record = match state
        .model
        .credentials
        .get(&key)
        .await
        .filter(|r| r.deleted.is_none())
    {
        // Connected again: the same credential, its new tokens.
        Some(r) if r.kind == sign_in.id => r,
        Some(_) => return back("oauth_error", "a credential of another kind has that name"),
        None => CredentialRecord {
            schema: SCHEMA,
            workspace,
            environment: PRODUCTION.to_owned(),
            name: name.clone(),
            kind: sign_in.id.clone(),
            fields: Vec::new(),
            sealed: String::new(),
            updated: now(),
            by: user.id,
            deleted: None,
        },
    };
    match keep(state, &mut record, &tokens, StatusCode::OK).await.status() {
        StatusCode::OK => back("connected", &name),
        _ => back("oauth_error", "the connection could not be kept"),
    }
}
