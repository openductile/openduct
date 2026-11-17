// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The studio's API: signing in, and who may do what where. The first visit makes the installation's admin and a
//! first workspace; after that every request names the workspace it is for (`x-openduct-workspace`, or the user's
//! first) and passes if the user's role there allows it - a viewer looks, an editor builds and runs, an admin also
//! manages the workspace's members. The installation's admin is an admin of every workspace.

use hyper::header::{self, HeaderValue};
use hyper::{Method, StatusCode};
use serde_json::{Value, json};

use crate::State;
use crate::auth::{MAX_PASSWORD, MIN_PASSWORD, SESSION, hash_password, password_ok};
use crate::http::{Answer, Call, json, problem};
use crate::model::{Member, Role, SCHEMA, User, Workspace, member_key, new_id, now};

/// The session cookie's name.
pub(crate) const COOKIE: &str = "openduct_session";

/// A hash no password matches, checked when an account does not exist, so a wrong address takes as long to refuse
/// as a wrong password and cannot be told from it.
const NO_ACCOUNT: &str =
    "pbkdf2-sha256$600000$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

pub(crate) async fn route(state: &State, call: Call) -> Answer {
    match (&call.method, call.path.as_str()) {
        (&Method::GET, "/health") => json(StatusCode::OK, &json!({ "status": "ok" })),
        (&Method::GET, "/api/status") => status(state).await,
        (&Method::POST, "/api/setup") => setup(state, &call).await,
        (&Method::POST, "/api/sign-in") => sign_in(state, &call).await,
        (&Method::POST, "/api/sign-out") => sign_out(state, &call).await,
        (&Method::POST, "/api/password") => password(state, &call).await,
        (&Method::GET, "/api/templates") => crate::templates::list(state, &call).await,
        (&Method::GET, "/api/apps") => crate::app_catalog::list(state, &call).await,
        (&Method::GET, "/api/runs") => crate::history::all_runs(state, &call).await,
        (&Method::GET, "/api/logs") => crate::logs::list(state, &call).await,
        (&Method::GET, "/api/dashboard") => crate::dashboard::dashboard(state, &call).await,
        (&Method::GET, "/api/overview") => crate::dashboard::overview(state, &call).await,
        (&Method::GET, "/api/session") => match signed_in(state, &call).await {
            Ok((user, cookie)) => json(StatusCode::OK, &session_body(state, &user, &cookie).await),
            Err(answer) => answer,
        },
        _ if let Some(answer) = crate::workspaces::route(state, &call).await => answer,
        _ if let Some(answer) = crate::workflows::route(state, &call).await => answer,
        _ if let Some(answer) = crate::credentials::route(state, &call).await => answer,
        _ if let Some(answer) = crate::oauth::route(state, &call).await => answer,
        _ if let Some(answer) = crate::files::route(state, &call).await => answer,
        _ if let Some(answer) = crate::pipelines::route(state, &call).await => answer,
        (_, path) if path.starts_with("/api/") || path.starts_with("/hooks/") => {
            problem(StatusCode::NOT_FOUND, "no such request")
        }
        (&Method::GET | &Method::HEAD, path) => crate::assets::serve(path),
        _ => problem(StatusCode::METHOD_NOT_ALLOWED, "not a request the studio takes"),
    }
}

async fn status(state: &State) -> Answer {
    if let Err(e) = state.model.users.refresh().await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    json(
        StatusCode::OK,
        &json!({ "setup": state.model.users.is_empty().await, "version": env!("CARGO_PKG_VERSION") }),
    )
}

pub(crate) fn text<'a>(body: &'a Value, name: &str) -> &'a str {
    body.get(name).and_then(Value::as_str).unwrap_or_default()
}

/// What is wrong with the details of a new account, if anything.
pub(crate) fn account_problem(email: &str, name: &str, password: &str) -> Option<String> {
    let at = email.find('@');
    if email.len() > 254
        || at.is_none_or(|i| i == 0 || i + 1 >= email.len())
        || email.contains(char::is_whitespace)
    {
        return Some("an e-mail address, as name@example.com".to_owned());
    }
    if name.trim().is_empty() || name.chars().count() > 100 {
        return Some("a name, of at most 100 characters".to_owned());
    }
    let length = password.chars().count();
    if !(MIN_PASSWORD..=MAX_PASSWORD).contains(&length) {
        return Some(format!(
            "a password of {MIN_PASSWORD} to {MAX_PASSWORD} characters"
        ));
    }
    None
}

/// A new user, its password hashed off the request's thread.
pub(crate) async fn new_user(email: &str, name: &str, password: &str, admin: bool) -> Result<User, String> {
    let password = password.to_owned();
    let hashed = tokio::task::spawn_blocking(move || hash_password(&password))
        .await
        .map_err(|_| "cannot keep the password".to_owned())??;
    Ok(User {
        schema: SCHEMA,
        id: new_id(),
        email: email.to_owned(),
        name: name.to_owned(),
        password: hashed,
        generation: 1,
        admin,
        created: now(),
        updated: now(),
        deleted: None,
    })
}

async fn setup(state: &State, call: &Call) -> Answer {
    // One at a time, so two first visits cannot both make an admin.
    let _one = state.setup.lock().await;
    if let Err(e) = state.model.users.refresh().await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    if !state.model.users.is_empty().await {
        return problem(StatusCode::CONFLICT, "the studio is set up already: sign in");
    }
    let email = text(&call.body, "email").trim().to_ascii_lowercase();
    let name = text(&call.body, "name").trim().to_owned();
    let password = text(&call.body, "password").to_owned();
    if let Some(why) = account_problem(&email, &name, &password) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, &why);
    }
    let user = match new_user(&email, &name, &password, true).await {
        Ok(u) => u,
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    let workspace = Workspace {
        schema: SCHEMA,
        id: new_id(),
        name: "My workspace".to_owned(),
        created: now(),
        updated: now(),
        deleted: None,
        alerts: Vec::new(),
    };
    let member = Member {
        schema: SCHEMA,
        workspace: workspace.id.clone(),
        user: user.id.clone(),
        role: Role::Admin,
        added: now(),
        by: user.id.clone(),
        deleted: None,
    };
    let kept = async {
        state.model.workspaces.put(&workspace.id, &workspace).await?;
        state
            .model
            .members
            .put(&member_key(&workspace.id, &user.id), &member)
            .await?;
        state.model.users.put(&email, &user).await
    };
    if let Err(e) = kept.await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    tracing::info!(
        email,
        "studio set up: the admin's account and a first workspace made"
    );
    with_session(state, StatusCode::CREATED, &user).await
}

async fn sign_in(state: &State, call: &Call) -> Answer {
    if !state.failures.may_try(call.peer) {
        return problem(
            StatusCode::TOO_MANY_REQUESTS,
            "too many failed sign-ins: wait a minute",
        );
    }
    let email = text(&call.body, "email").trim().to_ascii_lowercase();
    let password = text(&call.body, "password").to_owned();
    if password.chars().count() > MAX_PASSWORD {
        return problem(
            StatusCode::UNAUTHORIZED,
            "the e-mail address or the password is wrong",
        );
    }
    let _ = state.model.users.refresh().await;
    let user = state
        .model
        .users
        .get(&email)
        .await
        .filter(|u| u.deleted.is_none());
    let stored = user
        .as_ref()
        .map_or(NO_ACCOUNT, |u| u.password.as_str())
        .to_owned();
    let matches = tokio::task::spawn_blocking(move || password_ok(&stored, &password))
        .await
        .unwrap_or(false);
    match user {
        Some(user) if matches => {
            state.failures.succeeded(call.peer);
            with_session(state, StatusCode::OK, &user).await
        }
        _ => {
            state.failures.failed(call.peer);
            problem(
                StatusCode::UNAUTHORIZED,
                "the e-mail address or the password is wrong",
            )
        }
    }
}

async fn sign_out(state: &State, call: &Call) -> Answer {
    let (mut user, _) = match changing(state, call).await {
        Ok(signed) => signed,
        Err(answer) => return answer,
    };
    // Every session of the user ends: a new generation makes the old cookies worthless.
    user.generation += 1;
    user.updated = now();
    if let Err(e) = state.model.users.put(&user.email.clone(), &user).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    let mut answer = json(StatusCode::OK, &json!({ "signed_out": true }));
    set_cookie(state, &mut answer, "", 0);
    answer
}

/// A new password for the signed-in user, given the present one; every other session ends.
async fn password(state: &State, call: &Call) -> Answer {
    let (mut user, _) = match changing(state, call).await {
        Ok(signed) => signed,
        Err(answer) => return answer,
    };
    let (present, new) = (
        text(&call.body, "password").to_owned(),
        text(&call.body, "new_password").to_owned(),
    );
    let stored = user.password.clone();
    let checks = tokio::task::spawn_blocking(move || password_ok(&stored, &present))
        .await
        .unwrap_or(false);
    if !checks {
        state.failures.failed(call.peer);
        return problem(StatusCode::UNAUTHORIZED, "the present password is wrong");
    }
    if let Some(why) = account_problem(&user.email, &user.name, &new) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, &why);
    }
    match tokio::task::spawn_blocking(move || hash_password(&new)).await {
        Ok(Ok(h)) => user.password = h,
        _ => return problem(StatusCode::INTERNAL_SERVER_ERROR, "cannot keep the password"),
    }
    user.generation += 1;
    user.updated = now();
    if let Err(e) = state.model.users.put(&user.email.clone(), &user).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    with_session(state, StatusCode::OK, &user).await
}

/// The signed-in user, and the session's cookie value; a 401 answer if there is none.
pub(crate) async fn signed_in(state: &State, call: &Call) -> Result<(User, String), Answer> {
    let refused = || problem(StatusCode::UNAUTHORIZED, "sign in first");
    let cookie = call.cookie.clone().ok_or_else(refused)?;
    let (email, generation) = state.keys.check(&cookie).ok_or_else(refused)?;
    let user = state.model.users.get(&email).await.ok_or_else(refused)?;
    if user.generation != generation || user.deleted.is_some() {
        return Err(refused());
    }
    Ok((user, cookie))
}

/// As [`signed_in`], for a change: the session's token must come with it.
pub(crate) async fn changing(state: &State, call: &Call) -> Result<(User, String), Answer> {
    let (user, cookie) = signed_in(state, call).await?;
    match &call.csrf {
        Some(token) if state.keys.csrf_ok(&cookie, token) => Ok((user, cookie)),
        _ => Err(problem(
            StatusCode::FORBIDDEN,
            "a change without the session's token",
        )),
    }
}

/// Who asks, in which workspace, with which role.
#[derive(Debug, Clone)]
pub(crate) struct Access {
    pub user: User,
    pub workspace: Workspace,
    pub role: Role,
}

/// The signed-in user in the workspace the request names, if its role there is at least `need`; a change must carry
/// the session's token.
pub(crate) async fn access(state: &State, call: &Call, need: Role) -> Result<Access, Answer> {
    let change = call.method != Method::GET && call.method != Method::HEAD;
    let (user, _) = if change {
        changing(state, call).await?
    } else {
        signed_in(state, call).await?
    };
    let mine = state.model.workspaces_of(&user).await;
    let found = match &call.workspace {
        Some(id) => mine.into_iter().find(|(w, _)| &w.id == id),
        None => mine.into_iter().next(),
    };
    let Some((workspace, role)) = found else {
        return Err(problem(StatusCode::NOT_FOUND, "no such workspace"));
    };
    if role < need {
        let what = match need {
            Role::Admin => "an admin's",
            Role::Editor => "an editor's",
            Role::Viewer => "a viewer's",
        };
        return Err(problem(
            StatusCode::FORBIDDEN,
            &format!("this needs {what} role in the workspace"),
        ));
    }
    Ok(Access {
        user,
        workspace,
        role,
    })
}

async fn session_body(state: &State, user: &User, cookie: &str) -> Value {
    let workspaces: Vec<Value> = state
        .model
        .workspaces_of(user)
        .await
        .into_iter()
        .map(|(w, role)| json!({ "id": w.id, "name": w.name, "role": role }))
        .collect();
    json!({
        "account": { "id": user.id, "email": user.email, "name": user.name, "admin": user.admin },
        "workspaces": workspaces,
        "csrf": state.keys.csrf(cookie),
    })
}

async fn with_session(state: &State, status: StatusCode, user: &User) -> Answer {
    let cookie = state.keys.session(&user.email, user.generation);
    let mut answer = json(status, &session_body(state, user, &cookie).await);
    set_cookie(state, &mut answer, &cookie, SESSION.as_secs());
    answer
}

fn set_cookie(state: &State, answer: &mut Answer, value: &str, max_age: u64) {
    let secure = if state.secure_cookies { "; Secure" } else { "" };
    let cookie = format!("{COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}{secure}");
    if let Ok(v) = HeaderValue::from_str(&cookie) {
        answer.headers_mut().insert(header::SET_COOKIE, v);
    }
}
