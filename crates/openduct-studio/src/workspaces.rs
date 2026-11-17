// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workspaces and their members: an agency keeps one workspace for each client, its workflows, credentials, files
//! and runs apart from every other's. The installation's admin makes and removes workspaces; a workspace's admin
//! adds and removes its members (a new person is given an account, with a first password to change).

use hyper::{Method, StatusCode};
use serde_json::{Value, json};

use crate::State;
use crate::api::{account_problem, changing, new_user, signed_in, text};
use crate::http::{Answer, Call, json, problem};
use crate::model::{Member, Role, SCHEMA, Workspace, member_key, new_id, now};

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "workspaces"]) => list(state, call).await,
        (&Method::POST, ["api", "workspaces"]) => create(state, call).await,
        (&Method::PUT, ["api", "workspaces", id]) => rename(state, call, id).await,
        (&Method::DELETE, ["api", "workspaces", id]) => remove(state, call, id).await,
        (&Method::GET, ["api", "workspaces", id, "members"]) => members(state, call, id).await,
        (_, ["api", "workspaces", id, "alerts", ..]) => return crate::alerts::route(state, call, id).await,
        (&Method::POST, ["api", "workspaces", id, "members"]) => add_member(state, call, id).await,
        (&Method::PUT, ["api", "workspaces", id, "members", user]) => set_role(state, call, id, user).await,
        (&Method::DELETE, ["api", "workspaces", id, "members", user]) => {
            remove_member(state, call, id, user).await
        }
        _ => return None,
    })
}

async fn list(state: &State, call: &Call) -> Answer {
    let (user, _) = match signed_in(state, call).await {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    let _ = state.model.workspaces.refresh().await;
    let _ = state.model.members.refresh().await;
    let workspaces: Vec<Value> = state
        .model
        .workspaces_of(&user)
        .await
        .into_iter()
        .map(|(w, role)| json!({ "id": w.id, "name": w.name, "role": role, "created": w.created }))
        .collect();
    json(StatusCode::OK, &json!({ "workspaces": workspaces }))
}

fn name_problem(name: &str) -> Option<&'static str> {
    (name.trim().is_empty() || name.chars().count() > 100).then_some("name: 1 to 100 characters")
}

async fn create(state: &State, call: &Call) -> Answer {
    let (user, _) = match changing(state, call).await {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    if !user.admin {
        return problem(
            StatusCode::FORBIDDEN,
            "only the installation's admin makes workspaces",
        );
    }
    let name = text(&call.body, "name").trim().to_owned();
    if let Some(why) = name_problem(&name) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, why);
    }
    let workspace = Workspace {
        schema: SCHEMA,
        id: new_id(),
        name,
        created: now(),
        updated: now(),
        deleted: None,
        alerts: Vec::new(),
    };
    if let Err(e) = state.model.workspaces.put(&workspace.id, &workspace).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    json(
        StatusCode::CREATED,
        &json!({ "id": workspace.id, "name": workspace.name, "role": Role::Admin }),
    )
}

/// The signed-in user, as an admin of workspace `id` (the change's token checked); the refusal if not.
async fn admin_of(state: &State, call: &Call, id: &str) -> Result<(crate::model::User, Workspace), Answer> {
    let (user, _) = changing(state, call).await?;
    let found = state
        .model
        .workspaces_of(&user)
        .await
        .into_iter()
        .find(|(w, _)| w.id == id);
    match found {
        Some((w, Role::Admin)) => Ok((user, w)),
        Some(_) => Err(problem(
            StatusCode::FORBIDDEN,
            "this needs an admin's role in the workspace",
        )),
        None => Err(problem(StatusCode::NOT_FOUND, "no such workspace")),
    }
}

async fn rename(state: &State, call: &Call, id: &str) -> Answer {
    let (_, mut workspace) = match admin_of(state, call, id).await {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    let name = text(&call.body, "name").trim().to_owned();
    if let Some(why) = name_problem(&name) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, why);
    }
    workspace.name = name;
    workspace.updated = now();
    match state.model.workspaces.put(id, &workspace).await {
        Ok(()) => json(StatusCode::OK, &json!({ "id": id, "name": workspace.name })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn remove(state: &State, call: &Call, id: &str) -> Answer {
    let (user, mut workspace) = match admin_of(state, call, id).await {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    if !user.admin {
        return problem(
            StatusCode::FORBIDDEN,
            "only the installation's admin removes workspaces",
        );
    }
    let running = state
        .model
        .workflows
        .all()
        .await
        .iter()
        .any(|(_, w)| w.workspace == id && w.active && w.deleted.is_none());
    if running {
        return problem(StatusCode::CONFLICT, "turn the workspace's workflows off first");
    }
    workspace.deleted = Some(now());
    match state.model.workspaces.put(id, &workspace).await {
        Ok(()) => json(StatusCode::OK, &json!({ "removed": id })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn members(state: &State, call: &Call, id: &str) -> Answer {
    let (user, _) = match signed_in(state, call).await {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    if !state
        .model
        .workspaces_of(&user)
        .await
        .iter()
        .any(|(w, _)| w.id == id)
    {
        return problem(StatusCode::NOT_FOUND, "no such workspace");
    }
    let users = state.model.users.all().await;
    let out: Vec<Value> = state
        .model
        .members
        .all()
        .await
        .into_iter()
        .filter(|(_, m)| m.workspace == id && m.deleted.is_none())
        .filter_map(|(_, m)| {
            users
                .iter()
                .find(|(_, u)| u.id == m.user && u.deleted.is_none())
                .map(|(_, u)| json!({ "user": u.id, "email": u.email, "name": u.name, "role": m.role }))
        })
        .collect();
    json(StatusCode::OK, &json!({ "members": out }))
}

fn role_of(body: &Value) -> Result<Role, Answer> {
    serde_json::from_value(body.get("role").cloned().unwrap_or(json!("editor")))
        .map_err(|_| problem(StatusCode::UNPROCESSABLE_ENTITY, "role: admin, editor or viewer"))
}

async fn add_member(state: &State, call: &Call, id: &str) -> Answer {
    let (admin, _) = match admin_of(state, call, id).await {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    let role = match role_of(&call.body) {
        Ok(r) => r,
        Err(answer) => return answer,
    };
    let email = text(&call.body, "email").trim().to_ascii_lowercase();
    let _ = state.model.users.refresh().await;
    let user = match state
        .model
        .users
        .get(&email)
        .await
        .filter(|u| u.deleted.is_none())
    {
        Some(u) => u,
        None => {
            // Someone new: an account, with the first password the admin gives them to change.
            let name = text(&call.body, "name").trim().to_owned();
            let password = text(&call.body, "password").to_owned();
            if let Some(why) = account_problem(&email, &name, &password) {
                return problem(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("a new person needs {why}"),
                );
            }
            let user = match new_user(&email, &name, &password, false).await {
                Ok(u) => u,
                Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
            };
            if let Err(e) = state.model.users.put(&email, &user).await {
                return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
            }
            user
        }
    };
    let member = Member {
        schema: SCHEMA,
        workspace: id.to_owned(),
        user: user.id.clone(),
        role,
        added: now(),
        by: admin.id,
        deleted: None,
    };
    match state.model.members.put(&member_key(id, &user.id), &member).await {
        Ok(()) => json(
            StatusCode::CREATED,
            &json!({ "user": user.id, "email": user.email, "name": user.name, "role": role }),
        ),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn set_role(state: &State, call: &Call, id: &str, user: &str) -> Answer {
    if let Err(answer) = admin_of(state, call, id).await {
        return answer;
    }
    let role = match role_of(&call.body) {
        Ok(r) => r,
        Err(answer) => return answer,
    };
    let key = member_key(id, user);
    let Some(mut member) = state
        .model
        .members
        .get(&key)
        .await
        .filter(|m| m.deleted.is_none())
    else {
        return problem(StatusCode::NOT_FOUND, "not a member of the workspace");
    };
    member.role = role;
    match state.model.members.put(&key, &member).await {
        Ok(()) => json(StatusCode::OK, &json!({ "user": user, "role": role })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn remove_member(state: &State, call: &Call, id: &str, user: &str) -> Answer {
    let (admin, _) = match admin_of(state, call, id).await {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    if admin.id == user {
        return problem(
            StatusCode::CONFLICT,
            "an admin cannot remove themselves: ask another admin",
        );
    }
    let key = member_key(id, user);
    let Some(mut member) = state
        .model
        .members
        .get(&key)
        .await
        .filter(|m| m.deleted.is_none())
    else {
        return problem(StatusCode::NOT_FOUND, "not a member of the workspace");
    };
    member.deleted = Some(now());
    match state.model.members.put(&key, &member).await {
        Ok(()) => json(StatusCode::OK, &json!({ "removed": user })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}
