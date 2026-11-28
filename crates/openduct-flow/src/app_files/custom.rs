// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The apps a workspace adds itself - custom apps, made on the studio's "Add an app" page. Each is its workspace's
//! alone: a workflow is read with its own workspace's custom apps in reach ([`within`]) and no other's, so another
//! workspace can neither see one nor name it in a step. Its sign-ins are kept as credential kinds of their own (the
//! sign-in's id and a mark of the workspace and app), so a credential of one is sealed and masked as any app's is,
//! and no two workspaces' kinds are the same.
//!
//! A custom app passes the checks a shipped app passes, and more: it reaches only the hosts it declares (no address
//! from a credential), it signs in with keys, tokens, a user name and password or a client ID and secret (a sign-in
//! at the vendor needs the installation's own registration, so is for the apps the installation adds), it has no
//! triggers yet, at most [`MAX_ACTIONS`] actions, and no name of a step or of an app OpenDuct carries.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, RwLock};

use serde_json::{Value as Json, json};

use super::definition::{AppDef, Grant, SignIn};

/// The most actions a custom app has.
pub const MAX_ACTIONS: usize = 100;
/// The most custom apps a workspace has.
pub const MAX_APPS: usize = 200;
/// The largest definition of one, in bytes.
pub const MAX_TEXT: usize = 512 * 1024;
/// The longest description of one, in characters.
pub const MAX_DESCRIPTION: usize = 1000;
/// The most versions of custom apps one process reads before it asks to be restarted: each is kept for as long as
/// a workflow read with it may run.
const MAX_KEPT: usize = 20_000;

fn custom() -> &'static RwLock<BTreeMap<String, Vec<&'static AppDef>>> {
    static CUSTOM: RwLock<BTreeMap<String, Vec<&'static AppDef>>> = RwLock::new(BTreeMap::new());
    &CUSTOM
}

/// Every version read, by a digest of its workspace and text: read once each.
fn kept() -> &'static Mutex<HashMap<String, &'static AppDef>> {
    static KEPT: std::sync::OnceLock<Mutex<HashMap<String, &'static AppDef>>> = std::sync::OnceLock::new();
    KEPT.get_or_init(|| Mutex::new(HashMap::new()))
}

thread_local! {
    /// The workspace whose custom apps are in reach while a workflow is read.
    static SCOPE: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn digest(parts: &[&str]) -> String {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    for p in parts {
        ctx.update(p.as_bytes());
        ctx.update(&[0]);
    }
    hex(ctx.finish().as_ref())
}

/// The credential kind a custom app's sign-in is kept as: its id (cut to 31 characters) and a mark of the
/// workspace and the app.
pub fn kind_of(workspace: &str, app: &str, sign_in: &str) -> String {
    let mark = digest(&[workspace, app]);
    let id = sign_in.get(..sign_in.len().min(31)).unwrap_or(sign_in);
    format!("{id}_{}", mark.get(..8).unwrap_or_default())
}

/// Runs `read` with `workspace`'s custom apps in reach: a workflow of the workspace is read so, and only so.
pub fn within<T>(workspace: &str, read: impl FnOnce() -> T) -> T {
    struct Restore(Option<String>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let before = self.0.take();
            SCOPE.with(|s| *s.borrow_mut() = before);
        }
    }
    let before = SCOPE.with(|s| s.borrow_mut().replace(workspace.to_owned()));
    let _restore = Restore(before);
    read()
}

/// The custom app called `id` of the workspace in reach, if there is one.
pub(super) fn scoped(id: &str) -> Option<&'static AppDef> {
    let workspace = SCOPE.with(|s| s.borrow().clone())?;
    custom()
        .read()
        .ok()?
        .get(&workspace)?
        .iter()
        .copied()
        .find(|a| a.id == id)
}

/// The custom app whose sign-in is of credential kind `kind`, and that sign-in.
pub(super) fn by_kind(kind: &str) -> Option<(&'static AppDef, &'static SignIn)> {
    let all = custom().read().ok()?;
    all.values()
        .flatten()
        .copied()
        .find_map(|a| a.sign_in(kind).map(|s| (a, s)))
}

/// The workspace a custom credential kind belongs to, if it is one.
pub fn owner_of_kind(kind: &str) -> Option<String> {
    let all = custom().read().ok()?;
    all.iter()
        .find(|(_, apps)| apps.iter().any(|a| a.sign_in(kind).is_some()))
        .map(|(w, _)| w.clone())
}

/// The custom apps of `workspace`.
pub fn of(workspace: &str) -> Vec<&'static AppDef> {
    custom()
        .read()
        .map(|all| all.get(workspace).cloned().unwrap_or_default())
        .unwrap_or_default()
}

/// Reads a custom app's definition as written, checked as a shipped app's is and against a custom app's own
/// rules. `source` is where it is said to be, in messages.
pub fn read(text: &str, source: &str) -> Result<AppDef, String> {
    if text.len() > MAX_TEXT {
        return Err(format!(
            "{source}: a custom app's definition is at most {} KB",
            MAX_TEXT / 1024
        ));
    }
    let json = crate::yaml::to_json(text).map_err(|e| format!("{source}: {e}"))?;
    if json
        .get("triggers")
        .is_some_and(|t| t.as_object().is_some_and(|t| !t.is_empty()))
    {
        return Err(format!(
            "{source}: triggers: a custom app has no triggers yet (an app the installation adds may)"
        ));
    }
    if json.get("base_url_field").is_some() {
        return Err(format!(
            "{source}: base_url_field: a custom app reaches only the hosts it declares, not an address its credential gives"
        ));
    }
    let app = AppDef::parse(text, source)?;
    if super::STEPS.contains(&app.id.as_str()) {
        return Err(format!(
            "{source}: {} is a step of OpenDuct's own: name the app otherwise",
            app.id
        ));
    }
    if super::shipped(&app.id).is_some() {
        return Err(format!(
            "{source}: {} is an app OpenDuct carries: name yours otherwise",
            app.id
        ));
    }
    if app.actions.len() > MAX_ACTIONS {
        return Err(format!(
            "{source}: actions: a custom app has at most {MAX_ACTIONS}"
        ));
    }
    if app.description.chars().count() > MAX_DESCRIPTION {
        return Err(format!(
            "{source}: description: at most {MAX_DESCRIPTION} characters"
        ));
    }
    for s in &app.sign_ins {
        if s.url_field.is_some() {
            return Err(format!(
                "{source}: sign_in {}: a custom app reaches only the hosts it declares, not an address its credential gives",
                s.id
            ));
        }
        if s.oauth2
            .as_ref()
            .is_some_and(|o| o.grant == Grant::AuthorizationCode)
        {
            return Err(format!(
                "{source}: sign_in {}: a sign-in at the vendor needs the installation's own registration, so is for \
                 apps the installation adds (--apps-dir); a custom app signs in with a key, a token, a user name and \
                 password, or a client ID and secret",
                s.id
            ));
        }
    }
    Ok(app)
}

/// The definition as kept for `workspace`: each sign-in's id its credential kind ([`kind_of`]), and every `by:` of
/// an action with it.
fn as_kept(workspace: &str, text: &str, id: &str) -> Result<String, String> {
    let mut json = crate::yaml::to_json(text)?;
    let mut renamed: Vec<(String, String)> = Vec::new();
    if let Some(list) = json.get_mut("sign_in").and_then(Json::as_array_mut) {
        for s in list {
            if let Some(old) = s.get("id").and_then(Json::as_str).map(str::to_owned) {
                let new = kind_of(workspace, id, &old);
                s["id"] = json!(new);
                renamed.push((old, new));
            }
        }
    }
    if let Some(actions) = json.get_mut("actions").and_then(Json::as_object_mut) {
        for a in actions.values_mut() {
            if let Some(by) = a.get_mut("by").and_then(Json::as_object_mut) {
                let moved: Vec<(String, Json)> = std::mem::take(by)
                    .into_iter()
                    .map(|(k, v)| {
                        let k = renamed
                            .iter()
                            .find(|(o, _)| o == &k)
                            .map_or(k, |(_, n)| n.clone());
                        (k, v)
                    })
                    .collect();
                by.extend(moved);
            }
        }
    }
    // JSON is YAML too.
    serde_json::to_string(&json).map_err(|e| e.to_string())
}

/// Sets the custom apps of `workspace` - `(id, definition)` of each - in place of those it had. One that does not
/// read is left out, and said; so are those beyond [`MAX_APPS`].
pub fn set(workspace: &str, apps: &[(String, String)]) -> Vec<String> {
    let mut problems = Vec::new();
    let mut made: Vec<&'static AppDef> = Vec::new();
    for (id, text) in apps {
        if made.len() >= MAX_APPS {
            problems.push(format!("{id}: a workspace has at most {MAX_APPS} custom apps"));
            continue;
        }
        let key = digest(&[workspace, text]);
        let known = kept().lock().ok().and_then(|k| k.get(&key).copied());
        let app = match known {
            Some(a) => a,
            None => match keep(workspace, text, id, key) {
                Ok(a) => a,
                Err(e) => {
                    problems.push(e);
                    continue;
                }
            },
        };
        if made.iter().any(|m| m.id == app.id) {
            problems.push(format!("{id}: the workspace has another app of that name"));
            continue;
        }
        made.push(app);
    }
    if let Ok(mut all) = custom().write() {
        if made.is_empty() {
            all.remove(workspace);
        } else {
            all.insert(workspace.to_owned(), made);
        }
    }
    problems
}

fn keep(workspace: &str, text: &str, id: &str, key: String) -> Result<&'static AppDef, String> {
    let source = format!("{id} (a custom app)");
    let written = read(text, &source)?;
    if written.id != *id {
        return Err(format!("{source}: its definition is of {}", written.id));
    }
    let app = AppDef::parse(&as_kept(workspace, text, id)?, &source)?;
    let mut kept = kept()
        .lock()
        .map_err(|_| "the custom apps cannot be read now".to_owned())?;
    if kept.len() >= MAX_KEPT {
        return Err(format!(
            "{source}: more than {MAX_KEPT} versions of custom apps were read since this node started: restart it"
        ));
    }
    let app: &'static AppDef = Box::leak(Box::new(app));
    kept.insert(key, app);
    Ok(app)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    const CRM: &str = r#"
app: acme_crm_test
name: Acme CRM
description: Contacts of Acme CRM.
hosts: [api.acme-crm.example]
base_url: https://api.acme-crm.example/v1
sign_in:
  - { id: acme_crm_test, label: An API key, fields: [{ name: api_key, label: Key, secret: true }], headers: { X-Key: "{{ credential.api_key }}" } }
actions:
  list_contacts: { label: List contacts, method: GET, path: /contacts, items: value }
"#;

    #[test]
    fn a_custom_app_is_its_workspace_s_alone() {
        assert!(set("ws-a", &[("acme_crm_test".to_owned(), CRM.to_owned())]).is_empty());
        assert!(
            super::super::find("acme_crm_test").is_none(),
            "out of reach without a workspace"
        );
        assert!(within("ws-a", || super::super::find("acme_crm_test")).is_some());
        assert!(within("ws-b", || super::super::find("acme_crm_test")).is_none());
        // Its sign-in is a credential kind of its own, which says whose it is.
        let kind = kind_of("ws-a", "acme_crm_test", "acme_crm_test");
        assert_ne!(kind, kind_of("ws-b", "acme_crm_test", "acme_crm_test"));
        let (app, s) = super::super::by_credential(&kind).unwrap();
        assert_eq!(
            (app.id.as_str(), s.label.as_str()),
            ("acme_crm_test", "An API key")
        );
        assert_eq!(owner_of_kind(&kind).as_deref(), Some("ws-a"));
        assert_eq!(crate::apps::credential_kind(&kind).unwrap().secrets, ["api_key"]);
        // The same name in another workspace is another app.
        let other = CRM.replace("Acme CRM", "Acme CRM of B");
        assert!(set("ws-b", &[("acme_crm_test".to_owned(), other)]).is_empty());
        assert_eq!(
            within("ws-b", || super::super::find("acme_crm_test"))
                .unwrap()
                .name,
            "Acme CRM of B"
        );
        assert_eq!(
            within("ws-a", || super::super::find("acme_crm_test"))
                .unwrap()
                .name,
            "Acme CRM"
        );
        // Taken away.
        assert!(set("ws-b", &[]).is_empty());
        assert!(within("ws-b", || super::super::find("acme_crm_test")).is_none());
    }

    #[test]
    fn a_custom_app_keeps_to_its_own_rules() {
        let wrong = |from: &str, to: &str| read(&CRM.replace(from, to), "crm").unwrap_err();
        assert!(wrong("app: acme_crm_test", "app: slack").contains("OpenDuct carries"));
        assert!(wrong("app: acme_crm_test", "app: http").contains("a step of OpenDuct's own"));
        assert!(
            wrong(
                "actions:",
                "triggers:\n  new: { label: New, poll: { action: list_contacts, id: item.id } }\nactions:"
            )
            .contains("no triggers yet")
        );
        assert!(
            wrong(
                "base_url: https://api.acme-crm.example/v1",
                "base_url: \"\"\nbase_url_field: api_key"
            )
            .contains("hosts it declares")
        );
        let oauth = CRM.replace(
            "{ id: acme_crm_test, label: An API key, fields: [{ name: api_key, label: Key, secret: true }], headers: { X-Key: \"{{ credential.api_key }}\" } }",
            "{ id: acme_crm_test, label: Sign in, oauth2: { authorize: \"https://acme-crm.example/a\", token: \"https://acme-crm.example/t\" } }",
        );
        assert!(
            read(&oauth, "crm")
                .unwrap_err()
                .contains("installation's own registration")
        );
        let many: String = (0..=MAX_ACTIONS)
            .map(|i| format!("  a{i}: {{ label: A, method: GET, path: /a }}\n"))
            .collect();
        assert!(
            read(
                &CRM.replace(
                    "  list_contacts: { label: List contacts, method: GET, path: /contacts, items: value }\n",
                    &many
                ),
                "crm"
            )
            .unwrap_err()
            .contains("at most 100")
        );
        assert!(
            read(&format!("{CRM}#{}", "x".repeat(MAX_TEXT)), "crm")
                .unwrap_err()
                .contains("at most")
        );
        // One that does not read is said, and the others kept.
        let problems = set(
            "ws-c",
            &[
                ("acme_crm_test".to_owned(), CRM.to_owned()),
                ("broken".to_owned(), "app: [".to_owned()),
            ],
        );
        assert_eq!(problems.len(), 1);
        assert_eq!(of("ws-c").len(), 1);
    }
}
