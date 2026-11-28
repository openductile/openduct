// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Apps defined by files: `apps/<id>/app.yaml` - data, not code - run by one engine. A workflow step names the app
//! (`stripe: {action: create_customer, credential: shop, email: ...}`); the engine builds the request from the app's
//! definition, the credential and the step's inputs, sends it through the workflow's effects (so the private-network
//! rule and previews hold as for any HTTP step), and reads the answer. The apps OpenDuct comes with are built in; a
//! folder of one's own adds more ([`add_dir`]).

pub mod custom; // app generator: the apps a workspace adds itself
mod definition;
mod engine;
mod examples; // test runs
pub(crate) mod follow_ups; // messaging
pub mod generate; // app generator: apps made from an API description or an MCP server
pub mod hook; // G3 app triggers
pub(crate) mod mcp_action; // app generator
pub mod oauth;
pub mod poll; // G3 app triggers
pub mod quality; // app generator
pub mod recorded;
mod recorded_triggers; // G3 app triggers
#[cfg(test)]
mod trigger_tests; // G3 app triggers
pub mod triggers; // G3 app triggers

use std::sync::RwLock;

pub use definition::{Action, AppDef, BodyKind, Field, Grant, Input, OAuth2, SignIn, TOKEN_FIELDS};
pub use engine::{Answer, answer, request};
pub use examples::example; // test runs

mod built_in {
    include!(concat!(env!("OUT_DIR"), "/apps.rs"));
}

/// Step kinds that are not apps: an app may not take their names.
const STEPS: &[&str] = &[
    "set",
    "where",
    "if",
    "for_each",
    "http",
    "lookup",
    "postgres",
    "javascript",
    "python",
    "email",
    "switch",
    "merge",
    "list",
    "group",
    "respond",
    "run_workflow",
    "datetime",
    "topic",
    "wait",
    "stop",
    "ai",
    "embed",
    // G7 more AI steps
    "classify",
    "extract",
    "summarise",
    "sentiment",
    "split_text",    // retrieval
    "vector_store",  // retrieval
    "ask_documents", // retrieval
    // The other steps added beside them: no app may take a step's name.
    "agent",
    "mcp",
    "form",
    "approval",
    "table",
    "state",
    "extract_file",
    "convert",
    "file_details",
    "mysql",     // sql databases
    "sqlserver", // sql databases
];
/// Credential kinds that are not apps': no app's sign-in may take their names.
const CREDENTIALS: &[&str] = &[
    "bearer",
    "header",
    "basic",
    "ai",
    "postgres",
    "smtp",
    "test",
    "mysql",     // sql databases
    "sqlserver", // sql databases
    // storage places
    "azure_blob",
    "gcs",
    "box_token",
];

fn apps() -> &'static RwLock<Vec<&'static AppDef>> {
    static APPS: RwLock<Vec<&'static AppDef>> = RwLock::new(Vec::new());
    static LOADED: std::sync::Once = std::sync::Once::new();
    LOADED.call_once(|| {
        let mut loaded: Vec<&'static AppDef> = Vec::new();
        for (source, text) in built_in::BUILT_IN {
            match AppDef::parse(text, source).and_then(|a| fits(&a, &loaded).map(|()| a)) {
                Ok(app) => loaded.push(Box::leak(Box::new(app))),
                // A built-in app is checked by its own tests: one that does not read is left out, and said.
                Err(e) => tracing::error!(error = e, "a built-in app cannot be read"),
            }
        }
        if let Ok(mut all) = APPS.write() {
            *all = loaded;
        }
    });
    &APPS
}

/// Why `app` cannot join `others`: its name, or a sign-in's, is taken.
fn fits(app: &AppDef, others: &[&AppDef]) -> Result<(), String> {
    if STEPS.contains(&app.id.as_str()) {
        return Err(format!("{}: {} is a step of OpenDuct's own", app.source, app.id));
    }
    for s in &app.sign_ins {
        if CREDENTIALS.contains(&s.id.as_str()) {
            return Err(format!(
                "{}: sign-in {} is a credential kind of OpenDuct's own",
                app.source, s.id
            ));
        }
    }
    for other in others.iter().filter(|o| o.id != app.id) {
        if let Some(s) = app.sign_ins.iter().find(|s| other.sign_in(&s.id).is_some()) {
            return Err(format!(
                "{}: sign-in {} is {}'s already",
                app.source, s.id, other.id
            ));
        }
    }
    Ok(())
}

/// Every app: built in, and added.
pub fn all() -> Vec<&'static AppDef> {
    apps().read().map(|a| a.clone()).unwrap_or_default()
}

/// The app called `id`: one OpenDuct carries or the installation adds, else (while a workflow of a workspace is
/// read, see [`custom::within`]) one of that workspace's own.
pub fn find(id: &str) -> Option<&'static AppDef> {
    shipped(id).or_else(|| custom::scoped(id)) // app generator: a workspace's custom apps
}

/// The app called `id` that OpenDuct carries or the installation adds. // app generator
fn shipped(id: &str) -> Option<&'static AppDef> {
    apps().read().ok()?.iter().copied().find(|a| a.id == id)
}

/// The app a credential kind (a sign-in's id) belongs to, and that sign-in.
pub fn by_credential(kind: &str) -> Option<(&'static AppDef, &'static SignIn)> {
    let all = apps().read().ok()?;
    all.iter()
        .copied()
        .find_map(|a| a.sign_in(kind).map(|s| (a, s)))
        .or_else(|| custom::by_kind(kind)) // app generator: a custom app's sign-in
}

/// Adds an app read from `text` (`source` is where it came from); one of the same id replaces it. Refused when its
/// name or a sign-in's is taken by something else.
pub fn add(text: &str, source: &str) -> Result<&'static AppDef, String> {
    let app = AppDef::parse(text, source)?;
    let mut all = apps()
        .write()
        .map_err(|_| "the apps cannot be changed now".to_owned())?;
    fits(&app, &all)?;
    let app: &'static AppDef = Box::leak(Box::new(app));
    all.retain(|o| o.id != app.id);
    all.push(app);
    Ok(app)
}

/// Adds every `<dir>/<id>/app.yaml`: the apps of one's own (`--apps-dir`). What cannot be read is said, with where.
pub fn add_dir(dir: &std::path::Path) -> Result<Vec<&'static AppDef>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    let mut added = Vec::new();
    let mut files: Vec<std::path::PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path().join("app.yaml"))
        .filter(|p| p.is_file())
        .collect();
    files.sort();
    for file in files {
        let text =
            std::fs::read_to_string(&file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
        let app = add(&text, &file.display().to_string())?;
        // test runs: its recorded exchanges, whose results a test run answers with.
        if let Some(dir) = file.parent() {
            examples::add_dir(&app.id, &dir.join("tests"));
        }
        added.push(app);
    }
    Ok(added)
}

/// An app's sign-in as a kind of credential: its label, its fields (`?` after an optional one), and which are
/// secret. Made once for each, and kept.
pub fn credential_kind(kind: &str) -> Option<&'static crate::apps::CredentialKind> {
    static MADE: RwLock<Vec<&'static crate::apps::CredentialKind>> = RwLock::new(Vec::new());
    let (app, sign_in) = by_credential(kind)?;
    // app generator: a custom app's sign-in may change its fields, so one made before is taken only while they hold.
    let same = |k: &crate::apps::CredentialKind| {
        k.kind == kind
            && k.fields.len() == sign_in.fields.len()
            && k.fields
                .iter()
                .zip(&sign_in.fields)
                .all(|(a, f)| a.trim_end_matches('?') == f.name && a.ends_with('?') == f.optional)
            && k.secrets.len() == sign_in.fields.iter().filter(|f| f.secret).count()
    };
    if let Some(found) = MADE.read().ok()?.iter().copied().find(|k| same(k)) {
        return Some(found);
    }
    let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
    let fields: Vec<&'static str> = sign_in
        .fields
        .iter()
        .map(|f| {
            leak(if f.optional {
                format!("{}?", f.name)
            } else {
                f.name.clone()
            })
        })
        .collect();
    let secrets: Vec<&'static str> = sign_in
        .fields
        .iter()
        .filter(|f| f.secret)
        .map(|f| leak(f.name.clone()))
        .collect();
    let made: &'static crate::apps::CredentialKind = Box::leak(Box::new(crate::apps::CredentialKind {
        kind: leak(sign_in.id.clone()),
        label: leak(format!("{}: {}", app.name, sign_in.label)),
        fields: Box::leak(fields.into_boxed_slice()),
        secrets: Box::leak(secrets.into_boxed_slice()),
    }));
    let mut all = MADE.write().ok()?;
    if let Some(found) = all.iter().copied().find(|k| same(k)) {
        return Some(found);
    }
    all.push(made);
    Some(made)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;
    use crate::apps::Credential;
    use openduct_expr::Value;
    use serde_json::json;

    const SHOP: &str = r#"
app: shop_test
name: Shop
hosts: [api.shop.example, "*.shop.example"]
base_url: https://api.shop.example/v1
sign_in:
  - id: shop_test_key
    label: An API key
    fields:
      - { name: key, label: The key, secret: true }
    headers: { Authorization: "Bearer {{ credential.key }}" }
actions:
  add_customer:
    label: Add a customer
    write: true
    method: POST
    path: /customers
    body: form
    inputs:
      - { name: email, label: Email, required: true }
      - { name: note, label: A note }
  orders:
    label: Orders
    method: GET
    path: /orders
    query: { limit: "2" }
    items: value.data
    next: "value.more ? {'after': value.data[1].id} : null"
error: value.error.message
"#;

    fn key() -> Credential {
        Credential {
            kind: "shop_test_key".to_owned(),
            fields: [("key".to_owned(), "k1".to_owned())].into_iter().collect(),
        }
    }

    #[test]
    fn the_example_apps_read_and_their_recorded_exchanges_hold() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/apps");
        let mut ran = 0;
        for app in add_dir(&dir).unwrap() {
            for entry in std::fs::read_dir(dir.join(&app.id).join("tests"))
                .unwrap()
                .flatten()
            {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "yaml") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    recorded::check(app, &text, &path.display().to_string()).unwrap();
                    ran += 1;
                }
            }
        }
        assert!(
            ran >= 3,
            "only {ran} recorded exchanges of the example apps found"
        );
    }

    #[test]
    fn every_app_s_recorded_exchanges_hold() {
        let apps = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../apps");
        let mut ran = 0;
        for app in all() {
            let tests = apps.join(&app.id).join("tests");
            let Ok(entries) = std::fs::read_dir(&tests) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "yaml") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    recorded::check(app, &text, &path.display().to_string()).unwrap();
                    ran += 1;
                }
            }
        }
        assert!(ran >= 9, "only {ran} recorded exchanges found");
    }

    #[test]
    fn every_built_in_app_reads() {
        let ids: Vec<&str> = all().iter().map(|a| a.id.as_str()).collect();
        for id in ["telegram", "slack", "discord", "teams", "google_chat", "whatsapp"] {
            assert!(ids.contains(&id), "{id} missing from {ids:?}");
        }
        assert!(by_credential("slack_webhook").is_some());
        assert_eq!(
            crate::apps::credential_kind("whatsapp").unwrap().secrets,
            ["access_token", "app_secret"] // G3 app triggers: the app's secret signs what Meta sends
        );
    }

    #[test]
    fn a_link_header_gives_its_addresses_by_rel() {
        let both = r#"<https://s.myshopify.com/admin/api/2025-01/orders.json?limit=2&page_info=a>; rel="previous", <https://s.myshopify.com/admin/api/2025-01/orders.json?limit=2&page_info=b>; rel="next""#;
        assert_eq!(
            engine::links_of(Some(both)),
            json!({
                "previous": "https://s.myshopify.com/admin/api/2025-01/orders.json?limit=2&page_info=a",
                "next": "https://s.myshopify.com/admin/api/2025-01/orders.json?limit=2&page_info=b",
            })
        );
        assert_eq!(
            engine::links_of(Some("<https://x/1>; REL=next last")),
            json!({ "next": "https://x/1", "last": "https://x/1" })
        );
        assert_eq!(engine::links_of(None), json!({}));
    }

    #[test]
    fn an_action_s_request_and_answer_follow_its_definition() {
        let app = AppDef::parse(SHOP, "shop.yaml").unwrap();
        let (_, add) = app.action(Some("add_customer")).unwrap();
        let inputs = Value::from_json(&json!({ "email": "a@b.example" }));
        let r = request(&app, add, &key(), &inputs, &Value::Null).unwrap();
        assert_eq!(
            (r.method.as_str(), r.url.as_str(), r.read),
            ("POST", "https://api.shop.example/v1/customers", false)
        );
        assert!(
            r.headers
                .contains(&("authorization".to_owned(), "Bearer k1".to_owned()))
        );
        let (kind, body) = r.body.unwrap();
        assert_eq!(
            (kind.as_str(), String::from_utf8(body).unwrap().as_str()),
            ("application/x-www-form-urlencoded", "email=a%40b.example")
        );

        // A list: page after page, each page's records, and where the next one is.
        let (_, orders) = app.action(Some("orders")).unwrap();
        let first = request(&app, orders, &key(), &Value::Null, &Value::Null).unwrap();
        assert_eq!(first.query, [("limit".to_owned(), "2".to_owned())]);
        let page = Value::from_json(
            &json!({ "status": 200, "body": { "data": [{ "id": "o1" }, { "id": "o2" }], "more": true } }),
        );
        let got = answer(&app, orders, &key(), &page, &Value::Null).unwrap();
        assert_eq!(got.items.len(), 2);
        let second = request(&app, orders, &key(), &Value::Null, &got.next).unwrap();
        assert!(
            second.query.contains(&("after".to_owned(), "o2".to_owned())),
            "{:?}",
            second.query
        );

        // A refusal, in the app's own words.
        let refused =
            Value::from_json(&json!({ "status": 402, "body": { "error": { "message": "card declined" } } }));
        assert_eq!(
            answer(&app, add, &key(), &refused, &Value::Null).unwrap_err(),
            "Shop answered 402: card declined"
        );
        // An input it needs that came empty is said, not sent as nothing.
        for empty in [json!({ "email": "" }), json!({ "email": null }), json!({})] {
            let e = request(&app, add, &key(), &Value::from_json(&empty), &Value::Null).unwrap_err();
            assert!(e.contains("needs email") && e.contains("came empty"), "{e}");
        }
        // A credential of another app is refused.
        let other = Credential {
            kind: "slack".to_owned(),
            ..key()
        };
        assert!(
            request(&app, add, &other, &inputs, &Value::Null)
                .unwrap_err()
                .contains("takes shop_test_key")
        );
    }

    #[test]
    fn a_definition_s_mistakes_are_found_with_where_they_are() {
        let wrong = |from: &str, to: &str| AppDef::parse(&SHOP.replace(from, to), "shop.yaml").unwrap_err();
        assert!(
            wrong(
                "base_url: https://api.shop.example/v1",
                "base_url: https://elsewhere.example"
            )
            .contains("is not among its hosts")
        );
        assert!(wrong("base_url: https://api", "base_url: http://api").contains("https://"));
        assert!(wrong("method: GET", "method: FETCH").contains("actions.orders.method"));
        assert!(wrong("items: value.data", "items: value.data +").contains("actions.orders.items"));
        assert!(
            wrong("label: Orders\n", "label: Orders\n    colour: red\n").contains("unknown field `colour`")
        );
        assert!(wrong("app: shop_test", "app: Shop Test").contains("lowercase"));
        // A next page's address that leaves the app's hosts is refused.
        let app = AppDef::parse(SHOP, "shop.yaml").unwrap();
        let (_, orders) = app.action(Some("orders")).unwrap();
        let away = Value::text("https://evil.example/orders");
        assert!(
            request(&app, orders, &key(), &Value::Null, &away)
                .unwrap_err()
                .contains("not one of the app's hosts")
        );
        let sub = Value::text("https://eu.shop.example/orders?page=2");
        assert!(request(&app, orders, &key(), &Value::Null, &sub).is_ok());
    }

    // more apps (G1): an action may wait longer for a service that answers only when its work is done.
    #[test]
    fn an_action_s_request_waits_as_long_as_it_says_and_no_longer_than_five_minutes() {
        let app = AppDef::parse(SHOP, "shop.yaml").unwrap();
        let (_, orders) = app.action(Some("orders")).unwrap();
        let r = request(&app, orders, &key(), &Value::Null, &Value::Null).unwrap();
        assert_eq!(r.timeout, std::time::Duration::from_secs(30));
        let slow = AppDef::parse(
            &SHOP.replace("label: Orders\n", "label: Orders\n    timeout: 300\n"),
            "shop.yaml",
        )
        .unwrap();
        let (_, orders) = slow.action(Some("orders")).unwrap();
        let r = request(&slow, orders, &key(), &Value::Null, &Value::Null).unwrap();
        assert_eq!(r.timeout, std::time::Duration::from_secs(300));
        for wrong in ["0", "301", "-1", "soon"] {
            let e = AppDef::parse(
                &SHOP.replace(
                    "label: Orders\n",
                    &format!("label: Orders\n    timeout: {wrong}\n"),
                ),
                "shop.yaml",
            )
            .unwrap_err();
            assert!(e.contains("actions.orders"), "{wrong}: {e}");
        }
    }
}
