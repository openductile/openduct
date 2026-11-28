// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workflows through CI/CD, end to end: `openduct start`, two workspaces marked as test and production, API tokens
//! made in the studio, and the command line against it - pull, a file edited, diff, push (a dry run, then for real),
//! a wrong file refused, rollback, promotion refused for a missing credential and then made, the production
//! workspace's lock, and what tokens may not do: another workspace, workflows outside their scopes, a maker who lost
//! their role, a revoked token.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};
use studio::{OPENDUCT, call, owner, start, until};

/// The signed-in headers, for another workspace.
fn in_ws(signed: &[String], workspace: &str) -> Vec<String> {
    let mut out = signed.to_vec();
    out.push(format!("x-openduct-workspace: {workspace}"));
    out
}

/// `openduct workflow <args>` against the studio with `token`: its exit code, standard output and error.
fn cli(studio: &str, token: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(OPENDUCT)
        .arg("workflow")
        .args(args)
        .args(["--studio", &format!("http://{studio}")])
        .env("OPENDUCT_TOKEN", token)
        .env_remove("OPENDUCT_TOKEN_FILE")
        .output()
        .unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn token(studio: &str, signed: &[String], body: Value) -> String {
    let made = call(studio, signed, "POST", "/api/tokens", Some(&body.to_string()));
    assert_eq!(made.status, 201, "{}", made.body);
    assert!(made.body.get("hash").is_none(), "never the hash");
    made.body["token"].as_str().unwrap().to_owned()
}

fn hook(name: &str, url: &str) -> Value {
    json!({
        "name": name,
        "trigger": { "webhook": {} },
        "steps": [ { "name": "call", "http": { "credential": "shop", "url": url } } ],
    })
}

fn versions(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let v = call(
        studio,
        signed,
        "GET",
        &format!("/api/workflows/{id}/versions"),
        None,
    );
    assert_eq!(v.status, 200, "{}", v.body);
    v.body["versions"].as_array().unwrap().clone()
}

fn workflow_named(studio: &str, signed: &[String], name: &str) -> Value {
    let all = call(studio, signed, "GET", "/api/workflows", None);
    all.body["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == name)
        .unwrap_or_else(|| panic!("no workflow {name}: {}", all.body))
        .clone()
}

fn credential(studio: &str, signed: &[String], name: &str) {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": name, "kind": "bearer", "fields": { "token": "t-SECRET" } }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
}

fn read(dir: &Path, file: &str) -> String {
    std::fs::read_to_string(dir.join(file)).unwrap()
}

#[test]
fn workflows_are_pulled_diffed_pushed_rolled_back_and_promoted_from_the_command_line() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let session = call(&studio, &signed, "GET", "/api/session", None);
    let test_ws = session.body["workspaces"][0]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &studio,
            &signed,
            "PUT",
            &format!("/api/workspaces/{test_ws}"),
            Some(r#"{"name": "Test"}"#)
        )
        .status,
        200
    );
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/workspaces",
        Some(r#"{"name": "Production"}"#),
    );
    let prod_ws = made.body["id"].as_str().unwrap().to_owned();
    let (test, prod) = (in_ws(&signed, &test_ws), in_ws(&signed, &prod_ws));

    // The two workspaces marked as environments, production locked to pushes and promotions.
    let marked = call(
        &studio,
        &test,
        "PUT",
        "/api/cicd/settings",
        Some(r#"{"environment": "test", "order": 1}"#),
    );
    assert_eq!(marked.status, 200, "{}", marked.body);
    let marked = call(
        &studio,
        &prod,
        "PUT",
        "/api/cicd/settings",
        Some(r#"{"environment": "production", "order": 2, "locked": true}"#),
    );
    assert_eq!(
        (marked.status, marked.body["locked"].as_bool()),
        (200, Some(true)),
        "{}",
        marked.body
    );
    assert_eq!(
        call(
            &studio,
            &test,
            "PUT",
            "/api/cicd/settings",
            Some(r#"{"environment": "staging"}"#)
        )
        .status,
        422
    );

    // Workflows built in the studio, in Test.
    credential(&studio, &test, "shop");
    for (name, url) in [
        ("Order alerts", "https://shop.example.com/a"),
        ("Stock check", "https://shop.example.com/s"),
    ] {
        let made = call(
            &studio,
            &test,
            "POST",
            "/api/workflows",
            Some(&json!({ "definition": hook(name, url) }).to_string()),
        );
        assert_eq!(made.status, 201, "{}", made.body);
    }
    let alerts = workflow_named(&studio, &test, "Order alerts")["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        call(
            &studio,
            &test,
            "POST",
            &format!("/api/workflows/{alerts}/active"),
            Some(r#"{"active": true}"#)
        )
        .status,
        200
    );

    let ci = token(&studio, &test, json!({ "name": "CI deploy", "role": "editor" }));
    assert!(ci.starts_with("odapi_"));
    let listed = call(&studio, &test, "GET", "/api/tokens", None);
    assert_eq!(listed.body["tokens"][0]["name"], "CI deploy");
    assert!(!listed.body.to_string().contains(&ci), "a token is shown once");

    // Pull: a file a workflow, named after it, `on: true` for the one that runs.
    let dir = tempfile::tempdir().unwrap();
    let (code, out, err) = cli(&studio, &ci, &["pull", dir.path().to_str().unwrap()]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("2 workflow(s) of Test"), "{out}");
    let file = read(dir.path(), "order-alerts.yaml");
    assert!(file.starts_with("name: Order alerts\non: true\n"), "{file}");
    assert!(!read(dir.path(), "stock-check.yaml").contains("on:"));
    let (code, out, _) = cli(&studio, &ci, &["pull", dir.path().to_str().unwrap()]);
    assert_eq!(code, 0);
    assert!(
        out.contains("0 written, 2 unchanged"),
        "pulling again writes nothing: {out}"
    );

    // An edit, a new workflow; diff says what a push would change, step by step.
    std::fs::write(
        dir.path().join("order-alerts.yaml"),
        file.replace("shop.example.com/a", "shop.example.com/a2"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("new-one.yaml"),
        "name: New one\non: true\ntrigger: manual\nsteps:\n  - name: tag\n    set: { tagged: true }\n",
    )
    .unwrap();
    let (code, out, err) = cli(
        &studio,
        &ci,
        &["diff", dir.path().to_str().unwrap(), "--exit-code"],
    );
    assert_eq!(code, 3, "a push would change something: {out}{err}");
    assert!(
        out.contains("~ Order alerts (order-alerts.yaml): v1 -> v2, on"),
        "{out}"
    );
    assert!(out.contains("    ~ step call: http.url"), "{out}");
    assert!(
        out.contains("+ New one (new-one.yaml): new, v1, turned on"),
        "{out}"
    );
    assert!(
        out.contains("= Stock check (stock-check.yaml): unchanged, off"),
        "{out}"
    );
    let (code, out, _) = cli(
        &studio,
        &ci,
        &["push", dir.path().to_str().unwrap(), "--dry-run", "--json"],
    );
    assert_eq!(code, 0);
    let plan: Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(plan["summary"]["create"], 1);
    assert_eq!(
        versions(&studio, &test, &alerts).len(),
        1,
        "a dry run changes nothing"
    );

    // A wrong file: refused before anything is sent.
    std::fs::write(
        dir.path().join("broken.yaml"),
        "name: Broken\ntrigger: { nonsense: {} }\n",
    )
    .unwrap();
    let (code, _, err) = cli(&studio, &ci, &["push", dir.path().to_str().unwrap()]);
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("broken.yaml") && err.contains("nothing was sent"),
        "{err}"
    );
    std::fs::remove_file(dir.path().join("broken.yaml")).unwrap();

    // The push: a new version, a new workflow turned on, each recorded as a push with the token's name.
    let (code, out, err) = cli(&studio, &ci, &["push", dir.path().to_str().unwrap()]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(
        out.contains("1 new, 1 changed, 1 unchanged, 1 turned on, 0 turned off"),
        "{out}"
    );
    let v = versions(&studio, &test, &alerts);
    assert_eq!(v.len(), 2);
    assert_eq!(
        v[0]["source"],
        json!({ "source": "push", "token": "CI deploy", "by": "ada@example.com", "detail": null })
    );
    assert_eq!(v[1]["source"]["source"], "studio");
    assert_eq!(workflow_named(&studio, &test, "New one")["active"], true);
    let (code, _, _) = cli(
        &studio,
        &ci,
        &["diff", dir.path().to_str().unwrap(), "--exit-code"],
    );
    assert_eq!(code, 0, "nothing left to change");

    // Rollback: v1 again, as v3.
    let (code, out, err) = cli(&studio, &ci, &["rollback", "Order alerts"]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("Order alerts: v3 is v1 again"), "{out}");
    let v = versions(&studio, &test, &alerts);
    assert_eq!(v[0]["source"]["detail"], "to v1");
    assert_eq!(v[0]["definition"], v[2]["definition"]);
    let (code, _, err) = cli(&studio, &ci, &["rollback", "Nobody"]);
    assert_eq!((code, err.contains("no workflow called")), (1, true), "{err}");

    // Promotion to production: refused while production lacks the credential, then made.
    let deploy = token(&studio, &prod, json!({ "name": "Prod deploy" }));
    let (code, _, err) = cli(
        &studio,
        &deploy,
        &["promote", "--all", "--from", "Test", "--to", "Production"],
    );
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("Order alerts") && err.contains("needs credential shop"),
        "{err}"
    );
    assert!(
        call(&studio, &prod, "GET", "/api/workflows", None).body["workflows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let (code, _, err) = cli(
        &studio,
        &ci,
        &["promote", "Order alerts", "--from", "Test", "--to", "Production"],
    );
    assert_eq!(code, 1, "a test token does not write into production: {err}");
    assert!(err.contains("promote with a token of Production"), "{err}");
    credential(&studio, &prod, "shop");
    let (code, out, err) = cli(
        &studio,
        &deploy,
        &["promote", "--all", "--from", "Test", "--to", "Production"],
    );
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("3 new"), "{out}");
    let promoted = workflow_named(&studio, &prod, "Order alerts");
    assert_eq!(promoted["active"], false, "promoted off unless asked");
    let v = versions(&studio, &prod, promoted["id"].as_str().unwrap());
    assert_eq!(v[0]["source"]["source"], "promotion");
    assert_eq!(v[0]["source"]["detail"], "from Test v3");
    assert_eq!(v[0]["source"]["token"], "Prod deploy");
    assert!(
        !call(&studio, &prod, "GET", "/api/credentials", None)
            .body
            .to_string()
            .contains("t-SECRET")
    );
    let (code, _, err) = cli(
        &studio,
        &deploy,
        &["promote", "--all", "--from", "Production", "--to", "Test"],
    );
    assert_ne!(code, 0, "only forward");
    assert!(
        err.contains("promotion goes forward") || err.contains("token"),
        "{err}"
    );

    // Production takes changes only by push or promotion: a person's edit there is refused.
    let edit = call(
        &studio,
        &prod,
        "PUT",
        &format!("/api/workflows/{}", promoted["id"].as_str().unwrap()),
        Some(&json!({ "definition": hook("Order alerts", "https://elsewhere.example.com") }).to_string()),
    );
    assert_eq!(edit.status, 409, "{}", edit.body);
    assert!(
        edit.body["error"]
            .as_str()
            .unwrap()
            .contains("only by push or promotion")
    );
    let made = call(
        &studio,
        &prod,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": hook("Sneaky", "https://x.example") }).to_string()),
    );
    assert_eq!(made.status, 409);

    // A token reaches its own workspace only.
    let other = studio::http(
        &studio,
        "GET",
        "/api/workflows",
        &[
            &format!("authorization: Bearer {ci}"),
            &format!("x-openduct-workspace: {prod_ws}"),
        ],
        None,
    );
    assert_eq!(other.status, 403, "{}", other.body);
    let mine = studio::http(
        &studio,
        "GET",
        "/api/workflows",
        &[&format!("authorization: Bearer {ci}")],
        None,
    );
    assert_eq!(mine.status, 200, "an unscoped token reaches the rest of the API");
    let people = studio::http(
        &studio,
        "GET",
        "/api/tokens",
        &[&format!("authorization: Bearer {ci}")],
        None,
    );
    assert_eq!(people.status, 403, "a token makes no tokens");

    // A token limited to some workflows: it pulls, pushes and prunes only those.
    let scoped = token(
        &studio,
        &test,
        json!({ "name": "Stock only", "workflows": ["Stock*"] }),
    );
    let small = tempfile::tempdir().unwrap();
    let (code, out, _) = cli(&studio, &scoped, &["pull", small.path().to_str().unwrap()]);
    assert_eq!(code, 0);
    assert!(out.starts_with("1 workflow(s)"), "{out}");
    std::fs::copy(
        dir.path().join("order-alerts.yaml"),
        small.path().join("order-alerts.yaml"),
    )
    .unwrap();
    let (code, _, err) = cli(&studio, &scoped, &["push", small.path().to_str().unwrap()]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("outside the workflows this token reaches"), "{err}");
    let beyond = studio::http(
        &studio,
        "GET",
        "/api/workflows",
        &[&format!("authorization: Bearer {scoped}")],
        None,
    );
    assert_eq!(beyond.status, 403, "a scoped token makes only the CI/CD requests");

    // A maker who has lost their role takes the token's power with them.
    let added = call(
        &studio,
        &test,
        "POST",
        &format!("/api/workspaces/{test_ws}/members"),
        Some(
            r#"{"email": "grace@example.com", "name": "Grace", "password": "another horse battery", "role": "admin"}"#,
        ),
    );
    assert_eq!(added.status, 201, "{}", added.body);
    let grace_id = added.body["user"].as_str().unwrap().to_owned();
    let signed_in = studio::http(
        &studio,
        "POST",
        "/api/sign-in",
        &[studio::JSON],
        Some(r#"{"email": "grace@example.com", "password": "another horse battery"}"#),
    );
    let grace = vec![
        format!("cookie: {}", signed_in.cookie()),
        format!("x-openduct-csrf: {}", signed_in.body["csrf"].as_str().unwrap()),
        studio::JSON.to_owned(),
        format!("x-openduct-workspace: {test_ws}"),
    ];
    let graces = token(&studio, &grace, json!({ "name": "Grace's", "role": "admin" }));
    assert_eq!(
        cli(&studio, &graces, &["push", dir.path().to_str().unwrap()]).0,
        0
    );
    let demoted = call(
        &studio,
        &test,
        "PUT",
        &format!("/api/workspaces/{test_ws}/members/{grace_id}"),
        Some(r#"{"role": "viewer"}"#),
    );
    assert_eq!(demoted.status, 200);
    let (code, _, err) = cli(&studio, &graces, &["push", dir.path().to_str().unwrap()]);
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("the token has less"), "{err}");
    assert_eq!(
        cli(&studio, &graces, &["pull", small.path().to_str().unwrap()]).0,
        0,
        "a viewer still pulls"
    );

    // Revoked: refused at once on this node (and within seconds on any other).
    let id = call(&studio, &test, "GET", "/api/tokens", None).body["tokens"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "CI deploy")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        call(&studio, &test, "DELETE", &format!("/api/tokens/{id}"), None).status,
        200
    );
    until(10, "the revoked token is refused", || {
        studio::http(
            &studio,
            "GET",
            "/api/cicd/whoami",
            &[&format!("authorization: Bearer {ci}")],
            None,
        )
        .status
            == 401
    });
    let (code, _, err) = cli(&studio, &ci, &["pull", small.path().to_str().unwrap()]);
    assert_eq!((code, err.contains("revoked")), (1, true), "{err}");
    let (code, _, err) = cli(
        &studio,
        "odapi_not-a-real-one",
        &["pull", small.path().to_str().unwrap()],
    );
    assert_eq!(code, 1, "{err}");

    // Every request made with a token is in the log, with its name.
    until(10, "the log names the token", || {
        std::fs::read_to_string(data.path().join("openduct.log"))
            .unwrap_or_default()
            .contains("Prod deploy")
    });
}
