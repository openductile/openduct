// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct app`: the kit for building an app - start one, check it, run its recorded exchanges, and try one of its
//! actions against the real service with a credential of your own. See docs/apps.md.

use std::path::{Path, PathBuf};

use clap::Subcommand;
use openduct_expr::Value;
use openduct_flow::Credential;
use openduct_flow::app_files::{self, AppDef};

#[derive(Debug, Subcommand)]
pub(crate) enum AppCommand {
    /// Start a new app: a folder with its definition, a README and a recorded test, ready to change.
    New {
        /// The app's id: lowercase letters, digits and '_' (a workflow step is named after it).
        id: String,
        /// The folder the app's folder goes in.
        #[arg(long, default_value = "apps")]
        dir: PathBuf,
    },
    /// Check apps' definitions: names, hosts, every template and expression; and say how finished each is.
    Check {
        /// An app's folder, or a folder of apps' folders (by default ./apps).
        paths: Vec<PathBuf>,
        /// Fail for an app whose quality score is below this (0 to 100; the bar for an app OpenDuct ships is 80).
        #[arg(long)]
        bar: Option<u32>,
    },
    /// Make an app from a vendor's API description: OpenAPI 3.0 or 3.1, or Swagger 2.0, as JSON or YAML.
    FromOpenapi {
        /// The description: a file, or its https:// address.
        spec: String,
        /// The app's id: lowercase letters, digits and '_' (a workflow step is named after it).
        #[arg(long)]
        name: String,
        /// Its name as people know it (by default the description's title).
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        category: Option<String>,
        /// How many actions at most: the best of the description's operations (see docs/apps.md).
        #[arg(long, default_value_t = openduct_flow::app_files::generate::DEFAULT_ACTIONS)]
        actions: usize,
        /// Only operations with one of these tags (with commas).
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,
        /// Exactly these actions, by name (with commas), in place of the best.
        #[arg(long, value_delimiter = ',')]
        choose: Vec<String>,
        /// How it signs in when the description does not say: bearer, basic, header:<name> or query:<name>.
        #[arg(long)]
        sign_in: Option<String>,
        /// The API's address, when the description names none (or to use another).
        #[arg(long)]
        base_url: Option<String>,
        /// The vendor's API reference, when the description names none.
        #[arg(long)]
        docs: Option<String>,
        /// What the README says the description came from (by default its address or file's name).
        #[arg(long)]
        source: Option<String>,
        /// The folder to write it in (by default apps/<name>).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Write over an app that is there.
        #[arg(long)]
        force: bool,
        /// Let the description's address be on this machine's own network.
        #[arg(long)]
        allow_private_network: bool,
    },
    /// Make an app whose actions are the tools of a vendor's MCP server.
    FromMcp {
        /// The server's MCP address (https://).
        url: String,
        /// The app's id: lowercase letters, digits and '_'.
        #[arg(long)]
        name: String,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        category: Option<String>,
        /// How many tools at most (by default all, up to 100).
        #[arg(long, default_value_t = 0)]
        actions: usize,
        /// The token the server takes, as `token=...` in a file only you may read (chmod 600).
        #[arg(long)]
        token_file: Option<PathBuf>,
        #[arg(long)]
        docs: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        allow_private_network: bool,
    },
    /// Check apps and run their recorded exchanges (tests/*.yaml): no account, no network.
    Test {
        /// An app's folder, or a folder of apps' folders (by default ./apps).
        paths: Vec<PathBuf>,
    },
    /// Show the request one action makes - or, with --send, make it and show what came back.
    Try {
        /// An app's folder, or the id of an app this build carries.
        app: String,
        /// The action.
        action: String,
        /// The sign-in the credential is of (by default the app's first).
        #[arg(long)]
        sign_in: Option<String>,
        /// The credential's fields: one `name=value` a line, in a file only you may read (chmod 600).
        #[arg(long)]
        credential_file: Option<PathBuf>,
        /// For a sign-in at the vendor: this install's registration with it (`client_id=`, `client_secret=`), in a
        /// file only you may read. A renewed token is kept in the credential file.
        #[arg(long)]
        registration_file: Option<PathBuf>,
        /// An input: `name=value` (a JSON value, or text). Give it once for each.
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Send it to the service, as a workflow step does (a token fetched or renewed, every page), rather than
        /// only show it.
        #[arg(long)]
        send: bool,
    },
    /// List the apps this build of OpenDuct carries.
    List,
}

/// The app folders `paths` name: an app's own folder, or a folder of them; by default ./apps.
fn folders(paths: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    let paths = if paths.is_empty() {
        vec![PathBuf::from("apps")]
    } else {
        paths.to_vec()
    };
    let mut out = Vec::new();
    for p in paths {
        if p.join("app.yaml").is_file() {
            out.push(p);
            continue;
        }
        let entries = std::fs::read_dir(&p).map_err(|e| format!("cannot read {}: {e}", p.display()))?;
        let mut found: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|d| d.join("app.yaml").is_file())
            .collect();
        found.sort();
        if found.is_empty() {
            return Err(format!(
                "{}: has no app.yaml, nor folders that have one",
                p.display()
            ));
        }
        out.extend(found);
    }
    Ok(out)
}

fn load(folder: &Path) -> Result<&'static AppDef, String> {
    let file = folder.join("app.yaml");
    let text = std::fs::read_to_string(&file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    app_files::add(&text, &file.display().to_string())
}

fn summary(app: &AppDef) -> String {
    let sign_ins: Vec<&str> = app.sign_ins.iter().map(|s| s.id.as_str()).collect();
    // G3 app triggers: what starts a workflow from it, when it has any.
    let triggers = match app.triggers.len() {
        0 => String::new(),
        1 => ", 1 trigger".to_owned(),
        n => format!(", {n} triggers"),
    };
    format!(
        "{} ({}): {} action{}{triggers}, signs in as {}",
        app.id,
        app.name,
        app.actions.len(),
        if app.actions.len() == 1 { "" } else { "s" },
        sign_ins.join(" or ")
    )
}

pub(crate) async fn command(command: AppCommand) -> Result<(), String> {
    match command {
        AppCommand::New { id, dir } => new(&id, &dir),
        AppCommand::Check { paths, bar } => check(&paths, bar),
        // app generator
        AppCommand::FromOpenapi {
            spec,
            name,
            title,
            category,
            actions,
            only,
            choose,
            sign_in,
            base_url,
            docs,
            source,
            out,
            force,
            allow_private_network,
        } => {
            let options = openduct_flow::app_files::generate::Options {
                name: title,
                category,
                actions,
                only,
                chosen: (!choose.is_empty()).then_some(choose),
                sign_in: sign_in.as_deref().map(str::parse).transpose()?,
                base_url,
                docs,
                source: source.unwrap_or_default(),
                ..Default::default()
            };
            crate::app_gen::from_openapi(crate::app_gen::FromOpenapi {
                spec,
                id: name,
                options,
                out,
                force,
                allow_private: allow_private_network,
            })
            .await
        }
        AppCommand::FromMcp {
            url,
            name,
            title,
            category,
            actions,
            token_file,
            docs,
            out,
            force,
            allow_private_network,
        } => {
            let options = openduct_flow::app_files::generate::McpOptions {
                id: name,
                name: title,
                category,
                actions,
                docs,
                ..Default::default()
            };
            crate::app_gen::from_mcp(crate::app_gen::FromMcp {
                url,
                options,
                token_file,
                out,
                force,
                allow_private: allow_private_network,
            })
            .await
        }
        AppCommand::Test { paths } => test(&paths),
        AppCommand::Try {
            app,
            action,
            sign_in,
            credential_file,
            registration_file,
            inputs,
            send,
        } => {
            let files = Files {
                credential: credential_file.as_deref(),
                registration: registration_file.as_deref(),
            };
            try_one(&app, &action, sign_in, files, &inputs, send).await
        }
        AppCommand::List => {
            for app in app_files::all() {
                println!("{}", summary(app));
            }
            Ok(())
        }
    }
}

/// `openduct app check`: each app read and checked, and how finished it is; with a bar, those below it fail.
fn check(paths: &[PathBuf], bar: Option<u32>) -> Result<(), String> {
    let (mut failed, mut below) = (0, 0);
    for folder in folders(paths)? {
        match load(&folder) {
            Ok(app) => {
                println!("✓ {}", summary(app));
                // app generator: the quality report.
                let report = crate::app_gen::quality_of(app, &folder);
                for line in report.lines() {
                    println!("    {line}");
                }
                if bar.is_some_and(|b| !report.meets(b)) {
                    below += 1;
                    println!(
                        "✕ {}: {}/100 is below the bar of {}",
                        app.id,
                        report.score,
                        bar.unwrap_or_default()
                    );
                }
            }
            Err(e) => {
                failed += 1;
                println!("✕ {e}");
            }
        }
    }
    if failed > 0 {
        return Err(format!(
            "{failed} app{} could not be read",
            if failed == 1 { "" } else { "s" }
        ));
    }
    if below > 0 {
        return Err(format!(
            "{below} app{} below the bar",
            if below == 1 { " is" } else { "s are" }
        ));
    }
    Ok(())
}

fn test(paths: &[PathBuf]) -> Result<(), String> {
    let (mut passed, mut failed) = (0, 0);
    for folder in folders(paths)? {
        let app = match load(&folder) {
            Ok(a) => a,
            Err(e) => {
                failed += 1;
                println!("✕ {e}");
                continue;
            }
        };
        let tests = folder.join("tests");
        let mut files: Vec<PathBuf> = std::fs::read_dir(&tests)
            .map(|e| e.filter_map(Result::ok).map(|e| e.path()).collect())
            .unwrap_or_default();
        files.retain(|f| f.extension().is_some_and(|e| e == "yaml"));
        files.sort();
        if files.is_empty() {
            failed += 1;
            println!(
                "✕ {}: no recorded exchanges in {} (each action wants one)",
                app.id,
                tests.display()
            );
            continue;
        }
        for file in files {
            let text =
                std::fs::read_to_string(&file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
            match app_files::recorded::check(app, &text, &file.display().to_string()) {
                Ok(()) => {
                    passed += 1;
                    println!("✓ {}", file.display());
                }
                Err(e) => {
                    failed += 1;
                    println!("✕ {e}");
                }
            }
        }
    }
    println!("{passed} passed, {failed} failed");
    if failed > 0 {
        return Err("some recorded exchanges do not hold".to_owned());
    }
    Ok(())
}

/// An input as given: a JSON value, or else text.
fn input(given: &str) -> Result<(String, serde_json::Value), String> {
    let (name, value) = given
        .split_once('=')
        .ok_or_else(|| format!("--input {given:?}: is name=value"))?;
    let value = serde_json::from_str(value).unwrap_or_else(|_| serde_json::Value::String(value.to_owned()));
    Ok((name.trim().to_owned(), value))
}

/// A request's text with the credential's secrets as •••.
fn masked(text: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|s| s.len() >= 4)
        .fold(text.to_owned(), |t, s| t.replace(s.as_str(), "•••"))
}

/// The files a try reads its credential, and the install's registration, from.
struct Files<'a> {
    credential: Option<&'a Path>,
    registration: Option<&'a Path>,
}

async fn try_one(
    app: &str,
    action_name: &str,
    sign_in: Option<String>,
    files: Files<'_>,
    given: &[String],
    send: bool,
) -> Result<(), String> {
    let def = if Path::new(app).join("app.yaml").is_file() {
        load(Path::new(app))?
    } else {
        app_files::find(app)
            .ok_or_else(|| format!("{app}: no such app here, nor a folder with an app.yaml"))?
    };
    let (_, action) = def
        .action(Some(action_name))
        .ok_or_else(|| format!("{} has no action {action_name}", def.name))?;
    let kind = match sign_in {
        Some(s) => s,
        None => def.sign_ins.first().map(|s| s.id.clone()).unwrap_or_default(),
    };
    let fields = match files.credential {
        Some(path) => crate::secrets_file::read(path)?,
        None => std::collections::BTreeMap::new(),
    };
    let secrets: Vec<String> = def
        .sign_in(&kind)
        .ok_or_else(|| format!("{} has no sign-in {kind}", def.name))?
        .fields
        .iter()
        .filter(|f| f.secret)
        .filter_map(|f| fields.get(&f.name).cloned())
        .collect();
    let mut record = serde_json::Map::new();
    for g in given {
        let (k, v) = input(g)?;
        if !action.inputs.iter().any(|i| i.name == k) {
            return Err(format!("--input {k}: not an input of {action_name}"));
        }
        record.insert(k, v);
    }
    for i in action.inputs.iter().filter(|i| i.required) {
        if !record.contains_key(&i.name) {
            return Err(format!(
                "--input {}: {action_name} needs it ({})",
                i.name, i.label
            ));
        }
    }
    if send {
        return crate::app_try::send(crate::app_try::Given {
            app: def,
            action: action_name,
            kind,
            fields,
            credential_file: files.credential,
            registration_file: files.registration,
            inputs: record,
        })
        .await;
    }
    let credential = Credential { kind, fields };
    let inputs = Value::from_json(&serde_json::Value::Object(record));
    let request = app_files::request(def, action, &credential, &inputs, &Value::Null)?;
    println!("{} {}", request.method, masked(&request.url, &secrets));
    for (k, v) in &request.query {
        println!("  ?{k}={}", masked(v, &secrets));
    }
    for (k, v) in &request.headers {
        println!("  {k}: {}", masked(v, &secrets));
    }
    if let Some((kind, body)) = &request.body {
        println!("  ({kind}) {}", masked(&String::from_utf8_lossy(body), &secrets));
    }
    println!("(not sent: add --send to send it)");
    Ok(())
}

fn new(id: &str, dir: &Path) -> Result<(), String> {
    let ok = !id.is_empty()
        && id.len() <= 40
        && id.starts_with(|c: char| c.is_ascii_lowercase())
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !ok {
        return Err(format!(
            "{id:?}: an app's id is lowercase letters, digits and '_', at most 40"
        ));
    }
    let folder = dir.join(id);
    if folder.exists() {
        return Err(format!("{} is there already", folder.display()));
    }
    std::fs::create_dir_all(folder.join("tests"))
        .map_err(|e| format!("cannot make {}: {e}", folder.display()))?;
    let name: String = id
        .split('_')
        .map(|w| {
            let mut c = w.chars();
            c.next()
                .map_or_else(String::new, |f| f.to_ascii_uppercase().to_string() + c.as_str())
        })
        .collect::<Vec<_>>()
        .join(" ");
    let write = |file: &str, text: String| {
        std::fs::write(folder.join(file), text).map_err(|e| format!("cannot write {file}: {e}"))
    };
    write("app.yaml", STARTER.replace("{id}", id).replace("{name}", &name))?;
    write("README.md", README.replace("{id}", id).replace("{name}", &name))?;
    write("tests/add_contact.yaml", TEST.replace("{id}", id))?;
    println!(
        "Made {}: change app.yaml to the service's API, then:",
        folder.display()
    );
    println!("  openduct app check {}", folder.display());
    println!("  openduct app test {}", folder.display());
    println!(
        "  openduct app try {} list_contacts --credential-file my-key.env",
        folder.display()
    );
    Ok(())
}

const STARTER: &str = r#"# What {name} is to OpenDuct: how it signs in, and what it can do. See docs/apps.md.
app: {id}
name: {name}
category: other
description: What it does, in a line.
docs: https://example.com/api
hosts: [api.example.com]
base_url: https://api.example.com/v1
sign_in:
  - id: {id}
    label: An API key
    fields:
      - { name: api_key, label: The API key, secret: true }
    headers:
      Authorization: "Bearer {{ credential.api_key }}"
error: value.error.message
actions:
  list_contacts:
    label: List contacts
    method: GET
    path: /contacts
    query: { limit: "100" }
    items: value.data
    next: "value.has_more ? {'after': value.data[size(value.data) - 1].id} : null"
  add_contact:
    label: Add a contact
    write: true
    method: POST
    path: /contacts
    inputs:
      - { name: email, label: Email, required: true }
      - { name: name, label: Name }
    send:
      email: "{{ input.email }}"
      name: "{{ input.name }}"
    result: value
"#;

const README: &str = r#"# {name}

What the {name} app does in OpenDuct, and what each action is for.

## Signing in

Where to get the API key in {name}, and which permissions it needs.

## Actions

- **List contacts** (`list_contacts`)
- **Add a contact** (`add_contact`)

## Trying it

```sh
openduct app check apps/{id}
openduct app test apps/{id}
openduct app try apps/{id} list_contacts --credential-file my-key.env
```

`my-key.env` holds `api_key=...`, and only you may read it (`chmod 600 my-key.env`).
"#;

const TEST: &str = r#"description: A contact added
action: add_contact
credential: { api_key: test-key }
input: { email: ann@example.com, name: Ann }
exchanges:
  - sent:
      method: POST
      url: https://api.example.com/v1/contacts
      headers: { authorization: Bearer test-key }
      body: { email: ann@example.com, name: Ann }
    answer: { status: 201, body: { id: c_1, email: ann@example.com } }
result: { id: c_1, email: ann@example.com }
"#;
