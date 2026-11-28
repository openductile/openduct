// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a push (or a promotion, or a deploy from Git) would do to a workspace, worked out before anything is
//! changed: every file checked as a workflow, every credential, lookup file, data table and other workflow it names
//! found in the workspace, and for each workflow whether it is new, changed (step by step) or the same, and whether
//! it will run. Any problem refuses the whole push, saying each one.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Serialize;
use serde_json::{Value, json};

use crate::model::new_id;
use crate::workflow_files::{Changes, changes, in_scope, name_of, needs, rename_workflows};

/// A workflow of the workspace, as it is now.
#[derive(Debug, Clone)]
pub(crate) struct Current {
    pub id: String,
    pub name: String,
    pub version: u64,
    pub active: bool,
    pub definition: Value,
}

/// A workflow as a push brings it.
#[derive(Debug, Clone)]
pub(crate) struct Incoming {
    /// Where it came from: a file's name, or the workflow it is promoted from.
    pub file: String,
    pub definition: Value,
    /// Whether the file turns it on or off.
    pub on: Option<bool>,
}

/// What a workspace holds that workflows name.
#[derive(Debug, Clone, Default)]
pub(crate) struct Inventory {
    pub credentials: BTreeSet<String>,
    pub files: BTreeSet<String>,
    pub tables: BTreeSet<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Options {
    /// Turn on every workflow pushed whose file does not say.
    pub activate: bool,
    /// Turn off the workflows of the workspace not pushed.
    pub prune: bool,
    /// The workflows the push may touch (a token's scopes); `None`: all.
    pub scopes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Create,
    Update,
    Unchanged,
    TurnOff,
}

/// One workflow's part of a push.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Planned {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    pub action: Action,
    /// Its id: the one it has, or the one it will have.
    pub id: String,
    /// The version it has now, and the one it will have.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_version: Option<u64>,
    pub was_on: bool,
    pub on: bool,
    pub changes: Changes,
    /// The definition kept for a new or changed workflow, its other workflows named by their ids here.
    #[serde(skip)]
    pub definition: Option<Value>,
}

/// Why a push is refused: what is wrong with its files, or what the workspace lacks.
#[derive(Debug, Clone)]
pub(crate) enum Refusal {
    Problems(Vec<Value>),
    Missing(Vec<Value>),
}

impl Refusal {
    /// The refusal as the API answers it.
    pub(crate) fn body(&self) -> Value {
        match self {
            Self::Problems(p) => {
                let said: Vec<String> = p
                    .iter()
                    .take(SAID)
                    .map(|p| format!("{}: {}", text(&p["file"]), text(&p["problem"])))
                    .collect();
                json!({
                    "error": format!("{} of the workflows cannot be pushed ({}): nothing was changed", p.len(), said.join("; ")),
                    "problems": p,
                })
            }
            Self::Missing(m) => {
                let said: Vec<String> = m.iter().take(SAID).map(lacking).collect();
                json!({
                    "error": format!("the workspace lacks what the workflows name ({}): nothing was changed", said.join("; ")),
                    "missing": m,
                })
            }
        }
    }
}

/// How many workflows a refusal's words name; the rest are in its list.
const SAID: usize = 5;

fn text(v: &Value) -> &str {
    v.as_str().unwrap_or("?")
}

/// What one workflow lacks, in words: `Order alerts needs credential shop, table leads`.
fn lacking(m: &Value) -> String {
    let mut lacks = Vec::new();
    for (key, what) in [
        ("credentials", "credential"),
        ("files", "lookup file"),
        ("tables", "table"),
        ("workflows", "workflow"),
    ] {
        for n in m[key].as_array().into_iter().flatten() {
            lacks.push(format!("{what} {}", text(n)));
        }
    }
    format!("{} needs {}", text(&m["name"]), lacks.join(", "))
}

fn problem(file: &str, name: Option<&str>, why: &str) -> Value {
    json!({ "file": file, "name": name, "problem": why })
}

/// What pushing `incoming` into a workspace holding `current` and `inventory` would do.
pub(crate) fn plan(
    current: &[Current],
    incoming: &[Incoming],
    inventory: &Inventory,
    options: &Options,
) -> Result<Vec<Planned>, Refusal> {
    let scopes = options.scopes.as_deref();
    let mut problems = Vec::new();
    // Names: each pushed once, and found at most once in the workspace.
    let mut here: HashMap<&str, Vec<&Current>> = HashMap::new();
    for c in current {
        here.entry(c.name.as_str()).or_default().push(c);
    }
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    let mut named: Vec<(&Incoming, String)> = Vec::new();
    for i in incoming {
        let Some(name) = name_of(&i.definition) else {
            problems.push(problem(&i.file, None, "name: the workflow's name"));
            continue;
        };
        if let Some(other) = seen.get(name) {
            problems.push(problem(
                &i.file,
                Some(name),
                &format!("{other} is called {name} too: a name once in a folder"),
            ));
            continue;
        }
        seen.insert(name.to_owned(), &i.file);
        if !in_scope(scopes, name) {
            problems.push(problem(
                &i.file,
                Some(name),
                "outside the workflows this token reaches",
            ));
            continue;
        }
        if here.get(name).is_some_and(|found| found.len() > 1) {
            problems.push(problem(
                &i.file,
                Some(name),
                &format!(
                    "the workspace has several workflows called {name}: rename all but one in the studio"
                ),
            ));
            continue;
        }
        named.push((i, name.to_owned()));
    }
    // Ids: the one a workflow has, or a new one; other workflows are found by id, or by name among both.
    let ids: Vec<String> = named
        .iter()
        .map(|(_, name)| {
            here.get(name.as_str())
                .and_then(|f| f.first())
                .map_or_else(new_id, |c| c.id.clone())
        })
        .collect();
    let mut by_name: HashMap<String, String> =
        current.iter().map(|c| (c.name.clone(), c.id.clone())).collect();
    for ((_, name), id) in named.iter().zip(&ids) {
        by_name.insert(name.clone(), id.clone());
    }
    let known: BTreeSet<&str> = current
        .iter()
        .map(|c| c.id.as_str())
        .chain(ids.iter().map(String::as_str))
        .collect();
    let mut missing = Vec::new();
    let mut planned = Vec::new();
    for ((incoming, name), id) in named.iter().zip(&ids) {
        let mut definition = incoming.definition.clone();
        rename_workflows(&mut definition, &|w| {
            if known.contains(w) {
                None
            } else {
                by_name.get(w).cloned()
            }
        });
        if let Err(e) = openduct_flow::Workflow::from_json(&definition, Some(id)) {
            problems.push(problem(&incoming.file, Some(name), &e));
            continue;
        }
        let need = needs(&definition);
        let lacks = |wanted: &BTreeSet<String>, held: &BTreeSet<String>| -> Vec<String> {
            wanted.difference(held).cloned().collect()
        };
        let (credentials, files, tables) = (
            lacks(&need.credentials, &inventory.credentials),
            lacks(&need.files, &inventory.files),
            lacks(&need.tables, &inventory.tables),
        );
        let workflows: Vec<&String> = need
            .workflows
            .iter()
            .filter(|w| !known.contains(w.as_str()))
            .collect();
        if !(credentials.is_empty() && files.is_empty() && tables.is_empty() && workflows.is_empty()) {
            missing.push(json!({
                "file": incoming.file, "name": name, "credentials": credentials, "files": files, "tables": tables,
                "workflows": workflows,
            }));
            continue;
        }
        let existing = here.get(name.as_str()).and_then(|f| f.first());
        let was_on = existing.is_some_and(|c| c.active);
        let on = incoming.on.unwrap_or(options.activate || was_on);
        planned.push(match existing {
            None => Planned {
                name: name.clone(),
                file: Some(incoming.file.clone()),
                action: Action::Create,
                id: id.clone(),
                from_version: None,
                to_version: Some(1),
                was_on,
                on,
                changes: changes(&json!({}), &definition),
                definition: Some(definition),
            },
            Some(c) => {
                let changed = c.definition != definition;
                Planned {
                    name: name.clone(),
                    file: Some(incoming.file.clone()),
                    action: if changed {
                        Action::Update
                    } else {
                        Action::Unchanged
                    },
                    id: id.clone(),
                    from_version: Some(c.version),
                    to_version: Some(if changed { c.version + 1 } else { c.version }),
                    was_on,
                    on,
                    changes: changes(&c.definition, &definition),
                    definition: changed.then_some(definition),
                }
            }
        });
    }
    if !problems.is_empty() {
        return Err(Refusal::Problems(problems));
    }
    if !missing.is_empty() {
        return Err(Refusal::Missing(missing));
    }
    if options.prune {
        let mut left: Vec<&Current> = current
            .iter()
            .filter(|c| c.active && !seen.contains_key(&c.name) && in_scope(scopes, &c.name))
            .collect();
        left.sort_by(|a, b| a.name.cmp(&b.name));
        for c in left {
            planned.push(Planned {
                name: c.name.clone(),
                file: None,
                action: Action::TurnOff,
                id: c.id.clone(),
                from_version: Some(c.version),
                to_version: Some(c.version),
                was_on: true,
                on: false,
                changes: Changes::default(),
                definition: None,
            });
        }
    }
    Ok(planned)
}

/// Whether a planned workflow changes anything at all.
pub(crate) fn changes_anything(p: &Planned) -> bool {
    p.action != Action::Unchanged || p.was_on != p.on
}

/// What a plan comes to, counted.
pub(crate) fn summary(planned: &[Planned]) -> Value {
    let count = |a: Action| planned.iter().filter(|p| p.action == a).count();
    json!({
        "create": count(Action::Create),
        "update": count(Action::Update),
        "unchanged": count(Action::Unchanged),
        "turn_off": count(Action::TurnOff),
        "turn_on": planned.iter().filter(|p| p.on && !p.was_on).count(),
        "changes": planned.iter().any(changes_anything),
    })
}

#[cfg(test)]
mod tests;
