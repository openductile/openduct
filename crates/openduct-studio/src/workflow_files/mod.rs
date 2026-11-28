// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workflows as files in a folder, for CI/CD: what `openduct workflow pull` writes, `diff` compares and `push` sends,
//! and what the studio's Git sync commits and deploys.
//!
//! - **One file a workflow**, named after it (`order-alerts.yaml`), the same name every time it is written.
//! - **`on: true`** in a file turns the workflow on when it is pushed (`on: false` turns it off); it is a setting of
//!   the file, not of the workflow, so it is taken out before the workflow is kept.
//! - **Other workflows by name**: a `run_workflow` step or a `failed` trigger names the workflow it means by the id
//!   it has in its workspace, which another workspace does not share. A folder names it by its name instead, and a
//!   push or a promotion finds the id it has where it lands.
//! - **What a workflow needs** of its workspace - credentials, lookup files, data tables, other workflows - by name,
//!   so a push can say what the workspace it lands in lacks.

mod diff;
mod yaml;

use std::collections::{BTreeSet, HashSet};

use serde_json::{Map, Value};

pub use diff::{Changes, StepChange, changes};
pub use yaml::to_yaml;

/// The most workflow files a folder holds.
pub const MAX_FILES: usize = 500;
/// The largest workflow file, in bytes.
pub const MAX_FILE: usize = 512 * 1024;
/// The setting of a file that says whether the workflow runs.
pub const ON: &str = "on";

/// A workflow file, read: its definition (without `on`) and what `on` says, if anything.
#[derive(Debug, Clone)]
pub struct WorkflowFile {
    /// The file's name, as `order-alerts.yaml`.
    pub file: String,
    pub definition: Value,
    pub on: Option<bool>,
}

/// The name of a workflow's file in a folder: its name in small letters and dashes, `.yaml`; `taken` holds the names
/// given already, and a second workflow of the same name gets `-2`, a third `-3`.
pub fn file_name(name: &str, taken: &mut HashSet<String>) -> String {
    let mut stem = openduct_flow::slug(name);
    if stem.is_empty() {
        stem = "workflow".to_owned();
    }
    let mut candidate = format!("{stem}.yaml");
    let mut n = 2;
    while taken.contains(&candidate) {
        candidate = format!("{stem}-{n}.yaml");
        n += 1;
    }
    taken.insert(candidate.clone());
    candidate
}

/// Whether a file of a folder is a workflow file: `.yaml`, `.yml` or `.json`, not hidden.
#[must_use]
pub fn is_workflow_file(name: &str) -> bool {
    !name.starts_with('.')
        && [".yaml", ".yml", ".json"]
            .iter()
            .any(|ext| name.len() > ext.len() && name.to_ascii_lowercase().ends_with(ext))
}

/// A workflow file's text read: YAML (JSON is YAML too), its `on` taken out.
pub fn read(file: &str, text: &str) -> Result<WorkflowFile, String> {
    if text.len() > MAX_FILE {
        return Err(format!("{file}: larger than {MAX_FILE} bytes"));
    }
    let mut definition = if file.to_ascii_lowercase().ends_with(".json") {
        serde_json::from_str(text).map_err(|e| format!("{file}: not JSON: {e}"))?
    } else {
        openduct_flow::yaml_to_json(text).map_err(|e| format!("{file}: {e}"))?
    };
    let on = match definition.as_object_mut().and_then(|d| d.shift_remove(ON)) {
        None => None,
        Some(Value::Bool(b)) => Some(b),
        Some(_) => return Err(format!("{file}: on: true or false")),
    };
    Ok(WorkflowFile {
        file: file.to_owned(),
        definition,
        on,
    })
}

/// A workflow's file: its definition as YAML, `on: true` beside its name when it is on.
#[must_use]
pub fn write(definition: &Value, on: bool) -> String {
    let mut shown = Map::new();
    if let Some(fields) = definition.as_object() {
        if let Some(name) = fields.get("name") {
            shown.insert("name".to_owned(), name.clone());
        }
        if on {
            shown.insert(ON.to_owned(), Value::Bool(true));
        }
        for (k, v) in fields {
            if k != "name" && k != ON {
                shown.insert(k.clone(), v.clone());
            }
        }
    }
    to_yaml(&Value::Object(shown))
}

/// What a workflow needs of the workspace it runs in, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Needs {
    pub credentials: BTreeSet<String>,
    /// Lookup files, by path (`/rates.csv`).
    pub files: BTreeSet<String>,
    /// Data tables.
    pub tables: BTreeSet<String>,
    /// Other workflows it runs, or whose failures start it: by id or by name, as written.
    pub workflows: BTreeSet<String>,
}

impl Needs {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.credentials.is_empty()
            && self.files.is_empty()
            && self.tables.is_empty()
            && self.workflows.is_empty()
    }
}

/// A name a definition gives literally: an expression (`{{ }}`) is worked out in the run, and not known here.
fn literal(value: &Value) -> Option<&str> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.contains("{{"))
}

/// What a definition names of its workspace: every `credential` set anywhere in it (steps, a trigger, an agent's
/// tools), each `lookup` step's file, each `table` step's table, and the workflows of `run_workflow` steps and of a
/// `failed` trigger.
#[must_use]
pub fn needs(definition: &Value) -> Needs {
    let mut needs = Needs::default();
    walk(definition, None, &mut needs, 0);
    needs
}

/// How deep [`needs`] and [`rename_workflows`] look: a workflow file nests at most 64 deep.
const MAX_DEPTH: usize = 80;

fn walk(value: &Value, key: Option<&str>, needs: &mut Needs, depth: usize) {
    if depth > MAX_DEPTH {
        return;
    }
    match value {
        Value::Object(fields) => {
            if let Some(name) = fields.get("credential").and_then(literal) {
                needs.credentials.insert(name.to_owned());
            }
            match key {
                Some("lookup") => {
                    if let Some(file) = fields.get("file").and_then(literal) {
                        needs.files.insert(file.to_owned());
                    }
                }
                Some("table") => {
                    if let Some(table) = fields.get("name").and_then(literal) {
                        needs.tables.insert(table.to_owned());
                    }
                }
                Some("run_workflow" | "failed") => {
                    if let Some(workflow) = fields.get("workflow").and_then(literal) {
                        needs.workflows.insert(workflow.to_owned());
                    }
                }
                _ => {}
            }
            for (k, v) in fields {
                walk(v, Some(k), needs, depth + 1);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(item, key, needs, depth + 1);
            }
        }
        _ => {}
    }
}

/// The definition with each other workflow it names (a `run_workflow` step's, a `failed` trigger's) given as
/// `rename` says: an id for a name when it lands in a workspace, a name for an id when it is written to a file.
/// Those `rename` says nothing of stay as they are.
pub fn rename_workflows(definition: &mut Value, rename: &dyn Fn(&str) -> Option<String>) {
    renamed(definition, None, rename, 0);
}

fn renamed(value: &mut Value, key: Option<&str>, rename: &dyn Fn(&str) -> Option<String>, depth: usize) {
    if depth > MAX_DEPTH {
        return;
    }
    match value {
        Value::Object(fields) => {
            if matches!(key, Some("run_workflow" | "failed"))
                && let Some(slot) = fields.get_mut("workflow")
                && let Some(to) = literal(slot).and_then(rename)
            {
                *slot = Value::String(to);
            }
            for (k, v) in fields.iter_mut() {
                renamed(v, Some(k.as_str()), rename, depth + 1);
            }
        }
        Value::Array(items) => {
            for item in items {
                renamed(item, key, rename, depth + 1);
            }
        }
        _ => {}
    }
}

/// The name a definition gives itself.
#[must_use]
pub fn name_of(definition: &Value) -> Option<&str> {
    definition
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|n| !n.is_empty())
}

/// Whether a token's workflow scopes (`None`: every workflow) reach the workflow called `name`: a scope is a name,
/// or a beginning ending in `*` (`billing-*`).
#[must_use]
pub fn in_scope(scopes: Option<&[String]>, name: &str) -> bool {
    scopes.is_none_or(|scopes| {
        scopes.iter().any(|s| match s.strip_suffix('*') {
            Some(start) => name.starts_with(start),
            None => s == name,
        })
    })
}

#[cfg(test)]
mod tests;
