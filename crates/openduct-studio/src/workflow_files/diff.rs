// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What changes between two definitions of a workflow, step by step: the workflow's own settings that differ
//! (its trigger, description ...), and each step added, taken out or changed - which of its settings - and whether
//! the steps were put in another order. Steps are told apart by their names, an unnamed one by its kind and place.

use serde::Serialize;
use serde_json::{Map, Value};

/// What a step's change is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StepChange {
    /// The step's name (or `<kind> #<n>` for one without).
    pub step: String,
    /// `added`, `removed` or `changed`.
    pub change: &'static str,
    /// For a change: which of its settings, as `http.url`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
}

/// The changes between two definitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Changes {
    /// The workflow's settings that differ, beside its steps: `trigger`, `description`, `name` ...
    pub settings: Vec<String>,
    pub steps: Vec<StepChange>,
    /// The steps kept are in another order.
    pub reordered: bool,
}

impl Changes {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.settings.is_empty() && self.steps.is_empty() && !self.reordered
    }
}

/// The most settings a changed step lists.
const MAX_FIELDS: usize = 20;

/// What changes from `old` to `new`.
#[must_use]
pub fn changes(old: &Value, new: &Value) -> Changes {
    let empty = Map::new();
    let (o, n) = (
        old.as_object().unwrap_or(&empty),
        new.as_object().unwrap_or(&empty),
    );
    let mut settings: Vec<String> = Vec::new();
    for key in o.keys().chain(n.keys()) {
        if key != "steps" && o.get(key) != n.get(key) && !settings.contains(key) {
            settings.push(key.clone());
        }
    }
    let (old_steps, new_steps) = (named(o.get("steps")), named(n.get("steps")));
    let mut steps = Vec::new();
    for (name, step) in &new_steps {
        match old_steps.iter().find(|(n, _)| n == name) {
            None => steps.push(StepChange {
                step: name.clone(),
                change: "added",
                fields: Vec::new(),
            }),
            Some((_, before)) if before != step => steps.push(StepChange {
                step: name.clone(),
                change: "changed",
                fields: fields(before, step),
            }),
            Some(_) => {}
        }
    }
    for (name, _) in &old_steps {
        if !new_steps.iter().any(|(n, _)| n == name) {
            steps.push(StepChange {
                step: name.clone(),
                change: "removed",
                fields: Vec::new(),
            });
        }
    }
    let kept_old: Vec<&String> = old_steps
        .iter()
        .map(|(n, _)| n)
        .filter(|n| new_steps.iter().any(|(m, _)| m == *n))
        .collect();
    let kept_new: Vec<&String> = new_steps
        .iter()
        .map(|(n, _)| n)
        .filter(|n| old_steps.iter().any(|(m, _)| m == *n))
        .collect();
    Changes {
        settings,
        steps,
        reordered: kept_old != kept_new,
    }
}

/// Each step with the name it is told apart by.
fn named(steps: Option<&Value>) -> Vec<(String, &Value)> {
    let Some(Value::Array(steps)) = steps else {
        return Vec::new();
    };
    let mut out: Vec<(String, &Value)> = Vec::new();
    for step in steps {
        let mut name = step
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                let kind = step
                    .as_object()
                    .and_then(|s| {
                        s.keys()
                            .find(|k| !matches!(k.as_str(), "name" | "retry" | "on_error"))
                    })
                    .cloned()
                    .unwrap_or_else(|| "step".to_owned());
                let n = out
                    .iter()
                    .filter(|(o, _)| o.starts_with(&format!("{kind} #")))
                    .count()
                    + 1;
                format!("{kind} #{n}")
            });
        while out.iter().any(|(o, _)| *o == name) {
            name.push('\'');
        }
        out.push((name, step));
    }
    out
}

/// The settings of a step that differ: a top setting, or one level in (`http.url`).
fn fields(old: &Value, new: &Value) -> Vec<String> {
    let empty = Map::new();
    let (o, n) = (
        old.as_object().unwrap_or(&empty),
        new.as_object().unwrap_or(&empty),
    );
    let mut out: Vec<String> = Vec::new();
    for key in o.keys().chain(n.keys()) {
        let (a, b) = (o.get(key), n.get(key));
        if a == b || out.iter().any(|f| f == key || f.starts_with(&format!("{key}."))) {
            continue;
        }
        match (a, b) {
            (Some(Value::Object(x)), Some(Value::Object(y))) => {
                for inner in x.keys().chain(y.keys()) {
                    let path = format!("{key}.{inner}");
                    if x.get(inner) != y.get(inner) && !out.contains(&path) {
                        out.push(path);
                    }
                }
            }
            _ => out.push(key.clone()),
        }
    }
    out.truncate(MAX_FIELDS);
    out
}
