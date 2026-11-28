// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow offered as a tool (`tool:` beside its trigger): its name, what it does in words a model reads, and the
//! arguments it takes. The studio's MCP server lists a workspace's tools for AI assistants and runs a workflow for
//! each call; an agent step can offer the same workflows to its model. A call's arguments are checked against the
//! inputs before a run starts, and become the run's `value`.
//!
//! ```yaml
//! tool:
//!   description: The price of an item from the price list, for a quantity.
//!   input:
//!     sku: { type: string, description: The item's code, required: true }
//!     qty: integer
//! ```

use std::time::Duration;

use serde_json::{Map, Value as Json, json};

/// The most inputs a tool takes.
pub const MAX_INPUTS: usize = 50;
/// The longest a call waits for its run's answer: by default and at most.
pub const TOOL_TIME: Duration = Duration::from_secs(60);
pub const MAX_TOOL_TIME: Duration = Duration::from_secs(300);
/// The longest description a tool, and each of its inputs, has.
const MAX_DESCRIPTION: usize = 2000;
/// The most choices an input lists.
const MAX_CHOICES: usize = 100;

/// A workflow offered as a tool.
#[derive(Debug, Clone, PartialEq)]
pub struct Tool {
    /// What callers name it by: letters, digits, `_`, `-` and `.`, 1 to 64 (the workflow's name made one by default).
    pub name: String,
    /// What it does, for the model that chooses it.
    pub description: String,
    pub inputs: Vec<Input>,
    /// How long a call waits for the run's answer.
    pub timeout: Duration,
}

/// One argument a tool takes.
#[derive(Debug, Clone, PartialEq)]
pub struct Input {
    pub name: String,
    pub kind: InputKind,
    pub description: Option<String>,
    pub required: bool,
    /// The only values it may have, when it lists them.
    pub choices: Vec<Json>,
}

/// What an input holds, by its JSON Schema type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Text,
    Number,
    Integer,
    Boolean,
    List,
    Record,
}

impl InputKind {
    fn read(name: &str) -> Option<Self> {
        Some(match name {
            "string" | "text" => Self::Text,
            "number" => Self::Number,
            "integer" => Self::Integer,
            "boolean" => Self::Boolean,
            "array" | "list" => Self::List,
            "object" | "record" => Self::Record,
            _ => return None,
        })
    }

    /// Its JSON Schema type.
    pub fn schema_type(self) -> &'static str {
        match self {
            Self::Text => "string",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::List => "array",
            Self::Record => "object",
        }
    }

    fn holds(self, value: &Json) -> bool {
        match self {
            Self::Text => value.is_string(),
            Self::Number => value.is_number(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Boolean => value.is_boolean(),
            Self::List => value.is_array(),
            Self::Record => value.is_object(),
        }
    }
}

/// Whether `name` may name a tool.
pub fn tool_name_ok(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

fn description(value: Option<&Json>, at: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(Json::Null) => Ok(None),
        Some(Json::String(s)) if s.trim().is_empty() => Ok(None),
        Some(Json::String(s)) if s.chars().count() <= MAX_DESCRIPTION => Ok(Some(s.trim().to_owned())),
        Some(Json::String(_)) => Err(format!("{at}: at most {MAX_DESCRIPTION} characters")),
        Some(_) => Err(format!("{at}: text")),
    }
}

/// A workflow's `tool:` setting, read: none when it has none. A workflow started by a topic or by another's failure
/// reads its events from there, where a call cannot reach: it is not offered as a tool.
pub(crate) fn offered(
    value: Option<&Json>,
    workflow: &str,
    trigger: &crate::model::Trigger,
) -> Result<Option<Tool>, String> {
    use crate::model::Trigger;
    match value {
        None | Some(Json::Null) => Ok(None),
        Some(_) if matches!(trigger, Trigger::Topic { .. } | Trigger::Failed { .. }) => Err(
            "tool: a workflow started by a topic or by another's failure is not offered as a tool: start it by \
             hand, by a webhook or on a schedule"
                .to_owned(),
        ),
        Some(v) => Tool::read(v, workflow).map(Some).map_err(|e| format!("tool: {e}")),
    }
}

impl Tool {
    /// Reads a workflow's `tool:` settings; `workflow` is the workflow's name, the tool's name when it gives none.
    pub(crate) fn read(value: &Json, workflow: &str) -> Result<Self, String> {
        let s = value
            .as_object()
            .ok_or("a set of settings: description, and the input it takes")?;
        if let Some(k) = s
            .keys()
            .find(|k| !matches!(k.as_str(), "name" | "description" | "input" | "timeout"))
        {
            return Err(format!(
                "{k}: not a setting of a tool (name, description, input, timeout)"
            ));
        }
        let name = match s.get("name").and_then(Json::as_str) {
            Some(n) => n.trim().to_owned(),
            None => crate::model::slug(workflow).replace('-', "_"),
        };
        if !tool_name_ok(&name) {
            return Err(format!(
                "name: {name:?} is letters, digits, '_', '-' and '.', 1 to 64 of them"
            ));
        }
        let description = description(s.get("description"), "description")?
            .ok_or("description: what the tool does, in words a model reads to choose it")?;
        let timeout = match s.get("timeout").and_then(Json::as_str) {
            None => TOOL_TIME,
            Some(t) => crate::model::duration(t)
                .filter(|d| !d.is_zero() && *d <= MAX_TOOL_TIME)
                .ok_or("timeout: how long a call waits for its answer, as 30s, at most 5m")?,
        };
        let inputs = match s.get("input") {
            None | Some(Json::Null) => Vec::new(),
            Some(Json::Object(fields)) => {
                if fields.len() > MAX_INPUTS {
                    return Err(format!("input: at most {MAX_INPUTS} of them"));
                }
                fields
                    .iter()
                    .map(|(name, v)| Input::read(name, v).map_err(|e| format!("input.{name}: {e}")))
                    .collect::<Result<_, _>>()?
            }
            Some(_) => {
                return Err("input: the arguments, each by its name: its type and description".to_owned());
            }
        };
        Ok(Self {
            name,
            description,
            inputs,
            timeout,
        })
    }

    /// The JSON Schema of its arguments, as MCP lists a tool's `inputSchema`: an object of the inputs, no others.
    pub fn input_schema(&self) -> Json {
        let mut properties = Map::new();
        for input in &self.inputs {
            let mut p = Map::new();
            p.insert("type".to_owned(), json!(input.kind.schema_type()));
            if let Some(d) = &input.description {
                p.insert("description".to_owned(), json!(d));
            }
            if !input.choices.is_empty() {
                p.insert("enum".to_owned(), Json::Array(input.choices.clone()));
            }
            properties.insert(input.name.clone(), Json::Object(p));
        }
        let required: Vec<&str> = self
            .inputs
            .iter()
            .filter(|i| i.required)
            .map(|i| i.name.as_str())
            .collect();
        let mut schema = json!({ "type": "object", "properties": properties, "additionalProperties": false });
        if !required.is_empty()
            && let Some(fields) = schema.as_object_mut()
        {
            fields.insert("required".to_owned(), json!(required));
        }
        schema
    }

    /// Whether a call's arguments are what the tool takes: a record of its inputs, those required among them, each
    /// of its type (and one of its choices, when it lists them). What is wrong, in words a model can correct.
    pub fn check(&self, arguments: &Json) -> Result<(), String> {
        let none = Map::new();
        let given = match arguments {
            Json::Null => &none,
            Json::Object(m) => m,
            _ => return Err("the arguments are a record of named values".to_owned()),
        };
        if let Some(unknown) = given.keys().find(|k| !self.inputs.iter().any(|i| &i.name == *k)) {
            let known: Vec<&str> = self.inputs.iter().map(|i| i.name.as_str()).collect();
            return Err(if known.is_empty() {
                format!("{unknown}: this tool takes no arguments")
            } else {
                format!("{unknown}: not an argument of this tool ({})", known.join(", "))
            });
        }
        for input in &self.inputs {
            match given.get(&input.name) {
                None | Some(Json::Null) if input.required => {
                    return Err(format!("{}: it is needed", input.name));
                }
                None | Some(Json::Null) => {}
                Some(v) if !input.kind.holds(v) => {
                    return Err(format!(
                        "{}: a value of type {}",
                        input.name,
                        input.kind.schema_type()
                    ));
                }
                Some(v) if !input.choices.is_empty() && !input.choices.contains(v) => {
                    let shown: Vec<String> = input.choices.iter().map(Json::to_string).collect();
                    return Err(format!("{}: one of {}", input.name, shown.join(", ")));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

impl Input {
    fn read(name: &str, value: &Json) -> Result<Self, String> {
        if !name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || name.len() > 64
        {
            return Err("an input's name is letters, digits and '_', starting with a letter".to_owned());
        }
        let types = "string, number, integer, boolean, array or object";
        let (kind, s) = match value {
            Json::String(t) => (
                InputKind::read(t).ok_or_else(|| format!("{t:?} is not a type: {types}"))?,
                Map::new(),
            ),
            Json::Object(s) => {
                if let Some(k) = s.keys().find(|k| {
                    !matches!(
                        k.as_str(),
                        "type" | "description" | "required" | "enum" | "choices"
                    )
                }) {
                    return Err(format!(
                        "{k}: not a setting of an input (type, description, required, choices)"
                    ));
                }
                let kind = match s.get("type").and_then(Json::as_str) {
                    None => InputKind::Text,
                    Some(t) => InputKind::read(t).ok_or_else(|| format!("type: {t:?} is not {types}"))?,
                };
                (kind, s.clone())
            }
            _ => return Err(format!("its type ({types}), or its settings")),
        };
        let choices = match s.get("choices").or_else(|| s.get("enum")) {
            None | Some(Json::Null) => Vec::new(),
            Some(Json::Array(list)) if !list.is_empty() && list.len() <= MAX_CHOICES => {
                if let Some(bad) = list.iter().find(|c| !kind.holds(c)) {
                    return Err(format!("choices: {bad} is not of type {}", kind.schema_type()));
                }
                list.clone()
            }
            Some(_) => return Err(format!("choices: a list of 1 to {MAX_CHOICES} values")),
        };
        Ok(Self {
            name: name.to_owned(),
            kind,
            description: description(s.get("description"), "description")?,
            required: match s.get("required") {
                None | Some(Json::Null) => false,
                Some(Json::Bool(b)) => *b,
                Some(_) => return Err("required: true or false".to_owned()),
            },
            choices,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn tool(json: Json) -> Result<Tool, String> {
        Tool::read(&json, "Price an item")
    }

    #[test]
    fn a_tool_is_named_after_its_workflow_and_its_inputs_make_a_schema() {
        let t = tool(json!({
            "description": "The price of an item",
            "input": {
                "sku": { "type": "string", "description": "The item's code", "required": true },
                "qty": "integer",
                "size": { "choices": ["S", "M", "L"] },
            },
        }))
        .unwrap();
        assert_eq!(t.name, "price_an_item");
        assert_eq!(t.timeout, TOOL_TIME);
        assert_eq!(
            t.input_schema(),
            json!({
                "type": "object",
                "properties": {
                    "sku": { "type": "string", "description": "The item's code" },
                    "qty": { "type": "integer" },
                    "size": { "type": "string", "enum": ["S", "M", "L"] },
                },
                "additionalProperties": false,
                "required": ["sku"],
            })
        );
        assert!(t.check(&json!({ "sku": "TOTE", "qty": 3 })).is_ok());
        assert!(t.check(&json!({ "sku": "TOTE", "size": "M" })).is_ok());
    }

    #[test]
    fn arguments_that_do_not_fit_are_refused_saying_what_to_change() {
        let t = tool(json!({
            "name": "price",
            "description": "The price of an item",
            "timeout": "20s",
            "input": { "sku": { "required": true }, "qty": "integer", "size": { "choices": ["S", "M"] } },
        }))
        .unwrap();
        assert_eq!(t.timeout, Duration::from_secs(20));
        assert_eq!(t.check(&json!({})).unwrap_err(), "sku: it is needed");
        assert_eq!(
            t.check(&json!({ "sku": "A", "qty": 1.5 })).unwrap_err(),
            "qty: a value of type integer"
        );
        assert_eq!(
            t.check(&json!({ "sku": "A", "colour": "red" })).unwrap_err(),
            "colour: not an argument of this tool (sku, qty, size)"
        );
        assert_eq!(
            t.check(&json!({ "sku": "A", "size": "XL" })).unwrap_err(),
            r#"size: one of "S", "M""#
        );
        assert!(t.check(&json!([1])).is_err());
        let none = tool(json!({ "description": "Now" })).unwrap();
        assert!(none.check(&Json::Null).is_ok());
        assert_eq!(
            none.check(&json!({ "x": 1 })).unwrap_err(),
            "x: this tool takes no arguments"
        );
    }

    #[test]
    fn a_tool_without_a_description_or_with_a_bad_setting_is_refused() {
        assert!(tool(json!({})).unwrap_err().starts_with("description:"));
        assert!(
            tool(json!({ "description": "x", "name": "a b" }))
                .unwrap_err()
                .starts_with("name:")
        );
        assert!(
            tool(json!({ "description": "x", "timeout": "1h" }))
                .unwrap_err()
                .starts_with("timeout:")
        );
        assert!(tool(json!({ "description": "x", "color": 1 })).is_err());
        assert!(tool(json!({ "description": "x", "input": { "1x": "string" } })).is_err());
        assert!(tool(json!({ "description": "x", "input": { "a": "date" } })).is_err());
        assert!(
            tool(json!({ "description": "x", "input": { "a": { "type": "integer", "choices": ["one"] } } }))
                .is_err()
        );
        let many: Map<String, Json> = (0..=MAX_INPUTS)
            .map(|i| (format!("a{i}"), json!("string")))
            .collect();
        assert!(tool(json!({ "description": "x", "input": many })).is_err());
        assert!(tool(json!({ "description": "x".repeat(MAX_DESCRIPTION + 1) })).is_err());
    }
}
