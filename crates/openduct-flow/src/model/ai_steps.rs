// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the AI steps built on the `ai` step: classify, sentiment, extract, summarise, and an `ai` step with a
//! `schema` (see [`crate::ai_steps`]).

use serde_json::{Map, Value as Json};

use super::{Kind, Reader, text};
use crate::ai_steps::{AiStep, Label, MAX_FIELDS, MAX_LABELS, Task};

/// The settings every one of them takes: the model's, as the `ai` step has them, and more instructions.
const MODEL: &[&str] = &[
    "credential",
    "model",
    "temperature",
    "top_p",
    "max_tokens",
    "instructions",
];
/// The longest name of a category, a label or a field, and the longest description of one.
const MAX_NAME: usize = 100;
const MAX_DESCRIPTION: usize = 2_000;
/// The characters of a piece of a long text to summarise: by default, and the fewest and most a step may set.
const PIECE: usize = 12_000;
const PIECES: std::ops::RangeInclusive<usize> = 1_000..=100_000;

fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

fn flag(s: &Map<String, Json>, name: &str, at: &str, default: bool) -> Result<bool, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

fn description(value: Option<&Json>, at: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(Json::Null) => Ok(None),
        Some(Json::String(d)) if d.chars().count() <= MAX_DESCRIPTION => Ok(Some(d.clone())),
        Some(_) => Err(format!(
            "{at}.description: a text of at most {MAX_DESCRIPTION} characters"
        )),
    }
}

impl Reader {
    pub(super) fn ai_step(&mut self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        if kind == "ai" {
            return self.ai_with_schema(s, at);
        }
        let own: &[&str] = match kind {
            "classify" => &["text", "categories", "several", "other", "default"],
            "sentiment" => &["text", "labels"],
            "extract" => &["text", "fields", "schema"],
            _ => &["text", "length", "style", "piece_size"],
        };
        if let Some(k) = s
            .keys()
            .find(|k| !MODEL.contains(&k.as_str()) && !own.contains(&k.as_str()))
        {
            return Err(format!(
                "{at}.{k}: not a setting of {kind} ({}, {})",
                own.join(", "),
                MODEL.join(", ")
            ));
        }
        let credential = text(s.get("credential"))
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| format!("{at}.credential: the model's credential (an ai one)"))?
            .to_owned();
        let mut fields = Vec::new();
        for name in ["model", "temperature", "top_p", "max_tokens"] {
            if let Some(v) = s.get(name) {
                fields.push((name.to_owned(), self.tree(v, &format!("{at}.{name}"))?));
            }
        }
        let given = s
            .get("text")
            .ok_or_else(|| format!("{at}.text: what to work on, as {{{{ value.text }}}}"))?;
        let text = Some(self.tree(given, &format!("{at}.text"))?);
        let instructions = s
            .get("instructions")
            .map(|v| self.template(v, &format!("{at}.instructions")))
            .transpose()?;
        let task = match kind {
            "classify" => self.classify(s, at)?,
            "sentiment" => Task::Sentiment {
                labels: match s.get("labels") {
                    None | Some(Json::Null) => crate::ai_steps::plain_labels(),
                    given => self.labels(given, &format!("{at}.labels"), 2, None)?,
                },
            },
            "extract" => extract(s, at)?,
            _ => summarise(s, at)?,
        };
        Ok(Kind::Ai(Box::new(AiStep {
            task,
            credential,
            fields,
            text,
            instructions,
        })))
    }

    /// An `ai` step with a `schema`: the `ai` step's own settings, and the schema its answer must meet.
    fn ai_with_schema(&mut self, s: &Map<String, Json>, at: &str) -> Result<Kind, String> {
        let mut rest = s.clone();
        let schema = rest.remove("schema").unwrap_or(Json::Null);
        crate::schema::check(&schema).map_err(|e| format!("{at}.schema: {e}"))?;
        let spec = crate::apps::app("ai").ok_or_else(|| format!("{at}: no ai step"))?;
        let app = self.app(spec, &rest, at)?;
        Ok(Kind::Ai(Box::new(AiStep {
            task: Task::Ask { schema },
            credential: app.credential,
            fields: app.fields,
            text: None,
            instructions: None,
        })))
    }

    fn classify(&mut self, s: &Map<String, Json>, at: &str) -> Result<Task, String> {
        let several = flag(s, "several", at, false)?;
        let other = flag(s, "other", at, true)?;
        if !other && s.contains_key("default") {
            return Err(format!(
                "{at}.default: the steps for other - which other: false leaves out"
            ));
        }
        // `other` is the fallback's name, kept for it even when the model must choose.
        let reserved = Some(crate::ai_steps::OTHER);
        let labels = self.labels(s.get("categories"), &format!("{at}.categories"), 1, reserved)?;
        let fallback = if other {
            Some(self.steps(s.get("default"), &format!("{at}.default"))?)
        } else {
            None
        };
        Ok(Task::Classify {
            labels,
            several,
            other: fallback,
        })
    }

    /// Categories or labels: each a name, or `{name, description, steps}`.
    fn labels(
        &mut self,
        list: Option<&Json>,
        at: &str,
        least: usize,
        reserved: Option<&str>,
    ) -> Result<Vec<Label>, String> {
        let Some(Json::Array(items)) = list else {
            return Err(format!(
                "{at}: a list, each a name or {{name, description, steps}}"
            ));
        };
        if items.len() < least || items.len() > MAX_LABELS {
            return Err(format!("{at}: {least} to {MAX_LABELS} of them"));
        }
        let mut labels: Vec<Label> = Vec::with_capacity(items.len());
        for (i, item) in items.iter().enumerate() {
            let place = format!("{at}[{i}]");
            let label = match item {
                Json::String(name) => Label {
                    name: name.clone(),
                    description: None,
                    steps: Vec::new(),
                },
                Json::Object(o) => {
                    if let Some(k) = o
                        .keys()
                        .find(|k| !matches!(k.as_str(), "name" | "description" | "steps"))
                    {
                        return Err(format!(
                            "{place}.{k}: not one of its settings (name, description, steps)"
                        ));
                    }
                    Label {
                        name: text(o.get("name")).unwrap_or_default().to_owned(),
                        description: description(o.get("description"), &place)?,
                        steps: self.steps(o.get("steps"), &format!("{place}.steps"))?,
                    }
                }
                _ => return Err(format!("{place}: a name, or {{name, description, steps}}")),
            };
            let name = label.name.trim();
            if name.is_empty() || name.chars().count() > MAX_NAME || name != label.name {
                return Err(format!(
                    "{place}.name: a name of 1 to {MAX_NAME} characters, with no spaces around it"
                ));
            }
            if reserved.is_some_and(|r| r.eq_ignore_ascii_case(name)) {
                return Err(format!(
                    "{place}.name: {name:?} is kept for the fallback: its steps go in default"
                ));
            }
            if labels.iter().any(|l| l.name.eq_ignore_ascii_case(name)) {
                return Err(format!("{place}.name: {name:?} is there twice"));
            }
            labels.push(label);
        }
        Ok(labels)
    }
}

fn extract(s: &Map<String, Json>, at: &str) -> Result<Task, String> {
    match (s.get("fields"), s.get("schema")) {
        (Some(_), Some(_)) => Err(format!("{at}: fields or a schema, not both")),
        (None, None) => Err(format!(
            "{at}.fields: the fields to pull out, each {{name, type, description}} (or a JSON schema, as schema)"
        )),
        (None, Some(schema)) => {
            crate::schema::check(schema).map_err(|e| format!("{at}.schema: {e}"))?;
            Ok(Task::Extract {
                schema: schema.clone(),
                simple: false,
            })
        }
        (Some(list), None) => {
            let Json::Array(items) = list else {
                return Err(format!(
                    "{at}.fields: a list, each {{name, type, description, required}}"
                ));
            };
            if items.is_empty() || items.len() > MAX_FIELDS {
                return Err(format!("{at}.fields: 1 to {MAX_FIELDS} fields"));
            }
            let mut fields: Vec<crate::ai_steps::Field> = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let place = format!("{at}.fields[{i}]");
                let field = field(item, &place)?;
                if fields.iter().any(|f| f.name == field.name) {
                    return Err(format!("{place}.name: {:?} is there twice", field.name));
                }
                fields.push(field);
            }
            Ok(Task::Extract {
                schema: crate::ai_steps::schema_of(&fields),
                simple: true,
            })
        }
    }
}

fn field(item: &Json, place: &str) -> Result<crate::ai_steps::Field, String> {
    let (name, kind, about, required) = match item {
        Json::String(name) => (name.clone(), "string".to_owned(), None, false),
        Json::Object(o) => {
            if let Some(k) = o
                .keys()
                .find(|k| !matches!(k.as_str(), "name" | "type" | "description" | "required"))
            {
                return Err(format!(
                    "{place}.{k}: not one of its settings (name, type, description, required)"
                ));
            }
            (
                text(o.get("name")).unwrap_or_default().to_owned(),
                text(o.get("type")).unwrap_or("string").to_owned(),
                description(o.get("description"), place)?,
                flag(o, "required", place, false)?,
            )
        }
        _ => return Err(format!("{place}: a field's name, or {{name, type, description}}")),
    };
    let good = name.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_alphanumeric() || c == '_')
        && name.chars().count() <= 64;
    if !good {
        return Err(format!(
            "{place}.name: letters, digits and '_', starting with a letter, at most 64"
        ));
    }
    if !crate::ai_steps::FIELD_TYPES.contains(&kind.as_str()) {
        return Err(format!(
            "{place}.type: {}, not {kind:?}",
            crate::ai_steps::FIELD_TYPES.join(", ")
        ));
    }
    Ok(crate::ai_steps::Field {
        name,
        kind,
        description: about,
        required,
    })
}

fn summarise(s: &Map<String, Json>, at: &str) -> Result<Task, String> {
    let words = |name: &str| -> Result<Option<String>, String> {
        match s.get(name) {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(t)) if t.chars().count() <= 200 => Ok(Some(t.clone())),
            Some(_) => Err(format!("{at}.{name}: a few words, as short or 3 sentences")),
        }
    };
    let piece = match s.get("piece_size") {
        None | Some(Json::Null) => PIECE,
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| PIECES.contains(n))
            .ok_or_else(|| {
                format!(
                    "{at}.piece_size: the characters of each piece of a long text, {} to {}",
                    PIECES.start(),
                    PIECES.end()
                )
            })?,
    };
    Ok(Task::Summarise {
        length: words("length")?,
        style: words("style")?,
        piece,
    })
}
