// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An API description read as JSON - from JSON or YAML text - within bounds: its size, how deep it nests, how many
//! values it holds. YAML is read event by event, so an alias is refused before it could be copied over and over (a
//! few lines of aliases can stand for billions of values).

use serde_json::{Map, Number, Value as Json};
use yaml_rust2::parser::{Event, EventReceiver, Parser};
use yaml_rust2::scanner::TScalarStyle;

/// The largest description read, in bytes.
pub const MAX_SPEC: usize = 32 * 1024 * 1024;
/// The most values (records, lists, texts, numbers) a description may hold.
const MAX_NODES: usize = 3_000_000;
/// The deepest a description may nest.
const MAX_DEPTH: usize = 100;

/// The description in `text`, as JSON.
pub(crate) fn document(text: &str) -> Result<Json, String> {
    if text.len() > MAX_SPEC {
        return Err(format!(
            "the description is larger than {} MB",
            MAX_SPEC / (1024 * 1024)
        ));
    }
    let text = text.trim_start_matches('\u{feff}');
    let json = if text.trim_start().starts_with('{') {
        // serde_json refuses what nests deeper than 128 on its own, before it could exhaust the stack.
        serde_json::from_str(text).map_err(|e| format!("the description is not valid JSON: {e}"))?
    } else {
        yaml(text)?
    };
    bounded(&json)?;
    Ok(json)
}

/// Whether `json` is within the bounds: depth and count, walked without recursion.
fn bounded(json: &Json) -> Result<(), String> {
    let mut stack: Vec<(&Json, usize)> = vec![(json, 0)];
    let mut nodes = 0usize;
    while let Some((node, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_NODES {
            return Err(format!("the description holds more than {MAX_NODES} values"));
        }
        if depth > MAX_DEPTH {
            return Err(format!("the description nests more than {MAX_DEPTH} deep"));
        }
        match node {
            Json::Array(items) => stack.extend(items.iter().map(|i| (i, depth + 1))),
            Json::Object(fields) => stack.extend(fields.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

enum Frame {
    List(Vec<Json>),
    /// A record, and the name of the field whose value comes next (none while a name is awaited).
    Record(Map<String, Json>, Option<String>),
}

/// Builds JSON from a YAML parser's events: the first document only; an alias, a name that is not text, too many
/// values or too deep a nesting stop it (what comes after is passed over).
#[derive(Default)]
struct Builder {
    stack: Vec<Frame>,
    done: Option<Json>,
    nodes: usize,
    failed: Option<String>,
}

impl Builder {
    fn fail(&mut self, why: String) {
        if self.failed.is_none() {
            self.failed = Some(why);
        }
    }

    /// A finished value: into the list or record it is in, or the document itself.
    fn value(&mut self, v: Json) {
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return self.fail(format!("the description holds more than {MAX_NODES} values"));
        }
        match self.stack.last_mut() {
            None => {
                if self.done.is_none() {
                    self.done = Some(v);
                }
            }
            Some(Frame::List(items)) => items.push(v),
            Some(Frame::Record(fields, name)) => match name.take() {
                Some(n) => {
                    fields.insert(n, v);
                }
                None => match v {
                    Json::String(s) => *name = Some(s),
                    Json::Number(n) => *name = Some(n.to_string()),
                    Json::Bool(b) => *name = Some(b.to_string()),
                    Json::Null => *name = Some("null".to_owned()),
                    _ => self.fail("a field's name in the description is not text".to_owned()),
                },
            },
        }
    }

    fn open(&mut self, frame: Frame) {
        if self.stack.len() >= MAX_DEPTH {
            return self.fail(format!("the description nests more than {MAX_DEPTH} deep"));
        }
        self.stack.push(frame);
    }
}

/// A plain scalar's value as YAML 1.2's core schema reads it: null, true or false, a number, or text.
fn plain(text: &str) -> Json {
    match text {
        "" | "~" | "null" | "Null" | "NULL" => return Json::Null,
        "true" | "True" | "TRUE" => return Json::Bool(true),
        "false" | "False" | "FALSE" => return Json::Bool(false),
        _ => {}
    }
    let looks_numeric = text
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || c == '-' || c == '+' || c == '.');
    if looks_numeric {
        if let Ok(i) = text.parse::<i64>() {
            return Json::Number(i.into());
        }
        let decimal = text
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | '+' | '.' | 'e' | 'E'));
        if decimal && let Some(n) = text.parse::<f64>().ok().and_then(Number::from_f64) {
            return Json::Number(n);
        }
    }
    Json::String(text.to_owned())
}

impl EventReceiver for Builder {
    fn on_event(&mut self, ev: Event) {
        if self.failed.is_some() || (self.done.is_some() && self.stack.is_empty()) {
            return;
        }
        match ev {
            Event::Alias(_) => self.fail(
                "the description uses YAML aliases (*name): write it out in full, or give its JSON"
                    .to_owned(),
            ),
            Event::Scalar(text, style, _, tag) => {
                let as_text = style != TScalarStyle::Plain || tag.as_ref().is_some_and(|t| t.suffix == "str");
                self.value(if as_text { Json::String(text) } else { plain(&text) });
            }
            Event::SequenceStart(..) => self.open(Frame::List(Vec::new())),
            Event::MappingStart(..) => self.open(Frame::Record(Map::new(), None)),
            Event::SequenceEnd | Event::MappingEnd => match self.stack.pop() {
                Some(Frame::List(items)) => self.value(Json::Array(items)),
                Some(Frame::Record(fields, _)) => self.value(Json::Object(fields)),
                None => {}
            },
            _ => {}
        }
    }
}

fn yaml(text: &str) -> Result<Json, String> {
    let mut builder = Builder::default();
    Parser::new_from_str(text)
        .load(&mut builder, false)
        .map_err(|e| format!("the description is neither JSON nor valid YAML: {e}"))?;
    if let Some(why) = builder.failed {
        return Err(why);
    }
    builder.done.ok_or_else(|| "the description is empty".to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn json_and_yaml_read_alike() {
        let yaml = "openapi: 3.1.0\ninfo: { title: Shop, version: '1' }\npaths:\n  /a:\n    get:\n      parameters:\n        - { name: n, in: query, schema: { type: integer, maximum: 100 } }\n";
        let from_yaml = document(yaml).unwrap();
        assert_eq!(from_yaml["openapi"], json!("3.1.0"));
        assert_eq!(from_yaml["info"]["version"], json!("1"));
        assert_eq!(
            from_yaml["paths"]["/a"]["get"]["parameters"][0]["schema"]["maximum"],
            json!(100)
        );
        let from_json = document(&serde_json::to_string(&from_yaml).unwrap()).unwrap();
        assert_eq!(from_json, from_yaml);
        // Plain scalars as YAML reads them; quoted ones are text.
        let typed = document("a: true\nb: ~\nc: 1.5\nd: '1.5'\ne: 0.0.1\nf: -3\n").unwrap();
        assert_eq!(
            typed,
            json!({ "a": true, "b": null, "c": 1.5, "d": "1.5", "e": "0.0.1", "f": -3 })
        );
    }

    #[test]
    fn aliases_depth_and_size_are_refused() {
        let laughs = "a: &a [x, x, x]\nb: &b [*a, *a, *a]\nc: [*b, *b, *b]\n";
        assert!(document(laughs).unwrap_err().contains("aliases"));
        let deep = format!("{}1{}", "[".repeat(150), "]".repeat(150));
        assert!(document(&deep).is_err());
        let deep_yaml = (0..120)
            .map(|i| format!("{}a:\n", "  ".repeat(i)))
            .collect::<String>();
        assert!(document(&deep_yaml).unwrap_err().contains("deep"));
        let big = " ".repeat(MAX_SPEC + 1);
        assert!(document(&big).unwrap_err().contains("larger than"));
        assert!(document("a: [1, 2").unwrap_err().contains("YAML"));
        assert!(document("{\"a\": ").unwrap_err().contains("JSON"));
    }
}
