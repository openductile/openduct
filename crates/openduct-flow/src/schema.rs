// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! JSON schemas a model's answer must meet: [`check`] says whether a schema is one OpenDuct can hold an answer to
//! (when a workflow is read), and [`validate`] says how an answer misses it, in plain words with where - so a step can
//! ask the model once more, telling it what to put right.
//!
//! The keywords checked: `type` (one or a list), `enum`, `const`, `properties`, `required`, `additionalProperties`,
//! `items`, `minItems`, `maxItems`, `uniqueItems`, `minLength`, `maxLength`, `pattern`, `minimum`, `maximum`,
//! `exclusiveMinimum`, `exclusiveMaximum`, `multipleOf`, `anyOf`, `oneOf`, `allOf`, `nullable`, `format` (`date`,
//! `date-time` and `email` are checked; any other is a note for the model), and `$ref` to the schema's own `$defs`
//! or `definitions`. Words that only describe (`description`, `title`, `examples`, `default`, `x-...`) are notes for
//! the model. Any other keyword is refused when the schema is read, rather than passed over unchecked.

use serde_json::{Map, Value as Json};

/// The largest schema, as JSON.
pub(crate) const MAX_SCHEMA: usize = 64 * 1024;
/// How deep a schema nests, and how deep an answer is followed.
const MAX_DEPTH: usize = 32;
/// The most mismatches said of one answer.
const MAX_PROBLEMS: usize = 20;
/// The most `$ref`s followed one inside another.
const MAX_REFS: usize = 16;
/// The most places of an answer checked against the schema's parts: choices within choices (anyOf of $refs to
/// anyOfs) multiply, and a schema written to make that last for ever stops here.
const MAX_WORK: usize = 100_000;

const CHECKED: &[&str] = &[
    "type",
    "enum",
    "const",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "minItems",
    "maxItems",
    "uniqueItems",
    "minLength",
    "maxLength",
    "pattern",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "anyOf",
    "oneOf",
    "allOf",
    "nullable",
    "format",
    "$ref",
    "$defs",
    "definitions",
];
const NOTES: &[&str] = &[
    "description",
    "title",
    "examples",
    "example",
    "default",
    "$schema",
    "$id",
    "$comment",
    "readOnly",
    "writeOnly",
    "deprecated",
    "propertyOrdering",
    "strict",
];
const TYPES: &[&str] = &[
    "string", "number", "integer", "boolean", "object", "array", "null",
];

/// Whether `schema` is one an answer can be checked against: its keywords known, well formed, and bounded.
pub(crate) fn check(schema: &Json) -> Result<(), String> {
    if schema.to_string().len() > MAX_SCHEMA {
        return Err(format!("a schema of at most {} KiB", MAX_SCHEMA / 1024));
    }
    let mut seen = 0;
    check_at(schema, schema, "$", 0, &mut seen)
}

fn check_at(root: &Json, schema: &Json, at: &str, depth: usize, seen: &mut usize) -> Result<(), String> {
    *seen += 1;
    if depth > MAX_DEPTH || *seen > 5_000 {
        return Err(format!("{at}: a schema nested at most {MAX_DEPTH} deep"));
    }
    let fields = match schema {
        Json::Bool(_) => return Ok(()),
        Json::Object(fields) => fields,
        _ => {
            return Err(format!(
                "{at}: a schema is a record of keywords, as {{\"type\": \"string\"}}"
            ));
        }
    };
    for (key, value) in fields {
        if !CHECKED.contains(&key.as_str()) && !NOTES.contains(&key.as_str()) && !key.starts_with("x-") {
            return Err(format!(
                "{at}: {key:?} is not a keyword OpenDuct checks (it checks {}; and takes description, title, \
                 examples and default as notes)",
                CHECKED.join(", ")
            ));
        }
        let here = format!("{at}.{key}");
        match key.as_str() {
            "type" => {
                let names: Vec<&Json> = match value {
                    Json::Array(list) if !list.is_empty() => list.iter().collect(),
                    Json::String(_) => vec![value],
                    _ => return Err(format!("{here}: a type's name, or a list of them")),
                };
                for name in names {
                    if !name.as_str().is_some_and(|n| TYPES.contains(&n)) {
                        return Err(format!("{here}: {name} is not a type ({})", TYPES.join(", ")));
                    }
                }
            }
            "enum" => {
                let ok = value
                    .as_array()
                    .is_some_and(|l| !l.is_empty() && l.len() <= 1_000);
                if !ok {
                    return Err(format!("{here}: a list of 1 to 1,000 values"));
                }
            }
            "properties" | "$defs" | "definitions" => {
                let Json::Object(inner) = value else {
                    return Err(format!("{here}: names and their schemas"));
                };
                for (name, sub) in inner {
                    check_at(root, sub, &format!("{here}.{name}"), depth + 1, seen)?;
                }
            }
            "required" => {
                if !value.as_array().is_some_and(|l| l.iter().all(Json::is_string)) {
                    return Err(format!("{here}: a list of field names"));
                }
            }
            "additionalProperties" | "items" => check_at(root, value, &here, depth + 1, seen)?,
            "anyOf" | "oneOf" | "allOf" => {
                let Some(list) = value.as_array().filter(|l| !l.is_empty()) else {
                    return Err(format!("{here}: a list of schemas"));
                };
                for (i, sub) in list.iter().enumerate() {
                    check_at(root, sub, &format!("{here}[{i}]"), depth + 1, seen)?;
                }
            }
            "minItems" | "maxItems" | "minLength" | "maxLength" => {
                if value.as_u64().is_none() {
                    return Err(format!("{here}: a whole number, 0 or more"));
                }
            }
            "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" => {
                if !value.is_number() {
                    return Err(format!("{here}: a number"));
                }
            }
            "multipleOf" => {
                if !value.as_f64().is_some_and(|n| n > 0.0) {
                    return Err(format!("{here}: a number above 0"));
                }
            }
            "uniqueItems" | "nullable" => {
                if !value.is_boolean() {
                    return Err(format!("{here}: true or false"));
                }
            }
            "pattern" => {
                let source = value
                    .as_str()
                    .ok_or_else(|| format!("{here}: a regular expression"))?;
                pattern(source).map_err(|e| format!("{here}: {e}"))?;
            }
            "format" => {
                if !value.is_string() {
                    return Err(format!("{here}: a format's name, as date"));
                }
            }
            "$ref" => {
                let target = value.as_str().unwrap_or_default();
                if referred(root, target).is_none() {
                    return Err(format!(
                        "{here}: {target:?} is not one of this schema's own $defs (as #/$defs/address)"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// The schema a `$ref` names: one of the schema's own `$defs` or `definitions`.
fn referred<'a>(root: &'a Json, target: &str) -> Option<&'a Json> {
    if target == "#" {
        return Some(root);
    }
    let rest = target.strip_prefix("#/")?;
    let (group, name) = rest.split_once('/')?;
    if !matches!(group, "$defs" | "definitions") || name.contains('/') {
        return None;
    }
    let name = name.replace("~1", "/").replace("~0", "~");
    root.get(group)?.get(name.as_str())
}

fn pattern(source: &str) -> Result<regex::Regex, String> {
    regex::RegexBuilder::new(source)
        .size_limit(1 << 20)
        .dfa_size_limit(1 << 20)
        .build()
        .map_err(|_| format!("{source:?} is not a regular expression OpenDuct reads"))
}

/// How `value` misses `schema`: nothing when it meets it, else each mismatch with where it is (`$.lines[2].qty`).
pub(crate) fn validate(schema: &Json, value: &Json) -> Result<(), Vec<String>> {
    let mut problems = Vec::new();
    let work = std::cell::Cell::new(0);
    let mut walk = Walk {
        root: schema,
        problems: &mut problems,
        refs: 0,
        work: &work,
        patterns: Vec::new(),
    };
    walk.at(schema, value, "$", 0);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

struct Walk<'a> {
    root: &'a Json,
    problems: &'a mut Vec<String>,
    refs: usize,
    /// The places checked so far, all walks of one answer together.
    work: &'a std::cell::Cell<usize>,
    /// The patterns read so far, each once.
    patterns: Vec<(String, Option<regex::Regex>)>,
}

fn kind_of(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "true or false",
        Json::Number(_) => "a number",
        Json::String(_) => "a text",
        Json::Array(_) => "a list",
        Json::Object(_) => "a record",
    }
}

fn is_type(value: &Json, name: &str) -> bool {
    match name {
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_f64().is_some_and(|n| n.fract() == 0.0) || value.is_i64() || value.is_u64(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => false,
    }
}

fn shown(value: &Json) -> String {
    let text = value.to_string();
    if text.chars().count() > 60 {
        format!("{}...", text.chars().take(57).collect::<String>())
    } else {
        text
    }
}

impl Walk<'_> {
    fn said(&mut self, problem: String) {
        if self.problems.len() < MAX_PROBLEMS {
            self.problems.push(problem);
        }
    }

    /// Whether `text` matches the pattern `source` (one that does not read, which a checked schema has not, matches).
    fn matches(&mut self, source: &str, text: &str) -> bool {
        let at = match self.patterns.iter().position(|(s, _)| s == source) {
            Some(at) => at,
            None => {
                self.patterns.push((source.to_owned(), pattern(source).ok()));
                self.patterns.len() - 1
            }
        };
        self.patterns
            .get(at)
            .and_then(|(_, re)| re.as_ref())
            .is_none_or(|re| re.is_match(text))
    }

    /// Whether `value` meets `schema`, counting nothing against the answer (for anyOf and oneOf).
    fn meets(&mut self, schema: &Json, value: &Json, at: &str, depth: usize) -> bool {
        let mut quiet = Vec::new();
        let mut inner = Walk {
            root: self.root,
            problems: &mut quiet,
            refs: self.refs,
            work: self.work,
            patterns: Vec::new(),
        };
        inner.at(schema, value, at, depth);
        quiet.is_empty()
    }

    #[allow(clippy::too_many_lines)] // one keyword after another, each a few lines
    fn at(&mut self, schema: &Json, value: &Json, at: &str, depth: usize) {
        if depth > MAX_DEPTH {
            self.said(format!("{at}: nested more than {MAX_DEPTH} deep"));
            return;
        }
        self.work.set(self.work.get() + 1);
        if self.work.get() > MAX_WORK {
            self.said("the schema takes too long to check against this answer".to_owned());
            return;
        }
        let fields: &Map<String, Json> = match schema {
            Json::Bool(true) => return,
            Json::Bool(false) => {
                self.said(format!("{at}: nothing is allowed here"));
                return;
            }
            Json::Object(fields) => fields,
            _ => return,
        };
        if let Some(target) = fields.get("$ref").and_then(Json::as_str) {
            self.refs += 1;
            if self.refs > MAX_REFS {
                self.said(format!("{at}: more than {MAX_REFS} $refs one inside another"));
                return;
            }
            if let Some(found) = referred(self.root, target) {
                self.at(found, value, at, depth + 1);
            }
            self.refs -= 1;
        }
        let nullable = fields.get("nullable").and_then(Json::as_bool) == Some(true);
        if value.is_null() && nullable {
            return;
        }
        if let Some(wanted) = fields.get("type") {
            let names: Vec<&str> = match wanted {
                Json::String(s) => vec![s.as_str()],
                Json::Array(list) => list.iter().filter_map(Json::as_str).collect(),
                _ => Vec::new(),
            };
            if !names.is_empty() && !names.iter().any(|n| is_type(value, n)) {
                self.said(format!("{at}: {}, not {}", names.join(" or "), kind_of(value)));
                return;
            }
        }
        if let Some(Json::Array(allowed)) = fields.get("enum")
            && !allowed.contains(value)
        {
            let list: Vec<String> = allowed.iter().take(12).map(shown).collect();
            let more = if allowed.len() > 12 { ", ..." } else { "" };
            self.said(format!(
                "{at}: one of {}{more}, not {}",
                list.join(", "),
                shown(value)
            ));
        }
        if let Some(only) = fields.get("const")
            && only != value
        {
            self.said(format!("{at}: {}, not {}", shown(only), shown(value)));
        }
        for (key, list) in ["allOf", "anyOf", "oneOf"]
            .iter()
            .filter_map(|k| Some((*k, fields.get(*k)?.as_array()?)))
        {
            match key {
                "allOf" => {
                    for sub in list {
                        self.at(sub, value, at, depth + 1);
                    }
                }
                _ => {
                    let met = list
                        .iter()
                        .filter(|sub| self.meets(sub, value, at, depth + 1))
                        .count();
                    if met == 0 {
                        self.said(format!("{at}: {} meets none of the {key} choices", shown(value)));
                    } else if key == "oneOf" && met > 1 {
                        self.said(format!(
                            "{at}: {} meets more than one of the oneOf choices",
                            shown(value)
                        ));
                    }
                }
            }
        }
        match value {
            Json::String(text) => self.text(fields, text, at),
            Json::Number(_) => self.number(fields, value, at),
            Json::Array(items) => {
                let count = items.len() as u64;
                if let Some(least) = fields.get("minItems").and_then(Json::as_u64)
                    && count < least
                {
                    self.said(format!("{at}: at least {least} items, not {count}"));
                }
                if let Some(most) = fields.get("maxItems").and_then(Json::as_u64)
                    && count > most
                {
                    self.said(format!("{at}: at most {most} items, not {count}"));
                }
                if fields.get("uniqueItems").and_then(Json::as_bool) == Some(true) {
                    for (i, item) in items.iter().enumerate() {
                        if items.iter().take(i).any(|before| before == item) {
                            self.said(format!("{at}[{i}]: {} is there twice", shown(item)));
                            break;
                        }
                    }
                }
                if let Some(each) = fields.get("items") {
                    for (i, item) in items.iter().enumerate() {
                        self.at(each, item, &format!("{at}[{i}]"), depth + 1);
                    }
                }
            }
            Json::Object(record) => self.record(fields, record, at, depth),
            Json::Null | Json::Bool(_) => {}
        }
    }

    fn text(&mut self, fields: &Map<String, Json>, text: &str, at: &str) {
        let length = text.chars().count() as u64;
        if let Some(least) = fields.get("minLength").and_then(Json::as_u64)
            && length < least
        {
            self.said(format!("{at}: at least {least} characters, not {length}"));
        }
        if let Some(most) = fields.get("maxLength").and_then(Json::as_u64)
            && length > most
        {
            self.said(format!("{at}: at most {most} characters, not {length}"));
        }
        if let Some(source) = fields.get("pattern").and_then(Json::as_str)
            && !self.matches(source, text)
        {
            self.said(format!(
                "{at}: text that matches {source:?}, not {}",
                shown(&Json::from(text))
            ));
        }
        let format = fields.get("format").and_then(Json::as_str).unwrap_or_default();
        let fits = match format {
            "date" => chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok() && text.len() == 10,
            "date-time" => chrono::DateTime::parse_from_rfc3339(text).is_ok(),
            "email" => text.split_once('@').is_some_and(|(who, place)| {
                !who.is_empty() && place.contains('.') && !text.contains(char::is_whitespace)
            }),
            _ => true,
        };
        if !fits {
            let what = match format {
                "date" => "a date, as 2026-11-27",
                "date-time" => "a date and time, as 2026-11-27T09:30:00Z",
                _ => "an e-mail address",
            };
            self.said(format!("{at}: {what}, not {}", shown(&Json::from(text))));
        }
    }

    fn number(&mut self, fields: &Map<String, Json>, value: &Json, at: &str) {
        let Some(n) = value.as_f64() else { return };
        let bound = |name: &str| fields.get(name).and_then(Json::as_f64);
        if let Some(least) = bound("minimum")
            && n < least
        {
            self.said(format!("{at}: at least {least}, not {value}"));
        }
        if let Some(most) = bound("maximum")
            && n > most
        {
            self.said(format!("{at}: at most {most}, not {value}"));
        }
        if let Some(least) = bound("exclusiveMinimum")
            && n <= least
        {
            self.said(format!("{at}: more than {least}, not {value}"));
        }
        if let Some(most) = bound("exclusiveMaximum")
            && n >= most
        {
            self.said(format!("{at}: less than {most}, not {value}"));
        }
        if let Some(step) = bound("multipleOf").filter(|s| *s > 0.0) {
            let times = n / step;
            if (times - times.round()).abs() > 1e-9 {
                self.said(format!("{at}: a multiple of {step}, not {value}"));
            }
        }
    }

    fn record(&mut self, fields: &Map<String, Json>, record: &Map<String, Json>, at: &str, depth: usize) {
        if let Some(Json::Array(required)) = fields.get("required") {
            for name in required.iter().filter_map(Json::as_str) {
                if !record.contains_key(name) {
                    self.said(format!("{at}: {name} is missing"));
                }
            }
        }
        let properties = fields.get("properties").and_then(Json::as_object);
        for (name, inner) in record {
            let here = format!("{at}.{name}");
            match properties.and_then(|p| p.get(name)) {
                Some(sub) => self.at(sub, inner, &here, depth + 1),
                None => match fields.get("additionalProperties") {
                    Some(Json::Bool(false)) => self.said(format!("{here}: not a field of the schema")),
                    Some(sub @ Json::Object(_)) => self.at(sub, inner, &here, depth + 1),
                    _ => {}
                },
            }
        }
    }
}

/// A record of null for each of a schema's own fields: what a test run, which asks no model, shows for an answer.
pub(crate) fn blank(schema: &Json) -> Json {
    match schema.get("properties").and_then(Json::as_object) {
        Some(properties) => Json::Object(properties.keys().map(|k| (k.clone(), Json::Null)).collect()),
        None => Json::Object(Map::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn problems(schema: &Json, value: &Json) -> Vec<String> {
        validate(schema, value).err().unwrap_or_default()
    }

    #[test]
    fn an_answer_that_meets_its_schema_passes_and_one_that_misses_says_where() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "minLength": 1 },
                "age": { "type": ["integer", "null"], "minimum": 0 },
                "tags": { "type": "array", "items": { "enum": ["a", "b"] }, "uniqueItems": true, "maxItems": 3 },
                "when": { "type": "string", "format": "date" },
                "email": { "type": "string", "format": "email" },
                "code": { "type": "string", "pattern": "^[A-Z]{2}-\\d+$" },
            },
            "required": ["name", "age"],
            "additionalProperties": false,
        });
        check(&schema).unwrap();
        let good = json!({ "name": "Sam", "age": null, "tags": ["a", "b"], "when": "2026-11-27", "email": "sam@x.example", "code": "AB-12" });
        assert_eq!(validate(&schema, &good), Ok(()));
        let bad = json!({ "age": 3.5, "tags": ["a", "c", "a"], "when": "27/11/2026", "email": "sam", "code": "x", "other": 1 });
        let said = problems(&schema, &bad);
        for wanted in [
            "$: name is missing",
            "$.age: integer or null, not a number",
            "$.tags[1]: one of \"a\", \"b\", not \"c\"",
            "$.tags[2]: \"a\" is there twice",
            "$.when: a date, as 2026-11-27, not \"27/11/2026\"",
            "$.email: an e-mail address, not \"sam\"",
            "$.other: not a field of the schema",
        ] {
            assert!(said.iter().any(|p| p == wanted), "{wanted:?} not in {said:#?}");
        }
        assert!(
            said.iter().any(|p| p.starts_with("$.code: text that matches")),
            "{said:#?}"
        );
    }

    #[test]
    fn choices_references_and_bounds() {
        let schema = json!({
            "$defs": { "money": { "type": "object", "properties": { "amount": { "type": "number", "exclusiveMinimum": 0, "multipleOf": 0.01 } }, "required": ["amount"] } },
            "type": "object",
            "properties": {
                "total": { "$ref": "#/$defs/money" },
                "note": { "anyOf": [{ "type": "string" }, { "type": "null" }] },
                "phone": { "type": "string", "nullable": true },
                "score": { "oneOf": [{ "type": "integer" }, { "type": "number", "maximum": 1 }] },
            },
        });
        check(&schema).unwrap();
        assert_eq!(
            validate(
                &schema,
                &json!({ "total": { "amount": 12.5 }, "note": null, "phone": null, "score": 7 })
            ),
            Ok(())
        );
        let said = problems(
            &schema,
            &json!({ "total": { "amount": 0 }, "note": 3, "score": 0.5 }),
        );
        assert!(
            said.contains(&"$.total.amount: more than 0, not 0".to_owned()),
            "{said:#?}"
        );
        assert!(
            said.contains(&"$.note: 3 meets none of the anyOf choices".to_owned()),
            "{said:#?}"
        );
        assert_eq!(
            problems(&schema, &json!({ "score": 1 })),
            ["$.score: 1 meets more than one of the oneOf choices"]
        );
        assert_eq!(
            problems(&schema, &json!({ "total": { "amount": 1.005 } })),
            ["$.total.amount: a multiple of 0.01, not 1.005"]
        );
        // A schema that refers to itself is followed only so far.
        let looping = json!({ "$defs": { "a": { "$ref": "#/$defs/a" } }, "$ref": "#/$defs/a" });
        check(&looping).unwrap();
        assert!(problems(&looping, &json!(1))[0].contains("$refs one inside another"));
        let many = json!({ "type": "array", "items": { "type": "string" } });
        let list: Vec<Json> = (0..100).map(|i| json!(i)).collect();
        assert_eq!(
            problems(&many, &Json::Array(list)).len(),
            MAX_PROBLEMS,
            "a long list of mistakes is cut short"
        );
    }

    #[test]
    fn a_schema_it_cannot_hold_an_answer_to_is_refused_in_plain_words() {
        let refused = |schema: Json| check(&schema).unwrap_err();
        assert!(refused(json!({ "type": "text" })).contains("\"text\" is not a type"));
        assert!(
            refused(json!({ "if": { "type": "string" } }))
                .contains("\"if\" is not a keyword OpenDuct checks")
        );
        assert!(
            refused(json!({ "properties": { "a": { "$ref": "#/$defs/missing" } } }))
                .contains("not one of this schema's own $defs")
        );
        assert!(
            refused(json!({ "$ref": "https://example.com/schema.json" }))
                .contains("not one of this schema's own")
        );
        assert!(refused(json!({ "pattern": "(" })).contains("not a regular expression"));
        assert!(refused(json!({ "enum": [] })).contains("1 to 1,000"));
        assert!(refused(json!("string")).contains("a record of keywords"));
        let mut deep = json!({ "type": "string" });
        for _ in 0..40 {
            deep = json!({ "items": deep });
        }
        assert!(refused(deep).contains("nested at most"));
        let big: Map<String, Json> = (0..3_000)
            .map(|i| (format!("field_{i}"), json!({ "type": "string" })))
            .collect();
        assert!(refused(json!({ "properties": big })).contains("KiB"));
        // Notes for the model, and words of its own, pass.
        check(&json!({ "title": "Lead", "description": "x", "x-ui": 1, "properties": { "a": { "type": "string", "examples": ["b"] } } })).unwrap();
    }

    #[test]
    fn choices_within_choices_stop_rather_than_go_on_for_ever() {
        // Twelve levels of ten choices, each one the next level: 10^12 ways, in a schema of a few hundred bytes.
        let mut defs = Map::new();
        for i in 0..12 {
            let next = json!({ "$ref": format!("#/$defs/d{}", i + 1) });
            defs.insert(format!("d{i}"), json!({ "anyOf": vec![next; 10] }));
        }
        defs.insert("d12".to_owned(), json!({ "type": "string" }));
        let schema = json!({ "$defs": defs, "$ref": "#/$defs/d0" });
        check(&schema).unwrap();
        let started = std::time::Instant::now();
        assert!(!problems(&schema, &json!(1)).is_empty());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(
            validate(&schema, &json!("a")),
            Ok(()),
            "an answer that meets it is found at once"
        );
    }

    #[test]
    fn a_blank_answer_has_each_field_null() {
        assert_eq!(
            blank(&json!({ "properties": { "a": {}, "b": {} } })),
            json!({ "a": null, "b": null })
        );
        assert_eq!(blank(&json!({ "type": "array" })), json!({}));
    }
}
