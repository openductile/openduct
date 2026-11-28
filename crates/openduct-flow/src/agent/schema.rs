// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The part of JSON Schema an agent's step uses, for the arguments of its tools and for an answer asked as JSON: a
//! schema read when the workflow is (so a mistake is said before a run), and a value checked against it.
//!
//! Checked: `type` (one, or a list), `enum`, `const`, `properties`, `required`, `additionalProperties` (true or
//! false), `items`, `anyOf`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems`, `maxItems`. Read and passed
//! to the model, but not checked: `title`, `description`, `default`, `examples`, `format`, `pattern`, `$schema`.

use serde_json::{Map, Value as Json};

/// The largest schema, in bytes as JSON.
pub(crate) const MAX_SCHEMA: usize = 16 * 1024;
/// How deep a schema nests.
const MAX_DEPTH: usize = 16;
/// The most problems one check says.
const MAX_PROBLEMS: usize = 10;

const TYPES: &[&str] = &[
    "string", "number", "integer", "boolean", "array", "object", "null",
];
const CHECKED: &[&str] = &[
    "type",
    "enum",
    "const",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "anyOf",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
];
const NOTED: &[&str] = &[
    "title",
    "description",
    "default",
    "examples",
    "format",
    "pattern",
    "$schema",
];

/// Reads a schema: an error says where it is wrong.
pub(crate) fn check(schema: &Json, at: &str) -> Result<(), String> {
    if schema.to_string().len() > MAX_SCHEMA {
        return Err(format!("{at}: a schema of at most {} KiB", MAX_SCHEMA / 1024));
    }
    check_at(schema, at, 0)
}

fn check_at(schema: &Json, at: &str, depth: usize) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err(format!("{at}: a schema nested at most {MAX_DEPTH} deep"));
    }
    let Json::Object(s) = schema else {
        return Err(format!("{at}: a schema is a record, as {{type: string}}"));
    };
    for key in s.keys() {
        if !CHECKED.contains(&key.as_str()) && !NOTED.contains(&key.as_str()) {
            return Err(format!(
                "{at}.{key}: not a schema keyword OpenDuct reads ({})",
                CHECKED.join(", ")
            ));
        }
    }
    match s.get("type") {
        None => {}
        Some(Json::String(t)) if TYPES.contains(&t.as_str()) => {}
        Some(Json::Array(list))
            if !list.is_empty()
                && list
                    .iter()
                    .all(|t| t.as_str().is_some_and(|t| TYPES.contains(&t))) => {}
        Some(_) => {
            return Err(format!(
                "{at}.type: one of {}, or a list of them",
                TYPES.join(", ")
            ));
        }
    }
    if let Some(e) = s.get("enum")
        && !e.as_array().is_some_and(|l| !l.is_empty())
    {
        return Err(format!("{at}.enum: a list of the values allowed"));
    }
    match s.get("properties") {
        None => {}
        Some(Json::Object(p)) => {
            for (name, inner) in p {
                check_at(inner, &format!("{at}.properties.{name}"), depth + 1)?;
            }
        }
        Some(_) => return Err(format!("{at}.properties: each field's name and its schema")),
    }
    if let Some(r) = s.get("required")
        && !r.as_array().is_some_and(|l| l.iter().all(Json::is_string))
    {
        return Err(format!("{at}.required: a list of field names"));
    }
    if let Some(a) = s.get("additionalProperties")
        && !a.is_boolean()
    {
        return Err(format!("{at}.additionalProperties: true or false"));
    }
    if let Some(items) = s.get("items") {
        check_at(items, &format!("{at}.items"), depth + 1)?;
    }
    if let Some(any) = s.get("anyOf") {
        let Some(list) = any.as_array().filter(|l| !l.is_empty()) else {
            return Err(format!("{at}.anyOf: a list of schemas"));
        };
        for (i, inner) in list.iter().enumerate() {
            check_at(inner, &format!("{at}.anyOf[{i}]"), depth + 1)?;
        }
    }
    for bound in ["minimum", "maximum"] {
        if s.get(bound).is_some_and(|b| !b.is_number()) {
            return Err(format!("{at}.{bound}: a number"));
        }
    }
    for count in ["minLength", "maxLength", "minItems", "maxItems"] {
        if s.get(count).is_some_and(|c| c.as_u64().is_none()) {
            return Err(format!("{at}.{count}: a whole number"));
        }
    }
    Ok(())
}

/// What is wrong with `value` against `schema` (nothing when it matches), each with where: at most a few.
pub(crate) fn problems(value: &Json, schema: &Json) -> Vec<String> {
    let mut out = Vec::new();
    walk(value, schema, "", &mut out);
    out.truncate(MAX_PROBLEMS);
    out
}

fn type_name(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

/// A word with its article: a string, an object.
fn a(word: &str) -> String {
    if word.starts_with(['a', 'e', 'i', 'o', 'u']) {
        format!("an {word}")
    } else {
        format!("a {word}")
    }
}

fn is_type(value: &Json, t: &str) -> bool {
    match t {
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value.as_f64().is_some_and(|f| f.fract() == 0.0)
        }
        "number" => value.is_number(),
        other => type_name(value) == other,
    }
}

fn walk(value: &Json, schema: &Json, at: &str, out: &mut Vec<String>) {
    if out.len() >= MAX_PROBLEMS {
        return;
    }
    let Json::Object(s) = schema else { return };
    let here = if at.is_empty() { "the answer" } else { at };
    let types: Vec<&str> = match s.get("type") {
        Some(Json::String(t)) => vec![t.as_str()],
        Some(Json::Array(list)) => list.iter().filter_map(Json::as_str).collect(),
        _ => Vec::new(),
    };
    if !types.is_empty() && !types.iter().any(|t| is_type(value, t)) {
        out.push(format!(
            "{here}: {}, not {}",
            a(&types.join(" or ")),
            a(type_name(value))
        ));
        return;
    }
    if let Some(Json::Array(allowed)) = s.get("enum")
        && !allowed.contains(value)
    {
        out.push(format!("{here}: one of {}", Json::Array(allowed.clone())));
    }
    if let Some(wanted) = s.get("const")
        && wanted != value
    {
        out.push(format!("{here}: {wanted}"));
    }
    if let Some(Json::Array(any)) = s.get("anyOf")
        && !any.iter().any(|inner| problems(value, inner).is_empty())
    {
        out.push(format!("{here}: none of the shapes it may have"));
    }
    match value {
        Json::Object(fields) => object(fields, s, at, out),
        Json::Array(items) => {
            let n = items.len() as u64;
            bounds(n, s, "minItems", "maxItems", "items", here, out);
            if let Some(inner) = s.get("items") {
                for (i, item) in items.iter().enumerate() {
                    walk(item, inner, &format!("{at}[{i}]"), out);
                }
            }
        }
        Json::String(text) => {
            let n = text.chars().count() as u64;
            bounds(n, s, "minLength", "maxLength", "characters", here, out);
        }
        Json::Number(n) => {
            let n = n.as_f64().unwrap_or(0.0);
            if let Some(min) = s.get("minimum").and_then(Json::as_f64)
                && n < min
            {
                out.push(format!("{here}: at least {min}"));
            }
            if let Some(max) = s.get("maximum").and_then(Json::as_f64)
                && n > max
            {
                out.push(format!("{here}: at most {max}"));
            }
        }
        _ => {}
    }
}

fn bounds(
    n: u64,
    s: &Map<String, Json>,
    least: &str,
    most: &str,
    what: &str,
    here: &str,
    out: &mut Vec<String>,
) {
    if let Some(min) = s.get(least).and_then(Json::as_u64)
        && n < min
    {
        out.push(format!("{here}: at least {min} {what}"));
    }
    if let Some(max) = s.get(most).and_then(Json::as_u64)
        && n > max
    {
        out.push(format!("{here}: at most {max} {what}"));
    }
}

fn object(fields: &Map<String, Json>, s: &Map<String, Json>, at: &str, out: &mut Vec<String>) {
    let field_at = |name: &str| {
        if at.is_empty() {
            name.to_owned()
        } else {
            format!("{at}.{name}")
        }
    };
    if let Some(Json::Array(required)) = s.get("required") {
        for name in required.iter().filter_map(Json::as_str) {
            if !fields.contains_key(name) {
                out.push(format!("{}: missing", field_at(name)));
            }
        }
    }
    let properties = s.get("properties").and_then(Json::as_object);
    for (name, value) in fields {
        match properties.and_then(|p| p.get(name)) {
            Some(inner) => walk(value, inner, &field_at(name), out),
            None if s.get("additionalProperties") == Some(&Json::Bool(false)) => {
                out.push(format!("{}: not a field it has", field_at(name)));
            }
            None => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_schema_is_read_and_a_mistake_in_it_said_where() {
        let good = json!({
            "type": "object",
            "description": "an order",
            "properties": {
                "id": { "type": "string", "minLength": 1 },
                "lines": { "type": "array", "items": { "type": "object", "properties": { "qty": { "type": "integer" } } } },
                "rush": { "type": ["boolean", "null"] },
                "size": { "enum": ["s", "m", "l"] },
            },
            "required": ["id"],
            "additionalProperties": false,
        });
        check(&good, "output.schema").unwrap();
        let e = check(&json!({ "type": "text" }), "p").unwrap_err();
        assert!(e.starts_with("p.type: one of string"), "{e}");
        let e = check(&json!({ "properties": { "a": { "oneOf": [] } } }), "p").unwrap_err();
        assert!(e.starts_with("p.properties.a.oneOf: not a schema keyword"), "{e}");
        assert!(
            check(&json!({ "required": "id" }), "p")
                .unwrap_err()
                .contains("p.required")
        );
        assert!(check(&json!("string"), "p").unwrap_err().contains("a record"));
        let mut deep = json!({ "type": "string" });
        for _ in 0..20 {
            deep = json!({ "items": deep });
        }
        assert!(check(&deep, "p").unwrap_err().contains("nested at most"));
    }

    #[test]
    fn a_value_is_checked_against_its_schema_with_where_each_problem_is() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": { "type": "string", "minLength": 2 },
                "qty": { "type": "integer", "minimum": 1, "maximum": 10 },
                "tags": { "type": "array", "items": { "type": "string" }, "maxItems": 2 },
                "size": { "enum": ["s", "m"] },
            },
            "required": ["id", "qty"],
            "additionalProperties": false,
        });
        assert!(problems(&json!({ "id": "A-1", "qty": 3, "tags": ["x"] }), &schema).is_empty());
        assert!(
            problems(&json!({ "id": "A-1", "qty": 3.0 }), &schema).is_empty(),
            "3.0 is a whole number"
        );
        let said = problems(
            &json!({ "id": "A", "qty": 11, "tags": ["x", 2, "z"], "size": "xl", "extra": 1 }),
            &schema,
        );
        assert_eq!(
            said,
            vec![
                "id: at least 2 characters",
                "qty: at most 10",
                "tags: at most 2 items",
                "tags[1]: a string, not a number",
                "size: one of [\"s\",\"m\"]",
                "extra: not a field it has",
            ]
        );
        assert_eq!(problems(&json!({ "qty": 1 }), &schema), vec!["id: missing"]);
        assert_eq!(
            problems(&json!([1]), &schema),
            vec!["the answer: an object, not an array"]
        );
        let any = json!({ "anyOf": [{ "type": "string" }, { "type": "number" }] });
        assert!(problems(&json!(5), &any).is_empty());
        assert_eq!(
            problems(&json!(true), &any),
            vec!["the answer: none of the shapes it may have"]
        );
    }
}
