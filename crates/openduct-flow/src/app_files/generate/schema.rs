// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a JSON schema of a description says, as an app needs it: its kind, its fields, the list in it, and a value
//! that fits it (for recorded tests made without an account). `allOf` is merged; of `oneOf` and `anyOf` the first is
//! taken where one value is needed. Every walk is bounded in depth, so a schema that names itself ends.

use serde_json::{Map, Value as Json, json};

use super::spec::resolve;

/// The deepest a schema is followed.
const MAX_DEPTH: usize = 12;
/// The most fields of a record given in a sample answer.
const SAMPLE_FIELDS: usize = 12;
/// The names a list of records goes by in an answer, most likely first.
const LIST_NAMES: &[&str] = &[
    "data",
    "items",
    "results",
    "entries",
    "records",
    "values",
    "list",
    "elements",
    "collection",
    "content",
    "objects",
    "rows",
    "hits",
    "nodes",
];

static NOTHING: Json = Json::Null;

/// What a schema is needed for: what a request sends (its read-only fields left out) or what an answer holds (its
/// write-only ones left out).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Purpose {
    Request,
    Answer,
}

/// The schema with its `$ref` followed and its `allOf` merged into one: properties, required fields and type.
pub(super) fn merged(root: &Json, schema: &Json) -> Json {
    merged_at(root, schema, 0)
}

fn merged_at(root: &Json, schema: &Json, depth: usize) -> Json {
    let Ok(schema) = resolve(root, schema) else {
        return json!({});
    };
    let Some(parts) = schema.get("allOf").and_then(Json::as_array) else {
        return schema.clone();
    };
    if depth >= MAX_DEPTH {
        return json!({});
    }
    let mut out = schema.clone();
    if let Some(fields) = out.as_object_mut() {
        fields.remove("allOf");
    }
    for part in parts {
        let part = merged_at(root, part, depth + 1);
        let (Some(into), Some(from)) = (out.as_object_mut(), part.as_object()) else {
            continue;
        };
        for (k, v) in from {
            match (k.as_str(), into.get_mut(k)) {
                ("properties", Some(Json::Object(have))) => {
                    if let Json::Object(more) = v {
                        for (name, p) in more {
                            have.entry(name.clone()).or_insert_with(|| p.clone());
                        }
                    }
                }
                ("required", Some(Json::Array(have))) => {
                    if let Json::Array(more) = v {
                        for r in more {
                            if !have.contains(r) {
                                have.push(r.clone());
                            }
                        }
                    }
                }
                (_, Some(_)) => {}
                (_, None) => {
                    into.insert(k.clone(), v.clone());
                }
            }
        }
    }
    out
}

/// The schema's type, without `null` (OpenAPI 3.1 writes `type: [string, "null"]`).
fn type_of(schema: &Json) -> Option<&str> {
    match schema.get("type") {
        Some(Json::String(t)) => Some(t.as_str()),
        Some(Json::Array(types)) => types.iter().filter_map(Json::as_str).find(|t| *t != "null"),
        _ => None,
    }
}

/// The first choice of a `oneOf` or `anyOf`, if it is one (a `null` choice passed over).
fn first_choice<'a>(root: &'a Json, schema: &'a Json) -> Option<&'a Json> {
    let choices = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Json::as_array)?;
    choices.iter().find(|c| {
        resolve(root, c)
            .map(|c| type_of(c).is_some() || c.get("type").is_none())
            .unwrap_or(false)
            && resolve(root, c).ok().and_then(|c| c.get("type")) != Some(&json!("null"))
    })
}

/// What kind of value an input of this schema takes: text, number, boolean, list or record.
pub(super) fn kind(root: &Json, schema: &Json) -> &'static str {
    kind_at(root, schema, 0)
}

fn kind_at(root: &Json, schema: &Json, depth: usize) -> &'static str {
    let s = merged(root, schema);
    match type_of(&s) {
        Some("integer" | "number") => "number",
        Some("boolean") => "boolean",
        Some("array") => "list",
        Some("object") => "record",
        Some(_) => "text",
        None if s.get("properties").is_some() => "record",
        None if s.get("items").is_some() => "list",
        None if depth < MAX_DEPTH => first_choice(root, &s).map_or("text", |c| kind_at(root, c, depth + 1)),
        None => "text",
    }
}

/// A record schema's fields: each name, its schema (as written) and whether it is required - those a request may
/// not send, or an answer does not hold, left out.
pub(super) fn properties(root: &Json, schema: &Json, purpose: Purpose) -> Vec<(String, Json, bool)> {
    let s = merged(root, schema);
    let required: Vec<&str> = s
        .get("required")
        .and_then(Json::as_array)
        .map(|r| r.iter().filter_map(Json::as_str).collect())
        .unwrap_or_default();
    let Some(props) = s.get("properties").and_then(Json::as_object) else {
        return Vec::new();
    };
    props
        .iter()
        .filter(|(_, p)| {
            let p = resolve(root, p).unwrap_or(&NOTHING);
            let hidden = match purpose {
                Purpose::Request => "readOnly",
                Purpose::Answer => "writeOnly",
            };
            !p.get(hidden).and_then(Json::as_bool).unwrap_or(false)
        })
        .map(|(name, p)| (name.clone(), p.clone(), required.contains(&name.as_str())))
        .collect()
}

/// The list of records an answer of this schema holds: where it is (no names for the answer itself, else the
/// field it is in) and the schema of each record.
pub(super) fn list_in(root: &Json, schema: &Json) -> Option<(Vec<String>, Json)> {
    let s = merged(root, schema);
    if type_of(&s) == Some("array") || s.get("items").is_some() && s.get("properties").is_none() {
        return Some((Vec::new(), s.get("items").cloned().unwrap_or_else(|| json!({}))));
    }
    let props = properties(root, &s, Purpose::Answer);
    let arrays: Vec<&(String, Json, bool)> =
        props.iter().filter(|(_, p, _)| kind(root, p) == "list").collect();
    let chosen = LIST_NAMES
        .iter()
        .find_map(|n| arrays.iter().find(|(name, _, _)| name.eq_ignore_ascii_case(n)))
        .or(match arrays.as_slice() {
            [only] => Some(only),
            _ => None,
        })?;
    let items = merged(root, &chosen.1)
        .get("items")
        .cloned()
        .unwrap_or_else(|| json!({}));
    Some((vec![chosen.0.clone()], items))
}

/// The first field, at most `depth` records down (never into a list), whose name is one of `names` (compared
/// without case, `_` and `-`), and its schema.
pub(super) fn field_named(
    root: &Json,
    schema: &Json,
    names: &[&str],
    depth: usize,
) -> Option<(Vec<String>, Json)> {
    let squash = |s: &str| s.to_ascii_lowercase().replace(['_', '-'], "");
    let wanted: Vec<String> = names.iter().map(|n| squash(n)).collect();
    let mut level: Vec<(Vec<String>, Json)> = vec![(Vec::new(), schema.clone())];
    for _ in 0..depth {
        let mut next = Vec::new();
        for (path, s) in &level {
            for (name, p, _) in properties(root, s, Purpose::Answer) {
                let mut at = path.clone();
                at.push(name.clone());
                if wanted.contains(&squash(&name)) && kind(root, &p) != "list" {
                    return Some((at, p));
                }
                if kind(root, &p) == "record" {
                    next.push((at, p));
                }
            }
        }
        level = next;
    }
    None
}

/// Whether a text in a description's example is fit to stand in a recorded test: an id or a word, never a
/// sentence, an address of e-mail or a person's name.
fn plain_example(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 64
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
}

/// A value that fits `schema`, for a recorded test: its own example when that is a number or an id, its first
/// choice of an enum, else one made from its type and format. For a request only the required fields of a record
/// are given; for an answer up to a dozen of them.
pub(super) fn sample(root: &Json, schema: &Json, name: &str, purpose: Purpose) -> Json {
    sample_at(root, schema, name, purpose, 0)
}

fn sample_at(root: &Json, schema: &Json, name: &str, purpose: Purpose, depth: usize) -> Json {
    let s = merged(root, schema);
    if depth >= 5 {
        return Json::Null;
    }
    if let Some(c) = s.get("const") {
        return c.clone();
    }
    if let Some(first) = s
        .get("enum")
        .and_then(Json::as_array)
        .and_then(|e| e.iter().find(|v| !v.is_null()))
    {
        return first.clone();
    }
    let example = s
        .get("example")
        .or_else(|| s.get("examples").and_then(Json::as_array).and_then(|e| e.first()));
    match example {
        Some(e @ (Json::Number(_) | Json::Bool(_))) => return e.clone(),
        Some(Json::String(t)) if plain_example(t) && type_of(&s) != Some("array") => return json!(t),
        _ => {}
    }
    let kind = kind(root, &s);
    match kind {
        "number" => {
            let low = s.get("minimum").and_then(Json::as_f64).unwrap_or(1.0).max(1.0);
            let high = s.get("maximum").and_then(Json::as_f64).unwrap_or(f64::MAX);
            let n = low.min(high).ceil();
            json!(n as i64)
        }
        "boolean" => json!(true),
        "list" => {
            let items = s.get("items").unwrap_or(&NOTHING);
            json!([sample_at(root, items, name, purpose, depth + 1)])
        }
        "record" => {
            if s.get("properties").is_none() {
                if let Some(c) = first_choice(root, &s) {
                    return sample_at(root, c, name, purpose, depth + 1);
                }
                return json!({});
            }
            let mut out = Map::new();
            for (field, p, required) in properties(root, &s, purpose) {
                let wanted = match purpose {
                    Purpose::Request => required,
                    Purpose::Answer => out.len() < SAMPLE_FIELDS,
                };
                if wanted {
                    let v = sample_at(root, &p, &field, purpose, depth + 1);
                    if !v.is_null() || purpose == Purpose::Request {
                        out.insert(field, v);
                    }
                }
            }
            Json::Object(out)
        }
        _ => {
            if s.get("type").is_none()
                && let Some(c) = first_choice(root, &s)
            {
                return sample_at(root, c, name, purpose, depth + 1);
            }
            json!(text_sample(&s, name))
        }
    }
}

/// A text that fits a text schema: by its format, else by its name.
fn text_sample(s: &Json, name: &str) -> String {
    let made = match s.get("format").and_then(Json::as_str).unwrap_or_default() {
        "date-time" => "2026-01-15T09:30:00Z".to_owned(),
        "date" => "2026-01-15".to_owned(),
        "time" => "09:30:00".to_owned(),
        "email" | "idn-email" => "ann@example.com".to_owned(),
        "uri" | "url" | "iri" | "uri-reference" => "https://example.com/1".to_owned(),
        "uuid" => "00000000-0000-4000-8000-000000000001".to_owned(),
        "byte" => "aGVsbG8=".to_owned(),
        "hostname" => "example.com".to_owned(),
        "ipv4" => "192.0.2.1".to_owned(),
        "ipv6" => "2001:db8::1".to_owned(),
        "password" => "test-password".to_owned(),
        _ => {
            let lower = name.to_ascii_lowercase();
            if lower == "id" || lower.ends_with("_id") || lower.ends_with("id") && lower.len() > 2 {
                format!("{}_1", super::rank::snake(name).trim_end_matches("_id"))
            } else if lower.contains("email") {
                "ann@example.com".to_owned()
            } else {
                "example".to_owned()
            }
        }
    };
    let min = s
        .get("minLength")
        .and_then(Json::as_u64)
        .and_then(|m| usize::try_from(m).ok())
        .unwrap_or(0)
        .min(64);
    let max = s
        .get("maxLength")
        .and_then(Json::as_u64)
        .and_then(|m| usize::try_from(m).ok())
        .unwrap_or(usize::MAX)
        .max(1);
    let mut out = made;
    while out.len() < min {
        out.push('x');
    }
    out.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    #[test]
    fn all_of_merges_and_kinds_follow_the_type() {
        let root = json!({ "Base": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] } });
        let s = json!({ "allOf": [{ "$ref": "#/Base" }, { "properties": { "n": { "type": ["integer", "null"] } } }] });
        let m = merged(&root, &s);
        assert_eq!(m["properties"].as_object().unwrap().len(), 2);
        assert_eq!(m["required"], json!(["id"]));
        assert_eq!(kind(&root, &m["properties"]["n"]), "number");
        assert_eq!(kind(&root, &json!({ "items": {} })), "list");
        assert_eq!(
            kind(
                &root,
                &json!({ "oneOf": [{ "type": "null" }, { "type": "boolean" }] })
            ),
            "boolean"
        );
        let props = properties(
            &root,
            &json!({ "properties": { "a": { "readOnly": true }, "b": { "writeOnly": true } } }),
            Purpose::Request,
        );
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].0, "b");
    }

    #[test]
    fn a_list_is_found_where_an_answer_keeps_it() {
        let root = json!({});
        let page = json!({ "type": "object", "properties": {
            "has_more": { "type": "boolean" },
            "data": { "type": "array", "items": { "type": "object", "properties": { "id": { "type": "string" } } } },
        } });
        let (path, items) = list_in(&root, &page).unwrap();
        assert_eq!(path, ["data"]);
        assert_eq!(items["properties"]["id"]["type"], json!("string"));
        assert_eq!(
            list_in(&root, &json!({ "type": "array", "items": { "type": "string" } }))
                .unwrap()
                .0
                .len(),
            0
        );
        assert!(
            list_in(
                &root,
                &json!({ "type": "object", "properties": { "a": { "type": "string" } } })
            )
            .is_none()
        );
        let meta = json!({ "properties": { "meta": { "type": "object", "properties": { "next_cursor": { "type": "string" } } } } });
        assert_eq!(
            field_named(&root, &meta, &["nextCursor"], 2).unwrap().0,
            ["meta", "next_cursor"]
        );
        assert!(field_named(&root, &meta, &["nextCursor"], 1).is_none());
    }

    #[test]
    fn samples_fit_their_schemas_and_hold_no_one_s_details() {
        let root = json!({});
        let s = json!({ "type": "object", "required": ["email", "when", "n", "tags"], "properties": {
            "email": { "type": "string", "format": "email", "example": "jane.doe@corp.test" },
            "when": { "type": "string", "format": "date-time" },
            "n": { "type": "integer", "minimum": 5, "maximum": 9 },
            "tags": { "type": "array", "items": { "type": "string", "enum": ["a", "b"] } },
            "note": { "type": "string" },
        } });
        assert_eq!(
            sample(&root, &s, "", Purpose::Request),
            json!({ "email": "ann@example.com", "when": "2026-01-15T09:30:00Z", "n": 5, "tags": ["a"] })
        );
        let answer = sample(&root, &s, "", Purpose::Answer);
        assert_eq!(answer["note"], json!("example"));
        assert_eq!(
            sample(&root, &json!({ "type": "string" }), "folder_id", Purpose::Request),
            json!("folder_1")
        );
        assert_eq!(
            sample(
                &root,
                &json!({ "type": "string", "example": "fld_123" }),
                "x",
                Purpose::Request
            ),
            json!("fld_123")
        );
        // A schema that names itself ends.
        let selfish =
            json!({ "Node": { "type": "object", "properties": { "child": { "$ref": "#/Node" } } } });
        let v = sample(&selfish, &json!({ "$ref": "#/Node" }), "", Purpose::Answer);
        assert!(v.is_object());
    }
}
