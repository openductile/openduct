// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! JSON Schemas, as far as a registry needs them: read, checked, and compared - is every document one schema
//! accepts also accepted by another?
//!
//! That question is what compatibility means for JSON Schema: a new version is backward compatible when it
//! accepts everything the old one did (a consumer on the new schema reads what producers on the old one wrote).
//! It is answered keyword by keyword, the way Confluent's registry answers it:
//!
//! - **Types** may widen (`integer` to `number`, a type added to a list) and not narrow.
//! - **Limits** (`maximum`, `minLength`, `maxItems`, `pattern`, `multipleOf`, ...) may be loosened or dropped, not
//!   tightened or added.
//! - **`enum`** may gain values and not lose them; **`const`** may not change.
//! - **Objects.** A property the old schema did not name: taken when the old schema was closed
//!   (`additionalProperties: false`, so no old document has it), refused when it was open (an old document may
//!   hold anything under that name). A property the new schema no longer names: taken when the new schema is open,
//!   refused when it is closed. A property newly `required` is refused unless it has a `default`.
//!   `additionalProperties` may open and not close.
//! - **`anyOf` and `oneOf`** may gain branches and not lose them; **`allOf`** may lose parts and not gain them.
//! - **`$ref`** within the schema (`#/definitions/...`, `#/$defs/...`) is followed, once round a loop.
//!
//! Where a keyword this does not weigh differs between the two schemas (`patternProperties`, `dependencies`,
//! `if`, `not`, ...), the answer is no: more is refused than must be, and never less. Nothing here panics on any
//! input.

use std::collections::BTreeSet;
use std::fmt;

use serde_json::Value;

const MAX_DEPTH: usize = 64;

/// A JSON Schema read and checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    root: Value,
}

/// Why a text is not a JSON Schema this registry takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid(pub String);

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

const TYPES: [&str; 7] = [
    "null", "boolean", "integer", "number", "string", "array", "object",
];
/// Keywords whose value is one schema.
const ONE: [&str; 9] = [
    "additionalProperties",
    "additionalItems",
    "contains",
    "propertyNames",
    "not",
    "if",
    "then",
    "else",
    "items",
];
/// Keywords whose value is a list of schemas.
const LIST: [&str; 3] = ["allOf", "anyOf", "oneOf"];
/// Keywords whose value is a map of names to schemas.
const MAP: [&str; 4] = ["properties", "patternProperties", "definitions", "$defs"];
/// Keywords that must be a number of zero or more.
const COUNTS: [&str; 6] = [
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "minProperties",
    "maxProperties",
];
/// Keywords this does not weigh: a difference in one is a refusal.
const UNWEIGHED: [&str; 10] = [
    "patternProperties",
    "dependencies",
    "dependentRequired",
    "dependentSchemas",
    "propertyNames",
    "contains",
    "if",
    "then",
    "else",
    "format",
];

fn check(schema: &Value, root: &Value, depth: usize) -> Result<(), Invalid> {
    let bad = |why: String| Err(Invalid(why));
    if depth > MAX_DEPTH {
        return bad(format!("the schema nests deeper than {MAX_DEPTH} levels"));
    }
    let object = match schema {
        Value::Bool(_) => return Ok(()),
        Value::Object(object) => object,
        _ => return bad("a schema is an object, or true or false".to_owned()),
    };
    match object.get("type") {
        None => {}
        Some(Value::String(t)) if TYPES.contains(&t.as_str()) => {}
        Some(Value::Array(list))
            if !list.is_empty()
                && list
                    .iter()
                    .all(|t| t.as_str().is_some_and(|t| TYPES.contains(&t))) => {}
        Some(other) => {
            return bad(format!(
                "\"type\" {other} is not a JSON Schema type, or a list of them"
            ));
        }
    }
    for key in COUNTS {
        if object.get(key).is_some_and(|n| n.as_u64().is_none()) {
            return bad(format!("{key:?} must be a whole number of 0 or more"));
        }
    }
    for key in ["minimum", "maximum", "multipleOf"] {
        if object.get(key).is_some_and(|n| !n.is_number()) {
            return bad(format!("{key:?} must be a number"));
        }
    }
    if object
        .get("multipleOf")
        .and_then(Value::as_f64)
        .is_some_and(|m| m <= 0.0)
    {
        return bad("\"multipleOf\" must be more than 0".to_owned());
    }
    if object
        .get("required")
        .is_some_and(|r| !r.as_array().is_some_and(|r| r.iter().all(Value::is_string)))
    {
        return bad("\"required\" must be a list of property names".to_owned());
    }
    if object.get("enum").is_some_and(|e| !e.is_array()) {
        return bad("\"enum\" must be a list of values".to_owned());
    }
    if object.get("pattern").is_some_and(|p| !p.is_string()) {
        return bad("\"pattern\" must be text".to_owned());
    }
    if let Some(target) = object.get("$ref") {
        let Some(target) = target.as_str() else {
            return bad("\"$ref\" must be text".to_owned());
        };
        if !target.starts_with('#') {
            return bad(format!(
                "\"$ref\" {target:?} points outside this schema, at nothing it lists in its references"
            ));
        }
        if resolve(root, target).is_none() {
            return bad(format!("\"$ref\" {target:?} points at nothing in this schema"));
        }
    }
    for key in ONE {
        match object.get(key) {
            // `items` may also be a list of schemas (a tuple).
            Some(Value::Array(list)) if key == "items" => {
                for item in list {
                    check(item, root, depth + 1)?;
                }
            }
            Some(one) => check(one, root, depth + 1)?,
            None => {}
        }
    }
    for key in LIST {
        match object.get(key) {
            Some(Value::Array(list)) if !list.is_empty() => {
                for part in list {
                    check(part, root, depth + 1)?;
                }
            }
            Some(_) => return bad(format!("{key:?} must be a list of at least one schema")),
            None => {}
        }
    }
    for key in MAP {
        match object.get(key) {
            Some(Value::Object(map)) => {
                for part in map.values() {
                    check(part, root, depth + 1)?;
                }
            }
            Some(_) => return bad(format!("{key:?} must be an object of schemas")),
            None => {}
        }
    }
    Ok(())
}

/// Every `$ref` in `schema` that `to` gives a new target for is pointed there.
fn repoint(schema: &mut Value, to: &dyn Fn(&str) -> Option<String>, depth: usize) {
    if depth > MAX_DEPTH * 2 {
        return;
    }
    match schema {
        Value::Object(map) => {
            if let Some(target) = map.get("$ref").and_then(Value::as_str)
                && let Some(new) = to(target)
            {
                map.insert("$ref".to_owned(), Value::String(new));
            }
            for (key, value) in map.iter_mut() {
                // What an enum, a const, a default or an example holds is data, not schema.
                if !matches!(key.as_str(), "enum" | "const" | "default" | "examples") {
                    repoint(value, to, depth + 1);
                }
            }
        }
        Value::Array(list) => list.iter_mut().for_each(|v| repoint(v, to, depth + 1)),
        _ => {}
    }
}

/// What a `$ref` inside the schema points at (`#`, `#/definitions/Name`, ...).
fn resolve<'a>(root: &'a Value, target: &str) -> Option<&'a Value> {
    let pointer = target.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(root);
    }
    // A JSON pointer, with `%25`-style escapes left as written: names in schemas are plain.
    root.pointer(pointer)
}

impl Schema {
    /// Reads a schema from its JSON text.
    pub fn parse(text: &str) -> Result<Self, Invalid> {
        let root: Value = serde_json::from_str(text).map_err(|e| Invalid(format!("not JSON: {e}")))?;
        Self::from_json(root)
    }

    pub fn from_json(root: Value) -> Result<Self, Invalid> {
        Self::from_json_with(root, &[])
    }

    /// As [`Schema::from_json`], for a schema whose `$ref`s point at other schemas - each given here by the name
    /// the `$ref` uses. They are taken into this one (under `$defs`), so that it stands on its own from then on:
    /// `"$ref": "<name>"` becomes a reference to that place, and `"<name>#/definitions/x"` to the part of it.
    pub fn from_json_with(mut root: Value, others: &[(String, &Schema)]) -> Result<Self, Invalid> {
        if !others.is_empty() {
            let keys: Vec<(String, String)> = others
                .iter()
                .enumerate()
                .map(|(i, (name, _))| (name.clone(), format!("__ref_{i}")))
                .collect();
            // This schema's references to the others, by their new place.
            repoint(
                &mut root,
                &|target| {
                    let (file, part) = target.split_once('#').unwrap_or((target, ""));
                    keys.iter()
                        .find(|(name, _)| name == file)
                        .map(|(_, key)| format!("#/$defs/{key}{part}"))
                },
                0,
            );
            let Some(map) = root.as_object_mut() else {
                return Err(Invalid(
                    "a schema that refers to others must be an object".to_owned(),
                ));
            };
            let defs = map
                .entry("$defs")
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
            let Some(defs) = defs.as_object_mut() else {
                return Err(Invalid("\"$defs\" must be an object of schemas".to_owned()));
            };
            for ((_, other), (_, key)) in others.iter().zip(&keys) {
                // Inside the other schema, "#/x" meant its own root: now it means its new place.
                let mut taken = other.root.clone();
                repoint(
                    &mut taken,
                    &|target| {
                        target
                            .strip_prefix('#')
                            .map(|part| format!("#/$defs/{key}{part}"))
                    },
                    0,
                );
                defs.insert(key.clone(), taken);
            }
        }
        check(&root, &root, 0)?;
        Ok(Self { root })
    }

    /// Whether every document `writer` accepts is accepted by this schema too; if not, the first reason why.
    pub fn can_read(&self, writer: &Schema) -> Result<(), String> {
        Compare {
            old: &writer.root,
            new: &self.root,
            seen: BTreeSet::new(),
        }
        .schemas(&writer.root, &self.root, "the schema", 0)
    }
}

struct Compare<'a> {
    old: &'a Value,
    new: &'a Value,
    /// Pairs of `$ref` targets already being compared.
    seen: BTreeSet<(String, String)>,
}

/// A schema that accepts anything: `true`, or an object with no keyword that limits.
fn anything(schema: &Value) -> bool {
    match schema {
        Value::Bool(open) => *open,
        Value::Object(object) => object.keys().all(|k| {
            matches!(
                k.as_str(),
                "title"
                    | "description"
                    | "default"
                    | "examples"
                    | "$comment"
                    | "$id"
                    | "$schema"
                    | "definitions"
                    | "$defs"
            )
        }),
        _ => false,
    }
}

/// The types a schema takes; `None` when it does not say (any).
fn types(schema: &serde_json::Map<String, Value>) -> Option<BTreeSet<&str>> {
    match schema.get("type")? {
        Value::String(t) => Some(BTreeSet::from([t.as_str()])),
        Value::Array(list) => Some(list.iter().filter_map(Value::as_str).collect()),
        _ => None,
    }
}

fn number(schema: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    schema.get(key).and_then(Value::as_f64)
}

impl<'a> Compare<'a> {
    fn schemas(&mut self, old: &'a Value, new: &'a Value, at: &str, depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH * 2 {
            return Err(format!("{at}: nested too deep to compare"));
        }
        // References, followed - once round a loop.
        let target = |v: &'a Value| v.get("$ref").and_then(Value::as_str);
        if let (Some(o), Some(n)) = (target(old), target(new))
            && !self.seen.insert((o.to_owned(), n.to_owned()))
        {
            return Ok(());
        }
        if let Some(to) = target(old) {
            let old = resolve(self.old, to).ok_or_else(|| format!("{at}: {to} points at nothing"))?;
            return self.schemas(old, new, at, depth + 1);
        }
        if let Some(to) = target(new) {
            let new = resolve(self.new, to).ok_or_else(|| format!("{at}: {to} points at nothing"))?;
            return self.schemas(old, new, at, depth + 1);
        }
        if old == new || anything(new) || old == &Value::Bool(false) {
            return Ok(());
        }
        let (Value::Object(o), Value::Object(n)) = (old, new) else {
            return Err(match new {
                Value::Bool(false) => format!("{at}: the new schema accepts nothing"),
                _ => format!("{at}: the old schema accepted anything, the new one does not"),
            });
        };
        if self.combined(o, n, old, new, at, depth)? {
            return Ok(());
        }
        for key in UNWEIGHED
            .into_iter()
            .chain(["not", "additionalItems", "uniqueItems"])
        {
            let (was, is) = (o.get(key), n.get(key));
            let tighter = match key {
                "uniqueItems" => is == Some(&Value::Bool(true)) && was != Some(&Value::Bool(true)),
                _ => is.is_some() && was != is,
            };
            if tighter {
                return Err(format!("{at}: {key:?} was added or changed"));
            }
        }
        self.kinds(o, n, at)?;
        self.limits(o, n, at)?;
        self.values(o, n, at)?;
        self.items(o, n, at, depth)?;
        self.properties(o, n, at, depth)
    }

    /// `anyOf` and `oneOf` may gain branches; `allOf` may lose parts.
    fn combined(
        &mut self,
        o: &'a serde_json::Map<String, Value>,
        n: &'a serde_json::Map<String, Value>,
        old: &'a Value,
        new: &'a Value,
        at: &str,
        depth: usize,
    ) -> Result<bool, String> {
        let list = |m: &'a serde_json::Map<String, Value>, key: &str| m.get(key).and_then(Value::as_array);
        for key in ["anyOf", "oneOf"] {
            match (list(o, key), list(n, key)) {
                (Some(was), Some(is)) => {
                    for (i, branch) in was.iter().enumerate() {
                        if !self.any(is, |c, candidate| c.schemas(branch, candidate, at, depth + 1)) {
                            return Err(format!(
                                "{at}: no branch of the new {key} accepts what branch {} of the old one did",
                                i + 1
                            ));
                        }
                    }
                }
                // What was one schema is now one of several: taken when a branch accepts all it did.
                (None, Some(is)) => {
                    if !self.any(is, |c, candidate| c.schemas(old, candidate, at, depth + 1)) {
                        return Err(format!(
                            "{at}: no branch of the new {key} accepts what the old schema did"
                        ));
                    }
                    // The branches hold the new schema's conditions: nothing more to weigh here.
                    return Ok(true);
                }
                (Some(_), None) => return Err(format!("{at}: {key:?} was removed")),
                (None, None) => {}
            }
        }
        let was = list(o, "allOf");
        for (i, part) in list(n, "allOf").into_iter().flatten().enumerate() {
            let met = match was {
                Some(was) => self.any(was, |c, candidate| c.schemas(candidate, part, at, depth + 1)),
                None => false,
            };
            if !met {
                return Err(format!(
                    "{at}: part {} of the new allOf is a condition the old schema did not have",
                    i + 1
                ));
            }
        }
        let _ = new;
        Ok(false)
    }

    /// Whether `attempt` succeeds for any candidate, leaving what was seen as it was after each that fails.
    fn any(
        &mut self,
        candidates: &'a [Value],
        mut attempt: impl FnMut(&mut Self, &'a Value) -> Result<(), String>,
    ) -> bool {
        let before = self.seen.clone();
        for candidate in candidates {
            if attempt(self, candidate).is_ok() {
                return true;
            }
            self.seen = before.clone();
        }
        false
    }

    fn kinds(
        &self,
        o: &serde_json::Map<String, Value>,
        n: &serde_json::Map<String, Value>,
        at: &str,
    ) -> Result<(), String> {
        let Some(now) = types(n) else {
            return Ok(());
        };
        let Some(was) = types(o) else {
            return Err(format!(
                "{at}: the old schema took any type, the new one only {now:?}"
            ));
        };
        match was
            .iter()
            .find(|t| !(now.contains(*t) || (**t == "integer" && now.contains("number"))))
        {
            Some(lost) => Err(format!("{at}: the type {lost} is no longer taken")),
            None => Ok(()),
        }
    }

    fn limits(
        &self,
        o: &serde_json::Map<String, Value>,
        n: &serde_json::Map<String, Value>,
        at: &str,
    ) -> Result<(), String> {
        // An upper limit may rise or go; a lower limit may fall or go.
        for (key, upper) in [
            ("maximum", true),
            ("exclusiveMaximum", true),
            ("maxLength", true),
            ("maxItems", true),
            ("maxProperties", true),
            ("minimum", false),
            ("exclusiveMinimum", false),
            ("minLength", false),
            ("minItems", false),
            ("minProperties", false),
        ] {
            let Some(is) = number(n, key) else {
                // A true/false exclusive limit (draft 4) that changed.
                if n.get(key).is_some_and(|v| v.is_boolean()) && o.get(key) != n.get(key) {
                    return Err(format!("{at}: {key:?} changed"));
                }
                continue;
            };
            match number(o, key) {
                Some(was) if (upper && was <= is) || (!upper && was >= is) => {}
                Some(was) => {
                    return Err(format!("{at}: {key} was {was} and is now {is}, which takes less"));
                }
                None if !upper && is <= 0.0 && key != "minimum" && key != "exclusiveMinimum" => {}
                None => return Err(format!("{at}: {key} {is} was added")),
            }
        }
        if let Some(is) = number(n, "multipleOf") {
            match number(o, "multipleOf") {
                Some(was) if ((was / is).round() - was / is).abs() < 1e-9 => {}
                Some(was) => return Err(format!("{at}: multiples of {was} are not all multiples of {is}")),
                None => return Err(format!("{at}: multipleOf {is} was added")),
            }
        }
        match (o.get("pattern"), n.get("pattern")) {
            (was, Some(is)) if was != Some(is) => Err(format!("{at}: the pattern was added or changed")),
            _ => Ok(()),
        }
    }

    fn values(
        &self,
        o: &serde_json::Map<String, Value>,
        n: &serde_json::Map<String, Value>,
        at: &str,
    ) -> Result<(), String> {
        // What the old schema allowed, when it listed it: its enum, or its one const.
        let allowed = |m: &serde_json::Map<String, Value>| -> Option<Vec<Value>> {
            m.get("const")
                .map(|c| vec![c.clone()])
                .or_else(|| m.get("enum").and_then(Value::as_array).cloned())
        };
        let Some(now) = allowed(n) else {
            return Ok(());
        };
        let Some(was) = allowed(o) else {
            return Err(format!("{at}: the values are now limited to a list"));
        };
        match was.iter().find(|v| !now.contains(v)) {
            Some(lost) => Err(format!("{at}: the value {lost} is no longer allowed")),
            None => Ok(()),
        }
    }

    fn items(
        &mut self,
        o: &'a serde_json::Map<String, Value>,
        n: &'a serde_json::Map<String, Value>,
        at: &str,
        depth: usize,
    ) -> Result<(), String> {
        let here = format!("{at}, its items");
        match (o.get("items"), n.get("items")) {
            (_, None) => Ok(()),
            (Some(Value::Array(was)), Some(Value::Array(is))) if was.len() == is.len() => {
                for (i, (was, is)) in was.iter().zip(is).enumerate() {
                    self.schemas(was, is, &format!("{at}, item {}", i + 1), depth + 1)?;
                }
                Ok(())
            }
            (Some(Value::Array(_)), Some(_)) | (Some(_), Some(Value::Array(_))) => {
                Err(format!("{here}: the list of item schemas changed shape"))
            }
            (Some(was), Some(is)) => self.schemas(was, is, &here, depth + 1),
            (None, Some(is)) if anything(is) => Ok(()),
            (None, Some(_)) => Err(format!("{here}: the old schema took any items")),
        }
    }

    fn properties(
        &mut self,
        o: &'a serde_json::Map<String, Value>,
        n: &'a serde_json::Map<String, Value>,
        at: &str,
        depth: usize,
    ) -> Result<(), String> {
        static NONE: std::sync::LazyLock<serde_json::Map<String, Value>> =
            std::sync::LazyLock::new(serde_json::Map::new);
        let named = |m: &'a serde_json::Map<String, Value>| {
            m.get("properties").and_then(Value::as_object).unwrap_or(&NONE)
        };
        let (was, is) = (named(o), named(n));
        // What a schema says of properties it does not name: nothing (any), false (none), or a schema.
        let others = |m: &'a serde_json::Map<String, Value>| m.get("additionalProperties");
        let closed = |rest: Option<&Value>| rest == Some(&Value::Bool(false));
        for (name, new) in is {
            let here = format!("{at}, property {name}");
            match (was.get(name), others(o)) {
                (Some(old), _) => self.schemas(old, new, &here, depth + 1)?,
                (None, rest) if closed(rest) => {}
                (None, _) if anything(new) => {}
                (None, Some(rest)) if rest != &Value::Bool(true) => {
                    self.schemas(rest, new, &here, depth + 1)?
                }
                (None, _) => {
                    return Err(format!(
                        "{here}: added to a schema that took any other property, so old documents may hold \
                         anything under that name"
                    ));
                }
            }
        }
        for (name, old) in was {
            if is.contains_key(name) {
                continue;
            }
            let here = format!("{at}, property {name}");
            match others(n) {
                rest if closed(rest) => {
                    return Err(format!(
                        "{here}: removed from a schema that takes no other property"
                    ));
                }
                Some(rest) if rest != &Value::Bool(true) => self.schemas(old, rest, &here, depth + 1)?,
                _ => {}
            }
        }
        match (others(o), others(n)) {
            (was, Some(is)) if closed(Some(is)) && !closed(was) => {
                return Err(format!("{at}: other properties are no longer taken"));
            }
            (None | Some(Value::Bool(true)), Some(is)) if !anything(is) && !closed(Some(is)) => {
                return Err(format!("{at}: other properties are now held to a schema"));
            }
            (Some(was), Some(is)) if was.is_object() && is.is_object() => {
                self.schemas(was, is, &format!("{at}, its other properties"), depth + 1)?;
            }
            _ => {}
        }
        let required = |m: &'a serde_json::Map<String, Value>| -> Vec<&'a str> {
            m.get("required")
                .and_then(Value::as_array)
                .map(|r| r.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default()
        };
        let before = required(o);
        for name in required(n) {
            let has_default = is.get(name).is_some_and(|p| p.get("default").is_some());
            if !before.contains(&name) && !has_default {
                return Err(format!(
                    "{at}: the property {name} is now required and has no default"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Whether the new schema accepts everything the old one did.
    fn takes(new: Value, old: Value) -> Result<(), String> {
        Schema::from_json(new)
            .unwrap()
            .can_read(&Schema::from_json(old).unwrap())
    }

    fn refused(new: Value, old: Value, why: &str) {
        let e = takes(new.clone(), old.clone()).expect_err(&format!("{old} -> {new} should be refused"));
        assert!(e.contains(why), "{old} -> {new}: {e}");
    }

    #[test]
    fn schemas_are_read_and_what_is_not_one_is_refused_with_the_reason() {
        assert!(
            Schema::parse(r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#)
                .is_ok()
        );
        assert!(Schema::parse("true").is_ok());
        assert!(
            Schema::parse(r##"{"$ref":"#/definitions/a","definitions":{"a":{"type":"string"}}}"##).is_ok()
        );
        for (bad, why) in [
            ("{", "not JSON"),
            ("7", "an object, or true or false"),
            (r#"{"type":"text"}"#, "not a JSON Schema type"),
            (r#"{"type":[]}"#, "not a JSON Schema type"),
            (r#"{"minLength":-1}"#, "whole number"),
            (r#"{"maximum":"ten"}"#, "must be a number"),
            (r#"{"multipleOf":0}"#, "more than 0"),
            (r#"{"required":"id"}"#, "list of property names"),
            (r#"{"properties":[]}"#, "object of schemas"),
            (r#"{"anyOf":[]}"#, "at least one schema"),
            (
                r#"{"properties":{"a":{"type":"nothing"}}}"#,
                "not a JSON Schema type",
            ),
            (r##"{"$ref":"#/definitions/missing"}"##, "points at nothing"),
            (
                r#"{"$ref":"other.json#/a"}"#,
                "nothing it lists in its references",
            ),
        ] {
            let e = Schema::parse(bad).unwrap_err();
            assert!(e.0.contains(why), "{bad}: {e}");
        }
        let deep = format!("{}true{}", r#"{"items":"#.repeat(100), "}".repeat(100));
        assert!(Schema::parse(&deep).unwrap_err().0.contains("deeper"));
    }

    #[test]
    fn a_schema_may_point_at_others_it_refers_to() {
        let money = |amount: &str| {
            Schema::from_json(json!({
                "definitions": {"amount": {"type": amount}},
                "type": "object",
                "properties": {"amount": {"$ref": "#/definitions/amount"}, "currency": {"type": "string"}},
                "additionalProperties": false
            }))
            .unwrap()
        };
        let order = json!({
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "total": {"$ref": "money.json"},
                "tip": {"$ref": "money.json#/definitions/amount"}
            },
            "additionalProperties": false
        });
        assert!(
            Schema::from_json(order.clone())
                .unwrap_err()
                .0
                .contains("references")
        );
        let (wide, narrow) = (money("number"), money("integer"));
        let new = Schema::from_json_with(order.clone(), &[("money.json".to_owned(), &wide)]).unwrap();
        let old = Schema::from_json_with(order.clone(), &[("money.json".to_owned(), &narrow)]).unwrap();
        // Compared through what they point at, and through the references inside that.
        assert_eq!(new.can_read(&old), Ok(()));
        let why = old.can_read(&new).unwrap_err();
        assert!(
            why.contains("property total, property amount") || why.contains("property tip"),
            "{why}"
        );
        // A schema that points at one that points at another.
        let basket = json!({"type": "array", "items": {"$ref": "order.json"}});
        assert!(Schema::from_json_with(basket, &[("order.json".to_owned(), &new)]).is_ok());
        // Text that looks like a reference inside data is left alone.
        let data = json!({"enum": [{"$ref": "money.json"}], "$defs": {}});
        assert!(Schema::from_json_with(data, &[("money.json".to_owned(), &wide)]).is_ok());
    }

    #[test]
    fn types_and_limits_may_loosen_and_not_tighten() {
        assert_eq!(
            takes(json!({"type": "number"}), json!({"type": "integer"})),
            Ok(())
        );
        assert_eq!(
            takes(json!({"type": ["string", "null"]}), json!({"type": "string"})),
            Ok(())
        );
        assert_eq!(takes(json!({}), json!({"type": "string"})), Ok(()));
        refused(
            json!({"type": "integer"}),
            json!({"type": "number"}),
            "number is no longer taken",
        );
        refused(
            json!({"type": "string"}),
            json!({"type": ["string", "null"]}),
            "null is no longer taken",
        );
        refused(json!({"type": "string"}), json!({}), "took any type");

        assert_eq!(
            takes(
                json!({"type": "integer", "maximum": 20}),
                json!({"type": "integer", "maximum": 10})
            ),
            Ok(())
        );
        assert_eq!(
            takes(
                json!({"type": "integer"}),
                json!({"type": "integer", "maximum": 10})
            ),
            Ok(())
        );
        refused(
            json!({"type": "integer", "maximum": 5}),
            json!({"type": "integer", "maximum": 10}),
            "takes less",
        );
        refused(
            json!({"type": "integer", "maximum": 5}),
            json!({"type": "integer"}),
            "maximum 5 was added",
        );
        refused(
            json!({"type": "string", "minLength": 3}),
            json!({"type": "string", "minLength": 1}),
            "takes less",
        );
        assert_eq!(
            takes(
                json!({"type": "string", "minLength": 0}),
                json!({"type": "string"})
            ),
            Ok(())
        );
        refused(
            json!({"type": "string", "pattern": "^a"}),
            json!({"type": "string"}),
            "pattern",
        );
        assert_eq!(takes(json!({"multipleOf": 2}), json!({"multipleOf": 4})), Ok(()));
        refused(
            json!({"multipleOf": 4}),
            json!({"multipleOf": 2}),
            "not all multiples of 4",
        );
        refused(
            json!({"type": "array", "uniqueItems": true}),
            json!({"type": "array"}),
            "uniqueItems",
        );
        refused(json!({"format": "date"}), json!({}), "format");
    }

    #[test]
    fn listed_values_may_grow_and_not_shrink() {
        assert_eq!(
            takes(json!({"enum": ["a", "b", "c"]}), json!({"enum": ["a", "b"]})),
            Ok(())
        );
        refused(
            json!({"enum": ["a"]}),
            json!({"enum": ["a", "b"]}),
            "\"b\" is no longer allowed",
        );
        refused(
            json!({"enum": ["a"]}),
            json!({"type": "string"}),
            "limited to a list",
        );
        assert_eq!(takes(json!({"enum": ["a", "b"]}), json!({"const": "a"})), Ok(()));
        refused(json!({"const": "b"}), json!({"const": "a"}), "no longer allowed");
    }

    #[test]
    fn a_property_added_or_removed_depends_on_whether_the_schema_was_open() {
        let object = |properties: Value, extra: Value| {
            let mut o = json!({"type": "object", "properties": properties});
            o.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            o
        };
        let id = json!({"id": {"type": "integer"}});
        let id_and_note = json!({"id": {"type": "integer"}, "note": {"type": "string"}});
        let closed = json!({"additionalProperties": false});
        // Added to an open schema: an old document may hold anything as "note".
        refused(
            object(id_and_note.clone(), json!({})),
            object(id.clone(), json!({})),
            "property note: added",
        );
        // Added to a closed one: no old document has it.
        assert_eq!(
            takes(
                object(id_and_note.clone(), closed.clone()),
                object(id.clone(), closed.clone())
            ),
            Ok(())
        );
        // Removed from an open schema: still taken, as any other property. From a closed one: refused.
        assert_eq!(
            takes(
                object(id.clone(), json!({})),
                object(id_and_note.clone(), json!({}))
            ),
            Ok(())
        );
        refused(
            object(id.clone(), closed.clone()),
            object(id_and_note.clone(), closed.clone()),
            "property note: removed",
        );
        // A schema may open and not close.
        assert_eq!(
            takes(object(id.clone(), json!({})), object(id.clone(), closed.clone())),
            Ok(())
        );
        refused(
            object(id.clone(), closed.clone()),
            object(id.clone(), json!({})),
            "no longer taken",
        );
        // A property's own schema follows the same rules, and says where.
        refused(
            object(json!({"id": {"type": "string"}}), json!({})),
            object(id.clone(), json!({})),
            "property id: the type integer",
        );
        // Newly required: refused, unless it has a default. No longer required: fine.
        let required = json!({"required": ["id"]});
        refused(
            object(id.clone(), required.clone()),
            object(id.clone(), json!({})),
            "now required",
        );
        assert_eq!(
            takes(
                object(json!({"id": {"type": "integer", "default": 0}}), required.clone()),
                object(id.clone(), json!({}))
            ),
            Ok(())
        );
        assert_eq!(
            takes(object(id.clone(), json!({})), object(id.clone(), required)),
            Ok(())
        );
    }

    #[test]
    fn arrays_combinations_and_references() {
        let list = |items: Value| json!({"type": "array", "items": items});
        assert_eq!(
            takes(list(json!({"type": "number"})), list(json!({"type": "integer"}))),
            Ok(())
        );
        refused(
            list(json!({"type": "integer"})),
            list(json!({"type": "number"})),
            "its items",
        );
        refused(
            list(json!({"type": "integer"})),
            json!({"type": "array"}),
            "took any items",
        );
        // One of several may gain a branch and not lose one.
        let text = json!({"type": "string"});
        let whole = json!({"type": "integer"});
        assert_eq!(
            takes(json!({"oneOf": [text, whole]}), json!({"oneOf": [text]})),
            Ok(())
        );
        refused(
            json!({"oneOf": [text]}),
            json!({"oneOf": [text, whole]}),
            "branch 2",
        );
        assert_eq!(takes(json!({"anyOf": [text, whole]}), text.clone()), Ok(()));
        refused(text.clone(), json!({"anyOf": [text, whole]}), "anyOf");
        // All of several may lose a part and not gain one.
        let short = json!({"maxLength": 5});
        assert_eq!(
            takes(json!({"allOf": [text]}), json!({"allOf": [text, short]})),
            Ok(())
        );
        refused(
            json!({"allOf": [text, short]}),
            json!({"allOf": [text]}),
            "part 2",
        );
        // References are followed, a schema that refers to itself once.
        let tree = |value: &str| {
            json!({
                "$ref": "#/definitions/node",
                "definitions": {"node": {
                    "type": "object",
                    "properties": {"value": {"type": value}, "next": {"$ref": "#/definitions/node"}},
                    "additionalProperties": false
                }}
            })
        };
        assert_eq!(takes(tree("number"), tree("integer")), Ok(()));
        refused(tree("integer"), tree("number"), "property value");
        // true and false.
        assert_eq!(takes(json!(true), json!({"type": "string"})), Ok(()));
        refused(json!(false), json!({"type": "string"}), "accepts nothing");
        assert_eq!(takes(json!({"type": "string"}), json!(false)), Ok(()));
    }
}
