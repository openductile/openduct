// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A metadata filter, written once for every store: a record whose every field must hold. A field given text, a
//! number, or true or false is equal to it; a field given a list holds every value of the list (it is a list
//! itself, as a piece's `tags`); a field given a record is a field inside a field, on the stores whose metadata keeps
//! its shape (PostgreSQL, Qdrant, Milvus, Redis, MongoDB, the run's memory). PostgreSQL reads it as pgvector's `@>`,
//! which is this rule.
//!
//! Each store's own filter language is made from the conditions here ([`conditions`]); a filter one cannot express -
//! a field inside a field on Weaviate, Chroma or Pinecone, a list of numbers on Pinecone, a name a store does not take
//! - is refused, with why, when the workflow is read ([`check`], on the filter as written) or else when it runs.

use serde_json::Value as Json;

use super::{StoreKind, label};

/// The most conditions one filter makes, and how deep its records go.
const MAX_CONDITIONS: usize = 64;
const MAX_DEPTH: usize = 6;

/// A value a condition compares with.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Scalar {
    Text(String),
    Number(f64),
    Bool(bool),
}

impl Scalar {
    pub(crate) fn json(&self) -> Json {
        match self {
            Self::Text(t) => Json::String(t.clone()),
            Self::Number(n) => number_json(*n),
            Self::Bool(b) => Json::Bool(*b),
        }
    }

    /// The value as one text, the same for equal values: `"sales"`, `2020`, `true`.
    pub(crate) fn canonical(&self) -> String {
        self.json().to_string()
    }

    fn of(value: &Json) -> Option<Self> {
        match value {
            Json::String(s) => Some(Self::Text(s.clone())),
            Json::Number(n) => n.as_f64().filter(|f| f.is_finite()).map(Self::Number),
            Json::Bool(b) => Some(Self::Bool(*b)),
            _ => None,
        }
    }

    fn kind(&self) -> u8 {
        match self {
            Self::Text(_) => 0,
            Self::Number(_) => 1,
            Self::Bool(_) => 2,
        }
    }
}

/// A whole number as a whole number (`2020`, not `2020.0`); any other as it is.
pub(crate) fn number_json(n: f64) -> Json {
    if n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 {
        // A whole number within the range an f64 holds exactly, checked.
        #[allow(clippy::cast_possible_truncation)]
        let whole = n as i64;
        Json::from(whole)
    } else {
        serde_json::Number::from_f64(n).map_or(Json::Null, Json::Number)
    }
}

/// One condition: the field (its path, from the metadata's top), and what it holds.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cond {
    /// The field is equal to the value (a list field: holds it).
    Is(Vec<String>, Scalar),
    /// The field, a list, holds every one of the values (each of one type).
    HoldsAll(Vec<String>, Vec<Scalar>),
}

impl Cond {
    pub(crate) fn path(&self) -> &[String] {
        match self {
            Self::Is(p, _) | Self::HoldsAll(p, _) => p,
        }
    }
}

/// The conditions of a filter (a record), as `kind` can be sent them; or why it cannot express it.
pub(crate) fn conditions(filter: &Json, kind: StoreKind) -> Result<Vec<Cond>, String> {
    let mut out = Vec::new();
    walk(filter, kind, &mut Vec::new(), &mut out)?;
    Ok(out)
}

/// Whether `kind` can express the filter as a workflow writes it (`{{ }}` texts stand for values worked out when it
/// runs); an expression is checked when it runs instead.
pub(crate) fn check(filter: &Json, kind: StoreKind, at: &str) -> Result<(), String> {
    if !filter.is_object() {
        return Ok(());
    }
    walk(filter, kind, &mut Vec::new(), &mut Vec::new()).map_err(|e| format!("{at}: {e}"))
}

fn walk(value: &Json, kind: StoreKind, path: &mut Vec<String>, out: &mut Vec<Cond>) -> Result<(), String> {
    let Json::Object(fields) = value else {
        return Err("a filter is a record of fields, as {team: sales}".to_owned());
    };
    if path.len() >= MAX_DEPTH {
        return Err(format!("a filter goes at most {MAX_DEPTH} records deep"));
    }
    for (name, v) in fields {
        let at = if path.is_empty() {
            name.clone()
        } else {
            format!("{}.{name}", path.join("."))
        };
        if path.is_empty() || kind.nested() {
            kind.field_ok(name).map_err(|e| format!("filter: {e}"))?;
        }
        path.push(name.clone());
        match v {
            Json::Object(_) if !kind.nested() => {
                return Err(format!(
                    "filter.{at}: {} filters only by the top fields of a piece's metadata, not by a field inside one",
                    label(kind)
                ));
            }
            Json::Object(_) => walk(v, kind, path, out)?,
            Json::Array(items) => {
                if items.is_empty() {
                    return Err(format!("filter.{at}: a list with at least one value"));
                }
                let values = items
                    .iter()
                    .map(|i| {
                        Scalar::of(i)
                            .ok_or_else(|| format!("filter.{at}: a list of texts, numbers or true and false"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let one_kind = values
                    .windows(2)
                    .all(|w| w.first().map(Scalar::kind) == w.get(1).map(Scalar::kind));
                if !one_kind && !matches!(kind, StoreKind::Postgres | StoreKind::Memory) {
                    return Err(format!(
                        "filter.{at}: a list of one kind of value (all texts, or all numbers)"
                    ));
                }
                let texts = values.iter().all(|s| matches!(s, Scalar::Text(_)));
                if !kind.lists_of_any() && !texts {
                    return Err(format!("filter.{at}: {} lists hold text only", label(kind)));
                }
                out.push(Cond::HoldsAll(path.clone(), values));
            }
            Json::Null => return Err(format!("filter.{at}: a value to be equal to, not nothing")),
            other => {
                let s = Scalar::of(other)
                    .ok_or_else(|| format!("filter.{at}: not a value a filter compares with"))?;
                out.push(Cond::Is(path.clone(), s));
            }
        }
        path.pop();
        if out.len() > MAX_CONDITIONS {
            return Err(format!("a filter of at most {MAX_CONDITIONS} conditions"));
        }
    }
    Ok(())
}

/// Whether `metadata` holds `filter` as pgvector's `@>` reads it: every field of a record there and holding its
/// value; every value of a list in the list; anything else equal (numbers by their value).
pub(crate) fn holds(metadata: &Json, filter: &Json) -> bool {
    match (metadata, filter) {
        (Json::Object(m), Json::Object(f)) => {
            f.iter().all(|(k, v)| m.get(k).is_some_and(|have| holds(have, v)))
        }
        (Json::Array(have), Json::Array(want)) => want.iter().all(|w| have.iter().any(|h| holds(h, w))),
        // A list holds a value it has.
        (Json::Array(have), w) => have.iter().any(|h| holds(h, w)),
        (Json::Number(a), Json::Number(b)) => a.as_f64() == b.as_f64(),
        (a, b) => a == b,
    }
}

/// The text tokens a store that filters by one list of tokens (Redis, MongoDB) keeps for a piece's metadata, and a
/// filter's conditions ask for: `team="sales"`, `year=2020`, `author.name="Ann"`, one for each value of a list.
pub(crate) fn tokens(metadata: &Json) -> Vec<String> {
    let mut out = Vec::new();
    token_walk(metadata, &mut Vec::new(), &mut out);
    out.sort();
    out.dedup();
    out
}

fn token_walk(value: &Json, path: &mut Vec<String>, out: &mut Vec<String>) {
    if out.len() >= 1_000 || path.len() > MAX_DEPTH {
        return;
    }
    match value {
        Json::Object(fields) => {
            for (k, v) in fields {
                path.push(k.clone());
                token_walk(v, path, out);
                path.pop();
            }
        }
        Json::Array(items) if !path.is_empty() => {
            for item in items {
                if let Some(s) = Scalar::of(item) {
                    out.push(token(path, &s));
                }
            }
        }
        other => {
            if let (Some(s), false) = (Scalar::of(other), path.is_empty()) {
                out.push(token(path, &s));
            }
        }
    }
}

/// One token: a field's path and a value.
pub(crate) fn token(path: &[String], value: &Scalar) -> String {
    format!("{}={}", path.join("."), value.canonical())
}

/// The tokens a filter's conditions ask for, every one.
pub(crate) fn wanted_tokens(conditions: &[Cond]) -> Vec<String> {
    let mut out = Vec::new();
    for c in conditions {
        match c {
            Cond::Is(p, s) => out.push(token(p, s)),
            Cond::HoldsAll(p, values) => out.extend(values.iter().map(|s| token(p, s))),
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_filter_becomes_conditions_on_the_fields_it_names() {
        let f = json!({ "team": "sales", "year": 2020, "tags": ["faq", "top"], "author": { "name": "Ann" } });
        let c = conditions(&f, StoreKind::Qdrant).unwrap();
        assert_eq!(
            c,
            vec![
                Cond::Is(vec!["team".into()], Scalar::Text("sales".into())),
                Cond::Is(vec!["year".into()], Scalar::Number(2020.0)),
                Cond::HoldsAll(
                    vec!["tags".into()],
                    vec![Scalar::Text("faq".into()), Scalar::Text("top".into())]
                ),
                Cond::Is(vec!["author".into(), "name".into()], Scalar::Text("Ann".into())),
            ]
        );
    }

    #[test]
    fn a_filter_a_store_cannot_express_is_refused_with_why() {
        let nested = json!({ "author": { "name": "Ann" } });
        for kind in [StoreKind::Weaviate, StoreKind::Chroma, StoreKind::Pinecone] {
            let e = check(&nested, kind, "steps[0].filter").unwrap_err();
            assert!(e.contains("not by a field inside one"), "{kind:?}: {e}");
        }
        assert!(check(&nested, StoreKind::Mongodb, "f").is_ok());
        let numbers = json!({ "years": [2020, 2021] });
        assert!(
            check(&numbers, StoreKind::Pinecone, "f")
                .unwrap_err()
                .contains("lists hold text only")
        );
        assert!(check(&numbers, StoreKind::Chroma, "f").is_ok());
        assert!(check(&json!({ "a": null }), StoreKind::Qdrant, "f").is_err());
        assert!(check(&json!({ "a": [] }), StoreKind::Qdrant, "f").is_err());
        assert!(check(&json!({ "bad name": 1 }), StoreKind::Weaviate, "f").is_err());
        assert!(
            check(&json!({ "piece_source": "x" }), StoreKind::Chroma, "f")
                .unwrap_err()
                .contains("store's own")
        );
        assert!(check(&json!({ "a": [1, "x"] }), StoreKind::Qdrant, "f").is_err());
        assert!(check(&json!({ "a": [1, "x"] }), StoreKind::Postgres, "f").is_ok());
    }

    #[test]
    fn containment_is_read_as_pgvector_reads_it() {
        let m =
            json!({ "team": "sales", "tags": ["faq", "top"], "author": { "name": "Ann", "id": 7 }, "n": 2 });
        assert!(holds(&m, &json!({ "team": "sales" })));
        assert!(holds(&m, &json!({ "tags": ["top"] })));
        assert!(holds(&m, &json!({ "tags": "faq" })));
        assert!(holds(&m, &json!({ "author": { "name": "Ann" } })));
        assert!(holds(&m, &json!({ "n": 2.0 })));
        assert!(!holds(&m, &json!({ "tags": ["top", "new"] })));
        assert!(!holds(&m, &json!({ "team": "it" })));
    }

    #[test]
    fn metadata_and_a_filter_make_the_same_tokens() {
        let m =
            json!({ "team": "sales", "year": 2020.0, "tags": ["faq", "top"], "author": { "name": "Ann" } });
        assert_eq!(
            tokens(&m),
            [
                "author.name=\"Ann\"",
                "tags=\"faq\"",
                "tags=\"top\"",
                "team=\"sales\"",
                "year=2020"
            ]
        );
        let c = conditions(&json!({ "year": 2020, "tags": ["top"] }), StoreKind::Redis).unwrap();
        assert_eq!(wanted_tokens(&c), ["tags=\"top\"", "year=2020"]);
    }
}
