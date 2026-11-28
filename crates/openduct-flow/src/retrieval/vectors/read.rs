// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading where a `vector_store` or `ask_documents` step keeps its pieces, when the workflow is read: the store, its
//! credential, the table or collection (its name checked for its store), the distance, and Pinecone's namespace or
//! MongoDB's index - and, with the store known, whether it can express the step's filter.

use serde_json::{Map, Value as Json};

use super::{MAX_NAME, StoreKind, Target, check_filter};
use crate::retrieval::{Distance, table_name};

/// The settings of a target, beside a step's own.
pub(crate) const SETTINGS: &[&str] = &[
    "store",
    "credential",
    "table",
    "collection",
    "distance",
    "namespace",
    "index",
];

fn text<'a>(s: &'a Map<String, Json>, name: &str) -> Option<&'a str> {
    s.get(name)
        .and_then(Json::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

/// A name written for a store not known until the step runs: plain characters only, checked again for its store.
fn plain_name(name: &str, at: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{at}: {name:?} is not a collection's name: letters, digits, _, -, . and :, at most {MAX_NAME}"
        ))
    }
}

/// The step's target, as its settings say.
pub(crate) fn target(s: &Map<String, Json>, at: &str) -> Result<Target, String> {
    let store = match text(s, "store") {
        None => None,
        Some(t) => Some(StoreKind::parse(t).ok_or_else(|| {
            format!(
                "{at}.store: {}, not {t:?}",
                StoreKind::ALL.map(StoreKind::name).join(", ")
            )
        })?),
    };
    let credential = text(s, "credential").map(str::to_owned);
    match (store, &credential) {
        (Some(StoreKind::Memory), Some(_)) => {
            return Err(format!("{at}.credential: the run's memory takes no credential"));
        }
        (Some(StoreKind::Memory), None) => {}
        (_, None) => {
            return Err(format!(
                "{at}.credential: the store's credential (a PostgreSQL database with pgvector, or a vector database)"
            ));
        }
        _ => {}
    }
    let (written, field) = match (text(s, "table"), text(s, "collection")) {
        (Some(_), Some(_)) => return Err(format!("{at}: table or collection, one of the two")),
        (Some(t), None) => (t, "table"),
        (None, Some(c)) => (c, "collection"),
        (None, None) if store == Some(StoreKind::Memory) => ("default", "collection"),
        (None, None) => {
            return Err(format!(
                "{at}.table: the table (or collection) its pieces are kept in, as doc_pieces"
            ));
        }
    };
    let place = format!("{at}.{field}");
    match store {
        Some(kind) => kind.check_name(written, &place)?,
        // A table with no store named is PostgreSQL's unless its credential says otherwise: its name is checked as a
        // table's now - it may be written into SQL - and for its store again when the step runs.
        None if field == "table" => {
            table_name(written, &place)?;
        }
        None => plain_name(written, &place)?,
    }
    let distance = match text(s, "distance").unwrap_or("cosine") {
        "cosine" => Distance::Cosine,
        "l2" => Distance::L2,
        "inner_product" => Distance::InnerProduct,
        other => {
            return Err(format!(
                "{at}.distance: cosine, l2 or inner_product, not {other:?}"
            ));
        }
    };
    let only_for = |name: &str, kind: StoreKind| -> Result<Option<String>, String> {
        let Some(v) = text(s, name) else { return Ok(None) };
        if store.is_some_and(|k| k != kind) {
            return Err(format!("{at}.{name}: a setting of {} only", super::label(kind)));
        }
        if v.len() > MAX_NAME
            || !v
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        {
            return Err(format!(
                "{at}.{name}: letters, digits, _ and -, at most {MAX_NAME}"
            ));
        }
        Ok(Some(v.to_owned()))
    };
    Ok(Target {
        store,
        credential,
        name: written.to_owned(),
        distance,
        namespace: only_for("namespace", StoreKind::Pinecone)?,
        index: only_for("index", StoreKind::Mongodb)?,
    })
}

/// Whether the target's store can express a filter as the step writes it.
pub(crate) fn filter(target: &Target, filter: Option<&Json>, at: &str) -> Result<(), String> {
    match (target.store, filter) {
        (Some(kind), Some(f)) if !matches!(kind, StoreKind::Postgres | StoreKind::Memory) => {
            check_filter(f, kind, at)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn read(j: Json) -> Result<Target, String> {
        target(j.as_object().unwrap(), "steps[0].vector_store")
    }

    #[test]
    fn a_target_is_read_with_its_store_and_its_name_checked_for_it() {
        let t = read(json!({ "store": "qdrant", "credential": "q", "collection": "Docs-1" })).unwrap();
        assert_eq!((t.store, t.name.as_str()), (Some(StoreKind::Qdrant), "Docs-1"));
        let t = read(json!({ "credential": "db", "table": "kb.pieces" })).unwrap();
        assert_eq!(t.store, None, "the credential's kind says, when it runs");
        assert!(
            read(json!({ "credential": "db", "table": "Docs" }))
                .unwrap_err()
                .contains("is not a table name")
        );
        assert!(read(json!({ "credential": "q", "collection": "Docs" })).is_ok());
        let m = read(json!({ "store": "memory" })).unwrap();
        assert_eq!((m.credential, m.name.as_str()), (None, "default"));
        for (bad, says) in [
            (
                json!({ "store": "faiss", "credential": "x", "table": "t" }),
                "store: postgres, qdrant",
            ),
            (
                json!({ "store": "memory", "credential": "x" }),
                "takes no credential",
            ),
            (
                json!({ "store": "qdrant", "collection": "t" }),
                "credential: the store's credential",
            ),
            (
                json!({ "credential": "x", "table": "t", "collection": "c" }),
                "one of the two",
            ),
            (
                json!({ "store": "pinecone", "credential": "x", "collection": "Docs" }),
                "small letters",
            ),
            (
                json!({ "store": "mongodb", "credential": "x", "collection": "pieces" }),
                "database.collection",
            ),
            (
                json!({ "store": "qdrant", "credential": "x", "collection": "c", "namespace": "n" }),
                "of Pinecone only",
            ),
            (
                json!({ "store": "qdrant", "credential": "x", "collection": "c", "distance": "dot" }),
                "cosine, l2",
            ),
            (
                json!({ "credential": "x", "collection": "a b" }),
                "not a collection's name",
            ),
        ] {
            let e = read(bad.clone()).unwrap_err();
            assert!(e.contains(says), "{bad}: {e}");
        }
    }

    #[test]
    fn a_filter_the_store_cannot_express_is_refused_when_read() {
        let t = read(json!({ "store": "chroma", "credential": "c", "collection": "kb1" })).unwrap();
        let e = filter(
            &t,
            Some(&json!({ "author": { "name": "Ann" } })),
            "steps[0].filter",
        )
        .unwrap_err();
        assert!(
            e.starts_with("steps[0].filter: filter.author: Chroma filters only by the top fields"),
            "{e}"
        );
        assert!(filter(&t, Some(&json!({ "team": "{{ value.team }}" })), "f").is_ok());
        let pg = read(json!({ "credential": "db", "table": "t" })).unwrap();
        assert!(filter(&pg, Some(&json!({ "author": { "name": "Ann" } })), "f").is_ok());
    }
}
