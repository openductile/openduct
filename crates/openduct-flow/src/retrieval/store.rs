// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The work of a `vector_store` step - pieces added (their table or collection made first when asked), the nearest
//! found, a document's pieces deleted - and the search `ask_documents` shares, the same on every store ([`Backend`]).
//! Each request goes through the run's effects: a test run shows it, a preview runs what reads and shows what writes.

use std::collections::HashMap;
use std::time::Instant;

use openduct_expr::{Env, Value};
use serde_json::{Value as Json, json};

use super::api::{self, Vectors};
use super::pg::number;
use super::vectors::{Backend, Got, Hit, Piece, RunVectors, StoreKind, Target, conditions};
use super::{
    Embedder, MAX_DIMENSIONS, MAX_PIECE, MAX_PIECES, Search, Store, StoreAction, as_json, credential_of,
    merged, millis, shown_result,
};
use crate::run::Effects;

/// The longest search text, document name, piece id; the largest metadata and filter (as JSON).
const MAX_QUERY: usize = 8 * 1024;
const MAX_SOURCE: usize = 1_000;
const MAX_ID: usize = 512;
const MAX_METADATA: usize = 16 * 1024;
/// The most documents one delete names.
const MAX_SOURCES: usize = 1_000;

/// One piece found.
#[derive(Debug, Clone)]
pub(super) struct Match {
    pub(super) id: String,
    pub(super) source: String,
    pub(super) position: Json,
    pub(super) text: String,
    pub(super) metadata: Json,
    pub(super) distance: f64,
    pub(super) score: f64,
    /// The vector's score, when a rerank model's replaced it.
    pub(super) vector_score: Option<f64>,
}

impl Match {
    pub(super) fn to_json(&self) -> Json {
        let mut out = json!({
            "id": self.id, "source": self.source, "position": self.position, "text": self.text,
            "metadata": self.metadata, "score": self.score, "distance": self.distance,
        });
        if let (Some(v), Some(fields)) = (self.vector_score, out.as_object_mut()) {
            fields.insert("vector_score".to_owned(), json!(v));
        }
        out
    }
}

/// What a search found, or - in a test run - what it would have asked.
#[derive(Debug)]
pub(super) enum Found {
    Matches(Vec<Match>, serde_json::Map<String, Json>),
    Shown(Json),
}

/// The fields every result names its store by: the table or collection, and - beside PostgreSQL - the store.
fn place(target: &Target, kind: StoreKind) -> serde_json::Map<String, Json> {
    let mut out = serde_json::Map::new();
    out.insert("table".to_owned(), json!(target.shown_name()));
    if kind != StoreKind::Postgres {
        out.insert("store".to_owned(), json!(kind.name()));
    }
    out
}

fn with(mut fields: Json, more: &serde_json::Map<String, Json>) -> Json {
    if let Some(o) = fields.as_object_mut() {
        for (k, v) in more {
            o.insert(k.clone(), v.clone());
        }
    }
    fields
}

/// The pieces nearest a query - its text made a vector by `embed`, or a vector given - with their scores; reranked
/// when the search says, cut to its top-k and its least score.
pub(super) async fn find(
    backend: &Backend<'_>,
    kind: StoreKind,
    target: &Target,
    embed: Option<&Embedder>,
    search: &Search,
    effects: &dyn Effects,
    env: &Env<'_>,
) -> Result<Found, String> {
    let mut metrics = serde_json::Map::new();
    let filter = match search.filter.as_ref().map(|f| f.value(env)).transpose()? {
        None | Some(Value::Null) => None,
        Some(Value::Record(r)) if r.is_empty() => None,
        Some(r @ Value::Record(_)) => Some(as_json(&r)),
        Some(other) => return Err(format!("filter: a record of fields, not {}", other.describe())),
    };
    if filter
        .as_ref()
        .is_some_and(|f| f.to_string().len() > MAX_METADATA)
    {
        return Err(format!("filter: at most {} KiB", MAX_METADATA / 1024));
    }
    // A filter worked out by an expression is checked here, before anything is asked: one the store cannot express
    // is refused as the workflow's check refuses a written one.
    if let (Some(f), false) = (&filter, matches!(kind, StoreKind::Postgres | StoreKind::Memory)) {
        conditions(f, kind).map_err(|e| format!("filter: {e}"))?;
    }
    let query = match &search.query {
        Some(t) => {
            let text = t.render(env).map_err(|e| format!("query: {e}"))?;
            if text.trim().is_empty() {
                return Err("query: the text to search for is empty".to_owned());
            }
            if text.len() > MAX_QUERY {
                return Err(format!("query: at most {} KiB of text", MAX_QUERY / 1024));
            }
            Some(text)
        }
        None => None,
    };
    let k = search.rerank.as_ref().map_or(search.top_k, |r| r.candidates);
    let statements = backend.statements(false, filter.is_some());
    let vector = match (&search.vector, &query, embed) {
        (Some(expr), _, _) => {
            let given = expr.evaluate(env).map_err(|e| format!("vector: {e}"))?;
            api::numbers(&given)
                .ok_or_else(|| format!("vector: a list of numbers, not {}", given.describe()))?
        }
        (None, Some(text), Some(embedder)) => {
            let credential = credential_of(effects, &embedder.credential, "ai", "embed_with")?;
            let (made, ms) =
                api::embed(effects, embedder, &credential, env, std::slice::from_ref(text)).await?;
            metrics.insert("embed_ms".to_owned(), json!(ms));
            match made {
                Vectors::Shown(request) => {
                    let fields = with(json!({ "matches": [] }), &place(target, kind));
                    return Ok(Found::Shown(shown_result(&request, &statements, fields)));
                }
                Vectors::Made(mut v) => v.pop().ok_or("the model gave no vector")?,
            }
        }
        _ => return Err("a search needs a query (and embed_with) or a vector".to_owned()),
    };
    if vector.is_empty() || vector.len() > MAX_DIMENSIONS as usize {
        return Err(format!(
            "the vector has {} numbers: 1 to {MAX_DIMENSIONS}",
            vector.len()
        ));
    }
    let started = Instant::now();
    let hits: Vec<Hit> = match backend.search(&vector, k, filter.as_ref()).await? {
        Got::Shown(shown) => {
            let fields = with(json!({ "matches": [] }), &place(target, kind));
            return Ok(Found::Shown(merged(shown, &statements, fields)));
        }
        Got::Done(hits) => hits,
    };
    metrics.insert("search_ms".to_owned(), json!(millis(started)));
    let mut matches: Vec<Match> = hits
        .into_iter()
        .map(|h| Match {
            id: h.id,
            source: h.source,
            position: h.position,
            text: h.text,
            metadata: h.metadata,
            score: target.distance.score(h.distance),
            distance: h.distance,
            vector_score: None,
        })
        .collect();
    // Nearest first, whatever order a store gave.
    matches.sort_by(|a, b| b.score.total_cmp(&a.score));
    if let (Some(rerank), Some(text), false) = (&search.rerank, &query, matches.is_empty()) {
        let name = rerank
            .credential
            .as_deref()
            .or(embed.map(|e| e.credential.as_str()))
            .ok_or("rerank.credential: the AI credential of the rerank model")?;
        let credential = credential_of(effects, name, "ai", "rerank")?;
        let model = rerank
            .model
            .render(env)
            .map_err(|e| format!("rerank.model: {e}"))?;
        let texts: Vec<String> = matches.iter().map(|m| m.text.clone()).collect();
        let started = Instant::now();
        let order = api::rerank(effects, &credential, &model, text, &texts, search.top_k).await?;
        metrics.insert("rerank_ms".to_owned(), json!(millis(started)));
        if let Some(order) = order {
            matches = order
                .into_iter()
                .filter_map(|(i, relevance)| {
                    let mut m = matches.get(i)?.clone();
                    m.vector_score = Some(m.score);
                    m.score = relevance;
                    Some(m)
                })
                .collect();
        }
    }
    if let Some(least) = search.min_score {
        matches.retain(|m| m.score >= least);
    }
    matches.truncate(search.top_k);
    Ok(Found::Matches(matches, metrics))
}

/// One piece to add, read from what the step was given; its vector, when it came with one.
fn piece(item: &Value, i: usize) -> Result<(Piece, Option<Vec<f64>>), String> {
    let at = format!("pieces[{i}]");
    let Value::Record(r) = item else {
        return Err(format!(
            "{at}: a record with its text and source (as split_text gives), not {}",
            item.describe()
        ));
    };
    let text = r
        .get("text")
        .or_else(|| r.get("content"))
        .map(crate::template::as_text)
        .unwrap_or_default();
    if text.trim().is_empty() {
        return Err(format!("{at}.text: the piece's text is empty"));
    }
    if text.len() > MAX_PIECE {
        return Err(format!(
            "{at}.text: at most {} KiB a piece: split into smaller pieces",
            MAX_PIECE / 1024
        ));
    }
    let source = r.get("source").map(crate::template::as_text).unwrap_or_default();
    if source.trim().is_empty() || source.len() > MAX_SOURCE {
        return Err(format!(
            "{at}.source: the document it is from, 1 to {MAX_SOURCE} characters"
        ));
    }
    let position = match r.get("position") {
        None | Some(Value::Null) => i64::try_from(i).unwrap_or(0),
        Some(p) => number(Some(p))
            .filter(|n| n.fract() == 0.0 && n.abs() < 2_147_483_647.0)
            // A whole number within an integer's range, checked.
            .map(|n| n as i64)
            .ok_or_else(|| format!("{at}.position: a whole number"))?,
    };
    let id = match r.get("id").map(crate::template::as_text) {
        Some(id) if !id.trim().is_empty() => id,
        _ => format!("{source}#{position}"),
    };
    if id.len() > MAX_ID {
        return Err(format!("{at}.id: at most {MAX_ID} characters"));
    }
    let metadata = match r.get("metadata") {
        None | Some(Value::Null) => json!({}),
        Some(m @ Value::Record(_)) => as_json(m),
        Some(other) => return Err(format!("{at}.metadata: a record, not {}", other.describe())),
    };
    if metadata.to_string().len() > MAX_METADATA {
        return Err(format!("{at}.metadata: at most {} KiB", MAX_METADATA / 1024));
    }
    let vector = match r.get("embedding") {
        None | Some(Value::Null) => None,
        Some(v) => Some(api::numbers(v).ok_or_else(|| format!("{at}.embedding: a list of numbers"))?),
    };
    Ok((
        Piece {
            id,
            source,
            position,
            text,
            metadata,
            vector: Vec::new(),
        },
        vector,
    ))
}

/// The pieces to add with their vectors (given, or made by the step's embedder); or a test run's showing.
async fn with_vectors(
    s: &Store,
    action: &StoreAction,
    backend: &Backend<'_>,
    kind: StoreKind,
    effects: &dyn Effects,
    env: &Env<'_>,
    metrics: &mut serde_json::Map<String, Json>,
) -> Result<Result<Vec<Piece>, Json>, String> {
    let StoreAction::Add {
        pieces, embeddings, ..
    } = action
    else {
        return Err("not an add".to_owned());
    };
    let given = match pieces.evaluate(env).map_err(|e| format!("pieces: {e}"))? {
        Value::List(items) => items,
        Value::Null => std::sync::Arc::new(Vec::new()),
        other => return Err(format!("pieces: a list, not {}", other.describe())),
    };
    if given.len() > MAX_PIECES {
        return Err(format!(
            "pieces: {} of them, at most {MAX_PIECES} a step",
            given.len()
        ));
    }
    let read: Vec<(Piece, Option<Vec<f64>>)> = given
        .iter()
        .enumerate()
        .map(|(i, p)| piece(p, i))
        .collect::<Result<_, _>>()?;
    let mut vectors: Vec<Option<Vec<f64>>> = read.iter().map(|(_, v)| v.clone()).collect();
    let mut list: Vec<Piece> = read.into_iter().map(|(p, _)| p).collect();
    if list.is_empty() {
        return Ok(Ok(list));
    }
    if let Some(expr) = embeddings {
        let made = match expr.evaluate(env).map_err(|e| format!("embeddings: {e}"))? {
            Value::List(v) => v,
            other => return Err(format!("embeddings: a list of vectors, not {}", other.describe())),
        };
        if made.len() != list.len() {
            return Err(format!(
                "embeddings: {} vectors for {} pieces",
                made.len(),
                list.len()
            ));
        }
        for (i, (slot, v)) in vectors.iter_mut().zip(made.iter()).enumerate() {
            *slot = Some(api::numbers(v).ok_or_else(|| format!("embeddings[{i}]: a list of numbers"))?);
        }
    } else if vectors.iter().any(Option::is_none) {
        let embedder = s
            .embed
            .as_ref()
            .ok_or("pieces without vectors: name embed_with (an AI credential), or give embeddings")?;
        let credential = credential_of(effects, &embedder.credential, "ai", "embed_with")?;
        let texts: Vec<String> = list.iter().map(|p| p.text.clone()).collect();
        let (made, ms) = api::embed(effects, embedder, &credential, env, &texts).await?;
        metrics.insert("embed_ms".to_owned(), json!(ms));
        match made {
            Vectors::Shown(request) => {
                let fields = with(json!({ "added": list.len() }), &place(&s.target, kind));
                return Ok(Err(shown_result(
                    &request,
                    &backend.statements(true, false),
                    fields,
                )));
            }
            Vectors::Made(made) => {
                for (slot, v) in vectors.iter_mut().zip(made) {
                    *slot = Some(v);
                }
            }
        }
    }
    for (p, v) in list.iter_mut().zip(vectors) {
        p.vector = v.unwrap_or_default();
    }
    Ok(Ok(list))
}

/// Adds pieces (made vectors here, unless given), then removes what their documents no longer have.
async fn add(
    s: &Store,
    action: &StoreAction,
    backend: &Backend<'_>,
    kind: StoreKind,
    effects: &dyn Effects,
    env: &Env<'_>,
) -> Result<Json, String> {
    let StoreAction::Add {
        create,
        dimensions,
        replace,
        ..
    } = action
    else {
        return Err("not an add".to_owned());
    };
    let where_ = place(&s.target, kind);
    let mut metrics = serde_json::Map::new();
    let list = match with_vectors(s, action, backend, kind, effects, env, &mut metrics).await? {
        Ok(list) => list,
        Err(shown) => return Ok(shown),
    };
    if list.is_empty() {
        return Ok(with(json!({ "added": 0, "removed": 0, "sources": [] }), &where_));
    }
    let width = list.first().map_or(0, |p| p.vector.len());
    let expected = dimensions.map_or(width, |d| d as usize);
    for (i, p) in list.iter().enumerate() {
        let n = p.vector.len();
        if n != expected || n == 0 || n > MAX_DIMENSIONS as usize {
            return Err(format!(
                "pieces[{i}]: a vector of {n} numbers, where the store's have {expected}"
            ));
        }
    }
    // One piece per id: the last given wins, in the place of the first.
    let mut kept: Vec<Piece> = Vec::with_capacity(list.len());
    let mut place_of: HashMap<String, usize> = HashMap::new();
    for p in list {
        match place_of.get(&p.id).and_then(|i| kept.get_mut(*i)) {
            Some(slot) => *slot = p,
            None => {
                place_of.insert(p.id.clone(), kept.len());
                kept.push(p);
            }
        }
    }
    backend.check(&kept)?;
    let shown_with =
        |shown: Json, sql: &[String]| merged(shown, sql, with(json!({ "added": kept.len() }), &where_));
    let mut created = false;
    if *create {
        let dims = u32::try_from(expected).unwrap_or(MAX_DIMENSIONS);
        match backend.ensure(dims, &kept).await? {
            Got::Done(made) => created = made,
            Got::Shown(shown) => return Ok(shown_with(shown, &[])),
        }
    }
    let started = Instant::now();
    let added = match backend.upsert(&kept).await? {
        Got::Done(n) => n,
        Got::Shown(shown) => return Ok(shown_with(shown, &[])),
    };
    let mut sources: Vec<&str> = kept.iter().map(|p| p.source.as_str()).collect();
    sources.sort_unstable();
    sources.dedup();
    let mut removed = Some(0);
    if *replace {
        let ids: Vec<&str> = kept.iter().map(|p| p.id.as_str()).collect();
        removed = match backend.remove_stale(&sources, &ids).await? {
            Got::Done(n) => n,
            Got::Shown(shown) => return Ok(shown_with(shown, &[])),
        };
    }
    metrics.insert("write_ms".to_owned(), json!(millis(started)));
    Ok(with(
        json!({
            "added": added, "removed": removed, "sources": sources, "dimensions": expected, "created": created,
            "metrics": metrics,
        }),
        &where_,
    ))
}

/// Deletes every piece of the documents named.
async fn delete(
    target: &Target,
    sources: &openduct_expr::Compiled,
    backend: &Backend<'_>,
    kind: StoreKind,
    env: &Env<'_>,
) -> Result<Json, String> {
    let named: Vec<String> = match sources.evaluate(env).map_err(|e| format!("source: {e}"))? {
        Value::List(items) => items.iter().map(crate::template::as_text).collect(),
        Value::Null => Vec::new(),
        one => vec![crate::template::as_text(&one)],
    };
    let named: Vec<String> = named.into_iter().filter(|s| !s.trim().is_empty()).collect();
    if named.is_empty() {
        return Err("source: the document (or documents) whose pieces to delete".to_owned());
    }
    if named.len() > MAX_SOURCES || named.iter().any(|s| s.len() > MAX_SOURCE) {
        return Err(format!(
            "source: at most {MAX_SOURCES} documents, each named in at most {MAX_SOURCE} characters"
        ));
    }
    let list: Vec<&str> = named.iter().map(String::as_str).collect();
    let where_ = place(target, kind);
    match backend.delete(&list).await? {
        Got::Shown(shown) => Ok(merged(
            shown,
            &[],
            with(json!({ "deleted": 0, "sources": named }), &where_),
        )),
        Got::Done(n) => Ok(with(json!({ "deleted": n, "sources": named }), &where_)),
    }
}

/// Runs a `vector_store` step.
pub(super) async fn run(
    s: &Store,
    effects: &dyn Effects,
    env: &Env<'_>,
    held: &RunVectors,
) -> Result<Value, String> {
    let (backend, kind) = Backend::open(&s.target, effects, held, "a vector store")?;
    let result = match &s.action {
        action @ StoreAction::Add { .. } => add(s, action, &backend, kind, effects, env).await?,
        StoreAction::Delete { sources } => delete(&s.target, sources, &backend, kind, env).await?,
        StoreAction::Search(search) => {
            match find(&backend, kind, &s.target, s.embed.as_ref(), search, effects, env).await? {
                Found::Shown(shown) => shown,
                Found::Matches(matches, metrics) => with(
                    json!({
                        "matches": matches.iter().map(Match::to_json).collect::<Vec<_>>(),
                        "count": matches.len(),
                        "distance": s.target.distance.name(),
                        "metrics": metrics,
                    }),
                    &place(&s.target, kind),
                ),
            }
        }
    };
    Ok(Value::from_json(&result))
}
