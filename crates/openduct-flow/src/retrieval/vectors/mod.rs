// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The stores a `vector_store` or `ask_documents` step keeps its pieces in, beside PostgreSQL with pgvector: Qdrant,
//! Weaviate, Milvus (and Zilliz), Chroma, Pinecone, Redis's vector search, MongoDB Atlas Vector Search - and a store
//! in the run's own memory, for small sets and tests.
//!
//! Every store does the same five things - make its collection when asked (its vectors' width taken from the first
//! pieces), add pieces (a piece added again by its id replaces the old one), remove what a document added again no
//! longer has, delete a document's pieces, and find the pieces nearest a vector with a metadata filter - so the steps
//! work the same on each, with the same output, bounds, and test and preview rules. A store is chosen by the step's
//! `store`, or else by its credential's kind.
//!
//! A metadata filter is written once, as a record (`{team: sales, tags: [faq]}`: a field equal to a value, a list
//! field holding every value given), and each store is sent it in its own filter language. A filter a store cannot
//! express is refused when the workflow is read (or, for one worked out by an expression, when the step runs).
//!
//! Every request goes through the run's effects: HTTP for the stores with a web API, which keeps the private-network
//! rule; Redis's and MongoDB's own protocols over the connections the storage steps open, which keep it too. A test
//! run sends nothing and shows the first request; a preview does what reads (a search, a look at whether a
//! collection is there) and shows what writes.

use std::sync::Mutex;

use serde_json::{Map, Value as Json, json};

use super::{Distance, Table};
use crate::apps::Credential;
use crate::run::Effects;

mod chroma;
mod filter;
mod http;
mod memory;
mod milvus;
mod mongo;
mod pinecone;
mod qdrant;
pub(crate) mod read;
mod redis;
mod weaviate;

pub(crate) use filter::{Cond, Scalar, check as check_filter, conditions, holds};
pub(crate) use memory::RunVectors;

/// The longest name of a collection, an index or a namespace.
pub(crate) const MAX_NAME: usize = 100;

/// A store of pieces and their vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    Postgres,
    Qdrant,
    Weaviate,
    Milvus,
    Chroma,
    Pinecone,
    Redis,
    Mongodb,
    Memory,
}

impl StoreKind {
    pub(crate) const ALL: [Self; 9] = [
        Self::Postgres,
        Self::Qdrant,
        Self::Weaviate,
        Self::Milvus,
        Self::Chroma,
        Self::Pinecone,
        Self::Redis,
        Self::Mongodb,
        Self::Memory,
    ];

    /// The store as a step names it (`store: qdrant`).
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Qdrant => "qdrant",
            Self::Weaviate => "weaviate",
            Self::Milvus => "milvus",
            Self::Chroma => "chroma",
            Self::Pinecone => "pinecone",
            Self::Redis => "redis",
            Self::Mongodb => "mongodb",
            Self::Memory => "memory",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.name() == text)
    }

    /// The kind of credential it takes; none for the run's own memory. Each store's credential kind is its name.
    pub(crate) fn credential(self) -> Option<&'static str> {
        (self != Self::Memory).then(|| self.name())
    }

    /// The store a credential of `kind` reaches.
    pub(crate) fn of_credential(kind: &str) -> Option<Self> {
        Self::parse(kind).filter(|k| *k != Self::Memory)
    }

    /// The store's word for what holds the pieces.
    pub(crate) fn holder(self) -> &'static str {
        match self {
            Self::Postgres => "table",
            Self::Pinecone | Self::Redis => "index",
            _ => "collection",
        }
    }

    /// Whether a filter may name a field inside a field (`{author: {name: Ann}}`).
    pub(crate) fn nested(self) -> bool {
        matches!(
            self,
            Self::Postgres | Self::Memory | Self::Qdrant | Self::Milvus | Self::Redis | Self::Mongodb
        )
    }

    /// Whether a list in a filter may hold numbers or true and false as well as text.
    pub(crate) fn lists_of_any(self) -> bool {
        self != Self::Pinecone
    }

    /// Whether the store keeps metadata as fields of its own, beside the piece's (Weaviate's properties, Chroma's and
    /// Pinecone's metadata): their names are then names it takes, and `piece_` begins only its own.
    pub(crate) fn flat(self) -> bool {
        matches!(self, Self::Weaviate | Self::Chroma | Self::Pinecone)
    }

    /// Whether a field's name is one a filter on this store may name.
    pub(crate) fn field_ok(self, name: &str) -> Result<(), String> {
        let plain = |extra: &[char]| {
            !name.is_empty()
                && name.len() <= 64
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || extra.contains(&c))
        };
        let ok = match self {
            Self::Postgres | Self::Memory | Self::Redis | Self::Mongodb => {
                !name.is_empty() && name.len() <= 200 && !name.starts_with('$')
            }
            Self::Weaviate => plain(&[]) && name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_'),
            _ => plain(&['-']),
        };
        if !ok {
            return Err(format!(
                "{name:?} is not a field {} filters by: letters, digits, _ and - (for Weaviate, a letter first and no -)",
                label(self)
            ));
        }
        if self.flat() && name.starts_with("piece_") {
            return Err(format!(
                "{name}: names beginning piece_ are the store's own fields (piece_id, piece_source ...)"
            ));
        }
        Ok(())
    }

    /// Whether `name` names a collection (an index, a table) this store takes.
    pub(crate) fn check_name(self, name: &str, at: &str) -> Result<(), String> {
        let chars = |extra: &[char]| {
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || extra.contains(&c))
        };
        let starts_alnum = name.starts_with(|c: char| c.is_ascii_alphanumeric());
        let (ok, rule): (bool, &str) = match self {
            Self::Postgres => return super::table_name(name, at).map(|_| ()),
            Self::Qdrant | Self::Memory => (
                starts_alnum && chars(&['-']),
                "letters, digits, _ and -, starting with a letter or a digit",
            ),
            Self::Weaviate => (
                name.starts_with(|c: char| c.is_ascii_alphabetic()) && chars(&[]),
                "letters, digits and _, starting with a letter (Weaviate makes the first a capital)",
            ),
            Self::Milvus => (
                name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') && chars(&[]),
                "letters, digits and _, starting with a letter",
            ),
            Self::Chroma => (
                name.len() >= 3
                    && starts_alnum
                    && name.ends_with(|c: char| c.is_ascii_alphanumeric())
                    && chars(&['-', '.']),
                "3 or more letters, digits, _, - and ., starting and ending with a letter or a digit",
            ),
            Self::Pinecone => (
                name.len() <= 45
                    && starts_alnum
                    && name.ends_with(|c: char| c.is_ascii_alphanumeric())
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "small letters, digits and -, at most 45, starting and ending with a letter or a digit",
            ),
            Self::Redis => (
                starts_alnum && chars(&['-', ':', '.']),
                "letters, digits, _, -, : and ., starting with a letter or a digit",
            ),
            Self::Mongodb => {
                let parts: Vec<&str> = name.split('.').collect();
                (
                    parts.len() == 2
                        && parts.iter().all(|p| {
                            !p.is_empty()
                                && p.len() <= 64
                                && p.chars()
                                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
                        }),
                    "database.collection, each of letters, digits, _ and -",
                )
            }
        };
        if ok && !name.is_empty() && name.len() <= MAX_NAME {
            Ok(())
        } else {
            Err(format!(
                "{at}: {name:?} is not a {} {} takes: {rule}, at most {MAX_NAME}",
                self.holder(),
                label(self)
            ))
        }
    }
}

/// The store as people know it.
pub(crate) fn label(kind: StoreKind) -> &'static str {
    match kind {
        StoreKind::Postgres => "PostgreSQL",
        StoreKind::Qdrant => "Qdrant",
        StoreKind::Weaviate => "Weaviate",
        StoreKind::Milvus => "Milvus",
        StoreKind::Chroma => "Chroma",
        StoreKind::Pinecone => "Pinecone",
        StoreKind::Redis => "Redis",
        StoreKind::Mongodb => "MongoDB",
        StoreKind::Memory => "the run's memory",
    }
}

/// What a store did, or - in a test run, or a preview's write - what it would have done, shown.
#[derive(Debug)]
pub(crate) enum Got<T> {
    Done(T),
    Shown(Json),
}

/// One piece to add: its vector made or given already.
#[derive(Debug, Clone)]
pub(crate) struct Piece {
    pub(crate) id: String,
    pub(crate) source: String,
    pub(crate) position: i64,
    pub(crate) text: String,
    pub(crate) metadata: Json,
    pub(crate) vector: Vec<f64>,
}

/// One piece found: its distance as pgvector gives it (the cosine distance, the negative inner product, the
/// Euclidean distance), whatever its store said, so a score means the same on every store.
#[derive(Debug, Clone)]
pub(crate) struct Hit {
    pub(crate) id: String,
    pub(crate) source: String,
    pub(crate) position: Json,
    pub(crate) text: String,
    pub(crate) metadata: Json,
    pub(crate) distance: f64,
}

/// Where a step's pieces are, ready to be reached.
#[derive(Debug)]
pub(crate) enum Backend<'a> {
    Postgres(super::pg::Pg<'a>),
    Qdrant(qdrant::Qdrant<'a>),
    Weaviate(weaviate::Weaviate<'a>),
    Milvus(milvus::Milvus<'a>),
    Chroma(chroma::Chroma<'a>),
    Pinecone(pinecone::Pinecone<'a>),
    Redis(redis::Redis<'a>),
    Mongodb(mongo::Mongo<'a>),
    Memory(memory::InRun<'a>),
}

/// Which store a step reaches, and where in it.
#[derive(Debug, Clone)]
pub struct Target {
    /// The store the step names; none: the one its credential's kind says, worked out when it runs.
    pub store: Option<StoreKind>,
    /// The store's credential; none only for the run's own memory.
    pub credential: Option<String>,
    /// The table, collection or index, as written: checked for its store when the workflow is read (or, with no
    /// `store`, when the step runs).
    pub name: String,
    pub distance: Distance,
    /// Pinecone's namespace inside its index.
    pub namespace: Option<String>,
    /// MongoDB's vector search index (`vector_index` unless said).
    pub index: Option<String>,
}

impl Target {
    /// The table, collection or index as a step's result names it.
    pub(crate) fn shown_name(&self) -> String {
        match (self.store, super::table_name(&self.name, "table")) {
            (Some(StoreKind::Postgres) | None, Ok((schema, name))) => Table {
                schema,
                name,
                distance: self.distance,
            }
            .quoted(),
            _ => self.name.clone(),
        }
    }
}

impl<'a> Backend<'a> {
    /// The store a step's target names, its credential checked: refused when it is not of the store's kind.
    pub(crate) fn open(
        target: &Target,
        effects: &'a dyn Effects,
        held: &'a RunVectors,
        what: &str,
    ) -> Result<(Self, StoreKind), String> {
        if target.store == Some(StoreKind::Memory) {
            target
                .store
                .map_or(Ok(()), |k| k.check_name(&target.name, "collection"))?;
            return Ok((
                Self::Memory(memory::InRun::new(held, &target.name, target.distance)),
                StoreKind::Memory,
            ));
        }
        let name = target
            .credential
            .as_deref()
            .ok_or("credential: the store's credential")?;
        let credential = effects.credential(name)?;
        let kind = match target.store {
            Some(k) => k,
            None => StoreKind::of_credential(&credential.kind).unwrap_or(StoreKind::Postgres),
        };
        let wanted = kind.credential().unwrap_or("postgres");
        if credential.kind != wanted && credential.kind != "test" {
            // PostgreSQL's is said as it always was: the store a step takes when it names none.
            let on = match kind {
                StoreKind::Postgres => String::new(),
                other => format!(" on {}", label(other)),
            };
            return Err(format!(
                "{name} is a {} credential: {what}{on} takes a {wanted} one",
                credential.kind
            ));
        }
        kind.check_name(&target.name, kind.holder())?;
        let backend = match kind {
            StoreKind::Postgres => {
                let (schema, table) = super::table_name(&target.name, "table")?;
                Self::Postgres(super::pg::Pg {
                    effects,
                    table: Table {
                        schema,
                        name: table,
                        distance: target.distance,
                    },
                    db: credential,
                })
            }
            StoreKind::Qdrant => Self::Qdrant(qdrant::Qdrant::open(effects, &credential, target)?),
            StoreKind::Weaviate => Self::Weaviate(weaviate::Weaviate::open(effects, &credential, target)?),
            StoreKind::Milvus => Self::Milvus(milvus::Milvus::open(effects, &credential, target)?),
            StoreKind::Chroma => Self::Chroma(chroma::Chroma::open(effects, &credential, target)?),
            StoreKind::Pinecone => Self::Pinecone(pinecone::Pinecone::open(effects, &credential, target)?),
            StoreKind::Redis => Self::Redis(redis::Redis::open(effects, credential, target)),
            StoreKind::Mongodb => Self::Mongodb(mongo::Mongo::open(effects, credential, target)?),
            StoreKind::Memory => Self::Memory(memory::InRun::new(held, &target.name, target.distance)),
        };
        Ok((backend, kind))
    }

    /// The statements a test run shows beside the request it shows first (a model's, for vectors): PostgreSQL's.
    pub(crate) fn statements(&self, adding: bool, filtered: bool) -> Vec<String> {
        match self {
            Self::Postgres(s) if adding => s.upsert_sql(),
            Self::Postgres(s) => s.search_sql(filtered),
            _ => Vec::new(),
        }
    }

    /// Refuses pieces this store cannot keep, before anything is sent.
    pub(crate) fn check(&self, pieces: &[Piece]) -> Result<(), String> {
        match self {
            Self::Pinecone(p) => flat_check(pieces).and_then(|()| p.check(pieces)),
            Self::Mongodb(_) => mongo::check(pieces),
            Self::Weaviate(_) | Self::Chroma(_) => flat_check(pieces),
            _ => Ok(()),
        }
    }

    /// Makes the collection when it is not there, for vectors of `dimensions` (and, on Weaviate, the metadata fields
    /// of the first `pieces`); whether it made it.
    pub(crate) async fn ensure(&self, dimensions: u32, pieces: &[Piece]) -> Result<Got<bool>, String> {
        match self {
            Self::Postgres(s) => s.ensure(dimensions).await,
            Self::Qdrant(s) => s.ensure(dimensions).await,
            Self::Weaviate(s) => s.ensure(pieces).await,
            Self::Milvus(s) => s.ensure(dimensions).await,
            Self::Chroma(s) => s.ensure().await,
            Self::Pinecone(s) => s.ensure(dimensions).await,
            Self::Redis(s) => s.ensure(dimensions).await,
            Self::Mongodb(s) => s.ensure(dimensions).await,
            Self::Memory(s) => Ok(Got::Done(s.ensure())),
        }
    }

    /// Adds the pieces (a piece already there by its id replaced); how many.
    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        match self {
            Self::Postgres(s) => s.upsert(pieces).await,
            Self::Qdrant(s) => s.upsert(pieces).await,
            Self::Weaviate(s) => s.upsert(pieces).await,
            Self::Milvus(s) => s.upsert(pieces).await,
            Self::Chroma(s) => s.upsert(pieces).await,
            Self::Pinecone(s) => s.upsert(pieces).await,
            Self::Redis(s) => s.upsert(pieces).await,
            Self::Mongodb(s) => s.upsert(pieces).await,
            Self::Memory(s) => s.upsert(pieces).map(Got::Done),
        }
    }

    /// Removes the pieces of `sources` whose id is not in `keep`; how many, when the store says.
    pub(crate) async fn remove_stale(
        &self,
        sources: &[&str],
        keep: &[&str],
    ) -> Result<Got<Option<u64>>, String> {
        match self {
            Self::Postgres(s) => s.remove(sources, Some(keep)).await,
            Self::Qdrant(s) => s.remove(sources, Some(keep)).await,
            Self::Weaviate(s) => s.remove(sources, Some(keep)).await,
            Self::Milvus(s) => s.remove(sources, Some(keep)).await,
            Self::Chroma(s) => s.remove(sources, Some(keep)).await,
            Self::Pinecone(s) => s.remove(sources, Some(keep)).await,
            Self::Redis(s) => s.remove(sources, Some(keep)).await,
            Self::Mongodb(s) => s.remove(sources, Some(keep)).await,
            Self::Memory(s) => Ok(Got::Done(Some(s.remove(sources, Some(keep))))),
        }
    }

    /// Deletes every piece of `sources`; how many, when the store says.
    pub(crate) async fn delete(&self, sources: &[&str]) -> Result<Got<Option<u64>>, String> {
        match self {
            Self::Postgres(s) => s.remove(sources, None).await,
            Self::Qdrant(s) => s.remove(sources, None).await,
            Self::Weaviate(s) => s.remove(sources, None).await,
            Self::Milvus(s) => s.remove(sources, None).await,
            Self::Chroma(s) => s.remove(sources, None).await,
            Self::Pinecone(s) => s.remove(sources, None).await,
            Self::Redis(s) => s.remove(sources, None).await,
            Self::Mongodb(s) => s.remove(sources, None).await,
            Self::Memory(s) => Ok(Got::Done(Some(s.remove(sources, None)))),
        }
    }

    /// The `k` pieces nearest `vector` whose metadata holds `filter` (a record), nearest first.
    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        match self {
            Self::Postgres(s) => s.search(vector, k, filter).await,
            Self::Qdrant(s) => s.search(vector, k, filter).await,
            Self::Weaviate(s) => s.search(vector, k, filter).await,
            Self::Milvus(s) => s.search(vector, k, filter).await,
            Self::Chroma(s) => s.search(vector, k, filter).await,
            Self::Pinecone(s) => s.search(vector, k, filter).await,
            Self::Redis(s) => s.search(vector, k, filter).await,
            Self::Mongodb(s) => s.search(vector, k, filter).await,
            Self::Memory(s) => s.search(vector, k, filter).map(Got::Done),
        }
    }
}

/// A credential's field, trimmed; none when empty.
pub(crate) fn field(credential: &Credential, name: &str) -> Option<String> {
    credential
        .fields
        .get(name)
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// A web store's address, from its credential's `url`: http or https, without a last `/`.
pub(crate) fn base_url(credential: &Credential, kind: StoreKind) -> Result<String, String> {
    // A test run of a workflow whose credential is not made yet sends nothing: it shows where the request would go.
    if credential.kind == "test" && field(credential, "url").is_none() {
        return Ok(format!("https://<the {} credential's url>", kind.name()));
    }
    let url = field(credential, "url").ok_or_else(|| {
        format!(
            "the {} credential has no url: its address, as https://{}.example.com",
            kind.name(),
            kind.name()
        )
    })?;
    if !(url.starts_with("https://") || url.starts_with("http://")) || url.contains(char::is_whitespace) {
        return Err(format!("url: {url:?} is not an http or https address"));
    }
    Ok(url.trim_end_matches('/').to_owned())
}

/// A piece's id as a UUID, for stores whose ids are UUIDs (Qdrant, Weaviate): the same id gives the same UUID, a
/// name-based one of RFC 9562's eighth version, from the SHA-256 of the id.
pub(crate) fn uuid_of(id: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, id.as_bytes());
    let mut b = [0u8; 16];
    for (slot, byte) in b.iter_mut().zip(digest.as_ref()) {
        *slot = *byte;
    }
    b[6] = (b[6] & 0x0f) | 0x80;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        hex.get(..8).unwrap_or_default(),
        hex.get(8..12).unwrap_or_default(),
        hex.get(12..16).unwrap_or_default(),
        hex.get(16..20).unwrap_or_default(),
        hex.get(20..).unwrap_or_default()
    )
}

/// The fields of a piece's metadata a flat store keeps as its own (to filter by): those whose name it takes and whose
/// value is text, a number, true or false, or a list of one of these - the whole metadata is kept beside them.
pub(crate) fn flat_fields(kind: StoreKind, metadata: &Json) -> Map<String, Json> {
    let mut out = Map::new();
    let Json::Object(m) = metadata else { return out };
    for (k, v) in m {
        if kind.field_ok(k).is_err() {
            continue;
        }
        let keep = match v {
            Json::String(_) | Json::Number(_) | Json::Bool(_) => true,
            Json::Array(items) => {
                !items.is_empty()
                    && (items.iter().all(Json::is_string)
                        || (kind.lists_of_any()
                            && (items.iter().all(Json::is_number) || items.iter().all(Json::is_boolean))))
            }
            _ => false,
        };
        if keep {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

/// A flat store's pieces: metadata whose names begin `piece_` would be mistaken for the store's own.
fn flat_check(pieces: &[Piece]) -> Result<(), String> {
    for (i, p) in pieces.iter().enumerate() {
        if let Json::Object(m) = &p.metadata
            && let Some(k) = m.keys().find(|k| k.starts_with("piece_"))
        {
            return Err(format!(
                "pieces[{i}].metadata.{k}: names beginning piece_ are the store's own fields"
            ));
        }
    }
    Ok(())
}

/// The metadata a flat store gives back: the whole metadata kept as JSON text, or else its own fields.
pub(crate) fn metadata_back(kept: Option<&Json>, fields: Option<&Map<String, Json>>) -> Json {
    if let Some(Json::String(text)) = kept
        && let Ok(m @ Json::Object(_)) = serde_json::from_str::<Json>(text)
    {
        return m;
    }
    let mut out = Map::new();
    for (k, v) in fields.into_iter().flatten() {
        if !k.starts_with("piece_") {
            out.insert(k.clone(), v.clone());
        }
    }
    Json::Object(out)
}

/// A distance as pgvector gives it, from a similarity (cosine, inner product) or a squared Euclidean distance.
pub(crate) fn from_similarity(distance: Distance, similarity: f64) -> f64 {
    match distance {
        Distance::Cosine => 1.0 - similarity,
        Distance::InnerProduct => -similarity,
        Distance::L2 => similarity.max(0.0).sqrt(),
    }
}

/// A vector's numbers as a JSON list, refused when one is not a number a vector holds.
pub(crate) fn vector_json(vector: &[f64]) -> Result<Json, String> {
    if let Some((i, x)) = vector
        .iter()
        .enumerate()
        .find(|(_, x)| !x.is_finite() || x.abs() > f64::from(f32::MAX))
    {
        return Err(format!("the vector's number {i} ({x}) is not one a vector holds"));
    }
    Ok(json!(vector))
}

/// A text value of a record field, or empty.
pub(crate) fn text_of(record: &Json, name: &str) -> String {
    match record.get(name) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// A store's answer that is not what its documentation says it gives.
pub(crate) fn odd(kind: StoreKind, what: &str) -> String {
    format!("{} answered without {what}", label(kind))
}

/// Guards a value kept across a run; a poisoned lock is taken as it is (what it holds is only the run's).
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_store_takes_its_own_names() {
        assert!(StoreKind::Qdrant.check_name("doc-pieces_2", "c").is_ok());
        assert!(StoreKind::Qdrant.check_name("doc/pieces", "c").is_err());
        assert!(StoreKind::Weaviate.check_name("DocPieces", "c").is_ok());
        assert!(StoreKind::Weaviate.check_name("doc-pieces", "c").is_err());
        assert!(StoreKind::Milvus.check_name("_pieces", "c").is_ok());
        assert!(StoreKind::Chroma.check_name("ab", "c").is_err());
        assert!(StoreKind::Chroma.check_name("kb.v2", "c").is_ok());
        assert!(StoreKind::Pinecone.check_name("docs-1", "c").is_ok());
        assert!(StoreKind::Pinecone.check_name("Docs", "c").is_err());
        assert!(StoreKind::Pinecone.check_name("docs_1", "c").is_err());
        assert!(StoreKind::Redis.check_name("kb:pieces", "c").is_ok());
        assert!(StoreKind::Mongodb.check_name("shop.pieces", "c").is_ok());
        assert!(StoreKind::Mongodb.check_name("pieces", "c").is_err());
        assert!(StoreKind::Postgres.check_name("kb.pieces", "c").is_ok());
        assert!(StoreKind::Postgres.check_name("Pieces", "c").is_err());
        let e = StoreKind::Pinecone
            .check_name("A", "steps[1].collection")
            .unwrap_err();
        assert!(
            e.starts_with("steps[1].collection: \"A\" is not a index Pinecone takes"),
            "{e}"
        );
    }

    #[test]
    fn a_piece_s_uuid_is_the_same_each_time_and_well_formed() {
        let a = uuid_of("handbook#0");
        assert_eq!(a, uuid_of("handbook#0"));
        assert_ne!(a, uuid_of("handbook#1"));
        assert_eq!(a.len(), 36);
        assert_eq!(a.chars().nth(14), Some('8'), "version 8: {a}");
        assert!(
            matches!(a.chars().nth(19), Some('8' | '9' | 'a' | 'b')),
            "the variant: {a}"
        );
    }

    #[test]
    fn a_flat_store_keeps_the_fields_it_can_filter_by() {
        let m = json!({ "team": "sales", "year": 2020, "tags": ["a", "b"], "nested": { "x": 1 }, "mixed": [1, "a"],
                        "bad name": "x", "piece_id": "x", "flags": [true] });
        let kept = flat_fields(StoreKind::Chroma, &m);
        let names: Vec<&str> = kept.keys().map(String::as_str).collect();
        assert_eq!(names, ["team", "year", "tags", "flags"]);
        let kept = flat_fields(StoreKind::Pinecone, &m);
        assert!(!kept.contains_key("flags"), "Pinecone's lists are of text");
        assert_eq!(
            metadata_back(Some(&json!(m.to_string())), None),
            m,
            "the whole metadata comes back"
        );
    }

    #[test]
    fn distances_are_given_as_pgvector_gives_them() {
        assert!((from_similarity(Distance::Cosine, 0.75) - 0.25).abs() < 1e-12);
        assert!((from_similarity(Distance::InnerProduct, 3.0) + 3.0).abs() < 1e-12);
        assert!((from_similarity(Distance::L2, 4.0) - 2.0).abs() < 1e-12);
        assert!(vector_json(&[f64::NAN]).is_err());
    }
}
