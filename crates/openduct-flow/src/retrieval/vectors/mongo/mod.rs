// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MongoDB Atlas Vector Search (and MongoDB 8.2 or later with its search process), over MongoDB's own protocol
//! ([`wire`]): a test run sends nothing and shows the command; a preview runs what reads (an aggregation) and shows
//! what writes.
//!
//! The step's collection is `database.collection`. A piece is a document `{_id: <piece id>, source, position, text,
//! metadata, tags, embedding}`; its vector search index (`vector_index` unless the step names another) is made, when
//! asked, with `createSearchIndexes` - the vector at `embedding` with the step's similarity (`cosine`, `euclidean`
//! or `dotProduct`) and two `filter` fields: `source`, and `tags`, one text for each value of the metadata
//! (`team="sales"`), since a vector search filters only by fields its index names. Atlas builds a new index in the
//! background: a search right after it is made may find nothing yet.
//!
//! Pieces are written with `update` (each replacing the document of its id, or added), deleted with `delete`, and
//! found with an aggregation whose first stage is `$vectorSearch` (its filter `{tags: <text>}` for each condition,
//! joined by `$and`) and whose score is `vectorSearchScore`: `(1 + cosine) / 2`, `(1 + dot product) / 2`, or `1 / (1
//! + the Euclidean distance)`.

mod wire;

use bson::{Bson, Document, doc};
use serde_json::{Value as Json, json};

use super::filter::{tokens, wanted_tokens};
use super::{Got, Hit, Piece, StoreKind, Target, conditions};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;

use wire::{to_bson, to_json};

/// The most dimensions Atlas Vector Search indexes; how many documents one command writes.
const MAX_DIMENSIONS: usize = 8_192;
const BATCH: usize = 100;
/// The candidates a search looks at for each piece it keeps, and the most (Atlas's own).
const CANDIDATES_EACH: usize = 20;
const MAX_CANDIDATES: usize = 10_000;

pub(crate) struct Mongo<'a> {
    effects: &'a dyn Effects,
    credential: Credential,
    db: String,
    collection: String,
    index: String,
    distance: Distance,
}

impl std::fmt::Debug for Mongo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mongo")
            .field("db", &self.db)
            .field("collection", &self.collection)
            .finish_non_exhaustive()
    }
}

/// Refuses vectors wider than Atlas indexes.
pub(crate) fn check(pieces: &[Piece]) -> Result<(), String> {
    match pieces.first() {
        Some(p) if p.vector.len() > MAX_DIMENSIONS => Err(format!(
            "MongoDB's vector search indexes vectors of at most {MAX_DIMENSIONS} numbers; these have {}",
            p.vector.len()
        )),
        _ => Ok(()),
    }
}

/// A command as a test run shows it, its vectors by their size.
fn shown(why: &str, db: &str, command: &Document) -> Json {
    json!({ "test": why, "mongodb": { "database": db, "command": short(to_json(&Bson::Document(command.clone()))) } })
}

fn short(value: Json) -> Json {
    match value {
        Json::Array(items) if items.len() > 8 && items.iter().all(Json::is_number) => {
            json!(format!("<a vector of {} numbers>", items.len()))
        }
        Json::Array(items) => Json::Array(items.into_iter().map(short).collect()),
        Json::Object(fields) => Json::Object(fields.into_iter().map(|(k, v)| (k, short(v))).collect()),
        other => other,
    }
}

fn first_batch(answer: &Document) -> Vec<Document> {
    answer
        .get_document("cursor")
        .and_then(|c| c.get_array("firstBatch"))
        .map(|b| {
            b.iter()
                .filter_map(|d| match d {
                    Bson::Document(d) => Some(d.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn count(answer: &Document) -> u64 {
    match answer.get("n") {
        Some(Bson::Int32(n)) => u64::try_from(*n).unwrap_or(0),
        Some(Bson::Int64(n)) => u64::try_from(*n).unwrap_or(0),
        _ => 0,
    }
}

impl<'a> Mongo<'a> {
    pub(crate) fn open(
        effects: &'a dyn Effects,
        credential: Credential,
        target: &Target,
    ) -> Result<Self, String> {
        let (db, collection) = target
            .name
            .split_once('.')
            .ok_or("collection: database.collection, as shop.pieces")?;
        let index = target.index.clone().unwrap_or_else(|| "vector_index".to_owned());
        Ok(Self {
            effects,
            credential,
            db: db.to_owned(),
            collection: collection.to_owned(),
            index,
            distance: target.distance,
        })
    }

    /// A connection, when this run may send `command`; else the command shown.
    async fn conn(&self, writes: bool, command: &Document) -> Result<Result<wire::Conn, Json>, String> {
        if self.credential.kind != "mongodb" {
            return Ok(Err(shown("not sent", &self.db, command)));
        }
        match self.effects.storage().allows(writes) {
            Ok(places) => Ok(Ok(wire::Conn::open(&self.credential, places).await?)),
            Err(why) => Ok(Err(shown(why, &self.db, command))),
        }
    }

    fn similarity(&self) -> &'static str {
        match self.distance {
            Distance::Cosine => "cosine",
            Distance::L2 => "euclidean",
            Distance::InnerProduct => "dotProduct",
        }
    }

    pub(crate) async fn ensure(&self, dimensions: u32) -> Result<Got<bool>, String> {
        let look = doc! {
            "aggregate": self.collection.as_str(), "pipeline": [{ "$listSearchIndexes": { "name": self.index.as_str() } }], "cursor": {},
        };
        let mut conn = match self.conn(false, &look).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        // A collection not there yet has no index (MongoDB says the namespace is not found).
        if let Ok(answer) = conn.command(&self.db, look).await
            && !first_batch(&answer).is_empty()
        {
            return Ok(Got::Done(false));
        }
        let made = doc! {
            "createSearchIndexes": self.collection.as_str(),
            "indexes": [{
                "name": self.index.as_str(), "type": "vectorSearch",
                "definition": { "fields": [
                    { "type": "vector", "path": "embedding", "numDimensions": i64::from(dimensions), "similarity": self.similarity() },
                    { "type": "filter", "path": "source" },
                    { "type": "filter", "path": "tags" },
                ] },
            }],
        };
        if let Err(why) = self.effects.storage().allows(true) {
            return Ok(Got::Shown(shown(why, &self.db, &made)));
        }
        if let Err(e) = conn
            .command(&self.db, doc! { "create": self.collection.as_str() })
            .await
            && !e.contains("(48)")
        {
            return Err(e);
        }
        conn.command(&self.db, made).await?;
        Ok(Got::Done(true))
    }

    fn document(p: &Piece) -> Document {
        doc! {
            "_id": p.id.as_str(), "source": p.source.as_str(), "position": p.position, "text": p.text.as_str(),
            "metadata": to_bson(&p.metadata),
            "tags": tokens(&p.metadata),
            "embedding": p.vector.iter().map(|x| Bson::Double(*x)).collect::<Vec<_>>(),
        }
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let commands: Vec<Document> = pieces
            .chunks(BATCH)
            .map(|batch| {
                let updates: Vec<Bson> = batch
                    .iter()
                    .map(|p| {
                        Bson::Document(
                            doc! { "q": { "_id": p.id.as_str() }, "u": Self::document(p), "upsert": true },
                        )
                    })
                    .collect();
                doc! { "update": self.collection.as_str(), "updates": updates, "ordered": false }
            })
            .collect();
        let Some(first) = commands.first() else {
            return Ok(Got::Done(0));
        };
        let mut conn = match self.conn(true, first).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        let mut added = 0;
        for command in commands {
            added += count(&conn.command(&self.db, command).await?);
        }
        Ok(Got::Done(added))
    }

    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let texts = |list: &[&str]| Bson::Array(list.iter().map(|s| Bson::String((*s).to_owned())).collect());
        let mut q = doc! { "source": { "$in": texts(sources) } };
        if let Some(keep) = keep.filter(|k| !k.is_empty()) {
            q.insert("_id", doc! { "$nin": texts(keep) });
        }
        let command = doc! { "delete": self.collection.as_str(), "deletes": [{ "q": q, "limit": 0 }] };
        let mut conn = match self.conn(true, &command).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        Ok(Got::Done(Some(count(&conn.command(&self.db, command).await?))))
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let wanted = match filter {
            Some(f) => wanted_tokens(&conditions(f, StoreKind::Mongodb)?),
            None => Vec::new(),
        };
        let candidates = (k * CANDIDATES_EACH).clamp(100, MAX_CANDIDATES);
        let mut stage = doc! {
            "index": self.index.as_str(), "path": "embedding",
            "queryVector": vector.iter().map(|x| Bson::Double(*x)).collect::<Vec<_>>(),
            "numCandidates": i64::try_from(candidates).unwrap_or(100),
            "limit": i64::try_from(k).unwrap_or(1),
        };
        let parts: Vec<Document> = wanted.iter().map(|t| doc! { "tags": t.as_str() }).collect();
        match parts.len() {
            0 => {}
            1 => {
                stage.insert("filter", parts.into_iter().next().unwrap_or_default());
            }
            _ => {
                stage.insert("filter", doc! { "$and": parts });
            }
        }
        let command = doc! {
            "aggregate": self.collection.as_str(),
            "pipeline": [
                { "$vectorSearch": stage },
                { "$project": { "_id": 1, "source": 1, "position": 1, "text": 1, "metadata": 1, "score": { "$meta": "vectorSearchScore" } } },
            ],
            "cursor": { "batchSize": i64::try_from(k).unwrap_or(1) },
        };
        let mut conn = match self.conn(false, &command).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        let answer = conn.command(&self.db, command).await?;
        Ok(Got::Done(
            first_batch(&answer)
                .iter()
                .map(|d| {
                    let fields = to_json(&Bson::Document(d.clone()));
                    let s = fields.get("score").and_then(Json::as_f64).unwrap_or(f64::NAN);
                    Hit {
                        id: super::text_of(&fields, "_id"),
                        source: super::text_of(&fields, "source"),
                        position: fields.get("position").cloned().unwrap_or(Json::Null),
                        text: super::text_of(&fields, "text"),
                        metadata: fields.get("metadata").cloned().unwrap_or_else(|| json!({})),
                        distance: distance_of(self.distance, s),
                    }
                })
                .collect(),
        ))
    }
}

/// pgvector's distance from Atlas's score.
pub(crate) fn distance_of(distance: Distance, score: f64) -> f64 {
    match distance {
        // score = (1 + cosine) / 2, so the cosine distance is 1 - cosine = 2 - 2 * score.
        Distance::Cosine => 2.0 - 2.0 * score,
        // score = (1 + dot) / 2: the negative dot product is 1 - 2 * score.
        Distance::InnerProduct => 1.0 - 2.0 * score,
        // score = 1 / (1 + d).
        Distance::L2 => {
            if score > 0.0 {
                1.0 / score - 1.0
            } else {
                f64::INFINITY
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atlas_scores_are_turned_back_into_distances() {
        assert!((distance_of(Distance::Cosine, 0.875) - 0.25).abs() < 1e-12);
        assert!((distance_of(Distance::InnerProduct, 2.0) + 3.0).abs() < 1e-12);
        assert!((distance_of(Distance::L2, 0.2) - 4.0).abs() < 1e-12);
    }

    #[test]
    fn a_shown_command_gives_its_vectors_by_their_size() {
        let c = doc! { "aggregate": "pieces", "queryVector": (0..20).map(f64::from).collect::<Vec<_>>() };
        assert_eq!(
            shown("not sent", "shop", &c),
            json!({ "test": "not sent", "mongodb": { "database": "shop", "command": { "aggregate": "pieces", "queryVector": "<a vector of 20 numbers>" } } })
        );
    }
}
