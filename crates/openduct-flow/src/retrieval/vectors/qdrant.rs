// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Qdrant, through its REST API (`https://<cluster>:6333`, a key in the `api-key` header).
//!
//! A collection holds one unnamed vector a point (`PUT /collections/<c>`, `{vectors: {size, distance}}`: `Cosine`,
//! `Euclid` or `Dot`), with keyword indexes on `source` and `piece_id` to filter by. A piece is a point whose id is
//! a UUID made from the piece's id, its payload `{piece_id, source, position, text, metadata}`. Points are written
//! with `PUT .../points?wait=true`, found with `POST .../points/query` (`{query, limit, filter, with_payload}`), and
//! deleted with `POST .../points/delete` and a filter - counted first with `POST .../points/count`.
//!
//! A filter's conditions go under `must`, each `{key: "metadata.<field>", match: {value}}` (Qdrant matches a list
//! field when one of its values is equal); a number that is not whole goes as a range from it to it, as `match` takes
//! whole numbers, text and true or false only. Qdrant's score is the cosine similarity, the dot product, or the
//! Euclidean distance itself.

use serde_json::{Value as Json, json};

use super::http::{Api, segment};
use super::{
    Cond, Got, Hit, Piece, Scalar, StoreKind, Target, base_url, conditions, field, odd, uuid_of, vector_json,
};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;

/// How many points one request writes.
const BATCH: usize = 100;

#[derive(Debug)]
pub(crate) struct Qdrant<'a> {
    api: Api<'a>,
    collection: String,
    distance: Distance,
}

impl<'a> Qdrant<'a> {
    pub(crate) fn open(
        effects: &'a dyn Effects,
        credential: &Credential,
        target: &Target,
    ) -> Result<Self, String> {
        let mut headers = Vec::new();
        if let Some(key) = field(credential, "api_key") {
            headers.push(("api-key".to_owned(), key));
        }
        Ok(Self {
            api: Api {
                effects,
                base: base_url(credential, StoreKind::Qdrant)?,
                headers,
                kind: StoreKind::Qdrant,
            },
            collection: segment(&target.name),
            distance: target.distance,
        })
    }

    fn path(&self, rest: &str) -> String {
        format!("/collections/{}{rest}", self.collection)
    }

    pub(crate) async fn ensure(&self, dimensions: u32) -> Result<Got<bool>, String> {
        let there = match self.api.ok("GET", &self.path("/exists"), None, true).await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(body) => body
                .pointer("/result/exists")
                .and_then(Json::as_bool)
                .ok_or_else(|| odd(StoreKind::Qdrant, "whether the collection exists"))?,
        };
        if there {
            return Ok(Got::Done(false));
        }
        let distance = match self.distance {
            Distance::Cosine => "Cosine",
            Distance::L2 => "Euclid",
            Distance::InnerProduct => "Dot",
        };
        let made = json!({ "vectors": { "size": dimensions, "distance": distance } });
        if let Got::Shown(s) = self.api.ok("PUT", &self.path(""), Some(&made), false).await? {
            return Ok(Got::Shown(s));
        }
        for name in ["source", "piece_id"] {
            let index = json!({ "field_name": name, "field_schema": "keyword" });
            if let Got::Shown(s) = self
                .api
                .ok("PUT", &self.path("/index?wait=true"), Some(&index), false)
                .await?
            {
                return Ok(Got::Shown(s));
            }
        }
        Ok(Got::Done(true))
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let mut added = 0;
        for batch in pieces.chunks(BATCH) {
            let points = batch
                .iter()
                .map(|p| {
                    Ok(json!({
                        "id": uuid_of(&p.id),
                        "vector": vector_json(&p.vector)?,
                        "payload": { "piece_id": p.id, "source": p.source, "position": p.position, "text": p.text, "metadata": p.metadata },
                    }))
                })
                .collect::<Result<Vec<Json>, String>>()?;
            let body = json!({ "points": points });
            if let Got::Shown(s) = self
                .api
                .ok("PUT", &self.path("/points?wait=true"), Some(&body), false)
                .await?
            {
                return Ok(Got::Shown(s));
            }
            added += batch.len() as u64;
        }
        Ok(Got::Done(added))
    }

    /// Removes the pieces of `sources` (but those in `keep`): counted, then deleted.
    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let mut filter = json!({ "must": [{ "key": "source", "match": { "any": sources } }] });
        if let (Some(keep), Some(f)) = (keep, filter.as_object_mut())
            && !keep.is_empty()
        {
            let ids: Vec<String> = keep.iter().map(|id| uuid_of(id)).collect();
            f.insert("must_not".to_owned(), json!([{ "has_id": ids }]));
        }
        let counted = match self
            .api
            .ok(
                "POST",
                &self.path("/points/count"),
                Some(&json!({ "filter": filter, "exact": true })),
                true,
            )
            .await?
        {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(body) => body.pointer("/result/count").and_then(Json::as_u64),
        };
        if counted == Some(0) {
            return Ok(Got::Done(Some(0)));
        }
        match self
            .api
            .ok(
                "POST",
                &self.path("/points/delete?wait=true"),
                Some(&json!({ "filter": filter })),
                false,
            )
            .await?
        {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(_) => Ok(Got::Done(counted)),
        }
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let mut body = json!({ "query": vector_json(vector)?, "limit": k, "with_payload": true });
        if let (Some(f), Some(b)) = (filter, body.as_object_mut()) {
            b.insert("filter".to_owned(), filter_of(&conditions(f, StoreKind::Qdrant)?));
        }
        let answer = match self
            .api
            .ok("POST", &self.path("/points/query"), Some(&body), true)
            .await?
        {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(a) => a,
        };
        let points = answer
            .pointer("/result/points")
            .and_then(Json::as_array)
            .ok_or_else(|| odd(StoreKind::Qdrant, "the points found"))?;
        let hits = points
            .iter()
            .map(|p| {
                let payload = p.get("payload").cloned().unwrap_or(Json::Null);
                let score = p.get("score").and_then(Json::as_f64).unwrap_or(f64::NAN);
                let distance = match self.distance {
                    Distance::Cosine => 1.0 - score,
                    Distance::InnerProduct => -score,
                    Distance::L2 => score,
                };
                Hit {
                    id: super::text_of(&payload, "piece_id"),
                    source: super::text_of(&payload, "source"),
                    position: payload.get("position").cloned().unwrap_or(Json::Null),
                    text: super::text_of(&payload, "text"),
                    metadata: payload.get("metadata").cloned().unwrap_or_else(|| json!({})),
                    distance,
                }
            })
            .collect();
        Ok(Got::Done(hits))
    }
}

/// A filter's conditions as Qdrant's: every one under `must`.
pub(crate) fn filter_of(conditions: &[Cond]) -> Json {
    let mut must = Vec::new();
    for c in conditions {
        let key = format!("metadata.{}", c.path().join("."));
        let values: Vec<&Scalar> = match c {
            Cond::Is(_, s) => vec![s],
            Cond::HoldsAll(_, values) => values.iter().collect(),
        };
        for s in values {
            must.push(match s {
                Scalar::Number(n) if n.fract() != 0.0 => {
                    json!({ "key": key, "range": { "gte": n, "lte": n } })
                }
                other => json!({ "key": key, "match": { "value": other.json() } }),
            });
        }
    }
    json!({ "must": must })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_goes_as_qdrant_s_must_conditions() {
        let c = conditions(
            &json!({ "team": "sales", "tags": ["faq", "top"], "score": 0.5, "year": 2020, "author": { "vip": true } }),
            StoreKind::Qdrant,
        )
        .unwrap();
        assert_eq!(
            filter_of(&c),
            json!({ "must": [
                { "key": "metadata.team", "match": { "value": "sales" } },
                { "key": "metadata.tags", "match": { "value": "faq" } },
                { "key": "metadata.tags", "match": { "value": "top" } },
                { "key": "metadata.score", "range": { "gte": 0.5, "lte": 0.5 } },
                { "key": "metadata.year", "match": { "value": 2020 } },
                { "key": "metadata.author.vip", "match": { "value": true } },
            ] })
        );
    }
}
