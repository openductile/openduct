// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Chroma, through its HTTP API v2 (`https://<host>:8000`, a key in `x-chroma-token`; a tenant and a database other
//! than `default_tenant` and `default_database` as `tenant` and `database`).
//!
//! A collection is found by its name (`GET .../collections/<name>`), or made with the step's distance (`POST
//! .../collections`, `{name, configuration: {hnsw: {space}}, get_or_create: true}`); its records are reached by its
//! id. A piece is a record: its id, its vector, its text as the document, and metadata of its own (`piece_source`,
//! `piece_position`, the whole metadata as `piece_metadata`) beside the metadata's top fields a filter may use.
//! Records are written with `.../upsert`, found with `.../query`, and deleted with `.../delete` - by a `where` filter,
//! or (removing what a document no longer has) by the ids `.../get` finds for its source.
//!
//! A filter's conditions go as `where`: `{field: {$eq: value}}`, `{field: {$contains: value}}` for each value of a
//! list, joined by `$and`. Chroma gives the cosine distance, 1 minus the inner product, or the squared Euclidean
//! distance.

use std::sync::Mutex;

use serde_json::{Map, Value as Json, json};

use super::http::{Api, Reply, segment};
use super::{
    Cond, Got, Hit, Piece, StoreKind, Target, base_url, conditions, field, flat_fields, from_similarity,
    lock, metadata_back, odd, vector_json,
};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;

/// How many records one request writes, or one look for a document's ids reads; the most ids looked at.
const BATCH: usize = 100;
const PAGE: usize = 1_000;
const MAX_SCAN: usize = 20_000;

#[derive(Debug)]
pub(crate) struct Chroma<'a> {
    api: Api<'a>,
    /// `/api/v2/tenants/<tenant>/databases/<database>/collections`.
    collections: String,
    name: String,
    distance: Distance,
    /// The collection's id, once found.
    id: Mutex<Option<String>>,
}

impl<'a> Chroma<'a> {
    pub(crate) fn open(
        effects: &'a dyn Effects,
        credential: &Credential,
        target: &Target,
    ) -> Result<Self, String> {
        let mut headers = Vec::new();
        if let Some(key) = field(credential, "api_key") {
            headers.push(("x-chroma-token".to_owned(), key));
        }
        let tenant = field(credential, "tenant").unwrap_or_else(|| "default_tenant".to_owned());
        let database = field(credential, "database").unwrap_or_else(|| "default_database".to_owned());
        Ok(Self {
            api: Api {
                effects,
                base: base_url(credential, StoreKind::Chroma)?,
                headers,
                kind: StoreKind::Chroma,
            },
            collections: format!(
                "/api/v2/tenants/{}/databases/{}/collections",
                segment(&tenant),
                segment(&database)
            ),
            name: target.name.clone(),
            distance: target.distance,
            id: Mutex::new(None),
        })
    }

    /// The collection's id: found by its name, the first time.
    async fn id(&self) -> Result<Got<String>, String> {
        if let Some(id) = lock(&self.id).clone() {
            return Ok(Got::Done(id));
        }
        let path = format!("{}/{}", self.collections, segment(&self.name));
        let found = match self.api.send("GET", &path, None, true).await? {
            Reply::Shown(s) => return Ok(Got::Shown(s)),
            Reply::Got(404, _) => {
                return Err(format!(
                    "Chroma has no collection {}: make it, or add pieces with create: true",
                    self.name
                ));
            }
            reply => match self.api.checked(reply)? {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(c) => c,
            },
        };
        let id = super::text_of(&found, "id");
        if id.is_empty() {
            return Err(odd(StoreKind::Chroma, "the collection's id"));
        }
        *lock(&self.id) = Some(id.clone());
        Ok(Got::Done(id))
    }

    pub(crate) async fn ensure(&self) -> Result<Got<bool>, String> {
        let path = format!("{}/{}", self.collections, segment(&self.name));
        match self.api.send("GET", &path, None, true).await? {
            Reply::Shown(s) => return Ok(Got::Shown(s)),
            Reply::Got(404, _) => {}
            reply => {
                if let Got::Done(c) = self.api.checked(reply)? {
                    *lock(&self.id) = Some(super::text_of(&c, "id"));
                }
                return Ok(Got::Done(false));
            }
        }
        let space = match self.distance {
            Distance::Cosine => "cosine",
            Distance::L2 => "l2",
            Distance::InnerProduct => "ip",
        };
        let made = json!({ "name": self.name, "configuration": { "hnsw": { "space": space } }, "get_or_create": true });
        match self.api.ok("POST", &self.collections, Some(&made), false).await? {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(c) => {
                *lock(&self.id) = Some(super::text_of(&c, "id"));
                Ok(Got::Done(true))
            }
        }
    }

    fn records(&self, id: &str, what: &str) -> String {
        format!("{}/{}/{what}", self.collections, segment(id))
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let id = match self.id().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(id) => id,
        };
        let mut added = 0;
        for batch in pieces.chunks(BATCH) {
            let mut embeddings = Vec::with_capacity(batch.len());
            for p in batch {
                embeddings.push(vector_json(&p.vector)?);
            }
            let metadatas: Vec<Json> = batch
                .iter()
                .map(|p| {
                    let mut m = flat_fields(StoreKind::Chroma, &p.metadata);
                    m.insert("piece_source".to_owned(), json!(p.source));
                    m.insert("piece_position".to_owned(), json!(p.position));
                    m.insert("piece_metadata".to_owned(), json!(p.metadata.to_string()));
                    Json::Object(m)
                })
                .collect();
            let body = json!({
                "ids": batch.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(),
                "embeddings": embeddings,
                "documents": batch.iter().map(|p| p.text.as_str()).collect::<Vec<_>>(),
                "metadatas": metadatas,
            });
            if let Got::Shown(s) = self
                .api
                .ok("POST", &self.records(&id, "upsert"), Some(&body), false)
                .await?
            {
                return Ok(Got::Shown(s));
            }
            added += batch.len() as u64;
        }
        Ok(Got::Done(added))
    }

    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let id = match self.id().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(id) => id,
        };
        let of_sources = json!({ "piece_source": { "$in": sources } });
        let Some(keep) = keep else {
            return match self
                .api
                .ok(
                    "POST",
                    &self.records(&id, "delete"),
                    Some(&json!({ "where": of_sources })),
                    false,
                )
                .await?
            {
                Got::Shown(s) => Ok(Got::Shown(s)),
                Got::Done(d) => Ok(Got::Done(d.get("deleted").and_then(Json::as_u64))),
            };
        };
        // Chroma filters records by their metadata, not by their ids: the source's ids are read, and the others
        // than those kept deleted by id.
        let mut stale: Vec<String> = Vec::new();
        let mut offset = 0;
        while offset < MAX_SCAN {
            let body = json!({ "where": of_sources, "limit": PAGE, "offset": offset, "include": [] });
            let page = match self
                .api
                .ok("POST", &self.records(&id, "get"), Some(&body), true)
                .await?
            {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(p) => p,
            };
            let ids: Vec<String> = page
                .get("ids")
                .and_then(Json::as_array)
                .map(|ids| ids.iter().filter_map(Json::as_str).map(str::to_owned).collect())
                .unwrap_or_default();
            let count = ids.len();
            stale.extend(ids.into_iter().filter(|i| !keep.contains(&i.as_str())));
            if count < PAGE {
                break;
            }
            offset += PAGE;
        }
        let mut removed = 0;
        for batch in stale.chunks(PAGE) {
            match self
                .api
                .ok(
                    "POST",
                    &self.records(&id, "delete"),
                    Some(&json!({ "ids": batch })),
                    false,
                )
                .await?
            {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(d) => {
                    removed += d
                        .get("deleted")
                        .and_then(Json::as_u64)
                        .unwrap_or(batch.len() as u64)
                }
            }
        }
        Ok(Got::Done(Some(removed)))
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let wanted = filter.map(|f| conditions(f, StoreKind::Chroma)).transpose()?;
        let id = match self.id().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(id) => id,
        };
        let mut body = json!({
            "query_embeddings": [vector_json(vector)?], "n_results": k, "include": ["documents", "metadatas", "distances"],
        });
        if let (Some(c), Some(b)) = (wanted.as_deref().filter(|c| !c.is_empty()), body.as_object_mut()) {
            b.insert("where".to_owned(), where_of(c));
        }
        let answer = match self
            .api
            .ok("POST", &self.records(&id, "query"), Some(&body), true)
            .await?
        {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(a) => a,
        };
        let first = |name: &str| {
            answer
                .pointer(&format!("/{name}/0"))
                .and_then(Json::as_array)
                .cloned()
        };
        let ids = first("ids").ok_or_else(|| odd(StoreKind::Chroma, "the ids found"))?;
        let documents = first("documents").unwrap_or_default();
        let metadatas = first("metadatas").unwrap_or_default();
        let distances = first("distances").unwrap_or_default();
        let hits = ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let m = metadatas.get(i).cloned().unwrap_or(Json::Null);
                let raw = distances.get(i).and_then(Json::as_f64).unwrap_or(f64::NAN);
                let distance = match self.distance {
                    Distance::Cosine => raw,
                    Distance::InnerProduct => raw - 1.0,
                    Distance::L2 => from_similarity(Distance::L2, raw),
                };
                Hit {
                    id: id.as_str().map(str::to_owned).unwrap_or_default(),
                    source: super::text_of(&m, "piece_source"),
                    position: m.get("piece_position").cloned().unwrap_or(Json::Null),
                    text: documents
                        .get(i)
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    metadata: metadata_back(m.get("piece_metadata"), m.as_object()),
                    distance,
                }
            })
            .collect();
        Ok(Got::Done(hits))
    }
}

/// A filter's conditions as Chroma's `where`.
pub(crate) fn where_of(conditions: &[Cond]) -> Json {
    let mut parts: Vec<Json> = Vec::new();
    for c in conditions {
        let name = c.path().join(".");
        match c {
            Cond::Is(_, s) => parts.push(one(&name, "$eq", s.json())),
            Cond::HoldsAll(_, values) => {
                parts.extend(values.iter().map(|s| one(&name, "$contains", s.json())))
            }
        }
    }
    if parts.len() == 1 {
        return parts.pop().unwrap_or(Json::Null);
    }
    json!({ "$and": parts })
}

fn one(name: &str, op: &str, value: Json) -> Json {
    let mut inner = Map::new();
    inner.insert(op.to_owned(), value);
    let mut outer = Map::new();
    outer.insert(name.to_owned(), Json::Object(inner));
    Json::Object(outer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_goes_as_chroma_s_where() {
        let c = conditions(
            &json!({ "team": "sales", "tags": ["faq", "top"] }),
            StoreKind::Chroma,
        )
        .unwrap();
        assert_eq!(
            where_of(&c),
            json!({ "$and": [
                { "team": { "$eq": "sales" } },
                { "tags": { "$contains": "faq" } },
                { "tags": { "$contains": "top" } },
            ] })
        );
        let one = conditions(&json!({ "year": 2020 }), StoreKind::Chroma).unwrap();
        assert_eq!(where_of(&one), json!({ "year": { "$eq": 2020 } }));
    }
}
