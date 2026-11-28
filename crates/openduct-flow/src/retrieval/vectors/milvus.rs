// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Milvus, and Zilliz Cloud, through Milvus's RESTful API v2 (`https://<host>:19530`, a token - `user:password`, or an
//! API key - as `Authorization: Bearer`; a database other than the default as `database`).
//!
//! A collection (`POST /v2/vectordb/collections/create`) has the fields `id` (text, the piece's id, its primary key),
//! `vector`, `source`, `position`, `text` and `metadata` (JSON), an index on the vector for the step's distance
//! (`COSINE`, `L2` or `IP`), and is loaded. Pieces are written with `entities/upsert`, found with `entities/search`
//! and deleted with `entities/delete` and a filter. Every answer carries a `code`: anything but 0 is a refusal.
//!
//! A filter is a Milvus expression on the JSON field - `metadata["team"] == {v0}`, `json_contains_all(metadata["tags"],
//! {v1})`, joined by `and` - whose values go apart from it, as templating's `exprParams` (Milvus 2.5 and later), so
//! no value is written into the expression. Milvus gives the cosine similarity, the inner product, or the squared
//! Euclidean distance.

use serde_json::{Map, Value as Json, json};

use super::http::Api;
use super::{
    Cond, Got, Hit, Piece, Scalar, StoreKind, Target, base_url, conditions, field, from_similarity, odd,
    vector_json,
};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;

/// How many entities one request writes.
const BATCH: usize = 100;
const FIELDS: [&str; 5] = ["id", "source", "position", "text", "metadata"];

#[derive(Debug)]
pub(crate) struct Milvus<'a> {
    api: Api<'a>,
    collection: String,
    database: Option<String>,
    distance: Distance,
}

impl<'a> Milvus<'a> {
    pub(crate) fn open(
        effects: &'a dyn Effects,
        credential: &Credential,
        target: &Target,
    ) -> Result<Self, String> {
        let mut headers = Vec::new();
        if let Some(token) = field(credential, "token") {
            headers.push(("authorization".to_owned(), format!("Bearer {token}")));
        }
        Ok(Self {
            api: Api {
                effects,
                base: base_url(credential, StoreKind::Milvus)?,
                headers,
                kind: StoreKind::Milvus,
            },
            collection: target.name.clone(),
            database: field(credential, "database"),
            distance: target.distance,
        })
    }

    fn metric(&self) -> &'static str {
        match self.distance {
            Distance::Cosine => "COSINE",
            Distance::L2 => "L2",
            Distance::InnerProduct => "IP",
        }
    }

    /// A request's body: the collection (and database), and `more`.
    fn body(&self, more: Json) -> Json {
        let mut out = Map::new();
        if let Some(db) = &self.database {
            out.insert("dbName".to_owned(), json!(db));
        }
        out.insert("collectionName".to_owned(), json!(self.collection));
        if let Json::Object(more) = more {
            out.extend(more);
        }
        Json::Object(out)
    }

    /// Sends a request; its `data`, refused when its `code` is not 0.
    async fn call(&self, path: &str, more: Json, read: bool) -> Result<Got<Json>, String> {
        match self.api.ok("POST", path, Some(&self.body(more)), read).await? {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(answer) => {
                let code = answer.get("code").and_then(Json::as_i64).unwrap_or(0);
                if code != 0 {
                    return Err(format!("Milvus refused ({code}): {}", super::http::said(&answer)));
                }
                Ok(Got::Done(answer.get("data").cloned().unwrap_or(Json::Null)))
            }
        }
    }

    pub(crate) async fn ensure(&self, dimensions: u32) -> Result<Got<bool>, String> {
        match self.call("/v2/vectordb/collections/has", json!({}), true).await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(data) if data.get("has").and_then(Json::as_bool) == Some(true) => {
                return Ok(Got::Done(false));
            }
            Got::Done(_) => {}
        }
        let text = |name: &str, most: u32| json!({ "fieldName": name, "dataType": "VarChar", "elementTypeParams": { "max_length": most } });
        let schema = json!({
            "autoID": false,
            "enableDynamicField": false,
            "fields": [
                { "fieldName": "id", "dataType": "VarChar", "isPrimary": true, "elementTypeParams": { "max_length": 512 } },
                { "fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": { "dim": dimensions } },
                text("source", 1024),
                { "fieldName": "position", "dataType": "Int64" },
                text("text", 65_535),
                { "fieldName": "metadata", "dataType": "JSON" },
            ],
        });
        let index = json!([{
            "fieldName": "vector", "indexName": "vector_index", "metricType": self.metric(),
            "params": { "index_type": "AUTOINDEX" },
        }]);
        let made = json!({ "schema": schema, "indexParams": index });
        if let Got::Shown(s) = self.call("/v2/vectordb/collections/create", made, false).await? {
            return Ok(Got::Shown(s));
        }
        match self
            .call("/v2/vectordb/collections/load", json!({}), false)
            .await?
        {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(_) => Ok(Got::Done(true)),
        }
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let mut added = 0;
        for batch in pieces.chunks(BATCH) {
            let data = batch
                .iter()
                .map(|p| {
                    Ok(json!({
                        "id": p.id, "vector": vector_json(&p.vector)?, "source": p.source, "position": p.position,
                        "text": p.text, "metadata": p.metadata,
                    }))
                })
                .collect::<Result<Vec<Json>, String>>()?;
            match self
                .call("/v2/vectordb/entities/upsert", json!({ "data": data }), false)
                .await?
            {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(d) => {
                    added += d
                        .get("upsertCount")
                        .and_then(Json::as_u64)
                        .unwrap_or(batch.len() as u64)
                }
            }
        }
        Ok(Got::Done(added))
    }

    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let mut filter = "source in {sources}".to_owned();
        let mut params = json!({ "sources": sources });
        if let (Some(keep), Some(p)) = (keep.filter(|k| !k.is_empty()), params.as_object_mut()) {
            filter.push_str(" and id not in {keep}");
            p.insert("keep".to_owned(), json!(keep));
        }
        match self
            .call(
                "/v2/vectordb/entities/delete",
                json!({ "filter": filter, "exprParams": params }),
                false,
            )
            .await?
        {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(d) => Ok(Got::Done(d.get("deleteCount").and_then(Json::as_u64))),
        }
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let mut more = json!({
            "data": [vector_json(vector)?], "annsField": "vector", "limit": k, "outputFields": FIELDS,
            "searchParams": { "metricType": self.metric() },
        });
        if let Some(f) = filter {
            let (expression, params) = expression_of(&conditions(f, StoreKind::Milvus)?);
            if let (false, Some(m)) = (expression.is_empty(), more.as_object_mut()) {
                m.insert("filter".to_owned(), json!(expression));
                m.insert("exprParams".to_owned(), params);
            }
        }
        let data = match self.call("/v2/vectordb/entities/search", more, true).await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(d) => d,
        };
        let found = data
            .as_array()
            .ok_or_else(|| odd(StoreKind::Milvus, "what it found"))?;
        Ok(Got::Done(
            found
                .iter()
                .map(|e| {
                    // Output fields come beside `id` and `distance`, or (in some versions) inside `entity`.
                    let fields = e.get("entity").filter(|x| x.is_object()).unwrap_or(e);
                    let raw = e.get("distance").and_then(Json::as_f64).unwrap_or(f64::NAN);
                    Hit {
                        id: super::text_of(e, "id"),
                        source: super::text_of(fields, "source"),
                        position: fields.get("position").cloned().unwrap_or(Json::Null),
                        text: super::text_of(fields, "text"),
                        metadata: fields.get("metadata").cloned().unwrap_or_else(|| json!({})),
                        distance: from_similarity(self.distance, raw),
                    }
                })
                .collect(),
        ))
    }
}

/// A filter's conditions as a Milvus expression on the `metadata` JSON field, its values apart: `{v0}`, `{v1}` ...
pub(crate) fn expression_of(conditions: &[Cond]) -> (String, Json) {
    let mut parts = Vec::new();
    let mut params = Map::new();
    for (i, c) in conditions.iter().enumerate() {
        // Field names are checked: letters, digits, _ and - only, so nothing in them ends the quoted key.
        let field: String = std::iter::once("metadata".to_owned())
            .chain(c.path().iter().map(|k| format!("[\"{k}\"]")))
            .collect();
        let name = format!("v{i}");
        match c {
            Cond::Is(_, s) => {
                parts.push(format!("{field} == {{{name}}}"));
                params.insert(name, s.json());
            }
            Cond::HoldsAll(_, values) => {
                parts.push(format!("json_contains_all({field}, {{{name}}})"));
                params.insert(name, Json::Array(values.iter().map(Scalar::json).collect()));
            }
        }
    }
    (parts.join(" and "), Json::Object(params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_goes_as_a_milvus_expression_with_its_values_apart() {
        let c = conditions(
            &json!({ "team": "sales\" or 1 == 1", "tags": ["faq"], "author": { "id": 7 } }),
            StoreKind::Milvus,
        )
        .unwrap();
        let (expression, params) = expression_of(&c);
        assert_eq!(
            expression,
            "metadata[\"team\"] == {v0} and json_contains_all(metadata[\"tags\"], {v1}) and metadata[\"author\"][\"id\"] == {v2}"
        );
        assert_eq!(
            params,
            json!({ "v0": "sales\" or 1 == 1", "v1": ["faq"], "v2": 7 })
        );
    }
}
