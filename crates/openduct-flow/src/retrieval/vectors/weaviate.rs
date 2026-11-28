// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Weaviate, through its REST API (`https://<cluster>`, a key as `Authorization: Bearer`), with vectors brought by the
//! step (`vectorizer: none`).
//!
//! A collection (`POST /v1/schema`; Weaviate names it with a capital first letter) has the piece's own properties -
//! `piece_id`, `piece_source` (both matched whole: `tokenization: field`), `piece_position`, `piece_text`, and
//! `piece_metadata` (the whole metadata as JSON text) - and one property for each top field of the metadata a filter
//! may use (text, numbers, true or false, or lists of one of them): those the first pieces have are made with it,
//! whole-value text; Weaviate makes later ones itself, splitting their text into words. Objects are written with
//! `POST /v1/batch/objects`, each with a UUID made from the piece's id; deleted with `DELETE /v1/batch/objects` and a
//! `where` filter. A search is `POST /v1/search/<Collection>/near-vector` (Weaviate 1.39.8 and later); a Weaviate that
//! has no such route is asked through GraphQL's `nearVector` instead.
//!
//! A filter's conditions go as `where` operands joined by `And`: `Equal` with `valueText`, `valueNumber` or
//! `valueBoolean`; `ContainsAll` with a list. Weaviate's distance is the cosine distance, the negative dot product, or
//! the squared Euclidean distance.

use serde_json::{Value as Json, json};

use super::http::{Api, Reply, segment};
use super::{
    Cond, Got, Hit, Piece, Scalar, StoreKind, Target, base_url, conditions, field, flat_fields,
    from_similarity, metadata_back, odd, uuid_of, vector_json,
};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;

/// How many objects one batch writes; the most one delete removes (Weaviate's own default), and how many times a
/// delete is repeated for more.
const BATCH: usize = 100;
const DELETE_LIMIT: u64 = 10_000;
const DELETE_ROUNDS: usize = 10;
const OWN: [&str; 5] = [
    "piece_id",
    "piece_source",
    "piece_position",
    "piece_text",
    "piece_metadata",
];

#[derive(Debug)]
pub(crate) struct Weaviate<'a> {
    api: Api<'a>,
    class: String,
    distance: Distance,
}

impl<'a> Weaviate<'a> {
    pub(crate) fn open(
        effects: &'a dyn Effects,
        credential: &Credential,
        target: &Target,
    ) -> Result<Self, String> {
        let mut headers = Vec::new();
        if let Some(key) = field(credential, "api_key") {
            headers.push(("authorization".to_owned(), format!("Bearer {key}")));
        }
        let mut class: Vec<char> = target.name.chars().collect();
        if let Some(first) = class.first_mut() {
            *first = first.to_ascii_uppercase();
        }
        Ok(Self {
            api: Api {
                effects,
                base: base_url(credential, StoreKind::Weaviate)?,
                headers,
                kind: StoreKind::Weaviate,
            },
            class: segment(&class.into_iter().collect::<String>()),
            distance: target.distance,
        })
    }

    pub(crate) async fn ensure(&self, pieces: &[Piece]) -> Result<Got<bool>, String> {
        match self
            .api
            .send("GET", &format!("/v1/schema/{}", self.class), None, true)
            .await?
        {
            Reply::Shown(s) => return Ok(Got::Shown(s)),
            Reply::Got(404, _) => {}
            reply => {
                self.api.checked(reply)?;
                return Ok(Got::Done(false));
            }
        }
        let distance = match self.distance {
            Distance::Cosine => "cosine",
            Distance::L2 => "l2-squared",
            Distance::InnerProduct => "dot",
        };
        let whole =
            |name: &str, kind: &str| json!({ "name": name, "dataType": [kind], "tokenization": "field" });
        let mut properties = vec![
            whole("piece_id", "text"),
            whole("piece_source", "text"),
            json!({ "name": "piece_position", "dataType": ["int"] }),
            json!({ "name": "piece_text", "dataType": ["text"] }),
            json!({ "name": "piece_metadata", "dataType": ["text"], "indexFilterable": false, "indexSearchable": false }),
        ];
        let mut named: Vec<String> = Vec::new();
        for p in pieces {
            for (k, v) in flat_fields(StoreKind::Weaviate, &p.metadata) {
                if named.contains(&k) || named.len() >= 50 {
                    continue;
                }
                let (kind, text) = match &v {
                    Json::String(_) => ("text", true),
                    Json::Number(_) => ("number", false),
                    Json::Bool(_) => ("boolean", false),
                    Json::Array(items) => match items.first() {
                        Some(Json::String(_)) => ("text[]", true),
                        Some(Json::Number(_)) => ("number[]", false),
                        _ => ("boolean[]", false),
                    },
                    _ => continue,
                };
                properties.push(if text {
                    whole(&k, kind)
                } else {
                    json!({ "name": k, "dataType": [kind] })
                });
                named.push(k);
            }
        }
        let made = json!({
            "class": self.class, "vectorizer": "none", "vectorIndexConfig": { "distance": distance },
            "properties": properties,
        });
        match self.api.ok("POST", "/v1/schema", Some(&made), false).await? {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(_) => Ok(Got::Done(true)),
        }
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let mut added = 0;
        for batch in pieces.chunks(BATCH) {
            let objects = batch
                .iter()
                .map(|p| {
                    let mut properties = flat_fields(StoreKind::Weaviate, &p.metadata);
                    properties.insert("piece_id".to_owned(), json!(p.id));
                    properties.insert("piece_source".to_owned(), json!(p.source));
                    properties.insert("piece_position".to_owned(), json!(p.position));
                    properties.insert("piece_text".to_owned(), json!(p.text));
                    properties.insert("piece_metadata".to_owned(), json!(p.metadata.to_string()));
                    Ok(json!({ "class": self.class, "id": uuid_of(&p.id), "vector": vector_json(&p.vector)?, "properties": properties }))
                })
                .collect::<Result<Vec<Json>, String>>()?;
            let answer = match self
                .api
                .ok(
                    "POST",
                    "/v1/batch/objects",
                    Some(&json!({ "objects": objects })),
                    false,
                )
                .await?
            {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(a) => a,
            };
            let results = answer
                .as_array()
                .ok_or_else(|| odd(StoreKind::Weaviate, "a result for each object"))?;
            for r in results {
                if let Some(message) = r.pointer("/result/errors/error/0/message").and_then(Json::as_str) {
                    return Err(format!(
                        "Weaviate refused a piece: {}",
                        message.chars().take(300).collect::<String>()
                    ));
                }
                added += 1;
            }
        }
        Ok(Got::Done(added))
    }

    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let mut operands =
            vec![json!({ "path": ["piece_source"], "operator": "ContainsAny", "valueTextArray": sources })];
        if let Some(keep) = keep.filter(|k| !k.is_empty()) {
            operands
                .push(json!({ "path": ["piece_id"], "operator": "ContainsNone", "valueTextArray": keep }));
        }
        let body =
            json!({ "match": { "class": self.class, "where": joined(operands) }, "output": "minimal" });
        let mut removed = 0;
        for _ in 0..DELETE_ROUNDS {
            let answer = match self
                .api
                .ok("DELETE", "/v1/batch/objects", Some(&body), false)
                .await?
            {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(a) => a,
            };
            let done = answer
                .pointer("/results/successful")
                .and_then(Json::as_u64)
                .unwrap_or(0);
            let matched = answer
                .pointer("/results/matches")
                .and_then(Json::as_u64)
                .unwrap_or(0);
            let limit = answer
                .pointer("/results/limit")
                .and_then(Json::as_u64)
                .unwrap_or(DELETE_LIMIT);
            removed += done;
            if matched < limit || done == 0 {
                break;
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
        let filter = filter.map(|f| conditions(f, StoreKind::Weaviate)).transpose()?;
        let filter = filter.as_deref().filter(|c| !c.is_empty()).map(where_of);
        let mut body = json!({
            "vector": vector_json(vector)?, "limit": k, "returnProperties": OWN, "returnMetadata": ["distance"],
        });
        if let (Some(w), Some(b)) = (&filter, body.as_object_mut()) {
            b.insert("where".to_owned(), w.clone());
        }
        let path = format!("/v1/search/{}/near-vector", self.class);
        let results = match self.api.send("POST", &path, Some(&body), true).await? {
            Reply::Shown(s) => return Ok(Got::Shown(s)),
            // A Weaviate before the search route: GraphQL, as it has always answered.
            Reply::Got(404 | 405, _) => return self.graphql(vector, k, filter.as_ref()).await,
            reply => match self.api.checked(reply)? {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(a) => a
                    .get("results")
                    .and_then(Json::as_array)
                    .cloned()
                    .ok_or_else(|| odd(StoreKind::Weaviate, "its results"))?,
            },
        };
        Ok(Got::Done(
            results
                .iter()
                .map(|r| {
                    let properties = r.get("properties").cloned().unwrap_or(Json::Null);
                    let distance = r
                        .pointer("/metadata/distance")
                        .and_then(Json::as_f64)
                        .unwrap_or(f64::NAN);
                    self.hit(&properties, distance)
                })
                .collect(),
        ))
    }

    async fn graphql(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let mut args = format!("nearVector: {{vector: {}}}, limit: {k}", vector_json(vector)?);
        if let Some(w) = filter {
            args.push_str(", where: ");
            args.push_str(&graphql_value(w));
        }
        let query = format!(
            "{{ Get {{ {}({args}) {{ {} _additional {{ distance }} }} }} }}",
            self.class,
            OWN.join(" ")
        );
        let answer = match self
            .api
            .ok("POST", "/v1/graphql", Some(&json!({ "query": query })), true)
            .await?
        {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(a) => a,
        };
        if let Some(message) = answer.pointer("/errors/0/message").and_then(Json::as_str) {
            return Err(format!(
                "Weaviate refused the search: {}",
                message.chars().take(300).collect::<String>()
            ));
        }
        let found = answer
            .pointer(&format!("/data/Get/{}", self.class))
            .and_then(Json::as_array)
            .ok_or_else(|| odd(StoreKind::Weaviate, "its results"))?;
        Ok(Got::Done(
            found
                .iter()
                .map(|r| {
                    let distance = r
                        .pointer("/_additional/distance")
                        .and_then(Json::as_f64)
                        .unwrap_or(f64::NAN);
                    self.hit(r, distance)
                })
                .collect(),
        ))
    }

    fn hit(&self, properties: &Json, distance: f64) -> Hit {
        let distance = match self.distance {
            Distance::L2 => from_similarity(Distance::L2, distance),
            // The cosine distance, and the negative dot product, are pgvector's own.
            Distance::Cosine | Distance::InnerProduct => distance,
        };
        Hit {
            id: super::text_of(properties, "piece_id"),
            source: super::text_of(properties, "piece_source"),
            position: properties.get("piece_position").cloned().unwrap_or(Json::Null),
            text: super::text_of(properties, "piece_text"),
            metadata: metadata_back(properties.get("piece_metadata"), properties.as_object()),
            distance,
        }
    }
}

/// Operands joined by `And` (one alone as it is).
fn joined(mut operands: Vec<Json>) -> Json {
    if operands.len() == 1 {
        return operands.pop().unwrap_or(Json::Null);
    }
    json!({ "operator": "And", "operands": operands })
}

/// A filter's conditions as a search's `where` - the JSON GraphQL takes, which Weaviate's search route takes too: a
/// list's values in the field of their kind (`valueText: [...]`). (A batch delete's `where` names lists' fields
/// `valueTextArray`, as its REST form does.)
pub(crate) fn where_of(conditions: &[Cond]) -> Json {
    let operands = conditions
        .iter()
        .map(|c| {
            let path = json!([c.path().join(".")]);
            match c {
                Cond::Is(_, s) => json!({ "path": path, "operator": "Equal", value_field(s): s.json() }),
                Cond::HoldsAll(_, values) => {
                    let name = values.first().map_or("valueText", value_field);
                    let list: Vec<Json> = values.iter().map(Scalar::json).collect();
                    json!({ "path": path, "operator": "ContainsAll", name: list })
                }
            }
        })
        .collect();
    joined(operands)
}

fn value_field(s: &Scalar) -> &'static str {
    match s {
        Scalar::Text(_) => "valueText",
        Scalar::Number(_) => "valueNumber",
        Scalar::Bool(_) => "valueBoolean",
    }
}

/// A `where` filter as a GraphQL value: names bare, an operator as the enum it is, a list's field by GraphQL's own
/// name (`valueText: [...]`), every text as a quoted string.
fn graphql_value(value: &Json) -> String {
    match value {
        Json::Object(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|(name, v)| {
                    let shown = match (name.as_str(), v) {
                        ("operator", Json::String(op)) if op.chars().all(|c| c.is_ascii_alphabetic()) => {
                            op.clone()
                        }
                        _ => graphql_value(v),
                    };
                    format!("{name}: {shown}")
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        Json::Array(items) => format!(
            "[{}]",
            items.iter().map(graphql_value).collect::<Vec<_>>().join(", ")
        ),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_goes_as_weaviate_s_where() {
        let c = conditions(
            &json!({ "team": "sales", "year": 2020, "tags": ["faq", "top"] }),
            StoreKind::Weaviate,
        )
        .unwrap();
        let w = where_of(&c);
        assert_eq!(
            w,
            json!({ "operator": "And", "operands": [
                { "path": ["team"], "operator": "Equal", "valueText": "sales" },
                { "path": ["year"], "operator": "Equal", "valueNumber": 2020 },
                { "path": ["tags"], "operator": "ContainsAll", "valueText": ["faq", "top"] },
            ] })
        );
        assert_eq!(
            graphql_value(&w),
            "{operator: And, operands: [{path: [\"team\"], operator: Equal, valueText: \"sales\"}, {path: [\"year\"], \
             operator: Equal, valueNumber: 2020}, {path: [\"tags\"], operator: ContainsAll, valueText: [\"faq\", \"top\"]}]}"
        );
        let one = where_of(&conditions(&json!({ "vip": true }), StoreKind::Weaviate).unwrap());
        assert_eq!(
            one,
            json!({ "path": ["vip"], "operator": "Equal", "valueBoolean": true })
        );
        // A text that tries to end the string is written as the quoted string it is.
        assert_eq!(
            graphql_value(&json!({ "valueText": "a\"}) { x" })),
            "{valueText: \"a\\\"}) { x\"}"
        );
    }
}
