// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Pinecone, through its REST API, version 2026-07 (`Api-Key` and `X-Pinecone-Api-Version` headers on every request).
//!
//! An index is found on the control plane (`GET https://api.pinecone.io/indexes/<name>`), which says the address of
//! its own host; made, when asked, as a serverless index with one dense vector field (`POST /indexes`, `schema.fields
//! ._values: {type: dense_vector, dimension, metric}`, on the credential's `cloud` and `region`, `aws` and
//! `us-east-1` unless said), and waited for until it is ready. Vectors are written with `POST <host>/vectors/upsert`,
//! found with `POST <host>/query`, and deleted with `POST <host>/vectors/delete` and a metadata filter, in the step's
//! `namespace` when it names one.
//!
//! A piece's text and its own fields are metadata (`piece_id`, `piece_source`, `piece_position`, `piece_text`, the
//! whole metadata as `piece_metadata`), beside the metadata's top fields a filter may use - text, numbers, true or
//! false, lists of text - within Pinecone's 40 KB a record. A filter's conditions go as `{field: {$eq: value}}`
//! joined by `$and` (a list field matches a value it holds). Pinecone gives the cosine similarity, the dot product,
//! or the squared Euclidean distance; it does not say how many vectors a delete removed.

use std::sync::Mutex;

use serde_json::{Map, Value as Json, json};

use super::http::{Api, Reply, segment};
use super::{
    Cond, Got, Hit, Piece, StoreKind, Target, field, flat_fields, from_similarity, lock, metadata_back, odd,
    uuid_of, vector_json,
};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;

/// The API version every request names.
pub(crate) const VERSION: &str = "2026-07";
const CONTROL: &str = "https://api.pinecone.io";
/// The most metadata a vector has, in bytes as JSON; how many vectors, and bytes, one upsert sends.
const MAX_METADATA: usize = 40_000;
const BATCH: usize = 100;
const BATCH_BYTES: usize = 1_500_000;
/// How long a new index is waited for, a look at a time.
const READY_LOOKS: usize = 45;
const READY_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug)]
pub(crate) struct Pinecone<'a> {
    api: Api<'a>,
    index: String,
    namespace: Option<String>,
    distance: Distance,
    cloud: String,
    region: String,
    /// The index's own address, once found.
    host: Mutex<Option<String>>,
}

impl<'a> Pinecone<'a> {
    pub(crate) fn open(
        effects: &'a dyn Effects,
        credential: &Credential,
        target: &Target,
    ) -> Result<Self, String> {
        // A test run of a workflow whose credential is not made yet shows the key as a test run shows secrets.
        let key = match field(credential, "api_key") {
            Some(k) => k,
            None if credential.kind == "test" => crate::apps::MASK.to_owned(),
            None => return Err("the pinecone credential has no api_key".to_owned()),
        };
        let control = match field(credential, "url") {
            Some(u) if u.starts_with("https://") || u.starts_with("http://") => {
                u.trim_end_matches('/').to_owned()
            }
            Some(u) => return Err(format!("url: {u:?} is not an http or https address")),
            None => CONTROL.to_owned(),
        };
        Ok(Self {
            api: Api {
                effects,
                base: control,
                headers: vec![
                    ("api-key".to_owned(), key),
                    ("x-pinecone-api-version".to_owned(), VERSION.to_owned()),
                ],
                kind: StoreKind::Pinecone,
            },
            index: target.name.clone(),
            namespace: target.namespace.clone(),
            distance: target.distance,
            cloud: field(credential, "cloud").unwrap_or_else(|| "aws".to_owned()),
            region: field(credential, "region").unwrap_or_else(|| "us-east-1".to_owned()),
            host: Mutex::new(None),
        })
    }

    /// Refuses a piece whose metadata is past Pinecone's most.
    pub(crate) fn check(&self, pieces: &[Piece]) -> Result<(), String> {
        for (i, p) in pieces.iter().enumerate() {
            if Json::Object(self.metadata(p)).to_string().len() > MAX_METADATA {
                return Err(format!(
                    "pieces[{i}]: Pinecone keeps at most 40 KB of a piece's text and metadata: split into smaller pieces"
                ));
            }
        }
        Ok(())
    }

    fn metadata(&self, p: &Piece) -> Map<String, Json> {
        let mut m = flat_fields(StoreKind::Pinecone, &p.metadata);
        m.insert("piece_id".to_owned(), json!(p.id));
        m.insert("piece_source".to_owned(), json!(p.source));
        m.insert("piece_position".to_owned(), json!(p.position));
        m.insert("piece_text".to_owned(), json!(p.text));
        m.insert("piece_metadata".to_owned(), json!(p.metadata.to_string()));
        m
    }

    /// What the control plane says of the index: its host, and whether it is ready; none when there is none.
    async fn describe(&self) -> Result<Got<Option<(String, bool)>>, String> {
        let path = format!("/indexes/{}", segment(&self.index));
        let body = match self.api.send("GET", &path, None, true).await? {
            Reply::Shown(s) => return Ok(Got::Shown(s)),
            Reply::Got(404, _) => return Ok(Got::Done(None)),
            reply => match self.api.checked(reply)? {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(b) => b,
            },
        };
        let host = super::text_of(&body, "host");
        let ready = body
            .pointer("/status/ready")
            .and_then(Json::as_bool)
            .unwrap_or(false);
        Ok(Got::Done((!host.is_empty()).then_some((host, ready))))
    }

    /// The index's address, `https://<host>` (as the control plane's scheme).
    fn address(&self, host: &str) -> String {
        if host.starts_with("https://") || host.starts_with("http://") {
            return host.trim_end_matches('/').to_owned();
        }
        let scheme = if self.api.base.starts_with("http://") {
            "http"
        } else {
            "https"
        };
        format!("{scheme}://{}", host.trim_end_matches('/'))
    }

    async fn host(&self) -> Result<Got<String>, String> {
        if let Some(h) = lock(&self.host).clone() {
            return Ok(Got::Done(h));
        }
        match self.describe().await? {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(None) => Err(format!(
                "Pinecone has no index {}: make it, or add pieces with create: true",
                self.index
            )),
            Got::Done(Some((host, _))) => {
                let address = self.address(&host);
                *lock(&self.host) = Some(address.clone());
                Ok(Got::Done(address))
            }
        }
    }

    pub(crate) async fn ensure(&self, dimensions: u32) -> Result<Got<bool>, String> {
        match self.describe().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(Some((host, _))) => {
                *lock(&self.host) = Some(self.address(&host));
                return Ok(Got::Done(false));
            }
            Got::Done(None) => {}
        }
        let metric = match self.distance {
            Distance::Cosine => "cosine",
            Distance::L2 => "euclidean",
            Distance::InnerProduct => "dotproduct",
        };
        let made = json!({
            "name": self.index,
            "deployment": { "deployment_type": "managed", "cloud": self.cloud, "region": self.region },
            "schema": { "fields": { "_values": { "type": "dense_vector", "dimension": dimensions, "metric": metric } } },
            "deletion_protection": "disabled",
        });
        if let Got::Shown(s) = self.api.ok("POST", "/indexes", Some(&made), false).await? {
            return Ok(Got::Shown(s));
        }
        for _ in 0..READY_LOOKS {
            match self.describe().await? {
                Got::Shown(s) => return Ok(Got::Shown(s)),
                Got::Done(Some((host, true))) => {
                    *lock(&self.host) = Some(self.address(&host));
                    return Ok(Got::Done(true));
                }
                Got::Done(_) => self.api.effects.sleep(READY_WAIT).await,
            }
        }
        Err(format!(
            "Pinecone made the index {} but it was not ready in time: run the step again shortly",
            self.index
        ))
    }

    fn with_namespace(&self, mut body: Json) -> Json {
        if let (Some(ns), Some(b)) = (&self.namespace, body.as_object_mut()) {
            b.insert("namespace".to_owned(), json!(ns));
        }
        body
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let host = match self.host().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(h) => h,
        };
        let mut vectors: Vec<Json> = Vec::new();
        let mut bytes = 0;
        let mut added = 0;
        for (i, p) in pieces.iter().enumerate() {
            let id = if p.id.is_ascii() && p.id.len() <= 512 {
                p.id.clone()
            } else {
                uuid_of(&p.id)
            };
            let v = json!({ "id": id, "values": vector_json(&p.vector)?, "metadata": self.metadata(p) });
            bytes += v.to_string().len();
            vectors.push(v);
            let last = i + 1 == pieces.len();
            if vectors.len() == BATCH || bytes > BATCH_BYTES || last {
                let body = self.with_namespace(json!({ "vectors": std::mem::take(&mut vectors) }));
                bytes = 0;
                let url = format!("{host}/vectors/upsert");
                match self
                    .api
                    .checked(self.api.send_to(&url, "POST", Some(&body), false).await?)?
                {
                    Got::Shown(s) => return Ok(Got::Shown(s)),
                    Got::Done(d) => added += d.get("upsertedCount").and_then(Json::as_u64).unwrap_or(0),
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
        let host = match self.host().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(h) => h,
        };
        let of_sources = json!({ "piece_source": { "$in": sources } });
        let filter = match keep.filter(|k| !k.is_empty()) {
            Some(keep) => json!({ "$and": [of_sources, { "piece_id": { "$nin": keep } }] }),
            None => of_sources,
        };
        let body = self.with_namespace(json!({ "filter": filter }));
        let url = format!("{host}/vectors/delete");
        match self
            .api
            .checked(self.api.send_to(&url, "POST", Some(&body), false).await?)?
        {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(_) => Ok(Got::Done(None)),
        }
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let wanted = filter
            .map(|f| super::conditions(f, StoreKind::Pinecone))
            .transpose()?;
        let host = match self.host().await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(h) => h,
        };
        let mut body = json!({ "vector": vector_json(vector)?, "topK": k, "includeMetadata": true });
        if let (Some(c), Some(b)) = (wanted.as_deref().filter(|c| !c.is_empty()), body.as_object_mut()) {
            b.insert("filter".to_owned(), filter_of(c));
        }
        let body = self.with_namespace(body);
        let url = format!("{host}/query");
        let answer = match self
            .api
            .checked(self.api.send_to(&url, "POST", Some(&body), true).await?)?
        {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(a) => a,
        };
        let matches = answer
            .get("matches")
            .and_then(Json::as_array)
            .ok_or_else(|| odd(StoreKind::Pinecone, "its matches"))?;
        Ok(Got::Done(
            matches
                .iter()
                .map(|m| {
                    let md = m.get("metadata").cloned().unwrap_or(Json::Null);
                    let score = m.get("score").and_then(Json::as_f64).unwrap_or(f64::NAN);
                    let id = super::text_of(&md, "piece_id");
                    Hit {
                        id: if id.is_empty() {
                            super::text_of(m, "id")
                        } else {
                            id
                        },
                        source: super::text_of(&md, "piece_source"),
                        position: md.get("piece_position").cloned().unwrap_or(Json::Null),
                        text: super::text_of(&md, "piece_text"),
                        metadata: metadata_back(md.get("piece_metadata"), md.as_object()),
                        distance: from_similarity(self.distance, score),
                    }
                })
                .collect(),
        ))
    }
}

/// A filter's conditions as Pinecone's metadata filter.
pub(crate) fn filter_of(conditions: &[Cond]) -> Json {
    let mut parts = Vec::new();
    for c in conditions {
        let name = c.path().join(".");
        let values = match c {
            Cond::Is(_, s) => vec![s.json()],
            Cond::HoldsAll(_, values) => values.iter().map(super::Scalar::json).collect(),
        };
        for v in values {
            let mut inner = Map::new();
            inner.insert("$eq".to_owned(), v);
            let mut outer = Map::new();
            outer.insert(name.clone(), Json::Object(inner));
            parts.push(Json::Object(outer));
        }
    }
    if parts.len() == 1 {
        return parts.pop().unwrap_or(Json::Null);
    }
    json!({ "$and": parts })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_goes_as_pinecone_s() {
        let c = super::super::conditions(
            &json!({ "team": "sales", "tags": ["faq", "top"] }),
            StoreKind::Pinecone,
        )
        .unwrap();
        assert_eq!(
            filter_of(&c),
            json!({ "$and": [{ "team": { "$eq": "sales" } }, { "tags": { "$eq": "faq" } }, { "tags": { "$eq": "top" } }] })
        );
    }
}
