// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Redis's vector search (Redis 8, and Redis Stack before it), over Redis's own protocol on the connection the
//! `redis` step opens: the same credential (`redis://` or `rediss://`), the same private-network rule. A test run
//! sends nothing and shows the command; a preview runs what reads (`FT.INFO`, `FT.SEARCH`) and shows what writes.
//!
//! An index (`FT.CREATE <index> ON HASH PREFIX 1 <index>: SCHEMA embedding VECTOR HNSW 6 TYPE FLOAT32 DIM <n>
//! DISTANCE_METRIC COSINE source_tag TAG tags TAG`) covers the hashes whose keys begin with its name: a piece is the
//! hash `<index>:<id>`, with `piece_id`, `source`, `position`, `text`, `metadata` (JSON), its vector as 32-bit floats,
//! and two tag fields to filter by - `source_tag`, and `tags`, one tag for each value of the metadata (as Redis's own
//! guide suggests for fields not known in advance). A tag is the start of the SHA-256 of `field=value`, so no text
//! of a value needs escaping in a query.
//!
//! A search is `FT.SEARCH <index> "(@tags:{<a>} @tags:{<b>})=>[KNN <k> @embedding $vec AS vector_distance]" PARAMS 2
//! vec <bytes> SORTBY vector_distance ... DIALECT 2`; a document's pieces are found by `@source_tag` and deleted with
//! `DEL`. Redis gives the cosine distance, 1 minus the inner product, or the Euclidean distance.

use serde_json::{Value as Json, json};

use super::filter::{tokens, wanted_tokens};
use super::{Got, Hit, Piece, Target, conditions};
use crate::apps::Credential;
use crate::retrieval::Distance;
use crate::run::Effects;
use crate::storage::redis::{Conn, Reply};

/// How many keys one look for a document's pieces gives, the most looked at, and how many one `DEL` removes.
const PAGE: usize = 1_000;
const MAX_SCAN: usize = 20_000;
const DEL_BATCH: usize = 500;
/// The fields a search gives back.
const RETURNED: [&str; 6] = [
    "piece_id",
    "source",
    "position",
    "text",
    "metadata",
    "vector_distance",
];

pub(crate) struct Redis<'a> {
    effects: &'a dyn Effects,
    credential: Credential,
    index: String,
    distance: Distance,
}

impl std::fmt::Debug for Redis<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Redis")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

/// A tag: the start of a SHA-256, in hexadecimal.
pub(crate) fn tag(text: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    digest
        .as_ref()
        .iter()
        .take(12)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A vector as Redis keeps one: 32-bit floats, little-endian.
pub(crate) fn blob(vector: &[f64]) -> Vec<u8> {
    // A vector's numbers are checked to fit a 32-bit float before they come here.
    #[allow(clippy::cast_possible_truncation)]
    vector.iter().flat_map(|x| (*x as f32).to_le_bytes()).collect()
}

/// A command as a test run shows it: its words, a vector by its size.
fn shown(why: &str, args: &[Vec<u8>], blob_at: Option<usize>) -> Json {
    let words: Vec<String> = args
        .iter()
        .enumerate()
        .map(|(i, a)| {
            if Some(i) == blob_at {
                format!("<a vector of {} numbers>", a.len() / 4)
            } else {
                String::from_utf8_lossy(a).chars().take(500).collect()
            }
        })
        .collect();
    json!({ "test": why, "redis": words })
}

fn words(list: &[&str]) -> Vec<Vec<u8>> {
    list.iter().map(|w| w.as_bytes().to_vec()).collect()
}

impl<'a> Redis<'a> {
    pub(crate) fn open(effects: &'a dyn Effects, credential: Credential, target: &Target) -> Self {
        Self {
            effects,
            credential,
            index: target.name.clone(),
            distance: target.distance,
        }
    }

    /// A connection, when this run may send the command; else the command shown.
    async fn conn(
        &self,
        writes: bool,
        args: &[Vec<u8>],
        blob_at: Option<usize>,
    ) -> Result<Result<Conn, Json>, String> {
        if self.credential.kind != "redis" {
            return Ok(Err(shown("not sent", args, blob_at)));
        }
        match self.effects.storage().allows(writes) {
            Ok(places) => Ok(Ok(Conn::open(&self.credential, places).await?)),
            Err(why) => Ok(Err(shown(why, args, blob_at))),
        }
    }

    /// One command, on a connection of its own.
    async fn one(
        &self,
        writes: bool,
        args: Vec<Vec<u8>>,
        blob_at: Option<usize>,
    ) -> Result<Got<Reply>, String> {
        let mut conn = match self.conn(writes, &args, blob_at).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        Ok(Got::Done(conn.call(&args).await?))
    }

    fn key(&self, id: &str) -> Vec<u8> {
        format!("{}:{id}", self.index).into_bytes()
    }

    pub(crate) async fn ensure(&self, dimensions: u32) -> Result<Got<bool>, String> {
        match self.one(false, words(&["FT.INFO", &self.index]), None).await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(Reply::Refused(_)) => {}
            Got::Done(_) => return Ok(Got::Done(false)),
        }
        let metric = match self.distance {
            Distance::Cosine => "COSINE",
            Distance::L2 => "L2",
            Distance::InnerProduct => "IP",
        };
        let prefix = format!("{}:", self.index);
        let dims = dimensions.to_string();
        let args = words(&[
            "FT.CREATE",
            &self.index,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            &prefix,
            "SCHEMA",
            "embedding",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            &dims,
            "DISTANCE_METRIC",
            metric,
            "source_tag",
            "TAG",
            "tags",
            "TAG",
        ]);
        match self.one(true, args, None).await? {
            Got::Shown(s) => Ok(Got::Shown(s)),
            Got::Done(Reply::Refused(e)) => {
                Err(format!("Redis could not make the index {}: {e}", self.index))
            }
            Got::Done(_) => Ok(Got::Done(true)),
        }
    }

    fn hset(&self, p: &Piece) -> Vec<Vec<u8>> {
        let tags: Vec<String> = tokens(&p.metadata).iter().map(|t| tag(t)).collect();
        let mut args = vec![b"HSET".to_vec(), self.key(&p.id)];
        for (name, value) in [
            ("piece_id", p.id.clone()),
            ("source", p.source.clone()),
            ("source_tag", tag(&p.source)),
            ("position", p.position.to_string()),
            ("text", p.text.clone()),
            ("metadata", p.metadata.to_string()),
            ("tags", tags.join(",")),
        ] {
            args.push(name.as_bytes().to_vec());
            args.push(value.into_bytes());
        }
        args.push(b"embedding".to_vec());
        args.push(blob(&p.vector));
        args
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let Some(first) = pieces.first() else {
            return Ok(Got::Done(0));
        };
        let args = self.hset(first);
        let at = args.len() - 1;
        let mut conn = match self.conn(true, &args, Some(at)).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        for p in pieces {
            if let Reply::Refused(e) = conn.call(&self.hset(p)).await? {
                return Err(format!("Redis refused the piece {}: {e}", p.id));
            }
        }
        Ok(Got::Done(pieces.len() as u64))
    }

    /// The keys of the pieces of `sources`, a page at a time.
    fn find_keys(&self, sources: &[&str], offset: usize) -> Vec<Vec<u8>> {
        let tags: Vec<String> = sources.iter().map(|s| tag(s)).collect();
        let query = format!("@source_tag:{{{}}}", tags.join(" | "));
        let (offset, page) = (offset.to_string(), PAGE.to_string());
        words(&[
            "FT.SEARCH",
            &self.index,
            &query,
            "NOCONTENT",
            "LIMIT",
            &offset,
            &page,
            "DIALECT",
            "2",
        ])
    }

    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let first = self.find_keys(sources, 0);
        let mut conn = match self.conn(true, &first, None).await? {
            Ok(c) => c,
            Err(s) => return Ok(Got::Shown(s)),
        };
        let kept: Vec<Vec<u8>> = keep.unwrap_or_default().iter().map(|id| self.key(id)).collect();
        let mut removed = 0;
        let mut offset = 0;
        let mut looked = 0;
        while looked < MAX_SCAN {
            let keys = match conn.call(&self.find_keys(sources, offset)).await? {
                Reply::List(items) => items
                    .into_iter()
                    .skip(1)
                    .filter_map(|k| match k {
                        Reply::Text(t) => Some(t),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                Reply::Refused(e) => return Err(format!("Redis could not search {}: {e}", self.index)),
                _ => Vec::new(),
            };
            let count = keys.len();
            looked += count;
            let stale: Vec<Vec<u8>> = keys.into_iter().filter(|k| !kept.contains(k)).collect();
            let left_in_place = count - stale.len();
            for batch in stale.chunks(DEL_BATCH) {
                let mut args = vec![b"DEL".to_vec()];
                args.extend(batch.iter().cloned());
                if let Reply::Number(n) = conn.call(&args).await? {
                    removed += u64::try_from(n).unwrap_or(0);
                }
            }
            if count < PAGE {
                break;
            }
            // What was deleted is gone from the next page; what was kept is passed over.
            offset += left_in_place;
        }
        Ok(Got::Done(Some(removed)))
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let wanted = match filter {
            Some(f) => wanted_tokens(&conditions(f, super::StoreKind::Redis)?),
            None => Vec::new(),
        };
        let pre = if wanted.is_empty() {
            "*".to_owned()
        } else {
            let parts: Vec<String> = wanted.iter().map(|t| format!("@tags:{{{}}}", tag(t))).collect();
            format!("({})", parts.join(" "))
        };
        let query = format!("{pre}=>[KNN {k} @embedding $vec AS vector_distance]");
        let (count, top) = (RETURNED.len().to_string(), k.to_string());
        let mut args = words(&["FT.SEARCH", &self.index, &query, "PARAMS", "2", "vec"]);
        args.push(blob(vector));
        let at = args.len() - 1;
        args.extend(words(&["SORTBY", "vector_distance", "ASC", "RETURN", &count]));
        args.extend(words(&RETURNED));
        args.extend(words(&["LIMIT", "0", &top, "DIALECT", "2"]));
        let items = match self.one(false, args, Some(at)).await? {
            Got::Shown(s) => return Ok(Got::Shown(s)),
            Got::Done(Reply::List(items)) => items,
            Got::Done(Reply::Refused(e)) => {
                return Err(format!("Redis could not search {}: {e}", self.index));
            }
            Got::Done(_) => return Err(super::odd(super::StoreKind::Redis, "what it found")),
        };
        let mut hits = Vec::new();
        for pair in items.get(1..).unwrap_or_default().chunks(2) {
            let Some(Reply::List(fields)) = pair.get(1) else {
                continue;
            };
            let mut got = serde_json::Map::new();
            for kv in fields.chunks(2) {
                if let (Some(Reply::Text(k)), Some(Reply::Text(v))) = (kv.first(), kv.get(1)) {
                    got.insert(
                        String::from_utf8_lossy(k).into_owned(),
                        json!(String::from_utf8_lossy(v)),
                    );
                }
            }
            let got = Json::Object(got);
            let raw: f64 = super::text_of(&got, "vector_distance")
                .parse()
                .unwrap_or(f64::NAN);
            hits.push(Hit {
                id: super::text_of(&got, "piece_id"),
                source: super::text_of(&got, "source"),
                position: super::text_of(&got, "position")
                    .parse::<i64>()
                    .map_or(Json::Null, |n| json!(n)),
                text: super::text_of(&got, "text"),
                metadata: serde_json::from_str(&super::text_of(&got, "metadata"))
                    .unwrap_or_else(|_| json!({})),
                distance: match self.distance {
                    Distance::InnerProduct => raw - 1.0,
                    Distance::Cosine | Distance::L2 => raw,
                },
            });
        }
        Ok(Got::Done(hits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_and_vectors_are_written_as_redis_reads_them() {
        assert_eq!(tag("team=\"sales\"").len(), 24);
        assert_eq!(tag("a"), tag("a"));
        assert!(tag("a").chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(blob(&[1.0, -2.0]), [0, 0, 128, 63, 0, 0, 0, 192]);
        let s = shown("not sent", &[b"HSET".to_vec(), vec![0; 12]], Some(1));
        assert_eq!(
            s,
            json!({ "test": "not sent", "redis": ["HSET", "<a vector of 3 numbers>"] })
        );
    }
}
