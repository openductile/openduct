// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `store: memory`: pieces kept in the run's own memory, for small sets and for trying a workflow out - added by one
//! step, searched by a later step of the same run, and gone when the run ends (a run that waits and takes up again
//! starts with none, as does a workflow another runs). Nothing leaves the run, so a test run and a preview keep and
//! search for real. A search compares every piece (exactly), its filter read as PostgreSQL's `@>` reads it.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::Value as Json;

use super::{Hit, Piece, holds, lock};
use crate::retrieval::Distance;

/// The most pieces a run keeps, every collection's together; the most bytes of text, metadata and vectors; the most
/// collections.
pub(crate) const MAX_PIECES: usize = 10_000;
pub(crate) const MAX_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_COLLECTIONS: usize = 20;

/// The pieces a run keeps in its own memory, by collection.
#[derive(Debug, Default)]
pub(crate) struct RunVectors {
    held: Mutex<BTreeMap<String, Collection>>,
}

#[derive(Debug, Default)]
struct Collection {
    /// By id, in the order first added.
    pieces: Vec<Piece>,
    bytes: usize,
}

fn size(p: &Piece) -> usize {
    p.text.len() + p.source.len() + p.id.len() + p.metadata.to_string().len() + p.vector.len() * 8 + 64
}

/// A collection of the run's memory, as a step reaches it.
#[derive(Debug)]
pub(crate) struct InRun<'a> {
    held: &'a RunVectors,
    name: String,
    distance: Distance,
}

impl<'a> InRun<'a> {
    pub(crate) fn new(held: &'a RunVectors, name: &str, distance: Distance) -> Self {
        Self {
            held,
            name: name.to_owned(),
            distance,
        }
    }

    /// Makes the collection when it is not there: whether it made it.
    pub(crate) fn ensure(&self) -> bool {
        let mut all = lock(&self.held.held);
        if all.contains_key(&self.name) || all.len() >= MAX_COLLECTIONS {
            return false;
        }
        all.insert(self.name.clone(), Collection::default());
        true
    }

    pub(crate) fn upsert(&self, pieces: &[Piece]) -> Result<u64, String> {
        let mut all = lock(&self.held.held);
        if !all.contains_key(&self.name) && all.len() >= MAX_COLLECTIONS {
            return Err(format!(
                "a run keeps at most {MAX_COLLECTIONS} collections in its memory"
            ));
        }
        let total_pieces: usize = all.values().map(|c| c.pieces.len()).sum();
        let total_bytes: usize = all.values().map(|c| c.bytes).sum();
        let c = all.entry(self.name.clone()).or_default();
        if let (Some(first), Some(new)) = (c.pieces.first(), pieces.first())
            && first.vector.len() != new.vector.len()
        {
            return Err(format!(
                "{} holds vectors of {} numbers, and these have {}",
                self.name,
                first.vector.len(),
                new.vector.len()
            ));
        }
        let (mut more_pieces, mut more_bytes) = (0usize, 0usize);
        for p in pieces {
            match c.pieces.iter().position(|old| old.id == p.id) {
                Some(i) => more_bytes += size(p).saturating_sub(c.pieces.get(i).map_or(0, size)),
                None => {
                    more_pieces += 1;
                    more_bytes += size(p);
                }
            }
        }
        if total_pieces + more_pieces > MAX_PIECES || total_bytes + more_bytes > MAX_BYTES {
            return Err(format!(
                "a run keeps at most {MAX_PIECES} pieces ({} MiB) in its memory: use a vector database for more",
                MAX_BYTES / 1024 / 1024
            ));
        }
        for p in pieces {
            match c.pieces.iter_mut().find(|old| old.id == p.id) {
                Some(old) => {
                    c.bytes = c.bytes.saturating_sub(size(old)) + size(p);
                    *old = p.clone();
                }
                None => {
                    c.bytes += size(p);
                    c.pieces.push(p.clone());
                }
            }
        }
        Ok(pieces.len() as u64)
    }

    pub(crate) fn remove(&self, sources: &[&str], keep: Option<&[&str]>) -> u64 {
        let mut all = lock(&self.held.held);
        let Some(c) = all.get_mut(&self.name) else {
            return 0;
        };
        let before = c.pieces.len();
        c.pieces.retain(|p| {
            !sources.contains(&p.source.as_str()) || keep.is_some_and(|k| k.contains(&p.id.as_str()))
        });
        c.bytes = c.pieces.iter().map(size).sum();
        (before - c.pieces.len()) as u64
    }

    pub(crate) fn search(&self, vector: &[f64], k: usize, filter: Option<&Json>) -> Result<Vec<Hit>, String> {
        let all = lock(&self.held.held);
        let Some(c) = all.get(&self.name) else {
            return Err(format!(
                "the run's memory has no collection {}: a step of this run adds pieces to it first",
                self.name
            ));
        };
        if let Some(first) = c.pieces.first()
            && first.vector.len() != vector.len()
        {
            return Err(format!(
                "{} holds vectors of {} numbers, and the query has {}",
                self.name,
                first.vector.len(),
                vector.len()
            ));
        }
        let mut found: Vec<(f64, &Piece)> = c
            .pieces
            .iter()
            .filter(|p| filter.is_none_or(|f| holds(&p.metadata, f)))
            .map(|p| (distance(self.distance, vector, &p.vector), p))
            .collect();
        found.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(found
            .into_iter()
            .take(k)
            .map(|(d, p)| Hit {
                id: p.id.clone(),
                source: p.source.clone(),
                position: Json::from(p.position),
                text: p.text.clone(),
                metadata: p.metadata.clone(),
                distance: d,
            })
            .collect())
    }
}

/// The distance between two vectors, as pgvector gives it.
pub(crate) fn distance(kind: Distance, a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    match kind {
        Distance::Cosine => {
            let norm = |v: &[f64]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
            let n = norm(a) * norm(b);
            if n == 0.0 { 1.0 } else { 1.0 - dot / n }
        }
        Distance::InnerProduct => -dot,
        Distance::L2 => a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum::<f64>().sqrt(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn piece(id: &str, source: &str, v: Vec<f64>, team: &str) -> Piece {
        Piece {
            id: id.to_owned(),
            source: source.to_owned(),
            position: 0,
            text: format!("text of {id}"),
            metadata: json!({ "team": team }),
            vector: v,
        }
    }

    #[test]
    fn a_run_keeps_finds_replaces_and_forgets_its_pieces() {
        let held = RunVectors::default();
        let c = InRun::new(&held, "kb", Distance::Cosine);
        assert!(
            c.search(&[1.0, 0.0], 3, None)
                .unwrap_err()
                .contains("adds pieces to it first")
        );
        assert!(c.ensure());
        assert!(!c.ensure(), "made once");
        c.upsert(&[
            piece("a", "doc", vec![1.0, 0.0], "x"),
            piece("b", "doc", vec![0.0, 1.0], "y"),
        ])
        .unwrap();
        let found = c.search(&[0.9, 0.1], 2, None).unwrap();
        assert_eq!(
            found.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        let only_y = c.search(&[0.9, 0.1], 2, Some(&json!({ "team": "y" }))).unwrap();
        assert_eq!(only_y.len(), 1);
        assert!(
            c.upsert(&[piece("c", "doc", vec![1.0, 0.0, 0.0], "x")])
                .unwrap_err()
                .contains("2 numbers")
        );
        assert_eq!(c.remove(&["doc"], Some(&["a"])), 1);
        assert_eq!(c.remove(&["doc"], None), 1);
        assert!(c.search(&[1.0, 0.0], 2, None).unwrap().is_empty());
    }

    #[test]
    fn a_run_s_memory_is_bounded() {
        let held = RunVectors::default();
        for i in 0..MAX_COLLECTIONS {
            InRun::new(&held, &format!("c{i}"), Distance::L2).ensure();
        }
        let more = InRun::new(&held, "one-more", Distance::L2);
        assert!(
            more.upsert(&[piece("a", "d", vec![1.0], "x")])
                .unwrap_err()
                .contains("at most 20 collections")
        );
    }

    #[test]
    fn distances_are_pgvector_s() {
        assert!((distance(Distance::Cosine, &[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-12);
        assert!((distance(Distance::InnerProduct, &[1.0, 2.0], &[3.0, 4.0]) + 11.0).abs() < 1e-12);
        assert!((distance(Distance::L2, &[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-12);
    }
}
