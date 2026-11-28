// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The stores with protocols of their own: Redis (RESP, with the search commands `FT.INFO`, `FT.CREATE`, `FT.SEARCH`
//! and the hash and key commands the steps use) and MongoDB (`OP_MSG` messages of BSON documents: `hello`, and the
//! commands of a vector search collection). Each keeps every command it was sent.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use bson::{Bson, Document, doc};
use serde_json::{Value as Json, json};

use super::cosine;

// --- Redis ---

/// A hash's fields.
type Hash = BTreeMap<String, Vec<u8>>;

#[derive(Debug, Default)]
pub(crate) struct RedisState {
    pub(crate) index: Option<Vec<String>>,
    pub(crate) hashes: BTreeMap<Vec<u8>, Hash>,
    /// Plain keys: their value, and the TTL they were set with (milliseconds).
    pub(crate) keys: BTreeMap<Vec<u8>, (Vec<u8>, Option<u64>)>,
    /// Every command, its words (a vector by its size).
    pub(crate) commands: Vec<Vec<String>>,
}

fn bulk(b: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", b.len()).into_bytes();
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
    out
}

fn array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for i in items {
        out.extend_from_slice(i);
    }
    out
}

fn read_command(reader: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let n: usize = line.trim().strip_prefix('*')?.parse().ok()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let mut len = String::new();
        reader.read_line(&mut len).ok()?;
        let len: usize = len.trim().strip_prefix('$')?.parse().ok()?;
        let mut buf = vec![0u8; len + 2];
        reader.read_exact(&mut buf).ok()?;
        buf.truncate(len);
        out.push(buf);
    }
    Some(out)
}

fn floats(blob: &[u8]) -> Vec<f64> {
    blob.chunks(4)
        .map(|c| f64::from(f32::from_le_bytes([c[0], c[1], c[2], c[3]])))
        .collect()
}

/// The tags inside `@field:{a | b}`.
fn tags_in(clause: &str) -> Vec<String> {
    let inner = clause.split_once('{').unwrap().1.trim_end_matches('}');
    inner.split('|').map(|t| t.trim().to_owned()).collect()
}

fn redis_answer(state: &mut RedisState, args: &[Vec<u8>]) -> Vec<u8> {
    let word = |i: usize| String::from_utf8_lossy(&args[i]).into_owned();
    let name = word(0).to_ascii_uppercase();
    let shown: Vec<String> = args
        .iter()
        .map(|a| {
            if a.iter().all(|b| b.is_ascii() && !b.is_ascii_control()) {
                String::from_utf8_lossy(a).into_owned()
            } else {
                format!("<{} bytes>", a.len())
            }
        })
        .collect();
    state.commands.push(shown);
    match name.as_str() {
        "FT.INFO" => match &state.index {
            Some(_) => array(&[bulk(b"index_name"), bulk(&args[1])]),
            None => b"-Unknown index name\r\n".to_vec(),
        },
        "FT.CREATE" => {
            state.index = Some(
                args.iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect(),
            );
            b"+OK\r\n".to_vec()
        }
        "HSET" => {
            let fields = state.hashes.entry(args[1].clone()).or_default();
            for pair in args[2..].chunks(2) {
                fields.insert(String::from_utf8_lossy(&pair[0]).into_owned(), pair[1].clone());
            }
            format!(":{}\r\n", (args.len() - 2) / 2).into_bytes()
        }
        "DEL" => {
            let mut n = 0;
            for k in &args[1..] {
                n += usize::from(state.hashes.remove(k).is_some())
                    + usize::from(state.keys.remove(k).is_some());
            }
            format!(":{n}\r\n").into_bytes()
        }
        "GET" => match state.keys.get(&args[1]) {
            Some((v, _)) => bulk(v),
            None => b"$-1\r\n".to_vec(),
        },
        "SET" => {
            let ttl = args
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"PX"))
                .map(|i| word(i + 1).parse().unwrap());
            state.keys.insert(args[1].clone(), (args[2].clone(), ttl));
            b"+OK\r\n".to_vec()
        }
        "FT.SEARCH" => {
            let prefix = format!("{}:", word(1)).into_bytes();
            let query = word(2);
            let after = |w: &str| {
                args.iter()
                    .position(|a| a.eq_ignore_ascii_case(w.as_bytes()))
                    .map(|i| i + 1)
            };
            let (offset, count) = after("LIMIT").map_or((0, 10), |i| {
                (word(i).parse().unwrap(), word(i + 1).parse().unwrap())
            });
            let mine: Vec<(&Vec<u8>, &Hash)> = state
                .hashes
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .collect();
            if let Some((pre, knn)) = query.split_once("=>[KNN ") {
                let k: usize = knn.split_whitespace().next().unwrap().parse().unwrap();
                let wanted: Vec<String> = pre
                    .trim_matches(['(', ')'])
                    .split_whitespace()
                    .filter(|c| c.starts_with("@tags:"))
                    .flat_map(tags_in)
                    .collect();
                let vec_at = after("PARAMS").unwrap() + 2;
                let q = floats(&args[vec_at]);
                let mut found: Vec<(f64, &Vec<u8>, &Hash)> = mine
                    .into_iter()
                    .filter(|(_, h)| {
                        let tags = String::from_utf8_lossy(h.get("tags").map_or(&[][..], Vec::as_slice))
                            .into_owned();
                        let have: Vec<&str> = tags.split(',').collect();
                        wanted.iter().all(|w| have.contains(&w.as_str()))
                    })
                    .map(|(key, h)| (1.0 - cosine(&q, &floats(&h["embedding"])), key, h))
                    .collect();
                found.sort_by(|a, b| a.0.total_cmp(&b.0));
                let mut out = vec![format!(":{}\r\n", found.len()).into_bytes()];
                for (d, key, h) in found.into_iter().take(k.min(count)) {
                    out.push(bulk(key));
                    let mut fields = Vec::new();
                    for f in ["piece_id", "source", "position", "text", "metadata"] {
                        fields.push(bulk(f.as_bytes()));
                        fields.push(bulk(&h[f]));
                    }
                    fields.push(bulk(b"vector_distance"));
                    fields.push(bulk(d.to_string().as_bytes()));
                    out.push(array(&fields));
                }
                return array(&out);
            }
            let sources = tags_in(&query);
            assert!(
                args.iter().any(|a| a == b"NOCONTENT"),
                "a look for keys asks for no content"
            );
            let keys: Vec<&Vec<u8>> = mine
                .into_iter()
                .filter(|(_, h)| sources.contains(&String::from_utf8_lossy(&h["source_tag"]).into_owned()))
                .map(|(k, _)| k)
                .collect();
            let mut out = vec![format!(":{}\r\n", keys.len()).into_bytes()];
            out.extend(keys.into_iter().skip(offset).take(count).map(|k| bulk(k)));
            array(&out)
        }
        _ => format!("-ERR unknown command '{name}'\r\n").into_bytes(),
    }
}

/// A Redis server with the search commands; its port and what it holds.
pub(crate) fn redis() -> (u16, Arc<Mutex<RedisState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let state: Arc<Mutex<RedisState>> = Arc::default();
    let kept = Arc::clone(&state);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let state = Arc::clone(&kept);
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut out = stream;
                while let Some(args) = read_command(&mut reader) {
                    let answer = redis_answer(&mut state.lock().unwrap(), &args);
                    if out.write_all(&answer).is_err() {
                        break;
                    }
                }
            });
        }
    });
    (port, state)
}

// --- MongoDB ---

#[derive(Debug, Default)]
pub(crate) struct MongoState {
    pub(crate) collections: BTreeMap<String, Vec<Document>>,
    pub(crate) indexes: Vec<Document>,
    /// Every command, as sent.
    pub(crate) commands: Vec<Document>,
}

fn to_f64(b: &Bson) -> f64 {
    match b {
        Bson::Double(d) => *d,
        Bson::Int32(n) => f64::from(*n),
        Bson::Int64(n) => *n as f64,
        _ => f64::NAN,
    }
}

fn vector_of(b: Option<&Bson>) -> Vec<f64> {
    match b {
        Some(Bson::Array(items)) => items.iter().map(to_f64).collect(),
        _ => Vec::new(),
    }
}

fn holds_tags(d: &Document, filter: &Document) -> bool {
    if let Ok(all) = filter.get_array("$and") {
        return all
            .iter()
            .all(|f| matches!(f, Bson::Document(f) if holds_tags(d, f)));
    }
    let want = filter.get_str("tags").unwrap();
    d.get_array("tags")
        .unwrap()
        .iter()
        .any(|t| t.as_str() == Some(want))
}

fn strings(b: Option<&Bson>) -> Vec<String> {
    match b {
        Some(Bson::Array(items)) => items
            .iter()
            .filter_map(|i| i.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

fn mongo_answer(state: &mut MongoState, body: &Document) -> Document {
    state.commands.push(body.clone());
    let (name, _) = body.iter().next().unwrap();
    let collection = body.get_str(name).unwrap_or_default().to_owned();
    match name.as_str() {
        "hello" | "isMaster" => {
            doc! { "ok": 1.0, "isWritablePrimary": true, "maxWireVersion": 25, "minWireVersion": 0,
            "maxBsonObjectSize": 16_777_216, "maxMessageSizeBytes": 48_000_000 }
        }
        "create" => {
            if state.collections.contains_key(&collection) {
                return doc! { "ok": 0.0, "errmsg": "Collection already exists", "code": 48 };
            }
            state.collections.insert(collection, Vec::new());
            doc! { "ok": 1.0 }
        }
        "createSearchIndexes" => {
            for i in body.get_array("indexes").unwrap() {
                state.indexes.push(i.as_document().unwrap().clone());
            }
            doc! { "ok": 1.0, "indexesCreated": [{ "id": "i-1", "name": "vector_index" }] }
        }
        "update" => {
            let docs = state.collections.entry(collection).or_default();
            let mut n = 0;
            for u in body.get_array("updates").unwrap() {
                let u = u.as_document().unwrap();
                assert_eq!(u.get_bool("upsert").ok(), Some(true));
                let id = u.get_document("q").unwrap().get_str("_id").unwrap().to_owned();
                docs.retain(|d| d.get_str("_id").ok() != Some(id.as_str()));
                docs.push(u.get_document("u").unwrap().clone());
                n += 1;
            }
            doc! { "ok": 1.0, "n": n }
        }
        "delete" => {
            let docs = state.collections.entry(collection).or_default();
            let q = body.get_array("deletes").unwrap()[0]
                .as_document()
                .unwrap()
                .get_document("q")
                .unwrap()
                .clone();
            let sources = strings(q.get_document("source").unwrap().get("$in"));
            let keep = q
                .get_document("_id")
                .map(|k| strings(k.get("$nin")))
                .unwrap_or_default();
            let before = docs.len();
            docs.retain(|d| {
                !sources
                    .iter()
                    .any(|s| d.get_str("source").ok() == Some(s.as_str()))
                    || keep.iter().any(|k| d.get_str("_id").ok() == Some(k.as_str()))
            });
            doc! { "ok": 1.0, "n": i32::try_from(before - docs.len()).unwrap() }
        }
        "aggregate" => {
            let pipeline = body.get_array("pipeline").unwrap();
            let first = pipeline[0].as_document().unwrap();
            let batch: Vec<Bson> = if let Ok(look) = first.get_document("$listSearchIndexes") {
                let name = look.get_str("name").unwrap();
                state
                    .indexes
                    .iter()
                    .filter(|i| i.get_str("name").ok() == Some(name))
                    .map(|i| Bson::Document(i.clone()))
                    .collect()
            } else {
                let s = first.get_document("$vectorSearch").unwrap();
                assert!(
                    state
                        .indexes
                        .iter()
                        .any(|i| i.get_str("name").ok() == s.get_str("index").ok()),
                    "a search needs its index"
                );
                assert_eq!(s.get_str("path").ok(), Some("embedding"));
                let q = vector_of(s.get("queryVector"));
                let limit = s.get_i64("limit").unwrap() as usize;
                assert!(s.get_i64("numCandidates").unwrap() as usize >= limit);
                let docs = state.collections.get(&collection).cloned().unwrap_or_default();
                let mut found: Vec<(f64, Document)> = docs
                    .into_iter()
                    .filter(|d| s.get_document("filter").map_or(true, |f| holds_tags(d, f)))
                    .map(|d| ((1.0 + cosine(&q, &vector_of(d.get("embedding")))) / 2.0, d))
                    .collect();
                found.sort_by(|a, b| b.0.total_cmp(&a.0));
                found
                    .into_iter()
                    .take(limit)
                    .map(|(score, d)| {
                        let mut out = Document::new();
                        for k in ["_id", "source", "position", "text", "metadata"] {
                            if let Some(v) = d.get(k) {
                                out.insert(k, v.clone());
                            }
                        }
                        out.insert("score", score);
                        Bson::Document(out)
                    })
                    .collect()
            };
            doc! { "ok": 1.0, "cursor": { "firstBatch": batch, "id": 0_i64, "ns": format!("shop.{collection}") } }
        }
        other => doc! { "ok": 0.0, "errmsg": format!("no such command: '{other}'"), "code": 59 },
    }
}

fn mongo_conversation(stream: TcpStream, state: &Arc<Mutex<MongoState>>) {
    let mut reader = stream.try_clone().unwrap();
    let mut out = stream;
    loop {
        let mut head = [0u8; 16];
        if reader.read_exact(&mut head).is_err() {
            return;
        }
        let length = i32::from_le_bytes(head[0..4].try_into().unwrap()) as usize;
        let id = i32::from_le_bytes(head[4..8].try_into().unwrap());
        assert_eq!(
            i32::from_le_bytes(head[12..16].try_into().unwrap()),
            2013,
            "OP_MSG only"
        );
        let mut rest = vec![0u8; length - 16];
        reader.read_exact(&mut rest).unwrap();
        assert_eq!(rest[4], 0, "a body section first");
        let body = Document::from_reader(&rest[5..]).unwrap();
        assert!(body.get_str("$db").is_ok(), "every command names its database");
        let answer = mongo_answer(&mut state.lock().unwrap(), &body).to_vec().unwrap();
        let mut message = Vec::new();
        let total = i32::try_from(16 + 4 + 1 + answer.len()).unwrap();
        for n in [total, 1, id, 2013] {
            message.extend_from_slice(&n.to_le_bytes());
        }
        message.extend_from_slice(&0u32.to_le_bytes());
        message.push(0);
        message.extend_from_slice(&answer);
        if out.write_all(&message).is_err() {
            return;
        }
    }
}

/// A MongoDB server with vector search, as Atlas has it; its port and what it holds.
pub(crate) fn mongo() -> (u16, Arc<Mutex<MongoState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let state: Arc<Mutex<MongoState>> = Arc::default();
    let kept = Arc::clone(&state);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let state = Arc::clone(&kept);
            std::thread::spawn(move || mongo_conversation(stream, &state));
        }
    });
    (port, state)
}

/// A command as JSON, for reading in a test.
pub(crate) fn json_of(d: &Document) -> Json {
    fn of(b: &Bson) -> Json {
        match b {
            Bson::Document(d) => Json::Object(d.iter().map(|(k, v)| (k.clone(), of(v))).collect()),
            Bson::Array(items) => Json::Array(items.iter().map(of).collect()),
            Bson::String(s) => json!(s),
            Bson::Boolean(b) => json!(b),
            Bson::Int32(n) => json!(n),
            Bson::Int64(n) => json!(n),
            Bson::Double(d) => json!(d),
            Bson::Null => Json::Null,
            other => json!(other.to_string()),
        }
    }
    of(&Bson::Document(d.clone()))
}
