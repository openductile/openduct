// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The web stores' stand-ins: Qdrant's REST API, Weaviate's REST API (and its GraphQL, for a server before the search
//! route), Milvus's RESTful API v2, Chroma's HTTP API v2, Pinecone's control and data planes. Each answers in its
//! documented shapes and reads the filters it is sent in its own language.

use serde_json::{Value as Json, json};

use super::{Asked, Handler, Server, at, cosine, is_or_holds, numbers, same, serve};

fn sorted(mut found: Vec<(f64, Json)>, limit: usize) -> Vec<(f64, Json)> {
    found.sort_by(|a, b| b.0.total_cmp(&a.0));
    found.truncate(limit);
    found
}

// --- Qdrant ---

fn qdrant_holds(point: &Json, filter: &Json) -> bool {
    let cond = |c: &Json| -> bool {
        if let Some(ids) = c.get("has_id").and_then(Json::as_array) {
            return ids.iter().any(|i| i == &point["id"]);
        }
        let have = at(&point["payload"], c["key"].as_str().unwrap());
        if let Some(any) = c.pointer("/match/any").and_then(Json::as_array) {
            return any.iter().any(|v| is_or_holds(have, v));
        }
        if let Some(v) = c.pointer("/match/value") {
            return is_or_holds(have, v);
        }
        if let Some(r) = c.get("range") {
            let x = have.and_then(Json::as_f64).unwrap_or(f64::NAN);
            return r["gte"].as_f64().is_none_or(|g| x >= g) && r["lte"].as_f64().is_none_or(|l| x <= l);
        }
        panic!("a condition Qdrant does not have: {c}")
    };
    let all = |name: &str| {
        filter
            .get(name)
            .and_then(Json::as_array)
            .cloned()
            .unwrap_or_default()
    };
    all("must").iter().all(cond) && !all("must_not").iter().any(cond)
}

/// Qdrant, its collection `kb`, its key `qk`.
pub(crate) fn qdrant() -> Server {
    serve(|_| {
        let mut made: Option<Json> = None;
        let mut points: Vec<Json> = Vec::new();
        Box::new(move |a: &Asked| -> (u16, Json) {
            assert_eq!(
                a.headers.get("api-key").map(String::as_str),
                Some("qk"),
                "the key in api-key"
            );
            let ok = |result: Json| (200, json!({ "result": result, "status": "ok", "time": 0.001 }));
            let path = a.path.as_str();
            match (a.method.as_str(), path) {
                ("GET", "/collections/kb/exists") => ok(json!({ "exists": made.is_some() })),
                ("PUT", "/collections/kb") => {
                    made = Some(a.body.clone());
                    ok(json!(true))
                }
                _ if made.is_none() => (
                    404,
                    json!({ "status": { "error": "Not found: Collection `kb` doesn't exist!" } }),
                ),
                ("PUT", "/collections/kb/index?wait=true") => {
                    ok(json!({ "operation_id": 1, "status": "completed" }))
                }
                ("PUT", "/collections/kb/points?wait=true") => {
                    for p in a.body["points"].as_array().unwrap() {
                        assert_eq!(p["id"].as_str().unwrap().len(), 36, "a UUID: {p}");
                        points.retain(|old| old["id"] != p["id"]);
                        points.push(p.clone());
                    }
                    ok(json!({ "operation_id": 2, "status": "completed" }))
                }
                ("POST", "/collections/kb/points/count") => {
                    let n = points
                        .iter()
                        .filter(|p| qdrant_holds(p, &a.body["filter"]))
                        .count();
                    ok(json!({ "count": n }))
                }
                ("POST", "/collections/kb/points/delete?wait=true") => {
                    points.retain(|p| !qdrant_holds(p, &a.body["filter"]));
                    ok(json!({ "operation_id": 3, "status": "completed" }))
                }
                ("POST", "/collections/kb/points/query") => {
                    let q = numbers(&a.body["query"]);
                    let found = points
                        .iter()
                        .filter(|p| a.body.get("filter").is_none_or(|f| qdrant_holds(p, f)))
                        .map(|p| (cosine(&q, &numbers(&p["vector"])), p.clone()))
                        .collect();
                    let limit = a.body["limit"].as_u64().unwrap() as usize;
                    let shown: Vec<Json> = sorted(found, limit)
                        .into_iter()
                        .map(|(s, p)| json!({ "id": p["id"], "version": 1, "score": s, "payload": p["payload"] }))
                        .collect();
                    ok(json!({ "points": shown }))
                }
                _ => (404, json!({ "status": { "error": format!("no route {path}") } })),
            }
        }) as Handler
    })
}

// --- Weaviate ---

/// Whether an object's properties hold a `where` filter: a search's (GraphQL's JSON: a list in `valueText`), or -
/// `batch` - a batch delete's (REST's: a list in `valueTextArray`).
fn weaviate_holds(properties: &Json, w: &Json, batch: bool) -> bool {
    match w["operator"].as_str().unwrap() {
        "And" => w["operands"]
            .as_array()
            .unwrap()
            .iter()
            .all(|o| weaviate_holds(properties, o, batch)),
        op => {
            let have = properties.get(w["path"][0].as_str().unwrap());
            let value = ["valueText", "valueNumber", "valueBoolean"]
                .iter()
                .find_map(|k| w.get(*k));
            let names: [&str; 3] = if batch {
                ["valueTextArray", "valueNumberArray", "valueBooleanArray"]
            } else {
                ["valueText", "valueNumber", "valueBoolean"]
            };
            let list = names.iter().find_map(|k| w.get(*k).and_then(Json::as_array));
            match op {
                "Equal" => is_or_holds(have, value.unwrap()),
                "ContainsAll" => list.unwrap().iter().all(|v| is_or_holds(have, v)),
                "ContainsAny" => list.unwrap().iter().any(|v| is_or_holds(have, v)),
                "ContainsNone" => !list.unwrap().iter().any(|v| is_or_holds(have, v)),
                other => panic!("an operator Weaviate's filter does not have: {other}"),
            }
        }
    }
}

/// Weaviate, its collection `Kb`, its key `wk`; `rest`: whether it has the search route (1.39.8 on), or answers
/// searches through GraphQL alone.
pub(crate) fn weaviate(rest: bool) -> Server {
    serve(move |_| {
        let mut class: Option<Json> = None;
        let mut objects: Vec<Json> = Vec::new();
        Box::new(move |a: &Asked| -> (u16, Json) {
            assert_eq!(
                a.headers.get("authorization").map(String::as_str),
                Some("Bearer wk")
            );
            match (a.method.as_str(), a.path.as_str()) {
                ("GET", "/v1/schema/Kb") => match &class {
                    Some(c) => (200, c.clone()),
                    None => (404, json!({ "error": [{ "message": "class Kb not found" }] })),
                },
                ("POST", "/v1/schema") => {
                    class = Some(a.body.clone());
                    (200, a.body.clone())
                }
                ("POST", "/v1/batch/objects") => {
                    let mut out = Vec::new();
                    for o in a.body["objects"].as_array().unwrap() {
                        assert_eq!(o["class"], "Kb");
                        objects.retain(|old| old["id"] != o["id"]);
                        objects.push(o.clone());
                        out.push(json!({ "class": "Kb", "id": o["id"], "result": { "status": "SUCCESS" } }));
                    }
                    (200, json!(out))
                }
                ("DELETE", "/v1/batch/objects") => {
                    let w = &a.body["match"]["where"];
                    let before = objects.len();
                    objects.retain(|o| !weaviate_holds(&o["properties"], w, true));
                    let n = before - objects.len();
                    (
                        200,
                        json!({ "match": a.body["match"], "output": "minimal", "dryRun": false,
                                  "results": { "matches": n, "limit": 10000, "successful": n, "failed": 0 } }),
                    )
                }
                ("POST", "/v1/search/Kb/near-vector") if rest => {
                    let q = numbers(&a.body["vector"]);
                    let found = objects
                        .iter()
                        .filter(|o| {
                            a.body
                                .get("where")
                                .is_none_or(|w| weaviate_holds(&o["properties"], w, false))
                        })
                        .map(|o| (cosine(&q, &numbers(&o["vector"])), o.clone()))
                        .collect();
                    let wanted: Vec<String> = a.body["returnProperties"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|p| p.as_str().unwrap().to_owned())
                        .collect();
                    let results: Vec<Json> = sorted(found, a.body["limit"].as_u64().unwrap() as usize)
                        .into_iter()
                        .map(|(s, o)| {
                            let props: serde_json::Map<String, Json> = wanted
                                .iter()
                                .map(|k| (k.clone(), o["properties"][k].clone()))
                                .collect();
                            json!({ "id": o["id"], "properties": props, "metadata": { "distance": 1.0 - s } })
                        })
                        .collect();
                    (200, json!({ "results": results, "tookMs": 1 }))
                }
                ("POST", "/v1/graphql") => {
                    let query = a.body["query"].as_str().unwrap();
                    assert!(query.starts_with("{ Get { Kb(nearVector: {vector: ["), "{query}");
                    let limit: usize = query
                        .split("limit: ")
                        .nth(1)
                        .unwrap()
                        .split(|c: char| !c.is_ascii_digit())
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap();
                    let start = query.find("vector: [").unwrap() + 8;
                    let end = start + query[start..].find(']').unwrap() + 1;
                    let q = numbers(&serde_json::from_str(&query[start..end]).unwrap());
                    let found = objects
                        .iter()
                        .map(|o| (cosine(&q, &numbers(&o["vector"])), o.clone()))
                        .collect();
                    let got: Vec<Json> = sorted(found, limit)
                        .into_iter()
                        .map(|(s, o)| {
                            let mut p = o["properties"].clone();
                            p["_additional"] = json!({ "distance": 1.0 - s });
                            p
                        })
                        .collect();
                    (200, json!({ "data": { "Get": { "Kb": got } } }))
                }
                (_, path) => (
                    404,
                    json!({ "error": [{ "message": format!("no route {path}") }] }),
                ),
            }
        }) as Handler
    })
}

// --- Milvus ---

/// The value an expression's field names: `metadata["a"]["b"]`.
fn milvus_field<'a>(entity: &'a Json, field: &str) -> Option<&'a Json> {
    let rest = field.strip_prefix("metadata").unwrap();
    rest.split("][")
        .map(|k| {
            k.trim_start_matches("[\"")
                .trim_end_matches("\"]")
                .trim_matches('"')
        })
        .try_fold(&entity["metadata"], |v, k| v.get(k))
}

fn milvus_holds(entity: &Json, expression: &str, params: &Json) -> bool {
    expression.split(" and ").all(|part| {
        let param = |p: &str| &params[p.trim().trim_start_matches('{').trim_end_matches(['}', ')'])];
        if let Some(rest) = part.strip_prefix("json_contains_all(") {
            let (field, p) = rest.split_once(", ").unwrap();
            let have = milvus_field(entity, field);
            return param(p).as_array().unwrap().iter().all(|v| is_or_holds(have, v));
        }
        if let Some((field, p)) = part.split_once(" == ") {
            return is_or_holds(milvus_field(entity, field), param(p));
        }
        if let Some((field, p)) = part.split_once(" not in ") {
            return !param(p)
                .as_array()
                .unwrap()
                .iter()
                .any(|v| same(&entity[field.trim()], v));
        }
        if let Some((field, p)) = part.split_once(" in ") {
            return param(p)
                .as_array()
                .unwrap()
                .iter()
                .any(|v| same(&entity[field.trim()], v));
        }
        panic!("an expression the stand-in does not read: {part}")
    })
}

/// Milvus, its collection `kb`, its token `root:Milvus`.
pub(crate) fn milvus() -> Server {
    serve(|_| {
        let mut made: Option<Json> = None;
        let mut entities: Vec<Json> = Vec::new();
        Box::new(move |a: &Asked| -> (u16, Json) {
            assert_eq!(
                a.headers.get("authorization").map(String::as_str),
                Some("Bearer root:Milvus")
            );
            assert_eq!(a.body["collectionName"], "kb", "{}", a.body);
            assert_eq!(a.method, "POST");
            let ok = |data: Json| (200, json!({ "code": 0, "data": data }));
            match a.path.as_str() {
                "/v2/vectordb/collections/has" => ok(json!({ "has": made.is_some() })),
                "/v2/vectordb/collections/create" => {
                    made = Some(a.body.clone());
                    ok(json!({}))
                }
                "/v2/vectordb/collections/load" => ok(json!({})),
                _ if made.is_none() => (
                    200,
                    json!({ "code": 100, "message": "collection not found[collection=kb]" }),
                ),
                "/v2/vectordb/entities/upsert" => {
                    let data = a.body["data"].as_array().unwrap();
                    for e in data {
                        entities.retain(|old| old["id"] != e["id"]);
                        entities.push(e.clone());
                    }
                    ok(
                        json!({ "upsertCount": data.len(), "upsertIds": data.iter().map(|e| e["id"].clone()).collect::<Vec<_>>() }),
                    )
                }
                "/v2/vectordb/entities/delete" => {
                    let filter = a.body["filter"].as_str().unwrap();
                    let before = entities.len();
                    entities.retain(|e| !milvus_holds(e, filter, &a.body["exprParams"]));
                    ok(json!({ "deleteCount": before - entities.len() }))
                }
                "/v2/vectordb/entities/search" => {
                    assert_eq!(a.body["annsField"], "vector");
                    let q = numbers(&a.body["data"][0]);
                    let filter = a.body["filter"].as_str();
                    let found = entities
                        .iter()
                        .filter(|e| filter.is_none_or(|f| milvus_holds(e, f, &a.body["exprParams"])))
                        .map(|e| (cosine(&q, &numbers(&e["vector"])), e.clone()))
                        .collect();
                    let hits: Vec<Json> = sorted(found, a.body["limit"].as_u64().unwrap() as usize)
                        .into_iter()
                        .map(|(s, e)| {
                            json!({ "id": e["id"], "distance": s, "source": e["source"], "position": e["position"],
                                    "text": e["text"], "metadata": e["metadata"] })
                        })
                        .collect();
                    ok(json!(hits))
                }
                other => (
                    200,
                    json!({ "code": 404, "message": format!("no route {other}") }),
                ),
            }
        }) as Handler
    })
}

// --- Chroma ---

fn chroma_holds(metadata: &Json, w: &Json) -> bool {
    let (k, cond) = w.as_object().unwrap().iter().next().unwrap();
    if k == "$and" {
        return cond.as_array().unwrap().iter().all(|c| chroma_holds(metadata, c));
    }
    let have = metadata.get(k);
    let (op, v) = cond.as_object().unwrap().iter().next().unwrap();
    match op.as_str() {
        "$eq" => have.is_some_and(|h| same(h, v)),
        "$contains" => have
            .and_then(Json::as_array)
            .is_some_and(|items| items.iter().any(|i| same(i, v))),
        "$in" => v
            .as_array()
            .unwrap()
            .iter()
            .any(|x| have.is_some_and(|h| same(h, x))),
        other => panic!("an operator Chroma's where does not have: {other}"),
    }
}

/// Chroma, its collection `kb1` (id `c-1`) in the default tenant and database, its key `ck`.
pub(crate) fn chroma() -> Server {
    serve(|_| {
        let mut made = false;
        let mut records: Vec<Json> = Vec::new();
        Box::new(move |a: &Asked| -> (u16, Json) {
            assert_eq!(a.headers.get("x-chroma-token").map(String::as_str), Some("ck"));
            let base = "/api/v2/tenants/default_tenant/databases/default_database/collections";
            let rest = a.path.strip_prefix(base).unwrap_or_else(|| panic!("{}", a.path));
            let collection = json!({ "id": "c-1", "name": "kb1", "configuration_json": {}, "tenant": "default_tenant",
                                     "database": "default_database", "log_position": 0, "version": 0 });
            match (a.method.as_str(), rest) {
                ("GET", "/kb1") if made => (200, collection),
                ("GET", "/kb1") => (
                    404,
                    json!({ "error": "NotFoundError", "message": "Collection [kb1] does not exist" }),
                ),
                ("POST", "") => {
                    assert_eq!(a.body["get_or_create"], true);
                    made = true;
                    (200, collection)
                }
                ("POST", "/c-1/upsert") => {
                    let ids = a.body["ids"].as_array().unwrap();
                    for (i, id) in ids.iter().enumerate() {
                        records.retain(|r| &r["id"] != id);
                        records.push(json!({ "id": id, "embedding": a.body["embeddings"][i], "document": a.body["documents"][i],
                                             "metadata": a.body["metadatas"][i] }));
                    }
                    (200, json!({}))
                }
                ("POST", "/c-1/query") => {
                    let q = numbers(&a.body["query_embeddings"][0]);
                    let found = records
                        .iter()
                        .filter(|r| {
                            a.body
                                .get("where")
                                .is_none_or(|w| chroma_holds(&r["metadata"], w))
                        })
                        .map(|r| (cosine(&q, &numbers(&r["embedding"])), r.clone()))
                        .collect();
                    let got = sorted(found, a.body["n_results"].as_u64().unwrap() as usize);
                    let list =
                        |f: &dyn Fn(&(f64, Json)) -> Json| json!([got.iter().map(f).collect::<Vec<_>>()]);
                    (
                        200,
                        json!({
                            "ids": list(&|(_, r)| r["id"].clone()),
                            "documents": list(&|(_, r)| r["document"].clone()),
                            "metadatas": list(&|(_, r)| r["metadata"].clone()),
                            "distances": list(&|(s, _)| json!(1.0 - s)),
                            "include": a.body["include"],
                        }),
                    )
                }
                ("POST", "/c-1/get") => {
                    let ids: Vec<Json> = records
                        .iter()
                        .filter(|r| chroma_holds(&r["metadata"], &a.body["where"]))
                        .skip(a.body["offset"].as_u64().unwrap_or(0) as usize)
                        .take(a.body["limit"].as_u64().unwrap_or(100) as usize)
                        .map(|r| r["id"].clone())
                        .collect();
                    (200, json!({ "ids": ids, "include": [] }))
                }
                ("POST", "/c-1/delete") => {
                    let before = records.len();
                    match (a.body.get("ids").and_then(Json::as_array), a.body.get("where")) {
                        (Some(ids), _) => records.retain(|r| !ids.contains(&r["id"])),
                        (None, Some(w)) => records.retain(|r| !chroma_holds(&r["metadata"], w)),
                        _ => panic!("a delete with neither ids nor where"),
                    }
                    (200, json!({ "deleted": before - records.len() }))
                }
                (_, other) => (
                    404,
                    json!({ "error": "NotFound", "message": format!("no route {other}") }),
                ),
            }
        }) as Handler
    })
}

// --- Pinecone ---

fn pinecone_holds(metadata: &Json, f: &Json) -> bool {
    f.as_object().unwrap().iter().all(|(k, cond)| {
        if k == "$and" {
            return cond
                .as_array()
                .unwrap()
                .iter()
                .all(|c| pinecone_holds(metadata, c));
        }
        let have = metadata.get(k);
        cond.as_object().unwrap().iter().all(|(op, v)| match op.as_str() {
            "$eq" => is_or_holds(have, v),
            "$in" => v.as_array().unwrap().iter().any(|x| is_or_holds(have, x)),
            "$nin" => !v.as_array().unwrap().iter().any(|x| is_or_holds(have, x)),
            other => panic!("an operator Pinecone's filter does not have: {other}"),
        })
    })
}

/// Pinecone's control plane and its index `kb`'s host, on one stand-in; its key `pk`, in namespace `docs`.
pub(crate) fn pinecone() -> Server {
    serve(|base| {
        let host = base.trim_start_matches("http://").to_owned();
        let mut made: Option<Json> = None;
        let mut vectors: Vec<Json> = Vec::new();
        Box::new(move |a: &Asked| -> (u16, Json) {
            assert_eq!(a.headers.get("api-key").map(String::as_str), Some("pk"));
            assert_eq!(
                a.headers.get("x-pinecone-api-version").map(String::as_str),
                Some("2026-07")
            );
            let described =
                json!({ "name": "kb", "host": host, "status": { "ready": true, "state": "Ready" } });
            match (a.method.as_str(), a.path.as_str()) {
                ("GET", "/indexes/kb") if made.is_some() => (200, described),
                ("GET", "/indexes/kb") => (
                    404,
                    json!({ "error": { "code": "NOT_FOUND", "message": "Index kb not found." }, "status": 404 }),
                ),
                ("POST", "/indexes") => {
                    made = Some(a.body.clone());
                    (
                        201,
                        json!({ "name": "kb", "host": host, "status": { "ready": false, "state": "Initializing" } }),
                    )
                }
                ("POST", "/vectors/upsert") => {
                    assert_eq!(a.body["namespace"], "docs");
                    let list = a.body["vectors"].as_array().unwrap();
                    for v in list {
                        vectors.retain(|old| old["id"] != v["id"]);
                        vectors.push(v.clone());
                    }
                    (200, json!({ "upsertedCount": list.len() }))
                }
                ("POST", "/query") => {
                    assert_eq!(a.body["namespace"], "docs");
                    let q = numbers(&a.body["vector"]);
                    let found = vectors
                        .iter()
                        .filter(|v| {
                            a.body
                                .get("filter")
                                .is_none_or(|f| pinecone_holds(&v["metadata"], f))
                        })
                        .map(|v| (cosine(&q, &numbers(&v["values"])), v.clone()))
                        .collect();
                    let matches: Vec<Json> = sorted(found, a.body["topK"].as_u64().unwrap() as usize)
                        .into_iter()
                        .map(|(s, v)| json!({ "id": v["id"], "score": s, "metadata": v["metadata"] }))
                        .collect();
                    (
                        200,
                        json!({ "matches": matches, "namespace": "docs", "usage": { "readUnits": 1 } }),
                    )
                }
                ("POST", "/vectors/delete") => {
                    assert_eq!(a.body["namespace"], "docs");
                    vectors.retain(|v| !pinecone_holds(&v["metadata"], &a.body["filter"]));
                    (200, json!({}))
                }
                (_, other) => (
                    404,
                    json!({ "error": { "code": "NOT_FOUND", "message": format!("no route {other}") } }),
                ),
            }
        }) as Handler
    })
}
