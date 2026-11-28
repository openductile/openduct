// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The vector stores end to end, each against a stand-in for its API (see `vector_support`): pieces added (the
//! collection made), found by a vector with filters on a field and on a list, a document added again with fewer
//! pieces, a document deleted - and in a test run and a preview. Each test also checks the requests its store was
//! sent against the shapes its vendor documents: the paths, the bodies, the filter in the store's own language.
//! `store: memory` keeps its pieces for the run alone; a filter a store cannot express is refused when read.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod vector_support;

use openduct_flow::Workflow;
use serde_json::json;
use vector_support::{
    ADD, SEARCH, credential, failed, go, live, native, pieces, result, scenario, test_run_and_preview, web,
    workflow,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qdrant_keeps_finds_replaces_and_deletes_by_its_rest_api() {
    let server = web::qdrant();
    let vs = credential("qdrant", &[("url", &server.base), ("api_key", "qk")]);
    scenario("qdrant", "kb", vs.clone(), true).await;
    let made = server.to("PUT", "/collections/kb");
    assert_eq!(made[0].path, "/collections/kb");
    assert_eq!(
        made[0].body,
        json!({ "vectors": { "size": 3, "distance": "Cosine" } })
    );
    let indexes: Vec<_> = made
        .iter()
        .filter(|a| a.path.starts_with("/collections/kb/index"))
        .map(|a| a.body.clone())
        .collect();
    assert_eq!(
        indexes,
        [
            json!({ "field_name": "source", "field_schema": "keyword" }),
            json!({ "field_name": "piece_id", "field_schema": "keyword" })
        ]
    );
    let upsert = &server.to("PUT", "/collections/kb/points?wait=true")[0].body;
    let point = &upsert["points"][0];
    assert_eq!(point["vector"], json!([1.0, 0.0, 0.0]));
    assert_eq!(point["payload"]["piece_id"], "handbook#0");
    assert_eq!(point["payload"]["metadata"]["team"], "support");
    let queries = server.to("POST", "/collections/kb/points/query");
    assert_eq!(queries[0].body["with_payload"], true);
    assert_eq!(queries[0].body["limit"], 2);
    assert_eq!(
        queries[1].body["filter"],
        json!({ "must": [{ "key": "metadata.team", "match": { "value": "it" } }] })
    );
    let deletes = server.to("POST", "/collections/kb/points/delete");
    assert!(
        deletes[0].body["filter"]["must_not"][0]["has_id"].is_array(),
        "the pieces kept are not removed"
    );
    assert_eq!(
        deletes[1].body["filter"],
        json!({ "must": [{ "key": "source", "match": { "any": ["accounts"] } }] })
    );
    test_run_and_preview("qdrant", "kb", vs, &|| server.count()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn weaviate_keeps_finds_replaces_and_deletes_and_an_older_one_is_searched_through_graphql() {
    let server = web::weaviate(true);
    let vs = credential("weaviate", &[("url", &server.base), ("api_key", "wk")]);
    scenario("weaviate", "kb", vs.clone(), true).await;
    let made = &server.to("POST", "/v1/schema")[0].body;
    assert_eq!(
        (made["class"].clone(), made["vectorizer"].clone()),
        (json!("Kb"), json!("none"))
    );
    assert_eq!(made["vectorIndexConfig"], json!({ "distance": "cosine" }));
    let props: Vec<&str> = made["properties"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        props,
        [
            "piece_id",
            "piece_source",
            "piece_position",
            "piece_text",
            "piece_metadata",
            "tags",
            "team",
            "year"
        ]
    );
    assert_eq!(
        made["properties"][5],
        json!({ "name": "tags", "dataType": ["text[]"], "tokenization": "field" })
    );
    let object = &server.to("POST", "/v1/batch/objects")[0].body["objects"][0];
    assert_eq!(object["properties"]["team"], "support");
    let kept: serde_json::Value =
        serde_json::from_str(object["properties"]["piece_metadata"].as_str().unwrap()).unwrap();
    assert_eq!(kept, pieces()[0]["metadata"], "the whole metadata, as JSON text");
    let searches = server.to("POST", "/v1/search/Kb/near-vector");
    assert_eq!(searches[0].body["returnMetadata"], json!(["distance"]));
    assert_eq!(
        searches[2].body["where"],
        json!({ "path": ["tags"], "operator": "ContainsAll", "valueText": ["money"] })
    );
    let deletes = server.to("DELETE", "/v1/batch/objects");
    assert_eq!(
        deletes[0].body["match"]["where"]["operands"][1]["operator"], "ContainsNone",
        "the pieces kept are not removed"
    );
    test_run_and_preview("weaviate", "kb", vs, &|| server.count()).await;

    let older = web::weaviate(false);
    let vs = credential("weaviate", &[("url", &older.base), ("api_key", "wk")]);
    let fx = live(vs);
    result(
        &go(
            &workflow(ADD, "weaviate", "kb", ""),
            json!({ "pieces": pieces() }),
            &fx,
        )
        .await,
        "store",
    );
    let found = result(
        &go(
            &workflow(SEARCH, "weaviate", "kb", ""),
            json!({ "v": [0.9, 0.1, 0.0] }),
            &fx,
        )
        .await,
        "find",
    );
    assert_eq!(found["matches"][0]["id"], "handbook#0", "{found}");
    assert_eq!(
        older.to("POST", "/v1/graphql").len(),
        1,
        "asked through GraphQL once its search route was not there"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn milvus_keeps_finds_replaces_and_deletes_with_its_values_apart_from_the_expression() {
    let server = web::milvus();
    let vs = credential("milvus", &[("url", &server.base), ("token", "root:Milvus")]);
    scenario("milvus", "kb", vs.clone(), true).await;
    let made = &server.to("POST", "/v2/vectordb/collections/create")[0].body;
    let fields: Vec<(&str, &str)> = made["schema"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["fieldName"].as_str().unwrap(), f["dataType"].as_str().unwrap()))
        .collect();
    assert_eq!(
        fields,
        [
            ("id", "VarChar"),
            ("vector", "FloatVector"),
            ("source", "VarChar"),
            ("position", "Int64"),
            ("text", "VarChar"),
            ("metadata", "JSON")
        ]
    );
    assert_eq!(made["schema"]["fields"][1]["elementTypeParams"]["dim"], 3);
    assert_eq!(made["indexParams"][0]["metricType"], "COSINE");
    assert_eq!(
        server.to("POST", "/v2/vectordb/collections/load").len(),
        1,
        "loaded once made"
    );
    let searches = server.to("POST", "/v2/vectordb/entities/search");
    assert_eq!(searches[1].body["filter"], "metadata[\"team\"] == {v0}");
    assert_eq!(searches[1].body["exprParams"], json!({ "v0": "it" }));
    assert_eq!(
        searches[2].body["filter"],
        "json_contains_all(metadata[\"tags\"], {v0})"
    );
    let deletes = server.to("POST", "/v2/vectordb/entities/delete");
    assert_eq!(
        deletes[0].body["filter"],
        "source in {sources} and id not in {keep}"
    );
    assert_eq!(
        deletes[1].body["exprParams"],
        json!({ "sources": ["handbook"], "keep": ["handbook#0"] })
    );
    assert_eq!(
        (
            deletes[2].body["filter"].clone(),
            deletes[2].body["exprParams"].clone()
        ),
        (json!("source in {sources}"), json!({ "sources": ["accounts"] }))
    );
    test_run_and_preview("milvus", "kb", vs, &|| server.count()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chroma_keeps_finds_replaces_and_deletes_by_its_http_api() {
    let server = web::chroma();
    let vs = credential("chroma", &[("url", &server.base), ("api_key", "ck")]);
    scenario("chroma", "kb1", vs.clone(), true).await;
    let base = "/api/v2/tenants/default_tenant/databases/default_database/collections";
    let made = server
        .to("POST", base)
        .into_iter()
        .find(|a| a.path == base)
        .unwrap();
    assert_eq!(
        made.body["configuration"],
        json!({ "hnsw": { "space": "cosine" } })
    );
    let upsert = &server.to("POST", &format!("{base}/c-1/upsert"))[0].body;
    assert_eq!(upsert["ids"][0], "handbook#0");
    assert_eq!(upsert["documents"][1], "Parcels ship in two working days.");
    let m = &upsert["metadatas"][0];
    assert_eq!(
        (m["team"].clone(), m["tags"].clone(), m["piece_source"].clone()),
        (json!("support"), json!(["faq", "money"]), json!("handbook"))
    );
    let queries = server.to("POST", &format!("{base}/c-1/query"));
    assert_eq!(queries[1].body["where"], json!({ "team": { "$eq": "it" } }));
    assert_eq!(
        queries[2].body["where"],
        json!({ "tags": { "$contains": "money" } })
    );
    assert_eq!(
        queries[0].body["include"],
        json!(["documents", "metadatas", "distances"])
    );
    let deletes = server.to("POST", &format!("{base}/c-1/delete"));
    assert_eq!(
        deletes[0].body,
        json!({ "ids": ["handbook#1"] }),
        "the stale piece by its id"
    );
    assert_eq!(
        deletes[1].body,
        json!({ "where": { "piece_source": { "$in": ["accounts"] } } })
    );
    test_run_and_preview("chroma", "kb1", vs, &|| server.count()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinecone_makes_its_index_and_keeps_finds_and_deletes_in_a_namespace() {
    let server = web::pinecone();
    let vs = credential("pinecone", &[("url", &server.base), ("api_key", "pk")]);
    // Pinecone's own vector API names the namespace on every request.
    let ns = "      namespace: docs\n";
    let fx = live(vs.clone());
    let added = result(
        &go(
            &workflow(ADD, "pinecone", "kb", ns),
            json!({ "pieces": pieces() }),
            &fx,
        )
        .await,
        "store",
    );
    assert_eq!(
        (added["added"].clone(), added["created"].clone()),
        (json!(3), json!(true)),
        "{added}"
    );
    let made = &server.to("POST", "/indexes")[0].body;
    assert_eq!(
        made,
        &json!({
            "name": "kb",
            "deployment": { "deployment_type": "managed", "cloud": "aws", "region": "us-east-1" },
            "schema": { "fields": { "_values": { "type": "dense_vector", "dimension": 3, "metric": "cosine" } } },
            "deletion_protection": "disabled",
        })
    );
    let v = &server.to("POST", "/vectors/upsert")[0].body["vectors"][0];
    assert_eq!(
        (v["id"].clone(), v["values"].clone()),
        (json!("handbook#0"), json!([1.0, 0.0, 0.0]))
    );
    assert_eq!(
        v["metadata"]["piece_text"],
        "Money back on any return within 14 days."
    );
    assert_eq!(v["metadata"]["tags"], json!(["faq", "money"]));
    let search = |more: &str| workflow(SEARCH, "pinecone", "kb", &format!("{ns}{more}"));
    let found = result(
        &go(&search(""), json!({ "v": [0.9, 0.1, 0.0] }), &fx).await,
        "find",
    );
    assert_eq!(found["matches"][0]["id"], "handbook#0", "{found}");
    assert_eq!(found["matches"][0]["metadata"], pieces()[0]["metadata"]);
    let money = result(
        &go(
            &search("      filter: { team: support, tags: [money] }\n"),
            json!({ "v": [0.0, 1.0, 0.0] }),
            &fx,
        )
        .await,
        "find",
    );
    assert_eq!(money["count"], 1, "{money}");
    let query = server.to("POST", "/query");
    assert_eq!(
        query[1].body["filter"],
        json!({ "$and": [{ "team": { "$eq": "support" } }, { "tags": { "$eq": "money" } }] })
    );
    assert_eq!(
        (
            query[0].body["topK"].clone(),
            query[0].body["includeMetadata"].clone()
        ),
        (json!(2), json!(true))
    );
    let again = result(
        &go(
            &workflow(ADD, "pinecone", "kb", ns),
            json!({ "pieces": [pieces()[0].clone()] }),
            &fx,
        )
        .await,
        "store",
    );
    assert_eq!(
        again["removed"],
        json!(null),
        "Pinecone does not say how many: {again}"
    );
    // The first add removed nothing of its own documents; the second, the handbook's piece it no longer has.
    let delete = &server.to("POST", "/vectors/delete")[1].body;
    assert_eq!(
        delete["filter"],
        json!({ "$and": [{ "piece_source": { "$in": ["handbook"] } }, { "piece_id": { "$nin": ["handbook#0"] } }] })
    );
    let left = result(
        &go(&search(""), json!({ "v": [0.0, 1.0, 0.0] }), &fx).await,
        "find",
    );
    assert!(
        left["matches"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["id"] != "handbook#1"),
        "{left}"
    );
    // A list of numbers is not one Pinecone's metadata holds: refused when the workflow is read.
    let e = Workflow::parse(&search("      filter: { years: [2024, 2025] }\n")).unwrap_err();
    assert!(e.contains("Pinecone lists hold text only"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_keeps_finds_replaces_and_deletes_by_its_search_commands() {
    let (port, state) = native::redis();
    let vs = credential("redis", &[("url", &format!("redis://127.0.0.1:{port}"))]);
    scenario("redis", "kb", vs.clone(), true).await;
    check_redis(&state.lock().unwrap());
    let sent = || state.lock().unwrap().commands.len();
    test_run_and_preview("redis", "kb", vs, &sent).await;
}

/// What the Redis stand-in was sent: the index, the hashes, the searches.
fn check_redis(held: &native::RedisState) {
    let create = held.index.clone().unwrap();
    assert_eq!(
        create.join(" "),
        "FT.CREATE kb ON HASH PREFIX 1 kb: SCHEMA embedding VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC COSINE \
         source_tag TAG tags TAG"
    );
    let piece = &held.hashes[&b"kb:handbook#0".to_vec()];
    assert_eq!(
        piece["embedding"],
        [0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0],
        "32-bit floats, little-endian"
    );
    assert_eq!(String::from_utf8_lossy(&piece["source"]), "handbook");
    let searches: Vec<&Vec<String>> = held
        .commands
        .iter()
        .filter(|c| c[0] == "FT.SEARCH" && c[2].contains("KNN"))
        .collect();
    assert_eq!(searches[0][2], "*=>[KNN 2 @embedding $vec AS vector_distance]");
    assert!(
        searches[1][2].starts_with("(@tags:{")
            && searches[1][2].ends_with("})=>[KNN 2 @embedding $vec AS vector_distance]")
    );
    assert_eq!(searches[0].last().unwrap(), "2", "DIALECT 2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mongodb_keeps_finds_replaces_and_deletes_with_atlas_vector_search() {
    let (port, state) = native::mongo();
    let vs = credential("mongodb", &[("url", &format!("mongodb://127.0.0.1:{port}"))]);
    scenario("mongodb", "shop.pieces", vs.clone(), true).await;
    check_mongo(&state.lock().unwrap());
    let sent = || state.lock().unwrap().commands.len();
    test_run_and_preview("mongodb", "shop.pieces", vs, &sent).await;
}

/// What the MongoDB stand-in was sent: the index, the documents, the searches.
fn check_mongo(held: &native::MongoState) {
    let index = native::json_of(&held.indexes[0]);
    assert_eq!(
        index,
        json!({ "name": "vector_index", "type": "vectorSearch", "definition": { "fields": [
            { "type": "vector", "path": "embedding", "numDimensions": 3, "similarity": "cosine" },
            { "type": "filter", "path": "source" },
            { "type": "filter", "path": "tags" },
        ] } })
    );
    let doc = native::json_of(&held.collections["pieces"][0]);
    assert_eq!(doc["_id"], "handbook#0");
    assert_eq!(
        doc["tags"],
        json!(["tags=\"faq\"", "tags=\"money\"", "team=\"support\"", "year=2024"])
    );
    let searches: Vec<serde_json::Value> = held
        .commands
        .iter()
        .map(native::json_of)
        .filter(|c| c["pipeline"][0].get("$vectorSearch").is_some())
        .collect();
    assert_eq!(
        searches[1]["pipeline"][0]["$vectorSearch"]["filter"],
        json!({ "tags": "team=\"it\"" })
    );
    assert_eq!(
        searches[0]["pipeline"][1]["$project"]["score"],
        json!({ "$meta": "vectorSearchScore" })
    );
    assert!(held.commands.iter().all(|c| c.get_str("$db").is_ok()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_run_s_memory_keeps_pieces_for_the_run_alone() {
    let flow = "name: memory\nsteps:\n  - name: keep\n    vector_store:\n      action: add\n      store: memory\n      collection: kb\n      pieces: value.pieces\n  - name: find\n    vector_store:\n      action: search\n      store: memory\n      collection: kb\n      vector: value.v\n      top_k: 2\n      filter: { tags: [money] }\n";
    let input = json!({ "pieces": pieces(), "v": [0.0, 1.0, 0.0] });
    // A test run keeps and finds too: nothing leaves the run.
    for fx in [
        &live(credential("test", &[])) as &dyn openduct_flow::Effects,
        &openduct_flow::DryRun::default(),
    ] {
        let record = go(flow, input.clone(), fx).await;
        let found = result(&record, "find");
        assert_eq!(
            (found["count"].clone(), found["store"].clone()),
            (json!(1), json!("memory")),
            "{found}"
        );
        assert_eq!(found["matches"][0]["id"], "handbook#0");
    }
    // Another run starts with none.
    let search_only = "name: s\nsteps:\n  - name: find\n    vector_store: { action: search, store: memory, collection: kb, vector: value.v }\n";
    let e = failed(
        &go(
            search_only,
            json!({ "v": [1.0, 0.0, 0.0] }),
            &openduct_flow::DryRun::default(),
        )
        .await,
    );
    assert!(e.contains("adds pieces to it first"), "{e}");
}

#[test]
fn a_filter_or_a_name_a_store_cannot_take_is_refused_when_the_workflow_is_read() {
    let read = |yaml: String| Workflow::parse(&yaml).map(|_| ()).unwrap_err();
    for (store, collection, more, says) in [
        (
            "weaviate",
            "kb",
            "      filter: { author: { name: Ann } }\n",
            "Weaviate filters only by the top fields",
        ),
        (
            "chroma",
            "kb1",
            "      filter: { a: null }\n",
            "a value to be equal to",
        ),
        (
            "qdrant",
            "kb",
            "      filter: { \"bad key\": x }\n",
            "is not a field Qdrant filters by",
        ),
        ("pinecone", "Docs", "", "small letters, digits and -"),
        ("mongodb", "pieces", "", "database.collection"),
        ("faiss", "kb", "", "store: postgres, qdrant, weaviate"),
        (
            "qdrant",
            "kb",
            "      namespace: x\n",
            "namespace: a setting of Pinecone only",
        ),
    ] {
        let e = read(workflow(SEARCH, store, collection, more));
        assert!(e.contains(says), "{store}: {e}");
    }
    // A store that keeps metadata's shape takes a field inside a field.
    assert!(
        Workflow::parse(&workflow(
            SEARCH,
            "mongodb",
            "shop.p",
            "      filter: { author: { name: Ann } }\n"
        ))
        .is_ok()
    );
    // With no store named, the credential's kind chooses it when the step runs; a filter worked out then is checked
    // then, before anything is sent.
    let flow = "name: t\nsteps:\n  - name: find\n    vector_store: { action: search, credential: vs, collection: kb1, vector: value.v, filter: value.f }\n";
    let server = web::chroma();
    let fx = live(credential("chroma", &[("url", &server.base), ("api_key", "ck")]));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let e = failed(&runtime.block_on(go(
        flow,
        json!({ "v": [1.0, 0.0, 0.0], "f": { "a": { "b": 1 } } }),
        &fx,
    )));
    assert!(e.contains("Chroma filters only by the top fields"), "{e}");
    assert_eq!(server.count(), 0, "nothing was asked");
    // A test run of a workflow whose credential is not made yet shows where each request would go.
    for (store, collection, path) in [
        (
            "qdrant",
            "kb",
            "https://<the qdrant credential's url>/collections/kb/points/query",
        ),
        ("pinecone", "kb", "https://api.pinecone.io/indexes/kb"),
        (
            "weaviate",
            "Kb",
            "https://<the weaviate credential's url>/v1/search/Kb/near-vector",
        ),
    ] {
        let record = runtime.block_on(go(
            &workflow(SEARCH, store, collection, ""),
            json!({ "v": [1.0, 0.0, 0.0] }),
            &openduct_flow::DryRun::default(),
        ));
        let shown = result(&record, "find");
        assert_eq!(
            (shown["test"].clone(), shown["request"]["url"].clone()),
            (json!("not sent"), json!(path)),
            "{shown}"
        );
    }
}
