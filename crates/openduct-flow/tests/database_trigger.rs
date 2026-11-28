// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A database trigger's polls, run by a workflow's runner on a broker in memory, against a stand-in for a table:
//! the first poll starts from now, each new row runs once - across a member taking the poll over and a node that
//! queues an old time late - and a poll that fails keeps what was seen and says so once in the history.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{ClientSecurity, Outgoing, Shared, records};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::databases::DatabaseRequest;
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::{Credential, Effects, HttpRequest, Workflow};
use openduct_storage::MemoryStore;
use serde_json::{Value as Json, json};
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

const ORDERS: &str = "name: Orders\ntrigger:\n  mysql: { credential: shop, table: orders, cursor: id, every: 1m }\nsteps:\n  - name: seen\n    set: { id: value.id }\n";

/// A stand-in for the `orders` table, answering the trigger's two queries; or, while `down`, refusing.
struct Table {
    ids: Mutex<Vec<i64>>,
    down: Mutex<bool>,
    asked: Mutex<Vec<String>>,
}

impl Effects for Table {
    fn database<'a>(&'a self, r: DatabaseRequest) -> Pending<'a> {
        Box::pin(async move {
            assert!(r.mode.read_only, "a poll only reads");
            if *self.down.lock().unwrap() {
                return Err("cannot connect to the MySQL server at db:3306: refused".to_owned());
            }
            let s = &r.statements[0];
            let ids = self.ids.lock().unwrap().clone();
            if s.sql == "SELECT MAX(`id`) AS openduct_last FROM `orders` WHERE 1 = 1" {
                self.asked.lock().unwrap().push("max".to_owned());
                return Ok(Value::from_json(
                    &json!({ "rows": [{ "openduct_last": ids.iter().max() }] }),
                ));
            }
            assert_eq!(
                s.sql,
                "SELECT * FROM `orders` WHERE `id` > ? ORDER BY `id` LIMIT 50"
            );
            let openduct_connect::sql::Param::Int(after) = s.params[0] else {
                panic!("{:?}", s.params)
            };
            self.asked.lock().unwrap().push(after.to_string());
            let rows: Vec<Json> = ids
                .iter()
                .filter(|id| **id > after)
                .map(|id| json!({ "id": id }))
                .collect();
            Ok(Value::from_json(&json!({ "rows": rows })))
        })
    }

    fn http<'a>(&'a self, _: HttpRequest) -> Pending<'a> {
        Box::pin(async { Err("no HTTP here".to_owned()) })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no writes here".to_owned()) })
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        assert_eq!(name, "shop");
        Ok(Credential {
            kind: "mysql".to_owned(),
            fields: [("host".to_owned(), "db".to_owned())].into_iter().collect(),
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path} here")) })
    }
}

async fn ready() -> (Broker, Arc<Shared>, Arc<Workflow>, Topics) {
    let broker = Broker::start(
        BrokerConfig::new("127.0.0.1:0".parse().unwrap()),
        Arc::new(MemoryStore::new()),
    )
    .await
    .unwrap();
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let workflow = Arc::new(Workflow::parse(ORDERS).unwrap());
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    (broker, shared, workflow, topics)
}

/// A poll's time, as a node queues it to the schedule's partition.
async fn tick(shared: &Shared, topics: &Topics, slot: i64) {
    let at = iso(slot);
    let message = Outgoing {
        key: Some(format!("schedule/{slot}").into_bytes().into()),
        value: Some(
            envelope(
                &json!({ "scheduled_at": at }),
                &json!({ "trigger": "schedule", "at": at, "slot": slot }),
            )
            .into(),
        ),
        headers: Vec::new(),
    };
    shared.produce(&topics.input, 0, vec![message]).await.unwrap();
}

async fn all(shared: &Shared, topic: &str, partition: i32) -> Vec<(String, Json)> {
    let mut client = shared.lend().await.unwrap();
    let fetched = client
        .fetch(topic, &[(partition, 0)], 0, 8 * 1024 * 1024)
        .await
        .unwrap();
    let mut out = Vec::new();
    for answer in fetched {
        let mut batches = answer.batches;
        while !batches.is_empty() {
            let Ok(set) =
                RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            else {
                break;
            };
            for r in set.records {
                let key = r
                    .key
                    .map(|k| String::from_utf8_lossy(&k).into_owned())
                    .unwrap_or_default();
                let value = r
                    .value
                    .and_then(|v| serde_json::from_slice(&v).ok())
                    .unwrap_or(Json::Null);
                out.push((key, value));
            }
        }
    }
    out
}

/// The ids the runs saw, in the order they were kept.
async fn ran(shared: &Shared, topics: &Topics) -> Vec<i64> {
    all(shared, &topics.runs, 0)
        .await
        .into_iter()
        .filter(|(_, run)| run["status"] == "done")
        .map(|(_, run)| run["steps"][0]["result"]["id"].as_i64().unwrap())
        .collect()
}

async fn failures(shared: &Shared, topics: &Topics) -> Vec<Json> {
    all(shared, &topics.runs, 0)
        .await
        .into_iter()
        .filter(|(_, run)| run["status"] == "failed")
        .map(|(_, run)| run)
        .collect()
}

async fn seen(shared: &Shared, topics: &Topics) -> Option<Json> {
    all(shared, &topics.input, 0)
        .await
        .into_iter()
        .filter(|(k, _)| k == "poll/seen")
        .map(|(_, v)| v["seen"].clone())
        .next_back()
}

async fn until<F: Future<Output = bool>>(what: &str, mut done: impl FnMut() -> F) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !done().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

fn start(
    workflow: &Arc<Workflow>,
    shared: &Arc<Shared>,
    table: &Arc<Table>,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<Result<(), String>>) {
    let (stop, stopped) = watch::channel(false);
    let runner = tokio::spawn(serve(
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(table) as Arc<dyn Effects>,
        stopped,
    ));
    (stop, runner)
}

fn table(ids: &[i64]) -> Arc<Table> {
    Arc::new(Table {
        ids: Mutex::new(ids.to_vec()),
        down: Mutex::new(false),
        asked: Mutex::new(Vec::new()),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_new_row_runs_once_across_a_takeover_and_none_is_passed_by() {
    let (broker, shared, workflow, topics) = ready().await;
    let orders = table(&[10, 11]);

    let (stop, runner) = start(&workflow, &shared, &orders);
    tick(&shared, &topics, 60_000).await;
    until("the first poll", || async {
        seen(&shared, &topics).await.is_some()
    })
    .await;
    assert_eq!(
        seen(&shared, &topics).await.unwrap()["cursor"],
        json!(11),
        "it starts from now"
    );

    orders.ids.lock().unwrap().extend([12, 13]);
    tick(&shared, &topics, 120_000).await;
    until("two runs", || async { ran(&shared, &topics).await.len() >= 2 }).await;

    // The member is lost; another takes the poll over. A node queues an old time late, then the next one.
    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    orders.ids.lock().unwrap().push(14);
    tick(&shared, &topics, 120_000).await;
    tick(&shared, &topics, 180_000).await;
    let (stop, runner) = start(&workflow, &shared, &orders);
    until("the run after the takeover", || async {
        ran(&shared, &topics).await.len() >= 3
    })
    .await;
    // Room for a repeat to show itself, had there been one.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut ids = ran(&shared, &topics).await;
    ids.sort_unstable();
    assert_eq!(ids, [12, 13, 14]);
    assert_eq!(seen(&shared, &topics).await.unwrap()["cursor"], json!(14));
    assert_eq!(orders.asked.lock().unwrap().clone(), ["max", "11", "13"]);

    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_poll_that_fails_keeps_what_was_seen_and_says_so_once() {
    let (broker, shared, workflow, topics) = ready().await;
    let orders = table(&[1]);
    let (stop, runner) = start(&workflow, &shared, &orders);
    tick(&shared, &topics, 60_000).await;
    until("the first poll", || async {
        seen(&shared, &topics).await.is_some()
    })
    .await;

    *orders.down.lock().unwrap() = true;
    orders.ids.lock().unwrap().push(2);
    tick(&shared, &topics, 120_000).await;
    tick(&shared, &topics, 180_000).await;
    until("two failed polls kept", || async {
        seen(&shared, &topics)
            .await
            .is_some_and(|s| s["slot"] == json!(180_000))
    })
    .await;
    let failed = failures(&shared, &topics).await;
    assert_eq!(failed.len(), 1, "said once: {failed:?}");
    assert!(failed[0]["error"].as_str().unwrap().contains("cannot connect"));
    assert_eq!(failed[0]["trigger"]["trigger"], "mysql");
    let kept = seen(&shared, &topics).await.unwrap();
    assert_eq!(kept["cursor"], json!(1), "what was seen is kept");

    *orders.down.lock().unwrap() = false;
    tick(&shared, &topics, 240_000).await;
    until("the row after the failure", || async {
        ran(&shared, &topics).await == [2]
    })
    .await;

    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    broker.shutdown().await;
}
