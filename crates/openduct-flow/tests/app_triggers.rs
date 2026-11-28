// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app's poll, run by a workflow's runner on a broker in memory, against a stand-in for Telegram: the first poll
//! starts from now, each new message runs once, a member taking the poll over neither repeats a message nor passes
//! one by, and a poll that fails keeps what was seen and says so once in the history.

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
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::{Credential, Effects, HttpRequest, Workflow};
use openduct_storage::MemoryStore;
use serde_json::{Value as Json, json};
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

const BOT: &str = "name: Bot\ntrigger:\n  telegram: { when: new_message, credential: bot, every: 1m }\nsteps:\n  - name: echo\n    set: { text: value.text }\n";

/// A stand-in for Telegram's Bot API: the messages waiting, answered from the offset asked for; or, while `down`, a
/// refusal.
struct Telegram {
    updates: Mutex<Vec<(i64, String)>>,
    down: Mutex<bool>,
    asked: Mutex<Vec<String>>,
}

impl Telegram {
    fn new() -> Self {
        Self {
            updates: Mutex::new(Vec::new()),
            down: Mutex::new(false),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn say(&self, id: i64, text: &str) {
        self.updates.lock().unwrap().push((id, text.to_owned()));
    }
}

impl Effects for Telegram {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            assert!(request.url.ends_with("/bot123:abc/getUpdates"), "{}", request.url);
            let offset: i64 = request
                .query
                .iter()
                .find(|(k, _)| k == "offset")
                .map_or(0, |(_, v)| v.parse().unwrap());
            self.asked.lock().unwrap().push(offset.to_string());
            if *self.down.lock().unwrap() {
                return Ok(Value::from_json(&json!({
                    "status": 502, "headers": {}, "body": { "ok": false, "description": "Bad Gateway" },
                })));
            }
            let result: Vec<Json> = self
                .updates
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, _)| *id >= offset)
                .map(|(id, text)| json!({ "update_id": id, "message": { "message_id": id, "text": text } }))
                .collect();
            Ok(Value::from_json(
                &json!({ "status": 200, "headers": {}, "body": { "ok": true, "result": result } }),
            ))
        })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no writes here".to_owned()) })
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        assert_eq!(name, "bot");
        Ok(Credential {
            kind: "telegram".to_owned(),
            fields: [("bot_token".to_owned(), "123:abc".to_owned())]
                .into_iter()
                .collect(),
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
    let workflow = Arc::new(Workflow::parse(BOT).unwrap());
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

/// Every record of a partition, as (key, JSON value).
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

/// What each run of the history echoed, in the order they were kept.
async fn echoed(shared: &Shared, topics: &Topics) -> Vec<String> {
    all(shared, &topics.runs, 0)
        .await
        .into_iter()
        .filter(|(_, run)| run["status"] == "done")
        .map(|(_, run)| {
            run["steps"][0]["result"]["text"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

/// What the polls have seen, as the schedule's partition last says.
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
    telegram: &Arc<Telegram>,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<Result<(), String>>) {
    let (stop, stopped) = watch::channel(false);
    let runner = tokio::spawn(serve(
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(telegram) as Arc<dyn Effects>,
        stopped,
    ));
    (stop, runner)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_new_message_runs_once_across_a_takeover_and_none_is_passed_by() {
    let (broker, shared, workflow, topics) = ready().await;
    let telegram = Arc::new(Telegram::new());
    telegram.say(100, "waiting before the workflow was on");

    let (stop, runner) = start(&workflow, &shared, &telegram);
    // The first poll starts from now: what waits is noted, and not run.
    tick(&shared, &topics, 60_000).await;
    until("the first poll", || async {
        seen(&shared, &topics).await.is_some()
    })
    .await;
    assert_eq!(seen(&shared, &topics).await.unwrap()["cursor"], json!(101));

    telegram.say(101, "hello");
    telegram.say(102, "are you there");
    tick(&shared, &topics, 120_000).await;
    until("two runs", || async { echoed(&shared, &topics).await.len() >= 2 }).await;
    let mut ran = echoed(&shared, &topics).await;
    ran.sort();
    assert_eq!(ran, ["are you there", "hello"]);

    // The member is lost; another takes the poll over. A node that queued an old time late, and the next time.
    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    telegram.say(103, "after the takeover");
    tick(&shared, &topics, 120_000).await;
    tick(&shared, &topics, 180_000).await;
    let (stop, runner) = start(&workflow, &shared, &telegram);
    until("the run after the takeover", || async {
        echoed(&shared, &topics).await.len() >= 3
    })
    .await;
    // Room for a repeat to show itself, had there been one.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut ran = echoed(&shared, &topics).await;
    ran.sort();
    assert_eq!(ran, ["after the takeover", "are you there", "hello"]);
    assert_eq!(seen(&shared, &topics).await.unwrap()["cursor"], json!(104));
    // Each poll asked from after the last message dealt with.
    let asked = telegram.asked.lock().unwrap().clone();
    assert_eq!(asked, ["0", "101", "103"], "{asked:?}");

    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_poll_that_fails_keeps_what_was_seen_and_says_so_once() {
    let (broker, shared, workflow, topics) = ready().await;
    let telegram = Arc::new(Telegram::new());
    let (stop, runner) = start(&workflow, &shared, &telegram);
    tick(&shared, &topics, 60_000).await;
    until("the first poll", || async {
        seen(&shared, &topics).await.is_some()
    })
    .await;

    // Telegram is down for two polls while a message waits.
    *telegram.down.lock().unwrap() = true;
    telegram.say(7, "sent while it was down");
    tick(&shared, &topics, 120_000).await;
    tick(&shared, &topics, 180_000).await;
    until("both failed polls", || async {
        seen(&shared, &topics)
            .await
            .is_some_and(|s| s["slot"] == json!(180_000))
    })
    .await;
    let failed: Vec<Json> = all(&shared, &topics.runs, 0)
        .await
        .into_iter()
        .map(|(_, r)| r)
        .filter(|r| r["status"] == "failed")
        .collect();
    assert_eq!(
        failed.len(),
        1,
        "one failure said, not one for each poll: {failed:?}"
    );
    assert!(
        failed[0]["error"]
            .as_str()
            .unwrap()
            .contains("Telegram answered 502"),
        "{failed:?}"
    );
    assert!(
        seen(&shared, &topics).await.unwrap()["failing"]
            .as_str()
            .unwrap()
            .contains("502")
    );

    // Back up: the message waiting is run - not passed by.
    *telegram.down.lock().unwrap() = false;
    tick(&shared, &topics, 240_000).await;
    until("the waiting message", || async {
        echoed(&shared, &topics).await.len() == 1
    })
    .await;
    assert_eq!(echoed(&shared, &topics).await, ["sent while it was down"]);
    assert!(seen(&shared, &topics).await.unwrap()["failing"].is_null());

    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_poll_that_fails_is_said_once_and_the_next_still_starts_from_now() {
    let (broker, shared, workflow, topics) = ready().await;
    let telegram = Arc::new(Telegram::new());
    *telegram.down.lock().unwrap() = true;
    telegram.say(1, "waiting before the workflow was on");
    let (stop, runner) = start(&workflow, &shared, &telegram);
    tick(&shared, &topics, 60_000).await;
    tick(&shared, &topics, 120_000).await;
    until("both failed first polls", || async {
        seen(&shared, &topics)
            .await
            .is_some_and(|s| s["slot"] == json!(120_000))
    })
    .await;
    assert_eq!(seen(&shared, &topics).await.unwrap()["pending"], json!(true));
    let failed = all(&shared, &topics.runs, 0).await;
    assert_eq!(failed.len(), 1, "said once: {failed:?}");

    // Up again: the first poll that works notes what waits, and runs nothing; what comes after runs.
    *telegram.down.lock().unwrap() = false;
    tick(&shared, &topics, 180_000).await;
    until("the first poll that works", || async {
        seen(&shared, &topics)
            .await
            .is_some_and(|s| s["pending"] == json!(false))
    })
    .await;
    telegram.say(2, "after");
    tick(&shared, &topics, 240_000).await;
    until("the message after", || async {
        echoed(&shared, &topics).await.len() == 1
    })
    .await;
    assert_eq!(echoed(&shared, &topics).await, ["after"]);

    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    broker.shutdown().await;
}
