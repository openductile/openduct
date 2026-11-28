// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The storage triggers on a broker in memory: a folder's new files run once each - with two members reading the
//! workflow (two nodes), each time queued by both, and across a restart - and a Redis channel's messages run once,
//! listened to by one node of two, and by another once it is gone.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod storage_support;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_broker::client::{ClientSecurity, Outgoing, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::storage::{Places, Reach, Roots};
use openduct_flow::{Credential, Effects, HttpRequest, Trigger, Workflow};
use openduct_storage::MemoryStore;
use serde_json::json;
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A node's effects: its folders and servers, and each run's call kept (the last part of its address).
struct Node {
    places: Places,
    redis: Option<String>,
    calls: Mutex<Vec<String>>,
}

impl Node {
    fn new(root: Option<&PathBuf>, redis: Option<String>) -> Arc<Self> {
        let given: Vec<String> = root
            .map(|r| format!("inbox={}", r.display()))
            .into_iter()
            .collect();
        Arc::new(Self {
            places: Places {
                roots: Arc::new(Roots::parse(&given).unwrap()),
                allow_private: true,
            },
            redis,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn ran(&self) -> Vec<String> {
        let mut ran = self.calls.lock().unwrap().clone();
        ran.sort();
        ran
    }
}

impl Effects for Node {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let last = request.url.rsplit('/').next().unwrap_or_default().to_owned();
            self.calls.lock().unwrap().push(last);
            Ok(Value::from_json(
                &json!({ "status": 200, "headers": {}, "body": {} }),
            ))
        })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no writes here".to_owned()) })
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        let url = self
            .redis
            .clone()
            .ok_or_else(|| format!("no credential {name} here"))?;
        Ok(Credential {
            kind: "redis".to_owned(),
            fields: [("url".to_owned(), url)].into_iter().collect(),
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path} here")) })
    }

    fn storage(&self) -> Reach<'_> {
        Reach::Live(&self.places)
    }
}

async fn broker() -> (Broker, Arc<Shared>) {
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
    (broker, shared)
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

/// How many polls have kept what they saw, in the schedule's partition.
async fn polls_kept(shared: &Shared, topics: &Topics) -> usize {
    use kafka_protocol::records::RecordBatchDecoder;
    let mut client = shared.lend().await.unwrap();
    let fetched = client
        .fetch(&topics.input, &[(0, 0)], 0, 4 * 1024 * 1024)
        .await
        .unwrap();
    let mut kept = 0;
    for answer in fetched.into_iter().filter(|f| f.partition == 0) {
        let mut batches = answer.batches;
        while !batches.is_empty() {
            let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                &mut batches,
                Some(openduct_broker::client::records::decompress),
            ) else {
                break;
            };
            kept += set
                .records
                .iter()
                .filter(|r| r.key.as_deref() == Some(&b"poll/seen"[..]))
                .count();
        }
    }
    kept
}

/// Ticks from `slot` on, a second apart, until a poll has kept what it saw: a time claimed while the group was
/// shared out again is passed by, as it should be.
async fn first_poll(shared: &Shared, topics: &Topics, mut slot: i64) {
    for _ in 0..30 {
        tick(shared, topics, slot).await;
        tick(shared, topics, slot).await;
        slot += 1_000;
        tokio::time::sleep(Duration::from_secs(1)).await;
        if polls_kept(shared, topics).await > 0 {
            return;
        }
    }
    panic!("no poll kept what it saw");
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

fn runner(
    workflow: &Arc<Workflow>,
    shared: &Arc<Shared>,
    node: &Arc<Node>,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<Result<(), String>>) {
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(serve(
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(node) as Arc<dyn Effects>,
        stopped,
    ));
    (stop, task)
}

const WATCH: &str = "name: inbox\ntrigger:\n  storage:\n    root: inbox\n    folder: in\n    pattern: '*.csv'\n    every: 10s\n\
    steps:\n  - name: call\n    http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{ value.name }}'\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_folders_new_files_run_once_across_two_nodes_and_a_restart() {
    let root = std::env::temp_dir().join(format!("openduct-watch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("in")).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    std::fs::write(root.join("in/a.csv"), "id\n1\n").unwrap();
    let (broker, shared) = broker().await;
    let workflow = Arc::new(Workflow::parse(WATCH).unwrap());
    assert!(matches!(&workflow.trigger, Trigger::Storage(_)));
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    let (one, two) = (Node::new(Some(&root), None), Node::new(Some(&root), None));
    let both = || {
        let mut all = one.ran();
        all.extend(two.ran());
        all.sort();
        all
    };
    let (stop_one, first) = runner(&workflow, &shared, &one);
    let (stop_two, second) = runner(&workflow, &shared, &two);

    // The first poll notes what is there and runs nothing; each time is queued by both nodes.
    first_poll(&shared, &topics, 10_000).await;
    assert!(both().is_empty(), "{:?}", both());

    std::fs::write(root.join("in/b.csv"), "id\n2\n").unwrap();
    std::fs::write(root.join("in/notes.txt"), "not a csv").unwrap();
    for slot in [100_000, 100_000, 101_000] {
        tick(&shared, &topics, slot).await;
    }
    until("the new file to run", || both() == ["b.csv"]).await;

    // A restart: the members go, another comes; a time queued late again, then a new one.
    stop_one.send(true).unwrap();
    stop_two.send(true).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let three = Node::new(Some(&root), None);
    let (stop_three, third) = runner(&workflow, &shared, &three);
    std::fs::write(root.join("in/c.csv"), "id\n3\n").unwrap();
    tick(&shared, &topics, 100_000).await;
    tick(&shared, &topics, 200_000).await;
    until("the next new file, after the restart", || {
        three.ran() == ["c.csv"]
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(both(), ["b.csv"], "nothing ran twice");
    assert_eq!(three.ran(), ["c.csv"]);
    stop_three.send(true).unwrap();
    third.await.unwrap().unwrap();
    broker.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

const LISTEN: &str = "name: orders\ntrigger:\n  redis:\n    credential: cache\n    channel: 'orders.*'\n\
    steps:\n  - name: call\n    http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{ value.id }}'\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channels_messages_run_once_listened_to_by_one_node() {
    let (port, state) = storage_support::redis("");
    let url = format!("redis://127.0.0.1:{port}");
    let (broker, shared) = broker().await;
    let workflow = Arc::new(Workflow::parse(LISTEN).unwrap());
    let Trigger::Storage(w) = &workflow.trigger else {
        panic!("not a storage trigger");
    };
    let channel = w.channel().unwrap().clone();
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let ran = Node::new(None, Some(url.clone()));
    let (stop_runner, runner_task) = runner(&workflow, &shared, &ran);
    let listen = |node: &Arc<Node>| {
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(openduct_flow::storage::listen(
            workflow.id.clone(),
            channel.clone(),
            Arc::clone(&shared),
            Arc::clone(node) as Arc<dyn Effects>,
            stopped,
        ));
        (stop, task)
    };
    let subscribed = || state.lock().unwrap().subscriptions;
    let (a, b) = (
        Node::new(None, Some(url.clone())),
        Node::new(None, Some(url.clone())),
    );
    let (stop_a, task_a) = listen(&a);
    let (stop_b, task_b) = listen(&b);
    until("a node to subscribe", || subscribed() >= 1).await;
    // The group settles (a node that joins shares it out again): then the subscriptions stay as they are.
    let mut settled = subscribed();
    loop {
        tokio::time::sleep(Duration::from_secs(4)).await;
        if subscribed() == settled {
            break;
        }
        settled = subscribed();
    }
    storage_support::publish(port, "orders.new", r#"{"id": 7}"#);
    storage_support::publish(port, "other", r#"{"id": 8}"#);
    until("the message to run", || ran.ran() == ["7"]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(ran.ran(), ["7"], "one node of two listens: the message runs once");

    // Both nodes go; another comes and takes the channel up.
    stop_a.send(true).unwrap();
    stop_b.send(true).unwrap();
    task_a.await.unwrap();
    task_b.await.unwrap();
    let c = Node::new(None, Some(url));
    let (stop_c, task_c) = listen(&c);
    until("the next node to subscribe", || subscribed() > settled).await;
    // The stand-in keeps every subscription it was given; only the live one takes the message.
    let mut given = 0;
    for _ in 0..50 {
        given = storage_support::publish(port, "orders.paid", r#"{"id": 9}"#);
        if ran.ran().contains(&"9".to_owned()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(given >= 1);
    until("the message after the takeover to run", || {
        ran.ran().contains(&"9".to_owned())
    })
    .await;
    stop_c.send(true).unwrap();
    task_c.await.unwrap();
    stop_runner.send(true).unwrap();
    runner_task.await.unwrap().unwrap();
    broker.shutdown().await;
}
