// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The feed trigger on a broker in memory, against a stand-in feed on this machine: its new items run once each -
//! with two members reading the workflow (two nodes), each time queued by both, and across a restart - the first
//! poll only noting what the feed holds, and an unchanged feed answering 304 with nothing.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod web_support;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_broker::client::{ClientSecurity, Outgoing, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::web::{Net, Reach};
use openduct_flow::{Credential, Effects, HttpRequest, Trigger, Workflow};
use openduct_storage::MemoryStore;
use serde_json::json;
use tokio::sync::watch;
use web_support::{Answer, Site};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A node's effects: the web, and each run's call kept (the last part of its address).
struct Node {
    net: Net,
    calls: Mutex<Vec<String>>,
}

impl Node {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            net: Net::new(true),
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
        Err(format!("no credential {name} here"))
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path} here")) })
    }

    fn web(&self) -> Reach<'_> {
        Reach::Live(&self.net)
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

/// Ticks from `slot` on, a second apart, until a poll has kept what it saw; the slot after the last one ticked.
async fn first_poll(shared: &Shared, topics: &Topics, mut slot: i64) -> i64 {
    for _ in 0..30 {
        tick(shared, topics, slot).await;
        tick(shared, topics, slot).await;
        slot += 1_000;
        tokio::time::sleep(Duration::from_secs(1)).await;
        if polls_kept(shared, topics).await > 0 {
            return slot;
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

/// The items a test adds to a feed: each one's id and title.
type Items = Arc<Mutex<Vec<(String, String)>>>;

/// A journal's feed whose newest items a test adds, with an ETag that changes as they do: asked with the one it
/// has, it answers 304.
fn journal() -> (Site, Items, Arc<Mutex<usize>>) {
    let items: Items = Arc::default();
    let unchanged: Arc<Mutex<usize>> = Arc::default();
    let (held, counted) = (Arc::clone(&items), Arc::clone(&unchanged));
    let site = Site::start(move |asked| {
        let items = held.lock().unwrap().clone();
        let etag = format!("\"v{}\"", items.len());
        if asked
            .headers
            .iter()
            .any(|(k, v)| k == "if-none-match" && *v == etag)
        {
            *counted.lock().unwrap() += 1;
            return Answer::of(304, "application/rss+xml", b"");
        }
        let newest: Vec<(&str, &str)> = items
            .iter()
            .rev()
            .map(|(id, t)| (id.as_str(), t.as_str()))
            .collect();
        let mut answer = Answer::of(
            200,
            "application/rss+xml",
            web_support::rss(&asked.host, &newest).as_bytes(),
        );
        answer.headers.push(("ETag".to_owned(), etag));
        answer
    });
    (site, items, unchanged)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feeds_new_items_run_once_across_two_nodes_and_a_restart() {
    let (site, items, unchanged) = journal();
    let yaml = format!(
        "name: journal\nruns_at_once: 1\ntrigger:\n  feed:\n    url: '{}'\n    every: 1m\nsteps:\n  - name: call\n    \
         http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{{{ value.id }}}}'\n",
        site.url("/feed.xml")
    );
    let (broker, shared) = broker().await;
    let workflow = Arc::new(Workflow::parse(&yaml).unwrap());
    let Trigger::Feed(t) = &workflow.trigger else {
        panic!("not a feed trigger");
    };
    assert_eq!(t.every(), Duration::from_secs(60));
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    let (one, two) = (Node::new(), Node::new());
    let both = || {
        let mut all = one.ran();
        all.extend(two.ran());
        all.sort();
        all
    };
    let (stop_one, first) = runner(&workflow, &shared, &one);
    let (stop_two, second) = runner(&workflow, &shared, &two);

    // The first poll notes what the feed holds and runs nothing; each time is queued by both nodes.
    first_poll(&shared, &topics, 10_000).await;
    assert!(both().is_empty(), "{:?}", both());

    items
        .lock()
        .unwrap()
        .push(("new-1".to_owned(), "A new post".to_owned()));
    for slot in [100_000, 100_000, 101_000] {
        tick(&shared, &topics, slot).await;
    }
    until("the new item to run", || both() == ["new-1"]).await;
    // Once more, with nothing new: the feed is asked with its ETag and answers 304.
    tick(&shared, &topics, 102_000).await;
    until("an unchanged feed to answer 304", || {
        *unchanged.lock().unwrap() >= 1
    })
    .await;

    // A restart: the members go, another comes; a time queued late again, then a new one.
    stop_one.send(true).unwrap();
    stop_two.send(true).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let three = Node::new();
    let (stop_three, third) = runner(&workflow, &shared, &three);
    items
        .lock()
        .unwrap()
        .push(("new-2".to_owned(), "Another post".to_owned()));
    items
        .lock()
        .unwrap()
        .push(("new-3".to_owned(), "A third post".to_owned()));
    tick(&shared, &topics, 100_000).await;
    tick(&shared, &topics, 200_000).await;
    until("the next new items, after the restart", || {
        three.ran() == ["new-2", "new-3"]
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(both(), ["new-1"], "nothing ran twice");
    assert_eq!(three.ran(), ["new-2", "new-3"]);
    // The runs went oldest first.
    assert_eq!(*three.calls.lock().unwrap(), ["new-2", "new-3"]);
    stop_three.send(true).unwrap();
    third.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[test]
fn a_feed_trigger_is_checked_when_the_workflow_is_read() {
    for (settings, says) in [
        ("{ every: 15m }", "feed.url"),
        ("{ url: 'ftp://x.example/f' }", "only http and https"),
        ("{ url: 'https://x.example/f', every: 10s }", "at least 1m"),
        ("{ url: 'https://x.example/f', max_items: 0 }", "1 to 500"),
        (
            "{ url: 'https://x.example/f', when: new }",
            "not a setting of feed",
        ),
    ] {
        let e = Workflow::parse(&format!("name: W\ntrigger:\n  feed: {settings}\nsteps: []\n")).unwrap_err();
        assert!(e.contains(says), "{settings}: {e}");
    }
    let w =
        Workflow::parse("name: W\ntrigger:\n  feed: { url: 'https://x.example/f' }\nsteps: []\n").unwrap();
    let Trigger::Feed(t) = &w.trigger else { panic!() };
    assert_eq!(t.every(), Duration::from_secs(900));
    assert_eq!(t.url(), "https://x.example/f");
}
