// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The storage trigger on the places reached over a vendor's API, on a broker in memory: a folder's new files run
//! once each - with two members reading the workflow (two nodes), each time queued by both, and across a restart -
//! read from the vendor's list of changes (Dropbox's cursor, Graph's delta, Drive's changes) or from the folder's
//! listing (Azure); a cursor the vendor no longer takes met by listing the folder again; a folder moved into the one
//! watched bringing its files. Against the stand-ins of `places_support`: no real account is reached.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod places_support;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_broker::client::{ClientSecurity, Outgoing, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::{Credential, Effects, HttpRequest, Workflow};
use openduct_storage::MemoryStore;
use places_support::{Kept, Rerouted, Stand, credential, effects};
use serde_json::json;
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A node's effects: the vendors' traffic to the stand-ins, and each run's call kept (the last part of its address).
struct Node {
    vendors: Rerouted,
    calls: Mutex<Vec<String>>,
}

impl Node {
    fn new(stand: &Arc<Stand>, secrets: &Arc<Kept>) -> Arc<Self> {
        Arc::new(Self {
            vendors: effects(stand, secrets, true),
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
        if let Some(name) = request.url.strip_prefix("http://127.0.0.1:9/") {
            self.calls.lock().unwrap().push(name.to_owned());
            return Box::pin(async {
                Ok(Value::from_json(
                    &json!({ "status": 200, "headers": {}, "body": {} }),
                ))
            });
        }
        self.vendors.http(request)
    }

    fn http_bytes<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<openduct_flow::files::RawAnswer, String>> + Send + 'a>> {
        if request.url.starts_with("http://127.0.0.1:9/") {
            return Box::pin(async move {
                let answer = self.http(request).await?;
                Ok(openduct_flow::files::RawAnswer { answer, body: None })
            });
        }
        self.vendors.http_bytes(request)
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no writes here".to_owned()) })
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        self.vendors.credential(name)
    }

    fn registration(&self, app: &str) -> Option<Credential> {
        self.vendors.registration(app)
    }

    fn keep_credential<'a>(
        &'a self,
        name: &'a str,
        fields: std::collections::BTreeMap<String, String>,
    ) -> Pending<'a> {
        self.vendors.keep_credential(name, fields)
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path} here")) })
    }

    fn storage(&self) -> openduct_flow::storage::Reach<'_> {
        self.vendors.storage()
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

/// How many looks have kept what they saw, in the schedule's partition.
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

/// Ticks from `slot` on, a second apart, until a look has kept what it saw.
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
    panic!("no look kept what it saw");
}

async fn until(what: &str, done: impl Fn() -> bool, ran: impl Fn() -> Vec<String>) {
    tokio::time::timeout(Duration::from_secs(40), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}: ran {:?}", ran()));
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

fn watching(place: &str) -> String {
    format!(
        "name: inbox\ntrigger:\n  storage:\n    {place}\n    folder: in\n    pattern: '*.csv'\n    every: 10s\n\
         steps:\n  - name: call\n    http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{{{ value.name }}}}'\n"
    )
}

/// The trigger of `workflow` across two nodes and a restart: `add` puts a file in the folder watched; after the
/// restart, `later` does what the place's own test asks (a cursor reset, a folder moved in) and names the files it
/// should run.
async fn runs_once(
    workflow: &str,
    stand: Arc<Stand>,
    secrets: Arc<Kept>,
    add: impl Fn(&Stand, &str),
    later: impl Fn(&Stand) -> Vec<String>,
) {
    add(&stand, "a.csv");
    let (broker, shared) = broker().await;
    let workflow = Arc::new(Workflow::parse(workflow).unwrap_or_else(|e| panic!("{e}")));
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    let (one, two) = (Node::new(&stand, &secrets), Node::new(&stand, &secrets));
    let both = || {
        let mut all = one.ran();
        all.extend(two.ran());
        all.sort();
        all
    };
    let (stop_one, first) = runner(&workflow, &shared, &one);
    let (stop_two, second) = runner(&workflow, &shared, &two);
    // The first look notes what is there and runs nothing; each time is queued by both nodes.
    first_poll(&shared, &topics, 10_000).await;
    assert!(both().is_empty(), "a trigger starts from now: {:?}", both());
    add(&stand, "b.csv");
    add(&stand, "notes.txt");
    for s in [100_000, 100_000, 101_000] {
        tick(&shared, &topics, s).await;
    }
    until("the new file to run", || both() == ["b.csv"], both).await;

    // A restart: the members go, another comes; a time queued late again, then new ones.
    stop_one.send(true).unwrap();
    stop_two.send(true).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let three = Node::new(&stand, &secrets);
    let (stop_three, third) = runner(&workflow, &shared, &three);
    add(&stand, "c.csv");
    tick(&shared, &topics, 100_000).await;
    tick(&shared, &topics, 200_000).await;
    until(
        "the next new file, after the restart",
        || three.ran() == ["c.csv"],
        || three.ran(),
    )
    .await;
    let mut expected = vec!["c.csv".to_owned()];
    expected.extend(later(&stand));
    expected.sort();
    tick(&shared, &topics, 300_000).await;
    until(
        "what the place's own test adds",
        || three.ran() == expected,
        || three.ran(),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(both(), ["b.csv"], "nothing ran twice");
    assert_eq!(three.ran(), expected);
    stop_three.send(true).unwrap();
    third.await.unwrap().unwrap();
    broker.shutdown().await;
}

fn token(kind: &str, token: &str) -> Credential {
    credential(
        kind,
        &[
            ("access_token", token),
            ("expires_at", "9999999999"),
            ("refresh_token", "r"),
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropbox_s_new_files_run_once_from_its_cursor_and_a_reset_cursor_lists_again() {
    let stand = Stand::start();
    let secrets = Kept::with(vec![("drop", token("dropbox", places_support::dropbox::TOKEN))]);
    runs_once(
        &watching("credential: drop"),
        stand,
        secrets,
        |s, name| {
            s.dropbox.lock().unwrap().put(&format!("/in/{name}"), b"x");
        },
        |s| {
            let mut d = s.dropbox.lock().unwrap();
            d.reset_next = true;
            d.put("/in/d.csv", b"x");
            vec!["d.csv".to_owned()]
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn onedrive_s_new_files_run_once_from_graph_s_delta_and_a_resync_lists_again() {
    let stand = Stand::start();
    let inbox = stand
        .graph
        .lock()
        .unwrap()
        .drive
        .add("ROOT", "in", true, Vec::new(), "");
    let secrets = Kept::with(vec![(
        "work",
        token("microsoft_365", places_support::graph::TOKEN),
    )]);
    let folder = inbox.clone();
    runs_once(
        &watching("credential: work"),
        stand,
        secrets,
        move |s, name| {
            s.graph
                .lock()
                .unwrap()
                .drive
                .add(&folder, name, false, b"x".to_vec(), "");
        },
        move |s| {
            let mut g = s.graph.lock().unwrap();
            g.resync_next = true;
            g.drive.add(&inbox, "d.csv", false, b"x".to_vec(), "");
            vec!["d.csv".to_owned()]
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn google_drive_s_new_files_run_once_from_its_changes_and_a_folder_moved_in_brings_its_files() {
    let stand = Stand::start();
    let inbox = stand
        .drive
        .lock()
        .unwrap()
        .drive
        .add("root", "in", true, Vec::new(), "");
    let secrets = Kept::with(vec![(
        "mine",
        token("google_drive", places_support::gdrive::TOKEN),
    )]);
    let folder = inbox.clone();
    runs_once(
        &watching("credential: mine\n    recursive: true"),
        stand,
        secrets,
        move |s, name| {
            s.drive
                .lock()
                .unwrap()
                .drive
                .add(&folder, name, false, b"x".to_vec(), "text/csv");
        },
        move |s| {
            let mut d = s.drive.lock().unwrap();
            let batch = d.drive.add("root", "batch", true, Vec::new(), "");
            d.drive.add(&batch, "e.csv", false, b"x".to_vec(), "text/csv");
            d.drive.relocate(&batch, &inbox, "batch");
            vec!["e.csv".to_owned()]
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn azure_s_new_blobs_run_once_from_the_folder_s_listing() {
    let stand = Stand::start();
    let endpoint = stand.azure_endpoint();
    let secrets = Kept::with(vec![(
        "lake",
        credential(
            "azure_blob",
            &[
                ("account", places_support::azure::ACCOUNT),
                ("shared_key", places_support::azure::KEY),
                ("container", "orders"),
                ("endpoint", endpoint.as_str()),
            ],
        ),
    )]);
    runs_once(
        &watching("credential: lake"),
        stand,
        secrets,
        |s, name| {
            s.azure
                .lock()
                .unwrap()
                .blobs
                .insert(format!("in/{name}"), (b"x".to_vec(), "text/csv".to_owned(), 1));
        },
        |_| Vec::new(),
    )
    .await;
}
