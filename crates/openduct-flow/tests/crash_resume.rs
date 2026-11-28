// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A runner killed after its runs were kept and before their place was committed: whoever takes the workflow up does
//! not run them again (runner/resume.rs). On a broker in memory.

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

use openduct_broker::client::{ClientSecurity, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::runner::{prepare, queue, serve};
use openduct_flow::{Credential, Effects, HttpRequest, Workflow};
use openduct_storage::MemoryStore;
use serde_json::json;
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A node's effects: each run's call kept (the last part of its address).
#[derive(Default)]
struct Node {
    calls: Mutex<Vec<String>>,
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
}

const ORDERS: &str = "name: orders\ntrigger:\n  webhook: {}\nsteps:\n  - name: call\n    http:\n      method: POST\n\
                      \x20     url: 'http://127.0.0.1:9/{{ value.id }}'\n";

async fn until(what: &str, within: Duration, done: impl Fn() -> bool) {
    tokio::time::timeout(within, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_kept_before_a_runner_died_are_not_run_again() {
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
    let send = |from: usize, to: usize| {
        let (shared, id) = (Arc::clone(&shared), workflow.id.clone());
        async move {
            let mut client = shared.lend().await.unwrap();
            for n in from..to {
                queue(
                    &mut client,
                    &id,
                    &json!({ "id": format!("o{n}") }),
                    &json!({ "trigger": "webhook" }),
                )
                .await
                .unwrap();
            }
        }
    };

    // The first runner runs forty orders, and dies at once: the place of none of them is committed yet (a commit
    // is made once a second, and these runs take a few milliseconds).
    let one = Arc::new(Node::default());
    let (_stop_one, stopped) = watch::channel(false);
    let first = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&one) as Arc<dyn Effects>,
        stopped,
    ));
    send(0, 40).await;
    until("the first runner's forty runs", Duration::from_secs(20), || {
        one.calls.lock().unwrap().len() == 40
    })
    .await;
    // Their runs are in the history a moment after their calls.
    tokio::time::sleep(Duration::from_millis(300)).await;
    first.abort();
    let _ = first.await;

    // Ten more arrive while nobody runs; then another runner takes the workflow up.
    send(40, 50).await;
    let two = Arc::new(Node::default());
    let (stop_two, stopped) = watch::channel(false);
    let second = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&two) as Arc<dyn Effects>,
        stopped,
    ));
    // It waits for the dead member's session to end before the partitions are its own.
    until("the second runner's ten runs", Duration::from_secs(90), || {
        two.calls.lock().unwrap().len() >= 10
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut ran = two.calls.lock().unwrap().clone();
    ran.sort();
    let mut expected: Vec<String> = (40..50).map(|n| format!("o{n}")).collect();
    expected.sort();
    assert_eq!(
        ran, expected,
        "only the new ones: the forty already run are passed by"
    );
    stop_two.send(true).unwrap();
    second.await.unwrap().unwrap();
    broker.shutdown().await;
}
