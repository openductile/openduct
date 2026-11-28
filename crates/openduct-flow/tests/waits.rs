// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Runs that wait, on a broker in memory: a run stops at an approval and is kept on the log; the runner (and the
//! node's memory of what waits) is started again from nothing; the first of two answers takes the run on from where
//! it waited, its earlier steps not run again; a wait for a call times out and goes on, or fails, as set; a wait
//! until a time goes on by itself; a run waiting inside a loop goes on item by item.

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
use openduct_flow::runner::{Topics, prepare, queue, serve};
use openduct_flow::waits::{Answered, Waits, keep_sweeping};
use openduct_flow::{Credential, Effects, HttpRequest, Workflow};
use openduct_storage::MemoryStore;
use serde_json::{Value as Json, json};
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Every HTTP call a run made (its address and body), and where its waits are kept.
struct Calls {
    made: Mutex<Vec<(String, String)>>,
    waits: Arc<Waits>,
}

impl Calls {
    fn made(&self) -> Vec<(String, String)> {
        self.made.lock().unwrap().clone()
    }
    fn to(&self, end: &str) -> Vec<String> {
        self.made()
            .into_iter()
            .filter(|(url, _)| url.ends_with(end))
            .map(|(_, body)| body)
            .collect()
    }
}

impl Effects for Calls {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let body = request
                .body
                .map(|(_, b)| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            self.made.lock().unwrap().push((request.url, body));
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
    fn waits(&self) -> Option<Arc<Waits>> {
        Some(Arc::clone(&self.waits))
    }
}

struct Node {
    stop: watch::Sender<bool>,
    runner: tokio::task::JoinHandle<Result<(), String>>,
    sweeper: tokio::task::JoinHandle<()>,
}

impl Node {
    async fn stop(self) {
        self.stop.send(true).unwrap();
        self.runner.await.unwrap().unwrap();
        self.sweeper.abort();
    }
}

/// A node: its own memory of what waits (read from the log afresh), a runner of `workflow`, a sweeper.
async fn node(
    broker: &str,
    shared: &Arc<Shared>,
    workflow: &Arc<Workflow>,
    calls: &Arc<Mutex<Vec<(String, String)>>>,
) -> (Node, Arc<Waits>, Arc<Calls>) {
    let waits = Arc::new(Waits::open(broker, "http://studio.test").await.unwrap());
    let effects = Arc::new(Calls {
        made: Mutex::new(calls.lock().unwrap().clone()),
        waits: Arc::clone(&waits),
    });
    let (stop, stopped) = watch::channel(false);
    let runner = tokio::spawn(serve(
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(&effects) as Arc<dyn Effects>,
        stopped.clone(),
    ));
    let sweeper = tokio::spawn(keep_sweeping(Arc::clone(&waits), stopped));
    (
        Node {
            stop,
            runner,
            sweeper,
        },
        waits,
        effects,
    )
}

async fn ready(definition: &str) -> (Broker, String, Arc<Shared>, Arc<Workflow>) {
    let broker = Broker::start(
        BrokerConfig::new("127.0.0.1:0".parse().unwrap()),
        Arc::new(MemoryStore::new()),
    )
    .await
    .unwrap();
    let addr = broker.local_addr().to_string();
    let shared = Arc::new(Shared::new(
        &addr,
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let workflow = Arc::new(Workflow::parse(definition).unwrap());
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    (broker, addr, shared, workflow)
}

async fn start_run(shared: &Shared, workflow: &Workflow, value: Json) -> String {
    let mut client = shared.lend().await.unwrap();
    queue(&mut client, &workflow.id, &value, &json!({ "trigger": "manual" }))
        .await
        .unwrap()
}

/// The workflow's history, oldest first.
async fn history(shared: &Shared, workflow: &Workflow) -> Vec<Json> {
    let topic = Topics::of(&workflow.id).runs;
    let mut client = shared.lend().await.unwrap();
    let fetched = client.fetch(&topic, &[(0, 0)], 0, 8 * 1024 * 1024).await.unwrap();
    let mut out = Vec::new();
    for answer in fetched {
        let mut batches = answer.batches;
        while !batches.is_empty() {
            let Ok(set) = kafka_protocol::records::RecordBatchDecoder::decode_with_custom_compression(
                &mut batches,
                Some(openduct_broker::client::records::decompress),
            ) else {
                break;
            };
            for r in set.records {
                if let Some(v) = r.value {
                    out.push(serde_json::from_slice(&v).unwrap());
                }
            }
        }
    }
    out
}

async fn until<F: Future<Output = bool>>(what: &str, mut done: impl FnMut() -> F) {
    tokio::time::timeout(Duration::from_secs(40), async {
        while !done().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

const REFUND: &str = r#"
name: Refund
trigger: manual
steps:
  - name: order
    set: { id: "value.id", amount: "value.amount" }
  - name: tell
    http: { method: POST, url: "http://127.0.0.1:9/tell", json: { link: "{{ links.ok }}", id: "{{ steps.order.id }}" } }
  - name: ok
    approval: { title: "Refund {{ steps.order.amount }} on {{ steps.order.id }}?", comment: true, timeout: 2d }
  - name: after
    http: { method: POST, url: "http://127.0.0.1:9/after", json: { decision: "{{ steps.ok.decision }}", note: "{{ steps.ok.comment }}" } }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_approval_waits_on_the_log_and_goes_on_once_with_the_first_answer() {
    let (broker, addr, shared, workflow) = ready(REFUND).await;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (first, waits, effects) = node(&addr, &shared, &workflow, &calls).await;
    let run = start_run(&shared, &workflow, json!({ "id": "A-7", "amount": 40 })).await;
    until("the run to wait", || async {
        waits.waiting(&workflow.id).await.unwrap().len() == 1
    })
    .await;
    let waiting = waits.waiting(&workflow.id).await.unwrap().remove(0);
    assert_eq!(
        (waiting.run.as_str(), waiting.step.as_str(), waiting.kind.as_str()),
        (run.as_str(), "ok", "approval")
    );
    assert_eq!(waiting.title, "Refund 40 on A-7?");
    // The step before handed the address out: the address it waits at.
    let told: Json = serde_json::from_str(&effects.to("/tell")[0]).unwrap();
    assert_eq!(
        told["link"],
        format!("http://studio.test/waits/{}", waiting.token)
    );
    let kept = history(&shared, &workflow).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0]["status"], "waiting");
    assert_eq!(
        (
            kept[0]["waiting"]["step"].as_str(),
            kept[0]["waiting"]["kind"].as_str()
        ),
        (Some("ok"), Some("approval"))
    );
    // Its form, as its page shows it.
    let (_, form) = waits.open_wait(&waiting.token).await.unwrap().unwrap();
    assert!(form.unwrap().approval.unwrap().comment);

    // The node goes; another starts from nothing but the log.
    first.stop().await;
    *calls.lock().unwrap() = Vec::new();
    let (second, waits, effects) = node(&addr, &shared, &workflow, &calls).await;
    assert_eq!(
        waits.waiting(&workflow.id).await.unwrap().len(),
        1,
        "what waits is read from the log"
    );
    let yes = json!({ "approved": true, "decision": "approved", "comment": "fine" });
    let no = json!({ "approved": false, "decision": "rejected", "comment": "late" });
    assert_eq!(
        waits.answer(&waiting.token, "answer", &yes).await.unwrap(),
        Answered::Taken
    );
    assert_eq!(
        waits.answer(&waiting.token, "answer", &no).await.unwrap(),
        Answered::Already("answer".to_owned())
    );
    until("the run to go on", || async {
        history(&shared, &workflow).await.len() == 2
    })
    .await;
    let done = history(&shared, &workflow).await.remove(1);
    assert_eq!(done["status"], "done", "{done}");
    assert_eq!(done["run"], run, "one run, from its start to its end");
    let names: Vec<&str> = done["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["order", "tell", "ok", "after"]);
    assert_eq!(done["steps"][2]["result"]["approved"], true);
    assert_eq!(done["steps"][2]["result"]["timed_out"], false);
    // The steps before the wait are not run again; the one after runs once, with the first answer.
    assert!(effects.to("/tell").is_empty(), "{:?}", effects.made());
    let after: Vec<Json> = effects
        .to("/after")
        .iter()
        .map(|b| serde_json::from_str(b).unwrap())
        .collect();
    assert_eq!(after, [json!({ "decision": "approved", "note": "fine" })]);
    until("the wait to be done with", || async {
        waits.waiting(&workflow.id).await.unwrap().is_empty()
    })
    .await;
    assert_eq!(
        waits.answer(&waiting.token, "answer", &no).await.unwrap(),
        Answered::Unknown
    );
    // Room for a second run of it to show itself, had there been one.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(history(&shared, &workflow).await.len(), 2);
    assert_eq!(effects.to("/after").len(), 1);
    second.stop().await;
    broker.shutdown().await;
}

const CALLBACK: &str = r#"
name: Callback
trigger: manual
steps:
  - name: start
    http: { method: POST, url: "http://127.0.0.1:9/start", json: { callback: "{{ links.back }}" } }
  - name: back
    wait: { webhook: true, timeout: 1s, on_timeout: continue }
  - name: then
    http: { method: POST, url: "http://127.0.0.1:9/then", json: { late: "{{ steps.back.timed_out }}" } }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_for_a_call_goes_on_with_the_call_or_its_timeout_as_set() {
    let (broker, addr, shared, workflow) = ready(CALLBACK).await;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (node, waits, effects) = node(&addr, &shared, &workflow, &calls).await;
    // Answered in time: the call is the step's result.
    start_run(&shared, &workflow, json!({})).await;
    until("the first run to wait", || async {
        waits.waiting(&workflow.id).await.unwrap().len() == 1
    })
    .await;
    let token = waits.waiting(&workflow.id).await.unwrap()[0].token.clone();
    let call = json!({ "method": "POST", "body": { "job": 9 }, "query": {}, "headers": {} });
    assert_eq!(
        waits.answer(&token, "answer", &call).await.unwrap(),
        Answered::Taken
    );
    until("the first run to end", || async {
        history(&shared, &workflow)
            .await
            .iter()
            .any(|r| r["status"] == "done")
    })
    .await;
    let done = history(&shared, &workflow)
        .await
        .into_iter()
        .find(|r| r["status"] == "done")
        .unwrap();
    assert_eq!(done["steps"][1]["result"]["body"]["job"], 9);
    // Not answered: after its timeout it goes on, saying so.
    start_run(&shared, &workflow, json!({})).await;
    until("the second run to time out and go on", || async {
        history(&shared, &workflow)
            .await
            .iter()
            .filter(|r| r["status"] == "done")
            .count()
            == 2
    })
    .await;
    let late: Vec<Json> = effects
        .to("/then")
        .iter()
        .map(|b| serde_json::from_str(b).unwrap())
        .collect();
    assert_eq!(late, [json!({ "late": false }), json!({ "late": true })]);
    node.stop().await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_that_fails_on_its_timeout_fails_the_run_and_a_time_comes_by_itself() {
    let definition = r#"
name: Deadlines
trigger: manual
steps:
  - name: soon
    wait: { until: "value.at" }
  - name: answer
    form: { title: Why?, fields: [{ name: why, required: true }], timeout: 1s }
"#;
    let (broker, addr, shared, workflow) = ready(definition).await;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (node, _waits, _) = node(&addr, &shared, &workflow, &calls).await;
    // Far enough ahead to be reached before it comes (a runner takes a moment to join its group).
    let at = chrono::Utc::now() + chrono::Duration::seconds(6);
    let run = start_run(&shared, &workflow, json!({ "at": at.to_rfc3339() })).await;
    until("the run to fail at the form's timeout", || async {
        history(&shared, &workflow)
            .await
            .iter()
            .any(|r| r["status"] == "failed")
    })
    .await;
    let kept = history(&shared, &workflow).await;
    let statuses: Vec<&str> = kept.iter().map(|r| r["status"].as_str().unwrap()).collect();
    assert_eq!(statuses, ["waiting", "waiting", "failed"], "{kept:?}");
    let failed = kept.last().unwrap();
    assert_eq!(failed["run"], run);
    assert_eq!(kept[0]["waiting"]["kind"], "time");
    assert_eq!(kept[1]["waiting"]["kind"], "form");
    assert_eq!(failed["error"], "no answer came within 1s");
    assert_eq!(failed["steps"][0]["result"]["timed_out"], false, "the time came");
    assert!(failed["steps"][0]["result"]["resumed_at"].is_string());
    node.stop().await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_waiting_in_a_loop_goes_on_item_by_item() {
    let definition = r#"
name: Each
trigger: manual
steps:
  - name: each
    for_each:
      items: "value.items"
      steps:
        - name: check
          approval: { title: "Ship {{ item }}?" }
        - name: ship
          http: { method: POST, url: "http://127.0.0.1:9/ship", json: { item: "{{ item }}", ok: "{{ steps.check.approved }}" } }
"#;
    let (broker, addr, shared, workflow) = ready(definition).await;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (node, waits, effects) = node(&addr, &shared, &workflow, &calls).await;
    start_run(&shared, &workflow, json!({ "items": ["a", "b"] })).await;
    let mut tokens = Vec::new();
    for (title, approved) in [("Ship a?", true), ("Ship b?", false)] {
        until("the run to wait for an item", || async {
            waits
                .waiting(&workflow.id)
                .await
                .unwrap()
                .iter()
                .any(|w| w.title == title)
        })
        .await;
        let token = waits.waiting(&workflow.id).await.unwrap()[0].token.clone();
        tokens.push(token.clone());
        let decision = if approved { "approved" } else { "rejected" };
        let answer = json!({ "approved": approved, "decision": decision, "comment": "" });
        assert_eq!(
            waits.answer(&token, "answer", &answer).await.unwrap(),
            Answered::Taken
        );
    }
    assert_ne!(
        tokens[0], tokens[1],
        "each time the step is reached, an address of its own"
    );
    until("the run to end", || async {
        history(&shared, &workflow)
            .await
            .iter()
            .any(|r| r["status"] == "done")
    })
    .await;
    let shipped: Vec<Json> = effects
        .to("/ship")
        .iter()
        .map(|b| serde_json::from_str(b).unwrap())
        .collect();
    assert_eq!(
        shipped,
        [
            json!({ "item": "a", "ok": true }),
            json!({ "item": "b", "ok": false })
        ]
    );
    let done = history(&shared, &workflow)
        .await
        .into_iter()
        .find(|r| r["status"] == "done")
        .unwrap();
    let names: Vec<&str> = done["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["check", "ship", "check", "ship", "each"]);
    node.stop().await;
    broker.shutdown().await;
}
