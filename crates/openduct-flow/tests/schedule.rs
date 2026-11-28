// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A schedule's times run once, across a takeover: the two ways a time ran twice when the member reading the
//! schedule's partition was lost, each made to happen here on purpose. (studio_cluster proves the whole of it on three
//! nodes, but reaches these only when a machine is slow enough.) A schedule at set times (a cron expression), queued
//! by two nodes at once, runs each of its times once too, across a takeover.

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

use openduct_broker::client::{ClientSecurity, Outgoing, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::{Credential, Effects, HttpRequest, Trigger, Workflow};
use openduct_storage::MemoryStore;
use serde_json::json;
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

const TICK: &str = "name: tick\ntrigger:\n  schedule:\n    every: 2s\nsteps:\n  - name: call\n    http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{ trigger.slot }}'\n";
/// Every second, by a cron expression in a time zone: what each node of a cluster queues for itself.
const EVERY_SECOND: &str = "name: seconds\ntrigger:\n  schedule:\n    cron: '* * * * * *'\n    timezone: Europe/Berlin\nsteps:\n  - name: call\n    http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{ trigger.slot }}'\n";

/// Each run's time, from the one step it has; and, when asked, where the group had committed the schedule's
/// partition while that run was going.
struct Calls {
    slots: Mutex<Vec<String>>,
    watch: Option<(Arc<Shared>, String, String)>,
    committed_during: Mutex<Vec<(String, i64)>>,
}

impl Calls {
    fn new(watch: Option<(Arc<Shared>, String, String)>) -> Self {
        Self {
            slots: Mutex::new(Vec::new()),
            watch,
            committed_during: Mutex::new(Vec::new()),
        }
    }

    fn ran(&self) -> Vec<String> {
        self.slots.lock().unwrap().clone()
    }

    /// The times run, in order of time: runs go several at once, so they finish in any order.
    fn ran_sorted(&self) -> Vec<String> {
        let mut ran = self.ran();
        ran.sort();
        ran
    }
}

impl Effects for Calls {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let slot = request.url.rsplit('/').next().unwrap_or_default().to_owned();
            if let Some((shared, group, topic)) = &self.watch {
                let mut client = shared.lend().await.map_err(|e| e.to_string())?;
                let committed = client
                    .committed(group, topic, &[0])
                    .await
                    .map_err(|e| e.to_string())?;
                let at = committed.iter().find(|(p, _)| *p == 0).map_or(-1, |(_, o)| *o);
                self.committed_during.lock().unwrap().push((slot.clone(), at));
            }
            self.slots.lock().unwrap().push(slot);
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

/// A broker in memory, a client of it, and the tick workflow's topics made.
async fn ready() -> (Broker, Arc<Shared>, Arc<Workflow>, Topics) {
    ready_with(TICK).await
}

/// A broker in memory, a client of it, and the topics of the workflow `text` made.
async fn ready_with(text: &str) -> (Broker, Arc<Shared>, Arc<Workflow>, Topics) {
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
    let workflow = Arc::new(Workflow::parse(text).unwrap());
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    (broker, shared, workflow, topics)
}

/// A tick for `slot`, as a node queues it to the schedule's partition; its offset.
async fn tick(shared: &Shared, topics: &Topics, slot: i64) -> i64 {
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
    shared.produce(&topics.input, 0, vec![message]).await.unwrap()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scheduled_time_is_claimed_before_it_runs() {
    let (broker, shared, workflow, topics) = ready().await;
    let offset = tick(&shared, &topics, 1_000).await;
    let calls = Arc::new(Calls::new(Some((
        Arc::clone(&shared),
        topics.group.clone(),
        topics.input.clone(),
    ))));
    let (stop, stopped) = watch::channel(false);
    let runner = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&calls) as Arc<dyn Effects>,
        stopped,
    ));
    until("the time to run", || calls.ran() == ["1000"]).await;
    // While it ran, the group's place was already past it: a member taking the partition over now never runs it.
    let during = calls.committed_during.lock().unwrap().clone();
    assert_eq!(during.len(), 1);
    assert!(
        during[0].1 > offset,
        "committed {} while the time at offset {offset} ran",
        during[0].1
    );
    stop.send(true).unwrap();
    runner.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_taking_the_schedule_over_does_not_run_a_time_again() {
    let (broker, shared, workflow, topics) = ready().await;
    let calls = Arc::new(Calls::new(None));

    // The first member runs 10 and 12. A node queued 10 late, after 12: the record just before where the group's
    // place ends up names 10, not the 12 already run.
    tick(&shared, &topics, 10_000).await;
    tick(&shared, &topics, 12_000).await;
    tick(&shared, &topics, 10_000).await;
    let (stop, stopped) = watch::channel(false);
    let first = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&calls) as Arc<dyn Effects>,
        stopped,
    ));
    until("the first member's two times", || {
        calls.ran_sorted() == ["10000", "12000"]
    })
    .await;
    stop.send(true).unwrap();
    first.await.unwrap().unwrap();

    // Another node queues 12 late, then 14 comes. The member taking over runs 14 - and not 12 again.
    tick(&shared, &topics, 12_000).await;
    tick(&shared, &topics, 14_000).await;
    let (stop, stopped) = watch::channel(false);
    let second = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&calls) as Arc<dyn Effects>,
        stopped,
    ));
    until("the next time, after the takeover", || calls.ran().len() >= 3).await;
    // Room for a repeat to show itself, had there been one.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(calls.ran_sorted(), ["10000", "12000", "14000"]);
    stop.send(true).unwrap();
    second.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_times_queued_by_two_nodes_run_once_each_across_a_takeover() {
    let (broker, shared, workflow, _) = ready_with(EVERY_SECOND).await;
    let Trigger::Calendar(calendar) = &workflow.trigger else {
        panic!("not set times: {:?}", workflow.trigger);
    };
    // Two nodes, each queueing every time the schedule names.
    let (stop_nodes, nodes_stopped) = watch::channel(false);
    let nodes: Vec<_> = (0..2)
        .map(|_| {
            tokio::spawn(openduct_flow::calendar::schedule(
                workflow.id.clone(),
                calendar.clone(),
                Arc::clone(&shared),
                nodes_stopped.clone(),
            ))
        })
        .collect();
    let calls = Arc::new(Calls::new(None));
    let (stop, stopped) = watch::channel(false);
    let first = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&calls) as Arc<dyn Effects>,
        stopped,
    ));
    until("three times run", || calls.ran().len() >= 3).await;
    stop.send(true).unwrap();
    first.await.unwrap().unwrap();

    // The member reading the schedule is gone; another takes it over while both nodes go on queueing.
    let before = calls.ran().len();
    let (stop, stopped) = watch::channel(false);
    let second = tokio::spawn(serve(
        Arc::clone(&workflow),
        Arc::clone(&shared),
        Arc::clone(&calls) as Arc<dyn Effects>,
        stopped,
    ));
    until("three more times, after the takeover", || {
        calls.ran().len() >= before + 3
    })
    .await;
    stop_nodes.send(true).unwrap();
    for node in nodes {
        node.await.unwrap();
    }
    // Room for a repeat to show itself, had there been one.
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.send(true).unwrap();
    second.await.unwrap().unwrap();
    broker.shutdown().await;

    let ran = calls.ran_sorted();
    let mut once = ran.clone();
    once.dedup();
    assert_eq!(once, ran, "a time ran twice");
    for slot in &ran {
        let at: i64 = slot.parse().unwrap();
        assert_eq!(at % 1000, 0, "{slot} is not a whole second");
        assert_eq!(
            calendar.next_after(at - 1000),
            Some(at),
            "{slot} is not a time it names"
        );
    }
}
