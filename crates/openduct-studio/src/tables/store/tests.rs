// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The store's records, as every node applies them; and two stores on one broker - two nodes of a cluster - changing
//! one row, and one workflow's state, at the same time: every change counted, on both.

use std::sync::Arc;

use super::*;
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

fn number(n: i64) -> Value {
    Value::from_json(&serde_json::json!(n))
}

fn whole(v: Option<&Value>) -> i64 {
    v.and_then(|v| v.to_json().parse().ok()).unwrap_or(0)
}

fn take(by: &str, at: i64) -> Option<Json> {
    serde_json::to_value(TurnRecord {
        op: "take".to_owned(),
        by: by.to_owned(),
        at,
        until: at + 10_000,
    })
    .ok()
}

fn free(by: &str) -> Option<Json> {
    serde_json::to_value(TurnRecord {
        op: "free".to_owned(),
        by: by.to_owned(),
        at: 0,
        until: 0,
    })
    .ok()
}

#[test]
fn a_turn_goes_to_the_first_who_asks_while_it_is_free_and_comes_back_only_from_its_holder() {
    let mut inner = Inner::default();
    apply(&mut inner, "l/table/x", take("a", 1_000));
    apply(&mut inner, "l/table/x", take("b", 2_000));
    assert_eq!(inner.turns["table/x"].0, "a", "b asked while a held it");
    apply(&mut inner, "l/table/x", free("b"));
    assert_eq!(inner.turns["table/x"].0, "a", "only its holder gives it back");
    apply(&mut inner, "l/table/x", free("a"));
    assert!(!inner.turns.contains_key("table/x"));
    apply(&mut inner, "l/table/x", take("b", 3_000));
    assert_eq!(inner.turns["table/x"].0, "b");
    // A holder that ran past its time loses it to the next who asks.
    apply(&mut inner, "l/table/x", take("c", 3_000 + 10_001));
    assert_eq!(inner.turns["table/x"].0, "c");
}

fn def(id: &str, workspace: &str, name: &str, columns: &[(&str, &str)]) -> TableDef {
    TableDef {
        schema: 1,
        id: id.to_owned(),
        workspace: workspace.to_owned(),
        name: name.to_owned(),
        columns: columns
            .iter()
            .map(|(n, k)| ColumnDef {
                name: (*n).to_owned(),
                kind: (*k).to_owned(),
            })
            .collect(),
        created: 0,
        updated: 0,
        by: String::new(),
    }
}

#[test]
fn rows_read_before_their_table_take_its_types_once_it_comes() {
    let mut inner = Inner::default();
    let row = RowRecord {
        id: 4,
        values: serde_json::from_str(r#"{"when": "2026-11-27", "n": "7"}"#).unwrap(),
        created: 1,
        updated: 1,
    };
    apply(&mut inner, &row_key("T", 4), serde_json::to_value(&row).ok());
    let events = def("T", "w", "events", &[("when", "datetime"), ("n", "number")]);
    apply(&mut inner, &def_key("T"), serde_json::to_value(&events).ok());
    let table = &inner.tables["T"];
    assert_eq!(table.name, "events");
    assert!(matches!(table.rows[&4].values["when"], Value::Time(_)));
    assert!(matches!(table.rows[&4].values["n"], Value::Number(_)));
    assert_eq!(table.next_id, 5, "the next row comes after the last one read");
    assert!(inner.bytes["T"] > 0);
    apply(&mut inner, &row_key("T", 4), None);
    assert!(inner.tables["T"].rows.is_empty());
    assert_eq!(inner.bytes["T"], 0);
    apply(&mut inner, &def_key("T"), None);
    assert!(inner.tables.is_empty() && inner.defs.is_empty());
}

/// A broker in memory, and two stores on it: two nodes of a cluster, which share their broker's topics.
async fn two_nodes() -> (Broker, Arc<Store>, Arc<Store>) {
    let broker = Broker::start(
        BrokerConfig::new("127.0.0.1:0".parse().unwrap()),
        Arc::new(MemoryStore::new()),
    )
    .await
    .unwrap();
    let addr = broker.local_addr().to_string();
    let a = Arc::new(Store::open(&addr).await.unwrap());
    let b = Arc::new(Store::open(&addr).await.unwrap());
    (broker, a, b)
}

/// The table `hits` of workspace `w`, made on `store`, with one row: `{name: "home", count: 0}`.
async fn hits(store: &Store) {
    let hits = def("H", "w", "hits", &[("name", "text"), ("count", "number")]);
    let row = Row {
        id: 1,
        values: Arc::new(
            [
                ("name".to_owned(), Value::text("home")),
                ("count".to_owned(), number(0)),
            ]
            .into_iter()
            .collect(),
        ),
        created: 0,
        updated: 0,
    };
    let changes = Changes {
        put: vec![row],
        next_id: 2,
        ..Changes::default()
    };
    store
        .in_turns(&["tables/w".to_owned()], |_| {
            let mut writes = vec![def_write(&hits, true)];
            writes.extend(row_writes("H", &TableData::default(), &changes));
            Ok((writes, ()))
        })
        .await
        .unwrap();
}

/// A run's change of `hits`: its one row's count, one more than it is.
fn bump<'a>() -> Plan<'a> {
    Box::new(|data: &TableData| {
        let row = data.rows.get(&1).ok_or("no row")?;
        let count = whole(row.values.get("count"));
        let mut values = row.values.as_ref().clone();
        values.insert("count".to_owned(), number(count + 1));
        Ok((
            Changes {
                put: vec![Row {
                    values: Arc::new(values),
                    ..row.clone()
                }],
                next_id: data.next_id,
                ..Changes::default()
            },
            Value::Null,
        ))
    })
}

fn count_of(data: &TableData) -> String {
    data.rows[&1].values["count"].to_json()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_updating_one_row_at_once_lose_no_update() {
    let (_broker, a, b) = two_nodes().await;
    hits(&a).await;
    let mut runs = Vec::new();
    for n in 0..8 {
        let store = Arc::clone(if n % 2 == 0 { &a } else { &b });
        runs.push(tokio::spawn(async move {
            for _ in 0..5 {
                store.change_table("w", "hits", bump()).await.unwrap();
            }
        }));
    }
    for run in runs {
        run.await.unwrap();
    }
    assert_eq!(count_of(&a.table("w", "hits").await.unwrap()), "40");
    assert_eq!(
        count_of(&b.table("w", "hits").await.unwrap()),
        "40",
        "the other node sees the same"
    );
    // A node started later reads the same table from the topic.
    let c = Store::open(&a.broker).await.unwrap();
    let later = c.table("w", "hits").await.unwrap();
    assert_eq!(count_of(&later), "40");
    assert_eq!(later.next_id, 2);
    assert!(matches!(later.rows[&1].values["count"], Value::Number(_)));
    // Every workspace has its own tables.
    assert!(
        c.table("other", "hits")
            .await
            .unwrap_err()
            .contains("there is no table hits")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_changing_one_workflow_s_state_at_once_lose_no_change() {
    let (_broker, a, b) = two_nodes().await;
    let mut runs = Vec::new();
    for n in 0..6 {
        let store = Arc::clone(if n % 2 == 0 { &a } else { &b });
        runs.push(tokio::spawn(async move {
            for _ in 0..5 {
                let plan: StatePlan<'_> = Box::new(|current: &Record| {
                    let seen = whole(current.get("seen"));
                    let mut next = current.clone();
                    next.insert("seen".to_owned(), number(seen + 1));
                    Ok((Some(next), Value::Null))
                });
                store.change_state("w", "flow-1", plan).await.unwrap();
            }
        }));
    }
    for run in runs {
        run.await.unwrap();
    }
    assert_eq!(a.state("w", "flow-1").await.unwrap()["seen"].to_json(), "30");
    assert_eq!(b.state("w", "flow-1").await.unwrap()["seen"].to_json(), "30");
    let listed = b.states("w").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, "flow-1");
    // Cleared, it is gone on every node.
    a.in_turns(&["state/w/flow-1".to_owned()], |_| {
        Ok((vec![state_cleared("w", "flow-1")], ()))
    })
    .await
    .unwrap();
    assert!(b.state("w", "flow-1").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_turn_its_holder_never_gave_back_runs_out_and_a_failed_plan_writes_nothing() {
    let (_broker, a, b) = two_nodes().await;
    hits(&a).await;
    // A node that stopped holding the table's turn: it runs out after its time (here, half a second).
    let at = now_ms();
    let stale = TurnRecord {
        op: "take".to_owned(),
        by: "a node that stopped".to_owned(),
        at,
        until: at + 500,
    };
    a.write(vec![(turn_key("table/H"), serde_json::to_value(&stale).ok())])
        .await
        .unwrap();
    let started = Instant::now();
    b.change_table("w", "hits", bump()).await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "it waited for the turn to run out"
    );
    assert_eq!(count_of(&a.table("w", "hits").await.unwrap()), "1");
    // A plan that fails (a limit, a wrong value) writes nothing and gives the turn back.
    let failing: Plan<'_> = Box::new(|_| Err("not this time".to_owned()));
    assert_eq!(
        b.change_table("w", "hits", failing).await.unwrap_err(),
        "not this time"
    );
    let started = Instant::now();
    a.change_table("w", "hits", bump()).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the turn was given back"
    );
    assert_eq!(count_of(&b.table("w", "hits").await.unwrap()), "2");
    // A change that breaks a table's limits is refused before anything is written.
    let wrong: Plan<'_> = Box::new(|data: &TableData| {
        let row = data.rows[&1].clone();
        let mut values = row.values.as_ref().clone();
        values.insert("count".to_owned(), Value::text("many"));
        Ok((
            Changes {
                put: vec![Row {
                    values: Arc::new(values),
                    ..row
                }],
                next_id: data.next_id,
                ..Changes::default()
            },
            Value::Null,
        ))
    });
    assert!(
        a.change_table("w", "hits", wrong)
            .await
            .unwrap_err()
            .contains("is not a number")
    );
    assert_eq!(count_of(&b.table("w", "hits").await.unwrap()), "2");
}
