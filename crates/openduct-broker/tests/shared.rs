// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One node's shared connections: many readers waiting at once in one request - some of the same partition from
//! different offsets - each answered with the records from its own offset on, as they arrive; a reader of a topic that
//! is gone told alone, the others answered.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{ClientSecurity, Fetched, Outgoing, Shared, records};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

fn message(text: &str) -> Outgoing {
    Outgoing {
        key: None,
        value: Some(Bytes::from(text.as_bytes().to_vec())),
        headers: Vec::new(),
    }
}

/// The offsets of the records in `answers` from `from` on.
fn offsets(answers: &[Fetched], from: i64) -> Vec<i64> {
    let mut out = Vec::new();
    for answer in answers {
        let mut batches = answer.batches.clone();
        while !batches.is_empty() {
            let set =
                RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
                    .unwrap();
            out.extend(set.records.iter().map(|r| r.offset).filter(|o| *o >= from));
        }
    }
    out
}

#[tokio::test]
async fn readers_wait_together_and_each_gets_its_own_records() {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let address = broker.local_addr().to_string();
    let shared = Arc::new(Shared::new(
        &address,
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let mut client = shared.lend().await.unwrap();
    client.create_topic("orders", 1, None).await.unwrap();
    client.create_topic("gone", 1, None).await.unwrap();
    for n in 0..3 {
        client
            .produce("orders", 0, &[message(&format!("o-{n}"))], 0)
            .await
            .unwrap();
    }
    drop(client);

    // Fifty readers of one partition waiting at the end, one behind them, one of another topic.
    let waiting: Vec<_> = (0..50)
        .map(|_| {
            let shared = Arc::clone(&shared);
            tokio::spawn(async move { shared.fetch("orders", &[(0, 3)]).await })
        })
        .collect();
    let behind = shared.fetch("orders", &[(0, 1)]).await.unwrap();
    assert_eq!(offsets(&behind, 1), vec![1, 2]);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        waiting.iter().all(|w| !w.is_finished()),
        "a reader at the end was answered with nothing new"
    );

    // A record arrives: every one of them is answered with it, and only it.
    shared
        .lend()
        .await
        .unwrap()
        .produce("orders", 0, &[message("o-3")], 0)
        .await
        .unwrap();
    for w in waiting {
        let got = tokio::time::timeout(Duration::from_secs(5), w)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(offsets(&got, 3), vec![3]);
    }

    // A topic deleted under a reader: that reader is told; one waiting alongside it still gets its records.
    shared.lend().await.unwrap().delete_topic("gone").await.unwrap();
    let lost = {
        let shared = Arc::clone(&shared);
        tokio::spawn(async move { shared.fetch("gone", &[(0, 0)]).await })
    };
    let alongside = {
        let shared = Arc::clone(&shared);
        tokio::spawn(async move { shared.fetch("orders", &[(0, 4)]).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    shared
        .lend()
        .await
        .unwrap()
        .produce("orders", 0, &[message("o-4")], 0)
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(15), alongside)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(offsets(&got, 4), vec![4]);
    let told = tokio::time::timeout(Duration::from_secs(15), lost)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(told.iter().all(|f| f.error_code != 0), "{told:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_at_once_go_together_and_each_is_told_its_own_offset() {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let mut client = shared.lend().await.unwrap();
    for t in ["runs-a", "runs-b", "runs-c"] {
        client.create_topic(t, 1, None).await.unwrap();
    }
    drop(client);

    // Two hundred writers at once: most to one partition, the rest to two others; some with two messages.
    let writing: Vec<_> = (0..200)
        .map(|n| {
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let topic = match n % 5 {
                    0 => "runs-b",
                    1 => "runs-c",
                    _ => "runs-a",
                };
                let count = if n % 7 == 0 { 2 } else { 1 };
                let messages: Vec<Outgoing> = (0..count).map(|i| message(&format!("{n}-{i}"))).collect();
                let first = shared.produce(topic, 0, messages).await.unwrap();
                (topic, n, count, first)
            })
        })
        .collect();
    let mut written = Vec::new();
    for w in writing {
        written.push(w.await.unwrap());
    }

    // Each writer's messages are where it was told, in its order, and no two writers were given the same place.
    for topic in ["runs-a", "runs-b", "runs-c"] {
        let mine: Vec<_> = written.iter().filter(|(t, ..)| *t == topic).collect();
        let total: i64 = mine.iter().map(|(_, _, c, _)| *c).sum();
        let mut client = shared.lend().await.unwrap();
        let fetched = client.fetch(topic, &[(0, 0)], 0, 16 << 20).await.unwrap();
        let mut values: Vec<(i64, String)> = Vec::new();
        for answer in &fetched {
            let mut batches = answer.batches.clone();
            while !batches.is_empty() {
                let set = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                )
                .unwrap();
                values.extend(set.records.iter().map(|r| {
                    (
                        r.offset,
                        String::from_utf8_lossy(r.value.as_ref().unwrap()).into_owned(),
                    )
                }));
            }
        }
        assert_eq!(i64::try_from(values.len()).unwrap(), total, "{topic}");
        for (_, n, count, first) in mine {
            for i in 0..*count {
                let at = values.iter().find(|(o, _)| *o == first + i).unwrap();
                assert_eq!(at.1, format!("{n}-{i}"), "{topic} at {}", first + i);
            }
        }
    }
}
