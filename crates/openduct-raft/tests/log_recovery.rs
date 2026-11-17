// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The Raft log across restarts, with space prepared ahead of its data: entries come back whole, a write cut short
//! is cut off, a truncated tail never comes back, and a segment left empty does not block the next.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use openduct_raft::flush::Flusher;
use openduct_raft::log::LogStore;
use openduct_raft::types::{Command, Entry, LogId};
use openraft::EntryPayload;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::vote::leader_id_adv::LeaderId;

fn log_id(term: u64, index: u64) -> LogId {
    LogId::new(LeaderId { term, node_id: 1 }, index)
}

fn entry(term: u64, index: u64) -> Entry {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(Command::Append {
            batch: Bytes::from(vec![b'a'; 200]),
        }),
    }
}

fn open(dir: &Path, segment_bytes: u64) -> LogStore {
    LogStore::open(dir, segment_bytes, Flusher::start().unwrap()).unwrap()
}

async fn append(log: &mut LogStore, term: u64, indexes: std::ops::RangeInclusive<u64>) {
    let entries: Vec<Entry> = indexes.map(|i| entry(term, i)).collect();
    let (tx, rx) = <openduct_raft::TypeConfig as TypeConfigExt>::oneshot();
    log.append(entries, IOFlushed::signal(tx)).await.unwrap();
    rx.await.unwrap().unwrap();
}

async fn all(log: &mut LogStore) -> Vec<(u64, u64)> {
    log.try_get_log_entries(0..)
        .await
        .unwrap()
        .into_iter()
        .map(|e| (e.log_id.committed_leader_id().term, e.log_id.index))
        .collect()
}

fn segments(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut s: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "raft"))
        .collect();
    s.sort();
    s
}

/// Where the entries of a segment end: frames back to back, then zeros.
fn logical_end(path: &Path) -> u64 {
    let bytes = std::fs::read(path).unwrap();
    let mut at = 0usize;
    while at + 8 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        if len == 0 {
            break;
        }
        at += 8 + len;
    }
    at as u64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn entries_come_back_whole_and_new_ones_start_a_new_segment() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = open(dir.path(), 1 << 20);
        append(&mut log, 1, 1..=100).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let newest = segments(dir.path()).pop().unwrap();
        assert!(
            std::fs::metadata(&newest).unwrap().len() > logical_end(&newest),
            "space was prepared ahead"
        );
    }
    let mut log = open(dir.path(), 1 << 20);
    assert_eq!(all(&mut log).await, (1..=100).map(|i| (1, i)).collect::<Vec<_>>());
    let newest = segments(dir.path()).pop().unwrap();
    assert_eq!(
        std::fs::metadata(&newest).unwrap().len(),
        logical_end(&newest),
        "cut back to its entries"
    );
    append(&mut log, 1, 101..=110).await;
    assert_eq!(
        segments(dir.path()).len(),
        2,
        "entries after a restart start a new segment"
    );
    drop(log);
    let mut log = open(dir.path(), 1 << 20);
    assert_eq!(all(&mut log).await, (1..=110).map(|i| (1, i)).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_cut_short_in_prepared_space_is_cut_off() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = open(dir.path(), 1 << 20);
        append(&mut log, 1, 1..=10).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // What a crash in the middle of the next write leaves: a header and the start of a body, then zeros.
    let newest = segments(dir.path()).pop().unwrap();
    let end = logical_end(&newest);
    let mut torn = Vec::new();
    torn.extend_from_slice(&3000u32.to_be_bytes());
    torn.extend_from_slice(&0xdead_beefu32.to_be_bytes());
    torn.extend_from_slice(&[0x55; 100]);
    let f = std::fs::OpenOptions::new().write(true).open(&newest).unwrap();
    std::os::unix::fs::FileExt::write_all_at(&f, &torn, end).unwrap();
    drop(f);
    let mut log = open(dir.path(), 1 << 20);
    assert_eq!(all(&mut log).await.len(), 10);
    append(&mut log, 1, 11..=11).await;
    assert_eq!(all(&mut log).await.last(), Some(&(1, 11)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_truncated_tail_never_comes_back() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = open(dir.path(), 1 << 20);
        append(&mut log, 1, 1..=20).await;
        log.truncate_after(Some(log_id(1, 10))).await.unwrap();
        // The new leader's entries, the same size as the ones cut.
        append(&mut log, 2, 11..=12).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let mut log = open(dir.path(), 1 << 20);
    let mut expected: Vec<(u64, u64)> = (1..=10).map(|i| (1, i)).collect();
    expected.extend([(2, 11), (2, 12)]);
    assert_eq!(all(&mut log).await, expected, "entries 13-20 of term 1 came back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_cut_back_to_nothing_does_not_block_the_next() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = open(dir.path(), 600);
    append(&mut log, 1, 1..=6).await;
    // Truncate to just before the newest segment's first entry: it is left empty.
    let newest = segments(dir.path()).pop().unwrap();
    let first: u64 = newest.file_stem().unwrap().to_str().unwrap().parse().unwrap();
    assert!(first > 1, "the entries span several segments");
    log.truncate_after(Some(log_id(1, first - 1))).await.unwrap();
    append(&mut log, 2, first..=first + 1).await;
    drop(log);
    let mut log = open(dir.path(), 600);
    let got = all(&mut log).await;
    assert_eq!(got.last(), Some(&(2, first + 1)));
    assert_eq!(got.len() as u64, first + 1);
}
