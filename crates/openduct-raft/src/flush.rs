// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Flushing for every Raft log a broker keeps - the controller's and each partition's - on one thread.
//!
//! An append writes its frames and hands their files here. The thread takes everything waiting at that moment, from
//! every log, flushes each file once (several files at a time: a cloud disk serves flushes in parallel), and then
//! tells each log, in the order its appends came, which of them are on the device. A log whose flush failed is
//! stopped: the device can no longer be trusted with what it was given.

use std::fs::File;
use std::io;
use std::sync::{Arc, mpsc};

use openraft::storage::IOFlushed;
use parking_lot::Mutex;

use crate::types::TypeConfig;

/// How many files are flushed at once.
const PARALLEL: usize = 8;

pub(crate) struct Flush {
    pub(crate) files: Vec<Arc<File>>,
    pub(crate) done: IOFlushed<TypeConfig>,
    /// The log's own: set when its flush failed.
    pub(crate) failed: Arc<Mutex<Option<String>>>,
}

/// The broker's flusher, shared by its logs.
#[derive(Debug)]
pub struct Flusher {
    tx: Mutex<mpsc::Sender<Flush>>,
}

impl Flusher {
    pub fn start() -> io::Result<Arc<Self>> {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("openduct-raft-flush".to_owned())
            .spawn(move || run(rx))?;
        Ok(Arc::new(Self { tx: Mutex::new(tx) }))
    }

    pub(crate) fn submit(&self, flush: Flush) -> io::Result<()> {
        self.tx
            .lock()
            .send(flush)
            .map_err(|_| io::Error::other("the replication flusher has stopped"))
    }
}

fn sync_all(files: &[Arc<File>]) -> Vec<Result<(), String>> {
    if files.len() <= 1 {
        return files
            .iter()
            .map(|f| f.sync_data().map_err(|e| e.to_string()))
            .collect();
    }
    let mut out = vec![Ok(()); files.len()];
    std::thread::scope(|scope| {
        let chunk = files.len().div_ceil(PARALLEL);
        let running: Vec<_> = files
            .chunks(chunk)
            .map(|part| {
                let handle = scope.spawn(move || {
                    part.iter()
                        .map(|f| f.sync_data().map_err(|e| e.to_string()))
                        .collect::<Vec<_>>()
                });
                (part.len(), handle)
            })
            .collect();
        let mut at = 0;
        for (len, handle) in running {
            // One answer per file whatever happened, so the answers stay in line with the files.
            let results = handle
                .join()
                .unwrap_or_else(|_| vec![Err("a flush panicked".to_owned()); len]);
            for r in results {
                if let Some(slot) = out.get_mut(at) {
                    *slot = r;
                }
                at += 1;
            }
        }
    });
    out
}

fn run(rx: mpsc::Receiver<Flush>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }
        let mut files: Vec<Arc<File>> = Vec::new();
        for f in batch.iter().flat_map(|b| b.files.iter()) {
            if !files.iter().any(|known| Arc::ptr_eq(known, f)) {
                files.push(Arc::clone(f));
            }
        }
        let results = sync_all(&files);
        for b in batch {
            let failure = b.files.iter().find_map(|f| {
                files
                    .iter()
                    .position(|known| Arc::ptr_eq(known, f))
                    .and_then(|i| results.get(i))
                    .and_then(|r| r.clone().err())
            });
            match failure {
                None => b.done.io_completed(Ok(())),
                Some(why) => {
                    tracing::error!(error = %why, "flushing a replication log failed; that log takes no more entries");
                    *b.failed.lock() = Some(why.clone());
                    b.done.io_completed(Err(io::Error::other(why)));
                }
            }
        }
    }
}
