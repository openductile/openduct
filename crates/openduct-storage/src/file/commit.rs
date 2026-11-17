// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Group commit for one partition: one flush to the device for every append waiting at that moment, instead of one
//! flush each.
//!
//! An append writes its frame under the partition's lock and takes a ticket; it then waits here, without the lock,
//! until a flush covers its ticket. The first to arrive while no flush is running flushes the segment for everyone
//! written so far; the others wait for it, and whoever is left over leads the next one. A batch becomes visible to
//! readers (`durable_next`) only once it is on the device: nothing is read, or acknowledged, before it is safe.
//!
//! Unflushed writes are only ever in the active segment: a segment is flushed before the next one is started
//! ([`Commits::rolled`]). A flush that fails stops the partition: its later appends are refused (the page cache can no
//! longer be trusted to hold what was written), and a restart reads back what the device really has.

use std::collections::VecDeque;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::error::StoreError;

#[derive(Debug)]
struct State {
    /// The active segment, flushed by the leader.
    file: Option<Arc<File>>,
    /// The last ticket handed out, and the last one on the device.
    written: u64,
    synced: u64,
    flushing: bool,
    failed: Option<String>,
    /// Tickets not yet on the device, with the offset after each one's batch.
    pending: VecDeque<(u64, i64)>,
}

#[derive(Debug)]
pub(super) struct Commits {
    state: Mutex<State>,
    flushed: Condvar,
    /// The offset after the last batch on the device: what readers may read up to.
    durable_next: AtomicI64,
    /// Flushes made, for the tests and the logs.
    flushes: AtomicI64,
}

fn failed(why: &str) -> StoreError {
    StoreError::Io(std::io::Error::other(format!(
        "this partition stopped taking writes after a flush to the device failed ({why}); restart the broker"
    )))
}

impl Commits {
    pub(super) fn new(durable_next: i64, file: Option<Arc<File>>) -> Self {
        Self {
            state: Mutex::new(State {
                file,
                written: 0,
                synced: 0,
                flushing: false,
                failed: None,
                pending: VecDeque::new(),
            }),
            flushed: Condvar::new(),
            durable_next: AtomicI64::new(durable_next),
            flushes: AtomicI64::new(0),
        }
    }

    pub(super) fn durable_next(&self) -> i64 {
        self.durable_next.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn flushes(&self) -> i64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Refuses an append once a flush has failed.
    pub(super) fn check(&self) -> Result<(), StoreError> {
        match &self.state.lock().failed {
            Some(why) => Err(failed(why)),
            None => Ok(()),
        }
    }

    /// A batch ending before `next` was written (under the partition's lock); its ticket, to wait for.
    pub(super) fn wrote(&self, next: i64) -> u64 {
        let mut st = self.state.lock();
        st.written += 1;
        let ticket = st.written;
        st.pending.push_back((ticket, next));
        ticket
    }

    /// A batch written with flushing left to the operating system: visible at once.
    pub(super) fn visible(&self, next: i64) {
        self.durable_next.fetch_max(next, Ordering::AcqRel);
    }

    /// The last ticket handed out: a retried batch is answered once everything written so far is on the device.
    pub(super) fn current(&self) -> u64 {
        self.state.lock().written
    }

    /// The partition starts a new active segment: everything written to the old one is flushed first.
    pub(super) fn rolled(&self, new: Arc<File>) -> Result<(), StoreError> {
        let mut st = self.state.lock();
        if st.written > st.synced {
            if let Some(old) = st.file.clone() {
                if let Err(e) = old.sync_data() {
                    st.failed = Some(e.to_string());
                    drop(st);
                    self.flushed.notify_all();
                    return Err(StoreError::Io(e));
                }
                self.flushes.fetch_add(1, Ordering::Relaxed);
            }
            let upto = st.written;
            self.settle(&mut st, upto);
        }
        st.file = Some(new);
        drop(st);
        self.flushed.notify_all();
        Ok(())
    }

    /// Everything up to `upto` is on the device: it becomes readable.
    fn settle(&self, st: &mut State, upto: u64) {
        st.synced = st.synced.max(upto);
        let mut next = None;
        while st.pending.front().is_some_and(|(t, _)| *t <= st.synced) {
            next = st.pending.pop_front().map(|(_, n)| n);
        }
        if let Some(n) = next {
            self.durable_next.fetch_max(n, Ordering::AcqRel);
        }
    }

    /// Returns once `ticket` is on the device - flushing it, and everything written before, when no one else is.
    pub(super) fn wait(&self, ticket: u64) -> Result<(), StoreError> {
        let mut st = self.state.lock();
        loop {
            if let Some(why) = &st.failed {
                return Err(failed(why));
            }
            if st.synced >= ticket {
                return Ok(());
            }
            if st.flushing {
                self.flushed.wait(&mut st);
                continue;
            }
            // This append leads: one flush for every write so far.
            st.flushing = true;
            let upto = st.written;
            let file = st.file.clone();
            drop(st);
            let flushed = match &file {
                Some(f) => f.sync_data(),
                None => Ok(()),
            };
            st = self.state.lock();
            st.flushing = false;
            match flushed {
                Ok(()) => {
                    self.flushes.fetch_add(1, Ordering::Relaxed);
                    self.settle(&mut st, upto);
                }
                Err(e) => st.failed = Some(e.to_string()),
            }
            self.flushed.notify_all();
        }
    }
}
