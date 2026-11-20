// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Log compaction on disk (the rules are in compact.rs): a partition's closed segments, written again without the
//! records a newer one replaced.
//!
//! - **Only closed segments**, oldest first, as far as `min.compaction.lag.ms` allows; the segment being written
//!   to is never touched.
//! - **A segment at a time.** The pass first reads every such segment for its keys, holding none of them; then
//!   takes each segment in turn, writes what is kept of it to a file beside it, flushes that, and renames it over
//!   the segment. A crash leaves a segment as it was or as it now is, whole, and at worst a stray `.clean` file
//!   that the next pass overwrites.
//! - **Offsets do not move**, so a segment keeps its name and the log has no hole (compact.rs).
//! - **What has been cleaned** is kept in `cleaner.json` beside the segments: it decides when a tombstone may
//!   go, and a pass with nothing new to do costs one look at it.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use bytes::Bytes;

use super::segments::{FilePartition, Frame, read_frame};
use super::{FRAME_HEADER, sync_dir, to_json, write_atomically};
use crate::batch::parse_stored_header;
use crate::compact::{Cleaner, Cleaning, KEY_BUDGET, Latest};
use crate::error::StoreError;
use crate::partition::{Entry, Location};

pub(super) const STATE: &str = "cleaner.json";

/// A partition's compaction so far; nothing, where it never ran (or its note does not read: the next pass then
/// cleans from the start, and tombstones wait again - more is kept, never less).
pub(super) fn load(dir: &Path) -> Cleaning {
    fs::read(dir.join(STATE))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Every batch of a closed segment, in order.
fn read_segment(path: &Path) -> Result<Vec<Bytes>, StoreError> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut reader = std::io::BufReader::new(file);
    let (mut position, mut out) = (0u64, Vec::new());
    loop {
        match read_frame(&mut reader, len.saturating_sub(position)) {
            Frame::End => return Ok(out),
            Frame::Ok { batch, len } => {
                position += len;
                out.push(batch);
            }
            Frame::Damaged { why, .. } => {
                return Err(StoreError::Corrupt(format!(
                    "{} at byte {position}: {why}",
                    path.display()
                )));
            }
        }
    }
}

impl FilePartition {
    /// One pass (see the top of this file). How many records went.
    /// The closed segments a pass may clean: from `floor` on (segments before it are left as they are - those
    /// object storage holds), oldest first, as far as the lag allows.
    fn cleanable(&self, now_ms: i64, lag_ms: i64, floor: i64) -> Vec<i64> {
        let active = self.active.as_ref().map(|a| a.base);
        self.segments
            .iter()
            .skip_while(|(base, _)| **base < floor)
            .take_while(|(base, seg)| {
                Some(**base) != active && seg.max_timestamp <= now_ms.saturating_sub(lag_ms)
            })
            .map(|(base, _)| *base)
            .collect()
    }

    /// Where the segment after `base` starts: the end of the log when there is none.
    fn end_of(&self, base: i64) -> i64 {
        self.segments
            .range(base + 1..)
            .next()
            .map_or(self.index.next_offset, |(next, _)| *next)
    }

    /// Whether a pass now would have anything to do - asked before one is ordered, without reading a record.
    pub(super) fn compaction_due(
        &self,
        now_ms: i64,
        lag_ms: i64,
        delete_retention_ms: i64,
        floor: i64,
    ) -> bool {
        self.cleanable(now_ms, lag_ms, floor).last().is_some_and(|last| {
            self.index
                .cleaning
                .due(self.end_of(*last), now_ms, delete_retention_ms)
        })
    }

    pub(super) fn compact(
        &mut self,
        now_ms: i64,
        lag_ms: i64,
        delete_retention_ms: i64,
        floor: i64,
    ) -> Result<usize, StoreError> {
        let mut bases = self.cleanable(now_ms, lag_ms, floor);
        // The keys of those segments; a pass that would hold too many cleans only as far as it read.
        let mut latest = Latest::of(self.index.fates());
        let mut read = 0usize;
        'segments: for base in &bases {
            let Some(seg) = self.segments.get(base) else { break };
            for batch in read_segment(&seg.path)? {
                if !latest.read(&batch, KEY_BUDGET) {
                    break 'segments;
                }
            }
            read += 1;
        }
        bases.truncate(read);
        let Some(last) = bases.last() else {
            return Ok(0);
        };
        // Cleaned up to where the next segment starts.
        let upto = self.end_of(*last);
        if !self.index.cleaning.due(upto, now_ms, delete_retention_ms) {
            return Ok(0);
        }
        let mut removed = 0usize;
        for base in &bases {
            let Some(path) = self.segments.get(base).map(|s| s.path.clone()) else {
                continue;
            };
            let batches = read_segment(&path)?;
            let mut cleaner = Cleaner::new(&latest, &self.index.cleaning.passes, now_ms, delete_retention_ms);
            batches.iter().for_each(|b| cleaner.push(b));
            let (kept, gone) = cleaner.finish();
            if kept == batches {
                continue;
            }
            // Beside the segment, on the device, then in its place.
            let fresh = path.with_extension("clean");
            let mut entries = Vec::with_capacity(kept.len());
            let mut position = 0u64;
            {
                let mut file = BufWriter::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&fresh)?,
                );
                for batch in &kept {
                    let header = parse_stored_header(batch)?;
                    let len = u32::try_from(batch.len())
                        .map_err(|_| StoreError::Corrupt("a batch too large for a frame".to_owned()))?;
                    file.write_all(&len.to_be_bytes())?;
                    file.write_all(&crc32c::crc32c(batch).to_be_bytes())?;
                    file.write_all(batch)?;
                    entries.push(Entry::of(
                        &header,
                        batch.len(),
                        Location::File {
                            segment: *base,
                            position,
                        },
                    ));
                    position += (FRAME_HEADER + batch.len()) as u64;
                }
                file.flush()?;
                file.get_ref().sync_all()?;
            }
            fs::rename(&fresh, &path)?;
            sync_dir(&self.dir)?;
            self.readers.remove(base);
            let end = self.end_of(*base);
            self.index.replace_between(*base, end, entries);
            if let Some(seg) = self.segments.get_mut(base) {
                seg.size = position;
            }
            removed += gone;
        }
        self.index.cleaning.ran(upto, now_ms);
        write_atomically(&self.dir.join(STATE), &to_json(&self.index.cleaning)?)?;
        Ok(removed)
    }
}
