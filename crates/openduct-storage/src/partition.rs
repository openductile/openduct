// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One partition's index - which offsets live where - and its idempotent-producer table. Shared by every store,
//! so the rules for offsets, duplicates and sequences are written once.

use std::collections::{HashMap, VecDeque};

use crate::batch::BatchHeader;
use crate::error::StoreError;

/// How many recent batches per producer are remembered to answer a retry with its original offset (as Kafka).
const RECENT_BATCHES: usize = 5;

/// Where a batch's bytes are kept, for the store that owns them.
#[derive(Debug, Clone)]
pub(crate) enum Location {
    Memory(bytes::Bytes),
    File { segment: i64, position: u64 },
}

#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) base_offset: i64,
    pub(crate) last_offset: i64,
    /// The leader epoch the batch was stored in.
    pub(crate) leader_epoch: i32,
    pub(crate) max_timestamp: i64,
    pub(crate) size: usize,
    pub(crate) location: Location,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Recent {
    first_sequence: i32,
    last_sequence: i32,
    base_offset: i64,
}

/// One idempotent producer as a partition knows it: its epoch and its last few batches.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Producer {
    epoch: i16,
    recent: VecDeque<Recent>,
}

impl Producer {
    pub(crate) fn admit(known: Option<&Producer>, header: &BatchHeader) -> Result<Admission, StoreError> {
        let Some(producer) = known else {
            // A producer this partition has never seen is taken at whatever sequence it is on, as Kafka does
            // (KIP-360): its state may have been forgotten (retention, a restart without it), and a client that
            // re-initialises keeps its sequences - refusing it here refused every retry for good (2026-11-15).
            return Ok(Admission::New);
        };
        if header.producer_epoch < producer.epoch {
            return Err(StoreError::ProducerFenced);
        }
        if header.producer_epoch > producer.epoch {
            // A new epoch restarts the sequence.
            return if header.base_sequence == 0 {
                Ok(Admission::New)
            } else {
                Err(StoreError::OutOfOrderSequence)
            };
        }
        if let Some(seen) = producer
            .recent
            .iter()
            .find(|r| r.first_sequence == header.base_sequence && r.last_sequence == header.last_sequence())
        {
            return Ok(Admission::Duplicate(seen.base_offset));
        }
        let expected = producer
            .recent
            .back()
            .map(|r| next_sequence(r.last_sequence))
            .unwrap_or(0);
        if header.base_sequence == expected {
            Ok(Admission::New)
        } else {
            Err(StoreError::OutOfOrderSequence)
        }
    }

    /// Record a stored batch of this producer: a new epoch starts its batches afresh.
    pub(crate) fn record(&mut self, header: &BatchHeader, base_offset: i64) {
        if header.producer_epoch != self.epoch {
            self.epoch = header.producer_epoch;
            self.recent.clear();
        }
        self.recent.push_back(Recent {
            first_sequence: header.base_sequence,
            last_sequence: header.last_sequence(),
            base_offset,
        });
        while self.recent.len() > RECENT_BATCHES {
            self.recent.pop_front();
        }
    }
}

/// What the producer table says about a new batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Append it.
    New,
    /// It is a retry of a batch already stored at this base offset: answer with that, store nothing.
    Duplicate(i64),
}

#[derive(Debug, Default)]
pub(crate) struct PartitionIndex {
    pub(crate) entries: VecDeque<Entry>,
    pub(crate) next_offset: i64,
    pub(crate) log_start: i64,
    producers: HashMap<i64, Producer>,
}

fn next_sequence(last: i32) -> i32 {
    if last == i32::MAX { 0 } else { last + 1 }
}

impl PartitionIndex {
    pub(crate) fn starting_at(offset: i64) -> Self {
        Self {
            next_offset: offset,
            log_start: offset,
            ..Self::default()
        }
    }

    /// The idempotent-producer rules (KIP-98, as Kafka applies them to a non-transactional producer):
    /// a retried batch is answered with its original offset; a stale epoch is fenced; a gap in sequence numbers
    /// is refused, so a producer's records are never stored out of order or twice.
    pub(crate) fn admit(&self, header: &BatchHeader) -> Result<Admission, StoreError> {
        if !header.is_idempotent() {
            return Ok(Admission::New);
        }
        Producer::admit(self.producers.get(&header.producer_id), header)
    }

    /// Record a stored batch: in the index, and in the producer table when it came from an idempotent producer.
    /// `header` is the stored batch's, so its leader epoch is the one the batch was stored in.
    pub(crate) fn record(&mut self, header: &BatchHeader, base_offset: i64, size: usize, location: Location) {
        let last_offset = base_offset + i64::from(header.last_offset_delta);
        self.entries.push_back(Entry {
            base_offset,
            last_offset,
            leader_epoch: header.partition_leader_epoch,
            max_timestamp: header.max_timestamp,
            size,
            location,
        });
        self.next_offset = last_offset + 1;
        if header.is_idempotent() {
            self.producers
                .entry(header.producer_id)
                .or_default()
                .record(header, base_offset);
        }
    }

    /// The position in `entries` of the first batch holding `offset` or anything after it.
    pub(crate) fn first_entry_for(&self, offset: i64) -> usize {
        self.entries.partition_point(|e| e.last_offset < offset)
    }

    /// The batches to return for a read from `offset`, up to `max_bytes` - but always at least one batch when there
    /// is one, so a consumer can make progress past a batch larger than its limit (as Kafka does, KIP-74).
    pub(crate) fn select(&self, offset: i64, max_bytes: usize) -> Result<Vec<Entry>, StoreError> {
        self.select_until(offset, max_bytes, self.next_offset)
    }

    /// As `select`, reading only batches before `end`: the high watermark readers see, behind the batches written
    /// but not yet on the device (the file store's group commit).
    pub(crate) fn select_until(
        &self,
        offset: i64,
        max_bytes: usize,
        end: i64,
    ) -> Result<Vec<Entry>, StoreError> {
        if offset < self.log_start || offset > end {
            return Err(StoreError::OffsetOutOfRange {
                requested: offset,
                log_start: self.log_start,
                high_watermark: end,
            });
        }
        let mut out = Vec::new();
        let mut total = 0usize;
        for entry in self
            .entries
            .iter()
            .skip(self.first_entry_for(offset))
            .take_while(|e| e.base_offset < end)
        {
            if !out.is_empty() && total.saturating_add(entry.size) > max_bytes {
                break;
            }
            total = total.saturating_add(entry.size);
            out.push(entry.clone());
            if total >= max_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// The first offset whose batch has a timestamp at or after `timestamp` (batch-granular: the base offset of the
    /// first batch whose largest timestamp reaches it).
    pub(crate) fn offset_for_timestamp(&self, timestamp: i64) -> Option<(i64, i64)> {
        self.offset_for_timestamp_until(timestamp, self.next_offset)
    }

    /// As `offset_for_timestamp`, among the batches before `end`.
    pub(crate) fn offset_for_timestamp_until(&self, timestamp: i64, end: i64) -> Option<(i64, i64)> {
        self.entries
            .iter()
            .take_while(|e| e.base_offset < end)
            .find(|e| e.max_timestamp >= timestamp)
            .map(|e| (e.base_offset, e.max_timestamp))
    }

    /// The offset of the batch with the largest timestamp, and that timestamp.
    pub(crate) fn max_timestamp(&self) -> Option<(i64, i64)> {
        self.max_timestamp_until(self.next_offset)
    }

    /// As `max_timestamp`, among the batches before `end`.
    pub(crate) fn max_timestamp_until(&self, end: i64) -> Option<(i64, i64)> {
        self.entries
            .iter()
            .take_while(|e| e.base_offset < end)
            .max_by_key(|e| e.max_timestamp)
            .map(|e| (e.last_offset, e.max_timestamp))
    }

    /// The leader epoch to answer beside `offset` (ListOffsets): the epoch of the batch holding it, or of the first
    /// batch after it; past the last batch before `end`, `current`. Never lower than the epoch the offset was written
    /// in, so a client checking its position with it (see [`PartitionIndex::epoch_end`]) never finds a truncation
    /// that did not happen.
    pub(crate) fn epoch_of(&self, offset: i64, current: i32, end: i64) -> i32 {
        self.entries
            .iter()
            .skip(self.first_entry_for(offset))
            .take_while(|e| e.base_offset < end)
            .map(|e| e.leader_epoch)
            .next()
            .unwrap_or(current)
    }

    /// Where `epoch` ended (Kafka's OffsetForLeaderEpoch): the first offset after the last batch stored in `epoch` or
    /// an earlier one, or `end` when that batch is the last. `None` for an epoch the partition cannot have: negative,
    /// or after `current`.
    ///
    /// Committed batches are never taken back here (a write is answered only once it is committed), so a client's
    /// position is always valid; the answer only has to be at or after it. It is: the batch the client read last
    /// was stored in `epoch` or before, so it is at or before the batch found. Batches stored before epochs were
    /// recorded carry the producer's value (usually -1); searching from the end keeps the answer at or after any
    /// position even then.
    pub(crate) fn epoch_end(&self, epoch: i32, current: i32, end: i64) -> Option<i64> {
        if epoch < 0 || epoch > current {
            return None;
        }
        let visible = self.entries.partition_point(|e| e.base_offset < end);
        let last = self
            .entries
            .range(..visible)
            .rposition(|e| e.leader_epoch <= epoch);
        Some(match last {
            Some(i) => self
                .entries
                .get(i + 1)
                .filter(|_| i + 1 < visible)
                .map_or(end, |e| e.base_offset),
            // Every batch left is from a later epoch: this one ended where they start.
            None => self
                .entries
                .front()
                .filter(|_| visible > 0)
                .map_or(end, |e| e.base_offset),
        })
    }

    /// The producer table, as a checkpoint keeps it: producers outlive the batches retention removes, and every copy
    /// of a replicated partition must decide a retry alike.
    pub(crate) fn producers(&self) -> Vec<(i64, Producer)> {
        let mut out: Vec<(i64, Producer)> = self.producers.iter().map(|(id, p)| (*id, p.clone())).collect();
        out.sort_by_key(|(id, _)| *id);
        out
    }

    pub(crate) fn set_producers(&mut self, producers: Vec<(i64, Producer)>) {
        self.producers = producers.into_iter().collect();
    }

    /// Drop index entries before `offset` (retention removed their bytes).
    pub(crate) fn truncate_before(&mut self, offset: i64) {
        while self.entries.front().is_some_and(|e| e.last_offset < offset) {
            self.entries.pop_front();
        }
        self.log_start = self.log_start.max(offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{parse_header, tests::batch};

    fn header(count: i32, pid: i64, epoch: i16, seq: i32) -> BatchHeader {
        parse_header(&batch(count, pid, epoch, seq, 0)).unwrap()
    }

    fn store(index: &mut PartitionIndex, h: &BatchHeader) -> i64 {
        let base = index.next_offset;
        index.record(h, base, 10, Location::Memory(bytes::Bytes::new()));
        base
    }

    #[test]
    fn a_retried_batch_gets_its_original_offset_and_a_gap_is_refused() {
        let mut index = PartitionIndex::default();
        let first = header(3, 9, 0, 0);
        assert_eq!(index.admit(&first).unwrap(), Admission::New);
        assert_eq!(store(&mut index, &first), 0);
        // The same batch again (the producer did not see our answer): the original offset, nothing stored.
        assert_eq!(index.admit(&first).unwrap(), Admission::Duplicate(0));
        let next = header(2, 9, 0, 3);
        assert_eq!(index.admit(&next).unwrap(), Admission::New);
        assert_eq!(store(&mut index, &next), 3);
        // Skipping sequence 5 is refused.
        assert!(matches!(
            index.admit(&header(1, 9, 0, 6)),
            Err(StoreError::OutOfOrderSequence)
        ));
    }

    #[test]
    fn a_producer_never_seen_is_taken_at_any_sequence() {
        let mut index = PartitionIndex::default();
        let late = header(2, 7, 0, 40);
        assert_eq!(index.admit(&late).unwrap(), Admission::New);
        assert_eq!(store(&mut index, &late), 0);
        // From there on its sequence must follow.
        assert_eq!(index.admit(&header(1, 7, 0, 42)).unwrap(), Admission::New);
        assert!(matches!(
            index.admit(&header(1, 7, 0, 50)),
            Err(StoreError::OutOfOrderSequence)
        ));
    }

    #[test]
    fn an_older_epoch_is_fenced_and_a_new_epoch_restarts_at_zero() {
        let mut index = PartitionIndex::default();
        store(&mut index, &header(1, 4, 2, 0));
        assert!(matches!(
            index.admit(&header(1, 4, 1, 1)),
            Err(StoreError::ProducerFenced)
        ));
        assert_eq!(index.admit(&header(1, 4, 3, 0)).unwrap(), Admission::New);
        assert!(matches!(
            index.admit(&header(1, 4, 3, 5)),
            Err(StoreError::OutOfOrderSequence)
        ));
    }

    #[test]
    fn a_read_always_returns_at_least_one_batch_and_refuses_offsets_outside_the_log() {
        let mut index = PartitionIndex::default();
        let h = header(2, -1, -1, -1);
        store(&mut index, &h);
        store(&mut index, &h);
        assert_eq!(
            index.select(0, 1).unwrap().len(),
            1,
            "one batch even over the limit"
        );
        assert_eq!(
            index.select(1, 1_000).unwrap().len(),
            2,
            "from inside the first batch"
        );
        assert!(
            index.select(4, 1_000).unwrap().is_empty(),
            "at the high watermark: nothing yet"
        );
        assert!(index.select(5, 1_000).is_err());
        index.truncate_before(2);
        assert!(index.select(1, 1_000).is_err(), "before the log start");
    }

    /// Batches of `count` records each, stored in the given epochs one after another.
    fn stored_in(epochs: &[(i32, i32)]) -> PartitionIndex {
        let mut index = PartitionIndex::default();
        for &(count, epoch) in epochs {
            let mut h = header(count, -1, -1, -1);
            h.partition_leader_epoch = epoch;
            store(&mut index, &h);
        }
        index
    }

    #[test]
    fn an_epoch_ends_where_the_next_one_starts() {
        // Offsets 0-1 in epoch 1, 2-4 in epoch 1, 5-6 in epoch 3, 7 in epoch 4; 8 is the end.
        let index = stored_in(&[(2, 1), (3, 1), (2, 3), (1, 4)]);
        let end = index.next_offset;
        assert_eq!(end, 8);
        assert_eq!(index.epoch_end(1, 4, end), Some(5));
        assert_eq!(
            index.epoch_end(2, 4, end),
            Some(5),
            "an epoch with no batches ends where the next begins"
        );
        assert_eq!(index.epoch_end(3, 4, end), Some(7));
        assert_eq!(
            index.epoch_end(4, 4, end),
            Some(8),
            "the current epoch runs to the end"
        );
        assert_eq!(index.epoch_end(4, 6, end), Some(8));
        assert_eq!(
            index.epoch_end(5, 6, end),
            Some(8),
            "a later epoch with nothing stored yet"
        );
        assert_eq!(index.epoch_end(0, 4, end), Some(0), "before every batch kept");
        assert_eq!(index.epoch_end(7, 6, end), None, "after the current epoch");
        assert_eq!(index.epoch_end(-1, 6, end), None);
        // Only what readers see counts: with the end at 5, epoch 1 runs to it.
        assert_eq!(index.epoch_end(1, 4, 5), Some(5));
        assert_eq!(index.epoch_end(3, 4, 5), Some(5));
    }

    #[test]
    fn an_empty_partition_ends_every_epoch_at_its_end() {
        let index = PartitionIndex::starting_at(40);
        assert_eq!(index.epoch_end(0, 0, 40), Some(40));
        assert_eq!(index.epoch_end(3, 5, 40), Some(40));
        assert_eq!(index.epoch_of(40, 5, 40), 5);
    }

    #[test]
    fn the_epoch_beside_an_offset_is_never_below_the_one_it_was_written_in() {
        let mut index = stored_in(&[(2, 1), (3, 2), (2, 5)]);
        let end = index.next_offset;
        assert_eq!(index.epoch_of(0, 6, end), 1);
        assert_eq!(index.epoch_of(4, 6, end), 2);
        assert_eq!(index.epoch_of(5, 6, end), 5);
        assert_eq!(
            index.epoch_of(7, 6, end),
            6,
            "past the last batch: the current epoch"
        );
        index.truncate_before(2);
        assert_eq!(
            index.epoch_of(0, 6, end),
            2,
            "before what is kept: the first kept batch's, never lower"
        );
    }

    #[test]
    fn any_position_a_reader_can_hold_is_at_or_before_its_epoch_end() {
        // Whatever the epochs (batches stored before epochs were recorded carry the producer's value), the end
        // found for the epoch of the batch a reader stopped in is never before the reader's position.
        let shapes: [&[(i32, i32)]; 4] = [
            &[(1, -1), (2, -1), (1, 3), (2, 3), (1, 7)],
            &[(2, 5), (1, -1), (3, 2), (1, 9)],
            &[(1, 0), (1, 0), (1, 0)],
            &[(3, 4), (1, 1), (2, 4), (1, 2)],
        ];
        for shape in shapes {
            let index = stored_in(shape);
            let end = index.next_offset;
            let current = 10;
            for e in &index.entries {
                for position in e.base_offset..=e.last_offset + 1 {
                    let epoch = e.leader_epoch.max(0);
                    let at = index.epoch_end(epoch, current, end).unwrap();
                    assert!(
                        at >= position.min(end),
                        "{shape:?}: epoch {epoch}, position {position}, end {at}"
                    );
                }
                for offset in e.base_offset..=e.last_offset {
                    let epoch = index.epoch_of(offset, current, end);
                    assert!(epoch >= e.leader_epoch, "{shape:?}: offset {offset}");
                    // -1 (a batch from before epochs were recorded) tells a client there is no epoch to check.
                    if epoch >= 0 {
                        assert!(
                            index.epoch_end(epoch, current, end).unwrap() > offset,
                            "{shape:?}: offset {offset}"
                        );
                    }
                }
            }
        }
    }
}
