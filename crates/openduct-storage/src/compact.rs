// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Log compaction, as every store does it: of the records with a key, only the newest of each key stays.
//!
//! A pass takes a run of batches (a store's closed segments, oldest first) and works in two steps, like Kafka's
//! log cleaner:
//!
//! 1. [`Latest::of`] reads every batch and notes, for each key, the offset of its newest record.
//! 2. [`Cleaner::push`] is given each batch again and answers with what to keep of it: the batch as it was, the
//!    batch written again without the records a newer one replaced (records.rs), or nothing but its offsets.
//!
//! What never changes: **offsets**. A record that stays keeps its offset, and the log has no holes - where every
//! record of a batch went, an empty batch covers its offsets (Kafka's own clients read past one). Neighbouring
//! empty batches are joined into one, so a compacted topic of small batches really does shrink; the last batch of
//! each idempotent producer is kept with its own header instead, so a restart still knows the producer's last
//! sequence number.
//!
//! **Tombstones.** A record with a key and no value deletes its key: it replaces the older records like any
//! newer one, and is itself removed later - `delete.retention.ms` after the pass that first cleaned past it
//! ([`Passes`]), so a consumer reading from the start has that long to see the delete.
//!
//! Compressed batches are read like any other, and what is left of one is compressed again as its producer
//! chose (records.rs, codec.rs).
//!
//! Transactions ([`Fates`]): the records of an aborted transaction are removed, whatever their keys, and replace
//! nothing (a consumer of committed data never saw them); those of a committed one are records like any other;
//! those of a transaction not yet decided stay as they are and replace nothing.
//!
//! What stays as it is, so compaction keeps more than it must and never less: a record without a key, a control
//! batch, and a batch whose records do not read or do not decompress (it replaces nothing either).

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::batch::{BatchHeader, HEADER_LEN, checksum, parse_stored_header};
use crate::records::{Retained, open};

/// The most bytes of keys one pass holds in memory; a pass that would need more cleans only as far as it read.
pub(crate) const KEY_BUDGET: usize = 256 * 1024 * 1024;

/// What became of the transactions a partition's batches belong to, as the partition's index tells it.
#[derive(Debug, Default)]
pub(crate) struct Fates {
    /// Every transaction before this offset is decided (the partition's last stable offset).
    stable: i64,
    /// By producer: the offsets of the first record and of the abort marker of each aborted transaction.
    aborted: HashMap<i64, Vec<(i64, i64)>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fate {
    /// In no transaction, or in one that was committed.
    Stands,
    Aborted,
    Undecided,
}

impl Fates {
    pub(crate) fn new(stable: i64, aborted: impl IntoIterator<Item = crate::AbortedTransaction>) -> Self {
        let mut by_producer: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
        for a in aborted {
            by_producer
                .entry(a.producer_id)
                .or_default()
                .push((a.first_offset, a.last_offset));
        }
        Self {
            stable,
            aborted: by_producer,
        }
    }

    pub(crate) fn of(&self, header: &BatchHeader) -> Fate {
        if !header.is_transactional() {
            return Fate::Stands;
        }
        if header.base_offset >= self.stable {
            return Fate::Undecided;
        }
        let aborted = self.aborted.get(&header.producer_id).is_some_and(|ranges| {
            ranges
                .iter()
                .any(|(first, last)| (*first..=*last).contains(&header.base_offset))
        });
        if aborted { Fate::Aborted } else { Fate::Stands }
    }
}

/// For each key, the offset of its newest record; and for each idempotent producer, where its last batch starts.
#[derive(Debug, Default)]
pub(crate) struct Latest {
    keys: HashMap<Vec<u8>, i64>,
    producers: HashMap<i64, i64>,
    bytes: usize,
    fates: Fates,
}

impl Latest {
    /// A pass over a partition whose transactions ended this way. (`Latest::default()` knows of none decided:
    /// every transaction's records stay.)
    pub(crate) fn of(fates: Fates) -> Self {
        Self {
            fates,
            ..Self::default()
        }
    }

    /// Reads one more batch. `false` once the key budget is spent: this batch was not read, and the pass cleans
    /// only the batches before it.
    pub(crate) fn read(&mut self, batch: &Bytes, budget: usize) -> bool {
        let opened = match open(batch) {
            // The records of a transaction that was aborted, or may yet be, replace nothing.
            Ok(opened) if self.fates.of(&opened.header) == Fate::Stands => opened,
            _ => {
                // Unreadable, or such a transaction's: its producer is still noted from the header.
                if let Ok(header) = parse_stored_header(batch) {
                    self.note_producer(&header);
                }
                return true;
            }
        };
        for r in &opened.records {
            let Some(key) = opened.key(r) else {
                continue;
            };
            let offset = opened.header.base_offset + i64::from(r.offset_delta);
            match self.keys.get_mut(key) {
                Some(newest) => *newest = (*newest).max(offset),
                None => {
                    if self.bytes.saturating_add(key.len()) > budget {
                        return false;
                    }
                    self.bytes += key.len() + 48;
                    self.keys.insert(key.to_vec(), offset);
                }
            }
        }
        self.note_producer(&opened.header);
        true
    }

    fn note_producer(&mut self, header: &BatchHeader) {
        if header.is_idempotent() {
            let last = self
                .producers
                .entry(header.producer_id)
                .or_insert(header.base_offset);
            *last = (*last).max(header.base_offset);
        }
    }
}

/// When each pass ran and how far it cleaned: a tombstone may go `delete.retention.ms` after the first pass that
/// cleaned past it. Kept with the partition, so the wait survives a restart.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Passes {
    /// (the offset the pass cleaned up to, when it ran), oldest first.
    done: Vec<(i64, i64)>,
}

impl Passes {
    /// The most passes remembered; forgetting an old one only makes a tombstone wait for the next.
    const KEPT: usize = 32;

    /// Whether a tombstone at `offset` has been cleaned past for at least `retention_ms`.
    pub(crate) fn tombstone_may_go(&self, offset: i64, now_ms: i64, retention_ms: i64) -> bool {
        self.done
            .iter()
            .any(|(upto, at)| *upto > offset && now_ms.saturating_sub(*at) >= retention_ms)
    }

    /// A pass that cleaned every offset before `upto`, at `now_ms`.
    pub(crate) fn ran(&mut self, upto: i64, now_ms: i64) {
        // One that reached no further than an earlier pass adds nothing.
        if self.done.iter().any(|(u, _)| *u >= upto) {
            return;
        }
        self.done.push((upto, now_ms));
        if self.done.len() > Self::KEPT {
            self.done.remove(0);
        }
    }

    /// How far the last pass cleaned.
    pub(crate) fn cleaned_upto(&self) -> i64 {
        self.done.iter().map(|(upto, _)| *upto).max().unwrap_or(0)
    }

    /// Whether a tombstone became removable between `since_ms` and `now_ms`: a reason to pass again over a log
    /// with nothing new in it.
    pub(crate) fn matured(&self, since_ms: i64, now_ms: i64, retention_ms: i64) -> bool {
        self.done.iter().any(|(_, at)| {
            let ripe = at.saturating_add(retention_ms);
            // At or after the last pass: a wait of nothing ends the moment the pass that started it does.
            ripe <= now_ms && ripe >= since_ms
        })
    }
}

/// A partition's compaction so far: its passes, and when it last ran.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Cleaning {
    pub(crate) passes: Passes,
    pub(crate) last_run_ms: i64,
}

impl Cleaning {
    /// Whether a pass up to `upto` has anything to do: offsets not cleaned yet, or a tombstone whose wait ended
    /// since the last pass.
    pub(crate) fn due(&self, upto: i64, now_ms: i64, delete_retention_ms: i64) -> bool {
        upto > self.passes.cleaned_upto()
            || self.passes.matured(self.last_run_ms, now_ms, delete_retention_ms)
    }

    pub(crate) fn ran(&mut self, upto: i64, now_ms: i64) {
        self.passes.ran(upto, now_ms);
        self.last_run_ms = now_ms;
    }
}

/// A run of neighbouring batches with no record left, to be written as one empty batch.
#[derive(Debug, Clone, Copy)]
struct Gap {
    base: i64,
    last: i64,
    epoch: i32,
    max_timestamp: i64,
}

/// Step 2 of a pass: batches in, what is kept of them out.
#[derive(Debug)]
pub(crate) struct Cleaner<'a> {
    latest: &'a Latest,
    passes: &'a Passes,
    now_ms: i64,
    delete_retention_ms: i64,
    gap: Option<Gap>,
    /// The batches to store in place of those pushed, in order.
    pub(crate) out: Vec<Bytes>,
    /// Records removed.
    pub(crate) removed: usize,
}

impl<'a> Cleaner<'a> {
    pub(crate) fn new(latest: &'a Latest, passes: &'a Passes, now_ms: i64, delete_retention_ms: i64) -> Self {
        Self {
            latest,
            passes,
            now_ms,
            delete_retention_ms,
            gap: None,
            out: Vec::new(),
            removed: 0,
        }
    }

    /// The next batch of the run.
    pub(crate) fn push(&mut self, batch: &Bytes) {
        let Ok(opened) = open(batch) else {
            return self.keep(batch.clone());
        };
        let header = opened.header;
        // A transaction's marker stays as it is.
        if header.is_control() {
            return self.keep(batch.clone());
        }
        let all = opened.records.len();
        match self.latest.fates.of(&header) {
            Fate::Stands => {}
            // It may yet be aborted (its records are then skipped by their offsets): as it is.
            Fate::Undecided => return self.keep(batch.clone()),
            // No consumer of committed data ever saw these.
            Fate::Aborted => {
                self.removed += all;
                return self.emptied(batch, &header);
            }
        }
        let mut kept = 0usize;
        let stays = |delta: i32, key: Option<&[u8]>, tombstone: bool| {
            let Some(newest) = key.and_then(|k| self.latest.keys.get(k)) else {
                return true;
            };
            let offset = header.base_offset + i64::from(delta);
            if offset < *newest {
                return false;
            }
            !(tombstone
                && self
                    .passes
                    .tombstone_may_go(offset, self.now_ms, self.delete_retention_ms))
        };
        let left = opened.retain(batch, |r, key| {
            let stay = stays(r.offset_delta, key, r.tombstone);
            kept += usize::from(stay);
            stay
        });
        match left {
            Ok(Retained::All) if all > 0 => self.keep(batch.clone()),
            Ok(Retained::Some(left)) => {
                self.removed += all - kept;
                self.keep(left);
            }
            // No record left (now, or since an earlier pass).
            Ok(Retained::None | Retained::All) => {
                self.removed += all;
                self.emptied(batch, &header);
            }
            Err(_) => self.keep(batch.clone()),
        }
    }

    fn keep(&mut self, batch: Bytes) {
        self.close_gap();
        self.out.push(batch);
    }

    /// A batch with nothing left: its own empty batch when it is its producer's last (the producer's sequence
    /// numbers live in its header), otherwise part of a run joined into one.
    fn emptied(&mut self, batch: &Bytes, header: &BatchHeader) {
        let last = header.base_offset + i64::from(header.last_offset_delta);
        let producers_last = header.is_idempotent()
            && self.latest.producers.get(&header.producer_id) == Some(&header.base_offset);
        if producers_last {
            self.close_gap();
            if let Some(own) = emptied_in_place(batch) {
                self.out.push(own);
            } else {
                self.out.push(batch.clone());
            }
            return;
        }
        match &mut self.gap {
            Some(gap)
                if gap.epoch == header.partition_leader_epoch
                    && gap.last + 1 == header.base_offset
                    && last - gap.base < i64::from(i32::MAX) =>
            {
                gap.last = last;
                gap.max_timestamp = gap.max_timestamp.max(header.max_timestamp);
            }
            _ => {
                self.close_gap();
                self.gap = Some(Gap {
                    base: header.base_offset,
                    last,
                    epoch: header.partition_leader_epoch,
                    max_timestamp: header.max_timestamp,
                });
            }
        }
    }

    fn close_gap(&mut self) {
        if let Some(gap) = self.gap.take() {
            self.out.push(empty_batch(&gap));
        }
    }

    /// The run is over (a segment's end): what is kept of it.
    pub(crate) fn finish(mut self) -> (Vec<Bytes>, usize) {
        self.close_gap();
        (self.out, self.removed)
    }
}

/// The batch's own header with no records: its offsets and its producer's sequence numbers stay.
fn emptied_in_place(batch: &Bytes) -> Option<Bytes> {
    let mut out = BytesMut::from(batch.get(..HEADER_LEN)?);
    let length = i32::try_from(HEADER_LEN - 12).ok()?;
    out.get_mut(8..12)?.copy_from_slice(&length.to_be_bytes());
    out.get_mut(57..61)?.copy_from_slice(&0i32.to_be_bytes());
    let crc = checksum(&out);
    out.get_mut(17..21)?.copy_from_slice(&crc.to_be_bytes());
    Some(out.freeze())
}

/// An empty batch covering a run of offsets, from no producer.
fn empty_batch(gap: &Gap) -> Bytes {
    let mut out = BytesMut::with_capacity(HEADER_LEN);
    out.put_i64(gap.base);
    out.put_i32(i32::try_from(HEADER_LEN - 12).unwrap_or(49));
    out.put_i32(gap.epoch);
    out.put_i8(2);
    out.put_u32(0);
    out.put_i16(0);
    out.put_i32(i32::try_from(gap.last - gap.base).unwrap_or(i32::MAX));
    out.put_i64(gap.max_timestamp);
    out.put_i64(gap.max_timestamp);
    out.put_i64(-1);
    out.put_i16(-1);
    out.put_i32(-1);
    out.put_i32(0);
    let crc = checksum(&out);
    if let Some(at) = out.get_mut(17..21) {
        at.copy_from_slice(&crc.to_be_bytes());
    }
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::stored_form;
    use crate::records::tests::keyed;

    type Rec<'a> = (Option<&'a str>, Option<&'a str>);

    /// Batches stored one after another from offset 0, each with these records.
    fn log(batches: &[&[Rec<'_>]]) -> Vec<Bytes> {
        let mut next = 0i64;
        batches
            .iter()
            .map(|records| {
                let b = stored_form(&keyed(records, 1_000 + next, 0), next, 0);
                next += records.len() as i64;
                b
            })
            .collect()
    }

    fn pass(batches: &[Bytes], passes: &Passes, now_ms: i64) -> (Vec<Bytes>, usize) {
        let mut latest = Latest::default();
        for b in batches {
            assert!(latest.read(b, KEY_BUDGET));
        }
        let mut cleaner = Cleaner::new(&latest, passes, now_ms, 1_000);
        for b in batches {
            cleaner.push(b);
        }
        cleaner.finish()
    }

    /// What a consumer reading from the start would see: (offset, key, value present).
    fn seen(batches: &[Bytes]) -> Vec<(i64, String, bool)> {
        let mut out = Vec::new();
        let mut next = batches
            .first()
            .map_or(0, |b| parse_stored_header(b).unwrap().base_offset);
        for b in batches {
            let opened = open(b).unwrap();
            let header = opened.header;
            assert_eq!(header.base_offset, next, "no hole in the log");
            next = header.base_offset + i64::from(header.last_offset_delta) + 1;
            for r in &opened.records {
                let key = opened
                    .key(r)
                    .map(|k| String::from_utf8(k.to_vec()).unwrap())
                    .unwrap_or_default();
                out.push((header.base_offset + i64::from(r.offset_delta), key, !r.tombstone));
            }
        }
        out
    }

    #[test]
    fn only_the_newest_record_of_each_key_stays_at_the_offset_it_had() {
        let batches = log(&[
            &[(Some("a"), Some("1")), (Some("b"), Some("1"))],
            &[(Some("a"), Some("2"))],
            &[
                (None, Some("no key")),
                (Some("b"), Some("2")),
                (Some("c"), Some("1")),
            ],
            &[(Some("a"), Some("3"))],
        ]);
        let (out, removed) = pass(&batches, &Passes::default(), 0);
        assert_eq!(removed, 3);
        assert_eq!(
            seen(&out),
            vec![
                (3, String::new(), true),
                (4, "b".into(), true),
                (5, "c".into(), true),
                (6, "a".into(), true)
            ]
        );
        // The first two batches lost everything: one empty batch covers offsets 0 to 2.
        assert_eq!(out.len(), 3);
        let first = parse_stored_header(&out[0]).unwrap();
        assert_eq!(
            (first.base_offset, first.last_offset_delta, first.record_count),
            (0, 2, 0)
        );
        // A second pass over the result changes nothing.
        let (again, removed) = pass(&out, &Passes::default(), 0);
        assert_eq!((again, removed), (out, 0));
    }

    #[test]
    fn a_tombstone_replaces_its_key_and_goes_only_after_the_wait() {
        let batches = log(&[
            &[(Some("a"), Some("1")), (Some("b"), Some("1"))],
            &[(Some("a"), None)],
        ]);
        let mut passes = Passes::default();
        let (out, _) = pass(&batches, &passes, 5_000);
        assert_eq!(seen(&out), vec![(1, "b".into(), true), (2, "a".into(), false)]);
        passes.ran(3, 5_000);
        // Not yet: a consumer has delete.retention.ms (1 s here) to read it.
        assert_eq!(pass(&out, &passes, 5_999).0, out);
        let (later, removed) = pass(&out, &passes, 6_000);
        assert_eq!(removed, 1);
        assert_eq!(seen(&later), vec![(1, "b".into(), true)]);
        // A tombstone written after that pass waits for a pass of its own.
        assert!(!passes.tombstone_may_go(3, 99_000, 1_000));
        assert!(passes.matured(5_999, 6_000, 1_000) && !passes.matured(6_001, 7_000, 1_000));
        assert!(
            passes.matured(5_000, 5_000, 0),
            "no wait: due on the very next pass"
        );
    }

    #[test]
    fn the_last_batch_of_an_idempotent_producer_keeps_its_header() {
        let from = |records: &[Rec<'_>], pid: i64, seq: i32, base: i64| {
            let mut b = keyed(records, 1, 0).to_vec();
            b[43..51].copy_from_slice(&pid.to_be_bytes());
            b[51..53].copy_from_slice(&0i16.to_be_bytes());
            b[53..57].copy_from_slice(&seq.to_be_bytes());
            let crc = checksum(&b);
            b[17..21].copy_from_slice(&crc.to_be_bytes());
            stored_form(&Bytes::from(b), base, 0)
        };
        let batches = vec![
            from(&[(Some("a"), Some("1"))], 7, 0, 0),
            from(&[(Some("a"), Some("2"))], 7, 1, 1),
            from(&[(Some("a"), Some("3"))], 9, 0, 2),
        ];
        let (out, removed) = pass(&batches, &Passes::default(), 0);
        assert_eq!(removed, 2);
        assert_eq!(seen(&out), vec![(2, "a".into(), true)]);
        // Producer 7's first batch is a plain gap; its last keeps producer and sequence for a restart.
        let headers: Vec<BatchHeader> = out.iter().map(|b| parse_stored_header(b).unwrap()).collect();
        assert_eq!(headers.len(), 3);
        assert_eq!((headers[0].producer_id, headers[0].record_count), (-1, 0));
        assert_eq!(
            (
                headers[1].producer_id,
                headers[1].base_sequence,
                headers[1].record_count
            ),
            (7, 1, 0)
        );
    }

    #[test]
    fn what_cannot_be_read_is_kept_whole_and_replaces_nothing() {
        let mut batches = log(&[&[(Some("a"), Some("1"))], &[(Some("a"), Some("2"))]]);
        // The second batch says it is gzip: its records are not read.
        let mut gz = batches[1].to_vec();
        gz[21..23].copy_from_slice(&1i16.to_be_bytes());
        let crc = checksum(&gz);
        gz[17..21].copy_from_slice(&crc.to_be_bytes());
        batches[1] = Bytes::from(gz);
        let (out, removed) = pass(&batches, &Passes::default(), 0);
        assert_eq!((out, removed), (batches, 0));
    }

    #[test]
    fn compressed_batches_are_compacted_like_any_other_and_stay_compressed() {
        use crate::records::tests::compressed;
        for codec in [1i16, 2, 3, 4] {
            // A log of mixed batches: compressed ones, and one that is not.
            let mut batches = log(&[
                &[(Some("a"), Some("1")), (Some("b"), Some("1"))],
                &[(Some("a"), Some("2")), (Some("c"), Some("1"))],
                &[(Some("b"), Some("2"))],
            ]);
            for i in [0, 1] {
                let base = parse_stored_header(&batches[i]).unwrap().base_offset;
                let plain = crate::batch::with_base_offset(&batches[i], 0);
                batches[i] = stored_form(&compressed(&plain, codec), base, 0);
            }
            let (out, removed) = pass(&batches, &Passes::default(), 0);
            assert_eq!(removed, 2, "codec {codec}: the first a and b went");
            assert_eq!(
                seen(&out),
                vec![
                    (2, "a".into(), true),
                    (3, "c".into(), true),
                    (4, "b".into(), true)
                ],
                "codec {codec}"
            );
            // The first batch lost everything; the second lost nothing and is the producer's own bytes still.
            assert_eq!(parse_stored_header(&out[0]).unwrap().record_count, 0);
            assert_eq!(out[1], batches[1]);
        }
        // One compressed batch that loses some of its records is written again in its codec.
        let mut batches = log(&[
            &[(Some("a"), Some("1")), (Some("b"), Some("1"))],
            &[(Some("a"), Some("2"))],
        ]);
        batches[0] = stored_form(&compressed(&batches[0], 4), 0, 0);
        let (out, removed) = pass(&batches, &Passes::default(), 0);
        assert_eq!(removed, 1);
        assert_eq!(
            parse_stored_header(&out[0]).unwrap().attributes & 0x07,
            4,
            "zstd still"
        );
        assert_eq!(seen(&out), vec![(1, "b".into(), true), (2, "a".into(), true)]);
    }

    #[test]
    fn a_pass_reads_no_more_keys_than_its_budget() {
        let batches = log(&[
            &[(Some("first-key"), Some("1"))],
            &[(Some("second-key"), Some("1"))],
        ]);
        let mut latest = Latest::default();
        assert!(latest.read(&batches[0], 12));
        assert!(!latest.read(&batches[1], 12), "the pass stops before this batch");
    }
}
