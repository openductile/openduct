// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The records inside a batch, read for log compaction - the one place OpenDuct looks inside a batch.
//!
//! Everywhere else a batch travels byte for byte (batch.rs). Compaction has to know each record's key, and to
//! write a batch again without the records a newer one replaced. It does both the way Kafka's log cleaner does:
//!
//! - the batch keeps its base offset, its last offset delta, its timestamps and its producer fields, so every
//!   record left keeps the offset it always had and the batch still ends where it ended;
//! - records are copied as they were written (their offset and timestamp deltas are relative to a base that did
//!   not move); only the batch's length, record count and checksum change.
//!
//! A compressed batch is decompressed to be read, and what is left of it is compressed again with the codec its
//! producer chose (codec.rs). A batch that does not decompress, or expands past the limit, is one the caller
//! keeps whole: compaction then keeps more than it must and never less. Nothing in this file panics on any input.
//!
//! A record, from the Kafka protocol specification (varints are zigzag-encoded):
//!
//! | Field            | Type                     |
//! |------------------|--------------------------|
//! | length           | varint: bytes after it   |
//! | attributes       | i8                       |
//! | timestamp delta  | varlong                  |
//! | offset delta     | varint                   |
//! | key              | varint length (-1: none), bytes |
//! | value            | varint length (-1: none), bytes |
//! | headers          | varint count, then key and value each as varint length and bytes |

use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};

use crate::batch::{BatchError, BatchHeader, HEADER_LEN, parse_header_of};

/// The attribute bits holding a batch's compression codec.
const COMPRESSION_MASK: i16 = 0x07;
const CONTROL_FLAG: i16 = 0x20;

/// Why a batch's records could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordsError {
    /// The batch is compressed (Kafka's codec number): [`records`] does not read it, [`open`] does.
    #[error("the batch is compressed (codec {0}); it is read with open")]
    Compressed(i16),
    /// Its records could not be decompressed, or what is left of them could not be compressed.
    #[error("the batch's compression: {0}")]
    Codec(String),
    #[error("the batch's records are malformed: {0}")]
    Malformed(&'static str),
    #[error(transparent)]
    Batch(#[from] BatchError),
}

/// One record of a batch, by where its parts lie in the batch's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordView {
    /// Its offset is the batch's base offset plus this.
    pub offset_delta: i32,
    pub timestamp_delta: i64,
    /// Where its key lies, when it has one.
    pub key: Option<Range<usize>>,
    /// No value: a tombstone, which deletes its key.
    pub tombstone: bool,
    /// The whole record, its length prefix included.
    pub whole: Range<usize>,
}

/// A cursor over a batch's bytes that never reads past the end.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, RecordsError> {
        let b = *self
            .bytes
            .get(self.at)
            .ok_or(RecordsError::Malformed("it ends early"))?;
        self.at += 1;
        Ok(b)
    }

    /// A zigzag varint of up to 64 bits.
    fn varlong(&mut self) -> Result<i64, RecordsError> {
        let mut raw: u64 = 0;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            raw |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                let half =
                    i64::try_from(raw >> 1).map_err(|_| RecordsError::Malformed("a number too large"))?;
                return Ok(if raw & 1 == 0 { half } else { -half - 1 });
            }
        }
        Err(RecordsError::Malformed("a number that never ends"))
    }

    fn varint(&mut self) -> Result<i32, RecordsError> {
        i32::try_from(self.varlong()?).map_err(|_| RecordsError::Malformed("a number too large"))
    }

    /// A length-prefixed run of bytes (`-1`: absent): where it lies.
    fn run(&mut self) -> Result<Option<Range<usize>>, RecordsError> {
        let len = self.varint()?;
        if len < 0 {
            return Ok(None);
        }
        let len = usize::try_from(len).map_err(|_| RecordsError::Malformed("a length too large"))?;
        let end = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(RecordsError::Malformed("a field longer than the batch"))?;
        let range = self.at..end;
        self.at = end;
        Ok(Some(range))
    }
}

/// A batch's records as they lie in `image`: the batch's header followed by its records, uncompressed.
fn parse(header: &BatchHeader, image: &[u8]) -> Result<Vec<RecordView>, RecordsError> {
    let mut r = Reader {
        bytes: image,
        at: HEADER_LEN,
    };
    let count = usize::try_from(header.record_count).unwrap_or(0);
    // Never sized by what the batch claims: a record is at least seven bytes.
    let mut out = Vec::with_capacity(count.min(image.len() / 7 + 1));
    for _ in 0..count {
        let start = r.at;
        let length = r.varint()?;
        let length =
            usize::try_from(length).map_err(|_| RecordsError::Malformed("a record of negative length"))?;
        let end =
            r.at.checked_add(length)
                .filter(|end| *end <= image.len())
                .ok_or(RecordsError::Malformed("a record longer than the batch"))?;
        r.byte()?; // attributes: unused
        let timestamp_delta = r.varlong()?;
        let offset_delta = r.varint()?;
        let key = r.run()?;
        let tombstone = r.run()?.is_none();
        if r.at > end {
            return Err(RecordsError::Malformed("a record's fields run past its length"));
        }
        if offset_delta < 0 || offset_delta > header.last_offset_delta {
            return Err(RecordsError::Malformed("a record outside its batch's offsets"));
        }
        // Headers are not read: the record's length says where it ends.
        r.at = end;
        out.push(RecordView {
            offset_delta,
            timestamp_delta,
            key,
            tombstone,
            whole: start..end,
        });
    }
    if r.at != image.len() {
        return Err(RecordsError::Malformed("bytes after the last record"));
    }
    Ok(out)
}

/// The records of a stored batch that is not compressed, in order, by where they lie in `batch`. A control batch
/// has none to speak of and answers an empty list. A compressed batch is read with [`open`].
pub fn records(batch: &[u8]) -> Result<(BatchHeader, Vec<RecordView>), RecordsError> {
    let header = parse_header_of(batch, true)?;
    if header.attributes & CONTROL_FLAG != 0 {
        return Ok((header, Vec::new()));
    }
    let codec = header.attributes & COMPRESSION_MASK;
    if codec != 0 {
        return Err(RecordsError::Compressed(codec));
    }
    let all = parse(&header, batch)?;
    Ok((header, all))
}

/// A stored batch opened for reading: its records, compressed or not.
#[derive(Debug, Clone)]
pub struct Opened {
    pub header: BatchHeader,
    /// In order. Empty for a control batch.
    pub records: Vec<RecordView>,
    /// The batch's header and then its records uncompressed: what the records' ranges lie in.
    image: Bytes,
    /// Kafka's codec number; 0 when the batch is not compressed.
    codec: i16,
}

/// Opens a stored batch, decompressing its records when the producer compressed them (codec.rs).
pub fn open(batch: &Bytes) -> Result<Opened, RecordsError> {
    let header = parse_header_of(batch, true)?;
    let codec = header.attributes & COMPRESSION_MASK;
    if header.attributes & CONTROL_FLAG != 0 {
        return Ok(Opened {
            header,
            records: Vec::new(),
            image: batch.clone(),
            codec,
        });
    }
    let image = if codec == 0 {
        batch.clone()
    } else {
        let packed = batch.get(HEADER_LEN..).ok_or(BatchError::Truncated)?;
        let records = crate::codec::decompress(codec, packed).map_err(RecordsError::Codec)?;
        let mut image = BytesMut::with_capacity(HEADER_LEN + records.len());
        image.put_slice(batch.get(..HEADER_LEN).ok_or(BatchError::Truncated)?);
        image.put_slice(&records);
        image.freeze()
    };
    let records = parse(&header, &image)?;
    Ok(Opened {
        header,
        records,
        image,
        codec,
    })
}

impl Opened {
    /// A record's key, when it has one.
    pub fn key(&self, record: &RecordView) -> Option<&[u8]> {
        record.key.clone().and_then(|k| self.image.get(k))
    }

    /// The batch `original` (the one this was opened from) with only the records `keep` says yes to. Offsets do
    /// not move (see the top of this file), and what is left is compressed as the producer compressed it. A batch
    /// that would come out larger than a batch may be is left whole.
    pub fn retain(
        &self,
        original: &Bytes,
        mut keep: impl FnMut(&RecordView, Option<&[u8]>) -> bool,
    ) -> Result<Retained, RecordsError> {
        let kept: Vec<&RecordView> = self.records.iter().filter(|r| keep(r, self.key(r))).collect();
        if kept.len() == self.records.len() {
            return Ok(Retained::All);
        }
        if kept.is_empty() {
            return Ok(Retained::None);
        }
        let mut left = Vec::new();
        for r in &kept {
            left.extend_from_slice(self.image.get(r.whole.clone()).ok_or(BatchError::Truncated)?);
        }
        if self.codec != 0 {
            left = crate::codec::compress(self.codec, &left).map_err(RecordsError::Codec)?;
        }
        if HEADER_LEN + left.len() > crate::MAX_BATCH_BYTES {
            return Ok(Retained::All);
        }
        let mut out = BytesMut::with_capacity(HEADER_LEN + left.len());
        out.put_slice(original.get(..HEADER_LEN).ok_or(BatchError::Truncated)?);
        out.put_slice(&left);
        let length =
            i32::try_from(out.len() - 12).map_err(|_| RecordsError::Malformed("a batch too large"))?;
        let count = i32::try_from(kept.len()).map_err(|_| RecordsError::Malformed("too many records"))?;
        put(&mut out, 8, &length.to_be_bytes())?;
        put(&mut out, 57, &count.to_be_bytes())?;
        let crc = crc32c::crc32c(out.get(21..).ok_or(BatchError::Truncated)?);
        put(&mut out, 17, &crc.to_be_bytes())?;
        Ok(Retained::Some(out.freeze()))
    }
}

/// What is left of a batch once some of its records are gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retained {
    /// Every record stays: the batch as it was.
    All,
    /// Some stay: the batch written again with only those.
    Some(Bytes),
    /// None stays.
    None,
}

/// The batch with only the records `keep` says yes to (it is given each record and its key): [`Opened::retain`].
pub fn retain(
    batch: &Bytes,
    keep: impl FnMut(&RecordView, Option<&[u8]>) -> bool,
) -> Result<Retained, RecordsError> {
    open(batch)?.retain(batch, keep)
}

fn put(out: &mut BytesMut, at: usize, bytes: &[u8]) -> Result<(), RecordsError> {
    out.get_mut(at..at + bytes.len())
        .ok_or(RecordsError::Batch(BatchError::Truncated))?
        .copy_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::batch::parse_header;

    fn varint(out: &mut Vec<u8>, n: i64) {
        let mut raw = ((n << 1) ^ (n >> 63)) as u64;
        loop {
            let b = (raw & 0x7f) as u8;
            raw >>= 7;
            if raw == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }

    /// A batch of real records: each a key (or none) and a value (or none, a tombstone), offsets from 0.
    pub(crate) fn keyed(records: &[(Option<&str>, Option<&str>)], timestamp: i64, attributes: i16) -> Bytes {
        let mut body = Vec::new();
        for (i, (key, value)) in records.iter().enumerate() {
            let mut r = vec![0u8];
            varint(&mut r, 0);
            varint(&mut r, i as i64);
            for field in [key, value] {
                match field {
                    Some(text) => {
                        varint(&mut r, text.len() as i64);
                        r.extend_from_slice(text.as_bytes());
                    }
                    None => varint(&mut r, -1),
                }
            }
            varint(&mut r, 1);
            varint(&mut r, 1);
            r.push(b'h');
            varint(&mut r, 2);
            r.extend_from_slice(b"hv");
            varint(&mut body, r.len() as i64);
            body.extend_from_slice(&r);
        }
        let count = records.len() as i32;
        let mut b = Vec::new();
        b.extend_from_slice(&0i64.to_be_bytes());
        b.extend_from_slice(&((HEADER_LEN - 12 + body.len()) as i32).to_be_bytes());
        b.extend_from_slice(&0i32.to_be_bytes());
        b.push(2);
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&attributes.to_be_bytes());
        b.extend_from_slice(&(count - 1).to_be_bytes());
        b.extend_from_slice(&timestamp.to_be_bytes());
        b.extend_from_slice(&timestamp.to_be_bytes());
        b.extend_from_slice(&(-1i64).to_be_bytes());
        b.extend_from_slice(&(-1i16).to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.extend_from_slice(&count.to_be_bytes());
        b.extend_from_slice(&body);
        let crc = crc32c::crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(b)
    }

    fn keys(batch: &Bytes) -> Vec<(i32, Option<String>, bool)> {
        let (_, all) = records(batch).unwrap();
        all.iter()
            .map(|r| {
                let key = r
                    .key
                    .clone()
                    .map(|k| String::from_utf8(batch[k].to_vec()).unwrap());
                (r.offset_delta, key, r.tombstone)
            })
            .collect()
    }

    #[test]
    fn records_are_read_with_their_keys_and_tombstones() {
        let b = keyed(
            &[(Some("a"), Some("1")), (None, Some("2")), (Some("b"), None)],
            5,
            0,
        );
        assert!(parse_header(&b).is_ok(), "a batch a producer could send");
        assert_eq!(
            keys(&b),
            vec![
                (0, Some("a".into()), false),
                (1, None, false),
                (2, Some("b".into()), true)
            ]
        );
    }

    #[test]
    fn a_batch_written_again_keeps_every_offset_and_checks_out() {
        let b = keyed(
            &[
                (Some("a"), Some("1")),
                (Some("b"), Some("2")),
                (Some("a"), Some("3")),
            ],
            9,
            0,
        );
        let b = crate::batch::stored_form(&b, 40, 7);
        let Retained::Some(left) = retain(&b, |r, _| r.offset_delta != 0).unwrap() else {
            panic!("two of three stay");
        };
        assert!(left.len() < b.len());
        let header = crate::batch::parse_stored_header(&left).unwrap();
        assert_eq!(
            (header.base_offset, header.last_offset_delta, header.record_count),
            (40, 2, 2)
        );
        assert_eq!((header.partition_leader_epoch, header.max_timestamp), (7, 9));
        assert_eq!(
            keys(&left),
            vec![(1, Some("b".into()), false), (2, Some("a".into()), false)]
        );
        // A producer could not send it (a record is missing): only a stored batch may look like this.
        assert!(matches!(
            parse_header(&left),
            Err(BatchError::InvalidRecordCount { .. })
        ));
        // Written again from what is left: the same again, and the first record gone, the last still at delta 2.
        let Retained::Some(last) = retain(&left, |_, key| key == Some(b"a")).unwrap() else {
            panic!("one stays");
        };
        assert_eq!(keys(&last), vec![(2, Some("a".into()), false)]);
        assert_eq!(retain(&last, |_, _| true).unwrap(), Retained::All);
        assert_eq!(retain(&last, |_, _| false).unwrap(), Retained::None);
    }

    #[test]
    fn a_compressed_batch_is_not_read_and_a_control_batch_has_nothing_to_compact() {
        // A batch that says gzip and is not: read by neither, and so kept whole by the caller.
        let gzip = keyed(&[(Some("a"), Some("1"))], 1, 1);
        assert_eq!(records(&gzip).unwrap_err(), RecordsError::Compressed(1));
        assert!(matches!(retain(&gzip, |_, _| false), Err(RecordsError::Codec(_))));
        let control = keyed(&[(Some("a"), Some("1"))], 1, 0x20);
        assert!(records(&control).unwrap().1.is_empty());
        assert_eq!(retain(&control, |_, _| false).unwrap(), Retained::All);
    }

    #[test]
    fn no_input_makes_it_panic_or_read_past_the_end() {
        let good = keyed(&[(Some("key"), Some("value")), (Some("k2"), None)], 1, 0);
        // Every single-byte change of the records, with the checksum made right again so the parser is reached.
        for at in HEADER_LEN..good.len() {
            for change in [0x00u8, 0x7f, 0x80, 0xff] {
                let mut b = good.to_vec();
                b[at] = change;
                let crc = crc32c::crc32c(&b[21..]);
                b[17..21].copy_from_slice(&crc.to_be_bytes());
                let b = Bytes::from(b);
                if let Ok((_, all)) = records(&b) {
                    for r in &all {
                        assert!(r.whole.end <= b.len());
                        assert!(r.key.as_ref().is_none_or(|k| k.end <= b.len()));
                    }
                    let _ = retain(&b, |r, _| r.offset_delta == 0);
                }
            }
        }
        // A record count far beyond the bytes there are.
        let mut b = good.to_vec();
        b[23..27].copy_from_slice(&(i32::MAX - 1).to_be_bytes());
        b[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
        let crc = crc32c::crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(records(&b), Err(RecordsError::Malformed(_))));
    }

    /// The batch with its records compressed by `codec`, as a producer would send it.
    pub(crate) fn compressed(batch: &Bytes, codec: i16) -> Bytes {
        let mut b = batch[..HEADER_LEN].to_vec();
        b.extend_from_slice(&crate::codec::compress(codec, &batch[HEADER_LEN..]).unwrap());
        let length = (b.len() - 12) as i32;
        b[8..12].copy_from_slice(&length.to_be_bytes());
        b[21..23].copy_from_slice(&codec.to_be_bytes());
        let crc = crc32c::crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(b)
    }

    #[test]
    fn a_compressed_batch_is_read_and_written_again_in_its_own_codec() {
        let plain = keyed(
            &[(Some("a"), Some("1")), (Some("b"), Some("2")), (Some("a"), None)],
            9,
            0,
        );
        for codec in [1i16, 2, 3, 4] {
            let batch = crate::batch::stored_form(&compressed(&plain, codec), 40, 3);
            assert!(
                parse_header(&batch).is_ok(),
                "codec {codec}: a batch a producer could send"
            );
            let opened = open(&batch).unwrap();
            let seen: Vec<_> = opened
                .records
                .iter()
                .map(|r| (r.offset_delta, opened.key(r).map(<[u8]>::to_vec), r.tombstone))
                .collect();
            assert_eq!(
                seen,
                vec![
                    (0, Some(b"a".to_vec()), false),
                    (1, Some(b"b".to_vec()), false),
                    (2, Some(b"a".to_vec()), true)
                ],
                "codec {codec}"
            );
            let Retained::Some(left) = retain(&batch, |r, _| r.offset_delta != 0).unwrap() else {
                panic!("codec {codec}: two of three stay");
            };
            let header = crate::batch::parse_stored_header(&left).unwrap();
            assert_eq!(
                header.attributes & 0x07,
                codec,
                "still compressed as the producer chose"
            );
            assert_eq!(
                (header.base_offset, header.last_offset_delta, header.record_count),
                (40, 2, 2)
            );
            let again = open(&left).unwrap();
            let deltas: Vec<i32> = again.records.iter().map(|r| r.offset_delta).collect();
            assert_eq!(deltas, vec![1, 2], "codec {codec}");
            assert_eq!(again.key(&again.records[0]), Some(&b"b"[..]));
            assert_eq!(retain(&left, |_, _| true).unwrap(), Retained::All);
        }
    }
}
