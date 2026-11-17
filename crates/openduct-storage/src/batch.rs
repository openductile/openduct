// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Kafka record batches (format v2, "magic 2") as OpenDuct sees them: a header to read and check, and a body
//! passed through exactly as the producer sent it.
//!
//! OpenDuct never decodes or re-encodes the records inside a batch - compressed or not, they travel byte for byte.
//! The broker reads the fixed 61-byte header, verifies the batch checksum, and sets the two fields it owns, as a
//! Kafka leader does: the base offset, and the partition leader epoch it was written in. Kafka's checksum (CRC-32C)
//! covers the batch from its `attributes` field to the end, so setting them never invalidates it.
//!
//! The header layout, from the Kafka protocol specification:
//!
//! | Bytes   | Field                   |
//! |---------|-------------------------|
//! | 0..8    | base offset (i64)       |
//! | 8..12   | batch length (i32): bytes after this field |
//! | 12..16  | partition leader epoch  |
//! | 16      | magic (i8), must be 2   |
//! | 17..21  | CRC-32C of bytes 21..end|
//! | 21..23  | attributes (i16)        |
//! | 23..27  | last offset delta (i32) |
//! | 27..35  | first timestamp (i64)   |
//! | 35..43  | max timestamp (i64)     |
//! | 43..51  | producer id (i64)       |
//! | 51..53  | producer epoch (i16)    |
//! | 53..57  | base sequence (i32)     |
//! | 57..61  | record count (i32)      |

use bytes::{Bytes, BytesMut};

/// Bytes before the batch-length field ends: base offset (8) and batch length (4).
const LOG_OVERHEAD: usize = 12;
/// The full fixed header of a v2 batch.
pub const HEADER_LEN: usize = 61;
const MAGIC_V2: i8 = 2;
const CRC_START: usize = 21;

/// Attribute bits (Kafka `RecordBatch`).
const TRANSACTIONAL_FLAG: i16 = 0x10;
const CONTROL_FLAG: i16 = 0x20;

/// Why a batch was refused. Every variant is the client's input being wrong; none is a broker fault.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
pub enum BatchError {
    #[error("the record batch is truncated")]
    Truncated,
    #[error("the record batch length {0} is invalid")]
    InvalidLength(i64),
    #[error("record batch format (magic) {0} is not supported; only format 2 is")]
    UnsupportedMagic(i8),
    #[error("the record batch checksum does not match its contents")]
    CrcMismatch,
    #[error("the record batch says {count} records but its last offset delta is {last_offset_delta}")]
    InvalidRecordCount { count: i32, last_offset_delta: i32 },
    #[error("transactional and control batches are not supported")]
    Transactional,
}

/// The parts of a batch header OpenDuct acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    pub base_offset: i64,
    /// The leader epoch the batch was written in: as the producer sent it until the broker stores the batch.
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

impl BatchHeader {
    /// The batch is from an idempotent producer: it carries a producer id and sequence numbers.
    pub fn is_idempotent(&self) -> bool {
        self.producer_id >= 0
    }

    /// The sequence number of the batch's last record, wrapping as Kafka does.
    pub fn last_sequence(&self) -> i32 {
        // Kafka sequences wrap from i32::MAX to 0.
        let next = i64::from(self.base_sequence) + i64::from(self.last_offset_delta);
        if next > i64::from(i32::MAX) {
            i32::try_from(next - i64::from(i32::MAX) - 1).unwrap_or(0)
        } else {
            i32::try_from(next).unwrap_or(i32::MAX)
        }
    }
}

fn read_i64(bytes: &[u8], at: usize) -> Result<i64, BatchError> {
    let raw: [u8; 8] = bytes
        .get(at..at + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or(BatchError::Truncated)?;
    Ok(i64::from_be_bytes(raw))
}

fn read_i32(bytes: &[u8], at: usize) -> Result<i32, BatchError> {
    let raw: [u8; 4] = bytes
        .get(at..at + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or(BatchError::Truncated)?;
    Ok(i32::from_be_bytes(raw))
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32, BatchError> {
    let raw: [u8; 4] = bytes
        .get(at..at + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or(BatchError::Truncated)?;
    Ok(u32::from_be_bytes(raw))
}

fn read_i16(bytes: &[u8], at: usize) -> Result<i16, BatchError> {
    let raw: [u8; 2] = bytes
        .get(at..at + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or(BatchError::Truncated)?;
    Ok(i16::from_be_bytes(raw))
}

/// Read and check one complete batch: its length matches the slice, the format is v2, the checksum holds, and the
/// record count agrees with the last offset delta.
pub fn parse_header(batch: &[u8]) -> Result<BatchHeader, BatchError> {
    if batch.len() < HEADER_LEN {
        return Err(BatchError::Truncated);
    }
    let length = i64::from(read_i32(batch, 8)?);
    let expected = i64::try_from(batch.len()).unwrap_or(i64::MAX) - i64::try_from(LOG_OVERHEAD).unwrap_or(0);
    if length != expected {
        return Err(BatchError::InvalidLength(length));
    }
    let magic = i8::from_be_bytes([*batch.get(16).ok_or(BatchError::Truncated)?]);
    if magic != MAGIC_V2 {
        return Err(BatchError::UnsupportedMagic(magic));
    }
    let crc = read_u32(batch, 17)?;
    let covered = batch.get(CRC_START..).ok_or(BatchError::Truncated)?;
    if crc32c::crc32c(covered) != crc {
        return Err(BatchError::CrcMismatch);
    }
    let header = BatchHeader {
        base_offset: read_i64(batch, 0)?,
        partition_leader_epoch: read_i32(batch, 12)?,
        attributes: read_i16(batch, 21)?,
        last_offset_delta: read_i32(batch, 23)?,
        first_timestamp: read_i64(batch, 27)?,
        max_timestamp: read_i64(batch, 35)?,
        producer_id: read_i64(batch, 43)?,
        producer_epoch: read_i16(batch, 51)?,
        base_sequence: read_i32(batch, 53)?,
        record_count: read_i32(batch, 57)?,
    };
    if header.record_count < 1
        || header.last_offset_delta < 0
        || i64::from(header.record_count) != i64::from(header.last_offset_delta) + 1
    {
        return Err(BatchError::InvalidRecordCount {
            count: header.record_count,
            last_offset_delta: header.last_offset_delta,
        });
    }
    Ok(header)
}

/// Refuse what OpenDuct does not support: transactional and control batches.
pub fn check_supported(header: &BatchHeader) -> Result<(), BatchError> {
    if header.attributes & (TRANSACTIONAL_FLAG | CONTROL_FLAG) != 0 {
        return Err(BatchError::Transactional);
    }
    Ok(())
}

/// Split a Produce request's `records` field into its batches, each checked by [`parse_header`].
pub fn split_batches(records: &Bytes) -> Result<Vec<(BatchHeader, Bytes)>, BatchError> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < records.len() {
        let rest = records.get(at..).ok_or(BatchError::Truncated)?;
        if rest.len() < LOG_OVERHEAD {
            return Err(BatchError::Truncated);
        }
        let length = read_i32(rest, 8)?;
        let body = usize::try_from(length).map_err(|_| BatchError::InvalidLength(i64::from(length)))?;
        let total = LOG_OVERHEAD
            .checked_add(body)
            .ok_or(BatchError::InvalidLength(i64::from(length)))?;
        if total < HEADER_LEN || total > rest.len() {
            return Err(if total > rest.len() {
                BatchError::Truncated
            } else {
                BatchError::InvalidLength(i64::from(length))
            });
        }
        let batch = records.slice(at..at + total);
        let header = parse_header(&batch)?;
        out.push((header, batch));
        at += total;
    }
    Ok(out)
}

/// The batch with its base offset set to `offset`.
pub fn with_base_offset(batch: &Bytes, offset: i64) -> Bytes {
    let mut owned = BytesMut::from(batch.as_ref());
    if let Some(head) = owned.get_mut(0..8) {
        head.copy_from_slice(&offset.to_be_bytes());
    }
    owned.freeze()
}

/// The batch as it is stored: its base offset set to `offset` and its partition leader epoch to `leader_epoch` -
/// the only bytes OpenDuct ever changes, in one copy.
pub fn stored_form(batch: &Bytes, offset: i64, leader_epoch: i32) -> Bytes {
    let mut owned = BytesMut::from(batch.as_ref());
    if let Some(head) = owned.get_mut(0..8) {
        head.copy_from_slice(&offset.to_be_bytes());
    }
    if let Some(epoch) = owned.get_mut(12..16) {
        epoch.copy_from_slice(&leader_epoch.to_be_bytes());
    }
    owned.freeze()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A minimal valid v2 batch with `count` empty records' worth of header and an opaque body.
    pub(crate) fn batch(
        count: i32,
        producer_id: i64,
        epoch: i16,
        base_sequence: i32,
        timestamp: i64,
    ) -> Bytes {
        let body = vec![0xABu8; 16];
        let mut b = Vec::with_capacity(HEADER_LEN + body.len());
        b.extend_from_slice(&0i64.to_be_bytes()); // base offset
        let length = i32::try_from(HEADER_LEN - LOG_OVERHEAD + body.len()).unwrap_or(0);
        b.extend_from_slice(&length.to_be_bytes());
        b.extend_from_slice(&0i32.to_be_bytes()); // partition leader epoch
        b.push(2u8); // magic
        b.extend_from_slice(&0u32.to_be_bytes()); // crc, filled below
        b.extend_from_slice(&0i16.to_be_bytes()); // attributes
        b.extend_from_slice(&(count - 1).to_be_bytes()); // last offset delta
        b.extend_from_slice(&timestamp.to_be_bytes()); // first timestamp
        b.extend_from_slice(&timestamp.to_be_bytes()); // max timestamp
        b.extend_from_slice(&producer_id.to_be_bytes());
        b.extend_from_slice(&epoch.to_be_bytes());
        b.extend_from_slice(&base_sequence.to_be_bytes());
        b.extend_from_slice(&count.to_be_bytes());
        b.extend_from_slice(&body);
        let crc = crc32c::crc32c(&b[CRC_START..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(b)
    }

    #[test]
    fn a_valid_batch_is_read_and_its_offset_set_without_breaking_the_checksum() {
        let b = batch(3, 7, 1, 10, 1_000);
        let h = parse_header(&b).unwrap();
        assert_eq!(h.record_count, 3);
        assert_eq!(h.last_offset_delta, 2);
        assert_eq!(h.producer_id, 7);
        assert_eq!(h.last_sequence(), 12);
        let moved = with_base_offset(&b, 42);
        let again = parse_header(&moved).unwrap();
        assert_eq!(
            again.base_offset, 42,
            "the checksum does not cover the base offset"
        );
    }

    #[test]
    fn a_flipped_byte_in_the_body_is_refused() {
        let mut raw = batch(1, -1, -1, -1, 0).to_vec();
        let last = raw.len() - 1;
        raw[last] ^= 0xFF;
        assert_eq!(parse_header(&raw), Err(BatchError::CrcMismatch));
    }

    #[test]
    fn truncated_wrong_format_and_bad_counts_are_errors_not_panics() {
        let good = batch(2, -1, -1, -1, 0);
        assert_eq!(parse_header(&good[..30]), Err(BatchError::Truncated));
        let mut old = good.to_vec();
        old[16] = 1;
        assert_eq!(parse_header(&old), Err(BatchError::UnsupportedMagic(1)));
        assert!(split_batches(&Bytes::from_static(&[0, 0, 0])).is_err());
        assert!(split_batches(&Bytes::from(vec![0u8; 12])).is_err());
    }

    #[test]
    fn several_batches_in_one_field_are_split() {
        let mut both = batch(1, -1, -1, -1, 0).to_vec();
        both.extend_from_slice(&batch(4, -1, -1, -1, 0));
        let split = split_batches(&Bytes::from(both)).unwrap();
        assert_eq!(
            split.iter().map(|(h, _)| h.record_count).collect::<Vec<_>>(),
            vec![1, 4]
        );
    }

    #[test]
    fn sequences_wrap_at_the_top() {
        let h = BatchHeader {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: 0,
            last_offset_delta: 2,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: 1,
            producer_epoch: 0,
            base_sequence: i32::MAX - 1,
            record_count: 3,
        };
        assert_eq!(h.last_sequence(), 0);
    }

    #[test]
    fn the_stored_form_sets_offset_and_epoch_and_keeps_the_checksum() {
        let b = batch(3, 7, 0, 0, 100);
        assert_eq!(parse_header(&b).unwrap().partition_leader_epoch, 0);
        let stored = stored_form(&b, 42, 9);
        let header = parse_header(&stored).unwrap();
        assert_eq!(header.base_offset, 42);
        assert_eq!(header.partition_leader_epoch, 9);
        assert_eq!(
            stored.get(16..),
            b.get(16..),
            "nothing the checksum covers changes"
        );
    }
}
