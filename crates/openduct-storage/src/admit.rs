// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a topic's settings ask of a batch before it is stored - the same for every store, decided once, by the
//! broker that takes the write (in a cluster the partition's leader, before it proposes the batch, so no copy
//! ever holds one that was refused and every copy holds the same bytes):
//!
//! - `max.message.bytes`: a larger batch is refused;
//! - a compacted topic takes only records with a key, as Kafka's does - a record without one could never be
//!   replaced, and would stay for ever. A compressed batch is opened to see (one that does not open is taken:
//!   compaction keeps it whole);
//! - `message.timestamp.type=LogAppendTime`: the batch is stamped with this broker's time.

use bytes::Bytes;

use crate::batch::with_log_append_time;
use crate::error::StoreError;
use crate::records::open;
use crate::settings::TopicSettings;

/// The time now, in milliseconds since 1970.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The batch as the topic stores it, or why the topic does not take it.
pub fn admitted(settings: &TopicSettings, batch: Bytes, now_ms: i64) -> Result<Bytes, StoreError> {
    if settings.is_empty() {
        return Ok(batch);
    }
    let limit = settings.max_message_bytes();
    if batch.len() > limit {
        return Err(StoreError::BatchTooLarge {
            size: batch.len(),
            limit,
        });
    }
    if settings.compacts()
        && let Ok(opened) = open(&batch)
        && let Some(keyless) = opened.records.iter().find(|r| r.key.is_none())
    {
        return Err(StoreError::KeylessRecord(keyless.offset_delta));
    }
    if settings.log_append_time() {
        return Ok(with_log_append_time(&batch, now_ms));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{LOG_APPEND_TIME_FLAG, parse_header};
    use crate::records::tests::{compressed, keyed};

    fn settings(pairs: &[(&str, &str)]) -> TopicSettings {
        let mut s = TopicSettings::default();
        for (name, value) in pairs {
            s.set(name, value).unwrap();
        }
        s
    }

    #[test]
    fn a_compacted_topic_refuses_a_record_without_a_key_compressed_or_not() {
        let compacting = settings(&[(crate::CLEANUP_POLICY, "compact")]);
        let with_keys = keyed(&[(Some("a"), Some("1")), (Some("b"), None)], 5, 0);
        let one_without = keyed(&[(Some("a"), Some("1")), (None, Some("2"))], 5, 0);
        assert_eq!(admitted(&compacting, with_keys.clone(), 9).unwrap(), with_keys);
        assert!(matches!(
            admitted(&compacting, one_without.clone(), 9),
            Err(StoreError::KeylessRecord(1))
        ));
        for codec in [1i16, 2, 3, 4] {
            assert!(admitted(&compacting, compressed(&with_keys, codec), 9).is_ok());
            assert!(matches!(
                admitted(&compacting, compressed(&one_without, codec), 9),
                Err(StoreError::KeylessRecord(1))
            ));
        }
        // A topic that does not compact takes it, and no topic looks inside a batch it has no reason to.
        assert!(admitted(&TopicSettings::default(), one_without.clone(), 9).is_ok());
        assert!(admitted(&settings(&[(crate::RETENTION_BYTES, "5")]), one_without, 9).is_ok());
    }

    #[test]
    fn log_append_time_stamps_the_batch_and_it_still_checks_out() {
        let stamping = settings(&[(crate::MESSAGE_TIMESTAMP_TYPE, "LogAppendTime")]);
        let batch = keyed(&[(Some("a"), Some("1")), (Some("b"), Some("2"))], 5, 0);
        let stored = admitted(&stamping, batch.clone(), 1_700_000_000_000).unwrap();
        let header = parse_header(&stored).unwrap();
        assert_eq!(header.max_timestamp, 1_700_000_000_000);
        assert_ne!(header.attributes & LOG_APPEND_TIME_FLAG, 0);
        assert_eq!(header.record_count, 2);
        assert_eq!(stored.len(), batch.len(), "only the header changed");
        // The producer's timestamps are kept by default.
        assert_eq!(
            admitted(&TopicSettings::default(), batch.clone(), 1).unwrap(),
            batch
        );
    }

    #[test]
    fn a_batch_larger_than_the_topic_takes_is_refused() {
        let small = settings(&[(crate::MAX_MESSAGE_BYTES, "70")]);
        let batch = keyed(
            &[(Some("a"), Some("a value that makes the batch too long"))],
            5,
            0,
        );
        assert!(matches!(
            admitted(&small, batch, 1),
            Err(StoreError::BatchTooLarge { limit: 70, .. })
        ));
    }
}
