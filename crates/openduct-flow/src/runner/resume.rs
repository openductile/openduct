// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Where a runner takes up its partitions, and which records after that place already have their run.
//!
//! A run is kept in the workflow's history when it ends, and the group's place is committed a moment later (once a
//! second: each commit is a write the store flushes). A runner that is killed between the two leaves runs that are
//! done and kept, past the committed place - and whoever takes the partition up would run them again. So before it
//! reads on, a runner looks at the end of the history for the runs of records past its place, and passes those
//! records by. What can still run twice is only a run that was going when the node died: begun, not yet kept.

use std::collections::{BTreeMap, HashSet};

use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{EARLIEST, LATEST, Membership, Routed, records};

/// The history looked at first, in records; four times more each time it holds a run past the place, up to MOST.
const FIRST: i64 = 256;
/// The most history looked at: beyond it a runner takes the records as not run (they run again, as before).
const MOST: i64 = 65_536;
const READ_BYTES: i32 = 4 * 1024 * 1024;

/// Where to read each partition from: the group's last commit, or the start (or the end) for one never read.
pub(super) async fn positions(
    client: &mut Routed,
    group: &str,
    source: &str,
    member: &Membership,
    earliest: bool,
) -> Result<BTreeMap<i32, i64>, String> {
    let committed: BTreeMap<i32, i64> = client
        .committed(group, source, &member.partitions)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect();
    let mut out = BTreeMap::new();
    for &p in &member.partitions {
        let offset = match committed.get(&p) {
            Some(o) if *o >= 0 => *o,
            _ => client
                .list_offset(source, p, if earliest { EARLIEST } else { LATEST })
                .await
                .map_err(|e| e.to_string())?,
        };
        out.insert(p, offset);
    }
    Ok(out)
}

/// A run's id as its history's key says it: `<partition>-<offset>` of the record it ran.
fn record_of(key: &[u8]) -> Option<(i32, i64)> {
    let (partition, offset) = std::str::from_utf8(key).ok()?.split_once('-')?;
    Some((partition.parse().ok()?, offset.parse().ok()?))
}

/// The records at or past `next` (each partition's place) whose run is already in the history `runs`. A history that
/// cannot be read gives none: those records run again, which is what a runner did before it looked.
pub(super) async fn already_run(
    client: &mut Routed,
    runs: &str,
    next: &BTreeMap<i32, i64>,
) -> HashSet<(i32, i64)> {
    let mut found = HashSet::new();
    let (Ok(end), Ok(start)) = (
        client.list_offset(runs, 0, LATEST).await,
        client.list_offset(runs, 0, EARLIEST).await,
    ) else {
        return found;
    };
    let (mut upto, mut back) = (end, FIRST);
    while upto > start && end - upto < MOST {
        let from = (end - back).max(start);
        let mut any = false;
        let mut at = from;
        while at < upto {
            let Ok(fetched) = client.fetch(runs, &[(0, at)], 0, READ_BYTES).await else {
                return found;
            };
            let Some(answer) = fetched.into_iter().find(|f| f.partition == 0) else {
                break;
            };
            let (mut batches, before) = (answer.batches, at);
            while !batches.is_empty() {
                let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut batches,
                    Some(records::decompress),
                ) else {
                    break;
                };
                for record in set.records {
                    at = at.max(record.offset + 1);
                    if record.offset >= upto {
                        continue;
                    }
                    if let Some((p, o)) = record.key.as_deref().and_then(record_of)
                        && next.get(&p).is_some_and(|place| o >= *place)
                    {
                        found.insert((p, o));
                        any = true;
                    }
                }
            }
            if at == before {
                break;
            }
        }
        // Runs are kept about in the order their records came: a stretch with none past the place ends the look.
        if !any {
            break;
        }
        upto = from;
        back *= 4;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::record_of;

    #[test]
    fn a_runs_key_names_the_record_it_ran() {
        assert_eq!(record_of(b"3-1042"), Some((3, 1042)));
        assert_eq!(record_of(b"poll/seen"), None);
        assert_eq!(record_of(b"3-"), None);
        assert_eq!(record_of(b"-7"), None);
    }
}
