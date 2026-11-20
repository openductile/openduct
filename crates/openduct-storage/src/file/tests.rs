// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::batch::tests::batch;

fn topic_config(partitions: i32) -> TopicConfig {
    TopicConfig {
        partitions,
        retention_ms: None,
    }
}

#[test]
fn data_survives_a_restart_byte_for_byte_and_offsets_continue() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<Bytes>;
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        store.append("t", 0, batch(3, -1, -1, -1, 10)).unwrap();
        store.append("t", 0, batch(2, -1, -1, -1, 20)).unwrap();
        written = store.read("t", 0, 0, usize::MAX).unwrap().batches;
    }
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    let read = store.read("t", 0, 0, usize::MAX).unwrap();
    assert_eq!(read.batches, written);
    assert_eq!(read.offsets.high_watermark, 5);
    assert_eq!(
        store
            .append("t", 0, batch(1, -1, -1, -1, 30))
            .unwrap()
            .base_offset,
        5
    );
}

#[test]
fn a_second_process_cannot_open_the_same_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _first = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert!(matches!(
        FileStore::open(dir.path(), FileStoreConfig::default()),
        Err(StoreError::Locked(_))
    ));
}

#[test]
fn an_unfinished_final_write_is_cut_off_and_the_log_goes_on() {
    let dir = tempfile::tempdir().unwrap();
    let segment;
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        store.append("t", 0, batch(2, -1, -1, -1, 0)).unwrap();
        segment = dir.path().join("topics/t/0").join(segment_name(0));
    }
    // Power fails halfway through the next frame.
    let mut f = OpenOptions::new().append(true).open(&segment).unwrap();
    f.write_all(&[0, 0, 0, 90, 1, 2]).unwrap();
    drop(f);
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 2);
    assert_eq!(
        store.append("t", 0, batch(1, -1, -1, -1, 0)).unwrap().base_offset,
        2
    );
    assert_eq!(store.read("t", 0, 0, usize::MAX).unwrap().batches.len(), 2);
}

#[test]
fn damage_inside_acknowledged_data_refuses_to_open() {
    let dir = tempfile::tempdir().unwrap();
    let segment;
    {
        let store = FileStore::open(
            dir.path(),
            FileStoreConfig {
                segment_bytes: 1,
                fsync: FsyncPolicy::Always,
            },
        )
        .unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        store.append("t", 0, batch(1, -1, -1, -1, 0)).unwrap();
        store.append("t", 0, batch(1, -1, -1, -1, 0)).unwrap();
        segment = dir.path().join("topics/t/0").join(segment_name(0));
    }
    // A bit flips in the first (no longer newest) segment.
    let mut raw = fs::read(&segment).unwrap();
    let last = raw.len() - 1;
    raw[last] ^= 0x01;
    fs::write(&segment, raw).unwrap();
    assert!(matches!(
        FileStore::open(dir.path(), FileStoreConfig::default()),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn damage_in_place_in_the_newest_segment_refuses_rather_than_dropping_what_follows() {
    let dir = tempfile::tempdir().unwrap();
    let segment;
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        store.append("t", 0, batch(1, -1, -1, -1, 0)).unwrap();
        store.append("t", 0, batch(1, -1, -1, -1, 0)).unwrap();
        segment = dir.path().join("topics/t/0").join(segment_name(0));
    }
    // A bit flips in the FIRST frame; an acknowledged frame follows it.
    let mut raw = fs::read(&segment).unwrap();
    raw[FRAME_HEADER + 30] ^= 0x01;
    fs::write(&segment, raw).unwrap();
    assert!(matches!(
        FileStore::open(dir.path(), FileStoreConfig::default()),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn a_topic_whose_creation_never_finished_is_skipped_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    drop(FileStore::open(dir.path(), FileStoreConfig::default()).unwrap());
    fs::create_dir_all(dir.path().join("topics/half/0")).unwrap();
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert!(store.topic("half").is_none());
    store.create_topic("half", topic_config(1)).unwrap();
}

#[test]
fn producer_state_survives_a_restart_so_a_retry_is_still_a_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let b = batch(2, 11, 0, 0, 0);
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        assert!(!store.append("t", 0, b.clone()).unwrap().duplicate);
    }
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    let again = store.append("t", 0, b).unwrap();
    assert!(again.duplicate);
    assert_eq!(again.base_offset, 0);
    assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 2);
}

#[test]
fn segments_roll_and_retention_deletes_whole_old_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(
        dir.path(),
        FileStoreConfig {
            segment_bytes: 1,
            fsync: FsyncPolicy::Never,
        },
    )
    .unwrap();
    store
        .create_topic(
            "r",
            TopicConfig {
                partitions: 1,
                retention_ms: Some(1_000),
            },
        )
        .unwrap();
    store.append("r", 0, batch(1, -1, -1, -1, 1_000)).unwrap();
    store.append("r", 0, batch(1, -1, -1, -1, 2_000)).unwrap();
    store.append("r", 0, batch(1, -1, -1, -1, 9_000)).unwrap();
    assert_eq!(store.enforce_retention(9_500).unwrap(), 2);
    let offsets = store.offsets("r", 0).unwrap();
    assert_eq!((offsets.log_start, offsets.high_watermark), (2, 3));
    drop(store);
    let reopened = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert_eq!(reopened.offsets("r", 0).unwrap().log_start, 2);
}

#[test]
fn producer_ids_and_group_offsets_are_durable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        assert_eq!(store.next_producer_id().unwrap(), 0);
        assert_eq!(store.next_producer_id().unwrap(), 1);
        store
            .commit_offsets(
                "g/1",
                &[CommittedOffset {
                    topic: "t".into(),
                    partition: 0,
                    offset: 7,
                    metadata: None,
                    commit_timestamp_ms: 1,
                }],
            )
            .unwrap();
    }
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert_eq!(store.next_producer_id().unwrap(), 2, "never the same id twice");
    assert_eq!(store.committed_offsets("g/1").unwrap()[0].offset, 7);
    assert_eq!(store.groups().unwrap(), vec!["g/1".to_owned()]);
}

#[test]
fn the_groups_with_offsets_are_listed_by_name_whatever_the_name_holds() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert!(
        store.groups().unwrap().is_empty(),
        "none yet, and no groups directory either"
    );
    let offset = CommittedOffset {
        topic: "t".into(),
        partition: 0,
        offset: 1,
        metadata: None,
        commit_timestamp_ms: 1,
    };
    for g in ["billing", "a/b:c+d@e", "Zürich-lager", "x.y_z"] {
        store.commit_offsets(g, std::slice::from_ref(&offset)).unwrap();
    }
    // Something else in the directory - a write interrupted, a stray file - is not a group.
    std::fs::write(dir.path().join("groups").join("62696c6c696e67.tmp"), b"{").unwrap();
    std::fs::write(dir.path().join("groups").join("not-hex.json"), b"[]").unwrap();
    let mut want = vec!["billing", "a/b:c+d@e", "Zürich-lager", "x.y_z"];
    want.sort_unstable();
    assert_eq!(store.groups().unwrap(), want);
    let memory = crate::MemoryStore::new();
    for g in ["b", "a"] {
        memory.commit_offsets(g, std::slice::from_ref(&offset)).unwrap();
    }
    assert_eq!(memory.groups().unwrap(), vec!["a", "b"]);
}

#[test]
fn a_groups_offsets_are_deleted_by_partition_or_whole_and_the_group_with_its_last() {
    let dir = tempfile::tempdir().unwrap();
    let file = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    let memory = crate::MemoryStore::new();
    let stores: [&dyn crate::Store; 2] = [&file, &memory];
    let offset = |topic: &str, partition: i32| CommittedOffset {
        topic: topic.into(),
        partition,
        offset: 5,
        metadata: None,
        commit_timestamp_ms: 1,
    };
    for store in stores {
        store
            .commit_offsets("g", &[offset("a", 0), offset("a", 1), offset("b", 0)])
            .unwrap();
        store.commit_offsets("h", &[offset("a", 0)]).unwrap();
        store
            .delete_offsets("g", Some(&[("a".into(), 1), ("zzz".into(), 7)]))
            .unwrap();
        let left: Vec<(String, i32)> = store
            .committed_offsets("g")
            .unwrap()
            .into_iter()
            .map(|o| (o.topic, o.partition))
            .collect();
        assert_eq!(
            left,
            vec![("a".into(), 0), ("b".into(), 0)],
            "only the partition named; unknown ones ignored"
        );
        store
            .delete_offsets("g", Some(&[("a".into(), 0), ("b".into(), 0)]))
            .unwrap();
        assert_eq!(
            store.groups().unwrap(),
            vec!["h"],
            "the group goes with its last offset"
        );
        store.delete_offsets("h", None).unwrap();
        store.delete_offsets("never-was", None).unwrap();
        assert!(store.groups().unwrap().is_empty());
    }
    // On disk too, after a restart.
    drop(file);
    let file = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert!(file.groups().unwrap().is_empty());
    assert!(file.committed_offsets("g").unwrap().is_empty());
}

#[test]
fn a_deleted_topic_is_gone_after_a_restart_and_its_name_can_be_used_again() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("old", topic_config(2)).unwrap();
        store.append("old", 1, batch(3, -1, -1, -1, 1_000)).unwrap();
        store
            .commit_offsets(
                "g",
                &[CommittedOffset {
                    topic: "old".into(),
                    partition: 1,
                    offset: 3,
                    metadata: None,
                    commit_timestamp_ms: 0,
                }],
            )
            .unwrap();
        store.delete_topic("old").unwrap();
        assert!(store.topic("old").is_none());
        assert!(matches!(
            store.append("old", 1, batch(1, -1, -1, -1, 1_000)),
            Err(StoreError::UnknownTopic(_))
        ));
        assert!(matches!(
            store.delete_topic("old"),
            Err(StoreError::UnknownTopic(_))
        ));
        assert!(store.committed_offsets("g").unwrap().is_empty());
        assert_eq!(fs::read_dir(dir.path().join("trash")).unwrap().count(), 0);
    }
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert!(store.topics().is_empty());
    store.create_topic("old", topic_config(1)).unwrap();
    assert_eq!(
        store.offsets("old", 0).unwrap().high_watermark,
        0,
        "a new topic, not the old one's data"
    );
}

#[test]
fn a_delete_interrupted_after_the_rename_finishes_on_the_next_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("going", topic_config(1)).unwrap();
        store.append("going", 0, batch(1, -1, -1, -1, 1_000)).unwrap();
    }
    // What a crash right after the rename leaves behind.
    fs::rename(dir.path().join("topics/going"), dir.path().join("trash/going-1")).unwrap();
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert!(store.topic("going").is_none());
    assert_eq!(fs::read_dir(dir.path().join("trash")).unwrap().count(), 0);
}

#[test]
fn each_topic_has_its_own_id_kept_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("a", topic_config(1)).unwrap();
        store.create_topic("b", topic_config(1)).unwrap();
        (store.topic("a").unwrap().id, store.topic("b").unwrap().id)
    };
    assert!(!a.is_nil() && !b.is_nil() && a != b);
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert_eq!(
        (store.topic("a").unwrap().id, store.topic("b").unwrap().id),
        (a, b)
    );
    store.delete_topic("a").unwrap();
    store.create_topic("a", topic_config(1)).unwrap();
    assert_ne!(
        store.topic("a").unwrap().id,
        a,
        "a topic made again under the same name is a new topic"
    );
}

#[test]
fn a_topic_written_before_ids_is_given_one_and_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("old", topic_config(1)).unwrap();
    }
    let settings = dir.path().join("topics").join("old").join("topic.json");
    std::fs::write(&settings, r#"{"name":"old","partitions":1,"retention_ms":null}"#).unwrap();
    let given = FileStore::open(dir.path(), FileStoreConfig::default())
        .unwrap()
        .topic("old")
        .unwrap()
        .id;
    assert!(!given.is_nil());
    let again = FileStore::open(dir.path(), FileStoreConfig::default())
        .unwrap()
        .topic("old")
        .unwrap()
        .id;
    assert_eq!(given, again);
}

#[test]
fn appends_waiting_together_share_one_flush_and_offsets_stay_dense() {
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(FileStore::open(dir.path(), FileStoreConfig::default()).unwrap());
    store.create_topic("t", topic_config(1)).unwrap();
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let store = std::sync::Arc::clone(&store);
            std::thread::spawn(move || {
                (0..50)
                    .map(|_| store.append("t", 0, batch(2, -1, -1, -1, 0)).unwrap().base_offset)
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut offsets: Vec<i64> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
    offsets.sort_unstable();
    assert_eq!(offsets, (0..800).map(|n| n * 2).collect::<Vec<_>>());
    let flushes = store.with_partition("t", 0, |p| Ok(p.commits.flushes())).unwrap();
    assert!(
        flushes < 800,
        "800 appends took {flushes} flushes: none were shared"
    );
    // Every acknowledged append is readable, and the high watermark says so.
    assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 1_600);
    assert_eq!(store.read("t", 0, 0, usize::MAX).unwrap().batches.len(), 800);
}

#[test]
fn a_retried_batch_is_answered_with_its_offset_once_it_is_on_the_device() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    store.create_topic("t", topic_config(1)).unwrap();
    assert_eq!(store.append("t", 0, batch(3, 9, 0, 0, 0)).unwrap().base_offset, 0);
    let again = store.append("t", 0, batch(3, 9, 0, 0, 0)).unwrap();
    assert_eq!((again.base_offset, again.duplicate), (0, true));
    assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 3);
}

#[test]
fn segments_rolled_under_concurrent_appends_lose_nothing_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = FileStoreConfig {
        segment_bytes: 4_096,
        fsync: FsyncPolicy::Always,
    };
    {
        let store = std::sync::Arc::new(FileStore::open(dir.path(), config).unwrap());
        store.create_topic("t", topic_config(1)).unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let store = std::sync::Arc::clone(&store);
                std::thread::spawn(move || {
                    for _ in 0..40 {
                        store.append("t", 0, batch(1, -1, -1, -1, 0)).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 320);
    }
    let store = FileStore::open(dir.path(), config).unwrap();
    assert_eq!(store.offsets("t", 0).unwrap().high_watermark, 320);
    assert_eq!(store.read("t", 0, 0, usize::MAX).unwrap().batches.len(), 320);
}

#[test]
fn a_retention_change_is_kept_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("t", topic_config(2)).unwrap();
        store.set_retention("t", Some(60_000)).unwrap();
        assert_eq!(store.topic("t").unwrap().retention_ms, Some(60_000));
        assert!(matches!(
            store.set_retention("nope", None),
            Err(StoreError::UnknownTopic(_))
        ));
    }
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    let t = store.topic("t").unwrap();
    assert_eq!(
        (t.retention_ms, t.partitions),
        (Some(60_000), 2),
        "the rest of the topic unchanged"
    );
    store.set_retention("t", None).unwrap();
    drop(store);
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert_eq!(store.topic("t").unwrap().retention_ms, None);
}

/// The segment files of partition 0 of `topic`.
fn segment_files(dir: &std::path::Path, topic: &str) -> usize {
    std::fs::read_dir(dir.join("topics").join(topic).join("0"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".log")
        })
        .count()
}

#[test]
fn deleted_records_are_gone_their_segments_with_them_and_the_start_lasts() {
    let dir = tempfile::tempdir().unwrap();
    let small = FileStoreConfig {
        segment_bytes: 1,
        fsync: FsyncPolicy::Never,
    };
    let store = FileStore::open(dir.path(), small).unwrap();
    store.create_topic("d", topic_config(1)).unwrap();
    // Ten batches of two records, one segment each: offsets 0 to 19.
    for i in 0..10 {
        store.append("d", 0, batch(2, -1, -1, -1, 1_000 + i)).unwrap();
    }
    assert_eq!(segment_files(dir.path(), "d"), 10);
    // Offset 7 is inside the batch of 6 and 7: the log starts there; the three segments before it go.
    assert_eq!(store.delete_records("d", 0, 7).unwrap(), 7);
    assert_eq!(store.offsets("d", 0).unwrap().log_start, 7);
    assert_eq!(segment_files(dir.path(), "d"), 7);
    assert!(matches!(
        store.read("d", 0, 5, 1 << 20),
        Err(StoreError::OffsetOutOfRange { .. })
    ));
    assert!(!store.read("d", 0, 7, 1 << 20).unwrap().batches.is_empty());
    // Never back; never past the end; -1 is the end.
    assert_eq!(store.delete_records("d", 0, 3).unwrap(), 7);
    assert!(matches!(
        store.delete_records("d", 0, 21),
        Err(StoreError::OffsetOutOfRange { .. })
    ));
    assert!(matches!(
        store.delete_records("d", 0, -5),
        Err(StoreError::OffsetOutOfRange { .. })
    ));
    assert_eq!(store.delete_records("d", 0, 12).unwrap(), 12);
    drop(store);

    let store = FileStore::open(dir.path(), small).unwrap();
    assert_eq!(
        store.offsets("d", 0).unwrap().log_start,
        12,
        "the start lasts across a restart"
    );
    assert!(matches!(
        store.read("d", 0, 11, 1 << 20),
        Err(StoreError::OffsetOutOfRange { .. })
    ));
    // The checkpoint carries it.
    let manifest = store.checkpoint(&dir.path().join("checkpoint")).unwrap();
    assert!(manifest.files.iter().any(|f| f.path == "topics/d/0/start.json"));
    assert_eq!(
        store.delete_records("d", 0, -1).unwrap(),
        20,
        "everything: the end"
    );
    store.append("d", 0, batch(1, -1, -1, -1, 5_000)).unwrap();
    assert_eq!(
        store.offsets("d", 0).unwrap().high_watermark,
        21,
        "offsets go on from where they were"
    );

    let memory = crate::MemoryStore::new();
    memory.create_topic("d", topic_config(1)).unwrap();
    for _ in 0..3 {
        memory.append("d", 0, batch(2, -1, -1, -1, 1)).unwrap();
    }
    assert_eq!(memory.delete_records("d", 0, 4).unwrap(), 4);
    assert!(matches!(
        memory.read("d", 0, 1, 1 << 20),
        Err(StoreError::OffsetOutOfRange { .. })
    ));
    assert!(matches!(
        memory.delete_records("d", 0, 9),
        Err(StoreError::OffsetOutOfRange { .. })
    ));
}

fn limited(bytes: &str) -> crate::TopicSettings {
    let mut s = crate::TopicSettings::default();
    s.set(crate::RETENTION_BYTES, bytes).unwrap();
    s
}

#[test]
fn retention_bytes_removes_the_oldest_segments_while_the_rest_still_holds_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    // A segment a batch: every batch closes the one before it.
    let small = FileStoreConfig {
        segment_bytes: 1,
        ..FileStoreConfig::default()
    };
    let store = FileStore::open(dir.path(), small).unwrap();
    store.create_topic("t", topic_config(1)).unwrap();
    for i in 0..6 {
        store.append("t", 0, batch(1, -1, -1, -1, 100 + i)).unwrap();
    }
    let one = store.read("t", 0, 0, usize::MAX).unwrap().batches[0].len() as u64;
    assert_eq!(
        store.enforce_retention(i64::MAX / 2).unwrap(),
        0,
        "no limit set: nothing goes"
    );

    // Room for two and a half batches: the oldest go while at least that much is left - three stay.
    store
        .set_topic_settings("t", limited(&(one * 5 / 2).to_string()))
        .unwrap();
    assert_eq!(store.enforce_retention(0).unwrap(), 3);
    let read = store.read("t", 0, 3, usize::MAX).unwrap();
    assert_eq!(read.offsets.log_start, 3);
    assert_eq!(read.batches.len(), 3);
    assert_eq!(
        store.enforce_retention(0).unwrap(),
        0,
        "and nothing more the next time"
    );

    // A limit of nothing still keeps the segment being written to.
    store.set_topic_settings("t", limited("0")).unwrap();
    store.enforce_retention(0).unwrap();
    let left = store.read("t", 0, 5, usize::MAX).unwrap();
    assert_eq!((left.offsets.log_start, left.offsets.high_watermark), (5, 6));
}

#[test]
fn a_topics_settings_are_kept_across_a_restart_and_a_topic_without_any_is_written_as_before() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
        store.create_topic("plain", topic_config(1)).unwrap();
        store.create_topic("limited", topic_config(1)).unwrap();
        store.set_topic_settings("limited", limited("4096")).unwrap();
        assert!(matches!(
            store.set_topic_settings("nowhere", limited("1")),
            Err(StoreError::UnknownTopic(_))
        ));
    }
    let plain = fs::read_to_string(dir.path().join("topics/plain/topic.json")).unwrap();
    assert!(
        !plain.contains("settings"),
        "an older build reads this file unchanged: {plain}"
    );
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    assert_eq!(
        store.topic("limited").unwrap().settings.retention_bytes(),
        Some(4096)
    );
    assert!(store.topic("plain").unwrap().settings.is_empty());
}

fn compacting(wait_ms: &str) -> crate::TopicSettings {
    let mut s = crate::TopicSettings::default();
    s.set(crate::CLEANUP_POLICY, "compact").unwrap();
    s.set(crate::DELETE_RETENTION_MS, wait_ms).unwrap();
    s
}

/// What a consumer reading a partition from `from` sees: (offset, key, has a value). Checks the log has no hole.
pub(crate) fn consumed(store: &dyn Store, topic: &str, from: i64) -> Vec<(i64, String, bool)> {
    let read = store.read(topic, 0, from, usize::MAX).unwrap();
    let mut out = Vec::new();
    let mut next = None;
    for b in &read.batches {
        let (header, all) = crate::records::records(b).unwrap();
        assert!(
            next.is_none_or(|n| n == header.base_offset),
            "a hole before {}",
            header.base_offset
        );
        next = Some(header.base_offset + i64::from(header.last_offset_delta) + 1);
        for r in all {
            let key = String::from_utf8(b[r.key.unwrap()].to_vec()).unwrap();
            out.push((header.base_offset + i64::from(r.offset_delta), key, !r.tombstone));
        }
    }
    assert_eq!(
        next.unwrap_or(read.offsets.high_watermark),
        read.offsets.high_watermark
    );
    out
}

/// The same five batches for both stores: a and b, a again, c, a tombstone for b, d.
pub(crate) fn write_keyed(store: &dyn Store) {
    use crate::records::tests::keyed;
    for (i, records) in [
        &[(Some("a"), Some("1")), (Some("b"), Some("1"))][..],
        &[(Some("a"), Some("2"))],
        &[(Some("c"), Some("1"))],
        &[(Some("b"), None)],
        &[(Some("d"), Some("1"))],
    ]
    .into_iter()
    .enumerate()
    {
        store.append("t", 0, keyed(records, 1_000 + i as i64, 0)).unwrap();
    }
}

#[test]
fn a_compacted_topic_keeps_the_newest_of_each_key_at_its_offset_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    // A segment a batch: the last one is the segment being written to, which is never cleaned.
    let small = FileStoreConfig {
        segment_bytes: 1,
        ..FileStoreConfig::default()
    };
    let after = vec![
        (2, "a".to_owned(), true),
        (3, "c".into(), true),
        (5, "d".into(), true),
    ];
    {
        let store = FileStore::open(dir.path(), small).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        store.create_topic("plain", topic_config(1)).unwrap();
        write_keyed(&store);
        assert_eq!(
            store.compact(10_000).unwrap(),
            0,
            "a topic that does not compact is left alone"
        );
        store.set_topic_settings("t", compacting("1000")).unwrap();
        let before = store
            .read("t", 0, 0, usize::MAX)
            .unwrap()
            .batches
            .iter()
            .map(Bytes::len)
            .sum::<usize>();

        assert_eq!(store.compact(10_000).unwrap(), 2, "the first a and b went");
        assert_eq!(
            consumed(&store, "t", 0),
            vec![
                (2, "a".to_owned(), true),
                (3, "c".into(), true),
                (4, "b".into(), false),
                (5, "d".into(), true)
            ]
        );
        let now = store.read("t", 0, 0, usize::MAX).unwrap();
        assert!(now.batches.iter().map(Bytes::len).sum::<usize>() < before);
        assert_eq!((now.offsets.log_start, now.offsets.high_watermark), (0, 6));
        assert_eq!(
            consumed(&store, "t", 4).first().unwrap().0,
            4,
            "a read from the middle starts there"
        );

        // The tombstone waits delete.retention.ms after the pass that cleaned past it.
        assert_eq!(store.compact(10_999).unwrap(), 0);
        assert_eq!(store.compact(11_000).unwrap(), 1);
        assert_eq!(consumed(&store, "t", 0), after);
        assert_eq!(store.compact(99_000).unwrap(), 0, "nothing new: nothing done");
    }
    let store = FileStore::open(dir.path(), small).unwrap();
    assert_eq!(
        consumed(&store, "t", 0),
        after,
        "read back from the segments as compaction left them"
    );
    assert_eq!(
        store.compact(99_500).unwrap(),
        0,
        "what was cleaned is remembered"
    );
    // Offsets go on from where they were, and a new value for a replaces the kept one on the next pass.
    use crate::records::tests::keyed;
    assert_eq!(
        store
            .append("t", 0, keyed(&[(Some("a"), Some("3"))], 2_000, 0))
            .unwrap()
            .base_offset,
        6
    );
    assert_eq!(
        store
            .append("t", 0, keyed(&[(Some("e"), Some("1"))], 2_001, 0))
            .unwrap()
            .base_offset,
        7
    );
    assert_eq!(store.compact(100_000).unwrap(), 1);
    assert_eq!(
        consumed(&store, "t", 0),
        vec![
            (3, "c".to_owned(), true),
            (5, "d".into(), true),
            (6, "a".into(), true),
            (7, "e".into(), true)
        ]
    );
    // A topic that only compacts keeps its newest records for ever: retention by time removes nothing.
    store.set_retention("t", Some(1)).unwrap();
    assert_eq!(store.enforce_retention(i64::MAX / 2).unwrap(), 0);
    assert!(
        !dir.path()
            .join("topics/t/0")
            .read_dir()
            .unwrap()
            .any(|f| { f.unwrap().file_name().to_string_lossy().ends_with(".clean") })
    );
}

#[test]
fn records_younger_than_the_lag_are_not_compacted() {
    let dir = tempfile::tempdir().unwrap();
    let small = FileStoreConfig {
        segment_bytes: 1,
        ..FileStoreConfig::default()
    };
    let store = FileStore::open(dir.path(), small).unwrap();
    store.create_topic("t", topic_config(1)).unwrap();
    let mut settings = compacting("1000");
    settings.set(crate::MIN_COMPACTION_LAG_MS, "500").unwrap();
    store.set_topic_settings("t", settings).unwrap();
    write_keyed(&store);
    // The batches were written at 1000 to 1004: at 1400 none is old enough, at 1501 the first two are.
    assert_eq!(store.compact(1_400).unwrap(), 0);
    assert_eq!(consumed(&store, "t", 0).len(), 6);
    assert_eq!(
        store.compact(1_501).unwrap(),
        1,
        "the first a, replaced by the second"
    );
    assert_eq!(consumed(&store, "t", 0).len(), 5);
}

#[test]
fn segment_ms_closes_a_segment_that_is_not_full_so_compaction_reaches_it() {
    use crate::records::tests::keyed;
    let dir = tempfile::tempdir().unwrap();
    // Segments of the usual size: none of these few batches would ever fill one.
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    store.create_topic("t", topic_config(1)).unwrap();
    let mut settings = compacting("1000");
    settings.set(crate::SEGMENT_MS, "100").unwrap();
    store.set_topic_settings("t", settings).unwrap();
    // a at 1000 and 1050 share a segment; 1100 is 100 ms after its first record and starts the next; so does 1200.
    for (i, at) in [1_000, 1_050, 1_100, 1_200].into_iter().enumerate() {
        let value = i.to_string();
        store
            .append("t", 0, keyed(&[(Some("a"), Some(&value))], at, 0))
            .unwrap();
    }
    let segments = || {
        let mut names: Vec<String> = dir
            .path()
            .join("topics/t/0")
            .read_dir()
            .unwrap()
            .map(|f| f.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".log"))
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        segments(),
        vec![
            "00000000000000000000.log".to_owned(),
            "00000000000000000002.log".into(),
            "00000000000000000003.log".into()
        ]
    );
    assert_eq!(
        store.compact(10_000).unwrap(),
        2,
        "the two closed segments are cleaned"
    );
    assert_eq!(
        consumed(&store, "t", 0),
        vec![(2, "a".to_owned(), true), (3, "a".into(), true)]
    );
    drop(store);
    // Read back: the segment being written to still knows when its first record was written.
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    store
        .append("t", 0, keyed(&[(Some("a"), Some("4"))], 1_250, 0))
        .unwrap();
    assert_eq!(
        segments().len(),
        3,
        "50 ms after the segment's first record: no new one"
    );
    store
        .append("t", 0, keyed(&[(Some("a"), Some("5"))], 1_300, 0))
        .unwrap();
    assert_eq!(segments().len(), 4);
}

#[test]
fn a_compacted_topic_refuses_a_keyless_record_and_log_append_time_stamps_what_is_stored() {
    use crate::records::tests::keyed;
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path(), FileStoreConfig::default()).unwrap();
    store.create_topic("t", topic_config(1)).unwrap();
    let mut settings = compacting("1000");
    settings
        .set(crate::MESSAGE_TIMESTAMP_TYPE, "LogAppendTime")
        .unwrap();
    store.set_topic_settings("t", settings.clone()).unwrap();
    assert!(matches!(
        store.append("t", 0, keyed(&[(None, Some("1"))], 5, 0)),
        Err(StoreError::KeylessRecord(0))
    ));
    let before = crate::now_ms();
    assert_eq!(
        store
            .append("t", 0, keyed(&[(Some("a"), Some("1"))], 5, 0))
            .unwrap()
            .base_offset,
        0
    );
    let stored = store.read("t", 0, 0, usize::MAX).unwrap().batches;
    let header = crate::batch::parse_stored_header(&stored[0]).unwrap();
    assert!(
        header.max_timestamp >= before,
        "the broker's time, not the producer's 5"
    );
    assert_ne!(header.attributes & crate::batch::LOG_APPEND_TIME_FLAG, 0);
    // One broker keeps one copy: it does not promise two.
    settings.set(crate::MIN_INSYNC_REPLICAS, "2").unwrap();
    match store.set_topic_settings("t", settings) {
        Err(StoreError::Unsupported(why)) => assert!(why.contains("one copy"), "{why}"),
        other => panic!("refused, not {other:?}"),
    }
    assert_eq!(store.topic("t").unwrap().settings.min_insync_replicas(), 1);
}

/// How long a pass holds a partition: not a check, a measurement. Run it with
/// `cargo test --release -p openduct-storage -- --ignored --nocapture how_long_a_pass_takes`.
#[test]
#[ignore = "a measurement: about 256 MiB written and compacted"]
fn how_long_a_pass_takes() {
    use crate::records::tests::keyed;
    let dir = tempfile::tempdir().unwrap();
    let config = FileStoreConfig {
        segment_bytes: 16 * 1024 * 1024,
        fsync: FsyncPolicy::Never,
    };
    let store = FileStore::open(dir.path(), config).unwrap();
    store.create_topic("t", topic_config(1)).unwrap();
    store.set_topic_settings("t", compacting("1000")).unwrap();
    // Batches of 16 records of 1 KiB over 20,000 keys, so most records are replaced later.
    let value = "v".repeat(1024);
    let mut written = 0usize;
    let mut n = 0u64;
    while written < 256 * 1024 * 1024 {
        let keys: Vec<String> = (0..16)
            .map(|i| format!("key-{}", (n * 16 + i) % 20_000))
            .collect();
        let records: Vec<(Option<&str>, Option<&str>)> = keys
            .iter()
            .map(|k| (Some(k.as_str()), Some(value.as_str())))
            .collect();
        let batch = keyed(&records, 1_000, 0);
        written += batch.len();
        store.append("t", 0, batch).unwrap();
        n += 1;
    }
    let started = std::time::Instant::now();
    let removed = store.compact(10_000).unwrap();
    let took = started.elapsed();
    let left: usize = store
        .read("t", 0, 0, usize::MAX)
        .unwrap()
        .batches
        .iter()
        .map(Bytes::len)
        .sum();
    println!(
        "compacted {} MiB in {:.2} s ({:.0} MiB/s): {removed} records removed, {} MiB left",
        written / (1024 * 1024),
        took.as_secs_f64(),
        written as f64 / (1024.0 * 1024.0) / took.as_secs_f64(),
        left / (1024 * 1024)
    );
}

/// The same for both stores: two producers' transactions side by side, one committed and one aborted.
pub(crate) fn transactions_are_tracked(store: &dyn Store) {
    use crate::batch::{Marker, tests::transactional};
    let offsets = |store: &dyn Store| {
        let o = store.offsets("t", 0).unwrap();
        (o.high_watermark, o.last_stable)
    };
    // Outside any transaction: the last stable offset is the end.
    store.append("t", 0, batch(2, -1, -1, -1, 1)).unwrap();
    assert_eq!(offsets(store), (2, 2));
    // Producer 7 opens a transaction at offset 2: nothing from there on is stable, whoever wrote it.
    assert_eq!(
        store
            .append("t", 0, transactional(2, 7, 0, 2))
            .unwrap()
            .base_offset,
        2
    );
    store.append("t", 0, batch(1, -1, -1, -1, 3)).unwrap();
    assert_eq!(offsets(store), (5, 2));
    // Producer 9 opens one at 5, and commits: still held back by producer 7's.
    store.append("t", 0, transactional(1, 9, 0, 4)).unwrap();
    let committed = store.end_transaction("t", 0, 9, 0, Marker::Commit, 1).unwrap();
    assert_eq!(committed.base_offset, 6, "a marker takes an offset of its own");
    assert_eq!(offsets(store), (7, 2));
    assert!(store.aborted_transactions("t", 0, 0, 100).unwrap().is_empty());
    // Producer 7 writes more, then aborts: everything is stable, and its records are named to be skipped.
    store.append("t", 0, transactional(3, 7, 2, 5)).unwrap();
    let aborted = store.end_transaction("t", 0, 7, 0, Marker::Abort, 1).unwrap();
    assert_eq!(aborted.base_offset, 10);
    assert_eq!(offsets(store), (11, 11));
    let skipped = crate::AbortedTransaction {
        producer_id: 7,
        first_offset: 2,
        last_offset: 10,
    };
    assert_eq!(store.aborted_transactions("t", 0, 0, 100).unwrap(), vec![skipped]);
    // Asked for by the range a consumer reads: only what overlaps it.
    assert_eq!(store.aborted_transactions("t", 0, 10, 11).unwrap(), vec![skipped]);
    assert!(store.aborted_transactions("t", 0, 11, 100).unwrap().is_empty());
    assert!(store.aborted_transactions("t", 0, 0, 2).unwrap().is_empty());
    // The same producer's next transaction is its own.
    store.append("t", 0, transactional(1, 7, 5, 6)).unwrap();
    assert_eq!(offsets(store), (12, 11));
    store.end_transaction("t", 0, 7, 0, Marker::Commit, 1).unwrap();
    assert_eq!(offsets(store), (13, 13));
    assert_eq!(store.aborted_transactions("t", 0, 0, 100).unwrap().len(), 1);
    // A client may not write a marker; every batch, markers too, reads back as it was stored.
    let forged = crate::batch::marker_batch(7, 0, Marker::Commit, 0, 0);
    assert!(matches!(store.append("t", 0, forged), Err(StoreError::Batch(_))));
    assert_eq!(store.read("t", 0, 0, usize::MAX).unwrap().batches.len(), 9);
}

#[test]
fn a_partitions_transactions_are_tracked_and_read_back_from_its_segments() {
    let dir = tempfile::tempdir().unwrap();
    let small = FileStoreConfig {
        segment_bytes: 200,
        ..FileStoreConfig::default()
    };
    {
        let store = FileStore::open(dir.path(), small).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        transactions_are_tracked(&store);
        // One more left open when the broker stops.
        store
            .append("t", 0, crate::batch::tests::transactional(1, 21, 0, 9))
            .unwrap();
    }
    let store = FileStore::open(dir.path(), small).unwrap();
    let offsets = store.offsets("t", 0).unwrap();
    assert_eq!(
        (offsets.high_watermark, offsets.last_stable),
        (14, 13),
        "the open one is open still"
    );
    assert_eq!(
        store.aborted_transactions("t", 0, 0, 100).unwrap(),
        vec![crate::AbortedTransaction {
            producer_id: 7,
            first_offset: 2,
            last_offset: 10
        }]
    );
    // Retention that removes an aborted transaction whole forgets it.
    store.set_retention("t", Some(1)).unwrap();
    store.enforce_retention(i64::MAX / 2).unwrap();
    let start = store.offsets("t", 0).unwrap().log_start;
    assert!(start > 10, "the test's segments are small enough to go: {start}");
    assert!(store.aborted_transactions("t", 0, 0, 100).unwrap().is_empty());
}

/// A batch of keyed records in a transaction of `producer`.
fn keyed_in_transaction(records: &[(Option<&str>, Option<&str>)], producer: i64, sequence: i32) -> Bytes {
    let mut b = crate::records::tests::keyed(records, 1_000, 0x10).to_vec();
    b[43..51].copy_from_slice(&producer.to_be_bytes());
    b[51..53].copy_from_slice(&0i16.to_be_bytes());
    b[53..57].copy_from_slice(&sequence.to_be_bytes());
    let crc = crate::batch::checksum(&b);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(b)
}

/// For both stores: compaction of a topic that holds transactions - one aborted, two committed, one still open.
pub(crate) fn compaction_minds_transactions(store: &dyn Store) {
    use crate::batch::Marker;
    use crate::records::tests::keyed;
    store.set_topic_settings("t", compacting("0")).unwrap();
    store
        .append("t", 0, keyed(&[(Some("a"), Some("1"))], 1_000, 0))
        .unwrap(); // 0
    // Aborted: a newer `a`, and a `b` nobody else wrote.
    store
        .append(
            "t",
            0,
            keyed_in_transaction(&[(Some("a"), Some("lost")), (Some("b"), Some("lost"))], 7, 0),
        )
        .unwrap(); // 1, 2
    store.end_transaction("t", 0, 7, 0, Marker::Abort, 0).unwrap(); // 3
    // Committed, twice: the second `c` replaces the first.
    store
        .append("t", 0, keyed_in_transaction(&[(Some("c"), Some("1"))], 8, 0))
        .unwrap(); // 4
    store.end_transaction("t", 0, 8, 0, Marker::Commit, 0).unwrap(); // 5
    store
        .append("t", 0, keyed_in_transaction(&[(Some("c"), Some("2"))], 8, 1))
        .unwrap(); // 6
    store.end_transaction("t", 0, 8, 0, Marker::Commit, 0).unwrap(); // 7
    // Still open: a newer `c` that may yet be aborted. Then records outside any transaction.
    store
        .append("t", 0, keyed_in_transaction(&[(Some("c"), Some("maybe"))], 9, 0))
        .unwrap(); // 8
    store
        .append("t", 0, keyed(&[(Some("d"), Some("1"))], 1_000, 0))
        .unwrap(); // 9
    store
        .append("t", 0, keyed(&[(Some("e"), Some("1"))], 1_000, 0))
        .unwrap(); // 10
    store
        .append("t", 0, keyed(&[(Some("f"), Some("1"))], 1_000, 0))
        .unwrap(); // 11: never cleaned
    let before = store.offsets("t", 0).unwrap();
    assert_eq!((before.high_watermark, before.last_stable), (12, 8));

    // (Markers carry the time they were written at: the pass runs after that.)
    let now = crate::now_ms() + 10_000;
    assert_eq!(store.compact(now).unwrap(), 3, "the aborted two, and the first c");
    // The aborted `a` replaced nothing: the committed one is still there. The open `c` replaced nothing either,
    // and stays.
    assert_eq!(
        consumed(store, "t", 0),
        [
            (0, "a".to_owned(), true),
            (6, "c".into(), true),
            (8, "c".into(), true),
            (9, "d".into(), true),
            (10, "e".into(), true),
            (11, "f".into(), true),
        ]
    );
    // What a consumer of committed data is told is unchanged: where the open transaction starts, and the
    // aborted one to skip (its marker is still there; its records are not).
    let after = store.offsets("t", 0).unwrap();
    assert_eq!((after.high_watermark, after.last_stable), (12, 8));
    assert_eq!(store.aborted_transactions("t", 0, 0, 100).unwrap().len(), 1);
    // The open one is aborted after all: its record goes with the next pass, and nothing else changes.
    store.end_transaction("t", 0, 9, 0, Marker::Abort, 0).unwrap(); // 12
    store
        .append("t", 0, keyed(&[(Some("g"), Some("1"))], 1_000, 0))
        .unwrap(); // 13: never cleaned
    assert_eq!(store.compact(now + 10_000).unwrap(), 1);
    assert_eq!(
        consumed(store, "t", 0).iter().map(|r| r.0).collect::<Vec<_>>(),
        [0, 6, 9, 10, 11, 13]
    );
}

#[test]
fn compaction_removes_aborted_records_and_leaves_open_transactions_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let small = FileStoreConfig {
        segment_bytes: 1,
        ..FileStoreConfig::default()
    };
    {
        let store = FileStore::open(dir.path(), small).unwrap();
        store.create_topic("t", topic_config(1)).unwrap();
        compaction_minds_transactions(&store);
    }
    // Read back from its segments: the same records, and nothing left open.
    let store = FileStore::open(dir.path(), small).unwrap();
    assert_eq!(
        consumed(&store, "t", 0).iter().map(|r| r.0).collect::<Vec<_>>(),
        [0, 6, 9, 10, 11, 13]
    );
    let offsets = store.offsets("t", 0).unwrap();
    assert_eq!((offsets.high_watermark, offsets.last_stable), (14, 14));
}
