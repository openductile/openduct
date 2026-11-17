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
