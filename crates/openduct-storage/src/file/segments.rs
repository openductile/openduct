// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One partition's segment files (see the store's own documentation for the layout and the recovery rules):
//! written in frames, recovered on open, read back, and removed by retention.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;

use super::commit::Commits;
use super::{FRAME_HEADER, FileStoreConfig, FsyncPolicy, MAX_BATCH_BYTES, sync_dir};
use crate::batch::{BatchHeader, HEADER_LEN, marker_of, parse_header, stored_form};
use crate::error::StoreError;
use crate::partition::{Admission, Location, PartitionIndex};
use crate::store::{Appended, PartitionOffsets, Pending, ReadResult};

#[derive(Debug)]
pub(super) struct Segment {
    pub(super) path: PathBuf,
    pub(super) size: u64,
    pub(super) max_timestamp: i64,
    /// The timestamp of its first batch: what `segment.ms` counts from.
    pub(super) first_timestamp: Option<i64>,
}

#[derive(Debug)]
pub(super) struct ActiveSegment {
    pub(super) base: i64,
    /// Shared with the partition's group commit, which flushes it.
    pub(super) file: Arc<File>,
    pub(super) size: u64,
}

#[derive(Debug)]
pub(super) struct FilePartition {
    pub(super) dir: PathBuf,
    pub(super) index: PartitionIndex,
    pub(super) segments: BTreeMap<i64, Segment>,
    pub(super) active: Option<ActiveSegment>,
    pub(super) readers: HashMap<i64, File>,
    /// What is on the device, and one flush for every append waiting (group commit).
    pub(super) commits: Arc<Commits>,
    /// `segment.ms`, where this partition's owner applies it on every append (a single broker's store does; a
    /// cluster's partition is told to roll by its leader instead, so every copy rolls at the same batch).
    pub(super) roll_ms: Option<i64>,
}

pub(super) fn segment_name(base: i64) -> String {
    format!("{base:020}.log")
}

pub(super) fn parse_segment_name(name: &str) -> Option<i64> {
    let digits = name.strip_suffix(".log")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[cfg(unix)]
pub(super) fn read_at(file: &File, buf: &mut [u8], position: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, position)
}

#[cfg(windows)]
pub(super) fn read_at(file: &File, mut buf: &mut [u8], mut position: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_read(buf, position)?;
        if n == 0 {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }
        buf = &mut buf[n..];
        position += n as u64;
    }
    Ok(())
}

/// Outcome of reading one frame during recovery.
pub(super) enum Frame {
    Ok {
        batch: Bytes,
        len: u64,
    },
    End,
    /// The frame here is damaged. `torn` when it is the file's unfinished last write (it runs into the end of the
    /// file), and not acknowledged data damaged in place.
    Damaged {
        torn: bool,
        why: String,
    },
}

/// Read the frame at the reader's position, `remaining` bytes before the end of the file.
pub(super) fn read_frame(reader: &mut impl Read, remaining: u64) -> Frame {
    if remaining == 0 {
        return Frame::End;
    }
    if remaining < FRAME_HEADER as u64 {
        return Frame::Damaged {
            torn: true,
            why: format!("{remaining} stray bytes"),
        };
    }
    let mut head = [0u8; FRAME_HEADER];
    if let Err(e) = read_full(reader, &mut head).and_then(|n| {
        if n == FRAME_HEADER {
            Ok(())
        } else {
            Err(std::io::Error::from(ErrorKind::UnexpectedEof))
        }
    }) {
        return Frame::Damaged {
            torn: true,
            why: e.to_string(),
        };
    }
    let [l0, l1, l2, l3, c0, c1, c2, c3] = head;
    let len = u32::from_be_bytes([l0, l1, l2, l3]) as usize;
    let crc = u32::from_be_bytes([c0, c1, c2, c3]);
    let body_room = remaining - FRAME_HEADER as u64;
    if !(HEADER_LEN..=MAX_BATCH_BYTES).contains(&len) {
        // A nonsense length in a tail too short to hold any batch is an unfinished write; with room for one after
        // it, it is damage in place.
        return Frame::Damaged {
            torn: body_room < HEADER_LEN as u64,
            why: format!("a frame length of {len}"),
        };
    }
    if len as u64 > body_room {
        return Frame::Damaged {
            torn: true,
            why: format!("a frame of {len} bytes with {body_room} left in the file"),
        };
    }
    let mut batch = vec![0u8; len];
    if let Err(e) = read_full(reader, &mut batch).and_then(|n| {
        if n == len {
            Ok(())
        } else {
            Err(std::io::Error::from(ErrorKind::UnexpectedEof))
        }
    }) {
        return Frame::Damaged {
            torn: true,
            why: e.to_string(),
        };
    }
    if crc32c::crc32c(&batch) != crc {
        return Frame::Damaged {
            // The very last frame of the file whose bytes never all reached the device; anything after it means the
            // damage is in place.
            torn: len as u64 == body_room,
            why: "a frame checksum mismatch".to_owned(),
        };
    }
    Frame::Ok {
        batch: Bytes::from(batch),
        len: (FRAME_HEADER + len) as u64,
    }
}

/// Read until `buf` is full or the reader ends; returns how many bytes were read.
pub(super) fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let rest = buf.get_mut(filled..).unwrap_or_default();
        match reader.read(rest) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

impl FilePartition {
    /// Open a partition directory, reading every segment back and checking it (see the module docs).
    pub(super) fn open(dir: PathBuf) -> Result<Self, StoreError> {
        fs::create_dir_all(&dir)?;
        let mut bases: Vec<(i64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(base) = parse_segment_name(name) {
                bases.push((base, entry.path()));
            }
        }
        bases.sort_by_key(|(b, _)| *b);
        let mut index = PartitionIndex::starting_at(bases.first().map(|(b, _)| *b).unwrap_or(0));
        let mut segments = BTreeMap::new();
        let count = bases.len();
        for (i, (base, path)) in bases.into_iter().enumerate() {
            let newest = i + 1 == count;
            if base != index.next_offset {
                return Err(StoreError::Corrupt(format!(
                    "{} starts at offset {base}, but the log before it ends at {}",
                    path.display(),
                    index.next_offset
                )));
            }
            let file = File::open(&path)?;
            let file_len = file.metadata()?.len();
            let mut reader = std::io::BufReader::new(file);
            let mut position = 0u64;
            let mut max_timestamp = i64::MIN;
            let mut first_timestamp = None;
            loop {
                match read_frame(&mut reader, file_len.saturating_sub(position)) {
                    Frame::End => break,
                    Frame::Ok { batch, len } => {
                        let header = crate::batch::parse_stored_header(&batch).map_err(|e| {
                            StoreError::Corrupt(format!("{} at byte {position}: {e}", path.display()))
                        })?;
                        if header.base_offset != index.next_offset {
                            return Err(StoreError::Corrupt(format!(
                                "{} at byte {position}: a batch at offset {} where {} was expected",
                                path.display(),
                                header.base_offset,
                                index.next_offset
                            )));
                        }
                        max_timestamp = max_timestamp.max(header.max_timestamp);
                        first_timestamp.get_or_insert(header.max_timestamp);
                        index.record(
                            &header,
                            header.base_offset,
                            batch.len(),
                            Location::File {
                                segment: base,
                                position,
                            },
                        );
                        index.note_transaction(&header, header.base_offset, marker_of(&batch));
                        position += len;
                    }
                    Frame::Damaged { torn, why } => {
                        if !newest || !torn {
                            return Err(StoreError::Corrupt(format!(
                                "{} at byte {position}: {why} - acknowledged data is damaged; refusing to serve a \
                                 log with a hole in it",
                                path.display()
                            )));
                        }
                        // An unfinished final write, never acknowledged: cut it off.
                        tracing::warn!(
                            segment = %path.display(),
                            at = position,
                            dropped = file_len.saturating_sub(position),
                            "recovery: removing an unfinished write at the end of the log ({why})"
                        );
                        let f = OpenOptions::new().write(true).open(&path)?;
                        f.set_len(position)?;
                        f.sync_all()?;
                        break;
                    }
                }
            }
            segments.insert(
                base,
                Segment {
                    path,
                    size: position,
                    max_timestamp,
                    first_timestamp,
                },
            );
        }
        let active = match segments.iter().next_back() {
            Some((base, seg)) => Some(ActiveSegment {
                base: *base,
                file: Arc::new(OpenOptions::new().append(true).open(&seg.path)?),
                size: seg.size,
            }),
            None => None,
        };
        // What recovery read back is on the device: all of it readable.
        let commits = Arc::new(Commits::new(
            index.next_offset,
            active.as_ref().map(|a| Arc::clone(&a.file)),
        ));
        // What compaction did here before (file/cleaner.rs).
        index.cleaning = super::cleaner::load(&dir);
        Ok(Self {
            dir,
            index,
            segments,
            active,
            readers: HashMap::new(),
            commits,
            roll_ms: None,
        })
    }

    /// Whether the segment being written to already spans `segment_ms` for a batch of this timestamp: it has
    /// records, and the first of them is that much older.
    pub(super) fn roll_due(&self, segment_ms: i64, timestamp: i64) -> bool {
        self.active
            .as_ref()
            .filter(|a| a.size > 0)
            .and_then(|a| self.segments.get(&a.base))
            .and_then(|seg| seg.first_timestamp)
            .is_some_and(|first| timestamp.saturating_sub(first) >= segment_ms)
    }

    /// Closes the segment being written to and starts the next, unless it is empty (or there is none yet).
    pub(super) fn roll(&mut self) -> Result<(), StoreError> {
        if self.active.as_ref().is_some_and(|a| a.size > 0) {
            self.start_segment()?;
        }
        Ok(())
    }

    /// Starts a segment when there is none, the current one is full, or (`roll_ms`) it spans too long for a
    /// batch of this timestamp.
    pub(super) fn roll_if_needed(
        &mut self,
        config: &FileStoreConfig,
        timestamp: i64,
    ) -> Result<(), StoreError> {
        let full = self
            .active
            .as_ref()
            .is_some_and(|a| a.size >= config.segment_bytes);
        let spans = self.roll_ms.is_some_and(|ms| self.roll_due(ms, timestamp));
        if self.active.is_some() && !full && !spans {
            return Ok(());
        }
        self.start_segment()
    }

    fn start_segment(&mut self) -> Result<(), StoreError> {
        let base = self.index.next_offset;
        let path = self.dir.join(segment_name(base));
        let file = Arc::new(OpenOptions::new().create_new(true).append(true).open(&path)?);
        sync_dir(&self.dir)?;
        // What was written to the old segment is flushed before anything goes to the new one.
        self.commits.rolled(Arc::clone(&file))?;
        self.segments.insert(
            base,
            Segment {
                path,
                size: 0,
                max_timestamp: i64::MIN,
                first_timestamp: None,
            },
        );
        self.active = Some(ActiveSegment { base, file, size: 0 });
        Ok(())
    }

    /// Write a batch; with `FsyncPolicy::Always` also the ticket to wait for before it is acknowledged.
    pub(super) fn append(
        &mut self,
        batch: &Bytes,
        config: &FileStoreConfig,
        leader_epoch: i32,
    ) -> Result<(Appended, Option<u64>), StoreError> {
        self.commits.check()?;
        let header = parse_header(batch)?;
        if batch.len() > MAX_BATCH_BYTES {
            return Err(StoreError::Batch(crate::batch::BatchError::InvalidLength(
                i64::try_from(batch.len()).unwrap_or(i64::MAX),
            )));
        }
        // A marker is its coordinator's: not a batch of the producer's sequence, and never a retry.
        if !header.is_control()
            && let Admission::Duplicate(base_offset) = self.index.admit(&header)?
        {
            // A retry of a batch that may still be on its way to the device: answered once it is there.
            let ticket = (config.fsync == FsyncPolicy::Always).then(|| self.commits.current());
            return Ok((
                Appended {
                    base_offset,
                    log_start_offset: self.index.log_start,
                    duplicate: true,
                },
                ticket,
            ));
        }
        self.roll_if_needed(config, header.max_timestamp)?;
        let base_offset = self.index.next_offset;
        let stored = stored_form(batch, base_offset, leader_epoch);
        let header = BatchHeader {
            partition_leader_epoch: leader_epoch,
            ..header
        };
        let len = u32::try_from(stored.len())
            .map_err(|_| StoreError::Corrupt("a batch too large for a frame".to_owned()))?;
        let mut frame = Vec::with_capacity(FRAME_HEADER + stored.len());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&crc32c::crc32c(&stored).to_be_bytes());
        frame.extend_from_slice(&stored);
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| StoreError::Corrupt("no active segment after rolling".to_owned()))?;
        let position = active.size;
        // Written here, flushed by the group commit (FileStore::append waits for it, without this lock).
        if let Err(e) = (&*active.file).write_all(&frame) {
            // Leave no partial frame behind: the log must stay readable whatever happened mid-write.
            let _ = active.file.set_len(position);
            return Err(StoreError::Io(e));
        }
        active.size += frame.len() as u64;
        let segment_base = active.base;
        if let Some(seg) = self.segments.get_mut(&segment_base) {
            seg.size = active.size;
            seg.max_timestamp = seg.max_timestamp.max(header.max_timestamp);
            seg.first_timestamp.get_or_insert(header.max_timestamp);
        }
        self.index.record(
            &header,
            base_offset,
            stored.len(),
            Location::File {
                segment: segment_base,
                position,
            },
        );
        self.index
            .note_transaction(&header, base_offset, marker_of(&stored));
        let next = self.index.next_offset;
        let ticket = match config.fsync {
            FsyncPolicy::Always => Some(self.commits.wrote(next)),
            FsyncPolicy::Never => {
                self.commits.visible(next);
                None
            }
        };
        Ok((
            Appended {
                base_offset,
                log_start_offset: self.index.log_start,
                duplicate: false,
            },
            ticket,
        ))
    }

    pub(super) fn read_entry(
        &mut self,
        segment: i64,
        position: u64,
        size: usize,
    ) -> Result<Bytes, StoreError> {
        if !self.readers.contains_key(&segment) {
            let path = self
                .segments
                .get(&segment)
                .map(|s| s.path.clone())
                .ok_or_else(|| StoreError::Corrupt(format!("segment {segment} is missing")))?;
            self.readers.insert(segment, File::open(path)?);
        }
        let file = self
            .readers
            .get(&segment)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {segment} is missing")))?;
        let mut frame = vec![0u8; FRAME_HEADER + size];
        read_at(file, &mut frame, position)?;
        let (head, batch) = frame.split_at(FRAME_HEADER);
        let crc = head
            .get(4..8)
            .and_then(|c| <[u8; 4]>::try_from(c).ok())
            .map(u32::from_be_bytes)
            .unwrap_or(0);
        // Checked on every read: a bit flipped on the device is reported, never served.
        if crc32c::crc32c(batch) != crc {
            return Err(StoreError::Corrupt(format!(
                "segment {segment} at byte {position}: checksum mismatch on read"
            )));
        }
        Ok(Bytes::copy_from_slice(batch))
    }

    /// Writes a batch, stored in `leader_epoch`; answered once it is as durable as the store promises.
    pub(super) fn begin(
        &mut self,
        batch: &Bytes,
        config: &FileStoreConfig,
        leader_epoch: i32,
    ) -> Result<Pending, StoreError> {
        let (appended, ticket) = self.append(batch, config, leader_epoch)?;
        let commits = Arc::clone(&self.commits);
        // On the device before it is acknowledged - flushed together with every append waiting with it.
        Ok(match ticket {
            Some(ticket) => Pending::later(move || commits.wait(ticket).map(|()| appended)),
            None => Pending::ready(appended),
        })
    }

    pub(super) fn read(&mut self, offset: i64, max_bytes: usize) -> Result<ReadResult, StoreError> {
        // Only what is on the device is read: an append still waiting for its flush is not there yet.
        let durable = self.commits.durable_next();
        let selected = self.index.select_until(offset, max_bytes, durable)?;
        let mut batches = Vec::with_capacity(selected.len());
        for e in selected {
            if let Location::File { segment, position } = e.location {
                batches.push(self.read_entry(segment, position, e.size)?);
            }
        }
        Ok(ReadResult {
            batches,
            offsets: PartitionOffsets {
                log_start: self.index.log_start,
                high_watermark: durable,
                last_stable: self.index.last_stable(durable),
            },
        })
    }

    pub(super) fn offsets(&self) -> PartitionOffsets {
        let durable = self.commits.durable_next();
        PartitionOffsets {
            log_start: self.index.log_start,
            high_watermark: durable,
            last_stable: self.index.last_stable(durable),
        }
    }

    pub(super) fn offset_for_timestamp(&self, timestamp: i64) -> Option<(i64, i64)> {
        self.index
            .offset_for_timestamp_until(timestamp, self.commits.durable_next())
    }

    pub(super) fn max_timestamp(&self) -> Option<(i64, i64)> {
        self.index.max_timestamp_until(self.commits.durable_next())
    }

    pub(super) fn epoch_of(&self, offset: i64, current: i32) -> i32 {
        self.index.epoch_of(offset, current, self.commits.durable_next())
    }

    pub(super) fn epoch_end(&self, epoch: i32, current: i32) -> Option<i64> {
        self.index.epoch_end(epoch, current, self.commits.durable_next())
    }

    /// Where the log would start after a DeleteRecords to `offset` (`-1`: the high watermark), checked: not past the
    /// high watermark, and never back before the start it has.
    pub(super) fn start_after_delete(&self, offset: i64) -> Result<i64, StoreError> {
        let end = self.commits.durable_next();
        let target = if offset == -1 { end } else { offset };
        if !(0..=end).contains(&target) {
            return Err(StoreError::OffsetOutOfRange {
                requested: offset,
                log_start: self.index.log_start,
                high_watermark: end,
            });
        }
        Ok(target.max(self.index.log_start))
    }

    /// Moves the log's start up to `start` (from [`Self::start_after_delete`], or as recorded): offsets before it are
    /// no longer read, and segments holding only offsets before it are removed (never the one being written).
    pub(super) fn move_start(&mut self, start: i64) -> Result<(), StoreError> {
        let start = start.min(self.index.next_offset);
        if start <= self.index.log_start {
            return Ok(());
        }
        self.index.truncate_before(start);
        let active = self.active.as_ref().map(|a| a.base);
        let bases: Vec<i64> = self.segments.keys().copied().collect();
        let mut removed = false;
        for (i, base) in bases.iter().enumerate() {
            // A segment holds the offsets from its base to the next one's.
            let next = bases.get(i + 1).copied().unwrap_or(self.index.next_offset);
            if Some(*base) == active || next > start {
                break;
            }
            if let Some(seg) = self.segments.remove(base) {
                self.readers.remove(base);
                fs::remove_file(&seg.path)?;
                removed = true;
            }
        }
        if removed {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    pub(super) fn enforce_retention(&mut self, cutoff: i64) -> Result<usize, StoreError> {
        let active = self.active.as_ref().map(|a| a.base);
        let expired: Vec<i64> = self
            .segments
            .iter()
            .take_while(|(base, seg)| Some(**base) != active && seg.max_timestamp < cutoff)
            .map(|(base, _)| *base)
            .collect();
        self.remove_segments(expired)
    }

    /// `retention.bytes`: the oldest closed segments go while what is left would still hold at least `keep`
    /// bytes - Kafka's rule. The segment being written to is never removed, so a partition can sit above its
    /// limit by up to one segment. How many batches went.
    pub(super) fn enforce_size(&mut self, keep: u64) -> Result<usize, StoreError> {
        let active = self.active.as_ref().map(|a| a.base);
        let mut total: u64 = self.segments.values().map(|s| s.size).sum();
        let mut oversize = Vec::new();
        for (base, seg) in &self.segments {
            if Some(*base) == active || total.saturating_sub(seg.size) < keep {
                break;
            }
            total -= seg.size;
            oversize.push(*base);
        }
        self.remove_segments(oversize)
    }

    /// Removes these segments (the oldest ones, in order) and moves the log's start past them.
    fn remove_segments(&mut self, bases: Vec<i64>) -> Result<usize, StoreError> {
        let mut removed = 0usize;
        for base in bases {
            if let Some(seg) = self.segments.remove(&base) {
                self.readers.remove(&base);
                fs::remove_file(&seg.path)?;
                let next = self
                    .segments
                    .keys()
                    .next()
                    .copied()
                    .unwrap_or(self.index.next_offset);
                let before = self.index.entries.len();
                self.index.truncate_before(next);
                removed += before - self.index.entries.len();
            }
        }
        if removed > 0 {
            sync_dir(&self.dir)?;
        }
        Ok(removed)
    }
}
