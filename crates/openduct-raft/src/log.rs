// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The Raft log on this broker's disk.
//!
//! Entries go into segment files (`<index of the first entry, 20 digits>.raft`), each entry one frame: its length (4
//! bytes), a CRC-32C of the rest (4 bytes), and the entry encoded. An append writes its frames at once and is
//! readable at once; the broker's flusher (`flush`) then flushes them to the device and only then tells Raft they are
//! safe - one flush for every append waiting at that moment, so a busy broker does not flush once per entry.
//!
//! Three small files sit beside the segments, each replaced by a rename:
//! - `vote`: flushed before the vote is answered; a broker never forgets whom it voted for.
//! - `purged`: the last entry removed after a snapshot, flushed before any segment it frees is deleted, so the log
//!   never appears to start later than it does.
//! - `committed`: left to the operating system - after a power cut the newest value may be lost, and the broker then
//!   re-applies a little less on its own and learns the rest from the leader.
//!
//! On open the segments are read back and checked. An unfinished last frame (a write a crash cut short, never
//! reported safe) is cut off; damage anywhere else stops the broker rather than serve a log with a hole.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::{OptionalSend, RaftLogReader};
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::flush::{Flush, Flusher};
use crate::stable::{self, Versioned};
use crate::types::{Entry, LogId, TypeConfig, Vote};

mod space;

const FRAME_HEADER: usize = 8;
/// The largest entry a frame may hold: a batch at the broker's limit, with room for the command around it.
const MAX_ENTRY_BYTES: usize = openduct_storage::MAX_BATCH_BYTES + 64 * 1024;

#[derive(Debug)]
struct Segment {
    file: Arc<File>,
    path: PathBuf,
    /// Where its entries end.
    size: u64,
    /// Zeros are written and flushed up to here (see `space`).
    prepared: u64,
    preparing: bool,
    /// Takes no more entries: the next go to a new segment.
    sealed: bool,
    /// Bytes written since `window` began, and whether the last full window was busy (see `space`).
    window: (std::time::Instant, u64),
    busy: bool,
}

impl Segment {
    fn new(file: Arc<File>, path: PathBuf, size: u64, prepared: u64, sealed: bool) -> Self {
        Self {
            file,
            path,
            size,
            prepared,
            preparing: false,
            sealed,
            window: (std::time::Instant::now(), 0),
            busy: false,
        }
    }

    fn wrote(&mut self, bytes: u64) {
        self.window.1 += bytes;
        let elapsed = self.window.0.elapsed();
        if elapsed >= std::time::Duration::from_secs(1) {
            self.busy = self.window.1 as f64 / elapsed.as_secs_f64() > space::BUSY_BYTES_PER_SEC as f64;
            self.window = (std::time::Instant::now(), 0);
        }
    }

    fn wants_space(&self) -> bool {
        !self.sealed
            && !self.preparing
            && !self.busy
            && self.prepared < self.size + space::ahead_for(self.size)
    }
}

/// Where one entry is.
#[derive(Debug, Clone)]
struct Slot {
    log_id: LogId,
    segment: u64,
    position: u64,
    len: usize,
}

#[derive(Debug, Default)]
struct Log {
    segments: BTreeMap<u64, Segment>,
    /// The entries present, in order; their indexes are consecutive.
    slots: VecDeque<Slot>,
    purged: Option<LogId>,
    vote: Option<Vote>,
    committed: Option<LogId>,
}

impl Log {
    fn last_log_id(&self) -> Option<LogId> {
        self.slots.back().map(|s| s.log_id).or(self.purged)
    }

    fn first_index(&self) -> Option<u64> {
        self.slots.front().map(|s| s.log_id.index)
    }

    fn next_index(&self) -> Option<u64> {
        match (self.slots.back(), &self.purged) {
            (Some(s), _) => Some(s.log_id.index + 1),
            (None, Some(p)) => Some(p.index + 1),
            (None, None) => None,
        }
    }
}

#[derive(Debug)]
struct Shared {
    dir: PathBuf,
    segment_bytes: u64,
    log: RwLock<Log>,
    flusher: Arc<Flusher>,
    /// Set once a flush failed: the device can no longer be trusted with what was written, so nothing more is.
    failed: Arc<Mutex<Option<String>>>,
    /// How far this log may be purged (snapshot.rs `Floor`): asked before every purge. `None` only for a log
    /// opened on its own (the storage tests).
    floor: Mutex<Option<Arc<crate::snapshot::Floor>>>,
}

/// The log store: given to Raft, and cloned as the log reader its replication uses.
#[derive(Debug, Clone)]
pub struct LogStore {
    shared: Arc<Shared>,
}

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn segment_name(first: u64) -> String {
    format!("{first:020}.raft")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".raft")?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn write_file(dir: &Path, name: &str, contents: &[u8], flush: bool) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(contents)?;
        if flush {
            f.sync_all()?;
        }
    }
    fs::rename(&tmp, dir.join(name))?;
    if flush {
        sync_dir(dir)?;
    }
    Ok(())
}

/// A small file's value, in OpenDuct's own shape and with its format checked.
fn read_value<T: DeserializeOwned>(dir: &Path, name: &str) -> io::Result<Option<T>> {
    let path = dir.join(name);
    match fs::read(&path) {
        Ok(raw) => {
            let versioned: Versioned<T> =
                serde_json::from_slice(&raw).map_err(|e| other(format!("{}: {e}", path.display())))?;
            versioned
                .open(&path.display().to_string())
                .map(Some)
                .map_err(other)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn json<T: Serialize + Clone>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(&Versioned::current(value.clone())).map_err(|e| other(format!("cannot encode: {e}")))
}

/// A frame's body: the format byte, then the entry in OpenDuct's own shape.
pub(crate) fn encode_entry(entry: &Entry) -> io::Result<Vec<u8>> {
    let mut body = vec![stable::FORMAT];
    postcard::to_io(&stable::Entry::from(entry), &mut body)
        .map_err(|e| other(format!("cannot encode an entry: {e}")))?;
    Ok(body)
}

fn frame(entry: &Entry) -> io::Result<Vec<u8>> {
    let body = encode_entry(entry)?;
    let len = u32::try_from(body.len()).map_err(|_| other("an entry too large for a frame"))?;
    let mut out = Vec::with_capacity(FRAME_HEADER + body.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&crc32c::crc32c(&body).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub(crate) fn decode(body: &[u8]) -> io::Result<Entry> {
    let (format, rest) = body.split_first().ok_or_else(|| other("an empty entry"))?;
    if *format != stable::FORMAT {
        return Err(other(format!(
            "an entry of format {format}; this build reads format {}: it was written by a newer OpenDuct",
            stable::FORMAT
        )));
    }
    let entry: stable::Entry =
        postcard::from_bytes(rest).map_err(|e| other(format!("cannot decode an entry: {e}")))?;
    entry.into_raft().map_err(other)
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], position: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, position)
}

#[cfg(windows)]
fn read_at(file: &File, mut buf: &mut [u8], mut position: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let n = std::os::windows::fs::FileExt::seek_read(file, buf, position)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = &mut buf[n..];
        position += n as u64;
    }
    Ok(())
}

/// What reading the next frame of a segment found.
enum Next {
    End,
    Entry(Entry, u64),
    /// Damage: `torn` when the frame runs past the end of the file, as a write cut short does.
    Damaged {
        torn: bool,
        why: String,
    },
}

fn next_frame(reader: &mut impl Read, remaining: u64) -> Next {
    if remaining == 0 {
        return Next::End;
    }
    if remaining < FRAME_HEADER as u64 {
        return Next::Damaged {
            torn: true,
            why: "a frame header cut short".to_owned(),
        };
    }
    let mut head = [0u8; FRAME_HEADER];
    if let Err(e) = reader.read_exact(&mut head) {
        return Next::Damaged {
            torn: true,
            why: e.to_string(),
        };
    }
    if head == [0u8; FRAME_HEADER] {
        // Prepared space: the entries end here.
        return Next::End;
    }
    let [l0, l1, l2, l3, c0, c1, c2, c3] = head;
    let len = u32::from_be_bytes([l0, l1, l2, l3]) as usize;
    let crc = u32::from_be_bytes([c0, c1, c2, c3]);
    if len > MAX_ENTRY_BYTES {
        return Next::Damaged {
            torn: false,
            why: format!("a frame of {len} bytes"),
        };
    }
    if (len + FRAME_HEADER) as u64 > remaining {
        return Next::Damaged {
            torn: true,
            why: "a frame cut short".to_owned(),
        };
    }
    let mut body = vec![0u8; len];
    if let Err(e) = reader.read_exact(&mut body) {
        return Next::Damaged {
            torn: true,
            why: e.to_string(),
        };
    }
    if crc32c::crc32c(&body) != crc {
        // A final frame with a bad checksum is also what an interrupted write can leave.
        return Next::Damaged {
            torn: (len + FRAME_HEADER) as u64 == remaining,
            why: "a checksum mismatch".to_owned(),
        };
    }
    match decode(&body) {
        Ok(entry) => Next::Entry(entry, (len + FRAME_HEADER) as u64),
        Err(e) => Next::Damaged {
            torn: false,
            why: e.to_string(),
        },
    }
}

impl LogStore {
    /// Opens (or creates) the log in `dir`, reading every segment back. `segment_bytes`: when a new segment starts;
    /// `flusher`: the broker's, which flushes every log's appends.
    pub fn open(dir: impl Into<PathBuf>, segment_bytes: u64, flusher: Arc<Flusher>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut log = Log {
            vote: read_value::<stable::Vote>(&dir, "vote")?.map(Into::into),
            committed: read_value::<Option<stable::LogId>>(&dir, "committed")?
                .flatten()
                .map(Into::into),
            purged: read_value::<stable::LogId>(&dir, "purged")?.map(Into::into),
            ..Log::default()
        };
        let mut firsts: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if let Some(first) = entry.file_name().to_str().and_then(parse_segment_name) {
                firsts.push((first, entry.path()));
            }
        }
        firsts.sort_by_key(|(f, _)| *f);
        let count = firsts.len();
        for (i, (first, path)) in firsts.into_iter().enumerate() {
            let newest = i + 1 == count;
            let size = Self::read_segment(&mut log, first, &path, newest)?;
            let file = Arc::new(OpenOptions::new().read(true).write(true).open(&path)?);
            if newest && file.metadata()?.len() > size {
                // Space prepared before a restart, cut back: only a segment written by this process has any.
                file.set_len(size)?;
                file.sync_all()?;
            }
            // Entries written after a restart start a new segment.
            log.segments
                .insert(first, Segment::new(file, path, size, size, true));
        }
        // Segments holding only purged entries (a crash came between the purge and their removal): removed now.
        Self::remove_purged_segments(&mut log)?;
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                segment_bytes,
                log: RwLock::new(log),
                flusher,
                failed: Arc::new(Mutex::new(None)),
                floor: Mutex::new(None),
            }),
        })
    }

    /// Ties this log to its machine's snapshots: from here it is never purged past one that is on disk.
    pub fn purge_no_further_than(&self, floor: Arc<crate::snapshot::Floor>) {
        *self.shared.floor.lock() = Some(floor);
    }

    /// Reads one segment into the log's slots; returns its size once checked (and cut, if its end was torn).
    fn read_segment(log: &mut Log, first: u64, path: &Path, newest: bool) -> io::Result<u64> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut position = 0u64;
        loop {
            match next_frame(&mut reader, len - position) {
                Next::End => break,
                Next::Entry(entry, size) => {
                    let log_id = entry.log_id;
                    if log.purged.as_ref().is_none_or(|p| log_id.index > p.index) {
                        if let Some(next) = log.next_index().filter(|n| *n != log_id.index) {
                            return Err(other(format!(
                                "{} at byte {position}: entry {} where {next} was expected",
                                path.display(),
                                log_id.index
                            )));
                        }
                        log.slots.push_back(Slot {
                            log_id,
                            segment: first,
                            position,
                            len: usize::try_from(size).unwrap_or(usize::MAX),
                        });
                    }
                    position += size;
                }
                Next::Damaged { torn, why } => {
                    // A write cut short in prepared space leaves whole sectors of zeros in or right after it.
                    let torn = torn || (newest && Self::zeros_near(path, position, len)?);
                    if !(newest && torn) {
                        return Err(other(format!(
                            "{} at byte {position}: {why} - the replication log is damaged",
                            path.display()
                        )));
                    }
                    tracing::warn!(segment = %path.display(), at = position, "recovery: cutting off an unfinished write ({why})");
                    let f = OpenOptions::new().write(true).open(path)?;
                    f.set_len(position)?;
                    f.sync_all()?;
                    break;
                }
            }
        }
        Ok(position)
    }

    /// Whether a whole sector of zeros lies within an entry's reach of `position` - what an interrupted write into
    /// prepared space leaves.
    fn zeros_near(path: &Path, position: u64, len: u64) -> io::Result<bool> {
        let reach = (MAX_ENTRY_BYTES + FRAME_HEADER) as u64 + 4096;
        let n = usize::try_from(len.saturating_sub(position).min(reach)).unwrap_or(0);
        let mut bytes = vec![0u8; n];
        read_at(&File::open(path)?, &mut bytes, position)?;
        Ok(space::has_zero_sector(&bytes, position))
    }

    /// Removes every segment, but the newest, whose entries are all purged.
    fn remove_purged_segments(log: &mut Log) -> io::Result<()> {
        let Some(purged) = log.purged else {
            return Ok(());
        };
        let firsts: Vec<u64> = log.segments.keys().copied().collect();
        let mut removed = false;
        for pair in firsts.windows(2) {
            let [this, next] = pair else { continue };
            // Every entry of `this` comes before `next`'s first one.
            if *next <= purged.index + 1
                && let Some(seg) = log.segments.remove(this)
            {
                fs::remove_file(&seg.path)?;
                removed = true;
            }
        }
        if removed
            && let Some(seg) = log.segments.values().next()
            && let Some(dir) = seg.path.parent()
        {
            sync_dir(dir)?;
        }
        Ok(())
    }

    /// Writes `entry` as the first of an empty log and waits until it is on disk: a new partition's opening entry, given
    /// to its replicas other than the first before their group starts (replica.rs).
    pub(crate) async fn seed(&mut self, entry: Entry) -> io::Result<()> {
        use openraft::type_config::TypeConfigExt;
        let (tx, rx) = TypeConfig::oneshot();
        self.append([entry], IOFlushed::signal(tx)).await?;
        rx.await
            .map_err(|_| other("the opening entry's flush was dropped"))?
    }

    fn check(&self) -> io::Result<()> {
        match &*self.shared.failed.lock() {
            Some(why) => Err(other(format!(
                "the replication log stopped after a failed flush: {why}"
            ))),
            None => Ok(()),
        }
    }

    /// The segment to write the entry `index` into: the newest, or a new one once the newest is full.
    fn active_for(&self, log: &mut Log, index: u64) -> io::Result<u64> {
        if let Some((first, seg)) = log.segments.iter().next_back()
            && seg.size < self.shared.segment_bytes
            && !seg.sealed
        {
            return Ok(*first);
        }
        // A sealed segment left empty (cut back to nothing) holds no entry: it goes, and its name with it - the new
        // segment starts at the same index.
        if let Some((first, seg)) = log.segments.iter().next_back()
            && seg.sealed
            && seg.size == 0
        {
            let (first, path) = (*first, seg.path.clone());
            log.segments.remove(&first);
            fs::remove_file(&path)?;
        }
        if let Some(seg) = log.segments.values_mut().next_back() {
            // Everything in the segment reaches the device before anything goes to the next.
            seg.file.sync_data()?;
            seg.sealed = true;
            if seg.prepared > seg.size {
                // Its unused prepared space goes; it takes nothing more.
                seg.file.set_len(seg.size)?;
                seg.prepared = seg.size;
            }
        }
        let path = self.shared.dir.join(segment_name(index));
        let file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?,
        );
        // The new segment's first stretch prepared now; the rest follows behind the appends.
        space::zero(&file, 0, space::MIN_CHUNK)?;
        file.sync_data()?;
        sync_dir(&self.shared.dir)?;
        log.segments
            .insert(index, Segment::new(file, path, 0, space::MIN_CHUNK, false));
        Ok(index)
    }

    /// Keeps prepared space ahead of the active segment's data: zeros written under the log's lock (so no append
    /// lands there meanwhile), flushed outside it.
    fn prepare(shared: &Shared, first: u64) {
        loop {
            let (file, to) = {
                let mut log = shared.log.write();
                let Some(seg) = log.segments.get_mut(&first) else {
                    return;
                };
                let from = seg.prepared.max(seg.size);
                let chunk = space::chunk_for(seg.size);
                let cap = shared.segment_bytes + chunk;
                if seg.sealed || seg.busy || from >= cap || from >= seg.size + space::ahead_for(seg.size) {
                    seg.preparing = false;
                    return;
                }
                let to = (from + chunk).min(cap);
                if let Err(e) = space::zero(&seg.file, from, to) {
                    tracing::warn!(segment = %seg.path.display(), error = %e, "cannot prepare space ahead");
                    seg.preparing = false;
                    return;
                }
                (Arc::clone(&seg.file), to)
            };
            let synced = file.sync_data();
            let mut log = shared.log.write();
            let Some(seg) = log.segments.get_mut(&first) else {
                return;
            };
            match synced {
                Ok(()) => seg.prepared = seg.prepared.max(to),
                Err(e) => {
                    tracing::warn!(segment = %seg.path.display(), error = %e, "cannot flush prepared space");
                    seg.preparing = false;
                    return;
                }
            }
        }
    }

    /// Writes framed entries after the last one, rolling to a new segment when one fills; the segments written to.
    fn write_entries(&self, log: &mut Log, framed: Vec<(LogId, Vec<u8>)>) -> io::Result<Vec<u64>> {
        let mut touched: Vec<u64> = Vec::new();
        let mut pending = Vec::new();
        let mut segment = None;
        for (log_id, bytes) in framed {
            let room = |log: &Log, s: u64, pending: &Vec<u8>| {
                log.segments
                    .get(&s)
                    .is_some_and(|x| x.size + (pending.len() as u64) < self.shared.segment_bytes)
            };
            let seg = match segment {
                Some(s) if room(log, s, &pending) => s,
                _ => {
                    if let Some(s) = segment {
                        Self::write_pending(log, s, &mut pending)?;
                    }
                    let s = self.active_for(log, log_id.index)?;
                    if !touched.contains(&s) {
                        touched.push(s);
                    }
                    segment = Some(s);
                    s
                }
            };
            let position = log
                .segments
                .get(&seg)
                .map(|s| s.size + pending.len() as u64)
                .unwrap_or(0);
            log.slots.push_back(Slot {
                log_id,
                segment: seg,
                position,
                len: bytes.len(),
            });
            pending.extend_from_slice(&bytes);
        }
        if let Some(s) = segment {
            Self::write_pending(log, s, &mut pending)?;
        }
        Ok(touched)
    }

    fn write_pending(log: &mut Log, segment: u64, pending: &mut Vec<u8>) -> io::Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let seg = log
            .segments
            .get_mut(&segment)
            .ok_or_else(|| other("the segment being written is gone"))?;
        if let Err(e) = space::write_at(&seg.file, pending, seg.size) {
            // Leave no partial frame behind.
            let _ = space::zero(&seg.file, seg.size, seg.size + pending.len() as u64);
            return Err(e);
        }
        seg.size += pending.len() as u64;
        seg.wrote(pending.len() as u64);
        pending.clear();
        Ok(())
    }

    fn entries(&self, start: u64, end: Option<u64>) -> io::Result<Vec<Entry>> {
        let log = self.shared.log.read();
        let Some(first) = log.first_index() else {
            return Ok(Vec::new());
        };
        let from = usize::try_from(start.saturating_sub(first)).unwrap_or(usize::MAX);
        let to = match end {
            Some(end) => usize::try_from(end.saturating_sub(first)).unwrap_or(usize::MAX),
            None => usize::MAX,
        }
        .min(log.slots.len());
        let mut out = Vec::with_capacity(to.saturating_sub(from));
        for slot in log.slots.range(from.min(to)..to) {
            let seg = log
                .segments
                .get(&slot.segment)
                .ok_or_else(|| other(format!("the segment of entry {} is missing", slot.log_id.index)))?;
            let mut buf = vec![0u8; slot.len];
            read_at(&seg.file, &mut buf, slot.position)?;
            let (head, body) = buf.split_at(FRAME_HEADER);
            let crc = head
                .get(4..8)
                .and_then(|c| <[u8; 4]>::try_from(c).ok())
                .map(u32::from_be_bytes)
                .unwrap_or(0);
            if crc32c::crc32c(body) != crc {
                return Err(other(format!(
                    "entry {}: checksum mismatch on read",
                    slot.log_id.index
                )));
            }
            out.push(decode(body)?);
        }
        Ok(out)
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let start = match range.start_bound() {
            Bound::Included(s) => *s,
            Bound::Excluded(s) => s.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => Some(e.saturating_add(1)),
            Bound::Excluded(e) => Some(*e),
            Bound::Unbounded => None,
        };
        self.entries(start, end)
    }

    /// What replication sends: entries from exactly `start`, or none when `start` was removed meanwhile by a
    /// snapshot's purge - replication then finds the follower needs the snapshot instead. Never a run that starts
    /// later than asked, which a follower would take for a gap.
    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> Result<Vec<Entry>, io::Error> {
        let entries = self.entries(start, Some(end))?;
        Ok(match entries.first() {
            Some(first) if first.log_id.index != start => Vec::new(),
            _ => entries,
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.shared.log.read().vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let log = self.shared.log.read();
        Ok(LogState {
            last_purged_log_id: log.purged,
            last_log_id: log.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.check()?;
        write_file(&self.shared.dir, "vote", &json(&stable::Vote::from(*vote))?, true)?;
        self.shared.log.write().vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        write_file(
            &self.shared.dir,
            "committed",
            &json(&committed.map(stable::LogId::from))?,
            false,
        )?;
        self.shared.log.write().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.shared.log.read().committed)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.check()?;
        // Encoded before anything changes: an entry that cannot be encoded leaves the log as it was.
        let framed = entries
            .into_iter()
            .map(|e| frame(&e).map(|bytes| (e.log_id, bytes)))
            .collect::<io::Result<Vec<_>>>()?;
        let mut log = self.shared.log.write();
        let mut next = log.next_index();
        for (log_id, _) in &framed {
            if let Some(n) = next.filter(|n| *n != log_id.index) {
                return Err(other(format!(
                    "entry {} appended where {n} comes next",
                    log_id.index
                )));
            }
            next = Some(log_id.index + 1);
        }
        let touched = match self.write_entries(&mut log, framed) {
            Ok(touched) => touched,
            Err(e) => {
                // Forget the entries whose frames did not reach their file.
                while log.slots.back().is_some_and(|x| {
                    log.segments
                        .get(&x.segment)
                        .is_none_or(|seg| x.position >= seg.size)
                }) {
                    log.slots.pop_back();
                }
                return Err(e);
            }
        };
        let files: Vec<Arc<File>> = touched
            .iter()
            .filter_map(|s| log.segments.get(s).map(|x| Arc::clone(&x.file)))
            .collect();
        let prepare = log.segments.iter_mut().next_back().and_then(|(first, seg)| {
            seg.wants_space().then(|| {
                seg.preparing = true;
                *first
            })
        });
        drop(log);
        if let Some(first) = prepare {
            let shared = Arc::clone(&self.shared);
            tokio::task::spawn_blocking(move || Self::prepare(&shared, first));
        }
        self.shared.flusher.submit(Flush {
            files,
            done: callback,
            failed: Arc::clone(&self.shared.failed),
        })
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        self.check()?;
        let mut log = self.shared.log.write();
        let keep = |index: u64| last_log_id.as_ref().is_some_and(|l| index <= l.index);
        let mut cut = None;
        while let Some(slot) = log.slots.back() {
            if keep(slot.log_id.index) {
                break;
            }
            cut = log.slots.pop_back().map(|s| (s.segment, s.position));
        }
        let Some((segment, position)) = cut else {
            return Ok(());
        };
        let later: Vec<u64> = log.segments.range(segment + 1..).map(|(f, _)| *f).collect();
        for first in later {
            if let Some(seg) = log.segments.remove(&first) {
                fs::remove_file(&seg.path)?;
            }
        }
        if let Some(seg) = log.segments.get_mut(&segment) {
            seg.file.set_len(position)?;
            seg.file.sync_all()?;
            seg.size = position;
            seg.prepared = position;
            // Nothing is written over the cut: the next entries start a new segment.
            seg.sealed = true;
        }
        sync_dir(&self.shared.dir)
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        self.check()?;
        // NEVER PAST A SNAPSHOT THAT IS ON THIS DISK. Raft asks for a purge after a snapshot; this store does not
        // take that on trust. Asked to go further than the newest snapshot read back from disk covers, it purges
        // up to what is covered and keeps the rest - more log than asked for costs disk, less is a copy that
        // cannot be rebuilt.
        let log_id = match self.shared.floor.lock().clone() {
            None => log_id,
            Some(floor) => match floor.covered() {
                Some(covered) if covered >= log_id.index => log_id,
                covered => {
                    let kept_from = covered.map_or(0, |c| c + 1);
                    tracing::warn!(
                        asked = %log_id,
                        covered = ?covered,
                        "asked to purge the log past the newest snapshot on disk: entries from {kept_from} are kept"
                    );
                    let within = covered.and_then(|c| {
                        let log = self.shared.log.read();
                        log.slots
                            .iter()
                            .rev()
                            .find(|s| s.log_id.index <= c)
                            .map(|s| s.log_id)
                    });
                    match within {
                        Some(lesser) => lesser,
                        None => return Ok(()),
                    }
                }
            },
        };
        // On the device first: a crash after a segment is removed must not show the log starting earlier.
        write_file(
            &self.shared.dir,
            "purged",
            &json(&stable::LogId::from(log_id))?,
            true,
        )?;
        let mut log = self.shared.log.write();
        log.purged = Some(log_id);
        while log.slots.front().is_some_and(|s| s.log_id.index <= log_id.index) {
            log.slots.pop_front();
        }
        Self::remove_purged_segments(&mut log)
    }
}
