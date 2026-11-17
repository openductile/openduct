// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Space prepared ahead of the active segment's data: zeros written and flushed past its end, so that an append
//! overwrites space the file system has already allocated and recorded, and its flush has no file size or extent
//! change to commit with it. On a cloud volume (gp3 under xfs, measured 2026-11-15) that makes a flush of a small
//! append ~0.4 ms instead of ~2 ms; preallocating with `fallocate` does not help (its extents are "unwritten").
//!
//! Only the active segment, written by this process, has such space: a segment is cut back to its last entry and
//! sealed when the log is opened or truncated, and trimmed when the next one starts. So bytes past a segment's end
//! are always zeros this process wrote - never a stale frame a later recovery could take for a current one.

use std::fs::File;
use std::io;

/// The least prepared at a time: what a new or quiet segment gets, so thousands of idle groups cost little.
pub(super) const MIN_CHUNK: u64 = 64 << 10;
const MAX_CHUNK: u64 = 4 << 20;

/// How much is prepared at a time for a segment holding `size` bytes: more as it fills, since a segment that has
/// taken a lot is a busy one.
pub(super) fn chunk_for(size: u64) -> u64 {
    (size / 8).next_power_of_two().clamp(MIN_CHUNK, MAX_CHUNK)
}

/// Above this many bytes a second into a segment, no space is prepared: appends that big share each flush among
/// many entries already, and zeros written ahead would double what reaches the disk.
pub(super) const BUSY_BYTES_PER_SEC: u64 = 4 << 20;

/// How far ahead of the data the prepared space is kept.
pub(super) fn ahead_for(size: u64) -> u64 {
    2 * chunk_for(size)
}
/// The unit a device writes whole: a torn write leaves whole sectors unwritten (zeros, here).
const SECTOR: usize = 512;

#[cfg(unix)]
pub(super) fn write_at(file: &File, buf: &[u8], position: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, position)
}

#[cfg(windows)]
pub(super) fn write_at(file: &File, mut buf: &[u8], mut position: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let n = std::os::windows::fs::FileExt::seek_write(file, buf, position)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
        position += n as u64;
    }
    Ok(())
}

/// Writes zeros over `[from, to)`.
pub(super) fn zero(file: &File, from: u64, to: u64) -> io::Result<()> {
    let block = vec![0u8; usize::try_from(MAX_CHUNK.min(to.saturating_sub(from))).unwrap_or(1 << 20)];
    let mut at = from;
    while at < to {
        let n = (to - at).min(block.len() as u64);
        let part = block.get(..usize::try_from(n).unwrap_or(0)).unwrap_or_default();
        write_at(file, part, at)?;
        at += n;
    }
    Ok(())
}

/// Whether `bytes`, read from the file at `offset`, hold a whole device sector of zeros - what a write cut short
/// leaves in space that was zeros before it.
pub(super) fn has_zero_sector(bytes: &[u8], offset: u64) -> bool {
    let skip = usize::try_from((SECTOR as u64 - offset % SECTOR as u64) % SECTOR as u64).unwrap_or(0);
    bytes.get(skip..).is_some_and(|rest| {
        rest.as_chunks::<SECTOR>()
            .0
            .iter()
            .any(|s| s.iter().all(|b| *b == 0))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_sector_is_found_only_when_whole_and_aligned() {
        let mut b = vec![1u8; 2048];
        assert!(!has_zero_sector(&b, 0));
        b[512..1024].fill(0);
        assert!(has_zero_sector(&b, 0));
        // Read from offset 100: the zeros at 512..1024 of the slice sit at file offsets 612..1124 - not a whole
        // aligned sector.
        assert!(!has_zero_sector(&b, 100));
        let mut c = vec![1u8; 2048];
        c[412..924].fill(0);
        assert!(has_zero_sector(&c, 100), "file offsets 512..1024");
    }
}
