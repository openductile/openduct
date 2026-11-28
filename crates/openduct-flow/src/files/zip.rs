// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! ZIP archives, as spreadsheets (XLSX) are kept in: read entry by entry - stored or deflated, each bounded so an
//! archive that unpacks to far more than it holds is refused - and written, deflated.

use std::io::{Read, Write};

/// The most entries an archive is read with.
const MAX_ENTRIES: usize = 10_000;
/// The most one entry unpacks to.
const MAX_ENTRY: usize = 32 * 1024 * 1024;
/// The most the entries read from one archive unpack to, in all.
const MAX_UNPACKED: usize = 64 * 1024 * 1024;

fn u16_at(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn usize_at(b: &[u8], at: usize, wide: bool) -> Option<usize> {
    if wide {
        usize::try_from(u32_at(b, at)?).ok()
    } else {
        Some(usize::from(u16_at(b, at)?))
    }
}

#[derive(Debug, Clone)]
struct Entry {
    name: String,
    method: u16,
    crc: u32,
    packed: usize,
    size: usize,
    header: usize,
}

/// An archive's entries, read from its central directory.
#[derive(Debug)]
pub(crate) struct Archive<'a> {
    bytes: &'a [u8],
    entries: Vec<Entry>,
    /// How much more the entries read may unpack to.
    left: std::cell::Cell<usize>,
}

const BROKEN: &str = "not a ZIP archive, or a broken one";

impl<'a> Archive<'a> {
    pub(crate) fn open(bytes: &'a [u8]) -> Result<Self, String> {
        // The end of the central directory: 22 bytes, then a comment of up to 64 KiB.
        let lowest = bytes.len().saturating_sub(22 + 65_535);
        let end = (lowest..=bytes.len().saturating_sub(22))
            .rev()
            .find(|&at| u32_at(bytes, at) == Some(0x0605_4b50))
            .ok_or(BROKEN)?;
        let count = usize_at(bytes, end + 10, false).ok_or(BROKEN)?;
        let mut at = usize_at(bytes, end + 16, true).ok_or(BROKEN)?;
        if count == 0xffff || at == 0xffff_ffff {
            return Err("a ZIP64 archive: too large to read here".to_owned());
        }
        if count > MAX_ENTRIES {
            return Err(format!("an archive of more than {MAX_ENTRIES} entries"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            if u32_at(bytes, at) != Some(0x0201_4b50) {
                return Err(BROKEN.to_owned());
            }
            let flags = u16_at(bytes, at + 8).ok_or(BROKEN)?;
            if flags & 1 != 0 {
                return Err("an archive protected by a password".to_owned());
            }
            let name_len = usize_at(bytes, at + 28, false).ok_or(BROKEN)?;
            let extra = usize_at(bytes, at + 30, false).ok_or(BROKEN)?;
            let comment = usize_at(bytes, at + 32, false).ok_or(BROKEN)?;
            let name = bytes.get(at + 46..at + 46 + name_len).ok_or(BROKEN)?;
            entries.push(Entry {
                name: String::from_utf8_lossy(name).into_owned(),
                method: u16_at(bytes, at + 10).ok_or(BROKEN)?,
                crc: u32_at(bytes, at + 16).ok_or(BROKEN)?,
                packed: usize_at(bytes, at + 20, true).ok_or(BROKEN)?,
                size: usize_at(bytes, at + 24, true).ok_or(BROKEN)?,
                header: usize_at(bytes, at + 42, true).ok_or(BROKEN)?,
            });
            at += 46 + name_len + extra + comment;
        }
        Ok(Self {
            bytes,
            entries,
            left: std::cell::Cell::new(MAX_UNPACKED),
        })
    }

    /// The entry called `name` (or one whose name differs only in case), unpacked; `None` when there is none.
    /// The entries' names and the sizes they unpack to, in the archive's order. // storage
    pub(crate) fn names(&self) -> Vec<(&str, usize)> {
        self.entries.iter().map(|e| (e.name.as_str(), e.size)).collect()
    }

    pub(crate) fn read(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        let name = name.trim_start_matches('/');
        let Some(entry) = self
            .entries
            .iter()
            .find(|e| e.name == name)
            .or_else(|| self.entries.iter().find(|e| e.name.eq_ignore_ascii_case(name)))
        else {
            return Ok(None);
        };
        let most = MAX_ENTRY.min(self.left.get());
        if entry.size > most {
            return Err(format!("{name} unpacks to more than {most} bytes"));
        }
        let at = entry.header;
        if u32_at(self.bytes, at) != Some(0x0403_4b50) {
            return Err(BROKEN.to_owned());
        }
        let start = at
            + 30
            + usize_at(self.bytes, at + 26, false).ok_or(BROKEN)?
            + usize_at(self.bytes, at + 28, false).ok_or(BROKEN)?;
        let packed = self
            .bytes
            .get(start..start.saturating_add(entry.packed))
            .ok_or(BROKEN)?;
        let out = match entry.method {
            0 => packed.to_vec(),
            8 => {
                let mut out = Vec::with_capacity(entry.size.min(most));
                flate2::read::DeflateDecoder::new(packed)
                    .take(u64::try_from(most).unwrap_or(u64::MAX) + 1)
                    .read_to_end(&mut out)
                    .map_err(|e| format!("{name} cannot be unpacked: {e}"))?;
                out
            }
            other => {
                return Err(format!(
                    "{name} is packed in a way not read here (method {other})"
                ));
            }
        };
        if out.len() > most {
            return Err(format!("{name} unpacks to more than {most} bytes"));
        }
        if out.len() != entry.size || crc32fast::hash(&out) != entry.crc {
            return Err(format!("{name} is damaged"));
        }
        self.left.set(self.left.get().saturating_sub(out.len()));
        Ok(Some(out))
    }
}

/// An archive of these entries, each deflated.
pub(crate) fn write(entries: &[(&str, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut directory = Vec::new();
    let small = |n: usize| u32::try_from(n).map_err(|_| "a file too large to write".to_owned());
    for (name, data) in entries {
        let mut packer = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        packer.write_all(data).map_err(|e| e.to_string())?;
        let packed = packer.finish().map_err(|e| e.to_string())?;
        let crc = crc32fast::hash(data);
        let offset = small(out.len())?;
        let name_len = u16::try_from(name.len()).map_err(|_| "a name too long".to_owned())?;
        // Version 2.0, names in UTF-8, deflated, at 1980-01-01 00:00 (so the same rows make the same file).
        let common = |into: &mut Vec<u8>| -> Result<(), String> {
            into.extend_from_slice(&20u16.to_le_bytes());
            into.extend_from_slice(&0x0800u16.to_le_bytes());
            into.extend_from_slice(&8u16.to_le_bytes());
            into.extend_from_slice(&0u16.to_le_bytes());
            into.extend_from_slice(&0x0021u16.to_le_bytes());
            into.extend_from_slice(&crc.to_le_bytes());
            into.extend_from_slice(&small(packed.len())?.to_le_bytes());
            into.extend_from_slice(&small(data.len())?.to_le_bytes());
            into.extend_from_slice(&name_len.to_le_bytes());
            into.extend_from_slice(&0u16.to_le_bytes());
            Ok(())
        };
        out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        common(&mut out)?;
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&packed);
        directory.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        directory.extend_from_slice(&20u16.to_le_bytes());
        common(&mut directory)?;
        // No comment, disk 0, no attributes, then where its local header is.
        directory.extend_from_slice(&[0; 6]);
        directory.extend_from_slice(&0u32.to_le_bytes());
        directory.extend_from_slice(&offset.to_le_bytes());
        directory.extend_from_slice(name.as_bytes());
    }
    let start = small(out.len())?;
    let count = u16::try_from(entries.len()).map_err(|_| "too many entries".to_owned())?;
    let size = small(directory.len())?;
    out.extend_from_slice(&directory);
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&start.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_archive_written_reads_back_and_a_broken_one_is_refused() {
        let big = "row ".repeat(10_000).into_bytes();
        let zip = write(&[("a.txt", b"hello".to_vec()), ("dir/b.xml", big.clone())]).unwrap();
        let archive = Archive::open(&zip).unwrap();
        assert_eq!(archive.read("a.txt").unwrap().unwrap(), b"hello");
        assert_eq!(archive.read("/DIR/B.xml").unwrap().unwrap(), big);
        assert!(archive.read("missing").unwrap().is_none());
        assert!(Archive::open(b"not a zip").is_err());
        let mut damaged = zip.clone();
        damaged[40] ^= 0xff;
        let read = Archive::open(&damaged).and_then(|a| a.read("a.txt"));
        assert!(read.is_err(), "{read:?}");
        for cut in [0, 10, zip.len() / 2, zip.len() - 1] {
            let _ = Archive::open(&zip[..cut]).and_then(|a| a.read("dir/b.xml"));
        }
    }
}
