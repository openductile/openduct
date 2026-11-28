// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Tar archives (POSIX ustar, with the long names of the pax and GNU forms): read entry by entry - only plain files
//! are taken; links, devices and folders are passed by - and written. Every header is checked (its checksum, its
//! size written in octal), and what is read is bounded by count and size.

/// The most entries an archive is read with.
const MAX_ENTRIES: usize = 10_000;
/// A long name (pax or GNU) of at most this many bytes.
const MAX_LONG_NAME: usize = 4096;
const BLOCK: usize = 512;

const BROKEN: &str = "not a tar archive, or a broken one";

/// A plain file of an archive: its path in it, and its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TarEntry {
    pub path: String,
    pub bytes: Vec<u8>,
}

fn octal(field: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(field).ok()?;
    let text = text.trim_matches(|c: char| c == '\0' || c == ' ');
    if text.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(text, 8).ok()
}

fn text(field: &[u8]) -> String {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(field.get(..end).unwrap_or_default()).into_owned()
}

fn checksum_ok(header: &[u8]) -> bool {
    let Some(written) = header.get(148..156).and_then(octal) else {
        return false;
    };
    let sum: u64 = header
        .iter()
        .enumerate()
        .map(|(i, b)| {
            if (148..156).contains(&i) {
                32
            } else {
                u64::from(*b)
            }
        })
        .sum();
    sum == written
}

/// A pax header's `path`, when it gives one.
fn pax_path(records: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(records);
    let mut rest = text.as_ref();
    while !rest.is_empty() {
        let (length, _) = rest.split_once(' ')?;
        let n: usize = length.parse().ok()?;
        let record = rest.get(..n)?;
        rest = rest.get(n..)?;
        let (_, field) = record.split_once(' ')?;
        if let Some(path) = field.strip_prefix("path=") {
            return Some(path.trim_end_matches('\n').to_owned());
        }
    }
    None
}

/// Whether `bytes` look like a tar archive: `ustar` where its first header says the format.
pub(crate) fn looks_like(bytes: &[u8]) -> bool {
    bytes.get(257..262) == Some(b"ustar")
}

/// The plain files of an archive, each kept when `wanted(path, size)` says so, each at most `most` bytes and
/// together at most `budget`.
pub(crate) fn read(
    bytes: &[u8],
    most: usize,
    budget: usize,
    mut wanted: impl FnMut(&str, usize) -> bool,
) -> Result<Vec<TarEntry>, String> {
    let mut at = 0;
    let mut out = Vec::new();
    let mut long_name: Option<String> = None;
    let mut taken = 0usize;
    for _ in 0..MAX_ENTRIES {
        let Some(header) = bytes.get(at..at + BLOCK) else {
            // An archive cut short after its last whole entry is read as far as it goes.
            return if at >= bytes.len() {
                Ok(out)
            } else {
                Err(BROKEN.to_owned())
            };
        };
        if header.iter().all(|b| *b == 0) {
            return Ok(out);
        }
        if !checksum_ok(header) {
            return Err(BROKEN.to_owned());
        }
        let size = header
            .get(124..136)
            .and_then(octal)
            .and_then(|s| usize::try_from(s).ok())
            .ok_or("an entry whose size is not read here (more than 8 GB)")?;
        let kind = header.get(156).copied().unwrap_or(b'0');
        let start = at + BLOCK;
        let data = bytes.get(start..start.saturating_add(size)).ok_or(BROKEN)?;
        at = start.saturating_add(size.div_ceil(BLOCK) * BLOCK);
        let mut name = text(header.get(0..100).unwrap_or_default());
        if header.get(257..262) == Some(b"ustar") {
            let prefix = text(header.get(345..500).unwrap_or_default());
            if !prefix.is_empty() {
                name = format!("{prefix}/{name}");
            }
        }
        match kind {
            b'x' | b'L' => {
                if size > MAX_LONG_NAME {
                    return Err("a name of more than 4 KB".to_owned());
                }
                long_name = if kind == b'x' {
                    pax_path(data)
                } else {
                    Some(text(data))
                };
                continue;
            }
            b'0' | 0 | b'7' => {
                let path = long_name.take().unwrap_or(name);
                if !wanted(&path, size) {
                    continue;
                }
                if size > most {
                    return Err(format!("{path} is {size} bytes: at most {most} are unpacked"));
                }
                taken = taken.saturating_add(size);
                if taken > budget {
                    return Err(format!("the archive unpacks to more than {budget} bytes"));
                }
                out.push(TarEntry {
                    path,
                    bytes: data.to_vec(),
                });
            }
            // Folders, links, devices and global headers: passed by.
            _ => long_name = None,
        }
    }
    Err(format!("an archive of more than {MAX_ENTRIES} entries"))
}

fn field(header: &mut [u8], at: usize, value: &[u8]) {
    if let Some(place) = header.get_mut(at..at + value.len()) {
        place.copy_from_slice(value);
    }
}

fn octal_field(header: &mut [u8], at: usize, width: usize, value: u64) {
    let text = format!("{value:0w$o}\0", w = width - 1);
    field(header, at, text.as_bytes());
}

fn header(name: &[u8], size: usize, kind: u8) -> Vec<u8> {
    let mut h = vec![0u8; BLOCK];
    field(&mut h, 0, name.get(..name.len().min(100)).unwrap_or_default());
    octal_field(&mut h, 100, 8, 0o644);
    octal_field(&mut h, 108, 8, 0);
    octal_field(&mut h, 116, 8, 0);
    octal_field(&mut h, 124, 12, u64::try_from(size).unwrap_or(0));
    octal_field(&mut h, 136, 12, 0);
    field(&mut h, 148, b"        ");
    if let Some(k) = h.get_mut(156) {
        *k = kind;
    }
    field(&mut h, 257, b"ustar\0");
    field(&mut h, 263, b"00");
    let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
    field(&mut h, 148, format!("{sum:06o}\0 ").as_bytes());
    h
}

fn pad(out: &mut Vec<u8>) {
    let rest = out.len() % BLOCK;
    if rest != 0 {
        out.resize(out.len() + BLOCK - rest, 0);
    }
}

/// An archive of these files: each a plain file; a name longer than a header holds (or not ASCII) given in a pax
/// header before it.
pub(crate) fn write(files: &[(String, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, bytes) in files {
        let plain = name.len() <= 100 && name.is_ascii();
        if !plain {
            let record = |n: usize| format!("{n} path={name}\n");
            // The record counts its own length's digits.
            let mut n = name.len() + 7;
            while record(n).len() != n {
                n = record(n).len();
            }
            let records = record(n);
            out.extend(header(b"././@PaxHeader", records.len(), b'x'));
            out.extend(records.as_bytes());
            pad(&mut out);
        }
        let short: String = name.chars().filter(char::is_ascii).take(100).collect();
        out.extend(header(short.as_bytes(), bytes.len(), b'0'));
        out.extend(*bytes);
        pad(&mut out);
    }
    out.resize(out.len() + 2 * BLOCK, 0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_archive_written_reads_back_with_long_names() {
        let long = format!("{}/report-été.csv", "folder".repeat(20));
        let tar = write(&[
            ("a.txt".to_owned(), b"hello"),
            (long.clone(), b"id\n1\n"),
            ("empty".to_owned(), b""),
        ]);
        assert!(looks_like(&tar));
        let back = read(&tar, 100, 1000, |_, _| true).unwrap_or_default();
        let paths: Vec<&str> = back.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, ["a.txt", long.as_str(), "empty"]);
        assert_eq!(back.first().map(|e| e.bytes.as_slice()), Some(&b"hello"[..]));
        let some = read(&tar, 100, 1000, |p, _| p.ends_with(".csv")).unwrap_or_default();
        assert_eq!(some.len(), 1);
    }

    #[test]
    fn a_broken_or_too_large_archive_is_refused() {
        let tar = write(&[("big.bin".to_owned(), &[7u8; 2000][..])]);
        assert!(
            read(&tar, 1000, 10_000, |_, _| true)
                .unwrap_err()
                .contains("at most 1000")
        );
        assert!(
            read(&tar, 10_000, 1000, |_, _| true)
                .unwrap_err()
                .contains("more than 1000")
        );
        let mut damaged = tar.clone();
        if let Some(b) = damaged.get_mut(10) {
            *b ^= 0xff;
        }
        assert_eq!(read(&damaged, 10_000, 10_000, |_, _| true).unwrap_err(), BROKEN);
        // Cut anywhere, it is read as far as it goes or refused - never more than it holds.
        for cut in [1, 100, 511, 512, 600, tar.len() - 1] {
            let _ = read(tar.get(..cut).unwrap_or_default(), 10_000, 10_000, |_, _| true);
        }
        assert!(read(b"", 10, 10, |_, _| true).unwrap_or_default().is_empty());
        assert!(!looks_like(b"not a tar"));
    }
}
