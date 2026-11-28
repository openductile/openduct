// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `compress` step: a run's files packed into an archive (ZIP, gzip, tar, tar.gz), an archive unpacked into run
//! files, or its entries listed. All in memory and bounded: an entry of more than a run's largest file, or an archive
//! that unpacks to more than [`MAX_UNPACKED`] (a "zip bomb"), is refused, and an archive of a few kilobytes that says
//! it holds gigabytes is refused before anything is unpacked.

use std::io::{Read, Write};
use std::sync::Arc;

use openduct_expr::{Compiled, Env, Record, Value};
use serde_json::json;

use super::paths::Pattern;
use crate::files::{FileData, MAX_FILE, RunFiles};
use crate::run::Effects;
use crate::template::Template;

/// The most an archive unpacks to, in all.
pub const MAX_UNPACKED: usize = 64 * 1024 * 1024;
/// The most files one archive is packed of.
const MAX_PACKED: usize = 1_000;
/// The most entries a listing gives.
const MAX_LISTED: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveAction {
    Pack,
    Unpack,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Zip,
    Gzip,
    Tar,
    TarGz,
}

impl Format {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "zip" => Self::Zip,
            "gzip" | "gz" => Self::Gzip,
            "tar" => Self::Tar,
            "tar.gz" | "tgz" => Self::TarGz,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            Self::Zip => "zip",
            Self::Gzip => "gzip",
            Self::Tar => "tar",
            Self::TarGz => "tar.gz",
        }
    }

    fn made(self) -> (&'static str, &'static str) {
        match self {
            Self::Zip => ("application/zip", "zip"),
            Self::Gzip => ("application/gzip", "gz"),
            Self::Tar => ("application/x-tar", "tar"),
            Self::TarGz => ("application/gzip", "tar.gz"),
        }
    }

    /// The format of an archive, from its first bytes.
    fn of(bytes: &[u8]) -> Option<Self> {
        if bytes.starts_with(b"PK\x03\x04") || bytes.starts_with(b"PK\x05\x06") {
            return Some(Self::Zip);
        }
        if bytes.starts_with(&[0x1f, 0x8b]) {
            return Some(Self::Gzip);
        }
        super::tar::looks_like(bytes).then_some(Self::Tar)
    }
}

/// A `compress` step, as read.
#[derive(Debug, Clone)]
pub struct ArchiveStep {
    pub action: ArchiveAction,
    /// Pack: what to make (ZIP by default). Unpack and list: from the archive's own bytes when not given.
    pub format: Option<Format>,
    /// Pack: a file, or a list of files.
    pub files: Option<Compiled>,
    /// Unpack and list: the archive.
    pub file: Option<Compiled>,
    /// Pack: the archive's name.
    pub name: Option<Template>,
    /// Unpack: only the entries whose names match.
    pub pattern: Option<Pattern>,
}

/// Bytes gunzipped: at most `most` of them.
fn gunzip(bytes: &[u8], most: usize) -> Result<(Vec<u8>, Option<String>), String> {
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder
        .by_ref()
        .take(u64::try_from(most).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut out)
        .map_err(|e| format!("not gzip, or broken: {e}"))?;
    if out.len() > most {
        return Err(format!("it unpacks to more than {most} bytes, and is refused"));
    }
    let name = decoder
        .header()
        .and_then(|h| h.filename())
        .map(|n| String::from_utf8_lossy(n).into_owned());
    Ok((out, name))
}

fn gzip(bytes: &[u8], name: Option<&str>) -> Result<Vec<u8>, String> {
    let mut builder = flate2::GzBuilder::new();
    if let Some(n) = name {
        builder = builder.filename(n.as_bytes());
    }
    let mut encoder = builder.write(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

/// The plain files an archive holds - their paths, and their bytes when `unpack` - those `wanted` keeps.
fn entries(
    file: &FileData,
    format: Option<Format>,
    unpack: bool,
    wanted: &dyn Fn(&str) -> bool,
) -> Result<Vec<(String, usize, Vec<u8>)>, String> {
    let bytes = file.bytes.as_slice();
    let format = format
        .or_else(|| Format::of(bytes))
        .ok_or_else(|| format!("{} is not a ZIP, gzip or tar archive", file.name))?;
    let take = |path: &str, size: usize| -> Result<bool, String> {
        let keep = wanted(path);
        if keep && unpack && size > MAX_FILE {
            return Err(format!(
                "{path} unpacks to {size} bytes: a run's files are of at most {MAX_FILE}"
            ));
        }
        Ok(keep)
    };
    let mut out = Vec::new();
    match format {
        Format::Zip => {
            let archive = crate::files::zip::Archive::open(bytes)?;
            let listed = archive.names();
            if listed.len() > MAX_LISTED {
                return Err(format!("an archive of more than {MAX_LISTED} entries"));
            }
            for (path, size) in listed {
                if path.ends_with('/') || !take(path, size)? {
                    continue;
                }
                let read = if unpack {
                    archive.read(path)?.unwrap_or_default()
                } else {
                    Vec::new()
                };
                out.push((path.to_owned(), size, read));
            }
        }
        Format::Gzip => {
            let (inner, name) = gunzip(bytes, MAX_UNPACKED)?;
            if super::tar::looks_like(&inner) {
                return tar_entries(&inner, unpack, wanted);
            }
            let name = name.map_or_else(|| file.name.trim_end_matches(".gz").to_owned(), |n| n.clone());
            if take(&name, inner.len())? {
                let size = inner.len();
                out.push((name, size, if unpack { inner } else { Vec::new() }));
            }
        }
        Format::Tar => return tar_entries(bytes, unpack, wanted),
        Format::TarGz => {
            let (inner, _) = gunzip(bytes, MAX_UNPACKED)?;
            return tar_entries(&inner, unpack, wanted);
        }
    }
    Ok(out)
}

fn tar_entries(
    bytes: &[u8],
    unpack: bool,
    wanted: &dyn Fn(&str) -> bool,
) -> Result<Vec<(String, usize, Vec<u8>)>, String> {
    let most = if unpack { MAX_FILE } else { usize::MAX };
    let read = super::tar::read(bytes, most, MAX_UNPACKED, |path, _| wanted(path))?;
    Ok(read
        .into_iter()
        .map(|e| {
            let size = e.bytes.len();
            (e.path, size, if unpack { e.bytes } else { Vec::new() })
        })
        .collect())
}

/// A name of the archive's own, kept apart from the others: `report.csv`, then `report (2).csv`.
fn unique(name: &str, taken: &mut Vec<String>) -> String {
    let mut candidate = name.to_owned();
    let mut n = 2;
    while taken.contains(&candidate) {
        candidate = match name.rsplit_once('.') {
            Some((stem, ext)) if !stem.is_empty() => format!("{stem} ({n}).{ext}"),
            _ => format!("{name} ({n})"),
        };
        n += 1;
    }
    taken.push(candidate.clone());
    candidate
}

async fn pack(
    effects: &dyn Effects,
    files: &RunFiles,
    a: &ArchiveStep,
    step: &str,
    env: &Env<'_>,
) -> Result<Value, String> {
    let given = a
        .files
        .as_ref()
        .ok_or("files: what to pack, as [steps.report, steps.photo]")?
        .evaluate(env)
        .map_err(|e| format!("files: {e}"))?;
    let items = match given {
        Value::List(items) => items.to_vec(),
        Value::Null => Vec::new(),
        one => vec![one],
    };
    if items.is_empty() {
        return Err("files: nothing to pack".to_owned());
    }
    if items.len() > MAX_PACKED {
        return Err(format!("files: at most {MAX_PACKED} in one archive"));
    }
    let mut held = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        held.push(
            files
                .read(effects, item)
                .await
                .map_err(|e| format!("files[{i}]: {e}"))?,
        );
    }
    let format = a.format.unwrap_or(Format::Zip);
    if format == Format::Gzip && held.len() != 1 {
        return Err(format!(
            "gzip packs one file, not {}: use zip or tar.gz for several",
            held.len()
        ));
    }
    let mut taken = Vec::new();
    let named: Vec<(String, Arc<Vec<u8>>)> = held
        .iter()
        .map(|f| (unique(&f.name, &mut taken), Arc::clone(&f.bytes)))
        .collect();
    let (media_type, extension) = format.made();
    let first = named.first().map(|(n, _)| n.clone()).unwrap_or_default();
    let name = match &a.name {
        Some(t) => t.render(env).map_err(|e| format!("name: {e}"))?,
        None if format == Format::Gzip => format!("{first}.gz"),
        None => format!("{step}.{extension}"),
    };
    let inner = first.clone();
    let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, String> {
        match format {
            Format::Zip => {
                let entries: Vec<(&str, Vec<u8>)> =
                    named.iter().map(|(n, b)| (n.as_str(), b.to_vec())).collect();
                crate::files::zip::write(&entries)
            }
            Format::Gzip => gzip(
                named.first().map(|(_, b)| b.as_slice()).unwrap_or_default(),
                Some(&inner),
            ),
            Format::Tar | Format::TarGz => {
                let entries: Vec<(String, &[u8])> =
                    named.iter().map(|(n, b)| (n.clone(), b.as_slice())).collect();
                let tar = super::tar::write(&entries);
                if format == Format::Tar {
                    Ok(tar)
                } else {
                    gzip(&tar, None)
                }
            }
        }
    })
    .await
    .map_err(|_| "the archive could not be made".to_owned())??;
    files.make(effects, &name, media_type, bytes).await
}

/// The step's result: the archive made, the files unpacked, or the entries listed.
pub(crate) async fn run(
    effects: &dyn Effects,
    files: &RunFiles,
    a: &ArchiveStep,
    step: &str,
    env: &Env<'_>,
) -> Result<Value, String> {
    if a.action == ArchiveAction::Pack {
        return pack(effects, files, a, step, env).await;
    }
    let given = a
        .file
        .as_ref()
        .ok_or("file: the archive, as steps.download.body")?
        .evaluate(env)
        .map_err(|e| format!("file: {e}"))?;
    let archive = files.read(effects, &given).await?;
    let unpack = a.action == ArchiveAction::Unpack;
    let (format, pattern) = (a.format, a.pattern.clone());
    let read = Arc::new(archive);
    let reading = Arc::clone(&read);
    let found = tokio::task::spawn_blocking(move || {
        let wanted = |path: &str| {
            let name = path.rsplit('/').next().unwrap_or(path);
            pattern.as_ref().is_none_or(|p| p.matches(name))
        };
        entries(&reading, format, unpack, &wanted)
    })
    .await
    .map_err(|_| "the archive could not be read".to_owned())?
    .map_err(|e| format!("{}: {e}", read.name))?;
    let mut out = Vec::with_capacity(found.len());
    for (path, size, bytes) in found {
        if !unpack {
            out.push(Value::from_json(&json!({
                "path": path,
                "name": path.rsplit('/').next().unwrap_or(&path),
                "size": size,
            })));
            continue;
        }
        let name = path.rsplit('/').next().unwrap_or(&path).to_owned();
        let made = files
            .make(effects, &name, crate::files::steps::guess_type(&name), bytes)
            .await
            .map_err(|e| format!("{path}: {e} - unpack fewer with pattern"))?;
        let Value::Record(r) = made else { continue };
        let mut file: Record = (*r).clone();
        file.insert("path".to_owned(), Value::text(path.as_str()));
        out.push(Value::record(file));
    }
    let mut result = Record::new();
    result.insert("count".to_owned(), Value::from_json(&json!(out.len())));
    result.insert(
        (if unpack { "files" } else { "entries" }).to_owned(),
        Value::list(out),
    );
    result.insert(
        "format".to_owned(),
        Value::text(
            format
                .or_else(|| Format::of(&read.bytes))
                .map_or("unknown", Format::name),
        ),
    );
    Ok(Value::record(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, bytes: Vec<u8>) -> FileData {
        FileData {
            name: name.to_owned(),
            media_type: "application/octet-stream".to_owned(),
            bytes: Arc::new(bytes),
        }
    }

    fn all(file: &FileData) -> Vec<(String, usize, Vec<u8>)> {
        entries(file, None, true, &|_| true).unwrap()
    }

    #[test]
    fn each_format_is_told_by_its_bytes_and_unpacks() {
        let zip = crate::files::zip::write(&[("a.csv", b"id\n1\n".to_vec()), ("dir/", Vec::new())]).unwrap();
        assert_eq!(Format::of(&zip), Some(Format::Zip));
        let got = all(&file("x.zip", zip));
        assert_eq!(got.len(), 1, "a folder entry is not a file");
        assert_eq!(got[0].2, b"id\n1\n");
        let gz = gzip(b"hello", Some("note.txt")).unwrap();
        assert_eq!(all(&file("whatever.gz", gz))[0].0, "note.txt");
        let tar = super::super::tar::write(&[("t/a.txt".to_owned(), b"x"), ("b.txt".to_owned(), b"yy")]);
        let tgz = gzip(&tar, None).unwrap();
        let got = all(&file("x.tgz", tgz));
        assert_eq!(
            got.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
            ["t/a.txt", "b.txt"]
        );
        assert!(entries(&file("x", b"plain".to_vec()), None, true, &|_| true).is_err());
    }

    #[test]
    fn a_zip_bomb_is_refused() {
        // A gzip of 80 MB of zeros, a few dozen kilobytes packed.
        let bomb = gzip(&vec![0u8; MAX_UNPACKED + 16 * 1024 * 1024], None).unwrap();
        assert!(bomb.len() < 200 * 1024);
        let err = entries(&file("bomb.gz", bomb), None, true, &|_| true).unwrap_err();
        assert!(err.contains("more than"), "{err}");
        // A zip entry larger than a run's file is refused before it is unpacked.
        let zip = crate::files::zip::write(&[("big.bin", vec![1u8; MAX_FILE + 1])]).unwrap();
        let err = entries(&file("big.zip", zip.clone()), None, true, &|_| true).unwrap_err();
        assert!(err.contains("a run's files are of at most"), "{err}");
        // Listed, it is only named.
        let listed = entries(&file("big.zip", zip), None, false, &|_| true).unwrap();
        assert_eq!(listed[0].1, MAX_FILE + 1);
        assert!(listed[0].2.is_empty());
    }

    #[test]
    fn names_are_kept_apart() {
        let mut taken = Vec::new();
        assert_eq!(unique("a.csv", &mut taken), "a.csv");
        assert_eq!(unique("a.csv", &mut taken), "a (2).csv");
        assert_eq!(unique("a.csv", &mut taken), "a (3).csv");
        assert_eq!(unique("README", &mut taken), "README");
        assert_eq!(unique("README", &mut taken), "README (2)");
    }
}
