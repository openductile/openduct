// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The four ways Kafka clients compress a batch's records, for log compaction: it reads a compressed batch's
//! records, and writes what is left of it with the codec the producer chose.
//!
//! Kafka's codec numbers (the low three bits of a batch's attributes): 1 gzip, 2 snappy, 3 lz4, 4 zstd.
//! What is written is the same on every broker for the same input - gzip's header carries no time and no
//! operating system - so the copies of a compacted partition stay byte for byte alike.
//!
//! A batch may claim to expand to anything: reading stops at [`MAX_EXPANDED`], and such a batch is one compaction
//! leaves whole.

use std::io::{Read, Write};

/// The most one compressed batch may expand to.
pub const MAX_EXPANDED: usize = 128 * 1024 * 1024;

pub const GZIP: i16 = 1;
pub const SNAPPY: i16 = 2;
pub const LZ4: i16 = 3;
pub const ZSTD: i16 = 4;

/// The framing most Kafka clients put around snappy blocks: a 16-byte header, then `[length: u32][block]` pairs.
const SNAPPY_FRAMED: &[u8] = b"\x82SNAPPY\x00";

fn too_large() -> String {
    format!(
        "a compressed batch expands past {} MiB",
        MAX_EXPANDED / (1024 * 1024)
    )
}

fn read_bounded(reader: impl Read) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let limit = u64::try_from(MAX_EXPANDED).unwrap_or(u64::MAX).saturating_add(1);
    reader
        .take(limit)
        .read_to_end(&mut out)
        .map_err(|e| e.to_string())?;
    if out.len() > MAX_EXPANDED {
        return Err(too_large());
    }
    Ok(out)
}

fn snappy(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut raw = snap::raw::Decoder::new();
    let mut block = |data: &[u8], out: &mut Vec<u8>| -> Result<(), String> {
        let size = snap::raw::decompress_len(data).map_err(|e| e.to_string())?;
        if out.len().saturating_add(size) > MAX_EXPANDED {
            return Err(too_large());
        }
        out.extend_from_slice(&raw.decompress_vec(data).map_err(|e| e.to_string())?);
        Ok(())
    };
    if !input.starts_with(SNAPPY_FRAMED) {
        block(input, &mut out)?;
        return Ok(out);
    }
    let mut rest = input.get(16..).ok_or("a snappy header cut short")?;
    while !rest.is_empty() {
        let length: [u8; 4] = rest
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .ok_or("a snappy block length cut short")?;
        let length = usize::try_from(u32::from_be_bytes(length)).map_err(|e| e.to_string())?;
        let end = length.checked_add(4).ok_or("a snappy block too long")?;
        let data = rest.get(4..end).ok_or("a snappy block cut short")?;
        block(data, &mut out)?;
        rest = rest.get(end..).unwrap_or_default();
    }
    Ok(out)
}

/// A batch's records, decompressed.
pub fn decompress(codec: i16, input: &[u8]) -> Result<Vec<u8>, String> {
    match codec {
        GZIP => read_bounded(flate2::read::MultiGzDecoder::new(input)),
        SNAPPY => snappy(input),
        LZ4 => read_bounded(lz4_flex::frame::FrameDecoder::new(input)),
        ZSTD => {
            read_bounded(ruzstd::decoding::StreamingDecoder::new(input).map_err(|e| format!("zstd: {e}"))?)
        }
        other => Err(format!("compression codec {other} is not one Kafka defines")),
    }
}

/// A batch's records, compressed as Kafka clients do.
pub fn compress(codec: i16, records: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    match codec {
        GZIP => {
            // No time, no operating system: the same bytes wherever it runs.
            let mut encoder = flate2::GzBuilder::new()
                .mtime(0)
                .operating_system(255)
                .write(&mut out, flate2::Compression::default());
            encoder.write_all(records).map_err(|e| e.to_string())?;
            encoder.finish().map_err(|e| e.to_string())?;
        }
        SNAPPY => {
            out.extend_from_slice(SNAPPY_FRAMED);
            out.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
            let mut encoder = snap::raw::Encoder::new();
            for chunk in records.chunks(32 * 1024) {
                let block = encoder.compress_vec(chunk).map_err(|e| e.to_string())?;
                let length = u32::try_from(block.len()).map_err(|e| e.to_string())?;
                out.extend_from_slice(&length.to_be_bytes());
                out.extend_from_slice(&block);
            }
        }
        LZ4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut out);
            encoder.write_all(records).map_err(|e| e.to_string())?;
            encoder.finish().map_err(|e| e.to_string())?;
        }
        ZSTD => {
            out = ruzstd::encoding::compress_to_vec(records, ruzstd::encoding::CompressionLevel::Fastest);
        }
        other => return Err(format!("compression codec {other} is not one Kafka defines")),
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_codec_gives_back_what_it_was_given_and_writes_the_same_bytes_each_time() {
        let records: Vec<u8> = (0..70_000u32).flat_map(|i| (i % 251).to_be_bytes()).collect();
        for codec in [GZIP, SNAPPY, LZ4, ZSTD] {
            let packed = compress(codec, &records).unwrap();
            assert!(packed.len() < records.len(), "codec {codec}");
            assert_eq!(decompress(codec, &packed).unwrap(), records, "codec {codec}");
            assert_eq!(
                compress(codec, &records).unwrap(),
                packed,
                "codec {codec}: the same bytes again"
            );
            assert!(decompress(codec, &packed[..packed.len() / 2]).is_err() || codec == SNAPPY);
        }
        // Snappy as some clients send it, one bare block.
        let bare = snap::raw::Encoder::new().compress_vec(&records).unwrap();
        assert_eq!(decompress(SNAPPY, &bare).unwrap(), records);
        assert!(decompress(0, &records).is_err() && decompress(7, &records).is_err());
    }

    #[test]
    fn rubbish_is_refused_not_trusted() {
        for codec in [GZIP, SNAPPY, LZ4, ZSTD] {
            for junk in [&b""[..], b"\x00", b"not compressed at all", &[0xffu8; 64]] {
                let _ = decompress(codec, junk);
            }
        }
        // A snappy block that claims four gigabytes is refused before anything is allocated for it.
        assert!(decompress(SNAPPY, &[0xff, 0xff, 0xff, 0xff, 0x0f, 0x00]).is_err());
    }
}
