// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What reading and writing messages needs besides the protocol: compressing and decompressing record batches the
//! way Kafka clients do, and choosing a partition for a key as they do.

use std::io::{Read, Write};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::records::Compression;

/// The most one compressed batch may expand to. A batch that claims more is refused rather than allowed to fill
/// memory.
pub const MAX_EXPANDED: usize = 128 * 1024 * 1024;

/// A batch's records, decompressed. Used as the record decoder's decompressor, so it is called for every batch.
pub fn decompress(buf: &mut Bytes, compression: Compression) -> anyhow::Result<Bytes> {
    let input = buf.split_to(buf.len());
    match compression {
        Compression::None => Ok(input),
        Compression::Gzip => read_bounded(flate2::read::MultiGzDecoder::new(input.as_ref())),
        Compression::Lz4 => read_bounded(lz4_flex::frame::FrameDecoder::new(input.as_ref())),
        Compression::Zstd => {
            let decoder = ruzstd::decoding::StreamingDecoder::new(input.as_ref())
                .map_err(|e| anyhow::anyhow!("zstd: {e}"))?;
            read_bounded(decoder)
        }
        Compression::Snappy => snappy(&input),
    }
}

/// A batch's records, compressed. Used as the record encoder's compressor.
pub fn compress(records: &mut BytesMut, out: &mut BytesMut, compression: Compression) -> anyhow::Result<()> {
    match compression {
        Compression::None => out.put_slice(records),
        Compression::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(out.writer(), flate2::Compression::default());
            encoder.write_all(records)?;
            encoder.finish()?;
        }
        Compression::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(out.writer());
            encoder.write_all(records)?;
            encoder.finish()?;
        }
        Compression::Zstd => out.put_slice(&ruzstd::encoding::compress_to_vec(
            &records[..],
            ruzstd::encoding::CompressionLevel::Fastest,
        )),
        Compression::Snappy => {
            // The framing Kafka's own clients write: a header, then `[length: u32][block]` for each 32 KiB.
            out.put_slice(SNAPPY_FRAMED);
            out.put_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
            let mut encoder = snap::raw::Encoder::new();
            for chunk in records.chunks(32 * 1024) {
                let block = encoder.compress_vec(chunk)?;
                out.put_u32(u32::try_from(block.len())?);
                out.put_slice(&block);
            }
        }
    }
    Ok(())
}

fn read_bounded(reader: impl Read) -> anyhow::Result<Bytes> {
    let mut out = Vec::new();
    let limit = u64::try_from(MAX_EXPANDED).unwrap_or(u64::MAX) + 1;
    reader.take(limit).read_to_end(&mut out)?;
    if out.len() > MAX_EXPANDED {
        anyhow::bail!(
            "a compressed batch expands past {} MiB",
            MAX_EXPANDED / (1024 * 1024)
        );
    }
    Ok(Bytes::from(out))
}

/// The framing most Kafka clients put around snappy blocks: a 16-byte header, then `[length: u32][block]` pairs.
const SNAPPY_FRAMED: &[u8] = b"\x82SNAPPY\x00";

fn snappy(input: &[u8]) -> anyhow::Result<Bytes> {
    let mut out = Vec::new();
    let mut raw = snap::raw::Decoder::new();
    let mut block = |data: &[u8], out: &mut Vec<u8>| -> anyhow::Result<()> {
        let size = snap::raw::decompress_len(data)?;
        if out.len().saturating_add(size) > MAX_EXPANDED {
            anyhow::bail!(
                "a compressed batch expands past {} MiB",
                MAX_EXPANDED / (1024 * 1024)
            );
        }
        out.extend_from_slice(&raw.decompress_vec(data)?);
        Ok(())
    };
    if !input.starts_with(SNAPPY_FRAMED) {
        block(input, &mut out)?;
        return Ok(Bytes::from(out));
    }
    let mut rest = input
        .get(16..)
        .ok_or_else(|| anyhow::anyhow!("a snappy header cut short"))?;
    while !rest.is_empty() {
        let length: [u8; 4] = rest
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| anyhow::anyhow!("a snappy block length cut short"))?;
        let length = usize::try_from(u32::from_be_bytes(length))?;
        let data = rest
            .get(4..4 + length)
            .ok_or_else(|| anyhow::anyhow!("a snappy block cut short"))?;
        block(data, &mut out)?;
        rest = rest.get(4 + length..).unwrap_or_default();
    }
    Ok(Bytes::from(out))
}

/// The partition a key goes to, as Kafka clients' default partitioner chooses it - murmur2 of the key's bytes - so
/// messages sent here and by any other client land together.
pub fn partition_for(key: &[u8], partitions: i32) -> i32 {
    let hash = i64::from(murmur2(key) & 0x7fff_ffff);
    i32::try_from(hash % i64::from(partitions.max(1))).unwrap_or(0)
}

fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    let length = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let mut h = SEED ^ length;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> 24;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if tail.len() >= 3 {
        h ^= u32::from(tail.get(2).copied().unwrap_or(0)) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail.get(1).copied().unwrap_or(0)) << 8;
    }
    if let Some(&first) = tail.first() {
        h ^= u32::from(first);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn keys_go_to_the_partitions_other_kafka_clients_choose() {
        // Reference values from Kafka's own default partitioner.
        for (key, hash, of_three, of_ten) in [
            ("", 275_646_681u32, 0, 1),
            ("a", 2_731_586_172, 1, 4),
            ("ab", 316_155_434, 2, 4),
            ("abc", 479_470_107, 0, 7),
            ("21", 3_321_034_988, 0, 0),
            ("sensor-1", 780_060_301, 1, 1),
            ("order-42", 501_153_024, 0, 4),
            ("customer-123456", 844_577_982, 0, 2),
            ("caf\u{e9}", 2_936_959_922, 0, 4),
        ] {
            assert_eq!(murmur2(key.as_bytes()), hash, "{key:?}");
            assert_eq!(partition_for(key.as_bytes(), 3), of_three, "{key:?} of 3");
            assert_eq!(partition_for(key.as_bytes(), 10), of_ten, "{key:?} of 10");
        }
    }

    #[test]
    fn every_compression_kafka_clients_use_is_read() {
        let text = b"sensor-1 21.4 sensor-2 19.8 ".repeat(200);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&text).unwrap();
        let gzip = gz.finish().unwrap();
        let mut lz = lz4_flex::frame::FrameEncoder::new(Vec::new());
        lz.write_all(&text).unwrap();
        let lz4 = lz.finish().unwrap();
        let zstd = ruzstd::encoding::compress_to_vec(&text[..], ruzstd::encoding::CompressionLevel::Fastest);
        let raw_snappy = snap::raw::Encoder::new().compress_vec(&text).unwrap();
        let mut framed = SNAPPY_FRAMED.to_vec();
        framed.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        framed.extend_from_slice(&u32::try_from(raw_snappy.len()).unwrap().to_be_bytes());
        framed.extend_from_slice(&raw_snappy);
        for (compression, data) in [
            (Compression::None, text.clone()),
            (Compression::Gzip, gzip),
            (Compression::Lz4, lz4),
            (Compression::Zstd, zstd),
            (Compression::Snappy, raw_snappy.clone()),
            (Compression::Snappy, framed),
        ] {
            let out = decompress(&mut Bytes::from(data), compression).unwrap();
            assert_eq!(out.as_ref(), &text[..], "{compression:?}");
        }
    }

    #[test]
    fn what_is_compressed_reads_back() {
        // More than one 32 KiB snappy block.
        let text = b"line 7: 21.4 degrees at the press; ".repeat(3_000);
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Snappy,
        ] {
            let mut out = BytesMut::new();
            compress(&mut BytesMut::from(&text[..]), &mut out, compression).unwrap();
            if compression != Compression::None {
                assert!(out.len() < text.len() / 4, "{compression:?} compresses");
            }
            let back = decompress(&mut out.freeze(), compression).unwrap();
            assert_eq!(back.as_ref(), &text[..], "{compression:?}");
        }
    }

    #[test]
    fn a_batch_that_expands_without_limit_is_refused() {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        let zeros = vec![0u8; 1024 * 1024];
        for _ in 0..(MAX_EXPANDED / zeros.len() + 1) {
            gz.write_all(&zeros).unwrap();
        }
        let bomb = gz.finish().unwrap();
        assert!(bomb.len() < 1024 * 1024, "a small input");
        assert!(decompress(&mut Bytes::from(bomb), Compression::Gzip).is_err());
    }
}
