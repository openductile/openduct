// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A PDF stream's bytes undone from the filters it was written with - Flate, LZW, ASCII85, hex, run length - each
//! bounded by what is left of the document's budget. Image filters are not text and are not undone.

use std::io::Read;

use super::lex::Object;

/// A stream's bytes, its filters undone; `budget` is how many bytes may still be made, and is lessened.
pub(super) fn decode(raw: &[u8], dict: &Object, budget: &mut usize) -> Result<Vec<u8>, String> {
    let filters: Vec<Vec<u8>> = match dict.get(b"Filter").or_else(|| dict.get(b"F")) {
        None => Vec::new(),
        Some(Object::Name(n)) => vec![n.clone()],
        Some(Object::Array(list)) => list.iter().filter_map(|o| o.name().map(<[u8]>::to_vec)).collect(),
        Some(_) => return Err("a stream with filters not read here".to_owned()),
    };
    let mut data = raw.to_vec();
    for filter in &filters {
        data = match filter.as_slice() {
            b"FlateDecode" | b"Fl" => flate(&data, *budget)?,
            b"LZWDecode" | b"LZW" => lzw(&data, *budget)?,
            b"ASCII85Decode" | b"A85" => ascii85(&data),
            b"ASCIIHexDecode" | b"AHx" => hex(&data),
            b"RunLengthDecode" | b"RL" => run_length(&data, *budget),
            other => {
                return Err(format!(
                    "a stream in {} is not text",
                    String::from_utf8_lossy(other)
                ));
            }
        };
        if data.len() > *budget {
            return Err("the PDF unpacks to too much to read".to_owned());
        }
    }
    *budget -= data.len();
    Ok(data)
}

fn flate(data: &[u8], limit: usize) -> Result<Vec<u8>, String> {
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut out = Vec::new();
    // Most streams are zlib; a few writers leave the header off, and some cut the end short.
    let zlib = flate2::read::ZlibDecoder::new(data)
        .take(cap)
        .read_to_end(&mut out);
    if zlib.is_err() && out.is_empty() {
        out.clear();
        flate2::read::DeflateDecoder::new(data)
            .take(cap)
            .read_to_end(&mut out)
            .map_err(|e| format!("a stream cannot be unpacked: {e}"))?;
    }
    Ok(out)
}

fn lzw(data: &[u8], limit: usize) -> Result<Vec<u8>, String> {
    let mut table: Vec<Vec<u8>> = (0..=255u8).map(|b| vec![b]).collect();
    table.push(Vec::new());
    table.push(Vec::new());
    let (mut out, mut previous): (Vec<u8>, Option<Vec<u8>>) = (Vec::new(), None);
    let (mut buffer, mut bits, mut width) = (0u32, 0u32, 9u32);
    for &byte in data {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        while bits >= width {
            let code = usize::try_from((buffer >> (bits - width)) & ((1 << width) - 1)).unwrap_or(0);
            bits -= width;
            if code == 256 {
                table.truncate(258);
                width = 9;
                previous = None;
                continue;
            }
            if code == 257 {
                return Ok(out);
            }
            let entry = match (table.get(code), &previous) {
                (Some(e), _) => e.clone(),
                (None, Some(p)) if code == table.len() => {
                    let mut e = p.clone();
                    e.push(p.first().copied().unwrap_or(0));
                    e
                }
                _ => return Err("a broken LZW stream".to_owned()),
            };
            out.extend_from_slice(&entry);
            if out.len() > limit {
                return Err("the PDF unpacks to too much to read".to_owned());
            }
            if let Some(mut p) = previous.take()
                && table.len() < 4096
            {
                p.push(entry.first().copied().unwrap_or(0));
                table.push(p);
            }
            previous = Some(entry);
            // Early change: the width grows one code before the table is full at it.
            width = match table.len() + 1 {
                n if n >= 2048 => 12,
                n if n >= 1024 => 11,
                n if n >= 512 => 10,
                _ => 9,
            };
        }
    }
    Ok(out)
}

fn ascii85(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut group, mut count) = (0u64, 0);
    for &b in data {
        match b {
            b'~' => break,
            b'z' if count == 0 => out.extend_from_slice(&[0; 4]),
            b'!'..=b'u' => {
                group = group * 85 + u64::from(b - b'!');
                count += 1;
                if count == 5 {
                    out.extend_from_slice(&u32::try_from(group & 0xffff_ffff).unwrap_or(0).to_be_bytes());
                    group = 0;
                    count = 0;
                }
            }
            _ => {}
        }
    }
    if count > 1 {
        for _ in count..5 {
            group = group * 85 + 84;
        }
        let bytes = u32::try_from(group & 0xffff_ffff).unwrap_or(0).to_be_bytes();
        out.extend_from_slice(bytes.get(..count - 1).unwrap_or_default());
    }
    out
}

fn hex(data: &[u8]) -> Vec<u8> {
    let digits: Vec<u8> = data
        .iter()
        .take_while(|b| **b != b'>')
        .filter_map(|b| char::from(*b).to_digit(16))
        .filter_map(|d| u8::try_from(d).ok())
        .collect();
    digits
        .chunks(2)
        .map(|p| (p.first().copied().unwrap_or(0) << 4) | p.get(1).copied().unwrap_or(0))
        .collect()
}

fn run_length(data: &[u8], limit: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(&n) = data.get(i) {
        if n == 128 || out.len() > limit {
            break;
        }
        if n < 128 {
            let count = usize::from(n) + 1;
            out.extend_from_slice(data.get(i + 1..i + 1 + count).unwrap_or_default());
            i += 1 + count;
        } else {
            let b = data.get(i + 1).copied().unwrap_or(0);
            out.extend(std::iter::repeat_n(b, 257 - usize::from(n)));
            i += 2;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn filters_undo_what_writers_do() {
        let mut packer = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        packer.write_all(b"BT (Hello) Tj ET").unwrap();
        let packed = packer.finish().unwrap();
        let dict = Object::Dict(vec![(b"Filter".to_vec(), Object::Name(b"FlateDecode".to_vec()))]);
        let mut budget = 1000;
        assert_eq!(decode(&packed, &dict, &mut budget).unwrap(), b"BT (Hello) Tj ET");
        assert_eq!(budget, 1000 - 16);
        let mut small = 4;
        assert!(decode(&packed, &dict, &mut small).is_err());
        assert_eq!(ascii85(b"87cURD]i,\"Ebo80~>"), b"Hello World!");
        assert_eq!(hex(b"48 65 6c6C6f>"), b"Hello");
        assert_eq!(run_length(&[2, b'a', b'b', b'c', 254, b'x', 128], 100), b"abcxxx");
        // "-----A---B" in LZW, as the PDF reference gives it.
        assert_eq!(
            lzw(&[0x80, 0x0b, 0x60, 0x50, 0x22, 0x0c, 0x0c, 0x85, 0x01], 100).unwrap(),
            b"-----A---B"
        );
        let image = Object::Dict(vec![(b"Filter".to_vec(), Object::Name(b"DCTDecode".to_vec()))]);
        assert!(decode(b"x", &image, &mut budget).is_err());
    }
}
