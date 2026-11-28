// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Files a run did not make are hostile input: PDFs, spreadsheets, CSV, web pages, JSON and multipart forms, each
//! mutated at random (with a fixed seed) and read - every one must give back an answer or a reason, without a panic
//! and in a moment.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::{Duration, Instant};

const PDF: &[u8] = include_bytes!("fixtures/invoice.pdf");
const XLS: &[u8] = include_bytes!("fixtures/orders.xls");
const XLSX: &[u8] = include_bytes!("fixtures/orders.xlsx");

/// A small random number maker, the same every run.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n.max(1)).unwrap()).unwrap()
    }
}

/// The seed changed a little: bytes flipped, a piece cut out or doubled, the end cut off, bytes put in.
fn mutate(r: &mut Random, seed: &[u8]) -> Vec<u8> {
    let mut out = seed.to_vec();
    for _ in 0..=r.below(4) {
        if out.is_empty() {
            out.push(0);
        }
        let at = r.below(out.len());
        match r.below(5) {
            0 => out[at] ^= 1 << r.below(8),
            1 => out[at] = [0, 0xff, b'(', b'<', b'/', b'0'][r.below(6)],
            2 => {
                let end = (at + r.below(64)).min(out.len());
                out.drain(at..end);
            }
            3 => {
                let end = (at + r.below(256)).min(out.len());
                let piece = out[at..end].to_vec();
                out.splice(at..at, piece);
            }
            _ => out.truncate(at),
        }
    }
    out
}

/// How many times more inputs to try (`OPENDUCT_HOSTILE_ROUNDS`, as the other hostile-input tests read it).
fn times() -> usize {
    std::env::var("OPENDUCT_HOSTILE_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

fn drive(name: &str, seed: &[u8], rounds: usize, read: impl Fn(Vec<u8>) -> Result<(), String>) {
    let mut r = Random(0x5eed_f11e ^ seed.len() as u64);
    for i in 0..rounds * times() {
        let input = if i == 0 {
            seed.to_vec()
        } else {
            mutate(&mut r, seed)
        };
        let started = Instant::now();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read(input.clone())));
        assert!(
            outcome.is_ok(),
            "{name}: input {i} ({} bytes) panicked",
            input.len()
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{name}: input {i} took {:?}",
            started.elapsed()
        );
    }
}

fn extract(format: &'static str) -> impl Fn(Vec<u8>) -> Result<(), String> {
    move |bytes| {
        let _ = openduct_flow::files::extract_bytes(bytes, "file", "application/octet-stream", Some(format));
        Ok(())
    }
}

#[test]
fn damaged_pdfs_spreadsheets_and_texts_are_refused_or_read_never_followed() {
    drive("pdf", PDF, 400, extract("pdf"));
    drive("xls", XLS, 300, extract("xls"));
    drive("xlsx", XLSX, 400, extract("xlsx"));
    drive(
        "csv",
        b"id,note\r\n1,\"a, b\"\n2,\"say \"\"hi\"\"\"\n",
        2_000,
        extract("csv"),
    );
    drive(
        "html",
        b"<html><title>T</title><script>x<y</script><p>a &amp; b<ul><li>c</li></ul><pre> d</pre>",
        2_000,
        extract("html"),
    );
    drive(
        "json",
        br#"{"a": [1, {"b": "c"}], "d": null}"#,
        2_000,
        extract("json"),
    );
    let form = b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nx\r\n--b\r\nContent-Disposition: \
                 form-data; name=\"f\"; filename=\"f.bin\"\r\nContent-Type: image/png\r\n\r\n\x89PNG\r\n--b--\r\n";
    drive("multipart", form, 3_000, |bytes| {
        let _ = openduct_flow::files::multipart::parse("multipart/form-data; boundary=b", &bytes);
        Ok(())
    });
}

#[test]
fn a_seed_reads_as_it_should_so_the_mutations_start_from_something_real() {
    let pdf = openduct_flow::files::extract_bytes(PDF.to_vec(), "x.pdf", "application/pdf", None).unwrap();
    assert!(pdf.to_json().contains("Invoice 2041"));
    let xlsx = openduct_flow::files::extract_bytes(XLSX.to_vec(), "x.xlsx", "", None).unwrap();
    assert!(xlsx.to_json().contains("Summary"));
    let xls = openduct_flow::files::extract_bytes(XLS.to_vec(), "x.xls", "", None).unwrap();
    assert!(xls.to_json().contains("Ada Lovelace"));
}
