// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;

use super::*;

/// A PDF printed by an operating system's own PDF writer: one page, a TrueType font in MacRoman, a line per `Tm`.
const PRINTED: &[u8] = include_bytes!("../../../tests/fixtures/invoice.pdf");

fn packed(data: &[u8]) -> Vec<u8> {
    let mut packer = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    packer.write_all(data).unwrap();
    packer.finish().unwrap()
}

/// A PDF of these objects (`(number, dictionary, stream)`), and a trailer naming the catalog.
fn pdf(objects: &[(u32, &str, Option<Vec<u8>>)], trailer: &str) -> Vec<u8> {
    let mut out = b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n".to_vec();
    for (n, dict, stream) in objects {
        out.extend_from_slice(format!("{n} 0 obj\n").as_bytes());
        match stream {
            Some(bytes) => {
                let dict = dict.replace("LEN", &bytes.len().to_string());
                out.extend_from_slice(format!("{dict}\nstream\n").as_bytes());
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\nendstream\n");
            }
            None => out.extend_from_slice(dict.as_bytes()),
        }
        out.extend_from_slice(b"\nendobj\n");
    }
    out.extend_from_slice(format!("trailer\n{trailer}\n%%EOF\n").as_bytes());
    out
}

#[test]
fn a_printed_pdf_reads_line_by_line() {
    let (text, pages) = text(PRINTED).unwrap();
    assert_eq!(pages, 1);
    assert_eq!(
        text,
        "Invoice 2041\nTotal due: 1,250.00 EUR\nThank you for your order."
    );
}

#[test]
fn composite_fonts_object_streams_forms_and_pages_read_in_order() {
    let cmap = b"/CIDInit /ProcSet findresource begin 12 dict begin begincmap 1 begincodespacerange <0000> <FFFF> \
                 endcodespacerange 3 beginbfchar <0001> <0048> <0002> <0069> <0003> <0021> endbfchar 1 beginbfrange \
                 <0010> <0012> <0061> endbfrange endcmap end end";
    // Page 1: "Hi!" then, a line down, "abc" and a word apart "Hi"; then a form that says "c!".
    let first = b"BT /F1 12 Tf 72 700 Td [<0001><0002>] TJ <0003> Tj 0 -14 Td [<00100011> -40 <0012> -600 <00010002>] TJ ET \
                  q /Fm1 Do Q BI /W 2 /H 2 /BPC 8 /CS /G ID \x00EI\xff EI Q";
    let form = b"BT /F1 9 Tf (ignored) Tj ET BT /F1 9 Tf 1 0 0 1 10 10 Tm <00120003> Tj ET";
    let second = b"BT /F1 12 Tf 72 700 Td (no font codes here) Tj ET";
    // The pages and the font are kept in an object stream: its header says where each is.
    let pages = "<< /Type /Pages /Kids [4 0 R 5 0 R] /Count 2 /Resources 6 0 R >>";
    let font = "<< /Type /Font /Subtype /Type0 /BaseFont /X /Encoding /Identity-H /ToUnicode 7 0 R >>";
    let header = format!("2 0 3 {} ", pages.len() + 1);
    let kept_stream = format!("{header}{pages}\n{font}");
    let header_len = header.len();
    let objects = [
        (1, "<< /Type /Catalog /Pages 2 0 R >>".to_owned(), None),
        (
            9,
            format!("<< /Type /ObjStm /N 2 /First {header_len} /Length LEN /Filter /FlateDecode >>"),
            Some(packed(kept_stream.as_bytes())),
        ),
        (
            4,
            "<< /Type /Page /Parent 2 0 R /Contents [8 0 R] >>".to_owned(),
            None,
        ),
        (
            5,
            "<< /Type /Page /Parent 2 0 R /Contents 10 0 R /Resources << >> >>".to_owned(),
            None,
        ),
        (
            6,
            "<< /Font << /F1 3 0 R >> /XObject << /Fm1 11 0 R >> >>".to_owned(),
            None,
        ),
        (
            7,
            "<< /Length LEN /Filter /FlateDecode >>".to_owned(),
            Some(packed(cmap)),
        ),
        (
            8,
            "<< /Length LEN /Filter /FlateDecode >>".to_owned(),
            Some(packed(first)),
        ),
        (10, "<< /Length LEN >>".to_owned(), Some(second.to_vec())),
        (
            11,
            "<< /Type /XObject /Subtype /Form /Length LEN /Resources 6 0 R >>".to_owned(),
            Some(form.to_vec()),
        ),
    ];
    let objects: Vec<(u32, &str, Option<Vec<u8>>)> = objects
        .iter()
        .map(|(n, d, s)| (*n, d.as_str(), s.clone()))
        .collect();
    let bytes = pdf(&objects, "<< /Root 1 0 R /Size 12 >>");
    let (text, pages) = text(&bytes).unwrap();
    assert_eq!(pages, 2);
    assert_eq!(text, "Hi!\nabc Hi\nc!\n\nno font codes here");
}

#[test]
fn protected_damaged_and_hostile_pdfs_are_refused_not_followed() {
    let protected = pdf(
        &[(1, "<< /Type /Catalog /Pages 2 0 R >>", None)],
        "<< /Root 1 0 R /Encrypt 5 0 R >>",
    );
    assert!(text(&protected).unwrap_err().contains("protected"));
    assert!(text(b"%PDF-1.4 nothing here").unwrap_err().contains("no pages"));
    assert!(text(b"GIF89a").is_err());
    for cut in [10, PRINTED.len() / 3, PRINTED.len() / 2, PRINTED.len() - 40] {
        let _ = text(&PRINTED[..cut]);
    }
    // Pages that are each other's kids.
    let looped = pdf(
        &[
            (1, "<< /Type /Catalog /Pages 2 0 R >>", None),
            (2, "<< /Type /Pages /Kids [3 0 R] >>", None),
            (3, "<< /Type /Pages /Kids [2 0 R] >>", None),
        ],
        "<< /Root 1 0 R >>",
    );
    assert!(text(&looped).is_err());
}
