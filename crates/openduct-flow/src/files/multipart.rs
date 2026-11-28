// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `multipart/form-data`, byte for byte: read into its parts (what a webhook's form upload sends) and written from
//! text fields and files (what an HTTP step sends).

/// The most parts a body is read as.
pub const MAX_PARTS: usize = 100;

/// One part of a form: its field's name, and - for a file - its file name and media type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub name: String,
    pub file_name: Option<String>,
    pub media_type: Option<String>,
    pub bytes: Vec<u8>,
}

/// A header's parameter (`name="x"`), quotes taken off.
fn parameter(header: &str, key: &str) -> Option<String> {
    let mut rest = header;
    while let Some(at) = rest.find(';') {
        rest = rest.get(at + 1..).unwrap_or_default();
        let (k, v) = rest.trim_start().split_once('=')?;
        if !k.trim().eq_ignore_ascii_case(key) {
            continue;
        }
        let v = v.trim_start();
        return Some(match v.strip_prefix('"') {
            Some(quoted) => {
                let mut out = String::new();
                let mut chars = quoted.chars();
                while let Some(c) = chars.next() {
                    match c {
                        '"' => break,
                        '\\' => out.extend(chars.next()),
                        c => out.push(c),
                    }
                }
                out
            }
            None => v.split(';').next().unwrap_or_default().trim().to_owned(),
        });
    }
    None
}

/// The boundary a `multipart/form-data` content type names.
pub fn boundary(content_type: &str) -> Option<String> {
    let essence = content_type.split(';').next().unwrap_or_default().trim();
    if !essence.eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    parameter(content_type, "boundary").filter(|b| !b.is_empty() && b.len() <= 200)
}

/// The parts of a multipart body; a part with no name is left out. At most [`MAX_PARTS`].
pub fn parse(content_type: &str, body: &[u8]) -> Result<Vec<Part>, String> {
    let boundary = boundary(content_type).ok_or("a multipart body names its boundary")?;
    let delimiter = format!("--{boundary}");
    let finder = memchr::memmem::Finder::new(delimiter.as_bytes());
    let mut places: Vec<usize> = finder.find_iter(body).collect();
    // A delimiter is at the start of the body or of a line.
    places.retain(|&p| {
        p == 0 || body.get(p.saturating_sub(2)..p) == Some(b"\r\n") || body.get(p - 1) == Some(&b'\n')
    });
    let mut parts = Vec::new();
    for pair in places.windows(2) {
        let (Some(&from), Some(&to)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        let start = from + delimiter.len();
        if body.get(start..start + 2) == Some(b"--") {
            break;
        }
        let end = if body.get(to.saturating_sub(2)..to) == Some(b"\r\n") {
            to - 2
        } else {
            to.saturating_sub(1)
        };
        let Some(part) = body.get(start..end.max(start)) else {
            continue;
        };
        let part = part
            .strip_prefix(b"\r\n")
            .or_else(|| part.strip_prefix(b"\n"))
            .unwrap_or(part);
        let (head, bytes) = match memchr::memmem::find(part, b"\r\n\r\n") {
            Some(at) => (
                part.get(..at).unwrap_or_default(),
                part.get(at + 4..).unwrap_or_default(),
            ),
            None => match memchr::memmem::find(part, b"\n\n") {
                Some(at) => (
                    part.get(..at).unwrap_or_default(),
                    part.get(at + 2..).unwrap_or_default(),
                ),
                None => continue,
            },
        };
        let head = String::from_utf8_lossy(head);
        let header = |name: &str| {
            head.lines()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(k, _)| k.trim().eq_ignore_ascii_case(name))
                })
                .map(|(_, v)| v.trim().to_owned())
        };
        let Some(disposition) = header("content-disposition") else {
            continue;
        };
        let Some(name) = parameter(&disposition, "name") else {
            continue;
        };
        if parts.len() >= MAX_PARTS {
            return Err(format!("a form of more than {MAX_PARTS} parts"));
        }
        parts.push(Part {
            name,
            file_name: parameter(&disposition, "filename"),
            media_type: header("content-type"),
            bytes: bytes.to_vec(),
        });
    }
    Ok(parts)
}

/// A name as a part's header holds it: quotes and line ends written as `%22`, `%0D` and `%0A`.
fn quoted(text: &str) -> String {
    text.replace('"', "%22").replace('\r', "%0D").replace('\n', "%0A")
}

/// A part to write: its field's name, and text or a file (its name, its media type, its bytes).
#[derive(Debug, Clone)]
pub(crate) enum Out<'a> {
    Text(&'a str, String),
    File(&'a str, &'a super::FileData),
}

/// A multipart body of these parts: its content type (naming its boundary) and its bytes.
pub(crate) fn write(parts: &[Out<'_>]) -> (String, Vec<u8>) {
    let mut random = [0u8; 12];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut random);
    let mut boundary = format!(
        "openduct-{}",
        random.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
    // A boundary is never in what it separates.
    let holds = |b: &str| {
        parts.iter().any(|p| match p {
            Out::Text(_, t) => t.contains(b),
            Out::File(_, f) => memchr::memmem::find(&f.bytes, b.as_bytes()).is_some(),
        })
    };
    while holds(&boundary) {
        boundary.push('x');
    }
    let mut body = Vec::new();
    for part in parts {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        match part {
            Out::Text(name, text) => {
                body.extend_from_slice(
                    format!(
                        "Content-Disposition: form-data; name=\"{}\"\r\n\r\n",
                        quoted(name)
                    )
                    .as_bytes(),
                );
                body.extend_from_slice(text.as_bytes());
            }
            Out::File(name, file) => {
                body.extend_from_slice(
                    format!(
                        "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\nContent-Type: {}\r\n\r\n",
                        quoted(name),
                        quoted(&file.name),
                        file.media_type
                    )
                    .as_bytes(),
                );
                body.extend_from_slice(&file.bytes);
            }
        }
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn a_form_written_reads_back_byte_for_byte() {
        let photo = super::super::FileData {
            name: "tote \"blue\".jpg".into(),
            media_type: "image/jpeg".into(),
            bytes: Arc::new(vec![0xff, 0xd8, b'\r', b'\n', b'-', b'-', 0, 1, 2]),
        };
        let (kind, body) = write(&[
            Out::Text("title", "Q3\r\nreport".into()),
            Out::File("photo", &photo),
        ]);
        let parts = parse(&kind, &body).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0],
            Part {
                name: "title".into(),
                file_name: None,
                media_type: None,
                bytes: b"Q3\r\nreport".to_vec()
            }
        );
        assert_eq!(parts[1].file_name.as_deref(), Some("tote %22blue%22.jpg"));
        assert_eq!(parts[1].media_type.as_deref(), Some("image/jpeg"));
        assert_eq!(parts[1].bytes, *photo.bytes);
    }

    #[test]
    fn a_form_as_browsers_send_it_reads_and_a_broken_one_is_refused() {
        let raw = b"--XyZ\r\nContent-Disposition: form-data; name=\"q3_email\"\r\n\r\nada@example.com\r\n\
                    --XyZ\r\ncontent-disposition: form-data; name=photo; filename=\"C:\\\\a\\\\x.png\"\r\n\
                    Content-Type: image/png\r\n\r\n\x89PNG\r\n\x1a\n\r\n--XyZ--\r\n";
        let parts = parse("multipart/form-data; boundary=\"XyZ\"", raw).unwrap();
        assert_eq!(parts[0].bytes, b"ada@example.com");
        assert_eq!(parts[1].name, "photo");
        assert_eq!(parts[1].file_name.as_deref(), Some("C:\\a\\x.png"));
        assert_eq!(parts[1].bytes, b"\x89PNG\r\n\x1a\n");
        assert!(parse("multipart/form-data", raw).is_err());
        assert!(parse("application/json", raw).is_err());
        assert!(
            parse("multipart/form-data; boundary=XyZ", b"--XyZ\r\nno headers")
                .unwrap()
                .is_empty()
        );
        let many: Vec<u8> = (0..=MAX_PARTS)
            .flat_map(|i| {
                format!("--b\r\nContent-Disposition: form-data; name=\"f{i}\"\r\n\r\nx\r\n").into_bytes()
            })
            .chain(b"--b--".iter().copied())
            .collect();
        assert!(parse("multipart/form-data; boundary=b", &many).is_err());
    }
}
