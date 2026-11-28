// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Files in `openduct start`: a webhook form's upload kept and given to the run, read into rows, made into a
//! spreadsheet, shown in the run's record and downloaded by a member - and what is refused: a forged file, a test
//! run's file, too many files in one delivery, a stranger.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::{Value, json};
use studio::{call, owner, start};

/// One request with a body of bytes; the answer's status, headers and body as they came.
fn raw(addr: &str, method: &str, path: &str, headers: &[String], body: &[u8]) -> (u16, String, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(40))).unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    for h in headers {
        head.push_str(h);
        head.push_str("\r\n");
    }
    head.push_str(&format!("content-length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).unwrap();
    let at = answer.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&answer[..at]).into_owned();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, head, answer[at + 4..].to_vec())
}

/// A form with a text field and these files (field, file name, type, bytes).
fn form(files: &[(&str, &str, &str, &[u8])]) -> (String, Vec<u8>) {
    let mut body = b"--b0und\r\nContent-Disposition: form-data; name=\"note\"\r\n\r\nQ3 orders\r\n".to_vec();
    for (field, name, kind, bytes) in files {
        body.extend_from_slice(
            format!(
                "--b0und\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{name}\"\r\n\
                 Content-Type: {kind}\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--b0und--\r\n");
    (
        "content-type: multipart/form-data; boundary=b0und".to_owned(),
        body,
    )
}

#[test]
fn an_upload_is_read_made_a_spreadsheet_and_downloaded_from_the_run() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let definition = json!({
        "name": "Orders sheet",
        "trigger": { "webhook": {} },
        "steps": [
            { "name": "rows", "extract_file": { "file": "value.orders" } },
            { "name": "sheet", "convert": { "format": "xlsx", "rows": "steps.rows.rows", "name": "orders.xlsx" } },
            { "name": "check", "extract_file": { "file": "steps.sheet" } },
            { "name": "answer", "respond": { "body": {
                "note": "{{ value.note }}", "count": "{{ steps.check.count }}", "sheet": "{{ steps.sheet }}",
                "upload": "{{ value.orders }}" } } },
        ],
    });
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    let hook = made.body["hook"].as_str().unwrap().to_owned();
    let on = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);

    let csv: &[u8] = b"id,qty\nA-1,3\nA-2,1\n";
    let (kind, body) = form(&[("orders", "orders.csv", "text/csv", csv)]);
    let answered = raw(&studio, "POST", &hook, &[kind], &body);
    assert_eq!(answered.0, 200, "{}", String::from_utf8_lossy(&answered.2));
    let answer: Value = serde_json::from_slice(&answered.2).unwrap();
    assert_eq!(answer["note"], "Q3 orders");
    assert_eq!(answer["count"], 2);
    assert_eq!(answer["upload"]["name"], "orders.csv");
    assert_eq!(answer["upload"]["type"], "text/csv");
    assert_eq!(answer["upload"]["size"], csv.len());
    let sheet = answer["sheet"].clone();
    assert_eq!(sheet["name"], "orders.xlsx");

    // The run's record names its files; a member downloads them, always as a download.
    let runs = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{id}/runs"),
        None,
    );
    let run = &runs.body["runs"][0];
    assert_eq!(run["status"], "done", "{run}");
    assert_eq!(run["steps"][1]["result"], sheet);
    assert_eq!(run["event"]["orders"]["name"], "orders.csv");
    let file = sheet["file"].as_str().unwrap();
    let headers: Vec<String> = signed
        .iter()
        .filter(|h| h.starts_with("cookie"))
        .cloned()
        .collect();
    let path = format!(
        "/api/runs/file?file={}&name=orders.xlsx",
        file.replace('/', "%2F")
    );
    let (status, head, bytes) = raw(&studio, "GET", &path, &headers, b"");
    assert_eq!(status, 200, "{head}");
    assert!(bytes.starts_with(b"PK"), "a spreadsheet's bytes");
    assert_eq!(bytes.len(), sheet["size"].as_u64().unwrap() as usize);
    let head = head.to_ascii_lowercase();
    assert!(head.contains("content-type: application/octet-stream"), "{head}");
    assert!(
        head.contains("content-disposition: attachment; filename=\"orders.xlsx\""),
        "{head}"
    );
    let upload = answer["upload"]["file"].as_str().unwrap().replace('/', "%2F");
    let (status, _, bytes) = raw(
        &studio,
        "GET",
        &format!("/api/runs/file?file={upload}&name=x"),
        &headers,
        b"",
    );
    assert_eq!(
        (status, bytes.as_slice()),
        (200, csv),
        "the upload, byte for byte"
    );

    // A forged file, a test run's file, a stranger: refused.
    let forged = format!("{}%2F{}", file.split('/').next().unwrap(), "0".repeat(64));
    assert_eq!(
        raw(
            &studio,
            "GET",
            &format!("/api/runs/file?file={forged}"),
            &headers,
            b""
        )
        .0,
        404
    );
    let memory = format!("m%2F{}", "0".repeat(64));
    assert_eq!(
        raw(
            &studio,
            "GET",
            &format!("/api/runs/file?file={memory}"),
            &headers,
            b""
        )
        .0,
        404
    );
    assert_eq!(raw(&studio, "GET", &path, &[], b"").0, 401);

    // Too many files in one delivery.
    let many: Vec<(&str, &str, &str, &[u8])> =
        (0..11).map(|_| ("f", "x.txt", "text/plain", &b"x"[..])).collect();
    let (kind, body) = form(&many);
    let (status, _, said) = raw(&studio, "POST", &hook, &[kind], &body);
    assert_eq!(status, 422, "{}", String::from_utf8_lossy(&said));
    assert!(String::from_utf8_lossy(&said).contains("at most 10 files"));
}
