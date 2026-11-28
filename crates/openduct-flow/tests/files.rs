// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Files between steps: downloaded from a stand-in server, read into rows and text (CSV, JSON, XLSX, XLS, HTML, PDF),
//! made from rows (CSV, JSON, XLSX, text, Base64), sent as a file or a multipart body, attached to an e-mail, kept in
//! a store and read back by a later run - and the limits and the checks on what a workflow says.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use openduct_expr::Value;
use openduct_flow::files::FileStore;
use openduct_flow::{Credential, DryRun, Effects, Event, Live, Outcome, RunRecord, Secrets, Workflow, run};
use serde_json::{Value as Json, json};

const PDF: &[u8] = include_bytes!("fixtures/invoice.pdf");
const XLS: &[u8] = include_bytes!("fixtures/orders.xls");
const XLSX: &[u8] = include_bytes!("fixtures/orders.xlsx");

/// A request the stand-in server was sent: its first line, its headers, its body.
#[derive(Debug, Clone)]
struct Sent {
    line: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// A web server on this machine that answers each path with a file, and keeps every request it is sent.
fn server() -> (String, Arc<Mutex<Vec<Sent>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let kept = Arc::new(Mutex::new(Vec::new()));
    let keeping = Arc::clone(&kept);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut headers = Vec::new();
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h.trim().is_empty() {
                    break;
                }
                let (k, v) = h.split_once(':').unwrap();
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
            }
            let length: usize = headers
                .iter()
                .find(|(k, _)| k == "content-length")
                .map_or(0, |(_, v)| v.parse().unwrap());
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
            keeping.lock().unwrap().push(Sent {
                line: line.trim().to_owned(),
                headers,
                body,
            });
            let (kind, extra, bytes): (&str, &str, Vec<u8>) = match path.as_str() {
                "/invoice.pdf" => ("application/pdf", "", PDF.to_vec()),
                "/orders.xls" => ("application/vnd.ms-excel", "", XLS.to_vec()),
                "/export" => (
                    "application/octet-stream",
                    "content-disposition: attachment; filename=\"orders.xlsx\"\r\n",
                    XLSX.to_vec(),
                ),
                "/orders.csv" => ("text/csv", "", b"id,qty\nA-1,3\nA-2,1\n".to_vec()),
                "/page" => (
                    "text/html",
                    "",
                    b"<html><title>Status</title><p>All <b>good</b></p></html>".to_vec(),
                ),
                "/missing" => ("text/plain", "", b"no such thing".to_vec()),
                _ => ("application/json", "", b"{\"ok\": true}".to_vec()),
            };
            let status = if path == "/missing" {
                "404 Not Found"
            } else {
                "200 OK"
            };
            let head = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {kind}\r\n{extra}content-length: {}\r\nconnection: close\r\n\r\n",
                bytes.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&bytes);
        }
    });
    (address, kept)
}

/// A store that keeps files in memory, as the studio keeps them in the log.
#[derive(Debug, Default)]
struct Memory {
    kept: Mutex<HashMap<String, Vec<u8>>>,
}

impl FileStore for Memory {
    fn keep<'a>(
        &'a self,
        bytes: Arc<Vec<u8>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async move {
            let mut kept = self.kept.lock().unwrap();
            let place = format!("{}.1", kept.len());
            kept.insert(place.clone(), bytes.to_vec());
            Ok(place)
        })
    }

    fn load<'a>(
        &'a self,
        place: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move {
            self.kept
                .lock()
                .unwrap()
                .get(place)
                .cloned()
                .ok_or_else(|| "gone".to_owned())
        })
    }
}

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

fn live(store: &Arc<Memory>) -> Live {
    Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_file_store(Arc::clone(store) as Arc<dyn FileStore>)
}

fn go(workflow: &str, value: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap_or_else(|e| panic!("{e}"));
    let event = Event {
        value: Value::from_json(&value),
        details: Value::Null,
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&workflow, &event, effects))
}

fn result(record: &RunRecord, step: &str) -> Json {
    let found = record
        .steps
        .iter()
        .rev()
        .find(|s| s.name == step)
        .unwrap_or_else(|| panic!("no step {step}"));
    serde_json::from_str(&found.result.to_json()).unwrap()
}

fn done(record: &RunRecord) {
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
}

#[test]
fn downloads_become_files_and_read_as_rows_and_text() {
    let (at, _) = server();
    let store = Arc::new(Memory::default());
    let workflow = format!(
        "name: read\nsteps:\n\
         - name: pdf\n  http: {{ url: '{at}/invoice.pdf' }}\n\
         - name: invoice\n  extract_file: {{ file: steps.pdf.body }}\n\
         - name: xls\n  http: {{ url: '{at}/orders.xls' }}\n\
         - name: old\n  extract_file: {{ file: steps.xls.body, sheet: Orders }}\n\
         - name: export\n  http: {{ url: '{at}/export' }}\n\
         - name: new\n  extract_file: {{ file: steps.export.body, sheet: '{{{{ value.sheet }}}}' }}\n\
         - name: csv\n  http: {{ url: '{at}/orders.csv' }}\n\
         - name: lines\n  extract_file: {{ file: steps.csv.body, format: csv }}\n\
         - name: page\n  http: {{ url: '{at}/page', response: file }}\n\
         - name: status\n  extract_file: {{ file: steps.page.body }}\n\
         - name: json\n  http: {{ url: '{at}/x.json' }}\n\
         - name: info\n  file_details: {{ file: steps.pdf.body, name: 'invoice-2041.pdf' }}\n"
    );
    let record = go(&workflow, json!({ "sheet": "Orders" }), &live(&store));
    done(&record);
    let pdf = result(&record, "pdf");
    assert_eq!(pdf["status"], 200);
    assert_eq!(pdf["body"]["name"], "invoice.pdf");
    assert_eq!(pdf["body"]["type"], "application/pdf");
    assert_eq!(pdf["body"]["size"], PDF.len());
    assert!(pdf["body"]["file"].as_str().unwrap().contains('/'));
    assert_eq!(
        result(&record, "invoice")["text"],
        "Invoice 2041\nTotal due: 1,250.00 EUR\nThank you for your order."
    );
    assert_eq!(result(&record, "invoice")["pages"], 1);

    let old = result(&record, "old");
    assert_eq!(
        old["columns"],
        json!(["id", "customer", "qty", "price", "total", "ordered", "paid"])
    );
    assert_eq!(old["sheets"], json!(["Orders", "Notes"]));
    assert_eq!(
        old["rows"][1],
        json!({ "id": "A-2", "customer": "Émilie du Châtelet", "qty": 1, "price": 1999.99, "total": null,
                "ordered": "2026-01-02", "paid": false })
    );
    // A formula gives the last value the program that saved the file kept; this one's writer keeps none.
    assert_eq!(
        old["rows"][0]["total"],
        Json::Null,
        "a formula without a kept value"
    );
    assert_eq!(
        old["rows"][0]["ordered"], "2026-11-27",
        "a date as its cell shows it"
    );
    let notes = go(
        &format!(
            "name: n\nsteps:\n- name: x\n  http: {{ url: '{at}/orders.xls' }}\n- name: notes\n  extract_file: {{ file: steps.x.body, sheet: Notes, header: false }}\n"
        ),
        json!({}),
        &live(&Arc::new(Memory::default())),
    );
    done(&notes);
    let notes = result(&notes, "notes");
    assert_eq!(
        notes["count"], 300,
        "shared texts read on past the record they start in"
    );
    assert_eq!(
        notes["rows"][299],
        json!([format!("note 299 {}", "x".repeat(40))])
    );

    let new = result(&record, "new");
    assert_eq!(
        result(&record, "export")["body"]["name"],
        "orders.xlsx",
        "named by its answer"
    );
    assert_eq!(new["sheet"], "Orders");
    assert_eq!(new["count"], 2);
    assert_eq!(
        new["rows"][0],
        json!({ "id": "A-1", "customer": "Ada Lovelace", "qty": 3, "price": 12.5, "ordered": "2026-11-27T09:30:00",
                "paid": true })
    );
    assert_eq!(new["rows"][1]["ordered"], "2026-01-02");

    assert_eq!(
        result(&record, "csv")["body"],
        "id,qty\nA-1,3\nA-2,1\n",
        "text stays text"
    );
    assert_eq!(
        result(&record, "lines")["rows"],
        json!([{ "id": "A-1", "qty": "3" }, { "id": "A-2", "qty": "1" }])
    );
    assert_eq!(result(&record, "status")["title"], "Status");
    assert_eq!(result(&record, "status")["text"], "All good");
    assert_eq!(
        result(&record, "json")["body"],
        json!({ "ok": true }),
        "JSON stays JSON"
    );
    let info = result(&record, "info");
    assert_eq!(info["name"], "invoice-2041.pdf");
    assert_eq!(info["extension"], "pdf");
    assert_eq!(info["format"], "pdf");
    assert_eq!(info["sha256"].as_str().unwrap().len(), 64);
    assert_eq!(
        store.kept.lock().unwrap().len(),
        4,
        "each file downloaded is kept once"
    );
}

#[test]
fn files_made_from_rows_are_sent_whole_and_as_a_form() {
    let (at, sent) = server();
    let store = Arc::new(Memory::default());
    let workflow = format!(
        "name: send\nsteps:\n\
         - name: sheet\n  convert: {{ format: xlsx, rows: value.orders, sheet: Orders, name: 'orders-{{{{ value.day }}}}.xlsx' }}\n\
         - name: table\n  convert: {{ format: csv, rows: value.orders, columns: [total, id], delimiter: ';' }}\n\
         - name: data\n  convert: {{ format: json, data: value.orders }}\n\
         - name: note\n  convert: {{ format: text, text: 'Orders of {{{{ value.day }}}}', name: note.txt }}\n\
         - name: logo\n  convert: {{ format: base64, base64: value.logo, name: logo.png }}\n\
         - name: whole\n  http: {{ method: PUT, url: '{at}/upload', file: steps.table }}\n\
         - name: form\n  http:\n    method: POST\n    url: '{at}/forms'\n    multipart:\n      title: 'Q3 {{{{ value.day }}}}'\n      \
           report: {{ file: steps.sheet }}\n      picture: {{ file: steps.logo, name: brand.png }}\n\
         - name: back\n  extract_file: {{ file: steps.sheet }}\n\
         - name: again\n  extract_file: {{ file: steps.table, delimiter: ';' }}\n\
         - name: decoded\n  extract_file: {{ file: steps.data }}\n"
    );
    let orders = json!([{ "id": "A-1", "total": 37.5, "paid": true }, { "id": "A-2", "total": 1999.99, "paid": false }]);
    let logo = "iVBORw0KGgo=";
    let record = go(
        &workflow,
        json!({ "orders": orders, "day": "2026-11-27", "logo": logo }),
        &live(&store),
    );
    done(&record);
    let sheet = result(&record, "sheet");
    assert_eq!(sheet["name"], "orders-2026-11-27.xlsx");
    assert_eq!(
        sheet["type"],
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    assert_eq!(result(&record, "table")["name"], "table.csv");
    assert_eq!(result(&record, "logo")["type"], "image/png");
    assert_eq!(result(&record, "logo")["size"], 8);
    assert_eq!(
        result(&record, "back")["rows"],
        orders,
        "a sheet made reads back as the rows it was made of"
    );
    assert_eq!(result(&record, "again")["columns"], json!(["total", "id"]));
    assert_eq!(result(&record, "decoded")["data"], orders);

    let sent = sent.lock().unwrap().clone();
    let whole = sent.iter().find(|s| s.line.starts_with("PUT /upload")).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&whole.body),
        "total;id\r\n37.5;A-1\r\n1999.99;A-2\r\n"
    );
    assert!(
        whole
            .headers
            .iter()
            .any(|(k, v)| k == "content-type" && v == "text/csv")
    );
    let form = sent.iter().find(|s| s.line.starts_with("POST /forms")).unwrap();
    let kind = &form.headers.iter().find(|(k, _)| k == "content-type").unwrap().1;
    let parts = openduct_flow::files::multipart::parse(kind, &form.body).unwrap();
    assert_eq!(parts.len(), 3);
    assert_eq!(
        (parts[0].name.as_str(), parts[0].bytes.as_slice()),
        ("title", &b"Q3 2026-11-27"[..])
    );
    assert_eq!(parts[1].file_name.as_deref(), Some("orders-2026-11-27.xlsx"));
    assert!(
        parts[1].bytes.starts_with(b"PK"),
        "the sheet's bytes, as they are"
    );
    assert_eq!(parts[2].file_name.as_deref(), Some("brand.png"));
    assert_eq!(
        parts[2].bytes,
        [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']
    );
}

#[test]
fn a_file_kept_by_one_run_is_read_by_the_next_and_a_changed_one_is_refused() {
    let store = Arc::new(Memory::default());
    let made = go(
        "name: make\nsteps:\n  - name: report\n    convert: { format: csv, rows: value.rows }\n",
        json!({ "rows": [{ "a": 1 }] }),
        &live(&store),
    );
    done(&made);
    let file = result(&made, "report");
    assert!(
        file["file"].as_str().unwrap().starts_with("0.1/"),
        "kept in the store: {file}"
    );
    let reader = "name: read\nsteps:\n  - name: rows\n    extract_file: { file: value.upload }\n";
    let read = go(reader, json!({ "upload": file }), &live(&store));
    done(&read);
    assert_eq!(result(&read, "rows")["rows"], json!([{ "a": "1" }]));
    store
        .kept
        .lock()
        .unwrap()
        .insert("0.1".to_owned(), b"a\r\n2\r\n".to_vec());
    let changed = go(reader, json!({ "upload": file }), &live(&store));
    assert!(
        changed.to_json()["error"]
            .as_str()
            .unwrap()
            .contains("not the one kept"),
        "{}",
        changed.to_json()
    );
    let test = go(reader, json!({ "upload": file }), &DryRun::default());
    assert!(
        test.to_json()["error"]
            .as_str()
            .unwrap()
            .contains("cannot be read"),
        "{}",
        test.to_json()
    );
}

#[test]
fn a_test_run_makes_reads_and_shows_files_without_sending_or_keeping_them() {
    let workflow = "name: try\nsteps:\n\
         - name: table\n  convert: { format: csv, rows: value.rows }\n\
         - name: rows\n  extract_file: { file: steps.table }\n\
         - name: post\n  http: { method: POST, url: 'https://example.com/in', file: steps.table }\n\
         - name: inner\n  run_workflow: { workflow: child }\n\
         - name: from_child\n  extract_file: { file: steps.inner.result }\n";
    #[derive(Debug)]
    struct Child;
    impl openduct_flow::Workflows for Child {
        fn definition<'a>(
            &'a self,
            _: &'a str,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async {
                Ok(Value::from_json(&json!({ "name": "child", "steps": [
                    { "name": "made", "convert": { "format": "text", "text": "from the child" } }
                ] })))
            })
        }
    }
    let test = DryRun {
        workflows: Some(Arc::new(Child)),
        ..DryRun::default()
    };
    let record = go(workflow, json!({ "rows": [{ "a": 1 }, { "a": 2 }] }), &test);
    done(&record);
    assert!(
        result(&record, "table")["file"]
            .as_str()
            .unwrap()
            .starts_with("m/"),
        "held by the test run alone"
    );
    assert_eq!(result(&record, "rows")["count"], 2);
    let post = result(&record, "post");
    assert_eq!(post["test"], "not sent");
    assert_eq!(
        post["request"]["body"],
        json!({ "type": "text/csv", "content": "a\r\n1\r\n2\r\n" })
    );
    assert_eq!(
        result(&record, "from_child")["text"],
        "from the child",
        "a file a workflow it ran made"
    );
}

#[test]
fn limits_and_mistakes_are_said_plainly() {
    let many = go(
        "name: many\nsteps:\n  - name: each\n    for_each:\n      items: value.n\n      steps:\n        - name: one\n          \
         convert: { format: text, text: 'x' }\n",
        json!({ "n": (0..25).collect::<Vec<_>>() }),
        &DryRun::default(),
    );
    assert!(
        many.to_json()["error"]
            .as_str()
            .unwrap()
            .contains("at most 20 files"),
        "{}",
        many.to_json()
    );
    let not_a_file = go(
        "name: x\nsteps:\n  - name: rows\n    extract_file: { file: value.n, format: csv }\n",
        json!({ "n": 5 }),
        &DryRun::default(),
    );
    assert!(
        not_a_file.to_json()["error"]
            .as_str()
            .unwrap()
            .contains("is not a file")
    );
    let unknown = go(
        "name: x\nsteps:\n  - name: rows\n    extract_file: { file: value.t }\n",
        json!({ "t": "plain" }),
        &DryRun::default(),
    );
    assert_eq!(
        result(&unknown, "rows")["text"],
        "plain",
        "text given is a text file"
    );
    let wrong = |text: &str| Workflow::parse(text).unwrap_err();
    assert!(wrong("name: x\nsteps:\n  - extract_file: { format: csv }\n").contains("extract_file.file"));
    assert!(
        wrong("name: x\nsteps:\n  - extract_file: { file: value.f, format: docx }\n")
            .contains("format: auto, csv")
    );
    assert!(wrong("name: x\nsteps:\n  - convert: { format: pdf, text: x }\n").contains("csv, json, xlsx"));
    assert!(wrong("name: x\nsteps:\n  - convert: { format: csv, text: x }\n").contains("made of rows"));
    assert!(
        wrong("name: x\nsteps:\n  - convert: { format: csv, rows: value.r, delimiter: ';;' }\n")
            .contains("delimiter")
    );
    assert!(
        wrong("name: x\nsteps:\n  - http: { url: 'https://x', json: {}, file: value.f }\n")
            .contains("one of json")
    );
    assert!(
        wrong("name: x\nsteps:\n  - http: { url: 'https://x', response: binary }\n")
            .contains("auto, file or text")
    );
    assert!(
        wrong("name: x\nsteps:\n  - http: { url: 'https://x', multipart: { a: { name: x } } }\n")
            .contains("file")
    );
    assert!(
        wrong("name: x\nsteps:\n  - file_details: { file: value.f, type: 'not a type' }\n")
            .contains("media type")
    );
}

/// A mail server that takes one message over plain SMTP and gives back what it was told.
fn mail_server() -> (u16, mpsc::Receiver<String>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (told, heard) = mpsc::channel();
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut out = stream.try_clone().unwrap();
        let mut lines = BufReader::new(stream);
        let mut kept = String::new();
        let _ = out.write_all(b"220 stand-in ESMTP\r\n");
        let mut line = String::new();
        loop {
            line.clear();
            if lines.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            let command = line.trim_end().to_ascii_uppercase();
            let answer: &[u8] = if command.starts_with("EHLO") {
                b"250-stand-in\r\n250 8BITMIME\r\n"
            } else if command == "DATA" {
                let _ = out.write_all(b"354 go on\r\n");
                loop {
                    line.clear();
                    if lines.read_line(&mut line).unwrap_or(0) == 0 || line == ".\r\n" {
                        break;
                    }
                    kept.push_str(&line);
                }
                b"250 queued\r\n"
            } else if command == "QUIT" {
                let _ = out.write_all(b"221 bye\r\n");
                break;
            } else {
                b"250 ok\r\n"
            };
            let _ = out.write_all(answer);
        }
        let _ = told.send(kept);
    });
    (port, heard)
}

#[test]
fn an_email_carries_its_attachments() {
    let (port, heard) = mail_server();
    let smtp = Credential {
        kind: "smtp".to_owned(),
        fields: [
            ("host", "127.0.0.1".to_owned()),
            ("port", port.to_string()),
            ("security", "none".to_owned()),
            ("from", "shop@example.com".to_owned()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect(),
    };
    let store = Arc::new(Memory::default());
    let effects = live(&store).with_secrets(Arc::new(Kept([("mail".to_owned(), smtp)].into())));
    let workflow = "name: mail\nsteps:\n\
         - name: sheet\n  convert: { format: csv, rows: value.rows, name: orders.csv }\n\
         - name: note\n  convert: { format: text, text: hello, name: note.txt }\n\
         - name: send\n  email:\n    credential: mail\n    to: ada@example.com\n    subject: Your orders\n    \
           text: Attached.\n    attachments: [steps.sheet, '[steps.note]']\n";
    let record = go(workflow, json!({ "rows": [{ "id": "A-1" }] }), &effects);
    done(&record);
    let told = heard.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(told.contains("multipart/mixed"), "{told}");
    assert!(told.contains("filename=\"orders.csv\""), "{told}");
    assert!(told.contains("filename=\"note.txt\""), "{told}");
    assert!(told.contains("Attached."), "{told}");
    let shown = go(workflow, json!({ "rows": [{ "id": "A-1" }] }), &DryRun::default());
    done(&shown);
    assert_eq!(
        result(&shown, "send")["email"]["attachments"],
        json!([{ "name": "orders.csv", "type": "text/csv", "size": 9 }, { "name": "note.txt", "type": "text/plain", "size": 5 }])
    );
}

#[test]
fn an_answer_that_failed_is_text_to_say_why() {
    let (at, _) = server();
    let store = Arc::new(Memory::default());
    let record = go(
        &format!("name: x\nsteps:\n  - name: get\n    http: {{ url: '{at}/missing', response: file }}\n"),
        json!({}),
        &live(&store),
    );
    assert!(
        record.to_json()["error"]
            .as_str()
            .unwrap()
            .contains("answered 404: no such thing"),
        "{}",
        record.to_json()
    );
    assert!(
        store.kept.lock().unwrap().is_empty(),
        "nothing kept for a failed answer"
    );
}
