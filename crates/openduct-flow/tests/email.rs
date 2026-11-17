// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `email` step, against a stand-in mail server that keeps what it was told: what a message holds and whom it
//! goes to, that a test run and a preview send nothing, and the rules on where and how it may be sent.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use openduct_expr::Value;
use openduct_flow::{
    Credential, DryRun, Effects, Event, Live, Outcome, Preview, RunRecord, Secrets, Workflow, run,
};
use serde_json::{Value as Json, json};

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

/// A mail server that takes one message over plain SMTP and gives back the conversation.
fn mail_server() -> (u16, mpsc::Receiver<String>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (told, heard) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
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
                kept.push_str(&line);
                let command = line.trim_end().to_ascii_uppercase();
                let answer: &[u8] = if command.starts_with("EHLO") || command.starts_with("HELO") {
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
                    b"250 queued as 42\r\n"
                } else if command == "QUIT" {
                    let _ = out.write_all(b"221 bye\r\n");
                    break;
                } else {
                    b"250 ok\r\n"
                };
                let _ = out.write_all(answer);
            }
            let _ = told.send(kept);
        }
    });
    (port, heard)
}

fn credential(port: u16, more: &[(&str, &str)]) -> Credential {
    let mut fields: BTreeMap<String, String> = [
        ("host".to_owned(), "127.0.0.1".to_owned()),
        ("port".to_owned(), port.to_string()),
        ("security".to_owned(), "none".to_owned()),
        ("from".to_owned(), "Shop <shop@example.com>".to_owned()),
    ]
    .into();
    for (k, v) in more {
        fields.insert((*k).to_owned(), (*v).to_owned());
    }
    Credential {
        kind: "smtp".to_owned(),
        fields,
    }
}

fn effects(credential: Credential, allow_private: bool) -> Live {
    Live::new("127.0.0.1:1", allow_private)
        .unwrap()
        .with_secrets(Arc::new(Kept([("mail".to_owned(), credential)].into())))
}

const WORKFLOW: &str = "name: tell\nsteps:\n  - name: mail\n    email:\n      credential: mail\n      to: '{{ value.email }}, ops@example.com'\n      bcc: audit@example.com\n      subject: 'Order {{ value.id }} is on its way'\n      text: 'Hello {{ value.name }}, order {{ value.id }} has shipped.'\n      html: '<p>Hello {{ value.name }}, order <b>{{ value.id }}</b> has shipped.</p>'\n";

fn go(workflow: &str, effects: &dyn Effects) -> RunRecord {
    let event = Event {
        value: Value::parse_json(r#"{"id": "A-1", "name": "Ada", "email": "ada@example.com"}"#).unwrap(),
        details: Value::Null,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&Workflow::parse(workflow).unwrap(), &event, effects))
}

fn result(record: &RunRecord) -> Json {
    serde_json::from_str(&record.steps[0].result.to_json()).unwrap()
}

#[test]
fn a_message_goes_to_its_addresses_with_its_text_and_html_and_the_bcc_out_of_sight() {
    let (port, heard) = mail_server();
    let record = go(WORKFLOW, &effects(credential(port, &[]), true));
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
    let sent = result(&record);
    assert_eq!(sent["sent"], true);
    assert_eq!(sent["to"], json!(["ada@example.com", "ops@example.com"]));
    let told = heard.recv_timeout(Duration::from_secs(10)).unwrap();
    for rcpt in [
        "RCPT TO:<ada@example.com>",
        "RCPT TO:<ops@example.com>",
        "RCPT TO:<audit@example.com>",
    ] {
        assert!(told.contains(rcpt), "{rcpt} in {told}");
    }
    assert!(told.contains("MAIL FROM:<shop@example.com>"), "{told}");
    assert!(told.contains("Subject: Order A-1 is on its way"), "{told}");
    assert!(told.contains("multipart/alternative"), "{told}");
    assert!(told.contains("Hello Ada, order A-1 has shipped."), "{told}");
    assert!(told.contains("<b>A-1</b>"), "{told}");
    assert!(
        !told.to_ascii_lowercase().contains("bcc:"),
        "the bcc is not in the message: {told}"
    );
}

#[test]
fn a_test_run_and_a_preview_show_the_message_and_send_nothing() {
    let (port, heard) = mail_server();
    let tested = go(WORKFLOW, &DryRun::default());
    assert_eq!(tested.outcome, Outcome::Done, "{}", tested.to_json());
    let shown = result(&tested);
    assert_eq!(shown["test"], "not sent");
    assert_eq!(shown["email"]["subject"], "Order A-1 is on its way");
    let previewed = go(
        WORKFLOW,
        &Preview::new(Arc::new(effects(credential(port, &[]), true))),
    );
    assert_eq!(result(&previewed)["test"], "not sent", "{}", previewed.to_json());
    assert!(
        heard.recv_timeout(Duration::from_millis(500)).is_err(),
        "nothing reached the server"
    );
    let wrong = go(
        &WORKFLOW.replace("{{ value.email }}", "not an address"),
        &DryRun::default(),
    );
    let Outcome::Failed { error, .. } = &wrong.outcome else {
        panic!("{}", wrong.to_json())
    };
    assert!(error.contains("not an e-mail address"), "{error}");
}

#[test]
fn a_mail_server_on_this_machines_own_network_is_refused_unless_allowed_and_a_password_needs_tls() {
    let (port, _heard) = mail_server();
    let refused = go(WORKFLOW, &effects(credential(port, &[]), false));
    let Outcome::Failed { error, .. } = &refused.outcome else {
        panic!("{}", refused.to_json())
    };
    assert!(error.contains("--allow-private-network"), "{error}");
    let in_clear = go(
        WORKFLOW,
        &effects(
            credential(port, &[("username", "shop"), ("password", "secret")]),
            true,
        ),
    );
    let Outcome::Failed { error, .. } = &in_clear.outcome else {
        panic!("{}", in_clear.to_json())
    };
    assert!(error.contains("only over TLS"), "{error}");
}
