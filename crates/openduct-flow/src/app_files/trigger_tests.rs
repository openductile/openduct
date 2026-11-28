// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The logic of app triggers, piece by piece: a poll's choice of what is new, a delivery's checks, and their mistakes.

#![allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]

use openduct_expr::Value;
use ring::hmac;
use serde_json::{Value as Json, json};

use super::AppDef;
use super::hook::{self, Delivery, Replays, Verdict};
use super::poll::{MOST_SEEN, Polling, Seen};
use crate::apps::Credential;

const APP: &str = r#"
app: trig_test
name: Trig
hosts: [api.trig.example]
base_url: https://api.trig.example
sign_in:
  - id: trig_test_key
    label: A key
    fields:
      - { name: key, label: The key, secret: true }
      - { name: signing_secret, label: The signing secret, secret: true, optional: true }
      - { name: verify_token, label: A token the app checks the address with, optional: true }
    headers: { Authorization: "Bearer {{ credential.key }}" }
actions:
  orders:
    label: Orders
    method: GET
    path: /orders
    query: { since: "{{ input.cursor }}" }
    items: value.orders
    next: "value.next == null ? null : {'page': value.next}"
  order:
    label: An order
    method: GET
    path: "/orders/{{ input.id }}"
    inputs:
      - { name: id, label: Id, required: true }
    result: value.order
triggers:
  new_order:
    label: A new order
    poll:
      action: orders
      id: item.id
      newest_first: true
      each: { action: order, input: { id: "{{ item.id }}" } }
      event: "{'id': item.id, 'total': item.total}"
  row:
    label: A new or changed row
    inputs:
      - { name: sheet, label: The sheet, required: true }
    poll:
      method: GET
      path: "/sheets/{{ input.sheet }}"
      items: value.values
      header_row: true
      id: string(item.row_number)
      version: item
      every: 2m
  paid:
    label: A payment (signed with pairs and a time)
    inputs:
      - { name: kinds, label: Which kinds }
    webhook:
      signature: { header: x-sig, pairs: { signature: v1, time: t }, signed: "{time}.{body}" }
      secret: signing_secret
      id: value.id
      only: "input.kinds == null ? true : value.type in input.kinds.split(',')"
  posted:
    label: A message (a prefix, a time header, a challenge)
    webhook:
      signature: { header: x-sign, prefix: "v0=", time_header: x-time, signed: "v0:{time}:{body}" }
      secret: signing_secret
      challenge: { when: "value.type == 'check'", answer: "{'challenge': value.challenge}" }
      events: value.events
      id: item.id
  order_made:
    label: An order (Base64 of the body)
    webhook:
      signature: { header: x-hmac, encoding: base64 }
      secret: [signing_secret]
      ping: "has(value.webhook_id)"
      confirm: { token: "query['hub.verify_token']", field: verify_token, answer: "query['hub.challenge']" }
  bot:
    label: A token
    webhook:
      signature: { header: x-token, algorithm: token }
      secret: signing_secret
"#;

fn app() -> AppDef {
    AppDef::parse(APP, "trig.yaml").unwrap()
}

fn key() -> Credential {
    Credential {
        kind: "trig_test_key".to_owned(),
        fields: [
            ("key".to_owned(), "k1".to_owned()),
            ("signing_secret".to_owned(), "shh".to_owned()),
        ]
        .into_iter()
        .collect(),
    }
}

fn answer(body: Json) -> Value {
    Value::from_json(&json!({ "status": 200, "headers": {}, "body": body }))
}

fn json_of(v: &Value) -> Json {
    serde_json::from_str(&v.to_json()).unwrap()
}

/// A poll driven by hand: each request's address, and the answers given in turn.
fn drive(p: &mut Polling<'_>, answers: Vec<Json>) -> Vec<String> {
    let mut asked = Vec::new();
    for body in answers {
        let r = p.request().unwrap().expect("a request");
        asked.push(format!(
            "{}?{}",
            r.url,
            r.query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&")
        ));
        p.answered(&answer(body)).unwrap();
    }
    assert!(
        p.request().unwrap().is_none(),
        "the poll asked for more than was answered"
    );
    asked
}

#[test]
fn the_first_poll_notes_what_is_there_and_runs_nothing() {
    let app = app();
    let (_, t) = app.trigger(Some("new_order")).unwrap();
    let mut p = Polling::new(&app, t, key(), &json!({}), "w", None, 50, 0).unwrap();
    // Newest first: the first page is all a first poll reads.
    let asked = drive(
        &mut p,
        vec![json!({ "orders": [{ "id": "o3" }, { "id": "o2" }], "next": "p2" })],
    );
    assert_eq!(asked, ["https://api.trig.example/orders?"]);
    let found = p.finish(7).unwrap();
    assert!(found.runs.is_empty());
    assert_eq!(
        found.seen.items,
        [("o2".to_owned(), String::new()), ("o3".to_owned(), String::new())]
    );
    assert_eq!(found.seen.slot, 7);
}

#[test]
fn new_items_run_oldest_first_each_read_in_full_and_listing_stops_at_a_page_seen() {
    let app = app();
    let (_, t) = app.trigger(Some("new_order")).unwrap();
    let seen = Seen {
        items: vec![("o1".into(), String::new()), ("o2".into(), String::new())],
        ..Seen::default()
    };
    let mut p = Polling::new(&app, t, key(), &json!({}), "w", Some(&seen), 50, 0).unwrap();
    let asked = drive(
        &mut p,
        vec![
            json!({ "orders": [{ "id": "o5" }, { "id": "o4" }], "next": "p2" }),
            json!({ "orders": [{ "id": "o3" }, { "id": "o2" }], "next": "p3" }),
            json!({ "order": { "id": "o3", "total": 3 } }),
            json!({ "order": { "id": "o4", "total": 4 } }),
            json!({ "order": { "id": "o5", "total": 5 } }),
        ],
    );
    assert_eq!(asked[1], "https://api.trig.example/orders?page=p2");
    assert_eq!(
        asked[2], "https://api.trig.example/orders/o3?",
        "the oldest new one is read first"
    );
    let found = p.finish(1).unwrap();
    let runs: Vec<Json> = found.runs.iter().map(|(_, v)| json_of(v)).collect();
    assert_eq!(
        runs,
        [
            json!({"id": "o3", "total": 3}),
            json!({"id": "o4", "total": 4}),
            json!({"id": "o5", "total": 5})
        ]
    );
    let ids: Vec<&str> = found.seen.items.iter().map(|(i, _)| i.as_str()).collect();
    assert_eq!(ids, ["o1", "o2", "o3", "o4", "o5"]);
}

#[test]
fn a_poll_takes_at_most_its_limit_and_leaves_the_rest_for_the_next() {
    let app = app();
    let (_, t) = app.trigger(Some("new_order")).unwrap();
    let seen = Seen::default();
    let mut p = Polling::new(&app, t, key(), &json!({}), "w", Some(&seen), 1, 0).unwrap();
    drive(
        &mut p,
        vec![
            json!({ "orders": [{ "id": "b" }, { "id": "a" }] }),
            json!({ "order": { "id": "a", "total": 1 } }),
        ],
    );
    let found = p.finish(1).unwrap();
    assert_eq!(found.runs.len(), 1);
    assert_eq!(
        found.seen.items,
        [("a".to_owned(), String::new())],
        "b is not seen: the next poll takes it"
    );
}

#[test]
fn rows_under_a_header_are_records_and_a_changed_row_runs_again() {
    let app = app();
    let (_, t) = app.trigger(Some("row")).unwrap();
    assert_eq!(t.polls().unwrap().every.as_secs(), 120);
    let sheet = |rows: Json| json!({ "values": rows });
    let first = vec![sheet(json!([["Name", "Qty"], ["tea", "1"], ["jam"]]))];
    let mut p = Polling::new(&app, t, key(), &json!({ "sheet": "S1" }), "w", None, 50, 0).unwrap();
    assert_eq!(drive(&mut p, first), ["https://api.trig.example/sheets/S1?"]);
    let seen = p.finish(1).unwrap().seen;
    assert_eq!(seen.items.len(), 2);

    // Row 3 changed, row 4 is new; row 2 is as it was.
    let mut p = Polling::new(&app, t, key(), &json!({ "sheet": "S1" }), "w", Some(&seen), 50, 0).unwrap();
    drive(
        &mut p,
        vec![sheet(json!([
            ["Name", "Qty"],
            ["tea", "1"],
            ["jam", "2"],
            ["oil", "3"]
        ]))],
    );
    let found = p.finish(2).unwrap();
    let runs: Vec<Json> = found.runs.iter().map(|(_, v)| json_of(v)).collect();
    assert_eq!(
        runs,
        [
            json!({ "row_number": 3, "Name": "jam", "Qty": "2" }),
            json!({ "row_number": 4, "Name": "oil", "Qty": "3" })
        ]
    );
    // Nothing changed since: nothing runs.
    let mut p = Polling::new(
        &app,
        t,
        key(),
        &json!({ "sheet": "S1" }),
        "w",
        Some(&found.seen),
        50,
        0,
    )
    .unwrap();
    drive(
        &mut p,
        vec![sheet(json!([
            ["Name", "Qty"],
            ["tea", "1"],
            ["jam", "2"],
            ["oil", "3"]
        ]))],
    );
    assert!(p.finish(3).unwrap().runs.is_empty());
}

#[test]
fn what_is_seen_is_kept_to_its_most_the_newest_kept() {
    let app = app();
    let (_, t) = app.trigger(Some("new_order")).unwrap();
    let old: Vec<(String, String)> = (0..MOST_SEEN)
        .map(|i| (format!("old{i}"), String::new()))
        .collect();
    let seen = Seen {
        items: old,
        ..Seen::default()
    };
    let mut p = Polling::new(&app, t, key(), &json!({}), "w", Some(&seen), 50, 0).unwrap();
    drive(
        &mut p,
        vec![
            json!({ "orders": [{ "id": "new" }] }),
            json!({ "order": { "id": "new", "total": 1 } }),
        ],
    );
    let found = p.finish(1).unwrap();
    assert_eq!(found.seen.items.len(), MOST_SEEN);
    assert_eq!(found.seen.items.last().unwrap().0, "new");
    assert_eq!(found.seen.items[0].0, "old1", "the oldest one goes");
}

fn signed_hex(secret: &str, message: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::sign(&key, message.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn delivery<'a>(headers: &'a [(String, String)], body: &'a str) -> Delivery<'a> {
    Delivery {
        method: "POST",
        address: "https://studio.example/hooks/h",
        headers,
        query: "",
        content_type: "application/json",
        body: body.as_bytes(),
    }
}

fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn a_signed_delivery_runs_and_an_unsigned_old_or_forged_one_is_refused() {
    let app = app();
    let (_, t) = app.trigger(Some("paid")).unwrap();
    let body = r#"{"id":"evt_1","type":"paid"}"#;
    let now = 1_700_000_000_000;
    let good = format!(
        "t=1700000000,v1={}",
        signed_hex("shh", &format!("1700000000.{body}"))
    );
    let ok = hook::check(
        t,
        Some("shh"),
        &json!({}),
        &delivery(&h(&[("x-sig", &good)]), body),
        now,
    )
    .unwrap();
    let Verdict::Run(runs) = ok else { panic!("{ok:?}") };
    assert_eq!(runs[0].0.as_deref(), Some("evt_1"));

    // Unsigned; signed with another secret; the body changed; signed too long ago; the time changed.
    let refused = |headers: Vec<(String, String)>, body: &str, at: i64| {
        hook::check(t, Some("shh"), &json!({}), &delivery(&headers, body), at).unwrap_err()
    };
    assert!(refused(vec![], body, now).reason.contains("not signed"));
    let other = format!(
        "t=1700000000,v1={}",
        signed_hex("guess", &format!("1700000000.{body}"))
    );
    assert!(
        refused(h(&[("x-sig", &other)]), body, now)
            .reason
            .contains("does not match")
    );
    assert!(
        refused(h(&[("x-sig", &good)]), r#"{"id":"evt_1","type":"refund"}"#, now)
            .reason
            .contains("does not match")
    );
    assert!(
        refused(h(&[("x-sig", &good)]), body, now + 301_000)
            .reason
            .contains("too long ago")
    );
    let moved = good.replace("t=1700000000", "t=1700000100");
    assert!(
        refused(h(&[("x-sig", &moved)]), body, now)
            .reason
            .contains("does not match")
    );
    assert_eq!(refused(vec![], body, now).status, 401);
    // No secret yet: refused, saying how to give one.
    let none = hook::check(t, None, &json!({}), &delivery(&h(&[("x-sig", &good)]), body), now).unwrap_err();
    assert!(none.reason.contains("no signing secret"));
    // A kind the workflow did not ask for is answered and passed.
    let passed = hook::check(
        t,
        Some("shh"),
        &json!({ "kinds": "refund" }),
        &delivery(&h(&[("x-sig", &good)]), body),
        now,
    );
    assert!(matches!(passed, Ok(Verdict::Pass(_))));
}

#[test]
fn a_challenge_is_answered_and_a_delivery_s_events_each_run() {
    let app = app();
    let (_, t) = app.trigger(Some("posted")).unwrap();
    let sign = |body: &str| {
        h(&[
            (
                "x-sign",
                &format!("v0={}", signed_hex("shh", &format!("v0:1700000000:{body}"))),
            ),
            ("x-time", "1700000000"),
        ])
    };
    let now = 1_700_000_000_000;
    let check = r#"{"type":"check","challenge":"abc"}"#;
    let got = hook::check(t, Some("shh"), &json!({}), &delivery(&sign(check), check), now).unwrap();
    assert!(matches!(got, Verdict::Answer(a) if a == json!({ "challenge": "abc" })));
    // An unsigned challenge is refused like anything else.
    assert!(hook::check(t, Some("shh"), &json!({}), &delivery(&[], check), now).is_err());

    let two = r#"{"type":"events","events":[{"id":"a"},{"id":"b"}]}"#;
    let Verdict::Run(runs) =
        hook::check(t, Some("shh"), &json!({}), &delivery(&sign(two), two), now).unwrap()
    else {
        panic!("no runs")
    };
    let ids: Vec<Option<String>> = runs.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(ids, [Some("a".to_owned()), Some("b".to_owned())]);
}

#[test]
fn base64_signatures_pings_tokens_and_a_get_that_checks_the_address() {
    let app = app();
    let (_, t) = app.trigger(Some("order_made")).unwrap();
    let body = r#"{"id":1}"#;
    let signer = hmac::Key::new(hmac::HMAC_SHA256, b"shh");
    let tag = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        hmac::sign(&signer, body.as_bytes()),
    );
    let ok = hook::check(
        t,
        Some("shh"),
        &json!({}),
        &delivery(&h(&[("x-hmac", &tag)]), body),
        0,
    )
    .unwrap();
    assert!(matches!(ok, Verdict::Run(_)));
    // An unsigned ping is answered, and runs nothing; an unsigned delivery that is not one is refused.
    let mut ping = delivery(&[], "webhook_id=7");
    ping.content_type = "application/x-www-form-urlencoded";
    assert!(matches!(
        hook::check(t, Some("shh"), &json!({}), &ping, 0),
        Ok(Verdict::Pass(_))
    ));
    assert!(hook::check(t, Some("shh"), &json!({}), &delivery(&[], body), 0).is_err());

    // The GET some apps check an address with: answered only with the credential's token.
    let mut credential = key();
    credential.fields.insert("verify_token".into(), "tok".into());
    let q = "hub.mode=subscribe&hub.verify_token=tok&hub.challenge=1158201444";
    assert_eq!(hook::confirm(t, &credential, q).unwrap(), "1158201444");
    assert_eq!(
        hook::confirm(t, &credential, &q.replace("=tok", "=bad"))
            .unwrap_err()
            .status,
        403
    );

    let (_, bot) = app.trigger(Some("bot")).unwrap();
    assert!(
        hook::check(
            bot,
            Some("shh"),
            &json!({}),
            &delivery(&h(&[("x-token", "shh")]), body),
            0
        )
        .is_ok()
    );
    assert!(
        hook::check(
            bot,
            Some("shh"),
            &json!({}),
            &delivery(&h(&[("x-token", "sh")]), body),
            0
        )
        .is_err()
    );
}

#[test]
fn a_delivery_seen_again_is_known_and_the_secret_kept_for_a_workflow_comes_first() {
    let replays = Replays::default();
    assert!(replays.first_time("w/evt_1", 0));
    assert!(!replays.first_time("w/evt_1", 1_000));
    assert!(replays.first_time("w/evt_2", 1_000));
    // A day later it is forgotten.
    assert!(replays.first_time("w/evt_1", 25 * 3_600_000));

    let app = app();
    let (_, t) = app.trigger(Some("paid")).unwrap();
    let mut c = key();
    assert_eq!(
        hook::secret_of(t.hook().unwrap(), &c, "w1").as_deref(),
        Some("shh")
    );
    c.fields
        .insert(hook::kept_name("signing_secret", "w1"), "kept".into());
    assert_eq!(
        hook::secret_of(t.hook().unwrap(), &c, "w1").as_deref(),
        Some("kept")
    );
    assert_eq!(
        hook::secret_of(t.hook().unwrap(), &c, "w2").as_deref(),
        Some("shh"),
        "another workflow's is not its"
    );
}

#[test]
fn a_trigger_s_mistakes_are_found_when_the_app_is_read() {
    let wrong = |from: &str, to: &str| AppDef::parse(&APP.replace(from, to), "trig.yaml").unwrap_err();
    assert!(
        wrong("      id: item.id\n      newest_first", "      newest_first")
            .contains("triggers.new_order.poll.id")
    );
    assert!(wrong("action: orders\n      id", "action: order\n      id").contains("polls a list"));
    assert!(wrong("{ action: order, input", "{ action: orders, input").contains("reads one item"));
    assert!(wrong("signed: \"{time}.{body}\"", "signed: \"{when}.{body}\"").contains("{when}"));
    assert!(
        wrong("prefix: \"v0=\", time_header: x-time, ", "prefix: \"v0=\", ").contains("no header gives one")
    );
    assert!(
        wrong(
            "      secret: signing_secret\n      id: value.id",
            "      secret: nowhere\n      id: value.id"
        )
        .contains("nowhere")
    );
    assert!(wrong("      every: 2m", "      every: 10s").contains("every"));
    assert!(wrong("  bot:\n    label: A token\n    webhook:\n      signature: { header: x-token, algorithm: token }\n", "  bot:\n    label: A token\n    webhook:\n").contains("signature"));
    assert!(
        wrong(
            "      - { name: sheet, label: The sheet, required: true }",
            "      - { name: cursor, label: The sheet }"
        )
        .contains("name of the trigger's own")
    );
}

#[test]
fn a_recording_that_does_not_hold_says_what_differs() {
    let app = app();
    let poll = r#"
trigger: new_order
credential: { key: k1 }
seen: { items: [["o1", ""]] }
exchanges:
  - answer: { body: { orders: [{ id: o2 }, { id: o1 }] } }
  - answer: { body: { order: { id: o2, total: 2 } } }
runs: [{ id: o2, total: 3 }]
"#;
    let e = super::recorded::check(&app, poll, "t.yaml").unwrap_err();
    assert!(e.contains("runs: it started"), "{e}");
    assert!(super::recorded::check(&app, &poll.replace("total: 3 }]", "total: 2 }]"), "t.yaml").is_ok());
    let short = poll.replace("  - answer: { body: { order: { id: o2, total: 2 } } }\n", "");
    assert!(
        super::recorded::check(&app, &short, "t.yaml")
            .unwrap_err()
            .contains("no more exchanges")
    );
}

#[test]
fn a_workflow_names_an_app_s_trigger_and_its_mistakes_are_said() {
    let ok = crate::Workflow::parse(
        "name: Bot\ntrigger:\n  telegram:\n    when: new_message\n    credential: bot\n    every: 2m\n    max_items: 10\n",
    )
    .unwrap();
    let crate::Trigger::App(t) = &ok.trigger else {
        panic!("{:?}", ok.trigger)
    };
    assert_eq!(
        (
            t.app.as_str(),
            t.when.as_str(),
            t.every.map(|d| d.as_secs()),
            t.max_items
        ),
        ("telegram", "new_message", Some(120), 10)
    );
    let hook =
        crate::Workflow::parse("name: Bot\ntrigger: { telegram: { when: message, credential: bot } }\n")
            .unwrap();
    assert!(matches!(&hook.trigger, crate::Trigger::App(t) if t.every.is_none()));

    let wrong = |trigger: &str| {
        crate::Workflow::parse(&format!("name: Bot\ntrigger: {{ {trigger} }}\n")).unwrap_err()
    };
    assert!(wrong("telegram: { when: new_mesage, credential: bot }").contains("has no trigger"));
    assert!(wrong("telegram: { when: new_message }").contains("credential"));
    assert!(wrong("telegram: { when: new_message, credential: bot, every: 10s }").contains("from 1m to 1d"));
    assert!(wrong("telegram: { when: message, credential: bot, every: 5m }").contains("it is not asked"));
    assert!(
        wrong("telegram: { when: message, credential: bot, max_items: 5 }")
            .contains("only a trigger that asks")
    );
    assert!(wrong("telegram: { when: new_message, credential: bot, max_items: 501 }").contains("1 to 500"));
    assert!(wrong("telegram: { when: new_message, credential: bot, colour: red }").contains("not a setting"));
    assert!(wrong("telegram: { credential: bot }").contains("which of Telegram's triggers"));

    super::add(APP, "trig.yaml").unwrap();
    assert!(wrong("trig_test: { when: row, credential: c }").contains("needs it"));
    assert!(
        wrong("trig_test: { when: row, credential: c, sheet: \"{{ value.x }}\" }")
            .contains("settings are values")
    );
    assert!(wrong("trig_test: { when: row, credential: c, sheet: [1] }").contains("a value as text"));
}

#[test]
fn every_app_s_trigger_recordings_hold_and_each_trigger_has_one() {
    let apps = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../apps");
    // A built-in app that does not read is left out (and said in the log): here it is a failure, with why.
    let unread: Vec<String> = std::fs::read_dir(&apps)
        .unwrap()
        .flatten()
        .map(|e| e.path().join("app.yaml"))
        .filter(|p| p.is_file())
        .filter_map(|p| AppDef::parse(&std::fs::read_to_string(&p).unwrap(), &p.display().to_string()).err())
        .collect();
    assert!(unread.is_empty(), "apps that do not read:\n{}", unread.join("\n"));
    let (mut ran, mut failed, mut untested) = (0, Vec::new(), Vec::new());
    for app in super::all() {
        let Ok(entries) = std::fs::read_dir(apps.join(&app.id).join("tests")) else {
            continue;
        };
        let mut tried = std::collections::BTreeSet::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if path.extension().is_none_or(|e| e != "yaml")
                || !text.contains("\ntrigger: ") && !text.starts_with("trigger: ")
            {
                continue;
            }
            if let Some(t) = text.lines().find_map(|l| l.strip_prefix("trigger: ")) {
                tried.insert(t.trim().to_owned());
            }
            ran += 1;
            if let Err(e) = super::recorded::check(app, &text, &path.display().to_string()) {
                failed.push(e);
            }
        }
        untested.extend(
            app.triggers
                .iter()
                .filter(|(n, _)| !tried.contains(n))
                .map(|(n, _)| format!("{}.{n}", app.id)),
        );
    }
    assert!(
        failed.is_empty(),
        "{} of {ran} trigger recordings do not hold:\n{}",
        failed.len(),
        failed.join("\n")
    );
    assert!(untested.is_empty(), "triggers with no recording: {untested:?}");
    assert!(ran >= 20, "only {ran} trigger recordings found");
}
