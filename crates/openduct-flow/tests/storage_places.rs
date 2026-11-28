// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The storage step on the places reached over a vendor's API - Azure Blob Storage, Google Cloud Storage, Dropbox, Box,
//! OneDrive and SharePoint, Google Drive - against stand-ins that answer as each vendor documents (see
//! `places_support`): every action on each, the same results as on any other place; signatures and tokens checked by
//! the stand-ins; a token that has run out refreshed and kept; and what a test run and a preview do instead, and
//! what is refused. No real account is reached.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::type_complexity
)]

mod places_support;

use std::sync::Arc;

use openduct_expr::Value;
use openduct_flow::{DryRun, Effects, Event, Outcome, Preview, RunRecord, Workflow, run};
use places_support::{Kept, Stand, credential, effects, fetch};
use serde_json::{Value as Json, json};

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
        .unwrap_or_else(|| panic!("no step {step}: {}", record.to_json()));
    serde_json::from_str(&found.result.to_json()).unwrap()
}

fn done(record: &RunRecord) {
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
}

fn failed(record: &RunRecord) -> String {
    match &record.outcome {
        Outcome::Failed { error, .. } => error.clone(),
        other => panic!("not failed: {other:?} {}", record.to_json()),
    }
}

/// The same steps on any place: `place` is `credential: x` (and what else the place takes).
fn every_action(place: &str) -> String {
    format!(
        "name: cycle\nsteps:\n\
         - name: report\n  convert: {{ format: csv, rows: value.rows, name: orders.csv }}\n\
         - name: put\n  storage: {{ action: write, {place}, path: in/orders.csv, file: steps.report }}\n\
         - name: again\n  storage: {{ action: write, {place}, path: in/orders.csv, file: steps.report }}\n  on_error: continue\n\
         - name: replace\n  storage: {{ action: write, {place}, path: in/orders.csv, file: \"'id\\\\n9\\\\n'\", overwrite: true }}\n\
         - name: note\n  storage: {{ action: write, {place}, path: in/deep/note.txt, file: \"'hello'\" }}\n\
         - name: listing\n  storage: {{ action: list, {place}, path: in }}\n\
         - name: csvs\n  storage: {{ action: list, {place}, path: in, pattern: '*.CSV', recursive: true }}\n\
         - name: everything\n  storage: {{ action: list, {place}, path: in, recursive: true }}\n\
         - name: got\n  storage: {{ action: read, {place}, path: in/orders.csv }}\n\
         - name: rows\n  extract_file: {{ file: steps.got }}\n\
         - name: copied\n  storage: {{ action: copy, {place}, path: in/orders.csv, to: archive/orders-copy.csv }}\n\
         - name: moved\n  storage: {{ action: move, {place}, path: in/orders.csv, to: 'done/{{{{ value.day }}}}.csv' }}\n\
         - name: made\n  storage: {{ action: make_folder, {place}, path: out/reports }}\n\
         - name: made_again\n  storage: {{ action: make_folder, {place}, path: out/reports }}\n\
         - name: gone\n  storage: {{ action: delete, {place}, path: 'done/{{{{ value.day }}}}.csv' }}\n\
         - name: gone_again\n  storage: {{ action: delete, {place}, path: 'done/{{{{ value.day }}}}.csv' }}\n\
         - name: missing\n  storage: {{ action: read, {place}, path: in/orders.csv }}\n  on_error: continue\n"
    )
}

/// What `every_action` should have done, by its results.
fn check_every_action(record: &RunRecord) {
    done(record);
    assert_eq!(result(record, "put")["written"], true);
    let again = result(record, "again")["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(again.contains("set overwrite to replace it"), "{again}");
    assert_eq!(result(record, "replace")["written"], true);
    let names: Vec<String> = result(record, "listing")["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| format!("{}:{}", f["name"].as_str().unwrap(), f["kind"].as_str().unwrap()))
        .collect();
    assert_eq!(names, ["deep:folder", "orders.csv:file"], "{}", record.to_json());
    let csvs = result(record, "csvs");
    assert_eq!(csvs["count"], 1, "{csvs}");
    assert_eq!(csvs["files"][0]["path"], "in/orders.csv");
    let all: Vec<String> = result(record, "everything")["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect();
    assert!(all.contains(&"in/deep/note.txt".to_owned()), "{all:?}");
    let got = result(record, "got");
    assert_eq!(
        (got["name"].as_str(), got["type"].as_str(), got["path"].as_str()),
        (Some("orders.csv"), Some("text/csv"), Some("in/orders.csv"))
    );
    assert_eq!(result(record, "rows")["rows"], json!([{ "id": "9" }]));
    assert_eq!(result(record, "copied")["copied"], true);
    assert_eq!(result(record, "moved")["to"], "done/2026-11-27.csv");
    assert_eq!(result(record, "made")["made"], true);
    assert_eq!(result(record, "made_again")["made"], false);
    assert_eq!(result(record, "gone")["deleted"], true);
    assert_eq!(result(record, "gone_again")["deleted"], false);
    let missing = result(record, "missing")["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(missing.contains("nothing at in/orders.csv"), "{missing}");
}

fn sample() -> Json {
    json!({ "day": "2026-11-27", "rows": [{ "id": "A-1" }, { "id": "A-2" }] })
}

fn list_of(place: &str) -> String {
    format!("name: x\nsteps:\n  - name: l\n    storage: {{ action: list, {place} }}\n")
}

#[test]
fn azure_blobs_are_listed_read_written_copied_deleted_and_shared_by_a_signed_address() {
    let stand = Stand::start();
    let endpoint = stand.azure_endpoint();
    let account = places_support::azure::ACCOUNT;
    let blob = |key: (&str, &str)| {
        credential(
            "azure_blob",
            &[
                ("account", account),
                key,
                ("container", "orders"),
                ("endpoint", endpoint.as_str()),
            ],
        )
    };
    let secrets = Kept::with(vec![
        ("lake", blob(("shared_key", places_support::azure::KEY))),
        ("by_sas", blob(("sas_token", places_support::azure::SAS))),
        ("wrong", blob(("shared_key", "d3Jvbmcta2V5"))),
    ]);
    let mut workflow = every_action("credential: lake");
    workflow.push_str("- name: link\n  storage: { action: presign, credential: lake, path: archive/orders-copy.csv, expires: 10m }\n");
    let record = go(&workflow, sample(), &effects(&stand, &secrets, true));
    check_every_action(&record);
    {
        let azure = stand.azure.lock().unwrap();
        assert_eq!(azure.refused, 0, "every request signed as Azure checks it");
        assert!(
            azure.blobs.contains_key("out/reports/"),
            "a folder is an empty blob named for it"
        );
        assert_eq!(azure.blobs["archive/orders-copy.csv"].0, b"id\n9\n");
    }
    let link = result(&record, "link");
    assert_eq!(
        fetch(&stand, link["url"].as_str().unwrap()),
        (200, b"id\n9\n".to_vec()),
        "{link}"
    );
    // A SAS token instead of the key: it goes with each request.
    let record = go(
        &list_of("credential: by_sas, path: archive"),
        json!({}),
        &effects(&stand, &secrets, true),
    );
    done(&record);
    assert_eq!(result(&record, "l")["count"], 1);
    let record = go(
        "name: x\nsteps:\n  - name: p\n    storage: { action: presign, credential: by_sas, path: archive/orders-copy.csv }\n",
        json!({}),
        &effects(&stand, &secrets, true),
    );
    assert!(failed(&record).contains("needs the account's shared_key"));
    // A wrong key, Azure's own word for it; this machine's network without the installation's leave.
    let record = go(
        &list_of("credential: wrong"),
        json!({}),
        &effects(&stand, &secrets, true),
    );
    assert!(
        failed(&record).contains("AuthenticationFailed"),
        "{}",
        record.to_json()
    );
    let record = go(
        &list_of("credential: lake"),
        json!({}),
        &effects(&stand, &secrets, false),
    );
    assert!(
        failed(&record).contains("--allow-private-network"),
        "{}",
        record.to_json()
    );
}

#[test]
fn google_cloud_storage_takes_a_service_account_s_key_and_signs_v4_addresses() {
    let stand = Stand::start();
    let key = places_support::gcs::key_file();
    let secrets = Kept::with(vec![
        (
            "gcs",
            credential("gcs", &[("key_file", key.as_str()), ("bucket", "orders")]),
        ),
        (
            "broken",
            credential(
                "gcs",
                &[
                    ("key_file", "{\"client_email\": \"a@b.example\"}"),
                    ("bucket", "orders"),
                ],
            ),
        ),
    ]);
    let mut workflow = every_action("credential: gcs");
    workflow.push_str("- name: link\n  storage: { action: presign, credential: gcs, path: archive/orders-copy.csv, expires: 1h }\n");
    let record = go(&workflow, sample(), &effects(&stand, &secrets, true));
    check_every_action(&record);
    {
        let gcs = stand.gcs.lock().unwrap();
        assert_eq!(gcs.jwt_refused, 0, "the JWT is one Google takes");
        assert!(gcs.tokens_given >= 1);
        assert!(gcs.objects.contains_key("out/reports/"));
    }
    let link = result(&record, "link");
    assert!(link["url"].as_str().unwrap().starts_with(
        "https://storage.googleapis.com/orders/archive/orders-copy.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256"
    ));
    assert_eq!(
        fetch(&stand, link["url"].as_str().unwrap()),
        (200, b"id\n9\n".to_vec()),
        "{link}"
    );
    let record = go(
        &list_of("credential: broken"),
        json!({}),
        &effects(&stand, &secrets, true),
    );
    assert!(failed(&record).contains("key_file"), "{}", record.to_json());
}

fn dropbox(token: &str) -> openduct_flow::Credential {
    credential(
        "dropbox",
        &[
            ("access_token", token),
            ("refresh_token", places_support::dropbox::REFRESH),
            ("expires_at", "9999999999"),
        ],
    )
}

#[test]
fn dropbox_files_are_reached_with_a_connected_account_whose_token_is_refreshed() {
    let stand = Stand::start();
    let secrets = Kept::with(vec![("box_of_files", dropbox(places_support::dropbox::OLD))]);
    let record = go(
        &every_action("credential: box_of_files"),
        sample(),
        &effects(&stand, &secrets, true),
    );
    check_every_action(&record);
    assert_eq!(
        stand.dropbox.lock().unwrap().refreshed,
        1,
        "the token that ran out was refreshed once"
    );
    assert_eq!(
        secrets.field("box_of_files", "access_token").as_deref(),
        Some(places_support::dropbox::TOKEN),
        "and kept"
    );
    // A name beyond ASCII goes in Dropbox's header as JSON that is safe there; a folder by its id.
    let folder = stand.dropbox.lock().unwrap().folders("/Team/Inbox");
    let workflow = format!(
        "name: x\nsteps:\n\
         - name: w\n  storage: {{ action: write, credential: box_of_files, path: /Team/Inbox/Über.csv, file: \"'a'\" }}\n\
         - name: r\n  storage: {{ action: read, credential: box_of_files, folder_id: 'id:{folder}', path: Über.csv }}\n\
         - name: l\n  storage: {{ action: list, credential: box_of_files, folder_id: 'id:{folder}' }}\n"
    );
    let record = go(&workflow, json!({}), &effects(&stand, &secrets, true));
    done(&record);
    assert_eq!(
        result(&record, "r")["path"],
        "Über.csv",
        "paths inside the folder_id's folder"
    );
    assert_eq!(result(&record, "l")["files"][0]["name"], "Über.csv");
    let record = go(
        "name: x\nsteps:\n  - name: p\n    storage: { action: presign, credential: box_of_files, path: a.csv }\n",
        json!({}),
        &effects(&stand, &secrets, true),
    );
    assert!(failed(&record).contains("not dropbox"), "{}", record.to_json());
    let record = go(
        &list_of("credential: box_of_files"),
        json!({}),
        &effects(&stand, &secrets, false),
    );
    assert!(
        failed(&record).contains("--allow-private-network"),
        "the run's guard holds for every place"
    );
}

#[test]
fn box_files_are_found_by_name_through_their_folders_with_oauth_or_a_developer_token() {
    let stand = Stand::start();
    let secrets = Kept::with(vec![
        (
            "boxed",
            credential(
                "box",
                &[
                    ("access_token", places_support::boxes::TOKEN),
                    ("expires_at", "9999999999"),
                ],
            ),
        ),
        (
            "trying",
            credential("box_token", &[("token", places_support::boxes::DEVELOPER)]),
        ),
    ]);
    let record = go(
        &every_action("credential: boxed"),
        sample(),
        &effects(&stand, &secrets, true),
    );
    check_every_action(&record);
    // A developer token reaches the same files; a folder's id is where paths start.
    let archive = {
        let b = stand.boxes.lock().unwrap();
        b.drive
            .child("0", "archive", true)
            .unwrap()
            .id
            .trim_start_matches('n')
            .to_owned()
    };
    let record = go(
        &list_of(&format!("credential: trying, folder_id: '{archive}'")),
        json!({}),
        &effects(&stand, &secrets, true),
    );
    done(&record);
    assert_eq!(result(&record, "l")["files"][0]["path"], "orders-copy.csv");
    let record = go(
        "name: x\nsteps:\n  - name: d\n    storage: { action: delete, credential: boxed, path: in }\n",
        json!({}),
        &effects(&stand, &secrets, true),
    );
    assert!(
        failed(&record).contains("only an empty folder"),
        "{}",
        record.to_json()
    );
}

#[test]
fn onedrive_and_a_sharepoint_library_are_reached_over_graph() {
    let stand = Stand::start();
    let secrets = Kept::with(vec![(
        "work",
        credential(
            "microsoft_365",
            &[
                ("access_token", places_support::graph::TOKEN),
                ("expires_at", "9999999999"),
            ],
        ),
    )]);
    let record = go(
        &every_action("credential: work"),
        sample(),
        &effects(&stand, &secrets, true),
    );
    check_every_action(&record);
    let record = go(
        &list_of("credential: work, drive: 'site:contoso.sharepoint.com,2C71,9658', path: archive"),
        json!({}),
        &effects(&stand, &secrets, true),
    );
    done(&record);
    assert_eq!(result(&record, "l")["count"], 1);
    let drives = stand.graph.lock().unwrap().drives.clone();
    assert_eq!(
        drives,
        ["/me/drive", "/sites/contoso.sharepoint.com,2C71,9658/drive"]
    );
    let record = go(
        &list_of("credential: work, drive: '../x'"),
        json!({}),
        &effects(&stand, &secrets, true),
    );
    assert!(failed(&record).contains("drive:"), "{}", record.to_json());
}

#[test]
fn google_drive_files_are_found_by_name_and_google_docs_are_exported() {
    let stand = Stand::start();
    let secrets = Kept::with(vec![(
        "mine",
        credential(
            "google_drive",
            &[
                ("access_token", places_support::gdrive::TOKEN),
                ("expires_at", "9999999999"),
            ],
        ),
    )]);
    let record = go(
        &every_action("credential: mine"),
        sample(),
        &effects(&stand, &secrets, true),
    );
    check_every_action(&record);
    {
        let mut d = stand.drive.lock().unwrap();
        let docs = d.drive.add("root", "docs", true, Vec::new(), "");
        d.drive.add(
            &docs,
            "Plan",
            false,
            b"Quarterly plan".to_vec(),
            places_support::gdrive::DOC,
        );
        d.drive
            .add(&docs, "quinn's notes", false, b"q".to_vec(), "text/plain");
    }
    let record = go(
        "name: x\nsteps:\n\
         - name: office\n  storage: { action: read, credential: mine, path: docs/Plan }\n\
         - name: text\n  storage: { action: read, credential: mine, path: docs/Plan, export: text }\n\
         - name: quoted\n  storage: { action: read, credential: mine, path: \"docs/quinn's notes\" }\n",
        json!({}),
        &effects(&stand, &secrets, true),
    );
    done(&record);
    assert_eq!(
        result(&record, "office")["name"],
        "Plan.docx",
        "a Google Doc read as Word by default"
    );
    let text = result(&record, "text");
    assert_eq!(
        (text["name"].as_str(), text["type"].as_str()),
        (Some("Plan.txt"), Some("text/plain"))
    );
    assert_eq!(
        result(&record, "quoted")["size"],
        1,
        "a name with a quote found, the quote escaped"
    );
}

#[test]
fn a_test_run_reaches_no_place_and_a_preview_only_reads() {
    let stand = Stand::start();
    let secrets = Kept::with(vec![(
        "boxed",
        credential(
            "box",
            &[
                ("access_token", places_support::boxes::TOKEN),
                ("expires_at", "9999999999"),
            ],
        ),
    )]);
    {
        let mut b = stand.boxes.lock().unwrap();
        let inbox = b.drive.add("0", "in", true, Vec::new(), "");
        b.drive.add(&inbox, "a.csv", false, b"id\n1\n".to_vec(), "");
    }
    let workflow = "name: p\nsteps:\n\
        - name: got\n  storage: { action: read, credential: boxed, path: in/a.csv }\n\
        - name: rows\n  extract_file: { file: steps.got, format: csv }\n\
        - name: put\n  storage: { action: write, credential: boxed, path: out/b.csv, file: steps.got }\n\
        - name: mv\n  storage: { action: move, credential: boxed, path: in/a.csv, to: done/a.csv }\n";
    let asked = stand.log.lock().unwrap().len();
    let dry = DryRun {
        credentials: [(
            "boxed".to_owned(),
            credential("box", &[("access_token", "x")]).masked(),
        )]
        .into_iter()
        .collect(),
        ..DryRun::default()
    };
    let record = go(workflow, json!({}), &dry);
    done(&record);
    assert_eq!(
        (
            result(&record, "got")["test"].as_str(),
            result(&record, "got")["storage"].as_str()
        ),
        (Some("not sent"), Some("box"))
    );
    assert_eq!(
        stand.log.lock().unwrap().len(),
        asked,
        "a test run reaches nothing"
    );
    let live: Arc<dyn Effects> = Arc::new(effects(&stand, &secrets, true));
    let record = go(workflow, json!({}), &Preview::new(live));
    assert_eq!(
        result(&record, "rows")["rows"],
        json!([{ "id": "1" }]),
        "{}",
        record.to_json()
    );
    assert_eq!(result(&record, "put")["test"], "not written");
    assert_eq!(result(&record, "mv")["moved"], false);
    let b = stand.boxes.lock().unwrap();
    assert!(b.drive.child("0", "out", true).is_none() && b.drive.at("0", "in/a.csv", true).is_some());
}

#[test]
fn what_a_workflow_says_of_these_places_is_checked_before_it_runs() {
    let wrong = |step: &str| Workflow::parse(&format!("name: x\nsteps:\n  - {step}\n")).unwrap_err();
    assert!(
        wrong("storage: { action: read, credential: d, path: x, export: rtf }")
            .contains("office, pdf, text, open")
    );
    assert!(
        wrong("storage: { action: write, credential: d, path: x, file: \"'a'\", export: pdf }")
            .contains("not a setting of storage's write")
    );
    assert!(wrong("storage: { action: list, credential: d, folder: x }").contains("not a setting"));
    Workflow::parse(
        "name: x\nsteps:\n  - storage: { action: list, credential: d, folder_id: 'id:abc', drive: me }\n",
    )
    .unwrap();
    let trigger = |t: &str| Workflow::parse(&format!("name: x\ntrigger:\n  {t}\nsteps: []\n"));
    assert!(trigger("storage: { credential: drop, folder: in, folder_id: 'id:abc', every: 1m }").is_ok());
    assert!(trigger("storage: { credential: work, folder: in, drive: 'site:x', every: 1m }").is_ok());
    assert!(
        trigger("storage: { credential: work, export: pdf }")
            .unwrap_err()
            .contains("not a setting")
    );
}
