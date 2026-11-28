// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Storage steps against stand-ins on this machine: a folder the admin allows (a temporary one), an S3 store that
//! checks every signature with its own signer, an FTP server whose passive answers name the wrong address, a Redis
//! server; archives packed and unpacked; and what a test run and a preview do instead, and what is refused.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::type_complexity
)]

mod storage_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::storage::Roots;
use openduct_flow::{
    Credential, DryRun, Effects, Event, Live, Outcome, Preview, RunRecord, Secrets, Workflow, run,
};
use serde_json::{Value as Json, json};

/// Credentials by name; fields a run keeps (an SFTP server's key) are kept here.
#[derive(Debug, Default)]
struct Kept(Mutex<BTreeMap<String, Credential>>);

impl Kept {
    fn with(list: &[(&str, &str, &[(&str, &str)])]) -> Arc<Self> {
        let kept = Self::default();
        for (name, kind, fields) in list {
            kept.0.lock().unwrap().insert(
                (*name).to_owned(),
                Credential {
                    kind: (*kind).to_owned(),
                    fields: fields
                        .iter()
                        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                        .collect(),
                },
            );
        }
        Arc::new(kept)
    }
}

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.lock().unwrap().get(name).cloned()
    }

    fn keep<'a>(
        &'a self,
        name: &'a str,
        fields: BTreeMap<String, String>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let mut all = self.0.lock().unwrap();
            let c = all.get_mut(name).ok_or("no such credential")?;
            c.fields.extend(fields);
            Ok(Value::Null)
        })
    }
}

fn folder(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "openduct-storage-{name}-{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::canonicalize(dir).unwrap()
}

fn live(roots: &[(&str, &Path)], secrets: &Arc<Kept>, allow_private: bool) -> Live {
    let given: Vec<String> = roots
        .iter()
        .map(|(n, p)| format!("{n}={}", p.display()))
        .collect();
    Live::new("127.0.0.1:1", allow_private)
        .unwrap()
        .with_storage_roots(Arc::new(Roots::parse(&given).unwrap()))
        .with_secrets(Arc::clone(secrets) as Arc<dyn Secrets>)
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

/// The same steps on any place: `place` is `root: inbox` or `credential: x`.
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
    assert!(
        result(record, "again")["error"]
            .as_str()
            .unwrap()
            .contains("set overwrite to replace it"),
        "{}",
        record.to_json()
    );
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
    assert_eq!(got["name"], "orders.csv");
    assert_eq!(got["type"], "text/csv");
    assert_eq!(got["path"], "in/orders.csv");
    assert_eq!(result(record, "rows")["rows"], json!([{ "id": "9" }]));
    assert_eq!(result(record, "copied")["copied"], true);
    assert_eq!(result(record, "moved")["to"], "done/2026-11-27.csv");
    assert_eq!(result(record, "made")["made"], true);
    assert_eq!(result(record, "made_again")["made"], false);
    assert_eq!(result(record, "gone")["deleted"], true);
    assert_eq!(result(record, "gone_again")["deleted"], false);
    assert!(
        result(record, "missing")["error"]
            .as_str()
            .unwrap()
            .contains("nothing at in/orders.csv")
    );
}

fn sample() -> Json {
    json!({ "day": "2026-11-27", "rows": [{ "id": "A-1" }, { "id": "A-2" }] })
}

#[test]
fn a_folder_the_admin_allows_is_listed_read_written_moved_and_cleaned_up() {
    let root = folder("inbox");
    let record = go(
        &every_action("root: inbox"),
        sample(),
        &live(&[("inbox", &root)], &Kept::with(&[]), false),
    );
    check_every_action(&record);
    assert_eq!(
        std::fs::read_to_string(root.join("archive/orders-copy.csv")).unwrap(),
        "id\n9\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("in/deep/note.txt")).unwrap(),
        "hello"
    );
    assert!(root.join("out/reports").is_dir());
    let left: Vec<String> = std::fs::read_dir(root.join("in"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        left.iter().all(|n| !n.contains("openduct-part")),
        "no half-written file is left: {left:?}"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn nothing_outside_the_folder_is_reached() {
    let base = folder("outside");
    let root = base.join("root");
    std::fs::create_dir_all(root.join("in")).unwrap();
    std::fs::write(base.join("secret.txt"), "not yours").unwrap();
    std::fs::write(root.join("in/full.txt"), "x").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(base.join("secret.txt"), root.join("in/leak.txt")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&base, root.join("up")).unwrap();
    let effects = live(&[("inbox", &root)], &Kept::with(&[]), false);
    let refused = |step: &str| {
        let record = go(
            &format!("name: x\nsteps:\n  - name: s\n    storage: {step}\n"),
            json!({ "p": "../secret.txt" }),
            &effects,
        );
        failed(&record)
    };
    assert!(refused("{ action: read, root: inbox, path: '{{ value.p }}' }").contains(".. would lead out"));
    assert!(refused("{ action: read, root: inbox, path: /etc/passwd }").contains("not one from the top"));
    assert!(refused("{ action: write, root: inbox, path: 'C:/x', file: \"'x'\" }").contains("drive"));
    assert!(refused("{ action: read, root: other, path: x }").contains("allows inbox"));
    assert!(refused("{ action: delete, root: inbox, path: in }").contains("only an empty folder"));
    #[cfg(unix)]
    {
        assert!(
            refused("{ action: read, root: inbox, path: in/leak.txt }").contains("leads out of the folder")
        );
        assert!(
            refused("{ action: read, root: inbox, path: up/secret.txt }").contains("leads out of the folder")
        );
        assert!(
            refused("{ action: write, root: inbox, path: up/new.txt, file: \"'x'\" }").contains("leads out")
        );
        assert!(
            refused("{ action: copy, root: inbox, path: in/leak.txt, to: in/copy.txt }")
                .contains("leads out")
        );
        // A listing leaves out what leads out; a delete takes the link away, never what it leads to.
        let record = go(
            "name: x\nsteps:\n  - name: l\n    storage: { action: list, root: inbox, path: in }\n  - name: d\n    storage: { action: delete, root: inbox, path: in/leak.txt }\n",
            json!({}),
            &effects,
        );
        done(&record);
        assert_eq!(result(&record, "l")["count"], 1);
        assert_eq!(result(&record, "d")["deleted"], true);
        assert_eq!(
            std::fs::read_to_string(base.join("secret.txt")).unwrap(),
            "not yours"
        );
        assert!(!base.join("new.txt").exists());
    }
    std::fs::remove_dir_all(base).unwrap();
}

#[test]
fn a_test_run_reaches_nothing_and_a_preview_only_reads() {
    let root = folder("preview");
    std::fs::create_dir_all(root.join("in")).unwrap();
    std::fs::write(root.join("in/a.csv"), "id\n1\n").unwrap();
    let workflow = "name: p\nsteps:\n\
        - name: got\n  storage: { action: read, root: inbox, path: in/a.csv }\n\
        - name: rows\n  extract_file: { file: steps.got, format: csv }\n\
        - name: put\n  storage: { action: write, root: inbox, path: out/b.csv, file: steps.got }\n\
        - name: mv\n  storage: { action: move, root: inbox, path: in/a.csv, to: done/a.csv }\n\
        - name: cache\n  redis: { action: set, credential: cache, key: k, value: v }\n";
    // A test run: nothing read, nothing written, every step shaped as it will be.
    let record = go(workflow, json!({}), &DryRun::default());
    done(&record);
    assert_eq!(result(&record, "got")["test"], "not sent");
    assert_eq!(result(&record, "got")["size"], 0, "an empty stand-in of the file");
    assert_eq!(result(&record, "rows")["rows"], json!([]));
    assert_eq!(result(&record, "put")["written"], false);
    assert_eq!(result(&record, "cache")["test"], "not sent");
    // A preview: the read is real, the writes shown.
    let effects: Arc<dyn Effects> = Arc::new(live(&[("inbox", &root)], &Kept::with(&[]), false));
    let record = go(workflow, json!({}), &Preview::new(effects));
    assert_eq!(
        result(&record, "rows")["rows"],
        json!([{ "id": "1" }]),
        "{}",
        record.to_json()
    );
    assert_eq!(result(&record, "put")["test"], "not written");
    assert_eq!(result(&record, "mv")["moved"], false);
    assert!(!root.join("out/b.csv").exists());
    assert!(root.join("in/a.csv").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn archives_are_packed_and_unpacked_and_a_bomb_is_refused() {
    let workflow = "name: zip\nsteps:\n\
        - name: a\n  convert: { format: csv, rows: value.rows, name: a.csv }\n\
        - name: b\n  convert: { format: text, text: 'hello', name: b.txt }\n\
        - name: zip\n  compress: { action: pack, files: '[steps.a, steps.b]', name: both.zip }\n\
        - name: listed\n  compress: { action: list, file: steps.zip }\n\
        - name: unzipped\n  compress: { action: unpack, file: steps.zip, pattern: '*.csv' }\n\
        - name: rows\n  extract_file: { file: 'steps.unzipped.files[0]' }\n\
        - name: tgz\n  compress: { action: pack, format: tar.gz, files: '[steps.a, steps.b, steps.a]' }\n\
        - name: untgz\n  compress: { action: unpack, file: steps.tgz }\n\
        - name: gz\n  compress: { action: pack, format: gzip, files: steps.b }\n\
        - name: ungz\n  compress: { action: unpack, file: steps.gz }\n\
        - name: two\n  compress: { action: pack, format: gzip, files: '[steps.a, steps.b]' }\n  on_error: continue\n";
    let record = go(workflow, json!({ "rows": [{ "id": "A-1" }] }), &DryRun::default());
    done(&record);
    assert_eq!(result(&record, "zip")["name"], "both.zip");
    assert_eq!(result(&record, "zip")["type"], "application/zip");
    assert_eq!(result(&record, "listed")["count"], 2);
    assert_eq!(result(&record, "unzipped")["count"], 1);
    assert_eq!(result(&record, "rows")["rows"], json!([{ "id": "A-1" }]));
    assert_eq!(result(&record, "tgz")["name"], "tgz.tar.gz");
    let untgz = result(&record, "untgz");
    let names: Vec<String> = untgz["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["a.csv", "b.txt", "a (2).csv"], "{names:?}");
    assert_eq!(result(&record, "gz")["name"], "b.txt.gz");
    assert_eq!(result(&record, "ungz")["files"][0]["name"], "b.txt");
    assert!(
        result(&record, "two")["error"]
            .as_str()
            .unwrap()
            .contains("gzip packs one file")
    );

    // 80 MB of zeros packed into a few dozen kilobytes is refused before it fills the run.
    use std::io::Write;
    let mut bomb = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    bomb.write_all(&vec![0u8; 80 * 1024 * 1024]).unwrap();
    let bomb = bomb.finish().unwrap();
    use base64::Engine as _;
    let text = base64::engine::general_purpose::STANDARD.encode(&bomb);
    let record = go(
        "name: bomb\nsteps:\n  - name: f\n    convert: { format: base64, base64: value.b, name: x.gz }\n  - name: u\n    compress: { action: unpack, file: steps.f }\n",
        json!({ "b": text }),
        &DryRun::default(),
    );
    assert!(failed(&record).contains("more than"), "{}", record.to_json());
}

#[test]
fn s3_objects_are_listed_got_put_copied_deleted_and_presigned() {
    let (endpoint, state) = storage_support::s3();
    let secrets = Kept::with(&[(
        "lake",
        "s3",
        &[
            ("access_key", storage_support::ACCESS),
            ("secret_key", storage_support::SECRET),
            ("endpoint", endpoint.as_str()),
            ("bucket", "orders"),
        ],
    )]);
    let mut workflow = every_action("credential: lake");
    workflow.push_str(
        "- name: link\n  storage: { action: presign, credential: lake, path: archive/orders-copy.csv, expires: 10m }\n",
    );
    let record = go(&workflow, sample(), &live(&[], &secrets, true));
    check_every_action(&record);
    {
        let s = state.lock().unwrap();
        assert_eq!(
            s.refused_signatures, 0,
            "every request is signed as S3 checks it: {:?}",
            s.requests
        );
        assert!(
            s.objects.contains_key("out/reports/"),
            "a folder is an empty object named for it"
        );
        assert_eq!(s.objects["archive/orders-copy.csv"].0, b"id\n9\n");
    }
    let link = result(&record, "link");
    let (status, body) = storage_support::fetch(link["url"].as_str().unwrap());
    assert_eq!((status, body.as_slice()), (200, &b"id\n9\n"[..]), "{link}");
    // The same store on this machine's own network, without the installation's leave: refused.
    let record = go(
        "name: x\nsteps:\n  - name: l\n    storage: { action: list, credential: lake }\n",
        json!({}),
        &live(&[], &secrets, false),
    );
    assert!(
        failed(&record).contains("--allow-private-network"),
        "{}",
        record.to_json()
    );
    // A wrong secret: S3 says why.
    let wrong = Kept::with(&[(
        "lake",
        "s3",
        &[
            ("access_key", storage_support::ACCESS),
            ("secret_key", "wrong"),
            ("endpoint", endpoint.as_str()),
            ("bucket", "orders"),
        ],
    )]);
    let record = go(
        "name: x\nsteps:\n  - name: l\n    storage: { action: list, credential: lake }\n",
        json!({}),
        &live(&[], &wrong, true),
    );
    assert!(
        failed(&record).contains("SignatureDoesNotMatch"),
        "{}",
        record.to_json()
    );
}

#[test]
fn an_ftp_server_is_used_as_a_folder_is_and_its_passive_address_is_not_taken() {
    let root = folder("ftp");
    let (port, state) = storage_support::ftp(&root);
    let port = port.to_string();
    let secrets = Kept::with(&[
        (
            "partner",
            "ftp",
            &[
                ("host", "127.0.0.1"),
                ("port", port.as_str()),
                ("username", "acme"),
                ("password", "secret"),
                ("security", "none"),
            ],
        ),
        (
            "strict",
            "ftp",
            &[
                ("host", "127.0.0.1"),
                ("port", port.as_str()),
                ("username", "acme"),
                ("password", "secret"),
            ],
        ),
        (
            "wrong",
            "ftp",
            &[
                ("host", "127.0.0.1"),
                ("port", port.as_str()),
                ("username", "acme"),
                ("password", "nope"),
                ("security", "none"),
            ],
        ),
    ]);
    let effects = live(&[], &secrets, true);
    let record = go(&every_action("credential: partner"), sample(), &effects);
    check_every_action(&record);
    assert_eq!(
        std::fs::read_to_string(root.join("archive/orders-copy.csv")).unwrap(),
        "id\n9\n"
    );
    let commands = state.lock().unwrap().commands.clone();
    assert!(commands.iter().any(|c| c == "PASS ***"));
    assert!(
        commands.iter().any(|c| c.starts_with("PASV")),
        "every transfer went to the server's own address: {commands:?}"
    );
    let one = |c: &str| {
        go(
            &format!("name: x\nsteps:\n  - name: l\n    storage: {{ action: list, credential: {c} }}\n"),
            json!({}),
            &effects,
        )
    };
    assert!(
        failed(&one("strict")).contains("does not offer TLS"),
        "TLS unless the credential says none"
    );
    assert!(failed(&one("wrong")).contains("refused the sign-in"));
    let record = go(
        "name: x\nsteps:\n  - name: l\n    storage: { action: list, credential: partner }\n",
        json!({}),
        &live(&[], &secrets, false),
    );
    assert!(failed(&record).contains("--allow-private-network"));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn redis_keeps_values_counts_lists_and_publishes() {
    let (port, state) = storage_support::redis("s3cret");
    let url = format!("redis://127.0.0.1:{port}/2");
    let secrets = Kept::with(&[
        ("cache", "redis", &[("url", url.as_str()), ("password", "s3cret")]),
        ("locked", "redis", &[("url", url.as_str()), ("password", "wrong")]),
    ]);
    let effects = live(&[], &secrets, true);
    let workflow = "name: r\nsteps:\n\
        - name: put\n  redis: { action: set, credential: cache, key: 'order:{{ value.id }}', value: '{{ value.order }}', ttl: 1h }\n\
        - name: get\n  redis: { action: get, credential: cache, key: 'order:{{ value.id }}' }\n\
        - name: once\n  redis: { action: set, credential: cache, key: 'order:{{ value.id }}', value: x, if_missing: true }\n\
        - name: n1\n  redis: { action: increment, credential: cache, key: seen }\n\
        - name: n2\n  redis: { action: increment, credential: cache, key: seen, by: 5 }\n\
        - name: q1\n  redis: { action: push, credential: cache, key: queue, value: first }\n\
        - name: q2\n  redis: { action: push, credential: cache, key: queue, value: second }\n\
        - name: all\n  redis: { action: range, credential: cache, key: queue }\n\
        - name: next\n  redis: { action: pop, credential: cache, key: queue }\n\
        - name: told\n  redis: { action: publish, credential: cache, channel: orders, value: '{{ value.order }}' }\n\
        - name: there\n  redis: { action: exists, credential: cache, key: seen }\n\
        - name: keep\n  redis: { action: expire, credential: cache, key: seen, ttl: 30s }\n\
        - name: gone\n  redis: { action: delete, credential: cache, key: seen }\n\
        - name: none\n  redis: { action: get, credential: cache, key: seen }\n";
    let record = go(
        workflow,
        json!({ "id": "A-1", "order": { "total": 12 } }),
        &effects,
    );
    done(&record);
    assert_eq!(result(&record, "put")["set"], true);
    assert_eq!(
        result(&record, "get")["value"],
        json!({ "total": 12 }),
        "a record set comes back a record"
    );
    assert_eq!(result(&record, "once")["set"], false);
    assert_eq!(result(&record, "n2")["value"], 6);
    assert_eq!(result(&record, "all")["items"], json!(["first", "second"]));
    assert_eq!(
        result(&record, "next")["value"],
        "first",
        "a list pushed to and popped from is a queue"
    );
    assert_eq!(result(&record, "told")["received"], 0);
    assert_eq!(result(&record, "there")["exists"], true);
    assert_eq!(result(&record, "keep")["set"], true);
    assert_eq!(result(&record, "gone")["deleted"], true);
    assert_eq!(result(&record, "none")["found"], false);
    {
        let s = state.lock().unwrap();
        assert_eq!(s.ttls.get(b"order:A-1".as_slice()), Some(&3_600_000));
        assert!(s.commands.iter().any(|c| c == "SELECT 2"));
        assert!(s.commands.iter().all(|c| !c.contains("s3cret")));
    }
    let record = go(
        "name: x\nsteps:\n  - name: g\n    redis: { action: get, credential: locked, key: k }\n",
        json!({}),
        &effects,
    );
    assert!(failed(&record).contains("refused the sign-in"));
}

#[test]
fn what_a_workflow_says_is_checked_before_it_runs() {
    let wrong = |step: &str| Workflow::parse(&format!("name: x\nsteps:\n  - {step}\n")).unwrap_err();
    assert!(wrong("storage: { action: read, root: a, credential: b, path: x }").contains("not both"));
    assert!(wrong("storage: { action: read, path: x }").contains("root"));
    assert!(wrong("storage: { action: write, root: a, path: x }").contains("file: what to write"));
    assert!(wrong("storage: { action: move, root: a, path: x }").contains("to: where it goes"));
    assert!(wrong("storage: { action: presign, root: a, path: x }").contains("presign is for S3"));
    assert!(
        wrong("storage: { action: list, root: a, overwrite: true }")
            .contains("not a setting of storage's list")
    );
    assert!(wrong("storage: { action: chmod, root: a, path: x }").contains("make_folder"));
    assert!(wrong("storage: { action: list, root: a, limit: 0 }").contains("1 to 10000"));
    assert!(wrong("compress: { action: pack }").contains("files: the files to pack"));
    assert!(
        wrong("compress: { action: unpack, file: steps.a, format: rar }")
            .contains("zip, gzip, tar or tar.gz")
    );
    assert!(wrong("redis: { action: set, credential: c, key: k }").contains("value: set needs it"));
    assert!(wrong("redis: { action: expire, credential: c, key: k }").contains("ttl"));
    assert!(
        wrong("redis: { action: get, credential: c, key: k, ttl: 1h }")
            .contains("not a setting of redis's get")
    );
    // Every storage step is a tool an agent may use.
    let agent = "name: a\nsteps:\n  - name: helper\n    agent:\n      credential: model\n      input: '{{ value.q }}'\n      tools:\n\
        \x20       - name: inbox\n          description: The files waiting in the inbox\n          storage: { action: list, root: inbox, path: in }\n\
        \x20       - name: remember\n          description: Keep a note\n          redis: { action: set, credential: cache, key: note, value: '{{ args.text }}' }\n";
    Workflow::parse(agent).unwrap();
    // Triggers.
    let trigger = |t: &str| Workflow::parse(&format!("name: x\ntrigger:\n  {t}\nsteps: []\n"));
    assert!(trigger("storage: { credential: partner, folder: in, pattern: '*.csv', every: 1m }").is_ok());
    assert!(trigger("redis: { credential: cache, channel: orders }").is_ok());
    assert!(
        trigger("storage: { root: inbox, every: 1s }")
            .unwrap_err()
            .contains("from 10s")
    );
}
