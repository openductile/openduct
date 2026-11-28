// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The mailbox step and trigger against a stand-in IMAP server on this machine: messages listed, read (their
//! attachments as files), marked, moved and deleted; signing in with a password or an OAuth token kept fresh; TLS with
//! the credential's CA; a hostile server bounded; what a test run and a preview do; and the trigger's new messages run
//! once across two nodes, a restart and a folder rebuilt, and asked for at once when the server says one came (IDLE).

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod imap_support;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_broker::client::{ClientSecurity, Outgoing, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::Value;
use openduct_flow::runner::{Topics, envelope, iso, prepare, serve};
use openduct_flow::storage::{Places, Reach, Roots};
use openduct_flow::{
    Credential, DryRun, Effects, Event, HttpRequest, Live, Outcome, Preview, RunRecord, Secrets, Trigger,
    Workflow, run,
};
use openduct_storage::MemoryStore;
use serde_json::{Value as Json, json};
use tokio::sync::watch;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Credentials by name; what a run keeps (a renewed token) is kept here.
#[derive(Debug, Default)]
struct Kept(Mutex<BTreeMap<String, Credential>>);

impl Kept {
    fn with(name: &str, fields: &[(&str, &str)]) -> Arc<Self> {
        let kept = Self::default();
        kept.0.lock().unwrap().insert(
            name.to_owned(),
            Credential {
                kind: "imap".to_owned(),
                fields: fields
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            },
        );
        Arc::new(kept)
    }

    fn field(&self, name: &str, field: &str) -> Option<String> {
        self.0.lock().unwrap().get(name)?.fields.get(field).cloned()
    }
}

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.lock().unwrap().get(name).cloned()
    }

    fn keep<'a>(&'a self, name: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
        Box::pin(async move {
            let mut all = self.0.lock().unwrap();
            all.get_mut(name)
                .ok_or("no such credential")?
                .fields
                .extend(fields);
            Ok(Value::Null)
        })
    }
}

fn plain(port: u16) -> Vec<(&'static str, String)> {
    vec![
        ("host", "127.0.0.1".into()),
        ("port", port.to_string()),
        ("security", "none".into()),
        ("username", "ann@example.com".into()),
    ]
}

fn kept(fields: &[(&'static str, String)]) -> Arc<Kept> {
    let borrowed: Vec<(&str, &str)> = fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
    Kept::with("mail", &borrowed)
}

fn live(secrets: &Arc<Kept>, allow_private: bool) -> Live {
    Live::new("127.0.0.1:1", allow_private)
        .unwrap()
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

const CYCLE: &str = "name: cycle\nsteps:\n\
    - name: unread\n  mailbox: { action: list, credential: mail, unseen: true, from: billing@example.com }\n\
    - name: email\n  mailbox: { action: read, credential: mail, uid: '{{ steps.unread.messages[0].uid }}' }\n\
    - name: rows\n  extract_file: { file: 'steps.email.attachments[0]', format: csv }\n\
    - name: seen\n  mailbox: { action: mark, credential: mail, uid: '{{ steps.email.uid }}', seen: true, flagged: true }\n\
    - name: filed\n  mailbox: { action: move, credential: mail, uid: '{{ steps.email.uid }}', to_folder: Archive }\n\
    - name: all\n  mailbox: { action: list, credential: mail }\n\
    - name: gone\n  mailbox: { action: delete, credential: mail, uid: '{{ steps.all.messages[0].uid }}' }\n\
    - name: folders\n  mailbox: { action: folders, credential: mail }\n";

#[test]
fn a_mailbox_is_listed_read_marked_moved_and_cleaned_up() {
    let (port, state) = imap_support::start(None);
    {
        let mut s = state.lock().unwrap();
        s.add(
            "INBOX",
            &imap_support::message("Lee <lee@example.com>", "Lunch", "See you at noon."),
        );
        s.add("INBOX", &imap_support::invoice("A-7", "99.50"));
        s.add(
            "INBOX",
            &imap_support::message("Kim <kim@example.com>", "Hello", "Hi there."),
        );
    }
    let secrets = kept(&[plain(port), vec![("password", "s3cret".into())]].concat());
    let record = go(CYCLE, json!({}), &live(&secrets, true));
    done(&record);
    let unread = result(&record, "unread");
    assert_eq!(unread["count"], 1, "{unread}");
    assert_eq!(unread["messages"][0]["subject"], "Invoice A-7");
    assert_eq!(unread["messages"][0]["from"]["address"], "billing@example.com");
    let email = result(&record, "email");
    assert_eq!(email["uid"], 2);
    assert_eq!(email["text"], "Invoice A-7 is attached.");
    assert_eq!(email["attachments"][0]["name"], "invoice-A-7.csv");
    assert_eq!(
        result(&record, "rows")["rows"],
        json!([{ "number": "A-7", "total": "99.50" }]),
        "the attachment is a run file the next steps read"
    );
    assert_eq!(result(&record, "seen")["seen"], true);
    assert_eq!(result(&record, "filed")["moved"], true);
    let all = result(&record, "all");
    assert_eq!(all["count"], 2, "the invoice went to Archive");
    assert_eq!(all["messages"][0]["subject"], "Hello", "newest first");
    assert_eq!(result(&record, "gone")["expunged"], true);
    let listed = result(&record, "folders");
    let folders: Vec<&str> = listed["folders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(folders, ["Archive", "INBOX"]);
    let s = state.lock().unwrap();
    let archived = &s.folders["Archive"].messages;
    assert_eq!(archived.len(), 1);
    assert!(
        archived[0].flags.contains(&"\\Seen".to_owned())
            && archived[0].flags.contains(&"\\Flagged".to_owned())
    );
    let inbox: Vec<u64> = s.folders["INBOX"].messages.iter().map(|m| m.uid).collect();
    assert_eq!(inbox, [1], "the newest was deleted, and only it");
    // Reading never marks a message seen by itself: the list and the read peeked.
    assert!(
        s.commands
            .iter()
            .all(|c| !c.contains("BODY[]") || c.contains("BODY.PEEK[]"))
    );
}

#[test]
fn signing_in_is_checked_and_an_oauth_token_is_kept_fresh() {
    let (port, state) = imap_support::start(None);
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("a@example.com", "One", "x"));
    let list = "name: l\nsteps:\n  - name: all\n    mailbox: { action: list, credential: mail }\n";
    // A wrong password: refused, in the server's words.
    let wrong = kept(&[plain(port), vec![("password", "guess".into())]].concat());
    let e = failed(&go(list, json!({}), &live(&wrong, true)));
    assert!(e.contains("refused the sign-in of ann@example.com"), "{e}");
    assert!(!e.contains("guess"), "the password is never said");

    // An access token that has run out: renewed at the token address first, kept, and used.
    state.lock().unwrap().tokens = vec!["fresh-token".into()];
    let secrets = kept(
        &[
            plain(port),
            vec![
                ("access_token", "old-token".into()),
                ("expires_at", "1".into()),
                ("refresh_token", "r1".into()),
                ("token_url", "https://login.example.com/token".into()),
                ("client_id", "c1".into()),
            ],
        ]
        .concat(),
    );
    struct TokenServer {
        live: Live,
        asked: Mutex<Vec<String>>,
    }
    impl Effects for TokenServer {
        fn http<'a>(&'a self, r: HttpRequest) -> Pending<'a> {
            Box::pin(async move {
                let body = String::from_utf8_lossy(&r.body.map(|b| b.1).unwrap_or_default()).into_owned();
                self.asked.lock().unwrap().push(format!("{} {body}", r.url));
                Ok(Value::from_json(&json!({
                    "status": 200, "headers": {},
                    "body": { "access_token": "fresh-token", "expires_in": 3600, "token_type": "Bearer" },
                })))
            })
        }
        fn write<'a>(&'a self, t: &'a str, k: Option<String>, v: String) -> Pending<'a> {
            self.live.write(t, k, v)
        }
        fn credential(&self, name: &str) -> Result<Credential, String> {
            self.live.credential(name)
        }
        fn keep_credential<'a>(&'a self, name: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
            self.live.keep_credential(name, fields)
        }
        fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
            self.live.file(path)
        }
        fn storage(&self) -> Reach<'_> {
            self.live.storage()
        }
    }
    let effects = TokenServer {
        live: live(&secrets, true),
        asked: Mutex::new(Vec::new()),
    };
    let record = go(list, json!({}), &effects);
    done(&record);
    assert_eq!(result(&record, "all")["count"], 1);
    let asked = effects.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 1, "{asked:?}");
    assert!(asked[0].starts_with(
        "https://login.example.com/token grant_type=refresh_token&refresh_token=r1&client_id=c1"
    ));
    assert_eq!(
        secrets.field("mail", "access_token").as_deref(),
        Some("fresh-token")
    );
    // A token the server refuses, though it has not run out: renewed once, and tried again.
    state.lock().unwrap().tokens = vec!["fresh-token".into()];
    secrets
        .0
        .lock()
        .unwrap()
        .get_mut("mail")
        .unwrap()
        .fields
        .insert("access_token".into(), "revoked".into());
    secrets
        .0
        .lock()
        .unwrap()
        .get_mut("mail")
        .unwrap()
        .fields
        .insert("expires_at".into(), "99999999999".into());
    done(&go(list, json!({}), &effects));
    assert_eq!(effects.asked.lock().unwrap().len(), 2);
}

#[test]
fn this_machines_network_is_refused_unless_allowed_and_a_hostile_server_is_bounded() {
    let (port, state) = imap_support::start(None);
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("a@example.com", "One", "x"));
    let secrets = kept(&[plain(port), vec![("password", "s3cret".into())]].concat());
    let list = "name: l\nsteps:\n  - name: all\n    mailbox: { action: list, credential: mail }\n";
    let e = failed(&go(list, json!({}), &live(&secrets, false)));
    assert!(e.contains("--allow-private-network"), "{e}");
    // A server that says a message is far larger than it sends: refused before anything is held.
    state.lock().unwrap().lie_about_size = true;
    let read = "name: r\nsteps:\n  - name: one\n    mailbox: { action: read, credential: mail, uid: '1' }\n";
    let e = failed(&go(read, json!({}), &live(&secrets, true)));
    assert!(e.contains("more than 64 MB in one answer"), "{e}");
    // A message that is not there, and a UID that is not one.
    state.lock().unwrap().lie_about_size = false;
    let missing =
        "name: r\nsteps:\n  - name: one\n    mailbox: { action: read, credential: mail, uid: '42' }\n";
    assert!(failed(&go(missing, json!({}), &live(&secrets, true))).contains("no message with UID 42"));
    let bad = "name: r\nsteps:\n  - name: one\n    mailbox: { action: read, credential: mail, uid: '{{ value.x }}' }\n";
    assert!(
        failed(&go(bad, json!({ "x": "1; DELETE" }), &live(&secrets, true)))
            .contains("is not a message's UID")
    );
    let folder = "name: r\nsteps:\n  - name: one\n    mailbox: { action: list, credential: mail, folder: '{{ value.f }}' }\n";
    let e = failed(&go(
        folder,
        json!({ "f": "INBOX\r\na9 DELETE Archive" }),
        &live(&secrets, true),
    ));
    assert!(e.contains("line break"), "{e}");
    assert!(state.lock().unwrap().folders.contains_key("Archive"));
}

#[test]
fn tls_from_the_start_with_the_credentials_ca() {
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let pem = made.cert.pem();
    let (port, state) =
        imap_support::start(Some((made.cert.der().to_vec(), made.signing_key.serialize_der())));
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("a@example.com", "Over TLS", "x"));
    let list = "name: l\nsteps:\n  - name: all\n    mailbox: { action: list, credential: mail }\n";
    let base = vec![
        ("host", "localhost".to_owned()),
        ("port", port.to_string()),
        ("username", "ann@example.com".to_owned()),
        ("password", "s3cret".to_owned()),
    ];
    let trusted = kept(&[base.clone(), vec![("ca", pem)]].concat());
    let record = go(list, json!({}), &live(&trusted, true));
    done(&record);
    assert_eq!(result(&record, "all")["messages"][0]["subject"], "Over TLS");
    // Without the CA its certificate is not accepted, and nothing is sent.
    let untrusted = kept(&base);
    let e = failed(&go(list, json!({}), &live(&untrusted, true)));
    assert!(
        e.contains("certificate was not accepted") || e.contains("TLS"),
        "{e}"
    );
}

#[test]
fn a_test_run_answers_with_a_sample_and_a_preview_only_reads() {
    let (port, state) = imap_support::start(None);
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::invoice("B-2", "10.00"));
    let workflow = "name: p\nsteps:\n\
        - name: email\n  mailbox: { action: read, credential: mail, uid: '1' }\n\
        - name: rows\n  extract_file: { file: 'steps.email.attachments[0]', format: csv }\n\
        - name: filed\n  mailbox: { action: move, credential: mail, uid: '1', to_folder: Archive }\n";
    let record = go(workflow, json!({}), &DryRun::default());
    done(&record);
    assert_eq!(result(&record, "email")["test"], "not sent");
    assert_eq!(
        result(&record, "email")["subject"],
        "Invoice A-1001",
        "a sample message"
    );
    assert_eq!(result(&record, "rows")["rows"][0]["number"], "A-1001");
    assert_eq!(result(&record, "filed")["moved"], false);
    assert_eq!(state.lock().unwrap().sessions, 0, "a test run reaches nothing");
    let secrets = kept(&[plain(port), vec![("password", "s3cret".into())]].concat());
    let effects: Arc<dyn Effects> = Arc::new(live(&secrets, true));
    let record = go(workflow, json!({}), &Preview::new(effects));
    done(&record);
    assert_eq!(
        result(&record, "rows")["rows"][0]["number"],
        "B-2",
        "a preview reads for real"
    );
    assert_eq!(result(&record, "filed")["test"], "not written");
    assert!(
        state.lock().unwrap().folders["Archive"].messages.is_empty(),
        "and moves nothing"
    );
}

#[test]
fn what_a_workflow_says_is_checked_before_it_runs() {
    for (bad, says) in [
        ("{ action: read, credential: mail }", "uid"),
        (
            "{ action: mark, credential: mail, uid: '1' }",
            "seen, flagged or both",
        ),
        ("{ action: move, credential: mail, uid: '1' }", "to_folder"),
        ("{ action: list, credential: mail, limit: 0 }", "1 to 500"),
        ("{ action: list, credential: mail, uid: '1' }", "not a setting"),
        ("{ action: send, credential: mail }", "list, read"),
        ("{ action: list }", "credential"),
    ] {
        let yaml = format!("name: x\nsteps:\n  - mailbox: {bad}\n");
        let e = Workflow::parse(&yaml).unwrap_err();
        assert!(e.contains(says), "{bad}: {e}");
    }
}

// ---- the trigger, on a broker in memory ----

/// A node's effects: the mailbox's credential, this machine's network allowed, and each run's call kept.
struct Node {
    places: Places,
    credential: Credential,
    calls: Mutex<Vec<String>>,
}

impl Node {
    fn new(port: u16) -> Arc<Self> {
        let fields: BTreeMap<String, String> = [plain(port), vec![("password", "s3cret".into())]]
            .concat()
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();
        Arc::new(Self {
            places: Places {
                roots: Arc::new(Roots::parse(&[]).unwrap()),
                allow_private: true,
            },
            credential: Credential {
                kind: "imap".into(),
                fields,
            },
            calls: Mutex::new(Vec::new()),
        })
    }

    fn ran(&self) -> Vec<String> {
        let mut ran = self.calls.lock().unwrap().clone();
        ran.sort();
        ran
    }
}

impl Effects for Node {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let last = request.url.rsplit('/').next().unwrap_or_default().to_owned();
            self.calls.lock().unwrap().push(last);
            Ok(Value::from_json(
                &json!({ "status": 200, "headers": {}, "body": {} }),
            ))
        })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no writes here".to_owned()) })
    }

    fn credential(&self, _: &str) -> Result<Credential, String> {
        Ok(self.credential.clone())
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path} here")) })
    }

    fn storage(&self) -> Reach<'_> {
        Reach::Live(&self.places)
    }
}

async fn broker() -> (Broker, Arc<Shared>) {
    let broker = Broker::start(
        BrokerConfig::new("127.0.0.1:0".parse().unwrap()),
        Arc::new(MemoryStore::new()),
    )
    .await
    .unwrap();
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    (broker, shared)
}

async fn tick(shared: &Shared, topics: &Topics, slot: i64) {
    let at = iso(slot);
    let message = Outgoing {
        key: Some(format!("schedule/{slot}").into_bytes().into()),
        value: Some(
            envelope(
                &json!({ "scheduled_at": at }),
                &json!({ "trigger": "schedule", "at": at, "slot": slot }),
            )
            .into(),
        ),
        headers: Vec::new(),
    };
    shared.produce(&topics.input, 0, vec![message]).await.unwrap();
}

async fn polls_kept(shared: &Shared, topics: &Topics) -> usize {
    use kafka_protocol::records::RecordBatchDecoder;
    let mut client = shared.lend().await.unwrap();
    let fetched = client
        .fetch(&topics.input, &[(0, 0)], 0, 4 * 1024 * 1024)
        .await
        .unwrap();
    let mut kept = 0;
    for answer in fetched.into_iter().filter(|f| f.partition == 0) {
        let mut batches = answer.batches;
        while !batches.is_empty() {
            let Ok(set) = RecordBatchDecoder::decode_with_custom_compression(
                &mut batches,
                Some(openduct_broker::client::records::decompress),
            ) else {
                break;
            };
            kept += set
                .records
                .iter()
                .filter(|r| r.key.as_deref() == Some(&b"poll/seen"[..]))
                .count();
        }
    }
    kept
}

/// Ticks until a poll has kept what it saw (a time claimed while the group was shared out is passed by).
async fn first_poll(shared: &Shared, topics: &Topics, mut slot: i64) -> i64 {
    for _ in 0..30 {
        tick(shared, topics, slot).await;
        tick(shared, topics, slot).await;
        slot += 1_000;
        tokio::time::sleep(Duration::from_secs(1)).await;
        if polls_kept(shared, topics).await > 0 {
            return slot;
        }
    }
    panic!("no poll kept what it saw");
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

fn runner(
    workflow: &Arc<Workflow>,
    shared: &Arc<Shared>,
    node: &Arc<Node>,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<Result<(), String>>) {
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(serve(
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(node) as Arc<dyn Effects>,
        stopped,
    ));
    (stop, task)
}

fn watching(idle: bool) -> String {
    format!(
        "name: mail-{idle}\ntrigger:\n  mailbox: {{ credential: mail, folder: INBOX, every: 1m, idle: {idle} }}\n\
         steps:\n  - name: call\n    http:\n      method: POST\n      url: 'http://127.0.0.1:9/{{{{ value.subject }}}}-{{{{ value.text.trim() }}}}'\n"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_folders_new_messages_run_once_across_two_nodes_a_restart_and_a_rebuild() {
    let (port, state) = imap_support::start(None);
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("a@example.com", "old", "before"));
    let (broker, shared) = broker().await;
    let workflow = Arc::new(Workflow::parse(&watching(false)).unwrap());
    assert!(matches!(&workflow.trigger, Trigger::Mailbox(_)));
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    let (one, two) = (Node::new(port), Node::new(port));
    let both = || {
        let mut all = one.ran();
        all.extend(two.ran());
        all.sort();
        all
    };
    let (stop_one, first) = runner(&workflow, &shared, &one);
    let (stop_two, second) = runner(&workflow, &shared, &two);
    // The first poll notes where the folder is, and runs nothing.
    let slot = first_poll(&shared, &topics, 10_000).await;
    assert!(both().is_empty(), "{:?}", both());

    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("b@example.com", "new", "hello"));
    for s in [slot + 100_000, slot + 100_000, slot + 101_000] {
        tick(&shared, &topics, s).await;
    }
    until("the new message to run", || both() == ["new-hello"]).await;

    // A restart; a time queued late again, then a new one.
    stop_one.send(true).unwrap();
    stop_two.send(true).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let three = Node::new(port);
    let (stop_three, third) = runner(&workflow, &shared, &three);
    state.lock().unwrap().add(
        "INBOX",
        &imap_support::message("c@example.com", "after", "restart"),
    );
    tick(&shared, &topics, slot + 100_000).await;
    tick(&shared, &topics, slot + 200_000).await;
    until("the next new message, after the restart", || {
        three.ran() == ["after-restart"]
    })
    .await;

    // The folder rebuilt (another UIDVALIDITY): its messages are not new, and the polls go on from now.
    state.lock().unwrap().rebuild("INBOX", 8);
    let polled = polls_kept(&shared, &topics).await;
    tick(&shared, &topics, slot + 300_000).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while polls_kept(&shared, &topics).await <= polled {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the poll after the rebuild");
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("d@example.com", "later", "on"));
    tick(&shared, &topics, slot + 400_000).await;
    until("the message after the rebuild", || three.ran().len() == 2).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(both(), ["new-hello"], "nothing ran twice");
    assert_eq!(three.ran(), ["after-restart", "later-on"]);
    stop_three.send(true).unwrap();
    third.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_with_idle_asks_for_a_poll_as_a_message_arrives() {
    let (port, state) = imap_support::start(None);
    let (broker, shared) = broker().await;
    let workflow = Arc::new(Workflow::parse(&watching(true)).unwrap());
    let Trigger::Mailbox(w) = &workflow.trigger else {
        panic!("not a mailbox trigger");
    };
    prepare(&mut shared.lend().await.unwrap(), &workflow.id)
        .await
        .unwrap();
    let topics = Topics::of(&workflow.id);
    let node = Node::new(port);
    let (stop_runner, runner_task) = runner(&workflow, &shared, &node);
    first_poll(&shared, &topics, 10_000).await;
    let (stop, stopped) = watch::channel(false);
    let idle = tokio::spawn(openduct_flow::mailbox::idle(
        workflow.id.clone(),
        w.as_ref().clone(),
        Arc::clone(&shared),
        Arc::clone(&node) as Arc<dyn Effects>,
        stopped,
    ));
    until("the server to be waited on", || {
        state.lock().unwrap().commands.iter().any(|c| c == "IDLE")
    })
    .await;
    // No time is queued from here: the message runs because the server said it came.
    state
        .lock()
        .unwrap()
        .add("INBOX", &imap_support::message("e@example.com", "pushed", "now"));
    until("the message the server announced to run", || {
        node.ran() == ["pushed-now"]
    })
    .await;
    stop.send(true).unwrap();
    idle.await.unwrap();
    stop_runner.send(true).unwrap();
    runner_task.await.unwrap().unwrap();
    broker.shutdown().await;
}
