// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The mongodb_cdc connector as a person runs it, against a real MongoDB replica set when `OPENDUCT_TEST_MONGODB`
//! names one (`cargo xtask services up` starts one). Each run works in a database and with a user of its own (read
//! access only, the least a person would give it), and removes them after.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use mongodb::bson::{DateTime, Decimal128, Document, doc, oid::ObjectId};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

async fn broker() -> (Broker, String) {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

fn openduct(addr: &str, args: &[&str]) -> String {
    let out = Command::new(OPENDUCT)
        .args(args)
        .args(["--bootstrap", addr])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn run_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Tests run at once, and a clock may count only microseconds: a count keeps their names apart.
    static COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("t{:x}{n}", nanos % (1 << 40))
}

struct Running {
    child: Child,
    log: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Running {
    fn start(addr: &str, file: &std::path::Path, env: &[(&str, &str)]) -> Self {
        let mut child = Command::new(OPENDUCT)
            .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", addr])
            .args(secrets_args(file, env))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, log) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = tx.send(line);
            }
        });
        Self {
            child,
            log,
            seen: Vec::new(),
        }
    }

    fn stop(mut self) -> String {
        Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        let status = self.child.wait().unwrap();
        while let Ok(line) = self.log.recv_timeout(Duration::from_secs(2)) {
            self.seen.push(line);
        }
        let log = self.seen.join("\n");
        assert!(
            status.success(),
            "the pipeline had stopped by itself ({status}):\n{log}"
        );
        log
    }

    fn ended(mut self) -> (bool, String) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "it did not stop: {:?}", self.seen);
            std::thread::sleep(Duration::from_millis(100));
        };
        while let Ok(line) = self.log.recv_timeout(Duration::from_secs(1)) {
            self.seen.push(line);
        }
        (status.success(), self.seen.join("\n"))
    }
}

fn wait_for(addr: &str, topic: &str, want: usize) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let messages: Vec<serde_json::Value> = openduct(addr, &["read", topic, "--format", "json"])
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        if messages.len() >= want || Instant::now() > deadline {
            return messages;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

fn change(message: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(message["value"].as_str().unwrap()).unwrap()
}

struct Server {
    url: String,
    user: String,
    password: String,
}

fn server() -> Option<Server> {
    Some(Server {
        url: std::env::var("OPENDUCT_TEST_MONGODB").ok()?,
        user: std::env::var("OPENDUCT_TEST_MONGODB_USER").ok()?,
        password: std::env::var("OPENDUCT_TEST_MONGODB_PASSWORD").ok()?,
    })
}

/// An independent client, signed in as the test's administrator.
async fn admin(s: &Server) -> mongodb::Client {
    let mut options = mongodb::options::ClientOptions::parse(&s.url).await.unwrap();
    options.credential = Some(
        mongodb::options::Credential::builder()
            .username(s.user.clone())
            .password(s.password.clone())
            .source("admin".to_owned())
            .build(),
    );
    mongodb::Client::with_options(options).unwrap()
}

/// A user who may only read the run's database (which includes reading its change streams).
async fn reader(client: &mongodb::Client, db: &str, user: &str, password: &str) {
    client
        .database("admin")
        .run_command(doc! { "createUser": user, "pwd": password, "roles": [{ "role": "read", "db": db }] })
        .await
        .unwrap();
}

async fn clean(client: &mongodb::Client, db: &str, user: &str) {
    let _ = client.database(db).drop().await;
    let _ = client
        .database("admin")
        .run_command(doc! { "dropUser": user })
        .await;
}

fn pipeline(dir: &tempfile::TempDir, name: &str, url: &str, user: &str, extra: &str) -> std::path::PathBuf {
    let file = dir.path().join(format!("{name}.yaml"));
    std::fs::write(
        &file,
        format!(
            "name: {name}\nfrom:\n  mongodb_cdc:\n    url: {url}\n    username: {user}\n    password: secret:MONGO_PASSWORD\n    auth_source: admin\n{extra}to:\n  topic: {name}\n"
        ),
    )
    .unwrap();
    file
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_documents_then_every_change_reach_a_topic_and_nothing_is_lost_while_stopped() {
    let Some(s) = server() else {
        eprintln!("skipped: OPENDUCT_TEST_MONGODB is not set (cargo xtask services up)");
        return;
    };
    let r = run_name();
    let client = admin(&s).await;
    let db = client.database(&r);
    // Documents as they were before a change are kept, so updates and deletes say what they changed from.
    db.run_command(doc! { "create": "orders", "changeStreamPreAndPostImages": { "enabled": true } })
        .await
        .unwrap();
    let orders = db.collection::<Document>("orders");
    let first = ObjectId::parse_str("6738a1f2c4e5d6a7b8c9d0e1").unwrap();
    orders
        .insert_many([
            doc! {
                "_id": first,
                "total": "19.90".parse::<Decimal128>().unwrap(),
                "placed": DateTime::from_millis(1_794_736_351_977),
                "items": [{ "sku": "TEE", "qty": 2 }],
                "status": "paid",
            },
            doc! { "_id": 2, "total": "5.00".parse::<Decimal128>().unwrap(), "status": "new" },
            doc! { "_id": 3, "status": "new", "note": "O'Brien" },
        ])
        .await
        .unwrap();
    reader(&client, &r, &format!("c{r}"), "cdc-test-secret").await;
    let (local, here) = broker().await;
    let (h, rr, url, rt) = (
        here.clone(),
        r.clone(),
        s.url.clone(),
        tokio::runtime::Handle::current(),
    );
    let (c2, orders2) = (client.clone(), orders.clone());
    let result = tokio::task::spawn_blocking(move || {
        let r = rr;
        let name = format!("{r}-in");
        openduct(&h, &["topic", "create", &name]);
        let dir = tempfile::tempdir().unwrap();
        let file = pipeline(
            &dir,
            &name,
            &url,
            &format!("c{r}"),
            &format!("    collections: [{r}.orders]\n"),
        );
        let env = [("MONGO_PASSWORD", "cdc-test-secret")];

        let running = Running::start(&h, &file, &env);
        let got = wait_for(&h, &name, 3);
        assert_eq!(got.len(), 3, "{got:?}");
        let text = got[0]["value"].as_str().unwrap();
        assert!(text.contains(r#""total":19.90"#), "exact, as stored: {text}");
        let one = change(&got[0]);
        assert_eq!(one["op"], "snapshot");
        assert_eq!(one["after"]["_id"], "6738a1f2c4e5d6a7b8c9d0e1");
        assert_eq!(one["after"]["placed"], "2026-11-15T09:52:31.977Z");
        assert_eq!(one["after"]["items"][0]["qty"], 2);
        assert_eq!(got[0]["key"], format!("{r}.orders/6738a1f2c4e5d6a7b8c9d0e1"));
        rt.block_on(async {
            orders2
                .insert_one(doc! { "_id": 4, "status": "new" })
                .await
                .unwrap();
            orders2
                .update_one(doc! { "_id": 4 }, doc! { "$set": { "status": "paid" } })
                .await
                .unwrap();
            orders2
                .replace_one(doc! { "_id": 3 }, doc! { "status": "shipped" })
                .await
                .unwrap();
            orders2.delete_one(doc! { "_id": 2 }).await.unwrap();
        });
        let got = wait_for(&h, &name, 7);
        let log = running.stop();
        assert_eq!(got.len(), 7, "{log}");
        let ops: Vec<String> = got[3..]
            .iter()
            .map(|m| change(m)["op"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ops, ["insert", "update", "replace", "delete"]);
        let update = change(&got[4]);
        assert_eq!(
            update["changes"]["updated"],
            serde_json::json!({ "status": "paid" })
        );
        assert_eq!(update["before"]["status"], "new", "the document as it was");
        assert_eq!(update["after"]["status"], "paid");
        let delete = change(&got[6]);
        assert_eq!(delete["after"], serde_json::Value::Null);
        assert_eq!(delete["before"]["status"], "new");
        assert_eq!(got[6]["key"], format!("{r}.orders/2"));

        // While it is stopped: a change, a transaction that is aborted, and another change.
        rt.block_on(async {
            orders2
                .insert_one(doc! { "_id": 5, "status": "new" })
                .await
                .unwrap();
            let mut session = c2.start_session().await.unwrap();
            session.start_transaction().await.unwrap();
            orders2
                .insert_one(doc! { "_id": 6, "status": "never" })
                .session(&mut session)
                .await
                .unwrap();
            session.abort_transaction().await.unwrap();
            orders2
                .insert_one(doc! { "_id": 7, "status": "new" })
                .await
                .unwrap();
        });
        let running = Running::start(&h, &file, &env);
        wait_for(&h, &name, 9);
        std::thread::sleep(Duration::from_secs(2));
        let log = running.stop();
        let got = wait_for(&h, &name, 9);
        let ids: Vec<String> = got
            .iter()
            .map(|m| {
                let c = change(m);
                let doc = if c["after"].is_null() {
                    &c["before"]
                } else {
                    &c["after"]
                };
                doc["_id"].to_string()
            })
            .collect();
        assert_eq!(
            ids,
            [
                r#""6738a1f2c4e5d6a7b8c9d0e1""#,
                "2",
                "3",
                "4",
                "4",
                "3",
                "2",
                "5",
                "7"
            ],
            "nothing lost, nothing twice, nothing aborted: {log}"
        );
    })
    .await;
    clean(&client, &r, &format!("c{r}")).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_password_stops_with_the_reason_and_a_delete_without_kept_documents_names_its_key() {
    let Some(s) = server() else {
        eprintln!("skipped: OPENDUCT_TEST_MONGODB is not set (cargo xtask services up)");
        return;
    };
    let r = run_name();
    let client = admin(&s).await;
    let doors = client.database(&r).collection::<Document>("doors");
    doors.insert_one(doc! { "_id": 1, "open": true }).await.unwrap();
    reader(&client, &r, &format!("c{r}"), "cdc-test-secret").await;
    let (local, here) = broker().await;
    let (h, rr, url, rt) = (
        here.clone(),
        r.clone(),
        s.url.clone(),
        tokio::runtime::Handle::current(),
    );
    let result = tokio::task::spawn_blocking(move || {
        let r = rr;
        let dir = tempfile::tempdir().unwrap();
        let wrong = "not-the-password-7340";
        let name = format!("{r}-wrong");
        openduct(&h, &["topic", "create", &name]);
        let file = pipeline(
            &dir,
            &name,
            &url,
            &format!("c{r}"),
            &format!("    collections: [{r}.doors]\n"),
        );
        let (ok, log) = Running::start(&h, &file, &[("MONGO_PASSWORD", wrong)]).ended();
        assert!(!ok && log.contains("refused the username or password"), "{log}");
        assert!(!log.contains(wrong), "the password is never shown: {log}");

        let name = format!("{r}-changes");
        openduct(&h, &["topic", "create", &name]);
        let file = pipeline(
            &dir,
            &name,
            &url,
            &format!("c{r}"),
            &format!("    collections: [{r}.doors]\n    snapshot: false\n"),
        );
        let running = Running::start(&h, &file, &[("MONGO_PASSWORD", "cdc-test-secret")]);
        std::thread::sleep(Duration::from_secs(3));
        rt.block_on(async {
            doors.delete_one(doc! { "_id": 1 }).await.unwrap();
        });
        let got = wait_for(&h, &name, 1);
        let log = running.stop();
        assert_eq!(
            got.len(),
            1,
            "only changes, not the document already there: {log}"
        );
        assert_eq!(
            change(&got[0])["before"],
            serde_json::json!({ "_id": 1 }),
            "the key, without a kept document"
        );
    })
    .await;
    clean(&client, &r, &format!("c{r}")).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
    local.shutdown().await;
}

#[test]
fn settings_that_cannot_work_are_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    let check = |name: &str, from: &str| {
        let file = dir.path().join(format!("{name}.yaml"));
        std::fs::write(
            &file,
            format!("name: {name}\nfrom:\n  mongodb_cdc:\n{from}to:\n  topic: changes\n"),
        )
        .unwrap();
        let out = Command::new(OPENDUCT)
            .args(["pipeline", "check", file.to_str().unwrap()])
            .output()
            .unwrap();
        (
            out.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    };
    let (ok, text) = check(
        "orders-in",
        "    url: mongodb+srv://cluster0.abcde.mongodb.net\n    username: secret:MONGO_USER\n    password: secret:MONGO_PASSWORD\n    collections: [shop.orders, shop.customers]\n",
    );
    assert!(ok, "{text}");
    assert!(
        text.contains("reads mongodb_cdc cluster0.abcde.mongodb.net shop.orders, shop.customers"),
        "{text}"
    );
    for (name, from, why) in [
        (
            "login",
            "    url: mongodb://me:pw@db\n    collections: [a.b]\n",
            "no username or password in it",
        ),
        ("none", "    url: mongodb://db\n", "collections: is required"),
        (
            "nodb",
            "    url: mongodb://db\n    collections: [orders]\n",
            "is database.collection",
        ),
        (
            "scheme",
            "    url: mongo://db\n    collections: [a.b]\n",
            "is mongodb://host:port",
        ),
        (
            "half",
            "    url: mongodb://db\n    collections: [a.b]\n    client_cert: file:/c.pem\n",
            "go together",
        ),
    ] {
        let (ok, text) = check(name, from);
        assert!(!ok && text.contains(why), "{name}: {text}");
    }
}

/// The secrets a pipeline names, in a secrets file beside it that only its owner may read: `--secrets-file <it>`.
fn secrets_args(file: &std::path::Path, secrets: &[(&str, &str)]) -> Vec<String> {
    let path = file.with_extension("secrets");
    let text: String = secrets
        .iter()
        .map(|(k, v)| {
            let v = v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
            format!("{k}=\"{v}\"\n")
        })
        .collect();
    std::fs::write(&path, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    vec!["--secrets-file".to_owned(), path.to_str().unwrap().to_owned()]
}
