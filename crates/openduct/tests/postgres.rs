// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The postgres_cdc connector as a person runs it, against a real PostgreSQL when `OPENDUCT_TEST_POSTGRES` names one
//! (`cargo xtask services up` starts one), with an independent client (tokio-postgres) making the changes and
//! reading what the server says the pipeline has confirmed. Each run works in a schema of its own and removes its
//! slot and publication after, so the test server keeps no WAL for it.

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

/// The change each message holds, as JSON.
fn change(message: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(message["value"].as_str().unwrap()).unwrap()
}

struct Database {
    url: String,
    user: String,
    password: String,
}

fn database() -> Option<Database> {
    Some(Database {
        url: std::env::var("OPENDUCT_TEST_POSTGRES").ok()?,
        user: std::env::var("OPENDUCT_TEST_POSTGRES_USER").ok()?,
        password: std::env::var("OPENDUCT_TEST_POSTGRES_PASSWORD").ok()?,
    })
}

/// An independent client, signed in as the test user.
async fn sql(db: &Database) -> tokio_postgres::Client {
    let rest = db.url.trim_start_matches("postgres://");
    let (address, name) = rest.split_once('/').unwrap();
    let (host, port) = address.split_once(':').unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host(host)
        .port(port.parse().unwrap())
        .dbname(name)
        .user(&db.user)
        .password(&db.password)
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

/// Remove what a run made in the server: its slots, publications and schema.
async fn clean(client: &tokio_postgres::Client, run: &str) {
    let _ = client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'openduct_{run}%' \
             AND NOT active; DO $$ DECLARE p text; BEGIN FOR p IN SELECT pubname FROM pg_publication WHERE pubname \
             LIKE 'openduct_{run}%' LOOP EXECUTE 'DROP PUBLICATION ' || quote_ident(p); END LOOP; END $$; DROP SCHEMA \
             IF EXISTS {run} CASCADE;"
        ))
        .await;
}

fn pipeline(dir: &tempfile::TempDir, name: &str, db: &Database, extra: &str) -> std::path::PathBuf {
    let file = dir.path().join(format!("{name}.yaml"));
    std::fs::write(
        &file,
        format!(
            "name: {name}\nfrom:\n  postgres_cdc:\n    url: {}\n    username: {}\n    password: secret:PG_PASSWORD\n{extra}to:\n  topic: {name}\n",
            db.url, db.user
        ),
    )
    .unwrap();
    file
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_rows_then_every_change_reach_a_topic_and_nothing_is_lost_while_stopped() {
    let Some(db) = database() else {
        eprintln!("skipped: OPENDUCT_TEST_POSTGRES is not set (cargo xtask services up)");
        return;
    };
    let run = run_name();
    let client = sql(&db).await;
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {run}; CREATE TABLE {run}.orders (id int PRIMARY KEY, total numeric(10,2), paid bool, meta \
             jsonb, note text, placed timestamptz); INSERT INTO {run}.orders VALUES (1, 19.90, true, '{{\"vip\": true}}', \
             'first', '2026-11-15 09:00:00+00'), (2, 5.00, false, null, 'second', null), (3, 1200.50, true, '[1, 2]', \
             'O''Brien', null);"
        ))
        .await
        .unwrap();
    let (local, here) = broker().await;
    let (h, r, rt) = (here.clone(), run.clone(), tokio::runtime::Handle::current());
    let password = db.password.clone();
    let db2 = Database {
        url: db.url.clone(),
        user: db.user.clone(),
        password: db.password.clone(),
    };
    let result = tokio::task::spawn_blocking(move || {
        let name = format!("{r}-in");
        openduct(&h, &["topic", "create", &name]);
        let dir = tempfile::tempdir().unwrap();
        let file = pipeline(&dir, &name, &db2, &format!("    tables: [{r}.orders]\n"));
        let env = [("PG_PASSWORD", password.as_str())];

        // The rows already there, then changes as they are made.
        let running = Running::start(&h, &file, &env);
        let got = wait_for(&h, &name, 3);
        assert_eq!(got.len(), 3, "{got:?}");
        let first = change(&got[0]);
        assert_eq!(first["op"], "snapshot");
        assert_eq!(first["table"], format!("{r}.orders"));
        assert!(got[0]["value"].as_str().unwrap().contains(r#""total":19.90"#), "exact, as stored: {}", got[0]);
        assert_eq!(first["after"]["paid"], true);
        assert_eq!(first["after"]["meta"]["vip"], true, "JSON as JSON");
        assert_eq!(got[0]["key"], format!("{r}.orders/1"));
        rt.block_on(client.batch_execute(&format!(
            "INSERT INTO {r}.orders VALUES (4, 7.25, false, null, 'fourth', null); UPDATE {r}.orders SET paid = true, \
             total = 7.50 WHERE id = 4; DELETE FROM {r}.orders WHERE id = 2;"
        )))
        .unwrap();
        let got = wait_for(&h, &name, 6);
        let log = running.stop();
        assert_eq!(got.len(), 6, "{log}");
        let ops: Vec<String> = got[3..].iter().map(|m| change(m)["op"].as_str().unwrap().to_owned()).collect();
        assert_eq!(ops, ["insert", "update", "delete"]);
        let update = change(&got[4]);
        assert!(got[4]["value"].as_str().unwrap().contains(r#""total":7.50"#), "{}", got[4]);
        assert_eq!(update["before"], serde_json::Value::Null, "not sent when the key did not change");
        assert!(update["committed"].as_str().unwrap().ends_with('Z'));
        assert_eq!(change(&got[5])["before"]["id"], 2);

        // What changes while it is stopped arrives after; a rolled-back change never does; nothing comes twice.
        let mut client = client;
        let last: String = rt.block_on(async {
            // One statement a request: several in one run as a single transaction, which a ROLLBACK would undo.
            for statement in [
                format!("INSERT INTO {r}.orders VALUES (5, 1, false, null, null, null)"),
                format!("BEGIN; INSERT INTO {r}.orders VALUES (6, 1, false, null, null, null); ROLLBACK"),
                format!("ALTER TABLE {r}.orders REPLICA IDENTITY FULL"),
                format!("UPDATE {r}.orders SET note = 'changed' WHERE id = 3"),
            ] {
                client.batch_execute(&statement).await.unwrap();
            }
            let tx = client.transaction().await.unwrap();
            tx.batch_execute(&format!("INSERT INTO {r}.orders VALUES (7, 1, false, null, null, null)"))
                .await
                .unwrap();
            // A position inside the last transaction: once it is stored, the server must hear it is safe.
            let at: String = tx.query_one("SELECT pg_current_wal_lsn()::text", &[]).await.unwrap().get(0);
            tx.commit().await.unwrap();
            at
        });
        let running = Running::start(&h, &file, &env);
        wait_for(&h, &name, 9);
        std::thread::sleep(Duration::from_secs(2));
        let log = running.stop();
        let got = wait_for(&h, &name, 9);
        let ids: Vec<i64> = got.iter().map(|m| {
            let c = change(m);
            c["after"]["id"].as_i64().or_else(|| c["before"]["id"].as_i64()).unwrap()
        }).collect();
        assert_eq!(ids, [1, 2, 3, 4, 4, 2, 5, 3, 7], "{log}");
        // With REPLICA IDENTITY FULL, the whole row before.
        let full = change(&got[7]);
        assert_eq!((full["before"]["note"].as_str(), full["after"]["note"].as_str()), (Some("O'Brien"), Some("changed")));

        // The server knows how far is safe, so it can remove the WAL before.
        let safe: bool = rt
            .block_on(client.query_one(
                &format!(
                    "SELECT confirmed_flush_lsn > '{last}'::pg_lsn FROM pg_replication_slots WHERE slot_name = \
                     'openduct_{r}_in'"
                ),
                &[],
            ))
            .unwrap()
            .get(0);
        assert!(safe, "the slot was told the last transaction is stored");
        client
    })
    .await;
    let client = match result {
        Ok(client) => client,
        Err(e) => {
            clean(&sql(&db).await, &run).await;
            std::panic::resume_unwind(e.into_panic());
        }
    };
    clean(&client, &run).await;
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_password_stops_with_the_reason_and_an_md5_user_signs_in() {
    let Some(db) = database() else {
        eprintln!("skipped: OPENDUCT_TEST_POSTGRES is not set (cargo xtask services up)");
        return;
    };
    let run = run_name();
    let client = sql(&db).await;
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {run}; CREATE TABLE {run}.doors (id int PRIMARY KEY, open bool); SET password_encryption = \
             'md5'; CREATE ROLE {run}_md5 LOGIN REPLICATION PASSWORD 'md5-test-secret'; GRANT USAGE ON SCHEMA {run} TO \
             {run}_md5; GRANT SELECT ON {run}.doors TO {run}_md5; CREATE PUBLICATION openduct_{run}_md5 FOR TABLE \
             {run}.doors;"
        ))
        .await
        .unwrap();
    let (local, here) = broker().await;
    let (h, r, rt) = (here.clone(), run.clone(), tokio::runtime::Handle::current());
    let db2 = Database {
        url: db.url.clone(),
        user: db.user.clone(),
        password: db.password.clone(),
    };
    let result = tokio::task::spawn_blocking(move || {
        let dir = tempfile::tempdir().unwrap();
        let wrong = "not-the-password-8812";
        let name = format!("{r}-wrong");
        openduct(&h, &["topic", "create", &name]);
        let file = pipeline(&dir, &name, &db2, &format!("    tables: [{r}.doors]\n"));
        let (ok, log) = Running::start(&h, &file, &[("PG_PASSWORD", wrong)]).ended();
        assert!(!ok && log.contains("refused the username or password"), "{log}");
        assert!(!log.contains(wrong), "the password is never shown: {log}");

        // A user whose password is stored as MD5, reading a publication an administrator made.
        let name = format!("{r}-md5");
        openduct(&h, &["topic", "create", &name]);
        let md5 = Database {
            url: db2.url.clone(),
            user: format!("{r}_md5"),
            password: String::new(),
        };
        let file = pipeline(&dir, &name, &md5, "    snapshot: false\n");
        let running = Running::start(&h, &file, &[("PG_PASSWORD", "md5-test-secret")]);
        std::thread::sleep(Duration::from_secs(3));
        rt.block_on(client.batch_execute(&format!("INSERT INTO {r}.doors VALUES (3, true)")))
            .unwrap();
        let got = wait_for(&h, &name, 1);
        let log = running.stop();
        assert_eq!(got.len(), 1, "{log}");
        assert_eq!(
            change(&got[0])["after"],
            serde_json::json!({"id": 3, "open": true})
        );
        client
    })
    .await;
    let client = match result {
        Ok(client) => client,
        Err(e) => {
            let c = sql(&db).await;
            clean(&c, &run).await;
            let _ = c.batch_execute(&format!("DROP ROLE IF EXISTS {run}_md5")).await;
            std::panic::resume_unwind(e.into_panic());
        }
    };
    clean(&client, &run).await;
    let _ = client
        .batch_execute(&format!("DROP ROLE IF EXISTS {run}_md5"))
        .await;
    local.shutdown().await;
}

#[test]
fn settings_that_cannot_work_are_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    let check = |name: &str, from: &str| {
        let file = dir.path().join(format!("{name}.yaml"));
        std::fs::write(
            &file,
            format!("name: {name}\nfrom:\n  postgres_cdc:\n{from}to:\n  topic: changes\n"),
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
        "    url: postgres://db.example.com/shop\n    username: secret:PG_USER\n    password: secret:PG_PASSWORD\n    tls: true\n    tables: [public.orders, customers]\n",
    );
    assert!(ok, "{text}");
    assert!(
        text.contains(
            "reads postgres_cdc postgres://db.example.com:5432/shop public.orders, public.customers as slot openduct_orders_in"
        ),
        "{text}"
    );
    for (name, from, why) in [
        (
            "userinurl",
            "    url: postgres://me:pw@db/shop\n    username: me\n",
            "settings of their own",
        ),
        (
            "nodb",
            "    url: postgres://db\n    username: me\n",
            "names no database",
        ),
        (
            "table",
            "    url: postgres://db/shop\n    username: me\n    tables: [\"orders; drop\"]\n",
            "is not schema.table",
        ),
        (
            "ca",
            "    url: postgres://db/shop\n    username: me\n    ca: file:/x.pem\n",
            "is for tls: true",
        ),
        (
            "slot",
            "    url: postgres://db/shop\n    username: me\n    slot: My-Slot\n",
            "lower case letters",
        ),
        ("nouser", "    url: postgres://db/shop\n", "username: is required"),
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
