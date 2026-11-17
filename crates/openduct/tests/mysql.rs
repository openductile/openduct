// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The mysql_cdc connector as a person runs it, against a real MySQL when `OPENDUCT_TEST_MYSQL` names one (`cargo
//! xtask services up` starts one), with an independent client (mysql_async) making the changes. Each run works in a
//! database and with users of its own, and removes them after.

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

use mysql_async::prelude::Queryable;
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

fn server() -> Option<(String, String)> {
    Some((
        std::env::var("OPENDUCT_TEST_MYSQL").ok()?,
        std::env::var("OPENDUCT_TEST_MYSQL_ROOT_PASSWORD").ok()?,
    ))
}

/// An independent client, signed in as root, to set things up and make changes.
async fn root(url: &str, password: &str) -> mysql_async::Conn {
    let (host, port) = url.trim_start_matches("mysql://").split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host.to_owned())
        .tcp_port(port.parse().unwrap())
        .user(Some("root"))
        .pass(Some(password.to_owned()));
    mysql_async::Conn::new(opts).await.unwrap()
}

/// Run statements one at a time: one request is one transaction, which a ROLLBACK in it would undo.
async fn run(conn: &mut mysql_async::Conn, statements: &[String]) {
    for s in statements {
        conn.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

fn pipeline(dir: &tempfile::TempDir, name: &str, url: &str, user: &str, extra: &str) -> std::path::PathBuf {
    let file = dir.path().join(format!("{name}.yaml"));
    std::fs::write(
        &file,
        format!(
            "name: {name}\nfrom:\n  mysql_cdc:\n    url: {url}\n    username: {user}\n    password: secret:MYSQL_PASSWORD\n{extra}to:\n  topic: {name}\n"
        ),
    )
    .unwrap();
    file
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_rows_then_every_change_reach_a_topic_and_nothing_is_lost_while_stopped() {
    let Some((url, password)) = server() else {
        eprintln!("skipped: OPENDUCT_TEST_MYSQL is not set (cargo xtask services up)");
        return;
    };
    let r = run_name();
    let mut conn = root(&url, &password).await;
    run(&mut conn, &[
        format!("CREATE DATABASE {r}"),
        format!(
            "CREATE TABLE {r}.orders (id INT PRIMARY KEY, total DECIMAL(10,2), meta JSON, note VARCHAR(100), placed \
             DATETIME(3), status ENUM('new','paid'), data BLOB, stamp TIMESTAMP NULL)"
        ),
        format!(
            "INSERT INTO {r}.orders VALUES (1, 19.90, '{{\"vip\": true, \"n\": [1, 2.5]}}', 'first', '2026-11-15 \
             09:00:00.123', 'paid', X'01FF', '2026-11-15 09:00:00'), (2, 5.00, NULL, 'second', NULL, 'new', NULL, \
             NULL), (3, 1200.50, '[1, 2]', 'O''Brien', '2026-01-02 03:04:05.006', 'paid', X'00', NULL)"
        ),
        format!("CREATE USER 'c{r}'@'%' IDENTIFIED BY 'cdc-test-secret'"),
        format!("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'c{r}'@'%'"),
        format!("GRANT SELECT ON {r}.* TO 'c{r}'@'%'"),
    ])
    .await;
    let (local, here) = broker().await;
    let (h, rr, u, rt) = (
        here.clone(),
        r.clone(),
        url.clone(),
        tokio::runtime::Handle::current(),
    );
    let result = tokio::task::spawn_blocking(move || {
        let r = rr;
        let name = format!("{r}-in");
        openduct(&h, &["topic", "create", &name]);
        let dir = tempfile::tempdir().unwrap();
        let file = pipeline(
            &dir,
            &name,
            &u,
            &format!("c{r}"),
            &format!("    tables: [{r}.orders]\n"),
        );
        let env = [("MYSQL_PASSWORD", "cdc-test-secret")];

        let running = Running::start(&h, &file, &env);
        let got = wait_for(&h, &name, 3);
        assert_eq!(got.len(), 3, "{got:?}");
        let first = change(&got[0]);
        assert_eq!(first["op"], "snapshot");
        assert_eq!(first["table"], format!("{r}.orders"));
        let text = got[0]["value"].as_str().unwrap();
        assert!(text.contains(r#""total":19.90"#), "exact, as stored: {text}");
        assert_eq!(first["after"]["meta"]["vip"], true, "JSON as JSON");
        assert_eq!(first["after"]["status"], "paid");
        assert_eq!(first["after"]["data"], "\\x01ff");
        assert_eq!(first["after"]["placed"], "2026-11-15 09:00:00.123");
        assert_eq!(got[0]["key"], format!("{r}.orders/1"));
        rt.block_on(run(
            &mut conn,
            &[
                format!("INSERT INTO {r}.orders VALUES (4, 7.25, NULL, 'fourth', NULL, 'new', NULL, NULL)"),
                format!("UPDATE {r}.orders SET total = 7.50, status = 'paid' WHERE id = 4"),
                format!("DELETE FROM {r}.orders WHERE id = 2"),
            ],
        ));
        let got = wait_for(&h, &name, 6);
        let log = running.stop();
        assert_eq!(got.len(), 6, "{log}");
        let ops: Vec<String> = got[3..]
            .iter()
            .map(|m| change(m)["op"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ops, ["insert", "update", "delete"]);
        let update = change(&got[4]);
        assert!(
            got[4]["value"].as_str().unwrap().contains(r#""total":7.50"#),
            "{}",
            got[4]
        );
        assert_eq!(update["before"]["status"], "new", "the whole row before");
        assert!(update["binlog"].as_str().unwrap().starts_with("binlog."));

        // While it is stopped: a change, one rolled back, and a change to a row the first copy read.
        rt.block_on(run(
            &mut conn,
            &[
                format!("INSERT INTO {r}.orders VALUES (5, 1, NULL, NULL, NULL, 'new', NULL, NULL)"),
                "START TRANSACTION".to_owned(),
                format!("INSERT INTO {r}.orders VALUES (6, 1, NULL, NULL, NULL, 'new', NULL, NULL)"),
                "ROLLBACK".to_owned(),
                format!("UPDATE {r}.orders SET note = 'changed' WHERE id = 1"),
                format!("INSERT INTO {r}.orders VALUES (7, 1, NULL, NULL, NULL, 'new', NULL, NULL)"),
            ],
        ));
        let running = Running::start(&h, &file, &env);
        wait_for(&h, &name, 9);
        std::thread::sleep(Duration::from_secs(2));
        let log = running.stop();
        let got = wait_for(&h, &name, 9);
        let ids: Vec<i64> = got
            .iter()
            .map(|m| {
                let c = change(m);
                c["after"]["id"]
                    .as_i64()
                    .or_else(|| c["before"]["id"].as_i64())
                    .unwrap()
            })
            .collect();
        assert_eq!(
            ids,
            [1, 2, 3, 4, 4, 2, 5, 1, 7],
            "nothing lost, nothing twice: {log}"
        );
        // A row reads the same in the first copy and in the stream, but for what changed.
        let (snapshot, streamed) = (change(&got[0])["after"].clone(), change(&got[7])["after"].clone());
        let mut expected = snapshot.clone();
        expected["note"] = serde_json::json!("changed");
        assert_eq!(streamed, expected);
        conn
    })
    .await;
    let mut conn = match result {
        Ok(conn) => conn,
        Err(e) => {
            let mut c = root(&url, &password).await;
            let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {r}")).await;
            let _ = c.query_drop(format!("DROP USER IF EXISTS 'c{r}'@'%'")).await;
            std::panic::resume_unwind(e.into_panic());
        }
    };
    run(
        &mut conn,
        &[format!("DROP DATABASE {r}"), format!("DROP USER 'c{r}'@'%'")],
    )
    .await;
    let _ = conn.disconnect().await;
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_password_stops_with_the_reason_and_a_native_password_user_reads() {
    let Some((url, password)) = server() else {
        eprintln!("skipped: OPENDUCT_TEST_MYSQL is not set (cargo xtask services up)");
        return;
    };
    let r = run_name();
    let mut conn = root(&url, &password).await;
    run(
        &mut conn,
        &[
            format!("CREATE DATABASE {r}"),
            format!("CREATE TABLE {r}.doors (id INT PRIMARY KEY, open BOOL)"),
            format!("CREATE USER 'n{r}'@'%' IDENTIFIED WITH mysql_native_password BY 'native-test-secret'"),
            format!("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'n{r}'@'%'"),
            format!("GRANT SELECT ON {r}.* TO 'n{r}'@'%'"),
        ],
    )
    .await;
    let (local, here) = broker().await;
    let (h, rr, u, rt) = (
        here.clone(),
        r.clone(),
        url.clone(),
        tokio::runtime::Handle::current(),
    );
    let result = tokio::task::spawn_blocking(move || {
        let r = rr;
        let dir = tempfile::tempdir().unwrap();
        let wrong = "not-the-password-5521";
        let name = format!("{r}-wrong");
        openduct(&h, &["topic", "create", &name]);
        let file = pipeline(
            &dir,
            &name,
            &u,
            &format!("n{r}"),
            &format!("    tables: [{r}.doors]\n"),
        );
        let (ok, log) = Running::start(&h, &file, &[("MYSQL_PASSWORD", wrong)]).ended();
        assert!(!ok && log.contains("refused the username or password"), "{log}");
        assert!(!log.contains(wrong), "the password is never shown: {log}");

        let name = format!("{r}-native");
        openduct(&h, &["topic", "create", &name]);
        let file = pipeline(
            &dir,
            &name,
            &u,
            &format!("n{r}"),
            &format!("    tables: [{r}.doors]\n    snapshot: false\n"),
        );
        let running = Running::start(&h, &file, &[("MYSQL_PASSWORD", "native-test-secret")]);
        std::thread::sleep(Duration::from_secs(3));
        rt.block_on(run(
            &mut conn,
            &[format!("INSERT INTO {r}.doors VALUES (3, true)")],
        ));
        let got = wait_for(&h, &name, 1);
        let log = running.stop();
        assert_eq!(got.len(), 1, "{log}");
        assert_eq!(change(&got[0])["after"], serde_json::json!({"id": 3, "open": 1}));
        conn
    })
    .await;
    let mut conn = match result {
        Ok(conn) => conn,
        Err(e) => {
            let mut c = root(&url, &password).await;
            let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {r}")).await;
            let _ = c.query_drop(format!("DROP USER IF EXISTS 'n{r}'@'%'")).await;
            std::panic::resume_unwind(e.into_panic());
        }
    };
    run(
        &mut conn,
        &[format!("DROP DATABASE {r}"), format!("DROP USER 'n{r}'@'%'")],
    )
    .await;
    let _ = conn.disconnect().await;
    local.shutdown().await;
}

#[test]
fn settings_that_cannot_work_are_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    let check = |name: &str, from: &str| {
        let file = dir.path().join(format!("{name}.yaml"));
        std::fs::write(
            &file,
            format!("name: {name}\nfrom:\n  mysql_cdc:\n{from}to:\n  topic: changes\n"),
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
        "    url: mysql://db.example.com\n    username: secret:MYSQL_USER\n    password: secret:MYSQL_PASSWORD\n    tls: true\n    tables: [shop.orders, shop.customers]\n    server_id: 4242\n",
    );
    assert!(ok, "{text}");
    assert!(
        text.contains(
            "reads mysql_cdc mysql://db.example.com:3306 shop.orders, shop.customers as server 4242"
        ),
        "{text}"
    );
    for (name, from, why) in [
        (
            "userinurl",
            "    url: mysql://me:pw@db\n    username: me\n    tables: [a.b]\n",
            "settings of their own",
        ),
        (
            "notable",
            "    url: mysql://db\n    username: me\n",
            "tables: is required",
        ),
        (
            "nodb",
            "    url: mysql://db\n    username: me\n    tables: [orders]\n",
            "is database.table",
        ),
        (
            "ca",
            "    url: mysql://db\n    username: me\n    tables: [a.b]\n    ca: file:/x.pem\n",
            "is for tls: true",
        ),
        (
            "id",
            "    url: mysql://db\n    username: me\n    tables: [a.b]\n    server_id: 0\n",
            "whole number from 1",
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
