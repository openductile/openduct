// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `mysql` and `sqlserver` steps, and a database trigger's poll, against stand-ins that speak MySQL's and SQL
//! Server's protocols (tests/stand_in): values go beside the SQL with their own types, rows come back in theirs,
//! read-only steps run where the database keeps them from writing, statements run in one transaction when asked,
//! limits hold, TLS trusts only the certificate it is given, and a test run or a preview sends no write. The
//! stand-ins are not real servers: what a real one does is the live tests' to show.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod stand_in;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use openduct_expr::Value;
use openduct_flow::{
    Credential, DryRun, Effects, Event, Live, Outcome, Preview, RunRecord, Secrets, Trigger, Workflow, run,
};
use serde_json::{Value as Json, json};
use stand_in::{mysql, tds};

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

fn credential(kind: &str, port: &str, user: &str, password: &str, more: &[(&str, &str)]) -> Credential {
    let mut fields: BTreeMap<String, String> = [
        ("host", "127.0.0.1"),
        ("port", port),
        ("username", user),
        ("password", password),
        ("tls", "disable"),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
    .collect();
    for (k, v) in more {
        fields.insert((*k).to_owned(), (*v).to_owned());
    }
    Credential {
        kind: kind.to_owned(),
        fields,
    }
}

fn live(credentials: Vec<(&str, Credential)>, allow_private: bool) -> Live {
    let kept = Kept(credentials.into_iter().map(|(n, c)| (n.to_owned(), c)).collect());
    Live::new("127.0.0.1:1", allow_private)
        .unwrap()
        .with_secrets(Arc::new(kept))
}

async fn go(workflow: &str, input: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap();
    let event = Event {
        value: Value::parse_json(&input.to_string()).unwrap(),
        details: Value::Null,
    };
    run(&workflow, &event, effects).await
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

fn result(record: &RunRecord, step: &str) -> Json {
    let found = record.steps.iter().find(|s| s.name == step).unwrap();
    serde_json::from_str(&found.result.to_json()).unwrap()
}

// --- MySQL ---

/// The stand-in's answers: an order by its id; an order added; a procedure that writes; many rows; a slow query.
fn shop() -> Arc<mysql::Answer> {
    use mysql::{Cell, Reply, col};
    Arc::new(|sql: &str, values: &[mysql::Val], session: &mysql::Session| {
        let columns = || {
            vec![
                col("id", mysql::LONGLONG),
                col("customer", mysql::VAR_STRING),
                col("total", mysql::NEWDECIMAL),
                col("paid", mysql::TINY),
                col("placed", mysql::DATETIME),
                col("notes", mysql::VAR_STRING),
                col("extra", mysql::JSON),
                col("ratio", mysql::DOUBLE),
            ]
        };
        match sql {
            s if s.starts_with("SELECT id, customer") => {
                let mysql::Val::Int(id) = values[0] else {
                    panic!("{values:?}")
                };
                Reply::Rows(
                    columns(),
                    vec![vec![
                        Cell::Int(id),
                        Cell::Text("Zoë".into()),
                        Cell::Text("19.90".into()),
                        Cell::Int(1),
                        Cell::DateTime(2026, 11, 27, 9, 30, 5, 123_000),
                        Cell::Null,
                        Cell::Text(r#"{"tags": ["a", "b"]}"#.into()),
                        Cell::Double(0.25),
                    ]],
                )
            }
            s if s.starts_with("INSERT") || s.starts_with("UPDATE") || s.starts_with("SELECT add_order") => {
                if session.read_only {
                    Reply::Error(
                        1792,
                        "25006",
                        "Cannot execute statement in a READ ONLY transaction.".into(),
                    )
                } else if values.contains(&mysql::Val::Int(999)) {
                    Reply::Error(
                        1452,
                        "23000",
                        "Cannot add or update a child row: a foreign key constraint fails".into(),
                    )
                } else {
                    Reply::Done {
                        affected: 1,
                        insert_id: if s.starts_with("INSERT") { 43 } else { 0 },
                    }
                }
            }
            "SELECT n FROM big" => Reply::Rows(
                vec![col("n", mysql::LONGLONG)],
                (0..20).map(|n| vec![Cell::Int(n)]).collect(),
            ),
            "SELECT 'slow'" => Reply::After(
                Duration::from_secs(10),
                Box::new(Reply::Done {
                    affected: 0,
                    insert_id: 0,
                }),
            ),
            other => Reply::Error(
                1064,
                "42000",
                format!("You have an error in your SQL syntax near '{other}'"),
            ),
        }
    })
}

async fn shop_db() -> (mysql::MySql, Live) {
    let server = mysql::MySql::start(shop()).await;
    let effects = live(
        vec![(
            "db",
            credential(
                "mysql",
                &server.port(),
                "shop",
                mysql::PASSWORD,
                &[("database", "shop")],
            ),
        )],
        true,
    );
    (server, effects)
}

const ORDER: &str = "name: order\nsteps:\n  - name: q\n    mysql:\n      credential: db\n      sql: SELECT id, customer, total, paid, placed, notes, extra, ratio FROM orders WHERE id = ?\n      params: [value.id]\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_rows_come_back_as_their_types_and_values_go_beside_the_sql() {
    let (server, effects) = shop_db().await;
    let record = go(ORDER, json!({ "id": 42 }), &effects).await;
    done(&record);
    let q = result(&record, "q");
    assert_eq!(q["count"], 1);
    assert_eq!(
        q["columns"],
        json!([
            "id", "customer", "total", "paid", "placed", "notes", "extra", "ratio"
        ])
    );
    let row = &q["rows"][0];
    assert_eq!(row["id"], 42);
    assert_eq!(row["customer"], "Zoë");
    // Exact, as a run's values are: without trailing zeros.
    assert_eq!(row["total"].to_string(), "19.9", "a decimal stays exact");
    assert_eq!(row["paid"], 1);
    assert_eq!(row["placed"], "2026-11-27 09:30:05.123");
    assert_eq!(row["notes"], Json::Null);
    assert_eq!(row["extra"], json!({ "tags": ["a", "b"] }));
    assert_eq!(row["ratio"], 0.25);

    let sneaky = "x'); DROP TABLE orders; --";
    let add = "name: add\nsteps:\n  - name: add\n    mysql:\n      credential: db\n      sql: INSERT INTO orders (customer, total, paid, at) VALUES (?, ?, ?, ?)\n      params: [value.customer, value.total, value.paid, timestamp('2026-11-27T09:30:05Z')]\n";
    let added = go(
        add,
        json!({ "customer": sneaky, "total": 19.9, "paid": true }),
        &effects,
    )
    .await;
    done(&added);
    let a = result(&added, "add");
    assert_eq!(a["affected"], 1);
    assert_eq!(a["insert_id"], 43);
    let executed = server.executed.lock().unwrap().clone();
    let (sql, values) = executed.last().unwrap();
    assert_eq!(
        sql, "INSERT INTO orders (customer, total, paid, at) VALUES (?, ?, ?, ?)",
        "the SQL as written"
    );
    assert_eq!(
        values,
        &[
            mysql::Val::Text(sneaky.into()),
            mysql::Val::Decimal("19.9".into()),
            mysql::Val::Tiny(1),
            mysql::Val::DateTime("2026-11-27 09:30:05.000000".into()),
        ]
    );
    // The session reads times in UTC.
    assert!(
        server
            .log()
            .contains(&"query: SET time_zone = '+00:00'".to_owned())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_only_step_runs_in_a_read_only_transaction_and_a_preview_sends_no_write() {
    let (server, effects) = shop_db().await;
    let reader = ORDER.replace(
        "      params: [value.id]\n",
        "      params: [value.id]\n      read_only: true\n",
    );
    done(&go(&reader, json!({ "id": 7 }), &effects).await);
    let log = server.log();
    let begin = log
        .iter()
        .position(|l| l == "query: START TRANSACTION READ ONLY")
        .unwrap();
    let ran = log
        .iter()
        .position(|l| l.starts_with("execute: SELECT id"))
        .unwrap();
    let back = log.iter().rposition(|l| l == "query: ROLLBACK").unwrap();
    assert!(begin < ran && ran < back, "{log:?}");

    // A preview: the read runs, read only; the INSERT is shown, never sent; a function the database refuses to run
    // in a read-only transaction is shown too.
    let both = "name: both\nsteps:\n  - name: q\n    mysql: { credential: db, sql: 'SELECT id, customer, total, paid, placed, notes, extra, ratio FROM orders WHERE id = ?', params: [value.id] }\n  - name: add\n    mysql: { credential: db, sql: 'INSERT INTO orders (customer) VALUES (?)', params: [value.customer] }\n  - name: sneaky\n    mysql: { credential: db, sql: 'SELECT add_order(?)', params: [value.customer] }\n";
    let preview = Preview::new(Arc::new(effects));
    let shown = go(both, json!({ "id": 7, "customer": "Ada" }), &preview).await;
    done(&shown);
    assert_eq!(result(&shown, "q")["rows"][0]["id"], 7, "read for real");
    let add = result(&shown, "add");
    assert_eq!(add["test"], "not written", "{add}");
    assert_eq!(add["params"], json!(["Ada"]));
    assert_eq!(result(&shown, "sneaky")["test"], "not written");
    let executed = server.executed.lock().unwrap().clone();
    assert!(
        !executed.iter().any(|(sql, _)| sql.starts_with("INSERT")),
        "{executed:?}"
    );

    // A test run sends nothing, and shows where it would go - without the password.
    let tested = go(both, json!({ "id": 7, "customer": "Ada" }), &DryRun::default()).await;
    done(&tested);
    let q = result(&tested, "q");
    assert_eq!(q["test"], "not run");
    assert_eq!(q["params"], json!([7]));
    let mut credentials = BTreeMap::new();
    credentials.insert(
        "db".to_owned(),
        credential(
            "mysql",
            &server.port(),
            "shop",
            mysql::PASSWORD,
            &[("database", "shop")],
        )
        .masked(),
    );
    let masked = DryRun {
        credentials,
        ..DryRun::default()
    };
    let tested = go(both, json!({ "id": 7, "customer": "Ada" }), &masked).await;
    let q = result(&tested, "q");
    assert_eq!(q["database"], format!("mysql://127.0.0.1:{}/shop", server.port()));
    assert!(!tested.to_json().to_string().contains(mysql::PASSWORD));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statements_in_a_transaction_are_kept_together_or_not_at_all() {
    let (server, effects) = shop_db().await;
    let pay = "name: pay\nsteps:\n  - name: pay\n    mysql:\n      credential: db\n      transaction: true\n      statements:\n        - { sql: 'UPDATE stock SET n = n - ? WHERE sku = ?', params: [value.qty, value.sku] }\n        - { sql: 'INSERT INTO orders (sku, qty) VALUES (?, ?)', params: [value.sku, value.qty] }\n";
    let record = go(pay, json!({ "qty": 2, "sku": "CAP" }), &effects).await;
    done(&record);
    let r = result(&record, "pay");
    assert_eq!(r["affected"], 2, "{r}");
    assert_eq!(r["results"].as_array().unwrap().len(), 2);
    let log = server.log();
    let begin = log.iter().position(|l| l == "query: START TRANSACTION").unwrap();
    let commit = log.iter().position(|l| l == "query: COMMIT").unwrap();
    assert_eq!(
        log[begin + 1..commit]
            .iter()
            .filter(|l| l.starts_with("execute:"))
            .count(),
        2,
        "{log:?}"
    );

    // The second fails: the first is rolled back with it, and the error says which.
    let record = go(pay, json!({ "qty": 999, "sku": "CAP" }), &effects).await;
    let e = failed(&record);
    assert!(e.contains("statement 1") && e.contains("foreign key"), "{e}");
    assert_eq!(server.log().last().unwrap(), "query: ROLLBACK");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_past_max_rows_and_a_query_past_its_time_are_refused() {
    let (_server, effects) = shop_db().await;
    let big = "name: big\nsteps:\n  - name: q\n    mysql: { credential: db, sql: 'SELECT n FROM big', max_rows: 5 }\n";
    let e = failed(&go(big, json!({}), &effects).await);
    assert!(e.contains("more than 5 rows"), "{e}");
    let fine = big.replace("max_rows: 5", "max_rows: 20");
    done(&go(&fine, json!({}), &effects).await);

    let slow = "name: slow\nsteps:\n  - name: q\n    mysql: { credential: db, sql: \"SELECT 'slow'\", timeout: 1s }\n";
    let started = Instant::now();
    let e = failed(&go(slow, json!({}), &effects).await);
    assert!(e.contains("did not finish within 1s"), "{e}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn this_machines_network_is_refused_unless_allowed_and_a_wrong_password_said_plainly() {
    let server = mysql::MySql::start(shop()).await;
    let db = credential("mysql", &server.port(), "shop", mysql::PASSWORD, &[]);
    let e = failed(&go(ORDER, json!({ "id": 1 }), &live(vec![("db", db)], false)).await);
    assert!(e.contains("--allow-private-network"), "{e}");
    assert!(server.log().is_empty(), "nothing reached the server");

    let wrong = credential("mysql", &server.port(), "shop", "not it", &[]);
    let e = failed(&go(ORDER, json!({ "id": 1 }), &live(vec![("db", wrong)], true)).await);
    assert!(e.contains("refused the username or password"), "{e}");
    assert!(!e.contains("not it"));

    // A credential of another kind is refused before anything is sent.
    let other = credential("sqlserver", &server.port(), "shop", mysql::PASSWORD, &[]);
    let e = failed(&go(ORDER, json!({ "id": 1 }), &live(vec![("db", other)], true)).await);
    assert!(e.contains("a mysql step takes a mysql one"), "{e}");
}

// --- SQL Server ---

fn books() -> Arc<tds::Answer> {
    use tds::{Cell, Kind, Reply};
    Arc::new(|r: &tds::Request| {
        if r.sql.starts_with("SELECT id, name, balance, active") {
            let tds::Val::Int(id) = r.values[0] else {
                panic!("{r:?}")
            };
            Reply::Rows(
                vec![
                    ("id", Kind::BigInt),
                    ("name", Kind::NVarChar),
                    ("balance", Kind::Decimal),
                    ("active", Kind::Bit),
                ],
                vec![
                    vec![
                        Cell::Int(id),
                        Cell::Text("Zoë".into()),
                        Cell::Cents(-1999),
                        Cell::Bit(true),
                    ],
                    vec![Cell::Int(id + 1), Cell::Null, Cell::Null, Cell::Bit(false)],
                ],
            )
        } else if r.sql.starts_with("UPDATE") {
            if r.values.contains(&tds::Val::Int(999)) {
                Reply::Error(
                    547,
                    "The UPDATE statement conflicted with the CHECK constraint \"positive\".".into(),
                )
            } else {
                Reply::Done(1)
            }
        } else {
            Reply::Error(208, format!("Invalid object name '{}'.", r.sql))
        }
    })
}

const ACCOUNT: &str = "name: account\nsteps:\n  - name: q\n    sqlserver:\n      credential: db\n      sql: SELECT id, name, balance, active FROM accounts WHERE id = @p1 AND note <> @p2\n      params: [value.id, value.note]\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlserver_rows_and_typed_values_go_through_sp_executesql() {
    let server = tds::Tds::start(books(), None).await;
    let db = credential(
        "sqlserver",
        &server.port(),
        "books",
        tds::PASSWORD,
        &[("database", "ledger")],
    );
    let effects = live(vec![("db", db)], true);
    let record = go(
        ACCOUNT,
        json!({ "id": 7, "note": "x'; DROP TABLE accounts; --" }),
        &effects,
    )
    .await;
    done(&record);
    let q = result(&record, "q");
    assert_eq!(q["count"], 2);
    assert_eq!(q["columns"], json!(["id", "name", "balance", "active"]));
    assert_eq!(
        q["rows"][0],
        json!({ "id": 7, "name": "Zoë", "balance": -19.99, "active": true })
    );
    assert_eq!(q["rows"][0]["balance"].to_string(), "-19.99");
    assert_eq!(q["rows"][1]["name"], Json::Null);
    let request = server.requests.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        request.sql,
        "SELECT id, name, balance, active FROM accounts WHERE id = @p1 AND note <> @p2"
    );
    assert_eq!(request.declared, "@p1 bigint,@p2 nvarchar(4000)");
    assert_eq!(
        request.values,
        [
            tds::Val::Int(7),
            tds::Val::Text("x'; DROP TABLE accounts; --".into())
        ]
    );
    assert!(server.log().contains(&"login: books to ledger".to_owned()));
    assert!(
        server.log().contains(&"prelogin: encryption 2".to_owned()),
        "tls: disable says so"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlserver_transactions_carry_their_descriptor_and_a_failure_rolls_back() {
    let server = tds::Tds::start(books(), None).await;
    let db = credential("sqlserver", &server.port(), "books", tds::PASSWORD, &[]);
    let effects = live(vec![("db", db)], true);
    let move_money = "name: move\nsteps:\n  - name: move\n    sqlserver:\n      credential: db\n      transaction: true\n      statements:\n        - { sql: 'UPDATE accounts SET balance = balance - @p1 WHERE id = @p2', params: [value.amount, value.from] }\n        - { sql: 'UPDATE accounts SET balance = balance + @p1 WHERE id = @p2', params: [value.amount, value.to] }\n";
    let record = go(
        move_money,
        json!({ "amount": 12.5, "from": 1, "to": 2 }),
        &effects,
    )
    .await;
    done(&record);
    assert_eq!(result(&record, "move")["affected"], 2);
    let requests = server.requests.lock().unwrap().clone();
    assert!(
        requests.iter().all(|r| r.in_transaction),
        "each request carried the transaction"
    );
    assert_eq!(requests[0].declared, "@p1 decimal(38,1),@p2 bigint");
    assert_eq!(requests[0].values[0], tds::Val::Decimal(true, 125, 1));
    let log = server.log();
    assert!(
        log.contains(&"batch: SET XACT_ABORT ON; BEGIN TRANSACTION".to_owned()),
        "{log:?}"
    );
    assert_eq!(log.last().unwrap(), "batch: COMMIT TRANSACTION");

    let e = failed(&go(move_money, json!({ "amount": 5, "from": 999, "to": 2 }), &effects).await);
    assert!(
        e.contains("statement 1") && e.contains("SQL Server error 547"),
        "{e}"
    );
    assert_eq!(
        server.log().last().unwrap(),
        "batch: IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION"
    );

    // To read only: in a transaction that is always rolled back.
    let reader = ACCOUNT.replace(
        "      params: [value.id, value.note]\n",
        "      params: [value.id, value.note]\n      read_only: true\n",
    );
    done(&go(&reader, json!({ "id": 3, "note": "" }), &effects).await);
    assert_eq!(
        server.log().last().unwrap(),
        "batch: IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlserver_over_tls_trusts_only_the_certificate_it_is_given() {
    let made = rcgen::generate_simple_self_signed(vec!["db.internal".to_owned()]).unwrap();
    let pem = made.cert.pem();
    let server = tds::Tds::start(
        books(),
        Some((made.cert.der().to_vec(), made.signing_key.serialize_der())),
    )
    .await;
    let with = |more: &[(&str, &str)]| {
        let mut db = credential("sqlserver", &server.port(), "books", tds::PASSWORD, more);
        db.fields.remove("tls");
        live(vec![("db", db)], true)
    };
    let record = go(
        ACCOUNT,
        json!({ "id": 7, "note": "" }),
        &with(&[("certificate", &pem)]),
    )
    .await;
    done(&record);
    assert_eq!(result(&record, "q")["rows"][0]["name"], "Zoë");
    let log = server.log();
    assert!(
        log.contains(&"prelogin: encryption 1".to_owned()) && log.contains(&"tls".to_owned()),
        "{log:?}"
    );

    // Another certificate, or none to trust a certificate of its own making by: refused.
    let other = rcgen::generate_simple_self_signed(vec!["db.internal".to_owned()])
        .unwrap()
        .cert
        .pem();
    let e = failed(
        &go(
            ACCOUNT,
            json!({ "id": 7, "note": "" }),
            &with(&[("certificate", &other)]),
        )
        .await,
    );
    assert!(e.contains("certificate"), "{e}");
    let e = failed(&go(ACCOUNT, json!({ "id": 7, "note": "" }), &with(&[])).await);
    assert!(!e.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlserver_says_a_wrong_password_plainly() {
    let server = tds::Tds::start(books(), None).await;
    let db = credential("sqlserver", &server.port(), "books", "not it", &[]);
    let e = failed(
        &go(
            ACCOUNT,
            json!({ "id": 7, "note": "" }),
            &live(vec![("db", db)], true),
        )
        .await,
    );
    assert!(e.contains("refused the user or password"), "{e}");
    assert!(!e.contains("not it"));
}

// --- agents, checks, and the trigger's poll ---

#[test]
fn an_agents_database_tool_that_only_reads_is_checked_when_read() {
    let agent = |sql: &str| {
        format!(
            "name: help\nsteps:\n  - name: agent\n    agent:\n      credential: brain\n      input: '{{{{ value.question }}}}'\n      tools:\n        - name: order\n          description: An order by its id\n          parameters: {{ type: object, properties: {{ id: {{ type: integer }} }}, required: [id] }}\n          mysql: {{ credential: db, sql: \"{sql}\", params: [args.id], read_only: true }}\n"
        )
    };
    assert!(Workflow::parse(&agent("SELECT id, total FROM orders WHERE id = ?")).is_ok());
    let e = Workflow::parse(&agent("DELETE FROM orders WHERE id = ?")).unwrap_err();
    assert!(e.contains("read_only"), "{e}");
    let e = Workflow::parse(&agent("SELECT 1; DROP TABLE orders WHERE id = ?")).unwrap_err();
    assert!(e.contains("read_only"), "{e}");
}

#[test]
fn old_postgres_steps_read_as_they_did() {
    let old = Workflow::parse(
        "name: p\nsteps:\n  - name: q\n    postgres: { credential: db, sql: 'SELECT * FROM t WHERE id = $1', params: [value.id] }\n",
    )
    .unwrap();
    let record = futures_lite(&old);
    let q = result(&record, "q");
    // The old step's test run, as before: the SQL and its values as text.
    assert_eq!(q["test"], "not run");
    assert_eq!(q["params"], json!(["5"]));
    let newer = Workflow::parse(
        "name: p\nsteps:\n  - name: q\n    postgres: { credential: db, sql: 'SELECT * FROM t WHERE id = $1', params: [value.id], read_only: true }\n",
    )
    .unwrap();
    let q = result(&futures_lite(&newer), "q");
    assert_eq!(q["test"], "not run");
    assert_eq!(
        q["params"],
        json!([5]),
        "the newer form's values keep their types"
    );
    let e = Workflow::parse(
        "name: p\nsteps:\n  - name: q\n    postgres: { credential: db, sql: 'SELECT $1, $2', params: [value.id], max_rows: 5 }\n",
    )
    .unwrap_err();
    assert!(e.contains("2 place(s)"), "{e}");
}

fn futures_lite(workflow: &Workflow) -> RunRecord {
    let event = Event {
        value: Value::parse_json(r#"{"id": 5}"#).unwrap(),
        details: Value::Null,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(workflow, &event, &DryRun::default()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trigger_notes_where_the_table_is_then_takes_each_new_row_once() {
    let orders: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(vec![1, 2, 3]));
    let table = Arc::clone(&orders);
    let answer: Arc<mysql::Answer> = Arc::new(move |sql: &str, values: &[mysql::Val], _: &mysql::Session| {
        use mysql::{Cell, Reply, col};
        let ids = table.lock().unwrap().clone();
        if sql == "SELECT MAX(`id`) AS openduct_last FROM `orders` WHERE 1 = 1 AND (status = 'paid')" {
            let max = ids.iter().max().map_or(Cell::Null, |m| Cell::Int(*m));
            return Reply::Rows(vec![col("openduct_last", mysql::LONGLONG)], vec![vec![max]]);
        }
        if sql == "SELECT * FROM `orders` WHERE `id` > ? AND (status = 'paid') ORDER BY `id` LIMIT 2" {
            let mysql::Val::Int(after) = values[0] else {
                panic!("{values:?}")
            };
            let rows = ids
                .iter()
                .filter(|id| **id > after)
                .take(2)
                .map(|id| vec![Cell::Int(*id), Cell::Text(format!("order {id}"))])
                .collect();
            return Reply::Rows(
                vec![col("id", mysql::LONGLONG), col("label", mysql::VAR_STRING)],
                rows,
            );
        }
        Reply::Error(1064, "42000", format!("not expected: {sql}"))
    });
    let server = mysql::MySql::start(answer).await;
    let effects = live(
        vec![(
            "db",
            credential("mysql", &server.port(), "shop", mysql::PASSWORD, &[]),
        )],
        true,
    );
    let workflow = Workflow::parse(
        "name: new orders\ntrigger:\n  mysql: { credential: db, table: orders, cursor: id, where: \"status = 'paid'\", every: 30s, max_rows: 2 }\nsteps:\n  - name: s\n    set: { id: value.id }\n",
    )
    .unwrap();
    let Trigger::Rows(t) = &workflow.trigger else {
        panic!()
    };
    let first = openduct_flow::databases::trigger::poll(&effects, t, None)
        .await
        .unwrap();
    assert!(first.runs.is_empty(), "the first poll takes nothing");
    assert_eq!(first.seen.cursor, json!(3));
    orders.lock().unwrap().extend([4, 5, 6]);
    let next = openduct_flow::databases::trigger::poll(&effects, t, Some(&first.seen))
        .await
        .unwrap();
    let ids: Vec<&str> = next.runs.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["4", "5"], "oldest first, two at most");
    let row: Json = serde_json::from_str(&next.runs[0].1.to_json()).unwrap();
    assert_eq!(row, json!({ "id": 4, "label": "order 4" }));
    let last = openduct_flow::databases::trigger::poll(&effects, t, Some(&next.seen))
        .await
        .unwrap();
    assert_eq!(last.runs.len(), 1);
    let none = openduct_flow::databases::trigger::poll(&effects, t, Some(&last.seen))
        .await
        .unwrap();
    assert!(none.runs.is_empty());
    assert_eq!(none.seen.cursor, json!(6));
    // Each poll read only.
    assert!(
        server
            .log()
            .iter()
            .filter(|l| *l == "query: START TRANSACTION READ ONLY")
            .count()
            >= 4
    );
}
