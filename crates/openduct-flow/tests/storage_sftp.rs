// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SFTP against a real SSH server: this machine's own OpenSSH `sshd`, started for the test as the user running it, on a
//! port of its own, with keys made for it by `ssh-keygen` - every action, the host key kept the first time and then
//! required, a changed key refused, each kind of host key and user key (Ed25519, ECDSA, RSA), and both ciphers and
//! both key exchanges. Where `sshd` or `ssh-keygen` is not installed, the test says so and passes by.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use openduct_expr::Value;
use openduct_flow::{Credential, Effects, Event, Live, Outcome, RunRecord, Secrets, Workflow, run};
use serde_json::{Value as Json, json};

#[derive(Debug, Default)]
struct Kept(Mutex<BTreeMap<String, Credential>>);

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
            self.0
                .lock()
                .unwrap()
                .get_mut(name)
                .ok_or("no such credential")?
                .fields
                .extend(fields);
            Ok(Value::Null)
        })
    }
}

impl Kept {
    fn put(&self, name: &str, fields: &[(&str, String)]) {
        self.0.lock().unwrap().insert(
            name.to_owned(),
            Credential {
                kind: "sftp".to_owned(),
                fields: fields.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect(),
            },
        );
    }

    fn field(&self, name: &str, field: &str) -> Option<String> {
        self.0.lock().unwrap().get(name)?.fields.get(field).cloned()
    }
}

/// An `sshd` started for the test, stopped when dropped.
struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn found(candidates: &[&str]) -> Option<PathBuf> {
    candidates.iter().map(PathBuf::from).find(|p| p.exists())
}

fn keygen(dir: &Path, name: &str, kind: &str, bits: Option<&str>) {
    let mut c = Command::new("ssh-keygen");
    c.args(["-q", "-t", kind, "-N", "", "-C", name, "-f"])
        .arg(dir.join(name));
    if let Some(b) = bits {
        c.args(["-b", b]);
    }
    assert!(c.status().unwrap().success(), "ssh-keygen {kind}");
}

fn start(sshd: &Path, dir: &Path, name: &str, extra: &str) -> Server {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = dir.join(format!("{name}.conf"));
    std::fs::write(
        &config,
        format!(
            "Port {port}\nListenAddress 127.0.0.1\nAuthorizedKeysFile {}\nPasswordAuthentication no\n\
             KbdInteractiveAuthentication no\nUsePAM no\nPidFile {}\nStrictModes no\nSubsystem sftp internal-sftp\n\
             LogLevel ERROR\n{extra}",
            dir.join("authorized_keys").display(),
            dir.join(format!("{name}.pid")).display()
        ),
    )
    .unwrap();
    let child = Command::new(sshd)
        .args(["-D", "-e", "-f"])
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < until, "sshd did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    Server { child, port }
}

fn go(workflow: &str, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap_or_else(|e| panic!("{e}"));
    let event = Event {
        value: Value::from_json(
            &json!({ "day": "2026-11-27", "rows": [{ "id": "A-1" }], "big": "0123456789abcdef".repeat(9000) }),
        ),
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
    let found = record.steps.iter().rev().find(|s| s.name == step).unwrap();
    serde_json::from_str(&found.result.to_json()).unwrap()
}

fn error(record: &RunRecord) -> String {
    match &record.outcome {
        Outcome::Failed { error, .. } => error.clone(),
        other => panic!("not failed: {other:?} {}", record.to_json()),
    }
}

fn fingerprint(public: &Path) -> String {
    let out = Command::new("ssh-keygen")
        .arg("-lf")
        .arg(public)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_owned()
}

const CYCLE: &str = "name: cycle\nsteps:\n\
    - name: report\n  convert: { format: csv, rows: value.rows, name: orders.csv }\n\
    - name: put\n  storage: { action: write, credential: box, path: in/orders.csv, file: steps.report }\n\
    - name: again\n  storage: { action: write, credential: box, path: in/orders.csv, file: steps.report }\n  on_error: continue\n\
    - name: big\n  storage: { action: write, credential: box, path: in/big.txt, file: value.big }\n  on_error: continue\n\
    - name: listing\n  storage: { action: list, credential: box, path: in, recursive: true }\n\
    - name: got\n  storage: { action: read, credential: box, path: in/orders.csv }\n\
    - name: rows\n  extract_file: { file: steps.got }\n\
    - name: copied\n  storage: { action: copy, credential: box, path: in/orders.csv, to: archive/copy.csv }\n\
    - name: moved\n  storage: { action: move, credential: box, path: in/orders.csv, to: 'done/{{ value.day }}.csv' }\n\
    - name: made\n  storage: { action: make_folder, credential: box, path: out/reports }\n\
    - name: gone\n  storage: { action: delete, credential: box, path: 'done/{{ value.day }}.csv' }\n\
    - name: gone_again\n  storage: { action: delete, credential: box, path: 'done/{{ value.day }}.csv' }\n";

const LIST: &str =
    "name: list\nsteps:\n  - name: l\n    storage: { action: list, credential: box, path: archive }\n";

#[test]
fn an_sftp_server_is_reached_its_key_kept_and_then_required() {
    let (Some(sshd), Some(_)) = (
        found(&[
            "/usr/sbin/sshd",
            "/usr/local/sbin/sshd",
            "/opt/homebrew/sbin/sshd",
        ]),
        Command::new("ssh-keygen").arg("-?").output().ok(),
    ) else {
        println!("passed by: no sshd or ssh-keygen on this machine");
        return;
    };
    let dir = std::env::temp_dir().join(format!("openduct-sftp-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("home")).unwrap();
    let dir = std::fs::canonicalize(dir).unwrap();
    for (name, kind, bits) in [
        ("host_ed25519", "ed25519", None),
        ("host_ecdsa", "ecdsa", Some("256")),
        ("host_rsa", "rsa", Some("3072")),
        ("user_ed25519", "ed25519", None),
        ("user_ecdsa", "ecdsa", Some("256")),
        ("user_rsa", "rsa", Some("3072")),
    ] {
        keygen(&dir, name, kind, bits);
    }
    let authorized: String = ["user_ed25519", "user_ecdsa", "user_rsa"]
        .iter()
        .map(|n| std::fs::read_to_string(dir.join(format!("{n}.pub"))).unwrap())
        .collect();
    std::fs::write(dir.join("authorized_keys"), authorized).unwrap();
    let hosts = ["host_ed25519", "host_ecdsa", "host_rsa"]
        .iter()
        .map(|n| format!("HostKey {}\n", dir.join(n).display()))
        .collect::<String>();
    let server = start(&sshd, &dir, "main", &hosts);
    let other = start(
        &sshd,
        &dir,
        "other",
        &format!(
            "HostKey {}\nCiphers chacha20-poly1305@openssh.com\nKexAlgorithms ecdh-sha2-nistp256\n",
            dir.join("host_ecdsa").display()
        ),
    );
    let user = std::env::var("USER").unwrap_or_else(|_| "root".to_owned());
    let key = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
    let home = dir.join("home").display().to_string();
    let kept = Arc::new(Kept::default());
    let effects = Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(Arc::clone(&kept) as Arc<dyn Secrets>);
    let credential = |port: u16, private_key: String, host_key: Option<String>| {
        let mut f = vec![
            ("host", "127.0.0.1".to_owned()),
            ("port", port.to_string()),
            ("username", user.clone()),
            ("private_key", private_key),
            ("folder", home.clone()),
        ];
        if let Some(h) = host_key {
            f.push(("host_key", h));
        }
        kept.put("box", &f);
    };

    // The first time: every action, and the server's key kept.
    credential(server.port, key("user_ed25519"), None);
    let record = go(CYCLE, &effects);
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
    assert!(
        result(&record, "again")["error"]
            .as_str()
            .unwrap()
            .contains("overwrite")
    );
    assert_eq!(result(&record, "big")["written"], true, "a write of many packets");
    let listed: Vec<String> = result(&record, "listing")["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(listed, ["in/big.txt", "in/orders.csv"], "{}", record.to_json());
    assert_eq!(result(&record, "rows")["rows"], json!([{ "id": "A-1" }]));
    assert_eq!(result(&record, "moved")["to"], "done/2026-11-27.csv");
    assert_eq!(result(&record, "made")["made"], true);
    assert_eq!(result(&record, "gone")["deleted"], true);
    assert_eq!(result(&record, "gone_again")["deleted"], false);
    assert_eq!(
        std::fs::read_to_string(dir.join("home/archive/copy.csv")).unwrap(),
        "id\r\nA-1\r\n"
    );
    assert_eq!(
        std::fs::read(dir.join("home/in/big.txt")).unwrap().len(),
        16 * 9000
    );
    let recorded = kept
        .field("box", "host_key")
        .expect("the host key is kept the first time");
    assert_eq!(
        recorded,
        format!("ssh-ed25519 {}", fingerprint(&dir.join("host_ed25519.pub")))
    );

    // From then on it is required: the same key passes, another is refused.
    let record = go(LIST, &effects);
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());
    credential(
        server.port,
        key("user_ed25519"),
        Some(format!("ssh-ed25519 {}", fingerprint(&dir.join("host_rsa.pub")))),
    );
    assert!(error(&go(LIST, &effects)).contains("not the one the credential knows"));

    // Each kind of host key, pinned as ssh-keyscan prints it; each kind of user key.
    for host in ["host_ecdsa", "host_rsa"] {
        credential(
            server.port,
            key("user_ed25519"),
            Some(key(&format!("{host}.pub"))),
        );
        let record = go(LIST, &effects);
        assert_eq!(record.outcome, Outcome::Done, "{host}: {}", record.to_json());
    }
    for user_key in ["user_ecdsa", "user_rsa"] {
        credential(server.port, key(user_key), Some(key("host_ed25519.pub")));
        let record = go(LIST, &effects);
        assert_eq!(record.outcome, Outcome::Done, "{user_key}: {}", record.to_json());
        assert_eq!(result(&record, "l")["count"], 1);
    }

    // ChaCha20-Poly1305 and an exchange on P-256.
    credential(other.port, key("user_rsa"), Some(key("host_ecdsa.pub")));
    let record = go(LIST, &effects);
    assert_eq!(record.outcome, Outcome::Done, "{}", record.to_json());

    // A password the server does not take, and a key it does not know.
    kept.put(
        "box",
        &[
            ("host", "127.0.0.1".to_owned()),
            ("port", server.port.to_string()),
            ("username", user.clone()),
            ("password", "not it".to_owned()),
            ("host_key", key("host_ed25519.pub")),
        ],
    );
    assert!(error(&go(LIST, &effects)).contains("refused the sign-in"));
    drop((server, other));
    let _ = std::fs::remove_dir_all(dir);
}
