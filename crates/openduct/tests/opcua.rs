// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The opcua connector as a person runs it, against a real OPC UA server started for each test (a press with four
//! values): values as they change, a deadband, a signed and encrypted session that needs the server's certificate
//! trusted first, sign-in refused, a node the server does not have, and a server that declares a message too large
//! to take.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use opcua::crypto::SecurityPolicy;
use opcua::server::address_space::Variable;
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{SimpleNodeManager, simple_node_manager};
use opcua::server::{ANONYMOUS_USER_TOKEN_ID, ServerBuilder, ServerHandle, ServerUserToken};
use opcua::types::{DataValue, MessageSecurityMode, NodeId, UAString, Variant};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");
const PASSWORD: &str = "press-7-pass";

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
            while let Ok(line) = self.log.try_recv() {
                self.seen.push(line);
            }
            assert!(
                Instant::now() < deadline,
                "it did not stop: {}",
                self.seen.join("\n")
            );
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

#[allow(dead_code)]
fn change(message: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(message["value"].as_str().unwrap()).unwrap()
}

/// A press's OPC UA server, with its values under `ns=<ns>;s=Press7.*`.
struct Press {
    handle: ServerHandle,
    manager: Arc<SimpleNodeManager>,
    ns: u16,
    port: u16,
    _pki: tempfile::TempDir,
}

impl Press {
    async fn start() -> Self {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let pki = tempfile::tempdir().unwrap();
        let users: &[&str] = &[ANONYMOUS_USER_TOKEN_ID, "operator"];
        let (server, handle) = ServerBuilder::new()
            .application_name("Test press")
            .application_uri("urn:test-press")
            .product_uri("urn:test-press")
            .create_sample_keypair(true)
            .certificate_path("own/cert.der")
            .private_key_path("private/private.pem")
            .pki_dir(pki.path())
            .host("127.0.0.1")
            .port(port)
            .discovery_urls(vec!["/".to_owned()])
            .add_user_token(
                "operator",
                ServerUserToken {
                    user: "operator".to_owned(),
                    pass: Some(PASSWORD.to_owned()),
                    ..Default::default()
                },
            )
            .add_endpoint(
                "none",
                ("/", SecurityPolicy::None, MessageSecurityMode::None, users),
            )
            .add_endpoint(
                "secure",
                (
                    "/",
                    SecurityPolicy::Basic256Sha256,
                    MessageSecurityMode::SignAndEncrypt,
                    users,
                ),
            )
            .trust_client_certs(true)
            .with_node_manager(simple_node_manager(
                NamespaceMetadata {
                    namespace_uri: "urn:press".to_owned(),
                    ..Default::default()
                },
                "press",
            ))
            .build()
            .unwrap();
        let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().unwrap();
        let ns = handle.get_namespace_index("urn:press").unwrap();
        {
            let space = manager.address_space();
            let mut space = space.write();
            let folder = NodeId::new(ns, "Press7");
            space.add_folder(&folder, "Press7", "Press7", &NodeId::objects_folder_id());
            let _ = space.add_variables(
                vec![
                    Variable::new(
                        &NodeId::new(ns, "Press7.Temperature"),
                        "Temperature",
                        "Temperature",
                        20.0f64,
                    ),
                    Variable::new(&NodeId::new(ns, "Press7.Running"), "Running", "Running", true),
                    Variable::new(&NodeId::new(ns, "Press7.Parts"), "Parts", "Parts", 0u32),
                    Variable::new(
                        &NodeId::new(ns, "Press7.Order"),
                        "Order",
                        "Order",
                        UAString::from("A-100"),
                    ),
                ],
                &folder,
            );
        }
        tokio::spawn(async move {
            let _ = server.run().await;
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "the OPC UA server did not start");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Self {
            handle,
            manager,
            ns,
            port,
            _pki: pki,
        }
    }

    fn set(&self, name: &str, value: impl Into<Variant>) {
        let node = NodeId::new(self.ns, name);
        self.manager
            .set_values(
                self.handle.subscriptions(),
                [(&node, None, DataValue::new_now(value.into()))].into_iter(),
            )
            .unwrap();
    }

    fn read(&self) -> String {
        let ns = self.ns;
        format!(
            "    read:\n      temperature: ns={ns};s=Press7.Temperature\n      running: ns={ns};s=Press7.Running\n      parts: ns={ns};s=Press7.Parts\n      order: ns={ns};s=Press7.Order\n"
        )
    }
}

impl Drop for Press {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}

fn file(dir: &std::path::Path, name: &str, text: &str) -> std::path::PathBuf {
    let f = dir.join(format!("{name}.yaml"));
    std::fs::write(&f, text).unwrap();
    f
}

#[tokio::test(flavor = "multi_thread")]
async fn values_arrive_as_they_change() {
    let press = Arc::new(Press::start().await);
    let (local, here) = broker().await;
    let (h, p) = (here.clone(), Arc::clone(&press));
    tokio::task::spawn_blocking(move || {
        for t in ["press", "temperature"] {
            openduct(&h, &["topic", "create", t]);
        }
        let dir = tempfile::tempdir().unwrap();
        let pki = dir.path().join("pki");
        let base = format!("    url: opc.tcp://127.0.0.1:{}\n    device: press-07\n    security: none\n    every: 100ms\n    pki: {}\n", p.port, pki.display());
        let all = file(dir.path(), "all", &format!("name: press-in\nfrom:\n  opcua:\n{base}{}to:\n  topic: press\n", p.read()));
        let ns = p.ns;
        let some = file(dir.path(), "some", &format!("name: temperature-in\nfrom:\n  opcua:\n{base}    deadband: 1\n    read:\n      temperature: ns={ns};s=Press7.Temperature\n      running: ns={ns};s=Press7.Running\nto:\n  topic: temperature\n"));
        let (a, b) = (Running::start(&h, &all, &[]), Running::start(&h, &some, &[]));
        let first = wait_for(&h, "press", 4);
        assert_eq!(first.len(), 4, "every value is read once at the start");
        wait_for(&h, "temperature", 2);
        p.set("Press7.Temperature", 20.3f64);
        std::thread::sleep(Duration::from_millis(600));
        p.set("Press7.Temperature", 22.0f64);
        p.set("Press7.Parts", 1u32);
        let got = wait_for(&h, "press", 7);
        let deadbanded = wait_for(&h, "temperature", 3);
        std::thread::sleep(Duration::from_millis(800));
        let (log_a, log_b) = (a.stop(), b.stop());
        assert_eq!(got.len(), 7, "{log_a}");
        let changes: Vec<serde_json::Value> = got.iter().map(change).collect();
        let order = changes.iter().find(|c| c["name"] == "order").unwrap();
        assert_eq!(order["reading"], "A-100");
        assert_eq!(order["node"], format!("ns={ns};s=Press7.Order"));
        assert_eq!(order["status"], "Good");
        assert!(order["source_time"].as_str().unwrap().ends_with('Z'), "{order}");
        assert_eq!(got[0]["key"], "press-07");
        let temps: Vec<f64> = changes.iter().filter(|c| c["name"] == "temperature").map(|c| c["reading"].as_f64().unwrap()).collect();
        assert_eq!(temps, [20.0, 20.3, 22.0], "{log_a}");
        let parts: Vec<u64> = changes.iter().filter(|c| c["name"] == "parts").map(|c| c["reading"].as_u64().unwrap()).collect();
        assert_eq!(parts, [0, 1]);
        // A change of less than the deadband (20.0 to 20.3) is not reported.
        // The deadband is for numbers: a yes/no value beside them is read as well.
        let temps: Vec<f64> = deadbanded.iter().map(change).filter(|c| c["name"] == "temperature").map(|c| c["reading"].as_f64().unwrap()).collect();
        assert_eq!(temps, [20.0, 22.0], "{log_b}");
        assert!(deadbanded.iter().map(change).any(|c| c["name"] == "running" && c["reading"] == true), "{log_b}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_encrypted_session_needs_the_server_trusted_and_the_right_password() {
    let press = Arc::new(Press::start().await);
    let (local, here) = broker().await;
    let (h, p) = (here.clone(), Arc::clone(&press));
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "secure"]);
        let dir = tempfile::tempdir().unwrap();
        let pki = dir.path().join("pki");
        let f = file(dir.path(), "secure", &format!("name: secure-in\nfrom:\n  opcua:\n    url: opc.tcp://127.0.0.1:{}\n    username: operator\n    password: secret:PRESS_PASSWORD\n    pki: {}\n{}to:\n  topic: secure\n", p.port, pki.display(), p.read()));

        // The server's certificate is not trusted yet: it is put aside, and the pipeline says where to move it.
        let (ok, log) = Running::start(&h, &f, &[("PRESS_PASSWORD", PASSWORD)]).ended();
        assert!(!ok && log.contains("certificate is not trusted"), "{log}");
        let rejected: Vec<_> = std::fs::read_dir(pki.join("rejected")).unwrap().map(|e| e.unwrap().path()).collect();
        assert_eq!(rejected.len(), 1, "{rejected:?}");
        std::fs::create_dir_all(pki.join("trusted")).unwrap();
        std::fs::rename(&rejected[0], pki.join("trusted").join(rejected[0].file_name().unwrap())).unwrap();

        let wrong = "not-the-password-2291";
        let (ok, log) = Running::start(&h, &f, &[("PRESS_PASSWORD", wrong)]).ended();
        assert!(!ok && log.contains("refused"), "{log}");
        assert!(!log.contains(wrong), "the password is never shown: {log}");

        let running = Running::start(&h, &f, &[("PRESS_PASSWORD", PASSWORD)]);
        let got = wait_for(&h, "secure", 4);
        let log = running.stop();
        assert_eq!(got.len(), 4, "{log}");
        assert!(log.contains("sign_and_encrypt"), "{log}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_the_server_does_not_have_stops_with_the_reason() {
    let press = Arc::new(Press::start().await);
    let (local, here) = broker().await;
    let (h, p) = (here.clone(), Arc::clone(&press));
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "t"]);
        let dir = tempfile::tempdir().unwrap();
        let f = file(dir.path(), "missing", &format!("name: missing-in\nfrom:\n  opcua:\n    url: opc.tcp://127.0.0.1:{}\n    security: none\n    pki: {}\n    read:\n      speed: ns={};s=Press7.Speed\nto:\n  topic: t\n", p.port, dir.path().join("pki").display(), p.ns));
        let (ok, log) = Running::start(&h, &f, &[]).ended();
        assert!(!ok && log.contains(&format!("refused watching speed (ns={};s=Press7.Speed): BadNodeIdUnknown", p.ns)), "{log}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

/// A server that answers the first message with a header declaring nearly 2 GB, then sends for as long as the
/// connection is open: how many bytes it got to send.
fn oversized_server() -> (u16, std::thread::JoinHandle<usize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut hello = [0u8; 8];
        stream.read_exact(&mut hello).unwrap();
        let mut header = b"ACKF".to_vec();
        header.extend_from_slice(&0x7fff_fff0u32.to_le_bytes());
        stream.write_all(&header).unwrap();
        let mut sent = header.len();
        let block = vec![0u8; 64 * 1024];
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(30) {
            match stream.write(&block) {
                Ok(0) | Err(_) => break,
                Ok(n) => sent += n,
            }
        }
        sent
    });
    (port, server)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_declaring_a_huge_message_is_refused_at_once() {
    let (local, here) = broker().await;
    let h = here.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&h, &["topic", "create", "t"]);
        let (port, server) = oversized_server();
        let dir = tempfile::tempdir().unwrap();
        let f = file(dir.path(), "huge", &format!("name: huge-in\nfrom:\n  opcua:\n    url: opc.tcp://127.0.0.1:{port}\n    security: none\n    pki: {}\n    read:\n      t: ns=2;s=T\nto:\n  topic: t\n", dir.path().join("pki").display()));
        let started = Instant::now();
        let (ok, log) = Running::start(&h, &f, &[]).ended();
        assert!(!ok, "{log}");
        assert!(started.elapsed() < Duration::from_secs(15), "it waited: {log}");
        let sent = server.join().unwrap();
        assert!(sent < 8 * 1024 * 1024, "the client read {sent} bytes before refusing: {log}");
    })
    .await
    .unwrap();
    local.shutdown().await;
}

#[test]
fn settings_that_cannot_work_are_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    let check = |name: &str, body: &str| {
        let f = file(
            dir.path(),
            name,
            &format!("name: {name}\nfrom:\n  opcua:\n{body}to:\n  topic: t\n"),
        );
        let out = Command::new(OPENDUCT)
            .args(["pipeline", "check", f.to_str().unwrap()])
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
    let read = "    read:\n      temperature: ns=2;s=Press7.Temperature\n      running: ns=2;i=1001\n";
    let (ok, text) = check("press", &format!("    url: opc.tcp://10.0.4.21\n{read}"));
    assert!(
        ok && text.contains(
            "reads opcua 2 values on opc.tcp://10.0.4.21:4840 every 1s (sign_and_encrypt, Basic256Sha256)"
        ),
        "{text}"
    );
    let (ok, text) = check(
        "plain",
        &format!("    url: opc.tcp://10.0.4.21:4841/plc\n    security: none\n{read}"),
    );
    assert!(
        ok && text.contains("on opc.tcp://10.0.4.21:4841/plc every 1s (no security)"),
        "{text}"
    );
    for (name, body, why) in [
        (
            "http",
            format!("    url: http://10.0.4.21\n{read}"),
            "is the server's address",
        ),
        (
            "noread",
            "    url: opc.tcp://10.0.4.21\n".to_owned(),
            "read: is required",
        ),
        (
            "node",
            "    url: opc.tcp://10.0.4.21\n    read:\n      t: Press7.Temperature\n".to_owned(),
            "is not a node id",
        ),
        (
            "twice",
            "    url: opc.tcp://10.0.4.21\n    read:\n      a: ns=2;i=5\n      b: ns=2;i=5\n".to_owned(),
            "is read twice",
        ),
        (
            "fast",
            format!("    url: opc.tcp://10.0.4.21\n    every: 10ms\n{read}"),
            "at least 100ms",
        ),
        (
            "policy",
            format!("    url: opc.tcp://10.0.4.21\n    security: none\n    policy: Basic256Sha256\n{read}"),
            "is for security sign",
        ),
        (
            "weak",
            format!("    url: opc.tcp://10.0.4.21\n    policy: Basic128Rsa15\n{read}"),
            "is one of Basic256Sha256",
        ),
        (
            "half",
            format!("    url: opc.tcp://10.0.4.21\n    username: operator\n{read}"),
            "give username and password together",
        ),
    ] {
        let (ok, text) = check(name, &body);
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
