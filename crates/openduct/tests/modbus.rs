// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The Modbus connector as a person runs it, against a Modbus TCP device made with an independent implementation
//! (tokio-modbus's server), and against stand-ins that answer what no device should.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;
use tokio_modbus::server::tcp::{Server, accept_tcp_connection};
use tokio_modbus::{ExceptionCode, Request, Response};

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

struct Running {
    child: Child,
    log: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Running {
    fn start(addr: &str, file: &std::path::Path) -> Self {
        let mut child = Command::new(OPENDUCT)
            .args(["pipeline", "run", file.to_str().unwrap(), "--bootstrap", addr])
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
        assert!(self.child.wait().unwrap().success(), "{:?}", self.seen);
        while let Ok(line) = self.log.recv_timeout(Duration::from_secs(2)) {
            self.seen.push(line);
        }
        self.seen.join("\n")
    }

    /// Wait for it to stop by itself; whether it succeeded, and its log.
    fn ended(mut self) -> (bool, String) {
        let deadline = Instant::now() + Duration::from_secs(30);
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

fn wait_for_values(addr: &str, topic: &str, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let lines: Vec<String> = openduct(addr, &["read", topic, "--format", "json"])
            .lines()
            .map(str::to_owned)
            .collect();
        if lines.len() >= want || Instant::now() > deadline {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A press controller: holding and input registers and coils at fixed addresses; any other address is refused.
#[derive(Clone, Default)]
struct Press {
    holding: Arc<Mutex<HashMap<u16, u16>>>,
    input: Arc<Mutex<HashMap<u16, u16>>>,
    coils: Arc<Mutex<HashMap<u16, bool>>>,
    requests: Arc<Mutex<usize>>,
}

fn words(map: &Mutex<HashMap<u16, u16>>, at: u16, n: u16) -> Result<Vec<u16>, ExceptionCode> {
    let map = map.lock().unwrap();
    (at..at + n)
        .map(|a| map.get(&a).copied().ok_or(ExceptionCode::IllegalDataAddress))
        .collect()
}

impl tokio_modbus::server::Service for Press {
    type Request = Request<'static>;
    type Response = Response;
    type Exception = ExceptionCode;
    type Future = std::future::Ready<Result<Response, ExceptionCode>>;

    fn call(&self, req: Request<'static>) -> Self::Future {
        *self.requests.lock().unwrap() += 1;
        std::future::ready(match req {
            Request::ReadHoldingRegisters(at, n) => {
                words(&self.holding, at, n).map(Response::ReadHoldingRegisters)
            }
            Request::ReadInputRegisters(at, n) => words(&self.input, at, n).map(Response::ReadInputRegisters),
            Request::ReadCoils(at, n) => {
                let coils = self.coils.lock().unwrap();
                (at..at + n)
                    .map(|a| coils.get(&a).copied().ok_or(ExceptionCode::IllegalDataAddress))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Response::ReadCoils)
            }
            _ => Err(ExceptionCode::IllegalFunction),
        })
    }
}

async fn device(press: Press) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Server::new(listener);
    tokio::spawn(async move {
        let on_connected = move |stream, addr| {
            let press = press.clone();
            async move { accept_tcp_connection(stream, addr, move |_| Ok(Some(press.clone()))) }
        };
        let _ = server.serve(&on_connected, |e| eprintln!("device: {e}")).await;
    });
    port
}

#[tokio::test(flavor = "multi_thread")]
async fn a_press_is_read_on_a_schedule_and_only_changes_are_written() {
    let (broker, addr) = broker().await;
    let press = Press::default();
    {
        let mut h = press.holding.lock().unwrap();
        h.insert(100, 215); // 21.5 degrees, in tenths
        h.insert(101, 0x4088); // 4.25 bar as a 32-bit float, high word first
        h.insert(102, 0x0000);
        h.insert(103, 0b1000); // status word: bit 3, door open
        press.input.lock().unwrap().extend([(20, 0x0001), (21, 0xe240)]); // 123456 parts
        press.coils.lock().unwrap().insert(3, true);
    }
    let port = device(press.clone()).await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "press"]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("press.yaml");
        std::fs::write(
            &file,
            format!(
                "name: press-in\nfrom:\n  modbus:\n    url: modbus://127.0.0.1:{port}\n    device: press-07\n    every: 200ms\n    changes_only: true\n    read:\n      temperature: {{ address: 100, type: i16, scale: 0.1 }}\n      pressure: {{ address: 101, type: f32 }}\n      door_open: {{ address: 103, bit: 3 }}\n      parts: {{ table: input, address: 20, type: u32 }}\n      running: {{ table: coil, address: 3 }}\nto:\n  topic: press\n"
            ),
        )
        .unwrap();
        let running = Running::start(&a, &file);
        let first = wait_for_values(&a, "press", 1);
        // Several more reads, all the same: nothing more is written.
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(wait_for_values(&a, "press", 1).len(), 1);
        press.holding.lock().unwrap().insert(100, 220);
        let after = wait_for_values(&a, "press", 2);
        let log = running.stop();
        assert!(log.contains("5 values in 3 reads every 200ms"), "{log}");
        assert_eq!(first.len(), 1, "{log}");
        assert!(
            first[0].contains(r#"{\"temperature\":21.5,\"pressure\":4.25,\"door_open\":true,\"parts\":123456,\"running\":true}"#),
            "{}",
            first[0]
        );
        assert!(first[0].contains(r#""key":"press-07""#), "{}", first[0]);
        assert_eq!(after.len(), 2, "{after:?}");
        assert!(after[1].contains(r#"\"temperature\":22,"#), "{}", after[1]);
        assert!(*press.requests.lock().unwrap() >= 3 * 6, "it kept reading");
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_address_the_device_does_not_have_stops_the_pipeline_with_the_reason() {
    let (broker, addr) = broker().await;
    let press = Press::default();
    press.holding.lock().unwrap().insert(100, 1);
    let port = device(press).await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "press"]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("press.yaml");
        std::fs::write(
            &file,
            format!("name: press-in\nfrom:\n  modbus:\n    url: modbus://127.0.0.1:{port}\n    read:\n      speed: {{ address: 900 }}\nto:\n  topic: press\n"),
        )
        .unwrap();
        let (ok, log) = Running::start(&a, &file).ended();
        assert!(!ok);
        assert!(
            log.contains("the device refused reading holding register 900: it has no such address (illegal data address)"),
            "{log}"
        );
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_no_device_should_send_are_refused() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "press"]);
        let dir = tempfile::tempdir().unwrap();
        // A length of 65535 bytes, and a protocol that is not Modbus: each answered to the first request.
        for (answer, reason) in [
            (vec![0, 1, 0, 0, 0xff, 0xff, 1], "claims 65535 bytes"),
            (vec![0, 1, 0, 7, 0, 5, 1, 3, 2, 0, 1], "is not Modbus TCP"),
            (vec![0, 1, 0, 0, 0, 5, 1, 3, 4, 0, 1], "with 4 bytes, where 2 were asked for"),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let mut request = [0u8; 12];
                    if stream.read_exact(&mut request).is_ok() {
                        let _ = stream.write_all(&answer);
                    }
                }
            });
            let file = dir.path().join("hostile.yaml");
            std::fs::write(
                &file,
                format!("name: hostile\nfrom:\n  modbus:\n    url: modbus://127.0.0.1:{port}\n    read:\n      speed: {{ address: 0 }}\nto:\n  topic: press\n"),
            )
            .unwrap();
            let (ok, log) = Running::start(&a, &file).ended();
            assert!(!ok);
            assert!(log.contains(reason), "{reason}: {log}");
        }
        assert_eq!(openduct(&a, &["read", "press"]), "", "nothing was written");

        // Settings that cannot work are refused before anything starts.
        let bad = dir.path().join("bad.yaml");
        for (read, reason) in [
            ("{ table: coil, address: 1, type: f32 }", "a coil is a bool"),
            ("{ address: 65535, type: u32 }", "runs past address 65535"),
            ("{ address: 1, type: bool }", "names its bit"),
            ("{ adress: 1 }", "\"adress\" is not a setting"),
        ] {
            std::fs::write(&bad, format!("name: bad\nfrom:\n  modbus:\n    url: modbus://127.0.0.1:502\n    read:\n      x: {read}\nto:\n  topic: press\n")).unwrap();
            let out = Command::new(OPENDUCT).args(["pipeline", "check", bad.to_str().unwrap()]).output().unwrap();
            assert!(!out.status.success());
            assert!(String::from_utf8_lossy(&out.stderr).contains(reason), "{reason}: {out:?}");
        }
    })
    .await
    .unwrap();
    broker.shutdown().await;
}
