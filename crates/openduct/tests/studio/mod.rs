// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What the studio's tests share: `openduct start` on ports of its own, a request over HTTP/1.1, an owner signed in.

#![allow(dead_code, unreachable_pub)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

pub struct Running {
    pub child: Child,
    pub studio: String,
    pub broker: String,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `openduct start` on ports of its own choosing, once it says where it is.
/// The log of the program last started on this thread, shown when a request to it fails.
fn log_of_last_start() -> String {
    LOG.with(|l| {
        l.borrow()
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .unwrap_or_default()
    })
}

thread_local! {
    static LOG: std::cell::RefCell<Option<std::path::PathBuf>> = const { std::cell::RefCell::new(None) };
}

pub fn start(data: &std::path::Path) -> Running {
    start_with(
        data,
        &["--listen", "127.0.0.1:0", "--studio-listen", "127.0.0.1:0"],
        &[],
    )
}

/// `openduct start` with `args` (beside --data-dir) and `env`, once it says where it is.
pub fn start_with(data: &std::path::Path, args: &[&str], env: &[(&str, &str)]) -> Running {
    LOG.with(|l| *l.borrow_mut() = Some(data.join("openduct.log")));
    let mut child = Command::new(OPENDUCT)
        .arg("start")
        .args(args)
        .arg("--data-dir")
        .arg(data)
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        // Its log, for when a test fails: a file beside its data (the store reads only its own folders).
        .stderr(std::fs::File::create(data.join("openduct.log")).unwrap())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let (mut studio, mut broker) = (String::new(), String::new());
    while studio.is_empty() || broker.is_empty() {
        let line = lines.next().expect("openduct start said nothing").unwrap();
        if let Some(rest) = line.trim().strip_prefix("Studio:") {
            studio = rest
                .trim()
                .trim_start_matches("http://")
                .split(' ')
                .next()
                .unwrap()
                .to_owned();
        }
        if let Some(rest) = line.trim().strip_prefix("Broker:") {
            broker = rest.trim().split(' ').next().unwrap().to_owned();
        }
    }
    // What it prints after is read and let go, so its output is never a closed pipe.
    std::thread::spawn(move || lines.for_each(drop));
    Running {
        child,
        studio,
        broker,
    }
}

/// Stops it as a service manager does (SIGTERM), and waits.
#[cfg(unix)]
pub fn stop(mut running: Running) {
    let pid = running.child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while running.child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "it did not stop");
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub struct Answer {
    pub status: u16,
    pub headers: String,
    pub body: serde_json::Value,
}

impl Answer {
    pub fn cookie(&self) -> String {
        self.headers
            .lines()
            .find_map(|l| {
                l.get(..12)
                    .filter(|h| h.eq_ignore_ascii_case("set-cookie: "))
                    .and(l.get(12..))
            })
            .and_then(|c| c.split(';').next())
            .unwrap()
            .to_owned()
    }
}

/// One HTTP/1.1 request; `extra` are more header lines.
pub fn http(addr: &str, method: &str, path: &str, extra: &[&str], body: Option<&str>) -> Answer {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut request = format!("{method} {path} HTTP/1.1\r\nconnection: close\r\n");
    if !extra.iter().any(|h| h.to_ascii_lowercase().starts_with("host:")) {
        request.push_str(&format!("host: {addr}\r\n"));
    }
    for header in extra {
        request.push_str(header);
        request.push_str("\r\n");
    }
    let body = body.unwrap_or_default();
    request.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    if let Err(e) = stream.read_to_end(&mut raw) {
        panic!(
            "{method} {path}: {e}\n--- the program's log ---\n{}",
            log_of_last_start()
        );
    }
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Answer {
        status,
        headers: head.to_owned(),
        body: serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
    }
}

pub const JSON: &str = "content-type: application/json";
pub const OWNER: &str =
    r#"{"name": "Ada Lovelace", "email": "Ada@Example.com", "password": "correct horse battery"}"#;

/// The owner's account made, and the headers a signed-in change carries: the cookie, its token, JSON.
pub fn owner(studio: &str) -> Vec<String> {
    let made = http(studio, "POST", "/api/setup", &[JSON], Some(OWNER));
    assert_eq!(made.status, 201, "{}", made.body);
    vec![
        format!("cookie: {}", made.cookie()),
        format!("x-openduct-csrf: {}", made.body["csrf"].as_str().unwrap()),
        JSON.to_owned(),
    ]
}

/// A request with the signed-in headers.
pub fn call(studio: &str, signed: &[String], method: &str, path: &str, body: Option<&str>) -> Answer {
    let headers: Vec<&str> = signed.iter().map(String::as_str).collect();
    http(studio, method, path, &headers, body)
}

/// Waits for `ready` to say yes, for up to `secs` seconds.
pub fn until(secs: u64, what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ready() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The broker's name of the signed-in person's first workspace's topic `name`: a workspace's topics are its own,
/// `w.<workspace>.<name>`.
#[allow(dead_code)]
pub fn workspace_topic(studio: &str, signed: &[String], name: &str) -> String {
    let session = call(studio, signed, "GET", "/api/session", None);
    let id = session.body["workspaces"][0]["id"].as_str().unwrap().to_owned();
    format!("w.{id}.{name}")
}
