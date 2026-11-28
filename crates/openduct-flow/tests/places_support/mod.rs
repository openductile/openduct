// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Stand-ins for the storage places reached over a vendor's API - Azure Blob Storage, Google Cloud Storage (and
//! Google's token address), Dropbox, Box, Microsoft Graph's drives, Google Drive - each answering the requests the
//! steps make as its vendor's documentation says it does, in one server on this machine. A run's effects are a real
//! run's ([`openduct_flow::Live`], its guard against this machine's network allowed here), with each vendor's address
//! sent to the stand-in instead: `https://api.box.com/2.0/...` goes to `http://127.0.0.1:<port>/api.box.com/2.0/...`.
//! No real account is reached.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::type_complexity,
    clippy::too_many_lines
)]

pub(crate) mod azure;
pub(crate) mod boxes;
pub(crate) mod dropbox;
pub(crate) mod gcs;
pub(crate) mod gdrive;
pub(crate) mod graph;
pub(crate) mod http;
pub(crate) mod tree;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::{Credential, Effects, HttpRequest, Live, Secrets};

/// The hosts whose requests go to the stand-ins.
pub(crate) const HOSTS: &[&str] = &[
    "api.dropboxapi.com",
    "content.dropboxapi.com",
    "api.box.com",
    "upload.box.com",
    "graph.microsoft.com",
    "login.microsoftonline.com",
    "www.googleapis.com",
    "oauth2.googleapis.com",
    "storage.googleapis.com",
];

/// Every stand-in, and the server they answer from.
pub(crate) struct Stand {
    pub port: u16,
    pub azure: Mutex<azure::Azure>,
    pub gcs: Mutex<gcs::Gcs>,
    pub dropbox: Mutex<dropbox::Dropbox>,
    pub boxes: Mutex<boxes::Boxes>,
    pub graph: Mutex<graph::Graph>,
    pub drive: Mutex<gdrive::GDrive>,
    /// Every request: method, host and path.
    pub log: Arc<Mutex<Vec<String>>>,
}

impl Stand {
    pub(crate) fn start() -> Arc<Self> {
        let log = Arc::new(Mutex::new(Vec::new()));
        let held: Arc<Mutex<Option<Arc<Self>>>> = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&held);
        let handler: http::Handler = Arc::new(move |r: &http::Request| {
            let stand = seen.lock().unwrap().clone().unwrap();
            stand.answer(r)
        });
        let port = http::serve(handler, Arc::clone(&log));
        let stand = Arc::new(Self {
            port,
            azure: Mutex::new(azure::Azure::default()),
            gcs: Mutex::new(gcs::Gcs::default()),
            dropbox: Mutex::new(dropbox::Dropbox::new()),
            boxes: Mutex::new(boxes::Boxes::new(port)),
            graph: Mutex::new(graph::Graph::new(port)),
            drive: Mutex::new(gdrive::GDrive::new()),
            log,
        });
        *held.lock().unwrap() = Some(Arc::clone(&stand));
        stand
    }

    fn answer(&self, r: &http::Request) -> http::Reply {
        match r.host.as_str() {
            "azure" => self.azure.lock().unwrap().handle(r),
            "oauth2.googleapis.com" => self.gcs.lock().unwrap().token(r),
            "storage.googleapis.com" => self.gcs.lock().unwrap().handle(r),
            "api.dropboxapi.com" | "content.dropboxapi.com" => self.dropbox.lock().unwrap().handle(r),
            "api.box.com" | "upload.box.com" | "dl.boxcloud.com" => self.boxes.lock().unwrap().handle(r),
            "graph.microsoft.com" | "files.1drv.com" => self.graph.lock().unwrap().handle(r),
            "www.googleapis.com" => self.drive.lock().unwrap().handle(r),
            _ => http::Reply::empty(404),
        }
    }

    /// The Azure endpoint a credential names, on this machine.
    pub(crate) fn azure_endpoint(&self) -> String {
        format!("http://127.0.0.1:{}/azure/{}", self.port, azure::ACCOUNT)
    }

    /// A vendor's address, on this machine.
    pub(crate) fn local(&self, url: &str) -> String {
        for host in HOSTS {
            if let Some(rest) = url.strip_prefix(&format!("https://{host}")) {
                return format!("http://127.0.0.1:{}/{host}{rest}", self.port);
            }
        }
        url.to_owned()
    }

    /// How many requests went to `host`.
    pub(crate) fn asked(&self, host: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.split(' ').nth(1).is_some_and(|p| p.starts_with(host)))
            .count()
    }
}

/// Credentials by name, and the install's registrations; fields a run keeps (a refreshed token) are kept here.
#[derive(Debug, Default)]
pub(crate) struct Kept {
    pub all: Mutex<BTreeMap<String, Credential>>,
    pub registrations: BTreeMap<String, Credential>,
}

pub(crate) fn credential(kind: &str, fields: &[(&str, &str)]) -> Credential {
    Credential {
        kind: kind.to_owned(),
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
    }
}

impl Kept {
    pub(crate) fn with(list: Vec<(&str, Credential)>) -> Arc<Self> {
        let registration = credential(
            "oauth_registration",
            &[("client_id", "cid"), ("client_secret", "cs")],
        );
        Arc::new(Self {
            all: Mutex::new(list.into_iter().map(|(n, c)| (n.to_owned(), c)).collect()),
            registrations: ["dropbox", "box", "microsoft_365", "google_drive"]
                .iter()
                .map(|a| ((*a).to_owned(), registration.clone()))
                .collect(),
        })
    }

    pub(crate) fn field(&self, name: &str, field: &str) -> Option<String> {
        self.all.lock().unwrap().get(name)?.fields.get(field).cloned()
    }
}

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.all.lock().unwrap().get(name).cloned()
    }

    fn registration(&self, app: &str) -> Option<Credential> {
        self.registrations.get(app).cloned()
    }

    fn keep<'a>(
        &'a self,
        name: &'a str,
        fields: BTreeMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let mut all = self.all.lock().unwrap();
            let c = all.get_mut(name).ok_or("no such credential")?;
            c.fields.extend(fields);
            Ok(Value::Null)
        })
    }
}

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A real run's effects, each vendor's address sent to the stand-ins.
pub(crate) struct Rerouted {
    pub live: Live,
    pub stand: Arc<Stand>,
}

impl Rerouted {
    fn moved(&self, mut r: HttpRequest) -> HttpRequest {
        r.url = self.stand.local(&r.url);
        r
    }
}

impl Effects for Rerouted {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move { self.live.http(self.moved(request)).await })
    }

    fn http_bytes<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<openduct_flow::files::RawAnswer, String>> + Send + 'a>> {
        Box::pin(async move { self.live.http_bytes(self.moved(request)).await })
    }

    fn write<'a>(&'a self, topic: &'a str, key: Option<String>, value: String) -> Pending<'a> {
        self.live.write(topic, key, value)
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        self.live.credential(name)
    }

    fn registration(&self, app: &str) -> Option<Credential> {
        self.live.registration(app)
    }

    fn keep_credential<'a>(&'a self, name: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
        self.live.keep_credential(name, fields)
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        self.live.file(path)
    }

    fn storage(&self) -> openduct_flow::storage::Reach<'_> {
        self.live.storage()
    }

    fn keep_file<'a>(
        &'a self,
        bytes: Arc<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send + 'a>> {
        self.live.keep_file(bytes)
    }
}

/// Effects for `secrets`, through the stand-ins; `allow_private` lets steps reach this machine (the stand-ins).
pub(crate) fn effects(stand: &Arc<Stand>, secrets: &Arc<Kept>, allow_private: bool) -> Rerouted {
    Rerouted {
        live: Live::new("127.0.0.1:1", allow_private)
            .unwrap()
            .with_secrets(Arc::clone(secrets) as Arc<dyn Secrets>),
        stand: Arc::clone(stand),
    }
}

/// A fetch of a presigned address, sent to the stand-ins: its status and body.
pub(crate) fn fetch(stand: &Stand, url: &str) -> (u16, Vec<u8>) {
    use std::io::{Read, Write};
    let local = stand.local(url);
    let rest = local.strip_prefix("http://").unwrap();
    let (host, target) = rest.split_once('/').unwrap();
    let mut stream = std::net::TcpStream::connect(host).unwrap();
    write!(
        stream,
        "GET /{target} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n"
    )
    .unwrap();
    let mut all = Vec::new();
    stream.read_to_end(&mut all).unwrap();
    let split = all.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&all[..split]).into_owned();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, all[split + 4..].to_vec())
}

/// JSON for a header: every character beyond ASCII as `\uXXXX`.
pub(crate) fn ascii(value: &serde_json::Value) -> String {
    let mut out = String::new();
    for c in value.to_string().chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            let mut units = [0u16; 2];
            for u in c.encode_utf16(&mut units) {
                out.push_str(&format!("\\u{u:04x}"));
            }
        }
    }
    out
}

/// A time in milliseconds as RFC 3339 writes it.
pub(crate) fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
