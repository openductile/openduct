// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Hostile input for OpenDuct: every place it reads bytes it did not write, as one function each. A client may send
//! anything; so may a device, a database or another cluster a connector reads from, and a disk may hand back damage.
//! Each function takes any bytes and must return - without a panic, without hanging, and without taking more memory
//! than the bytes justify. That is all they check: what the code does with good input is for the other tests.
//!
//! They are driven two ways: by the random tests in `tests/random.rs`, which mutate valid inputs with a fixed seed and
//! run with every `cargo test`; and by `cargo fuzz` (the `fuzz/` directory), guided by coverage, in CI.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use openduct_broker::client::Client;
use openduct_broker::security::scram::{MIN_ITERATIONS, ScramCredential, ScramMechanism};
use openduct_broker::security::{Credentials, Security, Users};
use openduct_broker::{Broker, BrokerConfig};
use openduct_expr::{Env, Value};
use openduct_storage::{FileStoreConfig, MemoryStore, PartitionLog};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A Kafka request frame (what follows its four-byte length), as the broker reads one: the header, checked against
/// the APIs and versions it serves, then the request.
pub fn kafka_request(frame: &[u8]) {
    if let Some((key, version, mut body)) = openduct_broker::hostile::request_header(frame) {
        let _ = kafka_protocol::messages::RequestKind::decode(key, &mut body, version);
    }
}

/// A Produce request's records: split into batches, and each batch's header read and checked.
pub fn record_batches(records: &[u8]) {
    if let Ok(batches) = openduct_storage::batch::split_batches(&Bytes::copy_from_slice(records)) {
        for (_, batch) in batches {
            let _ = openduct_storage::batch::parse_header(&batch);
        }
    }
}

/// The records inside a batch, as log compaction reads them (`openduct_storage::open`): the input is a batch's
/// records - its first byte how many the header claims - under a header whose length and checksum are right, so
/// the reader is reached; and the same input as the compressed records of each of Kafka's four codecs. Reading and writing the batch again must not panic or reach past the batch, and what is
/// written again must read back with the offsets it kept.
pub fn records(data: &[u8]) {
    let Some((claimed, body)) = data.split_first() else {
        return;
    };
    // As records that are not compressed, then as what each codec would have to decompress first.
    for codec in 0i16..=4 {
        records_under(i32::from(*claimed).max(1), codec, body);
    }
}

/// `body` as the records of a batch of `count` records compressed with `codec` (0: not compressed).
fn records_under(count: i32, codec: i16, body: &[u8]) {
    let Ok(length) = i32::try_from(openduct_storage::batch::HEADER_LEN - 12 + body.len()) else {
        return;
    };
    let mut b = Vec::with_capacity(openduct_storage::batch::HEADER_LEN + body.len());
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&length.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.push(2);
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&codec.to_be_bytes());
    b.extend_from_slice(&(count - 1).to_be_bytes());
    b.extend_from_slice(&[0u8; 16]);
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b.extend_from_slice(&count.to_be_bytes());
    b.extend_from_slice(body);
    let crc = openduct_storage::batch::checksum(&b);
    if let Some(at) = b.get_mut(17..21) {
        at.copy_from_slice(&crc.to_be_bytes());
    }
    let batch = Bytes::from(b);
    let Ok(opened) = openduct_storage::open(&batch) else {
        return;
    };
    // A key is read through the opened batch, which checks where it lies.
    for r in &opened.records {
        let _ = opened.key(r);
    }
    if let Ok(openduct_storage::Retained::Some(left)) = opened.retain(&batch, |r, _| r.offset_delta % 2 == 0)
    {
        let read = openduct_storage::open(&left);
        assert!(read.is_ok(), "a batch written again does not read back");
        let kept = read.map(|o| o.records).unwrap_or_default();
        assert!(
            kept.iter().all(|r| r.offset_delta % 2 == 0),
            "a record that was not kept"
        );
    }
}

/// The schema registry (`openduct_registry`), which reads what any client sends it: the input as an Avro schema,
/// a JSON Schema and a .proto file, each to read and to compare with itself and a plain one; as a request's body to every route that takes one, with
/// the input's first line as the path; and as a record of the registry's topic. Whatever it is, the registry
/// answers - a status and a body that is JSON - and what it says to write can be written and read back.
pub fn registry(data: &[u8]) {
    use openduct_registry::api::{Outcome, Request, handle};
    use openduct_registry::registry::Registry;
    let text = String::from_utf8_lossy(data);
    if let Ok(schema) = openduct_registry::avro::Schema::parse(&text) {
        let _ = schema.can_read(&schema);
        if let Ok(plain) = openduct_registry::avro::Schema::parse("\"string\"") {
            let _ = schema.can_read(&plain);
            let _ = plain.can_read(&schema);
        }
    }
    // The same input as a JSON Schema and as a .proto file.
    if let Ok(schema) = openduct_registry::json_schema::Schema::parse(&text) {
        let _ = schema.can_read(&schema);
        if let Ok(plain) = openduct_registry::json_schema::Schema::parse("{\"type\":\"object\"}") {
            let _ = schema.can_read(&plain);
            let _ = plain.can_read(&schema);
        }
    }
    if let Ok(schema) = openduct_registry::protobuf::Schema::parse(&text) {
        assert!(
            schema.can_read(&schema).is_ok(),
            "a .proto file that does not read what it wrote"
        );
    }
    let mut state = Registry::default();
    let (first, rest) = text.split_once('\n').unwrap_or((&text, ""));
    let (path, query) = first.split_once('?').unwrap_or((first, ""));
    let paths = [
        path,
        "/subjects/s/versions",
        "/subjects/s",
        "/compatibility/subjects/s/versions",
        "/config",
        "/mode",
    ];
    for method in ["GET", "POST", "PUT", "DELETE"] {
        for path in paths {
            for body in [data, rest.as_bytes()] {
                let request = Request {
                    method,
                    path,
                    query,
                    body,
                };
                let response = match handle(&state, &request) {
                    Outcome::Answer(response) => response,
                    Outcome::Write { changes, then } => {
                        for change in &changes {
                            let record = openduct_registry::log::encode(change);
                            let read = openduct_registry::log::decode(&record.key, record.value.as_deref());
                            assert!(
                                matches!(&read, Ok(Some(back)) if back == change),
                                "a change does not read back from its record"
                            );
                            assert!(
                                state.apply(change).is_ok(),
                                "a change the registry made does not apply"
                            );
                        }
                        then
                    }
                };
                assert!((200..600).contains(&response.status), "a status that is not one");
            }
        }
    }
    let (key, value) = data.split_at(data.len() / 2);
    if let Ok(Some(change)) = openduct_registry::log::decode(key, Some(value)) {
        let _ = state.apply(&change);
    }
    let _ = openduct_registry::log::decode(data, None);
}

/// A directory of its own under the system's temporary directory, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Option<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("openduct-hostile-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).ok()?;
        Some(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A segment file as a disk may hand it back: the partition opened over it (recovering, or refusing damage), then
/// read from the start, with its offsets and epochs asked.
pub fn segment(bytes: &[u8]) {
    let Some(dir) = Scratch::new() else { return };
    let written =
        std::fs::File::create(dir.path().join(format!("{:020}.log", 0))).and_then(|mut f| f.write_all(bytes));
    if written.is_err() {
        return;
    }
    if let Ok(log) = PartitionLog::open(dir.path(), FileStoreConfig::default()) {
        let offsets = log.offsets();
        let _ = log.read(offsets.log_start, 1 << 20);
        let _ = log.epoch_of(offsets.log_start, 0);
        let _ = log.epoch_end(0, 0);
    }
}

/// An expression in CEL, as `openduct pipeline check` compiles one and a pipeline works it out for each message -
/// within its limits. The parser and the evaluator recurse once per level: a debug build needs a thread with a
/// larger stack than a test's own (the random tests give it one), a release build does not.
pub fn expression(source: &[u8]) {
    let text = String::from_utf8_lossy(source);
    let Ok(expression) = openduct_expr::compile("cel", &text) else {
        return;
    };
    let _ = expression.evaluate(&Env::of(message()));
}

fn message() -> &'static Value {
    static MESSAGE: OnceLock<Value> = OnceLock::new();
    MESSAGE.get_or_init(|| {
        Value::parse_json(
            r#"{"name":"Ada","price":12.5,"qty":3,"tags":["a","b"],"at":"2026-11-16T09:00:00Z",
                "order":{"id":"o-1","lines":[{"sku":"x","qty":2}]},"status":503,"note":null}"#,
        )
        .unwrap_or(Value::Null)
    })
}

/// A pipeline file, as `openduct pipeline check` reads one.
pub fn pipeline(text: &[u8]) {
    let _ = openduct_pipeline::Pipeline::parse(&String::from_utf8_lossy(text));
}

/// Packets from an MQTT broker (the `mqtt` connector).
pub fn mqtt(bytes: &[u8]) {
    openduct_connect::hostile::mqtt(bytes);
}

/// Replication messages from PostgreSQL (`postgres_cdc`), as length-prefixed chunks (see [`chunks`]).
pub fn postgres(bytes: &[u8]) {
    openduct_connect::hostile::postgres_replication(chunks(bytes));
}

/// Binary log events from MySQL (`mysql_cdc`): the first byte's low bit says whether events end with a CRC32, the
/// rest are length-prefixed chunks (see [`chunks`]).
pub fn mysql(bytes: &[u8]) {
    let checksum = bytes.first().is_some_and(|b| b & 1 == 1);
    openduct_connect::hostile::mysql_binlog(checksum, chunks(bytes.get(1..).unwrap_or_default()));
}

/// `bytes` cut into messages: each a two-byte length and that many bytes (fewer at the end), so a mutation can
/// change one message and leave the others whole.
pub fn chunks(bytes: &[u8]) -> Vec<Bytes> {
    let mut out = Vec::new();
    let mut rest = bytes;
    while let Some((head, tail)) = rest.split_first_chunk::<2>() {
        let len = usize::from(u16::from_be_bytes(*head)).min(tail.len());
        let (chunk, after) = tail.split_at(len);
        out.push(Bytes::copy_from_slice(chunk));
        rest = after;
    }
    if !rest.is_empty() {
        out.push(Bytes::copy_from_slice(rest));
    }
    out
}

/// A running broker for [`broker`]: kept for a few hundred inputs, then started afresh so what they stored does not
/// pile up - and before its sign-in failures from this address reach the broker's limit (ten a minute, after which
/// it refuses the address), which would read here as a broker that stopped answering.
struct Running {
    broker: Broker,
    addr: String,
    used: u32,
    sign_ins: u32,
    /// A connection kept for asking whether the broker still answers; a fresh one every [`FRESH_PROBE_EVERY`]
    /// inputs shows it still takes new ones. (A new connection for every probe runs a machine out of ports.)
    probe: Option<Client>,
}

const FRESH_PROBE_EVERY: u32 = 50;

const INPUTS_PER_BROKER: u32 = 500;
const SIGN_INS_PER_BROKER: u32 = 8;
const SASL_HANDSHAKE: i16 = 17;
const SASL_AUTHENTICATE: i16 = 36;

fn runtime() -> Option<&'static tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<Option<tokio::runtime::Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .ok()
        })
        .as_ref()
}

/// The first panic anywhere in the process since it was last taken: a request that panics a connection's task ends
/// only that connection, so a probe alone would never see it. The hook before ours still runs (under `cargo fuzz`,
/// it stops the run).
static PANICKED: Mutex<Option<String>> = Mutex::new(None);

fn watch_panics() {
    static WATCHING: OnceLock<()> = OnceLock::new();
    WATCHING.get_or_init(|| {
        let before = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Ok(mut p) = PANICKED.lock() {
                p.get_or_insert_with(|| info.to_string());
            }
            before(info);
        }));
    });
}

async fn start_broker() -> Result<Running, String> {
    let mut credentials = std::collections::BTreeMap::new();
    for mechanism in ScramMechanism::ALL {
        let credential = ScramCredential::new(mechanism, "a-password-for-hostile-tests", MIN_ITERATIONS)?;
        credentials.insert(mechanism, credential);
    }
    let mut users = Users::default();
    users.users.insert("fuzz".to_owned(), credentials);
    let mut security = Security::open();
    security.credentials = Some(Arc::new(Credentials::fixed(users)));
    let mut config = BrokerConfig::new(std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
    config.advertised_port = 0;
    config.security = security;
    config.auto_create_topics = true;
    config.group_initial_rebalance_delay = Duration::ZERO;
    config.max_fetch_wait = Duration::from_millis(20);
    config.max_request_bytes = 1 << 20;
    let broker = Broker::start(config, Arc::new(MemoryStore::new()))
        .await
        .map_err(|e| e.to_string())?;
    let addr = broker.local_addr().to_string();
    Ok(Running {
        broker,
        addr,
        used: 0,
        sign_ins: 0,
        probe: None,
    })
}

fn is_sign_in(frame: &[u8]) -> bool {
    frame
        .first_chunk::<2>()
        .is_some_and(|k| matches!(i16::from_be_bytes(*k), SASL_HANDSHAKE | SASL_AUTHENTICATE))
}

/// Requests sent to a running broker over TCP, each chunk of `bytes` (see [`chunks`]) as one frame with its real
/// length - so they reach the decoder and the handlers, sign-in included - and the connection closed. Afterwards no
/// task of the broker may have panicked, and it must still answer a new connection; an `Err` says which failed.
pub fn broker(bytes: &[u8]) -> Result<(), String> {
    static RUNNING: Mutex<Option<Running>> = Mutex::new(None);
    watch_panics();
    let runtime = runtime().ok_or("no runtime")?;
    let mut running = RUNNING.lock().map_err(|_| "an earlier input panicked")?;
    let mut frames = chunks(bytes);
    let mut sign_ins = 0;
    frames.retain(|f| {
        let keep = !is_sign_in(f) || sign_ins < SIGN_INS_PER_BROKER;
        sign_ins += u32::from(is_sign_in(f) && keep);
        keep
    });
    if running
        .as_ref()
        .is_some_and(|r| r.used >= INPUTS_PER_BROKER || r.sign_ins + sign_ins > SIGN_INS_PER_BROKER)
        && let Some(old) = running.take()
    {
        runtime.block_on(old.broker.shutdown());
    }
    if running.is_none() {
        *running = Some(runtime.block_on(start_broker())?);
    }
    let Some(current) = running.as_mut() else {
        return Err("no broker".to_owned());
    };
    current.used += 1;
    current.sign_ins += sign_ins;
    let addr = current.addr.clone();
    let kept = current
        .probe
        .take()
        .filter(|_| current.used % FRESH_PROBE_EVERY != 0);
    let answered = runtime.block_on(async move {
        if let Ok(mut socket) = tokio::net::TcpStream::connect(&addr).await {
            let mut out = Vec::new();
            for frame in frames {
                let len = i32::try_from(frame.len()).unwrap_or(i32::MAX);
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(&frame);
            }
            let _ = socket.write_all(&out).await;
            let _ = socket.shutdown().await;
            // Whatever it answers, until it closes; a request it may rightly hold (a group join) is let go.
            let mut sink = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut sink)).await;
        }
        if let Some(mut probe) = kept
            && probe.api_versions().await.is_ok()
        {
            return Ok::<Client, String>(probe);
        }
        let mut probe = Client::connect(&addr, Duration::from_secs(5))
            .await
            .map_err(|e| format!("the broker no longer takes connections: {e}"))?;
        probe
            .api_versions()
            .await
            .map_err(|e| format!("the broker no longer answers: {e}"))?;
        Ok(probe)
    });
    if let Some(panic) = PANICKED.lock().ok().and_then(|mut p| p.take()) {
        return Err(format!("a request panicked the broker: {panic}"));
    }
    current.probe = Some(answered?);
    Ok(())
}
