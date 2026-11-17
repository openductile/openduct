// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The MQTT connector as a person runs it. Reading and writing run against a real MQTT broker when
//! `OPENDUCT_TEST_MQTT` names one (`cargo xtask services up` starts one); a refused login runs against a stand-in.

#![cfg(feature = "pipelines")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::MemoryStore;
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};

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

/// A name no earlier run used: the MQTT broker keeps sessions between runs.
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

/// The MQTT broker's host and port.
fn mqtt() -> Option<(String, u16)> {
    let url = std::env::var("OPENDUCT_TEST_MQTT").ok()?;
    let (host, port) = url.trim_start_matches("mqtt://").split_once(':')?;
    Some((host.to_owned(), port.parse().ok()?))
}

/// A running pipeline, with its log lines as they come.
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

    fn wait_for_log(&mut self, text: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Some(line) = self.seen.iter().find(|l| l.contains(text)) {
                return line.clone();
            }
            if let Ok(line) = self.log.recv_timeout(Duration::from_millis(200)) {
                self.seen.push(line);
            }
        }
        panic!("no log line with {text:?}: {:?}", self.seen);
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
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Publish with QoS 1, as a device does, waiting until the MQTT broker has every message.
async fn publish(host: &str, port: u16, messages: &[(String, &str)]) {
    let (client, mut events) = AsyncClient::new(MqttOptions::new(run_name(), host, port), 100);
    for (topic, payload) in messages {
        client
            .publish(topic, QoS::AtLeastOnce, false, payload.as_bytes().to_vec())
            .await
            .unwrap();
    }
    let mut acked = 0;
    while acked < messages.len() {
        if let Event::Incoming(Packet::PubAck(_)) = events.poll().await.unwrap() {
            acked += 1;
        }
    }
    client.disconnect().await.unwrap();
    while !matches!(
        events.poll().await,
        Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_)
    ) {}
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtt_readings_reach_a_topic_and_what_arrives_while_stopped_comes_after() {
    let Some((host, port)) = mqtt() else {
        eprintln!("skipped: OPENDUCT_TEST_MQTT is not set (cargo xtask services up)");
        return;
    };
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let run = run_name();
        openduct(&a, &["topic", "create", "readings"]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("readings.yaml");
        std::fs::write(
            &file,
            format!(
                "name: readings-in\nfrom:\n  mqtt:\n    url: mqtt://{host}:{port}\n    topics: [{run}/+/temperature]\n    client_id: openduct-test-{run}\nmap:\n  device: d\n  celsius: c\nto:\n  topic: readings\n"
            ),
        )
        .unwrap();
        let reading = |device: &str| (format!("{run}/{device}/temperature"), device.to_owned());
        let payload = |device: &str, c: f64| format!(r#"{{"d":"{device}","c":{c}}}"#);

        let mut running = Running::start(&a, &file, &[]);
        running.wait_for_log(&format!("reading mqtt mqtt://{host}:{port} {run}/+/temperature"));
        // The subscription is made just after; give it a moment before the first message.
        std::thread::sleep(Duration::from_secs(1));
        let first = [payload("boiler-1", 71.5), payload("boiler-2", 64.0), payload("boiler-1", 72.0)];
        let devices = ["boiler-1", "boiler-2", "boiler-1"];
        let sent: Vec<(String, &str)> = devices.iter().zip(&first).map(|(d, p)| (reading(d).0, p.as_str())).collect();
        rt.block_on(publish(&host, port, &sent));
        let got = wait_for_values(&a, "readings", 3);
        let log = running.stop();
        assert_eq!(got.len(), 3, "{got:?}\n{log}");
        assert!(got[0].contains(r#"\"device\":\"boiler-1\",\"celsius\":71.5"#), "{}", got[0]);
        assert!(got[0].contains(&format!(r#""key":"{run}/boiler-1/temperature""#)), "keyed by MQTT topic: {}", got[0]);
        assert!(got[0].contains("mqtt-topic"), "{}", got[0]);

        // While it is stopped, the MQTT broker keeps what devices send for its session.
        let later = [payload("boiler-3", 55.5), payload("boiler-4", 58.0)];
        let sent: Vec<(String, &str)> = ["boiler-3", "boiler-4"].iter().zip(&later).map(|(d, p)| (reading(d).0, p.as_str())).collect();
        rt.block_on(publish(&host, port, &sent));
        let running = Running::start(&a, &file, &[]);
        let got = wait_for_values(&a, "readings", 5);
        // Anything sent twice would arrive now.
        std::thread::sleep(Duration::from_secs(2));
        let log = running.stop();
        let got_all = wait_for_values(&a, "readings", 5);
        assert_eq!(got.len(), 5, "{got:?}\n{log}");
        assert_eq!(got_all.len(), 5, "nothing twice: {got_all:?}");
        assert!(got_all[4].contains("boiler-4"), "{got_all:?}");
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_are_published_by_key_and_one_that_cannot_be_is_set_aside() {
    let Some((host, port)) = mqtt() else {
        eprintln!("skipped: OPENDUCT_TEST_MQTT is not set (cargo xtask services up)");
        return;
    };
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let run = run_name();
        for topic in ["commands", "commands-refused"] {
            openduct(&a, &["topic", "create", topic]);
        }
        // A device's view: subscribed to every valve's commands.
        let heard: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
        let (client, mut events) = AsyncClient::new(MqttOptions::new(run_name(), host.clone(), port), 100);
        rt.block_on(client.subscribe(format!("{run}/valves/+/set"), QoS::AtLeastOnce)).unwrap();
        let h = Arc::clone(&heard);
        rt.spawn(async move {
            while let Ok(event) = events.poll().await {
                if let Event::Incoming(Packet::Publish(p)) = event {
                    h.lock().unwrap().push((p.topic, String::from_utf8_lossy(&p.payload).into_owned()));
                }
            }
        });
        std::thread::sleep(Duration::from_millis(500));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("commands.yaml");
        std::fs::write(
            &file,
            format!(
                "name: commands-out\nfrom:\n  topic: commands\n  start: earliest\nto:\n  mqtt:\n    url: mqtt://{host}:{port}\n    topic: \"{run}/valves/{{key}}/set\"\non_error:\n  topic: commands-refused\n"
            ),
        )
        .unwrap();
        openduct(&a, &["send", "commands", "--key", "valve-1", r#"{"open":true}"#]);
        openduct(&a, &["send", "commands", r#"{"open":true}"#]);
        openduct(&a, &["send", "commands", "--key", "valve-#", r#"{"open":false}"#]);
        openduct(&a, &["send", "commands", "--key", "valve-2", r#"{"open":false}"#]);
        let mut running = Running::start(&a, &file, &[]);
        running.wait_for_log(&format!("mqtt mqtt://{host}:{port} {run}/valves/{{key}}/set"));
        let refused = wait_for_values(&a, "commands-refused", 2);
        let deadline = Instant::now() + Duration::from_secs(20);
        while heard.lock().unwrap().len() < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
        let log = running.stop();
        let heard = heard.lock().unwrap().clone();
        assert_eq!(
            heard,
            vec![
                (format!("{run}/valves/valve-1/set"), r#"{"open":true}"#.to_owned()),
                (format!("{run}/valves/valve-2/set"), r#"{"open":false}"#.to_owned()),
            ],
            "{log}"
        );
        assert_eq!(refused.len(), 2, "{refused:?}\n{log}");
        assert!(refused[0].contains("no key for {key}"), "{}", refused[0]);
        assert!(refused[1].contains("has a + or #"), "{}", refused[1]);
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_login_stops_the_pipeline_with_a_plain_reason_and_no_password() {
    let (broker, addr) = broker().await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        // A stand-in MQTT broker that refuses every login (CONNACK, return code 4: bad username or password).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connects: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let c = Arc::clone(&connects);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut connect = [0u8; 512];
                let n = stream.read(&mut connect).unwrap_or(0);
                c.lock().unwrap().push(connect[..n].to_vec());
                let _ = stream.write_all(&[0x20, 0x02, 0x00, 0x04]);
            }
        });
        openduct(&a, &["topic", "create", "readings"]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("refused.yaml");
        std::fs::write(
            &file,
            format!(
                "name: refused\nfrom:\n  mqtt:\n    url: mqtt://127.0.0.1:{port}\n    topics: [plant/#]\n    username: robot\n    password: secret:OPENDUCT_TEST_MQTT_PASSWORD\nto:\n  topic: readings\n"
            ),
        )
        .unwrap();
        let running = Running::start(&a, &file, &[("OPENDUCT_TEST_MQTT_PASSWORD", "hunter2-never-shown")]);
        let (ok, log) = running.ended();
        assert!(!ok, "{log}");
        assert!(log.contains("the MQTT broker refused the username or password"), "{log}");
        assert!(!log.contains("hunter2"), "the password never appears: {log}");
        let connects = connects.lock().unwrap();
        assert_eq!(connects.len(), 1, "a refusal is not tried again");
        assert!(connects[0].windows(19).any(|w| w == b"hunter2-never-shown"), "the password was sent to the MQTT broker");

        // Settings that cannot work are refused before anything starts.
        let bad = dir.path().join("bad.yaml");
        std::fs::write(&bad, "name: bad\nfrom:\n  mqtt:\n    url: mqtt://127.0.0.1:1883\n    topics: [plant/#/temperature]\nto:\n  topic: readings\n").unwrap();
        let out = Command::new(OPENDUCT).args(["pipeline", "check", bad.to_str().unwrap()]).output().unwrap();
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("is not an MQTT topic filter"), "{out:?}");
    })
    .await
    .unwrap();
    broker.shutdown().await;
}

/// Certificates for a test: a CA, an MQTT broker's certificate for localhost and a device's client certificate,
/// all signed by the CA, as PEM.
struct Certificates {
    ca: String,
    server: (
        rustls::pki_types::CertificateDer<'static>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    client_cert: String,
    client_key: String,
}

fn certificates() -> Certificates {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    };
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().unwrap()).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let mut server = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server = server.signed_by(&server_key, &*ca).unwrap();
    let client_key = KeyPair::generate().unwrap();
    let mut client = CertificateParams::new(Vec::<String>::new()).unwrap();
    client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client = client.signed_by(&client_key, &*ca).unwrap();
    Certificates {
        ca: ca.pem(),
        server: (
            server.der().clone(),
            rustls::pki_types::PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        ),
        client_cert: client.pem(),
        client_key: client_key.serialize_pem(),
    }
}

/// One whole MQTT packet off `stream`.
async fn packet(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut out = vec![stream.read_u8().await.unwrap()];
    let mut len = 0usize;
    for shift in 0..4 {
        let byte = stream.read_u8().await.unwrap();
        out.push(byte);
        len |= usize::from(byte & 0x7f) << (7 * shift);
        if byte & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await.unwrap();
    out.extend(body);
    out
}

/// A stand-in MQTT broker over TLS that wants a client certificate from the test CA: it takes a login and a
/// subscription, sends one QoS 1 message, and reports the acknowledgement it gets back.
async fn tls_stand_in(certs: &Certificates) -> (u16, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
    use tokio::io::AsyncWriteExt;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::pem::PemObject::pem_slice_iter(certs.ca.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let verifier =
        rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
            .build()
            .unwrap();
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![certs.server.0.clone()], certs.server.1.clone_key())
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                continue;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                assert_eq!(packet(&mut tls).await[0], 0x10, "CONNECT");
                tls.write_all(&[0x20, 2, 0, 0]).await.unwrap();
                let subscribe = packet(&mut tls).await;
                assert_eq!(subscribe[0], 0x82, "SUBSCRIBE");
                tls.write_all(&[0x90, 3, subscribe[2], subscribe[3], 1])
                    .await
                    .unwrap();
                let mut publish = vec![0x32, 0, 0, 19];
                publish.extend_from_slice(b"plant/press-1/state");
                publish.extend_from_slice(&[0, 7]);
                publish.extend_from_slice(br#"{"on":true}"#);
                publish[1] = u8::try_from(publish.len() - 2).unwrap();
                tls.write_all(&publish).await.unwrap();
                tls.flush().await.unwrap();
                let _ = tx.send(packet(&mut tls).await);
            });
        }
    });
    (port, rx)
}

#[tokio::test(flavor = "multi_thread")]
async fn mqtts_with_a_private_ca_and_a_client_certificate_reads_and_acknowledges() {
    let (broker, addr) = broker().await;
    let certs = certificates();
    let (port, mut acks) = tls_stand_in(&certs).await;
    let other = certificates();
    let a = addr.clone();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        openduct(&a, &["topic", "create", "plant"]);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        for (name, pem) in [
            ("ca.pem", &certs.ca),
            ("other-ca.pem", &other.ca),
            ("robot.pem", &certs.client_cert),
            ("robot-key.pem", &certs.client_key),
        ] {
            std::fs::write(d.join(name), pem).unwrap();
        }
        let pipeline = |ca: &str| {
            format!(
                "name: plant-in\nfrom:\n  mqtt:\n    url: mqtts://localhost:{port}\n    topics: [plant/#]\n    ca: file:{0}/{ca}\n    client_cert: file:{0}/robot.pem\n    client_key: file:{0}/robot-key.pem\nto:\n  topic: plant\n",
                d.display()
            )
        };
        let file = d.join("plant.yaml");
        std::fs::write(&file, pipeline("ca.pem")).unwrap();
        let running = Running::start(&a, &file, &[]);
        let got = wait_for_values(&a, "plant", 1);
        let ack = rt.block_on(async { tokio::time::timeout(Duration::from_secs(10), acks.recv()).await });
        let log = running.stop();
        assert_eq!(got.len(), 1, "{log}");
        assert!(got[0].contains(r#"\"on\":true"#) && got[0].contains("plant/press-1/state"), "{}", got[0]);
        assert_eq!(ack.unwrap().unwrap(), vec![0x40, 2, 0, 7], "PUBACK for packet 7, once stored");

        // An MQTT broker whose certificate another CA signed is refused, and said so plainly.
        std::fs::write(&file, pipeline("other-ca.pem")).unwrap();
        let (ok, log) = Running::start(&a, &file, &[]).ended();
        assert!(!ok);
        assert!(log.contains("the MQTT broker's certificate was not accepted"), "{log}");
        assert!(!log.contains("PRIVATE KEY"), "{log}");
    })
    .await
    .unwrap();
    broker.shutdown().await;
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
