// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The registry served over HTTP against a running broker: registered on one node and read on another, two nodes
//! registering at once, read back after a restart, and its token asked for.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_broker::client::{ClientSecurity, Shared};
use openduct_broker::{Broker, BrokerConfig};
use openduct_registry::http;
use openduct_registry::service::Service;
use openduct_storage::{MemoryStore, Store};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ORDER: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}"#;

async fn broker(store: Arc<dyn Store>) -> Broker {
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    // Compaction of the registry's topic runs while the test does.
    config.retention_check_interval = Duration::from_millis(100);
    Broker::start(config, store).await.unwrap()
}

/// A registry node on `broker`, as another machine of its cluster would run it: its own connections, its own
/// state, its own listener.
async fn node(broker: &Broker, token: Option<&str>) -> (SocketAddr, Arc<Service>) {
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let service = Service::start(shared).await.unwrap();
    let (at, _task) = http::start(
        "127.0.0.1:0".parse().unwrap(),
        http::SignIn {
            token: token.map(Arc::from),
            users: None,
            certificates: false,
        },
        false,
        None,
        Arc::clone(&service),
    )
    .await
    .unwrap();
    (at, service)
}

/// One HTTP request, as a client sends it: the status and the body.
async fn call(
    at: SocketAddr,
    method: &str,
    target: &str,
    body: Option<Value>,
    authorization: Option<&str>,
) -> (u16, Value) {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let authorization = authorization
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {at}\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\n\
         Connection: close\r\n{authorization}Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut socket = tokio::net::TcpStream::connect(at).await.unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    socket.read_to_end(&mut answer).await.unwrap();
    let answer = String::from_utf8(answer).unwrap();
    let (head, body) = answer.split_once("\r\n\r\n").unwrap();
    assert!(head.contains("application/vnd.schemaregistry.v1+json"), "{head}");
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

async fn get(at: SocketAddr, target: &str) -> (u16, Value) {
    call(at, "GET", target, None, None).await
}

async fn until(what: &str, mut ready: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready().await {
        assert!(Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registered_on_one_node_read_on_another_and_read_back_after_a_restart() {
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
    let broker = broker(Arc::clone(&store)).await;
    let (a, _a) = node(&broker, None).await;
    let (b, _b) = node(&broker, None).await;
    assert!(
        store.topic("_schemas").unwrap().settings.compacts(),
        "its topic is compacted"
    );

    // A serializer registers through node A and is told the id; the id is there at once.
    assert_eq!(
        call(
            a,
            "POST",
            "/subjects/orders-value/versions",
            Some(json!({ "schema": ORDER })),
            None
        )
        .await,
        (200, json!({ "id": 1 }))
    );
    assert_eq!(get(a, "/schemas/ids/1").await, (200, json!({ "schema": ORDER })));
    // A consumer asks node B, which read it from the topic.
    until("node B has the schema", async || {
        get(b, "/schemas/ids/1").await.0 == 200
    })
    .await;
    assert_eq!(get(b, "/subjects").await.1, json!(["orders-value"]));

    // A level set through B holds on A: a schema that cannot read the old data is refused there.
    assert_eq!(
        call(
            b,
            "PUT",
            "/config/orders-value",
            Some(json!({ "compatibility": "FULL" })),
            None
        )
        .await
        .0,
        200
    );
    until("node A has the level", async || {
        get(a, "/config/orders-value").await.0 == 200
    })
    .await;
    let breaking = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"total","type":"double"}]}"#;
    let (status, why) = call(
        a,
        "POST",
        "/subjects/orders-value/versions",
        Some(json!({ "schema": breaking })),
        None,
    )
    .await;
    assert_eq!((status, &why["error_code"]), (409, &json!(409)), "{why}");

    // Versions come and go; the topic is compacted meanwhile; a node started later reads the same registry.
    for i in 0..20 {
        let schema = format!(r#"{{"type":"record","name":"T{i}","fields":[]}}"#);
        let subject = format!("/subjects/topic-{}-value/versions", i % 4);
        call(
            a,
            "PUT",
            &format!("/config/topic-{}-value", i % 4),
            Some(json!({ "compatibility": "NONE" })),
            None,
        )
        .await;
        assert_eq!(
            call(a, "POST", &subject, Some(json!({ "schema": schema })), None)
                .await
                .0,
            200
        );
    }
    assert_eq!(
        call(a, "DELETE", "/subjects/topic-0-value", None, None).await.0,
        200
    );
    assert_eq!(
        call(a, "DELETE", "/subjects/topic-0-value?permanent=true", None, None)
            .await
            .0,
        200
    );
    let (late, _late) = node(&broker, None).await;
    for target in [
        "/subjects",
        "/subjects?deleted=true",
        "/subjects/topic-1-value/versions",
        "/schemas/ids/7",
        "/config/orders-value",
    ] {
        assert_eq!(get(late, target).await, get(a, target).await, "{target}");
    }
    assert_eq!(get(late, "/subjects/topic-0-value/versions").await.0, 404);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_registering_at_once_end_with_one_registry_and_no_id_given_twice() {
    let broker = broker(Arc::new(MemoryStore::new())).await;
    let (a, _a) = node(&broker, None).await;
    let (b, _b) = node(&broker, None).await;
    // Forty different schemas, half through each node, all at the same moment.
    let mut calls = tokio::task::JoinSet::new();
    for i in 0..40 {
        let at = if i % 2 == 0 { a } else { b };
        calls.spawn(async move {
            let schema = format!(r#"{{"type":"record","name":"R{i}","fields":[]}}"#);
            let (status, answer) = call(
                at,
                "POST",
                &format!("/subjects/s{i}/versions"),
                Some(json!({ "schema": schema })),
                None,
            )
            .await;
            assert_eq!(status, 200, "{answer}");
            (i, answer["id"].as_i64().unwrap())
        });
    }
    let mut ids = std::collections::BTreeMap::new();
    while let Some(done) = calls.join_next().await {
        let (i, id) = done.unwrap();
        assert!(ids.insert(id, i).is_none(), "id {id} was given to two schemas");
    }
    assert_eq!(ids.len(), 40);
    // Each node gives every schema back under the id its registrant was told.
    until("both nodes have all forty", async || {
        get(a, "/subjects")
            .await
            .1
            .as_array()
            .is_some_and(|s| s.len() == 40)
            && get(b, "/subjects")
                .await
                .1
                .as_array()
                .is_some_and(|s| s.len() == 40)
    })
    .await;
    for (id, i) in &ids {
        let want = json!({ "schema": format!(r#"{{"type":"record","name":"R{i}","fields":[]}}"#) });
        assert_eq!(get(a, &format!("/schemas/ids/{id}")).await.1, want);
        assert_eq!(get(b, &format!("/schemas/ids/{id}")).await.1, want);
    }
    // The same schema registered through both at once is one id.
    let same = json!({ "schema": r#""long""# });
    let (one, two) = tokio::join!(
        call(a, "POST", "/subjects/x/versions", Some(same.clone()), None),
        call(b, "POST", "/subjects/y/versions", Some(same.clone()), None)
    );
    assert_eq!((one.0, two.0), (200, 200));
    assert_eq!(one.1, two.1, "one schema, one id");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_a_token_set_nothing_is_answered_without_it() {
    let broker = broker(Arc::new(MemoryStore::new())).await;
    let token = "a-registry-token-0123456789";
    let (at, _service) = node(&broker, Some(token)).await;
    assert_eq!(get(at, "/subjects").await.0, 401);
    assert_eq!(
        call(at, "GET", "/subjects", None, Some("Bearer wrong")).await.0,
        401
    );
    assert_eq!(
        call(at, "GET", "/subjects", None, Some(&format!("Bearer {token}"))).await,
        (200, json!([]))
    );
    // As a Confluent client sends basic.auth.user.info=anyone:<token>.
    let basic = "Basic YW55b25lOmEtcmVnaXN0cnktdG9rZW4tMDEyMzQ1Njc4OQ==";
    assert_eq!(
        call(
            at,
            "POST",
            "/subjects/s/versions",
            Some(json!({ "schema": ORDER })),
            Some(basic)
        )
        .await,
        (200, json!({ "id": 1 }))
    );
    // Beyond this machine it is not served without one.
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let service = Service::start(shared).await.unwrap();
    let refused = http::start(
        "0.0.0.0:0".parse().unwrap(),
        http::SignIn::default(),
        false,
        None,
        service,
    )
    .await
    .unwrap_err();
    assert!(refused.contains("--registry-token-file"), "{refused}");
    broker.shutdown().await;
}

/// A certificate authority for the test, and certificates it signs for `127.0.0.1`.
struct Pki {
    ca: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new() -> Self {
        let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        Self {
            ca: rcgen::CertifiedIssuer::self_signed(ca, rcgen::KeyPair::generate().unwrap()).unwrap(),
        }
    }

    fn issue(&self) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let cert = params.signed_by(&key, &*self.ca).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn beside_a_broker_that_asks_for_tls_and_sign_in_the_registry_signs_in_and_serves_https() {
    use openduct_broker::client::SignIn;
    use openduct_broker::security::scram::{DEFAULT_ITERATIONS, ScramCredential, ScramMechanism};
    use openduct_broker::security::{ClientCerts, ClientTls, Credentials, Security, Users};

    const PASSWORD: &str = "the-registry's-own-password";
    let pki = Pki::new();
    // The broker: TLS on its port, and nobody in without signing in.
    let (cert, key) = pki.issue();
    let mut users = std::collections::BTreeMap::new();
    let mut credentials = std::collections::BTreeMap::new();
    for mechanism in ScramMechanism::ALL {
        credentials.insert(
            mechanism,
            ScramCredential::new(mechanism, PASSWORD, DEFAULT_ITERATIONS).unwrap(),
        );
    }
    users.insert("registry".to_owned(), credentials);
    let mut security = Security::open();
    security.tls =
        Some(ClientTls::from_pem(cert.as_bytes(), key.as_bytes(), None, ClientCerts::None).unwrap());
    security.credentials = Some(Arc::new(Credentials::fixed(Users { format: 1, users })));
    security.require_authentication = true;
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.security = security;
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let address = broker.local_addr().to_string();

    // Without TLS and a sign-in the registry cannot reach its topic, and says so.
    let plain = Arc::new(Shared::new(
        &address,
        Duration::from_secs(3),
        &ClientSecurity::default(),
    ));
    assert!(Service::start(plain).await.is_err());

    // With them it starts, and serves with a certificate of its own.
    let to_broker = ClientSecurity {
        tls: Some(ClientSecurity::tls_from_pem(pki.ca.pem().as_bytes(), None).unwrap()),
        server_name: None,
        sign_in: Some(SignIn::Scram {
            mechanism: ScramMechanism::Sha512,
            user: "registry".to_owned(),
            password: PASSWORD.to_owned(),
        }),
    };
    let shared = Arc::new(Shared::new(&address, Duration::from_secs(10), &to_broker));
    let service = Service::start(shared).await.unwrap();
    let (cert, key) = pki.issue();
    let serving = ClientTls::from_pem(cert.as_bytes(), key.as_bytes(), None, ClientCerts::None)
        .unwrap()
        .acceptor();
    let token = "a-registry-token-0123456789";
    let (at, _task) = http::start(
        "127.0.0.1:0".parse().unwrap(),
        http::SignIn {
            token: Some(Arc::from(token)),
            users: None,
            certificates: false,
        },
        false,
        Some(serving),
        service,
    )
    .await
    .unwrap();

    // A client that trusts the authority, over HTTPS, with the token.
    let trust = ClientSecurity::tls_from_pem(pki.ca.pem().as_bytes(), None).unwrap();
    let https = |method: &'static str, target: &'static str, body: Option<Value>, authorized: bool| {
        let trust = Arc::clone(&trust);
        async move {
            let body = body.map(|b| b.to_string()).unwrap_or_default();
            let authorization = if authorized {
                format!("Authorization: Bearer {token}\r\n")
            } else {
                String::new()
            };
            let request = format!(
                "{method} {target} HTTP/1.1\r\nHost: {at}\r\nConnection: close\r\n{authorization}Content-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let socket = tokio::net::TcpStream::connect(at).await.unwrap();
            let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
            let mut secured = tokio_rustls::TlsConnector::from(trust)
                .connect(name, socket)
                .await
                .unwrap();
            secured.write_all(request.as_bytes()).await.unwrap();
            let mut answer = Vec::new();
            // The registry closes after its answer; a close without TLS's goodbye still leaves the answer read.
            let _ = secured.read_to_end(&mut answer).await;
            let answer = String::from_utf8(answer).unwrap();
            let (head, body) = answer.split_once("\r\n\r\n").unwrap();
            let status: u16 = head.split(' ').nth(1).unwrap().parse().unwrap();
            (status, serde_json::from_str::<Value>(body).unwrap_or(Value::Null))
        }
    };
    assert_eq!(
        https("GET", "/subjects", None, false).await.0,
        401,
        "TLS does not say who is asking"
    );
    assert_eq!(
        https(
            "POST",
            "/subjects/orders-value/versions",
            Some(json!({ "schema": ORDER })),
            true
        )
        .await,
        (200, json!({ "id": 1 }))
    );
    assert_eq!(
        https("GET", "/subjects", None, true).await,
        (200, json!(["orders-value"]))
    );
    // The same port does not answer plain HTTP: at most a TLS alert comes back, never an HTTP answer.
    let said = tokio::time::timeout(Duration::from_secs(5), async {
        let mut socket = tokio::net::TcpStream::connect(at).await.unwrap();
        socket
            .write_all(b"GET /subjects HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut answer = Vec::new();
        let _ = socket.read_to_end(&mut answer).await;
        answer
    })
    .await
    .unwrap();
    assert!(
        !said.starts_with(b"HTTP/"),
        "an HTTP answer to a client that does not speak TLS"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_brokers_users_sign_in_to_the_registry_and_are_held_to_its_rules() {
    use openduct_broker::security::scram::{DEFAULT_ITERATIONS, ScramCredential, ScramMechanism};
    use openduct_broker::security::{
        Authorizer, Credentials, Operation, Pattern, Permission, Resource, Rule, Rules, Security, Users,
    };

    let broker = broker(Arc::new(MemoryStore::new())).await;
    // Two teams and an administrator, as the broker's credentials file would hold them.
    let mut users = std::collections::BTreeMap::new();
    for (name, password) in [
        ("team-a", "team-a-password-long-enough"),
        ("team-b", "team-b-password-long-enough"),
        ("admin", "admin-password-long-enough"),
    ] {
        let mut credentials = std::collections::BTreeMap::new();
        for mechanism in ScramMechanism::ALL {
            credentials.insert(
                mechanism,
                ScramCredential::new(mechanism, password, DEFAULT_ITERATIONS).unwrap(),
            );
        }
        users.insert(name.to_owned(), credentials);
    }
    // Each team reads and registers under its own prefix; team B may also read team A's.
    let rule = |principal: &str, name: &str, operations: &[Operation]| Rule {
        principal: format!("User:{principal}"),
        host: "*".to_owned(),
        resource: Resource::Subject,
        pattern: Pattern::Prefixed,
        name: name.to_owned(),
        operations: operations.to_vec(),
        permission: Permission::Allow,
    };
    let mut security = Security::open();
    security.credentials = Some(Arc::new(Credentials::fixed(Users { format: 1, users })));
    security.rules = Some(Arc::new(
        Authorizer::fixed(Rules {
            super_users: vec!["User:admin".to_owned()],
            rules: vec![
                rule("team-a", "team-a.", &[Operation::Read, Operation::Write]),
                rule(
                    "team-b",
                    "team-b.",
                    &[Operation::Read, Operation::Write, Operation::Delete],
                ),
                rule("team-b", "team-a.", &[Operation::Read]),
            ],
            quotas: std::collections::BTreeMap::new(),
        })
        .unwrap(),
    ));
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let service = Service::start(shared).await.unwrap();
    let key = "an-everything-token-0123456789";
    let (at, _task) = http::start(
        "127.0.0.1:0".parse().unwrap(),
        http::SignIn {
            token: Some(Arc::from(key)),
            users: Some(security),
            certificates: false,
        },
        false,
        None,
        service,
    )
    .await
    .unwrap();
    // "name:password", as basic sign-in carries it.
    let basic = |name: &str, password: &str| {
        const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = format!("{name}:{password}").into_bytes();
        let mut out = String::from("Basic ");
        for chunk in bytes.chunks(3) {
            let n = u32::from(chunk[0]) << 16
                | u32::from(*chunk.get(1).unwrap_or(&0)) << 8
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(TABLE[((n >> (18 - 6 * i)) & 63) as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    };
    let team_a = basic("team-a", "team-a-password-long-enough");
    let team_b = basic("team-b", "team-b-password-long-enough");
    let admin = basic("admin", "admin-password-long-enough");
    let order = Some(json!({ "schema": ORDER }));

    // Nobody in without saying who; a wrong password is nobody.
    assert_eq!(get(at, "/subjects").await.0, 401);
    assert_eq!(
        call(
            at,
            "GET",
            "/subjects",
            None,
            Some(&basic("team-a", "not-the-password"))
        )
        .await
        .0,
        401
    );
    // Team A registers under its own prefix, and not under team B's.
    assert_eq!(
        call(
            at,
            "POST",
            "/subjects/team-a.orders/versions",
            order.clone(),
            Some(&team_a)
        )
        .await,
        (200, json!({ "id": 1 }))
    );
    let (status, why) = call(
        at,
        "POST",
        "/subjects/team-b.orders/versions",
        order.clone(),
        Some(&team_a),
    )
    .await;
    assert_eq!((status, &why["error_code"]), (403, &json!(40301)), "{why}");
    assert_eq!(
        call(
            at,
            "POST",
            "/subjects/team-b.orders/versions",
            order.clone(),
            Some(&team_b)
        )
        .await
        .0,
        200
    );
    // Each sees what it may read: team B reads team A's too, and may not change it.
    assert_eq!(
        call(at, "GET", "/subjects", None, Some(&team_a)).await.1,
        json!(["team-a.orders"])
    );
    assert_eq!(
        call(at, "GET", "/subjects", None, Some(&team_b)).await.1,
        json!(["team-a.orders", "team-b.orders"])
    );
    assert_eq!(
        call(at, "DELETE", "/subjects/team-a.orders", None, Some(&team_b))
            .await
            .0,
        403
    );
    assert_eq!(
        call(at, "DELETE", "/subjects/team-b.orders", None, Some(&team_b))
            .await
            .0,
        200
    );
    // The registry's own level is the administrator's (and the token holder's) to change.
    let full = Some(json!({ "compatibility": "FULL" }));
    assert_eq!(
        call(at, "PUT", "/config", full.clone(), Some(&team_a)).await.0,
        403
    );
    assert_eq!(
        call(at, "PUT", "/config", full.clone(), Some(&admin)).await.0,
        200
    );
    assert_eq!(
        call(at, "GET", "/config", None, Some(&format!("Bearer {key}")))
            .await
            .1,
        json!({ "compatibilityLevel": "FULL" })
    );
    assert_eq!(
        call(
            at,
            "GET",
            "/subjects?deleted=true",
            None,
            Some(&format!("Bearer {key}"))
        )
        .await
        .1,
        json!(["team-a.orders", "team-b.orders"])
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_serves_the_requests_sent_one_after_another_on_it() {
    let broker = broker(Arc::new(MemoryStore::new())).await;
    let (at, _service) = node(&broker, None).await;
    let mut socket = tokio::net::TcpStream::connect(at).await.unwrap();
    // One answer off the connection: its head and exactly the body its length says. What came after it (the
    // next answer, when two were sent together) is kept in `carry` for the next call.
    async fn one(socket: &mut tokio::net::TcpStream, carry: &mut Vec<u8>) -> (String, Value) {
        let mut bytes = std::mem::take(carry);
        let end = loop {
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            let mut chunk = [0u8; 1024];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0, "the connection closed before an answer");
            bytes.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8(bytes[..end].to_vec()).unwrap();
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let mut body = bytes[end..].to_vec();
        while body.len() < length {
            let mut chunk = [0u8; 1024];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0, "the connection closed inside an answer");
            body.extend_from_slice(&chunk[..n]);
        }
        *carry = body.split_off(length);
        (head, serde_json::from_slice(&body).unwrap())
    }
    let mut carry = Vec::new();
    let body = json!({ "schema": ORDER }).to_string();
    let register = format!(
        "POST /subjects/orders-value/versions HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    // A request, its answer, then two more sent together in one write: each answered in turn.
    socket.write_all(register.as_bytes()).await.unwrap();
    let (head, answer) = one(&mut socket, &mut carry).await;
    assert!(
        head.starts_with("HTTP/1.1 200") && head.contains("Connection: keep-alive"),
        "{head}"
    );
    assert_eq!(answer, json!({ "id": 1 }));
    socket
        .write_all(b"GET /subjects HTTP/1.1\r\nHost: x\r\n\r\nGET /schemas/ids/1 HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(one(&mut socket, &mut carry).await.1, json!(["orders-value"]));
    assert_eq!(one(&mut socket, &mut carry).await.1, json!({ "schema": ORDER }));
    // Asked to close, it says so and does.
    socket
        .write_all(b"GET /mode HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let (head, _) = one(&mut socket, &mut carry).await;
    assert!(head.contains("Connection: close"), "{head}");
    let mut rest = Vec::new();
    assert_eq!(
        socket.read_to_end(&mut rest).await.unwrap(),
        0,
        "closed, with nothing more said"
    );
    // An old client that does not ask to keep the connection gets one answer and a close.
    let mut old = tokio::net::TcpStream::connect(at).await.unwrap();
    old.write_all(b"GET /subjects HTTP/1.0\r\n\r\n").await.unwrap();
    let mut all = Vec::new();
    old.read_to_end(&mut all).await.unwrap();
    assert!(String::from_utf8_lossy(&all).contains("Connection: close"));
    broker.shutdown().await;
}

impl Pki {
    /// A certificate and key a client presents as `name`.
    fn issue_client(&self, name: &str) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        let cert = params.signed_by(&key, &*self.ca).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

/// One request over TLS: the status and body, or `None` when the registry would not shake hands.
async fn over_tls(
    at: SocketAddr,
    trust: Arc<rustls::ClientConfig>,
    method: &str,
    target: &str,
    body: Option<Value>,
    authorization: Option<&str>,
) -> Option<(u16, Value)> {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let authorization = authorization
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {at}\r\nConnection: close\r\n{authorization}Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let socket = tokio::net::TcpStream::connect(at).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
    let mut secured = tokio_rustls::TlsConnector::from(trust)
        .connect(name, socket)
        .await
        .ok()?;
    secured.write_all(request.as_bytes()).await.ok()?;
    let mut answer = Vec::new();
    let _ = secured.read_to_end(&mut answer).await;
    let answer = String::from_utf8(answer).ok()?;
    let (head, body) = answer.split_once("\r\n\r\n")?;
    let status = head.split(' ').nth(1)?.parse().ok()?;
    Some((status, serde_json::from_str(body).unwrap_or(Value::Null)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_certificate_says_who_asks_and_the_rules_hold_it() {
    use openduct_broker::security::{
        Authorizer, ClientCerts, ClientTls, Operation, Pattern, Permission, Resource, Rule, Rules, Security,
    };

    let broker = broker(Arc::new(MemoryStore::new())).await;
    let pki = Pki::new();
    // The principal a certificate names may read and register under its team's subjects.
    let mut security = Security::open();
    security.rules = Some(Arc::new(
        Authorizer::fixed(Rules {
            super_users: Vec::new(),
            rules: vec![Rule {
                principal: "User:team-a.example".to_owned(),
                host: "*".to_owned(),
                resource: Resource::Subject,
                pattern: Pattern::Prefixed,
                name: "team-a.".to_owned(),
                operations: vec![Operation::Read, Operation::Write],
                permission: Permission::Allow,
            }],
            quotas: std::collections::BTreeMap::new(),
        })
        .unwrap(),
    ));
    let shared = Arc::new(Shared::new(
        &broker.local_addr().to_string(),
        Duration::from_secs(10),
        &ClientSecurity::default(),
    ));
    let service = Service::start(shared).await.unwrap();
    // The registry's TLS: its certificate, and the authority whose certificates name its clients - one may be
    // presented, none has to be.
    let (cert, key) = pki.issue();
    let serving = ClientTls::from_pem(
        cert.as_bytes(),
        key.as_bytes(),
        Some(pki.ca.pem().as_bytes()),
        ClientCerts::Optional,
    )
    .unwrap()
    .acceptor();
    let token = "an-everything-token-0123456789";
    let (at, _task) = http::start(
        "127.0.0.1:0".parse().unwrap(),
        http::SignIn {
            token: Some(Arc::from(token)),
            users: Some(security),
            certificates: true,
        },
        false,
        Some(serving),
        service,
    )
    .await
    .unwrap();

    let ca = pki.ca.pem();
    let with = |identity: Option<(String, String)>| {
        ClientSecurity::tls_from_pem(
            ca.as_bytes(),
            identity.as_ref().map(|(c, k)| (c.as_bytes(), k.as_bytes())),
        )
        .unwrap()
    };
    let team_a = with(Some(pki.issue_client("team-a.example")));
    let stranger = with(Some(pki.issue_client("someone-else.example")));
    let nobody = with(None);
    let order = Some(json!({ "schema": ORDER }));

    // Team A's certificate is who it is: its own subjects, and only those.
    assert_eq!(
        over_tls(
            at,
            Arc::clone(&team_a),
            "POST",
            "/subjects/team-a.orders/versions",
            order.clone(),
            None
        )
        .await,
        Some((200, json!({ "id": 1 })))
    );
    let refused = over_tls(
        at,
        Arc::clone(&team_a),
        "POST",
        "/subjects/team-b.orders/versions",
        order.clone(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        (refused.0, &refused.1["error_code"]),
        (403, &json!(40301)),
        "{refused:?}"
    );
    assert_eq!(
        over_tls(at, Arc::clone(&team_a), "GET", "/subjects", None, None).await,
        Some((200, json!(["team-a.orders"])))
    );
    // A certificate the authority signed for someone the rules do not name sees nothing and changes nothing.
    assert_eq!(
        over_tls(at, Arc::clone(&stranger), "GET", "/subjects", None, None).await,
        Some((200, json!([])))
    );
    assert_eq!(
        over_tls(
            at,
            Arc::clone(&stranger),
            "GET",
            "/subjects/team-a.orders/versions",
            None,
            None
        )
        .await
        .unwrap()
        .0,
        403
    );
    // No certificate: nobody, unless it says who it is another way - here, the token.
    assert_eq!(
        over_tls(at, Arc::clone(&nobody), "GET", "/subjects", None, None)
            .await
            .unwrap()
            .0,
        401
    );
    assert_eq!(
        over_tls(
            at,
            Arc::clone(&nobody),
            "GET",
            "/subjects",
            None,
            Some(&format!("Bearer {token}"))
        )
        .await,
        Some((200, json!(["team-a.orders"])))
    );
    // A certificate from an authority the registry does not know: no request is served on it.
    let other = Pki::new();
    let forged = ClientSecurity::tls_from_pem(
        ca.as_bytes(),
        Some({
            let (c, k) = other.issue_client("team-a.example");
            (
                Box::leak(c.into_boxed_str()).as_bytes(),
                Box::leak(k.into_boxed_str()).as_bytes(),
            )
        }),
    )
    .unwrap();
    let served = over_tls(at, forged, "GET", "/subjects", None, None).await;
    assert_eq!(
        served, None,
        "nothing is answered to a certificate the authority did not sign"
    );
    broker.shutdown().await;
}
