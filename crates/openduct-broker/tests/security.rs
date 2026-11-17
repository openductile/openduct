// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The client port under zero trust, tried the way an attacker would: plaintext against TLS, no sign-in, a wrong
//! password, a forged or expired token, another tenant's topics, a listing that would leak names, a certificate
//! principal reaching beyond its rules - and a broker that would listen openly without being told to.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use openduct_broker::security::scram::{DEFAULT_ITERATIONS, ScramCredential, ScramMechanism};
use openduct_broker::security::{
    Authorizer, ClientCerts, ClientTls, Credentials, JwtSettings, Operation, Pattern, Permission, Resource,
    Rule, Rules, Security, Users,
};
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{MemoryStore, Store};
use ring::signature::{Ed25519KeyPair, KeyPair};
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{
    Client, ClientBuilder, Credentials as RsCredentials, OauthBearerCredentials, SaslConfig,
};
use rskafka::record::Record;

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

    /// A certificate and key (PEM) with these names; `uri` adds a URI name (a workload identity).
    fn issue(&self, names: &[&str], uri: Option<&str>) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params =
            rcgen::CertificateParams::new(names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>()).unwrap();
        if let Some(uri) = uri {
            params
                .subject_alt_names
                .push(rcgen::SanType::URI(uri.try_into().unwrap()));
        }
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &*self.ca).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn client_config(&self, identity: Option<(String, String)>) -> Arc<rustls::ClientConfig> {
        use rustls::pki_types::pem::PemObject;
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from_pem_slice(self.ca.pem().as_bytes()).unwrap())
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots);
        Arc::new(match identity {
            None => builder.with_no_client_auth(),
            Some((cert, key)) => builder
                .with_client_auth_cert(
                    vec![rustls::pki_types::CertificateDer::from_pem_slice(cert.as_bytes()).unwrap()],
                    rustls::pki_types::PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
                )
                .unwrap(),
        })
    }
}

const TENANT_PASSWORD: &str = "tenant-one-password-long-enough";
const ADMIN_PASSWORD: &str = "admin-password-long-enough-too";

fn users() -> Users {
    let mut users = BTreeMap::new();
    for (name, pass) in [("tenant1", TENANT_PASSWORD), ("admin", ADMIN_PASSWORD)] {
        let mut creds = BTreeMap::new();
        for m in ScramMechanism::ALL {
            creds.insert(m, ScramCredential::new(m, pass, DEFAULT_ITERATIONS).unwrap());
        }
        users.insert(name.to_owned(), creds);
    }
    Users { format: 1, users }
}

fn rule(principal: &str, resource: Resource, pattern: Pattern, name: &str, ops: &[Operation]) -> Rule {
    Rule {
        principal: principal.to_owned(),
        host: "*".to_owned(),
        resource,
        pattern,
        name: name.to_owned(),
        operations: ops.to_vec(),
        permission: Permission::Allow,
    }
}

fn rules() -> Rules {
    let mut quotas = BTreeMap::new();
    quotas.insert(
        "User:tenant1".to_owned(),
        openduct_broker::security::quota::Quota {
            produce_bytes_per_sec: Some(20_000),
            fetch_bytes_per_sec: None,
        },
    );
    Rules {
        quotas,
        super_users: vec!["User:admin".to_owned()],
        rules: vec![
            rule(
                "User:tenant1",
                Resource::Topic,
                Pattern::Prefixed,
                "t1.",
                &[Operation::All],
            ),
            rule(
                "User:tenant1",
                Resource::Group,
                Pattern::Prefixed,
                "t1.",
                &[Operation::Read],
            ),
            rule(
                "User:spiffe://test/hub",
                Resource::Topic,
                Pattern::Literal,
                "pod-events",
                &[Operation::Write],
            ),
            rule(
                "User:worker",
                Resource::Topic,
                Pattern::Literal,
                "pod-events",
                &[Operation::Read],
            ),
        ],
    }
}

struct Idp {
    key: Ed25519KeyPair,
    _dir: tempfile::TempDir,
    jwks: std::path::PathBuf,
}

impl Idp {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let jwks = serde_json::json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "k1",
            "x": B64URL.encode(key.public_key().as_ref())}]});
        let path = dir.path().join("jwks.json");
        std::fs::write(&path, jwks.to_string()).unwrap();
        Self {
            key,
            _dir: dir,
            jwks: path,
        }
    }

    fn token(&self, sub: &str, lifetime: i64) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let header = B64URL.encode(r#"{"alg":"EdDSA","kid":"k1"}"#);
        let body = B64URL.encode(
            serde_json::json!({"iss": "https://id.test", "aud": "openduct", "sub": sub, "exp": now + lifetime}).to_string(),
        );
        let signed = format!("{header}.{body}");
        format!("{signed}.{}", B64URL.encode(self.key.sign(signed.as_bytes())))
    }
}

async fn secured(pki: &Pki, idp: &Idp) -> (Broker, String) {
    let (cert, key) = pki.issue(&["127.0.0.1"], None);
    let mut security = Security::open()
        .with_tokens(JwtSettings {
            issuer: "https://id.test".to_owned(),
            audience: "openduct".to_owned(),
            jwks_file: idp.jwks.clone(),
            principal_claim: "sub".to_owned(),
            leeway: Duration::from_secs(5),
        })
        .unwrap();
    security.tls = Some(
        ClientTls::from_pem(
            cert.as_bytes(),
            key.as_bytes(),
            Some(pki.ca.pem().as_bytes()),
            ClientCerts::Optional,
        )
        .unwrap(),
    );
    security.credentials = Some(Arc::new(Credentials::fixed(users())));
    security.plain = true;
    security.rules = Some(Arc::new(Authorizer::fixed(rules()).unwrap()));
    security.require_authentication = true;
    security.max_session = Some(Duration::from_secs(3600));
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.group_initial_rebalance_delay = Duration::ZERO;
    config.security = security;
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
    let broker = Broker::start(config, store).await.unwrap();
    let addr = broker.local_addr().to_string();
    (broker, addr)
}

async fn connect(
    addr: &str,
    tls: Arc<rustls::ClientConfig>,
    sasl: Option<SaslConfig>,
) -> Result<Client, String> {
    let mut b = ClientBuilder::new(vec![addr.to_owned()]).tls_config(tls);
    if let Some(s) = sasl {
        b = b.sasl_config(s);
    }
    tokio::time::timeout(Duration::from_secs(10), b.build())
        .await
        .map_err(|_| "timed out".to_owned())?
        .map_err(|e| e.to_string())
}

fn scram(user: &str, pass: &str) -> SaslConfig {
    SaslConfig::ScramSha512(RsCredentials::new(user.to_owned(), pass.to_owned()))
}

fn record() -> Record {
    Record {
        key: None,
        value: Some(b"v".to_vec()),
        headers: Default::default(),
        timestamp: rskafka::chrono::Utc::now(),
    }
}

async fn produce(client: &Client, topic: &str) -> Result<(), String> {
    let pc = client
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await
        .map_err(|e| e.to_string())?;
    pc.produce(vec![record()], Compression::NoCompression)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn signed_in_tenants_reach_only_their_own_topics() {
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let tls = pki.client_config(None);

    let admin = connect(&addr, Arc::clone(&tls), Some(scram("admin", ADMIN_PASSWORD)))
        .await
        .unwrap();
    admin
        .controller_client()
        .unwrap()
        .create_topic("t2.secret", 1, 1, 5_000)
        .await
        .unwrap();
    admin
        .controller_client()
        .unwrap()
        .create_topic("pod-events", 1, 1, 5_000)
        .await
        .unwrap();

    let tenant = connect(&addr, Arc::clone(&tls), Some(scram("tenant1", TENANT_PASSWORD)))
        .await
        .unwrap();
    tenant
        .controller_client()
        .unwrap()
        .create_topic("t1.orders", 1, 1, 5_000)
        .await
        .unwrap();
    produce(&tenant, "t1.orders").await.unwrap();
    let e = tenant
        .controller_client()
        .unwrap()
        .create_topic("t2.orders", 1, 1, 5_000)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("TopicAuthorizationFailed"), "{e}");
    let seen: Vec<String> = tenant
        .list_topics()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(seen.contains(&"t1.orders".to_owned()));
    assert!(
        !seen.iter().any(|t| t == "t2.secret" || t == "pod-events"),
        "a listing leaked names: {seen:?}"
    );
    assert!(
        produce(&tenant, "t2.secret").await.is_err(),
        "wrote to another tenant's topic"
    );

    // PLAIN over TLS checks the same credentials.
    let plain = SaslConfig::Plain(RsCredentials::new(
        "tenant1".to_owned(),
        TENANT_PASSWORD.to_owned(),
    ));
    let via_plain = connect(&addr, Arc::clone(&tls), Some(plain)).await.unwrap();
    produce(&via_plain, "t1.orders").await.unwrap();
    broker.shutdown().await;
}

#[tokio::test]
async fn no_sign_in_a_wrong_password_or_plaintext_gets_nothing() {
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let tls = pki.client_config(None);
    for wrong in [
        scram("tenant1", "not-the-password-at-all"),
        scram("nobody", TENANT_PASSWORD),
    ] {
        let e = connect(&addr, Arc::clone(&tls), Some(wrong))
            .await
            .err()
            .unwrap_or_default();
        assert!(!e.is_empty(), "signed in with a wrong name or password");
    }
    // Connected over TLS without signing in: the first real request closes the connection.
    let anonymous = connect(&addr, Arc::clone(&tls), None).await;
    if let Ok(c) = anonymous {
        assert!(c.list_topics().await.is_err(), "served a request before sign-in");
    }
    // Plaintext against the TLS port.
    let plaintext = tokio::time::timeout(
        Duration::from_secs(5),
        ClientBuilder::new(vec![addr.clone()]).build(),
    )
    .await;
    assert!(!matches!(plaintext, Ok(Ok(_))), "a plaintext client got in");
    broker.shutdown().await;
}

#[tokio::test]
async fn tokens_and_certificates_name_principals_held_to_their_rules() {
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let admin = connect(
        &addr,
        pki.client_config(None),
        Some(scram("admin", ADMIN_PASSWORD)),
    )
    .await
    .unwrap();
    admin
        .controller_client()
        .unwrap()
        .create_topic("pod-events", 1, 1, 5_000)
        .await
        .unwrap();

    // A workload certificate: may write pod-events, nothing else.
    let hub_cert = pki.issue(&["hub.internal"], Some("spiffe://test/hub"));
    let hub = connect(&addr, pki.client_config(Some(hub_cert)), None)
        .await
        .unwrap();
    produce(&hub, "pod-events").await.unwrap();
    let pc = hub
        .partition_client("pod-events", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    assert!(
        pc.fetch_records(0, 1..100_000, 100).await.is_err(),
        "the hub read what it may only write"
    );

    // A token for `worker`: may read pod-events.
    let good = idp.token("worker", 300);
    let cb: rskafka::client::OauthCallback = Arc::new(move || {
        let t = good.clone();
        Box::pin(async move { Ok(t) })
    });
    let sasl = SaslConfig::Oauthbearer(OauthBearerCredentials {
        callback: cb,
        authz_id: None,
        bearer_kvs: vec![],
    });
    let worker = connect(&addr, pki.client_config(None), Some(sasl)).await.unwrap();
    let pc = worker
        .partition_client("pod-events", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let (records, _) = pc.fetch_records(0, 1..100_000, 1000).await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        produce(&worker, "pod-events").await.is_err(),
        "the worker wrote what it may only read"
    );

    // An expired token, and one from another provider, are refused.
    let other = Idp::new();
    for bad in [idp.token("worker", -120), other.token("worker", 300)] {
        let cb: rskafka::client::OauthCallback = Arc::new(move || {
            let t = bad.clone();
            Box::pin(async move { Ok(t) })
        });
        let sasl = SaslConfig::Oauthbearer(OauthBearerCredentials {
            callback: cb,
            authz_id: None,
            bearer_kvs: vec![],
        });
        assert!(
            connect(&addr, pki.client_config(None), Some(sasl)).await.is_err(),
            "a bad token signed in"
        );
    }
    broker.shutdown().await;
}

#[tokio::test]
async fn a_broker_will_not_listen_openly_unless_told_to() {
    let mut config = BrokerConfig::new("0.0.0.0:0".parse().unwrap());
    config.advertised_host = "127.0.0.1".to_owned();
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
    let e = Broker::start(config.clone(), Arc::clone(&store))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("TLS") && e.contains("sign-in") && e.contains("access rules"),
        "{e}"
    );
    config.security.allow_insecure = true;
    let broker = Broker::start(config, store).await.unwrap();
    broker.shutdown().await;
}

#[tokio::test]
async fn the_built_in_client_signs_in_over_tls_and_sees_only_what_it_may() {
    use openduct_broker::client::{Client, ClientSecurity, SignIn};
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let tls = ClientSecurity::tls_from_pem(pki.ca.pem().as_bytes(), None).unwrap();
    let as_user = |user: &str, password: &str| ClientSecurity {
        tls: Some(Arc::clone(&tls)),
        server_name: None,
        sign_in: Some(SignIn::Scram {
            mechanism: ScramMechanism::Sha256,
            user: user.to_owned(),
            password: password.to_owned(),
        }),
    };
    let mut admin = Client::connect_with(&addr, Duration::from_secs(5), &as_user("admin", ADMIN_PASSWORD))
        .await
        .unwrap();
    admin.create_topic("t2.private", 1, None).await.unwrap();
    admin.create_topic("t1.mine", 1, None).await.unwrap();
    let mut tenant = Client::connect_with(
        &addr,
        Duration::from_secs(5),
        &as_user("tenant1", TENANT_PASSWORD),
    )
    .await
    .unwrap();
    let names: Vec<String> = tenant
        .topics()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert_eq!(names, vec!["t1.mine".to_owned()]);
    let wrong = Client::connect_with(
        &addr,
        Duration::from_secs(5),
        &as_user("tenant1", "wrong-password-entirely"),
    )
    .await;
    assert!(wrong.is_err());
    // TLS without signing in: ApiVersions is answered, nothing else.
    let only_tls = ClientSecurity {
        tls: Some(Arc::clone(&tls)),
        server_name: None,
        sign_in: None,
    };
    let mut c = Client::connect_with(&addr, Duration::from_secs(5), &only_tls)
        .await
        .unwrap();
    c.api_versions().await.unwrap();
    assert!(c.topics().await.is_err());
    broker.shutdown().await;
}

#[tokio::test]
async fn a_tenant_over_its_quota_is_slowed_and_no_one_else_is() {
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let tls = pki.client_config(None);
    let admin = connect(&addr, Arc::clone(&tls), Some(scram("admin", ADMIN_PASSWORD)))
        .await
        .unwrap();
    admin
        .controller_client()
        .unwrap()
        .create_topic("t1.flood", 1, 1, 5_000)
        .await
        .unwrap();
    admin
        .controller_client()
        .unwrap()
        .create_topic("other", 1, 1, 5_000)
        .await
        .unwrap();
    let tenant = connect(&addr, Arc::clone(&tls), Some(scram("tenant1", TENANT_PASSWORD)))
        .await
        .unwrap();
    let big = |n: usize| Record {
        key: None,
        value: Some(vec![b'x'; n]),
        headers: Default::default(),
        timestamp: rskafka::chrono::Utc::now(),
    };
    // 80 KB at 20 KB/s, with one second's burst allowed: held back about three seconds.
    let pc = tenant
        .partition_client("t1.flood", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    for i in 0..4 {
        let write = pc.produce(vec![big(20_000)], Compression::NoCompression);
        let done = tokio::time::timeout(Duration::from_secs(20), write)
            .await
            .unwrap_or_else(|_| panic!("write {i} got no answer in 20 s"));
        done.unwrap();
    }
    let slowed = started.elapsed();
    assert!(
        slowed >= Duration::from_millis(2500),
        "a tenant over its quota went at {slowed:?}"
    );
    // The admin, without a quota, is not held back meanwhile.
    let other = admin
        .partition_client("other", 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    for _ in 0..4 {
        other
            .produce(vec![big(20_000)], Compression::NoCompression)
            .await
            .unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "another principal was slowed"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn a_principal_lists_and_describes_only_the_groups_it_may() {
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{
        DescribeGroupsRequest, GroupId, ListGroupsRequest, OffsetCommitRequest, TopicName,
    };
    use kafka_protocol::protocol::StrBytes;
    use openduct_broker::client::{Client, ClientSecurity, SignIn};
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let tls = ClientSecurity::tls_from_pem(pki.ca.pem().as_bytes(), None).unwrap();
    let as_user = |user: &str, password: &str| ClientSecurity {
        tls: Some(Arc::clone(&tls)),
        server_name: None,
        sign_in: Some(SignIn::Scram {
            mechanism: ScramMechanism::Sha256,
            user: user.to_owned(),
            password: password.to_owned(),
        }),
    };
    let text = |t: &str| StrBytes::from_string(t.to_owned());
    let mut admin = Client::connect_with(&addr, Duration::from_secs(5), &as_user("admin", ADMIN_PASSWORD))
        .await
        .unwrap();
    admin.create_topic("t1.orders", 1, None).await.unwrap();
    for group in ["t1.billing", "t2.payroll"] {
        let commit = OffsetCommitRequest::default()
            .with_group_id(GroupId(text(group)))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(text(""))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(text("t1.orders")))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default().with_committed_offset(1),
                    ]),
            ]);
        assert_eq!(
            admin.send(&commit, 8).await.unwrap().topics[0].partitions[0].error_code,
            0
        );
    }
    let all: Vec<String> = admin
        .send(&ListGroupsRequest::default(), 5)
        .await
        .unwrap()
        .groups
        .iter()
        .map(|g| g.group_id.0.to_string())
        .collect();
    assert_eq!(
        all,
        vec!["t1.billing", "t2.payroll"],
        "a super user sees every group"
    );

    let mut tenant = Client::connect_with(
        &addr,
        Duration::from_secs(5),
        &as_user("tenant1", TENANT_PASSWORD),
    )
    .await
    .unwrap();
    let seen: Vec<String> = tenant
        .send(&ListGroupsRequest::default(), 5)
        .await
        .unwrap()
        .groups
        .iter()
        .map(|g| g.group_id.0.to_string())
        .collect();
    assert_eq!(
        seen,
        vec!["t1.billing"],
        "another tenant's group is not even named"
    );
    let described = tenant
        .send(
            &DescribeGroupsRequest::default()
                .with_groups(vec![GroupId(text("t1.billing")), GroupId(text("t2.payroll"))])
                .with_include_authorized_operations(true),
            5,
        )
        .await
        .unwrap();
    assert_eq!(described.groups[0].error_code, 0);
    assert_eq!(
        described.groups[0].authorized_operations,
        (1 << 3) | (1 << 8),
        "Read on its groups, and the Describe it implies"
    );
    assert_eq!(described.groups[1].error_code, 30, "GROUP_AUTHORIZATION_FAILED");
    assert!(described.groups[1].group_state.is_empty());
    // Deleting needs Delete on the group: the tenant may only read its own.
    let deleted = tenant
        .send(
            &kafka_protocol::messages::DeleteGroupsRequest::default()
                .with_groups_names(vec![GroupId(text("t1.billing")), GroupId(text("t2.payroll"))]),
            2,
        )
        .await
        .unwrap();
    let codes: Vec<i16> = deleted.results.iter().map(|r| r.error_code).collect();
    assert_eq!(codes, vec![30, 30]);
    let mut seen_by_admin = admin.send(&ListGroupsRequest::default(), 5).await.unwrap().groups;
    seen_by_admin.retain(|g| g.group_id.0.as_str() == "t2.payroll");
    assert_eq!(seen_by_admin.len(), 1, "nothing was deleted");
    broker.shutdown().await;
}

#[tokio::test]
async fn settings_are_described_only_for_what_a_principal_may_see() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::{DescribeClusterRequest, DescribeConfigsRequest};
    use kafka_protocol::protocol::StrBytes;
    use openduct_broker::client::{Client, ClientSecurity, SignIn};
    let (pki, idp) = (Pki::new(), Idp::new());
    let (broker, addr) = secured(&pki, &idp).await;
    let tls = ClientSecurity::tls_from_pem(pki.ca.pem().as_bytes(), None).unwrap();
    let as_user = |user: &str, password: &str| ClientSecurity {
        tls: Some(Arc::clone(&tls)),
        server_name: None,
        sign_in: Some(SignIn::Scram {
            mechanism: ScramMechanism::Sha256,
            user: user.to_owned(),
            password: password.to_owned(),
        }),
    };
    let mut admin = Client::connect_with(&addr, Duration::from_secs(5), &as_user("admin", ADMIN_PASSWORD))
        .await
        .unwrap();
    admin.create_topic("t1.orders", 1, None).await.unwrap();
    admin.create_topic("t2.secret", 1, None).await.unwrap();
    let topic = |name: &str| {
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_string(name.to_owned()))
    };
    let broker_resource = DescribeConfigsResource::default()
        .with_resource_type(4)
        .with_resource_name(StrBytes::from_static_str("0"));
    let mut tenant = Client::connect_with(
        &addr,
        Duration::from_secs(5),
        &as_user("tenant1", TENANT_PASSWORD),
    )
    .await
    .unwrap();
    let res = tenant
        .send(
            &DescribeConfigsRequest::default().with_resources(vec![
                topic("t1.orders"),
                topic("t2.secret"),
                topic("t2.no-such-topic"),
                broker_resource.clone(),
            ]),
            4,
        )
        .await
        .unwrap();
    let codes: Vec<i16> = res.results.iter().map(|r| r.error_code).collect();
    // Another tenant's topic and one that does not exist are refused alike: the answer tells nothing of which.
    assert_eq!(codes, vec![0, 29, 29, 31]);
    assert!(res.results[1].configs.is_empty() && res.results[3].configs.is_empty());
    let admin_view = admin
        .send(
            &DescribeConfigsRequest::default().with_resources(vec![broker_resource]),
            4,
        )
        .await
        .unwrap();
    assert_eq!(admin_view.results[0].error_code, 0, "a super user may");
    // Changing a topic's retention needs AlterConfigs on it: the tenant may on its own, not on another's.
    let alter = |name: &str| {
        use kafka_protocol::messages::incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig,
        };
        kafka_protocol::messages::IncrementalAlterConfigsRequest::default().with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_string(name.to_owned()))
                .with_configs(vec![
                    AlterableConfig::default()
                        .with_name(StrBytes::from_static_str("retention.ms"))
                        .with_value(Some(StrBytes::from_static_str("60000"))),
                ]),
        ])
    };
    let own = tenant.send(&alter("t1.orders"), 1).await.unwrap();
    let other = tenant.send(&alter("t2.secret"), 1).await.unwrap();
    assert_eq!(
        (own.responses[0].error_code, other.responses[0].error_code),
        (0, 29)
    );
    // Adding partitions needs Alter on the topic.
    let grow = |name: &str| {
        use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
        kafka_protocol::messages::CreatePartitionsRequest::default().with_topics(vec![
            CreatePartitionsTopic::default()
                .with_name(kafka_protocol::messages::TopicName(StrBytes::from_string(
                    name.to_owned(),
                )))
                .with_count(2),
        ])
    };
    let own = tenant.send(&grow("t1.orders"), 3).await.unwrap();
    let other = tenant.send(&grow("t2.secret"), 3).await.unwrap();
    assert_eq!((own.results[0].error_code, other.results[0].error_code), (0, 29));
    // Deleting records needs Delete on the topic.
    let delete = |name: &str| {
        use kafka_protocol::messages::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
        kafka_protocol::messages::DeleteRecordsRequest::default().with_topics(vec![
            DeleteRecordsTopic::default()
                .with_name(kafka_protocol::messages::TopicName(StrBytes::from_string(
                    name.to_owned(),
                )))
                .with_partitions(vec![DeleteRecordsPartition::default().with_offset(-1)]),
        ])
    };
    let own = tenant.send(&delete("t1.orders"), 2).await.unwrap();
    let other = tenant.send(&delete("t2.secret"), 2).await.unwrap();
    assert_eq!(
        (
            own.topics[0].partitions[0].error_code,
            other.topics[0].partitions[0].error_code
        ),
        (0, 29)
    );
    let cluster = tenant
        .send(
            &DescribeClusterRequest::default()
                .with_endpoint_type(1)
                .with_include_cluster_authorized_operations(true),
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        (cluster.error_code, cluster.cluster_authorized_operations),
        (0, 0),
        "nothing on the cluster"
    );
    assert_eq!(
        cluster.brokers.len(),
        1,
        "the brokers are no secret: Metadata names them too"
    );
    broker.shutdown().await;
}
