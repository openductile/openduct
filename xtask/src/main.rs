// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The project's tasks, run with `cargo xtask <task>`:
//!
//! ```text
//! cargo xtask check    format, lint, tests, licences and advisories - everything CI runs
//! cargo xtask build    the release build of the `openduct` binary: target/release/openduct
//! cargo xtask test     tests only
//! cargo xtask services up | down | env    the systems the connector tests run against, in containers
//! ```

use std::process::{Command, ExitCode};

/// One step: a program and its arguments.
type Step = (&'static str, &'static [&'static str]);

const FORMAT: Step = ("cargo", &["fmt", "--all", "--check"]);
const LINT: Step = (
    "cargo",
    &[
        "clippy",
        "--workspace",
        "--all-targets",
        "--locked",
        "--",
        "-D",
        "warnings",
    ],
);
const TEST: Step = ("cargo", &["test", "--workspace", "--locked"]);
const LICENCES: Step = ("cargo", &["deny", "--locked", "check"]);
const BUILD: Step = ("cargo", &["build", "--release", "--locked", "-p", "openduct"]);

/// The containers the connector tests use: name, image, port on 127.0.0.1, port inside, and the rest of `docker run`.
const SERVICES: &[(&str, &str, u16, u16, &[&str])] = &[
    (
        "openduct-test-minio",
        "minio/minio:latest",
        19900,
        9000,
        &[
            "-e",
            "MINIO_ROOT_USER=openduct",
            "-e",
            "MINIO_ROOT_PASSWORD={secret}",
            "minio/minio:latest",
            "server",
            "/data",
        ],
    ),
    (
        "openduct-test-elasticsearch",
        "elasticsearch:8.17.0",
        19920,
        9200,
        &[
            "--memory",
            "1g",
            "-e",
            "discovery.type=single-node",
            "-e",
            "xpack.security.enabled=false",
            "elasticsearch:8.17.0",
        ],
    ),
    (
        "openduct-test-mosquitto",
        "eclipse-mosquitto:2",
        19883,
        1883,
        &[
            "eclipse-mosquitto:2",
            "mosquitto",
            "-c",
            "/mosquitto-no-auth.conf",
        ],
    ),
    (
        "openduct-test-kafka",
        "apache/kafka:4.4.0",
        19092,
        19092,
        &[
            "--memory",
            "1536m",
            "-p",
            "127.0.0.1:19093:19093",
            "-v",
            KAFKA_CONFIG_MOUNT,
            "apache/kafka:4.4.0",
        ],
    ),
    (
        "openduct-test-postgres",
        // PostgreSQL 18 with pgvector, for the postgres step's similarity searches; the same server otherwise.
        "pgvector/pgvector:0.8.6-pg18",
        19432,
        5432,
        &[
            "--memory",
            "512m",
            "-e",
            "POSTGRES_USER=openduct",
            "-e",
            "POSTGRES_PASSWORD={secret}",
            "-e",
            "POSTGRES_DB=shop",
            // md5 lets a user whose password is stored as MD5 sign in that way; SCRAM users still use SCRAM.
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=md5",
            "pgvector/pgvector:0.8.6-pg18",
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=20",
            "-c",
            "max_wal_senders=20",
        ],
    ),
    (
        "openduct-test-mysql",
        "mysql:8.4.11",
        19306,
        3306,
        &[
            "--memory",
            "768m",
            "-e",
            "MYSQL_ROOT_PASSWORD={secret}",
            "-e",
            "MYSQL_DATABASE=shop",
            "mysql:8.4.11",
            "--binlog-row-metadata=FULL",
            "--mysql-native-password=ON",
        ],
    ),
    (
        "openduct-test-mongodb",
        "mongo:8.0.34",
        19017,
        19017,
        &[
            "--memory",
            "768m",
            "--entrypoint",
            "bash",
            "mongo:8.0.34",
            "-c",
            MONGODB_START,
        ],
    ),
];

/// A one-member replica set (change streams need one) with sign-in on: a fresh key for its members, the set
/// started, and the first user made through MongoDB's localhost exception.
const MONGODB_START: &str = "\
set -e
head -c 756 /dev/urandom | base64 > /tmp/keyfile
chmod 400 /tmp/keyfile
mongod --replSet rs0 --port 19017 --bind_ip_all --keyFile /tmp/keyfile --dbpath /data/db --quiet &
until mongosh --port 19017 --quiet --eval 'db.adminCommand({ping: 1})' >/dev/null 2>&1; do sleep 1; done
mongosh --port 19017 --quiet --eval 'rs.initiate({_id: \"rs0\", members: [{_id: 0, host: \"127.0.0.1:19017\"}]})'
until mongosh --port 19017 --quiet --eval 'db.hello().isWritablePrimary' | grep -q true; do sleep 1; done
mongosh --port 19017 --quiet --eval 'db.getSiblingDB(\"admin\").createUser({user: \"openduct\", pwd: \"{secret}\", roles: [\"root\"]})'
wait
";

/// Stands for the folder holding [`KAFKA_SETTINGS`], mounted where the Kafka image reads its settings.
const KAFKA_CONFIG_MOUNT: &str = "{kafka-config}:/mnt/shared/config";

/// The Kafka test cluster: one node, plain on 19092, and SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) on 19093. The
/// SASL listener gives its address as 127.0.0.1:19094, where the tests put a TLS stand-in in front of it, so clients
/// meet SASL over TLS as they do on a hosted cluster. The login module lines are Kafka's own settings for SASL.
const KAFKA_SETTINGS: &str = "\
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:19092,SASL_PLAINTEXT://:19093,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://127.0.0.1:19092,SASL_PLAINTEXT://127.0.0.1:19094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
required user_openduct-bridge=\"{secret}\";
listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule \
required;
listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule \
required;
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.dirs=/tmp/kraft-combined-logs
";

/// Where [`KAFKA_SETTINGS`] is written for the container to read: the temporary folder, which Docker may read on
/// every system (on macOS it may not read the Documents folder).
fn kafka_config(secret: &str) -> std::io::Result<String> {
    let dir = std::env::temp_dir().join("openduct-test-kafka");
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        dir.join("server.properties"),
        KAFKA_SETTINGS.replace(SECRET, secret),
    )?;
    Ok(dir.canonicalize()?.display().to_string())
}

/// Stands for the test services' password in their settings: [`secret`] in its place as they start.
const SECRET: &str = "{secret}";

/// The test services' password: made at random on first use and kept, its owner's alone, in
/// `target/test-services.secret`, so services left running from an earlier `up` still match; `down` forgets it.
fn secret_file() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/test-services.secret")
}

fn secret() -> std::io::Result<String> {
    use std::hash::{BuildHasher, Hasher};
    let path = secret_file();
    if let Ok(kept) = std::fs::read_to_string(&path)
        && !kept.trim().is_empty()
    {
        return Ok(kept.trim().to_owned());
    }
    // Keys the system's randomness gave this process: no two runs make the same.
    let keys = std::collections::hash_map::RandomState::new();
    let made: String = (0..3u64)
        .map(|i| {
            let mut h = keys.build_hasher();
            h.write_u64(i);
            format!("{:016x}", h.finish())
        })
        .collect();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    std::io::Write::write_all(&mut options.open(&path)?, made.as_bytes())?;
    Ok(made)
}

/// What the tests read to find the services.
const SERVICE_ENV: &[(&str, &str)] = &[
    ("OPENDUCT_TEST_S3", "http://127.0.0.1:19900"),
    ("OPENDUCT_TEST_S3_ACCESS", "openduct"),
    ("OPENDUCT_TEST_S3_SECRET", "{secret}"),
    ("OPENDUCT_TEST_ELASTICSEARCH", "http://127.0.0.1:19920"),
    ("OPENDUCT_TEST_MQTT", "mqtt://127.0.0.1:19883"),
    ("OPENDUCT_TEST_KAFKA", "127.0.0.1:19092"),
    ("OPENDUCT_TEST_KAFKA_SASL", "127.0.0.1:19093"),
    ("OPENDUCT_TEST_KAFKA_USER", "openduct-bridge"),
    ("OPENDUCT_TEST_KAFKA_PASSWORD", "{secret}"),
    ("OPENDUCT_TEST_POSTGRES", "postgres://127.0.0.1:19432/shop"),
    ("OPENDUCT_TEST_POSTGRES_USER", "openduct"),
    ("OPENDUCT_TEST_POSTGRES_PASSWORD", "{secret}"),
    ("OPENDUCT_TEST_MYSQL", "mysql://127.0.0.1:19306"),
    ("OPENDUCT_TEST_MYSQL_ROOT_PASSWORD", "{secret}"),
    (
        "OPENDUCT_TEST_MONGODB",
        "mongodb://127.0.0.1:19017/?replicaSet=rs0",
    ),
    ("OPENDUCT_TEST_MONGODB_USER", "openduct"),
    ("OPENDUCT_TEST_MONGODB_PASSWORD", "{secret}"),
];

fn services(action: &str) -> ExitCode {
    match action {
        "up" => {
            let secret = match secret() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("xtask: cannot keep the test services' password: {e}");
                    return ExitCode::FAILURE;
                }
            };
            for (name, _, port, inside, rest) in SERVICES {
                let running = Command::new("docker")
                    .args(["ps", "-q", "--filter", &format!("name=^{name}$")])
                    .output()
                    .map(|o| !o.stdout.is_empty())
                    .unwrap_or(false);
                if running {
                    eprintln!("== {name} is running");
                    continue;
                }
                let _ = Command::new("docker").args(["rm", "-f", name]).output();
                let publish = format!("127.0.0.1:{port}:{inside}");
                let mount = if rest.contains(&KAFKA_CONFIG_MOUNT) {
                    match kafka_config(&secret) {
                        Ok(dir) => KAFKA_CONFIG_MOUNT.replace("{kafka-config}", &dir),
                        Err(e) => {
                            eprintln!("xtask: cannot write the Kafka settings: {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                } else {
                    String::new()
                };
                let mut args: Vec<String> = ["run", "-d", "--name", name, "-p", &publish]
                    .iter()
                    .map(|a| (*a).to_owned())
                    .collect();
                args.extend(rest.iter().map(|a| {
                    if *a == KAFKA_CONFIG_MOUNT {
                        mount.clone()
                    } else {
                        a.replace(SECRET, &secret)
                    }
                }));
                eprintln!("== docker {}", args.join(" ").replace(&secret, "•••"));
                if !Command::new("docker")
                    .args(&args)
                    .status()
                    .is_ok_and(|s| s.success())
                {
                    eprintln!("xtask: cannot start {name}");
                    return ExitCode::FAILURE;
                }
            }
            eprintln!("== for the tests: eval \"$(cargo xtask services env)\"");
            ExitCode::SUCCESS
        }
        "down" => {
            for (name, ..) in SERVICES {
                let _ = Command::new("docker").args(["rm", "-f", name]).status();
            }
            let _ = std::fs::remove_file(secret_file());
            ExitCode::SUCCESS
        }
        "env" => {
            let secret = match secret() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("xtask: cannot read the test services' password: {e}");
                    return ExitCode::FAILURE;
                }
            };
            for (k, v) in SERVICE_ENV {
                println!("export {k}={}", v.replace(SECRET, &secret));
            }
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("usage: cargo xtask services <up | down | env>");
            ExitCode::from(2)
        }
    }
}

fn main() -> ExitCode {
    let task = std::env::args().nth(1).unwrap_or_default();
    if task == "services" {
        return services(&std::env::args().nth(2).unwrap_or_default());
    }
    let steps: &[Step] = match task.as_str() {
        "check" => &[FORMAT, LINT, TEST, LICENCES],
        "build" => &[BUILD],
        "test" => &[TEST],
        _ => {
            eprintln!("usage: cargo xtask <check | build | test>");
            return ExitCode::from(2);
        }
    };
    for (program, arguments) in steps {
        eprintln!("== {program} {}", arguments.join(" "));
        match Command::new(program).args(*arguments).status() {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("xtask: `{program} {}` failed ({status})", arguments.join(" "));
                return ExitCode::FAILURE;
            }
            Err(error) => {
                eprintln!("xtask: cannot run {program}: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
