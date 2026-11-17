# OpenDuct

**The open metal streaming broker.** OpenDuct is an event broker that speaks the Kafka protocol, in one small
binary written in Rust. Use the Kafka clients you already have; run it on a server, an edge gateway, a robot or a
device; or put it inside your own program as a library. With it come pipelines and connectors that move and clean up
data, and a studio in the browser where workflows are built on top of the broker, with apps for the services a
business uses.

[![CI](https://github.com/openductile/openduct/actions/workflows/ci.yml/badge.svg)](https://github.com/openductile/openduct/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

```sh
openduct serve --data-dir /var/lib/openduct        # a durable broker on 127.0.0.1:9092
```

## Why OpenDuct

- **Your Kafka clients work.** OpenDuct speaks the Kafka wire protocol, so clients written for Kafka connect
  unchanged ([compatibility](#compatibility)).
- **One small binary.** Nothing else to install: no ZooKeeper, no separate controllers, no runtime. About 26 MB with
  pipelines and connectors, or 11 MB as a broker alone for small devices. On a laptop (arm64) it is listening about
  6 ms after launch and idles in 11 MiB of memory (9 MiB as a broker alone).
- **Durable by default.** Every write is on disk (`fsync`) before it is acknowledged. Every read is
  checksum-verified. After a crash, recovery cuts off only a write that never finished and was never acknowledged.
  If acknowledged data is damaged, OpenDuct refuses to start rather than serve a log with a hole in it
  ([storage](docs/storage.md)).
- **Bounded under load and under attack.** Every limit is enforced: request size, total request memory,
  connections, fetch size and every wait. A misbehaving or malicious client loses its own connection; it cannot
  exhaust the broker.
- **Memory-safe.** Rust, with `unsafe` forbidden in every OpenDuct crate. The broker alone has no C in it at
  all; pipelines and connectors use ring for TLS and signing, whose cryptography is partly C and assembly, so
  building them needs a C compiler as well ([small devices](docs/guide/running.md#small-devices-and-edge-gateways)).
- **Pipelines with expressions.** Clean up, reshape, filter and split messages as they flow from topic to topic,
  with one short file and expressions in CEL, an open standard ([pipelines](docs/pipelines.md),
  [expressions](docs/expressions.md)).
- **Connectors.** Pipelines read from HTTP APIs, webhooks, MQTT, Modbus, OPC UA, other Kafka clusters,
  PostgreSQL, MySQL and MongoDB (every change, as it happens), Salesforce, ServiceNow and Snowflake, and write to
  Elasticsearch, S3-compatible storage, MQTT, other Kafka clusters, Salesforce, ServiceNow and Snowflake, with
  more coming ([connectors](docs/connectors.md)).
- **Embeddable.** The broker is a library too. An edge gateway or vehicle computer can carry its own durable
  event log, with nothing else to install.
- **Workflows in the browser.** `openduct start` adds the studio: workflows started by webhooks, schedules, topics
  or a click, with steps for data, AI (any model, including one on your own machine), SQL, e-mail, code and HTTP,
  tried on sample input before they run. Each runs on the broker, so an event is never lost and events with the same
  key stay in order ([studio](docs/studio.md)).
- **Apps.** 26 apps - Stripe, Shopify, QuickBooks, Xero, HubSpot, Google Sheets, Gmail, Microsoft 365, Slack and
  more - each a definition file rather than code, with a kit to build your own ([apps](docs/apps.md)).
- **Open.** Apache License 2.0, with no usage caps and no licence keys.

## Status

OpenDuct is new: **0.1**. It runs as one broker or as a cluster. It is tested hard (see [testing](#testing)), but
it does not yet do everything Kafka does. Today it supports:

- producing (idempotent producers included) and fetching, with long polling;
- offsets, including lookup by timestamp;
- topics: create, list and delete (by name or by id), with time-based retention;
- consumer groups with committed offsets, spread over the brokers;
- compressed batches, passed through untouched;
- an in-memory store, and a durable store on local disk;
- replication: each partition kept by several brokers (a Raft group per partition, leaders spread over them), so a
  broker can be lost without losing a write; brokers join and leave, and partitions move, while clients carry on;
- tiered storage: older segments kept in S3-compatible storage, and read back from it;
- security: TLS 1.3, sign-in (SCRAM, OAuth bearer tokens, client certificates), access rules that deny by default,
  and quotas for each principal ([security](docs/security.md));
- upgrades one broker at a time ([upgrades](docs/upgrades.md));
- pipelines: read a topic, change each message with expressions, write another topic, with failed messages set
  aside and several copies sharing the work;
- the studio: workflows, workspaces and their members, credentials sealed and never shown back, run history,
  failed runs kept to run again, versions, templates and 26 apps.

**Not yet:** transactions, log compaction, changing settings other than a topic's retention, a schema registry,
app triggers, AI agents and single sign-on. [docs/capabilities.md](docs/capabilities.md) lists what OpenDuct does
and does not do yet, and [docs/protocol.md](docs/protocol.md) exactly what each Kafka API does.

## Quick start

Build from source with [Rust](https://rustup.rs). `rustup` installs the pinned toolchain from
`rust-toolchain.toml` by itself.

```sh
git clone https://github.com/openductile/openduct
cd openduct
cargo build --release
./target/release/openduct serve --memory
```

In another terminal:

```sh
./target/release/openduct topic create sensors --partitions 3
./target/release/openduct send sensors '{"temp_c": 21.4}' --key sensor-1
./target/release/openduct read sensors
```

Or start the broker with the studio, and open the address it prints to build a workflow:

```sh
./target/release/openduct start --data-dir ./data
```

Then use any Kafka client. The bundled example sends simulated sensor readings and reads them back:

```sh
cargo run -p sensor-telemetry -- produce --count 5
cargo run -p sensor-telemetry -- consume --from-beginning --max 20
```

From your own Rust program, with the [rskafka](https://crates.io/crates/rskafka) client:

```rust
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;

let client = ClientBuilder::new(vec!["127.0.0.1:9092".to_owned()]).build().await?;
let sensors = client.partition_client("sensors", 0, UnknownTopicHandling::Retry).await?;
let record = Record {
    key: Some(b"sensor-1".to_vec()),
    value: Some(br#"{"temp_c": 21.4}"#.to_vec()),
    headers: Default::default(),
    timestamp: chrono::Utc::now(),
};
let offsets = sensors.produce(vec![record], Compression::NoCompression).await?;
let (records, _high_watermark) = sensors.fetch_records(offsets[0], 1..1_000_000, 1_000).await?;
```

## Documentation

The [user guide](docs/guide/README.md) covers the rest:

- getting started in ten minutes;
- the concepts in plain words;
- connecting applications;
- running OpenDuct in production;
- every command and setting;
- troubleshooting;
- [pipelines](docs/pipelines.md), [expressions](docs/expressions.md) and [connectors](docs/connectors.md);
- [the studio and workflows](docs/studio.md), and [apps](docs/apps.md) with the kit to build them;
- [tutorials](docs/tutorials/README.md): a first workflow with real apps, triggers, forms, and building an app.

And beside it:

- [what OpenDuct does, and does not do yet](docs/capabilities.md);
- [OpenDuct beside Zapier, Make, n8n, Workato, MuleSoft, Kafka, Confluent, Redpanda, NATS, LangGraph and
  Temporal](docs/comparison.md);
- [what the broker is for](docs/use-cases.md): the edge, small machines, moving data, low trust, AI;
- [test results](docs/regression.md): the broker's suites as last run, and what was measured.

## Running it for real

```sh
openduct serve \
  --listen 0.0.0.0:9092 \
  --advertise broker-1.internal:9092 \
  --data-dir /var/lib/openduct \
  --tls-cert broker.pem --tls-key broker.key \
  --credentials-file users.json --acl-file rules.json
```

`--advertise` is the address clients are told to reconnect to, so it must be one they can reach. OpenDuct refuses
to listen on `0.0.0.0` without it rather than hand clients an address that cannot work - and without TLS, sign-in
and access rules, rather than serve a network it cannot trust: see [security](docs/security.md).

Every option can also be set through an environment variable (`OPENDUCT_LISTEN`, `OPENDUCT_DATA_DIR`, ...),
except secrets: a password, the cluster secret or a storage key comes from a file, never from the environment
([secrets are files](docs/deployment.md#secrets-are-files)). `openduct serve --help` lists them all. [docs/deployment.md](docs/deployment.md) covers systemd, Kubernetes
probes, small devices and sizing.

| Command | What it does |
|---|---|
| `openduct start` | Run the broker and the studio, where workflows are built in the browser. |
| `openduct serve` | Run the broker alone until Ctrl-C or SIGTERM, then stop cleanly. |
| `openduct topic create <name> [--partitions N] [--retention-ms MS]` | Create a topic. |
| `openduct topic list` | List topics and their partition counts. |
| `openduct topic delete <name>` | Delete a topic and its data. |
| `openduct send <topic> [message...]` | Send messages, or one per line of standard input. |
| `openduct read <topic> [--follow]` | Print the messages a topic holds, and optionally wait for more. |
| `openduct pipeline run <file>` | Run a pipeline (also `check` and `test`). |
| `openduct workflow check <file>` | Check a workflow file (also `test`, on sample events, sending nothing). |
| `openduct app new <id>` | Start an app (also `check`, `test`, `try` and `list`): see [apps](docs/apps.md). |
| `openduct cluster status` | A cluster's brokers, what each holds and leads, and the moves under way (also `rebalance` and `remove-broker <id>`). |
| `openduct user add <name>` | Let a name sign in with SCRAM, its password read from standard input (also `remove`). |
| `openduct health` | Exit 0 if the broker answers, 1 if not. |

## Examples

| Example | What it shows |
|---|---|
| [`examples/sensor-telemetry`](examples/sensor-telemetry) | Simulated sensors producing readings, keyed so each sensor stays in order, and a consumer following every partition (Rust, with an ordinary Kafka client). |
| [`examples/embedded-broker`](examples/embedded-broker) | OpenDuct inside your own program: an edge gateway with its own durable log that survives restarts. |
| [`examples/pipelines`](examples/pipelines) | Sixteen pipelines: cleaning orders, sensor alerts, access logs, payment events, masking personal data, splitting orders into lines, machine states read over MQTT, an energy meter read over Modbus, a curing oven watched over OPC UA, paid orders read from another Kafka cluster, order changes captured from PostgreSQL, stock levels captured from MySQL, a product catalogue captured from MongoDB, deals won in Salesforce, ServiceNow incidents opened for machine faults, and orders loaded into Snowflake. Each with sample input and its exact output. |

## As a library

```rust
use std::sync::Arc;
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{FileStore, FileStoreConfig};

let store = FileStore::open("edge-log", FileStoreConfig::default())?;
let broker = Broker::start(BrokerConfig::new("127.0.0.1:9092".parse()?), Arc::new(store)).await?;
// ... any Kafka client on the machine can now use 127.0.0.1:9092 ...
broker.shutdown().await;
```

The workspace's crates:

- **`openduct-storage`** holds the log. It defines the `Store` trait and provides an in-memory store and the
  durable file store.
- **`openduct-broker`** speaks the Kafka protocol on top of any store.
- **`openduct-raft`** keeps a log on several brokers: the controller and a Raft group for each partition.
- **`openduct-value`** holds the values a message carries: JSON in and out, numbers, times.
- **`openduct-expr`** holds the expressions pipelines and connectors use: CEL, with OpenDuct's limits.
- **`openduct-pipeline`** reads a pipeline file and works out what each message becomes.
- **`openduct-connect`** holds the connectors.
- **`openduct-flow`** runs workflows: their steps, the apps' definitions, and each workflow's consumer of its topic.
- **`openduct-script`** runs a code step's snippet in a locked-down process.
- **`openduct-studio`** serves the studio: its page, accounts, workspaces, credentials, webhooks and runs.
- **`openduct-hostile`** mutates input at random for the tests of every reader of untrusted bytes.
- **`openduct`** is the command-line tool.

[docs/architecture.md](docs/architecture.md) explains how they fit together.

## Compatibility

OpenDuct implements the Kafka APIs a producer, a consumer and a consumer group need, at the versions current clients
use; [docs/protocol.md](docs/protocol.md) lists each one. Every change is tested end to end over TCP with an
independent Kafka client, [rskafka](https://crates.io/crates/rskafka). Each run creates, lists and deletes topics,
produces and fetches, runs long polls, checks consumer groups and their commits and idempotent producers, and
sends hostile input. If a client does not work with OpenDuct, please
[open an issue](https://github.com/openductile/openduct/issues).

## Testing

Run `cargo xtask check` before every change. It runs the formatter, the linter (every warning is an error, and `unwrap`,
`expect`, `panic` and unchecked indexing are refused outside tests), all tests, and a licence and advisory check
of every dependency. The tests include:

- unit tests of the log's recovery: torn writes, damage in place, interrupted topic creation, interrupted deletes,
  and producer state after a restart;
- end-to-end tests over TCP with an independent Kafka client: durability across restarts, long polls, consumer
  groups, idempotent retries, and hostile input (oversized, negative, garbage and unknown-API frames, a request
  that never arrives);
- hostile input everywhere OpenDuct reads bytes it did not write - requests, record batches, segment files,
  expressions, pipeline files and what connectors read from other systems - mutated at random with a fixed seed in
  every run, and fuzzed with cargo-fuzz in CI ([fuzz](fuzz/README.md)).

[docs/regression.md](docs/regression.md) has the broker's suites as last run, and the measurements.

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) first. Every contributor agrees to the
[Contributor License Agreement](CLA.md) on their first pull request, and maintainers merge through reviewed pull
requests ([governance](GOVERNANCE.md)). Please follow the [Code of Conduct](CODE_OF_CONDUCT.md).

Report security problems privately, as [SECURITY.md](SECURITY.md) describes, never in a public issue.

## License

Copyright 2026 Ductile AI, Inc. Licensed under the [Apache License, Version 2.0](LICENSE); see [NOTICE](NOTICE).

Apache Kafka and Kafka are trademarks of the Apache Software Foundation. OpenDuct is an independent implementation
of the Kafka wire protocol, and is not affiliated with or endorsed by the Apache Software Foundation.
