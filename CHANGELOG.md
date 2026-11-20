# Changelog

All notable changes to OpenDuct are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-11-20

### Added

- Transactions: a producer with a `transactional.id` writes to several partitions and commits a consumer group's
  offsets together or not at all (exactly-once), on one broker and in a cluster, with `read_committed` consumers,
  timeouts, fencing, and Kafka 4's second transaction protocol. ListTransactions, DescribeTransactions,
  DescribeProducers and WriteTxnMarkers for operators ([protocol](docs/protocol.md#transactions)).
- Log compaction: `cleanup.policy=compact` (or `compact,delete`) keeps the newest record of each key at its offset,
  tombstones delete it, compressed batches included; `delete.retention.ms`, `min.compaction.lag.ms`, and `segment.ms`
  closing segments by time. On one broker or in a cluster ([protocol](docs/protocol.md#log-compaction)).
- A topic's settings changed while it runs (AlterConfigs, IncrementalAlterConfigs): `retention.bytes`,
  `max.message.bytes`, `message.timestamp.type=LogAppendTime`, `min.insync.replicas`
  ([protocol](docs/protocol.md#other-topic-settings)).
- A schema registry built in (`--registry-listen`): Confluent's REST API for Avro, JSON Schema and Protobuf -
  subjects, versions, ids, the seven compatibility levels, references between subjects, soft and permanent deletes -
  kept in a compacted topic and served by every broker of a cluster, over HTTPS and held to access rules
  ([schema registry](docs/registry.md)).

### Changed

- A cluster upgraded from 0.1.1 gains topic settings, compaction and transactions once every broker runs 0.1.2
  (feature levels 2, 3 and 4, [upgrades](docs/upgrades.md)); until then they answer `INVALID_CONFIG`.

### Fixed

- A scheduled workflow could run twice for one time when the node reading the schedule was lost. Each time is now
  claimed before it runs, so it runs at most once, across a node's loss too; a run cut short by that loss is not run
  again ([deployment](docs/deployment.md)).
- The email step's SMTP client is lettre 0.11.24: three advisories.

## [0.1.1] - 2026-11-18

### Added

- Metrics for Prometheus: `--metrics-listen` serves `/metrics` - requests and their durations by Kafka API, bytes
  produced and fetched, connections (open, accepted, refused), sign-in failures, partition offsets, consumer group
  lag, leaderless partitions, and under `openduct start` workflow runs by outcome and AI calls and tokens (totals,
  naming no workspace). Off unless given; beyond the loopback address it needs `--metrics-token-file`. A small
  responder of its own: the broker-only build gains no dependency.

### Fixed

- The Linux programs of a release ran only on glibc 2.39 or newer (Ubuntu 24.04 and later), because they were linked
  against the build machine's own glibc; NVIDIA Jetson's JetPack 5 and 6, Ubuntu 20.04 and 22.04, Debian 12 and RHEL
  9 could not start them. They are now built against glibc 2.28 (zig as the linker, through cargo-zigbuild), the
  release fails if either program needs anything newer, and CI runs them, with a JavaScript and a Python step, on
  Debian 10, Ubuntu 20.04 and Ubuntu 22.04, on x86-64 and ARM64.
- Salesforce: a rotated refresh token is kept for the run, so a second sign-in after a session ends no
  longer stops the pipeline; a used-up one (`invalid_grant`) stops it saying so, and points to client
  credentials or a JWT.

## [0.1.0] - 2026-11-17

### Added

- Secrets are never read from the environment. A pipeline names them `secret:NAME` (or `secret:NAME.field`): in
  the studio its own workspace's credentials (checked as it is written; a missing one is named, a replaced one taken
  up without a restart), on the command line `pipeline run --secrets-file`, a file only its owner may read. `env:` is
  refused everywhere and `file:` in the studio. The cluster secret comes from `--cluster-secret-file`, tiered
  storage's keys from `--tier-s3-access-key-file` and `--tier-s3-secret-key-file` (or a role), a client's password
  from `--password-file`; one of the old variables still set stops OpenDuct with where it goes now. Compose,
  Kubernetes and OpenTofu give every secret as a file under `/run/secrets`.
- Workflows run several at once (`runs_at_once`, 10 unless set), through a few connections a node shares; a
  partition with one copy rests its Raft clock; runs are kept and positions committed in batches. Measured with
  `tests/load.rs`: 1,000 workflows idle on 86 connections and run about 8,000 records a second on one node.
- The first working broker: the Kafka wire protocol for producing (idempotent producers included), fetching with
  long polling, offsets, topics (create, list, delete, retention) and consumer groups. It runs on an in-memory
  store or a durable on-disk log. `docs/protocol.md` lists exactly what is supported.
- Limits on every resource a client could exhaust:
  - request size, and a broker-wide request-memory budget;
  - connections;
  - fetch answer size;
  - request-read, idle and write timeouts.
  - the command's own client takes at most 64 MiB in an answer, and memory only as bytes arrive.
  - lengths inside a request or answer: a list, string or byte string that claims more than was sent is refused
    before memory is reserved for it (the Kafka protocol library is built with this change, see
    `vendor/kafka-protocol/OPENDUCT-CHANGES.md`).
- Clean shutdown on SIGTERM and Ctrl-C, without waiting out long polls.
- Replication: each partition kept by several brokers, a Raft group per partition with a controller for topics,
  placement and producer ids. Leaders spread over the brokers; a broker can be lost without losing a write;
  brokers join and leave and partitions move while clients carry on (`openduct cluster status | rebalance |
  remove-broker`); mutual TLS between brokers.
- Consumer groups spread over coordinators: committed offsets in the partitions of an internal topic, each group
  coordinated by the leader of its partition.
- Tiered storage: older segments kept in S3-compatible storage and read back from it.
- Security on the client port: TLS 1.3, sign-in with SCRAM-SHA-256/512, OAuth bearer tokens or client
  certificates (PLAIN over TLS only), access rules that deny by default, and quotas and connection limits for each
  principal. The broker refuses to listen openly without them. `openduct user add | remove`.
- Upgrades one broker at a time: versioned formats on disk and between brokers, and feature levels that rise only
  once every broker has them.
- Leader epochs: each batch is stored in the epoch of the leader that wrote it (0 for a broker on its own, the
  Raft term in a cluster, the same bytes on every copy). ListOffsets answers each offset's epoch, and
  OffsetForLeaderEpoch (API 23) where an epoch ended, so consumers can check their position after a leader change.
- Topic ids, and DeleteTopics by id.
- ListGroups and DescribeGroups: the groups each broker coordinates, with members or with committed offsets only,
  their state, members (client id and host) and assignments, as Kafka's group tools show them; a principal sees only
  the groups it may describe.
- DeleteGroups and OffsetDelete: a group with no members forgotten with its offsets, or its offsets of some
  partitions removed - never those of a topic its consumers subscribe to; replicated in a cluster like a commit.
- DescribeCluster and DescribeConfigs: the brokers and the controller, and the settings of a topic or a broker - only
  those OpenDuct acts on, with the values it acts on, read-only; a topic a principal may not see is refused whether
  it exists or not.
- IncrementalAlterConfigs: a topic's retention.ms changed or put back to for ever, through the controller in a
  cluster; every other setting refused with its name.
- CreatePartitions: partitions added to a topic, placed as a new topic's are or on the brokers named; never taken
  away. In a cluster each new partition knows the entry that made it, so a broker back with an empty disk treats
  it rightly.
- DeleteRecords: a partition starts later from now on - kept across restarts, carried in cluster snapshots,
  segments before it removed - as Kafka Streams uses it to purge its repartition topics.
- The `openduct` command: `serve`, `topic create`, `topic list`, `topic delete`, `send`, `read` and `health`.
  Every `serve` option can also be set by an environment variable.
- `openduct send` sends messages from the command line or standard input, with keys partitioned as other Kafka
  clients partition them. `openduct read` prints what a topic holds (as values, or as JSON with every detail),
  follows it, and resumes under a group name. It reads gzip, snappy, lz4 and zstd batches.
- A user guide: getting started, concepts, connecting applications, running in production, a command reference
  and troubleshooting.
- Expressions (`openduct-expr`) in CEL, the Common Expression Language: its standard functions with the math,
  sets, encoders, bindings and regex extensions and the strings extension's text functions, plus `round`, `sum`,
  `sha256` and two more ways to read a time. For business data, every number is a decimal and an operation on
  null gives null, as in SQL. Mistakes are found when an expression is compiled and said in plain words, with
  where they are; limits on size, nesting, steps, time and answer size keep any expression or message from taking
  over. Every example in the guide is a test (`docs/expressions.md`).
- Pipelines (`openduct-pipeline`, `openduct pipeline check | test | run`): read a topic, then `let`, `where`, `each`,
  `map`, `key` and `headers`, then write another topic. Results are written before progress is committed, a
  message that fails goes to an `on_error` topic with the reason (or stops the pipeline), and copies of a
  pipeline share the topic as one consumer group.
- Sixteen worked pipeline examples, each a test of its exact output.
- Connectors (`openduct-connect`): a pipeline's `from:` or `to:` can name a system instead of a topic (one side is
  always a topic). Secrets come from `env:` and `file:` values and are never shown. `http` polls an API with
  expressions for its records, position and next page, and carries on from its saved position; `webhook` receives
  signed POSTs and answers 202 only once a delivery is stored; `elasticsearch` indexes by key through the bulk API
  and sets refused documents aside; `s3` writes gzip JSON-lines objects to S3 or any S3-compatible store, signed
  with Signature Version 4, committing only what is stored. TLS is rustls with ring.
- The `mqtt` connector, both ways, for devices and machines (MQTT 3.1.1, with its own client). Reading
  subscribes with QoS 1 in a persistent session and acknowledges each message only once it is in the topic, so
  nothing a device sent is lost across a stop; writing publishes by key and counts a batch as stored once the
  MQTT broker has acknowledged it. `mqtts://` trusts the system's CAs or a private one, and can present a client
  certificate. A refused login, subscription or certificate stops the pipeline with the reason.
- The `modbus` connector: named values read from machines, meters and PLCs over Modbus TCP on a schedule, as
  one JSON message per read. Registers as 16-, 32- and 64-bit whole numbers and floats, in either word order,
  single bits of status words, coils and discrete inputs, and exact scaling. Neighbouring values are read in one
  request and gaps are never read; `changes_only` writes a reading only when it changed. Its own client checks
  every answer, and a refused read stops the pipeline with the reason.
- The `kafka` connector, both ways, for topics of another Kafka-protocol cluster (Kafka 2.4 or later, or another
  OpenDuct). Reading is as a member of a consumer group there, with Kafka's standard consumer protocol, so copies
  of a pipeline share the partitions, and each partition's offset is committed in that group only once what was
  read is stored. Writing places keys as Kafka's own clients do, waits for every replica (acks all), can compress
  with gzip, snappy, lz4 or zstd, and sets aside a message the topic refuses with the cluster's reason. TLS (the
  system's CAs or a private one, and client certificates) and SASL sign-in with SCRAM-SHA-512, SCRAM-SHA-256 or
  PLAIN (PLAIN only over TLS); the cluster's SCRAM proof is checked too. Tested against Apache Kafka 4.4.
- The `postgres_cdc` connector: every row inserted, updated and deleted in chosen tables of PostgreSQL 10 or
  later, from its logical replication with the built-in `pgoutput` plugin, so nothing is installed on the
  database. The first time, the rows already there are read too, from the same moment the stream starts from.
  Values keep their types (exact numbers, JSON as JSON); each change is keyed by table and primary key; a
  transaction is confirmed to the database only once it is stored, so nothing is lost while the pipeline is
  stopped. Its own client signs in with SCRAM-SHA-256 or MD5 (and a password in clear only over TLS) and checks
  the server's SCRAM proof.
- The `mysql_cdc` connector: every row inserted, updated and deleted in chosen tables of MySQL 8.0 or 8.4, read
  from the binary log as a replica reads it, so nothing is installed on the database. The first time, the rows
  already there are read too, in one consistent snapshot and without locks; the pipeline saves its binary log
  position only after a transaction's rows are stored. Values keep their types (exact decimals, JSON as JSON,
  ENUM and SET as text, TIMESTAMP in UTC, binary as hex); every event's CRC32 is checked. Its own client signs in
  with caching_sha2_password (encrypting the password to the server's key without TLS) or mysql_native_password.
- The `mongodb_cdc` connector: every document inserted, updated, replaced and deleted in chosen collections of
  MongoDB 6.0 or later (replica sets, sharded clusters, Atlas), from its change streams with MongoDB's own Rust
  driver (over rustls). The first time, the documents already there are read too; the stream's resume
  token is saved only once the changes before it are stored. Updates carry the whole document, what changed, and
  the document as it was where the collection keeps it; values are plain JSON (object ids as hex, dates in UTC,
  exact decimals).
- The `salesforce` connector, both ways, through the REST API with OAuth 2.0 (client credentials, a JWT signed
  with the app's private key, or a refresh token; a session that ends is begun again). Reading polls one object
  with SOQL through `queryAll`, so deletions are seen, in the order records last changed, with a position of time
  and id that neither skips nor repeats a record. Writing upserts by an external id (or inserts, or updates by
  Id) 200 records a call, and sets aside a record Salesforce refuses with its reason. Tested against a stand-in
  for the API until an org is given.
- The `servicenow` connector, both ways, through the REST Table API, with basic sign-in or OAuth 2.0 (the
  password grant or client credentials; a token that ends is got again). Reading polls one table in the order
  records were last updated, with a position of time and id that neither skips nor repeats a record, and an
  encoded query of the pipeline's own. Writing inserts, updates by `sys_id`, or upserts by a field of the
  pipeline's choosing, and sets aside a record ServiceNow refuses (a data policy, a business rule) with its
  reason. Tested against a stand-in for the API until an instance is given.
- The `snowflake` connector, both ways, through the SQL API, signed in with a key pair (a JWT, renewed before its
  hour ends) or a programmatic access token. Reading polls one table in the order of a column that grows and a
  unique id, with a position bound to the query that neither skips nor repeats a row; values become plain JSON
  (exact numbers, ISO 8601 times, semi-structured values as JSON). Writing inserts or merges many rows a
  statement, each value cast to its column's type, with a request id so a statement sent again is not run twice;
  a row Snowflake refuses is set aside with its reason. Statements still running are waited for, and answers in
  several parts read whole. Tested against a stand-in for the API until an account is given.
- The `opcua` connector reads named values of a machine, PLC or SCADA server as they change, through an OPC UA
  subscription: signed and encrypted by default (Basic256Sha256, Aes128_Sha256_RsaOaep or Aes256_Sha256_RsaPss),
  a server's certificate trusted only once it is moved to the trusted folder, anonymous or a user and password,
  and a deadband for numbers. The first connection must succeed; a dropped one is made again with the
  subscription. Built on async-opcua (MPL-2.0, pure Rust), held at one commit of its repository because the
  released 0.19.0 has a known RSA timing weakness, an unmaintained YAML parser and no early check of a message's
  declared size. Tested against a real OPC UA server, including a server that declares a message too large.
- `openduct pipeline run` logs what connectors report as they go (a connection made again, messages a cluster
  removed before they were read); `RUST_LOG` sets the detail.
- Example applications:
  - sensor telemetry, in Rust;
  - a broker embedded in your own program, with a durable log.
- End-to-end tests over TCP with an independent Kafka client (rskafka).
- Hostile-input tests (`crates/openduct-hostile`): every place OpenDuct reads bytes it did not write - Kafka requests,
  record batches, segment files, expressions, pipeline files, MQTT packets, PostgreSQL replication messages, MySQL
  binary log events, and requests to a running broker - fed mutated input with a fixed seed in every test run, and
  fuzzed with cargo-fuzz in CI (`fuzz/`).
- CI: format, lint, tests on Linux and macOS, cross-compile checks for 64-bit and 32-bit ARM and static Linux, a
  licence and advisory check, and the client checks.
- Project governance:
  - Apache License 2.0;
  - Contributor License Agreement;
  - Code of Conduct;
  - security policy;
  - contribution guide.
