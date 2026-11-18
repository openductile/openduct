# Deploying OpenDuct

OpenDuct is one binary with no runtime dependencies: no ZooKeeper, no separate coordination service. It runs
the same on a cloud server, an edge gateway, a robot's computer or a small device.

## Picking a store

| Where | Store | Why |
|---|---|---|
| Tests, development, CI | `--memory` | nothing to clean up |
| One node that must not lose data: an edge gateway, a robot, a single server | `--data-dir /var/lib/openduct` | durable on local disk, `fsync` before every acknowledgement |
| Several brokers sharing one log, so losing one loses nothing | `--data-dir` and `--brokers` (see [A cluster](#a-cluster)) | a write is acknowledged once most brokers have it on disk; the cluster carries on while most brokers are up |

## A single node

```sh
openduct serve \
  --listen 0.0.0.0:9092 \
  --advertise 10.0.0.5:9092 \
  --data-dir /var/lib/openduct
```

`--advertise` is the address clients are told to connect to (Kafka clients ask the broker where it is, then
reconnect there). It defaults to the listen address, which is wrong when listening on `0.0.0.0`: set it to an
address clients can reach.

### As a systemd service

```ini
[Unit]
Description=OpenDuct streaming broker
After=network-online.target
Wants=network-online.target

[Service]
User=openduct
ExecStart=/usr/local/bin/openduct serve --listen 0.0.0.0:9092 --advertise 10.0.0.5:9092 --data-dir /var/lib/openduct
Restart=on-failure
# OpenDuct stops cleanly on SIGTERM: it stops accepting, lets requests in flight finish, and releases its data.
TimeoutStopSec=30
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

## Secrets are files

OpenDuct reads no secret from the environment: an environment variable is seen by every process the server starts,
in crash reports and in `docker inspect`. Each secret is a file instead - the cluster secret
(`--cluster-secret-file`), tiered storage's keys (`--tier-s3-access-key-file`, `--tier-s3-secret-key-file`; or a
role on AWS, which needs none), TLS keys, the users file, a client's `--password-file`. Docker and Kubernetes give
secrets as files under `/run/secrets` (see `deploy/compose` and `deploy/kubernetes`). A pipeline's secrets are the
studio's credentials, or on the command line a secrets file only its owner may read ([connectors](connectors.md)).

## A cluster

Three brokers (or five) share the data: every partition is kept by three of them (`--replication-factor`) and led
by one, leaders spread over the brokers. Each broker is started with the whole list and its own id:

```sh
# The same secret on every broker, at least 16 characters, in a file only OpenDuct's user may read.
install -m 600 /dev/null /etc/openduct/cluster-secret && openssl rand -hex 32 > /etc/openduct/cluster-secret
openduct serve \
  --cluster-secret-file /etc/openduct/cluster-secret \
  --node-id 1 \
  --listen 0.0.0.0:9092 --advertise 10.0.1.11:9092 \
  --replication-listen 10.0.1.11:9093 \
  --data-dir /var/lib/openduct \
  --brokers 1=10.0.1.11:9093@10.0.1.11:9092,2=10.0.1.12:9093@10.0.1.12:9092,3=10.0.1.13:9093@10.0.1.13:9092
```

Each entry of `--brokers` is `id=replication-address@client-address`: where the other brokers reach it, and where
clients do (its `--advertise`). Clients may bootstrap from any broker; every broker's metadata names each
partition's leader, which takes its writes and reads, and the coordinator, which takes consumer groups. The secret
can also come from a file (`--cluster-secret-file`).

- **Writes** are acknowledged once most of a partition's brokers hold them on disk (two of three), whatever `acks`
  a producer asks for. A partition takes writes while most of its brokers are up; with fewer it waits, and loses
  nothing.
- **Losing a broker:** for each partition it led, the others elect a new leader within a few seconds
  (`NOT_LEADER_OR_FOLLOWER` meanwhile, which clients retry); the other partitions never stop. If it coordinated the
  consumer groups, they re-form on the new coordinator.
- **Starting a cluster:** the broker with the lowest id starts it the first time, so start that one too. Ids are
  for good: a broker keeps its id for life.
- **A broker back after a stop** catches up from the others. One back with an empty disk (a replaced volume)
  rejoins under its id - broker 1 too - and is rebuilt from the others' snapshots and logs; until it has caught up
  it does not vote, so it cannot help elect a leader missing what it had acknowledged.
- **Adding a broker:** start it like the others, with every broker in `--brokers` (itself included). It joins the
  cluster by itself; new topics use it at once. `openduct cluster rebalance --bootstrap <any broker>` then moves
  partitions onto it until every broker holds, and leads, about as many as the others.
- **Taking a broker out:** `openduct cluster remove-broker <id> --bootstrap <another broker>` moves its partitions
  to the others, waits for that, and takes it out of the cluster; then stop it. `openduct cluster status` shows
  each broker's share and the moves under way. Kafka's own `kafka-reassign-partitions.sh` works too.
- **Tiered storage:** `--tier-s3-bucket <bucket>` (with `--tier-s3-region`, and `--tier-s3-endpoint` for an
  S3-compatible store) keeps only the recent segments on the brokers' disks - `--tier-local-retention-mib` per
  partition, 1024 by default - and the rest in the bucket, read back when asked for. Give the brokers an IAM role
  that may get, put and delete objects under the prefix. See [storage](storage.md#tiered-storage---tier-s3-bucket).
- **Security:** TLS on the client port, sign-in and access rules are required beyond the loopback address; see
  [security](security.md).
- **Upgrades:** one broker at a time, clients running throughout; see [upgrades](upgrades.md).
- **Leaders go home:** after a broker comes back, each partition it led first is handed back to it once it has
  caught up, so the load spreads again by itself.
- **TLS between brokers:** give each broker a certificate signed by the cluster's own authority, naming the host of
  its replication address, and usable as both server and client (`--replication-tls-cert`, `--replication-tls-key`,
  `--replication-tls-ca`, or `OPENDUCT_REPLICATION_TLS_*`). Every connection between brokers is then TLS 1.3, and a
  broker accepts another only if the same authority signed its certificate. Without TLS everything between
  brokers, the secret too, travels as written: keep the replication port (`--replication-listen`, 9093 here) on a
  private network then. Clients' connections (the Kafka port) are not TLS yet.

### The studio and workflows in a cluster

`openduct start` takes every setting above too: each node runs a broker and, beside it, the studio, and together
they are one OpenDuct. Put the nodes' studio ports (5480) behind one load balancer, and send webhooks to it as well:

```sh
openduct start \
  --cluster-secret-file /etc/openduct/cluster-secret \
  --node-id 1 \
  --listen 0.0.0.0:9092 --advertise 10.0.1.11:9092 \
  --replication-listen 10.0.1.11:9093 \
  --studio-listen 0.0.0.0:5480 --studio-host flows.example.com --studio-secure-cookies \
  --data-dir /var/lib/openduct \
  --brokers 1=10.0.1.11:9093@10.0.1.11:9092,2=10.0.1.12:9093@10.0.1.12:9092,3=10.0.1.13:9093@10.0.1.13:9092
```

- **One studio, any node:** accounts, workspaces, workflows, credentials, files and runs are kept in the cluster,
  so every node shows the same, and a person signed in on one node is signed in on all of them.
- **Keys:** the key that signs sessions and the one that seals credentials are the cluster's: kept in it, each
  sealed with a key derived from the cluster secret, and the same on every node. Give every node the same secret
  from the start: a node given another one says so and does not start. A node that ran on its own before brings its
  keys when it joins (its `studio/` key files), so the credentials it sealed still open.
- **Runs spread over the nodes:** each workflow is read by every node as one consumer group, so its events run on
  whichever node reads them, and a node lost hands its share to the others (once its session in the group runs out,
  30 seconds). An event is run at least once: a run cut short by a lost node runs again on another.
- **Schedules run once:** every node queues each scheduled time, to the same partition and naming the time; the
  one node reading that partition runs each time once. A schedule goes on while any node is up.

## Small and embedded devices

- **Build for the device.** OpenDuct builds on the board itself, or cross-compiles for its Rust target, for example
  `aarch64-unknown-linux-gnu` (64-bit ARM: Raspberry Pi 4 and 5, NVIDIA Jetson) or
  `armv7-unknown-linux-gnueabihf` (32-bit ARM). Cross-compiling needs a C cross-compiler for the board: it links
  the binary, and with pipelines and connectors it also builds the part of ring (the cryptography for TLS and
  signing) that is written in C. The broker alone (`--no-default-features`) has no C to build. On Debian or
  Ubuntu, for 64-bit ARM:

  ```sh
  sudo apt install gcc-aarch64-linux-gnu
  rustup target add aarch64-unknown-linux-gnu
  CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    cargo build --release --target aarch64-unknown-linux-gnu
  ```

  A cross-linker for the target is needed (for example `gcc-aarch64-linux-gnu` on Debian or Ubuntu).
- **Bound the disk.** Give each topic a retention (`openduct topic create ... --retention-ms`), and size segments to
  the device (`--segment-mib`): retention removes whole segments, so smaller segments free space sooner.
- **Bound the memory.** Requests are refused above `--max-request-mib`. All requests in flight together hold at
  most `--max-request-memory-mib`; past that, reading waits. Fetch answers carry at most `--max-fetch-mib`. Lower
  all three, and `--max-connections`, on a small device. For example, on a board with 512 MiB of memory:
  `--max-request-mib 8 --max-request-memory-mib 32 --max-fetch-mib 8 --max-connections 256`.
- **Flash storage.** `--fsync always` (the default) writes to the device on every acknowledged batch. On flash with
  limited write endurance, batch more records per Produce request on the producer side (`linger.ms`,
  `batch.size`) rather than turning `fsync` off.

## Sizing and limits

| Option | Default | What it bounds |
|---|---|---|
| `--max-request-mib` | 100 | The largest single request. A producer's batches must fit. |
| `--max-request-memory-mib` | 256 | All requests being read or handled at once. Must be at least `--max-request-mib`. |
| `--request-read-timeout-secs` | 60 | How long a request's body may take to arrive once its size has. |
| `--max-fetch-mib` | 55 | One Fetch answer (the first batch is always sent whole). |
| `--max-connections` | 10000 | Client connections at once. More wait for a free slot. |
| `--idle-timeout-secs` | 600 | A connection that sends nothing, or does not read its answers. |
| `--segment-mib` | 64 | Log segment size: the unit retention removes. |

Raise the open-file limit (`LimitNOFILE`, `ulimit -n`) above `--max-connections` plus the number of segments.

## Networks

OpenDuct speaks the Kafka protocol in plaintext and has no authentication yet. Run it on a trusted network, or
behind something that adds encryption and authentication (a VPN, a service mesh, an authenticating TLS proxy).
TLS and SASL are on the roadmap.

## Logs

OpenDuct writes its log to stderr (`--log-format text` or `json`), where `docker logs`, `kubectl logs` and journald
pick it up, and keeps it on disk as OpenTelemetry log records: OTLP JSON Lines, each line one
`ExportLogsServiceRequest`, a file a day (UTC) named `openduct-YYYY-MM-DD.jsonl`.

- **Where**: `--log-dir` (`OPENDUCT_LOG_DIR`); by default the data directory's `logs` folder - `/data/logs` in the
  image, on its volume, so the log outlives the container. `--log-dir none` keeps no file; `--memory` keeps none
  unless `--log-dir` is given.
- **How long**: `--log-keep-days` (`OPENDUCT_LOG_KEEP_DAYS`, 14 by default); older files are removed.
- **What a record holds**: its time, severity (`severityNumber` 1/5/9/13/17 for trace/debug/info/warn/error), the
  module as the scope's name, the message as the body, the event's fields as attributes; the resource names the
  service (`openduct`), its version and the node (`service.instance.id`, `node-<id>`).
- **Shipping it**: an OpenTelemetry Collector reads the files with its `otlpjsonfile` receiver and sends them on
  to any backend, as any other OTLP source.
- **Reading it**: the studio's Logs page (the installation's admin only) shows the newest records, filtered by level
  and text, and follows them as they come. In a cluster each node keeps and shows its own.
- **How much**: `RUST_LOG` sets the level for both, as `RUST_LOG=info,openraft=warn`.

## Metrics

`--metrics-listen 127.0.0.1:9645` (`OPENDUCT_METRICS_LISTEN`) serves metrics for Prometheus at
`http://127.0.0.1:9645/metrics`, in the text format any Prometheus-compatible scraper reads: Prometheus itself, the
OpenTelemetry Collector's `prometheus` receiver, Grafana Agent, Datadog, the CloudWatch agent. It is off unless
given.

- **Beyond this machine** it needs a token: `--metrics-token-file <file>` (`OPENDUCT_METRICS_TOKEN_FILE`), a file
  only its owner may read whose first line (16 characters or more) is the token. A scrape without
  `Authorization: Bearer <token>` is answered `401`. The metrics name topics and groups, so OpenDuct refuses to serve
  them beyond the loopback address without one (unless `OPENDUCT_ALLOW_INSECURE=true` was said on purpose). On its
  own the token travels in clear: keep the port on a private network, or behind a TLS proxy.
- **What is measured** (names follow Prometheus' conventions; where OpenTelemetry's messaging conventions name the same
  thing, the meaning matches):

| Metric | Type | What it says |
|---|---|---|
| `openduct_build_info{version,node_id}` | gauge | The running version |
| `openduct_uptime_seconds` | gauge | Seconds since the broker started |
| `openduct_connections` | gauge | Client connections open now |
| `openduct_connections_accepted_total`, `openduct_connections_refused_total` | counter | Connections accepted; refused before any request (an address over its share, or kept out after failed sign-ins) |
| `openduct_sign_in_failures_total` | counter | Failed SASL sign-ins |
| `openduct_requests_total{api}`, `openduct_request_failures_total{api}` | counter | Requests by Kafka API; those that could not be answered |
| `openduct_request_duration_seconds{api}` | histogram | From a request read to its answer ready: a Produce until its batches are on the device, a Fetch including its long poll |
| `openduct_produced_bytes_total`, `openduct_fetched_bytes_total` | counter | Record batch bytes written and read |
| `openduct_partition_log_end_offset{topic,partition}`, `openduct_partition_log_start_offset{topic,partition}` | gauge | The high watermark and the first offset kept, by partition this broker leads |
| `openduct_consumer_group_lag{group,topic,partition}` | gauge | Messages a group has still to read, for the groups this broker coordinates |
| `openduct_partitions_led` | gauge | Partitions this broker leads |
| `openduct_partitions_without_leader` | gauge | Partitions with no leader now (none on a single broker): nothing can be written to them until one is chosen |
| `openduct_workflow_runs_total{outcome}`, `openduct_workflow_run_duration_seconds` | counter, histogram | `openduct start` only: workflow runs by how they ended (`done`, `skipped`, `failed`) and how long they took |
| `openduct_ai_calls_total`, `openduct_ai_tokens_total{kind}` | counter | `openduct start` only: AI calls answered and the tokens models reported (`prompt`, `completion`) |

Workflow metrics are totals: no workspace, workflow or model is named, so they tell no workspace about another; a
workflow's own figures are in the studio. In a cluster each broker reports the partitions it leads and the groups it
coordinates, so summed across brokers each counts once.

A Prometheus scrape of a broker beyond this machine:

```yaml
scrape_configs:
  - job_name: openduct
    authorization:
      credentials_file: /etc/prometheus/openduct.token
    static_configs:
      - targets: ["broker-1:9645", "broker-2:9645", "broker-3:9645"]
```

Worth an alert: `openduct_partitions_without_leader > 0` for more than a minute; a group's
`openduct_consumer_group_lag` growing for minutes; a jump in `openduct_sign_in_failures_total` or
`openduct_connections_refused_total`; `openduct_workflow_runs_total{outcome="failed"}` rising.

## Health

`openduct health --bootstrap <host:port>` asks the broker which Kafka APIs it supports and exits 0 when it answers.
Kubernetes can also check the Kafka port itself:

```yaml
livenessProbe:
  tcpSocket:
    port: 9092
readinessProbe:
  exec:
    command: ["openduct", "health", "--bootstrap", "127.0.0.1:9092"]
```
