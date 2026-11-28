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

### Containers, Kubernetes and a cloud's machine

- **Docker Compose**: `deploy/compose`, OpenDuct with an S3 store and PostgreSQL.
- **Kubernetes**: `deploy/kubernetes/openduct.yaml`, a manifest of three nodes; or the Helm chart
  `deploy/helm/openduct`, one node or (`values-cluster.yaml`) a cluster, its secret as a file, probes as
  [Health](#health) says ([its README](../deploy/helm/openduct/README.md)).
- **One machine on a cloud**: the OpenTofu modules `deploy/opentofu/aws-vm`, `gcp-vm` and `azure-vm` start Debian
  with the released program as a systemd service, every port closed but to the addresses given
  ([their README](../deploy/opentofu/README.md)).

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
- **A broker new to the cluster** - a founder started a little after the others, or one added later - starts with an
  empty disk too, but has nothing to rebuild: the cluster has no record of it, and its copies take part at once. The
  cluster records each broker before that broker holds a copy of anything, which is how a broker back with an empty
  disk is told from a new one (`openduct-raft` `empty_disk`).
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
  one node reading that partition claims each time (commits past it) before it runs it, so each time runs once -
  also when that node is lost and another takes the partition over. A schedule goes on while any node is up. A
  time whose run was cut short by its node's loss is not run again.

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

## Schema registry

`--registry-listen 127.0.0.1:8081` (`OPENDUCT_REGISTRY_LISTEN`) serves the schema registry beside the broker; beyond
this machine it needs `--registry-token-file`. See [schema registry](registry.md).

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

There are two questions, and a restart needs the second one.

**Does it answer?** `openduct health --bootstrap <host:port>` asks the broker which Kafka APIs it supports and exits 0
when it answers. Use it for liveness: a broker that does not answer should be restarted.

**Can it be relied on?** `openduct health --ready` exits 0 only when the broker has joined its cluster and every
partition it holds a copy of is started, is not being rebuilt, has a leader, and is level with that leader. A broker
that has just started answers long before that is true. Use it for readiness, so that a rolling restart or a node
replacement waits for the broker that came back before it stops the next one. Taking brokers down one at a time
protects nothing if "back" means "answers".

`--ready` asks the broker's own `--metrics-listen` address (`GET /ready`: 200, or 503 with the reasons, a line each),
so the broker must be started with one. `127.0.0.1:9645` is enough, and needs no token. A broker that is not part of a
cluster is ready when it answers.

```yaml
# Kubernetes
livenessProbe:
  exec:
    command: ["openduct", "health", "--bootstrap", "127.0.0.1:9092"]
readinessProbe:
  exec:
    command: ["openduct", "health", "--ready", "--ready-address", "127.0.0.1:9645"]
```

The same standing is in the metrics: `openduct_ready`, `openduct_partitions_held`, `_pending`, `_not_started`,
`_recovering`, `_without_leader` and `_behind`. Alert on `openduct_ready == 0` for longer than a restart takes.

Do not let an orchestrator's own rolling update wait on readiness alone when you change every broker at once: if the
cluster is already without a majority, no broker becomes ready, and the change that would mend it never reaches the
others. Replace the brokers that are not ready first, then the ready ones one at a time.

## Upgrading and rolling back

A release replaces the program; what OpenDuct keeps - topics, the studio's accounts, workflows, credentials and runs -
stays on its disks and is read by the new release. Upgrade one node at a time, as [upgrades](upgrades.md) says for
the brokers: a node asked to stop hands its partitions over first, so restarting one at a time leaves no partition
without a leader, and the studio keeps answering on the other nodes.

1. Stop one node (SIGTERM, and give it 30 seconds).
2. Start it again with the new release: same data directory, same settings.
3. Wait until `openduct health --ready` says yes on it - joined, every partition it holds led and level - then the
   next node.

- **systemd**: replace `/usr/local/bin/openduct` and `systemctl restart openduct`. On a machine made by the
  `deploy/opentofu/*-vm` modules, `sudo openduct-upgrade <version>` does it, the release's checksum checked.
- **Docker Compose** (`deploy/compose`): set `OPENDUCT_IMAGE` to the new image, then `docker compose up -d`.
- **Kubernetes**: change the image in `deploy/kubernetes/openduct.yaml` and apply it; with the Helm chart,
  `helm upgrade openduct deploy/helm/openduct --reuse-values --set image.tag=<version>`. The StatefulSet restarts one
  node at a time and waits for each to be ready (its readiness probe is `openduct health --ready`) before the next.
- **One node** is down while it restarts: a few seconds. Webhooks sent meanwhile are refused; senders that retry
  (most apps do) deliver them once it is back.

**Rolling back** goes the same way, one node at a time with the release before - possible while the cluster has not
moved to the new release's feature level, that is until the last node is upgraded ([upgrades](upgrades.md#feature-levels)).
After that, roll forward with a release that mends what was wrong. `helm rollback openduct` takes the chart's release
before; on a machine of the OpenTofu modules the release before stays as `/usr/local/bin/openduct.previous`. A
workflow is rolled back on its own, without touching the program: `openduct workflow rollback <name>` (below).

## CI/CD for workflows

A workflow is a file, so it can live in a repository, be reviewed in pull requests, and reach the studio through CI:

```sh
openduct workflow check orders.yaml             # in any CI: no studio needed
openduct workflow test orders.yaml              # its sample run, sending nothing
openduct workflow pull workflows                # the workspace's workflows, a file each (order-alerts.yaml)
openduct workflow diff workflows                # what a push would change, workflow by workflow, step by step
openduct workflow push workflows --dry-run      # the same, as a push would say it
openduct workflow push workflows [--activate] [--prune]
openduct workflow rollback "Order alerts" [--to 3]
openduct workflow promote --all --from Test --to Production
```

- **Signing in**: the commands that reach a studio take `--studio https://flows.example.com` (or `OPENDUCT_STUDIO`)
  and an API token, from `OPENDUCT_TOKEN` or a file given with `--token-file` (its first line; only its owner may read
  it). A token is made by a workspace's admin in the studio, under Settings, API tokens: a role (viewer, editor or
  admin, never above the admin's own), an expiry (90 days unless said, at most a year), and optionally the workflows
  it reaches (names, or beginnings as `billing-*`). The token goes over HTTPS only, unless the studio is on the same
  machine or `--allow-http` says so; the studio must be started with the name it is reached by (`--studio-host`).
- **Pull** writes one file a workflow, named after it, `on: true` in the file of each workflow that runs. A workflow
  keeps the file that names it already; `--prune` removes the folder's files that name no workflow.
- **Push** checks every file first and changes nothing if one is wrong, or if the workspace lacks a credential,
  lookup file, data table or other workflow a file names (it says which, for each workflow). New and changed
  workflows are saved as new versions; a new one stays off unless the file says `on: true` or `--activate` is given;
  one that runs goes on running, with its new version; `on: false` turns one off; `--prune` turns off (never
  deletes) the workflows the folder does not have. Credentials are named, never carried: each workspace holds its
  own values under the same names, which is how test and production reach different accounts.
- **Other workflows by name**: a file names the workflow a `run_workflow` step runs (or a `failed` trigger watches)
  by its name, and a push gives it the id it has where it lands - so one folder pushes into any workspace.
- **Environments**: in Settings, Environment, a workspace is marked as development, test or production, with an
  order; promotion goes forward (a lower order to a higher one) and copies each workflow's newest definition as a
  new version, after checking the target has everything it names. The CLI's token is the target's; its maker must
  see the source. The editor's **Promote** button does the same for one workflow. A production workspace can take
  changes only by push or promotion: with the switch on, people cannot add, change or delete its workflows in the
  studio (they can still turn one off).
- **History**: every version shows where it came from - the studio, a push, a promotion (`from Test v4`), a
  rollback (`to v2`) or Git - and the API token it was made with. Every request made with a token is written to the
  log with the token's name.
- **For CI**: `--json` prints the studio's answer as one JSON document. Exit codes: `0` done; `1` failed (the studio
  out of reach, a refusal); `2` refused before anything changed (a wrong file, something missing); `3` a push would
  change something (`diff --exit-code`); `4` saved in part (the answer says which workflows were).
- **Ready-made CI**: [examples/ci](../examples/ci) has a GitHub Actions workflow and a GitLab CI file: check and
  test on every pull request, push to test after a merge, promote to production on a tag or by hand, each with the
  released program or container and the tokens as the CI's secrets.

### Git sync

A workspace can keep its workflows in a repository on GitHub (or GitHub Enterprise Server), GitLab or Gitea, through
their REST APIs (no git program). In Settings, Git: the repository's https address, a branch, a folder, and a bearer
credential of the workspace holding a token that may read and write the repository.

- **Save to Git** writes each workflow as `pull` does and commits the files that changed, in one commit: to the
  branch, or (with "as a pull request") to a new branch with a pull request (merge request) into it.
- **Deploy from Git** reads the branch's folder and pushes it as `push` does: by hand, or on each push to the branch.
  For the latter, make the webhook's secret in Settings and add the webhook to the repository (address
  `https://<studio>/hooks/git/<workspace>`, push events, JSON, the secret). GitHub's and Gitea's deliveries are
  checked against their HMAC-SHA256 signature; GitLab's against its signing token when given (`webhook-signature`,
  at most five minutes old), else its secret token. An unsigned or forged delivery is refused.
- The last deploy - when, which commit, whether it was done or what was wrong - is shown in Settings.

What Git sync does not do yet: it never deletes a file (the file of a workflow deleted in the studio stays until
removed in the repository), a deploy never turns off the workflows the folder lacks (use `push --prune` from CI), a
commit to a branch that moved meanwhile is refused rather than merged, it signs in with a token only (no GitHub App),
Bitbucket and Azure DevOps are not among its hosts, and a delivery is answered once its deploy is done (a folder of
many files takes as many requests to read). Limits: 500 workflow files a folder, 512 KiB a file, and 1 MiB a push.

## When a copy is lost

A broker checks every copy before it serves it. Its replication log holds nothing before the last snapshot, so a copy
can be rebuilt on its broker only from a snapshot on that disk. When there is none - a lost disk, a damaged
directory - the broker sets the copy aside whole under `<data-dir>/set-aside/` (kept; nothing removes it), takes the
partition up again empty, and is caught up from the partition's leader. Until it is level it votes for no one and
does not stand for election, so it can never help elect a leader that is missing what it once acknowledged. This
needs nobody: one broker's loss repairs itself from the others. The same holds for a broker's copy of the cluster's
own state.

A snapshot that is there but cannot be read (a failed read, or one written by a newer build) is never removed; the
copy does not start until it reads.

### When most copies are lost

With most copies of a partition lost, the ones left cannot elect a leader: the brokers being rebuilt do not vote.
That is deliberate, and it means the partition waits. Nothing is removed while it waits.

A person may decide that the copies which kept their log are to be taken as they are. Start **every** broker with

    openduct serve ... --accept-surviving-copies <name>

where `<name>` is any name for this recovery (a date will do; `OPENDUCT_ACCEPT_SURVIVING_COPIES` in the environment
is the same). The brokers being rebuilt then vote, a copy that kept its log leads, and the others are rebuilt from
it. A write that only the lost copies held is gone - that is what is being accepted.

The name holds for one recovery. Once every partition a broker holds has a leader and is level, the broker writes the
name into `<data-dir>/recoveries-done` and it does nothing from then on, so it may stay in your settings. A later loss
needs a new name: it is never settled by a switch somebody left on.

A partition the surviving broker was leading recovers without any of this: it still holds its term, needs nobody's
vote, and the emptied copies are caught up from it.

### When no copy is left

It can happen that every copy of a partition lost what could rebuild it. `--accept-surviving-copies` does nothing for
such a partition - there is no surviving copy to accept - and it waits. `openduct health --ready` on any broker says
how many of its copies are being rebuilt; the same number on every broker, never going down, is this case.

A person may decide that such a partition is to be started again empty. Have **every** broker running, then start
them all with

    openduct serve ... --start-empty-where-no-copy-survived <name>

(`OPENDUCT_START_EMPTY_WHERE_NO_COPY_SURVIVED` in the environment is the same.) It includes
`--accept-surviving-copies`: a partition with a copy that kept its log is led by that copy within moments and the
others are rebuilt from it, exactly as above, and nothing of it is lost. Only a partition that has had no leader for
thirty seconds is started again from nothing.

What you are accepting: everything such a partition held is gone. So is what a copy that kept its log, but could not
be reached during those thirty seconds, alone held - which is why every broker should be up first. What was on each
disk is not removed: it stays under `<data-dir>/set-aside/`.

The name holds for one recovery, as above.

## Stopping a broker

A broker asked to stop (`SIGTERM`, or Ctrl-C) first gives away what it leads: for each partition it leads, the
follower that is furthest along takes over, and the broker goes on answering until it has. From that moment it takes
no leadership back - it refuses any that is offered and stands for no election - however long it takes to stop. A
restart of one broker at a time therefore leaves no partition without a leader. It allows ten seconds for the hand-over
and five for the requests in flight, then stops whatever is left. Give it at least that long before it is killed
(Kubernetes' default of thirty seconds is enough).

A broker that comes back gets its partitions back: each goes to its first broker two minutes after another broker took
it over, once the returning broker has caught up - long enough that a broker that is only stopping is never handed
anything. The wait is `--hand-back-after-secs` (`OPENDUCT_HAND_BACK_AFTER_SECS`; 120 by default, 1 to 3,600):
keep it longer than a broker takes to stop.
