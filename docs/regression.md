# Test results

The broker's test suites as last run in full, what each covers, and what has been measured. Every change runs all of
them in CI (`cargo xtask check`); this page is a snapshot to read without running them.

## The last full run

- **When:** 2026-11-17, commit `171ba6a`.
- **Where:** a laptop - Apple M2 Pro (arm64), 10 cores, 16 GB; macOS; rustc 1.100.0; debug build.
- **Run:** `cargo test --no-fail-fast -p openduct-storage -p openduct-broker -p openduct-raft -p openduct-hostile -p openduct-value`
  (3 min 30 s), then `cargo test --no-fail-fast -p openduct --features studio,pipelines --test cli --test cluster
  --test kafka --test studio_cluster --test start --test runs_at_once` (2 min 3 s).

**177 passed, 1 failed** (a timing failure under load, below), in 26 suites.

### Storage, broker and replication

| Suite | What it covers | Passed | Time |
|---|---|---|---|
| `openduct-storage` unit tests | Record batches and their checks; the partition index; idempotent producers; the file store's segments, `fsync`, recovery from torn writes and damage in place, interrupted creates and deletes, retention | 45 | 1.0 s |
| `openduct-broker` unit tests | Framing, request limits, each API's encoding and decoding, the group coordinator's states | 37 | 2.4 s |
| `broker/e2e` | Over TCP with an independent Kafka client (rskafka): create, list, delete; produce and fetch; long polls; durability across restarts; consumer groups and commits; idempotent retries; hostile frames | 15 | 0.5 s |
| `broker/security` | TLS 1.3, SCRAM-SHA-256/512, OAuth bearer tokens, client certificates, PLAIN over TLS only, access rules that deny by default, quotas | 8 | 35.0 s |
| `broker/configs` | DescribeConfigs, IncrementalAlterConfigs and AlterConfigs: what can change (retention by time and by size, the largest batch, the cleanup policy, timestamps, copies in sync, segment time) and what is refused; a topic created with settings; a batch too large for its topic; a compacted topic read back over the wire, plain and in each of the four codecs; a record without a key refused | 9 | 0.7 s |
| `broker/group_admin` | DescribeGroups, ListGroups, DeleteGroups, OffsetDelete | 2 | 0.0 s |
| `broker/partitions` | CreatePartitions: added, refused, validated only | 2 | 0.1 s |
| `broker/routed` | A client that follows leaders across brokers | 1 | 0.0 s |
| `broker/shared` | One set of connections for many readers and writers: readers of a partition sharing one fetch; a deleted topic failing only its own readers | 2 | 5.4 s |
| `openduct-raft` unit tests | Raft state, membership, log storage | 11 | 0.0 s |
| `raft/cluster` | Three brokers: writes survive a broker lost; leaders spread; brokers join and leave; partitions move while clients carry on; a broker rebuilt from an empty disk | 8 of 9 | 71.7 s |
| `raft/admin` | Reassignments, unregistering a broker, describing the cluster | 3 | 7.5 s |
| `raft/epochs` | Leader epochs and OffsetForLeaderEpoch after a change of leader | 1 | 5.8 s |
| `raft/log_recovery` | The Raft log after a crash: torn tails cut, damage refused | 4 | 0.3 s |
| `raft/quiet` | A one-copy partition's Raft clock stops at rest and starts again when it gains a copy, moves or loses its leader | 1 | 21.1 s |
| `raft/storage_suite` | openraft's own conformance suite against OpenDuct's log store | 2 | 10.6 s |
| `openduct-value` unit tests | Values: JSON in and out, numbers, times | 2 | 0.0 s |
| `hostile/random` | Requests, record batches, segment files, expressions and pipeline files mutated at random with a fixed seed: nothing panics, nothing grows without bound | 11 | 2.4 s |
| `hostile/harness` | The mutator itself | 1 | 0.0 s |
| doc-tests | The library example in the broker's documentation compiles and runs | 1 | 0.0 s |

### The program

| Suite | What it covers | Passed | Time |
|---|---|---|---|
| `cli` | The commands against a running broker: topics, send, read, health | 5 | 3.9 s |
| `registry/serve` | The schema registry over HTTP against a running broker: registered on one node and read on another, forty registrations at once through two, a token asked for, HTTPS beside a broker that asks for TLS and sign-in, the broker's users held to rules on subjects, a connection kept for several requests | 6 | 0.1 s |
| `cluster` | `openduct serve` as a three-node cluster: status, rebalance, removing a broker; one schema registry served by all three through the loss of the broker that led its topic | 5 | 36 s |
| `kafka` | Pipelines reading from and writing to another Kafka cluster (here, another OpenDuct) | 4 | 3.6 s |
| `start` | `openduct start`: the broker and the studio come up; secrets refused from the environment | 2 | 1.1 s |
| `runs_at_once` | A workflow's runs: many at once, in order per key, nothing skipped across a restart | 1 | 18.4 s |
| `studio_cluster` | The studio on three nodes: the same workspaces everywhere, runs shared, schedules once, a node lost | 1 | 68.8 s |

### The one failure

`raft/cluster: the_first_broker_back_with_an_empty_disk_rejoins_rather_than_starting_a_new_cluster` waited 60 s for a
broker rebuilt from an empty disk to catch up, and in that time it had not learned who leads the controller. It is
intermittent:

| Runs of `raft/cluster` (its 9 tests side by side) | Failed |
|---|---|
| At `171ba6a`, in the full run and 15 runs of the file since | 2 of 16 |
| At `171ba6a`, the test on its own | 0 of 3 |
| Before resting partitions (`9a98418`), the file | 0 of 8 |

Eight clean runs before resting partitions do not show that change caused it: at the rate seen since, eight in a row
pass about one time in three. A broker that waits a minute without a controller leader is a fault to find and fix,
not only a slow machine; until it is, this test can fail now and then.

## Not in that run

- **Connectors against real services** (PostgreSQL, MySQL, MongoDB, MQTT, OPC UA, Modbus, Kafka, Elasticsearch, S3)
  need their containers: `cargo xtask services up`, then `eval "$(cargo xtask services env)"` before `cargo test`.
  Without them those tests compile and skip.
- **Fuzzing** runs in CI with cargo-fuzz, on every target in [fuzz](../fuzz/README.md).
- **Apps** are tested by their recorded exchanges: `openduct app test` (98 pass). Which apps have also been run
  against live accounts is in each app's README.

## Measured

| What | Result | How |
|---|---|---|
| Start | Listening about 6 ms after launch | Release build, arm64 laptop |
| Idle | 9 MiB as a broker alone; 11 MiB with pipelines and connectors | As above |
| Binary | 11 MB as a broker alone; 26 MB with pipelines and connectors | `cargo build --release` |
| Workflows on one node | 1,000 workflows idle in 113 MiB, 8% of a core, 86 connections; about 8,000 runs a second in a burst (2026-11-16) | `cargo test --release -p openduct --features studio --test load -- --ignored --nocapture` |
| Resting partitions | 6,000 idle one-copy partitions on a node: from 2.5 cores to 0.5% of one (2026-11-16) | `raft/quiet`, and a node with 6,000 partitions watched with `ps` |

## Running them yourself

```sh
cargo xtask check                                  # format, lint, every test, licences - what CI runs
cargo test -p openduct-broker -p openduct-raft     # the broker and replication alone
cargo xtask services up && eval "$(cargo xtask services env)" && cargo test   # with the connectors' services
```
