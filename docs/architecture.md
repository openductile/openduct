# How OpenDuct is built

OpenDuct keeps two things apart: speaking the Kafka protocol, and keeping data safe. The broker turns requests
into calls on a `Store`; a store keeps the log and knows nothing about the network. A new kind of storage is a
new `Store`, with no change to the protocol side.

```text
 Kafka clients ──TCP──▶ openduct-broker ─────────────────────────▶ openduct-storage
                        server.rs    frames, limits, dispatch      Store trait
                        handlers.rs  one function per Kafka API    MemoryStore
                        groups.rs    the consumer-group coordinator FileStore (segments, recovery)
                        client.rs    a small client for the CLI     batch.rs, partition.rs
                                │
 openduct (CLI) ─────────────────┘  start, serve, topic, pipeline, workflow, app, cluster, health

 browser ──HTTP──▶ openduct-studio ──▶ openduct-flow (workflows, apps) ──▶ the broker's topics
```

## Crates

| Crate | What it holds |
|---|---|
| `openduct-storage` | The `Store` trait, record-batch parsing and checking (`batch.rs`), the per-partition index with the idempotent-producer rules (`partition.rs`), the in-memory store and the file store. No networking, no async. |
| `openduct-raft` | Replication: Raft groups (the controller, and one per partition), their logs on disk with one shared flusher, the state machines and their hard-link snapshots, the multiplexed connections between brokers, and the replicated `Store` the broker serves in a cluster. |
| `openduct-broker` | The TCP server, the request handlers, the group coordinator and a client (`client::Shared`: one set of connections for a node's workflows). Async on Tokio. |
| `openduct-value` | The values a message carries: JSON in and out, numbers, times. |
| `openduct-expr` | CEL expressions with OpenDuct's limits on time, size and depth ([expressions](expressions.md)). |
| `openduct-pipeline` | Pipeline files: what each message becomes; secrets named as `secret:NAME` ([pipelines](pipelines.md)). |
| `openduct-connect` | The connectors: sources and sinks, and the private-network rule (`reach`) ([connectors](connectors.md)). |
| `openduct-flow` | Workflows: the model, each run's steps, the runner that reads a workflow's topic, and the apps' engine (`app_files`) that runs every [app](apps.md) from its definition. |
| `openduct-script` | A code step's snippet, run in a locked-down process of its own. |
| `openduct-studio` | The studio's HTTP server and page: accounts, workspaces, credentials sealed with the cluster's key, webhooks, runs and the apps' sign-ins ([studio](studio.md)). |
| `openduct-hostile` | Random mutation of input, for the tests of every reader of untrusted bytes. |
| `openduct` | The command-line tool. |

## A request's life

1. **Accept.** The server accepts a connection if one of `--max-connections` slots is free. Otherwise the
   connection waits for one.
2. **Frame.** It reads the 4-byte size and refuses anything over `--max-request-mib` before reading further. The
   size is then reserved from the broker-wide request budget (`--max-request-memory-mib`), whole. When the budget
   is spent, reading waits, so request memory never grows past it. Requests cannot deadlock one another: none
   holds part of the budget while waiting for more. The body must arrive within `--request-read-timeout-secs`.
3. **Decode.** It reads the request header, checks the API and version against the supported list, and decodes
   the body. A list, string or byte string that claims more entries or bytes than the request holds is refused
   before memory is reserved for it. Anything malformed closes that one connection.
4. **Handle.** A handler does the work. Store calls run on Tokio's blocking pool, because they may wait on the
   disk, so a slow disk never stalls the network threads.
5. **Answer.** Answers on a connection go back in the order the requests arrived, as the protocol requires. A
   client that stops reading its answers is closed after `--idle-timeout-secs`.

## Producing and fetching

A Produce's records field holds one or more record batches. The broker splits them, checks each header (format,
CRC, record count, producer fields) and hands each batch to the store whole. The store assigns the base offset and
the leader epoch (as a Kafka leader does: 0 for a broker on its own, the Raft term of the leader in a cluster), writes
the batch, and remembers where it is. A Fetch reads the batches back and concatenates them into the answer. Records
are never decoded or re-encoded on the way through. That is why compressed batches cost the broker nothing, and why
a consumer receives the records exactly as the producer sent them.

Each append bumps a counter on a `watch` channel. A Fetch waiting for data waits on that counter, its deadline and
the broker's stop signal, whichever comes first. No polling loop runs.

## The partition index

Each partition keeps a small index entry per batch: base offset, record count, leader epoch, size, newest
timestamp, producer fields, and where the batch is (in memory, or segment and position on disk). Reads, offset lookups by time, and
retention all use the index. The idempotent-producer state (each producer's epoch and last five batches) lives
there too, and the file store rebuilds it from the log on open.

## The file store

Covered in [storage.md](storage.md):

- segments of checksummed frames;
- `fsync` before acknowledging;
- recovery that cuts off only an unacknowledged torn tail, and refuses to open on any other damage;
- atomic settings files;
- one process per directory, enforced by a lock;
- retention by whole segments;
- topic deletes by one atomic rename.

## Consumer groups

The coordinator (`groups.rs`) is a state machine per group: Empty, PreparingRebalance, CompletingRebalance,
Stable. A tick every 100 ms runs it:

- it expires members whose session ran out;
- it completes joins when every known member has rejoined, or the rebalance timeout has passed;
- it applies the initial rebalance delay for new groups.

Members waiting in a join or sync are never expired mid-rebalance. Membership is in memory. Committed offsets go to
the store, so the file store keeps them across restarts.

## Workflows on a node

Each workflow that is on reads its trigger's topic as a consumer group of its own, on every node: the nodes share its
partitions. On a node:

- **Several runs at once.** A workflow has up to `runs_at_once` runs going (10 unless it says, 1 to 50). Records with
  the same key in a partition run one after another, in order; others at once. With `runs_at_once: 1` each record
  runs after the one before.
- **Nothing lost.** A run is kept in the workflow's history (and its record sent to CatchErr when it failed) before
  its place is committed, and a partition is committed only up to the first record whose run has not finished. A crash
  repeats runs; it never skips one.
- **A few connections, however many workflows.** The node's workflows share its connections to the cluster
  (`client::Shared`): calls borrow one of 16 clients, joins one of 64 of their own (a new group waits a few seconds to
  settle), and every workflow waiting for records waits in one request, answered as soon as any of its partitions has
  some. Readers of the same partition share its place in that request.
- **Runs kept together.** Each run's history (and its CatchErr record) is written through the node's shared writer:
  whatever every workflow on the node writes at that moment goes in one request to each leader, each partition's
  messages one batch, up to 8 requests out at once.
- **Measured** (`tests/load.rs`, a laptop, one node, release build, a one-step workflow): 1,000 workflows idle at 113
  MiB, 8% of a core and 86 connections, and run about 8,000 records a second.

`cargo test --release -p openduct --features studio --test load -- --ignored --nocapture` measures how many
workflows a node holds: how long they take to start, what an idle node costs, and runs a second in a burst.

## Resting partitions

A partition with one copy - this broker, leading it - has no follower to show itself alive to and no election to
hold, so its Raft clock (heartbeats, the election timer) is stopped while it is so. It starts again the moment that
changes: a copy added, the partition moved, the leadership lost. Idle, a node holding 6,000 one-copy partitions went
from 2.5 cores to 0.5% of one.

## Shutdown

On SIGTERM or Ctrl-C, `openduct start` stops the studio first: every workflow lets its runs finish (10 s at most),
commits how far it got and leaves its group, while the broker still answers. Then the broker stops accepting
connections and wakes every waiting Fetch. It lets requests in
flight finish, stops the coordinator and retention tasks, and closes the store. That releases the data directory's
lock for the next process.

## Rules the code keeps

- **No `unsafe`.** `#![forbid(unsafe_code)]` in every crate.
- **No panics on input.** Outside tests, clippy refuses `unwrap`, `expect`, `panic!` and unchecked indexing.
  Untrusted bytes are parsed with checked reads that return errors.
- **Every resource is bounded:** request size, request memory, connections, fetch size, every wait, and partitions
  per topic.
- **No C of our own.** The broker alone has no C code to build. Pipelines and connectors use one library with C
  in it, ring (the cryptography for TLS and signing, partly C and assembly), so building them needs a C compiler
  too: one for the board when cross-compiling.
