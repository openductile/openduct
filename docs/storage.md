# Storage

OpenDuct keeps data in a **store**. The broker speaks the Kafka protocol; a store keeps record batches safe and finds
them again. Every store gives the same guarantees:

- **A write is acknowledged only once it is as durable as the store promises.** A batch is stored whole or not at
  all.
- **Offsets are dense and never reused.** Each record gets the next offset in its partition.
- **Records come back byte for byte.** OpenDuct never decodes, decompresses or re-encodes the records in a batch. It
  sets the batch's base offset and leader epoch - fields Kafka's checksum does not cover - and nothing else.
- **Idempotent producers are honoured.** A retried batch gets the offset it was first stored at; a gap in sequence
  numbers or a stale producer epoch is refused, so a producer's records are never stored twice or out of order.

## The memory store (`--memory`)

Everything is kept in the process and is gone when it stops. For tests, development and short-lived buffers.

## The file store (`--data-dir <dir>`)

A durable log on local disk, for a single node: an edge gateway, a robot, a single server.

```text
<dir>/LOCK                                   held exclusively while a broker has the store open
<dir>/meta.json                              format version and the next producer id
<dir>/topics/<topic>/topic.json              the topic's settings
<dir>/topics/<topic>/<partition>/<base>.log  segments, named by their first offset (20 digits)
<dir>/topics/<topic>/<partition>/start.json  where the partition starts, once DeleteRecords has moved it
<dir>/groups/<group, hex>.json               a consumer group's committed offsets
<dir>/trash/                                 deleted topics on their way out; emptied on open
```

### Segments and frames

A partition is a sequence of segment files. A segment holds one frame per batch:

```text
[ length: u32 ][ crc32c: u32 ][ the batch, as stored ]      (big-endian)
```

The frame checksum (CRC-32C) covers the stored batch, base offset included. It is checked when the log is opened
and again on **every read**, so a bit flipped on the device is reported, never served. A segment is closed and a new
one started once it reaches `--segment-mib` (64 MiB by default).

### Durability

By default (`--fsync always`) every append is flushed to the device (`fsync`) before Produce answers: nothing
acknowledged is lost on power failure. `--fsync never` leaves flushing to the operating system - faster, but a power
failure can lose recently acknowledged batches. Use it only for data that can be lost.

Settings files (`meta.json`, `topic.json`, `start.json`, group offsets) are replaced atomically: written to a temporary file,
flushed, renamed, and the directory flushed, so a crash leaves the old file or the new one, never a torn one.

### Recovery

When the store opens, every frame of every segment is read back and checked.

| What is found | What happens |
|---|---|
| A damaged frame at the very end of the newest segment - a partial frame, or a bad checksum on the file's last frame | An unfinished write that was never acknowledged (power loss, crash). It is cut off, and the log goes on. |
| Damage anywhere else | Data that was acknowledged has been lost or corrupted on the device. The store **refuses to open** rather than serve a log with a hole in it. |
| A topic directory without `topic.json` | A topic whose creation a crash interrupted. It is skipped and never served. |
| A second process opening the same directory | Refused: the `LOCK` file is held for the store's life. |
| A topic in `trash/` | A topic deleted before the stop whose files were not all removed yet. They are removed now. |

Idempotent-producer state is rebuilt from the batches themselves, so a producer's retry after a broker restart is
still recognised as a duplicate.

### Deleting a topic

A topic is deleted by one atomic rename out of `topics/` into `trash/`, synced before the delete is acknowledged, so
after a crash the topic is either still there whole or gone. Groups' committed offsets on it are removed too, so a
topic of the same name created later starts clean.

### Retention

A topic with a retention time loses whole segments once their newest batch is older than it; one with a retention size
(`retention.bytes`) loses its oldest closed segments for as long as what is left still holds at least that many bytes.
The segment being written to is never removed. Both can be changed while the topic runs (AlterConfigs), and retention
is checked every `--retention-check-secs` (60 by default).

### Compaction

A topic whose `cleanup.policy` has `compact` keeps, of the records with a key, only the newest of each key, at its own
offset; a record with a key and no value deletes the key. Closed segments are cleaned with the retention round, never
the one being written to: what stays of a segment (compressed batches again with their producer's codec) is written to
a file beside it, flushed, and renamed over it, so a crash leaves the segment as it was or as it now is, whole.
Offsets do not move, so a segment keeps its name. `segment.ms` closes a segment that spans that long, so a quiet topic
is still cleaned. In a cluster each partition's leader orders the pass and every copy cleans alike.
[protocol](protocol.md#log-compaction) has the rules a client sees, transactions included.

## Replicated (`--brokers`)

Brokers of a cluster keep their data together with Raft (the openraft library), in groups:

- **The controller**, over every broker, holds the cluster's state: topics and, per partition, the brokers holding
  it; producer ids; consumer groups' committed offsets. Its leader coordinates the consumer groups.
- **Each partition** is a group of its own over the brokers holding it (`--replication-factor`, three by default),
  led by one of them. A topic's partitions are placed round the brokers and led first by their first broker, so
  leaders - and the work - spread over the cluster.

Every change is an entry of a group's log: proposed by its leader, answered once most of the group has it on disk,
and applied in the same order by every member, so every copy is the same. Nothing in an entry depends on the broker
applying it: a topic's id and placement, and the time retention is enforced at, are chosen by the leader. A
partition's snapshot carries its producer table, which outlives the batches retention removes, so every copy decides
a producer's retry alike.

Under the data directory:

| Path | What it is |
|---|---|
| `broker.json` | Which broker this directory is, of which cluster, and which copy of it (a new one each time the broker starts with an empty disk). |
| `controller/raft/`, `partitions/<topic id>/<partition>/raft/` | A group's Raft log: entries in segments of frames (length, CRC-32C, the entry), with `vote` (flushed before a vote is answered), `purged` (flushed before older segments go) and `committed`. One flusher serves every group's log: one flush per file for everything waiting at that moment. |
| `controller/state/` | The controller's snapshots (`state.json`). Its state is small and kept in memory, rebuilt on start from the newest snapshot and the log after it. |
| `partitions/<topic id>/<partition>/store/live-<n>/` | The partition's log being served. It is never flushed: on every start it is rebuilt from the newest snapshot and the Raft log after it is applied again. |
| `.../store/snapshots/snap-<n>/` | A partition's snapshot: hard links to its segments with their lengths at one moment, and `producers.json` (`snapshot.json` records them, written last). Taking one costs a link per segment; the Raft log before it is then removed, keeping a little for copies just behind. |

A copy that is too far behind for the log is sent the leader's snapshot, file by file, and the log after it.

**A broker back with an empty disk** is a new copy of itself (`broker.json` is new). Before it starts anything it
asks the others whether the cluster exists, so broker 1 with an empty disk rejoins rather than starting a new
cluster. Each leader, seeing the new copy, lets that broker's log go back to empty once (Raft refuses a log that
goes back otherwise) and rebuilds it. Until each of its copies has applied what its leader has committed, it votes
for no one: an empty copy must not help elect a leader missing writes this broker acknowledged before.

## Tiered storage (`--tier-s3-bucket`)

In a cluster, each partition's leader copies its closed segments to an S3 bucket (AWS, or an S3-compatible store
with `--tier-s3-endpoint`) and records each one in the partition's own log, so every copy - and every snapshot -
knows which offsets are in the bucket. Once a segment is stored, local copies past `--tier-local-retention-mib`
(or older than `--tier-local-retention-hours`) are dropped on every broker, again by an entry of the partition's
log. A read of an offset no longer on the local disk fetches its segment from the bucket into a small cache beside
the partition (`remote-cache/`, a few segments). Retention removes segments from the bucket as it does from disk.

- Appends never wait on object storage; a segment is stored once it is closed (`--segment-mib`).
- A broker rebuilt from a snapshot gets only the local segments; older offsets come from the bucket when read.
- Every broker must use the same `--segment-mib`: which offsets are stored follows segment boundaries.
- Keys: `<prefix>/<topic id>/<partition>/<first offset>-<last offset>.log` (prefix `openduct/<cluster id>` by
  default). Credentials as the AWS tools find them: the environment, a web identity (EKS service accounts), the
  container endpoint (ECS tasks, EKS pod identity), the instance's metadata (EC2).
