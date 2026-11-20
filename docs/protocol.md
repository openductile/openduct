# The Kafka protocol in OpenDuct

OpenDuct answers the Kafka APIs below. A client asks which versions a broker supports (ApiVersions) and picks the
newest that both support. OpenDuct caps a version where a newer one would need something it does not have yet
(topic ids, transactions, the KIP-848 consumer protocol), so clients settle on a version that works.

A request for an API or version not listed here closes that client's connection. Well-behaved clients never send
one, because ApiVersions does not offer it. Nothing a client sends can stop the broker.

| API | Key | Versions | Notes |
|---|---|---|---|
| Produce | 0 | 3–12 | See [Producing](#producing). |
| Fetch | 1 | 4–12 | See [Fetching](#fetching). |
| ListOffsets | 2 | 1–7 | Earliest, latest, by timestamp, and the record with the largest timestamp (v7). From v4, with the leader epoch of the batch holding the offset (past the last batch, the epoch now). |
| DeleteRecords | 21 | 0–2 | A partition starts at the offset asked from now on (`-1`: its high watermark), answered with its new low watermark (Delete on the topic). Reads before it answer `OFFSET_OUT_OF_RANGE`; segments holding only offsets before it are removed; offsets go on from where they were. Never moved back; past the high watermark: `OFFSET_OUT_OF_RANGE`. Kept across restarts, and in a cluster resolved by the partition's leader and replicated, so every copy - one rebuilt from a snapshot too - starts at the same offset. |
| Metadata | 3 | 0–12 | Each partition's leader, replicas and leader epoch: the broker itself, or in a cluster the leader of the partition's group (`LEADER_NOT_AVAILABLE` during an election). It creates topics on request only when `--auto-create-topics` is on. |
| OffsetCommit | 8 | 2–8 | Committed offsets are kept by the store, durably for the file store. |
| OffsetFetch | 9 | 1–8 | One group (v1–7) or several (v8). Never-committed partitions answer `-1`. |
| FindCoordinator | 10 | 0–4 | A group's coordinator: on its own, the broker itself; in a cluster, the leader of the group's partition of the internal `__consumer_offsets` topic, so groups spread over the brokers (other brokers answer group requests `NOT_COORDINATOR`). Transaction coordinators: `COORDINATOR_NOT_AVAILABLE`. |
| JoinGroup | 11 | 0–9 | See [Consumer groups](#consumer-groups). |
| Heartbeat | 12 | 0–4 | |
| LeaveGroup | 13 | 0–5 | One member (v0–2) or several (v3+). |
| SyncGroup | 14 | 0–5 | |
| DescribeGroups | 15 | 0–6 | A group's state, protocol and members (client id, host, metadata, assignment), as `kafka-consumer-groups --describe` shows them. A group with committed offsets and no members is `Empty`; one that does not exist is `Dead` (before v6) or `GROUP_ID_NOT_FOUND` (v6). Describe on the group; the operations allowed (v3+) when asked. A group another broker coordinates answers `NOT_COORDINATOR`. |
| ListGroups | 16 | 0–5 | The groups this broker coordinates - with members, or with committed offsets only - filtered by state (v4+) and type (v5+; every group here is `classic`). In a cluster a client asks every broker. Describe on the cluster lists them all; otherwise only the groups the principal may describe are named. |
| DeleteGroups | 42 | 0–2 | A group with no members forgotten, with its committed offsets (Delete on the group). One with members answers `NON_EMPTY_GROUP`; one that does not exist, `GROUP_ID_NOT_FOUND`. |
| OffsetDelete | 47 | 0 | A group's committed offsets of some partitions removed (Delete on the group, Read on the topic); the group goes with its last one. In a consumer group with members, the offsets of a topic they subscribe to are kept (`GROUP_SUBSCRIBED_TO_TOPIC`); another kind of group with members answers `NON_EMPTY_GROUP`. In a cluster the removal is replicated like a commit. |
| ApiVersions | 18 | 0–3 | A version newer than 3 is answered as v0 with `UNSUPPORTED_VERSION` and the real list, as Kafka does, so the client retries with one it can use. |
| CreateTopics | 19 | 2–7 | See [Topics](#topics). |
| DeleteTopics | 20 | 1–6 | By name, or by id (v6). An id no topic has answers `UNKNOWN_TOPIC_ID`. |
| CreatePartitions | 37 | 0–3 | Partitions added to a topic, to the count asked (Alter on the topic); each new one starts empty and, in a cluster, is placed as a new topic's are, or on the brokers named (the first to lead it). Fewer or the same: `INVALID_PARTITIONS` (partitions are never taken away); placements that do not add up or name an unknown broker: `INVALID_REPLICA_ASSIGNMENT`. The offsets topic's partitions are fixed. `validate_only` checks without adding. Keys already written keep their partitions; new ones may spread differently, as with Kafka. |
| InitProducerId | 22 | 0–4 | An idempotent producer is given a new producer id. With a transactional id (see Transactions below) the id's producer id and its next epoch: the id's former holder is fenced, and what it left open is aborted. |
| AddPartitionsToTxn | 24 | 0–5 | These partitions are part of the producer's transaction (Write on the transactional id). Versions 4 and 5, as brokers use it: several transactions in one request, each added to or only asked about (asking needs ClusterAction on the cluster). |
| AddOffsetsToTxn | 25 | 0–3 | This group's offsets will be part of the transaction. |
| EndTxn | 26 | 0–5 | Commit or abort: answered once the marker is on every partition and, on a commit, the offsets are the group's. Version 5 (the second transaction protocol) also gives the producer its next epoch. |
| TxnOffsetCommit | 28 | 0–5 | The offsets the transaction commits for a group: the group's only if it commits. Also Read on the group. |
| WriteTxnMarkers | 27 | 0–1 | A commit or abort marker written to partitions directly: how an operator ends a transaction no coordinator will (`kafka-transactions abort`). ClusterAction or Alter on the cluster. |
| DescribeProducers | 61 | 0 | The producers a partition knows and where the open transaction of each starts; asked of the partition's leader. Read on the topic. |
| DescribeTransactions | 65 | 0 | A transactional id's stage, producer, timeout, start and partitions; asked of the coordinator. Describe on the id. |
| ListTransactions | 66 | 0–2 | The transactional ids this broker coordinates that the principal may see, by stage, producer id, how long open, or a pattern the id matches. |
| OffsetForLeaderEpoch | 23 | 2–4 | Where a leader epoch ended: the first offset written in a later epoch, or the end when none is. Consumers ask it after a leader change, to check that nothing they read was taken back; nothing committed ever is. An epoch after the current one answers `-1` and `-1`. A `current_leader_epoch` behind the leader's answers `FENCED_LEADER_EPOCH`, one ahead `UNKNOWN_LEADER_EPOCH`; the client refreshes its metadata and asks again. Describe on the topic. |
| DescribeConfigs | 32 | 1–4 | A topic's settings (DescribeConfigs on the topic) or this broker's (by its id; DescribeConfigs on the cluster): only settings OpenDuct acts on, with the values it acts on - `cleanup.policy` `delete`, `retention.ms` (`-1`: kept for ever), `max.message.bytes`, `compression.type` `producer`, `message.timestamp.type` `CreateTime` for a topic; limits, defaults and group timeouts for a broker. A topic's `retention.ms` can be changed (IncrementalAlterConfigs); every other setting is read-only. Synonyms (v1+) and types and documentation (v3+) when asked. A topic the principal may not see is refused whether it exists or not. Broker loggers are not served. |
| IncrementalAlterConfigs | 44 | 0–1 | A topic's `retention.ms` (`-1`, for ever), `retention.bytes` (the most bytes a partition keeps; `-1`, no limit), `max.message.bytes` (the largest record batch it takes), `cleanup.policy` (`delete`, `compact`, `compact,delete`), `delete.retention.ms`, `min.compaction.lag.ms`, `message.timestamp.type` (`CreateTime`, `LogAppendTime`), `min.insync.replicas` and `segment.ms`: each set, or deleted (back to its default); AlterConfigs on the topic. A request with one setting that cannot be taken changes none of them. Any other setting answers `INVALID_CONFIG` naming it - one OpenDuct would store and ignore is refused, not kept - a broker's `INVALID_REQUEST` (set where it starts). `validate_only` checks without changing. Retention is acted on from its next round; a batch larger than `max.message.bytes` is refused with `MESSAGE_TOO_LARGE`. In a cluster every change goes through the controller to every broker, and each partition's leader decides retention for all its copies alike; `retention.bytes` and `max.message.bytes` are taken once every broker runs a release that knows them (feature level 2 - see upgrades.md), `cleanup.policy=compact`, its two settings and `segment.ms`'s effect at level 3, and answer `INVALID_CONFIG` saying so until then. |
| AlterConfigs | 33 | 0–2 | The older form of the same, still what many tools send: a topic's settings REPLACED by the ones named - any it does not name goes back to its default, `retention.ms` included. The same settings, checks and refusals as IncrementalAlterConfigs. |
| AlterPartitionReassignments | 45 | 0–1 | In a cluster, moves partitions onto the brokers given (the first one to lead). A move under way is not cancelled. A broker on its own refuses: `INVALID_REPLICA_ASSIGNMENT`. |
| ListPartitionReassignments | 46 | 0 | The moves under way, with the brokers being added and removed. |
| UnregisterBroker | 64 | 0 | In a cluster, takes a broker out once no partition is on it any more. |
| DescribeCluster | 60 | 0–2 | The cluster id, the brokers and the controller - on its own, the broker itself; in a cluster, the controller's leader - and the operations allowed on the cluster when asked. Brokers only: the controllers' endpoint type answers `MISMATCHED_ENDPOINT_TYPE`. |
| SaslHandshake | 17 | 1 | The sign-in mechanisms the broker takes; see [security](security.md). |
| SaslAuthenticate | 36 | 0–2 | SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, and PLAIN over TLS only. |

## Producing

- **Record batches pass through untouched.** OpenDuct stores each batch byte for byte as the producer sent it,
  setting only its base offset and its leader epoch, as a Kafka leader does: 0 for a broker on its own, the term of
  the partition's leader in a cluster - the same on every copy. The batch's CRC covers neither, so it stays valid. Compressed
  batches (gzip, snappy, lz4, zstd) are stored compressed; consumers decompress them as they would from Kafka.
- **Every batch is checked first.** A wrong CRC answers `CORRUPT_MESSAGE`. A batch older than the v2 format
  (magic 0 or 1) answers `UNSUPPORTED_FOR_MESSAGE_FORMAT`. A malformed one answers `INVALID_RECORD`. Nothing is
  stored from a refused batch.
- **acks.** `acks=1` and `acks=-1` (all) are answered once the batch is as durable as the store promises; for
  the file store, that is on disk after `fsync`. There are no replicas to wait for, so both mean the same.
  `acks=0` is stored the same way but, as the protocol says, never answered. Any other value answers
  `INVALID_REQUIRED_ACKS`.
- **Idempotent producers.** Each producer's last five batches per partition are remembered, and the file store
  rebuilds this from the log after a restart:
  - a retry of a batch already stored is answered with its original offset, and nothing is stored twice;
  - a gap in the sequence answers `OUT_OF_ORDER_SEQUENCE_NUMBER`;
  - an older producer epoch answers `PRODUCER_FENCED`.
- **Transactions** work, on one broker and in a cluster (below).
- A request is refused before any of it is read if it is over `--max-request-mib`.

## Fetching

- A Fetch returns batches exactly as stored, from the batch holding the requested offset onwards. Clients skip
  the records before their offset, as they do with Kafka.
- **Long polling.** A Fetch with too little data waits up to its `max_wait_ms`, capped at 30 seconds by default.
  It answers as soon as anything is appended, or at once when the broker is stopping.
- **Sizes.** An answer carries at most `--max-fetch-mib` (55 MiB by default) whatever the client asks, and each
  partition at most its `partition_max_bytes`. The first batch is always sent whole, even if it is larger, so a
  consumer is never stuck behind one large batch. After that, a partition whose next batch would overfill the
  answer reports its offsets with no records, and the client fetches it next time.
- **No fetch sessions.** Every Fetch is a full fetch (session id 0), which every client supports.
- `read_committed` (`isolation_level` 1) is given nothing at or after the last stable offset - the first record of
  the oldest transaction still open - and the aborted transactions in what it is given, each by its producer and
  first offset, for the client to skip. `read_uncommitted` is given every batch. Both are told the last stable
  offset, and to ListOffsets a `read_committed` consumer's end is that offset. With no transaction open the two
  read the same.


## Transactions

A producer with a `transactional.id` writes to several partitions, and commits a consumer group's offsets, so that
all of it becomes visible together or none of it does - Kafka's exactly-once.

- **The coordinator** gives a transactional id its producer id and an epoch. Initialising the id again raises the
  epoch: the former holder is refused by the coordinator and by every partition it wrote to (`PRODUCER_FENCED`),
  and whatever it left open is aborted.
- **A transaction**: AddPartitionsToTxn, the records (marked transactional, in a Produce that names the id),
  AddOffsetsToTxn and TxnOffsetCommit for a group's offsets, then EndTxn. The coordinator records its decision,
  writes a commit or abort marker to every partition, makes the offsets the group's on a commit, and records that
  it is done. A decision recorded is carried out even if the broker stops in between.
- **The second transaction protocol** (Kafka 4: the feature `transaction.version` at 2, which ApiVersions
  announces once the store keeps transactions). A partition is part of a transaction by the transaction's
  first record there (Produce 12 and later): its leader tells the coordinator before it writes the record, and
  the producer sends no AddPartitionsToTxn or AddOffsetsToTxn. EndTxn 5 gives the producer its next epoch at the
  end of every transaction - the markers carry it, so whatever is late from the transaction before is refused
  by the coordinator and by each partition - and a producer that did not hear the answer and asks again is
  answered the same. When an id's epochs run out it goes on with a new producer id.
- **Reading.** `read_committed` sees a transaction's records only once it is committed, never if it is aborted,
  and nothing written after a transaction still open until that one ends (see Fetch above).
- **Timeouts.** A transaction open longer than its `transaction.timeout.ms` (at most 15 minutes) is aborted and
  its producer fenced - also one left open by a producer that never comes back, across a broker restart.
- What the coordinator knows is kept in the internal topic `__transaction_state` (compacted).
- **In a cluster** the topic has 16 partitions and each transactional id belongs to one (by the hash of the id
  Kafka uses), so the ids are shared out over the brokers: an id's coordinator is the broker leading its
  partition. FindCoordinator names it, and any other broker answers `NOT_COORDINATOR`. Markers reach the partitions other brokers lead, and a group's offsets its
  own coordinator, over the brokers' replication links. When the coordinator's broker stops, the topic's next
  leader reads it and carries on: what was decided is finished, what was open stays open until its producer
  ends it or its timeout does. A new cluster keeps transactions a few seconds after its first start, and an
  upgraded one once every broker is upgraded (feature level 4, [upgrades](upgrades.md)); until then the
  coordinator answers `COORDINATOR_NOT_AVAILABLE`, which clients wait out.

What it does not do yet:

- A partition takes a transaction's records from any producer whose request names a transactional id it may
  write with; it does not ask the coordinator first whether the partition was added. A transaction a producer
  leaves that way - written, and no coordinator told - is aborted by the partition's leader once it has been
  open longer than any transaction may run (16 minutes; a coordinator accepts timeouts up to 15), or sooner by
  an operator (`kafka-transactions abort`).

## Log compaction

A topic whose `cleanup.policy` has `compact` keeps, of the records with a key, only the newest of each key. It works
as Kafka's does where it matters to a client:

- **Offsets never move.** A record that stays keeps its offset; where every record of a batch went, an empty batch
  covers its offsets, so a consumer reads straight through.
- **Tombstones.** A record with a key and no value deletes its key. It replaces the older records, and is itself
  removed `delete.retention.ms` after the pass that first cleaned past it - the time a consumer reading from the
  start has to see the delete.
- **`min.compaction.lag.ms`** keeps a record uncompacted at least that long.
- **Transactions.** The records of an aborted transaction are removed and replace nothing; those of a committed
  one are records like any other; those of a transaction still open stay as they are until it ends. Markers stay.
- **Only records with a key are taken.** A batch with a record that has none is refused with `INVALID_RECORD`, as
  Kafka refuses it - compressed batches are opened to see.
- **`segment.ms`** (a week, by default) closes a segment once its records span that long, full or not, so a topic
  with little traffic is still cleaned. It counts by the records' timestamps, when a batch arrives.
- **`compact` alone** keeps the newest records for ever: retention by time and size applies only with `delete`
  (`compact,delete` does both).
- Only closed segments are cleaned, never the one being written to; a pass runs with the retention round.
- **Compressed batches** (gzip, snappy, lz4, zstd) are compacted like any other: what is left of one is compressed
  again with the codec its producer chose. One that does not decompress, or expands past 128 MiB, is kept whole.
- **In a cluster** each partition's leader orders the pass, with its clock and the topic's settings, and every copy
  cleans alike - the copies hold the same bytes afterwards. It starts once every broker runs a release that
  compacts (feature level 3, see upgrades.md); until then the settings answer `INVALID_CONFIG`. A compacted topic
  is not moved to object storage (as in Kafka), and segments already there are left as they were stored.

What it does not do yet: a pass does not run beside a partition's writes - they wait while it cleans (in a cluster,
that partition's writes wait on every copy), which is short for a small log and not for a very large one: about a second for each 300 MiB of closed segments
on a laptop's disk (`how_long_a_pass_takes` in the storage crate measures it).

## Other topic settings

- **`message.timestamp.type=LogAppendTime`**: the broker stamps each batch with its own time as it is stored - the
  timestamp-type bit and the batch's largest timestamp, as a Kafka leader does - and a Kafka client then gives
  every record of the batch that time. OpenDuct's own `messages` and pipelines read through a library that shows
  the producer's timestamp still.
- **`min.insync.replicas`**: a write is acknowledged once a majority of the partition's copies hold it, so any
  number up to that majority is accepted and kept (2 of 3, 3 of 5). More is refused with `INVALID_CONFIG`, as is
  more than 1 on a single broker: a promise nothing would keep.
- **`segment.ms`** applies to every topic, compacted or not.

## Topics

- **CreateTopics:**
  - `retention.ms` sets the topic's retention. Without it, or with `-1`, data is kept until the topic is
    deleted.
  - `cleanup.policy` (`delete`, `compact` or `compact,delete`), `delete.retention.ms`, `min.compaction.lag.ms`,
    `retention.bytes` and `max.message.bytes` are taken as IncrementalAlterConfigs takes them, and a value that
    cannot be taken answers `INVALID_CONFIG` with no topic made. See "Log compaction" below for what a compacted
    topic does today.
  - Other settings are accepted and have no effect: replication factors, segment sizes and similar tuning are the
    broker's and the store's to decide.
  - Any replication factor is accepted. Durability is the store's: for the file store, `fsync` on every
    acknowledged write; in a cluster, each partition is on `--replication-factor` brokers and a write is
    acknowledged once most of them have it on disk.
  - `validate_only` checks without creating.
- **Names** follow Kafka's rules: 1–249 characters from `a-z A-Z 0-9 . _ -`, not `.` or `..`. A topic has 1 to
  10,000 partitions.
- **DeleteTopics** removes the topic, its data, and every group's committed offsets on it. A topic of the same
  name created later starts empty.

## Consumer groups

- The classic group protocol: JoinGroup, SyncGroup, Heartbeat and LeaveGroup.
  - The group leader's client computes the assignment, as with Kafka (range, round-robin, sticky,
    cooperative-sticky).
  - The protocol every member supports is chosen by vote.
  - New groups wait `--group-initial-rebalance-delay-ms` (3 s by default) for more members before the first
    assignment, as Kafka does.
- `MEMBER_ID_REQUIRED` from JoinGroup v4: a new member is given its id and joins again with it.
- Session timeouts from 1 second to 30 minutes are accepted. A member that stops heartbeating is removed after its
  session timeout and the group rebalances. Members waiting in a join or sync are never expired mid-rebalance.
- `group.instance.id` (static membership) is accepted and reported; members are handled as dynamic members.
- **Membership lives in memory; committed offsets in the store.** After a broker restart, consumers rejoin as
  they do after any coordinator change, and carry on from their committed offsets.

## Errors a client may see

| Code | Name | When |
|---|---|---|
| 1 | `OFFSET_OUT_OF_RANGE` | A Fetch before the first kept offset or past the end. |
| 2 | `CORRUPT_MESSAGE` | A batch whose CRC does not match. |
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` | No such topic or partition. |
| 10 | `MESSAGE_TOO_LARGE` | A record batch larger than its topic's `max.message.bytes`. |
| 15 | `COORDINATOR_NOT_AVAILABLE` | The transaction coordinator's topic has no leader now, or the cluster does not keep transactions yet; a partition of the transaction is between leaders. |
| 17 | `INVALID_TOPIC_EXCEPTION` | A topic name outside Kafka's rules. |
| 21 | `INVALID_REQUIRED_ACKS` | acks other than 0, 1 or -1. |
| 22 | `ILLEGAL_GENERATION` | A group request from an old generation. |
| 23 | `INCONSISTENT_GROUP_PROTOCOL` | A member with no protocol in common with the group. |
| 25 | `UNKNOWN_MEMBER_ID` | A member the group does not know (expired, or after a broker restart): rejoin. |
| 26 | `INVALID_SESSION_TIMEOUT` | A session timeout outside 1 s to 30 min. |
| 27 | `REBALANCE_IN_PROGRESS` | The group is rebalancing: rejoin. |
| 30 | `GROUP_AUTHORIZATION_FAILED` | A group the principal may not use (or, for DescribeGroups, describe). |
| 35 | `UNSUPPORTED_VERSION` | An ApiVersions version newer than 3. |
| 36 | `TOPIC_ALREADY_EXISTS` | Creating a topic that exists. |
| 37 | `INVALID_PARTITIONS` | Fewer than 1 or more than 10,000 partitions. |
| 40 | `INVALID_CONFIG` | A topic setting OpenDuct cannot honour, such as compaction. |
| 42 | `INVALID_REQUEST` | A transactional batch in a request that names no transactional id. |
| 43 | `UNSUPPORTED_FOR_MESSAGE_FORMAT` | A batch older than the v2 format. |
| 45 | `OUT_OF_ORDER_SEQUENCE_NUMBER` | An idempotent producer's sequence has a gap. |
| 56 | `KAFKA_STORAGE_ERROR` | The store could not read or write. |
| 68 | `NON_EMPTY_GROUP` | DeleteGroups for a group with members; OffsetDelete in a group with members that is not a consumer group. |
| 69 | `GROUP_ID_NOT_FOUND` | DescribeGroups v6, DeleteGroups or OffsetDelete for a group that does not exist. |
| 74 | `FENCED_LEADER_EPOCH` | An OffsetForLeaderEpoch naming an older leader epoch than the leader's: refresh metadata. |
| 76 | `UNKNOWN_LEADER_EPOCH` | An OffsetForLeaderEpoch naming a newer leader epoch than this broker knows: ask again shortly. |
| 79 | `MEMBER_ID_REQUIRED` | A new group member: join again with the given id. |
| 86 | `GROUP_SUBSCRIBED_TO_TOPIC` | OffsetDelete for a topic the group's members subscribe to. |
| 87 | `INVALID_RECORD` | A malformed batch, or a control batch (a transaction's marker) sent by a client. |
| 90 | `PRODUCER_FENCED` | An idempotent producer's epoch is older than the newest seen. |
| 100 | `UNKNOWN_TOPIC_ID` | A DeleteTopics naming a topic by id. |
| 114 | `MISMATCHED_ENDPOINT_TYPE` | A DescribeCluster for the controllers' endpoint. |
| 115 | `UNSUPPORTED_ENDPOINT_TYPE` | A DescribeCluster for an endpoint type not known. |
