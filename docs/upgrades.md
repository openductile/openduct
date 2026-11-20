# Upgrading a cluster

A cluster is upgraded one broker at a time, with clients producing and consuming throughout, and can be rolled
back the same way. Two things make that safe: every format OpenDuct keeps or sends says its version, and new
behaviour waits until every broker can handle it.

## One broker at a time

1. Stop one broker (SIGTERM; it finishes what is in flight).
2. Start it again with the new release, same data directory, same settings.
3. Wait until it has caught up (`openduct cluster status` shows it holding and leading its share again), then the
   next broker.

While brokers of two releases run side by side, they speak the newest protocol both know: a release speaks its
own protocol between brokers and the one of the release before it. Two releases apart do not talk - upgrade
through each release in turn.

## Feature levels

Each release supports a **feature level**. Every broker registers its release and level with the cluster when it
starts, and the cluster moves to a new level only once **every** broker supports it. A behaviour new in a release -
a new kind of log entry, a new message between brokers, a newer file format - starts only at its level, so a
broker still on the release before never receives anything it cannot read.

The level never goes down. A broker whose release is older than the level in use:

- is refused by every other broker at every connection, so it is sent nothing;
- serves nothing itself: requests to it are answered "upgrade this broker".

So a rollback is possible while the cluster has not moved to the new level - that is, until the last broker is
upgraded. After that, roll forward.

| Level | What starts at it |
|---|---|
| 1 | Everything up to the first release. |
| 2 | A topic's own settings beside `retention.ms`: `retention.bytes` and `max.message.bytes`. Until the last broker is upgraded, setting one answers `INVALID_CONFIG` and nothing changes. |
| 3 | Log compaction: `cleanup.policy` with `compact`, `delete.retention.ms`, `min.compaction.lag.ms`; and `segment.ms` closing segments by time. |
| 4 | Transactions: a partition's log holds a transaction's records and its commit and abort markers. |

## Formats on disk

| What | Where | Format |
|---|---|---|
| Partition segments | `partitions/.../store/live-*/<offset>.log` | Kafka record batches in checksummed frames |
| Replicated logs | `controller/raft/`, `partitions/.../raft/` | each entry's first byte is its format; the active segment has zeros prepared past its entries (cut back on every start) |
| Votes, commit and purge marks | `.../raft/vote`, `committed`, `purged` | JSON with `format` |
| Snapshots | `.../snapshots/snap-*/snapshot.json`, `state.json`, `producers.json`, `tiered.json` | JSON with `format` |
| The broker's identity | `broker.json` | JSON with `format` |

A release reads every format of the release before it; a file of a newer format is refused with "written by a
newer OpenDuct" rather than misread. A release writes a newer format only once the cluster's feature level allows
it, so the release before can read everything while a rollback is still possible.
