# Concepts

OpenDuct keeps streams of messages and hands them to whoever reads them. A handful of ideas explain almost
everything it does.

## Messages

A message is a small piece of data: a sensor reading, an order, a log line, a click. OpenDuct does not look inside
it, so it can be JSON, text, CSV or binary. Each message can also carry:

- a **key**: for example the sensor or customer it is about;
- **headers**: small named values, such as a trace id or the content type;
- a **timestamp**: when it happened.

## Topics

A topic is a named stream of messages of one kind, such as `orders`, `sensor-readings` or `audit-log`. Programs
that send messages are **producers**; programs that read them are **consumers**. A topic keeps its messages after
they are read, so many consumers can read the same messages, each at its own pace, and a new consumer can start
from the beginning.

Topic names are 1 to 249 characters made of letters, digits, `.`, `_` and `-`.

## Partitions

A topic is split into one or more partitions, chosen when the topic is created. Partitions are how a topic is
shared out: each partition is read by one member of a consumer group at a time, so more partitions let more readers
work side by side.

- **Order is kept within a partition, not across partitions.** Messages with the same key always go to the same
  partition, so everything about one sensor or one customer stays in order.
- **Choose the count for the readers you will need.** One partition is right for a small stream. A busy topic read
  by a group of ten programs wants at least ten.

## Offsets

Every message in a partition has an offset, its position: 0, 1, 2 and so on. Offsets never change and are never
reused. A consumer remembers where it got to by its offsets, which is how it carries on after a restart without
missing or repeating messages.

## Consumer groups

Several copies of one program can share a topic's work as a **consumer group**. They all use the same group name,
and OpenDuct:

- gives each member its own partitions;
- moves partitions when a member joins, leaves or stops answering;
- keeps each member's progress (its committed offsets), so a new or restarted member carries on where the last one
  stopped.

Different groups do not affect each other. A billing service and a dashboard can each read every order.

## Retention

By default a topic keeps its messages until the topic is deleted. A topic can be given a retention time instead:

```sh
openduct topic create clicks --retention-ms 604800000    # one week
```

Older messages are then removed in whole chunks (segments), so a topic never holds less than its retention time
and the disk use stays bounded.

## Durability

With a data directory, OpenDuct writes every message to disk before it confirms it to the producer. A confirmed
message survives a crash or a power cut. Every read is checked against a checksum, so damaged data is never handed
out as if it were good. [Storage](../storage.md) covers the details.

With `--memory`, nothing is written to disk. That is fast and useful for tests, and everything is gone when OpenDuct
stops.

## Idempotent producers

Networks drop answers. A producer that never heard "stored" sends the message again. With idempotence switched on
(most clients' default), OpenDuct recognises the repeat and stores the message once, so a retry never creates a
duplicate.
