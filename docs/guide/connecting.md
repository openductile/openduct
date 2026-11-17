# Connecting your applications

OpenDuct speaks the Kafka wire protocol. Any Kafka client library, in any language, works with it unchanged: point
the client at OpenDuct's address and use it as you would use Kafka.

## The one setting every client needs

```text
bootstrap.servers = 127.0.0.1:9092
```

Use the address OpenDuct was started with. A client asks OpenDuct where it is and then reconnects to the address it
is told, the one set by `--advertise`. That is why a broker reachable from other machines must advertise an address
those machines can reach ([Running OpenDuct for real](running.md)).

## Recommended producer settings

| Setting | Value | Why |
|---|---|---|
| `enable.idempotence` | `true` (the default in most clients) | A retried send is stored once. |
| `acks` | `all` | Confirmed only once the message is on disk. `1` means the same with OpenDuct. |
| `compression.type` | `none`, `gzip`, `snappy`, `lz4` or `zstd` | OpenDuct stores compressed batches as they arrive, so compressing costs the broker nothing. |
| `linger.ms` | a few milliseconds | Fewer, larger batches: faster, and kinder to flash storage on small devices. |

## Recommended consumer settings

| Setting | Value | Why |
|---|---|---|
| `group.id` | a name for your application | Members with the same name share the work and its progress. |
| `auto.offset.reset` | `earliest` or `latest` | Where a new group starts: the oldest message kept, or only new ones. |
| `enable.auto.commit` | `false` if you commit after processing | Commit only what you have handled, so a crash never skips a message. |

## What works

- Producing, including idempotent producers and every compression type.
- Fetching, with long polling: a consumer waiting for data is answered the moment a message arrives.
- Offsets: earliest, latest, and lookup by timestamp.
- Consumer groups, with every standard assignment strategy (range, round-robin, sticky, cooperative-sticky).
- Creating, listing and deleting topics from a client's admin API.

## What does not work yet

| Feature | What a client sees |
|---|---|
| Transactions (`transactional.id`) | The producer is refused. Use an idempotent producer. |
| Log compaction (`cleanup.policy=compact`) | Creating the topic is refused. |
| Admin calls beyond create, list and delete topics (describe configs, list groups, ...) | The connection is closed. Well-behaved clients do not send them, because OpenDuct does not offer them. |
| TLS and authentication | Not available yet: run OpenDuct on a trusted network. |

[The protocol reference](../protocol.md) lists every API and version OpenDuct supports.

## From a Rust program

The two [examples](../../examples) use the [rskafka](https://crates.io/crates/rskafka) client. The heart of it:

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

## Inside your own program

OpenDuct can also run inside a Rust program, so a device or gateway carries its own broker with nothing else to
install. See the [embedded-broker example](../../examples/embedded-broker/src/main.rs).
