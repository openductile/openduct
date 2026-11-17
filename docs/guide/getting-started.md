# Getting started

In about ten minutes you will have OpenDuct running, a topic created, messages sent into it and read back out, and
the data still there after a restart.

## 1. Build OpenDuct

OpenDuct is built from source for now. Install Rust with [rustup](https://rustup.rs), then:

```sh
git clone https://github.com/openductile/openduct
cd openduct
cargo build --release
```

The first build takes a few minutes. The result is one file, `target/release/openduct`, which you can copy anywhere,
for example to `/usr/local/bin`. It needs nothing else to run.

```sh
./target/release/openduct --version
```

```text
openduct 0.1.0
```

The commands below assume `openduct` is on your `PATH`. Otherwise, write `./target/release/openduct` instead.

## 2. Start the broker

```sh
openduct serve --data-dir ./openduct-data
```

```text
INFO openduct::serve: log opened data_dir=./openduct-data fsync=Always
INFO openduct_broker::server: OpenDuct is listening local_addr=127.0.0.1:9092
```

OpenDuct is now listening on port 9092 of this machine, and keeping its data in `./openduct-data`. Every message is
written to disk before OpenDuct confirms it. Leave this terminal open and use a second one for the next steps.

> Only programs on this machine can reach it. That is the safe default. To accept other machines, see
> [Running OpenDuct for real](running.md).

## 3. Check that it answers

```sh
openduct health
```

```text
ok: 127.0.0.1:9092 answers 15 APIs
```

`health` exits with status 0 when the broker answers and 1 when it does not, so scripts and service managers can
use it.

## 4. Create a topic

A topic is a named stream of messages, such as sensor readings, orders or log lines.

```sh
openduct topic create sensors --partitions 3
openduct topic list
```

```text
created sensors with 3 partition(s)
sensors	3 partition(s)
```

Partitions let several readers share the work. Messages with the same key always land in the same partition, in
order. [Concepts](concepts.md) explains them in more detail.

## 5. Send messages and read them back

Send a message with a key. Messages with the same key always go to the same partition, so they stay in order:

```sh
openduct send sensors '{"sensor": "s-1", "temp_c": 21.4}' --key s-1
```

```text
sent 1 message to sensors
```

Or send a whole file, one message per line:

```sh
printf '{"sensor": "s-2", "temp_c": 19.8}\n{"sensor": "s-3", "temp_c": 22.1}\n' > readings.jsonl
openduct send sensors < readings.jsonl
```

```text
sent 2 messages to sensors
```

Read everything the topic holds:

```sh
openduct read sensors
```

```text
{"sensor": "s-2", "temp_c": 19.8}
{"sensor": "s-3", "temp_c": 22.1}
{"sensor": "s-1", "temp_c": 21.4}
```

The order is per partition: each partition's messages come out in the order they went in. To see everything about
a message, including its partition, offset, timestamp, key and headers:

```sh
openduct read sensors --format json --max 1
```

```text
{"topic":"sensors","partition":0,"offset":0,"timestamp":"2026-11-15T10:10:38.221Z","key":null,"headers":{},"value":"{\"sensor\": \"s-2\", \"temp_c\": 19.8}"}
```

The offset is a message's position in its partition: it starts at 0 and never changes. To keep watching for new
messages, add `--follow`, then send more from another terminal and watch them arrive. Ctrl-C stops it.

The [examples](../../examples) show the same from a program: `cargo run --release -p sensor-telemetry -- produce`
sends simulated sensor readings, and `consume` reads them.

## 6. Stop, start again, and find your data

Press **Ctrl-C** in the broker's terminal:

```text
INFO openduct::serve: stop requested
INFO openduct_broker::server: OpenDuct is stopping: letting requests in flight finish
INFO openduct::serve: OpenDuct stopped
```

Start it again with the same `--data-dir`, then read from the beginning. The readings are all still there, and new
ones continue from the next offset.

## Next steps

- [Concepts](concepts.md): topics, partitions, offsets, keys, consumer groups and retention, in plain words.
- [Connecting your applications](connecting.md): use OpenDuct from any Kafka client.
- [Pipelines](../pipelines.md): change messages as they flow from one topic to another, with expressions.
- [Running OpenDuct for real](running.md): other machines, services, containers and small devices.
- [Command reference](reference.md): every command and setting.
- [Troubleshooting](troubleshooting.md): what an error means and what to do about it.
