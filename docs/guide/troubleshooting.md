# Troubleshooting

Each entry starts with what you see, then explains what it means and what to do.

## Starting the broker

**`listening on 0.0.0.0:9092 without TLS, sign-in, access rules: set them, listen on 127.0.0.1, or run insecurely on purpose`**

A broker reachable from other machines must have TLS, a way to sign in and access rules - see
[security](../security.md). For a laptop only: `OPENDUCT_ALLOW_INSECURE=true`.

**`listening on 0.0.0.0:9092 needs --advertise host:port: clients are told where to reconnect, and 0.0.0.0 is not an address they can reach`**

A client connects to the address it is given, and `0.0.0.0` is not one. Add the address clients should use:
`--advertise 10.0.0.5:9092`, or a host name they can resolve.

**`cannot open the log: the data directory ./openduct-data is in use by another OpenDuct process`**

Another broker already has that directory open. Two brokers writing one log would corrupt it, so the second one
refuses. Stop the other one, or give this one its own `--data-dir`.

**`cannot listen on 127.0.0.1:9092: Address already in use (os error 48)`**

Something else has the port: another OpenDuct, or a different program. Use another port with `--listen`.

**`invalid configuration: max_request_bytes (...) is over max_request_memory (...): such a request could never be read`**

`--max-request-mib` is larger than `--max-request-memory-mib`. Raise the memory limit or lower the request limit.

**`cannot open the log: data is corrupt: the data format is version N; this OpenDuct reads version 1`**

The data directory was written by a different version of OpenDuct. Use the version that wrote it, or an upgrade
that says it reads that format.

**`cannot open the log: data is corrupt: <segment file> at byte N: ... - acknowledged data is damaged; refusing to serve a log with a hole in it`**

Data that was confirmed has been changed or lost on the disk, for example by a failing drive or a copy that went
wrong. OpenDuct will not serve a log with a hole in it. Restore the data directory from a backup. If losing that
partition's messages from that point on is acceptable, move the named segment file and the ones after it aside.

An unfinished write at the very end of the log (from a crash or power cut) is not damage: it was never confirmed,
so OpenDuct cuts it off and starts normally.

## Topics

**`cannot create sensors: topic "sensors" already exists`**

Choose another name, or delete the old topic first (`openduct topic delete sensors`, which cannot be undone).

**`cannot create bad/name: "bad/name" is not a valid topic name: 1 to 249 of letters, digits, '.', '_' and '-', and not '.' or '..'`**

Rename the topic with only those characters, for example `bad-name`.

**`cannot delete nothing: topic "nothing" does not exist`**

Check the name with `openduct topic list`.

## Sending and reading

**`cannot send to sensors: topic "sensors" does not exist`** (or `cannot read ...`)

Create the topic first: `openduct topic create sensors`.

**`sensors has partitions 0 to 2, not 4`**

Partitions are numbered from 0. Choose one in that range, or leave `--partition` out.

**`offset 5 is outside partition 0 of sensors, which holds offsets 0 to 1`**

`--from` names an offset the partition does not hold: past its end, or before its oldest message kept (retention
has removed the older ones). Use `--from earliest`, or an offset in that range.

**`cannot record the progress of group reports: ... in use by consumers that are running`**

`read --group` keeps progress under a name that a running consumer group is using. Stop that group, or read under
another name.

## Clients

**`cannot reach 127.0.0.1:9092: Connection refused`**

No broker is listening there. Check that `openduct serve` is running, and that `--bootstrap` (or the client's
`bootstrap.servers`) names the right host and port.

**A client connects, then cannot reach the broker, or times out**

The client reached the bootstrap address but was told to reconnect to an address it cannot reach. Start the broker
with an `--advertise` address that the client's machine can reach.

**A client reports an unknown topic**

Topics are not created on first use unless the broker runs with `--auto-create-topics`. Create the topic, or start
the broker with that flag.

**A producer using transactions is refused**

OpenDuct does not support transactions yet. Remove `transactional.id` and keep `enable.idempotence=true`. Each
message is still stored exactly once per send, however often the client retries.

**Creating a topic with `cleanup.policy=compact` is refused**

Compaction is not supported yet, and accepting it silently would lose data you expect kept. Use the default
`delete` policy with a retention time.

**A client's connection is closed as soon as it sends something**

It used an API or version OpenDuct does not offer, usually an admin call such as describing configs or listing
groups. Look for the failing call in the client's log, and check [the protocol reference](../protocol.md).

**A producer's connection is closed whenever it sends a large batch**

The request is over `--max-request-mib` (100 MiB by default), so the broker closes the connection before reading
it. Raise the limit, or make the client send smaller batches (`batch.size`, `max.request.size`).

## Getting help

If none of this matches, [open an issue](https://github.com/openductile/openduct/issues). Include:

- the command or client you ran;
- the full message;
- the broker's log, with `RUST_LOG=debug`.

Report security problems privately, as [SECURITY.md](../../SECURITY.md) describes.
