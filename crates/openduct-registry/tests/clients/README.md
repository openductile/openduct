# Confluent's own clients against OpenDuct's schema registry

These are not part of `cargo test`: they need software OpenDuct does not ship (Confluent's Python client; Docker
and Confluent's image for its console tools). Run them after a change to the registry.

Start a broker with the registry:

```
openduct serve --data-dir /tmp/proof --listen 127.0.0.1:19394 --registry-listen 127.0.0.1:18083
```

## Python (`confluent-kafka`)

```
python3 -m venv /tmp/ck && /tmp/ck/bin/pip install "confluent-kafka[avro,json,protobuf,schemaregistry]"
/tmp/ck/bin/python confluent_python.py 127.0.0.1:19394 http://127.0.0.1:18083
```

Eleven checks: Confluent's `AvroSerializer`, `JSONSerializer` and `ProtobufSerializer` each register their schema,
produce to the broker, and the matching deserializer reads the messages back; compatible schemas are taken and
breaking ones refused with `409`; `auto.register.schemas=false` finds the registered schema; an Avro schema
refers to another subject's; levels, listing and deletes go through `SchemaRegistryClient`.

Last run: 2026-11-18, confluent-kafka 2.16.2 (librdkafka 2.16.0), Python 3.14 - 11 of 11.

What it found the first time: the Protobuf serializer sends the schema as a file descriptor in base64, not as
`.proto` text. The registry reads both now and keeps the text.

## Confluent's console tools (Confluent's image)

```
openduct serve --data-dir /tmp/proof --listen 127.0.0.1:19394 --registry-listen 127.0.0.1:18083 --auto-create-topics
sh confluent_tools.sh
```

It runs Confluent's console producers and consumers - `kafka-avro-console-producer`,
`kafka-json-schema-console-producer`, `kafka-protobuf-console-producer` and their consumers - from
`confluentinc/cp-schema-registry` (about 2 GB, pulled once). Four checks: for each kind of schema the
console producer's serializer registers its schema and produces two records, and the console consumer's
deserializer prints them back; and a
breaking Avro schema is refused to the producer with `409`.

The broker tells clients to come back to `127.0.0.1`, which inside a container is the container; so the script
starts a forwarder from the same image (a few lines of Python) and runs the tools in its network. The broker is
started with `--auto-create-topics` because the console producers expect a topic to appear on first use, as on
Kafka's defaults.

Last run: 2026-11-18, the image's `latest` on Docker 28 - 4 of 4.
