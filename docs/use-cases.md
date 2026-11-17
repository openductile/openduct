# What the broker is for

OpenDuct is a Kafka-protocol broker written in Rust. That choice decides where it fits:

- **No garbage collector.** Nothing stops the world to tidy memory, so the time a write takes does not jump now and
  then, and memory stays where it is set.
- **Small.** One binary - 11 MB as a broker alone - that starts listening in about 6 ms and idles in 9-11 MiB.
- **Memory-safe.** `unsafe` is forbidden in every OpenDuct crate, and the broker alone has no C in it, so a malformed
  request cannot become a memory error.
- **Bounded.** Request size, request memory, connections, fetch size and every wait have a limit. A client that
  misbehaves loses its own connection; it cannot exhaust the broker.
- **One binary, cross-compiled.** The same program runs on a server, a 64-bit ARM board or a gateway, with nothing
  else to install, and builds as a library for your own program.
- **Durable.** Every write is on disk (`fsync`) before it is acknowledged, so a power cut loses nothing acknowledged.

Each use below names what it relies on. The [pipeline examples](../examples/pipelines) show several of them working,
each with its input and exact output.

## At the edge

### A factory gateway

Machines speak OPC UA, Modbus or MQTT; the plant's systems want events. A gateway runs OpenDuct with pipelines that
read the machines, turn readings into clean events on local topics, and raise alerts on the spot.

- Relies on: the OPC UA, Modbus and MQTT connectors; durable local topics; a small ARM binary.
- Examples: [oven zones](../examples/pipelines/oven-zones) (OPC UA), [energy meter](../examples/pipelines/energy-meter)
  (Modbus), [machine states](../examples/pipelines/machine-states) (MQTT), [sensor
  alerts](../examples/pipelines/sensor-alerts).

### A site that keeps working when its link is down

A shop, a branch, a ship or a mine keeps its own events while the line to head office is down, and sends them on
when it is back: a pipeline reads the local topic and writes to the central Kafka cluster. A pipeline commits only
what was confirmed written, so nothing is lost across the outage.

- Relies on: the Kafka sink; pipeline delivery ([pipelines](pipelines.md#delivery)); retention on the local topic
  long enough to cover an outage.

### Robots, vehicles and devices with their own log

The broker is a library. A robot controller or a vehicle computer starts it inside its own program and keeps a
durable event log across restarts, with no second process to install, supervise or update. Any Kafka client on the
machine - or the program itself - uses it on `127.0.0.1`.

- Relies on: `openduct-broker` and `openduct-storage` as crates; the file store's recovery
  ([storage](storage.md#recovery)).
- Example: [embedded broker](../examples/embedded-broker).

## On small machines

### Kafka for a small team

A business that wants Kafka's model - topics, consumer groups, replay - without running a Kafka cluster: one
OpenDuct on a small VM, or three for replication, with old segments in S3-compatible storage so a long retention
costs little disk.

- Relies on: Raft replication per partition; tiered storage; one binary per node ([deployment](deployment.md)).

### Kafka in development and CI

Tests that need Kafka start OpenDuct in milliseconds with `--memory`, and throw it away. Real Kafka clients, the real
protocol, without a container that takes a minute to start and a gigabyte to run.

- Relies on: fast start; the in-memory store; protocol coverage ([protocol](protocol.md)).

### A broker for each customer

A service that gives each customer a broker of their own - to keep their data apart, or to let them bring their own
clients - pays for each one's idle time. An idle OpenDuct costs megabytes, and a partition no one else holds stops
its replication clock, so thousands of quiet partitions cost almost nothing.

- Relies on: the small idle footprint; resting partitions ([architecture](architecture.md#resting-partitions)).

## Moving data between systems

### Every change in a database, as it happens

Changes in PostgreSQL, MySQL and MongoDB are read as they happen and become events: for a search index, a
warehouse, a cache or another service.

- Relies on: the change-capture connectors; the Elasticsearch, S3 and Snowflake sinks.
- Examples: [order changes](../examples/pipelines/order-changes) (PostgreSQL), [stock
  levels](../examples/pipelines/stock-levels) (MySQL), [catalogue changes](../examples/pipelines/catalog-changes)
  (MongoDB), [sales warehouse](../examples/pipelines/sales-warehouse) (Snowflake).

### Webhooks that are never lost

Payment providers, shops and forms call a webhook once and move on. A workflow's webhook keeps each delivery on a
topic before it answers, so a crash, a deploy or a slow step loses nothing, and failures wait in CatchErr to run again.
Deliveries carry no key yet, so several run at once in no set order; with *Runs at once* 1, each runs after the one
before.

- Relies on: the studio's webhooks; workflows on topics ([studio](studio.md)).
- Examples: [payment events](../examples/pipelines/payment-events), [paid orders](../examples/pipelines/paid-orders).

### Calling a service that limits how fast you may

Shopify, HubSpot and most APIs refuse a client that calls too often. A burst - a sale, an import, a thousand form
answers at once - is kept on the workflow's topic, and the workflow works through it at the pace *Runs at once*
allows, retrying what the service turned away. Nothing is dropped and the service is not flooded.

- Relies on: workflows on topics; Runs at once; retries; CatchErr ([studio](studio.md#running)).

### Business systems that talk to each other

Deals won in Salesforce open work elsewhere; machine faults open ServiceNow incidents; orders reach the warehouse.

- Relies on: the Salesforce, ServiceNow and Snowflake connectors; apps in workflows ([apps](apps.md)).
- Examples: [won deals](../examples/pipelines/won-deals), [fault tickets](../examples/pipelines/fault-tickets).

## Keeping events

### One stream, many readers

An order written once is read by the workflow that e-mails the customer, the one that updates the books, your own
program that feeds a dashboard, and an analyst's script - each a consumer group of its own, each at its own pace,
none waiting for another. Adding a reader changes nothing for the others.

- Relies on: consumer groups; any Kafka client ([connecting](guide/connecting.md)).

### Replay: run history again

A topic keeps its events for as long as its retention says. A new workflow or program can start from the earliest
event and work through the history; one fixed after a bug can be run over the events it got wrong. Failed runs wait in
CatchErr to run again once the cause is fixed.

- Relies on: retention; a topic trigger that starts at `earliest`; offsets by time; tiered storage for a long history
  ([storage](storage.md#tiered-storage---tier-s3-bucket)).

### A record of what happened

Every order, payment, change of status and message, in the order it happened, kept for as long as the business must -
older segments in S3-compatible storage. When someone asks what happened to an order, the answer is on the topic.

- Relies on: durable writes; retention by time; tiered storage; access rules on who may read it
  ([security](security.md)).

## Where trust is low

### Untrusted networks

A broker on a network it cannot trust - shared, public, or facing devices in the field - refuses to serve without
TLS, sign-in and access rules, and stays up under malformed or malicious requests.

- Relies on: TLS 1.3, SCRAM, OAuth bearer tokens and client certificates, access rules that deny by default, quotas
  ([security](security.md)); every limit enforced; hostile input tested on every run ([test results](regression.md)).

### Data that must stay on your machines

Personal, financial or health data, or a site with no outside connection. Everything runs on your own machines:
events, workflows, credentials and AI - with a model on the same machine. Workflows may not reach the machine's own
network unless allowed, and personal fields can be masked before data moves on.

- Relies on: self-hosting; models on the machine (Ollama, LM Studio); the private-network rule; a masking pipeline
  ([mask personal data](../examples/pipelines/mask-personal-data)).

## With AI

### AI on every event

Each support ticket sorted, each message answered, each document's fields read out - by a model, as events arrive,
with retries and a record of every answer. Texts become embeddings kept in PostgreSQL with pgvector, so a workflow
can find the nearest ones to a question.

- Relies on: the studio's AI and embedding steps, the PostgreSQL step ([studio](studio.md#steps)).

### Events in and out of AI agents

An agent built in code - with LangGraph, or any other framework - reads its work from a topic and writes what it
did to another, with any Kafka client. Work is not lost when the agent restarts, two agents can share a busy topic,
and what each did stays on record for people and other programs to read.

- Relies on: consumer groups; durable topics; any Kafka client ([comparison](comparison.md#ai-frameworks-langgraph-and-langchain)).
