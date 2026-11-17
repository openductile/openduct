# OpenDuct beside other tools

OpenDuct is three things in one program you run yourself: an **event broker** that speaks the Kafka protocol,
**pipelines** that clean up and move messages, and a **studio** where workflows are built on top of both. Most tools
do one of these. This page sets OpenDuct beside the ones people compare it with, says where each is stronger, and
when to choose it. What OpenDuct lacks is listed in full in [capabilities.md](capabilities.md#what-it-does-not-do-yet).

What is said of other tools is from their public material as of November 2026. Tools change: check their current
terms and prices before deciding.

## At a glance

| Tool | What it is | Where it runs | Licence |
|---|---|---|---|
| **OpenDuct** | Kafka-protocol broker, pipelines and a workflow studio, one binary | Your machines: a server, a cluster, an edge device, or inside your program | Apache 2.0 |
| **Zapier** | Workflow automation across thousands of SaaS apps | Zapier's cloud only | Proprietary service |
| **Make** | Visual scenarios across thousands of SaaS apps | Make's cloud only | Proprietary service |
| **n8n** | Workflow automation, self-hosted or in its cloud | Your machines or n8n's cloud | Sustainable Use License (source available; limits on offering it as a service) |
| **Workato** | Enterprise integration and automation (iPaaS) | Workato's cloud, with an agent to reach private systems | Proprietary service |
| **MuleSoft Anypoint** | Enterprise integration and API management | MuleSoft's cloud or your own runtimes | Proprietary |
| **Apache Kafka** | The event streaming platform the protocol comes from | Your machines, or many hosted offerings | Apache 2.0 |
| **Confluent** | Kafka with a schema registry, connectors, stream processing and a cloud service | Confluent Cloud or your machines | Mix of Apache 2.0, Confluent Community License and proprietary |
| **Redpanda** | A Kafka-compatible broker written in C++, one binary | Your machines or Redpanda's cloud | Business Source License for the core; enterprise licence for some features |
| **NATS (JetStream)** | A small messaging server with persistence | Your machines, the edge, or a hosted service | Apache 2.0 |
| **LangGraph / LangChain** | Libraries for building AI agents in code | Inside your program; a hosted platform from its makers | MIT (libraries) |
| **Temporal** | Durable execution: workflows written as code that survive crashes | Your machines or Temporal Cloud | MIT |

## Workflow and integration platforms

| | OpenDuct | Zapier | Make | n8n | Workato | MuleSoft |
|---|---|---|---|---|---|---|
| Run it yourself | Yes | No | No | Yes | Agent only | Yes |
| Price basis | Free and open | Per task | Per operation (credits) | Per execution (cloud); free self-hosted for internal use | Contract | Contract |
| Apps | 26, and a kit to build more from a definition file | Thousands | Thousands | Hundreds, plus community nodes | Over a thousand | Hundreds, enterprise systems |
| A durable log under each workflow | Yes: each event kept on a topic until handled | No | No | With a queue in queue mode | Internal | Internal |
| Order per key, many runs at once | Yes, for events with a key (a topic's; a webhook's have none yet) | No | In order, one at a time (an option) | No | Depends on the recipe | Depends on the flow |
| Failed runs kept to run again | CatchErr | Replay (paid plans) | Incomplete executions | Error workflows, retry | Yes | Yes |
| Code steps | Yes | Yes | Yes | Yes | Yes | Yes (DataWeave, scripts) |
| AI step, any model | Yes, models on your machine too | Yes | Yes | Yes | Yes | Yes |
| AI agents with tools and memory | No | Yes | Yes | Yes | Yes | Yes |
| MCP | No | Yes | Yes | Yes | Yes | Yes |
| Approvals and forms mid-run | No | Yes | Yes | Yes | Yes | Partly |
| Single sign-on, audit log | No | Paid plans | Paid plans | Enterprise | Yes | Yes |
| Speaks Kafka to your systems | It is the broker | Through an app | Through an app | Through a node | Through a connector | Through a connector |

**Where they are stronger.** Zapier and Make cover thousands of apps, with triggers for each, and nothing to run;
Make's visual mapping is the most direct way to draw data from one shape to another. n8n has the deepest
self-hosted AI tooling - agents, tools, memory, MCP - and a large community. Workato and MuleSoft bring enterprise
governance: environments and promotion, single sign-on, audit, API management, and connectors for SAP, mainframes and
EDI.

**Where OpenDuct differs.** Every event a workflow handles is first kept on a durable, replicated log. That gives
what a chain of HTTP calls does not: a crash repeats a run rather than losing it, events with the same key stay in
order while others run at once, failed events wait in CatchErr to run again, and other programs can read the same
events with any Kafka client. It runs anywhere one binary runs, keeps data on your own machines, and has no per-task
price. It is new: 26 apps, not thousands.

**Choose OpenDuct** when events must not be lost or reordered, when data has to stay on your machines, when the same
events feed workflows and your own programs, or when per-task prices grow with volume. **Choose Zapier or Make** for
the widest set of SaaS apps with nothing to run. **Choose n8n** for self-hosted automation with rich AI agents.
**Choose Workato or MuleSoft** for enterprise-wide integration with API management and governance.

## Brokers

| | OpenDuct | Apache Kafka | Confluent | Redpanda | NATS JetStream |
|---|---|---|---|---|---|
| Protocol | Kafka | Kafka | Kafka | Kafka | NATS (its own) |
| What to install | One binary | Brokers and controllers (KRaft) | Platform components | One binary | One binary |
| Smallest footprint | 9-11 MiB idle; 11 MB binary | Gigabytes of memory per broker is usual | As Kafka | About a gigabyte at the least; built for many cores | Tens of MB |
| Replication | Raft per partition | ISR, KRaft controllers | As Kafka | Raft per partition | Raft per stream |
| Tiered storage (S3) | Yes | Yes | Yes | Yes (enterprise licence) | No |
| Transactions, exactly-once | No | Yes | Yes | Yes | De-duplication by message id |
| Log compaction | No | Yes | Yes | Yes | Per subject, last value |
| Schema registry | No | No (separate) | Yes | Built in | No |
| Connectors | Its own pipelines' | Kafka Connect | Many, managed | Redpanda Connect | Through other tools |
| Stream processing | Per message (CEL) | Kafka Streams | Flink, ksqlDB | Per message (WebAssembly) | No |
| Workflows and apps | Built in | No | No | No | No |
| As a library in your program | Yes (Rust) | No | No | No | Yes (Go) |
| Memory safety | Rust, `unsafe` forbidden | Managed runtime | As Kafka | C++ | Go |

**Where they are stronger.** Kafka and Confluent are the most complete: transactions, compaction, Kafka Connect, Kafka
Streams and Flink, and years of use at the largest scale. Confluent adds a schema registry and a managed cloud.
Redpanda shows one binary can serve the Kafka protocol at very high throughput, with a schema registry and an HTTP
proxy built in. NATS is the lightest way to move messages between services and out to the edge. OpenDuct has not yet
been measured against any of them for throughput or latency at scale; what is measured is in
[test results](regression.md#measured).

**Where OpenDuct differs.** It is a Kafka-protocol broker small enough for a gateway, a robot or a single small VM,
and embeddable as a Rust library, with no `unsafe` code and every resource bounded against hostile clients. It is
licensed Apache 2.0 throughout - replication and tiered storage included - and carries pipelines, connectors and a
workflow studio in the same binary.

**Choose OpenDuct** for Kafka clients at the edge or on small machines, for a broker inside your own program, for
development and CI without a heavy cluster, or for events and the workflows that act on them in one place. **Choose
Kafka or Confluent** when you need transactions, compaction, Kafka Connect or stream processing with state, or the
largest scale. **Choose Redpanda** for the highest Kafka throughput per machine. **Choose NATS** when you do not need
the Kafka protocol.

## AI frameworks: LangGraph and LangChain

LangGraph and LangChain are libraries: you write an agent in code - a graph of steps where a model chooses tools,
keeps memory, waits for a person, and its state is checkpointed - and run it in your own program or the hosted
platform of their makers. OpenDuct is not a library for agents: its AI step asks a model one question inside a
workflow, the same form whatever the model, with the event, the retries and the history around it.

They fit together. An agent built with LangGraph can read and write OpenDuct topics with any Kafka client, or be
called from a workflow's HTTP step; OpenDuct gives it durable events in and out, ordering, retries and a record of
every run. **Choose LangGraph** for agents that loop, plan and use tools. **Choose OpenDuct** for the events around
them, and for AI steps that run on every order, ticket or message without code.

## Durable execution: Temporal

Temporal runs workflows written as code and keeps their state through crashes, for runs that last minutes or months,
with signals and timers. OpenDuct's workflows are built in the studio, run on events from a durable log, and are
short: a run waits for seconds or minutes, not months, and there are no signals into a running workflow. **Choose
Temporal** for long-lived processes written by developers. **Choose OpenDuct** for event-driven workflows that people
build and watch in a browser.
