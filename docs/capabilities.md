# What OpenDuct does, and what it does not do yet

OpenDuct is **0.1**. This page lists what it does today, in one place, and - as plainly - what it does not do yet,
with the tools that do. [comparison.md](comparison.md) sets it beside those tools one by one.

## What it does

### The broker

| Capability | |
|---|---|
| Kafka wire protocol | Producers, consumers and consumer groups with the clients you have ([protocol](protocol.md)) |
| Idempotent producers | Retries never write twice |
| Durable writes | `fsync` before every acknowledgement; every read checksum-verified; recovery cuts off only an unacknowledged torn write ([storage](storage.md)) |
| Replication | A Raft group per partition, leaders spread over the brokers; brokers join and leave, partitions move, while clients carry on |
| Tiered storage | Older segments in S3-compatible storage, read back from it |
| Retention | By time, per topic, changeable while running; `DeleteRecords` |
| Topics and partitions | Create, list, delete (by name or id), add partitions |
| Consumer groups | Classic protocol, committed offsets, spread over the brokers; describe, list, delete |
| Security | TLS 1.3; SCRAM, OAuth bearer tokens, client certificates, PLAIN over TLS; access rules that deny by default; quotas per principal ([security](security.md)) |
| Bounded | Request size, request memory, connections, fetch size and every wait are limited: a hostile client loses its own connection |
| Small | One binary: 11 MB as a broker alone, 26 MB with pipelines and connectors; listening about 6 ms after launch; idle in 9-11 MiB (an arm64 laptop) |
| Embeddable | The broker is a Rust library: a program can carry its own durable log ([example](../examples/embedded-broker)) |
| Quiet when idle | A partition with one copy stops its Raft clock: 6,000 idle partitions on a node cost 0.5% of a core |

### Pipelines and connectors

| Capability | |
|---|---|
| Pipelines | Read a topic, clean up, reshape, filter and split each message with CEL expressions, write another topic; failed messages set aside; several copies share the work ([pipelines](pipelines.md)) |
| Sources | HTTP APIs, webhooks, MQTT, Modbus, OPC UA, other Kafka clusters, PostgreSQL, MySQL and MongoDB (every change, as it happens), Salesforce, ServiceNow, Snowflake ([connectors](connectors.md)) |
| Sinks | Elasticsearch, S3-compatible storage, MQTT, other Kafka clusters, Salesforce, ServiceNow, Snowflake |

### Workflows and the studio

| Capability | |
|---|---|
| A visual builder | A canvas in the browser: trigger, steps, branches, switches, loops ([studio](studio.md)) |
| Triggers | Webhook (with an optional token, and an answer of the workflow's own), schedule, topic, by hand, another workflow's failure |
| Steps | Set, filter, branch, switch, loop, list tools, group and summarise, merge, date and time, lookup files, HTTP, SQL on PostgreSQL (with pgvector), e-mail, JavaScript and Python code, AI, embeddings, messengers, apps, write to a topic, respond to a webhook, run another workflow, wait, stop |
| JavaScript and Python | Built in (Boa and RustPython; no Node.js or CPython needed), each script in a sandboxed process with no files, network or credentials; the computing part of Python's standard library; no pip or npm packages ([README](../README.md#javascript-and-python-steps)) |
| Trying | Test (sends nothing), Preview (reads for real, shows writes), each step's input and output side by side |
| Reliability | Runs on the broker: each event runs at least once, in order per key, up to 50 at once; a crash repeats a run, never skips one |
| Failures | Retries per step; CatchErr keeps failed records to run again; a workflow can start when another fails; alerts to a messenger |
| History | Every run step by step; every saved version |
| Workspaces | Isolated topics, credentials and files; members as admin, editor or viewer |
| Cluster | Every node runs the studio and shares the runs; schedules run once |
| Templates | Workflows to start from |

### Apps and AI

| Capability | |
|---|---|
| Apps | 26: Stripe, PayPal, Square, Shopify, WooCommerce, HubSpot, Mailchimp, Calendly, Twilio, Airtable, Notion, Typeform, Zendesk, QuickBooks Online, Xero, Google Sheets, Drive, Calendar and Gmail, Microsoft 365, Telegram, Slack, Discord, Microsoft Teams, Google Chat, WhatsApp ([apps](apps.md)) |
| App kit | An app is a definition file, not code: `openduct app new / check / test / try`; recorded exchanges run in CI with no account |
| Sign-in at the vendor | OAuth 2.0 through each installation's own registration; tokens renewed; client credentials |
| AI | One step for any model: OpenAI, Anthropic, Google Gemini, Mistral, Groq, OpenRouter, Together, models on the machine (Ollama, LM Studio), any compatible endpoint; embeddings; similarity search in PostgreSQL |

### Security and operations

| Capability | |
|---|---|
| Secrets | Never in environment variables: the studio seals credentials; OpenDuct's own secrets come from files |
| Private networks | Workflows and pipelines may not reach the machine's own network unless allowed |
| Logs | stderr (text or JSON) and OpenTelemetry log files, shown in the studio's Logs page ([deployment](deployment.md#logs)) |
| Metrics | For Prometheus at `/metrics` (`--metrics-listen`; a token beyond this machine): requests and their durations by Kafka API, bytes in and out, connections, sign-in failures, offsets, consumer group lag, leaderless partitions, workflow runs and AI tokens ([deployment](deployment.md#metrics)) |
| Deployment | One binary, a container image, Docker Compose, Kubernetes, OpenTofu ([deployment](deployment.md)) |
| Upgrades | One broker at a time ([upgrades](upgrades.md)) |
| Hostile input | Every place OpenDuct reads bytes it did not write is mutated at random in every test run and fuzzed in CI ([test results](regression.md)) |

## What it does not do yet

### The broker

| Missing | Who has it |
|---|---|
| Transactions and exactly-once processing | Apache Kafka, Confluent, Redpanda |
| Log compaction (keep the last value of each key) | Apache Kafka, Confluent, Redpanda |
| Changing settings other than retention while running | Apache Kafka, Confluent, Redpanda |
| A schema registry (Avro, Protobuf, JSON Schema) | Confluent, Redpanda (built in) |
| The Kafka Connect API (OpenDuct has its own connectors instead) | Apache Kafka, Confluent |
| Stream processing with windows, joins and state (pipelines change one message at a time) | Kafka Streams, Apache Flink, ksqlDB |
| The newer consumer group protocol (KIP-848) and queue-style share groups (KIP-932) | Apache Kafka 4.x |
| An HTTP API for producing and consuming | Confluent REST Proxy, Redpanda HTTP Proxy |
| Copying a cluster to another with its offsets (pipelines copy messages, not offsets) | MirrorMaker 2, Confluent Cluster Linking |
| Tables of a topic in open formats (Iceberg) | Confluent Tableflow, Redpanda Iceberg topics |

### Workflows, apps and AI

| Missing | Who has it |
|---|---|
| Thousands of apps (OpenDuct has 26, and a kit to add more) | Zapier, Make, n8n, Workato, MuleSoft |
| A key for a webhook's deliveries, so those of one order run in order while others run at once (a topic's events with a key do) | Make keeps deliveries in order by running them one at a time |
| App triggers ("a new Stripe payment"), polled or from the vendor's webhooks | Zapier, Make, n8n, Workato |
| A place to share apps others built | n8n community nodes, Zapier and Make app directories |
| Human approval and forms in the middle of a run | n8n, Zapier, Make, Workato |
| AI agents: a model choosing tools, with memory, over several turns | LangGraph, n8n, Zapier, Make, Workato |
| MCP: workflows offered as tools to AI assistants, and MCP servers used as tools | Zapier, n8n, Make |
| Building a workflow by describing it | Zapier, Make, n8n, Workato |
| Tables to keep data between runs (beyond topics and lookup files) | Zapier Tables, Make data stores, n8n data tables |
| Schedules at times of day (cron), not only "every so often" | Zapier, Make, n8n, Workato |
| Development, test and production environments, with promotion and Git | n8n (enterprise), Workato, MuleSoft |
| A data mapper that draws fields from one shape to another | Make, Workato, MuleSoft (DataWeave) |
| API management: publishing, securing and metering APIs | MuleSoft, Workato |
| B2B and EDI, SAP and mainframe connectors | MuleSoft, Workato |

### Operations and enterprise

| Missing | Who has it |
|---|---|
| Single sign-on (SAML, OIDC) and SCIM | Zapier, Make, n8n, Workato, MuleSoft (paid plans) |
| An audit log of who changed what | Zapier, n8n (enterprise), Workato, MuleSoft |
| A hosted service, for those who do not want to run it (OpenDuct is run by its users) | Every other tool compared - by its makers or by others |
| Traces of a run across systems (OpenTelemetry traces) | Temporal, LangSmith, n8n (partly) |
