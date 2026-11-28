# What OpenDuct does, and what it does not do yet

OpenDuct is **0.1**. This page lists what it does today, in one place, and - as plainly - what it does not do yet,
with the tools that do. [comparison.md](comparison.md) sets it beside those tools one by one.

## What it does

### The broker

| Capability | |
|---|---|
| Kafka wire protocol | Producers, consumers and consumer groups with the clients you have ([protocol](protocol.md)) |
| Idempotent producers | Retries never write twice |
| Transactions | A producer writes to several partitions and commits a consumer group's offsets together or not at all (exactly-once), on one broker and in a cluster; `read_committed` consumers. [protocol](protocol.md#transactions) says what is missing |
| Durable writes | `fsync` before every acknowledgement; every read checksum-verified; recovery cuts off only an unacknowledged torn write ([storage](storage.md)) |
| Replication | A Raft group per partition, leaders spread over the brokers; brokers join and leave, partitions move, while clients carry on |
| Tiered storage | Older segments in S3-compatible storage, read back from it |
| Retention | By time and by size, per topic, changeable while running; `DeleteRecords` |
| Log compaction | `cleanup.policy=compact` (or `compact,delete`): the newest record of each key stays at its offset, tombstones delete, compressed batches included; on one broker or a cluster ([protocol](protocol.md#log-compaction)) |
| Schema registry | Built in (`--registry-listen`): Confluent's REST API for Avro, JSON Schema and Protobuf - subjects, versions, ids, the seven compatibility levels, references between subjects, soft and permanent deletes - kept in a compacted topic and served by every broker of a cluster ([schema registry](registry.md)) |
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
| Triggers | Webhook (with an optional token, and an answer of the workflow's own), schedule, topic, by hand, another workflow's failure, a form the studio serves |
| App triggers | 29 triggers of 20 apps, defined in the apps' files ([apps](apps.md#triggers)). Polls (Gmail, Sheets, Drive, Calendar, Telegram, Slack, Notion, Airtable, Zendesk, Microsoft 365 mail, HubSpot, Mailchimp, Twilio): each new item once, across restarts and a node's loss, starting from now. Webhooks (Stripe, Shopify, Typeform, Calendly, WooCommerce, Square, Telegram, Slack, WhatsApp): signed deliveries only, old and repeated ones refused; registered with the app as the workflow is turned on where its API allows. Limits: a poll asks at most every minute and lists at most 5,000 items; a delivery seen before is caught on the node that took it (across a cluster, the app's signed time bounds a repeat to five minutes where the app signs one) |
| Email and messengers | Any mailbox over IMAP, with OpenDuct's own client: messages listed and searched, read (text, HTML, headers, attachments as the run's files), marked, moved, deleted; signed in with a password or an OAuth token (XOAUTH2) kept fresh; TLS by default. A new-email trigger polled once across a cluster and through restarts (a folder's UIDVALIDITY and highest UID kept with its runs), with IDLE asking for a look at once where the server offers it. Discord through a bot (messages, threads, reactions, roles) and its slash commands and buttons, checked with Ed25519; Microsoft Teams through Graph (channels, chats, threads, Adaptive Cards), new messages through Graph's change notifications - subscribed, validated, renewed every 20 minutes and taken away by the workflow; Slack's most used methods, file uploads included; WhatsApp templates, media, buttons and lists; Telegram buttons, photos, documents, edits and button presses; Google Chat threads and cards ([studio](studio.md#email-and-messengers)). Not yet: POP3, Exchange's own protocols, sending through IMAP (sending is SMTP's or an app's), S/MIME and PGP, a message over 32 MB, Graph notifications with their content (each message is read after), Discord's gateway (a bot's events other than interactions), Slack's interactive buttons, WhatsApp media uploads, Telegram files from a run, Google Chat's cards as an app (a service account), any of them tried live |
| Chat | A chat as a trigger: a page of its own, on a site by a script tag or a frame, or JSON for a front end of one's own; each message a run given the conversation's earlier turns, its answer streamed as an AI step writes it; a password, the sites it may be on, limits per conversation and per address ([studio](studio.md#the-chat)). In a cluster, only a run on the node the message reached streams; a conversation is remembered a day |
| Steps | Set, filter, branch, switch, loop, list tools, group and summarise, merge, date and time, lookup files, HTTP, SQL on PostgreSQL (with pgvector), e-mail, JavaScript and Python code, AI (ask, classify, sentiment, extract, summarise), embeddings, messengers, apps, write to a topic, respond to a webhook, run another workflow, wait, ask with a form, ask for approval, stop |
| SQL databases | Steps for MySQL and MariaDB, SQL Server (2012 and later, Azure SQL; TDS 7.4 and 8) and PostgreSQL, each with its own client in OpenDuct: values sent as typed parameters, never written into the SQL; rows read in their columns' types; several statements in one transaction; `read_only` checked when the workflow is read and run where the database keeps it from writing (on SQL Server, a transaction always rolled back) - for an agent's tools; at most 10,000 rows, 4 MiB and 5 minutes a step. A trigger of a table's new or changed rows by a cursor column (an id, or a time with the rows' key), polled once across a cluster and through restarts, starting from now. Credentials sealed (host, port, database, user, password, TLS checked against the system's roots, a CA, or a server's own certificate). Not yet: SQLite, Oracle and other databases, Windows or Entra sign-in to SQL Server, deletes seen by the trigger, a pool of connections ([studio](studio.md#sql-databases)) |
| Forms and runs that wait | A form trigger (text, long text, number, e-mail, date, dropdown, checkbox, file up to 2 MB; checked in the browser and on the server; a page token, an optional password, answers per minute per address); a run that waits for a form, an approval, a call to a one-time address or a time - kept on the log, through restarts and a node's loss, going on once with the first answer, timing out as set, up to 90 days ([studio](studio.md#forms-and-runs-that-wait)). Not yet: waits in workflows started by a topic or run by another workflow, multi-page forms, a person's own list of forms to answer |
| JavaScript and Python | Built in (Boa and RustPython; no Node.js or CPython needed), each script in a sandboxed process with no files, network or credentials; the computing part of Python's standard library; no pip or npm packages ([README](../README.md#javascript-and-python-steps)) |
| Trying | Test (sends nothing: an app's action answers with its recorded example, an HTTP step with its sample answer, a table not made yet is an empty stand-in, each said in the step's record), Preview (reads for real, shows writes), each step's input and output side by side |
| Reliability | Runs on the broker: each event runs at least once, in order per key, up to 50 at once; a crash repeats a run, never skips one |
| Failures | Retries per step; CatchErr keeps failed records to run again; a workflow can start when another fails; alerts to a messenger |
| History | Every run step by step; every saved version |
| Workspaces | Isolated topics, credentials and files; members as admin, editor or viewer |
| Cluster | Every node runs the studio and shares the runs; schedules run once |
| Schedules | Every so often (`15m`, counted from midnight UTC), or at set times: cron expressions (5 fields, or 6 with seconds; `L` for a month's last day) and plain forms - every hour at a minute, every day, weekday, week (chosen days) or month (chosen dates, or the last day) at one or more times - in a time zone of the schedule's own; across clock changes a skipped time runs once as the clock jumps, a repeated one once; the next runs shown in the studio and by `openduct workflow check`; at most once a second. Not yet: "the second Monday" (`#`), the nearest weekday (`W`), every N days or weeks counted from a start date, catching up on times missed while every node was down |
| Templates | Workflows to start from |
| CI/CD for workflows | API tokens per workspace (a role at most the maker's, an expiry, optional workflow scopes, revoked within seconds); `openduct workflow pull / diff / push / rollback / promote` against a running studio, every file checked first and nothing changed if one is wrong or the workspace lacks a credential, file, table or workflow it names; development, test and production workspaces with promotion (Promote in the editor too) and a lock that keeps people's edits out of production; each version's source in its history; Git sync with GitHub, GitLab and Gitea over their REST APIs - Save to Git (a commit or a pull request), Deploy from Git by hand or on a signed push; ready-made GitHub Actions and GitLab CI ([deployment](deployment.md#cicd-for-workflows)). Not yet: Git sync deleting files or merging a branch that moved, Bitbucket and Azure DevOps, a GitHub App's sign-in, pushes over 1 MiB |
| MCP | Each workspace an MCP server: workflows offered as tools to AI assistants, signed in with the workspace's own tokens (scoped to tools, so many requests a minute, revocable), each call a run in the history; an MCP tool step calls any MCP server's tools. Streamable HTTP, protocol 2026-07-28 and the earlier 2025-11-25, 2025-06-18, 2025-03-26; tools only - no resources, prompts, notifications, stdio servers, or MCP's OAuth sign-in ([studio](studio.md#mcp-tools-for-ai-assistants-and-from-them)) |
| Data tables | A workspace's tables with typed columns (text, number, boolean, date-time, JSON): rows got, counted, inserted, updated, upserted and deleted by a step, with conditions as expressions or column comparisons; each workflow's own state between runs, for scripts too; browsed, edited, imported and exported as CSV in the studio. Kept in the broker, the same on every node; two runs changing one row change it one after the other. Up to 50,000 rows a table and 50 MiB a workspace, held in each node's memory and scanned, not indexed ([studio](studio.md#data-tables)) |
| Storage | Files in a folder of this machine the admin allows (`--files-root name=path`), on SFTP and FTP/FTPS servers, in S3 and the stores that speak its API (MinIO, R2, Wasabi, B2), and - over their own APIs - in Azure Blob Storage (a shared key or a SAS token; Azurite), Google Cloud Storage (a service account's key file, its JWT signed with RS256 on `ring`), Dropbox, Box (OAuth or a developer token), OneDrive and SharePoint document libraries (Microsoft Graph) and Google Drive (Google Docs exported when read): listed, read into a run's files, written, moved, copied, deleted, folders made, addresses presigned (S3; Azure with the account's key, a service SAS; Google Cloud Storage, V4) - one step for every place, its credential saying which. A person connects Dropbox, Box, OneDrive and Google Drive with the studio's Connect, the install's own registration with each vendor; a token that runs out is refreshed and kept. `..`, absolute paths and links leading out of a folder refused; servers on private addresses only with `--allow-private-network`; an SFTP server's key pinned, or kept the first time and then required (OpenDuct's own SSH client). ZIP, gzip, tar and tar.gz packed and unpacked, bounded (64 MiB unpacked). Redis: values with a time to live, counters, lists as queues, publish. Triggers: new or changed files in a folder, polled once across a cluster and through restarts (at most 5,000 files a folder) - read from the vendor's list of changes for Dropbox (its cursor), OneDrive (Graph's delta) and Google Drive (its changes), from the folder's listing elsewhere; a Redis channel's messages, on one node. Not yet: SMB/CIFS and NFS shares, WebDAV, Azure and Google Cloud sign-in by Entra ID or workload identity (keys, SAS tokens and key files only), Box's events (its folders are polled), a copy within OneDrive by Graph's own (the file is read and written again), Google Drive's shortcuts followed, SSH keys with a passphrase, SFTP through a jump host, files over 10 MiB (a run's file), messages published while no node listens ([studio](studio.md#storage-folders-servers-archives-and-redis)) |
| Web pages | A web step in OpenDuct's own code: a page's readable text as Markdown or text (headings, lists, tables and code kept; navigation, sidebars, banners, comments and footers left out), its title, description, language, canonical address, links and pictures, its HTML as text or a file; fields picked by CSS selectors (one or all, an attribute, records of fields); a site crawled to a depth and a number of pages, robots.txt and its Crawl-delay honoured (unless turned off), one host at a polite rate, from its sitemap when asked; sitemaps and sitemap indexes; RSS, Atom and JSON feeds; a feed trigger, polled once across a cluster and through restarts. Every address and redirect checked against this machine's network; a User-Agent naming OpenDuct. Limits: 10 MB and 2 minutes a page, 10 redirects, 200 pages and 10 minutes a crawl. Not a browser: a page that builds itself with scripts gives what its HTML holds; no proxies, no sign-in to a site but headers or a page fetched by an HTTP step ([studio](studio.md#web-pages-read-pick-out-crawl-sitemaps-and-feeds)) |
| Files between steps | Files passed from step to step by reference: read from CSV, JSON, XLSX, XLS, text, HTML and PDF (its text) into rows, data or text; made as CSV, JSON, XLSX, text or HTML, or from Base64; HTTP answers as files, files and multipart forms sent; webhook uploads; e-mail attachments; kept in the log for the runs' 14 days and downloaded from a run's record. 10 MiB a file, 20 files and 32 MiB a run. No text from pictures or protected PDFs, no XLS or PDF written, no Word or ODS files; scripts read files through an extract step ([studio](studio.md#files-between-steps)) |

### Apps and AI

| Capability | |
|---|---|
| Apps | 130, each a definition in [`apps/`](../apps) with its sign-ins, actions, triggers and recorded tests - written from each vendor's own API description or reference, and tested against recorded exchanges made from that documentation (not yet run against live accounts). An app sends JSON, forms, or a body as it is (Slack uploads a run's file so); a picture or video is otherwise one it fetches from the web ([apps](apps.md)) |
| App kit | An app is a definition file, not code: `openduct app new / check / test / try`; recorded exchanges run in CI with no account |
| App generator | `openduct app from-openapi` makes an app from a vendor's OpenAPI 3.0/3.1 or Swagger 2.0 description (a file or an address): its hosts, sign-ins (API keys, bearer, basic, OAuth 2.0 code and client credentials), up to 100 ranked actions with inputs, outputs and pages, a README and recorded tests that pass with no account; what it cannot take (file uploads, XML, callbacks, other sign-ins) listed, never guessed. `openduct app from-mcp` makes one whose actions call an MCP server's tools. `openduct app check` scores each app against a bar of 80 ([apps](apps.md#making-an-app-from-an-api-description)). Proven on Box, Resend, Intercom and Klaviyo (shipped), and Docusign (a test: its description names no sign-in). Not yet: webhooks and callbacks as triggers, file uploads, signing in to an MCP server at the vendor; a made app is not tried live until someone runs `openduct app try --send` |
| Custom apps | A workspace's own apps, made on the studio's Apps page from an API description, a file, an MCP server or an app.yaml, with the same generator: actions chosen, sign-ins picked, the quality report shown; kept on the log, offered and usable in that workspace only, with credentials of their own kind; at most 200 a workspace, 100 actions each ([studio](studio.md#custom-apps)). Not yet: triggers, a sign-in at the vendor, use in a workflow another workflow runs |
| Sign-in at the vendor | OAuth 2.0 through each installation's own registration; tokens renewed; client credentials |
| AI | One step for any model: OpenAI, Anthropic, Google Gemini, Mistral, Groq, OpenRouter, Together, Azure OpenAI, Amazon Bedrock, DeepSeek, xAI, models on the machine (Ollama, LM Studio), any compatible endpoint; embeddings; similarity search in PostgreSQL |
| AI steps | Classify a text into your categories (one or several, with a fallback) and sentiment (with a score), each routing as a switch does; extract typed fields; summarise a text or a list of texts of any length (in pieces, then the whole); any answer held to a JSON schema, with one repair turn. Limits: one call takes at most 200,000 characters, summarise at most 40 pieces, no streamed answers, Google Vertex AI only through a Gemini key ([AI steps](ai-steps.md)) |
| AI | One step for any model: OpenAI, Anthropic, Google Gemini, Mistral, Groq, OpenRouter, Together, models on the machine (Ollama, LM Studio), any compatible endpoint; embeddings; similarity search in PostgreSQL |
| AI agents | A model that chooses tools over several turns, on any model the AI step reaches: tools are HTTP requests, JavaScript and Python, other workflows, apps' actions and other agents, each with a JSON schema of its arguments, checked before it runs; `allow` limits which may run; memory per conversation (the last turns, kept on the broker - or in a PostgreSQL table, Redis or a data table - forgotten on a condition or after a while); text, or JSON checked against a schema with one turn to put it right; limits on turns, tool calls, time and tokens; every turn and call in the run's record ([studio](studio.md#ai-agents)). Not yet: a person's approval before a tool runs, answers streamed as they are written, MCP servers as tools, images and files to the model |
| Retrieval | Split text (by characters, paragraphs, sentences or Markdown headings, with an overlap; HTML read as text), a vector store on PostgreSQL with pgvector (pieces added with their vectors, the table made when asked; search with metadata filters, a top-k, a least score and a rerank model where the model's service offers one; delete by document), and answers from documents that show every passage, its document, its score and whether it was cited ([studio](studio.md#answers-from-your-documents)). Up to 5,000 pieces a step |
| Vector databases | The vector store and the answers from documents on Qdrant, Weaviate, Milvus and Zilliz, Chroma, Pinecone, Redis's vector search and MongoDB Atlas Vector Search, as on PostgreSQL: the same add, search and delete, outputs, bounds, and test and preview rules; the collection made on first use when asked; a metadata filter written once and sent in each store's own filter language, one a store cannot express refused when the workflow is read; this machine's network refused unless allowed. And `store: memory`, kept for one run only, for small sets and tests ([studio](studio.md#vector-databases)). Limits: tried against stand-ins of each API, not live accounts; Pinecone does not say how many it deleted; Weaviate and Chroma filter by top fields only, Pinecone by text lists; MongoDB signs in with SCRAM-SHA-256 only; Milvus filters need Milvus 2.5 or later; a new Atlas index answers after Atlas builds it <!-- vector stores --> |

### Security and operations

| Capability | |
|---|---|
| Secrets | Never in environment variables: the studio seals credentials; OpenDuct's own secrets come from files |
| Private networks | Workflows and pipelines may not reach the machine's own network unless allowed |
| Logs | stderr (text or JSON) and OpenTelemetry log files, shown in the studio's Logs page ([deployment](deployment.md#logs)) |
| Metrics | For Prometheus at `/metrics` (`--metrics-listen`; a token beyond this machine): requests and their durations by Kafka API, bytes in and out, connections, sign-in failures, offsets, consumer group lag, leaderless partitions, workflow runs and AI tokens ([deployment](deployment.md#metrics)) |
| Deployment | One binary, a container image, Docker Compose, Kubernetes (a manifest, or a Helm chart for one node or a cluster), OpenTofu (this machine's Docker, or one machine on AWS, Google Cloud or Azure); rolling upgrades and rollbacks one node at a time ([deployment](deployment.md)) |
| Upgrades | One broker at a time ([upgrades](upgrades.md)) |
| Hostile input | Every place OpenDuct reads bytes it did not write is mutated at random in every test run and fuzzed in CI ([test results](regression.md)) |

## What it does not do yet

### The broker

| Missing | Who has it |
|---|---|
| Changing a broker's settings while running (a topic's change while it runs) | Apache Kafka, Confluent, Redpanda |
| The Kafka Connect API (OpenDuct has its own connectors instead) | Apache Kafka, Confluent |
| Stream processing with windows, joins and state (pipelines change one message at a time) | Kafka Streams, Apache Flink, ksqlDB |
| The newer consumer group protocol (KIP-848) and queue-style share groups (KIP-932) | Apache Kafka 4.x |
| An HTTP API for producing and consuming | Confluent REST Proxy, Redpanda HTTP Proxy |
| Copying a cluster to another with its offsets (pipelines copy messages, not offsets) | MirrorMaker 2, Confluent Cluster Linking |
| Tables of a topic in open formats (Iceberg) | Confluent Tableflow, Redpanda Iceberg topics |

### Workflows, apps and AI

| Missing | Who has it |
|---|---|
| Thousands of apps (OpenDuct has 40, a kit to add more, and a generator that makes one from an API description or an MCP server) | Zapier, Make, Workato, MuleSoft |
| A key for a webhook's deliveries, so those of one order run in order while others run at once (a topic's events with a key do) | Make keeps deliveries in order by running them one at a time |
| App triggers for apps beyond the 20 that have them, and an app's triggers tried live from the command line (`openduct app try` runs actions) | Zapier, Make, Workato |
| A place to share apps others built | Zapier and Make app directories |
| Documents read from files (PDF, Word) for retrieval, which reads texts and web pages a run already has; Supabase's own vector API (its PostgreSQL is reached as PostgreSQL) | LangChain, LlamaIndex |
| Data tables larger than a node's memory, with indexes and joins (OpenDuct's are held in memory and scanned) | Airtable, Baserow; a database, through the PostgreSQL step |
| SQLite files, Oracle, DB2 and other databases beyond MySQL, MariaDB, SQL Server and PostgreSQL; Windows and Entra sign-in to SQL Server | Workato, MuleSoft |
| Network shares (SMB/CIFS, NFS), WebDAV; files larger than 10 MiB streamed through a run | MuleSoft, Workato, Make |
| Pages that build themselves with scripts (a browser that runs them, screenshots), crawls through proxies or of many sites at once | Apify (an app here), browser services |
| A data mapper that draws fields from one shape to another | Make, Workato, MuleSoft (DataWeave) |
| API management: publishing, securing and metering APIs | MuleSoft, Workato |
| B2B and EDI, SAP and mainframe connectors | MuleSoft, Workato |

| In OpenDuct's Enterprise edition, not in this one | Who else has it |
|---|---|
| Building a workflow by describing it | Zapier, Make, Workato |

### Operations and enterprise

| Missing | Who has it |
|---|---|
| Single sign-on (SAML, OIDC) and SCIM | Zapier, Make, Workato, MuleSoft (paid plans) |
| An audit log of who changed what | Zapier, Workato, MuleSoft |
| A hosted service, for those who do not want to run it (OpenDuct is run by its users) | Every other tool compared - by its makers or by others |
| Traces of a run across systems (OpenTelemetry traces) | Temporal, LangSmith |
