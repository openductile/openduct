# Changelog

All notable changes to OpenDuct are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **90 more apps**, for 130 in all: CRMs and sales (Salesforce, Pipedrive, Zoho CRM, HighLevel, Keap, ActiveCampaign,
  Agile CRM, Hunter), support (Freshdesk, Zammad, Help Scout, ServiceNow, PagerDuty), marketing and email (SendGrid,
  Brevo, Mailjet, Mailgun, MailerLite, lemlist, Mautic), work and projects (Asana, ClickUp, monday.com, Trello,
  Linear, Todoist, Coda, Baserow, NocoDB), files (Dropbox, Nextcloud), Google (Analytics, Ads, BigQuery, Contacts,
  Slides, Tasks, Translate, Books, Business Profile, Firestore, Workspace Admin) and Microsoft 365 (SharePoint, To Do,
  Entra ID), messengers (Matrix, Mattermost, Rocket.Chat, Gotify, Pushover) and more. Each is written from the
  vendor's own API description or reference - the source and its licence in its README - and tested against
  recorded exchanges made from that documentation; none has been run against a live account yet.
- **250 templates**, from 43: ready-to-use workflows in every category, each checked and test-run on its own sample.

<!-- vector stores -->
- **Vector databases, and stores for agents' memory.** `vector_store` and `ask_documents` keep and find pieces on
  Qdrant, Weaviate, Milvus (and Zilliz Cloud), Chroma, Pinecone, Redis's vector search and MongoDB Atlas Vector
  Search, as they do on PostgreSQL with pgvector - the store named by `store`, or chosen by the credential's kind; the
  same add (a document added again losing its old pieces), search (top-k, least score, metadata filters, rerank) and
  delete by document; the same output, bounds, and test and preview rules. `create: true` makes the collection on
  first use, its width taken from the first vectors. A metadata filter is written once and sent in each store's own
  language (Qdrant's `must`, Weaviate's `where`, a Milvus expression with its values apart, Chroma's `where`,
  Pinecone's filter, Redis tags, a `$vectorSearch` filter); one a store cannot express is refused when the workflow is
  read. The web stores go through the run's HTTP requests and Redis and MongoDB over the storage steps' connections,
  this machine's network refused unless allowed. New credential kinds: `qdrant`, `weaviate`, `milvus`, `chroma`,
  `pinecone`, `mongodb`. `store: memory` keeps pieces for one run only, for small sets and tests. An agent's `memory`
  may name a store - `postgres` (a table it makes when asked), `redis` (keys with a TTL) or `table` (a data table the
  studio offers to make) - with the broker's window, clear and forget-after rules and its per-workspace keys; the
  broker stays the default. The studio's vector store step has a *Kept in* picker, the credentials page the new
  kinds' forms, and the agent's memory where it is kept. Tested against stand-ins of each store's API, not live
  accounts.

<!-- test runs and needs -->
- **Test runs answer like the real thing, and use a stand-in for a table not made yet.** A test run answers an app's
  action with the app's own example answer - the result of its recorded exchanges - and an HTTP step with the sample
  answer it is given (`sample:`), so a list step, a loop or an expression over the items after it goes through them
  as in a real run; each such step is marked "sample answer, nothing sent", with the request it would have sent
  beside it. An app with no recording of the action, an HTTP step without a sample, and a preview are as before. A
  table not made yet is an empty stand-in in a test run, with the columns the step gives it, and its step says so; a
  real run still fails there, plainly.

- **SQL databases.** A `mysql` step (MySQL and MariaDB) and a `sqlserver` step (SQL Server 2012 and later, Azure SQL),
  each with a client of OpenDuct's own: values from expressions sent as parameters of their own types (`?`, `@p1`),
  never written into the SQL; rows read in their columns' types (numbers and decimals exact, dates and times as the
  database reads them back, JSON as JSON, binary as hex); several `statements` in one `transaction`; `read_only`,
  checked when the workflow is read and run where the database keeps it from writing - for an agent's tools; limits
  on rows (`max_rows`), time (`timeout`) and bytes. The `postgres` step takes the same settings; without them it reads
  and runs as before. A trigger of a table's new or changed rows (`trigger: {mysql|sqlserver|postgres: {table,
  cursor, key, where, every}}`), polled as app triggers are: once across a cluster, through restarts, from when it is
  turned on. Credentials of kinds `mysql` and `sqlserver`, sealed: host, port, database, user, password, and TLS
  checked against the system's roots, a CA, or the server's own certificate. Test runs send nothing; previews run
  what only reads. The builder has both steps and *A database* among the triggers. A template: *New paid orders in
  MySQL, added to a Google Sheet*. SQLite is not in it yet.
- **The app generator** (app generator). `openduct app from-openapi <file or address> --name <app>` makes an app's
  folder from a vendor's OpenAPI 3.0/3.1 or Swagger 2.0 description, JSON or YAML (bounded in size, depth and values;
  an address fetched through the private-address rule of the HTTP step): its hosts from the servers; its sign-ins
  from the security schemes (API key in a header, the query or a cookie, bearer, basic, OAuth 2.0 authorization code
  and client credentials); up to `--actions N` actions (25 by default), the main resources' list, look-up, create,
  change and remove first, each with inputs named plainly, an output and its pages (Link header, next address,
  cursor, last id, page number, offset); a README with the vendor's docs and how to register an OAuth app; and a
  recorded test for each action and a refusal, from the description's schemas, that `openduct app test` passes with
  no account. What it cannot take is listed under Not done. `openduct app from-mcp <address> --name <app>` makes an
  app whose actions call a vendor's MCP server's tools: an action may now name `mcp: <tool>` instead of a request,
  run through the MCP client, and be recorded with `call:` and `answer:`. An action may say what it gives
  (`output:`). `openduct app check` reports each app's quality - actions described, with an output and recorded,
  sign-ins, hosts - as a score; `--bar 80` fails one below the bar apps ship at. Four apps made with it from their
  vendors' own descriptions, fetched 2026-11-27: Box, Resend, Intercom and Klaviyo; and a template, *Welcome a new
  sign-up by e-mail with Resend*.

- **Custom apps** (app generator): the studio's Apps page adds a workspace's own app - from an API description's
  address or file, an MCP server's address, or an app.yaml - showing the actions proposed (to untick), the sign-ins
  and the quality report before it is saved; changes, exports and removes them. A custom app is kept on the log
  (`__openduct.model.custom-apps`), the same on every node, offered and read only in its workspace's workflows, with
  credential kinds of its own no other workspace may use. Editors and admins change them; viewers see them. API:
  `/api/apps/custom`.

- **Storage**: files in folders and on servers, read into a run's files and written from them. One *storage* step
  lists, reads, writes, moves, copies and deletes files and makes folders - in a folder of this machine the
  installation allows (`--files-root name=path`, several allowed), on an SFTP server, an FTP server (FTPS by
  default), or in S3 and the stores that speak its API (MinIO, Cloudflare R2, Wasabi, Backblaze B2; with S3's
  presigned addresses) - its credential saying which. Paths are checked before anything is reached: `..`, absolute
  paths and links that lead out of a folder are refused; servers on this machine's own network need
  `--allow-private-network`. SFTP runs on an SSH client of OpenDuct's own (ring only), the server's key pinned or
  kept the first time and then required; an FTP server's passive answer cannot send OpenDuct to another address.
  The *compress* step packs and unpacks ZIP, gzip, tar and tar.gz, bounded against archives that unpack to far
  more than they hold. The *redis* step gets and sets values (with a time to live, or only when missing), counts,
  pushes to and pops from lists, and publishes. Triggers: new (or changed) files in a folder, polled once across a
  cluster and through restarts; a Redis channel's messages, listened to by one node. S3 requests are signed by the
  same code as the S3 sink (`openduct_connect::sigv4`, checked against AWS's own examples). Test runs reach
  nothing; previews read and show writes; each step is an agent's tool. The builder has the steps, triggers and
  their credentials; a template reads a partner's new CSV files over SFTP into a data table.
<!-- web -->
- **Web pages**: a *web* step for what people mean by "fetch this page" and "scrape this site", in OpenDuct's own
  code (a page's markup built into a tree as a browser builds it, CSS selectors, the part a person reads found and
  written out). `fetch` gives a page's readable text as Markdown or plain text - in reading order, headings, lists,
  quotes, code and tables kept, navigation, sidebars, banners, cookie notices, comments and footers left out - with
  its title, description, language, canonical address, links and pictures at their full addresses, and its HTML as
  text or a run's file when asked. `select` picks fields out by CSS selectors (text, an attribute, Markdown or HTML;
  one or all; records of fields inside each element). `crawl` reads a site outward from an address - the same site
  or addresses matching patterns, to a depth, a number of pages and a time - honouring robots.txt (it can be turned
  off) and its Crawl-delay, one host at a polite rate, from its sitemap too when asked. `sitemap` lists a site's
  sitemap or sitemap index, gzipped ones too; `feed` reads RSS, Atom and JSON feeds. A page the run already has
  (`from`) is read without fetching. Limits on bytes, time, redirects, pages, items and a selector's matching; every
  address and redirect checked against this machine's own network; requests name OpenDuct as their User-Agent
  unless the step names another; pages in other character sets (through `encoding_rs`, already in the build) and
  gzip read. A test run fetches nothing and reads a sample page in its place; a preview fetches for real; as an
  agent's tool the model gives the address. A *feed* trigger runs a workflow for each new item, polled once across a
  cluster and through restarts, an unchanged feed answering with nothing. The builder has the step and the trigger;
  templates crawl a documentation site into a table and send a blog's new posts to Slack.
<!-- storage places -->
- **More places for the storage step**: Azure Blob Storage, Google Cloud Storage, Dropbox, Box, OneDrive and
  SharePoint document libraries, and Google Drive, as credential kinds of the same step - every action (list, read,
  write, move, copy, delete, make a folder; presign on Azure and Google Cloud Storage), the same path checks,
  limits, test and preview rules, each over the vendor's own API through the run's HTTP effects (the guard against
  this machine's own network holds). Azure signs each request with the account's Shared Key (a signer of its own, its
  string-to-sign checked against Microsoft's examples) or carries a SAS token, and works with Azurite; Google Cloud Storage trades a
  service account's key file for a token with a JWT signed RS256 on `ring`, and signs V4 addresses. Dropbox, Box,
  OneDrive and Google Drive are connected with the studio's Connect (a new Dropbox app carries the sign-in; the
  Microsoft 365 sign-in asks for `Files.ReadWrite.All` too, for SharePoint sites), their tokens refreshed and kept;
  Box also takes a developer token. Box, OneDrive and Google Drive find files by walking a path's names to ids, from
  a `folder_id` or the top; OneDrive takes a `drive` (`me`, a drive's id, `site:<id>`); a Google Doc is exported when
  read (`export: office`, `pdf`, `text`, `open`). The new-files trigger reads Dropbox's cursor, Graph's delta and
  Drive's changes instead of listing the folder each time, and lists Azure, Google Cloud Storage and Box - once
  across a cluster and through restarts, as before. The builder offers the credentials and each place's settings; a
  template keeps new Dropbox invoices in Azure Blob Storage.
- Forms and runs that wait. A form trigger: a page the studio serves for the workflow (text, long text, number,
  e-mail, date, dropdown, checkbox and file fields, each with help, whether it must be filled in and its limits),
  checked in the browser and again on the server, with a thank-you page or an address to go on to, a page token, an
  optional password and a limit of answers per minute from one address; each answer is a run. Steps that wait: *Ask
  with a form*, *Ask for approval* (approve or reject, with a comment), and *Wait* for a call to a one-time address,
  until a time, or for longer than an hour. A run that waits is kept on the log (`__openduct.flow.waits`), not in
  memory: it survives restarts and the loss of a node, goes on once - with the first answer in the log, or its
  timeout, after which it fails or goes on as the step says - and its earlier steps are not run again. Each step
  that waits has its address before the run reaches it, as `{{ links.<step> }}`. The builder has a fields list and a
  preview; the history shows runs as waiting, and the workflow's runs list what each waits for and its address.
  `--studio-url` sets what those addresses start with. A template: *Approve a refund*.

- **Data tables**: a workspace's own tables with typed columns (text, number, boolean, date-time, JSON), kept in the
  broker and the same on every node of a cluster. The *Data table* step gets, counts, inserts, updates (by a
  condition), upserts (by key columns) and deletes rows - a condition is an expression over `row`, columns equal to
  values, or comparisons - and two runs changing one row change it one after the other. The *Keep between runs* step
  (`state`) keeps a workflow's own values between its runs; a JavaScript or Python step with `state: true` is given
  them, and what it leaves there is kept. The Tables page makes tables (empty or from CSV), changes their columns, and
  shows, edits, imports and exports their rows; Test and Preview read tables and write nothing. The template *Keep new
  contacts in a table* uses them.
- App triggers: an app's definition says what starts a workflow from it - a poll (a list asked for every so often,
  each new item run once, oldest first, up to a limit; the first poll starts from now; what was seen kept in the same
  batch as the runs it starts, so a restart or a node's loss neither repeats nor skips an item, and one node of a
  cluster polls at a time) or a webhook (each delivery checked against the app's signature with the secret the
  credential holds; unsigned, forged, old and repeated ones refused; the address registered with the app when the
  workflow is turned on, where its API allows). 29 triggers of 20 apps: Gmail, Google Sheets, Drive and Calendar,
  Telegram, Slack, Stripe, Shopify, Typeform, Calendly, WhatsApp, WooCommerce, Square, Zendesk, Notion, Airtable,
  Microsoft 365, HubSpot, Mailchimp and Twilio, each with recorded tests. The studio's trigger window has an
  *An app* tile, and a template posts new mail from a supplier to Slack.
- `--start-empty-where-no-copy-survived <name>`: by a person's decision, a partition of which no copy kept its log is
  started again empty after thirty seconds without a leader. A partition with a surviving copy is rebuilt from it,
  as under `--accept-surviving-copies`, which this includes.
- A broker asked to stop gives its leadership to the other brokers first, so restarting brokers one at a time leaves
  no partition without a leader.
- Schedules at set times: a cron expression (5 fields, or 6 with the second first), or every hour at a minute, every
  day, weekday, week (on chosen days) or month (on chosen dates, or the last day) at one or more times - in a time
  zone of the schedule's own, UTC unless it says. Across clock changes a skipped time runs once as the clock jumps,
  and a repeated one once, the first time. The studio's trigger window sets them and shows the next runs on the
  schedule's clock (`POST /api/schedule`); `openduct workflow check` prints them. Each time runs once in a cluster,
  as an interval's do.
- AI steps on the AI step's credentials, with its run record: **Classify text** (one category or several, with a
  fallback, other) and **Sentiment** (a label and a score), whose categories and labels are branches with steps of
  their own; **Extract fields** (named fields with types, or a JSON schema); **Summarise** (a text or a list of texts,
  to a length and a style, a long one in pieces). An answer that must be JSON is checked against its schema and the
  model asked once more on a mismatch; **Ask AI** takes a `schema` too. See [AI steps](docs/ai-steps.md).
- Azure OpenAI, Amazon Bedrock, DeepSeek and xAI among the services an AI credential starts from.
- Template: triage customer messages with AI.
- The **AI agent** step: a model that, in each run, chooses tools over several turns to reach a goal, on any model the
  AI step reaches (both API shapes). Its tools are steps - an HTTP request, JavaScript or Python, another workflow,
  an app's action, another agent - each with a name, a description and the JSON schema of the arguments the model
  fills, checked before it runs; `allow` names those that may run. Memory, off by default, keeps each conversation's
  last turns under a key the step names, on the broker (`__openduct.memory`, compacted), forgotten when `clear` holds
  or after `forget_after`. The answer is text or JSON checked against a schema, with one turn to put it right.
  Limits on turns, tool calls, the step's time and tokens; every turn and tool call is in the run's record and shown
  in the studio, where the step is built with its tools. A template: *An assistant that looks up orders*.
- MCP: every workspace is an MCP server (`/mcp/<workspace>`) that offers its workflows marked `tool:` to AI
  assistants - their name, description and arguments - and runs a workflow for each call, kept in its history. Its
  admins make the tokens assistants sign in with (shown once, limited to some tools and so many requests a minute,
  revoked with a click); the address and tokens are on the Settings page. The new MCP tool step calls a tool of any
  MCP server, or lists its tools, under the HTTP step's rules. Protocol 2026-07-28 and the earlier 2025-11-25,
  2025-06-18 and 2025-03-26, over Streamable HTTP. The template "Your price list, as a tool for AI assistants" uses it.
- A chat trigger: a workflow's chat on a page of the studio's own, on a site by a script tag or a frame, or as JSON
  for a front end of one's own. Each message is a run given the message, the conversation's session id, its earlier
  turns (as an AI step's messages take them) and the person's details; the answer is the respond step's or the last
  step's text, shown as the AI step writes it when the run is on the node the message reached. A password, the
  sites it may be on, limits per conversation and per address, and the page's words and colours are the trigger's;
  the studio's trigger window has Try the chat ([studio](docs/studio.md#the-chat)).
- Answers from your documents, on PostgreSQL with pgvector: **Split text** cuts texts and web pages into pieces (by
  characters, paragraphs, sentences or Markdown headings, with an overlap), each keeping its document and place;
  **Vector store** adds pieces with their vectors (making the table and its indexes when asked, replacing a document's
  old pieces), searches them with metadata filters, a top-k, a least score and a rerank model, and deletes a
  document's pieces; **Ask your documents** answers a question from the passages found and gives back each passage,
  its document, its score and whether the answer cited it. A table's name is checked when the workflow is read and
  every value goes as a parameter. Two templates use them.
- Ten more apps, 36 in all: Google Docs, Supabase, Apify, WordPress, GitHub, Jira Cloud, LinkedIn, X, Facebook Pages
  and Instagram, and YouTube - each with its most used actions and recorded exchanges, built from the vendor's API
  reference. Sign-in at the vendor for Google Docs, YouTube, LinkedIn, X and GitHub; keys or tokens for the rest.
- An app's action may wait up to five minutes for its answer (`timeout`, in seconds), for a service that answers only
  when its work is done; 30 seconds is still the default.
- Files between steps: a file in a run is a value - a name, a media type, a size - passed by reference, its bytes kept
  in the log as long as the runs' history. `extract` reads CSV, JSON, XLSX, XLS, text, HTML and a PDF's text into
  rows, data or text; `convert` makes CSV, JSON, XLSX, text, HTML or Base64 files; `file_details` gives a file's
  details and renames it. The HTTP step makes an answer that is a file a file (`response: auto`, `file` or `text`) and
  sends a file (`file`) or a multipart form (`multipart`); the e-mail step takes `attachments`; a webhook's form gives
  each of its files to the run. A run's record lists its files, with a download for members. Limits: 10 MiB a file,
  20 files and 32 MiB a run, 10 files a webhook delivery.
- **CI/CD for workflows.** API tokens for the studio's API, per workspace, made by its admins in Settings: a role
  (never above the maker's, and never more than the maker still has), an expiry (90 days unless said, a year at
  most), optional workflow scopes, so many requests a minute; shown once, kept as a SHA-256, revoked within seconds
  on every node, each request with one logged with its name. `openduct workflow pull`, `diff`, `push` (`--activate`,
  `--prune`, `--dry-run`), `rollback` and `promote` against a running studio (`--studio`, `OPENDUCT_TOKEN` or
  `--token-file`), with exit codes and `--json` for CI: a push checks every file first and changes nothing if one is
  wrong or the workspace lacks a credential, lookup file, data table or workflow it names; files name other
  workflows by name. Workspaces marked as development, test or production, promotion between them (and a Promote
  button in the editor), a switch that keeps people's edits out of production, and each version's source (studio,
  push, promotion, rollback, Git, and the token) in its history. Git sync with GitHub, GitLab and Gitea through
  their REST APIs: Save to Git (one commit, or a pull request) and Deploy from Git, by hand or on a push delivered
  to a signed webhook. Ready-made GitHub Actions and GitLab CI in `examples/ci`.
- A Helm chart (`deploy/helm/openduct`: one node or a cluster, secrets as files, the health probes, rolling
  restarts), OpenTofu modules for one machine on AWS, Google Cloud and Azure, and *Upgrading and rolling back* in the
  deployment guide.

<!-- messaging -->
- **Email from any mailbox, and the messengers made complete.** A `mailbox` step and an `imap` credential: a folder's
  messages listed and searched, a message read - its text, HTML, headers, its attachments as the run's files - marked
  seen or flagged, moved, deleted, and the folders listed, over IMAP with OpenDuct's own client (TLS by default,
  STARTTLS, a password or an OAuth token through XOAUTH2 kept fresh with its refresh token; every answer bounded). A
  `mailbox` trigger for each new message, polled once across a cluster and through restarts, starting from now and
  again after a folder is rebuilt; `idle: true` waits for news between polls where the server offers IDLE. A test run
  answers with a sample message. Discord through a bot - send, reply, edit, react, threads, a server's channels, roles,
  answers and follow-ups - and a trigger for slash commands and buttons, checked with Discord's Ed25519 signature, its
  PING answered and each interaction deferred at once. Microsoft Teams through Graph - channels, chats, thread
  replies, Adaptive Cards, teams and channels listed, a message read - and a trigger for new channel and chat
  messages through Graph's change notifications: subscribed when the workflow is turned on, its validation answered,
  each notification's client state checked, renewed every 20 minutes and deleted when it is turned off. Slack: reply
  in a thread, update, delete, react, upload a file, open a direct message, list channels, find a person by email,
  schedule a message. WhatsApp: templates, media, reply buttons and lists, marking read. Telegram: buttons, photos,
  documents, edits, answering a button press, and a trigger for messages and button presses. Google Chat: a person's
  sign-in, thread replies, cards. The app kit gained what they needed: Ed25519 and body-carried secrets on webhooks,
  an address check answered back (`echo`), a delivery's own answer (`answer`), registrations renewed (`renew`),
  actions of several requests (`then`) and bodies sent as they are (`body: raw`). Three templates: a mailbox's
  invoices kept in a table, a Discord slash command answered from a table, Teams channel messages sorted by AI.

### Fixed

- A broker new to its cluster - a founder started a few seconds after the first, while the cluster formed - was
  taken for one that had lost its disk: it refused its vote to every partition made before it came, and with two
  such brokers of three those partitions never had a leader (seen as the schema registry never starting). A broker
  is now registered with the controller before it holds a copy of anything; one starting with an empty disk that the
  controller has no record of is new, and its copies vote at once. One on record still catches up before it votes.
- A broker that had begun to stop was handed back, five seconds later, every partition it had just given away - it was
  caught up, as a stopping broker is - and led them until it was killed. A stopping broker now takes no leadership,
  offered or by election; a leader hands a partition back to its first broker only two minutes after taking it over;
  and a stopping broker waits at most five seconds for requests in flight.
- A copy being rebuilt that was its partition's first replica started the group, and so led it with nothing in it -
  under `--accept-surviving-copies`, a partition of which no copy was left came back empty at once, unasked. A copy
  being rebuilt never starts a group.
- A broker's own snapshot was removed at its second clean stop, with the replication log behind it already purged: the
  partition could not start on that broker again ("state machine is behind the purged logs"). Two rolling restarts
  were enough; with every broker restarted twice a partition lost its majority. A snapshot is now removed only when
  another one replaced it and that one reads back from the disk.
- A snapshot whose record could not be read was removed at a start. It is kept, and the copy waits until it reads.
- One partition that could not start stopped every later partition of the broker from starting.
- That a copy was being rebuilt was held in memory only: a broker restarted half way came back able to vote with a
  short log. It is written beside the copy and removed the moment the copy is level with its leader.

### Added

- `openduct health --ready` and `GET /ready`: a broker says whether it can be relied on - joined, every partition
  it holds started, led, and level with its leader - not merely whether it answers
  ([deployment](docs/deployment.md#health)). Gauges `openduct_ready` and `openduct_partitions_*`.
- A copy that cannot be rebuilt is set aside and rebuilt from the partition's leader by itself; so is a broker's copy
  of the cluster's own state ([deployment](docs/deployment.md#when-a-copy-is-lost)).
- `--accept-surviving-copies <name>`: a person's decision, for one recovery, that the copies which kept their log
  are to be taken when most copies of a partition were lost
  ([deployment](docs/deployment.md#when-most-copies-are-lost)).

### Changed

- The replication log is purged only as far as a snapshot on disk, read back, covers - whatever asks for more.
- A broker removes no partition directory while its copy of the cluster's state is not level with a leader it hears
  from.

## [0.1.2] - 2026-11-20

### Added

- Transactions: a producer with a `transactional.id` writes to several partitions and commits a consumer group's
  offsets together or not at all (exactly-once), on one broker and in a cluster, with `read_committed` consumers,
  timeouts, fencing, and Kafka 4's second transaction protocol. ListTransactions, DescribeTransactions,
  DescribeProducers and WriteTxnMarkers for operators ([protocol](docs/protocol.md#transactions)).
- Log compaction: `cleanup.policy=compact` (or `compact,delete`) keeps the newest record of each key at its offset,
  tombstones delete it, compressed batches included; `delete.retention.ms`, `min.compaction.lag.ms`, and `segment.ms`
  closing segments by time. On one broker or in a cluster ([protocol](docs/protocol.md#log-compaction)).
- A topic's settings changed while it runs (AlterConfigs, IncrementalAlterConfigs): `retention.bytes`,
  `max.message.bytes`, `message.timestamp.type=LogAppendTime`, `min.insync.replicas`
  ([protocol](docs/protocol.md#other-topic-settings)).
- A schema registry built in (`--registry-listen`): Confluent's REST API for Avro, JSON Schema and Protobuf -
  subjects, versions, ids, the seven compatibility levels, references between subjects, soft and permanent deletes -
  kept in a compacted topic and served by every broker of a cluster, over HTTPS and held to access rules
  ([schema registry](docs/registry.md)).

### Changed

- A cluster upgraded from 0.1.1 gains topic settings, compaction and transactions once every broker runs 0.1.2
  (feature levels 2, 3 and 4, [upgrades](docs/upgrades.md)); until then they answer `INVALID_CONFIG`.

### Fixed

- A scheduled workflow could run twice for one time when the node reading the schedule was lost. Each time is now
  claimed before it runs, so it runs at most once, across a node's loss too; a run cut short by that loss is not run
  again ([deployment](docs/deployment.md)).
- The email step's SMTP client is lettre 0.11.24: three advisories.

## [0.1.1] - 2026-11-18

### Added

- Metrics for Prometheus: `--metrics-listen` serves `/metrics` - requests and their durations by Kafka API, bytes
  produced and fetched, connections (open, accepted, refused), sign-in failures, partition offsets, consumer group
  lag, leaderless partitions, and under `openduct start` workflow runs by outcome and AI calls and tokens (totals,
  naming no workspace). Off unless given; beyond the loopback address it needs `--metrics-token-file`. A small
  responder of its own: the broker-only build gains no dependency.

### Fixed

- The Linux programs of a release ran only on glibc 2.39 or newer (Ubuntu 24.04 and later), because they were linked
  against the build machine's own glibc; NVIDIA Jetson's JetPack 5 and 6, Ubuntu 20.04 and 22.04, Debian 12 and RHEL
  9 could not start them. They are now built against glibc 2.28 (zig as the linker, through cargo-zigbuild), the
  release fails if either program needs anything newer, and CI runs them, with a JavaScript and a Python step, on
  Debian 10, Ubuntu 20.04 and Ubuntu 22.04, on x86-64 and ARM64.
- Salesforce: a rotated refresh token is kept for the run, so a second sign-in after a session ends no
  longer stops the pipeline; a used-up one (`invalid_grant`) stops it saying so, and points to client
  credentials or a JWT.

## [0.1.0] - 2026-11-17

### Added

- Secrets are never read from the environment. A pipeline names them `secret:NAME` (or `secret:NAME.field`): in
  the studio its own workspace's credentials (checked as it is written; a missing one is named, a replaced one taken
  up without a restart), on the command line `pipeline run --secrets-file`, a file only its owner may read. `env:` is
  refused everywhere and `file:` in the studio. The cluster secret comes from `--cluster-secret-file`, tiered
  storage's keys from `--tier-s3-access-key-file` and `--tier-s3-secret-key-file` (or a role), a client's password
  from `--password-file`; one of the old variables still set stops OpenDuct with where it goes now. Compose,
  Kubernetes and OpenTofu give every secret as a file under `/run/secrets`.
- Workflows run several at once (`runs_at_once`, 10 unless set), through a few connections a node shares; a
  partition with one copy rests its Raft clock; runs are kept and positions committed in batches. Measured with
  `tests/load.rs`: 1,000 workflows idle on 86 connections and run about 8,000 records a second on one node.
- The first working broker: the Kafka wire protocol for producing (idempotent producers included), fetching with
  long polling, offsets, topics (create, list, delete, retention) and consumer groups. It runs on an in-memory
  store or a durable on-disk log. `docs/protocol.md` lists exactly what is supported.
- Limits on every resource a client could exhaust:
  - request size, and a broker-wide request-memory budget;
  - connections;
  - fetch answer size;
  - request-read, idle and write timeouts.
  - the command's own client takes at most 64 MiB in an answer, and memory only as bytes arrive.
  - lengths inside a request or answer: a list, string or byte string that claims more than was sent is refused
    before memory is reserved for it (the Kafka protocol library is built with this change, see
    `vendor/kafka-protocol/OPENDUCT-CHANGES.md`).
- Clean shutdown on SIGTERM and Ctrl-C, without waiting out long polls.
- Replication: each partition kept by several brokers, a Raft group per partition with a controller for topics,
  placement and producer ids. Leaders spread over the brokers; a broker can be lost without losing a write;
  brokers join and leave and partitions move while clients carry on (`openduct cluster status | rebalance |
  remove-broker`); mutual TLS between brokers.
- Consumer groups spread over coordinators: committed offsets in the partitions of an internal topic, each group
  coordinated by the leader of its partition.
- Tiered storage: older segments kept in S3-compatible storage and read back from it.
- Security on the client port: TLS 1.3, sign-in with SCRAM-SHA-256/512, OAuth bearer tokens or client
  certificates (PLAIN over TLS only), access rules that deny by default, and quotas and connection limits for each
  principal. The broker refuses to listen openly without them. `openduct user add | remove`.
- Upgrades one broker at a time: versioned formats on disk and between brokers, and feature levels that rise only
  once every broker has them.
- Leader epochs: each batch is stored in the epoch of the leader that wrote it (0 for a broker on its own, the
  Raft term in a cluster, the same bytes on every copy). ListOffsets answers each offset's epoch, and
  OffsetForLeaderEpoch (API 23) where an epoch ended, so consumers can check their position after a leader change.
- Topic ids, and DeleteTopics by id.
- ListGroups and DescribeGroups: the groups each broker coordinates, with members or with committed offsets only,
  their state, members (client id and host) and assignments, as Kafka's group tools show them; a principal sees only
  the groups it may describe.
- DeleteGroups and OffsetDelete: a group with no members forgotten with its offsets, or its offsets of some
  partitions removed - never those of a topic its consumers subscribe to; replicated in a cluster like a commit.
- DescribeCluster and DescribeConfigs: the brokers and the controller, and the settings of a topic or a broker - only
  those OpenDuct acts on, with the values it acts on, read-only; a topic a principal may not see is refused whether
  it exists or not.
- IncrementalAlterConfigs: a topic's retention.ms changed or put back to for ever, through the controller in a
  cluster; every other setting refused with its name.
- CreatePartitions: partitions added to a topic, placed as a new topic's are or on the brokers named; never taken
  away. In a cluster each new partition knows the entry that made it, so a broker back with an empty disk treats
  it rightly.
- DeleteRecords: a partition starts later from now on - kept across restarts, carried in cluster snapshots,
  segments before it removed - as Kafka Streams uses it to purge its repartition topics.
- The `openduct` command: `serve`, `topic create`, `topic list`, `topic delete`, `send`, `read` and `health`.
  Every `serve` option can also be set by an environment variable.
- `openduct send` sends messages from the command line or standard input, with keys partitioned as other Kafka
  clients partition them. `openduct read` prints what a topic holds (as values, or as JSON with every detail),
  follows it, and resumes under a group name. It reads gzip, snappy, lz4 and zstd batches.
- A user guide: getting started, concepts, connecting applications, running in production, a command reference
  and troubleshooting.
- Expressions (`openduct-expr`) in CEL, the Common Expression Language: its standard functions with the math,
  sets, encoders, bindings and regex extensions and the strings extension's text functions, plus `round`, `sum`,
  `sha256` and two more ways to read a time. For business data, every number is a decimal and an operation on
  null gives null, as in SQL. Mistakes are found when an expression is compiled and said in plain words, with
  where they are; limits on size, nesting, steps, time and answer size keep any expression or message from taking
  over. Every example in the guide is a test (`docs/expressions.md`).
- Pipelines (`openduct-pipeline`, `openduct pipeline check | test | run`): read a topic, then `let`, `where`, `each`,
  `map`, `key` and `headers`, then write another topic. Results are written before progress is committed, a
  message that fails goes to an `on_error` topic with the reason (or stops the pipeline), and copies of a
  pipeline share the topic as one consumer group.
- Sixteen worked pipeline examples, each a test of its exact output.
- Connectors (`openduct-connect`): a pipeline's `from:` or `to:` can name a system instead of a topic (one side is
  always a topic). Secrets come from `env:` and `file:` values and are never shown. `http` polls an API with
  expressions for its records, position and next page, and carries on from its saved position; `webhook` receives
  signed POSTs and answers 202 only once a delivery is stored; `elasticsearch` indexes by key through the bulk API
  and sets refused documents aside; `s3` writes gzip JSON-lines objects to S3 or any S3-compatible store, signed
  with Signature Version 4, committing only what is stored. TLS is rustls with ring.
- The `mqtt` connector, both ways, for devices and machines (MQTT 3.1.1, with its own client). Reading
  subscribes with QoS 1 in a persistent session and acknowledges each message only once it is in the topic, so
  nothing a device sent is lost across a stop; writing publishes by key and counts a batch as stored once the
  MQTT broker has acknowledged it. `mqtts://` trusts the system's CAs or a private one, and can present a client
  certificate. A refused login, subscription or certificate stops the pipeline with the reason.
- The `modbus` connector: named values read from machines, meters and PLCs over Modbus TCP on a schedule, as
  one JSON message per read. Registers as 16-, 32- and 64-bit whole numbers and floats, in either word order,
  single bits of status words, coils and discrete inputs, and exact scaling. Neighbouring values are read in one
  request and gaps are never read; `changes_only` writes a reading only when it changed. Its own client checks
  every answer, and a refused read stops the pipeline with the reason.
- The `kafka` connector, both ways, for topics of another Kafka-protocol cluster (Kafka 2.4 or later, or another
  OpenDuct). Reading is as a member of a consumer group there, with Kafka's standard consumer protocol, so copies
  of a pipeline share the partitions, and each partition's offset is committed in that group only once what was
  read is stored. Writing places keys as Kafka's own clients do, waits for every replica (acks all), can compress
  with gzip, snappy, lz4 or zstd, and sets aside a message the topic refuses with the cluster's reason. TLS (the
  system's CAs or a private one, and client certificates) and SASL sign-in with SCRAM-SHA-512, SCRAM-SHA-256 or
  PLAIN (PLAIN only over TLS); the cluster's SCRAM proof is checked too. Tested against Apache Kafka 4.4.
- The `postgres_cdc` connector: every row inserted, updated and deleted in chosen tables of PostgreSQL 10 or
  later, from its logical replication with the built-in `pgoutput` plugin, so nothing is installed on the
  database. The first time, the rows already there are read too, from the same moment the stream starts from.
  Values keep their types (exact numbers, JSON as JSON); each change is keyed by table and primary key; a
  transaction is confirmed to the database only once it is stored, so nothing is lost while the pipeline is
  stopped. Its own client signs in with SCRAM-SHA-256 or MD5 (and a password in clear only over TLS) and checks
  the server's SCRAM proof.
- The `mysql_cdc` connector: every row inserted, updated and deleted in chosen tables of MySQL 8.0 or 8.4, read
  from the binary log as a replica reads it, so nothing is installed on the database. The first time, the rows
  already there are read too, in one consistent snapshot and without locks; the pipeline saves its binary log
  position only after a transaction's rows are stored. Values keep their types (exact decimals, JSON as JSON,
  ENUM and SET as text, TIMESTAMP in UTC, binary as hex); every event's CRC32 is checked. Its own client signs in
  with caching_sha2_password (encrypting the password to the server's key without TLS) or mysql_native_password.
- The `mongodb_cdc` connector: every document inserted, updated, replaced and deleted in chosen collections of
  MongoDB 6.0 or later (replica sets, sharded clusters, Atlas), from its change streams with MongoDB's own Rust
  driver (over rustls). The first time, the documents already there are read too; the stream's resume
  token is saved only once the changes before it are stored. Updates carry the whole document, what changed, and
  the document as it was where the collection keeps it; values are plain JSON (object ids as hex, dates in UTC,
  exact decimals).
- The `salesforce` connector, both ways, through the REST API with OAuth 2.0 (client credentials, a JWT signed
  with the app's private key, or a refresh token; a session that ends is begun again). Reading polls one object
  with SOQL through `queryAll`, so deletions are seen, in the order records last changed, with a position of time
  and id that neither skips nor repeats a record. Writing upserts by an external id (or inserts, or updates by
  Id) 200 records a call, and sets aside a record Salesforce refuses with its reason. Tested against a stand-in
  for the API until an org is given.
- The `servicenow` connector, both ways, through the REST Table API, with basic sign-in or OAuth 2.0 (the
  password grant or client credentials; a token that ends is got again). Reading polls one table in the order
  records were last updated, with a position of time and id that neither skips nor repeats a record, and an
  encoded query of the pipeline's own. Writing inserts, updates by `sys_id`, or upserts by a field of the
  pipeline's choosing, and sets aside a record ServiceNow refuses (a data policy, a business rule) with its
  reason. Tested against a stand-in for the API until an instance is given.
- The `snowflake` connector, both ways, through the SQL API, signed in with a key pair (a JWT, renewed before its
  hour ends) or a programmatic access token. Reading polls one table in the order of a column that grows and a
  unique id, with a position bound to the query that neither skips nor repeats a row; values become plain JSON
  (exact numbers, ISO 8601 times, semi-structured values as JSON). Writing inserts or merges many rows a
  statement, each value cast to its column's type, with a request id so a statement sent again is not run twice;
  a row Snowflake refuses is set aside with its reason. Statements still running are waited for, and answers in
  several parts read whole. Tested against a stand-in for the API until an account is given.
- The `opcua` connector reads named values of a machine, PLC or SCADA server as they change, through an OPC UA
  subscription: signed and encrypted by default (Basic256Sha256, Aes128_Sha256_RsaOaep or Aes256_Sha256_RsaPss),
  a server's certificate trusted only once it is moved to the trusted folder, anonymous or a user and password,
  and a deadband for numbers. The first connection must succeed; a dropped one is made again with the
  subscription. Built on async-opcua (MPL-2.0, pure Rust), held at one commit of its repository because the
  released 0.19.0 has a known RSA timing weakness, an unmaintained YAML parser and no early check of a message's
  declared size. Tested against a real OPC UA server, including a server that declares a message too large.
- `openduct pipeline run` logs what connectors report as they go (a connection made again, messages a cluster
  removed before they were read); `RUST_LOG` sets the detail.
- Example applications:
  - sensor telemetry, in Rust;
  - a broker embedded in your own program, with a durable log.
- End-to-end tests over TCP with an independent Kafka client (rskafka).
- Hostile-input tests (`crates/openduct-hostile`): every place OpenDuct reads bytes it did not write - Kafka requests,
  record batches, segment files, expressions, pipeline files, MQTT packets, PostgreSQL replication messages, MySQL
  binary log events, and requests to a running broker - fed mutated input with a fixed seed in every test run, and
  fuzzed with cargo-fuzz in CI (`fuzz/`).
- CI: format, lint, tests on Linux and macOS, cross-compile checks for 64-bit and 32-bit ARM and static Linux, a
  licence and advisory check, and the client checks.
- Project governance:
  - Apache License 2.0;
  - Contributor License Agreement;
  - Code of Conduct;
  - security policy;
  - contribution guide.
