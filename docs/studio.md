# The studio and workflows

New to it? [Your first workflow](tutorials/first-workflow.md) builds one step by step, with screenshots.

`openduct start` runs the broker and, beside it, the **studio**: a page in the browser where workflows are built,
tried and watched. A workflow is a trigger and a list of steps. It runs on the broker underneath - each workflow
reads its own topic as a consumer group - so it keeps what Kafka-style storage gives: nothing lost on a crash, runs
spread over the nodes of a cluster, and each event kept until it has been handled.

```sh
openduct start --data-dir /var/lib/openduct
# OpenDuct is ready: open http://127.0.0.1:5480
```

The first visit makes the owner's account. Everything else is in the page.

## Workspaces and people

- A **workspace** holds workflows, credentials, lookup files and runs. Its topics are its own: a workflow's topic
  is kept as `w.<workspace>.<name>`, so two workspaces never read each other's messages.
- **Members** have one of three roles: **Admin** (members too), **Editor** (builds and runs), **Viewer** (looks).
  The installation's admin sees every workspace.
- **Alerts**: a workspace's admin chooses where failed runs are told - Slack, Microsoft Teams, Google Chat, Discord,
  Telegram or WhatsApp.

## Triggers

| Trigger | Starts a run when |
|---|---|
| **Webhook** | Another system calls the workflow's address. The address is random; a token may be asked for as well (header, bearer token, or `?token=`). The delivery is kept on the workflow's topic before it is answered `202`, or, with a *Respond to webhook* step, answered with the status and body the step gives. |
| **Schedule** | Every so often - `30s`, `15m`, `1h`, `1d` - counted from midnight UTC (every 15m runs at :00, :15, :30 and :45); or at [set times](#schedules-at-set-times), in a time zone. In a cluster each time runs once. |
| **Topic** | A message arrives on a topic. |
| **By hand** | Someone presses Run. |
| **When one fails** | Another workflow's run fails: its record, its error and where it failed. |
| **Chat** | Someone writes in the workflow's chat: on a page of its own, on a site (a script tag or a frame), or from a front end of one's own (JSON). See [the chat](#the-chat). |
| **A database** | A row is added to a table, or changed, in MySQL, SQL Server or PostgreSQL: see [database triggers](#database-triggers). |
| **New files** | A new (or changed) file is in a folder: on an SFTP or FTP server, in S3, or in a folder of this machine. See [storage](#triggers-new-files-and-a-redis-channel). |
| **Redis** | A message is published on a Redis channel. See [storage](#triggers-new-files-and-a-redis-channel). |
| **A feed** | A new item is in an RSS, Atom or JSON feed: a blog's post, a site's news. See [a feed's new items](#a-feeds-new-items). |

### Schedules at set times

In the trigger's window, **How often** chooses: every hour (at a minute past it), every day, weekdays, weekly (on
the days chosen), monthly (on the dates chosen, or the last day), or a cron expression. A schedule may list several
times, and has a **time zone** of its own (UTC unless it says). **Next runs** shows the next five times on that
clock as you change it - or what is wrong, in plain words. In a workflow file:

```yaml
trigger:
  schedule: { every: weekday, at: "08:30", timezone: America/New_York }
# every: hour (at: ":15"), day, weekday, week (on: [monday, thursday]), month (on: [1, 15, last]); at: one time or a list
# or: cron: "0 9 * * 1-5" - minute, hour, day of the month, month, day of the week (six fields: the second first)
```

- **Clock changes:** a time the clock skips runs once, as it jumps; a time it repeats runs once, the first time.
- **Limits:** at most once a second; up to 24 times or cron expressions in one schedule. A time missed while every
  node was down is not run afterwards.
- `value.scheduled_at` is the time it was due, in UTC; `trigger.slot` the same in milliseconds.
- `openduct workflow check` says when a schedule runs next, too.
| **Form** | Someone fills in a form the studio serves at the workflow's own address, `/forms/<random>`. Each answer, checked, is a run: `value.<field>`. See [forms and runs that wait](#forms-and-runs-that-wait). |
| **An app** | Something new in an app: an email, a row, a file, a message, an order, a payment, a form answered. Some apps are asked every so often (a poll), others send each one as it happens (a webhook). See below. |

### App triggers

Choose **An app** in the trigger window, then the app, what starts it (*A new email*, *A new row*, *An event*), the
credential, and the trigger's own settings - a search, a sheet, a channel, which events.

- **A trigger that asks the app** (Gmail, Google Sheets, Drive and Calendar, Telegram, Slack, Notion, Airtable,
  Zendesk, Microsoft 365 mail, HubSpot, Mailchimp, Twilio) asks every few minutes - *Every*, 1m to 1d - and runs each
  new item once, the oldest first, up to *At most* each time (the rest the next time). The first time, it only notes
  what is already there: only what comes after the workflow is turned on runs. What it has seen is kept with the
  workflow's runs, so a restart or a node's loss neither repeats an item nor passes one by; in a cluster one node
  asks at a time. When the app refuses (a credential no longer valid, the app down), the run history says why, once,
  and nothing is lost: the next time asks from the same place.
- **A trigger the app calls** (Stripe, Shopify, Typeform, Calendly, WooCommerce, Square, Telegram, Slack's Events API,
  WhatsApp) has an address, shown in the window. For Stripe, Shopify, Typeform, Calendly, WooCommerce, Square and
  Telegram, turning the workflow on gives the app the address (and takes it away when turned off): nothing to copy.
  Slack and WhatsApp are set up in their own pages, as the window says; their credential holds the app's signing
  secret (and WhatsApp's, a verify token of your choice). Each delivery is checked against the app's signature: one
  unsigned, forged, or signed more than five minutes ago is refused and runs nothing, and one the node has taken
  before is answered without running again.
- The studio must be reachable from the app at the address it gives (the name you reach the studio by, over HTTPS
  when it runs behind a proxy with `--secure-cookies`): an app cannot send to `localhost`.

A run's `value` is the item (an email with `from`, `to`, `subject`, `snippet` and the whole `message`; a row by its
column names and `row_number`; the app's event), and `trigger.id`, `trigger.app` and `trigger.when` say which one and
from where. The apps' own pages ([apps](apps.md)) say what each trigger gives.

## Steps

| Family | Steps |
|---|---|
| Data | Set fields, Filter (only if), Branch (if / else), Switch (several ways), Loop (for each), List tools (keep, sort, de-duplicate, limit), Group and summarise, Merge, Date and time, Look up a file |
| AI | Ask AI (one form whatever the model: OpenAI, Anthropic, Google Gemini, Mistral, Groq, OpenRouter, Together, Azure OpenAI, Amazon Bedrock, DeepSeek, xAI, a model on this machine through Ollama or LM Studio, or any compatible endpoint; its answer can be held to a JSON schema), Classify text and Sentiment (each category's or label's steps run), Extract fields, Summarise, Embed text: see [AI steps](ai-steps.md) |
| AI | Ask AI (one form whatever the model: OpenAI, Anthropic, Google Gemini, Mistral, Groq, OpenRouter, Together, a model on this machine through Ollama or LM Studio, or any compatible endpoint), Embed text |
| AI agents | AI agent: a model that uses tools over several turns to reach a goal, with memory ([below](#ai-agents)) |
| Data stores | Query a database (SQL on PostgreSQL; with pgvector, the texts nearest a question), Write to a topic |
| SQL databases | MySQL (and MariaDB), SQL Server: see [SQL databases](#sql-databases) |
| Messages | Send an e-mail (through your own mail server), the messengers, and every [app](apps.md) |
| Code | A snippet of your own, run in a locked-down process: no environment, no files but its input and output, limits on its memory, CPU time and processes, stopped after 60 seconds |
| Calls | HTTP request (any API), Run a workflow (another of the workspace, its result used), Respond to webhook |
| MCP | MCP tool: a tool of any MCP server called, or its tools listed ([below](#mcp-tools-for-ai-assistants-and-from-them)) |
| Control | Wait, Stop (end the run, or fail it with a reason) |
| Control | Wait (a while, until a time, or for a call to its one-time address), Ask with a form, Ask for approval, Stop (end the run, or fail it with a reason) |
| Data tables | Data table (get, count, add, update, add or update, delete rows), Keep between runs: see [Data tables](#data-tables) |
| Files | Extract from a file, Convert to a file, File details: see [files between steps](#files-between-steps) |
| Web | Read a web page: a page's text, fields picked by CSS selectors, a site crawled, a sitemap, a feed - see [web pages](#web-pages-read-pick-out-crawl-sitemaps-and-feeds) |

Each step names what it needs with [expressions](expressions.md): `value` is the event that started the run,
`steps.<name>` what an earlier step gave, `trigger` how the run started, and in a loop `item` and `index`. A text
field takes templates - `Order {{ value.id }} is late` - and a condition is an expression - `value.total > 100`.

**When a step fails:** it is tried again as often as its *Tries* say, with a wait between. If it still fails, the
run either stops and sends its record to **CatchErr** (by default), or carries on with the next step.

## AI agents

An **AI agent** step is a model that, in each run, works towards a goal over several turns: each turn it calls one
or more of its tools, or answers. What a tool gives back goes to the model for its next turn.

```yaml
- name: assistant
  agent:
    credential: model                  # an ai credential, as Ask AI's
    instructions: You answer the shop's customers. Look an order up before you say anything about it.
    input: "{{ value.message }}"       # the goal of this run
    tools:
      - name: order_status
        description: An order's status, by its id.
        parameters:                    # the JSON schema of the arguments the model fills
          type: object
          properties: { id: { type: string } }
          required: [id]
        http: { url: "https://shop.example.com/api/orders/{{ args.id }}", credential: shop }
    allow: [order_status]              # optional: the only tools that may run
    memory: { key: "{{ value.chat_id }}", window: 10, clear: "value.message == '/start'" }
    output: { schema: { type: object, properties: { answer: { type: string } }, required: [answer] } }
    max_turns: 10
```

- **The model**: the credential's (or `model`), asked as Ask AI asks it - so any model Ask AI reaches, in either API
  shape. `temperature` is optional; a model that refuses it is asked again without it.
- **Tools.** Each is a name (letters, digits, `_` and `-`), a description the model chooses it by, the JSON schema of
  its arguments, and one step it runs: an HTTP request (with this machine's network kept out of reach, as for the
  HTTP step), JavaScript or Python (sandboxed, as the code steps are), another workflow of the workspace
  (`run_workflow`: the arguments are its input unless it names one), an app's action (the inputs the step does not
  give are the model's to fill, and the schema is made from them), another agent (it takes its task as
  `args.input`), or any other step that answers - not those that shape a run (filter, branch, switch, loop, wait,
  stop, respond). A tool reads the model's arguments as `args`, beside `value`, `steps` and `trigger`. The arguments
  are checked against the schema before the tool runs; a mismatch, a failure or a refusal goes back to the model,
  which may try again. At most 32 tools; agents use other agents as tools at most 3 deep.
- **What may run.** `allow` names the tools that may run: the others are not offered to the model, and a call to one
  is refused. (A person's approval before a tool that changes data runs is still to come.) The arguments come from
  the model, which the message it is given can steer: take them as you would a webhook's input, and give a tool that
  changes data only what it needs.
- **Limits.** `max_turns` (10 by default, at most 50); `max_tool_calls` (20, at most 100 - after the last, the
  model is told to answer with what it has; and at most 32 in one turn); `timeout`, the step's time (5m, at most
  30m: a call still going then is cut off); `max_tokens`, one answer's (1,024); `max_total_tokens`, the whole step's
  (200,000).
- **Memory**, off unless asked. The conversation is kept under the key the step names - a chat's id, a customer's -
  and its last `window` turns (a message and its answer: 10 by default, at most 50) go back to the model in the next
  run with that key. `clear`, an expression, forgets it and starts afresh when it is true; a conversation is
  forgotten `forget_after` its last turn (30d by default, from 1m to 365d). Memories are kept on the broker, in the
  compacted topic `__openduct.memory` - durable, and the same on every node of a cluster - under the workspace, the
  workflow and the step, so no other workspace, workflow or step reads them. Only the messages and the answers are
  kept, each cut to 8,000 characters, not the tools' calls. Two runs of the same conversation at once each keep their
  own turn, the later one winning: a chat's messages in order (one at a time, or keyed) keep them all. An installation
  keeps at most 100,000 conversations, every workspace's together; past that, a new one lets go of the one nearest to
  being forgotten.
- **Memory kept elsewhere.** `store` keeps the conversations in a store of the workflow's own, with the same window,
  clear rule, time to forget and keys (`w.<workspace>.<workflow>/<step>/<key>`):
  `store: postgres, credential: app-db, table: agent_memory, create: true` - one row a conversation (`key`, `turns`
  as JSON, `forget_at`; the table made with an index on `forget_at` when `create` says), rows past their time not
  read and a few deleted on each write; `store: redis, credential: cache` - the key `openduct:memory:<key>`, its TTL
  the time to forget; `store: table, table: agent_memory` - a data table with the columns `key` (text), `turns`
  (JSON) and `forget_at` (date and time), which the studio offers to make, a row holding at most 64 KiB (the oldest
  turns let go to fit). A test run reads nothing and keeps nothing; a preview reads, and keeps nothing.
- **The answer** is text, or with `output: json` any JSON, or with `output: {schema: ...}` JSON matching a schema -
  checked, with one more turn to put a wrong answer right, and the step failing, saying why, if it is still wrong.
  The JSON is `steps.<name>.json`, the text `steps.<name>.text`.
- **Its record**: the answer, the `usage` of all its turns, every `turn` (its time, tokens and the tools it called)
  and every `call` (the tool, its arguments, its result cut short, its time, its error), and the conversation as a
  `transcript`; the studio shows each call. A **test** sends nothing: it shows the first request it would send and
  answers as a real run would, saying so, and starts with no memory. A **preview** asks the model for real, runs the
  tools that read, shows those that write without sending them, reads the memory and keeps nothing.
- **Schemas** are read for `type`, `enum`, `const`, `properties`, `required`, `additionalProperties`, `items`,
  `anyOf`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems` and `maxItems`; `description`, `title`,
  `default`, `examples`, `format` and `pattern` go to the model but are not checked.
## The chat

A workflow whose trigger is **Chat** has a chat at `/chat/<hook>` - a random address, as a webhook's is. Each message
is a run, and the run's answer goes back to the chat.

```yaml
trigger:
  chat:
    texts: { title: Help desk, welcome: "Hi! Ask me about your order.", placeholder: Your question }
    theme: { accent: "#0f766e", background: "#ffffff", text: "#1f2933" }
    origins: ["https://www.example.com"]   # the sites that may show it and send it messages
    password: ""                           # none; set one and the page asks for it
    history: 10                            # earlier turns each run is given (0 to 50)
    limits: { message: 4000, per_session: 20, per_address: 60 }
    wait: 2m                               # how long the chat waits for an answer (1s to 5m)
    stream: answer                         # the AI step shown as it is written (default: the last step, if AI)
steps:
  - name: answer
    ai:
      credential: model
      system: You answer our customers kindly, in a few sentences.
      messages: "{{ value.history }}"
      prompt: "{{ value.message }}"
```

**Where the chat is** (the trigger's window shows each, ready to copy):

- **Its page**: `https://<studio>/chat/<hook>`, made by the studio from the trigger's words and colours, with no script,
  style or font from anywhere else.
- **On a site**, as a chat button: `<script src="https://<studio>/chat/<hook>/embed.js" async></script>`
  (`data-open="true"` opens it at once, `data-side="left"` puts it on the left, `data-name` and `data-email` say who
  the person is); or in a frame: `<iframe src="https://<studio>/chat/<hook>">`. The site must be in `origins`.
- **From a front end of your own**: `POST /chat/<hook>/messages` with `{"message": "...", "session": "...",
  "person": {...}}`. The answer is `{"answer", "session", "run"}` (and `data`, a respond step's whole body), or, with
  `accept: text/event-stream`, events: `start`, `text` as the answer is written (`reset` when its step starts
  again), then `answer` - or `error`.

**What a run is given**: `value.message` (what the person wrote), `value.session` (the conversation's id, the same for
every message of it), `value.history` (its earlier messages, oldest first, as an AI step's `messages` takes them:
`[{role, content}]`), and `value.person` (what the site or the front end said about the person, or `{}`). `trigger` is
`{trigger: "chat", session, at, reply}`. Messages of one conversation run one after another, in order.

**The session is the conversation's key.** The page makes a random one and keeps it in the browser; a front end
sends its own (16 to 128 letters, digits, `-`, `_`, `.`, `:` - unguessable, since whoever has it continues the
conversation), or is given one in the first answer. An AI agent's memory keys a conversation by it.

**The answer** is the run's last respond step's body (its `text`, `answer` or `message`, or the body itself when it
is text), or else the last step's `text`, `answer`, `message`, `output` or `reply` - an AI step's `text`. A run that
is filtered out answers nothing; one that fails shows the trigger's error words, never the error.

**As it is written**: the AI step named by `stream` asks its model for its answer as events and the chat shows each
piece as it comes; the respond step's (or last step's) answer replaces it when the run is done. This is so when the
run is on the node the chat's message reached - always, on one node; in a cluster, a run on another node answers
whole when it is done.

**Earlier messages**: each node reads the workflow's history of runs (kept 14 days) as it is written, so a
conversation goes on whichever node its next message reaches, and across a restart. A node remembers a
conversation for a day after its last turn - its last 50 turns, within 48 KB (each message and answer kept to 8,000
characters) - read back from the last 1,000 runs when it starts.

**Protection**:

- No cookie is read or set: a site cannot act for anyone through a visitor's browser. A message from a browser must
  come from the chat's own page or a site in `origins`, as JSON; any other site is refused (and is not let read an
  answer).
- **Password** (optional): the page asks for it; a front end sends it as `authorization: Bearer <password>`, as
  `x-openduct-token`, or as `password` in the body. Ten wrong ones in a minute make an address wait a minute.
- **Limits**: messages a minute per conversation (`per_session`, 20) and per address (`per_address`, 60; 0 for none -
  behind a proxy every message comes from the proxy's address); a message of at most `limits.message` characters
  (4,000; at most 16,000) and 16 KB; a request of at most 64 KB; at most 512 messages of one chat waiting for their
  answers on a node. Limits are counted on each node.
- The page shows every word as text, never as HTML, and only the studio and the sites in `origins` may frame it.
- What a person writes, and what the site says about them, is a run's input: it is kept in the workflow's history
  of runs, as every run's input is, for 14 days.

**Try the chat**, in the trigger's window, runs the workflow as it is in the editor - saved or not, on or off - on
each message, as Preview does: the model is asked for real, what writes is only shown. The panel keeps the
conversation and sends it with each message; the last run's steps are shown below it.
## Answers from your documents

Three steps keep documents in PostgreSQL with pgvector - or in a vector database, or the run's own memory (see
[Vector databases](#vector-databases)) - and answer questions from them, showing what each answer rests on. The
templates *Keep documents for answers*, *Answer from your documents* and *Answer from documents kept in Qdrant* use
them.

| Step | What it does |
|---|---|
| **Split text** (`split_text`) | Cuts one text (`text`) or a list of documents (`documents`: texts, or records with `text`, `source` and `metadata`) into pieces of at most `size` characters (50 to 20,000; 1,000 unless it says): by `paragraphs` (the default), `sentences`, `markdown` headings (no piece crosses a heading; each keeps the headings it is under in its metadata) or `characters` (cut at a blank line, a line's end, a sentence's end or a space). `overlap` (up to half the size) begins each piece with the end of the one before. `format: html` reads a web page as text first. Each piece is `{id, source, position, text, start, end, metadata}`; `metadata` adds fields to every piece. Up to 2 MiB of text and 5,000 pieces a step. |
| **Vector store** (`vector_store`) | On a PostgreSQL credential and a `table` - or another `store` and its `collection`. `action: add` keeps `pieces` with their vectors - made by `embed_with` (an AI credential; its `embedding_model`, or the credential's), or given (`embeddings`, or each piece's own `embedding`); `create: true` makes the table and its indexes the first time; a document added again loses the pieces it no longer has (`replace: false` keeps them). `action: search` finds the `top_k` pieces (1 to 50, 5 unless it says) nearest a `query` (or a `vector`), only those whose metadata holds `filter`, at least `min_score`, reordered by a `rerank` model when it names one. `action: delete` removes every piece of a `source` (or a list of them). |
| **Ask your documents** (`ask_documents`) | Makes the `question` a vector, finds the nearest pieces as a search does (`top_k` 4 unless it says, `filter`, `min_score`, `rerank`), and has a model - `answer_with` (or the `embed_with` credential) and its `model` - answer from those passages alone, citing them as [1], [2]; `instructions` adds to what it is told. Its result: `text`, `answered`, `cited`, and `sources` - every passage it was found, with its document, place, score, whether it was given to the model (`used`) and whether the answer cited it (`cited`) - with `usage`, `transcript` and how long each part took. When nothing is found, no model is asked: the answer is `no_answer`. |

The table a store makes - or one made by hand the same way - is `id text PRIMARY KEY, source text, position integer,
content text, metadata jsonb, embedding vector(<dimensions>), updated_at timestamptz`, with an index on `source`, one
on `metadata` for filters, and an HNSW index on `embedding` for the store's `distance` (`cosine`, the default; `l2`;
`inner_product`).

- **No SQL from data.** The table's name is checked when the workflow is read (small letters, digits and `_`, with a
  schema before a `.`); every value - texts, vectors, filters, sources, the top-k - goes to the database as a
  parameter.
- **Scores are higher for nearer pieces:** the cosine similarity (1 - the cosine distance), the inner product, or
  1 / (1 + the Euclidean distance). With a rerank model, the score is its relevance, and the vector's is kept as
  `vector_score`.
- **Filters are exact.** A search with a filter compares every piece the filter matches (found through the metadata's
  index), so a narrow filter still finds its top-k; without one, the HNSW index finds the nearest, approximately, as
  pgvector does. Past 2,000 dimensions pgvector indexes no vector: the table is made without that index and searched
  exactly.
- **Reranking** goes to the AI credential's `<base_url>/rerank`, in the form services that rerank share (`{model,
  query, documents, top_n}`, answered with each passage's `relevance_score`). Of the services the studio lists,
  Together AI offers it (on a dedicated endpoint); a service without it answers 404, and the step says so.
- **Test and Preview.** A test run sends nothing: it shows the requests and the SQL it would send. A preview makes the
  vectors, searches and answers for real, and only shows what would add or delete.

### Vector databases

`store` names where a `vector_store` or `ask_documents` step keeps its pieces; without it, the store is the one its
credential's kind says (a `postgres` credential: PostgreSQL). Name it - the studio's *Kept in* always does - so that a
filter the store cannot express is refused when the workflow is read, not when it runs.

```yaml
- name: keep
  vector_store:
    action: add
    store: qdrant              # postgres, qdrant, weaviate, milvus, chroma, pinecone, redis, mongodb, memory
    credential: vectors        # a credential of the store's own kind
    collection: handbook       # PostgreSQL's is a table
    create: true               # made on first use, its vectors' width taken from the first pieces
    embed_with: model
    pieces: steps.split.pieces
- name: find
  vector_store:
    action: search
    store: qdrant
    credential: vectors
    collection: handbook
    embed_with: model
    query: "{{ value.question }}"
    filter: { team: support, tags: [faq] }
```

Every store does the same: `add` (a piece added again by its id replaced; a document added again loses the pieces it
no longer has), `search` (the same `top_k`, `min_score`, `filter` and `rerank`), `delete` by document - with the same
result (`table` names the collection, beside `store`), the same bounds (5,000 pieces a step, 32 KiB a piece, 16 KiB
of metadata), and the same test and preview rules: a test run sends nothing and shows its first request; a preview
searches for real and shows what would write. A score means the same on every store - the cosine similarity, the
inner product, or 1 / (1 + the Euclidean distance) - and `distance` is pgvector's, whatever the store answered.

**Filters** are written once. A field given text, a number or true or false is equal to it; a field given a list
holds every one of its values (it is a list too, as `tags`); a field given a record is a field inside a field, on the
stores whose metadata keeps its shape. Each store is sent its own filter language:

| Store | Credential (`kind`: fields) | Where a piece is | A filter becomes |
|---|---|---|---|
| PostgreSQL | `postgres` | a row of `table` (above) | pgvector's `metadata @> $2` |
| Qdrant | `qdrant`: `url`, `api_key` | a point of `collection` (its id a UUID made from the piece's), payload `{piece_id, source, position, text, metadata}`, keyword indexes on `source` and `piece_id` | `must` conditions on `metadata.<field>` (`match`, or a range from a number to itself when it is not whole) |
| Weaviate | `weaviate`: `url`, `api_key` | an object of `collection` (Weaviate makes its first letter a capital), `vectorizer: none`, with `piece_id`, `piece_source`, `piece_position`, `piece_text`, `piece_metadata` and one property for each top field of the metadata | `where`: `Equal`, `ContainsAll` for a list, joined by `And`; no field inside a field |
| Milvus, Zilliz | `milvus`: `url`, `token` (`user:password`, or an API key), `database` | an entity of `collection`: `id`, `vector`, `source`, `position`, `text`, `metadata` (JSON) | `metadata["team"] == {v0}`, `json_contains_all(metadata["tags"], {v1})`, its values apart as `exprParams` (Milvus 2.5 and later) |
| Chroma | `chroma`: `url`, `api_key`, `tenant`, `database` | a record of `collection` (3 or more characters): its text the document, metadata `piece_source`, `piece_position`, `piece_metadata` and the top fields | `where`: `$eq`, `$contains` for each value of a list, joined by `$and`; no field inside a field |
| Pinecone | `pinecone`: `api_key`, `cloud`, `region` | a vector of the serverless index `collection`, in `namespace` when named; its text and own fields are metadata (`piece_text` ...), within Pinecone's 40 KB a record | `{field: {$eq: value}}` joined by `$and`; lists of text only; no field inside a field |
| Redis 8, Redis Stack | `redis` (the `redis` step's) | the hash `<index>:<piece id>`, its vector 32-bit floats, under the index `collection` (`FT.CREATE ... ON HASH`) | tags: one for each `field=value` of the metadata (the start of its SHA-256), asked as `@tags:{...}` before the KNN |
| MongoDB Atlas | `mongodb`: `url` (`mongodb+srv://...`), `username`, `password`, `auth_source`, `tls`, `ca` | a document of `database.collection`, `{_id, source, position, text, metadata, tags, embedding}`, with the vector search `index` (`vector_index` unless named) | `$vectorSearch`'s `filter` on `tags` (`team="sales"`), joined by `$and` |
| The run's memory | none (`store: memory`) | kept by the run alone, compared exactly | as PostgreSQL's |

- **Requests.** The web stores (Qdrant, Weaviate, Milvus, Chroma, Pinecone) are reached through the run's HTTP
  requests, Redis and MongoDB over their own protocols on the connections the storage steps open: this machine's own
  network is refused unless OpenDuct is started with `--allow-private-network` (a store on `localhost` needs it).
- **Making the collection.** `create: true` makes what holds the pieces when it is not there, for the first pieces'
  width and the step's `distance`: Qdrant's collection and its indexes; Weaviate's collection, with the first pieces'
  metadata fields matched whole; Milvus's collection, its index (`AUTOINDEX`) and loaded; Chroma's collection with its
  `hnsw` space; a Pinecone serverless index on the credential's `cloud` and `region` (`aws`, `us-east-1`), waited
  for until ready (up to 90 seconds); Redis's index (`HNSW`); MongoDB's collection and its vector search index, which
  Atlas builds in the background - a search right after may find nothing yet.
- **What a store says.** Pinecone does not say how many vectors a delete removed: `removed` and `deleted` are null
  there. Weaviate's searches use its search route (`/v1/search/<Collection>/near-vector`, Weaviate 1.39.8 and later)
  and a Weaviate without it is asked through GraphQL. MongoDB signs in with SCRAM-SHA-256 (users made with it, as
  Atlas's are); a `mongodb+srv` cluster's hosts and options come from DNS and must be in the cluster's own domain.
- **Limits of a store** are said when a piece is added: Atlas indexes at most 8,192 dimensions; Pinecone keeps 40 KB of
  metadata a vector; flat stores keep metadata names beginning `piece_` for their own.
- **`store: memory`** keeps pieces in the run itself: added by one step and searched by a later step of the same run,
  gone when the run ends - a run that waits and takes up again, or a workflow another runs, starts with none. For small
  sets and for trying a workflow out: at most 10,000 pieces (64 MiB) and 20 collections a run. A test run keeps and
  searches them too: nothing leaves the run.
## Files between steps

A file in a run is a name, a media type, a size and its bytes. Steps pass it by reference: an HTTP answer that is a
file (`steps.download.body`), a file a step made (`steps.report`), a webhook form's upload (`value.photo`) is each a
record - `{file, name, type, size}` - and only a step that needs the bytes reads them.

| Step | What it does |
|---|---|
| **Extract from a file** (`extract_file`) | Reads a file: CSV and spreadsheets (XLSX, XLS) into `rows` - the first row names the columns - with `count` and `columns` (and a spreadsheet's `sheet` and `sheets`); JSON (or JSON lines) into `data`; text, a web page or a PDF into `text` (with a page's `title`, a PDF's `pages`); anything into `base64`. The format comes from the file's type, then its name, or from `format`. Settings: `sheet`, `header: false` (rows as lists), `delimiter` (`;`, `tab`...) |
| **Convert to a file** (`convert`) | Makes a file: CSV or XLSX of `rows` (records, or lists; `columns` picks them and their order - without it, every column by name, as expressions keep a record's fields; `sheet`, `delimiter`, `header`), JSON of `data`, text or HTML of `text` (a template), or a file of `base64` text. `name` names it (a template; by default the step's name), `type` sets its media type |
| **File details** (`file_details`) | A file's name, type, size, extension, format and SHA-256 - and the file again, renamed (`name`) or its type changed (`type`) |

```yaml
- name: download
  http: { url: "https://example.com/reports/q3.xlsx" }      # an answer that is a file is one: steps.download.body
- name: orders
  extract_file: { file: steps.download.body, sheet: Orders } # steps.orders.rows
- name: report
  convert: { format: csv, rows: steps.orders.rows, columns: [id, total], name: "q3-{{ value.day }}.csv" }
- name: send
  email: { credential: mail, to: ops@example.com, subject: Q3, text: Attached., attachments: [steps.report] }
```

- **HTTP:** an answer that is a file - not text, JSON, XML or a form, or sent as an attachment - is one, named by its
  answer or its address. `response: file` makes every answer a file, `response: text` none (an answer of 400 or more
  is always text, to say why). A request sends a file as its body (`file: steps.report`, with the file's type) or a
  form with files (`multipart: { title: "Q3", report: { file: steps.report, name: q3.csv } }`).
- **E-mail:** `attachments` - an expression giving a file or a list of files, or a list of them.
- **Webhooks:** a form's files (`multipart/form-data`) are kept before the delivery is answered, and each is its
  field's value (a field with several, a list).
- **Scripts** open no file - their sandbox has none. A script reads one through an extract step (as text, rows or
  `base64`), whose result it is given like every earlier step's; it makes one by giving back Base64 text that a
  convert step turns into a file.
- **In the studio:** a step's output and a run's record list the files they name - name, type and size - with a
  download for the workspace's members (always a download, never opened in the studio's page). A test run's files
  are held for that test alone; a preview reads the files a real run kept.
- **Where they are kept:** in the log, in a topic of the workspace's own with the same retention as the runs' history
  (14 days): a file goes when the runs that made it go, and every node of a cluster reads it. A record set aside in
  CatchErr longer than that names files that are gone, and its run says so.
- **Limits:** 10 MiB a file; a run makes at most 20 files and holds at most 32 MiB of them (the workflows it runs
  included); a webhook delivery brings at most 10 files and 11 MiB; an e-mail carries at most 10 files, 20 MiB in
  all; a file is read as at most 100,000 rows, and a step's result is at most 4 MiB.
- **PDF:** the text a PDF holds as text, page by page. Not the text of pictures (a scan holds none), and not a
  protected (encrypted) PDF, which is refused.

## SQL databases

A workflow reads and writes MySQL (and MariaDB), SQL Server (2012 and later, and Azure SQL) and PostgreSQL with a step
of each database's own - `mysql`, `sqlserver`, `postgres` - because the SQL is written for that database: its
placeholders, its quoting, its functions. In the builder they are *MySQL*, *SQL Server* and *Query a database*.

```yaml
steps:
  - name: order
    mysql:
      credential: shop-db
      sql: SELECT id, customer, total FROM orders WHERE id = ? AND status = ?
      params: [value.id, "'paid'"]
  - name: pay
    sqlserver:
      credential: books
      transaction: true
      statements:
        - { sql: "UPDATE accounts SET balance = balance - @p1 WHERE id = @p2", params: [value.amount, value.from] }
        - { sql: "UPDATE accounts SET balance = balance + @p1 WHERE id = @p2", params: [value.amount, value.to] }
```

- **Values go beside the SQL, never into it.** Each expression in `params` is one value: MySQL takes them for its
  `?`s in order, SQL Server for `@p1`, `@p2` ..., PostgreSQL for `$1`, `$2` .... A value is sent as a parameter of its
  own type, so a value from a message cannot change what the SQL does. SQL with `{{ }}` in it is refused when the
  workflow is read, and so is SQL whose places for values do not match its `params`.
- **The answer:** `rows` (each a record of its columns), `count`, `columns`, and `affected` (the rows a statement
  changed; for several, all of them together); MySQL's `insert_id` when a row was given one. With more than one
  result, `results` holds each, and `rows` is the last that had columns.
- **Several statements** (`statements: [{sql, params}, ...]`, up to 20) run one after another on one connection;
  with `transaction: true` they are kept together or, one failing, none of them is (on SQL Server with XACT_ABORT
  on). A failure says which statement.
- **Only reads** (`read_only: true`), for an agent's tool or any step that must not change anything: the SQL is
  checked when the workflow is read - one statement, starting with `SELECT` or `WITH` (MySQL and PostgreSQL also
  `SHOW`, `EXPLAIN`, `VALUES`, `TABLE`; MySQL `DESCRIBE`), and none of the words that write or reach outside
  (`INSERT`, `UPDATE`, `DELETE`, `INTO`, `CALL`, `EXEC`, `SET`, `LOAD_FILE`, `OPENROWSET`, `pg_read_file` ...)
  anywhere outside strings and comments - and it runs in a read-only transaction the database keeps from writing,
  rolled back after (SQL Server has no read-only transaction: there it runs in one that is always rolled back). The
  check refuses some SQL that only reads (a column named like one of those words: quote it); it cannot know what a
  function of your own does. A database user that may only read is the sure way.
- **Limits:** `max_rows` (1,000 unless it says, at most 10,000: more is an error, so add a `LIMIT`, or `TOP` on SQL
  Server), `timeout` (30s unless it says, 1s to 5m, signing in included: MySQL is told to stop a SELECT then, SQL
  Server is told to stop what it is doing, PostgreSQL's `statement_timeout` is set), 4 MiB of rows (the step's
  result), 100 KiB of SQL and 100 values a statement, 16 MiB of values. A write whose returned rows pass `max_rows`
  is still done.
- **Test and Preview.** A test run sends nothing: it shows the database, the SQL and its values. A preview runs SQL
  that only reads (by the check above), to read only; SQL that writes - or that the database refuses to run in a
  read-only transaction - is shown, not sent.
- **This machine's own network:** a database whose name leads to the machine itself, its local network or a cloud's
  metadata address is refused unless OpenDuct is started with `--allow-private-network`. The address checked is the
  one connected to.
- **As an agent's tool:** any of these steps, with `params` from the model's arguments (`args.id`) and
  `read_only: true`.

The old `postgres` step (`credential`, `sql`, `params`) reads and runs as it always did: its values go as text, read
as the type their place calls for. With any of `statements`, `transaction`, `read_only`, `max_rows` or `timeout` it is
read as the others are, and its values keep their types.

### How values and rows are read

| A value | MySQL and MariaDB | SQL Server | PostgreSQL |
|---|---|---|---|
| A whole number | BIGINT | bigint | as text, in its place's type |
| A number with a fraction | DECIMAL, exact | decimal(38, its digits), exact (past 38 digits, as text) | as text |
| `true`, `false` | 1, 0 | bit | `true`, `false` |
| Text | VARCHAR (utf8mb4) | nvarchar(4000), or nvarchar(max) when longer | text |
| A time (`timestamp(...)`) | DATETIME, in UTC | datetime2(7), in UTC | timestamptz |
| A list or a record | its JSON, as text | its JSON, as text | its JSON |
| `null` | NULL | NULL | NULL |

| Columns | In a run |
|---|---|
| Integers (MySQL's signed and unsigned, YEAR; SQL Server's tinyint 0 to 255, smallint, int, bigint) | numbers, exact |
| DECIMAL, NUMERIC; SQL Server's money and smallmoney | numbers, exact (without trailing zeros) |
| FLOAT, DOUBLE, REAL | numbers |
| SQL Server's bit; PostgreSQL's boolean | `true`, `false`; MySQL's BOOL is TINYINT(1): 1 or 0 |
| MySQL's BIT | a number |
| Text: CHAR, VARCHAR, TEXT, ENUM, SET; nchar, nvarchar, ntext, xml | text. SQL Server's char, varchar and text are read in their collation's code page: Windows-1252 (the Latin collations) or UTF-8 (`_UTF8` collations); in another code page the characters past ASCII read as `�` - cast such a column to nvarchar |
| JSON (MySQL, PostgreSQL) | JSON |
| Binary: BINARY, VARBINARY, BLOB, GEOMETRY; binary, varbinary, image, and SQL Server's hierarchyid, geometry and geography | `\x` and hex |
| uniqueidentifier | `6F9619FF-8B86-D011-B42D-00C04FC964FF` |
| DATE | `2026-11-27` |
| MySQL's DATETIME and TIMESTAMP | `2026-11-27 09:30:05.123`, with the column's own fraction digits; TIMESTAMP in UTC |
| MySQL's TIME | `-26:03:04`: hours past 24 for a long one |
| SQL Server's datetime, smalldatetime, datetime2 | `2026-11-27T09:30:05.123` (datetime to the millisecond, as SQL Server rounds it; datetime2 with its own digits) |
| SQL Server's time, datetimeoffset | `09:30:05.1234567`; `2026-11-28T01:30:00+02:00`, in its own time |
| sql_variant | as the type it holds |
| MySQL 9's VECTOR; pgvector's vector | a list of numbers |
| PostgreSQL's other types | as the database writes them, as text |

A time read from a column is text in a form the same database reads back, so it can be given to a later step as a
value. SQL Server 2025's json and vector columns come as text (as SQL Server sends them to a client that does not
ask for them as such: not tried against SQL Server 2025).

### Database triggers

Choose **A database** in the trigger window: the database, the credential, the table, the column rows are taken
after (the cursor), and how often to ask. Each row after the last one seen runs once, oldest first, up to *Most rows
each time* (the rest the next time); the run's `value` is the row, and `trigger.table` and `trigger.id` say which.

```yaml
trigger:
  mysql: { credential: shop-db, table: orders, cursor: id, where: "status = 'paid'", every: 1m }
trigger:
  sqlserver: { credential: books, table: dbo.invoices, cursor: updated_at, key: id }
```

- **The cursor** is an id that grows (new rows) or the time a row is changed at (new and changed rows). An id is
  taken as unique. A time can repeat - two rows changed in the same instant - so give the rows' own `key` beside it:
  the poll then asks from the last time on and leaves out the rows at that time it has run already. Without `key`,
  rows that share the last time a poll saw are not run.
- `where` keeps only the rows an SQL condition holds for (it is checked as read-only SQL; no values in it).
- `every` is 5s to 1d (1m unless it says); `max_rows` 1 to 500 (50 unless it says).
- The first poll takes nothing: it notes where the table is, so the trigger starts from when the workflow is turned
  on. What the polls have seen is kept on the log with the runs they start, in one write: a restart or a node's loss
  neither runs a row twice nor passes one by, and in a cluster one node asks at a time. A poll that fails keeps what
  was seen, says why in the run history once, and the next one asks again from the same place.
- Each poll only reads (as `read_only: true` does). A row deleted is not seen; a row changed twice between two polls
  runs once, as it is then.

### Database credentials

| Kind | Fields |
|---|---|
| `mysql` (MySQL and MariaDB) | `host`, `port` (3306), `database`, `username`, `password`, `tls`, `ca`, `certificate` |
| `sqlserver` | `host`, `port` (1433), `database`, `username`, `password` (a SQL Server user, not Windows sign-in), `tls`, `ca`, `certificate` |
| `postgres` | `url` (`postgres://host:port/database`), `username`, `password`, `tls`, `ca` |

`tls` is `require` unless it says: TLS, the server's certificate checked against the system's roots, or against `ca`
(PEM). `certificate` (PEM) trusts the server's own certificate as it is, whatever name it carries - for a server with
a certificate of its own making; it is still checked that the server holds its key. SQL Server takes `strict` too
(TLS before anything else: TDS 8, SQL Server 2022 and later). `disable` sends everything unencrypted - on SQL Server
the password too, only lightly disguised: only on a network you trust.

Not yet: SQLite files, Oracle, DB2 and other databases; SQL Server's Windows (Kerberos) and Microsoft Entra
sign-in, named instances found by name (give the port), and an availability group's read-only routing; MySQL sign-in
other than `caching_sha2_password` and `mysql_native_password`, and a client certificate; a pool of connections (each
step opens its own).
## Storage: folders, servers, archives and Redis

Files that live outside a run - on a partner's SFTP server, an FTP server, in S3 or a store that speaks its API
(MinIO, Cloudflare R2, Wasabi, Backblaze B2), or in a folder of the machine OpenDuct runs on - are read into the
run's files and written from them by one step, the same way wherever they are.

| Step | What it does |
|---|---|
| **Files on a server or a folder** (`storage`) | `action`: `list` a folder (its `files`: each one's `name`, `path`, `kind` - file or folder - `size` and `modified`; `pattern: "*.csv, *.xlsx"`, `recursive: true`, `limit`), `read` a file (its result is the file, as an HTTP download's is), `write` one (`file`: a run's file, or text), `move` or `copy` one (`to`), `delete` a file or an empty folder, `make_folder`, and for S3, Azure and Google Cloud Storage `presign` - an address that gets (or, with `method: put`, puts) one object without the credential, for `expires` (1h; at most 7d) |
| **Zip and unzip** (`compress`) | `action: pack` a run's files (`files`: a file or a list) into a ZIP, gzip (one file), tar or tar.gz archive (`format`, `name`); `unpack` an archive into the run's files (`pattern` for some of them); `list` its entries. The format of an archive read comes from its own bytes |
| **Redis** (`redis`) | `get` a value (a record or a list when it was set as one), `set` one (`ttl: 1h` to keep it a while, `if_missing: true` only when there is none), `delete`, `exists`, `increment` (`by`), `push` to a list and `pop` from it (`end: left` or `right`; pushed to the end and popped from the start by default, so a list is a queue), `range` (at most 1,000 items), `expire`, `publish` to a `channel` |

```yaml
- name: inbox
  storage: { action: list, credential: partner_sftp, path: outgoing, pattern: "*.csv" }
- name: each
  for_each:
    items: steps.inbox.files
    steps:
      - name: file
        storage: { action: read, credential: partner_sftp, path: "{{ item.path }}" }
      - name: rows
        extract_file: { file: steps.file }
      - name: done
        storage: { action: move, credential: partner_sftp, path: "{{ item.path }}", to: "done/{{ item.name }}" }
```

- **Where the files are:** `credential` names a server - its kind says how it is reached:
  - `sftp`: `host`, `port` (22), `username`, a `password` or a `private_key` (OpenSSH's form or PEM, Ed25519, ECDSA
    or RSA; not protected by a passphrase), `host_key`, and a `folder` paths are inside;
  - `ftp`: `host`, `port`, `username` (anonymous) and `password`, `security` - `starttls` by default (TLS is asked
    for), `tls` for a server that starts with it (port 990), `none` only when you say so - a `ca`, and a `folder`;
  - `s3`: `access_key`, `secret_key` (and `session_token`), `region` (us-east-1), `endpoint` for a store other than
    AWS, `path_style`, and a `bucket` (or the step's `bucket`) and a `prefix` every key is inside;
  - `redis`: `url` (`redis://host:6379/0`, or `rediss://` for TLS), `username` and `password`, a `ca`.
- **A folder of this machine** is named by `root`, and only the folders the installation's admin allows when
  OpenDuct starts exist: `openduct start --files-root invoices=/srv/invoices --files-root exports=/srv/exports` (or
  `OPENDUCT_FILES_ROOTS=invoices=/srv/invoices,exports=/srv/exports`). Every workspace may use every folder given, so
  give only what each of them may see. The builder offers them by name.
- **Never outside:** paths are plain names separated by `/`. `..`, a backslash and control characters are refused
  everywhere; a path from the top (`/upload/in`) only on a server whose credential names no folder. In a folder of
  this machine a link that leads out of it is refused - checked before a file is opened and again once it is open -
  a listing leaves such links out, and a delete or a move takes the link itself, never what it leads to.
- **Writes** go to a file of their own beside the one written and are renamed over it, so a reader never sees half a
  file. A write, a move or a copy that would replace a file fails, saying so, unless `overwrite: true`.
- **The server's key (SFTP):** checked every time. `host_key` pins it - the key as `ssh-keyscan` prints it, or its
  fingerprint (`SHA256:...`, as `ssh-keygen -l` shows); left empty, the key seen the first time is kept in the
  credential once the sign-in succeeds, and from then on required. A server whose key changes is refused, saying so.
  The SSH client is OpenDuct's own, on `ring`: curve25519 or P-256 key exchange, AES-GCM or ChaCha20-Poly1305, the
  strict key exchange OpenSSH offers.
- **FTP:** a server's passive answer names an address to connect to; it is not taken - the data connection goes to
  the server the control connection reached - so a server cannot send OpenDuct elsewhere.
- **This machine's own network:** a server on a private address (or one whose name leads to one) is reached only
  when the installation allows it (`--allow-private-network`), as for the HTTP step.
- **Test and Preview:** a test run reaches nothing - each step says what it would do, and a read gives an empty file
  of the name it would read, so the steps after it run as they will. A preview lists and reads for real and shows
  each write, move, delete and Redis change without doing it.
- **An agent's tools:** each of these steps is a tool an [agent](#ai-agents) may use.
- **Limits:** a file read or written is at most 10 MiB (a run's file), a run holds at most 20 files; a list gives at
  most 10,000 entries, 8 folders deep; a copy within a folder of this machine at most 256 MiB; a step's work with a
  place at most two minutes. An archive unpacks to at most 64 MiB, each file in it at most 10 MiB - an archive that
  says, or turns out, to hold more is refused before it fills the run. A Redis value is at most 4 MiB.

### Stores and drives over their own APIs
<!-- storage places -->

The same step - every action, the same results, checks, limits, test and preview rules - reaches stores and drives
over their vendors' own APIs, by the credential's kind. Their requests go through the run's HTTP effects, so a store
on this machine's own network is reached only with `--allow-private-network`, as for an HTTP step; a link a vendor
gives back (a next page, an upload's address) is followed only on that vendor's own host.

| Credential | Where | Holds |
|---|---|---|
| `azure_blob` | Azure Blob Storage | `account`, its `shared_key` (signed as Azure's Shared Key) or a `sas_token`, a `container` (or the step's `bucket`), an `endpoint` for a store other than Azure's (Azurite: `http://127.0.0.1:10000/devstoreaccount1`), a `prefix` every name is inside |
| `gcs` | Google Cloud Storage (its JSON API) | a service account's `key_file` (the JSON Google gives; its JWT signed with RS256, traded for a token kept in memory until it runs out), a `bucket` (or the step's `bucket`), a `prefix` |
| `dropbox` | Dropbox | connected (**Connect**, with the install's own Dropbox app - [apps/dropbox](../apps/dropbox/README.md)) |
| `box`, `box_token` | Box | connected (the Box app's sign-in), or a developer token from Box's console (an hour: for trying a workflow) |
| `microsoft_365` | OneDrive, a SharePoint document library | connected, as the Microsoft 365 app signs in (`Files.ReadWrite.All` reaches the sites the person may) |
| `google_drive` | Google Drive (a folder of a shared drive too, by its id) | connected, as the Google Drive app signs in |

```yaml
- name: invoice
  storage: { action: read, credential: dropbox, path: "/Invoices/{{ value.name }}" }
- name: keep
  storage: { action: write, credential: archive, path: "invoices/{{ value.name }}", file: steps.invoice }   # azure_blob
- name: plan
  storage: { action: read, credential: drive, path: Plans/Quarter, export: pdf }                    # a Google Doc
- name: library
  storage: { action: list, credential: work, drive: "site:contoso.sharepoint.com,2C712604-...,9658...", path: Contracts }
```

- **Paths** are names separated by `/`, checked as everywhere (`/Invoices/a.pdf` is `Invoices/a.pdf`); a prefix in
  the credential keeps every path inside it. Box, OneDrive and Google Drive work by ids: a path is walked name by
  name from the top - or from the folder whose id the step's (or trigger's) `folder_id` gives - and folders a write
  needs are made on the way. Dropbox takes `folder_id` too (`id:...`): paths are then inside that folder. Box matches
  names whatever their case; Google Drive lets two files have one name - a path names the one changed
  last.
- **Which drive** (Microsoft 365): `drive: me` (the person's OneDrive, by default), a drive's id, or
  `site:<a site's id>` for that site's document library.
- **Google Docs, Sheets, Slides and Drawings** hold no bytes of their own: a read exports them, as `export` says -
  `office` (Word, Excel, PowerPoint; the default), `pdf`, `text` (a sheet as CSV) or `open` (OpenDocument) - and the
  file gets its name's end (`Plan.docx`). Google exports at most 10 MB.
- **Writes** that may not replace a file are refused by the store itself where it can (Azure `If-None-Match: *`,
  Google Cloud Storage `ifGenerationMatch=0`, Dropbox's strict `add`, OneDrive's `fail`), so one written meanwhile
  is not replaced either. A delete is of a file or an empty folder, everywhere (OneDrive moves it to the recycle
  bin, Google Drive to the trash).
- **Copies** are made by the store (Azure's Copy Blob, waited for up to 30 seconds; Google's rewrite; Dropbox, Box,
  Google Drive), and in OneDrive by reading and writing the file again (Graph's own copy finishes later).
- **Presign:** Azure with the account's key (a service SAS for the one blob; a `sas_token` credential has no key to
  make one), Google Cloud Storage (V4, signed with the service account's key), at most 7 days. A put to an Azure
  address needs the header `x-ms-blob-type: BlockBlob`, which the result names.
- **Tokens:** a connected credential's token that runs out - or that the vendor no longer takes - is refreshed with
  its refresh token and kept in the credential, as an app step's is; one that cannot be is said: connect it again.
- **Limits:** as for every place; a step asks a store at most 400 times (a list of a large tree stops, saying so).

### Triggers: new files, and a Redis channel

```yaml
trigger:
  storage: { credential: partner_sftp, folder: outgoing, pattern: "*.csv", every: 5m }
  # or: root: invoices; when: new_or_changed; start: existing; max_items: 50; recursive: true; bucket: (S3)
```

A run for each new file in the folder, its `value` the file's `name`, `path`, `size`, `modified` and `change` (new
or changed): read it with a storage step. The folder is looked at every `every` (5m by default, from 10s to 1d). Its
looks are a schedule's times: in a cluster one node looks each time, and what it saw is kept with the runs it starts,
so a restart or a node's loss neither runs a file twice nor passes one by. The first look notes what is there and
runs nothing, unless `start: existing`; `when: new_or_changed` runs a file again when its size or time (S3: its ETag)
changes. A folder watched holds at most 5,000 files: move the files done into another folder. A look that fails is
said once in the workflow's history, and the next one tries again.

<!-- storage places -->
On Dropbox, OneDrive and Google Drive each look reads the vendor's list of changes since the last one - Dropbox's
cursor, Graph's `delta`, Drive's `changes` - rather than the whole folder; Azure, Google Cloud Storage and Box are
listed, as a server's folder is. The first look lists the folder and runs nothing (unless `start: existing`); what is
kept is each file and its version (Dropbox by path; OneDrive and Drive by id, with the folders below, since their
changes name no path), so `when: new` runs a file once and `new_or_changed` again when it changes. Files found beyond
`max_items` wait, and run first the next time. A cursor the vendor no longer takes (a Dropbox reset, Graph's 410) is
met by listing the folder again and comparing it with what was kept; a folder moved into the one watched brings its
files (`recursive: true`). `folder_id` and `drive` are the trigger's too.

```yaml
trigger:
  redis: { credential: cache, channel: "orders.*" }
```

A run for each message published on the channel (a pattern, with `*`, `?` or `[`), its `value` the message - a record
when it is JSON's - and `trigger.channel` the channel. In a cluster one node listens: the one that holds a lease the
broker keeps, taken over by another when it goes. A channel keeps nothing: what is published while no node listens -
while one takes over, or the connection is made again - is not seen; for messages that must not be lost, push them to
a list and pop it in a scheduled workflow.

## Web pages: read, pick out, crawl, sitemaps and feeds

The **web** step does what people mean by "fetch this page" and "scrape this site": it reads a page as a person
reads it, picks fields out of it, crawls a site, and reads sitemaps and feeds. The HTTP step is still the one for
APIs; this one is for pages.

| `action` | What it gives |
|---|---|
| `fetch` (the default) | One page: its readable `text` (Markdown, or plain text with `format: text`), `title`, `description`, `language`, `canonical`, `site_name`, `author`, `published`, `image`, the `feeds` it offers, its `links` (`url`, `text`) and `images` (`url`, `alt`) at their full addresses, and `url` (after redirects), `status`, `type`, `bytes`, `redirects`, `ms`. `html: true` adds its HTML as text, `html: file` as a run's file |
| `select` | `fields` picked out by CSS selectors, under `fields` - each the first element's text, or with `all: true` every one's; `attribute: href` an attribute instead (addresses whole); `as: markdown` or `as: html`; `fields` of its own for a record of fields inside each element matched |
| `crawl` | `pages`, each as `fetch` gives it with its `depth`, from the start address outward; `skipped` (each address passed by and why), `stopped` (`max_pages`, `time` or `size` when a limit ended it) |
| `sitemap` | `urls` (`url`, `modified`, `changes`, `priority`) a site's sitemap lists - given the site, the sitemaps its robots.txt names or its `/sitemap.xml`; a sitemap index's sitemaps read in turn, gzipped ones too - and the `sitemaps` read |
| `feed` | An RSS (2.0 and 1.0), Atom or JSON feed's `items`: `id`, `title`, `link`, `date` (an ISO time), `summary` and `content` (as text), `author`, `categories`, `enclosure`; and the feed's `title`, `link`, `description`. A page that offers a feed has its feed read |

```yaml
- name: post
  web: { url: "{{ value.url }}", max_chars: 20000 }
- name: product
  web:
    action: select
    url: https://shop.example.com/caps
    fields:
      title: h1
      price: .price
      links: { selector: a.product, attribute: href, all: true }
      cards:
        selector: .card
        all: true
        fields: { name: .name, price: .price, picture: { selector: img, attribute: src } }
- name: docs
  web: { action: crawl, url: "https://docs.example.com/", depth: 2, max_pages: 50, include: ["https://docs.example.com/guide/*"] }
```

- **The readable text** is the page's main part, in the page's order: headings as Markdown's (`## How`), paragraphs,
  lists (nested ones indented), quotes, code (fenced), data tables (as Markdown tables), links and pictures at their
  full addresses. Left out: scripts, styles, frames, forms' fields, what is marked hidden, and - for the main part -
  navigation, menus, headers and footers around the content, sidebars, banners, cookie notices, share bars, comments
  and lists of related links. The main part is the page's one `<article>`, else its `<main>`, else the block whose
  paragraphs hold the most text (scored by length and commas, less what is links), with the blocks beside it that
  read as part of it. `content: all` gives everything a person sees, navigation too; `within: "#content"` the part a
  selector picks. `links: false` and `images: false` leave those lists out. Not a browser: a page that builds itself
  with scripts gives what its HTML holds.
- **Character sets:** a page is read in the character set its answer or its markup names (Windows-1252, Shift_JIS,
  GBK ... as browsers read them), UTF-8 otherwise. Compressed answers (gzip) are unpacked, within the limit.
- **A page the run already has:** `from: steps.download.body` (text or a run's file) reads that instead of fetching -
  for a page behind a sign-in fetched by an HTTP step with its credential; `url` then says the address its links are
  read against. `fetch`, `select` and `feed` take it.
- **The crawl** reads one page at a time, nearest first, at most one every `rate` (1s; at least 100ms) on a host -
  longer when the site's robots.txt asks it with `Crawl-delay` (up to 30s) - to `depth` links from the start (1; at
  most 5) and `max_pages` pages (10; at most 200), within `time` (2m; at most 10m). It follows links on the start's
  site (with `www.` or without), or with `include` those whose address matches one of its patterns (`*` for any
  characters), never those that match `exclude`. Files that are not pages (PDFs, pictures, archives, scripts ...) are
  not fetched. `sitemap: true` starts from the pages the site's sitemap lists too. A page that asks not to be followed
  (`<meta name="robots" content="nofollow">`) is read and its links are not.
- **robots.txt:** honoured by every crawl unless `robots: false`: the rules for `OpenDuct` (or the first word of the
  step's `user_agent`), else those for `*`, the longest matching rule winning, as RFC 9309 says. A robots.txt that is
  not there allows everything; one that cannot be read allows nothing. **Turn it off only for a site that is yours or
  whose owner agreed**: robots.txt is how a site says what crawlers may read, and some sites' terms forbid crawling.
  `fetch` and `select` read the one page asked for, as a browser does, and do not read robots.txt.
- **Who asks:** every request names OpenDuct as its User-Agent (`OpenDuct/<version> (+<its repository>)`);
  `user_agent` names another. `headers` (fetch, select, sitemap, feed) adds others - sent to the address's own site only;
  a redirect to another site gets none of them.
- **This machine's own network:** never reached unless the installation allows it (`--allow-private-network`), as
  for the HTTP step - every address checked, a name as it is resolved, and again at each redirect. Only http and
  https, never an address with a user name or password in it.
- **Test and Preview:** a test run fetches nothing: a sample page of OpenDuct's own stands in for the address asked,
  read by the same code a real page is, so the steps after it see a page's fields, a crawl's pages, a sitemap's
  addresses or a feed's items - each answer marked "sample page, nothing fetched". A preview fetches for real: the
  step only reads.
- **Limits:** a page at most `max_bytes` (5MB; at most 10MB, compressed or not), each fetch at most `timeout` (30s; at
  most 2m), `max_redirects` followed (5; at most 10); a crawl's text at most 8 MiB in all; a sitemap at most
  `max_urls` addresses (1,000; at most 50,000) from at most 50 sitemaps; a feed at most `max_items` items (50; at most
  1,000); `max_chars` cuts a page's text (at a word's end); 1,000 links and 500 pictures a page; a selector at most
  1,000 characters, and its matching on one page bounded, so one made to be slow fails, saying so. A page answering
  400 or above fails `fetch` and `select` unless `fail_on_status: false`.

**As an agent's tool.** The step is a tool as it stands; a tool without `url` is given one by the model, with a
parameter `url` of its own. Describe what the tool gives and when to use it - the model chooses by the description -
and keep its answers short with `max_chars` and `links: false`:

```yaml
tools:
  - name: read_page
    description: >-
      Reads one web page and gives its main text as Markdown, with its title and description. Use it to read an
      address the user gives or one found by another tool. Give the page's whole address, starting https://.
    web: { action: fetch, max_chars: 8000, links: false, images: false }
  - name: crawl_docs
    description: >-
      Reads up to 10 pages of the product's documentation site, from the address given, and gives each page's
      title, address and text. Use it when one page is not enough to answer.
    web: { action: crawl, depth: 1, max_pages: 10, max_chars: 3000, include: ["https://docs.example.com/*"] }
```

An agent's tool answer is cut at 16 KiB for the model: a crawl tool's `max_pages` times its `max_chars` should stay
near that.

### A feed's new items

```yaml
trigger:
  feed: { url: "https://example.com/blog/feed.xml", every: 30m }
  # or: max_items: 20 (new items a look, 50 by default); user_agent
```

A run for each new item of an RSS, Atom or JSON feed, oldest first, its `value` the item as the `feed` action gives
it (`id`, `title`, `link`, `date`, `summary`, `content`, `author`, `categories`, `enclosure`) and `value.feed` the
feed's `title`, `link` and `url`. The feed is looked at every `every` (15m by default; from 1m to 7d), asked with its
ETag and date, so an unchanged feed answers with nothing. Its looks are a schedule's times: in a cluster one node looks
each time, and what it saw is kept with the runs it starts, so a restart or a node's loss neither runs an item twice
nor passes one by. The first look notes what the feed holds and runs nothing. An item is known by its id (an RSS
`guid`, an Atom `id`), else its link; the newest 5,000 are kept. A look that fails is said once in the workflow's
history, and the next one tries again.
<!-- messaging -->
## Email and messengers

### A mailbox, over IMAP

Any mailbox - Gmail, Outlook, a host's own - is read, searched and tidied by the **Mailbox** step, and its new mail
starts runs with the **New email** trigger. Sending mail is the **Send an e-mail** step's (SMTP), or Gmail's and
Microsoft 365's apps.

| Action | What it does |
| `list` | The `folder`'s messages (INBOX by default), the newest first, at most `limit` (50; up to 500): those that are `unseen`, `flagged`, `from`, `to`, with a `subject` or `text`, `since` and `before` a date (`2026-11-01`), when given. Each is `uid`, `subject`, `from` (`name`, `address`), `to`, `cc`, `reply_to`, `date`, `message_id`, `flags`, `seen`, `flagged` and `size`; `total` says how many met the criteria |
| `read` | One message (`uid`) in full: the same fields, its `text` and `html` (each up to 1 MB; `cut` when longer), its `headers` (names in small letters), and its `attachments` as the run's files - `files: false` gives their names and sizes only, `raw: true` the whole message as a file too (`.eml`). Reading never marks a message seen |
| `mark` | `seen` and `flagged`: `true` sets them, `false` takes them away |
| `move` | To `to_folder` (with MOVE, or a copy and that message alone removed) |
| `delete` | Marked deleted and removed - that message alone, where the server can (UIDPLUS); `expunged: false` otherwise |
| `folders` | The folders, by name, with what the server says of each (`\Sent`, `\Trash`) |

```yaml
- name: email
  mailbox: { action: read, credential: shop-mail, uid: "{{ value.uid }}" }
- name: rows
  extract_file: { file: "steps.email.attachments[0]", format: csv }
- name: filed
  mailbox: { action: move, credential: shop-mail, uid: "{{ value.uid }}", to_folder: Archive }
```

- **The credential** (`imap`): `host`, `port` (993; 143 for `starttls` or `none`), `security` - `tls` by default (TLS
  from the start), `starttls` (refused when the server does not offer it), `none` only when you say so - `username`,
  and a `password` or an OAuth `access_token` (XOAUTH2, as Google and Microsoft take it). With a `refresh_token`, the
  provider's `token_url` and the `client_id` (and `client_secret`), the access token is renewed before it runs out
  and once more when the server refuses it, and kept. A `ca` for a server whose certificate the system does not trust.
- **This machine's own network:** a server on a private address is reached only with `--allow-private-network`.
- **Bounded:** a line of the server's answer at most 4 MB, a message read at most 32 MB, an answer to one command at
  most 64 MB; a message's parts at most 10 deep and 200 in all; attachments become files within a run's limits (10
  MiB a file, 20 files) - one past them is listed with why, not kept. A step's work with the server at most two
  minutes.
- **Test and Preview:** a test run reaches nothing and answers with a sample message - a read with a small CSV
  invoice attached - so the steps after it run as they will. A preview lists and reads for real and shows a mark, a
  move or a delete without doing it.

```yaml
trigger:
  mailbox: { credential: shop-mail, folder: INBOX, from: billing@example.com, every: 5m }
  # or: unseen: true; to; subject; idle: true; start: existing; max_items: 50; read: false
```

A run for each new message in the folder: its `uid`, `uidvalidity`, `subject`, `from`, `to`, `date`, `flags`, `text`
and `html` (up to 100 KB each) and its attachments' names and sizes - read the files with a `read` step (`read:
false` gives the headers alone; a message over 4 MB is given its headers alone). The folder is asked every `every`
(5m by default, from 1m to 1d) for the messages after the last one dealt with; its looks are a schedule's times, so in
a cluster one node asks each time, and what it saw - the folder's UIDVALIDITY and the highest UID - is kept with the
runs it starts: a restart or a node's loss neither runs a message twice nor passes one by. The first look notes
where the folder is and runs nothing (unless `start: existing`); a folder rebuilt (its UIDVALIDITY changed) starts
again from then. With `idle: true`, one node also waits for news with IMAP's IDLE where the server offers it, and a
message arriving asks for a look at once - the look still decides what runs. A look that fails is said once in the
history, and the next tries again.

### Messengers

Each messenger is an app ([apps](apps.md)): its actions are steps, its triggers in the trigger window's app tile, and
its README says how to sign in and what each needs.

- **Discord** (a bot, or a channel's webhook): send to a channel, reply, edit, react, start a thread, list a server's
  channels, give or take a member's role. **A slash command or a button used** takes interactions at the workflow's
  address, each checked with Discord's Ed25519 signature; Discord's check is answered, and each command is answered
  at once that the bot is thinking - the workflow then answers with **Answer a slash command or a button**.
- **Microsoft Teams** (signed in with Microsoft, or a channel's webhook), its own app: send to a channel or a chat,
  reply in a thread, post an Adaptive Card, list teams and channels, read a message. **A new message in a channel or
  a chat** subscribes the workflow's address with Microsoft Graph as it is turned on, answers Graph's check, checks
  each notification's secret, renews the subscription every 20 minutes (once in a cluster) and deletes it as the
  workflow is turned off.
- **Slack**: send (Block Kit too), reply in a thread, update, delete, react, upload a file (a run's, or text), open a
  direct message, list channels, find a person by email, schedule a message; a channel's messages polled, or as they
  come through the Events API.
- **WhatsApp** (Cloud API): text, templates with their values, pictures, documents, video and sound, reply buttons
  and lists, marking a message read; new messages as Meta sends them.
- **Telegram**: messages, photos and documents with buttons under them, edits, answers to a button press; a trigger
  for messages, and one for messages and button presses together.
- **Google Chat** (a space's webhook, or a person's sign-in): messages, replies in a thread, cards.

## Building and trying

- **The canvas** shows the trigger and the steps; a branch, a switch and a loop show their own steps inside them.
- **A step's window** has three parts: **Input** (what the step can read - the sample input, each earlier step's
  result from the last test, how the run started; a field clicked goes into the box being written in, and a field
  the step names that is not there is said), **Settings**, and **Output** (what it gave in the last test).
- **Test** runs the workflow on a sample input and sends nothing: each step shows what it would have done.
  **Preview** does the reads for real (AI answers, GETs, lookups) and only shows the writes. **Run for real** runs the
  sample input as a real event.
<!-- test runs -->
- **What a test answers with.** So the steps after a request go through a list or read a field as they will in a
  real run, a test answers an app's action with **the app's own example answer**: the result of its recorded
  exchanges (`apps/<id>/tests/*.yaml`, as the action reads the vendor's answer; a recording made with the
  credential's sign-in first). An HTTP step answers with the **sample answer** you give it (`sample:`, the body of an
  answer like the API's, at most 64 KiB; `{status: 200, headers: {}, body: <sample>}`); without one it shows what it
  would send, as before - and so does an app with no recording of the action. Each such step is marked **sample
  answer, nothing sent**, with the request it would have sent beside it (in the run's JSON: `sample` and `request`
  beside `result`). A preview is not changed: its reads are real.
- **A table not made yet.** A test reads a table the workspace does not have as an empty one, with the columns the
  step gives it: a read finds nothing, a write is worked out and shown. The step says "the table leads does not exist
  yet: this test used an empty one" (`note`, beside its result), and the test goes on. A real run still fails there,
  plainly: "there is no table leads: make it under Tables".
- **Templates**: workflows to start from, by category.
- **Versions**: every save is a version; any one can be opened again.

## Running

- **Runs at once**: a workflow has up to `runs_at_once` runs going (10 unless it says, 1 to 50). Events with the same
  key run one after another, in order.
- **Nothing lost**: a run is kept in the workflow's history - and its record sent to CatchErr if it failed - before
  its place on the topic is committed. A crash repeats a run; it never skips one. Each event runs at least once.
- **Runs** shows each run, step by step, with what each gave and how long it took. **CatchErr** holds the records
  of runs that failed, to run again once the cause is fixed.
- **In a cluster** every node runs the studio, shows the same, and shares the runs: see
  [deployment](deployment.md#the-studio-and-workflows-in-a-cluster).

## Forms and runs that wait

**A form trigger** gives the workflow a page: its title, a description, and its fields - text, long text, number,
e-mail, date, dropdown, checkbox (one box, or several choices), file upload - each with a label, help text, whether
it must be filled in, and its own limits (lengths; a number's or a date's least and greatest; a file's largest size,
up to 2 MB, and its kinds, as `.pdf,image/*`). The builder lists the fields and shows a preview of the page. The
browser checks each field as the page marks it; the server checks the answer again the same way, and shows the page
again, saying what is wrong, when it is not right. A good answer starts a run with the answers as `value` (a file as
`{name, type, size, base64}`), then the page thanks the person - or sends them on to an address of yours.
Protection: a page carries a token of its own (a day long), so an answer comes from the page it was given on and
another site cannot post one through someone's browser; a form may ask for a password; and one address may send a
form at most 10 answers a minute (`rate_limit`, 0 for no limit; counted on each node).

```yaml
trigger:
  form:
    title: Contact us
    fields:
      - { name: email, type: email, required: true }
      - { name: topic, type: dropdown, options: [Order, Return, Other] }
      - { name: message, type: long_text, max_length: 2000 }
    thanks: Thank you - we answer within a day.     # or redirect: https://example.com/thanks
```

**A step that waits** stops the run where it is - it is not held in memory - and keeps what the run has done so
far on the log. The run goes on from that step when its answer comes: the steps before it are not run again, their
results taken as they were. A restart, or the loss of the node the run was on, changes nothing; any node takes the
answer. The run's history shows it as **waiting** (and the workflow's **Runs** lists what each waiting run waits for,
until when, and its form's address), then as one run from its start to its end.

| Step | Waits for | Its result |
|---|---|---|
| **Ask with a form** (`form`) | A form filled in, on its own page | `steps.<name>.<field>`, and `answered_at` |
| **Ask for approval** (`approval`) | Approve or reject (and a comment, with `comment: true`) | `approved`, `decision` (`approved`, `rejected`, `timed_out`), `comment` |
| **Wait** for a call (`wait: { webhook: true }`) | A call to its one-time address: any method | `method`, `body`, `query`, `headers` (no credentials) |
| **Wait** until a time (`wait: { until: value.at }`) | The time: 2026-12-01T09:00:00Z, a day, or milliseconds since 1970 | `until`, `resumed_at` |
| **Wait** a while (`wait: 2d`) | Up to an hour the run is held; longer, up to 90 days, it is kept on the log | |

Each step that waits has its one-time address **before the run reaches it**, as `{{ links.<step> }}`, so a step
before it sends it: an e-mail or a message with the form's link, an HTTP call that hands the address over as its
callback. A call that comes a moment before the run has stopped is held a few seconds for it. The address takes one
answer - the first in the log, the same on every node; a later one is told it has had its answer - and is gone once
the run has gone on. Reached again in a loop, the step has a new address each time.

A form or a call waits `timeout` (7 days unless it says; at most 90 days); with `on_timeout: fail` (the default) the
step then fails, with `on_timeout: continue` the run goes on with `timed_out: true` - an *if* after it takes the
other way. Texts of a form step may hold `{{ }}`, worked out when the run reaches it (`Refund {{ value.amount }}?`),
and a dropdown's choices may come from the run (`options: "{{ steps.list.names }}"`). **Test** and **Preview** do not
wait: a step that waits takes a sample answer (each field's *filled in with*, or a sample of its kind; an approval is
approved).

```yaml
steps:
  - name: tell
    email: { credential: mail, to: boss@example.com, subject: "Refund {{ value.amount }}?", text: "Answer here: {{ links.decide }}" }
  - name: decide
    approval: { title: "Refund {{ value.amount }} on {{ value.order }}?", comment: true, timeout: 3d, on_timeout: continue }
  - if: { when: "steps.decide.approved", then: [...], else: [...] }
```

Limits: a workflow that waits is started by a webhook, a form, a schedule or by hand (not by a topic or another
workflow's failure); a workflow run by another (`run_workflow`) does not wait - its step that waits fails; at most
10,000 runs of one workflow wait at once, each keeping at most 8 MB of what its steps did; a form's answer is at most
3 MB, its files 2 MB together. The addresses start with `--studio-url` (`OPENDUCT_STUDIO_URL`, as
`https://flows.example.com`), else the first `--studio-host`, else the studio's own address. A run edited while it
waits goes on with the workflow as it is then; if the steps before its wait changed, it fails, saying so. A link shown
in a chat may be opened by the chat's preview: give people a form or an approval (opening its page answers nothing),
not a wait for a call.

## Custom apps

<!-- app generator -->
A workspace can have apps of its own, beside those OpenDuct carries: the **Apps** page, under Connect.

1. **Add an app.** Choose what it is made from: an API description's address (OpenAPI 3.0 or 3.1, or Swagger 2.0,
   as JSON or YAML), a description file of up to 12 MB, an MCP server's address (with a token to list its tools, used
   once and not kept), or an app.yaml written by hand. Give it a name - lowercase letters, digits and `_`: what its
   steps are called - and, if you like, the name it is shown by.
2. **Look.** The page shows what the app would be: the actions proposed, the best ticked (ranked as `openduct app
   from-openapi` ranks them, [apps](apps.md#which-operations-become-actions)), each operation that cannot be one with
   why; the sign-ins, a sign-in at the vendor not offered (it needs the installation's own registration, so is for the
   apps an installation adds); the quality report against the bar of 80; and what is not done. Untick what you do not
   want and **Look again**.
3. **Save the app.** Its steps are offered in this workspace's builder at once, as any app's are; add a credential of
   its kind under Credentials, and use it in a step.

The list shows each app's actions, its score and where it was made from. **Change** edits its app.yaml (checked
again, its recorded tests run again; its name stays), **app.yaml** saves it as a file - to share it, or to add it to an
installation's `--apps-dir` - and **✕** removes it (workflows that use it stop working). Only editors and admins add,
change and remove apps; viewers see them.

A custom app is the workspace's alone: another workspace does not see it, cannot name it in a step, and cannot use
its kind of credential. It reaches only the hosts it declares, through the same rule as every request (this
machine's own network refused unless the studio allows it); it has no triggers yet; a workspace has at most 200, each
with at most 100 actions. See [apps](apps.md#custom-apps) for the rules and the API.

## Credentials

A credential holds what a step needs to reach a service: an API key, a token, a mail server's password, a
database's address. It is sealed when it is kept and never shown back; a test reads it as `•••`.

- **Apps signed in at the vendor** (Google, Microsoft 365, QuickBooks, Xero, GitHub, LinkedIn, X): the installation
  registers with the vendor once - its own client id and secret, under *Settings → App sign-ins* - and each workspace
  then presses **Connect**. OpenDuct takes no part beyond the installation's own registration, and renews tokens as they run out.
  See [apps](apps.md#signing-in-with-oauth-20).
- **No secret in an environment variable.** The studio keeps credentials sealed in the cluster; OpenDuct's own
  secrets (the cluster secret, storage keys, a password) come from files: see
  [deployment](deployment.md#secrets-are-files).
- **This machine's own network**: a workflow may not reach private addresses (the machine itself, the local network,
  cloud metadata) unless the installation allows it (`--allow-private-network`).
- **Databases** (MySQL, SQL Server, PostgreSQL): the server, who signs in and how TLS is checked - see
  [database credentials](#database-credentials).
- **A mailbox** (`imap`): the server, who signs in with a password or an OAuth token kept fresh - see
  [a mailbox, over IMAP](#a-mailbox-over-imap). <!-- messaging -->

## MCP: tools for AI assistants, and from them

MCP (the Model Context Protocol) is how AI assistants find and call tools. OpenDuct speaks it both ways: each
workspace is an MCP server that offers its workflows as tools, and the **MCP tool** step calls a tool of any MCP
server.

### A workflow offered as a tool

In a workflow's *Starts when* window, **Offer it as a tool to AI assistants**: say what it does, in words a model
reads to choose it, and the arguments it takes. In the file it is `tool:` beside the trigger:

```yaml
name: Price an item
trigger: manual
tool:
  description: The price of an item from the price list, for a quantity.
  input:
    sku: { type: string, description: "The item's code", required: true }
    qty: integer                   # just the type, when that says enough
  timeout: 30s                     # how long a call waits for the answer: 60s unless it says, at most 5m
steps:
  - ...
```

- Its **name** is the workflow's made one word (`price_an_item`) unless `name:` gives one: letters, digits, `_`, `-`
  and `.`. Two workflows with the same tool name: the older one is offered, and the Settings page says why the other
  is not.
- An input's **type** is `string`, `number`, `integer`, `boolean`, `array` or `object`; it may be `required`, say what
  it is (`description`) and list the only values it takes (`choices`).
- A call's arguments are checked against the inputs before anything runs: a missing, unknown or mistyped one is
  answered as an error the assistant can read and correct.
- A call **runs the workflow** with the arguments as `value` (`trigger.trigger` is `tool`, `trigger.tool` the tool,
  `trigger.token` the token's name), as any other run: it is in the workflow's history, and a failed one goes to
  CatchErr. Its **answer** is what the run ended with: its respond step's body if it has one, else its last step's
  result (a record is also sent as structured content). A failed run answers with where and why it failed.
- Only a workflow that is **on** is offered. One started by a topic or by another workflow's failure reads its
  events from there, so it cannot be a tool; a webhook or schedule workflow can be one too.

### The workspace's MCP server

*Settings → Tools for AI assistants (MCP)* shows its **address** - `https://<the studio>/mcp/<workspace id>` - the
workflows it offers, and its **tokens**. An assistant signs in with a token a workspace admin makes there:

- sent as `Authorization: Bearer odmcp_...`; shown once when made, kept only as its SHA-256;
- limited to every tool or the ones chosen, and to so many requests a minute (60 unless it says, up to 1,000);
- revoked with a click (on every node of a cluster within a few seconds).

Its limits: each token's requests a minute, counted on each node (`429` with `Retry-After` past it); 16 calls
running at once in a workspace on each node; a call waits up to the tool's timeout - the run goes on past it and its
history shows how it ends; an answer of at most 1 MiB and a request of at most 1 MiB; ten wrong tokens in a minute
make their address wait the minute out. A request from a page of another site (its `Origin` not one of the studio's names) is refused, so a
name pointed at this machine reaches no workspace's tools. Like a webhook, the address is reached by whatever name
the assistant knows (behind a proxy, the proxy's).

It speaks MCP's Streamable HTTP transport, version `2026-07-28` (each request carrying its version, nothing kept
between requests) and, for assistants that have not moved on, `2025-11-25`, `2025-06-18` and `2025-03-26` (after
their `initialize`, with no session). It offers tools only - no resources, prompts or notifications - and signs in
with its own tokens, not MCP's OAuth flow.

### The MCP tool step

```yaml
- name: weather
  mcp:
    url: https://tools.example.com/mcp
    credential: weather-token      # bearer, header or basic; none for a server that asks for none
    tool: get_forecast
    arguments: { city: "{{ value.city }}", days: 3 }
    timeout: 30s                   # each request: 30s unless it says, at most 5m
    read_only: true                # the tool only reads: a preview calls it
    fail_on_error: true            # false: a tool's error is its result instead of failing the step
```

- Its result: `answer` (the tool's structured answer, else its text read as JSON, else its text), `text`, `content`
  (every item, images and links included), `is_error`, and `protocol` (the version spoken). Without `tool:`, the
  step lists the server's tools instead: `tools` (each with `name`, `title`, `description`, `input_schema`) and
  `count`.
- In the builder, **list its tools** asks the server, with the step's credential, and each argument of the chosen
  tool gets its own field.
- It speaks `2026-07-28` first and, with a server that answers as an earlier version would, that version's
  handshake (and its session, ended when the step is done); what each server spoke is remembered for an hour.
- It is an HTTP request like any other: not to this machine's own network unless allowed; a test sends nothing and
  shows the call it would make; a preview lists tools for real but only shows a call, unless `read_only` says the tool
  only reads.

### For an agent step

An agent's tools can be an MCP server's: `openduct_flow::mcp::Session` lists them (`tools`, each with the name,
description and JSON Schema a model's tool definition takes) and calls the one the model picks (`call`), through the
run's effects. A workflow offered as a tool is `Workflow::tool`: `input_schema()` for the model, `check()` for the
arguments it sends. See the module's documentation.
## Data tables

A workspace's **Tables** page holds its data tables: rows its workflows read and write, kept between runs.

- **A table** has a name and typed columns: **text**, **number**, **boolean** (true or false), **datetime** (ISO 8601;
  a date alone is midnight UTC) and **JSON** (any value). Every row also has an **id** (given in the order rows are
  added, never twice), **created_at** and **updated_at**. Make one empty, or from a CSV file: its header names the
  columns, and each column's type is read from what it holds. Columns can be added, renamed, given another type (each
  value is read again as it; refused, saying which row, when one cannot be) and removed, with their values.
- **Its rows** show a page at a time, sorted by any column and filtered by an expression (`row.score > 10`). Click a
  cell to change it; add and delete rows; import CSV into the table and export it as CSV (a cell a spreadsheet would
  run as a formula is written with a `'` before it, and read back without it).

**The Data table step** (`table`) does one thing to one table:

```yaml
- name: known
  table: { name: leads, action: get, where: { email: "{{ value.email }}" }, limit: 1 }
- name: keep
  table: { name: leads, action: upsert, key: email, row: { email: "{{ value.email }}", score: "{{ value.score }}" } }
- name: warm
  table: { name: leads, action: update, where: "row.score > 50", set: { hot: true, score: "{{ row.score + 10 }}" } }
- name: quiet
  table: { name: leads, action: delete, match: any, where: [{ column: email, op: empty }, { column: score, op: "<", value: "1" }] }
```

| Action | Takes | Gives |
|---|---|---|
| `get` | `where`, `match`, `sort` (a column or a list; `-score` for highest first), `limit` (100 unless it says, at most 10,000), `offset` | the rows: each its `id`, its columns, `created_at`, `updated_at` |
| `count` | `where`, `match` | `{count}` |
| `insert` | `row` (its values by column), or `rows` (an expression giving a list of records) | `{count, rows}` |
| `update` | `where` (needed), `set` (values by column, where `row` is the row as it is) | `{count, rows}` |
| `upsert` | `key` (a column, or a list), `row` | `{count, inserted, updated, rows}`: the rows whose key columns hold the row's values are set to it; when none does, it is added |
| `delete` | `where` (needed) | `{count, rows}`, as they were |

`where` is an expression over `row` (`row.score > 50 && row.owner == value.owner`), columns that must hold values
(`{status: new}`), or comparisons (`[{column, op, value}]`: `=`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `starts_with`
and `ends_with` - those three whatever the letters' case - `empty` and `not_empty`; `match: any` for any one of them).
A value is text with `{{ }}`, read as its column's type. To change or delete every row, write `where: "true"`.

**What a workflow keeps between runs** - a cursor, a counter, the last id seen - is its **state**, a record of names
and values of its own:

- The **Keep between runs** step (`state`) reads it (`state: {}` for all of it, `{get: last_id}`, `{get: [a, b]}`),
  sets values (`{set: {runs: "{{ has(state.runs) ? state.runs + 1 : 1 }}"}}`, where `state` is what it keeps now; a
  value set to null is forgotten), forgets some (`{delete: [a]}`) or all (`{clear: true}`).
- A JavaScript or Python step with `state: true` (*Give it the workflow's state*) is given it as `state`, and what it
  leaves there is kept: `state.seen = (state.seen || 0) + 1`. The script never reaches where it is kept: the state goes
  in with what the script is given, and comes back with its answer.
- The Tables page shows each workflow's state, and clears it.

**Two runs at once.** A change is worked out from the rows as they are while the table's **turn** is held - one
writer at a time per table, on every node of a cluster - so two runs adding one to the same row count two, and two
runs upserting the same key make one row. The values a script changed in its state are kept unless another run changed
one of the same values while the script ran (a change to another value is kept beside the script's); if one did, the
script (which only computes) runs again on the new state, up to ten times, and the step fails after that, saying so.
Reads take no turn: they see every change written before them.

**Trying.** Test and Preview read the tables and work a write out, and show it (`"test": "not written"`); nothing is
written. A script's state that would have been kept is shown in the step's logs. `openduct workflow test` has no
tables: each reads as empty, any column taken.

**Limits.** 100 tables a workspace, 50 columns a table, 50,000 rows a table, 64 KiB a row, 50 MiB for a workspace's
tables in all; 1,000 values and 256 KiB for a workflow's state; a step changes at most 10,000 rows; a CSV of up to 1 MiB
is imported at a time. A change waits at most 20 seconds for its table's turn, and one whose work takes longer than 7
seconds is given up, writing nothing.

**Where they are kept.** In the broker, in a compacted topic of the studio's own (`__openduct.model.tables`), as the
studio keeps everything: no database to run. Every node of a cluster reads it, and holds every workspace's tables in
memory. A node that stops while holding a table's turn holds it at most 10 seconds; the nodes' clocks should agree to
within a second (NTP).

## From the command line

```sh
openduct workflow check orders.yaml                       # its trigger, every step, every expression
openduct workflow test orders.yaml --input events.jsonl   # one run per line, sending nothing, step by step
```

A workflow is a YAML (or JSON) file, so it can be kept and reviewed in your own repository too.
Against a running studio, with an API token: `openduct workflow pull`, `diff`, `push`, `rollback` and `promote`
([CI/CD for workflows](deployment.md#cicd-for-workflows)).

## CI/CD: API tokens, environments and Git

A workspace's workflows can be built in one workspace and delivered to the next by people or by CI; see
[CI/CD for workflows](deployment.md#cicd-for-workflows) for the command line and the ready-made CI.

- **API tokens** (Settings, its admins): for CI and other programs, each with a name, a role (at most the maker's),
  an expiry (90 days unless said, at most a year) and optionally the workflows it reaches; shown once, kept only as
  its SHA-256, and revoked with a click - within a few seconds on every node of a cluster. A token acts as its maker
  would, limited to its role: a maker who loses the role, or leaves the workspace, takes the token's power with
  them. A token with workflow scopes makes only the CI/CD requests (pull, diff, push, rollback, promote); a token
  never makes tokens, nor signs in at an app's vendor. Each makes at most 120 requests a minute unless its maker
  said, counted on each node.
- **Environment** (Settings): the workspace marked as development, test or production, with an order. The switch
  *Changes only by push or promotion* keeps people's edits out: in such a workspace a workflow is added, changed or
  deleted only by a push, a promotion, a rollback or Git (it can still be turned on and off).
- **Promote** (the editor's button): the workflow, as saved, copied to an environment after this one - checked
  first, then done. The target must have every credential, lookup file, data table and other workflow it names;
  nothing secret is copied. A new workflow arrives off.
- **Git** (Settings): the repository, Save to Git, Deploy from Git, the webhook a push to the branch calls and its
  secret (shown once), and what the last deploy did ([Git sync](deployment.md#git-sync)).
- **Versions** shows where each came from: the studio, a push, a promotion, a rollback or Git, and the token.
