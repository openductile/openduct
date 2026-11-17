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
| **Schedule** | Every so often - `30s`, `15m`, `1h`, `1d` - counted from midnight UTC (every 15m runs at :00, :15, :30 and :45). In a cluster each time runs once. |
| **Topic** | A message arrives on a topic. |
| **By hand** | Someone presses Run. |
| **When one fails** | Another workflow's run fails: its record, its error and where it failed. |

## Steps

| Family | Steps |
|---|---|
| Data | Set fields, Filter (only if), Branch (if / else), Switch (several ways), Loop (for each), List tools (keep, sort, de-duplicate, limit), Group and summarise, Merge, Date and time, Look up a file |
| AI | Ask AI (one form whatever the model: OpenAI, Anthropic, Google Gemini, Mistral, Groq, OpenRouter, Together, a model on this machine through Ollama or LM Studio, or any compatible endpoint), Embed text |
| Data stores | Query a database (SQL on PostgreSQL; with pgvector, the texts nearest a question), Write to a topic |
| Messages | Send an e-mail (through your own mail server), the messengers, and every [app](apps.md) |
| Code | A snippet of your own, run in a locked-down process: no environment, no files but its input and output, limits on its memory, CPU time and processes, stopped after 60 seconds |
| Calls | HTTP request (any API), Run a workflow (another of the workspace, its result used), Respond to webhook |
| Control | Wait, Stop (end the run, or fail it with a reason) |

Each step names what it needs with [expressions](expressions.md): `value` is the event that started the run,
`steps.<name>` what an earlier step gave, `trigger` how the run started, and in a loop `item` and `index`. A text
field takes templates - `Order {{ value.id }} is late` - and a condition is an expression - `value.total > 100`.

**When a step fails:** it is tried again as often as its *Tries* say, with a wait between. If it still fails, the
run either stops and sends its record to **CatchErr** (by default), or carries on with the next step.

## Building and trying

- **The canvas** shows the trigger and the steps; a branch, a switch and a loop show their own steps inside them.
- **A step's window** has three parts: **Input** (what the step can read - the sample input, each earlier step's
  result from the last test, how the run started; a field clicked goes into the box being written in, and a field
  the step names that is not there is said), **Settings**, and **Output** (what it gave in the last test).
- **Test** runs the workflow on a sample input and sends nothing: each step shows what it would have done.
  **Preview** does the reads for real (AI answers, GETs, lookups) and only shows the writes. **Run for real** runs the
  sample input as a real event.
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

## Credentials

A credential holds what a step needs to reach a service: an API key, a token, a mail server's password, a
database's address. It is sealed when it is kept and never shown back; a test reads it as `•••`.

- **Apps signed in at the vendor** (Google, Microsoft 365, QuickBooks, Xero): the installation registers with the
  vendor once - its own client id and secret, under *Settings → App sign-ins* - and each workspace then presses
  **Connect**. OpenDuct takes no part beyond the installation's own registration, and renews tokens as they run out.
  See [apps](apps.md#signing-in-with-oauth-20).
- **No secret in an environment variable.** The studio keeps credentials sealed in the cluster; OpenDuct's own
  secrets (the cluster secret, storage keys, a password) come from files: see
  [deployment](deployment.md#secrets-are-files).
- **This machine's own network**: a workflow may not reach private addresses (the machine itself, the local network,
  cloud metadata) unless the installation allows it (`--allow-private-network`).

## From the command line

```sh
openduct workflow check orders.yaml                       # its trigger, every step, every expression
openduct workflow test orders.yaml --input events.jsonl   # one run per line, sending nothing, step by step
```

A workflow is a YAML (or JSON) file, so it can be kept and reviewed in your own repository too.
