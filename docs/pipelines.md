# Pipelines

A pipeline reads messages from one topic, changes them with [expressions](expressions.md), and writes the results to
another topic, continuously, as messages arrive. Clean up raw data, keep only what matters, reshape one system's
messages for another: each is one short file.

```yaml
# orders-clean.yaml
name: orders-clean
from:
  topic: orders
where: status != 'test'
map:
  order_id: id
  customer: customer.name.trim()
  total:    round(lines.map(line, double(line.qty) * double(line.price)).sum(), 2)
  placed:   timestamp(created_at, '%Y-%m-%d %H:%M:%S')
key: order_id
to:
  topic: orders-clean
on_error:
  topic: orders-rejected
```

```sh
openduct pipeline run orders-clean.yaml
```

## Trying a pipeline before you run it

`openduct pipeline test` runs a pipeline over messages in a file, with no broker, and prints what it would write.
It is the quickest way to get the expressions right:

```sh
openduct pipeline test orders-clean.yaml --input orders.jsonl
```

The input has one message value per line: JSON, or plain text for a pipeline with `from.format: text`. The output
has one line per message written, and one per message that failed, in the order the messages came in:

```text
{"key":"A-100","value":{"order_id":"A-100","customer":"Ada Lovelace","total":24.5,"placed":"2026-11-14T22:30:00Z"}}
{"line":3,"error":"map.total: double: given text that is not a number"}
```

`openduct pipeline check orders-clean.yaml` only checks the file and its expressions, and says where any mistake is.

## The pipeline file

| Setting | Required | Meaning |
|---|---|---|
| `name` | yes | The pipeline's name: letters, digits, `.`, `_` and `-`. |
| `language` | no | The language its expressions are written in: `cel` (the default, see [Expressions](expressions.md)). |
| `from.topic` | yes, or a connector | The topic to read. A [connector](connectors.md) (`http:`, `webhook:`) reads from another system instead. |
| `from.group` | no | The consumer group that keeps the pipeline's progress. The pipeline's name if not given. |
| `from.start` | no | Where a new pipeline starts: `earliest` (every message kept) or `latest` (new messages only, the default). |
| `from.format` | no | `json` (the default): the value is JSON. `text`: the value is text, read as `value`. |
| `let` | no | Names and expressions: values worked out once, which `where`, `each`, `map`, `key` and `headers` can then use by name, and later `let` names too. |
| `where` | no | An expression. Only messages for which it is true go on. |
| `each` | no | An expression giving a list. The pipeline writes one message per item, with the item in reach as `item`. |
| `map` | no | Field names and expressions: the value written. Without it, the value goes on unchanged. |
| `key` | no | An expression for the key written. It reads the value as `map` made it. The key read, if not given. |
| `headers` | no | Header names and expressions, added to the headers read. They read the value as `map` made it. |
| `to.topic` | yes, or a connector | The topic to write. A [connector](connectors.md) (`elasticsearch:`, `s3:`) writes to another system instead. One side of a pipeline is always a topic. |
| `on_error.topic` | no | Where a message that fails goes (see below). |

The steps run in this order: `let`, `where`, `each`, `map`, then `key` and `headers`.

**Text in an expression goes in single quotes, and some expressions need YAML's double quotes around them.**
YAML takes a value's outer quotes for itself, so `currency: 'USD'` gives the expression `USD`, which reads a field
called USD: write `currency: "'USD'"`. An expression with `a ? b : c`, or one that starts with a quote or `!`,
goes in double quotes too: `status: "paid ? 'paid' : 'open'"`. One that starts with a name, a number or a
bracket needs none: `currency: currency.upperAscii()`. [Expressions](expressions.md#writing-them-in-a-pipeline-file)
has the whole rule.

## When a message fails

A message fails when an expression cannot work for it, for example when a field holds text where a number is
needed.

- **With `on_error.topic`**, the failed message is written there unchanged, with three headers added:
  - `openduct-error`: the reason;
  - `openduct-pipeline`: the pipeline's name;
  - `openduct-source`: where it came from, as `topic/partition/offset`.

  The pipeline carries on. Fix the cause, and the rejected messages can be sent through again.
- **Without it**, the pipeline stops at the failed message and reports the reason. Nothing is skipped silently. It
  starts again from that message once restarted.

## Delivery

A pipeline commits its progress only after the messages it wrote are confirmed on disk. If it stops for any
reason, it starts again from its last commit, so no message is lost. A message that was written just before a stop
can be written a second time, so a consumer of the output should tolerate seeing a message twice, for example by
using a key it can de-duplicate on.

## Running pipelines

```sh
openduct pipeline run orders-clean.yaml --bootstrap 127.0.0.1:9092
```

`pipeline run` runs until Ctrl-C or SIGTERM. Run several pipelines, or several copies of one to share a busy
topic's partitions between them. Copies of one pipeline form one consumer group, like any other consumer.

## In the studio

With `openduct start`, each workspace has a **Pipelines** page. A pipeline is written there, checked as it is typed,
and tried over sample messages (nothing is sent). Turned on, OpenDuct runs it on every node, the copies sharing the
topic's partitions as one group; one that reads from a connector runs on the first node only. The topics it names are
made if they are not there yet, with one partition each. Its counts (read, written, rejected) are shown as it runs,
and a run that stops is tried again by itself, the reason shown.

A workspace's topics are its own. In the studio they are written and shown by their plain names (`orders`); on the
broker they are named `w.<workspace>.<name>` (in workspace `6f1c…`, `orders` is `w.6f1c….orders`), so two workspaces may each
have an `orders` and neither reads the other's. A Kafka client, or `openduct send` and `openduct read`, uses the
full name; a workspace's Settings page shows it. Topic triggers and topic steps in workflows work the same way.

## Examples

The [pipeline examples](../examples/pipelines) each come with a pipeline file, sample input and the exact output
expected:

| Example | What it shows |
|---|---|
| [clean-orders](../examples/pipelines/clean-orders) | Tidy e-commerce orders: trim names, total the lines, read dates, drop test orders, reject bad ones. |
| [sensor-alerts](../examples/pipelines/sensor-alerts) | IoT readings: convert units, round, flag readings over a limit, ignore disabled sensors. |
| [access-log](../examples/pipelines/access-log) | Web server log lines (plain text) into fields, keeping only the failed requests. |
| [payment-events](../examples/pipelines/payment-events) | Events of different shapes from a payment provider, made into one kind of record. |
| [mask-personal-data](../examples/pipelines/mask-personal-data) | Customer records with e-mail addresses and phone numbers hidden, and a hash kept for matching. |
| [order-lines](../examples/pipelines/order-lines) | One order in, one message per order line out. |
