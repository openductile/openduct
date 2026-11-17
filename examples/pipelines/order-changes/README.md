# Order changes

A shop keeps its orders in PostgreSQL. This pipeline turns the `orders` table into a stream of what happens to
each order: the orders already there when it first starts, then every order placed, changed or removed, keyed by
order so each order's history stays in order in the `order-changes` topic. It uses PostgreSQL's own logical
replication, so nothing is installed on the database, and it confirms a transaction to the database only once its
rows are in the topic.

| Line | What happens |
|---|---|
| 1 | An order that was there when the pipeline first started: `existing`. |
| 2 | A new order: `placed`, with the time its transaction committed. |
| 3 | The same order updated: `changed`. The row before is not sent unless its key changed, so `before` is `null`. |
| 4 | An order deleted: `removed`. Only `before` has it (its key), so the order comes from `before.id`. |
| 5 | The table emptied with `TRUNCATE`: left out by `where`. |

Try the expressions on the sample, with no database needed (each line is a change as the connector writes it):

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, the database needs `wal_level = logical` and a user with the `REPLICATION` attribute that may read
the table. Set `PG_USER` and `PG_PASSWORD`, point `url` at the database, and create the topic:

```sh
openduct topic create order-changes
openduct pipeline run pipeline.yaml
```

The pipeline keeps its place in the replication slot `openduct_order_changes`. While the pipeline is stopped the
database keeps the changes for it, which takes disk space: when you retire the pipeline, remove the slot with
`SELECT pg_drop_replication_slot('openduct_order_changes');`.
