# Stock levels

A shop keeps its stock levels in MySQL, one row per product and warehouse. This pipeline keeps a `stock-levels`
topic of every product's stock as it changes, flags what is running low, and keys each by product and warehouse
so a product's levels stay in order. It reads MySQL's binary log as a replica does, so nothing is installed on the
database, and saves its place in the log only once a transaction's rows are in the topic.

| Line | What happens |
|---|---|
| 1 | A level that was there when the pipeline first started (`snapshot`): 42, above its reorder point of 10. |
| 2 | Sold down to 8: below the reorder point, so `low` is `true`. MySQL sends the whole row before and after. |
| 3 | A new product in another warehouse. |
| 4 | A product removed: left out by `where`. |

Try the expressions on the sample, with no database needed (each line is a change as the connector writes it):

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, the database needs its binary log of whole rows with column names (`binlog_row_metadata = FULL`) and a
user with `REPLICATION SLAVE`, `REPLICATION CLIENT` and `SELECT` on the table. Set `MYSQL_USER` and
`MYSQL_PASSWORD`, point `url` at the database, and create the topic:

```sh
openduct topic create stock-levels
openduct pipeline run pipeline.yaml
```
