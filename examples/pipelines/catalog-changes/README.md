# Catalog changes

A shop's product catalogue lives in MongoDB. This pipeline keeps a `catalog` topic with the latest of each
product, ready to feed a search index: every product already there when it first starts, then every one added,
changed or removed, keyed by product id so a product's versions stay in order. It reads MongoDB's change streams,
and saves its place in them only once the changes before it are in the topic.

| Line | What happens |
|---|---|
| 1 | A product that was there when the pipeline first started (`snapshot`). |
| 2 | Its price lowered and its stock sold out: `in_stock` is `false`. `after` is the whole product as it is now. |
| 3 | A new product. |
| 4 | A product removed: only its id is known, in `before`, so the record says `removed`. |

Try the expressions on the sample, with no database needed (each line is a change as the connector writes it):

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, the database is a replica set or a sharded cluster (MongoDB Atlas is), and the user may read the
collection (the `read` role on the database is enough). Set `MONGO_USER` and `MONGO_PASSWORD`, point `url` at the
cluster, and create the topic:

```sh
openduct topic create catalog
openduct pipeline run pipeline.yaml
```
