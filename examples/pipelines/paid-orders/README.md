# Paid orders

A shop's order events arrive in another team's Kafka cluster, a hosted one that asks for SASL over TLS. This
pipeline reads the `shop.orders` topic there as a member of the consumer group `openduct-paid-orders`, keeps the
paid orders, turns amounts in cents into money, and keys each by order so one order's events stay in order here
too. Each partition's offset is committed in that group only once what was read is in the `paid-orders` topic, so
the cluster's own tools show how far the pipeline has got.

| Line | What happens |
|---|---|
| 1 | Paid: 4599 cents is 45.99, the currency is written in capitals, the time is read. |
| 2 | Only created, not paid: left out by `where`. |
| 3 | The same order, now paid: written, keyed by its order. |
| 4 | A larger order in pounds: 100000 cents is 1000. |

Try the expressions on the sample, with no Kafka cluster needed:

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, set `KAFKA_USER` and `KAFKA_PASSWORD`, point `brokers` at your cluster, and create the topic:

```sh
openduct topic create paid-orders
openduct pipeline run pipeline.yaml
```

Each message also carries `kafka-topic`, `kafka-partition` and `kafka-offset` headers, saying where in the other
cluster it came from.
