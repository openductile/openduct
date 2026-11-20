# Other people's clients against OpenDuct's transactions

Not part of `cargo test`: it needs Confluent's Python client. Run it after a change to transactions.

```
openduct serve --data-dir /tmp/txn --listen 127.0.0.1:19395
python3 -m venv /tmp/ck && /tmp/ck/bin/pip install confluent-kafka
/tmp/ck/bin/python confluent_transactions.py 127.0.0.1:19395
```

Six checks, with librdkafka's transactional producer and `read_committed` consumers: a transaction over two topics
is seen only once committed; an aborted one never; an open one holds back what follows it; a second producer with
the same `transactional.id` fences the first, and what the first wrote is aborted; a group's offsets are committed
with the transaction or not at all (a copy that aborts is redone, one that commits is not); a transaction left
open past its timeout is aborted and its late commit refused.

Against a cluster: start three brokers (`--node-id`, `--brokers`, `--replication-listen`, `--cluster-secret-file`;
see docs/deployment.md), wait until they answer, and give the script any one of them.

Last run: 2026-11-18, confluent-kafka 2.16.2 (librdkafka 2.16.0), Python 3.14 - 6 of 6 on one broker, and 6 of 6
on a new three-broker cluster through each of its brokers.

## Apache Kafka's own tools

```
sh kafka_tools.sh 19395                 # one broker
sh kafka_tools.sh 19401 19402 19403     # a cluster: every broker's port
```

It needs Docker and the images `apache/kafka` and `confluentinc/cp-schema-registry` (the second only for a few
lines of Python that carry the containers' connections to this machine). Eleven checks: the performance test's
producer writing 3,000 records in transactions of 100 ms; Kafka's exactly-once copier
(`TransactionalMessageCopier`) moving 500 records 50 a transaction with aborts at random - none twice, none
missing; Kafka Streams' word count run with `processing.guarantee=exactly_once_v2`; and `kafka-transactions` (list,
describe, describe-producers) while a producer holds a transaction open, then `find-hanging` and `abort` once
that producer is killed.

Last run: 2026-11-18, apache/kafka 4.4.0, its client on the second transaction protocol - 11 of 11 on one broker and on a new three-broker cluster.

## A transaction no coordinator knows of

Kafka's clients never leave one, so a helper among the broker's tests does (it writes a transaction's record and
tells no coordinator):

```
openduct topic create hang --bootstrap 127.0.0.1:19395
OPENDUCT_STRAY=127.0.0.1:19395 OPENDUCT_STRAY_TOPIC=hang \
    cargo test -p openduct-broker --test transactions leave_a_stray_transaction -- --ignored
kafka-transactions.sh --bootstrap-server 127.0.0.1:19395 find-hanging --topic hang --max-transaction-timeout 0
kafka-transactions.sh --bootstrap-server 127.0.0.1:19395 abort --topic hang --partition 0 --start-offset 0
```

Last run: 2026-11-18, apache/kafka 4.4.0 - `find-hanging` names the transaction (producer 9001, offset 0), `abort`
ends it, and `find-hanging` then names none. Left alone, the partition's leader aborts it after 16 minutes.
