# Pipeline examples

Each folder holds a pipeline file, sample input, and the exact output expected. Each is also a test: the test
suite runs every example and fails if its output differs from `expected.jsonl` by a single character.

```sh
cd clean-orders
openduct pipeline test pipeline.yaml --input input.jsonl
```

| Example | What it shows |
|---|---|
| [clean-orders](clean-orders) | Tidy e-commerce orders: trim and capitalise names, total the lines, read dates, drop test orders, set bad ones aside. |
| [sensor-alerts](sensor-alerts) | IoT readings: convert units, round, flag readings over a limit, ignore disabled sensors. |
| [access-log](access-log) | Web server log lines (plain text) into fields, keeping only the failed requests. |
| [payment-events](payment-events) | Events of different shapes from a payment provider, made into one kind of record. |
| [mask-personal-data](mask-personal-data) | Customer records with e-mail addresses and phone numbers hidden, and a hash kept for matching. |
| [order-lines](order-lines) | One order in, one message per order line out. |
| [machine-states](machine-states) | Factory presses read over MQTT: running, idle or stopped by a fault, keyed by machine. |
| [energy-meter](energy-meter) | A panel's energy meter read over Modbus TCP: kW and a load level, written only on change. |
| [oven-zones](oven-zones) | A curing oven's zones watched over OPC UA, signed and encrypted: each change, and whether it is in the recipe's band. |
| [order-changes](order-changes) | A shop's orders table in PostgreSQL as a stream of what happens to each order: placed, changed, removed. |
| [stock-levels](stock-levels) | A shop's stock levels in MySQL as they change, with what is running low flagged. |
| [catalog-changes](catalog-changes) | A product catalogue in MongoDB kept as the latest of each product, ready for a search index. |
| [won-deals](won-deals) | Salesforce opportunities read as they change: a topic of the deals won. |
| [fault-tickets](fault-tickets) | A ServiceNow incident opened for each press stopped by a fault, and changed when the fault repeats. |
| [sales-warehouse](sales-warehouse) | A shop's orders loaded into Snowflake for its sales reports, merged by order, with no personal data. |
| [paid-orders](paid-orders) | Order events read from another Kafka cluster as a consumer group there, over SASL and TLS: the paid ones, in money. |

See [Pipelines](../../docs/pipelines.md) and [Expressions](../../docs/expressions.md) for everything they use.
