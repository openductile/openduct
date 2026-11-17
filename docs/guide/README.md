# OpenDuct user guide

1. [Getting started](getting-started.md): build, run, create a topic, and send and read messages, in about ten
   minutes.
2. [Concepts](concepts.md): topics, partitions, offsets, keys, consumer groups, retention and durability.
3. [Connecting your applications](connecting.md): use OpenDuct from any Kafka client, with the settings that matter.
4. [Running OpenDuct for real](running.md): other machines, services, environment settings, small devices, logs,
   backups, upgrades and monitoring.
5. [Command reference](reference.md): every command and setting.
6. [Troubleshooting](troubleshooting.md): what an error means and what to do about it.
7. [Pipelines](../pipelines.md): read a topic, change each message with expressions, write another topic.
8. [Expressions](../expressions.md): short pieces of CEL that pick out, clean up, calculate and decide.
9. [Connectors](../connectors.md): pipelines that read from HTTP APIs, webhooks, MQTT, Modbus, OPC UA, other Kafka
   clusters, PostgreSQL, MySQL, MongoDB, Salesforce, ServiceNow and Snowflake, and write to Elasticsearch, S3,
   MQTT, other Kafka clusters, Salesforce, ServiceNow and Snowflake.
10. [Pipeline examples](../../examples/pipelines): sixteen worked examples, each with input and expected output.
11. [The studio and workflows](../studio.md): build, try and watch workflows in the browser.
12. [Apps](../apps.md): the 26 apps, and the kit to build your own.

Beside the guide:

- [Tutorials](../tutorials/README.md): a first workflow with real apps, triggers, forms, and building an app - with
  screenshots.
- [What OpenDuct does, and does not do yet](../capabilities.md).
- [OpenDuct beside other tools](../comparison.md).
- [What the broker is for](../use-cases.md).
- [Test results](../regression.md).

For the details underneath:

- [Storage](../storage.md): how data is kept safe on disk.
- [Protocol](../protocol.md): every Kafka API and version OpenDuct supports.
- [Deployment](../deployment.md): sizing and limits.
- [Architecture](../architecture.md): how OpenDuct is built.
