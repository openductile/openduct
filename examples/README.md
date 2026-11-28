# Examples

Each example works against any Kafka-protocol broker. Start OpenDuct first:

```sh
cargo run --release -p openduct -- serve --memory
```

| Example | Run it | What it shows |
|---|---|---|
| [sensor-telemetry](sensor-telemetry/src/main.rs) | `cargo run -p sensor-telemetry -- produce`, then `cargo run -p sensor-telemetry -- consume --from-beginning` | Simulated sensors sending readings, keyed by sensor so each one's readings stay in order on one partition, and a consumer following every partition with long polling. |
| [embedded-broker](embedded-broker/src/main.rs) | `cargo run -p embedded-broker -- ./edge-log` (it needs no separate broker) | OpenDuct inside your own program, with a durable log. Run it twice: the log survives restarts. |

`--bootstrap host:port` points an example at another broker.

[ci](ci) holds ready-made CI for workflows kept in a repository - GitHub Actions and GitLab CI that check and test
them on every change, push them to a test workspace after a merge and promote them to production - and two workflow
files to start from ([CI/CD for workflows](../docs/deployment.md#cicd-for-workflows)).
