# Fuzzing

Every place OpenDuct reads bytes it did not write is a function in
[`crates/openduct-hostile`](../crates/openduct-hostile/src/lib.rs): Kafka requests, record batches, segment files,
expressions, pipeline files, MQTT packets, PostgreSQL replication messages, MySQL binary log events, and requests sent
to a running broker. Each must return for any bytes: no panic, no hang, no more memory than the bytes justify.

They are driven two ways:

- **Random tests**, with every `cargo test`: valid inputs mutated with a fixed seed
  (`crates/openduct-hostile/tests/random.rs`). `OPENDUCT_HOSTILE_ROUNDS=<n>` runs `n` times as many.
- **cargo-fuzz** (libFuzzer, guided by coverage), from this directory, in CI for half a minute per target:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
cd fuzz
cargo +nightly fuzz list
cargo +nightly fuzz run broker -- -max_total_time=600
```

A finding is written to `fuzz/artifacts/<target>/`; replay it with `cargo +nightly fuzz run <target> <file>`.

This directory is its own workspace and is never built into OpenDuct. libFuzzer itself is C++ (through
`libfuzzer-sys`); it exists only here.
