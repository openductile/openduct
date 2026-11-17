# kafka-protocol 0.18.0, as OpenDuct builds it

This folder is [kafka-protocol](https://github.com/tychedelia/kafka-protocol-rs) 0.18.0 as published on crates.io
(MIT OR Apache-2.0, see `LICENSE-MIT` and `LICENSE-APACHE`), with one change to how it decodes, and some files
left out or reworded (below). The workspace uses it in place of the published crate (`[patch.crates-io]` in the
top `Cargo.toml`).

## The change

When it decodes, the published crate reserves memory for as many entries as a length on the wire claims, before
it reads them: `Vec::with_capacity(n)` for a list, `vec![0; n]` for a string or byte string, and the same for the
records and headers of a record batch. A request of four bytes that claims a list of 2,147,483,647 entries makes
it ask for over 100 GB. Where the system refuses an allocation that size (Linux does, by default), the process
stops, so one small request could stop a broker.

Here, a length never sizes an allocation larger than the bytes left to read: a list reserves at most one entry per
byte left (each entry takes at least one), and a string or byte string longer than what is left is refused before
anything is allocated. What decodes is unchanged; what claims more than it holds is refused as truncated, as it
would have been once the reading ran out.

Changed places carry the comment `Changed for OpenDuct`, in `src/protocol/types.rs` and `src/records.rs`. `Cargo.toml` also
stops the compiler reporting warnings about the published code (a deprecated constant, items unused with some
features), which is otherwise left as it is.

## Left out and reworded

This copy leaves out the crate's changelog and its tests, which are not built here, and its `Cargo.toml` no
longer names the tests. Comments in `src/error.rs` and `src/compression/snappy.rs` are reworded to say in plain
words what their links to other projects' sources said; the code in those files is unchanged.

## Returning to the published crate

Once a kafka-protocol release limits allocations in the same way, remove this folder and the `[patch.crates-io]`
entry, and keep the broker test `a_request_claiming_a_huge_list_is_refused_and_the_broker_carries_on`, which fails
(on Linux) without the change.
