# Contributing to OpenDuct

Thank you for helping. OpenDuct carries other people's data in places where losing it is not an option - production
event pipelines, robots and machines, small devices at the edge - so we hold every change to a high bar. This guide
says what that bar is and how to get a change in.

## Before you start

- **Read the [Code of Conduct](CODE_OF_CONDUCT.md).** It applies everywhere the project lives.
- **Security problems are never filed as public issues.** See [SECURITY.md](SECURITY.md).
- **For anything bigger than a small fix, open an issue first** and describe what you want to change and why. A
  change to the wire protocol, the storage format or a guarantee needs a short design note agreed in the issue
  before code.

## The Contributor License Agreement

Every contribution is made under the [OpenDuct Contributor License Agreement](CLA.md). On your first pull request,
add this comment once:

> I have read the OpenDuct Contributor License Agreement (CLA.md) and I accept it for all my Contributions to the
> Project.

Pull requests without it are not merged.

## Building and testing

You need a stable Rust toolchain (the version is pinned in `rust-toolchain.toml`; `rustup` picks it up).

```sh
cargo xtask check    # format, lint, tests, licences and advisories
cargo xtask build    # release build of the `openduct` binary
cargo xtask test     # tests only
```

The connector tests that need a real system (S3, Elasticsearch) run when it is given. Start them in containers
with `cargo xtask services up`, then `eval "$(cargo xtask services env)"` before `cargo test`; stop them with
`cargo xtask services down`.

`cargo xtask check` needs [cargo-deny](https://github.com/EmbarkStudios/cargo-deny)
(`cargo install cargo-deny --locked`). CI also runs the cross-compile checks.

`cargo xtask check` must pass before you open a pull request. It runs:

| Step | Command | Why |
|---|---|---|
| Format | `cargo fmt --all --check` | one style, no review time spent on it |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | every warning is an error |
| Tests | `cargo test --workspace` | unit, protocol and end-to-end tests |
| Licences and advisories | `cargo deny check` | dependencies only under MIT, Apache-2.0, BSD or similar permissive licences, from crates.io, with no open security advisory |

CI also fuzzes every hostile-input target for half a minute each ([fuzz/README.md](fuzz/README.md)). A change that
reads new bytes from outside - a request field, a file, a connector's protocol - adds a target to
`crates/openduct-hostile` and seeds for it in its random tests.

## What we look for in a change

- **Correctness first.** A broker that is fast and loses data is useless. Changes to how data is written, read,
  acknowledged or recovered come with tests that show the guarantee holds, including after a crash or restart.
- **No panics on input.** Anything that arrives over the network or from disk is untrusted: malformed input is an
  error, never a crash. `unsafe` code is not allowed (`#![forbid(unsafe_code)]` in every crate).
- **Bounded resources.** Every buffer, queue and wait has a limit or a timeout, stated where it is set.
- **Small and focused.** One change per pull request, with a clear title and a description of what changed, why,
  and how it was tested.
- **Tests with the change.** New behaviour has tests; a bug fix has a test that fails without the fix.
- **Docs with the change.** If a change affects what OpenDuct supports or how it is run, `README.md` and `docs/`
  are updated in the same pull request, and `CHANGELOG.md` gets a line under "Unreleased".
- **Dependencies are a cost.** Each new crate is justified in the pull request, must pass `cargo deny`, and must be
  maintained.

## Commit messages

Write the subject as what the change does, in the present tense ("Fetch waits for new records instead of
polling"), keep it under about 72 characters, and explain the why in the body.

## Review and merging

Maintainers (see [GOVERNANCE.md](GOVERNANCE.md)) review every pull request. Changes reach `main` only through a
reviewed pull request with all checks passing, and only maintainers merge. We aim to give a first response within a
few working days.

## Licence

OpenDuct is licensed under the [Apache License, Version 2.0](LICENSE). Every source file starts with:

```rust
// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0
```
