## What this changes

<!-- What and why, in a few sentences. Link the issue it closes. -->

## How it was tested

<!-- The tests you added or ran. For anything touching writes, reads, offsets or recovery: how you showed the
     guarantee still holds, including after a crash or restart. -->

## Checklist

- [ ] `cargo xtask check` passes locally (format, lint, tests, licences)
- [ ] Tests cover the change; a bug fix has a test that fails without it
- [ ] `README.md`, `docs/` and `CHANGELOG.md` are updated where the change affects users
- [ ] No `unsafe`, no panics on network or disk input, every new buffer or wait is bounded
- [ ] First contribution: I have added the CLA statement from [CLA.md](../CLA.md) as a comment
