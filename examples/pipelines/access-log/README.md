# Access log

Each message is one line of a web server's access log, as plain text (`format: text`), so the whole line is
`value`. Regular expressions take it apart. `let` names the status once, so `where` and `map` both use it.

| Line | What happens |
|---|---|
| 1 | Status 200: dropped by `where`. |
| 2 | Status 502: kept as an `error`. The time `10:01:13 +0100` is written in UTC, `09:01:13Z`. |
| 3 | Status 503: kept as an `outage`. The query string stays part of the path. |
| 4 | Not a log line: no status, so it is kept as `unreadable` rather than dropped without a trace. |

```sh
openduct pipeline test pipeline.yaml --input input.txt      # prints what expected.jsonl holds
```
