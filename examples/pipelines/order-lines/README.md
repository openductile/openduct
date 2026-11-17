# Order lines

`each: lines` writes one message per item of `lines`. In `map`, `item` is the line and the order's own fields are
still in reach, so each line message carries its order's id and currency. `key` reads the value `map` built, so
each line gets a key of its own: `A-100/a`.

| Line | What happens |
|---|---|
| 1 | Three lines, three messages: 2 × 5 = 10, 0 × 7 = 0, 4 × 3.625 = 14.5. |
| 2 | No lines: no messages. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```
