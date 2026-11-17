# Clean orders

Raw orders from a web shop arrive with stray spaces in names, e-mail addresses in mixed case, prices sometimes
written as text, and test orders mixed in. This pipeline writes one clean order per real order and sets aside any
order it cannot read, with the reason.

| Line | What happens |
|---|---|
| 1 | Name trimmed, e-mail in small letters, 2 of the 3 lines have a quantity, total 2 × 5 + 0 × 7 + 4 × 3.625 = 24.5. |
| 2 | A test order (`status: "test"`): dropped by `where`. |
| 3 | The price is the text `"19.99"`, read as a number by `double(...)`: 3 × 19.99 = 59.97. No currency: `USD` by default. |
| 4 | The quantity is `"two"`: the order goes to `orders-rejected`, with the reason in its `openduct-error` header. |
| 5 | No e-mail and no date: those fields are `null`. No lines: 0 items and a total of 0. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```
