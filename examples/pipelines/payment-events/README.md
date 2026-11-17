# Payment events

A payment provider's webhook events come in several shapes: a charge carries its own id, a refund points at the
charge it refunds, and customer events are not payments at all. This pipeline makes every charge and refund one
record keyed by the payment it is about, so a consumer sees a payment's whole story in order.

| Line | What happens |
|---|---|
| 1 | A successful charge of 1999 cents: `paid`, 19.99 USD. |
| 2 | A refund of 500 cents against `ch_1`: keyed `ch_1` too, `refunded`, −5 USD. |
| 3 | A failed charge: `failed`, with the provider's reason. |
| 4 | A customer event: dropped by `where`. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```
