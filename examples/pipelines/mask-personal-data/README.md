# Mask personal data

Customer records go to an analytics team that needs to count and match customers, not to contact them. `map`
writes only the fields it names, so anything else (here a password that should never have been there) does not
travel on.

| Line | What happens |
|---|---|
| 1 | `A. Lovelace`, `a***@example.com`, the SHA-256 of `ada@example.com` (after trimming and lower-casing, so the same customer always hashes alike), and only the last four digits of the phone number. The password is not written. |
| 2 | The same, for a phone number written another way. |
| 3 | No e-mail and no phone: those fields are `null`, not made up. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```
