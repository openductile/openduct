# Won deals

The sales team works in Salesforce. This pipeline reads its opportunities as they change and keeps a `won-deals`
topic of the deals won, for invoicing, a dashboard or a welcome mail, keyed by opportunity so each deal's changes
stay in order. It reads through Salesforce's REST API every two minutes, asking only for won deals and only for
what changed since the last record it stored; nothing is installed in Salesforce.

| Line | What happens |
|---|---|
| 1 | A deal won: written with its account, amount and close date. |
| 2 | Another, a renewal. |
| 3 | A deal deleted (in the recycle bin): left out by `where`. |

Try the expressions on the sample, with no Salesforce org needed (each line is a record as the connector writes it):

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, make an External Client App with the client credentials flow and a run-as user
who may read opportunities. Set `SF_CLIENT_ID` and `SF_CLIENT_SECRET`, put the org's My Domain in `login_url`,
and create the topic:

```sh
openduct topic create won-deals
openduct pipeline run pipeline.yaml
```
