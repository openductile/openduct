# NocoDB

Records of [NocoDB](https://nocodb.com) tables - on NocoDB Cloud or a NocoDB of your own: list them by a filter and an
order, look one up, add, change, match-and-merge, remove, count and link them; find a workspace's bases and a base's
tables; and start a workflow for each record added, changed or removed.

Written by hand from NocoDB's own v3 API description, <https://nocodb.com/apis/v3/swagger-v3.json> (fetched
2026-11-27), and its webhook pages (<https://nocodb.com/docs/product/tables/table-operations/webhook>). NocoDB's code
is under its Sustainable Use License, not an open one: only the API's facts are used here, in our own words. The API
reference: <https://nocodb.com/apis/v3/data>.

## Signing in

- **An API token** (a NocoDB credential): in NocoDB, **Account Settings → API Tokens → Create New API Token** (a
  fine-grained token, for the projects it needs); sent in the `xc-token` header. For a NocoDB of your own, give its address too (`https://nocodb.example.com`): requests go there
  in place of NocoDB Cloud.

Tables and bases are named by their ids (`p...` for a base, `m...` for a table): **List a workspace's bases**, **List
a base's tables** and **Describe a table** give them, as does the address of a table open in NocoDB.

## Actions

- **List a table's records** (`list_records`): a filter (`(Status,eq,Open)~and(Amount,gt,100)`), an order by a field,
  only some fields, or a view's; every page, up to 1,000.
- **Look up a record** (`get_record`), **Add a record** (`create_record`, the default), **Change a record**
  (`update_record`), **Add or change a record by its fields** (`upsert_record`: matched on up to 3 fields), **Remove a
  record** (`delete_record`), **Count a table's records** (`count_records`).
- **List a record's linked records** (`list_linked_records`), **Link records** (`link_records`), **Unlink records**
  (`unlink_records`).
- **List workspaces** (`list_workspaces`, NocoDB Cloud), **List a workspace's bases** (`list_bases`), **List a base's
  tables** (`list_tables`), **Describe a table** (`get_table`: its fields and views).

Fields go by their titles, as the table shows them. A record is its `id` and its `fields`.

## Triggers

- **A record added (or changed, or removed)** (`record_event`): a NocoDB webhook on the table, for the operations
  chosen - `insert` unless said, or `update`, `delete`. Turning the workflow on adds the webhook with a secret of the
  workflow's own in a header (`x-webhook-token`), which each delivery must carry; turning the workflow off removes it.
  A delivery of several records (a bulk insert) runs once for each.

NocoDB signs no webhook: the secret header, which NocoDB's webhooks let a caller set, is what tells its deliveries from
others.

## Not done

- Uploading a file into an attachment field, the buttons' actions, NocoDB's documents, dashboards and scripts, and
  the meta API's changes to tables, fields, views and members.

## Tested

Recorded exchanges built from the v3 description (`tests/`): each action, the pages of records, a NocoDB of your own,
a refusal in NocoDB's words, and the trigger's registering, a delivery of two records, one without the secret, and
the removal. Not run against a live NocoDB.

```sh
openduct app check apps/nocodb --bar 80
openduct app test apps/nocodb
```
