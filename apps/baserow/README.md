# Baserow

Rows of [Baserow](https://baserow.io) tables - on Baserow's cloud or a Baserow of your own - read, added, changed and
removed, one at a time or up to 200 at once.

Made by `openduct app from-openapi` from Baserow's own API description, <https://api.baserow.io/api/schema.json>
(Baserow 2.4.3, fetched 2026-11-27, MIT licence), then finished by hand: each table's fields are its own, so a row is
given and read as a record of its fields by their names (`user_field_names`), not as the description's
`field_1, field_2, ...`. The webhook's delivery is as Baserow's own example shows it
(<https://baserow.io/blog/create-notification-system-using-webhooks>). The vendor's API reference:
<https://baserow.io/docs/apis/rest-api> (and each database's own API page in Baserow).

## Signing in

In Baserow, **My settings → Database tokens → Create token**: name it, choose the workspace, and what it may do
(create, read, update, delete rows) in which databases or tables. Copy the token into a Baserow credential. It is sent
as `Authorization: Token ...`.

For a Baserow of your own, give its address too (`https://baserow.example.com`); empty, Baserow's cloud
(`api.baserow.io`) is used.

A database token reaches rows and fields only: making tables, views and webhooks needs a signed-in person, so they
are done in Baserow itself.

## Actions

- **List a table's rows** (`list_rows`): every page; holding some words, of a view, in an order, or a Baserow filter
  (JSON) if given.
- **Look up a row** (`get_row`), **Add a row** (`create_row`, the default), **Change a row** (`update_row`), **Delete
  a row** (`delete_row`).
- **Add many rows** (`create_rows`), **Change many rows** (`update_rows`), **Delete many rows** (`delete_rows`): up
  to 200 at once.
- **List a table's fields** (`list_fields`), **List tables** (`list_tables`): names, kinds and ids.

A field is given by its name: `Name: Ann`, `Paid: true`, `Due: 2026-12-01`; a single select by its option's id
(from **List a table's fields**), a link to another table by the rows' ids.

## Triggers

- **A new row** (`new_row`), asked every 5 minutes: each row added to a table.
- **Rows added (as they are made)** (`rows_created`): Baserow's webhook, made by hand. In Baserow, the table's
  **••• → Webhooks → Create webhook**: method POST, the workflow's address as the URL, user field names on, the
  event **Rows created**, and a header `X-OpenDuct-Token` whose value is the credential's webhook token (a long
  random text you choose). Baserow does not sign its deliveries; that header is how a delivery is told from a forged
  one. Each row added is a run.

## Not done

- Rows changed or removed as a webhook trigger: Baserow's own pages show only the shape of a row added.
- Files (uploads are multipart), row comments and history, views, tables, fields and webhooks made through the API
  (a database token may not).

## Tested

Recorded exchanges built from Baserow's API description (`tests/`): what each request sends, answers of the documented
shape (two pages followed by Baserow's next address, a Baserow of one's own), refusals in Baserow's words, the poll,
and webhook deliveries with the right token, a wrong one, and another event. Not run against a live Baserow.

```sh
openduct app check apps/baserow --bar 80
openduct app test apps/baserow
```
