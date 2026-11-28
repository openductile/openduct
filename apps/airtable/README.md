# Airtable

Records of an [Airtable](https://airtable.com) base: read, added and changed.

## Signing in

In Airtable, **Builder hub → Personal access tokens → Create token**, with the scopes `data.records:read`,
`data.records:write` and `schema.bases:read`, and the bases it may reach. Put it in an Airtable credential. A base's id (`app...`) is in its
address in the browser.

## Actions

- **List bases** (`list_bases`) and **a base's tables** (`list_tables`): their ids and fields, for the others.
- **List records** (`list_records`): optionally a view's, or those a formula holds for; every page.
- **Add a record** (`add_record`): its fields by name.
- **Change a record** (`update_record`).

Field values are converted to the field's type (`typecast`), so text like `2026-11-20` fills a date field.

## Triggers

- **A new record** (`new_record`), asked every 5 minutes: records made since the poll before (with five minutes to
  spare for clocks that differ), in a table of a base. The token needs data.records:read.

## Tested

Recorded exchanges from Airtable's documented requests and answers (`tests/`).

Run against an Airtable test base on 2026-11-16: List bases, a base's tables, List records, Add a record and Change a record worked.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
