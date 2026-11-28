# Notion

Pages of a [Notion](https://www.notion.so) database: read and added.

## Signing in

In Notion, **Settings → Connections → Develop or manage integrations → New integration** (internal). Copy its secret
into a Notion credential, then open each database it should reach and **⋯ → Connections → add** the integration.

## Actions

- **Find pages and databases** (`search`): by words in the title, with each one's id; every page.
- **Read a database's pages** (`query_database`): optionally those a filter holds for; every page.
- **Add a page to a database** (`add_page`): its properties in Notion's own form.

## Triggers

- **A new item in a database** (`new_item`) and **An item changed in a database** (`updated_item`), asked every 5
  minutes: the database must be shared with the integration.

## Tested

Recorded exchanges from Notion's documented requests and answers (`tests/`).

Run against a Notion workspace on 2026-11-16: Find pages and databases worked. The test integration was given no database, so the two database actions were not tried live.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
