# Notion

Pages of a [Notion](https://www.notion.so) database: read and added.

## Signing in

In Notion, **Settings → Connections → Develop or manage integrations → New integration** (internal). Copy its secret
into a Notion credential, then open each database it should reach and **⋯ → Connections → add** the integration.

## Actions

- **Find pages and databases** (`search`): by words in the title, with each one's id; every page.
- **Read a database's pages** (`query_database`): optionally those a filter holds for; every page.
- **Add a page to a database** (`add_page`): its properties in Notion's own form.

## Tested

Recorded exchanges from Notion's documented requests and answers (`tests/`).

Run against a Notion workspace on 2026-11-16: Find pages and databases worked. The test integration was given no database, so the two database actions were not tried live.
