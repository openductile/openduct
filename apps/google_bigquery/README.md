# Google BigQuery

Questions asked of [Google BigQuery](https://cloud.google.com/bigquery) in SQL, rows added to its tables, and its
projects, datasets and tables listed - such as yesterday's sales read each morning for a report, or each new order
from the shop added to a table.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **BigQuery API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/bigquery`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google BigQuery**. One OAuth client
   serves every Google app of this installation: give it each app's scope, and the same ID and secret to each.

## Connecting an account

Under Credentials, add a Google BigQuery credential and **Connect**, as a person who may run queries in the project
(BigQuery Job User) and read - or, to add rows, change - its data (BigQuery Data Viewer or Data Editor). Queries are
billed to the project named in each step.

## Actions

- **Run a query** (`run_query`, the default): GoogleSQL, waiting up to about 3 minutes. Each row is a record of the
  columns' names - `{order_id: "A-1001", total: 120.5, paid: true, tags: ["gift"]}` - numbers and true/false as such,
  times as ISO 8601 text, a column of several values as a list. Up to 10,000 rows (or `max_rows`); a
  `max_bytes_billed` refuses a query that would bill more. `INSERT`, `UPDATE`, `DELETE` and `MERGE` run too.
- **Get a query's rows** (`get_query_results`): the rows of a query run before, by its job's id - one still running
  when Run a query stopped waiting (the step says its job), or every page of a large answer.
- **Add rows to a table** (`add_rows`): one record, or a list of them, streamed in by the columns' names.
- **List projects** (`list_projects`), **List datasets** (`list_datasets`), **List tables** (`list_tables`), **Get a
  table** (`get_table`: its columns, rows and size).

A column that is itself a record (a `STRUCT`) comes empty: name its fields in the query
(`SELECT ship_to.city AS city`). A result has at most 60 columns.

## Triggers

- **A new row in a query's answer** (`new_row`): the query is run every 15 minutes, and each row it gives that it did
  not give before - told apart by the column named (`order_id`) - starts a run. Write it newest first and limited
  (`ORDER BY created_at DESC LIMIT 500`): each run of it is billed as BigQuery bills queries.

## Not done

- Signing in with a Google service account's key (the way servers usually reach BigQuery): this app signs in as a
  person.
- Making and changing datasets and tables, loading and exporting files, copying tables, query parameters, legacy SQL,
  and the BigQuery Storage APIs.

## Source

Written by hand from Google's API Discovery document for BigQuery,
`https://bigquery.googleapis.com/$discovery/rest?version=v2`, fetched on 2026-11-27 (revision 20261117; Google's
documentation, CC BY 4.0; the API under the Google APIs Terms of Service), with the
[BigQuery API reference](https://cloud.google.com/bigquery/docs/reference/rest). Descriptions are our own words.

## Tested

Recorded exchanges from Google's documented requests and answers (`tests/`), with made-up projects and rows. Not run
against a live BigQuery project.
