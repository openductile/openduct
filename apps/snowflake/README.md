# Snowflake

Run SQL in a [Snowflake](https://www.snowflake.com) account from a workflow, through Snowflake's SQL API (REST):
queries, inserts, updates and other statements, then follow or cancel a long one. Moving data in bulk between
Snowflake and other systems is the work of OpenDuct's Snowflake pipeline connector (`crates/openduct-connect`), which
is separate from this app.

Source: Snowflake's SQL API description,
<https://github.com/snowflakedb/snowflake-rest-api-specs/blob/990e25d97236a11826c9eed40e587c2b859e5680/specifications/sqlapi.yaml>
(OpenAPI 3.0, Apache 2.0), and the SQL API guide, <https://docs.snowflake.com/en/developer-guide/sql-api/index>
(fetched 2026-11-27). Started with `openduct app from-openapi`, then finished by hand.

## Signing in

- **A programmatic access token** (`snowflake`): in Snowsight, **Governance & security → Users & roles**, pick the
  user, then **Programmatic access tokens → Generate new token** (copy it at once: it is shown only then). Snowflake
  takes a token only from a user under a network policy, unless an admin's authentication policy says otherwise.
  Give the account identifier as it is in the account's address: `myorg-myaccount` for
  `https://myorg-myaccount.snowflakecomputing.com`. The token is sent as a bearer token, with
  `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`.

The statement runs with the token's user's rights, in the warehouse, database, schema and role given (or the user's
defaults).

## Actions

- **Run SQL** (`run_sql`): one statement - or several, with `statement_count` - with `?` placeholders filled from
  `bindings` (`{"1": {type: TEXT, value: ...}, "2": {type: FIXED, value: "42"}}`), so values are never pasted into
  the SQL. It gives `columns` (the column names), `rows` (each a list of values, as text, in the columns' order),
  `row_count`, `partitions` and `stats` (rows inserted, updated, deleted). Snowflake waits up to 45 seconds; a
  statement still running then - or one run with `run_in_background` - gives its `statement_handle` and no rows yet.
- **Check a statement** (`check_statement`): the same, for a handle; with `partition`, that part of a large result
  (0 is the first; for the parts after it Snowflake sends the rows only).
- **Cancel a statement** (`cancel_statement`).

## Not done

- Rows as records keyed by column name: the expression language cannot yet pair the column names with each row's
  values, so rows come as lists beside `columns`.
- Signing in with a key pair (a JWT signed with the user's private key) and with Snowflake OAuth (each account has its
  own sign-in address) need engine changes; a programmatic access token is what this app takes.
- The other Snowflake REST APIs (databases, warehouses, tables, tasks, Cortex) are not taken.

## Tested

Recorded exchanges (`tests/`): a query with bound values, an insert, a statement still running, a later partition, a
cancel, and a compilation error refused in Snowflake's words - made-up values, shaped as the SQL API description and
guide say. Not run against a live Snowflake account.

```sh
openduct app check apps/snowflake --bar 80
openduct app test apps/snowflake
```
