# Supabase

Rows of a [Supabase](https://supabase.com) project's tables: added, found, changed and removed, through the
project's Data API.

## Signing in

- **The project's address**: in the Supabase dashboard, **Project Settings → Data API**, such as
  `https://abcdefgh.supabase.co` (or the address of a Supabase of your own).
- **A secret key**: **Project Settings → API Keys**, a secret key (`sb_secret_...`), or the older `service_role`
  key. Either reads and writes every table whatever its row-level security says, so keep it to workflows that need
  it. A secret key is sent in the `apikey` header; an older key in `Authorization` as well, as Supabase asks.

## Actions

- **Add a row** (`add_row`, the default): the row as each column and its value; gives the row as it was kept.
- **Find rows** (`list_rows`): rows with the values given (`match`), and other conditions (`where`, as
  `total.gt.100,status.neq.closed`); some columns, an order, at most some rows. Every row when no condition is
  given, 1,000 a page, up to 10,000.
- **Change rows** (`update_rows`): the rows with the values given get new ones; gives them as they are now.
- **Remove rows** (`delete_rows`): removes the rows with the values given; gives what was removed.

Conditions are sent as one: `and=(email.eq."ann@example.com",total.gt.100)`. A value is quoted, so a comma or a
bracket in it is part of it; a value that came empty asks for rows where it is empty (`is.null`). Changing and
removing always need a condition: an empty `match` is sent as `()`, which Supabase refuses, never as every row.

## Tested

Recorded exchanges built from Supabase's and PostgREST's documentation (`tests/`): both kinds of key, conditions,
quoting, an empty value, paging.

Not run against a live Supabase project.
