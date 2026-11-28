# Apify

Web scrapers and other Actors run on [Apify](https://apify.com), and the records they gathered: shops' prices,
maps listings, social posts, whole websites as text.

## Signing in

An **API token**: in the Apify Console, **Settings → API & Integrations**, copy a personal API token (or make one
with fewer permissions). It is sent as `Authorization: Bearer`, never in an address.

## Actions

- **Run an Actor and get its records** (`run_and_get_items`, the default): runs the Actor with its input, waits for it
  to finish and gives the records. Apify waits at most 5 minutes, and so does this step; a longer run is refused
  (408) - start it with Run an Actor instead. `limit` caps the records, `max_total_charge_usd` what the run may
  spend.
- **Run an Actor** (`run_actor`): starts a run and gives it (`id`, `status`, `defaultDatasetId`) without waiting, or
  waiting up to 60 seconds.
- **Run a saved task** (`run_task`): the same for one of the account's tasks, with changes to its input.
- **Look up a run** (`get_run`): its status, waiting up to 60 seconds for it to finish.
- **Get the records of a dataset** (`get_items`): a run's records, 1,000 a page, every page up to 10,000.
- **Get a record of a key-value store** (`get_record`): such as a run's `OUTPUT`.

An Actor is named as Apify shows it, `apify/web-scraper`, or by its id. Each run spends the account's credit, so
the run actions are writes: a preview shows them and does not send them.

## Tested

Recorded exchanges built from Apify's API reference (`tests/`): every action, paging by the
`X-Apify-Pagination-*` headers, and a run refused for running longer than Apify waits.

Not run against a live Apify account.
