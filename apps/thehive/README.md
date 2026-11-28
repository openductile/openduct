# TheHive

Security alerts and cases in [TheHive](https://strangebee.com/thehive/), StrangeBee's security incident response
platform: raised, turned into cases, and worked through with tasks, observables and comments.

It works with TheHive 5 and later, through their `v1` API: TheHive's own description says the older `v0` API is
deprecated and undocumented, and that new integrations should use `v1`.

## Source

Written by hand from StrangeBee's own API description of TheHive, <https://docs.strangebee.com/thehive/api-docs/docs.yaml>
(OpenAPI 3.1, "TheHive v6.0.0", fetched 2026-11-27; no licence stated), the description its API reference page
<https://docs.strangebee.com/thehive/api-docs/> shows. Only `/api/v1` endpoints are used. Labels and descriptions are
our own words.

## Signing in

**An API key** (a TheHive credential): a normal user manages their API key in their user settings; a key can also be
made for an account through TheHive's API (`POST /api/v1/user/{userId}/key/renew`). Give it with TheHive's address,
such as `https://thehive.example.com`, and, for a user in several organizations, the organization to work in (sent as
`X-Organisation`). A request acts as that user, with their profile's permissions. On the same machine or network as
OpenDuct, start OpenDuct with `--allow-private-network`.

## Actions

- **Raise an alert** (`create_alert`, with its observables), **Get an alert** (`get_alert`), **List alerts**
  (`list_alerts`), **Change an alert** (`update_alert`), **Make a case of an alert** (`create_case_from_alert`), **Add
  an alert to a case** (`merge_alert`), **Comment on an alert** (`comment_on_alert`), **Delete an alert**
  (`delete_alert`).
- **Open a case** (`create_case`), **Get a case** (`get_case`, by ID or number), **List cases** (`list_cases`),
  **Change a case** (`update_case`), **Delete a case** (`delete_case`), **Comment on a case** (`comment_on_case`).
- **Add an observable to a case** (`add_observable`) or **to an alert** (`add_observable_to_alert`), **List a case's
  observables** (`list_case_observables`).
- **Add a task to a case** (`create_task`), **Change a task** (`update_task`), **List a case's tasks**
  (`list_case_tasks`), **Log work on a task** (`add_task_log`).
- **Get who the key belongs to** (`get_current_user`).

IDs are TheHive's, with their `~` (`~8192`); a case may also be named by its number. Lists go through TheHive's query
API, newest first, up to a number (100 by default).

## Triggers

- **A new alert** (`new_alert`), **A new case** (`new_case`) and **A case changed** (`updated_case`), asked every 5
  minutes: the newest hundred each time.

## Not done

- Lists beyond one page (TheHive's query pages are a step inside the query, which a next page cannot change yet).
- TheHive's webhooks: they are set up in TheHive's own configuration, and its API description documents no
  signature on their deliveries, so the triggers poll instead.
- File observables and attachments (multipart), case templates, pages, procedures (TTPs), sharing between
  organizations, users and organizations, Cortex analyzers and responders.

## Tested

Recorded exchanges made from TheHive's API description (`tests/`), with made-up data. Not run against a live TheHive.

Triggers are tested with recorded exchanges (`tests/trigger_*.yaml`); none was run against a live account.
