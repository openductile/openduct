# Onfleet

Delivery tasks, drivers, teams and recipients in [Onfleet](https://onfleet.com).

## Source

Written by hand from Onfleet's API reference (v2), <https://docs.onfleet.com/reference/introduction>, read as markdown
on 2026-11-27 (the pages for tasks, workers, teams, recipients, organization, webhooks, webhook validation and
webhook authentication). Onfleet publishes no machine-readable description and states no licence for its reference:
labels and descriptions here are our own words.

## Signing in

**An API key** (an Onfleet credential): in the Onfleet dashboard, **Configuration → API & Webhook**, make an API key.
Onfleet takes the key as the user name of HTTP basic sign-in, with no password.

## Actions

- **Add a delivery task** (`create_task`): at an address in one line, for a recipient (name, phone, notes), with a
  time window (times as `2026-12-01T15:00:00Z`; Onfleet's milliseconds are worked out), given to a team, a driver, or
  the nearest driver on duty.
- **Get a task** (`get_task`), **Get a task by its short ID** (`get_task_by_short_id`), **List tasks** (`list_tasks`,
  since a time, every page), **Change a task** (`update_task`), **Mark a task done** (`complete_task`, delivered or
  failed), **Copy a task** (`clone_task`), **Delete a task** (`delete_task`).
- **List drivers** (`list_workers`), **Get a driver** (`get_worker`), **List teams** (`list_teams`).
- **Add a recipient** (`create_recipient`), **Find a recipient** (`find_recipient`, by exact phone or name),
  **Change a recipient** (`update_recipient`).
- **Get the organization's details** (`get_organization`).

A task's `state` is 0 waiting, 1 assigned, 2 under way, 3 done; its times are milliseconds since 1970.

## Triggers

- **A new task** (`new_task`) and **A task done** (`task_done`), asked every 5 minutes: Onfleet is asked for the
  tasks made (or completed) in the week before each poll, and each runs once.

## Not done

- **Onfleet's webhooks.** Onfleet signs each delivery with HMAC-SHA512 of the body, keyed with the webhook secret read
  as hexadecimal (`X-Onfleet-Signature`), and checks a new webhook's address with a GET whose `check` value must be
  answered back. OpenDuct's webhook triggers check HMAC-SHA256 or HMAC-SHA1 with the secret as text, and answer a GET
  only when its token equals a credential field: this needs an engine change (SHA-512, a key given in hex, and a GET
  check that answers its own value). The triggers poll instead.
- A recipient's name with a `/` in it (it is put in the address as it is).
- Orders, route plans and route optimization, task templates, custom fields, dependencies, barcodes, destinations on
  their own, hubs, and adding or changing drivers and teams.

## Tested

Recorded exchanges from Onfleet's documented requests and answers (`tests/`), with made-up data. Not run against a
live Onfleet account.

Triggers are tested with recorded exchanges (`tests/trigger_*.yaml`); none was run against a live account.
