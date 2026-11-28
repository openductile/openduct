# PagerDuty

Incidents, who is on call, and alerts sent to a service, in [PagerDuty](https://www.pagerduty.com).

## Signing in

- **An API key** (`pagerduty`): an account-wide key from **Integrations → API Access Keys → Create New API Key**, or
  your own from **My Profile → User Settings → Create API User Token**. It is sent as
  `Authorization: Token token=<key>`. A read-only key can only find and read.
- **Sign in with PagerDuty** (`pagerduty_oauth`): a person signs in at PagerDuty. The install registers its own app
  once: in PagerDuty, **Integrations → App Registration → New App**, add **OAuth 2.0** (Classic User OAuth, with
  the `write` permission; or Scoped OAuth with the scopes your workflows need), give it the callback address shown
  under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`), and enter the client id and secret
  there.

Changes (opening, acknowledging, resolving, notes) say who makes them: their **From** input is a PagerDuty user's
login email.

## Actions

- **Find incidents** (`list_incidents`): of a status, urgency, service, team or user, between two dates; every page.
- **Get an incident** (`get_incident`).
- **Open an incident** (`create_incident`): on a service, with an urgency, details, a priority, an escalation policy
  or someone assigned.
- **Acknowledge, resolve or change an incident** (`update_incident`).
- **Snooze an incident** (`snooze_incident`), **Merge incidents** (`merge_incidents`).
- **An incident's notes** (`list_incident_notes`), **Add a note to an incident** (`add_incident_note`).
- **An incident's alerts** (`list_incident_alerts`), **An incident's timeline** (`list_incident_log`).
- **Post a status update on an incident** (`post_status_update`).
- **Send an alert to a service** (`send_alert`): trigger, acknowledge or resolve an alert through a service's Events
  API v2 integration key (`https://events.pagerduty.com/v2/enqueue`); the service's rules decide the incident.
- **List services** (`list_services`), **Get a service** (`get_service`).
- **List users** (`list_users`), **Get a user** (`get_user`), **Who I am** (`me`).
- **Who is on call** (`list_oncalls`), **List schedules** (`list_schedules`), **Who is on a schedule**
  (`list_schedule_users`), **List escalation policies** (`list_escalation_policies`).
- **List priorities** (`list_priorities`), **List teams** (`list_teams`).
- **Start a maintenance window** (`create_maintenance_window`).

## Triggers

- **An incident opened or changed** (`incident_changed`): each incident event PagerDuty sends (by default
  `incident.triggered`; or those named, such as `incident.acknowledged`, `incident.resolved`), for the whole account
  or one service. Turning the workflow on creates a webhook subscription (v3) for its address and keeps the secret
  PagerDuty makes for it; each delivery's `X-PagerDuty-Signature` (HMAC-SHA256 of the body) is checked, and a
  delivery whose event id was seen is not run twice. Turning the workflow off removes the subscription. Creating
  subscriptions needs a key or user allowed to manage webhooks.
- **A new incident (asked every few minutes)** (`new_incident`): the newest incidents, asked for every few minutes;
  for a key that may not create webhook subscriptions.

## Source

Facts (hosts, paths, fields, sign-ins) from PagerDuty's own API descriptions:
<https://github.com/PagerDuty/api-schema/blob/229d990fd0c8916df2254340088a3e559afc055e/reference/REST/openapiv3.json>
and `reference/events-v2/openapiv3.json` at the same commit (fetched 2026-11-27; the repository states no licence, so
the labels and descriptions here are our own words), and PagerDuty's developer pages on webhooks, signatures and
OAuth (<https://docs.pagerduty.com/developer>, 2026-11-27). The app was started with `openduct app from-openapi` and
finished by hand.

## Not done

- Lists in the query take one value each (one status, one service): PagerDuty's `statuses[]=a&statuses[]=b` with
  several is not sent.
- Responder requests, custom fields, incident workflows, change events, schedules' overrides and the other ~430
  operations of the API (a custom app made from the same description can add them).
- The EU service region (`api.eu.pagerduty.com`, `events.eu.pagerduty.com`) is not among the app's hosts.

## Tested

Recorded exchanges (`tests/`) made from the API descriptions and PagerDuty's documented requests and answers, with
made-up values; the webhook deliveries are signed as PagerDuty documents, with a tool of our own. Not run against a
live PagerDuty account.

```sh
openduct app check apps/pagerduty --bar 80
openduct app test apps/pagerduty
```
