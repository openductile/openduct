# UptimeRobot

Watch websites, servers and APIs with [UptimeRobot](https://uptimerobot.com): monitors, their incidents, maintenance
windows and status pages.

## Signing in

- **An API key** (`uptimerobot`): in UptimeRobot, **Integrations & API → API → Main API key** (create it if there is
  none). It is sent as `Authorization: Bearer <key>` to the API v3. A read-only key can list and read, not change.

## Actions

- **List monitors** (`list_monitors`): of a status, name, address, tag or group; every page.
- **Get a monitor** (`get_monitor`).
- **Watch an address** (`create_monitor`): a web address (HTTP), a word on a page (KEYWORD), a server (PING, PORT) or
  a heartbeat; every 5 minutes unless told, with tags and an alert contact.
- **Change a monitor** (`update_monitor`), **Remove a monitor** (`delete_monitor`), **Pause a monitor**
  (`pause_monitor`), **Start a monitor** (`start_monitor`).
- **A monitor's uptime** (`monitor_uptime`), **A monitor's response times** (`monitor_response_times`).
- **List incidents** (`list_incidents`): of a monitor, open ones, between dates; **Get an incident**
  (`get_incident`); **Comment on an incident** (`comment_incident`), on the status page too if asked.
- **List maintenance windows** (`list_maintenance_windows`), **Plan a maintenance window**
  (`create_maintenance_window`), **Remove a maintenance window** (`delete_maintenance_window`).
- **List status pages** (`list_status_pages`), **Post an announcement on a status page** (`post_announcement`).
- **List alert contacts** (`list_alert_contacts`), **List monitor groups** (`list_monitor_groups`), **My account**
  (`me`).

## Triggers

- **A monitor went down** (`monitor_down`): each new downtime (a monitor down with an incident not seen before),
  asked for every few minutes.
- **A monitor's status changed** (`monitor_status_changed`): each monitor whose status is not the one seen before
  (down, back up, paused, started).

UptimeRobot's webhook alert contacts carry no signature and a body each account writes itself, so the triggers
poll.

## Source

Facts (host, paths, fields, sign-in) from UptimeRobot's own API v3 description,
<https://cdn.uptimerobot.com/api/openapi.yaml> (OpenAPI 3.0, last modified 2026-11-27, fetched that day; no licence
stated, so the labels and descriptions here are our own words), and the reference at <https://uptimerobot.com/api/v3/>.
Written by hand after `openduct app from-openapi` showed the operations.

## Not done

- DNS, API, UDP, visual-comparison and third-party-service monitors (their settings are records of their own), bulk
  pausing and starting, storm protection, integrations and personal alert contacts, status pages' settings, tags.
- The older API v2 (form posts with the key in the body), which v3 replaces.

## Tested

Recorded exchanges (`tests/`) made from the API description, with made-up values. Not run against a live
UptimeRobot account.

```sh
openduct app check apps/uptimerobot --bar 80
openduct app test apps/uptimerobot
```
