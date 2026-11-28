# Pushover

Notifications pushed to people's phones, tablets, watches and desktops through [Pushover](https://pushover.net) -
an order paid, a form answered, a freezer too warm.

Written by hand from Pushover's API documentation: <https://pushover.net/api>, with
<https://pushover.net/api/receipts> and <https://pushover.net/api/glances> (fetched 2026-11-27). Labels and
descriptions are our own words.

## Signing in

- **An application token** (`pushover`): in Pushover, **Your Applications → Create an Application/API Token** gives
  the token; the user key (or a group's key) of who gets the notifications is on the dashboard. Pushover takes both
  with each request's parameters, as its documentation says, not in a header.

## Actions

- **Send a notification** (`send_notification`, the default): a message with a title, a link, a sound, a priority
  (from silent to emergency), HTML, an image (as Base64), to all of a person's devices or one, to the credential's
  user or group or another key (several with commas). An emergency notification repeats until someone acknowledges
  it, and gives a receipt.
- **Look up an emergency notification** (`get_receipt`), **Stop an emergency notification** (`cancel_emergency`),
  **Stop emergency notifications by tag** (`cancel_emergency_by_tag`).
- **Check a user or group key** (`check_user`): whether it is right, and which devices it has.
- **List sounds** (`list_sounds`).
- **Update a widget** (`update_glance`): the figures on a person's watch or home-screen widget.

Pushover lets an account send 10,000 notifications a month free.

## Not done

- Images as files (they need a multipart form; Base64 is taken instead).
- The callback Pushover makes when an emergency notification is acknowledged carries no signature, so it cannot
  start a workflow; Look up an emergency notification asks instead.
- Subscriptions, delivery groups, Teams and licensing.

## Tested

Recorded exchanges built from Pushover's documented requests and answers (`tests/`), with made-up keys. Not run
against a live Pushover account.

```sh
openduct app check apps/pushover
openduct app test apps/pushover
```
