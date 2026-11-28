# Matrix

Messages and rooms on [Matrix](https://matrix.org), the open network for chat - on matrix.org or any homeserver
(Synapse, Dendrite, Conduit, or a hosted one): post to a room, make and join rooms, invite and remove people, read a
room's messages, and start a workflow when a message is posted.

Source: the Matrix Client-Server API description, <https://spec.matrix.org/v1.19/client-server-api/api.json>
(v1.19, built from <https://github.com/matrix-org/matrix-spec>, Apache 2.0; fetched 2026-11-27). Started with
`openduct app from-openapi`, then finished by hand in our own words.

## Signing in

- **An access token** (`matrix`): the token of the account the workflow acts as - a bot account is best. In Element,
  **All settings → Help & About → Access token**; or the one a login gives. Give the homeserver's address beside it,
  such as `https://matrix.example.org` (left empty, matrix.org's own: `https://matrix-client.matrix.org`). The token
  is sent as a bearer token. A homeserver on the machine's own network is reached only when OpenDuct is started
  with `--allow-private-network`.

## Actions

- **Send a message** (`send_message`): text, with HTML formatting if given, to a room by its id
  (`!abc123:example.org`); `m.notice` for a bot's notices. Matrix needs a **transaction id** for each message, used
  once only (sending again with the same one posts nothing new): give the run's or the event's id.
- **Send an event** (`send_event`): any room event with its content - a reaction, an event type of your own.
- **Remove a message** (`remove_message`).
- **Make a room** (`create_room`), **Join a room** (`join_room`, by id or `#address`), **Leave a room**
  (`leave_room`), **Rename a room** (`set_room_name`), **Change a room's topic** (`set_room_topic`).
- **Invite someone to a room** (`invite_user`), **Remove someone from a room** (`remove_user`), **Ban someone from a
  room** (`ban_user`), **Let someone back into a room** (`unban_user`).
- **List my rooms** (`list_joined_rooms`), **Read a room's messages** (`get_messages`, newest first, with an `end`
  token to read on), **List a room's members** (`list_members`), **Look up a message** (`get_event`), **Find a room by
  its address** (`find_room`), **List public rooms** (`list_public_rooms`), **Someone's profile** (`get_profile`),
  **Who I am** (`who_am_i`).

## Triggers

- **A new message in a room** (`new_message`): the room's newest 50 messages are asked for every minute; each not
  seen before starts a run, oldest first, with the event (`sender`, `content.body`, `origin_server_ts`). The
  account's own messages are among them: a workflow that answers should skip its own `sender`.

## Not done

- End-to-end encrypted rooms: the app sends and reads in clear, so in an encrypted room its messages are refused by
  the members' apps and the trigger sees only encrypted events.
- A fresh transaction id made by the app itself: the expression language has no source of unique values yet, so the
  workflow gives one.
- Uploading files (the media repository), syncing, spaces, threads, receipts, typing, push rules, account and
  device management, login.

## Tested

Recorded exchanges (`tests/`) for every action, a refusal in the homeserver's words, and the trigger (a later poll
and the first) - made-up values on `example.org`, in the shapes the specification gives. Not run against a live
homeserver.

```sh
openduct app check apps/matrix --bar 80
openduct app test apps/matrix
```
