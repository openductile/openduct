# Slack

Messages, threads, reactions, files and direct messages in [Slack](https://slack.com), and new messages as they are
posted.

## Signing in

Either an app's **bot token** (`xoxb-...`, the app invited to the channel) in a Slack credential, or a channel's
**incoming webhook** address in a Slack webhook credential (it posts to its own channel, and does nothing else). The
bot token needs the scopes of what it does: `chat:write` (send, reply, update, delete, schedule), `reactions:write`,
`files:write`, `im:write` (open a direct message), `channels:read` and `groups:read` (list channels),
`users:read.email` (find a person by email), `channels:history` (the polled trigger).

## Actions

- **Post a message** (`send`): with a bot token to a channel's id or `#name`; with a webhook to its own; Block Kit
  `blocks` when given.
- **Reply in a thread** (`reply`): under a message's `ts`; `also_in_channel` shows it in the channel too.
- **Update a message** (`update`) and **Delete a message** (`delete`), by channel and `ts`.
- **React to a message** (`react`): an emoji's name, `thumbsup` or `:thumbsup:`.
- **Upload a file** (`upload_file`): a run's file (from an earlier step) - or text given as `content` - shared in a
  channel, or a thread, with a title and a comment. Slack's upload takes three requests, made one after the other:
  an upload address asked for (`files.getUploadURLExternal`), the bytes sent there, the upload finished
  (`files.completeUploadExternal`).
- **Open a direct message** (`open_dm`): with one person or a few, its channel id to send to.
- **List channels** (`list_channels`): every page; public ones by default, `types` for more.
- **Find a person by email** (`find_user_by_email`).
- **Schedule a message** (`schedule`): at a time such as `2026-12-01T09:00:00Z`, up to 120 days ahead.

## Triggers

- **A new message in a channel** (`new_message`, asked every minute): each message in a channel the bot is in
  (`channel`, its id).
- **A message posted, as it comes** (`message`): through Slack's Events API. In the Slack app's settings, **Event
  Subscriptions**: turn them on, give the workflow's address as the Request URL (Slack checks it, and is answered),
  and subscribe to the bot event `message.channels`. The credential needs the app's **Signing Secret** (Basic
  Information): each delivery is checked against it, and one signed more than five minutes ago is refused.

## Tested

Recorded exchanges (`tests/`) from Slack's documented requests and answers - the upload's three requests included -
and OpenDuct's own workflow and alert tests. Triggers are tested with recorded exchanges and signed deliveries,
signatures made as Slack documents them. None was run against a live workspace.
