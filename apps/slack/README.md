# Slack

Messages posted to [Slack](https://slack.com) channels.

## Signing in

Either an app's **bot token** (`xoxb-...`, with the `chat:write` scope, the app invited to the channel) in a Slack credential, or a channel's **incoming webhook** address in a Slack webhook credential.

## Actions

- **Post a message** (`send`): with a bot token, to a channel's id or `#name`; with a webhook, to its own channel.

## Tested

Recorded exchanges (`tests/`), and OpenDuct's own workflow and alert tests.
