# Microsoft Teams

Messages and Adaptive Cards in [Microsoft Teams](https://www.microsoft.com/microsoft-teams) channels and chats,
through [Microsoft Graph](https://learn.microsoft.com/graph/api/resources/teams-api-overview); new messages as they are
posted; and a channel's incoming webhook. Teams has an app of its own, apart from Microsoft 365, so a workspace that
only posts to Teams signs in with Teams' permissions alone.

## Registering this installation with Microsoft (once, by its admin)

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), **Applications → App registrations → New
   registration** (or the registration Microsoft 365 uses), with a **Web** redirect URI - the one shown in OpenDuct
   under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`). Teams' messages are work and school
   accounts' only.
2. **API permissions → Add → Microsoft Graph → Delegated**: `offline_access`, `ChannelMessage.Send`,
   `ChatMessage.Send`, `ChannelMessage.Read.All` (an admin consents to it), `Chat.Read`, `Team.ReadBasic.All`,
   `Channel.ReadBasic.All`.
3. **Certificates & secrets → New client secret**; copy the **Application (client) ID** and the secret's **Value**
   into **Settings → App sign-ins → Microsoft Teams**.

## Connecting an account

Under Credentials, add a Microsoft Teams credential and **Connect**. Or, to only post to one channel, add a Teams
webhook credential with the channel's incoming webhook address (Workflows, or the channel's connectors).

## Actions

- **Post a message** (`send`): with a webhook to its channel; signed in, to `team_id` and `channel_id`.
- **Send a message to a channel** (`send_channel_message`), **Send a message to a chat** (`send_chat_message`, a chat
  that exists), **Reply in a channel's thread** (`reply_in_thread`): as text, or HTML with `format: html`.
- **Post an Adaptive Card** (`post_card`): the card's JSON (a record, or its text), to a channel - or to a chat when
  `chat_id` is given; Graph takes it as an attachment the message names.
- **List teams** (`list_teams`, the person's) and **List channels** (`list_channels`, a team's), every page.
- **Read a message** (`read_message`): a channel's message, a reply in its thread (`reply_id`) or a chat's message,
  in full - what the trigger's runs read.

## Triggers

- **A new message in a channel or a chat** (`new_message`): give `team_id` and `channel_id`, or `chat_id`. Turning
  the workflow on subscribes its address with Graph (a change notification subscription for created messages, without
  their content); Graph first checks the address with a validation token, answered as plain text. Each notification
  carries the secret the subscription was given (`clientState`): one without it, or with another, is refused with 401.
  Subscriptions are made for 55 minutes (so Graph needs no lifecycle address) and renewed every 20, once in a
  cluster; one Graph has let go is made again at the same address. Turning the workflow off deletes it. A run gets
  `kind` (channel or chat), `team_id`, `channel_id`, `chat_id`, `message_id`, `reply_id` (a reply in a thread) and
  `resource`: read the message itself with **Read a message**.

## Tested

Recorded exchanges (`tests/`) from Microsoft's documented requests and answers: sending, replying, cards, listing,
reading, and the subscription's creation, renewal (and creation again when it is gone), deletion, validation and
notifications, with made-up ids. None was run against a live tenant.
