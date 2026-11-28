# Rocket.Chat

Messages, channels, private groups and direct messages in a [Rocket.Chat](https://www.rocket.chat) workspace, and its
people; on Rocket.Chat Cloud (`*.rocket.chat`) or your own server.

## Signing in

- **A personal access token (Rocket.Chat Cloud)** (`rocketchat`): your workspace's name (the `acme` of
  `acme.rocket.chat`), your user id and a token.
- **A personal access token (your own server)** (`rocketchat_self_hosted`): your server's address (such as
  `https://chat.example.com`), your user id and a token.

In Rocket.Chat, **avatar → My Account → Personal Access Tokens → Add**: it shows the token and your user id once.
They are sent as the `X-Auth-Token` and `X-User-Id` headers. A bot user's token posts as the bot. Listing and adding
people needs an admin's token. An admin may have to allow personal access tokens for the user's role.

## Actions

- **Send a message** (`send_message`, the default): to `#channel`, `@username` or a room id; with an alias, an emoji
  or attachments. **Reply in a thread** (`reply_in_thread`).
- **Change a message** (`update_message`), **Remove a message** (`delete_message`), **Get a message**
  (`get_message`), **Find messages in a room** (`search_messages`), **React to a message** (`react`).
- **A channel's messages** (`channel_history`), **A private group's messages** (`group_history`): the newest first,
  between two times.
- **List channels** (`list_channels`), **Get a channel** (`get_channel`), **Make a channel** (`create_channel`),
  **Make a private group** (`create_group`), **Add someone to a channel** (`invite_to_channel`), **Take someone out
  of a channel** (`remove_from_channel`), **Set a channel's topic** (`set_channel_topic`).
- **Open a direct conversation** (`open_direct_message`).
- **List people** (`list_users`), **Get a person** (`get_user`), **Add a person** (`create_user`), **Who I am**
  (`me`).

## Triggers

- **A new message in a channel** (`new_channel_message`) and **in a private group** (`new_group_message`): each new
  message, the newest asked for every few minutes. Rocket.Chat's outgoing webhooks carry their token in the body,
  not as a signature the app kit can check, so the triggers poll.

## Source

Written by hand from Rocket.Chat's own OpenAPI descriptions,
<https://github.com/RocketChat/Rocket.Chat-Open-API/tree/d286ec7f7b6222cd9bdef9c99e33e224106e7b05> (`messaging.yaml`,
`rooms.yaml`, `user-management.yaml`, `authentication.yaml`; OpenAPI 3.0, fetched 2026-11-27; the repository states no
licence, so the labels and descriptions here are our own words), and the reference at
<https://developer.rocket.chat/apidocs>. The descriptions sign in with header parameters rather than a security
scheme, so the app was not generated from them.

## Not done

- File uploads (multipart), omnichannel (live chat), teams, integrations, settings and statistics.
- Signing in with a user name and password (`/api/v1/login`, a session token that runs out): a personal access token
  is used instead.

## Tested

Recorded exchanges (`tests/`) made from the API descriptions and their example answers, with made-up values. Not run
against a live Rocket.Chat workspace.

```sh
openduct app check apps/rocketchat --bar 80
openduct app test apps/rocketchat
```
