# Mattermost

Messages in a [Mattermost](https://mattermost.com) server's channels: post, reply, change, pin, react and search; find
teams, channels and people; open a direct message; and start a workflow for each new message in a channel.

Written by hand from Mattermost's API v4 description, `api/v4/source` (`posts.yaml`, `channels.yaml`, `users.yaml`,
`teams.yaml`, `reactions.yaml`, `introduction.yaml`) of <https://github.com/mattermost/mattermost> at commit
`2af9349f1333` (fetched 2026-11-27), and the incoming webhook page,
<https://docs.mattermost.com/developers/integrate/webhooks/incoming>. Mattermost's documentation is under CC BY-NC-SA
3.0, not an open licence for this use: only its facts are used, in our own words. The API reference:
<https://developers.mattermost.com/api-documentation/>.

## Signing in

Every Mattermost is its owner's: requests go to the address the credential gives. Two ways:

- **A personal access token (or a bot's)** (a Mattermost credential): the server's address (`https://chat.example.com`)
  and a token - in Mattermost, **Profile → Security → Personal Access Tokens** (an admin may need to allow them), or a
  bot account's token under **Integrations → Bot Accounts**. Sent as a bearer token; it acts as its owner.
- **A Mattermost incoming webhook** (a Mattermost webhook credential): the webhook's address, made under
  **Integrations → Incoming Webhooks**. It only posts messages - to its own channel, or another by name.

## Actions

- **Post a message** (`post_message`, the default): Markdown, in a channel or as a reply in a thread; with a webhook,
  to its channel (or another, or `@someone`), under another name and picture where the server allows it.
- **Look up a post** (`get_post`), **Change a post** (`update_post`), **Remove a post** (`delete_post`), **Pin a post**
  (`pin_post`), **React to a post** (`add_reaction`).
- **List a channel's posts** (`list_posts`: newest first, every page), **Read a thread** (`get_thread`), **Search
  posts** (`search_posts`: Mattermost's search, with `from:`, `in:`, `on:`, `before:`, `after:`).
- **Look up a channel** (`get_channel`), **Find a channel by name** (`find_channel`), **List a team's public
  channels** (`list_channels`), **List a user's channels in a team** (`list_my_channels`), **Create a channel**
  (`create_channel`), **Add people to a channel** (`add_to_channel`), **Open a direct message channel**
  (`open_direct_channel`).
- **Find a user by email** (`get_user_by_email`) or **by user name** (`get_user_by_username`), **Look up a user**
  (`get_user`: `me` for the token's owner), **Search users** (`search_users`).
- **Find a team by name** (`get_team_by_name`), **List a user's teams** (`list_teams`).

Posts come in Mattermost's own order (it gives them by id, and the order apart). A refusal fails the step in
Mattermost's words, with its message id.

## Triggers

- **A new message in a channel** (`new_post`): the channel's newest posts asked for every minute; replies too.

## Not done

- Mattermost's outgoing webhooks: they carry their token in the body, where OpenDuct's webhook triggers look for it in
  a header; the poll above stands in.
- Files and uploads, slash commands, interactive messages' actions, playbooks, and the server's administration.

## Tested

Recorded exchanges built from the description (`tests/`): each action, a reply, a message through a webhook (which
answers `ok` as plain text), a refusal in Mattermost's words, and the trigger. Not run against a live Mattermost.

```sh
openduct app check apps/mattermost --bar 80
openduct app test apps/mattermost
```
