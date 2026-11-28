# Discord

Messages, threads, reactions and roles in a [Discord](https://discord.com) server through a bot; slash commands and
buttons as people use them; and a channel's webhook.

## Signing in

- **A bot** (a Discord bot credential): in the [Developer Portal](https://discord.com/developers/applications), your
  application's **Bot** page gives the bot's token. Invite the bot to the server with the permissions it needs (Send
  Messages, Add Reactions, Create Public Threads, Manage Roles for roles). For slash commands and buttons, copy the
  **Public Key** from **General Information** into the credential too.
- **A webhook** (a Discord webhook credential): in a channel's settings, **Integrations → Webhooks → New Webhook**; copy
  its address. It posts to its own channel, and does nothing else.

## Actions

- **Post a message** (`send`): with a webhook to its channel, under a name of your choice; with a bot to `channel_id`.
- **Send a message to a channel** (`send_message`): with embeds and buttons (Discord's components) when given.
- **Reply to a message** (`reply`), **Edit a message** (`edit_message`), **React to a message** (`react`: an emoji
  such as 👍, or a server's own as `name:id`).
- **Start a thread from a message** (`create_thread`) and **Start a thread** (`start_thread`, a public one).
- **List a server's channels** (`list_channels`; Discord leaves threads out).
- **Give a member a role** (`add_role`) and **Take a role from a member** (`remove_role`).
- **Answer a slash command or a button** (`answer_interaction`): sets the reply the trigger began, within 15 minutes;
  **Send a follow-up** (`follow_up`), `private` for the person alone.

## Triggers

- **A slash command or a button used** (`interaction`): Discord sends each one to the workflow's address, signed
  with Ed25519 (the `X-Signature-Ed25519` and `X-Signature-Timestamp` headers, over the time and the body), checked
  with the credential's public key; an unsigned one, a forged one and one signed more than five minutes ago are
  refused with 401. Discord's check of the address (a PING) is answered with a PONG. Within its three seconds Discord
  is answered that the bot is thinking (for a button, that the message is being updated), and the workflow answers
  with **Answer a slash command or a button**. `command` keeps one slash command's runs; `private` makes the answer
  the person's alone. A run gets `kind` (command, button or form), `command`, `options` and `option` (the first
  option's value), `custom_id`, `user`, `guild_id`, `channel_id`, `token`, `application_id` and the whole
  `interaction`.

  Setting it up: turn the workflow on, then in the Developer Portal's **General Information** give its address as the
  **Interactions Endpoint URL** (Discord checks it then). Register the slash commands there or with Discord's API.

## Tested

Recorded exchanges (`tests/`) from Discord's documented requests and answers, and OpenDuct's own workflow and alert
tests. Interactions are tested with deliveries signed with a key made for the tests, as Discord documents the
signature (with Node's own Ed25519, not OpenDuct's). None was run against a live account.
