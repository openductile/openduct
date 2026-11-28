# Telegram

Messages, photos and documents sent through a [Telegram](https://telegram.org) bot, with buttons under them, and the
messages and button presses people send back.

## Signing in

Talk to **@BotFather** in Telegram, `/newbot`, and copy the bot's token into a Telegram credential. Add the bot to the
chat (or channel) it writes to.

## Actions

- **Send a message** (`send`): to a chat's id or `@channelname`, optionally marked up as MarkdownV2 or HTML, with
  buttons under it (`buttons`: rows of `{text, callback_data}` or `{text, url}` - an inline keyboard).
- **Send a photo** (`send_photo`) and **Send a document** (`send_document`): by an address on the internet or a
  `file_id` Telegram already has, with a caption and buttons.
- **Edit a message** (`edit_message`): its text and buttons (an empty list takes them away).
- **Answer a button press** (`answer_button`): Telegram waits for it after each press - a notice, or an alert.

## Triggers

- **A new message to the bot** (`new_message`, asked every minute): each message people send the bot.
- **A new message to the bot, as it comes** (`message`): Telegram sends each one to the workflow's address. Turning
  the workflow on gives Telegram the address and a secret it sends back with each delivery (checked); turning it off
  takes the address away.
- **A message or a button pressed, as it comes** (`update`): the same, with button presses too: a run gets `kind`
  (`message` or `button`), `chat_id`, `message_id`, `text` (a message's), `data` and `callback_query_id` (a press's,
  for **Answer a button press**), `from` and the `message`.

A bot is asked for its messages or sent them, and has one address: one of these triggers at a time per bot (while a
webhook one is on, Telegram refuses the polled one).

## Tested

Recorded exchanges (`tests/`), and OpenDuct's own workflow and alert tests. Triggers are tested with recorded
exchanges and deliveries carrying the secret as Telegram documents it. None was run against a live bot.
