# WhatsApp Business

WhatsApp messages sent from a business number, through the [Cloud API](https://developers.facebook.com/docs/whatsapp/cloud-api).

## Signing in

In Meta's developer dashboard, your app's **WhatsApp → API Setup**: a permanent access token (a system user's), the
phone number's id, and the Graph API version, in a WhatsApp credential.

## Actions

- **Send a message** (`send`): text, to a number with its country code. Outside the 24 hours after a customer writes,
  WhatsApp takes approved templates only.
- **Send a template message** (`send_template`): an approved template by its name and language, its body's
  placeholders filled in order (`values`) - the only kind that may start a conversation.
- **Send a picture, document, video or sound** (`send_media`): by its address (`link`) or an uploaded media's id,
  with a caption (not for audio) and a document's file name.
- **Send a message with reply buttons** (`send_buttons`): up to three, as titles or `{id, title}` records.
- **Send a message with a list to choose from** (`send_list`): a button that opens sections of rows (up to ten).
- **Mark a message as read** (`mark_read`): by its id, from the trigger.

A person's choice of a button or a row comes back to the trigger as a message of type `interactive`, with the id.

## Triggers

- **A new message to the business number** (`new_message`): each message sent to the number, as Meta sends it,
  signed with the app's secret; each runs by itself, with `from` and the sender's `name`. In the Meta app's WhatsApp
  **Configuration**, give the workflow's address as the callback URL and the credential's verify token (Meta checks
  the address with them), and subscribe to `messages`. The credential needs the app's secret (App settings → Basic).
  A delivery's first number's messages are read (Meta sends one number's at a time).

## Tested

Recorded exchanges (`tests/`) from Meta's documented requests and answers, and OpenDuct's own workflow and alert
tests. Triggers are tested with recorded exchanges and signed deliveries, signatures made as Meta documents them.
None was run against a live account.
