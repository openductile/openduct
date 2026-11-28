# Google Chat

Messages, thread replies and cards in [Google Chat](https://chat.google.com) spaces.

## Signing in

- **A webhook** (a Google Chat webhook credential): in the space, **Apps & integrations → Webhooks → Add webhook**;
  copy its address. It posts to that space.
- **A person's sign-in** (a Google Chat credential): the installation registers with Google once (as for Gmail: an
  OAuth client of the Google Cloud project with the Chat API turned on, the redirect URI shown under **Settings → App
  sign-ins**, and the scope `https://www.googleapis.com/auth/chat.messages.create`); then **Connect** under
  Credentials. It posts as that person to any space they are in (`space`: `spaces/AAAA`).

## Actions

- **Post a message** (`send`).
- **Reply in a thread** (`reply_in_thread`): by a thread key of your own (`order-A-100`: the first message with it
  starts the thread) or by the thread's name; a thread not found starts a new one
  (`messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`).
- **Post a card** (`send_card`): a card's JSON (`header`, `sections`, `widgets`) as `cardsV2`, with text above it, in a
  thread when a key is given. Google's guide for creating messages says a person's sign-in may send cards; its API
  reference speaks of text alone for that sign-in. Not tried live either way.

## Tested

Recorded exchanges (`tests/`) from Google's documented requests and answers, and OpenDuct's own workflow and alert
tests. None was run against a live space.
