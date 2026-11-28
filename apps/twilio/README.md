# Twilio

Text messages (SMS) sent and read through [Twilio](https://www.twilio.com).

## Signing in

In the Twilio Console's account dashboard, copy the **Account SID** and **Auth token** into a Twilio credential.

## Actions

- **Send a text message** (`send_sms`): to a number, from one of your Twilio numbers.
- **List messages** (`list_messages`): every page; optionally only those to one number.

## Triggers

- **A new message to a number** (`new_sms`), asked every minute: each text message sent to one of your numbers.

## Tested

Recorded exchanges from Twilio's documented requests and answers (`tests/`).

Tried on 2026-11-16: the test account's trial had run out, and Twilio's refusal was said as it gave it. Not otherwise run against a live account yet.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
