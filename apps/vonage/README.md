# Vonage

Text messages, WhatsApp, Viber, Messenger and RCS messages, one-time codes, and phone numbers, with
[Vonage](https://www.vonage.com/communications-apis/).

## Source

Written by hand from Vonage's own API descriptions (OpenAPI 3), fetched on 2026-11-27 from its developer site
(`https://developer.vonage.com/api/v1/developer/api/file/<api>?format=json&vendorId=vonage`, the links on each API's
reference page):

- Messages API 1.22.0 (Apache 2.0), <https://developer.vonage.com/en/api/messages>
- Verify API 2.1.1 (no licence stated), <https://developer.vonage.com/en/api/verify.v2>
- Account API 1.0.4 (Apache 2.0), Numbers API 1.1.0 (MIT), Number Insight API 1.2.1 (MIT), Pricing API 1.3.0 (no
  licence stated)

The signed-webhook facts below are from <https://developer.vonage.com/en/getting-started/concepts/webhooks>. Labels
and descriptions are our own words.

## Signing in

**An API key and secret** (a Vonage credential): both are at the top of the [Vonage API
Dashboard](https://dashboard.nexmo.com). Every action here takes them (HTTP basic sign-in); the Messages API's
channels other than SMS also need a sender set up in the dashboard: a WhatsApp business number, a Viber service ID, a
Facebook page or an RCS agent.

## Actions

- **Send a text message (SMS)** (`send_sms`), **Send a picture message (MMS)** (`send_mms`, US and Canada).
- **Send a WhatsApp message** (`send_whatsapp`, within 24 hours of the customer's last message), **Send a WhatsApp
  template message** (`send_whatsapp_template`, any time), **Send a picture on WhatsApp** (`send_whatsapp_image`).
- **Send a Viber message** (`send_viber`), **Send a Facebook Messenger message** (`send_messenger`), **Send an RCS
  message** (`send_rcs`).
- **Send a one-time code** (`start_verification`: by text, WhatsApp, voice call or email), **Check a one-time code**
  (`check_code`), **Cancel a one-time code** (`cancel_verification`).
- **Look up a phone number** (`lookup_number`: basic, free; or standard, with its carrier, paid).
- **Get the account balance** (`get_balance`), **List your numbers** (`list_numbers`, every page), **Find numbers to
  buy** (`search_numbers`), **Get prices for a country** (`get_prices`).

A message action gives the message's ID (`message_uuid`); whether it was delivered comes later, to the status
address set on the Vonage application.

## Not done

- **Triggers (incoming messages, delivery statuses).** Vonage signs its webhooks with a JWT in the `Authorization`
  header, signed with the account's signature secret (the body's SHA-256 in its `payload_hash` claim). OpenDuct's
  webhook triggers check an HMAC of the body, or a token, not a JWT; this needs an engine change: a signature kind
  that verifies a JWT from a header and compares a claim with the body's hash.
- **Voice calls**, and marking WhatsApp messages read: those sign in only with a JWT of a Vonage application (the
  Voice API and the Messages API's update say so), which OpenDuct does not make (another engine change: a sign-in
  that signs a JWT).
- Other message kinds (audio, video, files, stickers, cards, carousels), message failover, buying and cancelling
  numbers, sub-accounts, reports.

## Tested

Recorded exchanges made from Vonage's API descriptions (`tests/`), with made-up data. Not run against a live Vonage
account.
