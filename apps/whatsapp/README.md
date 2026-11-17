# WhatsApp Business

WhatsApp messages sent from a business number, through the [Cloud API](https://developers.facebook.com/docs/whatsapp/cloud-api).

## Signing in

In Meta's developer dashboard, your app's **WhatsApp → API Setup**: a permanent access token (a system user's), the phone number's id, and the Graph API version, in a WhatsApp credential.

## Actions

- **Send a message** (`send`): to a number with its country code. Outside the 24 hours after a customer writes, WhatsApp takes approved templates only.

## Tested

Recorded exchanges (`tests/`), and OpenDuct's own workflow and alert tests.
