# Mailjet

Emails sent, contacts and lists, campaigns, templates, and what happened to each message, in
[Mailjet](https://www.mailjet.com).

## Source

Written by hand from Mailjet's own API description, <https://dev.mailjet.com/_spec/openapi/openapi-mailjet.json>
(OpenAPI 3.1, "Mailjet API 1.0.0", fetched 2026-11-27 from its developer site; no licence stated), and its reference
pages, <https://dev.mailjet.com/docs/api-reference> (pages, limits and the Send API's errors) and the webhooks pages.
Labels and descriptions are our own words.

## Signing in

**An API key and secret key** (a Mailjet credential): in Mailjet, the account's **API Key Management** page
(<https://app.mailjet.com/account/apikeys>).

## Actions

- **Send an email** (`send_email`): to one or more addresses, written here (text and HTML) or from a Mailjet
  template filled with values; it gives each recipient's message ID.
- **List contacts** (`list_contacts`, every page), **Get a contact** (`get_contact`), **Add a contact**
  (`create_contact`), **Change a contact** (`update_contact`), **Get a contact's properties**
  (`get_contact_properties`), **Set a contact's properties** (`set_contact_properties`, from a record of names and
  values).
- **List contact lists** (`list_contact_lists`), **Get a contact list** (`get_contact_list`), **Add a contact list**
  (`create_contact_list`), **Delete a contact list** (`delete_contact_list`), **Add a contact to a list**
  (`add_to_list`), **Take a contact off a list** (`remove_from_list`: unsubscribed, or taken off), **List a contact's
  lists** (`list_contact_subscriptions`).
- **List messages** (`list_messages`), **Get a message** (`get_message`), **Get a message's events**
  (`get_message_history`).
- **List templates** (`list_templates`) and **Get a template** (`get_template`).
- **List campaign drafts** (`list_campaign_drafts`), **Draft a campaign** (`create_campaign_draft`), **Set a campaign
  draft's email** (`set_campaign_draft_email`), **Send a campaign now** (`send_campaign_draft`), **Schedule a
  campaign** (`schedule_campaign_draft`).
- **List sender addresses** (`list_senders`).

## Triggers

- **An email opened, clicked, bounced ...** (`email_event`): each event of one kind (sent, open, click, bounce, spam,
  blocked, unsub) on the emails the account sends. Turning the workflow on gives Mailjet the workflow's address for
  that kind of event; turning it off deletes it.

  Mailjet does not sign its deliveries. As its webhooks page advises, the address it is given carries a user name and
  a password made for the workflow (`https://openduct:<password>@...`), and a delivery is taken only when it brings
  that password back in its `Authorization` header. Leave Mailjet's "group events" setting off: each delivery is then
  one event.

## Not done

- Attachments, CC and BCC, and sending many different messages at once.
- Statistics, segments (contact filters), contact imports, the Parse API (inbound email), templates' content, and the
  Content API (`/v1`).
- Grouped webhook deliveries (several events in one).

## Tested

Recorded exchanges made from Mailjet's description and reference pages (`tests/`), with made-up data. Not run against
a live Mailjet account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), made as Mailjet documents them;
none was run against a live account.
