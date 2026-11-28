# Brevo

Emails and text messages sent through [Brevo](https://www.brevo.com) (once Sendinblue), its contacts and lists kept,
its email campaigns sent, and deals and companies added to its CRM - such as an order's confirmation sent from a
template, each new customer added to a list, or each bounced email marked in the CRM.

## Signing in

- **An API key** (`brevo`): one made in Brevo's settings, on its API keys page. It goes in the `api-key`
  header.

## Actions

- **Send an email** (`send_email`, the default): written here (HTML or plain text) or one of your Brevo templates
  (`template_id`) filled in with values (`params`), from one of your Brevo senders, to one person or several; with
  copies, a reply-to address, tags, or a time to send it at.
- **Send a text message** (`send_sms`): to a mobile number with its country code, from a sender name.
- **Contacts:** Add a contact (`create_contact`: or change the one with that address), Change a contact
  (`update_contact`), Get a contact (`get_contact`), Delete a contact (`delete_contact`), List contacts
  (`list_contacts`: all, on some lists, or added or changed since a time). A contact is named by their email address
  or id; attributes go by Brevo's names, in capitals (`FIRSTNAME`, `LASTNAME`, `SMS`).
- **Lists:** Put contacts on a list (`add_to_list`), Take contacts off a list (`remove_from_list`), List contact lists
  (`list_lists`), Make a contact list (`create_list`).
- **Campaigns and templates:** List email templates (`list_templates`), List email campaigns (`list_campaigns`), Send
  an email campaign now (`send_campaign`).
- **CRM:** Add a deal (`create_deal`), Add a company (`create_company`).

## Triggers

- **A new contact** (`new_contact`), asked every 5 minutes: each contact added, newest first as Brevo lists them.
- **An email delivered, opened, clicked or bounced** (`email_event`): the events of emails sent through Brevo's
  transactional email (as Send an email sends them), as Brevo sends them.
- **A contact added to a list, changed or unsubscribed** (`contact_event`): Brevo's marketing events - contacts added
  to lists, changed, deleted, unsubscribed, and campaign emails opened, clicked or bounced.

Turning a workflow with either of the last two on subscribes its address in Brevo, with a token of its own that Brevo
sends back in a header of each delivery (Brevo's webhooks carry custom headers, not a signature); a delivery without
it is refused. Turning the workflow off ends the subscription.

## Not done

- Several messages in one request (Brevo's message versions), attachments, WhatsApp, SMS and WhatsApp campaigns,
  making and changing email campaigns and templates, folders, segments, attributes, imports and exports, Brevo's
  e-commerce, loyalty and conversations APIs, and CRM tasks, notes and pipelines.
- Inbound email (Brevo's inbound parsing webhooks).

## Source

Written by hand from Brevo's OpenAPI description, <https://api.brevo.com/v3/swagger_definition_v3.yml>, fetched on
2026-11-27 (Brevo API 3.0.0, OpenAPI 3.0.1; MIT licence), and Brevo's page on
[transactional webhooks](https://developers.brevo.com/docs/transactional-webhooks). Descriptions are our own words.

## Tested

Recorded exchanges from Brevo's documented requests and answers (`tests/`), with made-up contacts and messages. Not
run against a live Brevo account.
