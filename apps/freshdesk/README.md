# Freshdesk

Support tickets, contacts and companies in [Freshdesk](https://www.freshworks.com/freshdesk/).

## Source

Written by hand from Freshdesk's API reference (v2), <https://developers.freshdesk.com/api/>, read on 2026-11-27.
Freshdesk publishes no machine-readable description and states no licence for its reference: labels and
descriptions here are our own words.

## Signing in

**An API key** (a Freshdesk credential): in Freshdesk, click your profile picture (top right), **Profile settings**;
the API key is beside the password settings. Give it with the helpdesk's name in its address (`mycompany` of
`mycompany.freshdesk.com`). Freshdesk takes the key as the user name of HTTP basic sign-in; a request acts as the agent
whose key it is, with their role's permissions.

## Actions

- **Open a ticket** (`create_ticket`): for a customer's email, with its priority and status in words (low, medium,
  high, urgent; open, pending, resolved, closed) or Freshdesk's numbers, tags and custom fields.
- **Email a customer first** (`send_outbound_email`): an email from one of the helpdesk's addresses, kept as a ticket.
- **Get a ticket** (`get_ticket`), **List tickets** (`list_tickets`, newest first, every page), **Change a ticket**
  (`update_ticket`), **Delete a ticket** (`delete_ticket`, to the trash), **Find tickets** (`search_tickets`, by a
  query of their fields).
- **Reply to a ticket** (`reply_to_ticket`), **Add a note to a ticket** (`add_note`), **List a ticket's
  conversation** (`list_conversations`).
- **List ticket fields** (`list_ticket_fields`): the names custom fields go by.
- **Add a contact** (`create_contact`), **Get a contact** (`get_contact`), **List contacts** (`list_contacts`),
  **Change a contact** (`update_contact`), **Find contacts** (`search_contacts`), **Delete a contact**
  (`delete_contact`, can be brought back).
- **Add a company** (`create_company`), **Get a company** (`get_company`), **List companies** (`list_companies`),
  **Change a company** (`update_company`).
- **List agents** (`list_agents`) and **List groups** (`list_groups`): the IDs tickets are given to.

A list with no filter gives Freshdesk's own default: tickets of the last 30 days (older ones with `updated_since`).
The searches give at most 10 pages of 30, as Freshdesk does.

## Triggers

- **A new ticket** (`new_ticket`) and **A ticket changed** (`updated_ticket`), asked every 5 minutes.

## Not done

- Attachments (they are sent as multipart forms), forwarding and merging tickets, bulk changes, time entries,
  satisfaction ratings, the knowledge base (solutions), discussions, canned responses and the admin settings.
- Webhooks: the API reference documents none that can be registered, or whose deliveries can be checked, so the
  triggers poll instead.

## Tested

Recorded exchanges from Freshdesk's documented requests and answers (`tests/`), with made-up data. Not run against a
live Freshdesk account.

Triggers are tested with recorded exchanges (`tests/trigger_*.yaml`); none was run against a live account.
