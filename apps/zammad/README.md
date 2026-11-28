# Zammad

Tickets of a [Zammad](https://zammad.org) help desk, their messages and tags, its customers and organisations; on
Zammad's hosting (`*.zammad.com`) or your own server.

## Signing in

- **An access token (Zammad hosted)** (`zammad`): your Zammad's name (the `acme` of `acme.zammad.com`) and a token.
- **An access token (your own Zammad)** (`zammad_self_hosted`): your Zammad's address (such as
  `https://support.example.com`) and a token.

A token is made in Zammad under **your avatar → Profile → Token Access**, with the permissions it needs
(`ticket.agent` for tickets; `admin.user`, `admin.organization`, `admin.webhook` and `admin.trigger` for the rest).
Tokens are on by default; an admin can turn them off (**System → API → Token Access**). It is sent as
`Authorization: Token token=<token>`. Zammad's own OAuth sign-in lives on each installation's address, which the
app kit's sign-in at a vendor cannot name: the token is used instead.

## Actions

- **List tickets** (`list_tickets`), **Find tickets** (`search_tickets`: Zammad's search, such as
  `state.name:open AND customer.email:ann@example.com`), **Get a ticket** (`get_ticket`).
- **Open a ticket** (`create_ticket`): in a group, for a customer by email (made if new), with its first message.
- **Change a ticket** (`update_ticket`): title, group, state, priority, owner, pending time; with a note if given.
- **Remove a ticket** (`delete_ticket`, an admin's token).
- **A ticket's messages** (`list_articles`), **Reply to a ticket, or add a note** (`add_article`): an internal note,
  a phone note, or an email to the customer.
- **A ticket's tags** (`list_ticket_tags`), **Tag a ticket** (`add_ticket_tag`), **Take a tag off a ticket**
  (`remove_ticket_tag`).
- **Who I am** (`me`), **Find people** (`search_users`), **Get a person** (`get_user`), **Add a customer**
  (`create_user`), **Change a person** (`update_user`).
- **Find organisations** (`search_organizations`), **Add an organisation** (`create_organization`).
- **List groups** (`list_groups`), **List ticket states** (`list_states`), **List ticket priorities**
  (`list_priorities`).

## Triggers

- **A ticket opened (or changed)** (`ticket_event`): each ticket Zammad sends when it is opened (`create`, the
  default) or changed (`update`), with its latest message. Turning the workflow on makes a webhook in Zammad for the
  workflow's address, signed with a token made for it, and a Zammad trigger that calls it; turning it off removes
  both. Each delivery's `X-Hub-Signature-256` (HMAC-SHA256 of the body) is checked, and one whose
  `X-Zammad-Delivery` id was seen is not run twice. It needs a token with `admin.webhook` and `admin.trigger`.

## Source

Written by hand from Zammad's REST API documentation, <https://docs.zammad.org/en/latest/api/intro.html> (its source,
<https://github.com/zammad/zammad-documentation/tree/1eb414863cd22c5effeb0251c32e156dee5cffa2/api>, AGPL-3.0), and
the webhook pages of its admin documentation
(<https://github.com/zammad/zammad-admin-documentation/tree/0f309e628ab27164094547886cc4bfa02e8d695e/manage/webhook>,
AGPL-3.0), fetched 2026-11-27. Zammad publishes no OpenAPI description. The labels and descriptions here are our own
words.

## Not done

- Attachments (sent as Base64 in an article; reading them back is a file), time accounting, links between tickets,
  mentions, checklists, knowledge base, the MCP server Zammad also offers.
- Signing in with a user name and password (Zammad advises against it) or with Zammad's OAuth.

## Tested

Recorded exchanges (`tests/`) made from the documented requests and answers, with made-up values; the webhook
deliveries are signed as Zammad documents, with a tool of our own. Not run against a live Zammad.

```sh
openduct app check apps/zammad --bar 80
openduct app test apps/zammad
```
