# Zendesk

Support tickets in [Zendesk](https://www.zendesk.com): opened, answered and read.

## Signing in

Two ways, both giving the account's name (`mycompany` of `mycompany.zendesk.com`):

- **An API token** (a Zendesk credential): in Zendesk's Admin Center, **Apps and integrations → APIs → API tokens**,
  turn on token access and add one; give it with an agent's email.
- **An OAuth client** (a Zendesk client credential): **Apps and integrations → APIs → OAuth clients → Add OAuth
  client**, a confidential client; give its unique identifier and secret. OpenDuct asks Zendesk for a token as it
  needs one, acting as the admin who made the client.

## Actions

- **Open a ticket** (`open_ticket`): subject, what it is about, for whom, how urgent.
- **Answer a ticket** (`answer_ticket`): to the customer or as a note, and its new status.
- **List tickets** (`list_tickets`): every page.

## Triggers

- **A new ticket** (`new_ticket`) and **A ticket changed** (`updated_ticket`), asked every 5 minutes.

## Tested

Recorded exchanges from Zendesk's documented requests and answers (`tests/`).

Tried on 2026-11-16 with an OAuth client: the test account's trial had run out (Zendesk answered its token request with a page), and that was said. Not otherwise run against a live account yet.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
