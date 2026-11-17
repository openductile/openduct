# Mailchimp

Audiences and their contacts in [Mailchimp](https://mailchimp.com).

## Signing in

In Mailchimp, **Profile → Extras → API keys → Create A Key**. In a Mailchimp credential, give the key and its data
center: the part after the dash at its end (a key ending `-us21` is in `us21`).

## Actions

- **Add a contact to an audience** (`add_contact`): subscribed, or pending (Mailchimp sends a confirmation first).
- **List audiences** (`list_audiences`): their ids, for the action above.

## Tested

Recorded exchanges from Mailchimp's documented requests and answers (`tests/`).

Run against a Mailchimp account on 2026-11-16: List audiences worked; Add a contact was refused for an example.com address, in Mailchimp's words. Adding a real address was not tried.
