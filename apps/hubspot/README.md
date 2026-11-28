# HubSpot

Contacts and deals in [HubSpot](https://www.hubspot.com)'s CRM.

## Signing in

In HubSpot, **Settings → Integrations → Private Apps → Create a private app**, with the scopes
`crm.objects.contacts.read`, `crm.objects.contacts.write`, `crm.objects.deals.write`. Copy its access token into a
HubSpot credential.

## Actions

- **Add a contact** (`create_contact`).
- **Find a contact by email** (`find_contact`).
- **List contacts** (`list_contacts`): every page.
- **Add a deal** (`create_deal`).

## Triggers

- **A new contact** (`new_contact`), asked every 5 minutes (crm.objects.contacts.read).

## Tested

Recorded exchanges from HubSpot's documented requests and answers (`tests/`).

Run against a HubSpot test account on 2026-11-16: List contacts, Find a contact by email, Create a contact and Create a deal worked.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
