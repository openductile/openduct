# Xero

Contacts and invoices in [Xero](https://www.xero.com).

## Registering this installation with Xero (once, by its admin)

1. At [developer.xero.com](https://developer.xero.com/app/manage), **New app**: a **Web app**, with the redirect URI
   shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
2. Generate a client secret, and copy the **Client id** and **Client secret** into **Settings → App sign-ins → Xero**.

## Connecting an organisation

Under Credentials, add a Xero credential and **Connect**: sign in at Xero and allow the organisation. OpenDuct keeps
the token and the organisation's id, and renews the token as it runs out.

## Actions

- **Add a contact** (`add_contact`).
- **Make an invoice** (`create_invoice`): its lines in Xero's own form; a draft unless you say AUTHORISED.
- **List invoices** (`list_invoices`): optionally only some statuses; every page.

## Tested

Recorded exchanges from Xero's documented requests and answers (`tests/`).

Tried on 2026-11-16: the test organisation's sign-in had run out (its refresh token was used up), so it needs Connect again before it can be run live.
