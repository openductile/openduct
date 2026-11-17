# Calendly

Bookings made through [Calendly](https://calendly.com), and who booked them.

## Signing in

In Calendly, **Integrations → API and webhooks → Personal access tokens → Generate new token**. Put it in a Calendly
credential.

## Actions

- **Who I am** (`me`): your user's address, which the next one asks for.
- **List bookings** (`list_bookings`): active or canceled, from a time on; every page.
- **Who booked a booking** (`list_invitees`): every page.

## Tested

Recorded exchanges from Calendly's documented requests and answers (`tests/`).

Run against a Calendly account on 2026-11-16: Who I am, List bookings and Who booked a booking worked.
