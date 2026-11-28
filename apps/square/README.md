# Square

Customers and payments in [Square](https://squareup.com).

## Signing in

In the Square Developer Console, open your application and copy its **Production access token** (or the Sandbox one,
to test) into a Square credential.

## Actions

- **Add a customer** (`create_customer`).
- **List payments** (`list_payments`), from a time on: every page.

## Triggers

- **A payment made (or changed)** (`payment`): each payment of the kinds chosen (`payment.created`,
  `payment.updated`), signed with the subscription's key over the workflow's address and the delivery. Turning the
  workflow on subscribes the address and keeps the key Square gives; turning it off deletes the subscription. Reach
  the studio at the address Square was given: a delivery signed for another address is refused.

## Tested

Recorded exchanges from Square's documented requests and answers (`tests/`).

Run against Square's sandbox on 2026-11-16: List payments and Add a customer worked.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
