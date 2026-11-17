# Square

Customers and payments in [Square](https://squareup.com).

## Signing in

In the Square Developer Console, open your application and copy its **Production access token** (or the Sandbox one,
to test) into a Square credential.

## Actions

- **Add a customer** (`create_customer`).
- **List payments** (`list_payments`), from a time on: every page.

## Tested

Recorded exchanges from Square's documented requests and answers (`tests/`).

Run against Square's sandbox on 2026-11-16: List payments and Add a customer worked.
