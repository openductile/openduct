# PayPal

Orders paid with [PayPal](https://www.paypal.com): created, looked up, and their payment taken once the buyer
approves.

## Signing in

In the PayPal Developer Dashboard, **Apps & Credentials**: create an app (Live, or Sandbox to test) and copy its
**Client ID** and **Secret** into a PayPal credential (or a PayPal sandbox one). OpenDuct trades them for a token as
it needs one; nothing else is asked.

## Actions

- **Create an order to pay** (`create_order`): the amount and currency; the answer's `approve` link is where the buyer
  pays.
- **Look up an order** (`get_order`).
- **Take the payment of an approved order** (`capture_order`).

## Tested

Recorded exchanges from PayPal's documented requests and answers (`tests/`).

Run against PayPal's sandbox on 2026-11-16: the token from the client ID and secret, Create an order and Look up an order worked; capturing an order no buyer approved was refused with PayPal's reason (ORDER_NOT_APPROVED). A capture after a buyer approves was not tried.
