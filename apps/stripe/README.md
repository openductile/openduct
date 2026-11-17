# Stripe

Customers, payments and refunds in [Stripe](https://stripe.com).

## Signing in

In Stripe's Dashboard, **Developers → API keys**: use a **restricted key** with only what your workflows need
(Customers: write; PaymentIntents: read; Refunds: write), or the secret key. Add it under Credentials as a Stripe
credential.

## Actions

- **Create a customer** (`create_customer`): email, and optionally name, phone, a note.
- **Find customers by email** (`find_customers`).
- **List payments** (`list_payments`), optionally one customer's: every page.
- **Refund a payment** (`refund_payment`): all of it, or an amount in cents.

## Tested

Recorded exchanges from Stripe's documented requests and answers (`tests/`).

Run against Stripe in test mode on 2026-11-16: List payments, Find customers by email and Create a customer worked; a refund of a payment that does not exist was refused in Stripe's words. A refund of a real payment was not tried.
