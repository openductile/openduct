# Shopify

Orders and customers of a [Shopify](https://www.shopify.com) store.

## Signing in

Two ways, both giving the store's name (`mystore` of `mystore.myshopify.com`):

- **An app's client ID and secret** (a Shopify app credential) - the way for new apps. In Shopify's
  [Dev Dashboard](https://dev.shopify.com/dashboard), create an app with the Admin API scopes `read_orders`,
  `read_customers` and `write_customers`, release a version, and install it on the store (the store must belong to the
  same organisation). Copy its **Client ID** and **Secret** from its settings. OpenDuct asks the store for a token as
  it needs one (each lasts a day).
- **An Admin API access token** (`shpat_...`, a Shopify credential), from a custom app made in the store's admin
  before 2026.

## Actions

- **List orders** (`list_orders`): open by default, or closed, cancelled, any; since a time; every page.
- **Look up an order** (`get_order`).
- **Add a customer** (`create_customer`).

## Tested

Recorded exchanges from Shopify's documented requests and answers (`tests/`).

Run against a development store on 2026-11-16, signed in with an app's client ID and secret (the token asked of the store itself): List orders, Get an order and Create a customer worked. The access-token sign-in was not tried live.
