# WooCommerce

Orders of a [WooCommerce](https://woocommerce.com) store, on your own site.

## Signing in

In WordPress, **WooCommerce → Settings → Advanced → REST API → Add key**: Read/Write. In a WooCommerce credential,
give the store's address (`https://...`), the consumer key and the consumer secret. The store is reached over https
only; on the same machine or network as OpenDuct, start OpenDuct with `--allow-private-network`.

## Actions

- **List orders** (`list_orders`): optionally by status, or since a time; every page.
- **Look up an order** (`get_order`).
- **Change an order's status** (`set_order_status`).

## Triggers

- **A new order** (`new_order`): each order placed, as WooCommerce sends it, signed. Turning the workflow on adds a
  webhook to the store with a secret of its own (the key needs read/write), and turning it off deletes it.

## Tested

Recorded exchanges from WooCommerce's documented requests and answers (`tests/`).

Run against a WooCommerce 11.3 store on 2026-11-16 (a local one, with 105 orders): List orders - all 105, page after
page, by status, and since a time - Look up an order and Change an order's status worked; an order that does not
exist and a status WooCommerce does not have were refused in its words.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
