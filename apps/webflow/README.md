# Webflow

Sites, CMS items, form answers and store orders in [Webflow](https://webflow.com), through its Data API v2.

Made by `openduct app from-openapi` from Webflow's own API description,
<https://github.com/webflow/openapi-spec/blob/f6db607359a4/openapi/v2.yml> (MIT licence, fetched 2026-11-27), then
finished by hand: plain labels, the single-item shape of new items, every page of a list, and the triggers. Webhook
signatures are as Webflow's guide [Working with webhooks](https://developers.webflow.com/data/docs/working-with-webhooks)
describes them. The vendor's API reference: <https://developers.webflow.com/data/reference/rest-introduction>.

## Signing in

- **A site token** (`webflow`): in Webflow, the site's **Site settings → Apps & integrations → API access →
  Generate API token**, with read and write access to Sites, Pages, CMS, Forms and Ecommerce as the actions need.
  One token reaches one site.
- **Sign in with Webflow** (`webflow_oauth`): a person signs in at Webflow and picks the sites. The installation
  registers its own app once: in a Webflow workspace, **Apps & integrations → Develop → Create an app**, with a Data
  Client, the redirect URI shown under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`), and the
  scopes `sites:read sites:write pages:read cms:read cms:write forms:read ecommerce:read ecommerce:write`. Its client
  ID and secret go in **Settings → App sign-ins → Webflow**.

## Actions

- Sites: **List sites** (`list_sites`), **Look up a site** (`get_site`), **Publish a site** (`publish_site`),
  **List pages** (`list_pages`).
- CMS: **List collections** (`list_collections`), **Look up a collection** (`get_collection`), **List items**
  (`list_items`), **Look up an item** (`get_item`), **Add an item** (`create_item`), **Change an item**
  (`update_item`), **Delete an item** (`delete_item`), **Publish items** (`publish_items`), **Add an item, live**
  (`create_live_item`), **Change a live item** (`update_live_item`). An item's fields are given by their slugs, as
  the collection names them (`name` and `slug` at least).
- Forms: **List forms** (`list_forms`), **List form answers** (`list_form_submissions`), **Look up a form answer**
  (`get_form_submission`).
- Store: **List products** (`list_products`), **Look up a product** (`get_product`), **Add a product**
  (`create_product`), **Change a product** (`update_product`), **List orders** (`list_orders`), **Look up an order**
  (`get_order`), **Change an order** (`update_order`: its note and shipping), **Mark an order fulfilled**
  (`fulfill_order`).

Lists give every page (100 a page, Webflow's most).

## Triggers

- **A form answered** (`new_form_submission`): each answer sent through the site's forms, or through the form of one
  name.
- **A new order** (`new_order`): each order placed in the store.
- **A new item** (`new_item`): each item added to the site's collections, or to one collection.

Turning the workflow on adds a webhook to the site (`POST /sites/{site_id}/webhooks`, so the token needs Sites
write access), and turning it off removes it. Each delivery is signed - an HMAC-SHA256 of `<timestamp>:<body>`, in
`x-webflow-signature`, with the time in `x-webflow-timestamp` - and one with no signature, a wrong one, or one more
than five minutes old is refused. With a site token, Webflow gives each webhook its own signing secret, which is kept,
sealed, for the workflow.

## Not done

- With **Sign in with Webflow**, Webflow signs webhooks with the installation's OAuth client secret rather than a
  secret of the webhook's own; the triggers do not yet take that secret, so use them with a site token.
- Webhooks added in Webflow's own pages carry no signature, so they cannot start a workflow: let the trigger add its
  own.
- Left out of the 126 operations: assets and fonts (file uploads), custom code, comments, redirects, robots.txt,
  analytics reports, site activity logs, inventory, users and the workspace operations; `openduct app from-openapi
  --choose` adds any of them.

## Tested

Recorded exchanges (`tests/`): those made from the API description's schemas, and by hand for the lists' pages, new
items and the triggers, with deliveries signed as Webflow's guide says. Not run against a live Webflow account.

```sh
openduct app check apps/webflow
openduct app test apps/webflow
```
