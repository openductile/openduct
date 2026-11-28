# ActiveCampaign

Contacts, tags, lists, automations and deals in [ActiveCampaign](https://www.activecampaign.com).

Written from ActiveCampaign's own API description, <https://developers.activecampaign.com/openapi/v3.json> (fetched
2026-11-27; OpenAPI 3.1, 587 operations), with `openduct app from-openapi`'s reading of the 25 operations chosen as
the start. The description states no licence, so only its facts are used - addresses, fields, answers - and every
label and description here is our own. The vendor's API reference: <https://developers.activecampaign.com/reference/overview>.

## Signing in

- **An API key** (`activecampaign`): in ActiveCampaign, **Settings → Developer** shows the account's API **URL**
  (`https://<account>.api-us1.com`) and **Key**. Give the account's name (the part before `.api-us1.com`) and the
  key; the key is sent as `Api-Token`. It acts as the user it belongs to, with that user's permissions.

## Actions

- **Add or update a contact** (`sync_contact`): by email - changed when there is one, added when not.
- **Add a contact** (`create_contact`), **Get a contact** (`get_contact`), **Change a contact** (`update_contact`),
  **Delete a contact** (`delete_contact`).
- **Find contacts** (`find_contacts`): by a search (email, name, phone, account), a list, a tag, or added since a
  date; the newest first, up to 1,000.
- **Tag a contact** (`add_tag_to_contact`), **Take a tag off a contact** (`remove_tag_from_contact`), **List a
  contact's tags** (`list_contact_tags`), **List tags** (`list_tags`), **Add a tag** (`create_tag`).
- **Subscribe a contact to a list** (`subscribe_to_list`; or unsubscribe them), **List lists** (`list_lists`).
- **Put a contact into an automation** (`add_contact_to_automation`), **List automations** (`list_automations`).
- **Add a deal** (`create_deal`: its value in cents), **Get a deal** (`get_deal`), **Find deals** (`list_deals`),
  **Change a deal** (`update_deal`: status 0 open, 1 won, 2 lost), **Add a note to a deal** (`add_deal_note`).
- **List pipelines** (`list_pipelines`), **List deal stages** (`list_stages`).
- **Set a contact's custom field** (`set_custom_field`), **List custom fields** (`list_custom_fields`).
- **Add an account** (`create_account`): a company that contacts and deals belong to.

Custom fields are given by id: **List custom fields** names them.

## Triggers

- **A new contact** (`new_contact`) and **A new deal** (`new_deal`), asked every 5 minutes.

ActiveCampaign documents no signature on its webhooks, so a delivery could not be told from a forged one; they are not
offered. The triggers poll instead.

## Not done

- 562 more operations of the description: campaigns and their reports, forms, e-commerce, SMS, segments, site
  tracking, notes on contacts and accounts, tasks, users, the AI features.
- Accounts on another address than `<account>.api-us1.com`.

## Tested

Recorded exchanges from the requests and answers the description documents (`tests/`), with made-up values: refusals
in ActiveCampaign's two error shapes, the triggers' polls. Not run against a live account.

```sh
openduct app check apps/activecampaign --bar 80
openduct app test apps/activecampaign
```
