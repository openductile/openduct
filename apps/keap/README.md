# Keap

Contacts, companies, tags, notes, tasks, opportunities and orders in [Keap](https://keap.com) (once Infusionsoft);
emails sent to contacts, contacts put in campaign sequences, and campaign goals reached.

Made with `openduct app from-openapi` from Keap's own API description (REST v2),
<https://github.com/infusionsoft/keap-sdk/blob/c386b1ea7d1318c270a16ac03dae95f96b6da351/sdks/v2/swagger.yml>
(OpenAPI 3.1, the commit of 2026-11-05). The description names its licence as Apache 2.0; the repository is MIT.
Labels are reworded in plain words. The vendor's API reference: <https://developer.keap.com/docs/restv2/>.

## Signing in

- **Sign in with Keap** (`keap`), OAuth 2.0: the installation registers an app with Keap once, in Keap's developer
  portal (<https://developer.keap.com>), with the callback address shown under **Settings → App sign-ins**
  (`https://<your studio>/oauth/callback`); Keap gives a client id and a secret, entered there. Keap has one scope,
  `full`. Each workspace then connects its own Keap account under Credentials.
- **A personal access token or service account key** (`keap_key`): made in Keap for one user (a personal access
  token acts as that user) or, by an admin, for the whole account (a service account key); sent as
  `Authorization: Bearer`, as Keap's own SDK samples send it. Keap allows such keys fewer requests than an app's sign-in.

## Good to know

- Filters are written as Keap writes them, `field==value` (`email==ann@example.com`, `given_name==Mar*`); each list's
  description of the filter says which fields it takes.
- **Send an email** takes the message as text or HTML and sends it in Base64, as Keap asks.
- **Achieve a campaign goal** reaches an API goal of an automation: its integration and call name are those set on
  the goal in Keap.

## Actions

- **List tasks** (`list_tasks`): Tasks, filtered and ordered as asked, every page.
- **List tags** (`list_tags`): The tags of the account; filter==NONE finds those with an empty field.
- **List orders** (`list_orders`): Orders, filtered and ordered as asked, every page.
- **List opportunities** (`list_opportunities`): Opportunities in the sales pipeline, filtered and ordered as asked, every page.
- **List contacts** (`list_contacts`): Contacts, filtered (by email, name, phone, company or when changed) and ordered as asked, every page.
- **List companies** (`list_companies`): Companies, filtered and ordered as asked, every page.
- **List a contact's notes** (`list_notes`): The notes on one contact.
- **List a contact's tags** (`list_contact_tags`): The tags applied to one contact.
- **Create a task** (`create_task`): A new task for a user, on a contact or an opportunity if given.
- **Create an order** (`create_order`): A one-time order for a contact, with its items.
- **Create an opportunity** (`create_opportunity`): A new opportunity for a contact, at a stage of the pipeline.
- **Create a contact** (`create_contact`): A new contact; with a duplicate check, one already there by email (or email and name) is changed instead.
- **Create a company** (`create_company`): A new company (a country code is needed when a region is given).
- **Add a note to a contact** (`create_note`): A note on a contact, written as a user.
- **Look up an order** (`get_order`): One order, with its items and what was paid.
- **Look up a contact** (`get_contact`): One contact, with the fields asked for.
- **Add contacts to a campaign sequence** (`add_to_campaign_sequence`): Puts contacts in a sequence of a campaign; the answer says how each went.
- **Change a task** (`update_task`): Changes the fields given of a task; the others stay.
- **Change an opportunity** (`update_opportunity`): Changes the fields given of an opportunity, such as its stage; the others stay.
- **Change a contact** (`update_contact`): Changes the fields given of a contact; the others stay.
- **Achieve a campaign goal** (`achieve_goal`): Tells an automation that a contact reached one of its API goals (by its integration and call name), so the contact moves on in it.
- **Remove a tag from contacts** (`remove_tag`): Takes a tag off each of the contacts given.
- **Apply a tag to contacts** (`apply_tag`): Puts a tag on each of the contacts given (which can start a campaign).
- **Delete a contact** (`delete_contact`): Removes a contact.
- **Send an email** (`send_email`): Sends an email to contacts, as a user of the account.

## Triggers

- **A new contact** (`new_contact`): Each contact added, the newest asked for every few minutes.
- **A contact added or changed** (`updated_contact`): Each contact added or changed, the last changed asked for every few minutes.
- **A new order** (`new_order`): Each order made, the newest asked for every few minutes.

All three are asked every 5 minutes. Keap's REST hooks (v1) are not used: they ask the address to answer with the
secret they send in a header, which a trigger does not do yet.

## Not done

- 374 more operations the description offers (products, subscriptions, discounts, payments, custom fields, files, reports); a custom app or a later version can add them.
- Lists of fields a call takes several times (fields, update_mask) are sent once.
- File uploads (multipart).
- Keap's REST hooks (deliveries as they happen): their address check echoes a header back, which an app's trigger cannot do yet.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Keap account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Keap documents it; none was run against a live account.

```sh
openduct app check apps/keap --bar 80
openduct app test apps/keap
```
