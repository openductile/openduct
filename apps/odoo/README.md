# Odoo

Contacts, CRM leads, sales orders, invoices - and any other records - of an [Odoo](https://www.odoo.com) database,
on Odoo Online, Odoo.sh or your own server, through Odoo's External JSON-2 API.

Written by hand from Odoo's External JSON-2 API reference for Odoo 19.0
(<https://github.com/odoo/documentation/blob/25dbca91214799bfdae08bef901dae76cba35dc7/content/developer/reference/external_api.rst>,
CC BY-SA 4.0) and the model methods it calls (`search_read`, `read`, `search_count`, `create`, `write`, `unlink`,
`message_post`, `action_confirm`, as in <https://github.com/odoo/odoo> 19.0 at `e74fd9959692`, LGPL-3.0), read
2026-11-27. The descriptions here are our own words.

## What it needs

- **Odoo 19 or later**: the JSON-2 API is new in 19.0. (The older XML-RPC and JSON-RPC are to be removed in Odoo 22,
  and Odoo Online 21.1.)
- On **Odoo Online**, a **Custom** plan: Odoo gives external API access on no other plan.

## Signing in

In Odoo, your avatar → **Preferences** → **Account Security** → **New API Key**: give it a description and a
duration, and copy the key (it is shown once). A key lasts at most three months: make a new one before it runs out.
Better still, make a user for the workflows with only the rights they need, and its key.

In an Odoo credential give:

- **The database's address**, such as `https://mycompany.odoo.com` (or your own server's).
- **The database's name**, when the address serves more than one database (sent as `X-Odoo-Database`).
- **The API key** (sent as `Authorization: bearer`).

Everything the key's user may not see or change, the API may not either.

## Actions

For any kind of record - named by its model, such as `res.partner` (contacts), `crm.lead`, `sale.order`,
`account.move` (invoices and bills), `product.template`, `project.task`, `hr.employee`:

- **Find records** (`search_records`): those an Odoo filter (a domain, such as `[["is_company", "=", true]]`) holds
  for, with the fields named, in an order; every page.
- **Read records** (`read_records`), **Count records** (`count_records`).
- **Add a record** (`create_record`, the default), **Change records** (`update_records`), **Delete records**
  (`delete_records`).
- **Post a message on a record** (`post_message`): a note in its chatter, or a message its followers are sent too.

Ready for the usual records:

- **Find contacts** (`find_contacts`), **Add a contact** (`create_contact`).
- **Add a lead** (`create_lead`), **Find leads** (`find_leads`).
- **Find sales orders** (`find_sale_orders`), **Confirm a quotation** (`confirm_sale_order`).
- **Find customer invoices** (`find_invoices`): by payment status.

A field that names another record (a many2one, such as `partner_id`) comes as `[id, "name"]`, and an empty field as
`false`, as Odoo gives them; to set one, give the other record's id.

## Triggers

Both asked every 5 minutes, for one kind of record:

- **A new record** (`new_record`): each one added - a new contact, lead, order, invoice.
- **A record added or changed** (`changed_record`): each one added or changed; one changed again runs again.

## Not done

- Odoo has no webhooks of its own to send; its automation rules can post to an address (*Send Webhook
  Notification*), unsigned, which OpenDuct's webhook triggers do not take yet. Until then, the polls.
- Files and reports (PDFs), and methods other than those above (any model's `action_...` buttons); each is a call
  of the same form, `/json/2/<model>/<method>`, to add when needed.
- Odoo versions before 19 (XML-RPC and JSON-RPC only).

## Tested

Recorded exchanges built from Odoo's reference (`tests/`): what each request sends, answers of the documented shape
(ids, true, lists of records, two pages), and refusals in Odoo's error format. Not run against a live Odoo database.

```sh
openduct app check apps/odoo --bar 80
openduct app test apps/odoo
```
