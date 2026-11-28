# Agile CRM

Contacts, companies, deals, notes, tasks and campaigns in [Agile CRM](https://www.agilecrm.com): contacts found,
added, changed, tagged, and put in or taken off campaigns; deals made and moved along; tasks and notes added.

Written by hand from Agile CRM's REST API documentation,
<https://github.com/agilecrm/rest-api/blob/8748e70b91e13afecff881d0b58546c3677e1794/README.md> (its last change,
2019-11-27, read on 2026-11-27): Agile CRM publishes no API description file, and the documentation names no licence,
so every label and description here is in our own words.

## Signing in

- **Your email and the REST API key** (`agile_crm`): in Agile CRM, **Admin Settings → API & Analytics → API Key**;
  use the first key (the one for REST clients). Give the account's name (`mycompany` of `mycompany.agilecrm.com`),
  your email and the key; they are sent as HTTP basic, and every request asks for JSON (Agile CRM answers in XML
  otherwise).

## Good to know

- A contact's fields are *properties* in Agile CRM: **Add a contact** and **Change a contact** take plain inputs (first
  name, email...) and custom fields as a list of names and values, and send them as properties.
- Dates (a deal's close, a task's due time) are given as times such as `2026-12-31T00:00:00Z` and sent in seconds
  since 1970, as Agile CRM keeps them.
- Milestone names are as the deal's track shows them, capitals included.

## Actions

- **List contacts** (`list_contacts`): Every contact of the account, in the order they were added; every page.
- **Look up a contact** (`get_contact`): One contact (or company), by id: its fields (properties), tags, score and owner.
- **Find a contact by email** (`find_contact_by_email`): The contact with this email, if there is one.
- **Find a contact by phone** (`find_contact_by_phone`): The contact with this phone number, if there is one.
- **Search contacts or companies** (`search`): Contacts (or companies) whose fields hold a word.
- **Add a contact** (`create_contact`): A new contact, with its name, email, phone, company, title, tags and score as given.
- **Change a contact** (`update_contact`): Changes the fields given of a contact (an email or phone of the same kind is replaced); the others stay.
- **Tag a contact** (`add_tags`): Adds tags to a contact; the tags it has stay.
- **Take tags off a contact** (`remove_tags`): Takes these tags off a contact (or company).
- **Delete a contact** (`delete_contact`): Removes a contact (or company).
- **List campaigns** (`list_campaigns`): The account's campaigns (workflows), with their ids.
- **Put a contact in a campaign** (`add_to_campaign`): Starts a campaign for the contact with this email.
- **Take a contact off a campaign** (`remove_from_campaign`): Stops a campaign for a contact.
- **List companies** (`list_companies`): The account's newest companies, up to 100.
- **Add a company** (`create_company`): A new company, with its address on the web, email and phone as given.
- **List deals** (`list_deals`): Every deal, in the order they were made; every page.
- **Look up a deal** (`get_deal`): One deal, with its contacts.
- **Add a deal** (`create_deal`): A new deal, at a milestone of a track (milestone names are as the track shows them, capitals included).
- **Change a deal** (`update_deal`): Changes the fields given of a deal, such as its milestone or value; the others stay.
- **Delete a deal** (`delete_deal`): Removes a deal.
- **Add a note** (`add_note`): A note on one or more contacts.
- **List tasks** (`list_tasks`): Tasks, of one kind, status or priority if asked, pending ones only if asked; every page.
- **Add a task** (`create_task`): A new task, due at a time, on contacts or deals if given.
- **Change a task** (`update_task`): Changes the fields given of a task, or marks it done; the others stay.

## Triggers

- **A new contact** (`new_contact`): Each contact added, the newest of the last day asked for every few minutes.
- **A contact added or changed** (`updated_contact`): Each contact added or changed, the last changed of the last day asked for every few minutes.

Both ask Agile CRM's dynamic filter, every 5 minutes, for the contacts created (or changed) in the last day, the newest
first, and run those not seen before (or changed since).

## Not done

- Next pages of **List companies**: its cursor goes in a form, and a list's next page is put in the query or a JSON body only (an engine change: next pages in a form body); it gives the newest 100.
- Events, tracks, documents, help desk tickets, scores and stars by email.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Agile CRM account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Agile CRM documents it; none was run against a live account.

```sh
openduct app check apps/agile_crm --bar 80
openduct app test apps/agile_crm
```
