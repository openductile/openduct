# Zoho CRM

Leads, contacts, accounts, deals, tasks and your own modules in [Zoho CRM](https://www.zoho.com/crm/): listed,
found, added, changed, converted and noted - such as each web form answer added as a lead, or each won deal copied to
the accounts system.

## Registering this installation with Zoho (once, by its admin)

1. In the [Zoho API Console](https://api-console.zoho.com), **Add Client → Server-based Applications**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
2. For accounts held in Zoho's other data centres (Europe, India, Australia, Japan, Canada, Saudi Arabia), open the
   client's **Settings** tab, turn on those data centres, and choose **Use the same OAuth credentials for all data
   centers** - OpenDuct keeps one client secret for the app.
3. Copy the **Client ID** and **Client secret** into **Settings → App sign-ins → Zoho CRM**.

Zoho's data centre in China has a console of its own (`api-console.zoho.com.cn`): an installation for accounts there
registers its client in it, and its people use the China sign-in.

## Connecting an account

Under Credentials, add a Zoho CRM credential with the sign-in of the data centre the account is held in - the end of
the address Zoho CRM opens at (`crm.zoho.com`, `crm.zoho.eu`, `crm.zoho.in`, ...) - and **Connect**. It asks to read
and change every module's records, run queries and read the modules' fields (`ZohoCRM.modules.ALL`,
`ZohoCRM.coql.READ`, `ZohoCRM.settings.fields.READ`).

## Actions

A module is named by its API name (`Leads`, `Contacts`, `Accounts`, `Deals`, `Tasks`, or your own), and fields by
theirs (`Last_Name`, `Email`, `Company`): **List a module's fields** (`list_fields`) shows them.

- **List records** (`list_records`): newest first, with the fields asked for (Zoho needs them named), or only a list
  view's; up to 2,000.
- **Get a record** (`get_record`): one record with every field.
- **Find records** (`search_records`): by email, phone, a word, or criteria such as
  `((Lead_Source:equals:Web)and(City:equals:Austin))`.
- **Ask with a query** (`query_records`): a COQL `select` over one module or several.
- **Add a record** (`create_record`, the default), **Change a record** (`update_record`), **Add or change a record**
  (`upsert_record`: matched on the email, or the fields named), **Delete a record** (`delete_record`: to the recycle
  bin).
- **Convert a lead** (`convert_lead`): into a contact and an account, and a deal when one is named.
- **Add a note to a record** (`add_note`).

Zoho's workflow rules, approvals and blueprints run on records added and changed here as they do on those added in
Zoho.

## Triggers

- **A new record** (`new_record`) and **A record added or changed** (`changed_record`), of the module named, asked every
  5 minutes; each new one is read in full (every field) for the run.

## Not done

- Zoho's notification channels (`/actions/watch`): Zoho sends the channel's verification token in the body, not in a
  header, so a delivery cannot be checked the way OpenDuct checks one, and a channel runs out at the time it was given.
  The triggers ask instead.
- Several records in one request, mass and bulk operations, attachments and photos, related lists, tags, blueprints,
  sharing, territories, and Zoho's settings (layouts, fields, modules) beyond reading the fields.
- Sandbox and developer accounts (`sandbox.zohoapis...`, `developer.zohoapis...`), and Zoho's UK data centre.

## Source

Written by hand from Zoho's own OpenAPI descriptions of the CRM API v8,
<https://github.com/zoho/crm-oas/tree/002cdbaf0ce8ececddf2d147a722db5f6a2cf4bb/v8.0> (`record`, `module_search`,
`coql`, `convert`, `notes`, `fields`; no licence stated), the data centres' addresses from Zoho's CRM SDK
<https://github.com/zoho/zohocrm-python-sdk-8.0/tree/1c4d4a45b726c7cc26f4cd9f0aee90f4abcb0948/zohocrmsdk/src/com/zoho/crm/api/dc>
(Apache-2.0), and Zoho's reference pages on
[multi data centre support](https://www.zoho.com/accounts/protocol/oauth/multi-dc.html),
[the authorisation request](https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html) and
[tokens](https://www.zoho.com/crm/developer/docs/api/v8/access-refresh.html), read on 2026-11-27. Descriptions are
our own words.

## Tested

Recorded exchanges from Zoho's documented requests and answers (`tests/`), with made-up records. Not run against a
live Zoho CRM account.
