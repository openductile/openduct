# HighLevel

Contacts, opportunities, messages and appointments of a [HighLevel](https://www.gohighlevel.com) sub-account (a
location): make and change contacts, tag them, add notes and tasks, start a workflow, move deals along, send a text or
an email, and book appointments.

Made with `openduct app from-openapi` from HighLevel's own API descriptions in
<https://github.com/GoHighLevel/highlevel-api-docs> at commit `0af86a4cbd48` (fetched 2026-11-27, CC0-1.0):
`apps/contacts.json`, `apps/opportunities.json`, `apps/conversations.json` and `apps/calendars.json`, joined into one;
the sign-in from `docs/oauth/Authorization.md` and `docs/oauth/Scopes.md` there. The API reference:
<https://marketplace.gohighlevel.com/docs/>. Requests go to `services.leadconnectorhq.com` with the API version each
part of the API asks for (`2021-07-28` for contacts and opportunities, `2021-04-15` for conversations and calendars).

## Signing in

- **A private integration token** (a HighLevel credential): in the sub-account's settings, under **Private
  Integrations**, make one with the scopes the actions need (contacts, opportunities, conversations and messages,
  calendars and their events); copy its token, and the sub-account's id (its location id, shown in its settings).
  Actions that name the sub-account use this id unless a step gives another.
- **Sign in with HighLevel** (a HighLevel sign-in): the installation's admin creates an app once in the
  [HighLevel Marketplace](https://marketplace.gohighlevel.com) (**My Apps → Create App**), with the callback address
  shown under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`) as its redirect URL and the scopes
  `contacts.readonly`, `contacts.write`, `opportunities.readonly`, `opportunities.write`, `conversations.readonly`,
  `conversations/message.readonly`, `conversations/message.write`, `calendars.readonly`, `calendars/events.readonly`,
  `calendars/events.write`; then copies its client ID and secret there. A workspace **Connects** and picks the
  sub-account; tokens last a day and are renewed. With this sign-in, steps give the sub-account's id
  (`location_id`) themselves.

## Actions

- **Create a contact** (`create_contact`, the default), **Look up a contact** (`get_contact`), **Change a contact**
  (`update_contact`), **Create or change a contact** (`upsert_contact`: by email or phone), **Remove a contact**
  (`delete_contact`), **Find a contact by email or phone** (`find_contact`).
- **Search contacts** (`search_contacts`): HighLevel's advanced search, its body as HighLevel's search documentation
  writes it (`locationId`, `query`, `filters`, `sort`, `page`, `pageLimit`).
- **Tag a contact** (`add_tags`), **Take tags off a contact** (`remove_tags`), **Add a note to a contact**
  (`add_note`), **Add a task for a contact** (`add_task`), **Add a contact to a workflow** (`add_to_workflow`).
- **Create an opportunity** (`create_opportunity`), **Look up** (`get_opportunity`) and **Change an opportunity**
  (`update_opportunity`), **Mark an opportunity won or lost** (`set_opportunity_status`), **Search opportunities**
  (`search_opportunities`: every page, up to 1,000), **List pipelines** (`list_pipelines`).
- **Send a message** (`send_message`): a text, email, WhatsApp or other message to a contact.
- **Search conversations** (`search_conversations`) and **List a conversation's messages** (`list_messages`: every
  page).
- **Book an appointment** (`create_appointment`), **List appointments** (`list_appointments`), **List a calendar's
  free times** (`get_free_slots`), **List calendars** (`list_calendars`).

A refusal fails the step in HighLevel's words (`message`, a text or a list).

## Not done

- **Triggers.** HighLevel sends its events (a contact made, an inbound message, an appointment booked ...) to a
  Marketplace app's webhook, signed with HighLevel's private key (`x-wh-signature`, RSA with SHA-256, checked with the
  public key it publishes). OpenDuct's webhook triggers check HMAC signatures and shared tokens only, so these are not
  here yet; a workflow's own webhook can take them, unchecked.
- **Send a message**: HighLevel's description marks `subType` and `status` as needed; they are inputs to give where
  HighLevel asks for them.
- Search contacts takes HighLevel's search body as it is: its fields are not in HighLevel's description.
- File uploads (message attachments), invoices, payments, products, social posting, funnels and the rest of the
  API's parts.

## Tested

Recorded exchanges (`tests/`): those made from the descriptions' schemas, and by hand the opportunity search's last
page, the sub-account's id from the credential, a contact made signed in with HighLevel, and refusals in HighLevel's
words. Not run against a live HighLevel sub-account.

```sh
openduct app check apps/highlevel --bar 80
openduct app test apps/highlevel
```
