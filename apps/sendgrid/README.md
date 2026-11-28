# SendGrid

Email sent through [Twilio SendGrid](https://sendgrid.com): your own words or a dynamic template, the marketing
contacts and their lists, and who unsubscribed, bounced or reported spam.

Made by `openduct app from-openapi` from Twilio SendGrid's own API descriptions,
<https://github.com/twilio/sendgrid-oai> at commit `51a0dee5a218` (`spec/json/`: `tsg_mail_v3`, `tsg_mc_contacts_v3`,
`tsg_mc_lists_v3`, `tsg_templates_v3`, `tsg_suppressions_v3`, `tsg_email_validation_v3`; MIT licence), merged into one
description on 2026-11-27, then finished by hand: plain names and inputs, one contact at a time, addresses separated
by commas. The vendor's API reference: <https://www.twilio.com/docs/sendgrid/api-reference>.

## Signing in

In SendGrid, **Settings → API Keys → Create API Key**. A **Restricted Access** key with only what your workflows
need is best: **Mail Send** to send, **Marketing** for contacts and lists, **Suppressions** for unsubscribes and
bounces, **Template Engine** (read) for templates. Copy the key (it starts `SG.`) into a SendGrid credential.

- **An API key** (`sendgrid`): sent as `Authorization: Bearer`, to `api.sendgrid.com`.
- **An API key of an EU subuser** (`sendgrid_eu`): a subuser whose data is kept in the EU is reached at
  `api.eu.sendgrid.com`. SendGrid's descriptions name that address for sending, templates and suppressions; contacts,
  lists and address checks are described at the global address only.

**Check an email address** needs SendGrid's separate Email Address Validation key (a Pro or Premier plan); give it
in a credential of its own.

## Actions

- **Send an email** (`send_email`, the default): to one or more addresses, with cc, bcc and a reply address; plain
  text, HTML or both - or a dynamic template (`d-...`) with its values. It may be sent later (`send_at`) and belong
  to an unsubscribe group. The from address must be a sender verified in SendGrid.
- **Add or update a contact** (`add_or_update_contact`): found by its email, added or changed, put on lists; SendGrid
  takes it in a moment and answers with the job's id.
- **Find contacts by email address** (`find_contacts_by_email`), **Search contacts** (`search_contacts`: SendGrid's
  query language, the first 50), **Look up a contact** (`get_contact`), **Delete contacts** (`delete_contacts`),
  **Count contacts** (`count_contacts`).
- **List contact lists** (`list_lists`), **Make a contact list** (`create_list`), **Look up a contact list**
  (`get_list`), **Rename a contact list** (`rename_list`), **Delete a contact list** (`delete_list`), **Take contacts
  off a list** (`remove_from_list`).
- **List email templates** (`list_templates`: dynamic and legacy unless one is named), **Look up an email template**
  (`get_template`).
- **Check an email address** (`validate_email`): a verdict, a score, a suggestion for a typo. Marked as a write so a
  preview does not spend a check.
- **List who unsubscribed from everything** (`list_unsubscribes`), **Unsubscribe from everything** (`unsubscribe`),
  **Take an address off the global unsubscribe list** (`resubscribe`).
- **List bounced addresses** (`list_bounces`), **Clear a bounced address** (`delete_bounce`), **List spam reports**
  (`list_spam_reports`).
- **List unsubscribe groups** (`list_unsubscribe_groups`), **Unsubscribe from a group** (`add_to_unsubscribe_group`),
  **Resubscribe to a group** (`remove_from_unsubscribe_group`).

## Triggers

- **Someone unsubscribed from everything** (`new_unsubscribe`), asked every 5 minutes: each address added to the
  global unsubscribe list since the poll before.
- **An email bounced** (`new_bounce`), asked every 5 minutes: each address added to the bounce list, with why.

## Not done

- SendGrid's **Event Webhook** (delivered, opened, clicked, ... for every email) as a trigger: SendGrid signs its
  deliveries with an ECDSA key pair (it keeps the private key and shows the public one), not with a shared secret,
  and OpenDuct's webhook triggers check shared-secret signatures only. Until that is there, the two polls above cover
  unsubscribes and bounces.
- Attachments, inbound mail (Inbound Parse posts multipart forms), single sends and segments, senders and domains,
  statistics, subusers: 48 more operations of the merged description.
- A contact's own suppression groups, and contacts by phone or external id.

## Tested

Recorded exchanges built from SendGrid's API descriptions and reference (`tests/`): what each request sends and an
answer of the documented shape, a refusal in SendGrid's own words, and the two polls (first poll, a later one).
Not run against a live SendGrid account.

```sh
openduct app check apps/sendgrid --bar 80
openduct app test apps/sendgrid
```
