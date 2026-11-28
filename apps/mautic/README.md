# Mautic

Contacts, segments, campaigns, emails and companies of a [Mautic](https://mautic.org) marketing site, on your own
server.

Written by hand from Mautic's developer documentation, <https://devdocs.mautic.org/en/7.2/>, as its source has it:
<https://github.com/mautic/developer-documentation-new> at commit `aa63e2eb9e58` (Apache-2.0) - `docs/rest_api/`
(authentication, contacts, segments, campaigns, emails, companies, notes, webhooks) and `docs/webhooks/` (events and
signatures). Labels and descriptions are our own words.

## Signing in

- **A Mautic user name and password** (`mautic`): Mautic takes HTTP basic once it is turned on, in Mautic's
  **Settings → Configuration → API Settings**: **API enabled** and **Enable HTTP basic auth**. Give the site's address
  (`https://mautic.example.com`), and the name and password of a Mautic user with the rights the actions need (best,
  a user made for this). The site is reached over https only; on the same machine or network as OpenDuct, start
  OpenDuct with `--allow-private-network`.

## Actions

Contacts come plainly - `id`, `email`, `first_name`, `last_name`, `company`, `phone`, `points`, `tags`, and every field
by its alias as `fields`.

- Contacts: **List contacts** (`list_contacts`: newest first, or by a Mautic search such as `ann@example.com`,
  `tag:customer` or `segment:newsletter`; at most 5,000 a step), **Look up a contact** (`get_contact`), **Add a
  contact** (`create_contact`), **Change a contact** (`update_contact`), **Delete a contact** (`delete_contact`), **Add
  points to a contact** (`add_points`), **Mark a contact do-not-contact** (`do_not_contact`), **Remove a
  do-not-contact mark** (`remove_do_not_contact`), **Add a note to a contact** (`add_note`).
- Segments: **List segments** (`list_segments`), **Add a contact to a segment** (`add_to_segment`), **Take a contact
  out of a segment** (`remove_from_segment`).
- Campaigns: **List campaigns** (`list_campaigns`), **Add a contact to a campaign** (`add_to_campaign`), **Take a
  contact out of a campaign** (`remove_from_campaign`).
- Emails: **List emails** (`list_emails`), **Send an email to a contact** (`send_email`: a template email, with its
  tokens filled in; Mautic skips a contact marked do-not-contact).
- Companies: **List companies** (`list_companies`), **Add a company** (`create_company`), **Link a contact to a
  company** (`add_to_company`).

## Triggers

- **A new contact** (`new_contact`), **A contact changed** (`changed_contact`), **A form answered**
  (`form_answered`).

Turning the workflow on adds a webhook to Mautic (`POST /api/hooks/new`, so the user needs the right to manage
webhooks) with a secret of its own, and turning it off deletes it. Mautic signs each delivery - an HMAC-SHA256 of the
body, base64, in `Webhook-Signature` - and one with no signature or a wrong one is refused. When Mautic queues its
webhooks (its **Webhook Settings → Queue Mode**), one delivery holds several events, and each starts a run. A webhook
made in Mautic by hand works too: give its secret in the credential.

## Not done

- **Signing in with Mautic's OAuth 2.0** (an API credential's client ID and secret), which Mautic recommends over
  basic: its token address is on the site's own server, and OpenDuct takes a client-credentials token address only
  on an app's declared hosts, which a self-hosted app has none of.
- Assets, dynamic content, focus items, forms and their fields, pages, points rules, reports, SMS, stages, users,
  and contact batches.

## Tested

Recorded exchanges built from Mautic's documented requests and answers (`tests/`), with deliveries signed as its
documentation says, and made-up people on reserved domains. Not run against a live Mautic site.

```sh
openduct app check apps/mautic
openduct app test apps/mautic
```
