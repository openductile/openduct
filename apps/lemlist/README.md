# lemlist

Outreach campaigns in [lemlist](https://www.lemlist.com): their leads, what happened to them (replies, opens,
clicks, LinkedIn steps), contacts, unsubscribes, and finding a person's email.

Hosts, paths and fields come from lemlist's own API description,
<https://developer.lemlist.com/api-reference/openapi/v2.json> (fetched 2026-11-27). It states no licence, so its
wording is not reused: the labels and descriptions here are our own. Made with `openduct app from-openapi --choose`
and finished by hand. The vendor's API reference: <https://developer.lemlist.com/api-reference/getting-started/overview>.

## Signing in

- **An API key** (`lemlist`): in lemlist, **Settings → Integrations → API**, generate a key. lemlist takes it as HTTP
  basic with an empty user name and the key as the password; the credential holds only the key.

## Actions

- Campaigns: **List campaigns** (`list_campaigns`), **Look up a campaign** (`get_campaign`), **Start a campaign**
  (`start_campaign`), **Pause a campaign** (`pause_campaign`).
- Leads: **List a campaign's leads** (`list_campaign_leads`), **Add a lead to a campaign** (`add_lead`), **Look up a
  lead by email** (`get_lead`), **Change a lead** (`update_lead`), **Take a lead out of a campaign** (`remove_lead`:
  unsubscribed, or deleted), **Pause a lead** (`pause_lead`), **Resume a lead** (`resume_lead`), **Mark a lead
  interested** (`mark_interested`), **Mark a lead not interested** (`mark_not_interested`).
- **List activities** (`list_activities`): replies, opens, clicks, bounces and LinkedIn steps, newest first (at most
  2,000 a step).
- Unsubscribes: **List unsubscribes** (`list_unsubscribes`), **Add an unsubscribe** (`add_unsubscribe`), **Remove an
  unsubscribe** (`remove_unsubscribe`) - an email, a whole domain, a LinkedIn address or a phone number.
- Contacts: **Find contacts** (`list_contacts`), **Look up a contact** (`get_contact`), **Add or change a contact**
  (`save_contact`).
- **Send an email** (`send_email`): from one of the team's mailboxes, as lemlist's inbox sends one.
- **Find a person's email or phone** (`enrich_person`, uses credits) and **Get an enrichment's result**
  (`get_enrichment`); **Look up the team** (`get_team`), **Look up the credits left** (`get_credits`).

## Triggers

- **A new activity** (`new_activity`): each new activity of the team, or of one kind (`emailsReplied`,
  `emailsBounced`, `linkedinInviteAccepted`...) or one campaign, asked for every five minutes (lemlist lists the
  newest first, as its reference's example shows).

## Not done

- lemlist's webhooks (`POST /hooks`) carry their secret in the body of each call, not in a header or as a signature;
  OpenDuct checks a webhook's signature or token in a header only, so the trigger polls instead.
- Left out of lemlist's 256 operations: sequences and their steps, schedules, senders, the people and company databases,
  LinkedIn, WhatsApp and SMS messages, tasks, companies, warm-up, signal agents, domains and mailboxes, and the CRM
  links.

## Tested

Recorded exchanges built from the API description's requests and answers (`tests/`), with made-up people on reserved
domains. Not run against a live lemlist account.

```sh
openduct app check apps/lemlist
openduct app test apps/lemlist
```
