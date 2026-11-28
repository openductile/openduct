# Hunter

Email addresses found and checked with [Hunter](https://hunter.io), companies and people described, and the leads
and email sequences kept there.

Written by hand from Hunter's API reference, <https://hunter.io/api-documentation/v2> (read 2026-11-27). Hunter
publishes no OpenAPI description; the descriptions here are our own words.

## Signing in

In Hunter, open the **API** page of your dashboard and copy an API key into a Hunter credential. It is sent in the
`X-API-KEY` header.

## Actions

Finding and checking - each spends searches, verifications or credits of your plan, so a preview does not send them
(they are marked as writes):

- **Find someone's email address** (`find_email`, the default): from a first and last name (or a full name) and the
  company's website or name - or a LinkedIn handle. Nothing is charged when none is found.
- **Check an email address** (`verify_email`): `valid`, `invalid`, `accept_all`, `webmail`, `disposable` or
  `unknown`, with a score. Hunter takes up to 20 seconds; one it has not finished is answered 202 - try it again.
- **Find a company's email addresses** (`domain_search`): the addresses at a website with names, positions and
  departments, and the company's pattern; up to 100 (10 on the Free plan).
- **Describe a person** (`enrich_person`), **Describe a company** (`enrich_company`), **Describe a person and their
  company** (`enrich_contact`).

Free:

- **Count a company's email addresses** (`count_emails`), **Find a company's website** (`find_domain`), **Find
  companies** (`discover_companies`: a description in plain words), **Account and credits left** (`account`).

Leads and sequences:

- **List leads** (`list_leads`), **Look up a lead** (`get_lead`), **Save a lead** (`save_lead`: added, or changed if
  its address is there), **Add a lead** (`create_lead`), **Change a lead** (`update_lead`), **Delete a lead**
  (`delete_lead`).
- **List leads lists** (`list_leads_lists`), **Look up a leads list** (`get_leads_list`), **Make a leads list**
  (`create_leads_list`).
- **List email sequences** (`list_sequences`), **List a sequence's recipients** (`list_sequence_recipients`), **Add
  people to a sequence** (`add_to_sequence`), **Take people off a sequence** (`remove_from_sequence`): up to 50
  addresses at a time.

## Triggers

- **A new lead** (`new_lead`), asked every 5 minutes: each lead saved, on one list or any.
- **Someone replied to a sequence** (`sequence_reply`), asked every 15 minutes: each recipient of a sequence whose
  status becomes replied.

## Not done

- Hunter documents no webhooks; the triggers poll.
- Bulk jobs, saved searches, signals and opportunities, the multi-domain search and its reveals, writing and
  starting sequences and their follow-ups, email accounts, messages, custom attribute definitions.
- Lists of leads beyond the first 100 of a leads list (`get_leads_list`); `list_leads` reads every page.

## Tested

Recorded exchanges built from Hunter's API reference (`tests/`): what each request sends, answers of the documented
shape, refusals in Hunter's error format, and the two polls. Not run against a live Hunter account.

```sh
openduct app check apps/hunter --bar 80
openduct app test apps/hunter
```
