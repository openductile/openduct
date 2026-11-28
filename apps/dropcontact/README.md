# Dropcontact

Find and check work email addresses, and fill in contacts' and companies' details (job, website, LinkedIn, phone,
size, industry; French companies' SIREN, SIRET and VAT numbers), with [Dropcontact](https://www.dropcontact.com).

Made by `openduct app from-openapi` from Dropcontact's own API description,
<https://developer.dropcontact.com/openapi.json> (Enrich API Service 0.1.0, OpenAPI 3.1; last changed 2026-11-09,
fetched 2026-11-27; no licence stated, so every label and description here is in our own words), then finished by
hand. The vendor's API reference: <https://developer.dropcontact.com>.

## Signing in

- **An API key** (`dropcontact`): in Dropcontact, the settings' API page gives the key. It is sent in the
  `X-Access-Token` header.

## Actions

Dropcontact works in two steps: a contact (or up to 250) is sent, and its result is ready a few seconds to minutes
later.

- **Find a contact's email and details** (`enrich_contact`, the default): one contact, by name and company or website,
  by LinkedIn profile, or by an email to check. It gives the `request_id`.
- **Find many contacts' emails and details** (`enrich_contacts`): up to 250 at once, each a record of the same fields;
  Dropcontact can also send the results to an address when they are ready (`callback_url`).
- **Get the enriched contacts** (`get_results`): the contacts of a request, with each email found and its
  qualification (how sure Dropcontact is: `nominative@pro` and the like). Until the whole request is done it fails,
  in Dropcontact's words ("try again in 30 seconds"): give the step a retry, or a wait before it; or ask for the
  contacts done so far (`done_so_far`), each with its `status`.
- **Check credits** (`get_credits`).
- **Look up**, **set** and **stop** the account's own address for results (`get_webhook`, `set_webhook`,
  `delete_webhook`), used when a request names none.

Sending contacts spends the account's credits, so those actions are writes: a preview shows them and does not send
them.

## Not done

- A trigger for the results Dropcontact sends to an address: its deliveries carry no signature, so a workflow's own
  webhook trigger takes them.
- One step that sends a contact and waits for its result: the app kit has no action that asks again until a job is
  done, so it is two steps.
- The `hashedInputs` tokens.

## Tested

Recorded exchanges (`tests/`), first made from the API description's schemas and then written by hand with made-up
people and companies: every action, the contacts done so far, a request not ready yet (an answer of 200 that fails
the step), and a key refused (401). Not run against a live Dropcontact account.

```sh
openduct app check apps/dropcontact --bar 80
openduct app test apps/dropcontact
```
