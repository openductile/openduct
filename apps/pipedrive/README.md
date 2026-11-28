# Pipedrive

Deals, people, organizations, leads, activities and notes of a [Pipedrive](https://www.pipedrive.com) company - and
triggers for new deals, deals changed, new people, organizations and activities.

Made with `openduct app from-openapi` from Pipedrive's own API descriptions, fetched 2026-11-27: API v2 at
<https://developers.pipedrive.com/docs/api/v1/openapi-v2.yaml> for deals, people, organizations, activities,
pipelines, stages and searches, and API v1 at <https://developers.pipedrive.com/docs/api/v1/openapi.yaml> for adding
leads and notes (which v2 does not have), joined into one. They state no licence: only their facts are used, and the
labels and descriptions here are our own. The API reference: <https://developers.pipedrive.com/docs/api/v1>.

## Signing in

- **A personal API token** (a Pipedrive credential): in Pipedrive, your profile, **Personal preferences → API**. It
  acts as you, sent in the `x-api-token` header to `api.pipedrive.com`.

## Actions

- **Create a deal** (`create_deal`, the default), **Look up** (`get_deal`), **Change** (`update_deal`: a new stage
  moves it along) and **Remove a deal** (`delete_deal`), **List deals** (`list_deals`), **Search deals**
  (`search_deals`), **Add a product to a deal** (`add_product_to_deal`).
- **Create a person** (`create_person`), **Look up** (`get_person`), **Change** (`update_person`), **List people**
  (`list_persons`), **Search people** (`search_people`: by name, email or phone).
- **Create an organization** (`create_organization`), **Look up** (`get_organization`), **Change**
  (`update_organization`), **List** (`list_organizations`) and **Search organizations** (`search_organizations`).
- **Create a lead** (`create_lead`) and **Search leads** (`search_leads`).
- **Add an activity** (`create_activity`), **Change an activity** (`update_activity`: mark it done), **List
  activities** (`list_activities`).
- **Add a note** (`add_note`) to a deal, lead, person or organization.
- **List pipelines** (`list_pipelines`) and **List stages** (`list_stages`): their ids, for the deals.

Records come out of Pipedrive's wrapping (`data`); lists and searches follow Pipedrive's cursor, every page up to
1,000. Custom fields go by their keys, in `custom_fields`. A refusal fails the step in Pipedrive's words.

## Triggers

Polls, every 5 minutes, newest first: **A new deal** (`new_deal`), **A deal made or changed** (`changed_deal`: a new
stage, won or lost), **A new person** (`new_person`), **A new organization** (`new_organization`), **A new
activity** (`new_activity`).

## Not done

- **Sign in with Pipedrive** (OAuth): requests then go to the company's own address, which Pipedrive gives in the
  token's answer (`api_domain`); OpenDuct keeps only the token from that answer, so this sign-in waits for that.
- **Pipedrive's webhooks**: Pipedrive guards them with a user name and password (HTTP basic) rather than a signature,
  which OpenDuct's webhook triggers cannot check yet; the polls above stand in.
- Filters Pipedrive takes as a list (several ids or statuses at once), files, products and their variations, mail,
  projects, goals and the rest of the API.

## Tested

Recorded exchanges (`tests/`): those made from the descriptions' schemas, and by hand the searches over two pages, a
refusal in Pipedrive's words, and the five triggers (a first poll, new items, a deal changed). Not run against a live
Pipedrive company.

```sh
openduct app check apps/pipedrive --bar 80
openduct app test apps/pipedrive
```
