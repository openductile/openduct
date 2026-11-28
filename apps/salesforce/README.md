# Salesforce

Leads, contacts, accounts, opportunities, cases, tasks and campaign members of a [Salesforce](https://www.salesforce.com)
org - and any other record, standard or custom, by its API name - with queries, searches and triggers for records
made or changed.

Written by hand from Salesforce's own reference, fetched 2026-11-27: the
[REST API Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm)
(Summer '26, API version 67.0), its [Object Reference](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/)
for the standard fields, the sign-in endpoints of `login.salesforce.com/.well-known/openid-configuration` and the
[identity URL](https://developer.salesforce.com/docs/atlas.en-us.mobile_sdk.meta/mobile_sdk/oauth_using_identity_urls.htm)
answer. Salesforce's documentation is not openly licensed: only its facts are used, in our own words.

## Signing in

Every request goes to the org's own address, `MyDomainName.my.salesforce.com`. Three ways:

- **Sign in with Salesforce** (a Salesforce sign-in) or **Sign in with Salesforce (a sandbox)**: the installation's
  admin creates an **external client app** once, in a Salesforce org's **Setup → External Client App Manager → New
  External Client App**, with OAuth on, the callback address shown under **Settings → App sign-ins**
  (`https://<your studio>/oauth/callback`) and the scopes `api`, `refresh_token` and `id` (the identity service, to
  learn the org's address); then copies its consumer key and secret there. A workspace then **Connects** under
  Credentials and signs in at
  `login.salesforce.com` (or `test.salesforce.com` for a sandbox); once connected, the org's address is read from the
  sign-in's user info (`urls.rest`) and kept with the token. Salesforce gives no time a token lasts: it is renewed
  when Salesforce stops taking it.
- **An external client app's id and secret** (server to server): in the org's own external client app, turn on the
  client credentials flow and name the user it runs as; give the org's address, the consumer key and the consumer
  secret. A token is asked of `https://<your org's address>/services/oauth2/token` whenever one is needed.

## Actions

- **Create a record** (`create_record`, the default), **Look up a record** (`get_record`), **Change a record**
  (`update_record`: only the fields given), **Remove a record** (`delete_record`: to the recycle bin): of any kind, its
  fields by their API names (`LastName`, `Company`, `Customer_Number__c`).
- **Create or change a record by an outside id** (`upsert_record`): by an external id field; or only change it.
- **Find records with a query** (`query`): SOQL, the first 2,000 records it finds.
- **Search with SOSL** (`search`) and **Search records for words** (`find_records`: plain words, of one kind or all).
- **Describe a kind of record** (`describe_object`) and **List the kinds of record** (`list_objects`).
- **Create a lead** (`create_lead`), **Create a contact** (`create_contact`), **Create an account**
  (`create_account`), **Create an opportunity** (`create_opportunity`), **Open a case** (`create_case`), **Add a task**
  (`create_task`), **Add someone to a campaign** (`add_to_campaign`): the fields people fill in most, in plain names.

A refusal says Salesforce's message and code, such as `INVALID_FIELD` or `REQUIRED_FIELD_MISSING`.

## Triggers

- **A new record** (`new_record`): each record of a kind made - a lead, contact, opportunity, case, or a custom
  object - asked for every 5 minutes (the newest 200 by `CreatedDate`), each read in full.
- **A record made or changed** (`changed_record`): the same by `LastModifiedDate`; a record changed again runs again.

Salesforce's own pushes (outbound messages, change data capture, platform events) are SOAP or streaming, not
webhooks: polling is how the triggers learn of records.

## Not done

- A query's further batches past its first (Salesforce gives the next as a path, `nextRecordsUrl`, on the org's
  address, and OpenDuct's pages follow only whole addresses or query values): write `LIMIT`, `ORDER BY` and a `WHERE`
  on a date to page by hand.
- Composite requests, bulk jobs (Bulk API 2.0), files and attachments, reports, approvals, Apex REST resources.
- Orgs outside `*.my.salesforce.com` (Government Cloud and the like).

## Tested

Recorded exchanges built from the REST API Developer Guide (`tests/`): each action, refusals in Salesforce's words (a
list of a message and a code), the sandbox and server-to-server sign-ins, and both triggers (a first poll, new leads
read in full, a contact changed again). Not run against a live Salesforce org.

```sh
openduct app check apps/salesforce --bar 80
openduct app test apps/salesforce
```
