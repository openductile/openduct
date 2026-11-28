# ServiceNow

Open, find and change incidents - and any other table's records: change requests, catalog tasks, users, the CMDB -
in a [ServiceNow](https://www.servicenow.com) instance, from a workflow's steps; and start workflows when incidents
are opened or changed.

Written by hand, in our own words, from ServiceNow's documentation (read 2026-11-27): the
[Table API](https://www.servicenow.com/docs/r/api-reference/rest-apis/c_TableAPI.html), the
[REST API overview](https://www.servicenow.com/docs/bundle/zurich-api-reference/page/integrate/inbound-rest/concept/c_RESTAPI.html)
(basic authentication or OAuth), and the platform security pages on the OAuth client credentials grant (token at the
instance's `oauth_token.do`). We found no OpenAPI description of the Table API outside each instance's own REST API
Explorer.

Moving a whole table in or out, record by record as it changes, is the `servicenow` pipeline connector's work; this
app is for single steps in a workflow.

## Signing in

The instance is named as in its address: `acme` for `acme.service-now.com`.

- **A user name and password** (`servicenow`): a user of the instance kept for the integration, with the roles its
  steps need on the tables they use. Sent as HTTP basic.
- **A client ID and secret** (`servicenow_client`, OAuth client credentials): in the instance, **System OAuth →
  Application Registry**, create an OAuth API endpoint for external clients with the client credentials grant, and an
  OAuth application user whose roles its steps use. The instance must allow the grant (the system property
  `glide.oauth.inbound.client.credential.grant_type.enabled`). The ID and secret are traded at the instance's
  `oauth_token.do` for a token whenever one is needed.

## Actions

- **Open an incident** (`create_incident`, the default): its short description, description, caller, category,
  urgency, impact, group and person to work on it.
- **Change an incident** (`update_incident`): its state, urgency, who works on it; a comment the caller sees or a work
  note they do not; the resolution code and notes when resolving.
- **Look up an incident** (`get_incident`) by its number, and **Find incidents** (`list_incidents`) by an encoded query
  (`active=true^priority=1`, as a list's filter gives it with **Copy query**), the newest first.
- **Find records of a table** (`list_records`), **Look up a record** (`get_record`), **Add a record**
  (`create_record`), **Change a record** (`update_record`) and **Remove a record** (`delete_record`), for any table.

People, groups and other references are their `sys_id`s; with `display_values` they are given, and shown, as people
see them (a group's name, a state's label). Lists are read 100 at a time, every page up to 1,000 records.

## Triggers

- **A new incident** (`new_incident`): each incident opened, or only those an encoded query picks.
- **An incident changed** (`updated_incident`): each incident opened or changed - its `sys_updated_on` is newer.
- **A new record in a table** (`new_record`): each record added to the table named.

Each asks the instance every 5 minutes for the newest 100; the first poll notes what is there and runs nothing.

## Not done

- A person signing in at the instance (OAuth's authorization code): its sign-in page is the instance's own
  (`<instance>.service-now.com/oauth_auth.do`), and the app kit takes only a fixed sign-in address for that grant.
- An instance at an address other than `<name>.service-now.com`.
- Attachments, the Import Set, Aggregate and Service Catalog APIs, and ServiceNow's own outbound REST messages (to
  call a workflow's webhook, set one up in the instance, Business Rule or Flow Designer).

## Tested

Recorded exchanges written from the Table API reference with made-up records (`tests/`): every action with each
sign-in kind, names given as display values, a number no incident has, incidents over two pages, a table the
instance does not have refused (400, with ServiceNow's message and detail), and the three triggers. Not run against
a live instance.

```sh
openduct app check apps/servicenow --bar 80
openduct app test apps/servicenow
```
