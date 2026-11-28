# Intercom

The intercom API reference.

Made by `openduct app from-openapi` from https://github.com/intercom/Intercom-OpenAPI/blob/41d37651259d/descriptions/2.18/api.intercom.io.yaml (MIT licence) (OpenAPI 3.0) on 2026-11-27. The vendor's API reference: <https://developers.intercom.com>.

## Signing in

- **An access token** (`intercom`): an access token or API key from Intercom's pages for developers, sent as `Authorization: Bearer`.

## Actions

- **List content import sources** (`list_content_import_sources`): You can retrieve a list of all content import sources for a workspace.
- **List all articles** (`list_articles`): You can fetch a list of all articles by making a GET request to https://api.intercom.io/articles.
- **Scroll over all companies** (`list_scroll`): The list all companies functionality does not work well for huge datasets, and can result in errors and performance problems when paging deeply.
- **List deleted contacts** (`list_deleted`): Fetch a paginated list of the contacts in your workspace that are no longer active, so you can recover ones whose id, email, or external_id you no longer have and then permanently delete them.
- **List all conversations** (`list_conversations`): You can fetch a list of all conversations.
- **Create a content import source** (`create_content_import_source`): You can create a new content import source by sending a POST request to this endpoint.
- **Create an article** (`create_article`): You can create a new article by making a POST request to https://api.intercom.io/articles.
- **Create or Update a company** (`create_company`): You can create or update a company.
- **Create contact** (`create_contact`): You can create a new contact (ie. user or lead).
- **Creates a conversation** (`create_conversation`): You can create a conversation that has been initiated by a contact (ie. user or lead).
- **Retrieve a content import source** (`get_content_import_source`): You can retrieve a content import source.
- **Retrieve an article** (`get_article`): You can fetch the details of a single article by making a GET request to https://api.intercom.io/articles/ .
- **Retrieve a company by ID** (`get_company`): You can fetch a single company.
- **Get a contact** (`get_contact`): You can fetch the details of a single contact.
- **Retrieve a conversation** (`get_conversation`): You can fetch the details of a single conversation.
- **Bulk update contacts** (`update_bulk`): You can bulk update contacts by submitting an array of contact objects with the fields to update.
- **Search for articles** (`get_search`): You can search for articles by making a GET request to https://api.intercom.io/articles/search.
- **Retrieve companies** (`get_companies`): You can fetch a single company by passing in company_id or name.
- **Update a content import source** (`update_content_import_source`): You can update an existing content import source.
- **Update an article** (`update_article`): You can update the details of a single article by making a PUT request to https://api.intercom.io/articles/ .
- **Update a company** (`update_company`): You can update a single company using the Intercom provisioned id.
- **Update a conversation** (`update_conversation`): You can update an existing conversation.
- **Merge two contacts** (`merge_contact`): You can merge one contact into another.
- **Search conversations** (`search_conversation`): You can search for multiple conversations by the value of their attributes in order to fetch exactly which ones you want.
- **Add a tag to an external page** (`tags_external_page`): Apply an existing tag to an external page.

## Not done

- 1 operation left out: a file upload (multipart)
- 251 more operations it could take (made with the best 25: --actions or the studio's list chooses others)
- get_conversation: the list include_part_types in the query (sent once for each of its values)
- get_conversation: the list exclude_part_types in the query (sent once for each of its values)

## Tested

Recorded exchanges made from the API description's schemas (`tests/`): what each request must send, and an answer that fits. Not run against a live Intercom account.

```sh
openduct app check apps/intercom
openduct app test apps/intercom
```
