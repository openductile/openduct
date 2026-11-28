# Elasticsearch

Documents in your own [Elasticsearch](https://www.elastic.co/elasticsearch) cluster, or an Elastic Cloud deployment:
found, counted, added, changed and removed; indices made, read and removed; ES|QL queries run. The cluster is the
installation's own: the credential gives its address.

Written by hand from Elastic's own API description,
<https://github.com/elastic/elasticsearch-specification/blob/b16f1b942354293454593720f754ce993a87e63e/output/openapi/elasticsearch-openapi.json>
(OpenAPI 3.0, Apache 2.0, the commit of 2026-11-27), and its API reference,
<https://www.elastic.co/docs/api/doc/elasticsearch/>. `openduct app from-openapi` made a first draft from it; that
draft offered every query-string option of each call and could not send a search's body, so the actions here keep the
inputs a workflow needs and send searches as JSON.

## Signing in

Both give the cluster's address, such as `https://search.example.com:9200` (an Elastic Cloud deployment's
Elasticsearch endpoint, or your own cluster's):

- **An API key** (`elasticsearch`): the key's encoded value, which Elasticsearch gives when the key is made (in
  Kibana, or with the create API key call); sent as `Authorization: ApiKey <encoded>`. Give the key only the indices
  and privileges the workflows need.
- **A user name and password** (`elasticsearch_user`): a user of the cluster's own, sent as HTTP basic.

## Good to know

- A search is written in Elasticsearch's query language, as JSON (`{"match": {"status": "paid"}}`), or as a simple
  search (`status:paid AND total:>100`).
- What is added or changed becomes searchable within about a second; `refresh: wait_for` waits until it is.
- **Find documents** reads up to 1,000 documents, a hundred at a time; **Search** gives one answer, with any
  aggregations.

## Actions

- **Find documents** (`find_documents`): Every document of an index that matches a search, up to 1,000, each with its id and fields.
- **Search** (`search`): One search's whole answer: the hits (10 unless a size is given), how many matched, and any aggregations.
- **Count documents** (`count_documents`): How many documents of an index match a search (all of them if none is given).
- **Look up a document** (`get_document`): One document, by its id, with its fields; found is false if there is none.
- **Look up several documents** (`get_documents`): Several documents of an index at once, by their ids.
- **Add a document** (`add_document`): Adds a document to an index; Elasticsearch gives it an id.
- **Add or replace a document** (`save_document`): Puts a document under the id given, in place of the one there if any.
- **Add a document with an id of its own** (`create_document`): Adds a document under the id given only if no document has it yet; one that does is refused.
- **Change a document** (`update_document`): Changes some fields of a document (the rest stay), or runs a script on it; adds it if asked when it is not there.
- **Delete a document** (`delete_document`): Removes one document, by its id.
- **Delete the documents a search finds** (`delete_matching`): Removes every document of an index that matches a search.
- **Change the documents a search finds** (`update_matching`): Runs a script on every document of an index that matches a search.
- **List indices** (`list_indices`): The cluster's indices, with their health, number of documents and size.
- **Look up an index** (`get_index`): An index's settings, fields (mappings) and aliases.
- **Create an index** (`create_index`): A new index, with its fields (mappings), settings and aliases if given.
- **Delete an index** (`delete_index`): Removes an index and every document in it.
- **Look up an index's fields** (`get_fields`): The fields an index has and their kinds (its mappings).
- **Add fields to an index** (`add_fields`): New fields for an index, each with its kind; fields already there stay as they are.
- **Refresh an index** (`refresh_index`): Makes what was just added or changed in an index searchable now.
- **Change aliases** (`change_aliases`): Adds and removes aliases (other names for indices) in one go.
- **Check the cluster's health** (`cluster_health`): The cluster's health (green, yellow or red), its nodes and its shards.
- **Run an ES|QL query** (`esql_query`): A query in ES|QL, Elasticsearch's piped query language: its columns and rows of values.

## Triggers

- **A new document** (`new_document`): Each document added to an index, the newest asked for every few minutes: the newest by a field that holds each document's time.

**A new document** needs a field that holds each document's time (`@timestamp` in logs and data streams): the newest
hundred are asked for each time, and those not seen before run, oldest first.

## Not done

- Bulk requests (`_bulk`, `_msearch`): their bodies are lines of JSON, which an app does not send yet.
- Scrolls and points in time, for reading more than 1,000 documents of a search.
- Data streams, index templates, lifecycle policies, snapshots, security, machine learning and the cluster's other administration (about 850 more operations).

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Elasticsearch account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Elasticsearch documents it; none was run against a live account.

```sh
openduct app check apps/elasticsearch --bar 80
openduct app test apps/elasticsearch
```
