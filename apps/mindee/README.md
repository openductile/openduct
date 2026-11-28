# Mindee

Read invoices, receipts, IDs and other documents into fields with [Mindee](https://www.mindee.com), using the models
of your Mindee account (the API v2).

## Signing in

- **An API key** (`mindee`): at <https://app.mindee.com>, **Settings → API Keys → Create API Key**. It is sent as the
  whole `Authorization` header, as Mindee's API v2 takes it. For the trigger, the credential also holds a model's
  webhook **Signing Secret** (below).

## How a document is read

Mindee reads a document in the background:

1. **Read a document into fields** (`extract_document`) sends it (a web address, or the file in Base64) to a model;
   the answer is a **job**.
2. A workflow waits a few seconds (a `wait` step), then **Get a job** (`get_job`): `Processing`, `Failed` or
   `Processed`, with its `result_url` once done.
3. **Get the fields read** (`get_extraction`) with the result's id (the end of `result_url`): each field the model
   reads, by its name, under `result.fields` (`result.fields.total_amount.value`).

Or the model's webhook sends the result when it is ready: the **A document was read** trigger.

## Actions

- **Read a document into fields** (`extract_document`), **Get the fields read** (`get_extraction`).
- **Sort a document** (`classify_document`), **Get what kind a document is** (`get_classification`).
- **Split a file into documents** (`split_document`), **Get where each document is** (`get_split`).
- **Find the documents in a picture** (`crop_document`), **Get the documents found in a picture** (`get_crop`).
- **Read a document's text** (`read_text`), **Get the text read** (`get_text`).
- **Get a job** (`get_job`), **List models** (`list_models`).

## Triggers

- **A document was read** (`document_read`): each result a model's webhook sends, checked against its `X-Signature`
  (HMAC-SHA256 of the body, in hex, with the webhook's Signing Secret). The webhook is made in Mindee's pages: open
  the model, **Webhooks → Add Webhook**, give the workflow's address, copy the **Signing Secret** into the credential,
  and send documents with the webhook's id (`webhook_ids`).

## Source

Facts (host, paths, fields, sign-in) from Mindee's own API v2 description, <https://api-v2.mindee.net/openapi.json>
(OpenAPI 3.1, fetched 2026-11-27; no licence stated, so the labels and descriptions here are our own words), its
documentation on keys, polling and webhooks (<https://docs.mindee.com>), and, for the signature, Mindee's official
Python library (<https://github.com/mindee/mindee-api-python/blob/3b503f766ae72c369dc0d2c1566c4afed50175cb/mindee/input/local_response.py>,
MIT licence). Written by hand after `openduct app from-openapi` showed the operations.

## Not done

- Sending a file as an upload (multipart): a file is sent by its web address or in Base64.
- A model's examples (retrieval documents), the data schema sent with a document, the older API v1.

## Tested

Recorded exchanges (`tests/`) made from the API description, with made-up values; the webhook delivery is signed as
Mindee's library checks it, with a tool of our own. Not run against a live Mindee account.

```sh
openduct app check apps/mindee --bar 80
openduct app test apps/mindee
```
