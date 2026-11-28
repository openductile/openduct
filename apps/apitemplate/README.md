# APITemplate.io

Make PDFs and images with [APITemplate.io](https://apitemplate.io): invoices, receipts, certificates and social
posts from the account's templates, or PDFs from HTML, Markdown or a web page.

Made by `openduct app from-openapi` from APITemplate.io's own API description,
<https://apitemplateio.s3.ap-southeast-1.amazonaws.com/redoc_apiv2/apitemplateiov2_api.yaml> (Apache 2.0 licence;
APITemplate.io API Reference Version 2.0, OpenAPI 3.0; the file is the one its reference page loads, last changed
2026-11-24, fetched 2026-11-27; one tab in it was read as a space), then finished by hand in our own words. The
vendor's API reference: <https://apitemplate.io/apiv2/>.

## Signing in

- **An API key** (`apitemplate`): in APITemplate.io, **API Integration** shows the account's key. It is sent in the
  `X-API-KEY` header. An account in another region gives it too (`au`, `de` or `us`): its requests then go to
  `rest-au`, `rest-de` or `rest-us.apitemplate.io`.

## Actions

- **Make a PDF from a template** (`create_pdf`, the default): the template filled with `data` (a record, as the
  template names its fields).
- **Make an image from a template** (`create_image`): an image template with its layers changed (`overrides`: each
  names a layer and what to set), as JPEG and PNG.
- **Make a PDF from HTML** (`create_pdf_from_html`), **from Markdown** (`create_pdf_from_markdown`) and **of a web
  page** (`create_pdf_from_url`), with page settings (paper size, orientation, margins).
- **Join PDFs into one** (`merge_pdfs`).
- **List templates** (`list_templates`) and **List the files made** (`list_objects`), every page.
- **Remove a file made** (`delete_object`).
- **Check the account** (`get_account`): its plan and how many files it may still make.

Each gives the file's address (`download_url`, or `primary_url` for a joined PDF), kept for good unless `expiration`
says how many minutes; `meta` is a reference of your own, shown with the file in the list. Making a file spends the
account's quota, so those actions are writes: a preview shows them and does not send them. APITemplate.io may answer
200 with `status: error`; the step then fails in its words.

## Not done

- The PDF itself as the answer (`export_type=file`) and base64; the account's own storage (post actions to S3, R2 or
  Azure); making a file later and being called back (`async`, `webhook_url`); e-invoices (Factur-X, ZUGFeRD).
- Reading and changing a template's HTML (`get-template`, `update-template`), which APITemplate.io marks as
  experimental.

## Tested

Recorded exchanges (`tests/`), first made from the API description's schemas and then written by hand with made-up
values: every action, an account in the Australian region, the files made over two pages, a template refused (400)
and an answer of 200 that says `status: error`. Not run against a live APITemplate.io account.

```sh
openduct app check apps/apitemplate --bar 80
openduct app test apps/apitemplate
```
