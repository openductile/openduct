# Resend

Resend is the email platform for developers.

Made by `openduct app from-openapi` from https://github.com/resend/resend-openapi/blob/90a7e8ecf9ae/resend.yaml (MIT licence) (OpenAPI 3.1) on 2026-11-27. The vendor's API reference: <https://resend.com/docs/api-reference>.

## Signing in

- **An access token** (`resend`): an access token or API key from Resend's pages for developers, sent as `Authorization: Bearer`.

## Actions

- **Retrieve a list of inboxes** (`list_inboxes`): Returns the inboxes of the team, newest first.
- **Retrieve a list of emails** (`list_emails`).
- **Retrieve a list of domains** (`list_domains`).
- **Retrieve a list of contacts** (`list_contacts`).
- **Retrieve a list of broadcasts** (`list_broadcasts`).
- **Create an inbox** (`create_inbox`): Creates an inbox that sends and receives email at an address on one of your domains.
- **Send an email** (`create_email`).
- **Create a new domain** (`create_domain`).
- **Create a new contact** (`create_contact`).
- **Create a broadcast** (`create_broadcast`).
- **Retrieve a single email** (`get_email`).
- **Retrieve a single domain** (`get_domain`).
- **Retrieve a single contact import** (`get_import`).
- **Retrieve a single broadcast** (`get_broadcast`).
- **Retrieve a single inbox by ID or email address** (`get_inbox`).
- **Update an existing inbox by ID or email address** (`update_inbox`): Updates the internal name of the inbox or the name recipients see when mail is sent from it.
- **Update a single email** (`update_email`).
- **Update an existing domain** (`update_domain`).
- **Retrieve a domain claim** (`get_claim`): Retrieve the latest claim for the placeholder domain created by the claim.
- **Update a single contact by ID or email** (`update_contact`).
- **Update an existing broadcast** (`update_broadcast`).
- **Trigger up to 100 batch emails at once** (`batch_email`).
- **Cancel a broadcast** (`cancel_broadcast`): Cancels a broadcast that is queued or scheduled.
- **Add labels to an inbox thread** (`labels_thread`): Applies one or more labels to the thread.
- **Remove an existing contact by ID or email** (`delete_contact`).

## Not done

- 1 operation left out: a file upload (multipart)
- 4 operations left out: deprecated
- 114 more operations it could take (made with the best 25: --actions or the studio's list chooses others)
- the vendor's webhooks and callbacks: a trigger for them is written by hand (docs/apps.md, Triggers)

## Tested

Recorded exchanges made from the API description's schemas (`tests/`): what each request must send, and an answer that fits. Not run against a live Resend account.

```sh
openduct app check apps/resend
openduct app test apps/resend
```
