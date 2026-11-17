# Apps

An app is how OpenDuct talks to a service - Stripe, Slack, QuickBooks - from a workflow. Each app is a folder in
[`apps/`](../apps) holding a definition file, a README, and recorded tests. The definition is data, not code: one
engine runs every app. So anyone can add one, and an app can do nothing but what its file says: it reaches only its
own hosts, gets secrets only from a credential, and reads nothing on the server.

A workflow step names the app, the action and the credential, then the action's inputs:

```yaml
steps:
  - name: welcome
    slack: { credential: team-slack, channel: "#sales", text: "New customer {{ value.email }}" }
  - name: customer
    stripe: { action: create_customer, credential: shop-stripe, email: "{{ value.email }}" }
```

A step's result is what the action gives; a list action gives every record of every page, up to its most.

## Build an app in 30 minutes

[The tutorial](tutorials/build-an-app.md) builds a real one, ntfy, step by step.

1. **Start it.** `openduct app new my_crm` makes `apps/my_crm/` with a definition, a README and a recorded test that
   already pass.
2. **Describe the service** in `apps/my_crm/app.yaml`: its hosts and base address, how it signs in, and one action
   at a time. The service's API reference says what each needs.
3. **Check it** as you go: `openduct app check apps/my_crm` - every name, host, template and expression, each mistake
   with where it is.
4. **Try an action for real**, with a credential of your own in a file only you may read:

   ```sh
   printf 'api_key=...\n' > my-key.env && chmod 600 my-key.env
   openduct app try apps/my_crm list_contacts --credential-file my-key.env            # shows the request
   openduct app try apps/my_crm list_contacts --credential-file my-key.env --send     # sends it
   ```

   `--send` runs the action as a workflow step runs it: a token fetched or renewed as its sign-in needs, every page
   of a list. For a sign-in at the vendor, give a refresh token in the credential file and the install's registration
   with `--registration-file reg.env` (`client_id=`, `client_secret=`); a token the vendor renews is written back into
   the credential file, since vendors that give a new refresh token each time take the old one no more.

5. **Record it.** For each action, a file in `tests/` says what it sends and what the service answered (take the
   answer from `try --send` or the API reference, with any real data changed). `openduct app test apps/my_crm` runs
   them with no account and no network - what CI runs on every pull request.
6. **Use it.** `openduct start --apps-dir apps` adds your apps to the studio. To share it, open a pull request with
   the folder; it is reviewed against the checklist below.

## The definition

```yaml
app: shop                        # the id: lowercase letters, digits, '_' (a step is named after it)
name: Shop                       # as people know it: what messages say
category: commerce
description: Customers and orders.
docs: https://shop.example/docs/api
hosts: [api.shop.example]        # the only hosts its addresses may name ("*.shop.example" for any subdomain)
base_url: https://api.shop.example/v1
base_url_field: base_url         # optional: a credential field that, given, replaces base_url (a self-hosted copy)
sign_in:
  - id: shop                     # a credential kind: unique among all apps
    label: An API key
    fields:
      - { name: api_key, label: The API key, secret: true }
      - { name: base_url, label: Another address, optional: true }
    headers: { Authorization: "Bearer {{ credential.api_key }}" }
default_action: create_customer  # optional: for a step that names no action
error: value.error.message        # what a refusal says (an expression over the answer)
actions:
  create_customer:
    label: Create a customer
    write: true                  # it changes something: a preview shows it and does not send it
    method: POST
    path: /customers
    body: form                   # json (the default), form, or none
    inputs:
      - { name: email, label: Email, required: true }
      - { name: name, label: Name }
    send: { email: "{{ input.email }}", name: "{{ input.name }}" }
    result: value                # the step's result (by default the whole answer)
  list_orders:
    label: List orders
    method: GET
    path: /orders
    query: { limit: "100", status: "{{ input.status }}" }
    inputs:
      - { name: status, label: Only orders of this status }
    items: value.data            # a list action: the records of one page
    next: "value.has_more ? {'starting_after': value.data[size(value.data) - 1].id} : null"
    max_pages: 10
```

- **Templates and expressions** are [CEL](expressions.md), as everywhere in OpenDuct. Text in `{{ }}` is worked out:
  `input` is the step's inputs, `credential` the credential's fields, `page` what `next` gave. `result`, `items`,
  `next` and `error` read the answer as `value` (and `status`, and `page`: what was asked for the page just read,
  null for the first - for a service that says nothing of a next page, such as WooCommerce's
  `size(value) < 100 ? null : {'page': (page == null ? 1 : int(page.page)) + 1}`). A template that is one `{{ }}` alone keeps its value's
  type (a number stays a number).
- **A field left empty** - an optional input not given - is left out of a JSON body and of the query.
- **Sign-ins.** Fields go on requests as `headers`, `query`, or `basic: {username, password}`. A webhook app's
  credential is its address: `url_field: url`. OAuth 2.0: `oauth2: {authorize, token, scopes}` - each install
  registers with the vendor itself (see the app's README).
- **Pages.** `next` gives the next page's query parameters (a record, taking the place of the action's own of the
  same name), or its whole address (text; it must be on the app's hosts), or null when there are no more. Besides
  `value`, `status` and `page`, it may read the answer's `headers` (names in lowercase) and `links`, its Link header's
  addresses by rel: Shopify's `next: links.next`.
- **What differs by sign-in** - Slack's bot token posts to its API, its webhook to the webhook's address - goes in
  `by: {<sign-in id>: {path, url, send, headers, result, error}}`.
- **Addresses from a credential.** `base_url` may name the credential's fields - Shopify's
  `https://{{ credential.shop }}.myshopify.com/admin/api/2025-01`, Zendesk's `https://{{ credential.subdomain }}.zendesk.com/api/v2`
  - and the address is still checked against `hosts` (`*.myshopify.com`) as each request is made. A sign-in may have
  a `base_url` of its own (PayPal's sandbox). A self-hosted service (WooCommerce) has none of its own:
  `base_url_field` names the field the whole address is in.
- **Empty fields, at any depth**, are left out of a JSON body, as is a record left empty by that. An input the action
  needs that comes empty when it runs - a value the event did not have - fails the step with its name, rather than
  sending an address such as `/forms//responses`.
- **A next page given as a whole address** (Calendly, Twilio, Microsoft Graph, Zendesk) is asked as it is: the
  action's query is not added again. `page_in: body` puts a next page's values in the JSON body instead (Notion).
- **Refusals.** An answer of 400 or more fails the step with the app's name, the status, and what `error` says (or the
  answer's own `error.message`, `message` or `description`). An answer below 400 for which `error` gives text fails
  the step too (Slack answers 200 with `ok: false`).

## Signing in with OAuth 2.0

Some services let a person sign in at the service itself rather than hand over a key (Google, Microsoft 365,
QuickBooks, Xero). OpenDuct takes no part in that sign-in beyond the install's own registration:

1. **The install registers with the vendor**, once. In the vendor's developer pages, the install's admin creates an
   app (an "OAuth client") and gives it the callback address shown under **Settings → App sign-ins** -
   `https://<your studio>/oauth/callback`. The vendor gives a client id and a client secret; the admin enters them
   there. They are sealed, and the secret is never shown again. Each app's README says where in the vendor's pages
   this is, and which permissions (scopes) to allow.
2. **A workspace connects its account**: under Credentials, **Connect** sends the person to the vendor's sign-in
   page; back from it, the tokens are kept as the workspace's credential, sealed. Each workspace - each client of an
   agency - connects its own account through the install's registration.
3. **Tokens are refreshed** as they run out, and once more if the vendor stops taking one; the new token is kept.

In the definition:

```yaml
sign_in:
  - id: google_sheets
    label: Sign in with Google
    oauth2:
      authorize: https://accounts.google.com/o/oauth2/v2/auth
      token: https://oauth2.googleapis.com/token
      scopes: [https://www.googleapis.com/auth/spreadsheets]
      extra: { access_type: offline, prompt: consent }   # what else the sign-in address takes
    headers: { Authorization: "Bearer {{ credential.access_token }}" }
```

A sign-in where a person signs in has no `fields`: its token is OpenDuct's to keep (`access_token`,
`refresh_token`, `expires_at`). PKCE guards the code, and the state is sealed so only the studio that started a
sign-in can finish it.

Some vendors say more on the way back: `callback_fields: {realm_id: realmId}` keeps QuickBooks' company id from the
callback; `after_connect: {url, keep: {tenant_id: "value[0].tenantId"}}` asks once connected and keeps what it says
(Xero's organisation). Both are kept in the credential beside the token.

**Client credentials** (PayPal and the like): the workspace's own client id and secret are traded for a token
whenever one is needed - no sign-in page:

```yaml
    fields:
      - { name: client_id, label: Client ID }
      - { name: client_secret, label: Secret, secret: true }
    oauth2: { grant: client_credentials, token: "https://api-m.paypal.com/v1/oauth2/token", client_auth: basic }
    headers: { Authorization: "Bearer {{ credential.access_token }}" }
```

`client_auth: basic` sends the client id and secret as HTTP basic; by default they go in the form. A store's own
token address may name the credential's fields - Shopify's
`https://{{ credential.shop }}.myshopify.com/admin/oauth/access_token` - and is then checked against the app's `hosts`
before anything is sent, so a field cannot send the secret elsewhere. Only client credentials sign-ins may do this.

## Recorded tests

```yaml
description: A customer created
action: create_customer
credential: { api_key: sk_test_1 }       # its fields; sign_in: names which, the first by default
input: { email: ann@example.com }
exchanges:                               # one for each request (a list: one for each page)
  - sent:                                # each part optional; what is given must match
      method: POST
      url: https://api.shop.example/v1/customers
      query: { limit: "100" }
      headers: { authorization: Bearer sk_test_1 }
      body: { email: ann@example.com }   # JSON, or a form as its names and values
    answer: { status: 200, body: { id: cus_1 } }
result: { id: cus_1 }                    # or error: words the refusal must hold
```

## Review checklist

- One folder: `app.yaml`, `README.md` (what it does, where to get the credential, which permissions), `tests/`.
- Its `hosts` are the vendor's own; nothing else is reached.
- Every action has a recorded test, and a refusal has one; `openduct app test` passes.
- The README's Tested section says what was run against a live account, when, and what was not.
- Writes are marked `write: true`.
- Names and labels in plain words for the person running a business: what it does, not how.
- Answers in tests hold no real person's data.
