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

An app can be written by hand (below), or made from the vendor's API description or MCP server
([Making an app from an API description](#making-an-app-from-an-api-description)) and then finished by hand. A
workspace can add apps of its own in the studio ([Custom apps](#custom-apps)).

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
    output: { id: text, total: number, paid: boolean }   # what each record holds (optional)
```

- **Templates and expressions** are [CEL](expressions.md), as everywhere in OpenDuct. Text in `{{ }}` is worked out:
  `input` is the step's inputs, `credential` the credential's fields, `page` what `next` gave. `result`, `items`,
  `next` and `error` read the answer as `value` (and `status`, and `page`: what was asked for the page just read,
  null for the first - for a service that says nothing of a next page, such as WooCommerce's
  `size(value) < 100 ? null : {'page': (page == null ? 1 : int(page.page)) + 1}`). A template that is one `{{ }}` alone keeps its value's
  type (a number stays a number).
- **A field left empty** - an optional input not given - is left out of a JSON body and of the query.
- **An input not given is not there at all**, and an expression that reads it is null as a whole: a default is
  `has(input.status) && input.status != null ? input.status : 'draft'`, not `input.status == null ? ...`.
- **What it gives.** `output` names the fields of the step's result - of each record, for a list - with their kinds
  (text, number, boolean, list or record), at most 60. It changes nothing a run does; it is what the quality report
  counts, and what the builder can offer later steps.
- **Waiting.** A request may take 30 seconds. An action of a service that answers only when its work is done says
  how long it may wait, in seconds, up to 300: `timeout: 300` (Apify's run-and-wait).
- **Sign-ins.** Fields go on requests as `headers`, `query`, or `basic: {username, password}` (the password
  left out when a vendor takes none). A webhook app's
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
<!-- messaging -->
- **Several requests.** `then:` lists up to three requests made after the action's own, in order, each written as an
  action is (`method`, `path` or a whole `url`, `query`, `body`, `send`, `headers`, `result`, `error`); each reads the
  step's inputs and `input.previous` (what the request before gave) and `input.answers` (what each gave, the action's
  own first). A whole `url` is checked against the app's hosts. The last one's result is the step's. Slack's upload:
  an upload address asked for, the bytes sent to it, the upload finished.
- **A body as it is.** `body: raw` sends `send` as it is: text's bytes, or a run's file (an input of kind `record`
  holding a file an earlier step gave), its bytes read where the run keeps them. A test run sends none.

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

<!-- test runs -->
A recording's `result` is also the action's **example answer**: a test run, which sends nothing, answers the action
with it, so the steps after it read the same shape as in a real run (a recording made with the credential's sign-in
first, then the one named after the action). Made-up values that look like the service's own make the test runs of
workflows that use the app more telling. An action with no recording answers a test run with the request it would
send.

## Triggers

An app can also start workflows: `triggers:` in its definition, beside `actions:`. A trigger either **polls** - asks
the app every so often for a list and starts a run for each item it has not seen before - or takes the app's own
**webhook** deliveries, each checked against the signature the app puts on it. A workflow names one as its trigger:

```yaml
trigger:
  gmail: { when: new_email, credential: shop-mail, q: "in:inbox from:orders@example.com", every: 5m }
```

`when` names the trigger (it may be left out when the app has one), `credential` the credential; the rest are the
trigger's inputs, plain values (there is no event yet to work them out of). A poll also takes `every` (1m to 1d; the
trigger's own by default) and `max_items` (how many items one poll starts runs for, 1 to 500; 50 by default). A run's
`value` is the item, or what the app sent; `trigger.id` tells it from the others, `trigger.app` and `trigger.when` say
what started it.

### A poll

```yaml
triggers:
  new_order:
    label: A new order
    inputs:
      - { name: status, label: Only orders of this status }
    poll:
      action: list_orders               # an action of the app's that lists (it has items)...
      input: { status: "{{ input.status }}" }
      # ...or a request of its own, written as an action is: method, path, query, items, next, error ...
      id: item.id                       # what tells one item from another (over item and index)
      newest_first: true                # the app lists the newest first: listing stops at a page with one seen
      each: { action: get_order, input: { order_id: "{{ item.id }}" } }   # each new one read in full (optional)
      event: "{'id': item.id, 'total': item.total}"                       # what a run gets (by default, the item)
      every: 5m                         # how often it asks unless the workflow says (1m to 1d)
```

- **What is new.** An item is new when its `id` was not seen before; with `version` (an expression: `item.updated_at`,
  or `item` for the whole of it) an item seen before whose version changed is new again ("a row changed"). New items
  run oldest first, up to `max_items` a poll; the rest are left for the next one.
- **The first poll starts from now.** It notes what is there, and runs nothing.
- **A cursor**, for an app that is asked "what came after this": `cursor` is worked out after each poll over `items`
  (those dealt with, oldest first) and `cursor` (the one before), and the requests read it as `input.cursor` -
  Telegram's `items[size(items) - 1].update_id + 1`, Slack's newest `ts`. One that gives null, or (when nothing was
  dealt with) cannot be worked out, keeps the one before. `input.now` is the poll's time, for an app asked "what
  changed since".
- **Rows under a header.** `header_row: true` makes the first item the names of the rest: each row after it is a
  record of them, with its `row_number` (Google Sheets).
- **What is seen is kept**, for each workflow, in the same batch as the runs it starts (on the workflow's own topic):
  a restart or a node's loss neither runs an item twice nor passes one by, and in a cluster one node polls at a time.
  The newest 5,000 items are remembered; a poll lists at most 5,000.
- A trigger's requests read `input`: the trigger's settings (each declared input, null when not given) and, beside
  them, `cursor`, `now`, `item` (for `each`) and `workflow`.

### A webhook

```yaml
triggers:
  paid:
    label: A payment
    inputs:
      - { name: events, label: Which events, required: true }
    webhook:
      signature:
        header: shop-signature          # the header the signature comes in
        pairs: { signature: v1, time: t }   # it holds names and values: t=1700000000,v1=5257a8...
        signed: "{time}.{body}"         # what is signed: {body}, {time}, {address}, {method} and text (the body alone by default)
        encoding: hex                   # or base64
        # prefix: "sha256="             # what comes before the signature, when it is the whole header
        # time_header: shop-timestamp   # a header the time is in (seconds; time_unit: milliseconds or text)
        # algorithm: hmac_sha256        # hmac_sha1, or token: the header is the secret itself
        # tolerance: 5m                 # how old a delivery may be, when it carries its time (at most 1h)
      secret: webhook_secret            # the credential field(s) the signing secret is in
      id: value.id                      # what tells one delivery from another: one seen again is not run again
      only: "value.type in input.events.split(',')"   # the others are answered, and nothing runs
      event: value.data                 # what a run gets (or events: a list, each item a run)
      register:                         # requests that give the app the address when the workflow is turned on
        - method: POST
          path: /webhooks
          send: { url: "{{ input.address }}", secret: "{{ input.secret }}" }
      keep: { webhook_secret: input.secret, webhook_id: value.id }   # kept, sealed, for this workflow
      unregister:                       # and take it away when it is turned off
        - { method: DELETE, path: "/webhooks/{{ input.registered.webhook_id }}" }
```

- **Signed, or refused.** A delivery with no signature, one that does not match, and one signed longer ago than the
  trigger allows (or later than now) are refused with `401`, and run nothing. A delivery whose `id` was seen before
  by the node is answered and not run again. The signature is checked over the body exactly as it came.
- **The secret** is a field of the credential: one kept on registering this workflow (`<field>@<workflow>` in the
  credential, sealed with it) comes before one a person entered (a webhook made in the app's own pages).
- **Registering.** `register` requests read `input.address` (the workflow's hook, as the studio is reached),
  `input.secret` (a random secret made for it, for an app that is told the secret it signs with), `input.workflow`
  and `input.previous` (what the request before gave). `keep` is worked out over the last answer (`value`) and
  `input`. A registration the app refuses keeps the workflow off, in the app's words. An app whose webhooks are set
  up in its own pages has none: `setup` says what to do there, shown beside the address.
- **Checks of the address.** `challenge: {when, answer}` answers a signed request that asks for something back
  (Slack's `url_verification`, Discord's PING); `confirm: {token, field, answer}` answers a GET whose token must equal a credential
  field (Meta's `hub.challenge`); `ping` passes an unsigned request that only checks the address (WooCommerce's).
<!-- messaging -->
- **Ed25519.** `algorithm: ed25519` checks a signature (hex, or `encoding: base64`) over what `signed` names with the
  public key the secret field holds, in hex - Discord's: `{header: x-signature-ed25519, algorithm: ed25519,
  time_header: x-signature-timestamp, signed: "{time}{body}"}`.
- **A secret in the body.** `algorithm: body_token` with `token:` (an expression over the body, giving the secret or a
  list of them, each of which must be the trigger's) checks an app that puts the secret it was given in each
  delivery - Microsoft Graph's `clientState`: `token: "value.value.map(n, n.clientState)"`. It takes no header.
- **A check answered back.** `echo:` (an expression over `query`, `headers` and the body) answers an unsigned request
  that asks for something back as plain text - Graph's `?validationToken=` - and runs nothing; it is answered even
  while the workflow is being turned on, as Graph checks the address then. Up to 2 KB, and refused when it holds
  `<`, `>`, quotes or `&`, which a page would read as markup.
- **The answer to a delivery that runs.** `answer:` (an expression over the delivery) is what the app is answered
  with once its runs are kept: JSON, or text. Discord wants an interaction answered within three seconds -
  `{'type': 5}`, the bot is thinking - and the workflow answers it fully later.
- **Registrations that run out.** `renew:` (requests, as `register`'s) and `renew_every: 20m` keep a registration
  going: made once in a cluster at each time (a schedule's), reading `input.registered` and `input.now` (the time, as
  RFC 3339: `{{ string(timestamp(input.now) + duration('55m')) }}`). When they fail - the app has let the registration
  go - the address it was registered at is registered again, when `keep` keeps it as `address`. Graph's
  subscriptions: made for 55 minutes, renewed every 20.

### Recorded tests of triggers

A recording with `trigger:` is of a trigger; `openduct app test` runs it as it runs the actions':

```yaml
trigger: new_order                       # a poll: what it asks, what the app answered, the runs it starts
credential: { api_key: sk_test_1 }
seen: { cursor: null, items: [[o1, ""]] }   # what the polls before saw; leave it out for the first poll
exchanges: [ ... ]
runs: [ { id: o2, total: 12 } ]          # each run's value, oldest first (ids: their ids; cursor: the next cursor)
```

```yaml
trigger: paid                            # a delivery, signed as the app signs it
credential: { api_key: sk_test_1, webhook_secret: whsec_1 }
input: { events: payment.succeeded }
delivery: { headers: { shop-signature: "t=1700000000,v1=..." }, body: '{"id": "evt_1", ...}', at: 1700000010 }
runs: [ ... ]                            # or answer: (a challenge), passed: true, refused: words
```

`register: true` (or `unregister: true`) with `workflow`, `address`, `exchanges` and `kept` records registering;
`confirm: <query>` with `answer` records a GET check. A recording registers with the secret `recorded-secret` and at
the time 2023-11-14T22:13:20Z, so what it sends of them can be recorded; `renew: true` records a renewal. A delivery
that runs, of a trigger with `answer`, records both its `runs` and its `answer`. Make each recording's signature as the app's documentation
says, with a tool of your own - not with OpenDuct - so the recording checks the definition against the app.

## Making an app from an API description

Most vendors publish an OpenAPI (or Swagger) description of their API. `openduct app from-openapi` reads one and
writes an app's folder - its definition, a README and recorded tests - checked as it is made:

```sh
openduct app from-openapi https://vendor.example/openapi.json --name vendor          # or a file: ./openapi.yaml
openduct app check apps/vendor --bar 80
openduct app test apps/vendor
```

- **It reads** OpenAPI 3.0 and 3.1, and Swagger 2.0, as JSON or YAML: at most 32 MB (10 MB fetched from an
  address), nested at most 100 deep, at most 3 million values, 5,000 operations; YAML aliases are refused. A
  description at an address is fetched as an HTTP step fetches one: an address on this machine's own network is
  refused unless `--allow-private-network`. `$ref`s are followed within the description; one into another file is
  listed as not done.
- **Where it reaches**: the first https server the description names (a server variable at its default, said in the
  README); `--base-url` gives another. Its host is the app's only host.
- **How it signs in**, from the security schemes: an API key in a header, the query or a cookie; a bearer token; a
  user name and password; OAuth 2.0's authorization code (a person signs in at the vendor, with the scopes the
  actions need, and the README says how to register the installation's OAuth app) and client credentials. Keys and
  tokens come first. OpenID Connect, the implicit and password grants, mutual TLS and digest are listed as not done.
  A description that names no scheme needs `--sign-in bearer`, `basic`, `header:<name>` or `query:<name>`: nothing
  is guessed.
- **Which operations become actions**: up to `--actions N` (25 by default, at most 100), chosen as below;
  `--only tag,...` keeps those with one of the tags, `--choose name,...` names them exactly.
- **Each action** gets its inputs from the path, the query, the headers and the JSON or form body, named plainly
  (`folderId` is `folder_id`); a body of one record (`{data: {...}}`) and JSON:API's `attributes` are opened so their
  fields are inputs; a field that may take one value (an enum of one, `const`), and a required parameter with a
  default (Klaviyo's `revision`), is sent as that value. Its label is the operation's summary, its description the
  description's first sentence, its `output` the answer's fields. Anything but GET is a write.
- **Pages**, when the description shows them: the `Link` header; a next page's address in the answer; a cursor the
  answer gives back (`next_cursor`, `pages.next.starting_after`, ...); the last record's id while the answer says
  `has_more`; a page number; an offset. The page size is the most the description allows, up to 100. A list whose
  description shows no way to the next page reads one.
- **Recorded tests**, one for each action and a refusal, made from the description's schemas with values that fit
  them (an enum's first, a format's example, `ann@example.com` for an e-mail - never the vendor's own example
  people): what each request must send, worked out from the description and not from the definition, and an answer
  that fits. A list whose pages are followed is recorded over two pages. `openduct app test` passes them with no
  account; they do not prove the vendor answers so - try it live with `openduct app try --send`.
- **Not done** - file uploads (multipart), XML and other bodies, callbacks and webhooks, cookies, lists in the query
  sent once for each value, nested fields of a form, deprecated operations - is listed in the README, never
  guessed. The vendor's webhooks become a trigger written by hand ([Triggers](#triggers)).

The app is a start: read its README's Not done, rename what reads oddly, add `result:` where the answer is wrapped,
and record a refusal in the vendor's own words. Made again with `--force`, its definition, README and the recordings
it made are written over; recordings written by hand stay.

### Which operations become actions

Each operation is scored: what it does (list 60, look up 50, create 50, change 40, read 35, another action 25,
remove 15), a summary 10, a description 5, 3 for each other operation on the same first part of its path (up to
18), less 6 for each path parameter beyond the fewest. Deprecated operations, file uploads and XML bodies are not
offered. The main resources come first - those with the most operations, then those the description names first -
each giving up to a fifth of the actions (at least 3), one of each kind (list, look up, create, change, remove)
before a second of any; what room is left goes to the best of the rest. An action is named for what it does and to
what (`list_files`, `get_file`, `copy_file`), qualified by what it is under when two would be alike
(`list_folder_comments`), then by the operation's id.

## Making an app from an MCP server

A vendor's MCP server already lists its tools, each with a schema of its arguments. `openduct app from-mcp` makes an
app whose actions are those tools:

```sh
printf 'token=...\n' > mcp-token.env && chmod 600 mcp-token.env
openduct app from-mcp https://mcp.vendor.example/mcp --name vendor_mcp --token-file mcp-token.env
openduct app test apps/vendor_mcp
```

Each tool is an action that names it with `mcp:`; its inputs are the tool's arguments, its output the tool's output
schema, and a tool the server marks as only reading is not a write. Such an action makes no request of its own: the
engine calls the tool through the MCP client the `mcp` step uses (either protocol version, through the run's
effects: the private-network rule, test runs and previews hold), signed in with the one header - or the user name and
password - its sign-in makes. Its result is the tool's structured answer, else its text read as JSON, else its text;
a tool that answers with an error fails the step in its words.

```yaml
base_url: https://mcp.vendor.example/mcp     # the server's MCP address
actions:
  open_issue:
    label: Open an issue
    write: true
    mcp: create-issue                        # the tool, as the server names it
    inputs: [{ name: title, label: Title, required: true }]
    send: { title: "{{ input.title }}" }     # its arguments (by default, the inputs as they are)
```

Its recordings say what the tool is called with and what it answered:

```yaml
action: open_issue
credential: { token: test-token }
input: { title: Typo on the About page }
call: { tool: create-issue, arguments: { title: Typo on the About page }, headers: { authorization: Bearer test-token } }
answer: { structuredContent: { id: B-7 } }   # or content: [{ type: text, text: ... }], isError: true
result: { id: B-7 }                           # or error: words the refusal holds
```

Signing in to an MCP server at the vendor (OAuth) is not done: the app takes a token the server accepts.

## How finished an app is: the quality report and the bar

`openduct app check` says, for each app, how many actions it has and how many of them have a description, an
output and a recorded test, whether a refusal is recorded, how it signs in and which hosts it reaches - and a score
out of 100:

| What | Points |
|---|---|
| Actions with a description | 20, in proportion |
| Actions with an output | 20, in proportion |
| Actions with a recorded test | 30, in proportion |
| A recorded refusal | 10 |
| Hosts declared | 5 |
| An https link to the vendor's API reference (`docs`) | 5 |
| A README | 5 |
| Every sign-in labelled | 5 |

**The bar is 80.** An app that OpenDuct ships meets it - `openduct app check apps/<id> --bar 80` fails below it - and
`openduct app test` passes. Later agents finishing generated apps hold to the same bar; the score says what is left
(`to do: ...`). The score measures what a definition says, not whether the vendor answers as it does: the README's
Tested section says that.

## Custom apps

The apps in `apps/` (and an installation's `--apps-dir`) are in every workspace. A workspace can also have apps of its
own - a custom app - made in the studio's **Apps** page ("Add an app") with the same generator: from an API
description's address, an uploaded description file, an MCP server's address, or an app.yaml pasted in. The person
picks the app's name, sees the actions proposed (ranked as above, the best ticked), unticks any, picks the sign-ins,
sees the quality report and what is not done, and saves ([studio](studio.md#custom-apps)).

- **A custom app is its workspace's alone**, kept on the log with the workspace's other records, the same on every
  node. Its steps are offered, and read, in that workspace's workflows only; another workspace can neither see it nor
  name it in a step. Its sign-ins are kinds of credential of its own (the sign-in's id and a mark of the workspace and
  app), sealed as any app's are, which no other workspace may use.
- **It passes the checks a shipped app passes**, and more: it reaches only the hosts it declares (no address from a
  credential), with the private-address rule of every request; secrets come only from credentials; at most 100
  actions, a description of at most 1,000 characters, a definition of at most 512 KB; at most 200 custom apps a
  workspace. Its name is never a step's or an app's OpenDuct carries.
- **It has no triggers yet**, and a sign-in at the vendor (OAuth's authorization code) is for the apps an
  installation adds, since it needs the installation's own registration: a custom app signs in with a key, a token,
  a user name and password, or a client ID and secret.
- **Editors and admins** add, change, export (as `app.yaml`) and remove them; viewers see them. A workflow that a
  workflow runs (`run_workflow`) does not yet find its workspace's custom apps.

The API: `POST /api/apps/custom` (`{from: openapi | mcp | definition, url | text, name, chosen, sign_ins, save}`: a
preview, or with `save: true` the app kept), `GET /api/apps/custom`, `GET`, `PUT` and `DELETE /api/apps/custom/<id>`,
and `GET /api/apps/custom/<id>/app.yaml`.

## Review checklist

- One folder: `app.yaml`, `README.md` (what it does, where to get the credential, which permissions), `tests/`.
- Its `hosts` are the vendor's own; nothing else is reached.
- Every action has a recorded test, and a refusal has one; `openduct app test` passes.
- It meets the bar: `openduct app check apps/<id> --bar 80`.
- The README's Tested section says what was run against a live account, when, and what was not.
- Writes are marked `write: true`.
- Names and labels in plain words for the person running a business: what it does, not how.
- Answers in tests hold no real person's data.
