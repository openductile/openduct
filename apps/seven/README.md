# seven

SMS, voice calls that read a message aloud, WhatsApp and RCS messages, phone number lookups and contacts, through
[seven](https://www.seven.io) (seven.io).

Made by `openduct app from-openapi` from seven's own API description,
<https://github.com/seven-io/api-schemes/blob/adc5f2304db92a0da257c80f410ef3cd9c25b7bd/json/openapi.json> (OpenAPI 3.0;
the repository is MIT-licensed, the description itself says Apache 2.0), then finished by hand: plain names, an API
key sign-in (the description names only OAuth), return codes read as refusals, a trigger. The vendor's API reference:
<https://docs.seven.io/en/rest-api>.

## Signing in

- **An API key** (`seven`): in seven's dashboard, **Developer → API keys**. Sent as `X-Api-Key`.
- **Sign in with seven** (`seven_oauth`): each install registers its own app with seven, once: in the dashboard,
  **Developer → OAuth apps**, with the redirect URL shown in OpenDuct under **Settings → App sign-ins**
  (`https://<your studio>/oauth/callback`) and the scopes `sms`, `voice`, `rcs`, `waba`, `status`, `journal`,
  `contacts`, `groups`, `lookup`, `balance`, `pricing`, `analytics`, `numbers`; enter its client ID and client secret
  there. Its tokens last an hour and are refreshed.

## Actions

- **Send an SMS** (`send_sms`): Sends one or multiple SMS to one or multiple recipients (numbers, or contact or group
  names, separated by commas).
- **Get an SMS's delivery status** (`get_sms_status`): Retrieves dispatch status for given SMS IDs.
- **Call a number and read a message aloud** (`send_voice_call`): Sends a text-to-speech message to a given recipient number.
- **Send a WhatsApp message** (`send_whatsapp`): Sends a WhatsApp message via the WhatsApp Business API.
- **Send an RCS message** (`send_rcs`): Sends a Rich Content Message.
- **List sent messages** (`list_sent_messages`): Retrieves outbound journals for the associated API key; every page.
- **List messages received** (`list_inbound_messages`): Retrieves inbound journals for the associated API key; every page.
- **List replies to your messages** (`list_replies`): Retrieves message replies for the associated API key; every page.
- **List voice calls made** (`list_voice_calls`): Retrieves voice calls for the associated API key; every page.
- **Check a phone number's format** (`check_number_format`): Retrieves number format info.
- **Check a mobile number is live (HLR)** (`lookup_hlr`): Retrieves home location register info.
- **Look up a number's network (portability)** (`lookup_portability`): Retrieves mobile number portability info.
- **Look up a caller's name** (`lookup_caller_name`): Retrieves Caller name info.
- **List contacts** (`list_contacts`): Retrieves contacts.
- **Get a contact** (`get_contact`): Retrieves a contact by its ID.
- **Add a contact** (`create_contact`): Creates a contact.
- **Change a contact** (`update_contact`): Updates a contact.
- **Delete a contact** (`delete_contact`): Delete a contact by its ID.
- **List contact groups** (`list_groups`): Retrieves Groups.
- **Get a contact group** (`get_group`): Retrieves a group by its ID.
- **Add a contact group** (`create_group`): Creates a group.
- **List your phone numbers** (`list_numbers`): Retrieves active phone numbers.
- **Get the account balance** (`get_balance`): Retrieves the account balance for given API key.
- **Prices by country** (`list_prices`): Retrieves pricing information for a single country or all.
- **Usage by day** (`analytics_by_date`): Retrieves analytics grouped by date.

seven answers a message it does not send with a return code (`success`) other than `100` (sent) or `101` (sent to
some recipients); **Send an SMS** and **Call a number** then fail with that code - `500` is too little credit, `900`
a key it does not know, and the rest are listed in seven's SMS reference.

## Triggers

- **A message received** (`new_inbound_sms`), asked every 5 minutes: each SMS that comes in to one of your numbers,
  read from the inbound journal.

## Not done

- seven's webhooks (`sms_mo`, `dlr`, `voice_status`, ...) as a trigger: each delivery is signed with HMAC-SHA256
  over `timestamp\nnonce\nmethod\nURL\nMD5(body)`, the nonce in an `X-Nonce` header; the app kit's signature check
  cannot yet put a second header or an MD5 of the body in what is signed. The inbound journal is polled instead.
- Deleting scheduled SMS (`DELETE /sms` takes a list of ids in a form), sub-accounts, numbers ordering, RCS events
  and WhatsApp templates: 29 more operations of the description.

## Tested

Recorded exchanges made from the API description's schemas and seven's documented answers (`tests/`), with made-up
values; the trigger's poll is recorded too. Not run against a live seven account.

```sh
openduct app check apps/seven --bar 80
openduct app test apps/seven
```
