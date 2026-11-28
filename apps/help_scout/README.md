# Help Scout

Conversations and customers in [Help Scout](https://www.helpscout.com)'s shared inboxes: conversations started,
answered, noted, tagged, assigned, moved and closed; customers found, added and changed; inboxes and users listed.

Written by hand from Help Scout's Mailbox API 2.0 reference, <https://developer.helpscout.com/mailbox-api/> (read on
2026-11-27): Help Scout publishes no API description file. Customers are listed and searched with its v3 calls
(cursor pages), which its reference puts in place of the v2 list. Labels and descriptions are our own words.

## Signing in

Help Scout signs in with OAuth 2.0 only, through an app made in Help Scout: **Your Profile → My apps → Create My
App**. Every credential acts as a user of the account (one with the Access API permission).

- **Sign in with Help Scout** (`help_scout`): for an installation that serves other Help Scout accounts. The
  installation's admin creates the app once with the callback address shown under **Settings → App sign-ins**
  (`https://<your studio>/oauth/callback`) as its redirection URL, and enters its App ID and App Secret there. Each
  workspace then connects its own account. Help Scout has no scopes.
- **An app's id and secret** (`help_scout_app`): for your own account. Make an app in My apps (any redirection
  URL will do), and give its App ID and App Secret; OpenDuct asks Help Scout for a token as it needs one (each lasts
  two days).

## Good to know

- Starting a conversation, replying, adding a note and adding a customer give the new record's id, which Help Scout
  sends in a header (`Resource-ID`).
- **Set a conversation's tags** gives it exactly the tags listed: send the old ones too to keep them.
- **Reply to a conversation** needs the customer replied to: the conversation's `primaryCustomer.id`.

## Actions

- **List conversations** (`list_conversations`): Conversations, the newest first unless sorted otherwise; active ones unless a status is given; every page.
- **Look up a conversation** (`get_conversation`): One conversation, with its threads (each message and note) if asked.
- **Start a conversation** (`create_conversation`): A new conversation in an inbox with its first message: from the customer (as if they wrote in), a reply to them, or a note. A customer not known by their email is added.
- **Reply to a conversation** (`reply_to_conversation`): Sends a reply to the customer of a conversation (or keeps it as a draft), and sets its status if asked.
- **Add a note to a conversation** (`add_note`): A note for the team on a conversation; the customer does not see it.
- **Change a conversation's status** (`set_conversation_status`): Sets a conversation active, pending, closed or spam.
- **Assign a conversation** (`assign_conversation`): Gives a conversation to a user or a team.
- **Move a conversation to another inbox** (`move_conversation`): Moves a conversation to another inbox.
- **Set a conversation's tags** (`set_tags`): Gives a conversation exactly these tags: those not listed are taken off, new ones are made.
- **Delete a conversation** (`delete_conversation`): Removes a conversation.
- **List a conversation's threads** (`list_threads`): Each message, reply and note of a conversation, the newest first.
- **List customers** (`list_customers`): Customers, the newest first, by name, email, or when added or changed; every page.
- **Search customers** (`search_customers`): Customers a search finds, such as email:(ann@example.com OR bob@example.com) or phone:"+15555550123".
- **Look up a customer** (`get_customer`): One customer, with their emails, phones, address and properties.
- **Add a customer** (`create_customer`): A new customer, with an email and phone if given.
- **Change a customer** (`update_customer`): Changes the fields given of a customer, and adds an email if given; the rest stay.
- **List inboxes** (`list_mailboxes`): The account's inboxes, with their ids and email addresses.
- **List users** (`list_users`): The account's users, or the one with an email, or those of an inbox.

## Triggers

- **A new conversation** (`new_conversation`): Each conversation started, the newest asked for every few minutes.
- **A conversation started, answered or changed** (`conversation_event`): Each event of the kinds chosen, as Help Scout sends it, signed. Turning the workflow on adds a webhook to Help Scout, and turning it off deletes it.
- **A customer added or changed** (`customer_event`): Each customer added, changed or deleted, as Help Scout sends it, signed. Turning the workflow on adds a webhook to Help Scout, and turning it off deletes it.

**A conversation started, answered or changed** and **A customer added or changed** add a webhook to Help Scout when
the workflow is turned on (`POST /v2/webhooks`, with a secret made for it) and delete it when it is turned off. Help
Scout signs each delivery with that secret: an HMAC-SHA1 of the body, in Base64, in the `X-HelpScout-Signature`
header. A run gets the event's name (`X-HelpScout-Event`) and the conversation or customer.

## Not done

- Attachments, Beacon chats, ratings, reports, saved replies, workflows, teams and tags' own calls.
- Customer properties and the customer's own address, phone and social entries (only the main fields and an email are changed here).
- Help Scout's Docs API (the knowledge base) is a separate API with its own key.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Help Scout account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Help Scout documents it; none was run against a live account.

```sh
openduct app check apps/help_scout --bar 80
openduct app test apps/help_scout
```
