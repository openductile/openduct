# Mailgun

Email sent through [Mailgun](https://www.mailgun.com), what became of it, mailing lists, and the addresses a domain
no longer sends to (bounces, unsubscribes, spam complaints).

Hosts, paths and fields come from Mailgun's own API description,
<https://documentation.mailgun.com/_bundle/docs/mailgun/api-reference/send/mailgun.yaml> (OpenAPI 3.1, fetched
2026-11-27), and its guides on [sending](https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/send-http)
and [event polling](https://documentation.mailgun.com/docs/mailgun/user-manual/events/event-polling). The description
states no licence, so its wording is not reused: labels and descriptions here are our own. Written by hand: the
description gives Mailgun's forms as multipart, and Mailgun asks for multipart only to attach files, so they are sent
here as ordinary forms. The vendor's API reference: <https://documentation.mailgun.com/docs/mailgun/api-reference/api-overview>.

## Signing in

- **An API key (US region)** (`mailgun`) or **(EU region)** (`mailgun_eu`, for an account hosted in the EU, at
  `api.eu.mailgun.net`): in Mailgun, **API Security → Add new key**. The credential also names the sending domain
  (`mg.example.com`); an action's `domain` input uses another of the account's domains.

## Actions

- **Send an email** (`send_email`, the default): plain text, HTML, or one of the domain's templates with its values
  (as JSON); copies, a reply-to address, a tag, a later delivery time, and test mode.
- **List email events** (`list_events`): accepted, delivered, failed, opened, clicked, unsubscribed, complained -
  newest first, by kind, recipient, sender, subject, tag or since a time (at most 3,000 a step).
- **List domains** (`list_domains`), **Look up a domain** (`get_domain`, with its DNS records), **List templates**
  (`list_templates`).
- Mailing lists: **List mailing lists** (`list_mailing_lists`), **Look up a mailing list** (`get_mailing_list`),
  **Make a mailing list** (`create_mailing_list`), **List a mailing list's members** (`list_members`), **Look up a
  mailing list member** (`get_member`), **Add someone to a mailing list** (`add_member`), **Change a mailing list
  member** (`update_member`), **Take someone off a mailing list** (`remove_member`).
- Suppressions: **List bounces** (`list_bounces`), **Look up a bounce** (`get_bounce`), **Clear a bounce**
  (`delete_bounce`), **List unsubscribes** (`list_unsubscribes`), **Add an unsubscribe** (`add_unsubscribe`),
  **Remove an unsubscribe** (`delete_unsubscribe`), **List spam complaints** (`list_complaints`).

## Triggers

- **An email event** (`email_event`): each new event of the credential's domain, or of one kind or tag, asked for
  every five minutes. Mailgun stores some events minutes after they happen, so, as its event-polling guide advises,
  each poll looks over the last half hour; the events seen before are not run again.

## Not done

- Attachments (they need a multipart form).
- Mailgun's webhooks sign each call inside its JSON body (`signature.timestamp`, `token`, `signature`), not in a
  header; OpenDuct checks webhook signatures in headers, so the trigger polls the events instead.
- Left out: domain setup and keys, DKIM, IP pools, routes, inbound mail, analytics and reports, alerts, subaccounts,
  and address validation (a separate product).

## Tested

Recorded exchanges built from the API description's requests and answers (`tests/`), with made-up addresses on
reserved domains. Not run against a live Mailgun account.

```sh
openduct app check apps/mailgun
openduct app test apps/mailgun
```
