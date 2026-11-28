# MailerLite

Subscribers, groups, segments, forms and email campaigns in [MailerLite](https://www.mailerlite.com).

## Source

Written by hand from MailerLite's API reference, <https://developers.mailerlite.com/> (the pages Getting started,
Subscribers, Groups, Segments, Fields, Campaigns, Forms and Webhooks), read on 2026-11-27. MailerLite publishes no
machine-readable description and states no licence for its reference: labels and descriptions here are our own words.

## Signing in

**An API key** (a MailerLite credential): in MailerLite, **Integrations → MailerLite API → Generate new token**. It
is shown once. A key works for as long as the person who made it is a user of the account.

For a webhook made by hand in MailerLite rather than by turning a workflow on, give its secret too.

## Actions

- **List subscribers** (`list_subscribers`, every page), **Get a subscriber** (`get_subscriber`, by ID or email),
  **Add or update a subscriber** (`add_subscriber`: MailerLite's own fields - name, last name, company, city, state,
  country, ZIP, phone - and groups), **Change a subscriber** (`update_subscriber`: any field by its key, and their
  groups), **Remove a subscriber** (`delete_subscriber`).
- **List groups** (`list_groups`), **Add a group** (`create_group`), **Rename a group** (`rename_group`), **Delete a
  group** (`delete_group`), **List a group's subscribers** (`list_group_subscribers`), **Add a subscriber to a group**
  (`add_to_group`), **Take a subscriber out of a group** (`remove_from_group`).
- **List segments** (`list_segments`) and **List a segment's subscribers** (`list_segment_subscribers`).
- **List fields** (`list_fields`) and **Add a field** (`create_field`).
- **List campaigns** (`list_campaigns`), **Get a campaign** (`get_campaign`), **Draft a campaign** (`create_campaign`:
  one email, to groups or segments, its HTML where the plan allows it), **Send or schedule a campaign**
  (`schedule_campaign`), **Cancel a scheduled campaign** (`cancel_campaign`).
- **List forms** (`list_forms`) and **List who signed up through a form** (`list_form_subscribers`).

## Triggers

Each, when its workflow is turned on, adds a webhook to MailerLite for its event and keeps the secret MailerLite
gives; turning it off deletes the webhook. Every delivery is checked against its `Signature` header (HMAC-SHA256 of
the body with that secret).

- **A new subscriber** (`new_subscriber`): the subscriber.
- **A subscriber unsubscribed** (`subscriber_unsubscribed`): the subscriber.
- **A subscriber added to a group** (`added_to_group`), optionally only for one group: the subscriber and the group.

## Not done

- A/B, auto-resend and multivariate campaigns; the simple editor's blocks; campaign reports.
- Automations, imports, batches, e-commerce, and forgetting a subscriber (it deletes their data for good).
- Webhooks of batched events (campaign opens and clicks, subscribers deleted).

## Tested

Recorded exchanges from MailerLite's documented requests and answers (`tests/`), with made-up data. Not run against a
live MailerLite account.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as
MailerLite documents them; none was run against a live account.
