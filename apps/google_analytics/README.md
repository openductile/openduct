# Google Analytics

Reports on a [Google Analytics](https://analytics.google.com) property - visitors, pages, sources, sales - and events
sent to it from elsewhere (an order taken by phone, a lead won in the CRM).

## Two sign-ins

- **Sign in with Google (reports)** (`google_analytics`): for the reports and lists. It may only read.
- **A data stream's Measurement Protocol secret (sending events)** (`google_analytics_events`): for **Send an event**
  alone. In Google Analytics: **Admin → Data streams →** the stream **→ Measurement Protocol API secrets → Create**;
  give the secret and the stream's **Measurement ID** (`G-...`). Say `eu` under region for a stream whose data is
  collected in the EU.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Analytics Data API** and the **Google Analytics Admin API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/analytics.readonly`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Analytics**. One OAuth client
   serves every Google app of this installation: give it each app's scope, and the same ID and secret to each.

The read-only Analytics scope is one Google checks before an External app may use it with anyone's account; for your
own Workspace (Internal), it is used at once.

## Connecting an account

Under Credentials, add a Google Analytics credential and **Connect**, as a person who may see the property.

## Actions

- **Run a report** (`run_report`, the default): metrics (`sessions, totalUsers`) by dimensions (`date, country`) over a
  range of days (the last 7 by default), optionally only where one dimension is exactly a value, largest first by a
  metric. Each row is a record of the names asked for: `{date: "20261101", country: Canada, sessions: 120}`, metrics
  as numbers.
- **Run a report of the last minutes** (`run_realtime_report`): the same over the last 30 minutes (or fewer).
- **List the dimensions and metrics a property has** (`list_dimensions_and_metrics`): the names a report takes,
  custom ones too.
- **List my properties** (`list_properties`): each property's number (what the reports ask for), name and account.
- **Send an event** (`send_event`): an event against a visitor's client ID, with its details. Google answers every
  request it receives alike, so a wrong name is not refused: check it in the property's Realtime report (or Google's
  event validation address) the first time.

Names of dimensions and metrics are [Google's](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema).

## Not done

- Filters beyond one dimension equal to one value (Google's filter expressions: and/or, numbers, lists), more than one
  range of days, comparisons, cohorts, pivot reports, and audience exports.
- A report's totals, minimums and maximums rows, and its sampling and quota notes: a run gets the rows.
- The Admin API's changes (properties, streams, custom definitions, key events): this app reads reports.

## Source

Written by hand from Google's API Discovery documents, fetched on 2026-11-27:
`https://analyticsdata.googleapis.com/$discovery/rest?version=v1beta` (revision 20261117) and
`https://analyticsadmin.googleapis.com/$discovery/rest?version=v1beta` (revision 20261121), and the
[Measurement Protocol reference](https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference)
(Google's documentation, CC BY 4.0; the API under the Google APIs Terms of Service). Descriptions are our own words.

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`), with made-up values. Not run
against a live Google Analytics property.
