# Google Sheets

Rows of a [Google Sheets](https://www.google.com/sheets/about/) spreadsheet: read, added and changed.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Sheets API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/spreadsheets`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Sheets**. One OAuth client serves every
   Google app of this installation: give it each app's scope, and the same ID and secret to each.


## Connecting an account

Under Credentials, add a Google Sheets credential and **Connect**: sign in at Google and allow it. OpenDuct keeps the
token and renews it as it runs out.

## Actions

- **Add a row** (`add_row`): at the end of a sheet, its values in order, read as if typed (`=SUM(...)` works).
- **Read rows** (`read_rows`): a range such as `Sheet1!A1:F100`, or a whole sheet.
- **Write over rows** (`set_rows`): a range, with a list of rows.

A spreadsheet's id is the long part of its address: `docs.google.com/spreadsheets/d/<id>/edit`.

## Triggers

- **A new row** (`new_row`) and **A new or changed row** (`new_or_updated_row`), asked every 5 minutes: each row
  under the header row of a sheet, as a record of the header's names with its `row_number`. A row is known by its
  number: putting rows in above it, or taking rows out, makes those below it look changed. The first 5,000 rows.

## Tested

Recorded exchanges from Google's documented requests and answers (`tests/`).

Tried against a Google account on 2026-11-16: its sign-in worked, and a spreadsheet that does not exist was refused in Google's words; the account had no spreadsheet, so reading and writing rows were not tried live.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
