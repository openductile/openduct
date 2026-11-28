# Google Calendar

Events of a [Google Calendar](https://calendar.google.com): read and added.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Calendar API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/calendar.events`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Calendar**. One OAuth client serves every
   Google app of this installation: give it each app's scope, and the same ID and secret to each.


## Connecting an account

Under Credentials, add a Google Calendar credential and **Connect**.

## Actions

- **List events** (`list_events`): of your own calendar (`primary`) or another's id, between two times; every page.
- **Add an event** (`add_event`): title, start and end (with their time zone offset), and who is asked.

## Triggers

- **A new event** (`new_event`) and **An event added or changed** (`changed_event`), asked every 5 minutes, in one
  calendar (`primary` for your own). Each poll asks for what changed since the newest change it dealt with.

## Tested

Recorded exchanges from Google's documented requests and answers (`tests/`).

Run against a Google account on 2026-11-16: List events and Add an event (nobody asked) worked.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
