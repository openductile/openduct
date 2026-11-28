# Zoom

[Zoom](https://zoom.us) meetings scheduled, changed and cancelled, people registered for them, and - once they are
over - who attended, their cloud recordings and their AI Companion summaries: such as each booking turned into a
meeting with its join link sent on, or each recording copied to a shared drive.

## Registering this installation with Zoom (once, by its admin)

1. In the [Zoom App Marketplace](https://marketplace.zoom.us), **Develop → Build App → General App**, managed by
   each user (user-managed).
2. In the app's OAuth settings, give the redirect URL shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`).
3. Under **Scopes**, add: `meeting:write:meeting`, `meeting:read:meeting`, `meeting:read:list_meetings`,
   `meeting:update:meeting`, `meeting:delete:meeting`, `meeting:write:registrant`, `meeting:read:list_registrants`,
   `meeting:read:list_past_participants`, `cloud_recording:read:list_user_recordings`,
   `cloud_recording:read:list_recording_files`, `meeting:read:summary` and `user:read:user`. Zoom asks a person for
   the scopes the app has; the sign-in names none of its own.
4. Copy the **Client ID** and **Client secret** into **Settings → App sign-ins → Zoom**.

## Connecting an account

Under Credentials, add a Zoom credential and **Connect**: it acts as the person who signs in, on their own meetings.

## Actions

Meetings are named by their number (`857 0000 0001`, spaces or not); a past meeting's attendees, recordings and
summary also by one occurrence's UUID.

- **Schedule a meeting** (`create_meeting`, the default): a topic, a time and time zone, how long, an agenda and
  passcode; a waiting room, joining before the host, registration (approved at once, or by the host), cloud
  recording. Gives the join link, the host's start link and the passcode.
- **List meetings** (`list_meetings`: upcoming, live, past or scheduled), **Get a meeting** (`get_meeting`), **Change a
  meeting** (`update_meeting`), **Cancel a meeting** (`delete_meeting`, telling those registered if asked).
- **Register someone for a meeting** (`add_registrant`: gives their own join link), **List a meeting's registrants**
  (`list_registrants`).
- **List who attended a meeting** (`list_past_participants`: paid Zoom accounts), **List cloud recordings**
  (`list_recordings`: at most a month at once), **Get a meeting's recordings** (`get_meeting_recordings`), **Get a
  meeting's summary** (`get_meeting_summary`: when AI Companion summaries were on).
- **Who I am** (`me`).

A recording's download link takes the person's Zoom token as a bearer token, as Zoom documents; this app gives the
links, and does not download the files.

## Triggers

- **A meeting recorded to the cloud** (`new_recording`), asked every 15 minutes: each meeting of the last 7 days whose
  recording is listed, with its files.
- **Someone registered for a meeting** (`new_registrant`), asked every 5 minutes.

## Not done

- **Zoom's webhooks** (a meeting started or ended, a participant joined, a recording completed, as it happens): Zoom
  checks a webhook's address by sending a token that the address must answer with the token's HMAC-SHA256 under the
  webhook's secret token, before it sends anything, and again every 72 hours. OpenDuct checks Zoom's deliveries'
  signatures (`x-zm-signature: v0=` over `v0:{timestamp}:{body}`) but cannot yet answer that check, so the triggers
  ask instead.
- **Server-to-Server OAuth** (an account-wide app, its token got with `grant_type=account_credentials` and the
  account's id): OpenDuct's client-credentials sign-in sends `client_credentials`.
- Webinars, recurring meetings' occurrences, polls, surveys, live streaming, reports, in-meeting controls, users and
  account settings, and Zoom Phone, Team Chat and Rooms.

## Source

Written by hand from Zoom's API reference pages, read on 2026-11-27:
[Meetings](https://developers.zoom.us/docs/api/meetings/), [Users](https://developers.zoom.us/docs/api/users/),
[OAuth](https://developers.zoom.us/docs/integrations/oauth/) and [webhooks](https://developers.zoom.us/docs/api/webhooks/)
(Zoom's documentation; no open licence). Zoom's downloadable OpenAPI
descriptions (`developers.zoom.us/api-hub/.../endpoints.json`) answered 504 that day. Descriptions are our own words.

## Tested

Recorded exchanges from Zoom's documented requests and answers (`tests/`), with made-up meetings and people. Not run
against a live Zoom account.
