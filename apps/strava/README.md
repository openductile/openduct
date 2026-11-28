# Strava

An athlete's activities on [Strava](https://www.strava.com) (runs, rides, swims), their laps, kudos and comments,
clubs, routes, segments and gear.

## Signing in

- **Sign in with Strava** (`strava`): the athlete signs in at Strava. The install registers its own app once: at
  <https://www.strava.com/settings/api>, create an API application; set its **Authorization Callback Domain** to the
  studio's host, and enter its Client ID and Client Secret under **Settings → App sign-ins** (the callback address
  is `https://<your studio>/oauth/callback`). The app asks for `read`, `profile:read_all`, `activity:read_all` and
  `activity:write` (Strava takes them as one value, separated by commas). Strava gives a new refresh token each
  time; the new one is kept.

## Actions

- **Who I am** (`me`), **An athlete's totals** (`athlete_stats`), **My heart-rate and power zones**
  (`athlete_zones`).
- **List my activities** (`list_activities`): the newest first, between two times; every page up to the most.
- **Get an activity** (`get_activity`), **Add an activity by hand** (`create_activity`), **Change an activity**
  (`update_activity`).
- **An activity's laps** (`list_activity_laps`), **heart-rate and power zones** (`activity_zones`), **comments**
  (`list_activity_comments`), **Who gave kudos** (`list_activity_kudos`), **recorded data**
  (`get_activity_streams`: time, distance, position, altitude, heart rate, cadence, power).
- **My clubs** (`list_clubs`), **Get a club** (`get_club`).
- **An athlete's routes** (`list_routes`), **Get a route** (`get_route`).
- **My starred segments** (`list_starred_segments`), **Get a segment** (`get_segment`), **Find segments in an
  area** (`explore_segments`).
- **Get gear** (`get_gear`), **Get an upload** (`get_upload`).

## Triggers

- **A new activity** (`new_activity`): each activity the athlete records or adds, asked for every 10 minutes by
  default (Strava's rate limits are small). Strava's own webhooks are one subscription for a whole application,
  confirmed by a check of its own, and their deliveries are not signed, so the trigger polls.

## Source

Facts (host, paths, fields, sign-in) from Strava's own API description,
<https://developers.strava.com/swagger/swagger.json> and the files its references name (Swagger 2.0, last modified
2026-11-17, fetched 2026-11-27; no licence stated, so the labels and descriptions here are our own words), bundled
into one file to be read, and the reference at <https://developers.strava.com/docs/reference/>. The description
names `/athletes/{id}/routes` without declaring `id`; the app asks for it.

## Not done

- Uploading a file (FIT, GPX, TCX): it is a file upload (multipart). Exporting a route as GPX or TCX (not JSON).
- Segment efforts and their streams, a club's members and activities, starring a segment, changing the athlete's
  weight.

## Tested

Recorded exchanges (`tests/`) made from the API description, with made-up values. Not run against a live Strava
account.

```sh
openduct app check apps/strava --bar 80
openduct app test apps/strava
```
