# Spotify

Music and podcasts on [Spotify](https://open.spotify.com): playlists made and filled, the library, what is playing,
and the catalogue searched - such as a shop's playlist kept up to date, or each liked song copied to a spreadsheet.

## Registering this installation with Spotify (once, by its admin)

1. In the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard), **Create app**; tick **Web API**.
2. Under its settings, add the redirect URI shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`).
3. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Spotify**.

A new app is in Spotify's *development mode*: the app's owner needs Spotify Premium, and only up to 5 Spotify
accounts may use it, each added first under the app's **User Management**. Others can sign in but are refused (403).
More accounts need Spotify's *extended quota mode*, which Spotify grants on request to organisations.

## Connecting an account

Under Credentials, add a Spotify credential and **Connect**. It asks to read and change playlists and the library,
read what is played, and control playback.

## Actions

Tracks, albums, artists, playlists and shows are named by their link (`https://open.spotify.com/track/...`), their
Spotify URI (`spotify:track:...`) or their ID. Adding, removing and saving take Spotify URIs, separated by commas.

- **Playlists:** List my playlists (`list_my_playlists`), Get a playlist (`get_playlist`), List a playlist's tracks
  (`list_playlist_items`), Make a playlist (`create_playlist`), Change a playlist (`change_playlist`), Add tracks to a
  playlist (`add_to_playlist`, the default), Remove tracks from a playlist (`remove_from_playlist`).
- **Library:** List my liked songs (`list_saved_tracks`), Save to my library (`save_to_library`: tracks, albums,
  shows, episodes; artists, people and playlists followed), Remove from my library (`remove_from_library`).
- **Listening:** Who I am (`me`), List my top artists or tracks (`list_top`), List what I played lately
  (`list_recently_played`), What is playing now (`currently_playing`).
- **Playback** (Spotify Premium only): Play (`play`), Pause (`pause`), Skip to the next track (`skip_to_next`), Add
  to the queue (`add_to_queue`), List my devices (`list_devices`).
- **Catalogue:** Search the catalogue (`search`, at most 10 of each kind), Get a track (`get_track`), Get an album
  (`get_album`), Get an artist (`get_artist`), List an artist's albums (`list_artist_albums`), List a podcast's
  episodes (`list_show_episodes`).

## Triggers

- **A song liked** (`new_saved_track`), asked every 5 minutes: the track and when it was saved. It reads the first 50
  of the library, taking Spotify to list it newest first as its own apps show it (Spotify's reference says the library
  is kept in the order saved, not which way it lists it); more than 50 likes between two polls leave the oldest of them
  out.
- **A track played** (`new_played_track`), asked every 10 minutes: each track played since the last poll (at
  most 50 a poll, the most Spotify gives at once), with when and from which album or playlist. Podcast episodes are not in Spotify's list.

## Not done

- Operations Spotify marks deprecated (several at once, recommendations, audio features, browse categories, new
  releases, following through the old addresses) are left out; their place is taken by the ones above where Spotify
  has one.
- A playlist's cover picture (an image upload), reordering a playlist, seeking, volume, shuffle and repeat, moving
  playback to another device, audiobooks.

## Source

Started with `openduct app from-openapi` from Spotify's Web API description,
<https://developer.spotify.com/reference/web-api/open-api-schema.yaml>, fetched on 2026-11-27 (Spotify Web API 1.0.0,
OpenAPI 3.0.3; under the [Spotify Developer Terms](https://developer.spotify.com/terms/), not an open licence), and
[Spotify's quota modes](https://developer.spotify.com/documentation/web-api/concepts/quota-modes) page. Names, labels
and descriptions are our own words.

## Tested

Recorded exchanges from Spotify's documented requests and answers (`tests/`), with made-up IDs. Not run against a live
Spotify account.
