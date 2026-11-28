# YouTube

Videos, playlists and channels on [YouTube](https://www.youtube.com): found, read, described and kept in order.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **YouTube Data API v3**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scope `https://www.googleapis.com/auth/youtube`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → YouTube**. One OAuth client serves
   every Google app of this installation.

## Connecting an account

Under Credentials, add a YouTube credential and **Connect** with the Google account that has the channel.

## Actions

- **Look up a video** (`get_video`, the default): its title, description, length, and counts of views, likes and
  comments.
- **Find videos** (`find_videos`): by words, by a channel, since a time, newest first or as YouTube ranks them. YouTube
  allows a project 100 searches a day, so this reads at most 2 pages of 50.
- **Change a video's title and description** (`update_video`): title, category, description and tags together -
  YouTube clears what is not given, so give what it should keep (Look up a video gives it).
- **List a playlist's videos** (`list_playlist_videos`): every page, up to 500. A channel's uploads are a playlist
  (Look up a channel gives its id).
- **Add a video to a playlist** (`add_to_playlist`) and **Take a video out of a playlist** (`remove_from_playlist`).
- **Make a playlist** (`make_playlist`): private unless said.
- **Look up a channel** (`get_channel`): yours, or another's by its id.

Not yet: uploading a video (it takes a file).

## Tested

Recorded exchanges built from the YouTube Data API reference (`tests/`).

Not run against a live YouTube channel.
