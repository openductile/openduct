# Google Slides

Presentations in [Google Slides](https://www.google.com/slides/about/): a template copied and filled in for each
customer, read slide by slide, pictured, and its slides added, copied, moved and removed.

Written by hand from Google's Slides API reference, <https://developers.google.com/workspace/slides/api/reference/rest>,
and its discovery document (`https://slides.googleapis.com/$discovery/rest?version=v1`, revision 20261118, read
2026-11-27); copying a presentation is Google Drive's `files.copy`
(<https://developers.google.com/workspace/drive/api/reference/rest/v3/files/copy>). The descriptions here are our own
words.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Slides API** and the **Google Drive API**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scopes
   `https://www.googleapis.com/auth/presentations` and `https://www.googleapis.com/auth/drive` (Drive's, to copy a
   template the person made; it is one of Google's restricted scopes, which a public app has verified).
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Slides**. One OAuth client
   serves every Google app of this installation.

## Connecting an account

Under Credentials, add a Google Slides credential and **Connect**.

## Actions

- **Copy a presentation** (`copy_presentation`): a template copied under a new name, into a folder if given; gives
  its id and address.
- **Fill in placeholders** (`fill_placeholders`, the default): a record of placeholders and values - `{{company}}:
  Example Co`, `{{total}}: 1250` - each replaced on every slide, capitals as given; gives how many places changed.
- **Replace text** (`replace_text`): one text made another, on every slide or only those named.
- **Put a picture in place of a shape** (`replace_with_picture`): each shape that says, say, `{{logo}}` becomes a
  picture from a public address (PNG, JPEG or GIF), fitted inside it or cropped to fill it.
- **Make a presentation** (`make_presentation`): a blank one in My Drive.
- **Read a presentation** (`read_presentation`): its title, and each slide's id and text in order.
- **Read a slide** (`get_slide`): one slide as Google keeps it, with the ids of its shapes.
- **Picture of a slide** (`slide_picture`): the address of a PNG of a slide. It works for 30 minutes, and for anyone
  who has it, as the signed-in account.
- **Add a slide** (`add_slide`), **Copy a slide** (`duplicate_slide`), **Move slides** (`move_slides`), **Delete a
  slide** (`delete_slide`), **Write into a shape** (`add_text`).

A presentation is named by its address (`https://docs.google.com/presentation/d/<id>/edit`) or by its id.

The usual workflow: **Copy a presentation** (the template), then **Fill in placeholders** on the copy, then
**Picture of a slide** or a Google Drive export to send it.

## Not done

- Exporting a presentation as PDF or PowerPoint (that is Google Drive's export, a file in the answer).
- Charts from Google Sheets, tables, shapes and lines drawn anew, text styles, speaker notes.
- Google Slides has no webhooks of its own; a new or changed presentation is a Google Drive trigger.

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`). Not run against a live Google
account.

```sh
openduct app check apps/google_slides --bar 80
openduct app test apps/google_slides
```
