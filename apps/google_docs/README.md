# Google Docs

Documents in [Google Docs](https://docs.google.com): made, read, added to, and filled in.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Docs API**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scope
   `https://www.googleapis.com/auth/documents`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Docs**. One OAuth client serves
   every Google app of this installation: give it each app's scope, and the same ID and secret to each.

## Connecting an account

Under Credentials, add a Google Docs credential and **Connect**.

## Actions

- **Make a document** (`make_document`): an empty document in My Drive; gives its id and address.
- **Read a document** (`read_document`): its title and text, paragraph by paragraph. Tables and pictures are left out.
- **Add text at the end** (`add_text`, the default): text after what the document has; a new line starts a new
  paragraph.
- **Replace text** (`replace_text`): every place it says one thing made to say another, such as `[customer]` in a copy
  of a letter; gives how many places changed.

A document is named by its address (`https://docs.google.com/document/d/<id>/edit`) or by its id.

Not yet: making a document inside a folder (the Docs API makes it in My Drive), or from a copy of another (that is
Google Drive's copy).

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`).

Not run against a live Google account.
