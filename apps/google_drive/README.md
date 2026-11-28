# Google Drive

Files and folders in [Google Drive](https://www.google.com/drive/): found, made and shared.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Drive API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/drive`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Drive**. One OAuth client serves every
   Google app of this installation: give it each app's scope, and the same ID and secret to each.

The full Drive scope is one Google reviews before an External app may use it with anyone's account; for your own Workspace (Internal), it is used at once.
## Connecting an account

Under Credentials, add a Google Drive credential and **Connect**.

## Actions

- **Find files** (`find_files`): as Drive searches (`name contains 'invoice' and trashed = false`); every page.
- **Make a folder** (`make_folder`): in My Drive, or inside a folder.
- **Share a file or folder** (`share_file`): with someone, as reader, commenter or writer; Google mails them.

## Files

The **Files on a server or a folder** step (`storage`) and its trigger take a Google Drive credential: files by
path in My Drive, or inside a folder by its id (`folder_id`; one of a shared drive's too); a Google Doc, Sheet or Slides is
exported when read (`export`); new files are read from Drive's list of changes - see [the studio's guide](../../docs/studio.md#stores-and-drives-over-their-own-apis).

## Triggers

- **A new file** (`new_file`) and **A file changed** (`changed_file`), asked every 5 minutes: in the whole Drive or one
  folder (`folder_id`); the run gets the file's id, name, type, times, link, folders and size.

## Tested

Recorded exchanges from Google's documented requests and answers (`tests/`).

Run against a Google account on 2026-11-16, its token renewed from the refresh token: Find files and Make a folder worked. Share was not tried (Google mails the person).

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
