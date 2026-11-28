# Dropbox

Files and folders in [Dropbox](https://www.dropbox.com): listed, found, made, moved, copied, shared by link, fetched
from the web, brought back to an earlier version - and file requests, to collect files from others.

The same sign-in serves the **Files on a server or a folder** step (`storage`): it lists, reads, writes, moves, copies
and deletes Dropbox files, and its trigger watches a folder - see
[the studio's guide](../../docs/studio.md#stores-and-drives-over-their-own-apis).

Written by hand from Dropbox's own API description,
<https://github.com/dropbox/dropbox-api-spec/blob/4876d1a552f34e10a46e293aaf7e344c452af13d/openapi/openapi.yaml> (MIT
licence, OpenAPI 3.1), on 2026-11-27, with `openduct app from-openapi` as a guide: Dropbox's lists go on at another
address (`.../continue`) with only the cursor, which each list here follows. The vendor's API reference:
<https://www.dropbox.com/developers/documentation/http/documentation>.

## Registering this installation with Dropbox (once, by its admin)

1. In the [Dropbox App Console](https://www.dropbox.com/developers/apps), **Create app**: **Scoped access**, and
   **Full Dropbox** (or **App folder**, to reach only a folder of its own).
2. On its **Permissions** tab, tick `account_info.read`, `files.metadata.read`, `files.metadata.write`,
   `files.content.read`, `files.content.write`, `sharing.read`, `sharing.write`, `file_requests.read` and
   `file_requests.write`, and **Submit**.
3. On its **Settings** tab, add the redirect URI shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`), and copy the **App key** and **App secret** into **Settings → App
   sign-ins → Dropbox**.

Then, under Credentials, add a Dropbox credential and **Connect**. The sign-in asks for a refresh token
(`token_access_type=offline`), so the connection lasts.

## Actions

- **List a folder** (`list_folder`, the default): its files and folders (and its folders' contents, if asked), every
  page. The top of Dropbox is an empty path.
- **Find files** (`search`): by words in names or contents, in a folder, by file endings; every page.
- **Look up a file or folder** (`get_metadata`): by path (`/Invoices/INV-1042.pdf`) or id (`id:...`).
- **Make a folder** (`create_folder`), **Delete a file or folder** (`delete`), **Move or rename a file or folder**
  (`move`), **Copy a file or folder** (`copy`).
- **Get a download link** (`get_temporary_link`): an address that works for four hours.
- **Save a file from the web** (`save_url`) and **Check a saved file** (`save_url_status`): Dropbox fetches the
  address itself - the way to put a file from another service into Dropbox.
- **List a file's versions** (`list_revisions`), **Bring back a file's version** (`restore`).
- **Share a file or folder by link** (`create_shared_link`): for anyone, the team, or only those with access; to view
  or edit; ending at a time. **List a file's shared links** (`list_shared_links`): the link it has already.
- **Ask for files** (`create_file_request`), **List file requests** (`list_file_requests`).
- **Your account** (`get_account`), **Space used** (`get_space_usage`).

## Triggers

Both asked every 5 minutes, for one folder (and its folders, if asked):

- **A new file in a folder** (`new_file`): each file put there.
- **A file added or changed in a folder** (`changed_file`): each file put there or given a new version.

Each poll reads the whole folder (2,000 entries a request).

## Not done

- Uploading and downloading a file's bytes (Dropbox's content endpoints take and give the file itself, with its
  arguments in a header): **Save a file from the web** and **Get a download link** stand in for them.
- Dropbox's webhooks: Dropbox checks the address with a GET whose `challenge` must be sent back, and each delivery
  only names the accounts that changed, not what changed. OpenDuct's webhook triggers neither echo such a challenge
  nor follow up with a list; the polls stand in for them.
- Team spaces and team folders (the `Dropbox-API-Path-Root` header), Paper docs, file locks, tags, sharing with
  people by email.

## Tested

Recorded exchanges built from Dropbox's API description (`tests/`): each request's address and body, answers of the
documented shape (two pages followed through `.../continue`), refusals in Dropbox's words (409, 401, and a plain-text
400), and the polls. Not run against a live Dropbox account.

```sh
openduct app check apps/dropbox --bar 80
openduct app test apps/dropbox
```
