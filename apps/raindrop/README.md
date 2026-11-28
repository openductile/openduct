# Raindrop.io

Bookmarks in [Raindrop.io](https://raindrop.io): saved, found, tagged and sorted into collections.

Written by hand from Raindrop.io's API reference, <https://developer.raindrop.io> (read 2026-11-27: raindrops,
collections, tags, user, authentication). The reference carries no licence, so only its facts are used and the
words here are our own.

## Signing in

- **An app's test token** (`raindrop`): in Raindrop.io, **Settings → Integrations** (the App Management Console,
  <https://app.raindrop.io/settings/integrations>), create an app, open its settings and copy its **Test token**. It reaches
  only your own account, which is what a workspace's credential is for. Sent as `Authorization: Bearer`.

## Actions

- **List bookmarks** (`list_bookmarks`): in one collection (`0` all, `-1` Unsorted, `-99` Trash, or a collection's id),
  the newest first or sorted, matching a search if given (Raindrop.io's search: words, `#tag`, a site); every page,
  up to 1,000.
- **Get a bookmark** (`get_bookmark`).
- **Save a bookmark** (`add_bookmark`): a link, and if given a title, description, note, tags, favourite mark and
  collection (Unsorted otherwise).
- **Save many bookmarks** (`add_bookmarks`): up to 100 at once.
- **Change a bookmark** (`update_bookmark`) and **Remove a bookmark** (`delete_bookmark`: to the Trash, or for good
  from the Trash).
- **List collections** (`list_collections`), **List the collections inside others** (`list_child_collections`),
  **Get a collection** (`get_collection`), **Add a collection** (`create_collection`), **Change a collection**
  (`update_collection`), **Remove a collection** (`delete_collection`: with those inside it; their bookmarks go to
  the Trash).
- **List tags** (`list_tags`), **Rename tags** (`rename_tag`: one renamed, or several merged), **Remove tags**
  (`remove_tags`).
- **Get the signed-in user** (`get_user`).

## Triggers

- **A new bookmark** (`new_bookmark`), asked every 5 minutes: each bookmark saved, in one collection or in any.

## Not done

- **Sign in with Raindrop.io** (OAuth): Raindrop.io documents its token request as a JSON body, and the app kit
  sends the standard form body; until a sign-in can send JSON (or Raindrop.io is seen to take the form), the app
  takes an app's test token.
- Uploading files and covers (multipart), highlights, filters, import and backups; changing many bookmarks at once.

## Tested

Recorded exchanges from the requests and answers the reference documents (`tests/`), with made-up values; the
trigger's poll is recorded too. Not run against a live account.

```sh
openduct app check apps/raindrop --bar 80
openduct app test apps/raindrop
```
