# Nextcloud

Share files and folders, read and remove files, find them, manage users and groups, and post to Talk, on your own
[Nextcloud](https://nextcloud.com).

Written by hand, in our own words, from Nextcloud's own OpenAPI descriptions and its WebDAV reference, read
2026-11-27:

- `apps/files_sharing/openapi.json`, `apps/provisioning_api/openapi-full.json` and `core/openapi.json` of
  <https://github.com/nextcloud/server> at commit e44a50265673 (AGPL-3.0-or-later);
- `openapi.json` of Nextcloud Talk, <https://github.com/nextcloud/spreed> at commit aa0cecdc7319 (AGPL-3.0-or-later);
- <https://docs.nextcloud.com/server/latest/developer_manual/client_apis/WebDAV/basic.html> for reading and removing
  files.

## Signing in

- **A user name and app password** (`nextcloud`): the Nextcloud's address (`https://cloud.example.com`), the user name,
  and an app password made in **Personal settings → Security → Devices & sessions** (needed where two-factor sign-in
  is on, and better everywhere: it can be revoked alone). Sent as HTTP basic, with the `OCS-APIRequest: true` header
  Nextcloud's OCS API asks for. Managing users and groups needs an admin (or a group's sub-admin).

## Actions

- **Share a file or folder** (`create_share`, the default): by a public link (by default), with a person, a group or
  an email address; with what they may do (`permissions`: 1 read, 2 change, 4 add, 8 remove, 16 share, added up), a
  password, an end date, a note. It gives the share with its link (`url`).
- **List shares** (`list_shares`), **Look up a share** (`get_share`), **Change a share** (`update_share`) and **Stop
  sharing** (`delete_share`).
- **Read a text file** (`read_file`): a `.txt`, `.csv`, `.md` or `.json` file's contents, as text.
- **Remove a file or folder** (`delete_file`): to the trash.
- **Search files** (`search_files`): files whose names match.
- **Users**: **Add a user** (`create_user`; without a password Nextcloud emails them to set one), **Look up a user**
  (`get_user`), **List users** (`list_users`), **Change a user** (`update_user`: one detail at a time), **Turn a user
  off** / **back on** (`disable_user`, `enable_user`), **Add a user to a group** (`add_user_to_group`).
- **Groups**: **List groups** (`list_groups`) and **Add a group** (`create_group`).
- **Talk**: **Post to a Talk conversation** (`send_talk_message`, by the conversation's token, the last part of its
  address) and **List Talk conversations** (`list_talk_conversations`).

## Triggers

- **A new message in Talk** (`new_talk_message`): each message posted to a conversation (not its own notices, such as
  someone joining), the latest 100 asked for every 5 minutes. The first poll notes what is there and runs nothing.

## Not done

- **Uploading a file, making a folder, moving and copying, and listing a folder**: they are WebDAV requests -
  `PUT` with the file's own bytes as the body, and the `MKCOL`, `MOVE`, `COPY` and `PROPFIND` methods - and the app kit
  sends JSON or form bodies only, with GET, POST, PUT, PATCH and DELETE. Reading a file other than text (a PDF, an
  image) needs the same: an answer kept as a file.
- A trigger for new files: Nextcloud tells of them through WebDAV searches or its webhook listeners (an admin's
  setting), not through a list the app kit can poll.
- Signing in at the Nextcloud (its OAuth 2.0): the sign-in page is each Nextcloud's own, and the app kit takes only a
  fixed one for that grant.
- Deck, Calendar and Contacts (CalDAV, CardDAV), Notes, Forms and other Nextcloud apps.

## Tested

Recorded exchanges written from the API descriptions with made-up people and files (`tests/`): every action, a share
with a person, a text file read, a file moved to the trash, a path that is not there refused (404, in Nextcloud's
words), and the Talk trigger leaving out the conversation's notices. Not run against a live Nextcloud.

```sh
openduct app check apps/nextcloud --bar 80
openduct app test apps/nextcloud
```
