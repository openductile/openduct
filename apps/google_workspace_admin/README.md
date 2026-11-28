# Google Workspace Admin

The people, groups and organisational units of a [Google Workspace](https://workspace.google.com) account, as its
admin manages them: someone joining gets an account and their groups, a leaver is suspended and signed out.

## Signing in

- **Sign in with Google (a Workspace admin)** (`google_workspace_admin`): an admin of the Workspace account signs in
  at Google. The install registers its own app once: in the Google Cloud console, **APIs & Services**, enable the
  **Admin SDK API**, then **Credentials → Create credentials → OAuth client ID** (a web application), with the
  callback address shown under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`); enter the
  client id and secret there. The app asks for the Directory scopes of users, groups, group members,
  organisational units, users' security (to sign someone out) and the domains (read only).

The actions act on the signed-in admin's own account (`my_customer`); an admin with fewer rights than a super admin
sees and changes only what their role allows.

## Actions

- **Find people** (`list_users`): the whole account or one domain, with Google's search (`email:ann*`,
  `orgUnitPath=/Sales`, `isSuspended=true`); every page up to the most. **Get a person** (`get_user`).
- **Add a person** (`create_user`), **Change a person** (`update_user`: name, unit, password, suspended or not, a
  new address), **Remove a person** (`delete_user`), **Restore a person** (`undelete_user`, within 20 days).
- **Make a person a super admin (or not)** (`make_admin`), **Sign a person out everywhere** (`sign_out_user`),
  **Give a person another address** (`add_user_alias`).
- **List groups** (`list_groups`: all, one person's, one domain's), **Get a group** (`get_group`), **Make a group**
  (`create_group`), **Change a group** (`update_group`), **Remove a group** (`delete_group`).
- **A group's members** (`list_members`), **Add someone to a group** (`add_member`), **Change a member's role**
  (`update_member`), **Take someone out of a group** (`remove_member`), **Is someone in a group** (`is_member`).
- **List organisational units** (`list_org_units`), **Make an organisational unit** (`create_org_unit`), **List
  domains** (`list_domains`).

## Triggers

- **A person added** (`new_user`): each new account, asked for every 15 minutes (every person is listed each time, so
  for an account of up to 5,000 people).
- **Someone joined a group** (`new_group_member`): each new member of a group.

The Directory API's push notifications are channels that expire and must be made again, which the app kit does not
do yet; the triggers poll instead.

## Source

Facts (host, paths, fields, scopes) from Google's own description of the API, its discovery document
<https://admin.googleapis.com/$discovery/rest?version=directory_v1> (revision 20261117, fetched 2026-11-27), and the
reference at <https://developers.google.com/workspace/admin/directory/reference/rest> (Google's pages are under
CC BY 4.0); the labels and descriptions here are our own words. Written by hand: the generator reads OpenAPI, not
Google's discovery format.

## Not done

- Devices (ChromeOS and mobile), roles and role assignments, calendar resources (rooms, buildings), custom user
  fields (schemas), printers, domain aliases, user photos, app passwords and backup codes.
- A service account acting for an admin (domain-wide delegation): the sign-in is an admin's own.

## Tested

Recorded exchanges (`tests/`) made from the discovery document's methods and schemas, with made-up values. Not run
against a live Workspace account.

```sh
openduct app check apps/google_workspace_admin --bar 80
openduct app test apps/google_workspace_admin
```
