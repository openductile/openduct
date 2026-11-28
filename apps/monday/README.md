# monday.com

Items, subitems, updates, groups and boards in [monday.com](https://monday.com) Work Management: added, changed,
moved, found and commented on.

Written by hand from monday.com's API reference, <https://developer.monday.com/api-reference> (API version
`2026-10`, the current one, read 2026-11-27). monday.com's API is GraphQL: each action posts one query to
`https://api.monday.com/v2` with its values as GraphQL variables, never pasted into the query. The descriptions here
are our own words.

## Signing in

Two ways:

- **A personal API token** (`monday`): in monday.com, your profile picture → **Developers** → **API token** →
  **Show** (an admin also finds it under **Administration → Connections → Personal API token**). It acts as you.
- **Sign in with monday.com** (`monday_oauth`), for a workspace that connects its own account.

Both send the `API-Version: 2026-10` header.

### Registering this installation with monday.com (once, by its admin)

1. In monday.com's **Developer Center**, create an app.
2. Under **OAuth & Permissions**, add the redirect URL shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`), and on the **Scopes** tab choose `boards:read`, `boards:write`,
   `updates:read`, `updates:write`, `users:read` and `me:read`.
3. Copy the app's **Client ID** and **Client secret** (its general settings) into **Settings → App sign-ins →
   monday.com**.

Then, under Credentials, add a monday.com credential and **Connect**.

## Actions

- **Add an item** (`create_item`, the default), **Look up an item** (`get_item`), **Change an item's columns**
  (`update_item`), **Set one column** (`set_column_value`), **Move an item to a group** (`move_to_group`),
  **Archive an item** (`archive_item`), **Delete an item** (`delete_item`), **Copy an item** (`duplicate_item`),
  **Add a subitem** (`create_subitem`).
- **Post an update on an item** (`add_update`), **List an item's updates** (`list_updates`: the newest 100).
- **List a board's items** (`list_items`: every page, by monday.com's cursor), **Find items by a column's value**
  (`find_items`: up to 500).
- **List boards** (`list_boards`), **Look up a board** (`get_board`: its groups and columns, with the ids the other
  actions ask for), **Make a board** (`create_board`), **Add a group to a board** (`create_group`).
- **List people** (`list_users`): their ids, for a people column.

Column values are given as a record of column id to text - `status: Done`, `date4: 2026-12-01`, `text0: Hello`,
`numbers: 12` - which monday.com reads for most kinds of column; the app writes them as the JSON text monday.com asks
for. With **Make status or dropdown labels**, a label that is not there yet is made.

## Triggers

Both asked every 5 minutes, for one board:

- **A new item on a board** (`new_item`): each item added.
- **An item added or changed on a board** (`item_changed`): each item added or changed; one changed again runs again.

Each poll reads the whole board (100 items a request): fine for most boards, slow for one of thousands of items.

## Not done

- monday.com's **webhooks** as a trigger (an item made, a column changed, an update posted - as it happens).
  monday.com checks a webhook's address with a challenge OpenDuct can answer, but it signs deliveries only for
  webhooks made with a monday app's own token, with a JWT in the `Authorization` header signed by the app's signing
  secret; webhooks made with a personal token are not signed at all. OpenDuct's webhook triggers check a signature
  over the body (an HMAC) or a shared token, so neither form can be accepted yet. Until then, the polls above.
- A column value that needs more than text (a person by id, a link with its words, a timeline, a location) through
  `create_item` or `update_item`: OpenDuct's expressions cannot yet write any record as JSON text, so each value is
  sent as text, and a column monday.com does not fill from text cannot be set yet.
- Files (uploads are multipart), docs, dashboards, workspaces, folders, tags, teams, activity logs, item
  descriptions.
- `find_items` and `list_updates` read one page.

## Tested

Recorded exchanges built from monday.com's API reference (`tests/`): each query and its variables, answers of the
documented shape (two pages followed by the cursor), a refusal answered 200 with an errors list, one answered 401,
and the polls. Not run against a live monday.com account.

```sh
openduct app check apps/monday --bar 80
openduct app test apps/monday
```
