# ClickUp

Tasks, lists, comments, checklists and time in a [ClickUp](https://clickup.com) workspace - and a trigger for each
task made or changed.

Made with `openduct app from-openapi` from ClickUp's own API description,
<https://developer.clickup.com/openapi/clickup-api-v2-reference.json> (API v2, fetched 2026-11-27; it states no
licence, so only its facts - addresses, fields, sign-in - are used, and every label and description here is our
own). Webhooks from <https://developer.clickup.com/docs/webhooks> and
<https://developer.clickup.com/docs/webhooksignature>. The API reference: <https://developer.clickup.com/reference>.

## Signing in

Two ways:

- **A personal API token** (a ClickUp credential): in ClickUp, your avatar, **Settings → Apps → API Token →
  Generate**. It starts `pk_`, acts as you, and does not run out. ClickUp takes it in the `Authorization` header as
  it is.
- **Sign in with ClickUp** (a ClickUp sign-in): the installation's admin, a workspace owner or admin in ClickUp,
  registers an app once at **Settings → Apps → Create new app**, with the callback address shown under **Settings →
  App sign-ins** (`https://<your studio>/oauth/callback`) as its redirect URL, and copies its client ID and secret
  there. A workspace then **Connects** under Credentials and chooses which of its ClickUp workspaces the token may
  reach. ClickUp has no scopes; its tokens do not run out.

Most actions name a workspace, space, folder or list by its id: **List your workspaces** gives the workspaces' ids,
and **List a workspace's spaces**, **List a space's folders** and **List a folder's lists** the rest.

## Actions

- **Create a task** (`create_task`, the default): in a list; its text (plain or Markdown), people, tags, status,
  priority, dates, estimate, points, parent, link and custom fields.
- **Look up a task** (`get_task`), **Change a task** (`update_task`: only what is given) and **Remove a task**
  (`delete_task`).
- **List a list's tasks** (`list_tasks`): closed ones and subtasks if asked, by when made, changed or due; every page,
  up to 1,000.
- **Find tasks in a workspace** (`find_tasks`): across all its lists, by the same dates; every page, up to 1,000.
- **Comment on a task** (`comment_on_task`) and **List a task's comments** (`list_task_comments`: newest first, every
  page).
- **Tag a task** (`tag_task`) and **Take a tag off a task** (`untag_task`).
- **Add a checklist to a task** (`add_checklist`) and **Add an item to a checklist** (`add_checklist_item`).
- **Set a task's custom field** (`set_custom_field`) and **List a list's custom fields** (`list_custom_fields`).
- **Track time** (`track_time`): a time entry, for a task or not.
- **List your workspaces** (`list_workspaces`), **List a workspace's spaces** (`list_spaces`), **List a space's
  folders** (`list_folders`), **List a folder's lists** (`list_lists`), **List a space's lists outside folders**
  (`list_space_lists`), **Look up a list** (`get_list`), **List who can reach a list** (`list_list_members`).
- **Create a list in a folder** (`create_list`), **Create a list in a space** (`create_space_list`), **Create a
  folder** (`create_folder`).

A task's own id is used unless `custom_task_ids` is set, with the workspace's id, for a workspace that numbers its
tasks itself. Dates are Unix time in milliseconds, as ClickUp takes them.

## Triggers

- **A task made or changed** (`task_event`): ClickUp's webhook for a workspace (or one space, folder or list of it),
  for the events chosen - `taskCreated` unless said, or `taskUpdated`, `taskStatusUpdated`, `taskAssigneeUpdated`,
  `taskCommentPosted`, `taskMoved`, `taskDeleted` and the others ClickUp names. Turning the workflow on creates the
  webhook and keeps the secret ClickUp gives for it; each delivery's `X-Signature` (HMAC-SHA256 of the body, hex) is
  checked with it; turning the workflow off removes the webhook. A run gets the event, the task's id and what
  changed (`changes`, ClickUp's history items): a **Look up a task** step reads the rest.

## Not done

- Attachments (a file upload), goals, views, guests, user groups, templates and the rest of ClickUp's 135
  operations; 25 of the most used are here.
- Filters that ClickUp takes as a list repeated in the query (statuses, assignees, tags, custom fields) on
  `list_tasks` and `find_tasks`.
- ClickUp's API v3 (docs, chat), which has a description of its own.

## Tested

Recorded exchanges (`tests/`): those made from the API description's schemas, and by hand the pages of tasks
(counted from 0, until `last_page`) and of comments (by the last one's `start` and `start_id`), a refusal in
ClickUp's words, a task made signed in with ClickUp, and the trigger's registering, a signed delivery, a delivery
not chosen, a forged one and the removal. Signatures made with HMAC-SHA256 outside OpenDuct, as ClickUp documents.
Not run against a live ClickUp workspace.

```sh
openduct app check apps/clickup --bar 80
openduct app test apps/clickup
```
