# Google Tasks

Tasks and task lists in [Google Tasks](https://tasks.google.com): added, changed, marked done, moved and cleared.

Written by hand from Google's Tasks API reference, <https://developers.google.com/workspace/tasks/reference/rest>,
and its discovery document (`https://tasks.googleapis.com/$discovery/rest?version=v1`, revision 20261117, read
2026-11-27). The descriptions here are our own words.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Google Tasks API**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scope `https://www.googleapis.com/auth/tasks`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Tasks**. One OAuth client
   serves every Google app of this installation.

## Connecting an account

Under Credentials, add a Google Tasks credential and **Connect**.

## Actions

- **List task lists** (`list_task_lists`), **Make a task list** (`create_task_list`), **Rename a task list**
  (`rename_task_list`), **Delete a task list** (`delete_task_list`).
- **List tasks** (`list_tasks`): every page; only those not done, or due between two days, if asked.
- **Look up a task** (`get_task`), **Add a task** (`create_task`, the default: at the top, or under or after another
  task), **Change a task** (`update_task`), **Mark a task done** (`complete_task`), **Move a task** (`move_task`:
  under or after another, or to another list), **Delete a task** (`delete_task`), **Clear done tasks**
  (`clear_completed`).

A list is named by its id; `@default` is the account's first list (My Tasks). A due day is a day: Google keeps no
time of day for it (`2026-12-01` is sent as `2026-12-01T00:00:00.000Z`).

## Triggers

Both asked every 5 minutes, for one list:

- **A new task** (`new_task`): each task added (tasks already done are left out).
- **A task done** (`task_completed`): each task marked done.

## Not done

- Google Tasks has no webhooks; the triggers poll.
- Tasks assigned from Google Docs or Chat are read as Google gives them (`assignmentInfo` is left out of the
  fields listed), and cannot be given notes.

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`): each request, answers of the
documented shape (two pages, an empty list), a refusal in Google's words, and the polls. Not run against a live
Google account.

```sh
openduct app check apps/google_tasks --bar 80
openduct app test apps/google_tasks
```
