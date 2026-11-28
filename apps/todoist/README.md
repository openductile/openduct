# Todoist

Tasks in [Todoist](https://todoist.com): added (in plain words, as in Todoist's own quick add, too), found, changed,
completed, moved and commented on - such as each order from the shop turned into a task for tomorrow, or each task
ticked off copied to a spreadsheet - and the projects, sections and labels they are kept in.

## Signing in

- **A personal API token** (`todoist`): in Todoist's integrations settings
  (<https://app.todoist.com/app/settings/integrations>). It acts as you. Its optional **webhook secret** is for the
  trigger *Something changed (as it happens)* (below).
- **Sign in with Todoist** (`todoist_oauth`): each person connects their own account. It asks to read, change and
  delete tasks, projects, labels and comments (`data:read_write,data:delete`).

### Registering this installation with Todoist (once, by its admin, for Sign in with Todoist)

1. In Todoist's [App Management Console](https://developer.todoist.com/appconsole.html), **Create a new app**, and
   give it the OAuth redirect URL shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`).
2. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Todoist**.

## Actions

Tasks, projects and sections are named by their ids (in a task's or project's address, or from the lists).

- **Tasks:** Add a task (`create_task`, the default: due in words such as "tomorrow at 9am" or "every Monday", or on a
  date; priority 4 urgent to 1 normal; labels by name), Add a task as you would type it (`quick_add`: "Call the
  supplier tomorrow #Orders @phone p1"), List tasks (`list_tasks`), Find tasks with a filter (`find_tasks`:
  `today | overdue`), Get a task (`get_task`), Change a task (`update_task`), Complete a task (`complete_task`), Reopen
  a task (`reopen_task`), Move a task (`move_task`), Delete a task (`delete_task`), List tasks completed
  (`list_completed_tasks`, at most 3 months at once).
- **Projects:** List projects (`list_projects`), Get a project (`get_project`), Make a project (`create_project`),
  Change a project (`update_project`), Archive a project (`archive_project`), List who shares a project
  (`list_collaborators`).
- **Sections, comments, labels:** List sections (`list_sections`), Make a section (`create_section`), List comments
  (`list_comments`), Comment on a task (`add_comment`), List labels (`list_labels`), Make a label (`create_label`).

## Triggers

- **A new task** (`new_task`), in every project or one, asked every 5 minutes.
- **A task completed** (`completed_task`), asked every 5 minutes: those ticked off in the last 7 days not seen before.
- **Something changed (as it happens)** (`event`): tasks added, changed, completed or deleted, comments, projects,
  sections and labels, as Todoist sends them, each checked against its signature (`X-Todoist-Hmac-SHA256`, the body's
  HMAC-SHA256 with the app's client secret, in Base64) and told from the others by its `X-Todoist-Delivery-ID`.
  Todoist sends these only through a Todoist app: in the [App Management Console](https://developer.todoist.com/appconsole.html),
  give the app's **Webhooks** this workflow's address, tick the events, and activate them; put the app's **client
  secret** in the credential as its webhook secret. Todoist then sends the events of each account that has signed in
  through that app - your own too, once you have signed in through it (Todoist's documentation shows how).

## Not done

- Reminders, location reminders, folders, workspaces, filters, templates, backups, sharing and inviting; attachments
  and file uploads; Todoist's `/sync` endpoint.
- Todoist's description of the API v1 says both "1 is highest" (adding and changing a task) and "1 (normal) to 4
  (urgent)" (the task itself, and its sync API) for a task's priority: this app follows the second.

## Source

Started with `openduct app from-openapi` from Todoist's own description of its API v1,
<https://developer.todoist.com/openapi.json>, fetched on 2026-11-27 (OpenAPI 3.1; no licence stated), with
[Todoist's API v1 reference](https://developer.todoist.com/api/v1/) on authorization, webhooks and errors. Names,
labels and descriptions are our own words.

## Tested

Recorded exchanges from Todoist's documented requests and answers (`tests/`), with made-up tasks, and deliveries
signed as Todoist's documentation says. Not run against a live Todoist account.
