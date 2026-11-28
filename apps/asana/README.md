# Asana

Tasks, projects, sections and comments in [Asana](https://asana.com): added, changed, moved along and found.

Made by `openduct app from-openapi` from Asana's own API description,
<https://github.com/Asana/openapi/blob/466e40b476c0/defs/asana_oas.yaml> (Apache 2.0 licence, OpenAPI 3.0), on
2026-11-27, then finished by hand: plain names and inputs, Asana's page tokens, the task fields most workflows read.
The vendor's API reference: <https://developers.asana.com/reference/rest-api-reference>.

## Signing in

Two ways:

- **A personal access token** (`asana`): in Asana's [developer console](https://app.asana.com/0/my-apps),
  **Create new token**. It acts as you, with all you can see and do.
- **Sign in with Asana** (`asana_oauth`), for a workspace that connects its own account.

### Registering this installation with Asana (once, by its admin)

1. In the [developer console](https://app.asana.com/0/my-apps), **Create new app**.
2. Under **OAuth**, add the redirect URL shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`), and choose the permission scopes `tasks:read`, `tasks:write`,
   `tasks:delete`, `projects:read`, `projects:write`, `stories:read`, `stories:write`, `workspaces:read`,
   `users:read`, `tags:read` and `teams:read`.
3. Copy the **Client ID** and **Client secret** into **Settings → App sign-ins → Asana**.

Then, under Credentials, add an Asana credential and **Connect**.

## Actions

- **Add a task** (`create_task`, the default), **Look up a task** (`get_task`), **Change a task** (`update_task`),
  **Mark a task done** (`complete_task`), **Delete a task** (`delete_task`: to the trash, for 30 days).
- **List a project's tasks** (`list_project_tasks`), **List someone's tasks** (`list_tasks`: a person's in a
  workspace, or a section's), **Search tasks** (`search_tasks`: Asana's advanced search, on its paid plans only - a
  free workspace is answered 402).
- **List a task's subtasks** (`list_subtasks`), **Add a subtask** (`create_subtask`).
- **Comment on a task** (`add_comment`), **List a task's comments** (`list_comments`).
- **Move a task to a section** (`move_to_section`), **Add a task to a project** (`add_to_project`), **Tag a task**
  (`add_tag`), **Add followers to a task** (`add_followers`).
- **List a project's sections** (`list_sections`), **Add a section to a project** (`create_section`).
- **List projects** (`list_projects`), **Look up a project** (`get_project`), **Make a project** (`create_project`).
- **List workspaces** (`list_workspaces`), **List a workspace's people** (`list_users`), **List a workspace's tags**
  (`list_tags`), **List an organization's teams** (`list_teams`): the ids the other actions ask for.

People are named as `me`, an email address or their id.

## Triggers

All asked every 5 minutes, for one project:

- **A new task in a project** (`new_task`): each task added (tasks already done are left out).
- **A task added or changed in a project** (`task_changed`): each task added or changed; a task changed again runs
  again.
- **A task done in a project** (`task_completed`): each task marked as done.

## Not done

- Asana's **webhooks** as a trigger: Asana opens each webhook with a handshake - it sends a secret of its own in an
  `X-Hook-Secret` header, which the receiver must send back in the same header and keep to check later deliveries.
  OpenDuct's webhook triggers neither answer with a header nor keep a secret the vendor chose. Until then, the polls
  above.
- Attachments (uploads are multipart), custom field definitions, portfolios, goals, status updates, time tracking,
  project templates: 244 more operations of the description.
- Search is not paged: Asana gives at most 100 results a search.

## Tested

Recorded exchanges built from Asana's API description and reference (`tests/`): what each request sends, answers of
the documented shape (two pages followed by Asana's offset token), refusals in Asana's own words, and the polls.
Not run against a live Asana account.

```sh
openduct app check apps/asana --bar 80
openduct app test apps/asana
```
