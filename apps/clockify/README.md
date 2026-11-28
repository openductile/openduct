# Clockify

Time tracking in [Clockify](https://clockify.me): log time and run timers, keep projects, tasks, clients and tags in
order, pull the detailed time report, and start a workflow when something happens in a workspace.

Source: the OpenAPI 3.0.1 description inside Clockify's API reference, <https://docs.clockify.me/> (Clockify API v1,
fetched 2026-11-27). It states no licence, so every label and description here is in our own words; hosts, paths and
fields are as the description gives them. The webhook header is from Clockify's developer documentation
(<https://dev-docs.marketplace.cake.com/clockify/build/manifest/webhooks.html>, fetched 2026-11-27). Started with
`openduct app from-openapi`, then finished by hand.

## Signing in

- **An API key** (`clockify`): in Clockify, **Profile settings → API → Generate**. It is sent as `x-api-key`. A key
  acts as the person who made it, in every workspace they belong to.
- A workspace kept in a region (EU, USA, UK, Australia) is reached at its own address: put it in **Only for a
  workspace kept in a region**, such as `https://euc1.clockify.me/api` (`use2`, `euw2`, `apse2` for the others).
  The detailed report is still asked of Clockify's global reports address, `reports.api.clockify.me`.

Most actions need the workspace's id: **List my workspaces** gives it.

## Actions

- **List my workspaces** (`list_workspaces`), **Who I am** (`get_me`), **List a workspace's members** (`list_users`).
- **List clients**, **Add a client**, **Change a client** (`list_clients`, `create_client`, `update_client`).
- **List projects**, **Look up a project**, **Add a project**, **Change a project** (`list_projects`, `get_project`,
  `create_project`, `update_project`).
- **List a project's tasks**, **Add a task to a project**, **Change a task** (`list_tasks`, `create_task`,
  `update_task`).
- **List tags**, **Add a tag** (`list_tags`, `create_tag`).
- **List a person's time entries**, **Look up a time entry**, **Add a time entry (or start a timer)**, **Add a time
  entry for someone else**, **Change a time entry**, **Remove a time entry**, **Stop a running timer**, **List
  running timers** (`list_time_entries`, `get_time_entry`, `create_time_entry`, `add_time_entry_for`,
  `update_time_entry`, `delete_time_entry`, `stop_timer`, `list_running_timers`).
- **Detailed time report** (`detailed_report`): every entry between two times, page after page.
- **List invoices** (`list_invoices`).

Lists read every page, 50 records at a time.

## Triggers

- **Something happened in Clockify** (`event`): one event of a workspace, chosen by its Clockify name -
  `NEW_TIME_ENTRY`, `NEW_TIMER_STARTED`, `TIMER_STOPPED`, `TIME_ENTRY_UPDATED`, `TIME_ENTRY_DELETED`, `NEW_PROJECT`,
  `NEW_TASK`, `NEW_CLIENT`, `NEW_TAG`, `NEW_INVOICE`, `NEW_APPROVAL_REQUEST`, `TIME_OFF_REQUESTED` and the others the
  API lists. Turning the workflow on adds a webhook to the workspace (the key's owner must be an admin or owner, and
  the plan must allow one more webhook); Clockify answers with the webhook's token, kept sealed for the workflow.
  Each delivery carries that token in its `clockify-signature` header, and one without it is refused. Turning the
  workflow off removes the webhook. The run gets what Clockify sent: the time entry, project, client and so on.

## Not done

- Expenses, invoices beyond listing, time off, scheduling, approvals, custom fields, user groups, holidays, rates and
  shared reports (138 more operations): add them with `openduct app from-openapi` and `--choose`.
- Receipt and photo uploads (multipart), and the summary, weekly and attendance reports.
- Filters that take a list in the query once for each value (tags on time entries; clients and users on projects).
- The report actions follow the global reports address only, not a regional one.

## Tested

Recorded exchanges (`tests/`): the requests each action sends, and answers shaped as Clockify's API description says
(made-up values); the detailed report over two pages; the webhook registered, removed, a delivery run and a forged one
refused. Not run against a live Clockify account.

```sh
openduct app check apps/clockify --bar 80
openduct app test apps/clockify
```
