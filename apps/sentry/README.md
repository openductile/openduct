# Sentry

The errors and issues [Sentry](https://sentry.io) catches in your software: issues found, looked up, resolved,
ignored and assigned; their events; projects, teams and members; releases and deploys recorded; user feedback.

Made with `openduct app from-openapi` from Sentry's own API description,
<https://github.com/getsentry/sentry-api-schema/blob/d8e95f7f3c95b8aa5f60983f6f2239b71ac529d6/openapi-derefed.json>
(OpenAPI 3.0, the commit of 2026-11-14). The description names its licence as Apache 2.0; the repository it is in is
under FSL-1.1-Apache-2.0. Labels are reworded in plain words. The vendor's API reference: <https://docs.sentry.io/api/>.

## Signing in

- **An auth token** (`sentry`), sent as `Authorization: Bearer` - one of:
  - a personal token: in Sentry, **User settings → Personal Tokens**, with the scopes the actions need (reading and
    changing issues and events, reading projects, teams, members and the organization, and releases);
  - an internal integration's token: **Settings → Developer Settings**, an internal integration for the organization
    (the same integration gives the webhook for **An issue created, resolved, assigned, archived or unresolved**, below).
- **Region**: an organization's data is kept in the `us` region (the default) or the `de` one, each at its own address
  (`us.sentry.io`, `de.sentry.io`); give `de` for an organization kept in Europe.

## Actions

- **List organizations** (`list_organizations`): The organizations the token can reach.
- **List issues** (`list_issues`): An organization's issues: unresolved ones unless a search says otherwise, every page.
- **List members** (`list_members`): The people in an organization.
- **List cron monitors** (`list_monitors`): An organization's cron monitors, with their environments.
- **List projects** (`list_projects`): An organization's projects.
- **List releases** (`list_releases`): An organization's releases, the newest first.
- **List teams** (`list_teams`): An organization's teams.
- **List a project's error events** (`list_project_events`): The error events of one project.
- **List a project's client keys** (`list_keys`): A project's client keys (DSNs), which apps send their errors with.
- **List an issue's events** (`list_issue_events`): Each time an issue happened.
- **Look up an organization** (`get_organization`): An organization's details: its teams, projects and the access the token has.
- **Create a release** (`create_release`): A new release of one or more projects, with its commits if given.
- **Find an event by its id** (`get_eventid`): The project, issue and details of the event with this id.
- **Look up a release** (`get_release`): One release's details, with its health data if asked.
- **Record a deploy** (`create_deploy`): A deploy of a release to an environment, such as production.
- **Find an issue by its short id** (`get_shortid`): The issue with this short id (such as BACKEND-1A), and its project.
- **Look up a project** (`get_project`): One project's details.
- **List user feedback** (`list_user_feedback`): What users of a project wrote when an error happened.
- **Create a project** (`create_team_project`): A new project, belonging to a team.
- **Look up an issue** (`get_issue`): One issue's details: its status, counts, first and last seen, and who it is assigned to.
- **Change issues** (`update_issues`): Resolves, ignores, assigns or otherwise changes up to 1,000 issues at once, those a search finds.
- **Look up a project's event** (`get_event`): One event of a project, in full.
- **Look up an issue's event** (`get_issue_event`): One event of an issue: by its id, or the latest, oldest or recommended one.
- **Change a release** (`update_release`): Changes a release: when it went out, its commits, its address or its status.
- **Change an issue** (`update_issue`): Resolves, ignores, assigns, bookmarks or otherwise changes one issue.

## Triggers

- **A new issue** (`new_issue`): Each issue Sentry finds for the first time in an organization, the newest asked for every few minutes.
- **An issue created, resolved, assigned, archived or unresolved** (`issue_changed`): Each change to an issue, as Sentry sends it to an internal integration's webhook, signed with the integration's client secret.

**An issue created, resolved, assigned, archived or unresolved** needs an internal integration in Sentry (**Settings → Developer Settings**) whose webhook URL is
the address the studio shows for the workflow, with read access to issues and events and the issue webhooks chosen.
Sentry signs each delivery with the integration's **Client Secret** (an HMAC-SHA256 of the body, in the
`Sentry-Hook-Signature` header): put it in the credential's webhook secret. Deliveries of other resources (errors,
comments, alerts) are answered and run nothing.

## Not done

- 265 more operations the description offers (dashboards, alerts and monitors' changes, replays, SCIM, debug files); a custom app or a later version can add them.
- Lists a query takes several times (environment, project, expand, collapse, an issue id for each issue) are sent once; Change issues finds its issues with a search instead.
- A self-hosted Sentry: the app reaches sentry.io's regions only.
- Files sent as uploads (release files, debug files, source maps) and the DSN sign-in.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Sentry account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Sentry documents it; none was run against a live account.

```sh
openduct app check apps/sentry --bar 80
openduct app test apps/sentry
```
