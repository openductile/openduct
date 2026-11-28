# Linear

Issues in [Linear](https://linear.app): opened, found, changed, moved along, commented on and archived - such as each
support ticket marked "bug" opened as an issue, or each finished issue announced in chat - and the teams, statuses,
people, labels and projects they belong to.

Linear's public API is GraphQL, at one address (`https://api.linear.app/graphql`): each action sends its query and
the step's values as the query's variables.

## Signing in

- **A personal API key** (`linear`): in Linear, **Settings → Security & access → Personal API keys**. It acts as you,
  with your permissions.
- **Sign in with Linear** (`linear_oauth`): each person connects their own account. It asks to read and write
  (`read,write`). Linear's access tokens last a day and are renewed from the refresh token, which Linear replaces each
  time.

### Registering this installation with Linear (once, by its admin, for Sign in with Linear)

1. In Linear, make a new OAuth2 application (<https://linear.app/settings/api/applications/new>), with the callback
   URL shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
2. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Linear**.

## Actions

Issues are named by their key (`ENG-123`) or id. Opening an issue takes the team's id, and statuses, people, labels
and projects their ids: the lists give them.

- **Open an issue** (`create_issue`, the default), **Change an issue** (`update_issue`: move it to a status, reassign
  it, add labels...), **Comment on an issue** (`add_comment`), **Archive an issue** (`archive_issue`).
- **List issues** (`list_issues`: by team key, status name, assignee's email, label or project name; up to 1,000),
  **Get an issue** (`get_issue`, with its comments), **Find issues** (`search_issues`: by words, best match first).
- **Who I am** (`me`), **List teams** (`list_teams`), **List a team's statuses** (`list_statuses`), **List people**
  (`list_users`), **List labels** (`list_labels`), **List projects** (`list_projects`).

## Triggers

- **A new issue** (`new_issue`) and **An issue opened or changed** (`changed_issue`), in every team or one, asked
  every 5 minutes. They work with either sign-in.
- **Something changed (as it happens)** (`event`): issues - or comments, projects, cycles, labels... - created,
  changed or removed, as Linear sends them, each checked against its signature (`Linear-Signature`, HMAC-SHA256 of the
  body). Turning the workflow on subscribes its address with a secret of its own, and turning it off ends the
  subscription; Linear lets only a workspace admin do that, so the credential must be an admin's. The run gets the
  action, the kind, the record, its address, what it was before (for a change) and who did it.

## Not done

- Linear's `webhookTimestamp` (when a delivery was sent) is in the signed body, and OpenDuct checks a delivery's age
  only from a header: a delivery is checked by its signature and told from others by its `Linear-Delivery` id, not by
  its age.
- Sub-issues, relations, cycles, documents, initiatives, customers and project updates; attachments and file uploads;
  deleting an issue (archiving keeps it restorable); Linear's own app identity for OAuth (`actor=app`).

## Source

Written by hand from Linear's GraphQL schema,
<https://github.com/linear/linear/blob/2440c73cfd297aec89c85d5e46ea2c7068d4b0ba/packages/sdk/src/schema.graphql>
(MIT licence), its webhook check in the same repository (`packages/sdk/src/webhooks/client.ts`), and Linear's
developer pages on [the GraphQL API](https://linear.app/developers/graphql),
[OAuth 2.0](https://linear.app/developers/oauth-2-0-authentication) and
[webhooks](https://linear.app/developers/webhooks), read on 2026-11-27. Linear publishes no OpenAPI description (its
API is GraphQL); this app uses that API rather than Linear's MCP server, since the triggers need its webhooks and
lists.

## Tested

Recorded exchanges from Linear's documented requests and answers (`tests/`), with made-up issues and people, and
deliveries signed as Linear's documentation says. Not run against a live Linear workspace.
