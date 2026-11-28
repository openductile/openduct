# Jira

Issues in [Jira Cloud](https://www.atlassian.com/software/jira): opened, found, commented on, changed and moved
along.

## Signing in

- **The site's name**: `mycompany` of `mycompany.atlassian.net`.
- **Your email and an API token**: at [id.atlassian.com](https://id.atlassian.com/manage-profile/security/api-tokens),
  **Security → Create and manage API tokens → Create API token**. The token acts as you: what you may see and do in
  Jira, it may.

## Actions

- **Open an issue** (`open_issue`, the default): project, summary, type (Task unless said), description, labels,
  priority, for whom (their account id); gives its id and key (`SUP-13`).
- **Look up an issue** (`get_issue`).
- **Find issues** (`find_issues`): by a JQL search (`project = SUP AND created >= -7d`); every page, up to 1,000.
- **Change an issue** (`update_issue`): only the fields given change.
- **Comment on an issue** (`comment_on_issue`).
- **List an issue's comments** (`list_comments`): every page.
- **List where an issue can move** (`list_transitions`) and **Move an issue along** (`move_issue`): its status, by
  the step's id.

A description or a comment is plain text, written as Jira's document format with one paragraph a line.

Not yet: other fields of a project's own (custom fields), attachments, and Jira Data Center (a server of your own).

## Tested

Recorded exchanges built from Atlassian's REST API v3 reference (`tests/`), with the search at `/search/jql`, which
took the place of `/search` in 2025.

Not run against a live Jira site.
