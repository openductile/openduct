# GitHub

Issues, comments and files of [GitHub](https://github.com) repositories.

## Signing in

Two ways:

- **A personal access token** (a GitHub credential): at **Settings → Developer settings → Personal access tokens**, a
  fine-grained token for the repositories it needs, with **Issues** and **Contents** read and write (or **Pull
  requests** read, to list them).
- **Sign in with GitHub** (a GitHub sign-in): the installation's admin registers an OAuth app once, at **Settings →
  Developer settings → OAuth Apps → New OAuth App**, with the callback URL shown under **Settings → App sign-ins**
  (`https://<your studio>/oauth/callback`), and copies its client ID and a client secret there. A workspace then
  **Connects** under Credentials; it asks for the `repo` scope.

## Actions

- **Open an issue** (`open_issue`, the default): title, text (Markdown), labels, for whom.
- **Change an issue** (`update_issue`): its title or text, close it (and why), its labels and assignees (these take
  the place of those it had).
- **Comment on an issue or pull request** (`comment_on_issue`).
- **Look up an issue** (`get_issue`).
- **List issues** (`list_issues`): open, closed or all, by label or since a time; pull requests among them. Every page,
  up to 1,000.
- **Read a file** (`read_file`): a text file under 1 MB, as text, with its `sha`.
- **List a folder's files** (`list_files`).
- **Save a file** (`save_file`): a commit writing a text file - a new one, or a new version (give the `sha` Read a
  file gave).
- **Remove a file** (`delete_file`): a commit removing it.
- **List pull requests** (`list_pull_requests`) and **List releases** (`list_releases`): every page.

Requests ask for the REST API's version `2026-03-10`.

Not yet: binary files (pictures and the like), and GitHub's own events (a webhook trigger in a workflow receives
them).

## Tested

Recorded exchanges built from GitHub's REST API reference (`tests/`), with both sign-ins.

Not run against a live GitHub account.
