# Travis CI

Builds on [Travis CI](https://www.travis-ci.com): started, followed, cancelled and restarted, with their jobs, logs,
branches and environment variables.

Written by hand from Travis CI's API v3 reference, <https://developer.travis-ci.com> (read 2026-11-27: builds, build,
requests, request, jobs, job, log, repositories, repository, branches, env vars, user, pagination, errors), and its
guide to starting builds, <https://docs.travis-ci.com/user/triggering-builds/>. The words here are our own.

## Signing in

- **An API token** (`travis_ci`): on your **Settings** page at travis-ci.com (or with the Travis CLI:
  `travis login --com`, then `travis token --com`). Sent as `Authorization: token ...`, with `Travis-API-Version: 3`.

A repository is named `owner/name` (the app sends the slash as `%2F`, as Travis CI asks) or by its Travis CI id.

## Actions

- **Start a build** (`trigger_build`): of a branch (the default one if not given), with a message, a commit, and
  changes to the `.travis.yml` configuration merged as chosen (`deep_merge_append` by default).
- **List a repository's build requests** (`list_requests`) and **Get a build request** (`get_request`): whether a
  request was taken, and the builds it made.
- **List a repository's builds** (`list_builds`): the newest first, of a branch, state or kind of event if given; up
  to 500.
- **List my builds** (`list_my_builds`), **Get a build** (`get_build`), **Cancel a build** (`cancel_build`),
  **Restart a build** (`restart_build`).
- **List a build's jobs** (`list_build_jobs`), **Get a job** (`get_job`), **Cancel a job** (`cancel_job`), **Restart a
  job** (`restart_job`), **Get a job's log** (`get_job_log`).
- **List my repositories** (`list_repos`), **Get a repository** (`get_repo`), **List a repository's branches**
  (`list_branches`).
- **List a repository's environment variables** (`list_env_vars`) and **Add an environment variable**
  (`add_env_var`: private unless `public` is set).
- **Get the signed-in user** (`get_user`).

## Triggers

- **A build ended** (`build_finished`), asked every 5 minutes: each build of a repository (one branch, if given) that
  ends in the state chosen - `failed` by default, or `passed`, `errored`, `canceled`.

Travis CI's own build notifications (webhooks) are set in each repository's `.travis.yml`, not through the API, and
are signed with a key the service publishes rather than a shared secret; they are not offered as a trigger.

## Not done

- Caches, crons, settings, key pairs, starring and turning repositories on or off; Travis CI Enterprise (another
  address).

## Tested

Recorded exchanges from the requests and answers the reference documents (`tests/`), with made-up values: a list
over two pages, a build started by slug and by id, the trigger's poll. Not run against a live account.

```sh
openduct app check apps/travis_ci --bar 80
openduct app test apps/travis_ci
```
