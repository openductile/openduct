# Cortex

Analyse observables - IP addresses, domains, URLs, hashes, email addresses - with the analyzers of your own
[Cortex](https://strangebee.com/cortex/) server (StrangeBee's analysis engine, the companion of TheHive), and read
the reports.

Written by hand from StrangeBee's Cortex API guide, <https://docs.strangebee.com/cortex/api/api-guide/> (Cortex 2 and
newer; read 2026-11-27). The guide is StrangeBee's ("all rights reserved"): only its facts are used - addresses,
fields, answers - and the words here are our own.

## Signing in

- **An API key** (`cortex`): your Cortex's address (such as `https://cortex.example.com:9001`) and an API key. In
  Cortex, an orgAdmin opens **Organization** and, on the user's row, **Create API Key**, then **Reveal** (a
  superAdmin does the same under **Users**). The user needs the `analyze` role to run analyzers. Sent as
  `Authorization: Bearer`.

Cortex runs on your own server, so the app reaches only the address the credential gives.

## Actions

- **List analyzers** (`list_analyzers`): the analyzers enabled in your organization, each with the kinds of
  observable it takes.
- **List the analyzers for a kind of observable** (`list_analyzers_for_type`): such as `ip`, `domain`, `url`, `hash`.
- **Get an analyzer** (`get_analyzer`).
- **Analyse an observable** (`run_analyzer`): starts a job; its TLP, a note and the analyzer's parameters if given.
  Cortex gives back a recent report of the same observable it keeps, unless `force` is set.
- **Wait for a job's report** (`wait_for_report`): waits for the job to finish (a minute by default, at most four),
  then gives it with its report. After **Analyse an observable**, this is the step that reads the result.
- **Get a job's report** (`get_job_report`): the report as it stands; while the job runs, only its status.
- **Get a job** (`get_job`): its status, without the report.
- **List jobs** (`list_jobs`): the newest first (100 by default), of one status or kind of observable if given.
- **Get what a job found** (`get_job_artifacts`): the observables extracted from a report, when the analyzer extracts
  them.
- **Delete a job** (`delete_job`): marked deleted; Cortex keeps its data.

## Triggers

- **A job finished** (`job_finished`), asked every 5 minutes: each job among the newest 100 that ended with success
  (or, if chosen, in failure).

## Not done

- Files as observables: they are sent as a multipart upload.
- Responders: StrangeBee's API guide does not describe their calls.
- Administration - organizations, users, keys, enabling and configuring analyzers - which is the Cortex admin's
  work in Cortex itself.

## Tested

Recorded exchanges from the requests and answers the API guide shows (`tests/`), with made-up values; the trigger's
poll is recorded too. Not run against a live Cortex.

```sh
openduct app check apps/cortex --bar 80
openduct app test apps/cortex
```
