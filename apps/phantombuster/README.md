# Phantombuster

Launch [Phantombuster](https://phantombuster.com)'s Phantoms - automations that gather leads from LinkedIn, Sales
Navigator, maps listings and the web - read what each launch found, and keep the workspace's leads.

Made by `openduct app from-openapi` from Phantombuster's own API description,
<https://github.com/phantombuster/public-gists/blob/30d0778a0456/swagger-api-v2.json> (ISC licence; Phantombuster
API v2 2.0.0, OpenAPI 3.0), fetched 2026-11-27, then finished by hand in our own words. The vendor's API reference:
<https://hub.phantombuster.com/reference>.

## Signing in

- **An API key** (`phantombuster`): in Phantombuster, **Workspace settings → API keys**, add a key. It is sent in the
  `X-Phantombuster-Key` header.

## Actions

- **Launch a Phantom** (`launch_agent`, the default): puts it in the queue with its saved settings, or with others for
  this launch (`argument`, or `bonus_argument` added to the saved ones). It gives the launch's `containerId`. A launch
  spends the workspace's execution time, so it is a write: a preview shows it and does not send it.
- **Get a Phantom's latest output** (`get_agent_output`): whether its latest launch is still running, how far it has
  got, and its console output.
- **Look up a Phantom** (`get_agent`) and **List Phantoms** (`list_agents`).
- **List a Phantom's launches** (`list_launches`), **Look up a launch** (`get_launch`, with its results and output if
  asked), **Get a launch's results** (`get_launch_result`: the `resultObject`, a JSON text, often a list of the
  records found) and **Get a launch's console output** (`get_launch_output`).
- **Stop a Phantom** (`stop_agent`), softly if asked, keeping what it found so far.
- **Check usage and limits** (`get_usage`): the plan, and the execution time, emails found and AI credit used.
- **Leads** (Phantombuster marks these beta): **List the leads of a list** (`list_leads`, 100 a page, every page),
  **Save a lead** (`save_lead`, by its LinkedIn profile), **Save many leads** (`save_leads`) and **Remove leads**
  (`delete_leads`).

A Phantom's id is in its address in Phantombuster (`.../phantoms/<id>/...`); a launch's id is the `containerId` a
launch gives.

## Triggers

- **A Phantom's launch finished** (`launch_finished`): each launch of the Phantom that has ended, read with its
  results (`get_launch`, `with_result`), asked every 5 minutes. The first poll notes the launches there are and runs
  none.

Phantombuster's own webhook (a Phantom's notification settings) is not signed, so it is not offered as a trigger: a
workflow's own webhook trigger can take it.

## Not done

- Launch's answer: the API description does not describe it; Phantombuster's reference and help pages say it gives
  the launch's container id, which the recordings write as `containerId`.
- The launch that streams its progress (`launch-sync`, NDJSON), scheduling a launch, saving a Phantom's settings and
  schedule, scripts, branches, lead lists themselves (their answers are not described), lead and company objects,
  identities, and the organisation's settings (55 more operations the description holds).
- The `X-Phantombuster-Org` header, for a key that reaches more than one workspace.

## Tested

Recorded exchanges (`tests/`), first made from the API description's schemas and then written by hand with made-up
values: every action, a lead list over two pages, the trigger's first poll and a finished launch read with its
results, and a launch that is not there refused (404). Not run against a live Phantombuster account.

```sh
openduct app check apps/phantombuster --bar 80
openduct app test apps/phantombuster
```
