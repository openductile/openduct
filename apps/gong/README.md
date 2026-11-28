# Gong

The calls [Gong](https://www.gong.io) records and what it learns from them: calls listed and looked up, with their
people, briefs, topics and transcripts; users and their activity and talk stats; scorecards; library folders;
meetings scheduled; Engage flows; calls and online interactions added from other systems.

Made with `openduct app from-openapi` from Gong's own API description: each page of Gong's API reference,
<https://help.gong.io/apidocs/introduction>, carries an OpenAPI 3.0 document for its call; the 24 pages used were
fetched on 2026-11-27 (their `.md` versions, listed in <https://help.gong.io/llms.txt>) and joined into one. Gong's
documentation has no open licence: hosts, paths and fields come from it, and every label and description here is in
our own words. The calls that take a filter in their body (calls with details, transcripts, users, stats, answered
scorecards) were written by hand from those documents, with plain inputs in place of the filter.

## Signing in

- **An access key and its secret** (`gong`): a technical administrator of Gong creates them on the Gong API page
  (<https://app.gong.io/company/api>, **Create**); they are sent as HTTP basic. The same page shows the company's API
  address (such as `https://us-12345.api.gong.io`): give it, or `api.gong.io` is used.

## Good to know

- Gong allows a company 3 calls a second and 10,000 a day, unless it asks Gong for more.
- **List calls** gives each call's basics; **List calls with their details** gives who took part and, with a content
  selector, the brief, outline, topics, trackers and CRM records; **Get calls' transcripts** gives what was said.

## Actions

- **List calls** (`list_calls`): Recorded calls that started in a time range: title, start, length, owner, direction and the system that recorded them; every page.
- **List users** (`list_users`): The company's Gong users, active or not; every page.
- **List flows** (`list_flows`): The Engage flows a user can see: the company's, their own and those shared with them; every page.
- **List scorecards** (`list_scorecards`): Every scorecard of the company and its questions.
- **List the calls in a library folder** (`list_folder_calls`): The calls kept in one library folder.
- **List library folders** (`list_folders`): The public library folders of a workspace.
- **Add a call** (`add_call`): Adds a call recorded elsewhere (a phone system, say) to Gong; its recording follows with the media address or is uploaded later.
- **List workspaces** (`list_workspaces`): The company's workspaces.
- **List answered scorecards** (`list_answered_scorecards`): Scorecards filled in for calls: of calls or reviews in a date range, of some users or scorecards if given; every page.
- **Look up a call** (`get_call`): One call's basics: title, start, length, owner, direction and the system that recorded it.
- **Schedule a meeting** (`schedule_meeting`): A new Gong meeting for a user, with its invitees; Gong records it.
- **List prospects' flow instances** (`list_flow_instances`): The Engage flow instances of the prospects given (by CRM id), or the flow instances given.
- **Look up a user** (`get_user`): One Gong user, active or not.
- **Change a meeting** (`update_meeting`): Changes a Gong meeting's time, title or invitees.
- **List trackers** (`list_trackers`): The keyword and smart trackers, of a workspace if given.
- **List calls with their details** (`list_calls_with_details`): Calls that match, with who took part and, if asked, what was said about them (brief, outline, topics, trackers), their CRM records and how people interacted; every page.
- **Get calls' transcripts** (`get_transcripts`): What was said in calls, by whom and when: the calls given, or those of a time range; every page.
- **Get users' interaction stats** (`get_interaction_stats`): How each user talks on calls in a date range (talk ratio, longest monologue, questions asked and the like), from calls Gong analyses; every page.
- **Get users' daily activity** (`get_daily_activity`): What each user did day by day in a date range: calls made and attended, calls shared and commented on, and the like; every page.
- **Get users' activity totals** (`get_activity_totals`): What each user did in a date range, added up: calls made and attended, calls shared and commented on, and the like; every page.
- **Find users** (`find_users`): Gong users by their ids, or those added in a time range, active or not; every page.
- **Add prospects to a flow** (`add_to_flow`): Puts 1 to 100 prospects (by CRM id) in an Engage flow, owned by a user.
- **Record a digital interaction** (`record_interaction`): Tells Gong that a person looked at or did something online (opened a document, visited a page), to show beside their deal.
- **Delete a meeting** (`delete_meeting`): Removes a Gong meeting.

## Triggers

- **A new call** (`new_call`): Each call recorded in the last day that Gong has finished with, asked for every few minutes (calls appear once Gong has processed them).

**A new call** is asked every 15 minutes for the calls of the last day, since a call appears only once Gong has
processed its recording; those not seen before run.

## Not done

- Signing in at Gong (OAuth): Gong's token answer names the company's own API address (`api_base_url_for_customer`), which a sign-in does not keep yet (an engine change: keeping a field of the token answer, as `callback_fields` keeps one of the callback).
- Uploading a call's recording (a file upload), CRM uploads, data-privacy erasure, permission profiles, auditing logs and the AI briefs.
- Gong's webhooks (automation rules in Gong) carry a JWT signed with Gong's RSA key in the Authorization header; a trigger checks HMAC signatures and tokens only (an engine change: RSA-signed JWTs).

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Gong account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Gong documents it; none was run against a live account.

```sh
openduct app check apps/gong --bar 80
openduct app test apps/gong
```
