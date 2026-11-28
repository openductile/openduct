# Ghost

The published posts, pages, tags, authors and membership tiers of a [Ghost](https://ghost.org) site, and the events
it sends (a post published, a member added); on Ghost(Pro) (`*.ghost.io`) or your own server.

## Signing in

- **A Content API key (Ghost(Pro))** (`ghost`): your site's Ghost(Pro) name (the `acme` of `acme.ghost.io`) and its
  Content API key.
- **A Content API key (your own Ghost)** (`ghost_self_hosted`): your Ghost's Content API address (your admin
  address followed by `/ghost/api/content`) and its key.

In Ghost Admin, **Settings → Integrations → Add custom integration**: it shows the Content API key (and the API URL).
The key is sent in the address (`?key=`), as Ghost's Content API takes it, with `Accept-Version: v6.0`. It reads
only what the site publishes. For the trigger, the credential also holds the secret given to the integration's
webhooks.

## Actions

- **List posts** (`list_posts`): the newest first, of a tag or author or a filter (`tag:news`, `featured:true`),
  with authors and tags if asked; every page up to the most. **Get a post** (`get_post`, by id or slug).
- **List pages** (`list_pages`), **Get a page** (`get_page`).
- **List tags** (`list_tags`), **Get a tag** (`get_tag`), **List authors** (`list_authors`), **Get an author**
  (`get_author`).
- **List membership tiers** (`list_tiers`), **The site's settings** (`get_settings`).

## Triggers

- **Something happened on the site** (`site_event`): each event a webhook of the custom integration sends (a post
  published or changed, a page, a tag, a member added, changed or deleted), checked against its `X-Ghost-Signature`
  (`sha256=<HMAC-SHA256 of the body followed by the time>, t=<time in milliseconds>`), and refused if signed more
  than 5 minutes before it came. The webhooks are made in Ghost Admin (the integration's **Add webhook**: the
  event, this workflow's address, and the secret), one for each event wanted.
- **A new post published** (`new_post`): each post published, the newest asked for every few minutes; with no
  webhook to set up.

## Source

Written by hand from Ghost's documentation (<https://ghost.org/docs/content-api/>, <https://ghost.org/docs/webhooks/>;
its source <https://github.com/TryGhost/docs/tree/6eb8d8ca8fb48658c672c10c50db7fe6f87024cb>, fetched 2026-11-27, no
open licence stated, so the labels and descriptions here are our own words) and, for the webhook signature, Ghost's own
source (<https://github.com/TryGhost/Ghost/blob/4131364b9648e8795f7b3275f6c94e1728ba793a/ghost/core/server/services/webhooks/webhook-trigger.js>,
MIT licence). Ghost publishes no OpenAPI description.

## Not done

- **Writing (the Admin API: posts, pages, members, newsletters, images, webhooks).** The Admin API signs each request
  with a short-lived token (a JWT, HS256, made from the Admin API key's id and secret, five minutes long, audience
  `/admin/`). The app kit has no sign-in that makes such a token, so the app cannot yet create a post or a member:
  that needs a JWT sign-in in the engine. The trigger's webhooks are therefore set up in Ghost Admin, not registered
  through the API.
- Members' own (private) content, and posts not yet published: the Content API gives published content only.

## Tested

Recorded exchanges (`tests/`) made from the documented requests and answers, with made-up values; the webhook
deliveries are signed as Ghost's source signs them, with a tool of our own. Not run against a live Ghost site.

```sh
openduct app check apps/ghost --bar 80
openduct app test apps/ghost
```
