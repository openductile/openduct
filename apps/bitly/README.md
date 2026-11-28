# Bitly

Short links, QR codes and campaigns in [Bitly](https://bitly.com), and how often each is clicked.

Made by `openduct app from-openapi` from Bitly's own API description, <https://dev.bitly.com/v4/v4.json> (fetched
2026-11-27; Apache 2.0 licence; Bitly API 4.0.0, OpenAPI 3.0), then finished by hand: plain names, the custom
domains and tags given whole, a sign-in at Bitly, a trigger. The vendor's API reference:
<https://dev.bitly.com/api-reference>.

## Signing in

- **An access token** (`bitly`): in Bitly, **Settings → Developer settings → API → Generate token**
  (<https://app.bitly.com/settings/api/>). Sent as `Authorization: Bearer`.
- **Sign in with Bitly** (`bitly_oauth`): each install registers its own app with Bitly, once. In Bitly's
  **Developer settings**, **Register new app**, with the redirect URI shown in OpenDuct
  under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`), and enter its client ID and client
  secret there. Bitly asks for no scopes; its tokens do not run out, so there is nothing to refresh.

Most actions need a group's GUID: **Get the signed-in user** gives the default one (`default_group_guid`), **List
groups** all of them.

## Actions

- **Shorten a link** (`shorten_link`): Converts a long url to a Bitlink.
- **Create a short link, with a title, tags or a keyword** (`create_link`): Converts a long url to a Bitlink and sets additional parameters.
- **Get a short link** (`get_link`): Returns information for the specified link.
- **Change a short link** (`update_link`): Updates fields in the specified link.
- **Delete a short link** (`delete_link`): Delete an unedited hash Bitlink.
- **Expand a short link** (`expand_link`): Returns the short link and long URL for the specified link.
- **Give a short link a custom back-half** (`add_custom_back_half`): Add a keyword (or "custom back-half") to a Bitlink with a Custom Domain (domains must match).
- **List a group's short links** (`list_links`): Returns a paginated collection of Bitlinks for a group, newest first; every page.
- **A short link's total clicks** (`get_clicks_summary`): Returns the click counts for the specified link rolled up into a single field.
- **List a short link's clicks by day** (`list_link_clicks`): Returns the click counts for the specified link in an array based on a date.
- **A short link's clicks by country** (`list_link_countries`): Returns the country origins of click traffic for the specified link.
- **A short link's clicks by referrer** (`list_link_referrers`): Returns referrer click counts for the specified link.
- **A group's clicks by day** (`list_group_clicks`): The clicks on all the short links of a group, by day.
- **How many links a group shortened, by day** (`list_shorten_counts`): Returns all the shorten counts for a group.
- **Create a QR code** (`create_qr_code`): Create a new QR Code and return its metadata.
- **Get a QR code** (`get_qr_code`): Gets the QR code with a matching id.
- **List a group's QR codes** (`list_qr_codes`): Retrieves a list of QR codes matching the filter settings; every page.
- **List campaigns** (`list_campaigns`): Returns the campaigns for the current authenticated user.
- **Create a campaign** (`create_campaign`): Creates a new campaign.
- **List a group's tags** (`list_tags`): Returns the tags currently used in the specified group.
- **List the custom domains you can shorten with** (`list_domains`): Fetch the names of the custom domains the authenticated user can shorten links with, across every organization and group they belong to.
- **Get the signed-in user** (`get_user`): Returns information for the current authenticated user.
- **List groups** (`list_groups`): Returns a list of groups in the organization.
- **Get a group** (`get_group`): Returns details for a group.
- **List organizations** (`list_organizations`): Retrieve a list of organizations for the authenticated user.

The clicks actions count by day over all time (`unit: day`, `units: -1`); `unit_reference` ends them earlier.

## Triggers

- **A new short link** (`new_link`), asked every 5 minutes: each short link made in a group (its GUID).

Bitly documents no signature on its webhooks (it calls back with an OAuth client of the receiver's), so they are not offered:
a delivery could not be told from a forged one.

## Not done

- 120 more operations of the description (Bitly Sites, bulk uploads, custom domain purchase, webhooks, more
  metrics); the studio's list or `--choose` adds them.
- Filters that are lists in the query (`tags`, `launchpad_ids`, `encoding_login` for `list_links`; `creating_login`,
  `qrc_type`, `tags` for `list_qr_codes`), sent once for each value.
- The QR code image (`/qr-codes/{id}/image`): an image, not a record.

## Tested

Recorded exchanges made from the API description's schemas and Bitly's documented answers (`tests/`), with made-up
values; the trigger's poll is recorded too (`tests/trigger_new_link.yaml`). Not run against a live Bitly account.

```sh
openduct app check apps/bitly --bar 80
openduct app test apps/bitly
```
