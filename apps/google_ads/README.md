# Google Ads

[Google Ads](https://ads.google.com) accounts from a workflow: list campaigns and their daily results, pause and start
campaigns, change budgets, ad groups, ads and keywords, send sales that happened offline back to Google Ads as
conversions (and retract refunded ones), ask for keyword ideas, and run any Google Ads Query Language report.

Source: the Google Ads API's discovery document,
<https://googleads.googleapis.com/$discovery/rest?version=v26> (v26, revision 20261118, fetched 2026-11-27), and its
REST reference, <https://developers.google.com/google-ads/api/rest/reference/rest>. Google's documentation text is
CC BY 4.0; the labels and descriptions here are our own. Written by hand.

## Signing in

Two things are needed: the install's registration with Google, and a **developer token**.

1. **The install registers with Google**, once ([Signing in with OAuth 2.0](../../docs/apps.md#signing-in-with-oauth-20)).
   In the [Google Cloud console](https://console.cloud.google.com): enable the **Google Ads API**, set up the OAuth
   consent screen, and create an OAuth client of type **Web application** whose redirect address is the studio's
   `https://<your studio>/oauth/callback`. Enter its client id and secret under **Settings → App sign-ins → Google
   Ads**. The permission asked for is `https://www.googleapis.com/auth/adwords`.
2. **A developer token**: in a Google Ads *manager* account, **Admin → API Center**. A new token works only with test
   accounts until Google grants it Basic or Standard access. Google ties a token to the Google Cloud project that
   first uses it, so it belongs to the install, beside the registration above.
3. **A workspace connects its account**: under Credentials, **Connect** sends the person to Google's sign-in.

Each action then takes:

- `developer_token` - sent as the `developer-token` header. **Until a sign-in can carry it** (see Not done), it is an
  input of each step: keep it out of shared workflow files.
- `customer_id` - the account to act on, as Google Ads shows it (`123-456-7890`; the dashes are taken out).
- `login_customer_id` - only when the person reaches the account through a manager account: the manager's id, sent as
  the `login-customer-id` header.

## Actions

- **List my Google Ads accounts** (`list_accessible_customers`).
- **List campaigns** (`list_campaigns`): every campaign not removed - id, name, status, serving status, type and budget.
- **Campaign results** (`campaign_performance`): each campaign's impressions, clicks, cost (`costMicros`, millionths
  of the account's currency), conversions and conversion value by day, over `TODAY`, `YESTERDAY`, `LAST_7_DAYS` (the
  default), `LAST_14_DAYS`, `LAST_30_DAYS`, `THIS_MONTH` or `LAST_MONTH`.
- **Search with a query** (`search`): any [Google Ads Query
  Language](https://developers.google.com/google-ads/api/docs/query/overview) query, every row of every page.
- **Pause or start a campaign** (`set_campaign_status`).
- **Change campaigns**, **Change campaign budgets**, **Change ad groups**, **Change ads**, **Change keywords**
  (`change_campaigns`, `change_campaign_budgets`, `change_ad_groups`, `change_ads`, `change_keywords`): the reference's
  operations as they are - `{create: {...}}`, `{update: {...}, updateMask: "..."}`, `{remove: "<resource name>"}` -
  with `validate_only` to check them first and `partial_failure` to keep the valid ones.
- **Send offline conversions** (`upload_click_conversions`): sales or sign-ups from a CRM or a shop, by the click's
  `gclid`, the conversion action, the time (`yyyy-mm-dd hh:mm:ss+hh:mm`), value and currency.
- **Adjust conversions** (`upload_conversion_adjustments`): retract or restate conversions already sent.
- **Keyword ideas** (`keyword_ideas`): from words, a page, or both, with average monthly searches and competition.

## Not done

- **The developer token in the sign-in.** A Google sign-in (OAuth) keeps only the tokens Google gives, so the
  developer token cannot be a field of the credential, and the install's registration holds only a client id and
  secret. An engine change - a registration (or an OAuth credential) holding more values, readable in a sign-in's
  headers - would move it out of the steps.
- Customer Match lists (offline user data jobs), the other 150 services (assets, experiments, reach planning,
  recommendations, billing, account links), streamed search, and media uploads.
- A trigger: Google Ads sends no webhooks; a scheduled workflow with Campaign results does the same.

## Tested

Recorded exchanges (`tests/`) for every action - paging by token, a manager account, the default period, words or a
page as keyword seeds - and a refusal in Google's words, with made-up ids and values in the shapes the discovery
document gives. Not run against a live Google Ads account.

```sh
openduct app check apps/google_ads --bar 80
openduct app test apps/google_ads
```
