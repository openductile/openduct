# urlscan.io

Scan a web address in a sandbox browser, see what it loads and whether it is malicious, and search past scans, with
[urlscan.io](https://urlscan.io).

## Signing in

- **An API key** (`urlscan`): in urlscan.io, **Settings & API → API Keys → New API key**. It is sent in the
  `api-key` header. A free account can scan and search; the actions marked **urlscan Pro** need a Pro plan.

## Actions

- **Scan a web address** (`scan_url`): public, unlisted or private, from a country, with tags. The answer is the
  scan's id; the result is ready some 10 to 30 seconds later.
- **Get a scan's result** (`get_scan_result`): the page, verdicts, domains, addresses and files. While the scan runs,
  urlscan.io answers 404: a workflow waits (a `wait` step) and asks again.
- **Get a scanned page's source** (`get_scan_dom`).
- **Search past scans** (`search`): with urlscan.io's search syntax (`domain:example.com`, `page.title:"Brand"`,
  `date:>now-7d`), the newest first, every page up to the most.
- **Find scans that look alike** (`find_similar_scans`, urlscan Pro).
- **A hostname's history** (`hostname_history`): certificates, scans and DNS that saw it.
- **How often something was seen malicious** (`check_malicious`, urlscan Pro): an IP address, hostname, domain or web
  address.
- **Change who may see a scan** (`set_scan_visibility`, urlscan Pro).
- **List saved searches** (`list_saved_searches`), **Save a search** (`create_saved_search`), **Change a saved
  search** (`update_saved_search`), **Remove a saved search** (`delete_saved_search`).
- **List subscriptions** (`list_subscriptions`).
- **How much of the quota is used** (`get_quotas`), **Countries a scan can be made from** (`list_countries`).

## Triggers

- **A new scan matching a search** (`new_search_result`): each new scan matching a query, asked for every 15 minutes
  by default (the search quota is small on a free plan).

## Source

Facts (host, paths, fields, sign-in) from urlscan.io's own API description,
<https://docs.urlscan.io/_bundle/apis/urlscan-openapi.yaml> (OpenAPI 3.0, fetched 2026-11-27; no licence stated, so
the labels and descriptions here are our own words), and its API reference <https://docs.urlscan.io/apis/urlscan-openapi>.
Started with `openduct app from-openapi`, finished by hand.

## Not done

- Screenshots (`/screenshots/<id>.png`), file downloads and data dumps: their answers are images and archives, which
  a step does not keep yet.
- Live scanning, incidents, channels, brands and the phishing feed (urlscan Pro); a saved search's results (the API
  answers with a redirect to a search).
- urlscan.io sends no webhooks a workflow could subscribe to: the trigger polls.

## Tested

Recorded exchanges (`tests/`) made from the API description, with made-up values. Not run against a live urlscan.io
account.

```sh
openduct app check apps/urlscan --bar 80
openduct app test apps/urlscan
```
