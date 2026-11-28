# Airtop

Cloud browsers a workflow drives in plain words, with [Airtop](https://www.airtop.ai): start a browser, open a page, ask
about it, click, type, scroll, wait for something to show, fill a form, read a page's text, take a screenshot - on
sites a person would sign in to, too, with a saved profile.

Made with `openduct app from-openapi` from Airtop's own API description, <https://docs.airtop.ai/openapi.json>
(fetched 2026-11-27; it states no licence, so only its facts are used, and the labels and descriptions here are our
own). The API reference: <https://docs.airtop.ai/api-reference/airtop-api>.

## Signing in

- **An API key** (an Airtop credential): from the [Airtop Developer Portal](https://portal.airtop.ai/api-keys); sent as
  a bearer token. Airtop counts each session's time and each AI request in credits on your account.

## Actions

A browser session holds windows; most actions name both, by the ids **Start a browser session** and **Open a page in
a new window** give.

- **Start a browser session** (`create_session`), **Look up** (`get_session`) and **List browser sessions**
  (`list_sessions`), **End a browser session** (`end_session`), **Keep the session's profile when it ends**
  (`save_profile`), **Remove saved profiles** (`delete_profiles`).
- **Open a page in a new window** (`open_window`), **Go to a page** (`go_to_page`), **List a session's windows**
  (`list_windows`), **Look up a window** (`get_window`: its live view address, to show a person), **Close a window**
  (`close_window`).
- **Ask about a page** (`page_query`, the default): Airtop's AI answers a question about the page - as JSON of a
  schema, when one is given.
- **Gather items over many pages** (`extract_pages`), **Read a page's text** (`scrape_page`), **Take a screenshot**
  (`screenshot`).
- **Click on a page** (`click`), **Type on a page** (`type_text`), **Point at something on a page** (`hover`),
  **Scroll a page** (`scroll`), **Wait for something on a page** (`wait_for`): what to act on is described in plain
  words.
- **Learn a page's form** (`learn_form`) and **Fill a form** (`fill_form`) with the values given.
- **List files** (`list_files`) and **Look up a file** (`get_file`): files the sessions downloaded, with a download
  address. **Look up a request's status** (`get_request_status`).

Each answer is Airtop's own: the result in `data` (an AI request's answer in `data.modelResponse`), with `meta`
(credits used, screenshots) and any `warnings`. The AI's work may take minutes: those actions wait up to 5 minutes, and
each can be told to stop past a number of seconds or credits.

A new session takes a few seconds to start (rarely a minute): **Look up a browser session** says `running` once it is
ready; a workflow may wait for that before opening a window.

## Not done

- Uploading a file into a page (a file input), the asynchronous forms of the AI actions, and Airtop's saved
  automations (list, describe, run).
- Connecting Puppeteer, Playwright or Selenium to a session (its `cdpUrl` is in the session's answer).

## Tested

Recorded exchanges (`tests/`): those made from the API description's schemas, a question answered as JSON by hand,
and a refusal in Airtop's problem form (a detail and its errors). Not run against a live Airtop account.

```sh
openduct app check apps/airtop --bar 80
openduct app test apps/airtop
```
