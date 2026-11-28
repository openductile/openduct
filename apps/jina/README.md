# Jina AI

Read any web page or PDF as clean text, and search the web, with [Jina AI](https://jina.ai)'s Reader: a page's
words for an AI step, a summary, a price check, or a list of what a search finds.

Written by hand from Jina AI's API reference, <https://docs.jina.ai> (read 2026-11-27), and the OpenAPI descriptions
the two services publish, <https://r.jina.ai/openapi.json> and <https://s.jina.ai/openapi.json> (Jina Reader API
0.5.0+77cadef, fetched 2026-11-27; the Reader is the open-source jina-ai/reader, Apache-2.0 licence). The labels and
descriptions are our own words.

Jina AI's embedding, reranking, classifying and chat models (`api.jina.ai`) are models, used through the AI step, so
they are not in this app.

## Signing in

- **An API key** (`jina`): at [jina.ai](https://jina.ai), the API dashboard gives a key (`jina_...`), with free
  tokens to start. It is sent as `Authorization: Bearer`. Each page read and each search spends the key's tokens.

## Actions

- **Read a web page** (`read_page`, the default): the page at `url` (a PDF too) as Markdown, or as HTML, plain text
  or a screenshot's address (`return_format`). It can keep only a part of the page (`target_selector`, a CSS
  selector), leave parts out (`remove_selector`), wait for something to show, list the page's links and images,
  describe images, and read the page afresh rather than from Jina's copy. It waits up to 2 minutes.
- **Search the web** (`search_web`): the best results for `q` (at most 20), on one site if asked, for a country,
  language or place. Each result's page is read as text too, unless `read_pages` is false (quicker and cheaper:
  titles, addresses and descriptions only).
- **Search the news** (`search_news`): news articles for `q`, with their dates.

These change nothing anywhere, so a preview runs them for real (and spends a few tokens).

## Not done

- The Reader's other options (cookies, a proxy, scripts run on the page, the viewport, Markdown styles, image
  search) and its EU addresses (`eu.r.jina.ai`, `eu.s.jina.ai`).
- Reading a PDF or HTML given as content rather than at an address.
- Answers streamed as they come (`text/event-stream`).

## Tested

Recorded exchanges written from the API reference with made-up values (`tests/`): a page read with a selector and
its links, a PDF read as text, a search on one site without reading the pages, a search that reads them, a news
search, and a key with no balance left refused (402). Not run against a live Jina AI account.

```sh
openduct app check apps/jina --bar 80
openduct app test apps/jina
```
