# SerpApi

Google's results as data, through [SerpApi](https://serpapi.com): web search, Google News, Google Maps places and
their reviews, Google Shopping, Google Jobs and Google Images - for price checks, lead lists of local businesses,
review monitoring, and alerts when a name is in the news.

Written by hand, in our own words, from SerpApi's API reference (read 2026-11-27): the
[Google Search API](https://serpapi.com/search-api), [Google News](https://serpapi.com/google-news-api),
[Google Maps](https://serpapi.com/google-maps-api), [Google Maps Reviews](https://serpapi.com/google-maps-reviews-api),
[Google Shopping](https://serpapi.com/google-shopping-api), [Google Jobs](https://serpapi.com/google-jobs-api),
[Google Images](https://serpapi.com/images-results), the [Account API](https://serpapi.com/account-api) and
[status and error codes](https://serpapi.com/api-status-and-error-codes). We found no OpenAPI description from SerpApi.

## Signing in

- **An API key** (`serpapi`): in SerpApi, **Your Account** shows the private API key. SerpApi takes it in the address
  (`api_key=`), as its reference says.

## Actions

- **Search Google** (`google_search`, the default): the web results (position, title, link, snippet), searched from
  a place, country and language if asked, with Google's filters (`tbs`) and a later page (`start`).
- **Search Google News** (`google_news`): articles with their source and date, the newest first if asked.
- **Search Google Maps** (`google_maps`): places with address, phone, website, rating, reviews and hours; around a
  point if asked.
- **Get a place's Google reviews** (`google_maps_reviews`): by the place's `place_id` or `data_id`, newest or best
  first; the answer's `serpapi_pagination.next_page_token` asks the next page.
- **Search Google Shopping** (`google_shopping`): products in a price range, the cheapest first if asked.
- **Search Google Jobs** (`google_jobs`): openings near a place, with how to apply.
- **Search Google Images** (`google_images`): 100 images a page.
- **Check the account** (`get_account`): the plan and the searches left. Its answer also holds the API key and the
  account's email; the step gives neither.

Each search is one page and spends one of the account's searches (a search SerpApi kept from the last hour is free);
none changes anything, so a preview runs it for real. A search Google has no results for gives none, not a failure.

## Triggers

- **A new news article** (`new_news_article`): each article Google News finds for `q`, the newest asked for every
  hour; each look spends a search. The first look notes what is there and runs nothing.

## Not done

- SerpApi's other engines (Bing, YouTube, Amazon, eBay, Google Scholar, Trends, Flights, Hotels ...), the locations
  list, and searches sent now and read later (`async` and the Searches Archive).
- Results as HTML or Markdown (`output`).

## Tested

Recorded exchanges written from the API reference with made-up results (`tests/`): every action, a search with no
results, the account without its key, an account out of searches refused (429), and the trigger's first poll and new
articles run oldest first. Not run against a live SerpApi account.

```sh
openduct app check apps/serpapi --bar 80
openduct app test apps/serpapi
```
