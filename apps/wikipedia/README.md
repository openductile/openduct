# Wikipedia

Search [Wikipedia](https://www.wikipedia.org) in any of its languages and read what it holds: an article's summary,
its text (as wikitext or HTML), its changes, the same article in other languages, its files.

Written by hand from the API descriptions Wikipedia itself serves, fetched 2026-11-27: the MediaWiki REST API at
<https://en.wikipedia.org/w/rest.php/specs/v0/module/-> (its description is under CC BY-SA 4.0) and the Wikimedia REST
API at <https://en.wikipedia.org/api/rest_v1/?spec> (Apache-2.0), for the summary; errors from
<https://schema.wikimedia.org/repositories/mediawiki/api/fragments/error-v1.json>. Only their facts are used, in our
own words. The API reference: <https://www.mediawiki.org/wiki/API:REST_API/Reference>.

## Signing in

No account is needed to read Wikipedia. A credential holds:

- **A contact**: an email or web address Wikimedia can reach you at. Wikimedia's rules ask every program to say who it
  is and how to reach its owner; it is sent with each request, as `OpenDuct-workflow/1.0 (<contact>)` in the
  User-Agent, and a program that says nothing may be slowed down or turned away.
- **A language** (optional): the Wikipedia to read by default, by its code (`en`, `fr`, `de`, `ja` ...). Each action
  may name another; English when neither does.
- **A Wikimedia API access token** (optional), for a higher rate than reading without one: an OAuth 2.0 access token
  of your own Wikimedia account, sent as a bearer token.

## Actions

- **Search articles** (`search_articles`): by words in the title or text, the best first, with a piece of each one's
  text. **Find articles by title** (`suggest_titles`): titles that begin with what is typed.
- **Read an article's summary** (`get_summary`, the default): its short description, first paragraph as plain text,
  picture and address.
- **Look up an article** (`get_article`), **Read an article's wikitext** (`get_article_source`), **Read an article as
  HTML** (`get_article_html`).
- **List an article's changes** (`get_history`: newest first, every page up to 200), **Look up a revision**
  (`get_revision`), **Compare two revisions** (`compare_revisions`).
- **List an article's languages** (`get_languages`), **List an article's files** (`get_article_files`), **Look up a
  file** (`get_file`).

Titles are given as Wikipedia writes them, with spaces or underscores (`Solar_System`). What Wikipedia gives is under
its licence (CC BY-SA): a workflow that republishes it says where it came from.

## Not done

- Editing articles: it needs a Wikimedia account signed in with OAuth, and Wikipedia's rules on automated edits (bot
  approval) apply - left out on purpose.
- Wikipedia's featured content, "on this day" and random-article feeds (served elsewhere, by Wikimedia's API portal),
  and the many other Wikimedia sites (Wiktionary, Wikidata, Commons).

## Tested

Recorded exchanges built from the descriptions (`tests/`): each action, another language with a token, the history's
pages by the older address Wikipedia gives, and refusals in each API's words. Not run against Wikipedia.

```sh
openduct app check apps/wikipedia --bar 80
openduct app test apps/wikipedia
```
