# Lingvanex

Text and web pages translated into other languages, and the language of a text found, with
[Lingvanex](https://lingvanex.com)'s Translator API.

## Source

Written by hand from Lingvanex's Translator API documentation, <https://docs.lingvanex.com/>, read on 2026-11-27: the
pages Translate Text and Getting the List of Languages (`https://api-b2b.backenster.com/b1/api/v3`), and Detect
Languages (`https://api-gl.lingvanex.com/language/translate/v2/detect`). Lingvanex publishes no machine-readable
description and states no licence for its documentation: labels and descriptions here are our own words.

## Signing in

**An API key** (a Lingvanex credential): in the [Lingvanex dashboard](https://account.lingvanex.com/dashboard/), make
an API key. It is sent as the `Authorization` header, as it is.

## Actions

- **Translate a text** (`translate_text`): into a language given as a language and country code (`de_DE`,
  `es_ES`, `en_GB`), from the one given or the one found; HTML keeps its tags.
- **Translate several texts** (`translate_texts`): in the same order.
- **Translate a text, with its transliteration** (`transliterate`): the text and its translation, each transliterated too.
- **List languages** (`list_languages`): their codes, and names in English or another language.
- **Find the language of a text** (`detect_language`): as a short code (`en`, `es`).

Lingvanex charges by the character translated; the dashboard sets daily and monthly limits.

## Not done

- Anything beyond translating text and finding its language.
- The Google-style translate and language list of `api-gl.lingvanex.com` (the same translations, in another form).

## Tested

Recorded exchanges from Lingvanex's documented requests and answers (`tests/`), with made-up data. Not run against a
live Lingvanex account.
