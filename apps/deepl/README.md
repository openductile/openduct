# DeepL

Translate text, correct and improve writing, and keep glossaries, with [DeepL](https://www.deepl.com).

Made by `openduct app from-openapi` from DeepL's own API description,
<https://github.com/DeepLcom/openapi/blob/2b5e50f83888/openapi.yaml> (MIT licence; DeepL API Documentation 3.14.0,
OpenAPI 3.0), fetched 2026-11-27, then finished by hand. The vendor's API reference:
<https://developers.deepl.com/api-reference>.

## Signing in

- **An API key** (`deepl`): in the DeepL account, **API Keys**, copy a key. It is sent as
  `Authorization: DeepL-Auth-Key <key>`. A key of DeepL API Free ends in `:fx` and is taken only at
  `api-free.deepl.com`, so such a key's requests go there; every other key's go to `api.deepl.com`.

## Actions

- **Translate text** (`translate_text`, the default): one text, or a list of them, into `target_lang` (`DE`, `FR`,
  `EN-US`, `PT-BR` ...), from `source_lang` or the language DeepL finds. With a glossary, a style rule list, a
  translation memory, custom instructions, formality, and markup (`tag_handling: html`) kept as it is.
- **Improve writing** (`improve_text`) and **Correct spelling and grammar** (`correct_text`): DeepL Write, for one
  text or a list.
- **Check usage and limits** (`get_usage`) and **List languages** (`get_languages`, for text translation unless
  `resource` says another).
- **Glossaries**: list (`list_glossaries`), look up (`get_glossary`), get one language pair's entries
  (`get_glossary_entries`), create (`create_glossary`), change (`update_glossary`), replace a pair's entries
  (`replace_glossary_entries`, tab-separated lines by default) and remove (`delete_glossary`).
- **Check a document's translation** (`get_document_status`): for a document sent to DeepL elsewhere.
- **List style rule lists** (`list_style_rules`) and **List translation memories** (`list_translation_memories`).

Translating, improving and correcting change nothing in the account, so a preview runs them for real (and counts
their characters); the glossary changes are writes, which a preview shows and does not send.

## Not done

- Sending a document to translate, and downloading the translation: a file upload (multipart) and a file answer.
- The admin pages (developer keys, analytics), spoken terms, voice, style profiles and changes to style rules and
  translation memories: left out of this app (48 more operations the description holds).
- Two deprecated operations (the v2 languages and glossary language pairs).
- `get_languages`' `include` list (asked once for each value) is not offered.

## Tested

Recorded exchanges (`tests/`), first made from the API description's schemas and then written by hand with made-up
values: every action, a Free key sent to `api-free.deepl.com`, two pages of style rule lists, and the character limit
reached (456) refused in DeepL's words. Not run against a live DeepL account.

```sh
openduct app check apps/deepl --bar 80
openduct app test apps/deepl
```
