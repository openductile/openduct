# Google Translate

Text translated between languages, and the language of a text told, through Google's
[Cloud Translation - Basic](https://cloud.google.com/translate/docs/basic/translating-text) (v2).

Written by hand from Google's discovery document, <https://translation.googleapis.com/$discovery/rest?version=v2>
(revision 20170525, fetched 2026-11-27), and its [reference pages](https://cloud.google.com/translate/docs/reference/rest/v2/translate)
(CC BY 4.0). Labels and descriptions are our own words.

## Signing in

- **An API key** (`google_translate`): in the [Google Cloud console](https://console.cloud.google.com), pick a project
  with billing, turn on the **Cloud Translation API**, then **APIs & Services → Credentials → Create credentials → API
  key**, and restrict the key to the Cloud Translation API. It is sent as the `key` query parameter.
- **Sign in with Google** (`google_translate_oauth`): the installation registers an OAuth client once, as for the
  other Google apps (**Settings → App sign-ins**), with the scope `https://www.googleapis.com/auth/cloud-translation`;
  the person signing in needs access to a project with the Cloud Translation API turned on.

## Actions

- **Translate text** (`translate_text`, the default): one text into another language; the language it is in is told
  when not given. Plain text by default; HTML keeps its tags.
- **Translate several texts** (`translate_texts`): up to 128 at once, in the order given.
- **Tell a text's language** (`detect_language`): the most likely language, and how sure Google is.
- **List languages** (`list_languages`): the languages it translates between, named in a language of your choice.

Google charges for each character translated; see its pricing page.

## Not done

- Cloud Translation - Advanced (v3): glossaries, custom models, document and batch translation. It needs a project
  and region in every address, and a service account; this app keeps to the Basic edition.

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`). Not run against a live Google
Cloud project.

```sh
openduct app check apps/google_translate
openduct app test apps/google_translate
```
