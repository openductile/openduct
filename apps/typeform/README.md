# Typeform

Forms made in [Typeform](https://www.typeform.com), and the answers people gave.

## Signing in

In Typeform, **Account → Your settings → Personal tokens → Generate a new token**, with the scopes `forms:read` and
`responses:read`. Put it in a Typeform credential.

## Actions

- **List forms** (`list_forms`): every page.
- **A form's answers** (`list_answers`): since a time; every page, newest first.

## Tested

Recorded exchanges from Typeform's documented requests and answers (`tests/`).

Run against a Typeform account on 2026-11-16: List forms and a form's answers worked.
