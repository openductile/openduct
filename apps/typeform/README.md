# Typeform

Forms made in [Typeform](https://www.typeform.com), and the answers people gave.

## Signing in

In Typeform, **Account → Your settings → Personal tokens → Generate a new token**, with the scopes `forms:read` and
`responses:read`. Put it in a Typeform credential.

## Actions

- **List forms** (`list_forms`): every page.
- **A form's answers** (`list_answers`): since a time; every page, newest first.

## Triggers

- **A form answered** (`new_answer`): each answer given to a form (`form_id`), as Typeform sends it, signed. Turning
  the workflow on adds a webhook to the form with a secret of its own (the token needs webhooks:write); turning it off
  deletes it. The run gets the answer: `answers`, `submitted_at`, `token` and the rest.

## Tested

Recorded exchanges from Typeform's documented requests and answers (`tests/`).

Run against a Typeform account on 2026-11-16: List forms and a form's answers worked.

Triggers are tested with recorded exchanges and signed deliveries (`tests/trigger_*.yaml`), signatures made as the app documents them; none was run against a live account.
