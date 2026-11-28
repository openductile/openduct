# Gotify

Push notifications from your own [Gotify](https://gotify.net) server to its phone and web apps, and the messages it
holds.

Written from Gotify's own API description, `docs/spec.json` of gotify/server v3.1.1
(<https://github.com/gotify/server/blob/e6361caa512547052b1013dbf4544e6d7ff32604/docs/spec.json>; Swagger 2.0, MIT
licence), with `openduct app from-openapi`'s reading of it as the start; names, labels and descriptions are our
own. The vendor's API reference: <https://gotify.net/api-docs>.

## Signing in

Gotify runs on your own server: each credential gives its address (such as `https://gotify.example.com`), and the
app reaches only that. Gotify has two kinds of token, both sent as `X-Gotify-Key`:

- **An application's token** (`gotify`): it can only send messages. In Gotify's web UI, **Apps → Create
  Application**, then copy its token.
- **A client's token** (`gotify_client`): it reads and removes messages and manages applications, and sends as any
  of your applications (name it with `app_id`). In the web UI, **Clients → Create Client**, then copy its token.

## Actions

- **Send a notification** (`send_message`): a message (Markdown allowed), a title and a priority (0 to 10) if given;
  extras for the apps that show it.
- **List messages** (`list_messages`) and **List an application's messages** (`list_app_messages`): the newest
  first, every page (up to 2,000).
- **Remove a message** (`delete_message`), **Remove an application's messages** (`delete_app_messages`), **Remove
  every message** (`delete_all_messages`).
- **List applications** (`list_applications`), **Add an application** (`create_application`), **Change an
  application** (`update_application`), **Remove an application** (`delete_application`).
- **Get the signed-in user** (`get_current_user`).
- **Check the server's health** (`get_health`) and **Get the server's version** (`get_version`): these need no token.

## Triggers

- **A new message** (`new_message`), asked every minute with a client's token: each message any application sends.

## Not done

- The live stream of new messages (`/stream`): a websocket; the trigger polls instead.
- An application's image (a file upload), users, clients, plugins and sign-in sessions: the server's administration,
  done in Gotify itself.

## Tested

Recorded exchanges made from the API description's schemas (`tests/`), with made-up values: both kinds of token, a
list over two pages, the trigger's poll. Not run against a live Gotify server.

```sh
openduct app check apps/gotify --bar 80
openduct app test apps/gotify
```
