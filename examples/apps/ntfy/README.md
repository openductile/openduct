# ntfy

Push notifications to your phone or desktop through [ntfy](https://ntfy.sh): on ntfy.sh, or on a server of your own.
It is the app [the tutorial](../../../docs/tutorials/build-an-app.md) builds, step by step.

## Signing in

Sign up at ntfy.sh, then **Account → Access tokens → Create access token**. Put the token in an ntfy credential;
for a server of your own, give its address too. On your phone, install the ntfy app and subscribe to a topic.

## Actions

- **Send a notification** (`send`): to a topic, with an optional title, priority (1 to 5) and an address to open.

## Using it

OpenDuct carries this app as an example, not built in: `openduct start --apps-dir examples/apps` adds it to the
studio.

## Tested

Recorded exchanges from ntfy's documented requests and answers (`tests/`). Not yet run against a live account.
