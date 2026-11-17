# Gmail

Mail sent from a [Gmail](https://mail.google.com) or Google Workspace address, and mail found.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Gmail API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/gmail.send and https://www.googleapis.com/auth/gmail.readonly`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Gmail**. One OAuth client serves every
   Google app of this installation: give it each app's scope, and the same ID and secret to each.

Reading mail is a scope Google reviews before an External app may use it with anyone's account; for your own Workspace (Internal), it is used at once. To only send, give the OAuth consent screen gmail.send alone.
## Connecting an account

Under Credentials, add a Gmail credential and **Connect**.

## Actions

- **Send an email** (`send_email`): to one or more addresses, a subject and the text (plain text).
- **Find emails** (`find_emails`): as Gmail searches (`from:shop@example.com newer_than:7d`); every page.
- **Read an email** (`read_email`).

## Tested

Recorded exchanges from Google's documented requests and answers (`tests/`).

Run against a Google account on 2026-11-16: Find emails (every page) and Read an email worked. Send was not tried (it sends mail).
