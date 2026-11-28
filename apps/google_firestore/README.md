# Google Cloud Firestore

Documents in a [Cloud Firestore](https://cloud.google.com/firestore) database (Google Cloud or Firebase): read,
added, changed, queried and counted.

## Source

Written by hand from Google's REST reference for Firestore v1, <https://firebase.google.com/docs/firestore/reference/rest>
(CC BY 4.0; read 2026-11-27), and its discovery document,
<https://firestore.googleapis.com/$discovery/rest?version=v1> (revision 20261109, fetched 2026-11-27). Labels and
descriptions are our own words.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **Cloud Firestore API**.
2. **APIs & Services → OAuth consent screen**: set it up (Internal, for your own Google Workspace; External
   otherwise), with the scope `https://www.googleapis.com/auth/datastore`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Cloud Firestore**. One OAuth
   client serves every Google app of this installation: give it each app's scope.

## Connecting an account

Under Credentials, add a Google Cloud Firestore credential and **Connect**: sign in at Google with an account that
may use the database (a role such as Cloud Datastore User on its project). Each action names the project, and the
database when it is not the `(default)` one.

## Actions

- **Get a document** (`get_document`), **List documents** (`list_documents`, every page), **Add a document**
  (`create_document`), **Change a document** (`update_document`: only the fields given, on a document that is there),
  **Write a whole document** (`set_document`: added, or replaced), **Delete a document** (`delete_document`).
- **Find documents** (`find_documents`): those whose field is equal to, less or greater than, or contains a value, in
  the order of a field, up to a number.
- **Count documents** (`count_documents`): all of a collection's, or those whose field compares to a value.
- **List collections** (`list_collections`): at the top, or under a document.

A collection may be one under a document: `orders/A-100/lines`. Fields are written, and come back, as Firestore keeps
them: each value with its kind, such as `{"total": {"doubleValue": 42.5}, "name": {"stringValue": "Pat"}}`
(`integerValue` is text; also `booleanValue`, `timestampValue`, `mapValue`, `arrayValue`, `nullValue`).

## Triggers

- **A new document** (`new_document`) and **A document added or changed** (`changed_document`), in a collection,
  asked every 5 minutes. The first 5,000 documents of the collection are watched.

## Not done

- Plain values for fields (`{"total": 42.5}`): OpenDuct's expressions cannot turn a record into another record key by
  key, so the kinds are written out.
- Count asks for "up to" the largest number Firestore takes, since an empty record (`count: {}`, which Firestore's
  reference gives for an unbounded count) is left out of a request body by OpenDuct.
- Queries of more than one condition, transactions, batch writes, real-time listening, field transforms
  (increments, server times), and the database admin (indexes, backups, databases).
- Signing in as a service account (a JWT signed with its key).

## Tested

Recorded exchanges from Google's documented requests and answers (`tests/`), with made-up data. Not run against a
live Firestore database.

Triggers are tested with recorded exchanges (`tests/trigger_*.yaml`); none was run against a live account.
