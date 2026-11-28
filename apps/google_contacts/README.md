# Google Contacts

The people in a Google account's [contacts](https://contacts.google.com), and their groups (labels), through
Google's People API.

Written by hand from Google's discovery document for the People API,
<https://people.googleapis.com/$discovery/rest?version=v1> (revision 20261122, fetched 2026-11-27), and its
[reference pages](https://developers.google.com/people/api/rest) (CC BY 4.0). Labels and descriptions are our own
words.

## Registering this installation with Google (once, by its admin)

1. In the [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and turn on the
   **People API**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scope
   `https://www.googleapis.com/auth/contacts`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Contacts**. One OAuth client
   serves every Google app of this installation: give it each app's scope.

## Connecting an account

Under Credentials, add a Google Contacts credential and **Connect**.

## Actions

Each contact is given plainly - `name`, `given_name`, `family_name`, `email` and `emails`, `phone` and `phones`,
`company`, `job_title`, `notes`, `groups`, its `resource_name` (`people/...`) and `etag` - with Google's whole record
beside them as `person`.

- **List contacts** (`list_contacts`): every contact, most recently changed first.
- **Find contacts** (`search_contacts`): those whose name, email, phone or company starts with the words given (at
  most 30; Google's index of a new contact may take a moment).
- **Look up a contact** (`get_contact`).
- **Add a contact** (`create_contact`): a name, email, phone, company, job title and notes.
- **Change a contact** (`update_contact`): the fields given replace the contact's own; Google asks for the `etag` that
  Look up a contact gave, and refuses the change if the contact changed meanwhile.
- **Delete a contact** (`delete_contact`).
- **List contact groups** (`list_groups`), **Make a contact group** (`create_group`), **Add contacts to a group**
  (`add_to_group`; `contactGroups/starred` stars them), **Take contacts out of a group** (`remove_from_group`).

A contact is named by its resource name (`people/c123...`) or by its id alone; a group by `contactGroups/...` or its
id.

## Triggers

- **A new contact** (`new_contact`) and **A contact added or changed** (`changed_contact`): asked for every five
  minutes, most recently changed first.

## Not done

- Addresses, birthdays, websites and the other fields of a contact are read (in `person`) but not yet written.
- Photos, the "Other contacts" Google keeps from mail, and the directory of a Workspace domain.

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`), with made-up people on reserved
domains. Not run against a live Google account.

```sh
openduct app check apps/google_contacts
openduct app test apps/google_contacts
```
