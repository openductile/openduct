# Microsoft 365

Mail and calendar in Outlook, rows of Excel tables in OneDrive, the lists and document libraries of SharePoint
sites, and tasks in Microsoft To Do, through [Microsoft Graph](https://learn.microsoft.com/graph/overview); and, for
the organisation's admins, the users, groups and licences of its directory,
[Microsoft Entra ID](https://learn.microsoft.com/entra/fundamentals/whatis).

The SharePoint actions and triggers are written by hand from Microsoft Graph's v1.0 reference
(<https://github.com/microsoftgraph/microsoft-graph-docs-contrib> at commit `951930b591f9`, CC BY 4.0, read
2026-11-27): sites, lists, list items and drive items. Their descriptions are our own words.

Written by hand from Microsoft's Graph v1.0 reference (<https://learn.microsoft.com/graph/api/overview>); the To Do
actions from its To Do pages (<https://learn.microsoft.com/graph/api/resources/todo-overview>, read 2026-11-27; the
reference is published openly at github.com/microsoftgraph/microsoft-graph-docs-contrib, CC BY 4.0). Descriptions are our own.

The Microsoft Entra ID actions were written by hand from Microsoft Graph's v1.0 API reference,
<https://github.com/microsoftgraph/microsoft-graph-docs-contrib/tree/951930b591f9248117793f036e0b13cb779769d1/api-reference/v1.0/api>
(the commit of 2026-11-26, CC BY 4.0): the users, groups, group members, invitations, licences and managers pages.
Labels and descriptions are our own words.

## Signing in

Three sign-ins, one for each kind of use:

- **Sign in with Microsoft** (`microsoft_365`): a person's own mail, calendar and files (work, school or personal
  account).
- **Sign in as an admin of the organisation** (`microsoft_entra_admin`): the directory's users, groups and licences,
  acting as an admin who signs in (a work account, with an admin role that allows what the workflow does, such as
  User Administrator or Groups Administrator). The organisation's admin consents to the permissions once.
- **An app of the organisation's own** (`microsoft_entra_app`): the same, with no person signed in: an app registered
  in the organisation's own directory, given the application permissions below by an admin; OpenDuct asks Microsoft
  for a token with its id and secret as it needs one.

The directory actions need one of the last two; the mail, calendar, Excel, SharePoint and To Do actions need the first.

## Registering this installation with Microsoft (once, by its admin)

For **Sign in with Microsoft** and **Sign in as an admin of the organisation**:

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), **Applications → App registrations → New
   registration**: accounts in any organisation (and personal accounts, if wanted), with a **Web** redirect URI - the
   one shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
2. **API permissions → Add → Microsoft Graph → Delegated**: `offline_access`, `Mail.Send`, `Mail.Read`,
   `Calendars.ReadWrite`, `Files.ReadWrite`, `Files.ReadWrite.All` (the files of SharePoint sites the person may
   reach, for the storage step and trigger) and `Sites.ReadWrite.All` (SharePoint lists and libraries). None needs
   an admin's consent; an organization may still ask for it. `Tasks.ReadWrite` for Microsoft To Do. For the admin sign-in also `User.ReadWrite.All`,
   `Group.ReadWrite.All`, `GroupMember.ReadWrite.All`, `User.Invite.All`, `User.RevokeSessions.All`,
   `LicenseAssignment.ReadWrite.All` (these need an admin's consent).
3. **Certificates & secrets → New client secret**. Copy the **Application (client) ID** and the secret's **Value**
   into **Settings → App sign-ins**, under each sign-in used.

For **An app of the organisation's own**, the organisation registers an app in its own directory (accounts in this
organisational directory only), adds the same Microsoft Graph permissions as **Application** permissions, has an admin
**Grant admin consent**, makes a client secret, and gives the directory's tenant id, the app's client id and the
secret's value in the credential.

An account connected before `Mail.Read`, the SharePoint permissions and `Tasks.ReadWrite` were asked for connects
again (**Connect**) to allow them.

## Connecting an account

Under Credentials, add a Microsoft 365 credential and **Connect**: sign in at Microsoft and allow it. To Do works
with work or school accounts and personal Microsoft accounts alike.

## Actions

Mail, calendar and Excel (`microsoft_365`):

- **Send an email** (`send_email`): Sends a plain-text email from the signed-in mailbox, and keeps a copy in Sent Items.
- **List calendar events** (`list_events`): between two times, repeating events each time they happen; every page.
- **Add a calendar event** (`add_event`): title, start, end and their time zone.
- **Add a row to an Excel table** (`add_excel_row`): a workbook in OneDrive, a table in it, the row's values.
- **List To Do lists** (`list_task_lists`): the signed-in user's lists, with the id each task action needs; every page.
- **Add a To Do list** (`add_task_list`).
- **List the tasks of a To Do list** (`list_tasks`): every task, or those matching a filter such as
  `status ne 'completed'`; every page.
- **Get a To Do task** (`get_task`).
- **Add a To Do task** (`add_task`): a title, and if given notes, a due date (in a named time zone, UTC by default),
  an importance (`low`, `normal`, `high`) and categories.
- **Change a To Do task** (`update_task`): its title, status (`notStarted`, `inProgress`, `completed`,
  `waitingOnOthers`, `deferred`), importance, due date or categories; what is not given stays.
- **Mark a To Do task done** (`complete_task`).
- **Delete a To Do task** (`delete_task`).

## Files

The **Files on a server or a folder** step (`storage`) and its trigger take a Microsoft 365 credential: files in the
person's OneDrive (`drive: me`, the default), another drive (its id), or a SharePoint site's document library
(`drive: site:<the site's id>`) - see [the studio's guide](../../docs/studio.md#stores-and-drives-over-their-own-apis).

SharePoint - a site is named by its id, which **Look up a SharePoint site** gives from the site's address:

- **Find SharePoint sites** (`find_sites`), **Look up a SharePoint site** (`get_site`: from
  `https://example.sharepoint.com/sites/sales`).
- **List a site's lists** (`list_lists`): lists and document libraries, with their ids.
- **List a SharePoint list's items** (`list_list_items`: every page, a filter such as `fields/Status eq 'Open'` if
  given), **Look up a SharePoint list item** (`get_list_item`), **Add a SharePoint list item** (`add_list_item`),
  **Change a SharePoint list item** (`update_list_item`), **Delete a SharePoint list item** (`delete_list_item`).
  Columns are named by their internal names (`Title`, and the list's own, as in its settings' column links).
- **List files in a SharePoint library** (`list_library_files`: at the top of the site's library or in a folder
  path), **Find files in a SharePoint library** (`find_library_files`), **Get a sharing link to a SharePoint file**
  (`share_library_file`: to view or edit, for the organization or anyone).

Microsoft Entra ID (`microsoft_entra_admin` or `microsoft_entra_app`):

- **List users** (`list_users`), **Look up a user** (`get_user`), **Add a user** (`create_user`), **Change a user**
  (`update_user`: also blocks or allows their sign-in), **Delete a user** (`delete_user`).
- **Sign a user out everywhere** (`sign_user_out`), **List a user's groups** (`list_user_groups`), **Look up a user's
  manager** (`get_manager`), **Set a user's manager** (`set_manager`).
- **Invite a guest** (`invite_guest`).
- **List the organisation's licences** (`list_licenses`), **Give or take away licences** (`assign_licenses`).
- **List groups** (`list_groups`), **Look up a group** (`get_group`), **Create a group** (`create_group`: a Microsoft
  365 or a security group), **Change a group** (`update_group`), **Delete a group** (`delete_group`).
- **List a group's members** (`list_group_members`), **Add a member to a group** (`add_group_member`), **Remove a
  member from a group** (`remove_group_member`).

A search (`search`) of users or groups is sent with `ConsistencyLevel: eventual` and `$count`, as Graph asks.

## Triggers

- **A new email** (`new_email`), asked every 5 minutes: each email that arrives in the inbox (Mail.Read).
- **A new item in a SharePoint list** (`new_list_item`) and **An item added or changed in a SharePoint list**
  (`changed_list_item`), asked every 5 minutes: each poll reads the whole list.
- **A new file in a SharePoint library** (`new_library_file`), asked every 5 minutes: each file put in a folder of
  the site's library (folders put there start nothing).
- **A new To Do task** (`new_task`), asked every 5 minutes: each task added to one list.

## Not done

- Making a folder in a library: Graph asks for an empty `folder: {}` record, and OpenDuct leaves empty records out
  of a request body.
- Uploading a file to a library (Graph takes the file's bytes as the body), and downloading one.
- SharePoint pages, list and column definitions, permissions, Graph's change notifications (webhooks) and delta
  queries.
- To Do's checklist items, linked resources and attachments; tasks of another user (`/users/{id}/todo`, which
  needs application permissions).
- Changing a task's notes: Graph takes them as HTML only on a change.
- Directory changes as they happen: Graph's change notifications check the address by echoing a token as plain text,
  and its delta queries hand back a link to keep; a trigger does neither yet.
- Directory roles, administrative units, devices, app registrations and conditional access.
- Resetting a user's password (it needs a permission of its own and, for admins, a role above theirs).

## Tested

Recorded exchanges from Microsoft's documented requests and answers (`tests/`), with made-up values. Not yet run
against a live account.

Triggers are tested with recorded exchanges (`tests/trigger_*.yaml`); none was run against a live account.
