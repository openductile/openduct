# Microsoft 365

Mail and calendar in Outlook, and rows of Excel tables in OneDrive, through
[Microsoft Graph](https://learn.microsoft.com/graph/overview).

## Registering this installation with Microsoft (once, by its admin)

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), **Applications → App registrations → New
   registration**: accounts in any organisation (and personal accounts, if wanted), with a **Web** redirect URI - the
   one shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
2. **API permissions → Add → Microsoft Graph → Delegated**: `offline_access`, `Mail.Send`, `Calendars.ReadWrite`,
   `Files.ReadWrite`.
3. **Certificates & secrets → New client secret**. Copy the **Application (client) ID** and the secret's **Value**
   into **Settings → App sign-ins → Microsoft 365**.

## Connecting an account

Under Credentials, add a Microsoft 365 credential and **Connect**: sign in at Microsoft and allow it.

## Actions

- **Send an email** (`send_email`).
- **List calendar events** (`list_events`): between two times; every page.
- **Add a calendar event** (`add_event`): title, start, end and their time zone.
- **Add a row to an Excel table** (`add_excel_row`): a workbook in OneDrive, a table in it, the row's values.

## Tested

Recorded exchanges from Microsoft's documented requests and answers (`tests/`). Not yet run against a live account.
