# Harvest

Time tracking in [Harvest](https://www.getharvest.com): time logged, changed, deleted, and timers started and
stopped; projects and their tasks, clients and their contacts, tasks, people, invoices and expenses.

Written by hand from Harvest's API v2 reference, <https://help.getharvest.com/api-v2/> (read on 2026-11-27): Harvest
publishes no API description file. Labels and descriptions are our own words.

## Signing in

Every request names the Harvest account (`Harvest-Account-Id`) and, as Harvest asks, the application and a way to
reach it (`User-Agent: OpenDuct (https://github.com/openductile/openduct)`).

- **A personal access token** (`harvest`): in Harvest ID's **Developers** section
  (<https://id.getharvest.com/developers>), create a personal access token; the page lists the token and your account
  ids. Give both. The token acts as you, in every account you have.
- **Sign in with Harvest** (`harvest_oauth`), OAuth 2.0: the installation registers an OAuth2 application once, in
  the same Developers section, with the callback address shown under **Settings → App sign-ins**
  (`https://<your studio>/oauth/callback`) as its redirect URL and Harvest as its product; it gives a client id and
  secret, entered there. On connecting, the first Harvest account the person allowed is kept for the credential.

## Good to know

- **Log time** takes hours on an account that tracks time by duration, or a start and end time on one that tracks by
  start and end; with neither, the entry's timer starts running.
- Time is logged on a project's task: **List a project's tasks** gives the tasks a project has.
- Harvest allows 100 requests every 15 seconds.

## Actions

- **List time entries** (`list_time_entries`): Time entries, by person, client, project, task, dates or when changed; every page.
- **Look up a time entry** (`get_time_entry`): One time entry.
- **Log time** (`create_time_entry`): A new time entry on a project's task: hours spent (an account that tracks by duration), or a start and end time (one that tracks by start and end); with neither, its timer starts running.
- **Change a time entry** (`update_time_entry`): Changes the fields given of a time entry; the others stay.
- **Delete a time entry** (`delete_time_entry`): Removes a time entry (one not approved, on a project and task not archived).
- **Restart a time entry's timer** (`restart_time_entry`): Starts the timer of a stopped time entry again.
- **Stop a time entry's timer** (`stop_time_entry`): Stops the timer of a running time entry.
- **List projects** (`list_projects`): Projects, active or archived, of a client if given; every page.
- **Look up a project** (`get_project`): One project.
- **Create a project** (`create_project`): A new project for a client, with how it is billed and budgeted.
- **Change a project** (`update_project`): Changes the fields given of a project, or archives it; the others stay.
- **List a project's tasks** (`list_project_tasks`): The tasks assigned to a project (time is logged on these), with their rates and budgets.
- **List clients** (`list_clients`): Clients, active or archived; every page.
- **Add a client** (`create_client`): A new client.
- **List contacts** (`list_contacts`): Clients' contacts, of one client if given; every page.
- **Add a contact** (`create_contact`): A new contact of a client, and whether invoices go to them.
- **List tasks** (`list_tasks`): The account's tasks (the kinds of work time is logged on); every page.
- **Add a task** (`create_task`): A new kind of work to log time on.
- **List people** (`list_users`): The account's people, active or not; every page.
- **Who is signed in** (`get_me`): The person the credential acts as.
- **List invoices** (`list_invoices`): Invoices, by client, project, state or dates; every page.
- **Look up an invoice** (`get_invoice`): One invoice, with its line items.
- **Create an invoice** (`create_invoice`): A new draft invoice for a client, with its line items.
- **List expenses** (`list_expenses`): Expenses, by person, client, project or dates; every page.
- **Log an expense** (`create_expense`): A new expense on a project, in an expense category.

## Triggers

- **A time entry logged or changed** (`time_entry_changed`): Each time entry added or changed, asked for every few minutes (those changed since the poll before, with five minutes to spare for clocks that differ).
- **An invoice created or changed** (`invoice_changed`): Each invoice created or changed (sent, paid, closed), asked for every few minutes; with five minutes to spare for clocks that differ.

Harvest sends no webhooks: both triggers ask, every 5 minutes, for what changed since the poll before (with five
minutes to spare), and run each record that is new or whose `updated_at` moved on.

## Not done

- Estimates, retainers, invoice payments and messages, e-invoices, expense categories, roles, user and project assignments, and reports.
- Receipts sent with an expense (a file upload).
- Forecast, Harvest's planning product, has an API of its own.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Harvest account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Harvest documents it; none was run against a live account.

```sh
openduct app check apps/harvest --bar 80
openduct app test apps/harvest
```
