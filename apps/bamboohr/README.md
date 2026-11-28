# BambooHR

Employees, time off, timesheets and hiring in [BambooHR](https://www.bamboohr.com).

## Source

Started with `openduct app from-openapi` from BambooHR's own API description,
<https://github.com/BambooHR/bhr-api-python/blob/9dfad95472650b7d84c42e444c7d4844e50bafe4/specs/public.yaml>
(BambooHR's official SDK repository, MIT licence; generated 2026-04-27, fetched 2026-11-27), then finished by hand
against the API reference, <https://documentation.bamboohr.com/reference>, and its webhooks and sign-in pages.

## Signing in

**An API key** (a BambooHR credential): in BambooHR, click your name in the lower left corner, then **API Keys**, and
add one. Give it with the company's name in its address (`mycompany` of `mycompany.bamboohr.com`). BambooHR takes
the key as the user name of HTTP basic sign-in; a request acts as the person whose key it is, and sees and changes
only what they may. Every request asks for JSON.

## Actions

- **List employees** (`list_employees`): the company directory.
- **Get an employee** (`get_employee`): the fields named, or the usual ones; **List fields** (`list_fields`) gives
  every field's name.
- **Add an employee** (`create_employee`) and **Change an employee** (`update_employee`).
- **List employees changed since a time** (`list_changed_employees`).
- **Get an employee's table rows** (`get_table_rows`) and **Add a row to an employee's table** (`add_table_row`):
  job history, pay, employment status.
- **List time off requests** (`list_time_off_requests`), **Request time off** (`request_time_off`), **Approve or
  deny a time off request** (`set_time_off_status`), **List who's out** (`whos_out`), **List time off types**
  (`list_time_off_types`), **List time off policies** (`list_time_off_policies`), **Get an employee's time off
  balances** (`get_time_off_balances`).
- **List an employee's files** (`list_employee_files`).
- **Get the company's details** (`get_company`).
- **List job applications** (`list_applications`, every page), **Get a job application** (`get_application`),
  **Move a job application to a status** (`set_application_status`), **Comment on a job application**
  (`comment_on_application`), **List job openings** (`list_job_openings`).
- **List timesheet entries** (`list_timesheet_entries`), **Clock an employee in** (`clock_in`) and **out**
  (`clock_out`).

## Triggers

- **A new employee** (`new_employee`) and **An employee changed** (`changed_employee`), asked every 5 minutes: BambooHR
  is asked what changed in the week before each poll, and each employee (or each change, for the second) runs once.
  The run gets the employee's ID, what happened and when: a **Get an employee** step reads the rest.
- **A new job application** (`new_application`), optionally for one job: the newest asked for first.

## Not done

- **Sign in with BambooHR (OAuth).** BambooHR's sign-in and token pages are on each company's own address
  (`https://<company>.bamboohr.com/authorize.php`), so the address must come from the company's name before the
  sign-in starts; OpenDuct's OAuth sign-in takes fixed addresses. The API key does everything here.
- **BambooHR's webhooks.** They are signed (HMAC-SHA256 over the body followed by the `X-BambooHR-Timestamp` header),
  but BambooHR does not say what form that time takes, and a signed time must be read as one to be checked. The
  triggers poll instead.
- Files: uploading and downloading them, and employee photos (not JSON).
- New candidates and job openings (their forms are multipart), benefits, payroll, goals, training, compensation
  planning, custom reports and datasets: 164 more operations in the description are not actions here.

## Tested

Recorded exchanges made from BambooHR's description and reference pages (`tests/`), with made-up data. Not run
against a live BambooHR account.

Triggers are tested with recorded exchanges (`tests/trigger_*.yaml`); none was run against a live account.
