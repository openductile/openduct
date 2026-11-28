# uProc

[uProc](https://uproc.io)'s data tools: small tools that check, clean and fill in data - such as a company found by
its name, or a card number's checksum checked. List data tools gives the whole catalogue.

Written by hand from uProc's external API reference, <https://docs.uproc.io/api/> (fetched 2026-11-27): the
data-process tools' catalogue, a tool's description, and running one (`POST
/api/v1/external/modules/data-process/actions/execute` with `processor` and `params`). Labels and descriptions are
our own words.

## Signing in

- **A user API key** (`uproc`): the key of your uProc user, sent as `Authorization: Bearer`, as the reference's
  Authentication section says.
- **A customer API key** (`uproc_customer`): a customer's key with its domain and the email of the user it acts as,
  sent as `x-api-key`, `x-customer-domain` and `x-user-email`, as the reference's examples send them.

## Actions

- **Run a data tool** (`run_tool`, the default): a tool by its key, with its values by name - such as
  `get-company-by-name` with `{"name": "..."}`. It gives uProc's answer: `success`, `message`, and `data`, what the
  tool found. Each run costs credits (Look up a data tool says how many).
- **List data tools** (`list_tools`): the catalogue, each tool's key, what it does and the values it takes; listing
  costs nothing.
- **Look up a data tool** (`get_tool`): the values a tool takes and their forms, what it gives, and its cost.
- **Look up your profile** (`get_profile`): the user the key belongs to, to check a credential works.

## Not done

- Searching the catalogue by words: the reference does not name the search's parameter, so List data tools lists
  them all.
- Batch runs over a Data Management table, and uProc's other modules (invoices, leads, orders...).

## Tested

Recorded exchanges built from the reference's documented requests and answers (`tests/`), with made-up data. Not run
against a live uProc account.

```sh
openduct app check apps/uproc
openduct app test apps/uproc
```
