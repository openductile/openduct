# Coda

Docs, pages and tables in [Coda](https://coda.io), now called Superhuman Docs: rows listed, added, changed and
removed; buttons pushed; a doc's automations run.

Made with `openduct app from-openapi` from Coda's own API description,
<https://docs.superhuman.com/apis/v1/openapi.json> (Coda API 1.7.0, OpenAPI 3.0), fetched on 2026-11-27
(the same file as <https://coda.io/apis/v1/openapi.yaml>). Its licence is the vendor's developer terms
(<https://docs.superhuman.com/trust/developer>), not an open licence: the hosts, paths and fields are taken from it, and
every label and description here is in our own words. The vendor's API reference:
<https://docs.superhuman.com/developers/apis/v1>.

## Signing in

- **An API token** (`coda`): made on the **My account** page in Coda (<https://docs.superhuman.com/account>). A token can be limited to
  one doc or table, and to reading or writing only; it is sent as `Authorization: Bearer`.

## Good to know

- Changes (adding, changing and removing rows, pages and docs) are queued by Coda and made a few seconds later: each
  gives a `requestId`, and **Check that a change is done** (`get_mutation_status`) says when it is.
- Ids are better than names for tables and columns: a name changes when someone renames it.

## Actions

- **List docs** (`list_docs`): The docs you can open and have opened at least once, every page.
- **List pages** (`list_pages`): The pages of a doc, every page of results.
- **List tables** (`list_tables`): The tables and views of a doc.
- **List named formulas** (`list_formulas`): The named formulas of a doc.
- **List controls** (`list_controls`): The controls on a doc's pages: sliders, pickers, checkboxes and the like.
- **Create a doc** (`create_doc`): A new doc: empty, or a copy of another.
- **List a table's columns** (`list_columns`): The columns of a table or view: names, formats and formulas.
- **List a table's rows** (`list_rows`): The rows of a table or view, every page; only those with a value in a column, if asked.
- **Look up a doc** (`get_doc`): A doc's details: its name, owner, size, folder and when it was changed.
- **Add a page** (`create_page`): A new page in a doc, at the top or under another page.
- **Look up a page** (`get_page`): A page's details: its name, where it sits, its sub-pages and who changed it last.
- **Look up a table** (`get_table`): A table's or view's details: its display column, number of rows, sorts and filter.
- **Add or update rows** (`upsert_rows`): Adds rows to a table; given key columns, a row whose keys match one already there changes that one instead.
- **Look up a named formula** (`get_formula`): A named formula and its value now.
- **Look up a control** (`get_control`): A control: its kind and its value now.
- **Rename a doc** (`update_doc`): Changes a doc's title or icon.
- **Look up a row** (`get_row`): One row's values, and when it was added and last changed.
- **Look up a column** (`get_column`): A column's details: its format, formula and default value.
- **Change a page** (`update_page`): Changes a page's name, subtitle, icon, cover or content, or hides it.
- **Run an automation** (`run_automation`): Starts one of the doc's automations that runs when called (a webhook rule), with what is sent to it.
- **Check that a change is done** (`get_mutation_status`): Coda makes changes a moment after it is asked: this says whether the one with this request id is done.
- **Change a row** (`update_row`): Changes some cells of one row.
- **Push a button** (`push_button`): Pushes the button in a button column, on one row: what the button does is done.
- **Delete rows** (`delete_rows`): Removes the rows with these ids from a table.
- **Delete a row** (`delete_row`): Removes one row from a table.

## Triggers

- **A new row** (`new_row`): Each row added to a table, asked for every few minutes. The table is read from its first row, so it watches tables of up to 5,000 rows.
- **A row added or changed** (`changed_row`): Each row added to a table, or whose cells were changed (not by a formula), asked for every few minutes; for tables of up to 5,000 rows.

Both are asked every 5 minutes by default, with the values named by column name.

## Not done

- 99 more operations the description offers (sharing, publishing, folders, Packs, analytics); a custom app or a later version can add them.
- list_tables: only one kind of table can be asked for at a time (the description's tableTypes list is sent once).
- The triggers read a table from its first row each time (Coda lists rows oldest first, and its sync token is not kept between polls): they suit tables of up to 5,000 rows.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Coda account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Coda documents it; none was run against a live account.

```sh
openduct app check apps/coda --bar 80
openduct app test apps/coda
```
