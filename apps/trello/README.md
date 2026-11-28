# Trello

Boards, lists and cards of [Trello](https://trello.com): make, move, change and archive cards, comment, label and
assign them, add checklists and links, search, and start a workflow for what is done on a board.

Written by hand from Trello's REST API description, <https://developer.atlassian.com/cloud/trello/swagger.v3.json>
(fetched 2026-11-27; it states no licence, so only its facts are used, in our own words). The API reference:
<https://developer.atlassian.com/cloud/trello/rest/>.

## Signing in

- **An API key and a token** (a Trello credential): at <https://trello.com/power-ups/admin>, make a Power-Up (or open
  one), and under **API key** generate a key; the **Token** link beside it asks you to allow access and gives a token
  for your account. Both go in each request's address (`key`, `token`), as Trello takes them; the token acts as you.

Most actions name a board, list or card by its id: **List your boards**, **List a board's lists** and **List a list's
cards** give them (a card's or board's short link works too).

## Actions

- **Create a card** (`create_card`, the default), **Look up a card** (`get_card`), **Change a card** (`update_card`:
  move it, mark it done, archive it), **Remove a card** (`delete_card`).
- **Comment on a card** (`comment_on_card`), **Label a card** (`add_label`) and **Take a label off** (`remove_label`),
  **Add someone to a card** (`add_member`) and **Take someone off** (`remove_member`).
- **Add a checklist to a card** (`add_checklist`), **Add an item to a checklist** (`add_checklist_item`), **Tick a
  checklist item** (`check_item`), **Attach a link to a card** (`attach_link`).
- **List your boards** (`list_boards`), **Look up a board** (`get_board`), **Create a board** (`create_board`).
- **List a board's lists** (`list_lists`), **Create a list** (`create_list`), **Archive a list** (`archive_list`).
- **List a list's cards** (`list_cards`), **List a board's cards** (`list_board_cards`), **List a board's labels**
  (`list_labels`), **List a board's members** (`list_members`).
- **Search cards** (`search`) and **List a board's activity** (`list_board_activity`).

A refusal fails the step in Trello's few words (`invalid id`, `invalid token`), which it sends as plain text.

## Triggers

- **Something done on a board** (`board_activity`): each card made (or, as chosen, changed or moved, commented on,
  given a member or a checklist ...), from the board's activity every 5 minutes, newest first. The run gets what was
  done, by whom, and the card, list and board.

## Not done

- **Trello's webhooks.** Trello signs its deliveries in a way OpenDuct checks (HMAC-SHA1 of the body and the callback
  address, in base64, `X-Trello-Webhook`), but before it makes a webhook it sends a `HEAD` request to the address and
  wants `200`; OpenDuct's hook addresses answer `GET` and `POST` only, so registering one would be refused. The poll
  above stands in until hook addresses answer `HEAD`.
- File uploads (attachments from a file), custom fields, Power-Up data, stickers, workspaces' own settings, and signing
  in with Atlassian (OAuth 2.0).

## Tested

Recorded exchanges built from the description (`tests/`): each action, refusals as Trello sends them, and the trigger
over a board's activity. Not run against a live Trello account.

```sh
openduct app check apps/trello --bar 80
openduct app test apps/trello
```
