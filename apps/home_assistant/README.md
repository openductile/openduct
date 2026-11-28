# Home Assistant

A [Home Assistant](https://www.home-assistant.io) installation from a workflow: read any entity's state, turn
lights, switches and other devices on and off, call any service (an integration's action), fire events, set a state
for dashboards, and read history, the logbook and calendars - and start a workflow when an entity changes.

Source: Home Assistant's REST API reference, <https://developers.home-assistant.io/docs/api/rest/>
(`docs/api/rest.md` in <https://github.com/home-assistant/developers.home-assistant> at `50b518898989`, fetched
2026-11-27). That text is CC BY-NC-SA 4.0, so only its facts are used: every word here is our own. The "Entity not
found." refusal is the API's own message (`homeassistant/components/api`). Written by hand.

## Where it reaches: the installation's own address

Each installation has its own address, given in the credential - such as `http://homeassistant.local:8123` or
`http://192.168.1.20:8123` (port 8123, or 80 on Home Assistant OS). Such an address is on the local network, and
OpenDuct reaches the machine's own network only when it is started with **`--allow-private-network`**
([connectors](../../docs/connectors.md#this-machines-own-network)): without it, every step of this app is refused.
An installation reached from the internet (through Home Assistant Cloud or a reverse proxy, over https) needs no
such setting.

## Signing in

- **A long-lived access token** (`home_assistant`): in Home Assistant, open your profile, **Security → Long-lived
  access tokens → Create token**, and copy it (it is shown once). It acts with that person's rights. Give the
  installation's address beside it. The token is sent as a bearer token.

## Actions

- **Check the connection** (`check_api`), **Installation settings** (`get_config`), **Check the configuration**
  (`check_config`).
- **List every entity's state** (`list_states`), **An entity's state** (`get_state`).
- **Turn on**, **Turn off**, **Switch over** (`turn_on`, `turn_off`, `toggle`): any entity that can be switched,
  one or several with commas; they give the states that changed.
- **Call a service** (`call_service`): any integration's service with its data - `light.turn_on` with
  `{entity_id: light.porch, brightness_pct: 50}`, `notify.<device>` with `{message: ...}`,
  `climate.set_temperature` - and, with `return_response`, the service's own answer (a weather forecast, a calendar's
  events).
- **List services** (`list_services`), **Fire an event** (`fire_event`).
- **Set an entity's state as shown** (`set_state`): what Home Assistant shows, not the device (a counter of orders
  on a dashboard, say).
- **An entity's history** (`history`), **Logbook** (`logbook`): over the past day, or from a start.
- **Fill in a template** (`render_template`): Home Assistant's own template language, filled in with its states.
- **List calendars** (`list_calendars`), **A calendar's events** (`calendar_events`).

## Triggers

- **An entity changed** (`state_changed`): one entity's state is asked for every minute, and each update since the
  last poll (its `last_updated` moved) starts a run with the new state and attributes. Changes between two polls
  are seen as one.

## Not done

- Camera pictures (the answer is an image), intents, removing an entity, the error log.
- Home Assistant's WebSocket API, which streams every change as it happens: this app polls.
- Signing in through Home Assistant's own OAuth flow: a long-lived access token is what it takes.

## Tested

Recorded exchanges (`tests/`) for every action, a refusal, and the trigger with a change and without one - made-up
values in the shapes the reference gives. Not run against a live installation.

```sh
openduct app check apps/home_assistant --bar 80
openduct app test apps/home_assistant
```
