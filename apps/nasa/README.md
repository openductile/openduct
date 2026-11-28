# NASA

NASA's open data from a workflow: the astronomy picture of the day, asteroids passing Earth, space weather from
NASA's DONKI database, and the EPIC camera's pictures of the whole sunlit Earth.

Sources (fetched 2026-11-27):

- NASA's open API pages, <https://api.nasa.gov> - their source is <https://github.com/nasa/api-docs> (`gh-pages` at
  `e05c7649a269`), a US Government work; the APOD part of it says the picture of the day moved to
  `science.nasa.gov/wp-json/wp/v2/apod-basic` on 2026-09-10 (the old address goes offline on 2026-12-01), with its
  guide at `assets/files/apod/APOD-APIs-and-RSS-info.pdf` in the same repository.
- The field names of asteroids from NeoWs' own client, <https://github.com/SpaceRocks/neows-ruby> (at `b81655f31357`,
  MIT), by the team NASA names as NeoWs' maintainers.
- Refusals in the words of api.data.gov, which issues api.nasa.gov keys:
  <https://api.data.gov/docs/developer-manual/>.

Written by hand; every label is in our own words.

## Signing in

- **An api.nasa.gov key** (`nasa`): sign up at <https://api.nasa.gov> (a name and an email; the key comes at once).
  It is sent as the `api_key` parameter. `DEMO_KEY` tries the app out, at 30 calls an hour; a key of your own allows
  1,000 an hour.

## Actions

- **Astronomy picture of the day** (`picture_of_the_day`): one day's picture or video, by its date (sent as the feed's
  `YYMMDD` code): title, explanation, credit, `url`, `hdurl` (the full-size image) and an HTML page.
- **Astronomy pictures between two days** (`list_pictures`): every one from a day to another, page after page.
- **Asteroids passing Earth** (`asteroids_by_date`): between two days at most 7 apart: `element_count`, and
  `near_earth_objects` grouped by day, each with its size, whether it is potentially hazardous, and its close
  approaches (speed, miss distance).
- **Look up an asteroid** (`get_asteroid`), **Browse asteroids** (`browse_asteroids`, one page at a time).
- **Space weather events** (`space_weather_events`): one kind from DONKI - `FLR` solar flares, `CME` coronal mass
  ejections (and `CMEAnalysis`), `GST` geomagnetic storms, `IPS` interplanetary shocks, `SEP` solar energetic
  particles, `MPC` magnetopause crossings, `RBE` radiation belt enhancements, `HSS` high speed streams,
  `WSAEnlilSimulations` - between two days.
- **Space weather notifications** (`space_weather_notifications`): the notices sent between two days (at most 30).
- **Pictures of the whole Earth** (`earth_pictures`): a day's EPIC pictures, true color (`natural`) or `enhanced`,
  each with its time, caption and positions.

## Triggers

- **A new astronomy picture of the day** (`new_picture`): asked for every hour; the last three days are listed and a
  picture not seen before starts a run with its title, explanation, credit and image address.

## Not done

- The fields of DONKI's events and notifications are given as DONKI sends them, without a list of them here: its own
  reference (`kauai.ccmc.gsfc.nasa.gov/DONKI/WS/`) could not be reached when the app was written.
- The EPIC image files themselves, the list of EPIC days, EONET, the Exoplanet archive, GIBS map tiles, the image and
  video library, TechTransfer, TLE, Techport and the other services on api.nasa.gov.
- The APOD RSS feed and the future-APOD list.

## Tested

Recorded exchanges (`tests/`) for every action and the trigger, and a refused key - made-up values in the shapes the
sources above give (DONKI as an empty week). Not run against NASA.

```sh
openduct app check apps/nasa --bar 80
openduct app test apps/nasa
```
