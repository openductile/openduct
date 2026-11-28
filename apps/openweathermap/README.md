# OpenWeatherMap

The weather for any place from [OpenWeather](https://openweathermap.org): now, every 3 hours for 5 days, day by day
and hour by hour, the air's quality, and national weather services' alerts - such as a delivery round warned of
storms, or a café's terrace staffed by tomorrow's forecast.

## Signing in

- **An API key** (`openweathermap`): from your OpenWeather account, **API keys**.

The weather now, the 5-day forecast, air quality and finding places come with every OpenWeather account, the free one
too. The day-by-day and hour-by-hour forecasts and the alerts are OpenWeather's **One Call API**, a subscription of its
own (1,000 calls a day free, then paid by the call): a key without it is refused (401).

## Actions

The weather actions take a latitude and longitude: **Find a place by name** or **Find a place by postcode** gives
them. Temperatures are in °C and wind in m/s unless `units` says `imperial` (°F, mph) or `standard` (kelvin);
descriptions are in English unless `lang` names another language.

- **Find a place by name** (`find_place`): up to 5 places with the name (`London`, `London,GB`, `Austin,TX,US`).
- **Find a place by postcode** (`find_place_by_zip`): `90210,US`, `E14,GB`.
- **Name the place at a point** (`place_at`).
- **The weather now** (`current_weather`, the default).
- **Forecast for 5 days** (`forecast`): every 3 hours.
- **Air quality now** (`air_quality`) and **Air quality forecast** (`air_quality_forecast`, hour by hour over 4
  days): the index from 1 (good) to 5 (very poor), and each pollutant.
- **Forecast day by day** (`daily_forecast`, One Call): the next 10 days.
- **Forecast hour by hour** (`hourly_forecast`, One Call): the next 48 hours.
- **Get a weather alert** (`get_alert`, One Call): one alert in full.

## Triggers

- **A weather alert for a place** (`new_alert`, One Call), asked every 30 minutes: each new alert a national weather
  service issues for the latitude and longitude, read in full (who issued it, from when to when, what it says).

## Not done

- History (One Call's timelines into the past, the history and statistics products), the minute-by-minute and
  15-minute forecasts, the hourly 4-day, daily 16-day and 30-day forecast products, weather maps,
  road risk, fire weather, solar and agricultural products, bulk files.
- Places by OpenWeather's old city ids or the built-in name search, which OpenWeather has deprecated: find the place
  first.

## Source

Written by hand from OpenWeather's API reference pages, read on 2026-11-27: [current weather](https://openweathermap.org/current),
[5 day / 3 hour forecast](https://openweathermap.org/forecast5),
[geocoding](https://openweathermap.org/api/geocoding-api),
[air pollution](https://openweathermap.org/api/air-pollution) and
[One Call API 4.0](https://openweathermap.org/api/one-call-4) (OpenWeather's documentation; no open licence stated).
OpenWeather publishes no OpenAPI description. Descriptions are our own words.

## Tested

Recorded exchanges from OpenWeather's documented requests and answers (`tests/`), with made-up values. Not run against
a live OpenWeather account.
