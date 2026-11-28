# CoinGecko

Prices, market data and history of crypto coins and tokens, exchanges and categories, from
[CoinGecko](https://www.coingecko.com).

## Signing in

- **A Demo API key (free)** (`coingecko`): in CoinGecko's **Developer Dashboard**
  (<https://www.coingecko.com/en/developers/dashboard>), **Add new key**. Sent as `x-cg-demo-api-key` to
  `api.coingecko.com`.
- **A paid plan's API key (Pro)** (`coingecko_pro`): a key of a paid plan, from the same dashboard. Sent as
  `x-cg-pro-api-key` to `pro-api.coingecko.com` (a Pro key is refused at the free address, and the other way round).

Some answers (hourly history beyond 90 days, for one) need a paid plan: CoinGecko refuses them with a Demo key, and
the step fails in its words.

## Actions

- **Price of coins** (`get_price`): by ids, symbols or names, in one or more currencies; with market cap, 24-hour
  volume and change if asked. The answer is keyed by coin, then currency: `result.bitcoin.usd`.
- **Price of tokens by contract address** (`get_token_price`): on a blockchain, keyed by address.
- **Currencies prices can be given in** (`list_currencies`), **List all coins** (`list_coins`), **List
  blockchains** (`list_platforms`): the ids the other actions take.
- **Coins with their market data** (`list_coin_markets`): ranked by market cap (or volume, or id), of a category or
  the coins given; up to 1,000 coins (4 pages of 250).
- **Get a coin** (`get_coin`), **Get a coin by contract address** (`get_coin_by_contract`).
- **A coin on a past date** (`get_coin_history`), **A coin's price history** (`get_coin_chart`), **between two
  times** (`get_coin_chart_range`), **A coin's candles** (`get_coin_ohlc`, each candle a record of time, open, high,
  low and close).
- **Where a coin trades** (`list_coin_tickers`).
- **Search coins, exchanges and categories** (`search`), **Trending coins** (`trending`).
- **The whole crypto market** (`global_market`), **Bitcoin's exchange rates** (`exchange_rates`).
- **Coin categories** (`list_categories`), **List exchanges** (`list_exchanges`), **Get an exchange**
  (`get_exchange`).
- **Is CoinGecko up** (`ping`).

## Source

Facts (hosts, paths, parameters, keys) from CoinGecko's own API descriptions,
<https://docs.coingecko.com/openapi-specs/demo-api.json> and <https://docs.coingecko.com/openapi-specs/pro-api.json>
(OpenAPI 3.0, fetched 2026-11-27; no licence stated, so the labels and descriptions here are our own words), and the
pages on errors and webhooks at <https://docs.coingecko.com>. Written by hand after `openduct app from-openapi` showed
the operations.

## Not done

- No trigger. CoinGecko's webhooks (paid plans, beta) sign `{timestamp}:{event id}:{body}`, the event id taken from
  the `x-cg-event-id` header: the app kit's signature can name the time and the body but not another header, so a
  webhook trigger waits for that. They are set up in CoinGecko's dashboard, not through the API.
- The onchain (GeckoTerminal) data, NFTs, derivatives, public treasuries, tokenised real-world assets, and the
  Pro-only actions (top gainers, new coins, supply charts).
- A key in the address (`x_cg_demo_api_key=`), which the API also takes: the header is used.

## Tested

Recorded exchanges (`tests/`) made from the API descriptions and their example answers, with made-up values. Not run
against a live CoinGecko account.

```sh
openduct app check apps/coingecko --bar 80
openduct app test apps/coingecko
```
